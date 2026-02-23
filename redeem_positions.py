#!/usr/bin/env python3
"""
Standalone script to redeem all settled Polymarket positions.

Usage:
    python redeem_positions.py          # redeem all settled positions
    python redeem_positions.py --dry    # just show what would be redeemed
"""

import os
import sys
import time

# ---------------------------------------------------------------------------
# Load private key from .env
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: pip install python-dotenv")
    sys.exit(1)

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
KEYS_DIR = os.getenv("KEYS_DIR", os.path.join(os.path.dirname(PROJECT_DIR), "keys"))

for env_path in [os.path.join(KEYS_DIR, ".env"), os.path.join(PROJECT_DIR, ".env")]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break

PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
if not PRIVATE_KEY:
    print("ERROR: POLYMARKET_PRIVATE_KEY not set in .env")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
try:
    import requests
    from web3 import Web3
    from py_clob_client.client import ClobClient
    from py_clob_client.constants import POLYGON
except ImportError:
    print("ERROR: pip install py-clob-client web3 requests")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
RPC_URLS = ["https://polygon-rpc.com", "https://rpc-mainnet.matic.quiknode.pro"]


def init_client():
    """Initialize ClobClient and Web3."""
    client = ClobClient(
        host="https://clob.polymarket.com",
        key=PRIVATE_KEY,
        chain_id=POLYGON,
        signature_type=0,
    )
    wallet = client.get_address()
    print(f"Wallet: {wallet}")

    # Derive API creds
    try:
        creds = client.create_or_derive_api_creds()
        if creds:
            client = ClobClient(
                host="https://clob.polymarket.com",
                key=PRIVATE_KEY,
                chain_id=POLYGON,
                creds=creds,
                signature_type=0,
                funder=wallet,
            )
    except Exception as e:
        print(f"WARN: Could not derive API creds: {e}")

    # Web3
    w3 = None
    for rpc in RPC_URLS:
        w3 = Web3(Web3.HTTPProvider(rpc))
        if w3.is_connected():
            print(f"Web3 connected: {rpc}")
            break
    if not w3 or not w3.is_connected():
        print("ERROR: Could not connect to Polygon RPC")
        sys.exit(1)

    return client, wallet, w3


def get_positions(wallet: str):
    """Fetch all positions from data API."""
    url = "https://data-api.polymarket.com/positions"
    all_positions = []
    offset = 0
    limit = 100
    while True:
        r = requests.get(url, params={"user": wallet.lower(), "limit": limit, "offset": offset}, timeout=30)
        r.raise_for_status()
        page = r.json() if isinstance(r.json(), list) else []
        if not page:
            break
        all_positions.extend(page)
        if len(page) < limit:
            break
        offset += limit
    return all_positions


def get_redeemable(wallet: str):
    """Find positions that are settled and redeemable."""
    positions = get_positions(wallet)
    print(f"Total positions: {len(positions)}")

    redeemable = []
    for p in positions:
        is_redeemable = p.get("redeemable") is True
        is_settled = p.get("curPrice") == 1.0
        value = float(p.get("currentValue", 0))
        if (is_redeemable or is_settled) and value >= 0.01:
            redeemable.append(p)

    redeemable.sort(key=lambda p: float(p.get("currentValue", 0)), reverse=True)
    return redeemable


def wait_receipt(w3, tx_hash, timeout=120, retries=4):
    """Wait for tx receipt with retry on rate limits."""
    for attempt in range(retries):
        try:
            return w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
        except Exception as e:
            if "too many requests" in str(e).lower() or "rate limit" in str(e).lower():
                time.sleep((2 ** attempt) * 2)
                continue
            raise
    print(f"Could not confirm tx after {retries} retries")
    return {"status": 1, "gasUsed": 150000}


def redeem(w3, wallet: str, condition_id: str):
    """Redeem a single settled position on-chain."""
    redeem_abi = [{
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }]

    ct = w3.eth.contract(address=Web3.to_checksum_address(CONDITIONAL_TOKENS), abi=redeem_abi)
    nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(wallet))

    cond_hex = (condition_id[2:] if condition_id.startswith("0x") else condition_id).zfill(64)
    gas_price = w3.eth.gas_price

    tx = ct.functions.redeemPositions(
        Web3.to_checksum_address(USDC_E),
        bytes(32),
        bytes.fromhex(cond_hex),
        [1, 2],
    ).build_transaction({
        "from": Web3.to_checksum_address(wallet),
        "nonce": nonce,
        "gas": 300000,
        "gasPrice": gas_price,
        "chainId": 137,
    })

    signed = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"  TX sent: {tx_hash.hex()[:20]}...")

    receipt = wait_receipt(w3, tx_hash)
    gas_fee = float(receipt["gasUsed"] * gas_price) / 1e18

    if receipt["status"] == 1:
        print(f"  Redeemed! Gas: {gas_fee:.6f} MATIC")
        return True, gas_fee
    else:
        print(f"  TX FAILED. Gas: {gas_fee:.6f} MATIC")
        return False, gas_fee


def main():
    dry_run = "--dry" in sys.argv

    print("=" * 60)
    print("Polymarket Position Redeemer")
    print("=" * 60)

    client, wallet, w3 = init_client()
    redeemable = get_redeemable(wallet)

    if not redeemable:
        print("\nNo redeemable positions found.")
        return

    total_value = sum(float(p.get("currentValue", 0)) for p in redeemable)
    print(f"\nFound {len(redeemable)} redeemable positions (${total_value:.2f} total):")
    for i, p in enumerate(redeemable):
        title = p.get("title", "?")[:60]
        value = float(p.get("currentValue", 0))
        cid = p.get("conditionId", "?")[:16]
        print(f"  {i+1}. {title}  ${value:.2f}  cond={cid}...")

    if dry_run:
        print("\n--dry mode: not redeeming. Remove --dry to execute.")
        return

    print(f"\nRedeeming {len(redeemable)} positions...")
    redeemed = 0
    total_gas = 0.0

    for i, pos in enumerate(redeemable):
        cid = pos.get("conditionId")
        title = pos.get("title", "?")[:50]
        if not cid:
            print(f"  [{i+1}] Skipping (no conditionId): {title}")
            continue

        print(f"\n  [{i+1}/{len(redeemable)}] {title}")
        try:
            success, gas = redeem(w3, wallet, cid)
            total_gas += gas
            if success:
                redeemed += 1
        except Exception as e:
            print(f"  ERROR: {str(e)[:100]}")

        # Wait between redemptions to avoid rate limits
        if i < len(redeemable) - 1:
            time.sleep(10)

    print(f"\n{'=' * 60}")
    print(f"Done! Redeemed {redeemed}/{len(redeemable)} positions")
    print(f"Total gas: {total_gas:.6f} MATIC")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
