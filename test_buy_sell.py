#!/usr/bin/env python3
"""Quick buy+sell test: buys 5 shares of the cheaper BTC side, then sells them.
Proves the full order lifecycle works (buy fill -> sell fill).
Calls the CLOB directly so all errors are visible (not swallowed).

Usage:  python test_buy_sell.py
"""
import os, sys, time
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3

# Load .env same as the bot
_PROJECT_DIR = Path(__file__).resolve().parent
_KEYS_DIR = os.getenv("KEYS_DIR", str(_PROJECT_DIR.parent / "keys"))
for env_path in [os.path.join(_KEYS_DIR, ".env"), str(_PROJECT_DIR / ".env")]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break

# Force LIVE mode so the client actually places orders
os.environ["MODE"] = "LIVE"

from src.feeds.polymarket import PolymarketClient
from py_clob_client.clob_types import OrderArgs, OrderType

PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
if not PRIVATE_KEY:
    print("ERROR: POLYMARKET_PRIVATE_KEY not found")
    sys.exit(1)

print("=" * 55)
print("  BUY + SELL TEST (5 shares, BTC cheaper side)")
print("=" * 55)

# Initialize the feed client
feed = PolymarketClient()
clob = feed._clob

# Web3 for balance checks
w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
if not w3.is_connected():
    w3 = Web3(Web3.HTTPProvider("https://rpc-mainnet.matic.quiknode.pro"))
CT_ADDR = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
ct_abi = [{"inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}],
           "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
           "stateMutability": "view", "type": "function"}]
ct = w3.eth.contract(address=Web3.to_checksum_address(CT_ADDR), abi=ct_abi)
wallet = Web3.to_checksum_address(feed._wallet_address)

# ── Ensure ERC-1155 approvals (required for SELL orders) ──────────────
# The CLOB checks on-chain that the exchange operators are approved to
# transfer conditional tokens on your behalf.  Without this, SELL orders
# are rejected with "not enough balance / allowance".
OPERATORS = [
    ("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8ED0a90", "CTF Exchange (legacy)"),
    ("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E", "CTF Exchange (SDK)"),
    ("0xC5d563A36AE78145C45a50134d48A1215220f80a", "Neg Risk CTF Exchange"),
    ("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296", "Neg Risk Adapter"),
]
approval_abi = [
    {"inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
     "name": "setApprovalForAll", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "operator", "type": "address"}],
     "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "view", "type": "function"},
]
ct_approval = w3.eth.contract(address=Web3.to_checksum_address(CT_ADDR), abi=approval_abi)
print("\n[0] Checking conditional-token approvals...")
for op_addr, op_name in OPERATORS:
    op = Web3.to_checksum_address(op_addr)
    approved = ct_approval.functions.isApprovedForAll(wallet, op).call()
    if approved:
        print(f"    {op_name}: already approved")
    else:
        print(f"    {op_name}: NOT approved — sending approval tx...")
        nonce = w3.eth.get_transaction_count(wallet)
        tx = ct_approval.functions.setApprovalForAll(op, True).build_transaction({
            'from': wallet, 'nonce': nonce, 'gas': 100000,
            'gasPrice': w3.eth.gas_price, 'chainId': 137,
        })
        signed_tx = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt['status'] == 1:
            print(f"    {op_name}: approved (tx {tx_hash.hex()[:16]}...)")
        else:
            print(f"    {op_name}: approval tx FAILED")
            sys.exit(1)

# Find BTC market
print("\n[1] Finding current BTC market...")
markets = feed.get_current_hour_markets()
btc = None
for m in markets:
    if m.crypto == "BTC":
        btc = m
        break

if btc is None:
    print("ERROR: No BTC market found for this hour")
    sys.exit(1)

print(f"    Slug: {btc.slug}")

# Get book for both sides
print("\n[2] Reading order books...")
up_book = feed.get_top_of_book(btc.outcome_up_id)
dn_book = feed.get_top_of_book(btc.outcome_down_id)
print(f"    Up   ask={up_book.ask:.3f}  bid={up_book.bid:.3f}")
print(f"    Down ask={dn_book.ask:.3f}  bid={dn_book.bid:.3f}")

# Pick cheaper side
if dn_book.ask <= up_book.ask:
    side_name, token_id, buy_price = "Down", btc.outcome_down_id, dn_book.ask
else:
    side_name, token_id, buy_price = "Up", btc.outcome_up_id, up_book.ask

import math
buy_price = round(max(0.01, min(0.99, buy_price)), 3)
# CLOB requires: min 5 shares AND min $1 total order value
qty = max(5, math.ceil(1.01 / buy_price))
cost = buy_price * qty
print(f"\n    Picking: {side_name} (cheaper @ ${buy_price:.3f})")
print(f"    Qty: {qty} shares (${cost:.2f} total, meets $1 min)")

# Check on-chain balance BEFORE buy
bal_before = ct.functions.balanceOf(wallet, int(token_id)).call()
print(f"\n[3] On-chain balance before buy: {bal_before}")

# BUY — call CLOB directly
print(f"\n[4] Placing BUY order (direct CLOB call)...")
try:
    args = OrderArgs(
        price=buy_price,
        size=qty,
        side="BUY",
        token_id=token_id,
    )
    signed = clob.create_order(args)
    response = clob.post_order(signed, OrderType.GTC)
    print(f"    Raw CLOB response: {response}")
except Exception as e:
    print(f"\n    BUY ERROR: {e}")
    print(f"    Error type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Parse buy response
if not response or not isinstance(response, dict):
    print(f"    Unexpected response: {response}")
    sys.exit(1)

status = response.get("status", "").lower()
tx_hashes = response.get("transactionsHashes", []) or response.get("transactionHashes", [])
size_matched = response.get("size_matched") or 0

if status == "matched" or tx_hashes:
    fill_qty = int(size_matched) if size_matched else qty
    print(f"    FILLED: {fill_qty} shares")
elif status == "live":
    print(f"    Order is LIVE (resting). Waiting up to 5s for fill...")
    oid = response.get("orderID", "")
    fill_qty = 0
    for _ in range(50):
        time.sleep(0.1)
        try:
            order = clob.get_order(oid)
            if order and isinstance(order, dict):
                s = order.get("status", "").lower()
                if s in ("matched", "filled"):
                    fill_qty = int(order.get("size_matched", qty) or qty)
                    break
        except Exception:
            pass
    if fill_qty > 0:
        print(f"    FILLED: {fill_qty} shares")
    else:
        print(f"    BUY did not fill within 5s. Status: {status}")
        sys.exit(1)
else:
    print(f"    Unexpected status: {status}")
    sys.exit(1)

fill_price = buy_price

# Wait and poll for on-chain settlement
print(f"\n[5] Waiting for on-chain settlement...")
for i in range(12):
    time.sleep(2.5)
    bal_now = ct.functions.balanceOf(wallet, int(token_id)).call()
    gained = bal_now - bal_before
    print(f"    {(i+1)*2.5:.0f}s: on-chain balance={bal_now}  (gained={gained})")
    if gained >= fill_qty:
        print(f"    Tokens settled!")
        break
else:
    print(f"    WARNING: tokens may not have settled after 30s")

# Give the CLOB indexer time to sync the on-chain settlement
print("    Waiting 5s for CLOB to sync...")
time.sleep(5)

# Re-read book for sell price
book = feed.get_top_of_book(token_id)
sell_price = book.bid
print(f"\n    Current bid: ${sell_price:.3f}")

# SELL — call CLOB directly
print(f"\n[6] Placing SELL order: {fill_qty} @ ${sell_price:.3f}")
sell_price = round(max(0.01, min(0.99, sell_price)), 3)
try:
    args = OrderArgs(
        price=sell_price,
        size=int(fill_qty),
        side="SELL",
        token_id=token_id,
    )
    signed = clob.create_order(args)
    response = clob.post_order(signed, OrderType.GTC)
    print(f"    Raw CLOB response: {response}")

    if response and isinstance(response, dict):
        status = response.get("status", "").lower()
        if status == "matched" or response.get("transactionsHashes") or response.get("transactionHashes"):
            print(f"\n    SELL FILLED!")
            print("\n" + "=" * 55)
            print("  SUCCESS: Buy and sell both worked!")
            print("=" * 55)
        elif status == "live":
            print(f"\n    SELL order is live (resting on book)")
            print(f"    Order ID: {response.get('orderID', 'N/A')}")
        else:
            print(f"\n    Unexpected status: {status}")
    else:
        print(f"    Unexpected response type: {type(response)}: {response}")

except Exception as e:
    print(f"\n    SELL ERROR: {e}")
    print(f"    Error type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    err_str = str(e).lower()
    if "allowance" in err_str or "balance" in err_str:
        print(f"\n    On-chain balance: {ct.functions.balanceOf(wallet, int(token_id)).call()}")
    print("\n" + "=" * 55)
    print("  FAIL: Sell error — see above for details")
    print("=" * 55)
    sys.exit(1)
