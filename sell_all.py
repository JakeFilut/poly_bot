#!/usr/bin/env python3
"""Emergency sell-all: dumps every position at the best available bid.

Usage:  python sell_all.py
        python sell_all.py --dry-run     (show positions without selling)

Reads on-chain conditional token balances for all current-hour markets,
then sells each position at the best bid (or bid - 1c for faster fill).
"""
import math, os, sys, time
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3

# Load .env
_PROJECT_DIR = Path(__file__).resolve().parent
_KEYS_DIR = os.getenv("KEYS_DIR", str(_PROJECT_DIR.parent / "keys"))
for env_path in [os.path.join(_KEYS_DIR, ".env"), str(_PROJECT_DIR / ".env")]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break

os.environ["MODE"] = "LIVE"

from src.feeds.polymarket import PolymarketClient
from py_clob_client.clob_types import OrderArgs, OrderType

DRY_RUN = "--dry-run" in sys.argv or "-n" in sys.argv

PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
if not PRIVATE_KEY:
    print("ERROR: POLYMARKET_PRIVATE_KEY not found")
    sys.exit(1)

print("=" * 55)
print("  SELL ALL POSITIONS" + ("  [DRY RUN]" if DRY_RUN else ""))
print("=" * 55)

# ── Init ──
feed = PolymarketClient()
clob = feed._clob
wallet = Web3.to_checksum_address(feed._wallet_address)
print(f"  Wallet: {wallet}")

# Web3 for on-chain balance checks
w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
if not w3.is_connected():
    w3 = Web3(Web3.HTTPProvider("https://rpc-mainnet.matic.quiknode.pro"))

CT_ADDR = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
ct_abi = [{"inputs": [{"name": "account", "type": "address"},
                       {"name": "id", "type": "uint256"}],
           "name": "balanceOf",
           "outputs": [{"name": "", "type": "uint256"}],
           "stateMutability": "view", "type": "function"}]
ct = w3.eth.contract(address=Web3.to_checksum_address(CT_ADDR), abi=ct_abi)

# ── Ensure sell approvals ──
OPERATORS = [
    ("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8ED0a90", "CTF Exchange (legacy)"),
    ("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E", "CTF Exchange (SDK)"),
    ("0xC5d563A36AE78145C45a50134d48A1215220f80a", "Neg Risk CTF Exchange"),
    ("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296", "Neg Risk Adapter"),
]
approval_abi = [
    {"inputs": [{"name": "operator", "type": "address"},
                {"name": "approved", "type": "bool"}],
     "name": "setApprovalForAll", "outputs": [],
     "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "owner", "type": "address"},
                {"name": "operator", "type": "address"}],
     "name": "isApprovedForAll",
     "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "view", "type": "function"},
]
ct_approval = w3.eth.contract(address=Web3.to_checksum_address(CT_ADDR),
                               abi=approval_abi)

print("\n[1] Checking sell approvals...")
for op_addr, op_name in OPERATORS:
    op = Web3.to_checksum_address(op_addr)
    approved = ct_approval.functions.isApprovedForAll(wallet, op).call()
    if approved:
        print(f"    {op_name}: OK")
    else:
        print(f"    {op_name}: NOT approved — sending tx...")
        nonce = w3.eth.get_transaction_count(wallet)
        tx = ct_approval.functions.setApprovalForAll(op, True).build_transaction({
            'from': wallet, 'nonce': nonce, 'gas': 100000,
            'gasPrice': w3.eth.gas_price, 'chainId': 137,
        })
        signed_tx = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt['status'] == 1:
            print(f"    {op_name}: approved OK")
        else:
            print(f"    {op_name}: FAILED — sell may not work")

# ── Discover markets ──
print("\n[2] Discovering current-hour markets...")
markets = feed.get_current_hour_markets()
if not markets:
    print("  No markets found for current hour.")
    sys.exit(0)

for m in markets:
    print(f"  {m.crypto}: {m.slug}")

# ── Check balances for each token ──
print("\n[3] Checking on-chain balances...")
positions = []  # (market, outcome, token_id, qty, book)

for m in markets:
    for outcome, token_id in [("Up", m.outcome_up_id), ("Down", m.outcome_down_id)]:
        if not token_id:
            continue
        try:
            raw = ct.functions.balanceOf(wallet, int(token_id)).call()
            # Polymarket CT tokens (ERC-1155) use 6 decimals, same as USDC
            qty = float(raw) / 1e6
        except Exception as e:
            print(f"  ERROR reading {m.crypto} {outcome}: {e}")
            continue

        if qty < 0.01:
            continue

        # Get orderbook for price
        try:
            book = feed.get_top_of_book(token_id)
            bid = book.bid if book else 0.0
        except Exception:
            bid = 0.0

        positions.append((m, outcome, token_id, qty, bid))
        value = qty * bid if bid > 0 else 0
        print(f"  {m.crypto:4s} {outcome:5s}: {qty:10.2f} shares  "
              f"bid=${bid:.3f}  value=${value:.2f}")

if not positions:
    print("\n  No positions found — wallet is flat.")
    sys.exit(0)

total_value = sum(qty * bid for _, _, _, qty, bid in positions)
print(f"\n  Total positions: {len(positions)}  |  "
      f"Estimated value: ${total_value:.2f}")

if DRY_RUN:
    print("\n  [DRY RUN] — not selling. Run without --dry-run to sell.")
    sys.exit(0)

# ── Confirm ──
print(f"\n  About to sell ALL {len(positions)} positions.")
confirm = input("  Type 'yes' to confirm: ").strip().lower()
if confirm != "yes":
    print("  Aborted.")
    sys.exit(0)

# ── Sell each position ──
print("\n[4] Selling positions...")
results = []

for m, outcome, token_id, qty, bid in positions:
    sell_qty = math.floor(qty * 100) / 100  # Round down to 2 decimals
    if sell_qty < 1:
        print(f"  SKIP {m.crypto} {outcome}: qty={qty:.2f} < 1 share")
        continue

    # Sell at bid - 1c for faster fill (aggressive)
    sell_price = max(0.01, round(bid - 0.01, 3)) if bid > 0.02 else 0.01

    print(f"\n  SELL {m.crypto} {outcome}: {sell_qty:.2f} shares @ ${sell_price:.3f}")

    filled = False
    for attempt in range(3):
        try:
            price = max(0.01, round(sell_price - (attempt * 0.02), 3))
            args = OrderArgs(
                price=price,
                size=sell_qty,
                side="SELL",
                token_id=token_id,
            )
            signed = clob.create_order(args)
            response = clob.post_order(signed, OrderType.GTC)

            if response and isinstance(response, dict):
                status = response.get("status", "").lower()
                oid = response.get("orderID", "")
                tx_hashes = response.get("transactionsHashes", [])
                size_matched = response.get("size_matched", 0)

                if status == "matched" and tx_hashes:
                    print(f"    SOLD @ ${price:.3f} (matched)")
                    filled = True
                    results.append((m.crypto, outcome, sell_qty, price, "FILLED"))
                    break
                elif status == "live" and oid:
                    # Poll briefly for fill
                    print(f"    Order resting (attempt {attempt+1})... polling")
                    for _ in range(5):
                        time.sleep(1)
                        try:
                            order = clob.get_order(oid)
                            if order and order.get("status", "").lower() in ("matched", "filled"):
                                print(f"    SOLD @ ${price:.3f} (filled after polling)")
                                filled = True
                                results.append((m.crypto, outcome, sell_qty, price, "FILLED"))
                                break
                        except Exception:
                            pass
                    if filled:
                        break
                    # Cancel resting order, try lower price
                    try:
                        clob.cancel(order_id=oid)
                    except Exception:
                        pass
                else:
                    print(f"    Attempt {attempt+1}: status={status}")
        except Exception as e:
            err_msg = str(e)[:100]
            print(f"    Attempt {attempt+1} ERROR: {err_msg}")
            # If allowance error, wait and retry
            if "allowance" in err_msg.lower() or "not enough" in err_msg.lower():
                time.sleep(3)

    if not filled:
        print(f"    FAILED to sell {m.crypto} {outcome}")
        results.append((m.crypto, outcome, sell_qty, 0, "FAILED"))

# ── Summary ──
print("\n" + "=" * 55)
print("  SELL SUMMARY")
print("=" * 55)
total_proceeds = 0.0
for crypto, outcome, qty, price, status in results:
    proceeds = qty * price if status == "FILLED" else 0
    total_proceeds += proceeds
    marker = "OK" if status == "FILLED" else "FAILED"
    print(f"  {marker:6s}  {crypto:4s} {outcome:5s}  {qty:9.2f} shares @ ${price:.3f}  "
          f"= ${proceeds:.2f}")

print(f"\n  Total proceeds: ${total_proceeds:.2f}")
print(f"  Positions attempted: {len(results)}")
print(f"  Filled: {sum(1 for r in results if r[4] == 'FILLED')}")
print(f"  Failed: {sum(1 for r in results if r[4] == 'FAILED')}")
