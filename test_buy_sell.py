#!/usr/bin/env python3
"""Quick buy+sell test using FOK (Fill-Or-Kill) market orders.
Buys shares of the cheaper BTC side, then immediately sells them.
FOK orders fill instantly or cancel entirely — no resting orders left behind.

Usage:  python test_buy_sell.py
"""
import math, os, sys, time
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
from py_clob_client.clob_types import OrderType

PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
if not PRIVATE_KEY:
    print("ERROR: POLYMARKET_PRIVATE_KEY not found")
    sys.exit(1)

# Slippage buffer added to ask / subtracted from bid for FOK worst-price limit
SLIPPAGE_CENTS = 0.02

print("=" * 55)
print("  BUY + SELL TEST  (FOK market orders, BTC)")
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
    side_name, token_id, ask_price = "Down", btc.outcome_down_id, dn_book.ask
    bid_price = dn_book.bid
else:
    side_name, token_id, ask_price = "Up", btc.outcome_up_id, up_book.ask
    bid_price = up_book.bid

ask_price = round(max(0.01, min(0.99, ask_price)), 3)
# FOK BUY amount is in dollars; need >= $1 minimum
buy_dollars = max(1.01, round(ask_price * 5, 2))  # ~5 shares worth, at least $1.01
# Worst-price limit = ask + slippage buffer
buy_limit = round(min(0.99, ask_price + SLIPPAGE_CENTS), 3)
expected_shares = math.floor(buy_dollars / ask_price)

print(f"\n    Picking: {side_name} (cheaper @ ${ask_price:.3f})")
print(f"    FOK BUY: ${buy_dollars:.2f} @ limit ${buy_limit:.3f} (~{expected_shares} shares)")

# Check on-chain balance BEFORE buy
bal_before = ct.functions.balanceOf(wallet, int(token_id)).call()
print(f"\n[3] On-chain balance before buy: {bal_before}")

# ── BUY (FOK market order) ────────────────────────────────────────────
print(f"\n[4] Placing FOK BUY...")
try:
    signed = clob.create_market_order(
        token_id=token_id,
        side="BUY",
        amount=buy_dollars,
        price=buy_limit,
    )
    response = clob.post_order(signed, OrderType.FOK)
    print(f"    Raw CLOB response: {response}")
except Exception as e:
    print(f"\n    BUY ERROR: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

if not response or not isinstance(response, dict):
    print(f"    Unexpected response: {response}")
    sys.exit(1)

status = response.get("status", "").lower()
tx_hashes = response.get("transactionsHashes", []) or response.get("transactionHashes", [])

if status == "matched" or tx_hashes:
    taking = response.get("takingAmount", "")
    fill_qty = int(taking) if taking else expected_shares
    print(f"    FILLED: {fill_qty} shares")
else:
    print(f"    FOK not filled (status={status}). No resting order left.")
    sys.exit(1)

# Wait for on-chain settlement
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
current_bid = book.bid
print(f"\n    Current bid: ${current_bid:.3f}")

# Worst-price limit = bid - slippage buffer
sell_limit = round(max(0.01, current_bid - SLIPPAGE_CENTS), 3)

# ── SELL (FOK market order) ───────────────────────────────────────────
print(f"\n[6] Placing FOK SELL: {fill_qty} shares @ limit ${sell_limit:.3f}")
try:
    signed = clob.create_market_order(
        token_id=token_id,
        side="SELL",
        amount=fill_qty,
        price=sell_limit,
    )
    response = clob.post_order(signed, OrderType.FOK)
    print(f"    Raw CLOB response: {response}")

    status = (response or {}).get("status", "").lower()
    tx_hashes = (response or {}).get("transactionsHashes", []) or (response or {}).get("transactionHashes", [])

    if status == "matched" or tx_hashes:
        print(f"\n    SELL FILLED!")
        print("\n" + "=" * 55)
        print("  SUCCESS: Buy and sell both worked!")
        print("=" * 55)
    else:
        print(f"\n    FOK SELL not filled (status={status}). Shares remain in wallet.")
        print("=" * 55)
        print("  PARTIAL: Buy worked, sell did not fill (no liquidity on bid)")
        print("=" * 55)
        sys.exit(1)

except Exception as e:
    print(f"\n    SELL ERROR: {e}")
    import traceback; traceback.print_exc()
    err_str = str(e).lower()
    if "allowance" in err_str or "balance" in err_str:
        print(f"\n    On-chain balance: {ct.functions.balanceOf(wallet, int(token_id)).call()}")
    print("\n" + "=" * 55)
    print("  FAIL: Sell error — see above for details")
    print("=" * 55)
    sys.exit(1)
