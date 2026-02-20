#!/usr/bin/env python3
"""Quick buy+sell test: buys 5 shares of the cheaper BTC side, then sells them.
Proves the full order lifecycle works (buy fill -> sell fill).

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

# Web3 for balance checks
w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
if not w3.is_connected():
    w3 = Web3(Web3.HTTPProvider("https://rpc-mainnet.matic.quiknode.pro"))
CT_ADDR = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
ct_abi = [{"inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}],
           "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
           "stateMutability": "view", "type": "function"}]
ct = w3.eth.contract(address=Web3.to_checksum_address(CT_ADDR), abi=ct_abi)
wallet = Web3.to_checksum_address(feed.wallet_address)

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

qty = 5
cost = buy_price * qty
print(f"\n    Picking: {side_name} (cheaper @ ${buy_price:.3f})")
print(f"    Cost: {qty} x ${buy_price:.3f} = ${cost:.2f}")

# Check on-chain balance BEFORE buy
bal_before = ct.functions.balanceOf(wallet, int(token_id)).call()
print(f"\n[3] On-chain balance before buy: {bal_before}")

# BUY
print(f"\n[4] Placing BUY order...")
buy_result = feed.place_limit_order(token_id, "BUY", buy_price, qty, post_only=False)
print(f"    Result: {buy_result}")

if not buy_result.get("filled"):
    print("    BUY did not fill.")
    sys.exit(1)

fill_qty = buy_result["fill_qty"]
fill_price = buy_result["fill_price"]
print(f"    FILLED: {fill_qty} @ ${fill_price:.3f}")

# Wait and poll for on-chain settlement
print("\n[5] Waiting for on-chain settlement...")
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

# Re-read book for sell price
book = feed.get_top_of_book(token_id)
sell_price = book.bid
print(f"\n    Current bid: ${sell_price:.3f}")

# SELL — call CLOB directly to see the raw error
print(f"\n[6] Placing SELL order: {fill_qty} @ ${sell_price:.3f}")
sell_price = round(max(0.01, min(0.99, sell_price)), 3)
try:
    args = OrderArgs(
        price=sell_price,
        size=int(fill_qty),
        side="SELL",
        token_id=token_id,
    )
    signed = feed._clob.create_order(args)
    response = feed._clob.post_order(signed, OrderType.GTC)
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
    # Check if it's an allowance/balance issue
    err_str = str(e).lower()
    if "allowance" in err_str or "balance" in err_str:
        print("\n    This is a balance/allowance error.")
        print(f"    On-chain balance: {ct.functions.balanceOf(wallet, int(token_id)).call()}")
        print("    Approvals were confirmed OK by check_sell_ready.py")
        print("    The CLOB may need more time to reflect your balance.")
    print("\n" + "=" * 55)
    print("  FAIL: Sell error — see above for details")
    print("=" * 55)
    sys.exit(1)
