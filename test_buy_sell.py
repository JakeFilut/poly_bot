#!/usr/bin/env python3
"""Quick buy+sell test: buys 5 shares of the cheaper BTC side, then sells them.
Proves the full order lifecycle works (buy fill -> sell fill).

Usage:  python test_buy_sell.py
"""
import os, sys, time
from pathlib import Path
from dotenv import load_dotenv

# Load .env same as the bot
_PROJECT_DIR = Path(__file__).resolve().parent
_KEYS_DIR = os.getenv("KEYS_DIR", str(_PROJECT_DIR.parent / "keys"))
for env_path in [os.path.join(_KEYS_DIR, ".env"), str(_PROJECT_DIR / ".env")]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break

# Force LIVE mode so the client actually places orders
os.environ["MODE"] = "LIVE"

from src.feeds.polymarket import PolymarketFeed

PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
if not PRIVATE_KEY:
    print("ERROR: POLYMARKET_PRIVATE_KEY not found")
    sys.exit(1)

print("=" * 55)
print("  BUY + SELL TEST (5 shares, BTC cheaper side)")
print("=" * 55)

# Initialize the feed client
feed = PolymarketFeed(private_key=PRIVATE_KEY)

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
print(f"    Up   token: {btc.outcome_up_id}")
print(f"    Down token: {btc.outcome_down_id}")

# Get book for both sides
print("\n[2] Reading order books...")
up_book = feed.get_top_of_book(btc.outcome_up_id)
dn_book = feed.get_top_of_book(btc.outcome_down_id)
print(f"    Up   ask={up_book.ask:.3f}  bid={up_book.bid:.3f}")
print(f"    Down ask={dn_book.ask:.3f}  bid={dn_book.bid:.3f}")

# Pick cheaper side
if dn_book.ask <= up_book.ask:
    side_name = "Down"
    token_id = btc.outcome_down_id
    buy_price = dn_book.ask
    book = dn_book
else:
    side_name = "Up"
    token_id = btc.outcome_up_id
    buy_price = up_book.ask
    book = up_book

qty = 5  # CLOB minimum
cost = buy_price * qty
print(f"\n    Picking: {side_name} (cheaper @ ${buy_price:.3f})")
print(f"    Cost: {qty} x ${buy_price:.3f} = ${cost:.2f}")

# Confirm
print(f"\n[3] About to BUY {qty} {side_name} @ ${buy_price:.3f} (${cost:.2f})")
confirm = input("    Proceed? [y/N]: ").strip().lower()
if confirm != "y":
    print("    Aborted.")
    sys.exit(0)

# BUY
print(f"\n[4] Placing BUY order...")
buy_result = feed.place_limit_order(token_id, "BUY", buy_price, qty, post_only=False)
print(f"    Result: {buy_result}")

if not buy_result.get("filled"):
    print("\n    BUY did not fill. Order may be resting on the book.")
    print(f"    Order ID: {buy_result.get('order_id', 'N/A')}")
    print("    Try running again with a more aggressive price, or wait for fill.")
    sys.exit(1)

fill_qty = buy_result["fill_qty"]
fill_price = buy_result["fill_price"]
print(f"    FILLED: {fill_qty} @ ${fill_price:.3f}")

# Wait a moment for settlement
print("\n[5] Waiting 3s for on-chain settlement...")
time.sleep(3)

# Re-read book for sell price
book = feed.get_top_of_book(token_id)
sell_price = book.bid
print(f"    Current bid: ${sell_price:.3f}")

# SELL
print(f"\n[6] Placing SELL order: {fill_qty} @ ${sell_price:.3f}")
sell_result = feed.place_limit_order(token_id, "SELL", sell_price, fill_qty, post_only=False)
print(f"    Result: {sell_result}")

if sell_result.get("filled"):
    sell_qty = sell_result["fill_qty"]
    sell_px = sell_result["fill_price"]
    pnl = (sell_px - fill_price) * sell_qty
    print(f"\n    SELL FILLED: {sell_qty} @ ${sell_px:.3f}")
    print(f"    P&L: ${pnl:+.4f}")
    print("\n" + "=" * 55)
    print("  SUCCESS: Buy and sell both worked!")
    print("=" * 55)
elif sell_result.get("status") == "error":
    print(f"\n    SELL FAILED: {sell_result}")
    print("\n" + "=" * 55)
    print("  FAIL: Sell error — check approvals with check_sell_ready.py")
    print("=" * 55)
    sys.exit(1)
else:
    print(f"\n    SELL resting (not filled yet): {sell_result}")
    print("    The sell order is on the book. You may need to wait or cancel.")
    print(f"    Order ID: {sell_result.get('order_id', 'N/A')}")
