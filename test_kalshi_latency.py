"""
Test script to measure Kalshi API latency for fill detection.
Places small orders and measures how long until status updates.
"""

import os
import sys
import time
from dotenv import load_dotenv

from config import KEYS_DIR

# Load environment from keys directory
env_path = os.path.join(KEYS_DIR, ".env")
load_dotenv(env_path)

from kalshi_client import KalshiClient

def test_order_latency(kalshi: KalshiClient, ticker: str, side: str, qty: int, price_cents: int):
    """
    Place an order and measure how long until API shows fill status.

    Returns dict with timing data.
    """
    print(f"\n{'='*60}")
    print(f"Testing: {qty} {side.upper()} @ {price_cents}c on {ticker}")
    print(f"{'='*60}")

    # Place order
    place_start = time.time()
    order_resp = kalshi.place_order(ticker, side, qty, price_cents)
    place_end = time.time()
    place_latency = (place_end - place_start) * 1000

    if not order_resp:
        print(f"  Order placement FAILED")
        return None

    order_data = order_resp.get("order", {})
    order_id = order_data.get("order_id", "")
    initial_status = order_data.get("status", "").lower()
    initial_remaining = order_data.get("remaining_count", -1)
    initial_filled = order_data.get("count_filled", 0) or 0

    print(f"  Placement latency: {place_latency:.0f}ms")
    print(f"  Initial response: status={initial_status}, remaining={initial_remaining}, filled={initial_filled}")

    # Check if immediately filled
    if (initial_status == "executed" and initial_remaining == 0) or initial_filled > 0:
        print(f"  >>> IMMEDIATE FILL detected in placement response!")
        return {
            "placement_ms": place_latency,
            "fill_detected_ms": 0,
            "poll_count": 0,
            "method": "immediate"
        }

    # Poll for status changes
    poll_start = time.time()
    poll_count = 0
    check_times = [50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000, 1500, 2000]
    results = []

    print(f"\n  Polling status...")

    for target_ms in check_times:
        # Wait until target time
        elapsed = (time.time() - poll_start) * 1000
        if elapsed < target_ms:
            time.sleep((target_ms - elapsed) / 1000)

        poll_count += 1
        check_start = time.time()
        resp = kalshi.get_order_status(order_id)
        check_latency = (time.time() - check_start) * 1000
        actual_elapsed = (time.time() - poll_start) * 1000

        if resp:
            od = resp.get("order", resp)
            status = od.get("status", "").lower()
            remaining = od.get("remaining_count", -1)
            filled = od.get("count_filled", 0) or 0

            is_filled = (status == "executed" and remaining == 0) or filled > 0

            result = {
                "target_ms": target_ms,
                "actual_ms": actual_elapsed,
                "api_latency_ms": check_latency,
                "status": status,
                "remaining": remaining,
                "filled": filled,
                "is_filled": is_filled
            }
            results.append(result)

            status_icon = "✓" if is_filled else "○"
            print(f"    {target_ms:4d}ms: {status_icon} status={status}, remaining={remaining}, filled={filled} (api:{check_latency:.0f}ms)")

            if is_filled:
                print(f"\n  >>> FILL detected at {actual_elapsed:.0f}ms (poll #{poll_count})")

                # Cancel not needed if filled
                return {
                    "placement_ms": place_latency,
                    "fill_detected_ms": actual_elapsed,
                    "poll_count": poll_count,
                    "method": "poll",
                    "results": results
                }

    # Not filled after all checks - cancel order
    print(f"\n  Order did not fill after {check_times[-1]}ms - cancelling...")
    kalshi.cancel_order(order_id)

    return {
        "placement_ms": place_latency,
        "fill_detected_ms": None,
        "poll_count": poll_count,
        "method": "no_fill",
        "results": results
    }


def find_active_market(kalshi: KalshiClient):
    """Find an active 15-minute crypto market to test with."""
    for crypto in ["BTC", "SOL", "ETH"]:
        # Try to find a market
        markets = kalshi.get_markets(crypto)
        if markets:
            for m in markets:
                ticker = m.get("ticker", "")
                status = m.get("status", "")
                if status == "active" and "CRYPTO" in ticker:
                    return ticker, crypto
    return None, None


def main():
    print("=" * 60)
    print("  KALSHI API LATENCY TEST")
    print("=" * 60)

    # Get credentials
    kalshi_key_id = os.getenv("KALSHI_API_KEY_ID", "").strip()
    kalshi_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip()

    if not kalshi_key_id or not kalshi_key_path:
        print("ERROR: Missing Kalshi credentials in .env")
        sys.exit(1)

    # Initialize client
    print("\nConnecting to Kalshi...")
    kalshi = KalshiClient(kalshi_key_id, kalshi_key_path)

    # Check balance
    balance = kalshi.get_balance()
    print(f"Balance: ${balance:.2f}")

    if balance < 5:
        print("ERROR: Need at least $5 balance to run tests")
        sys.exit(1)

    # Find active market
    print("\nFinding active market...")
    ticker, crypto = find_active_market(kalshi)

    if not ticker:
        print("ERROR: No active crypto market found")
        sys.exit(1)

    print(f"Using market: {ticker} ({crypto})")

    # Get orderbook to find good prices
    ob = kalshi.get_orderbook(ticker)
    if not ob:
        print("ERROR: Could not get orderbook")
        sys.exit(1)

    book = ob.get("orderbook", {})
    yes_bids = book.get("yes", [])
    no_bids = book.get("no", [])

    if not yes_bids and not no_bids:
        print("ERROR: Empty orderbook")
        sys.exit(1)

    # Calculate prices that should fill immediately (pay the ask)
    # YES ask = 100 - highest NO bid
    # NO ask = 100 - highest YES bid

    if no_bids:
        best_no_bid = max(b[0] for b in no_bids)
        yes_ask = 100 - best_no_bid
        print(f"\nYES orderbook: best ask = {yes_ask}c (from NO bid {best_no_bid}c)")
    else:
        yes_ask = None

    if yes_bids:
        best_yes_bid = max(b[0] for b in yes_bids)
        no_ask = 100 - best_yes_bid
        print(f"NO orderbook: best ask = {no_ask}c (from YES bid {best_yes_bid}c)")
    else:
        no_ask = None

    # Run tests
    all_results = []

    print("\n" + "=" * 60)
    print("  RUNNING LATENCY TESTS (1 contract each)")
    print("=" * 60)

    # Test YES side if available
    if yes_ask and yes_ask < 95:  # Don't buy at extreme prices
        test_price = yes_ask + 2  # Pay 2c above ask to ensure fill
        result = test_order_latency(kalshi, ticker, "yes", 1, test_price)
        if result:
            result["side"] = "yes"
            all_results.append(result)
        time.sleep(1)  # Brief pause between tests

    # Test NO side if available
    if no_ask and no_ask < 95:
        test_price = no_ask + 2  # Pay 2c above ask to ensure fill
        result = test_order_latency(kalshi, ticker, "no", 1, test_price)
        if result:
            result["side"] = "no"
            all_results.append(result)

    # Summary
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)

    if not all_results:
        print("No successful tests to summarize")
        return

    immediate_fills = [r for r in all_results if r.get("method") == "immediate"]
    poll_fills = [r for r in all_results if r.get("method") == "poll"]
    no_fills = [r for r in all_results if r.get("method") == "no_fill"]

    print(f"\nTotal tests: {len(all_results)}")
    print(f"  Immediate fills (in placement response): {len(immediate_fills)}")
    print(f"  Poll-detected fills: {len(poll_fills)}")
    print(f"  No fills: {len(no_fills)}")

    if poll_fills:
        fill_times = [r["fill_detected_ms"] for r in poll_fills]
        avg_fill_time = sum(fill_times) / len(fill_times)
        min_fill_time = min(fill_times)
        max_fill_time = max(fill_times)

        print(f"\nPoll-detected fill times:")
        print(f"  Average: {avg_fill_time:.0f}ms")
        print(f"  Min: {min_fill_time:.0f}ms")
        print(f"  Max: {max_fill_time:.0f}ms")

        # Recommendation
        recommended = max(max_fill_time * 1.5, 500)  # 50% margin, min 500ms
        print(f"\n  RECOMMENDATION: Set MAX_WAIT_FOR_FILL to {recommended:.0f}ms ({recommended/1000:.2f}s)")

    if immediate_fills:
        print(f"\n{len(immediate_fills)} fills detected immediately in placement response!")
        print("  The placement API returns fill status right away when order crosses spread.")

    # Final balance
    final_balance = kalshi.get_balance()
    print(f"\nFinal balance: ${final_balance:.2f} (spent ${balance - final_balance:.2f} on test orders)")


if __name__ == "__main__":
    main()
