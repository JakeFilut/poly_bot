#!/usr/bin/env python3
"""Standalone Binance BTC price monitor - run in a separate terminal.

Polls Binance every second and prints the BTC/USDT price so you can
visually compare it to the BTC price shown on Kalshi.

Usage:
    python binance_monitor.py
"""

import time
import requests
from datetime import datetime, timezone

SYMBOL = "BTCUSDT"
URL = f"https://api.binance.com/api/v3/ticker/price?symbol={SYMBOL}"
POLL_INTERVAL = 1  # seconds


def main():
    print(f"[binance] Polling {SYMBOL} every {POLL_INTERVAL}s  (Ctrl+C to stop)")
    print("-" * 55)

    prev_price = None
    while True:
        try:
            r = requests.get(URL, timeout=2)
            if r.status_code == 200:
                price = float(r.json()["price"])
                now = datetime.now(timezone.utc).strftime("%H:%M:%S")

                if prev_price is not None:
                    diff = price - prev_price
                    arrow = "^" if diff > 0 else ("v" if diff < 0 else "=")
                    print(f"[binance] {now} UTC  BTC: ${price:,.2f}  {arrow} {diff:+.2f}")
                else:
                    print(f"[binance] {now} UTC  BTC: ${price:,.2f}")

                prev_price = price
            else:
                print(f"[binance] HTTP {r.status_code}")
        except requests.RequestException as e:
            print(f"[binance] Error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[binance] Stopped.")
