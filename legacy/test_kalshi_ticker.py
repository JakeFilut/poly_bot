#!/usr/bin/env python3
"""
Test Kalshi WebSocket ticker channel.
Fetches current open BTC market tickers via REST, then subscribes via WebSocket.

Usage: python test_kalshi_ticker.py
"""

import asyncio
import base64
import json
import os
import time
from pathlib import Path

import requests
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv

# Load .env from keys directory
KEYS_DIR = os.getenv("ARB_KEYS_DIR", str(Path(__file__).resolve().parent.parent / "keys"))
load_dotenv(os.path.join(KEYS_DIR, ".env"))

KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip()

# Resolve key path
expanded = os.path.expanduser(PRIVATE_KEY_PATH)
if not os.path.isabs(expanded):
    expanded = os.path.join(KEYS_DIR, expanded)

with open(expanded, "rb") as f:
    PRIVATE_KEY = serialization.load_pem_private_key(f.read(), password=None)

BASE_URL = "https://api.elections.kalshi.com"
WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"


def make_rest_headers(method, path):
    ts = str(int(time.time() * 1000))
    msg = f"{ts}{method}{path}"
    sig = PRIVATE_KEY.sign(
        msg.encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json",
    }


def make_ws_headers():
    ts = str(int(time.time() * 1000))
    msg = f"{ts}GET/trade-api/ws/v2"
    sig = PRIVATE_KEY.sign(
        msg.encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }


def fetch_open_market_tickers():
    """Fetch current open BTC 15m market tickers via REST API."""
    path = "/trade-api/v2/markets"
    headers = make_rest_headers("GET", path)
    r = requests.get(
        f"{BASE_URL}{path}",
        headers=headers,
        params={"series_ticker": "KXBTC15M", "status": "open", "limit": 5},
        timeout=10,
    )
    if r.status_code != 200:
        print(f"REST error {r.status_code}: {r.text[:200]}")
        return []

    markets = r.json().get("markets", [])
    tickers = []
    for m in markets:
        ticker = m.get("ticker", "")
        strike = m.get("floor_strike", "?")
        title = m.get("subtitle", "") or m.get("title", "")
        print(f"  Found: {ticker}  strike={strike}  ({title})")
        # Also print ALL fields to see what's available
        if not tickers:  # Print full fields for first market only
            print(f"\n  --- Full market fields ---")
            for k, v in m.items():
                print(f"    {k}: {v}")
            print(f"  --- End fields ---\n")
        tickers.append(ticker)
    return tickers


async def main():
    # Step 1: Get actual market tickers via REST
    print("Fetching open BTC 15m markets...")
    tickers = fetch_open_market_tickers()
    if not tickers:
        print("No open markets found!")
        return

    print(f"\nSubscribing to {len(tickers)} market(s): {tickers}\n")

    # Step 2: Connect WebSocket and subscribe
    print(f"Connecting to {WS_URL}...")
    headers = make_ws_headers()

    async with websockets.connect(WS_URL, additional_headers=headers, ping_interval=10, ping_timeout=30) as ws:
        print("Connected!")

        sub = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker"],
                "market_tickers": tickers,
            },
        }
        await ws.send(json.dumps(sub))
        print(f"Subscribed to ticker channel for: {tickers}")
        print("Waiting for ticker updates (Ctrl+C to stop)...\n")

        count = 0
        async for raw in ws:
            data = json.loads(raw)
            count += 1
            print(f"--- Message #{count} ---")
            print(json.dumps(data, indent=2))
            print()

            if count >= 50:
                print("Got 50 messages, stopping.")
                break


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as e:
        print(f"\nError: {e}")
