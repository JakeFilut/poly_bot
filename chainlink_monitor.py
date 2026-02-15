#!/usr/bin/env python3
"""Standalone Chainlink price monitor - run in a separate terminal."""

import asyncio
import json
import websockets

SYMBOLS = {
    'BTCUSDT': 'BTC', 'ETHUSDT': 'ETH', 'SOLUSDT': 'SOL', 'XRPUSDT': 'XRP',
    'btc/usd': 'BTC', 'eth/usd': 'ETH', 'sol/usd': 'SOL', 'xrp/usd': 'XRP',
    'BTC/USD': 'BTC', 'ETH/USD': 'ETH', 'SOL/USD': 'SOL', 'XRP/USD': 'XRP',
}

async def main():
    url = 'wss://ws-live-data.polymarket.com'
    while True:
        try:
            async with websockets.connect(url, ping_interval=10, ping_timeout=30) as ws:
                sub = {
                    'action': 'subscribe',
                    'subscriptions': [{
                        'topic': 'crypto_prices_chainlink',
                        'type': '*',
                        'filters': ''
                    }]
                }
                await ws.send(json.dumps(sub))
                print('[chainlink] Subscribed to Chainlink feed...')

                async for msg in ws:
                    if not msg or not msg.strip():
                        continue
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    items = []
                    if isinstance(data, dict) and data.get('topic') == 'crypto_prices_chainlink':
                        p = data.get('payload', {})
                        if p:
                            items = [p]
                    elif isinstance(data, list):
                        items = data
                    elif isinstance(data, dict) and 'symbol' in data:
                        items = [data]

                    for item in items:
                        sym = item.get('symbol', '')
                        val = item.get('value')
                        ts = item.get('timestamp')
                        crypto = SYMBOLS.get(sym.upper()) or SYMBOLS.get(sym)
                        if crypto and val:
                            v = float(val)
                            if v < 10:
                                fmt = f'${v:,.5f}'
                            elif v < 100:
                                fmt = f'${v:,.4f}'
                            else:
                                fmt = f'${v:,.2f}'
                            print(f'[chainlink] {crypto}: {fmt}  (ts={ts})')

        except Exception as e:
            print(f'[chainlink] Connection lost: {e} — reconnecting in 3s...')
            await asyncio.sleep(3)

if __name__ == '__main__':
    asyncio.run(main())
