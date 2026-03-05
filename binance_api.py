"""
binance_api.py – Binance spot price feed with rolling tape for momentum signals.

Maintains a short rolling tape per symbol for computing returns:
  - ret_30s  = (px_now / px_30s_ago) - 1
  - ret_120s = (px_now / px_120s_ago) - 1

Provides both sync (legacy) and async methods.  Async methods use the
global httpx.AsyncClient from http_client.py for HTTP/2 connection reuse.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Deque, Dict, Optional, Tuple

import requests

from config import Config
from logger import Logger


# Symbol mapping: our asset names → Binance spot pairs
_BINANCE_PAIRS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "XRP": "XRPUSDT",
}


class BinanceAPI:
    """Binance spot price feed with rolling tape."""

    def __init__(self, cfg: Config, logger: Logger):
        self.cfg = cfg
        self.log = logger
        self._base_url = "https://api.binance.com"

        # Rolling price tape: symbol -> deque of (epoch_sec, price)
        self._tapes: Dict[str, Deque[Tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=600)  # ~5 min at 2 samples/sec
        )

        # Last known prices
        self._last_prices: Dict[str, float] = {}
        self._last_fetch_ts: float = 0.0

    # ------------------------------------------------------------------
    # Price fetch
    # ------------------------------------------------------------------
    def get_last_price(self, asset: str) -> float | None:
        """Fetch latest spot price for an asset (e.g. 'BTC')."""
        symbol = _BINANCE_PAIRS.get(asset)
        if not symbol:
            return None

        try:
            resp = requests.get(
                f"{self._base_url}/api/v3/ticker/price",
                params={"symbol": symbol},
                timeout=3,
            )
            resp.raise_for_status()
            data = resp.json()
            price = float(data["price"])
            self._record(asset, price)
            return price
        except Exception as e:
            self.log.api_error(fn="binance_get_price", asset=asset, error=str(e))
            return self._last_prices.get(asset)

    def refresh_all(self) -> Dict[str, float]:
        """Fetch prices for all configured assets in one batch call."""
        now = time.time()
        prices: Dict[str, float] = {}

        try:
            resp = requests.get(
                f"{self._base_url}/api/v3/ticker/price",
                timeout=5,
            )
            resp.raise_for_status()
            data = resp.json()
            price_map = {item["symbol"]: float(item["price"]) for item in data}

            for asset in self.cfg.ASSETS:
                symbol = _BINANCE_PAIRS.get(asset)
                if symbol and symbol in price_map:
                    px = price_map[symbol]
                    self._record(asset, px)
                    prices[asset] = px

            self._last_fetch_ts = now
        except Exception as e:
            self.log.api_error(fn="binance_refresh_all", error=str(e))
            prices = dict(self._last_prices)

        return prices

    def _record(self, asset: str, price: float) -> None:
        """Record price observation to tape."""
        now = time.time()
        self._tapes[asset].append((now, price))
        self._last_prices[asset] = price

    # ------------------------------------------------------------------
    # Returns computation
    # ------------------------------------------------------------------
    def ret_30s(self, asset: str) -> float | None:
        """30-second return for the asset."""
        return self._ret(asset, 30.0)

    def ret_120s(self, asset: str) -> float | None:
        """120-second return for the asset."""
        return self._ret(asset, 120.0)

    def _ret(self, asset: str, lookback_sec: float) -> float | None:
        """Compute return over the last `lookback_sec` seconds."""
        tape = self._tapes.get(asset)
        if not tape or len(tape) < 2:
            return None

        now_ts, now_px = tape[-1]
        target_ts = now_ts - lookback_sec

        # Find the observation closest to target_ts
        best_ts, best_px = tape[0]
        for ts, px in tape:
            if ts <= target_ts:
                best_ts, best_px = ts, px
            else:
                break

        # Need at least some history
        if now_ts - best_ts < lookback_sec * 0.5:
            return None

        if best_px <= 0:
            return None

        return (now_px / best_px) - 1.0

    def last_price(self, asset: str) -> float | None:
        return self._last_prices.get(asset)

    # ==================================================================
    # Async methods — use the global httpx.AsyncClient (HTTP/2, pooled)
    # ==================================================================
    async def async_refresh_all(self) -> Dict[str, float]:
        """Fetch prices for all configured assets using the shared async client."""
        from http_client import get_client
        client = get_client()
        now = time.time()
        prices: Dict[str, float] = {}

        try:
            resp = await client.get(
                f"{self._base_url}/api/v3/ticker/price",
                timeout=5.0,
            )
            resp.raise_for_status()
            data = resp.json()
            price_map = {item["symbol"]: float(item["price"]) for item in data}

            for asset in self.cfg.ASSETS:
                symbol = _BINANCE_PAIRS.get(asset)
                if symbol and symbol in price_map:
                    px = price_map[symbol]
                    self._record(asset, px)
                    prices[asset] = px

            self._last_fetch_ts = now
        except Exception as e:
            self.log.api_error(fn="async_binance_refresh_all", error=str(e))
            prices = dict(self._last_prices)

        return prices
