"""
binance_api.py – Binance spot price feed with rolling tape for momentum signals.

Maintains a short rolling tape per symbol for computing returns:
  - ret_5s   = (px_now / px_5s_ago) - 1
  - ret_30s  = (px_now / px_30s_ago) - 1
  - ret_60s  = (px_now / px_60s_ago) - 1
  - ret_120s = (px_now / px_120s_ago) - 1
  - ret_300s = (px_now / px_5m_ago) - 1
  - ret_900s = (px_now / px_15m_ago) - 1

Also captures:
  - 24h volume (from /ticker/24hr)
  - Binance bid/ask spread
  - Rolling realized volatility (σ of 30s returns over 5min window)

Provides both sync (legacy) and async methods.  Async methods use the
global httpx.AsyncClient from http_client.py for HTTP/2 connection reuse.
"""
from __future__ import annotations

import math
import time
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional, Tuple

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
    """Binance spot price feed with rolling tape, volume, and volatility."""

    def __init__(self, cfg: Config, logger: Logger):
        self.cfg = cfg
        self.log = logger
        self._base_url = "https://api.binance.com"

        # Rolling price tape: symbol -> deque of (epoch_sec, price)
        self._tapes: Dict[str, Deque[Tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=1200)  # ~10 min at 2 Hz (extended for 5m/15m returns)
        )

        # Last known prices
        self._last_prices: Dict[str, float] = {}
        self._last_fetch_ts: float = 0.0

        # --- Volume / 24hr ticker data ---
        # asset -> {volume_24h, quote_volume_24h, bid_price, ask_price, bid_qty, ask_qty}
        self._ticker_data: Dict[str, dict] = {}
        self._ticker_fetch_ts: float = 0.0
        self._ticker_ttl: float = 5.0  # refresh every 5s (rate-limit friendly)

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
    def ret_5s(self, asset: str) -> float | None:
        """5-second return for the asset (short-term momentum)."""
        return self._ret(asset, 5.0)

    def ret_30s(self, asset: str) -> float | None:
        """30-second return for the asset."""
        return self._ret(asset, 30.0)

    def ret_60s(self, asset: str) -> float | None:
        """60-second return for the asset."""
        return self._ret(asset, 60.0)

    def ret_120s(self, asset: str) -> float | None:
        """120-second return for the asset."""
        return self._ret(asset, 120.0)

    def ret_300s(self, asset: str) -> float | None:
        """5-minute return for the asset (medium-term trend)."""
        return self._ret(asset, 300.0)

    def ret_900s(self, asset: str) -> float | None:
        """15-minute return for the asset (longer trend)."""
        return self._ret(asset, 900.0)

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

    # ------------------------------------------------------------------
    # Rolling realized volatility
    # ------------------------------------------------------------------
    def realized_vol(self, asset: str, window_sec: float = 300.0,
                     sample_interval: float = 30.0) -> float | None:
        """Rolling realized volatility: σ of log returns sampled at `sample_interval`
        over the last `window_sec` seconds.

        Default: σ of 30s returns over a 5-minute window (~10 samples).
        Returns annualized-ish value (per-sample σ, not annualized).
        """
        tape = self._tapes.get(asset)
        if not tape or len(tape) < 5:
            return None

        now_ts = tape[-1][0]
        cutoff = now_ts - window_sec

        # Sample prices at regular intervals from the tape
        prices_at_intervals: List[float] = []
        target_ts = cutoff
        for ts, px in tape:
            if ts < cutoff:
                continue
            if ts >= target_ts:
                prices_at_intervals.append(px)
                target_ts = ts + sample_interval

        if len(prices_at_intervals) < 3:
            return None

        # Compute log returns
        log_rets = []
        for i in range(1, len(prices_at_intervals)):
            if prices_at_intervals[i - 1] > 0:
                log_rets.append(
                    math.log(prices_at_intervals[i] / prices_at_intervals[i - 1])
                )

        if len(log_rets) < 2:
            return None

        # Standard deviation of log returns
        mean_r = sum(log_rets) / len(log_rets)
        var = sum((r - mean_r) ** 2 for r in log_rets) / (len(log_rets) - 1)
        return math.sqrt(var)

    # ------------------------------------------------------------------
    # 24hr ticker: volume + bid/ask spread
    # ------------------------------------------------------------------
    def refresh_ticker(self) -> None:
        """Fetch 24hr ticker data for volume and bid/ask spread.
        Rate-limited to once per _ticker_ttl seconds.
        """
        now = time.time()
        if now - self._ticker_fetch_ts < self._ticker_ttl:
            return

        try:
            # Fetch only our symbols using the weight-efficient bookTicker endpoint
            for asset in self.cfg.ASSETS:
                symbol = _BINANCE_PAIRS.get(asset)
                if not symbol:
                    continue

                # bookTicker: bid/ask price+qty (weight=2, very cheap)
                resp_book = requests.get(
                    f"{self._base_url}/api/v3/ticker/bookTicker",
                    params={"symbol": symbol},
                    timeout=3,
                )
                if resp_book.status_code == 200:
                    bd = resp_book.json()
                    entry = self._ticker_data.get(asset, {})
                    entry["bid_price"] = float(bd.get("bidPrice", 0))
                    entry["ask_price"] = float(bd.get("askPrice", 0))
                    entry["bid_qty"] = float(bd.get("bidQty", 0))
                    entry["ask_qty"] = float(bd.get("askQty", 0))
                    bp = entry["bid_price"]
                    ap = entry["ask_price"]
                    entry["spread"] = (ap - bp) if bp > 0 and ap > 0 else 0.0
                    entry["spread_bps"] = (
                        (ap - bp) / ((ap + bp) / 2) * 10000
                        if bp > 0 and ap > 0 else 0.0
                    )
                    self._ticker_data[asset] = entry

            # 24hr volume (slightly heavier, but only 4 symbols)
            resp_24hr = requests.get(
                f"{self._base_url}/api/v3/ticker/24hr",
                timeout=5,
            )
            if resp_24hr.status_code == 200:
                vol_map = {
                    item["symbol"]: item for item in resp_24hr.json()
                }
                for asset in self.cfg.ASSETS:
                    symbol = _BINANCE_PAIRS.get(asset)
                    if symbol and symbol in vol_map:
                        vd = vol_map[symbol]
                        entry = self._ticker_data.get(asset, {})
                        entry["volume_24h"] = float(vd.get("volume", 0))
                        entry["quote_volume_24h"] = float(vd.get("quoteVolume", 0))
                        self._ticker_data[asset] = entry

            self._ticker_fetch_ts = now
        except Exception as e:
            self.log.api_error(fn="binance_refresh_ticker", error=str(e))

    def volume_24h(self, asset: str) -> float | None:
        """24h base volume for the asset."""
        entry = self._ticker_data.get(asset)
        return entry.get("volume_24h") if entry else None

    def quote_volume_24h(self, asset: str) -> float | None:
        """24h quote (USDT) volume for the asset."""
        entry = self._ticker_data.get(asset)
        return entry.get("quote_volume_24h") if entry else None

    def binance_spread(self, asset: str) -> float | None:
        """Current Binance bid-ask spread in absolute price terms."""
        entry = self._ticker_data.get(asset)
        return entry.get("spread") if entry else None

    def binance_spread_bps(self, asset: str) -> float | None:
        """Current Binance bid-ask spread in basis points."""
        entry = self._ticker_data.get(asset)
        return entry.get("spread_bps") if entry else None

    def binance_bid(self, asset: str) -> float | None:
        """Best bid price on Binance."""
        entry = self._ticker_data.get(asset)
        return entry.get("bid_price") if entry else None

    def binance_ask(self, asset: str) -> float | None:
        """Best ask price on Binance."""
        entry = self._ticker_data.get(asset)
        return entry.get("ask_price") if entry else None

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

    async def async_refresh_ticker(self) -> None:
        """Async version of refresh_ticker for volume + bid/ask spread."""
        from http_client import get_client
        client = get_client()
        now = time.time()
        if now - self._ticker_fetch_ts < self._ticker_ttl:
            return

        try:
            # bookTicker for bid/ask
            for asset in self.cfg.ASSETS:
                symbol = _BINANCE_PAIRS.get(asset)
                if not symbol:
                    continue
                resp = await client.get(
                    f"{self._base_url}/api/v3/ticker/bookTicker",
                    params={"symbol": symbol},
                    timeout=3.0,
                )
                if resp.status_code == 200:
                    bd = resp.json()
                    entry = self._ticker_data.get(asset, {})
                    entry["bid_price"] = float(bd.get("bidPrice", 0))
                    entry["ask_price"] = float(bd.get("askPrice", 0))
                    entry["bid_qty"] = float(bd.get("bidQty", 0))
                    entry["ask_qty"] = float(bd.get("askQty", 0))
                    bp = entry["bid_price"]
                    ap = entry["ask_price"]
                    entry["spread"] = (ap - bp) if bp > 0 and ap > 0 else 0.0
                    entry["spread_bps"] = (
                        (ap - bp) / ((ap + bp) / 2) * 10000
                        if bp > 0 and ap > 0 else 0.0
                    )
                    self._ticker_data[asset] = entry

            # 24hr volume
            resp_24hr = await client.get(
                f"{self._base_url}/api/v3/ticker/24hr",
                timeout=5.0,
            )
            if resp_24hr.status_code == 200:
                vol_map = {
                    item["symbol"]: item for item in resp_24hr.json()
                }
                for asset in self.cfg.ASSETS:
                    symbol = _BINANCE_PAIRS.get(asset)
                    if symbol and symbol in vol_map:
                        vd = vol_map[symbol]
                        entry = self._ticker_data.get(asset, {})
                        entry["volume_24h"] = float(vd.get("volume", 0))
                        entry["quote_volume_24h"] = float(vd.get("quoteVolume", 0))
                        self._ticker_data[asset] = entry

            self._ticker_fetch_ts = now
        except Exception as e:
            self.log.api_error(fn="async_binance_refresh_ticker", error=str(e))
