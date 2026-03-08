"""
features.py – Feature engineering for each (slug, outcome) at time t.

Computes:
  - best_bid, best_ask, mid, spread
  - spread_percentile_60s
  - binance ret_5s, ret_30s, ret_60s, ret_120s, ret_300s, ret_900s
  - realized volatility (rolling σ)
  - binance bid/ask spread
  - orderbook delta (depth change vs previous tick)
  - poly-binance divergence
All fast, no blocking I/O.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from binance_api import BinanceAPI
from market_universe import MarketUniverse
from polymarket_api import BookSnapshot, PolymarketAPI
from state import StateManager


@dataclass
class TokenFeatures:
    """Computed features for a single (slug, outcome) token."""
    slug: str
    outcome: str
    token_id: str
    asset: str

    # Book
    best_bid: float = 0.0
    best_ask: float = 1.0
    mid: float = 0.5
    spread: float = 1.0
    bid_size: float = 0.0
    ask_size: float = 0.0

    # Spread percentile
    spread_pctl_60s: float = 0.0

    # Binance momentum
    ret_5s: float | None = None
    ret_30s: float | None = None
    ret_60s: float | None = None
    ret_120s: float | None = None
    ret_300s: float | None = None   # 5-minute return
    ret_900s: float | None = None   # 15-minute return

    # Rolling realized volatility (σ of 30s returns over 5min)
    realized_vol: float | None = None

    # Binance bid/ask spread (bps)
    binance_spread_bps: float | None = None

    # Binance volume
    binance_volume_24h: float | None = None

    # Orderbook delta (change vs previous tick)
    bid_depth_delta: float = 0.0
    ask_depth_delta: float = 0.0
    imbalance_delta: float = 0.0

    # Poly-Binance divergence
    # Direction: positive = Poly mid rising faster than Binance (or Binance lagging)
    poly_binance_div_bps: float | None = None

    # Book snapshot (for execution)
    book: BookSnapshot | None = None

    # Freshness
    book_age_ms: float = 99999.0

    @property
    def has_book(self) -> bool:
        return self.book is not None and self.spread < 1.0


@dataclass
class SlugFeatures:
    """Features for both outcomes of a slug."""
    slug: str
    asset: str
    up: TokenFeatures | None = None
    down: TokenFeatures | None = None
    ret_5s: float | None = None
    ret_30s: float | None = None
    ret_60s: float | None = None
    ret_120s: float | None = None
    ret_300s: float | None = None
    ret_900s: float | None = None
    realized_vol: float | None = None
    binance_spread_bps: float | None = None
    binance_volume_24h: float | None = None


class FeatureEngine:
    """Computes features for all active slugs. Called each loop iteration."""

    def __init__(self, state: StateManager, api: PolymarketAPI,
                 binance: BinanceAPI, universe: MarketUniverse,
                 book_max_stale_ms: int = 5000):
        self._state = state
        self._api = api
        self._binance = binance
        self._universe = universe

        # Cache: token_id -> last BookSnapshot
        # Populated by background_tasks.poll_orderbooks(); read-only in tick.
        self._book_cache: Dict[str, BookSnapshot] = {}
        self._book_cache_ts: Dict[str, float] = {}
        self._book_max_stale_sec = book_max_stale_ms / 1000.0

        # Last computed features (for analytics lookups)
        self._last_features: Dict[str, "SlugFeatures"] = {}

        # Previous tick depth for delta computation
        self._prev_depth: Dict[str, tuple] = {}  # token_id -> (bid_size, ask_size, imbalance)

    def compute_all(self) -> Dict[str, SlugFeatures]:
        """Compute features for all active slugs.  Returns slug -> SlugFeatures."""
        now = time.time()
        result: Dict[str, SlugFeatures] = {}

        for slug, pair in self._universe.pairs.items():
            sf = SlugFeatures(slug=slug, asset=pair.asset)

            # Binance returns (shared for both outcomes)
            sf.ret_5s = self._binance.ret_5s(pair.asset)
            sf.ret_30s = self._binance.ret_30s(pair.asset)
            sf.ret_60s = self._binance.ret_60s(pair.asset)
            sf.ret_120s = self._binance.ret_120s(pair.asset)
            sf.ret_300s = self._binance.ret_300s(pair.asset)
            sf.ret_900s = self._binance.ret_900s(pair.asset)

            # Realized volatility + Binance spread + volume
            sf.realized_vol = self._binance.realized_vol(pair.asset)
            sf.binance_spread_bps = self._binance.binance_spread_bps(pair.asset)
            sf.binance_volume_24h = self._binance.volume_24h(pair.asset)

            # Up token features
            sf.up = self._compute_token(
                slug, "Up", pair.up_token_id, pair.asset,
                sf.ret_5s, sf.ret_30s, sf.ret_60s, sf.ret_120s,
                sf.ret_300s, sf.ret_900s,
                sf.realized_vol, sf.binance_spread_bps, sf.binance_volume_24h,
                now,
            )

            # Down token features
            sf.down = self._compute_token(
                slug, "Down", pair.down_token_id, pair.asset,
                sf.ret_5s, sf.ret_30s, sf.ret_60s, sf.ret_120s,
                sf.ret_300s, sf.ret_900s,
                sf.realized_vol, sf.binance_spread_bps, sf.binance_volume_24h,
                now,
            )

            result[slug] = sf

        self._last_features = result
        return result

    def _compute_token(self, slug: str, outcome: str, token_id: str,
                       asset: str, ret_5s, ret_30s, ret_60s, ret_120s,
                       ret_300s, ret_900s,
                       realized_vol, binance_spread_bps, binance_volume_24h,
                       now: float) -> TokenFeatures:
        """Compute features for one token."""
        tf = TokenFeatures(
            slug=slug, outcome=outcome, token_id=token_id, asset=asset,
            ret_5s=ret_5s, ret_30s=ret_30s, ret_60s=ret_60s, ret_120s=ret_120s,
            ret_300s=ret_300s, ret_900s=ret_900s,
            realized_vol=realized_vol,
            binance_spread_bps=binance_spread_bps,
            binance_volume_24h=binance_volume_24h,
        )

        # Get orderbook (with cache)
        book = self._get_book_cached(token_id, now)
        if book is None:
            return tf

        tf.book = book
        tf.best_bid = book.best_bid
        tf.best_ask = book.best_ask
        tf.mid = book.mid
        tf.spread = book.spread
        tf.bid_size = book.bid_size
        tf.ask_size = book.ask_size
        tf.book_age_ms = (now - book.ts) * 1000

        # Record spread to tape
        self._state.spread_tapes[token_id].add(now, book.spread)

        # Compute spread percentile over last 60s
        tf.spread_pctl_60s = self._spread_percentile(token_id, book.spread, now)

        # Orderbook delta vs previous tick
        prev = self._prev_depth.get(token_id)
        if prev is not None:
            tf.bid_depth_delta = book.bid_size - prev[0]
            tf.ask_depth_delta = book.ask_size - prev[1]
            total_now = book.bid_size + book.ask_size
            imb_now = book.bid_size / max(total_now, 1e-9) if total_now > 0 else 0.5
            tf.imbalance_delta = imb_now - prev[2]
        self._prev_depth[token_id] = (
            book.bid_size, book.ask_size,
            book.bid_size / max(book.bid_size + book.ask_size, 1e-9)
        )

        # Poly-Binance divergence
        binance_px = self._binance.last_price(asset)
        if binance_px and binance_px > 0 and book.mid > 0:
            # Both are prices in different scales, so we track Poly mid change
            # vs Binance change. For now, store the raw values for the tape;
            # the replay engine will compute the meaningful divergence from the
            # time-series.
            tf.poly_binance_div_bps = binance_px  # stored as raw; strategy interprets

        return tf

    def _get_book_cached(self, token_id: str, now: float) -> BookSnapshot | None:
        """Read order-book from cache.  Background tasks keep it warm.

        No HTTP calls happen here — the decision loop must never block on
        network I/O.  If the cached data is older than _book_max_stale_sec
        it is treated as missing so the strategy skips the token.
        """
        book = self._book_cache.get(token_id)
        if book is None:
            return None
        age = now - self._book_cache_ts.get(token_id, 0)
        if age > self._book_max_stale_sec:
            return None  # too stale — refuse
        return book

    def _spread_percentile(self, token_id: str, current_spread: float,
                           now: float) -> float:
        """Percentile of current spread vs last 60s of observations."""
        tape = self._state.spread_tapes.get(token_id)
        if tape is None or len(tape) < 5:
            return 0.5  # not enough data

        values = tape.values(now)
        if len(values) < 5:
            return 0.5

        count_below = sum(1 for v in values if v <= current_spread)
        return count_below / len(values)
