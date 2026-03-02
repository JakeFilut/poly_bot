"""
features.py – Feature engineering for each (slug, outcome) at time t.

Computes:
  - best_bid, best_ask, mid, spread
  - spread_percentile_60s
  - binance ret_30s, ret_120s
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
    ret_30s: float | None = None
    ret_120s: float | None = None

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
    ret_30s: float | None = None
    ret_120s: float | None = None


class FeatureEngine:
    """Computes features for all active slugs. Called each loop iteration."""

    def __init__(self, state: StateManager, api: PolymarketAPI,
                 binance: BinanceAPI, universe: MarketUniverse):
        self._state = state
        self._api = api
        self._binance = binance
        self._universe = universe

        # Cache: token_id -> last BookSnapshot
        self._book_cache: Dict[str, BookSnapshot] = {}
        self._book_cache_ts: Dict[str, float] = {}
        self._book_cache_ttl = 0.5  # 500ms

        # Last computed features (for analytics lookups)
        self._last_features: Dict[str, "SlugFeatures"] = {}

    def compute_all(self) -> Dict[str, SlugFeatures]:
        """Compute features for all active slugs.  Returns slug -> SlugFeatures."""
        now = time.time()
        result: Dict[str, SlugFeatures] = {}

        for slug, pair in self._universe.pairs.items():
            sf = SlugFeatures(slug=slug, asset=pair.asset)

            # Binance returns (shared for both outcomes)
            sf.ret_30s = self._binance.ret_30s(pair.asset)
            sf.ret_120s = self._binance.ret_120s(pair.asset)

            # Up token features
            sf.up = self._compute_token(
                slug, "Up", pair.up_token_id, pair.asset, sf.ret_30s, sf.ret_120s, now
            )

            # Down token features
            sf.down = self._compute_token(
                slug, "Down", pair.down_token_id, pair.asset, sf.ret_30s, sf.ret_120s, now
            )

            result[slug] = sf

        self._last_features = result
        return result

    def _compute_token(self, slug: str, outcome: str, token_id: str,
                       asset: str, ret_30s, ret_120s, now: float) -> TokenFeatures:
        """Compute features for one token."""
        tf = TokenFeatures(
            slug=slug, outcome=outcome, token_id=token_id, asset=asset,
            ret_30s=ret_30s, ret_120s=ret_120s,
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

        return tf

    def _get_book_cached(self, token_id: str, now: float) -> BookSnapshot | None:
        """Get orderbook with TTL cache."""
        cached_ts = self._book_cache_ts.get(token_id, 0)
        if now - cached_ts < self._book_cache_ttl:
            return self._book_cache.get(token_id)

        book = self._api.get_orderbook(token_id)
        if book is not None:
            self._book_cache[token_id] = book
            self._book_cache_ts[token_id] = now
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
