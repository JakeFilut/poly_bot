"""
background_tasks.py – Async background tasks that continuously update
shared in-memory state (order-book snapshots, Binance prices, universe,
fill sync).

The decision loop never does HTTP; it reads the latest snapshots that
these tasks keep warm.

Each task uses a fixed cadence with jitter to avoid thundering-herd.
"""
from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime, timezone

from binance_api import BinanceAPI
from config import Config
from features import FeatureEngine
from logger import Logger
from market_universe import MarketUniverse
from polymarket_api import PolymarketAPI


# ──────────────────────────────────────────────────────────────────────
# 1.  Binance price poller
# ──────────────────────────────────────────────────────────────────────
async def poll_binance(
    binance: BinanceAPI,
    cfg: Config,
    log: Logger,
    *,
    interval_ms: int = 500,
) -> None:
    """Continuously poll Binance spot prices into the rolling tape."""
    interval = interval_ms / 1000.0
    while True:
        try:
            await binance.async_refresh_all()
        except Exception as e:
            log.api_error(fn="bg_poll_binance", error=str(e))
        # Fixed cadence + small jitter
        await asyncio.sleep(interval + random.uniform(0, interval * 0.1))


# ──────────────────────────────────────────────────────────────────────
# 2.  Order-book poller (all active tokens, concurrent)
# ──────────────────────────────────────────────────────────────────────
async def poll_orderbooks(
    features: FeatureEngine,
    universe: MarketUniverse,
    api: PolymarketAPI,
    log: Logger,
    *,
    interval_ms: int = 300,
) -> None:
    """Continuously refresh order-book snapshots for every active token.

    All tokens are fetched concurrently with asyncio.gather, then results
    are written into features._book_cache for the decision loop to read.
    """
    interval = interval_ms / 1000.0

    while True:
        token_ids = universe.all_token_ids()
        if token_ids:
            tasks = [
                _fetch_and_cache_book(features, api, tid)
                for tid in token_ids
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(interval + random.uniform(0, interval * 0.1))


async def _fetch_and_cache_book(
    features: FeatureEngine,
    api: PolymarketAPI,
    token_id: str,
) -> None:
    """Fetch one order-book and deposit it in the feature cache."""
    book = await api.async_get_orderbook(token_id)
    if book is not None:
        features._book_cache[token_id] = book
        features._book_cache_ts[token_id] = time.time()


# ──────────────────────────────────────────────────────────────────────
# 3.  Universe refresher
# ──────────────────────────────────────────────────────────────────────
async def refresh_universe(
    engine: "StrategyEngine",
    cfg: Config,
    log: Logger,
    *,
    check_interval_sec: float = 10.0,
) -> None:
    """Refresh the market universe when needed (periodic or rollover).

    Checks for rollover/staleness frequently but only does the actual
    HTTP fetch when MarketUniverse signals a need.  After refresh,
    purges stale caches on the strategy engine.
    """
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            rollover = engine.universe.needs_rollover(now_utc)
            if rollover or engine.universe.needs_refresh():
                if rollover:
                    log.info(
                        "bg_universe_rollover_triggered",
                        now_utc=now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                        earliest_end=(
                            engine.universe.earliest_end_utc().strftime("%Y-%m-%dT%H:%M:%SZ")
                            if engine.universe.earliest_end_utc() else "?"
                        ),
                        active_slugs=engine.universe.active_slugs(),
                    )
                await engine.universe.async_refresh()
                removed_tids = engine.universe.get_removed_token_ids()
                if removed_tids:
                    engine._purge_stale_caches(removed_tids)
        except Exception as e:
            log.error(f"bg_universe_refresh_error: {e}")

        await asyncio.sleep(check_interval_sec + random.uniform(0, 2))


# ──────────────────────────────────────────────────────────────────────
# 4.  Fill syncer (LIVE mode only)
# ──────────────────────────────────────────────────────────────────────
async def sync_fills(
    engine: "StrategyEngine",
    log: Logger,
    *,
    interval_ms: int = 1000,
) -> None:
    """Periodically sync exchange fills into local state.

    Runs the existing sync execution.sync_fills() in a thread to avoid
    blocking the event loop (py-clob-client uses requests internally).
    """
    interval = interval_ms / 1000.0
    while True:
        try:
            fill_count = await asyncio.to_thread(engine.execution.sync_fills)
            if fill_count > 0:
                log.info("bg_fills_synced", count=fill_count)
        except Exception as e:
            log.error(f"bg_fill_sync_error: {e}")
        await asyncio.sleep(interval + random.uniform(0, interval * 0.1))


# ──────────────────────────────────────────────────────────────────────
# 5.  Warmup — run once at startup to pre-populate caches
# ──────────────────────────────────────────────────────────────────────
async def warmup(
    engine: "StrategyEngine",
    log: Logger,
) -> None:
    """Pre-populate all caches concurrently at startup."""
    log.info("warmup_begin")

    # Universe + Binance concurrently
    await asyncio.gather(
        engine.universe.async_refresh(),
        engine.binance.async_refresh_all(),
        return_exceptions=True,
    )

    # All order-books concurrently
    token_ids = engine.universe.all_token_ids()
    if token_ids:
        tasks = [
            _fetch_and_cache_book(engine.features, engine.pm_api, tid)
            for tid in token_ids
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    log.info(
        "warmup_complete",
        universe_count=len(engine.universe.pairs),
        book_cache_count=len(engine.features._book_cache),
        binance_prices=len(engine.binance._last_prices),
    )
