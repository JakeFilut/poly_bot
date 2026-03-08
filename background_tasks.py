"""
background_tasks.py – Async background tasks that continuously update
shared in-memory state (order-book snapshots, Binance prices, universe,
fill sync).

The decision loop never does HTTP; it reads the latest snapshots that
these tasks keep warm.

Safety features:
  - Every task loop is wrapped in try/except — tasks never die silently
  - Per-task heartbeat timestamps exposed via TaskHeartbeats
  - asyncio.Semaphore caps concurrent order-book HTTP calls
  - Exponential backoff with jitter on 429 / 5xx per token
  - Fixed cadence + small jitter to avoid thundering-herd
"""
from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime, timezone
from typing import Dict

import httpx

from binance_api import BinanceAPI
from config import Config
from features import FeatureEngine
from logger import Logger
from market_universe import MarketUniverse
from polymarket_api import PolymarketAPI


# ──────────────────────────────────────────────────────────────────────
# Heartbeat tracker — shared mutable state read by metrics logger
# ──────────────────────────────────────────────────────────────────────
class TaskHeartbeats:
    """Timestamps of the last successful iteration per background task."""

    def __init__(self) -> None:
        self.binance: float = 0.0
        self.orderbooks: float = 0.0
        self.universe: float = 0.0
        self.fills: float = 0.0
        self.conn_warmup: float = 0.0
        # per-token error backoff: token_id -> next_allowed_epoch
        self._book_backoff: Dict[str, float] = {}

    def snapshot(self, now: float | None = None) -> dict:
        """Return ms-ago values for all heartbeats."""
        now = now or time.time()

        def _ago(ts: float) -> float:
            return round((now - ts) * 1000, 1) if ts > 0 else -1.0

        return {
            "last_binance_update_ms_ago": _ago(self.binance),
            "last_orderbook_update_ms_ago": _ago(self.orderbooks),
            "last_universe_refresh_ms_ago": _ago(self.universe),
            "last_fills_sync_ms_ago": _ago(self.fills),
            "last_conn_warmup_ms_ago": _ago(self.conn_warmup),
        }


# Module-level singleton so main.py and tasks can share it
heartbeats = TaskHeartbeats()


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
            await binance.async_refresh_ticker()
            heartbeats.binance = time.time()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.api_error(fn="bg_poll_binance", error=str(e))
        await asyncio.sleep(interval + random.uniform(0, interval * 0.1))


# ──────────────────────────────────────────────────────────────────────
# 2.  Order-book poller (semaphore-limited, per-token backoff)
# ──────────────────────────────────────────────────────────────────────
async def poll_orderbooks(
    features: FeatureEngine,
    universe: MarketUniverse,
    api: PolymarketAPI,
    log: Logger,
    *,
    interval_ms: int = 300,
    max_inflight: int = 10,
) -> None:
    """Continuously refresh order-book snapshots for every active token.

    Concurrency is capped by an asyncio.Semaphore (BOOK_MAX_INFLIGHT).
    Individual tokens that return 429 or 5xx get exponential backoff.
    """
    interval = interval_ms / 1000.0
    sem = asyncio.Semaphore(max_inflight)

    while True:
        try:
            token_ids = universe.all_token_ids()
            if token_ids:
                now = time.time()
                tasks = [
                    _fetch_and_cache_book(features, api, tid, sem, log, now)
                    for tid in token_ids
                ]
                await asyncio.gather(*tasks, return_exceptions=True)
                heartbeats.orderbooks = time.time()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.api_error(fn="bg_poll_orderbooks", error=str(e))
        await asyncio.sleep(interval + random.uniform(0, interval * 0.1))


async def _fetch_and_cache_book(
    features: FeatureEngine,
    api: PolymarketAPI,
    token_id: str,
    sem: asyncio.Semaphore,
    log: Logger,
    now: float,
) -> None:
    """Fetch one order-book (semaphore-gated) with 429/5xx backoff."""
    # Check per-token backoff
    backoff_until = heartbeats._book_backoff.get(token_id, 0)
    if now < backoff_until:
        return  # still in backoff

    async with sem:
        try:
            book = await api.async_get_orderbook(token_id)
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 429 or status >= 500:
                # Exponential backoff with jitter for this token
                current_bo = heartbeats._book_backoff.get(token_id, 0)
                prev_wait = max(0, current_bo - now)
                next_wait = min(30.0, max(1.0, prev_wait * 2) + random.uniform(0, 0.5))
                heartbeats._book_backoff[token_id] = time.time() + next_wait
                log.api_error(
                    fn="bg_book_fetch", token_id=token_id[:16],
                    status=status, backoff_sec=round(next_wait, 1),
                )
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    if book is not None:
        features._book_cache[token_id] = book
        features._book_cache_ts[token_id] = time.time()
        # Clear any backoff on success
        heartbeats._book_backoff.pop(token_id, None)


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
    """Refresh the market universe when needed (periodic or rollover)."""
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
                heartbeats.universe = time.time()
        except asyncio.CancelledError:
            raise
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
    """Periodically sync exchange fills into local state."""
    interval = interval_ms / 1000.0
    while True:
        try:
            fill_count = await asyncio.to_thread(engine.execution.sync_fills)
            if fill_count > 0:
                log.info("bg_fills_synced", count=fill_count)
            heartbeats.fills = time.time()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"bg_fill_sync_error: {e}")
        await asyncio.sleep(interval + random.uniform(0, interval * 0.1))


# ──────────────────────────────────────────────────────────────────────
# 5.  Connection warm-up keep-alive task
# ──────────────────────────────────────────────────────────────────────
async def warm_connection(
    cfg: Config,
    log: Logger,
    *,
    interval_sec: float = 15.0,
) -> None:
    """Periodically hit Gamma /status to keep the HTTP/2 connection warm
    and confirm connection reuse is working in practice.

    Logs total_ms and http_version on every probe.
    """
    from http_client import get_client
    url = f"{cfg.GAMMA_API_URL}/markets"  # lightweight endpoint

    while True:
        try:
            client = get_client()
            t0 = time.monotonic()
            resp = await client.get(url, params={"limit": 1}, timeout=5.0)
            elapsed_ms = (time.monotonic() - t0) * 1000
            http_ver = getattr(resp, "http_version", "unknown")
            log.log(
                "CONN_KEEPALIVE",
                url=url,
                status=resp.status_code,
                total_ms=round(elapsed_ms, 1),
                http_version=http_ver,
            )
            heartbeats.conn_warmup = time.time()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.api_error(fn="bg_warm_connection", error=str(e))
        await asyncio.sleep(interval_sec + random.uniform(0, 2))


# ──────────────────────────────────────────────────────────────────────
# 6.  Startup warmup + connection-reuse probe
# ──────────────────────────────────────────────────────────────────────
async def warmup(
    engine: "StrategyEngine",
    log: Logger,
) -> None:
    """Pre-populate all caches concurrently at startup."""
    log.info("warmup_begin")

    # ── Connection-reuse proof: 3 sequential GETs to the same origin ──
    await _probe_connection_reuse(engine.cfg, log)

    # Universe + Binance concurrently
    await asyncio.gather(
        engine.universe.async_refresh(),
        engine.binance.async_refresh_all(),
        return_exceptions=True,
    )
    heartbeats.universe = time.time()
    heartbeats.binance = time.time()

    # All order-books concurrently (with semaphore)
    token_ids = engine.universe.all_token_ids()
    if token_ids:
        sem = asyncio.Semaphore(engine.cfg.BOOK_MAX_INFLIGHT)
        tasks = [
            _fetch_and_cache_book(engine.features, engine.pm_api, tid, sem, log, time.time())
            for tid in token_ids
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    heartbeats.orderbooks = time.time()

    log.info(
        "warmup_complete",
        universe_count=len(engine.universe.pairs),
        book_cache_count=len(engine.features._book_cache),
        binance_prices=len(engine.binance._last_prices),
    )


async def _probe_connection_reuse(cfg: Config, log: Logger) -> None:
    """Fire 3 sequential GETs to prove HTTP/2 connection reuse.

    Expect: first ~100ms (TLS handshake), second/third ~20-30ms.
    """
    from http_client import get_client
    client = get_client()
    url = f"{cfg.GAMMA_API_URL}/markets"
    timings = []

    for i in range(3):
        try:
            t0 = time.monotonic()
            resp = await client.get(url, params={"limit": 1}, timeout=10.0)
            elapsed_ms = (time.monotonic() - t0) * 1000
            http_ver = getattr(resp, "http_version", "unknown")
            timings.append({
                "seq": i + 1,
                "total_ms": round(elapsed_ms, 1),
                "status": resp.status_code,
                "http_version": http_ver,
            })
        except Exception as e:
            timings.append({"seq": i + 1, "error": str(e)})

    reuse_confirmed = (
        len(timings) >= 3
        and all("total_ms" in t for t in timings)
        and timings[1]["total_ms"] < timings[0]["total_ms"] * 0.6
    )
    log.log(
        "CONN_REUSE_PROBE",
        timings=timings,
        reuse_confirmed=reuse_confirmed,
    )
