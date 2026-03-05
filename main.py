#!/usr/bin/env python3
"""
main.py – Async orchestrator for the Polymarket scalper.

ARCHITECTURE (hot-snapshot model):

  Background fetch tasks (async, continuous):
    1. poll_binance        – Binance spot prices every ~500ms
    2. poll_orderbooks     – CLOB order-books every ~300ms (all tokens, concurrent)
    3. refresh_universe    – Market discovery every ~10s (only fetches when stale)
    4. sync_fills          – LIVE fill reconciliation every ~1s

  Decision loop (sync, fast, 5–20ms tick):
    - Reads ONLY in-memory snapshots (no HTTP awaits)
    - Computes features (CPU)
    - Runs strategy (CPU)
    - Executes actions (DRY_RUN: instant mock; LIVE: py-clob-client)

  Wallet tracker (isolated daemon thread):
    - Unchanged: polls F247 wallet, logs to CSV/JSONL
    - Zero shared execution path with trading engine

  Metrics:
    - Loop tick time: p50 / p95 / p99
    - Snapshot age: binance, order-books
    - Logged every LOG_ROLLUP_SEC

Usage:
    # DRY_RUN (default)
    MODE=DRY_RUN python main.py

    # LIVE trading
    MODE=LIVE POLYMARKET_API_KEY=... POLYMARKET_API_SECRET=... \\
        POLYMARKET_PRIVATE_KEY=0x... python main.py

See RUNBOOK section at bottom for systemd example.
"""
from __future__ import annotations

import asyncio
import bisect
import os
import random
import signal
import time
import uuid

from background_tasks import heartbeats
from config import load_config
from logger import Logger, _utc_iso


# ──────────────────────────────────────────────────────────────────────
# Tick-time metrics (lock-free, fixed-size ring)
# ──────────────────────────────────────────────────────────────────────
class TickMetrics:
    """Lightweight rolling percentile tracker for loop tick times."""

    def __init__(self, window: int = 500):
        self._window = window
        self._times: list[float] = []   # sorted insert for percentiles
        self._raw: list[float] = []     # insertion-order ring
        self._idx = 0

    def record(self, elapsed_ms: float) -> None:
        if len(self._raw) < self._window:
            self._raw.append(elapsed_ms)
            bisect.insort(self._times, elapsed_ms)
        else:
            # evict oldest
            old = self._raw[self._idx]
            pos = bisect.bisect_left(self._times, old)
            if pos < len(self._times) and self._times[pos] == old:
                self._times.pop(pos)
            bisect.insort(self._times, elapsed_ms)
            self._raw[self._idx] = elapsed_ms
            self._idx = (self._idx + 1) % self._window

    def percentile(self, p: float) -> float:
        if not self._times:
            return 0.0
        idx = int(len(self._times) * p)
        idx = min(idx, len(self._times) - 1)
        return self._times[idx]

    def snapshot(self) -> dict:
        return {
            "tick_p50_ms": round(self.percentile(0.50), 3),
            "tick_p95_ms": round(self.percentile(0.95), 3),
            "tick_p99_ms": round(self.percentile(0.99), 3),
            "tick_count": len(self._raw),
        }


class Bot:
    """Async orchestrator: runs background fetch tasks + fast decision loop
    + wallet tracker (isolated thread)."""

    def __init__(self):
        # -- Config (read-only, shared) --
        self.cfg = load_config()

        # -- RUN_ID: unique per process, included in every log event --
        self._run_id = uuid.uuid4().hex[:12]

        # -- Logger (shared for lifecycle events only) --
        self.log = Logger(
            log_file=self.cfg.LOG_FILE,
            rollup_sec=self.cfg.LOG_ROLLUP_SEC,
            run_id=self._run_id,
        )
        self.log.log_config(self.cfg.redacted_dict())

        # ── Module A: Strategy Engine (owns ALL trading) ─────────────
        from strategy_engine import StrategyEngine
        self.engine = StrategyEngine(self.cfg, self.log)

        # ── Module B: Wallet Tracker (read-only observer) ────────────
        self._tracker = None
        if self.cfg.TRACK_F247_WALLET:
            from wallet_tracker import WalletTracker
            self._tracker = WalletTracker(
                log_fn=self.log.info,
                bot_logger=self.log,
                diag_callback=self.engine.diagnostics.on_copywallet_fill,
            )

        # -- Shutdown flag --
        self._running = True
        self._last_state_flush = time.monotonic()

        # -- Metrics --
        self._tick_metrics = TickMetrics()
        self._last_metrics_log = time.monotonic()
        self._last_heartbeat_log = time.monotonic()

        # -- Background task handles --
        self._bg_tasks: list[asyncio.Task] = []

        self.log.info(
            "bot_startup",
            run_id=self._run_id,
            start_ts_utc=_utc_iso(),
            architecture="hot_snapshot_async",
            strategy_engine="strategy_engine.StrategyEngine",
            wallet_tracker="wallet_tracker.WalletTracker" if self._tracker else "disabled",
            isolation="ENFORCED — zero shared execution path",
        )

    # ------------------------------------------------------------------
    # Signal handling (asyncio-compatible)
    # ------------------------------------------------------------------
    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._handle_signal, sig)

    def _handle_signal(self, signum) -> None:
        sig_name = signal.Signals(signum).name
        self.log.info(f"signal_received: {sig_name}, shutting down gracefully")
        self._running = False

    # ------------------------------------------------------------------
    # Main async entry point
    # ------------------------------------------------------------------
    async def run(self) -> None:
        """Start background tasks, warm caches, then run the fast decision loop."""
        loop = asyncio.get_running_loop()
        self._install_signal_handlers(loop)

        # 1. Initialise the global HTTP/2 client
        from http_client import init_client
        await init_client()
        self.log.info("http2_client_initialised",
                      max_connections=100, max_keepalive=20)

        # 2. Async warmup: universe + Binance + order-books concurrently
        from background_tasks import warmup
        await warmup(self.engine, self.log)

        # 3. Sync startup (LIVE reconciliation, cash, baselines)
        self.engine.startup_sync()

        # 4. Launch background fetch tasks
        from background_tasks import (
            poll_binance, poll_orderbooks, refresh_universe, sync_fills,
            warm_connection,
        )

        self._bg_tasks = [
            asyncio.create_task(
                poll_binance(self.engine.binance, self.cfg, self.log,
                             interval_ms=self.cfg.BINANCE_POLL_MS),
                name="poll_binance",
            ),
            asyncio.create_task(
                poll_orderbooks(self.engine.features, self.engine.universe,
                                self.engine.pm_api, self.log,
                                interval_ms=self.cfg.BOOK_POLL_MS,
                                max_inflight=self.cfg.BOOK_MAX_INFLIGHT),
                name="poll_orderbooks",
            ),
            asyncio.create_task(
                refresh_universe(self.engine, self.cfg, self.log,
                                 check_interval_sec=10.0),
                name="refresh_universe",
            ),
            asyncio.create_task(
                warm_connection(self.cfg, self.log, interval_sec=15.0),
                name="warm_connection",
            ),
        ]

        if self.cfg.MODE == "LIVE":
            self._bg_tasks.append(
                asyncio.create_task(
                    sync_fills(self.engine, self.log,
                               interval_ms=self.cfg.FILLS_POLL_MS),
                    name="sync_fills",
                )
            )

        # 5. Start wallet tracker (isolated daemon thread)
        if self._tracker is not None:
            self._tracker.start()

        self.log.info("main_loop_started", mode=self.cfg.MODE,
                      target_loop_ms=self.cfg.TARGET_LOOP_MS,
                      bg_tasks=[t.get_name() for t in self._bg_tasks])

        # 6. Fast decision loop
        await self._decision_loop()

        # 7. Graceful shutdown
        await self._shutdown()

    # ------------------------------------------------------------------
    # Decision loop (reads in-memory snapshots only)
    # ------------------------------------------------------------------
    async def _decision_loop(self) -> None:
        """Tight loop: read cached data → compute features → strategy → execute.

        Target tick time: 5–20ms.  No HTTP awaits in this path.
        """
        loop_count = 0
        target_sec = self.cfg.TARGET_LOOP_MS / 1000.0

        while self._running:
            loop_start = time.monotonic()
            loop_count += 1

            # ── Strategy engine tick (CPU only) ──────────────────────
            try:
                self.engine.tick(loop_count)
                self.engine.risk.clear_errors()
            except KeyboardInterrupt:
                self._running = False
                break
            except Exception as e:
                self.log.error(f"tick_error: {e}", loop=loop_count)
                self.engine.risk.record_error()

            # State flush (periodic)
            now_mono = time.monotonic()
            if now_mono - self._last_state_flush > self.cfg.STATE_FLUSH_SEC:
                self.engine.flush_state()
                self._last_state_flush = now_mono

            # Post-tick analytics (rollups, hourly PnL, diagnostics)
            self.engine.post_tick()

            # Record tick time & maybe log metrics
            elapsed_ms = (time.monotonic() - loop_start) * 1000.0
            self._tick_metrics.record(elapsed_ms)
            self._maybe_log_metrics(now_mono)

            # Yield to event loop — lets background tasks run
            sleep_time = target_sec - (time.monotonic() - loop_start)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                await asyncio.sleep(0)  # yield anyway

    # ------------------------------------------------------------------
    # Metrics logging
    # ------------------------------------------------------------------
    def _maybe_log_metrics(self, now_mono: float) -> None:
        # Heartbeat log every ~5s (separate from rollup metrics)
        if now_mono - self._last_heartbeat_log >= 5.0:
            self._last_heartbeat_log = now_mono
            hb = heartbeats.snapshot()
            hb["bg_tasks_alive"] = sum(1 for t in self._bg_tasks if not t.done())
            self.log.log("HEARTBEAT", **hb)

        if now_mono - self._last_metrics_log < self.cfg.LOG_ROLLUP_SEC:
            return
        self._last_metrics_log = now_mono

        # Snapshot ages (detailed)
        book_ages = []
        for tid in self.engine.universe.all_token_ids():
            ts = self.engine.features._book_cache_ts.get(tid, 0)
            if ts > 0:
                book_ages.append((time.time() - ts) * 1000)
        avg_book_age = (sum(book_ages) / len(book_ages)) if book_ages else -1
        max_book_age = max(book_ages) if book_ages else -1

        bin_ts = self.engine.binance._last_fetch_ts
        binance_age = (time.time() - bin_ts) * 1000 if bin_ts > 0 else -1

        self.log.log(
            "LOOP_METRICS",
            **self._tick_metrics.snapshot(),
            **heartbeats.snapshot(),
            avg_book_age_ms=round(avg_book_age, 1),
            max_book_age_ms=round(max_book_age, 1),
            binance_age_ms=round(binance_age, 1),
            bg_tasks_alive=sum(1 for t in self._bg_tasks if not t.done()),
        )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def _shutdown(self) -> None:
        """Cancel background tasks, close HTTP client, shutdown engine.

        Order of operations matters for safety:
          1. Stop decision loop (already done — we exited _decision_loop)
          2. Cancel background tasks (no new data / no new fills)
          3. Shutdown engine (cancels pending orders, flushes state)
          4. Close HTTP client last (engine shutdown may need it)
        """
        self.log.info("shutdown_begin")

        # 1. Cancel background tasks
        for task in self._bg_tasks:
            task.cancel()
        results = await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        failed = [
            (self._bg_tasks[i].get_name(), repr(r))
            for i, r in enumerate(results)
            if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError)
        ]
        self.log.info("background_tasks_cancelled",
                      count=len(self._bg_tasks),
                      failed=failed if failed else None)

        # 2. Stop wallet tracker (it's just an observer)
        if self._tracker is not None:
            self._tracker.stop()

        # 3. Log pending order state before engine shutdown
        pending_count = len(self.engine.execution.pending_orders) if hasattr(self.engine, 'execution') and hasattr(self.engine.execution, 'pending_orders') else 0
        if pending_count > 0:
            self.log.info("shutdown_pending_orders",
                          count=pending_count,
                          action="engine.shutdown will cancel_all")

        # 4. Shutdown strategy engine (cancels orders, flushes state)
        self.engine.shutdown()

        # 5. Close the global HTTP client last
        from http_client import close_client
        await close_client()
        self.log.info("http2_client_closed")

        self.log.info("shutdown_complete")
        self.log.close()


def main():
    # Deterministic RNG for reproducible DRY_RUN auditing
    seed_env = os.environ.get("RANDOM_SEED")
    if seed_env is not None:
        seed_val = int(seed_env)
        random.seed(seed_val)
        print(f"[INIT] RANDOM_SEED={seed_val} — deterministic RNG enabled")

    bot = Bot()
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()


# ═══════════════════════════════════════════════════════════════════════════
# RUNBOOK
# ═══════════════════════════════════════════════════════════════════════════
#
# 1. DRY_RUN mode (no real orders, simulates fills):
#
#     MODE=DRY_RUN python main.py
#
#    Or with env file:
#     set -a; source .env; set +a; python main.py
#
# 2. LIVE mode (real orders on Polymarket):
#
#     MODE=LIVE \
#     POLYMARKET_API_KEY="your-key" \
#     POLYMARKET_API_SECRET="your-secret" \
#     POLYMARKET_API_PASSPHRASE="your-passphrase" \
#     POLYMARKET_PRIVATE_KEY="0xyour-private-key" \
#     python main.py
#
# 3. Example systemd unit (/etc/systemd/system/polybot.service):
#
#     [Unit]
#     Description=F247 Polymarket Scalper
#     After=network.target
#
#     [Service]
#     Type=simple
#     User=polybot
#     WorkingDirectory=/opt/poly_bot
#     EnvironmentFile=/opt/poly_bot/.env
#     ExecStart=/usr/bin/python3 /opt/poly_bot/main.py
#     Restart=on-failure
#     RestartSec=10
#     KillSignal=SIGTERM
#     TimeoutStopSec=30
#
#     [Install]
#     WantedBy=multi-user.target
#
#    Then:
#     sudo systemctl daemon-reload
#     sudo systemctl enable polybot
#     sudo systemctl start polybot
#     journalctl -u polybot -f   # watch logs
#
# 4. tmux usage:
#
#     tmux new -s polybot
#     MODE=DRY_RUN python main.py
#     # Ctrl+B D to detach
#     # tmux attach -t polybot to reattach
#
# 5. Key environment variables (see config.py for full list):
#
#     MODE                          DRY_RUN or LIVE
#     MAX_TOTAL_EXPOSURE_USD        Total position cap (default 1500)
#     MAX_POSITION_USD_PER_OUTCOME  Per-outcome cap (default 150)
#     TARGET_LOOP_MS                Decision loop interval (default 10)
#     ORDER_TTL_MS                  Cancel stale orders after (default 2500)
#     SPREAD_PCTL_MIN               Min spread percentile to trade (default 0.75)
#     CLIP_UNIT_USD                 Base order size (default 1.10)
#     LOG_FILE                      Path to log file (empty = stdout only)
#     BINANCE_POLL_MS               Binance refresh interval (default 500)
#     BOOK_POLL_MS                  Order-book refresh interval (default 300)
#
# 6. Monitoring:
#     - Watch stdout/journalctl for structured JSON logs
#     - LOOP_METRICS events show tick p50/p95/p99, snapshot ages
#     - ROLLUP events every 60s show buy/sell counts, exposure, top positions
#     - DECISION events show every trade/skip with full reasoning
#     - API_ERROR events track connectivity issues
#
# 7. Architecture (hot-snapshot model):
#     - background_tasks.py: Async tasks that continuously fetch data
#     - http_client.py: Global HTTP/2 client with connection pooling
#     - strategy_engine.py: ALL trading logic (reads snapshots, no HTTP)
#     - wallet_tracker.py: Read-only F247 wallet observer (daemon thread)
#     - ZERO shared execution path between trading engine and wallet tracker
