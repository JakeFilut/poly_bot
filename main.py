#!/usr/bin/env python3
"""
main.py – Orchestrator for the Polymarket scalper.

ARCHITECTURE:
  Two completely isolated modules run concurrently:

  A) StrategyEngine (strategy_engine.py)
     - Generates trade signals, places orders, manages inventory/PnL/risk
     - The ONLY module that may place orders or mutate trading state

  B) WalletTracker (wallet_tracker.py)
     - Polls the F247 wallet, logs trades to CSV/JSONL
     - Read-only observer — CANNOT place orders, modify inventory,
       or influence trade decisions

  SAFETY RULES:
    - No imports from wallet_tracker into strategy_engine
    - No imports from strategy_engine into wallet_tracker
    - Zero shared execution path
    - Only shared components: logging utilities, read-only config

Usage:
    # DRY_RUN (default)
    MODE=DRY_RUN python main.py

    # LIVE trading
    MODE=LIVE POLYMARKET_API_KEY=... POLYMARKET_API_SECRET=... \\
        POLYMARKET_PRIVATE_KEY=0x... python main.py

See RUNBOOK section at bottom for systemd example.
"""
from __future__ import annotations

import os
import random
import signal
import time

from config import load_config
from logger import Logger


class Bot:
    """Thin orchestrator: runs StrategyEngine and WalletTracker
    concurrently but with ZERO shared execution path."""

    def __init__(self):
        # -- Config (read-only, shared) --
        self.cfg = load_config()

        # -- Logger (shared for lifecycle events only) --
        self.log = Logger(
            log_file=self.cfg.LOG_FILE,
            rollup_sec=self.cfg.LOG_ROLLUP_SEC,
        )
        self.log.log_config(self.cfg.redacted_dict())

        # ── Module A: Strategy Engine (owns ALL trading) ─────────────
        # Imported here — strategy_engine.py contains the full trading
        # pipeline: universe, features, strategy, execution, risk, PnL.
        from strategy_engine import StrategyEngine
        self.engine = StrategyEngine(self.cfg, self.log)

        # ── Module B: Wallet Tracker (read-only observer) ────────────
        # Imported here — wallet_tracker.py wraps f247_copywallet_tracker.
        # It runs in a daemon thread with NO access to StrategyEngine.
        self._tracker = None
        if self.cfg.TRACK_F247_WALLET:
            from wallet_tracker import WalletTracker
            self._tracker = WalletTracker(
                log_fn=self.log.info,  # simple log function, no state leak
            )

        # -- Shutdown flag --
        self._running = True
        self._last_state_flush = time.monotonic()

        # -- Register signal handlers --
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self.log.info(
            "architecture_initialized",
            strategy_engine="strategy_engine.StrategyEngine",
            wallet_tracker="wallet_tracker.WalletTracker" if self._tracker else "disabled",
            isolation="ENFORCED — zero shared execution path",
        )

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------
    def _handle_signal(self, signum, frame):
        sig_name = signal.Signals(signum).name
        self.log.info(f"signal_received: {sig_name}, shutting down gracefully")
        self._running = False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def run(self) -> None:
        """Run strategy engine + wallet tracker concurrently."""
        # 1. Strategy engine startup sync
        self.engine.startup_sync()

        # 2. Start wallet tracker (isolated daemon thread)
        if self._tracker is not None:
            self._tracker.start()

        self.log.info("main_loop_started", mode=self.cfg.MODE,
                      target_loop_ms=self.cfg.TARGET_LOOP_MS)

        loop_count = 0
        target_sec = self.cfg.TARGET_LOOP_MS / 1000.0

        while self._running:
            loop_start = time.monotonic()
            loop_count += 1

            # ── Strategy engine tick (the ONLY trading path) ─────────
            try:
                self.engine.tick(loop_count)
                self.engine.risk.clear_errors()
            except KeyboardInterrupt:
                self._running = False
                break
            except Exception as e:
                self.log.error(f"tick_error: {e}", loop=loop_count)
                self.engine.risk.record_error()

            # State flush
            now_mono = time.monotonic()
            if now_mono - self._last_state_flush > self.cfg.STATE_FLUSH_SEC:
                self.engine.flush_state()
                self._last_state_flush = now_mono

            # Post-tick analytics (rollups, hourly PnL, diagnostics)
            self.engine.post_tick()

            # Sleep to maintain target loop rate
            elapsed = time.monotonic() - loop_start
            sleep_time = target_sec - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

        # Graceful shutdown
        self._shutdown()

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def _shutdown(self) -> None:
        """Graceful shutdown: stop tracker, then strategy engine."""
        self.log.info("shutdown_begin")

        # Stop wallet tracker FIRST (it's just an observer)
        if self._tracker is not None:
            self._tracker.stop()

        # Shutdown strategy engine (cancels orders, flushes state)
        self.engine.shutdown()

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
    bot.run()


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
#     TARGET_LOOP_MS                Main loop interval (default 500)
#     ORDER_TTL_MS                  Cancel stale orders after (default 2500)
#     SPREAD_PCTL_MIN               Min spread percentile to trade (default 0.75)
#     CLIP_UNIT_USD                 Base order size (default 1.10)
#     LOG_FILE                      Path to log file (empty = stdout only)
#
# 6. Monitoring:
#     - Watch stdout/journalctl for structured JSON logs
#     - ROLLUP events every 60s show buy/sell counts, exposure, top positions
#     - DECISION events show every trade/skip with full reasoning
#     - API_ERROR events track connectivity issues
#
# 7. Architecture:
#     - strategy_engine.py: ALL trading logic (signals, orders, inventory, PnL)
#     - wallet_tracker.py: Read-only F247 wallet observer (CSV/JSONL logs)
#     - ZERO shared execution path between the two modules
