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
import uuid

from config import load_config
from logger import Logger, _utc_iso


class Bot:
    """Thin orchestrator: runs StrategyEngine and WalletTracker
    concurrently but with ZERO shared execution path."""

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
        # Imported here — wallet_tracker.py wraps f247_copywallet_tracker.
        # It runs in a daemon thread with NO access to StrategyEngine.
        self._tracker = None
        if self.cfg.TRACK_F247_WALLET:
            from wallet_tracker import WalletTracker
            self._tracker = WalletTracker(
                log_fn=self.log.info,  # simple log function, no state leak
                bot_logger=self.log,   # for COPYWALLET_FILL events
                diag_callback=self.engine.diagnostics.on_copywallet_fill,
            )

        # -- Shutdown flag --
        self._running = True
        self._last_state_flush = time.monotonic()

        # -- Register signal handlers --
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self.log.info(
            "bot_startup",
            run_id=self._run_id,
            start_ts_utc=_utc_iso(),
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

    def _tick(self, loop_count: int) -> None:
        """One iteration of the main loop."""
        # 1. Refresh universe periodically
        if self.universe.needs_refresh():
            self.universe.refresh()

        # 2. Binance price update
        self.binance.refresh_all()

        # 3. Compute features for all active slugs
        all_features = self.features.compute_all()

        # 4. Sync fills (LIVE mode) – track sell PnL for entry quality
        pnl_before = self.state.realized_pnl
        fill_count = self.execution.sync_fills()
        if fill_count > 0:
            self.log.info("fills_synced", count=fill_count)
            pnl_delta = self.state.realized_pnl - pnl_before
            if pnl_delta != 0:
                self.strategy.record_sell_pnl(pnl_delta)

        # 5. Run strategy to generate actions
        actions = self.strategy.generate_actions(
            all_features=all_features,
            risk_allows_buy=self.risk.allows_buy,
            risk_allows_sell=self.risk.allows_sell,
        )

        # 6. Execute actions – track sell PnL for entry quality
        pnl_before = self.state.realized_pnl
        if actions:
            self.execution.execute_actions(actions)
        pnl_delta = self.state.realized_pnl - pnl_before
        if pnl_delta != 0:
            self.strategy.record_sell_pnl(pnl_delta)

        # 7. Update cash estimate after fills
        if self.cfg.MODE == "DRY_RUN":
            # In DRY_RUN, cash = initial - exposure
            exposure = self.state.total_exposure_usd()
            self.risk.update_cash(
                self.cfg.MAX_TOTAL_EXPOSURE_USD * 2 - exposure
            )

    # ------------------------------------------------------------------
    # Mark-to-market helpers
    # ------------------------------------------------------------------
    def _get_mark_for_token(self, token_id: str) -> tuple[float, str]:
        """Return (mark_price, mark_source) for a token.

        Hierarchy:
          1. mid_book    – both bids & asks present → (best_bid+best_ask)/2
          2. bid_only    – only bids present → best_bid
          3. ask_only    – only asks present → best_ask
          4. last_mid_cache – previous valid mid stored in _last_valid_mid
          5. missing_zero – no data at all → 0.0  (do NOT assume 0.50)
        """
        book = self.features._book_cache.get(token_id)
        if book is not None:
            has_bids = len(book.bids) > 0
            has_asks = len(book.asks) > 0
            if has_bids and has_asks:
                mid = (book.best_bid + book.best_ask) / 2
                self._last_valid_mid[token_id] = mid
                return mid, "mid_book"
            if has_bids:
                self._last_valid_mid[token_id] = book.best_bid
                return book.best_bid, "bid_only"
            if has_asks:
                return book.best_ask, "ask_only"

        # Fallback: last known valid mid
        cached_mid = self._last_valid_mid.get(token_id)
        if cached_mid is not None:
            return cached_mid, "last_mid_cache"

        # Nothing available → contribute 0
        return 0.0, "missing_zero"

    def _compute_unrealized_with_marks(self) -> tuple[float, list[dict]]:
        """Compute total unrealized PnL and per-token mark details.

        Returns (total_unrealized_usd, [mark_detail_per_position]).
        """
        fee_bps = self.cfg.SIM_FEE_BPS
        fee_rate = fee_bps / 10_000.0 if fee_bps > 0 else 0.0
        total = 0.0
        marks: list[dict] = []

        for (slug, outcome), inv in self.state.inventory.items():
            if inv.shares <= 0:
                continue
            pair = self.universe.get_pair(slug)
            if pair is None:
                continue
            token_id = pair.up_token_id if outcome == "Up" else pair.down_token_id
            mark, source = self._get_mark_for_token(token_id)
            pnl = inv.shares * (mark - inv.avg_cost)
            if fee_rate > 0 and mark > 0:
                pnl -= fee_rate * inv.shares * (mark + inv.avg_cost)
            total += pnl
            marks.append({
                "position": f"{slug}:{outcome}",
                "shares": round(inv.shares, 2),
                "avg_cost": round(inv.avg_cost, 4),
                "mark": round(mark, 4),
                "mark_source": source,
                "unrealized": round(pnl, 4),
            })

        return total, marks

    def _estimate_unrealized(self) -> float:
        """Rough unrealized P&L estimate based on actual market prices."""
        total, _ = self._compute_unrealized_with_marks()
        return total

    # ------------------------------------------------------------------
    # Conservative mark-to-market (best_bid for longs)
    # ------------------------------------------------------------------
    def _estimate_unrealized_conservative(self) -> float:
        """Mark-to-market using actual prices. Applies SIM_FEE_BPS if configured."""
        return self._estimate_unrealized()

    # ------------------------------------------------------------------
    # Hourly PnL tracking
    # ------------------------------------------------------------------
    def _get_current_hour_utc(self) -> datetime:
        """Return the current hour boundary (truncated to hour)."""
        now = datetime.now(timezone.utc)
        return now.replace(minute=0, second=0, microsecond=0)

    def _check_hourly_pnl(self) -> None:
        """Emit HOURLY_PNL event if an hour boundary has been crossed."""
        now_hour = self._get_current_hour_utc()
        if now_hour <= self._current_hour_utc:
            return

        # Hour boundary crossed — compute metrics for the completed hour
        unrealized_end, mark_details = self._compute_unrealized_with_marks()
        realized_this_hour = self.state.realized_pnl - self._hourly_realized_start
        net_pnl = realized_this_hour + (unrealized_end - self._hourly_unrealized_start)

        # Fill stats from logger
        fill_stats = self.log.get_and_reset_hourly_fills()

        # Top positions
        inv_snap = self.state.inventory_snapshot()
        sorted_inv = sorted(
            inv_snap.items(),
            key=lambda kv: kv[1].get("usd_value", 0),
            reverse=True,
        )[:5]
        top_positions = [{"position": k, **v} for k, v in sorted_inv]

        # Inventory notional at end
        inv_notional = sum(v.get("usd_value", 0) for v in inv_snap.values())

        # ET time for the completed hour
        hour_et = self._current_hour_utc.astimezone(self._et)

        self.log.hourly_pnl(
            hour_start_utc=self._current_hour_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            hour_start_et=hour_et.strftime("%Y-%m-%d %H:%M ET"),
            realized_usd=round(realized_this_hour, 4),
            unrealized_start_usd=round(self._hourly_unrealized_start, 4),
            unrealized_end_usd=round(unrealized_end, 4),
            net_pnl_usd=round(net_pnl, 4),
            inventory_notional_end_usd=round(inv_notional, 4),
            top_positions=top_positions,
            mark_details=mark_details,
            **fill_stats,
        )

        # Entry quality report
        eq = self.strategy.get_and_reset_entry_quality()
        self.log.entry_quality_report(
            hour_start_utc=self._current_hour_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            hour_start_et=hour_et.strftime("%Y-%m-%d %H:%M ET"),
            avg_buy_price=eq["avg_buy_price"],
            avg_edge_at_entry=eq["avg_edge_at_entry"],
            total_buys=eq["total_buys"],
            total_sells=eq["total_sells"],
            profit_per_sell=eq["profit_per_sell"],
            skips_by_gate=eq["skips_by_gate"],
            total_skipped=eq["total_skipped"],
        )

        # Reset for next hour
        self._current_hour_utc = now_hour
        self._hourly_unrealized_start = unrealized_end
        self._hourly_realized_start = self.state.realized_pnl

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
