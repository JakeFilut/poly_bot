#!/usr/bin/env python3
"""
main.py – F247-style Polymarket scalper main loop.

Unattended operation: no interactive input, structured JSON logs,
signal handling for graceful shutdown.

Usage:
    # DRY_RUN (default)
    MODE=DRY_RUN python main.py

    # LIVE trading
    MODE=LIVE POLYMARKET_API_KEY=... POLYMARKET_API_SECRET=... \
        POLYMARKET_PRIVATE_KEY=0x... python main.py

See RUNBOOK section at bottom for systemd example.
"""
from __future__ import annotations

import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from config import load_config
from logger import Logger
from state import StateManager
from polymarket_api import PolymarketAPI
from binance_api import BinanceAPI
from market_universe import MarketUniverse
from features import FeatureEngine
from strategy import Strategy
from execution import ExecutionEngine
from risk import RiskManager


class Bot:
    """Main bot orchestrator.  Single-threaded event loop."""

    def __init__(self):
        # -- Config --
        self.cfg = load_config()

        # -- Logger --
        self.log = Logger(
            log_file=self.cfg.LOG_FILE,
            rollup_sec=self.cfg.LOG_ROLLUP_SEC,
        )
        self.log.log_config(self.cfg.redacted_dict())

        # -- State --
        self.state = StateManager(self.cfg.STATE_DB_PATH)
        self.log.info(
            "state_loaded",
            inventory_count=len(self.state.inventory),
            open_orders=len(self.state.open_orders),
        )

        # -- APIs --
        self.pm_api = PolymarketAPI(self.cfg, self.log)
        self.binance = BinanceAPI(self.cfg, self.log)

        # -- Subsystems --
        self.universe = MarketUniverse(self.cfg, self.pm_api, self.log)
        self.features = FeatureEngine(self.state, self.pm_api, self.binance, self.universe)
        self.risk = RiskManager(self.cfg, self.state, self.log)
        self.strategy = Strategy(self.cfg, self.state, self.log)
        self.execution = ExecutionEngine(self.cfg, self.pm_api, self.state, self.log)

        # Wire features cache into execution for probabilistic book lookups
        self.execution._features = self.features

        # Self-test mode: force-fill the next N orders
        if self.cfg.DRY_RUN_SELFTEST:
            self.execution._selftest_remaining = self.cfg.DRY_RUN_SELFTEST_N
            self.log.info(
                "selftest_enabled",
                force_fill_count=self.cfg.DRY_RUN_SELFTEST_N,
            )

        # -- Shutdown flag --
        self._running = True
        self._last_state_flush = time.monotonic()

        # -- Hourly PnL tracking --
        self._et = ZoneInfo("America/New_York")
        self._current_hour_utc = self._get_current_hour_utc()
        self._hourly_unrealized_start = 0.0
        self._hourly_realized_start = 0.0

        # -- Mark-to-market cache: last valid mid per token_id --
        self._last_valid_mid: dict[str, float] = {}

        # -- Register signal handlers --
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------
    def _handle_signal(self, signum, frame):
        sig_name = signal.Signals(signum).name
        self.log.info(f"signal_received: {sig_name}, shutting down gracefully")
        self._running = False

    # ------------------------------------------------------------------
    # Startup sync
    # ------------------------------------------------------------------
    def _startup_sync(self) -> None:
        """Sync state with exchange on startup."""
        self.log.info("startup_sync_begin")

        # 1. Refresh universe
        self.universe.refresh()

        # 2. Fetch initial Binance prices
        prices = self.binance.refresh_all()
        self.log.info("binance_prices_loaded", prices=prices)

        # 3. In LIVE mode, sync open orders and positions
        if self.cfg.MODE == "LIVE":
            self._sync_live_state()

        # 4. Set initial cash estimate
        # In DRY_RUN, start with exposure headroom
        if self.cfg.MODE == "DRY_RUN":
            self.risk.update_cash(self.cfg.MAX_TOTAL_EXPOSURE_USD * 2)
        else:
            # In LIVE, we'd query the actual USDC balance
            # For now, estimate from config
            self.risk.update_cash(self.cfg.MAX_TOTAL_EXPOSURE_USD * 2)

        self.log.info("startup_sync_complete", risk=self.risk.snapshot())

    def _sync_live_state(self) -> None:
        """In LIVE mode: reconcile open orders with exchange."""
        try:
            remote_orders = self.pm_api.get_open_orders()
            remote_ids = {o.get("id", "") or o.get("orderID", "")
                          for o in remote_orders if o}

            # Remove local orders not on exchange
            stale = [oid for oid in self.state.open_orders
                     if oid not in remote_ids]
            for oid in stale:
                self.state.remove_order(oid)
                self.log.info("removed_stale_order", order_id=oid)

            self.log.info(
                "live_sync_orders",
                remote=len(remote_orders),
                local=len(self.state.open_orders),
                stale_removed=len(stale),
            )
        except Exception as e:
            self.log.error(f"live_sync_failed: {e}")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def run(self) -> None:
        """Run the main trading loop until shutdown signal."""
        self._startup_sync()

        # Initialize hourly PnL baselines after state is loaded
        self._hourly_unrealized_start = self._estimate_unrealized_conservative()
        self._hourly_realized_start = self.state.realized_pnl

        self.log.info("main_loop_started", mode=self.cfg.MODE,
                      target_loop_ms=self.cfg.TARGET_LOOP_MS)

        loop_count = 0
        target_sec = self.cfg.TARGET_LOOP_MS / 1000.0

        while self._running:
            loop_start = time.monotonic()
            loop_count += 1

            try:
                self._tick(loop_count)
                self.risk.clear_errors()
            except KeyboardInterrupt:
                self._running = False
                break
            except Exception as e:
                self.log.error(f"tick_error: {e}", loop=loop_count)
                self.risk.record_error()

            # State flush
            now_mono = time.monotonic()
            if now_mono - self._last_state_flush > self.cfg.STATE_FLUSH_SEC:
                self.state.flush()
                self._last_state_flush = now_mono

            # Rollup logging
            unreal, mark_details = self._compute_unrealized_with_marks()
            self.log.maybe_rollup(
                inventory_snapshot=self.state.inventory_snapshot(),
                unrealized_usd=unreal,
                realized_usd=self.state.realized_pnl,
                mark_details=mark_details,
            )

            # Hourly PnL check
            self._check_hourly_pnl()

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

        # 4. Sync fills (LIVE mode)
        fill_count = self.execution.sync_fills()
        if fill_count > 0:
            self.log.info("fills_synced", count=fill_count)

        # 5. Run strategy to generate actions
        actions = self.strategy.generate_actions(
            all_features=all_features,
            risk_allows_buy=self.risk.allows_buy,
            risk_allows_sell=self.risk.allows_sell,
        )

        # 6. Execute actions
        if actions:
            self.execution.execute_actions(actions)

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

        # Reset for next hour
        self._current_hour_utc = now_hour
        self._hourly_unrealized_start = unrealized_end
        self._hourly_realized_start = self.state.realized_pnl

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def _shutdown(self) -> None:
        """Graceful shutdown: cancel orders, flush state."""
        self.log.info("shutdown_begin")

        # Cancel all open orders
        cancelled = self.execution.cancel_all_open()
        self.log.info("shutdown_cancelled_orders", count=cancelled)

        # Flush state
        self.state.flush()
        self.state.close()

        # Final risk snapshot
        self.log.info("shutdown_complete", risk=self.risk.snapshot())
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
