"""
strategy_engine.py – Isolated strategy + execution engine.

ARCHITECTURE BOUNDARY: This module owns ALL trade execution, inventory
management, PnL tracking, and risk logic.  It is the ONLY module that
may place orders or mutate trading state.

SAFETY RULE: This module MUST NOT import wallet_tracker or
f247_copywallet_tracker.  The wallet tracker is a read-only observer
that runs in a separate thread with no shared execution path.
"""
from __future__ import annotations

# ── Structural safety assertion ──────────────────────────────────────
# This module MUST NOT contain any import of wallet_tracker or
# f247_copywallet_tracker.  The two modules share the same process
# (main.py orchestrates both) but must have ZERO import dependencies
# between them.  This is enforced by:
#   1. This file never importing those modules (structural)
#   2. The runtime check below verifying this file's own globals
import sys as _sys


def _verify_no_tracker_in_own_namespace() -> None:
    """Runtime guardrail: verify strategy_engine's own module namespace
    does not reference wallet tracker modules.  Called once at init."""
    own_mod = _sys.modules.get(__name__)
    if own_mod is None:
        return
    for attr_name in dir(own_mod):
        obj = getattr(own_mod, attr_name, None)
        if hasattr(obj, "__module__") and obj.__module__ in (
            "wallet_tracker", "f247_copywallet_tracker",
        ):
            raise RuntimeError(
                f"SAFETY VIOLATION: strategy_engine.{attr_name} references "
                f"module '{obj.__module__}'.  The wallet tracker must not "
                f"share an execution path with the trading engine."
            )


# ── Standard imports ─────────────────────────────────────────────────
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from analytics import AnalyticsTracker
from config import Config
from diagnostics import Diagnostics
from logger import Logger
from state import StateManager
from polymarket_api import PolymarketAPI
from binance_api import BinanceAPI
from market_universe import MarketUniverse
from features import FeatureEngine
from strategy import Strategy
from execution import ExecutionEngine
from risk import LiveRiskGuard, RiskManager


class StrategyEngine:
    """Owns the complete trading pipeline: universe → features → strategy
    → execution → risk → PnL.

    This is the ONLY object that may:
      - Place or cancel orders (via ExecutionEngine)
      - Mutate inventory (via StateManager)
      - Track realized / unrealized PnL

    It does NOT:
      - Poll external wallets
      - Log third-party trade activity
      - Import or reference wallet_tracker in any way
    """

    def __init__(self, cfg: Config, log: Logger):
        # Runtime safety check: ensure no tracker leakage
        _verify_no_tracker_in_own_namespace()

        self.cfg = cfg
        self.log = log

        # -- State (trading database — exclusive to strategy engine) --
        self.state = StateManager(cfg.STATE_DB_PATH)
        log.info(
            "strategy_engine_state_loaded",
            inventory_count=len(self.state.inventory),
            open_orders=len(self.state.open_orders),
        )

        # -- APIs (order placement + market data) --
        self.pm_api = PolymarketAPI(cfg, log)
        self.binance = BinanceAPI(cfg, log)

        # -- Subsystems --
        self.universe = MarketUniverse(cfg, self.pm_api, log)
        self.features = FeatureEngine(
            self.state, self.pm_api, self.binance, self.universe,
            book_max_stale_ms=cfg.BOOK_MAX_STALE_MS,
        )
        self.risk = RiskManager(cfg, self.state, log)
        self.strategy = Strategy(cfg, self.state, log)
        self.execution = ExecutionEngine(cfg, self.pm_api, self.state, log)

        # -- Analytics --
        self.analytics = AnalyticsTracker()

        # -- Diagnostics --
        self.diagnostics = Diagnostics(self.state)

        # -- LIVE risk guard --
        self.live_risk: LiveRiskGuard | None = None
        if cfg.MODE == "LIVE":
            self.live_risk = LiveRiskGuard(cfg, self.state, log)
            log.info(
                "live_risk_guard_initialized",
                max_capital=cfg.LIVE_MAX_CAPITAL_USD,
                max_daily_loss=cfg.MAX_DAILY_LOSS_USD,
                max_dd=cfg.MAX_INTRADAY_DRAWDOWN_USD,
                max_cross_per_hr=cfg.MAX_CROSS_NOTIONAL_PER_HOUR_USD,
                pause_minutes=cfg.PAUSE_MINUTES_ON_DD,
            )

        # Wire cross-references within the strategy engine
        self.execution._features = self.features
        self.execution.analytics = self.analytics
        self.execution.live_risk = self.live_risk
        self.execution.diagnostics = self.diagnostics
        self.execution._risk_checker = self.risk

        # Self-test mode
        if cfg.DRY_RUN_SELFTEST:
            self.execution._selftest_remaining = cfg.DRY_RUN_SELFTEST_N
            log.info("selftest_enabled", force_fill_count=cfg.DRY_RUN_SELFTEST_N)

        # -- Hourly PnL tracking --
        self._et = ZoneInfo("America/New_York")
        self._current_hour_utc = self._get_current_hour_utc()
        self._hourly_unrealized_start = 0.0
        self._hourly_realized_start = 0.0

        # -- Mark-to-market cache --
        self._last_valid_mid: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------
    def startup_sync(self) -> None:
        """Sync state with exchange on startup.

        NOTE: Universe refresh and Binance price fetch are now handled
        by the async warmup in background_tasks.py.  This method only
        does the parts that must be synchronous (LIVE reconciliation,
        cash estimate, hourly baselines).
        """
        self.log.info("strategy_engine_startup_sync_begin")

        # 1. Universe + Binance: populated by async warmup before this runs

        # 2. LIVE mode: reconcile open orders
        if self.cfg.MODE == "LIVE":
            self._sync_live_state()

        # 3. Cash estimate
        self.risk.update_cash(self.cfg.MAX_TOTAL_EXPOSURE_USD * 2)

        # 4. Initialize hourly PnL baselines
        self._hourly_unrealized_start = self._estimate_unrealized_conservative()
        self._hourly_realized_start = self.state.realized_pnl

        self.log.info("strategy_engine_startup_sync_complete",
                      risk=self.risk.snapshot())

    def _sync_live_state(self) -> None:
        """In LIVE mode: reconcile open orders with exchange."""
        try:
            remote_orders = self.pm_api.get_open_orders()
            remote_ids = {o.get("id", "") or o.get("orderID", "")
                          for o in remote_orders if o}
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
    # Tick (one iteration of the trading loop)
    # ------------------------------------------------------------------
    def tick(self, loop_count: int) -> None:
        """One iteration of the strategy + execution loop.

        NO NETWORK I/O happens here.  Background tasks keep the caches
        warm (order-books, Binance prices, universe, fills).  This tick
        only reads in-memory snapshots, runs the strategy, and executes.
        """
        # 1. Universe refresh + Binance refresh + fill sync:
        #    ALL handled by background_tasks.py — nothing to do here.

        # 2. Compute features for all active slugs (CPU only — reads cache)
        all_features = self.features.compute_all()

        # 3. LIVE risk guard (CPU only)
        if self.live_risk is not None:
            unreal_for_risk = self._estimate_unrealized()
            self.live_risk.update_equity(
                realized_pnl=self.state.realized_pnl,
                unrealized_pnl=unreal_for_risk,
            )
            if not self.live_risk.trading_allowed:
                if self.state.open_orders:
                    cancelled = self.execution.cancel_all_open()
                    if cancelled > 0:
                        self.log.info(
                            "risk_halt_cancelled_orders", count=cancelled,
                            halted=self.live_risk.is_halted,
                            paused=self.live_risk.is_paused,
                        )
                return

        # 4. Strategy → actions (CPU only)
        actions = self.strategy.generate_actions(
            all_features=all_features,
            risk_allows_buy=self.risk.allows_buy,
            risk_allows_sell=self.risk.allows_sell,
        )

        # 5. Execute (DRY_RUN: instant mock; LIVE: HTTP via py-clob-client)
        if actions:
            from strategy import cadence_weight, _seconds_from_quarter
            now_utc = datetime.now(timezone.utc)
            bw, sw = cadence_weight(self.cfg, now_utc)
            sec_q = _seconds_from_quarter(now_utc)
            self.execution._cadence_info = {
                'sec': sec_q, 'buy_w': bw, 'sell_w': sw,
            }
            self.execution.execute_actions(actions)

        # 6. Cash estimate (CPU only)
        if self.cfg.MODE == "DRY_RUN":
            exposure = self.state.total_exposure_usd()
            self.risk.update_cash(
                self.cfg.MAX_TOTAL_EXPOSURE_USD * 2 - exposure
            )

    # ------------------------------------------------------------------
    # Post-tick analytics (called by main loop after tick)
    # ------------------------------------------------------------------
    def post_tick(self) -> None:
        """Rollup logging, analytics, hourly PnL — called after tick()."""
        # Rollup logging
        unreal, mark_details = self._compute_unrealized_with_marks()
        self.log.maybe_rollup(
            inventory_snapshot=self.state.inventory_snapshot(),
            unrealized_usd=unreal,
            realized_usd=self.state.realized_pnl,
            mark_details=mark_details,
        )

        # Equity tracking
        equity = self.state.realized_pnl + unreal
        self.analytics.update_equity(equity)

        # MINUTE_ROLLUP
        inv_snap = self.state.inventory_snapshot()
        inv_notional = sum(v.get("usd_value", 0) for v in inv_snap.values())
        sorted_inv = sorted(
            inv_snap.items(),
            key=lambda kv: kv[1].get("usd_value", 0),
            reverse=True,
        )[:5]
        top_pos = [{"position": k, **v} for k, v in sorted_inv]
        minute_payload = self.analytics.maybe_minute_rollup(
            inventory_notional=inv_notional,
            top_positions=top_pos,
        )
        if minute_payload:
            self.log.log("MINUTE_ROLLUP", **minute_payload)
            self.analytics.cleanup_stale()

        # Hourly PnL check
        self._check_hourly_pnl()

        # Diagnostics
        self.diagnostics.maybe_flush_reports()

    # ------------------------------------------------------------------
    # Mark-to-market helpers
    # ------------------------------------------------------------------
    def _get_mark_for_token(self, token_id: str) -> tuple[float, str]:
        """Return (mark_price, mark_source) for a token."""
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
        cached_mid = self._last_valid_mid.get(token_id)
        if cached_mid is not None:
            return cached_mid, "last_mid_cache"
        return 0.0, "missing_zero"

    def _compute_unrealized_with_marks(self) -> tuple[float, list[dict]]:
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
        total, _ = self._compute_unrealized_with_marks()
        return total

    def _estimate_unrealized_conservative(self) -> float:
        return self._estimate_unrealized()

    # ------------------------------------------------------------------
    # Cache purging
    # ------------------------------------------------------------------
    def _purge_stale_caches(self, removed_token_ids: list[str]) -> None:
        purged = 0
        for tid in removed_token_ids:
            if tid in self.features._book_cache:
                del self.features._book_cache[tid]
                self.features._book_cache_ts.pop(tid, None)
                purged += 1
            self.state.spread_tapes.pop(tid, None)
            self._last_valid_mid.pop(tid, None)
        if purged > 0:
            self.log.info("caches_purged", token_ids=purged)

    # ------------------------------------------------------------------
    # Hourly PnL
    # ------------------------------------------------------------------
    def _get_current_hour_utc(self) -> datetime:
        now = datetime.now(timezone.utc)
        return now.replace(minute=0, second=0, microsecond=0)

    def _check_hourly_pnl(self) -> None:
        now_hour = self._get_current_hour_utc()
        if now_hour <= self._current_hour_utc:
            return

        unrealized_end, mark_details = self._compute_unrealized_with_marks()
        realized_this_hour = self.state.realized_pnl - self._hourly_realized_start
        net_pnl = realized_this_hour + (unrealized_end - self._hourly_unrealized_start)
        fill_stats = self.log.get_and_reset_hourly_fills()

        inv_snap = self.state.inventory_snapshot()
        sorted_inv = sorted(
            inv_snap.items(),
            key=lambda kv: kv[1].get("usd_value", 0),
            reverse=True,
        )[:5]
        top_positions = [{"position": k, **v} for k, v in sorted_inv]
        inv_notional = sum(v.get("usd_value", 0) for v in inv_snap.values())
        hour_et = self._current_hour_utc.astimezone(self._et)

        analytics_hourly = self.analytics.get_and_reset_hourly_analytics(
            unrealized_start=self._hourly_unrealized_start,
            unrealized_end=unrealized_end,
        )

        live_risk_snap = {}
        if self.live_risk is not None:
            live_risk_snap = self.live_risk.snapshot()

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
            **analytics_hourly,
            **live_risk_snap,
        )

        self._current_hour_utc = now_hour
        self._hourly_unrealized_start = unrealized_end
        self._hourly_realized_start = self.state.realized_pnl

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        """Cancel orders, flush state, final snapshot."""
        self.log.info("strategy_engine_shutdown_begin")
        cancelled = self.execution.cancel_all_open()
        self.log.info("shutdown_cancelled_orders", count=cancelled)
        self.diagnostics.flush_reports(final=True)
        self.state.flush()
        self.state.close()
        self.log.info("strategy_engine_shutdown_complete",
                      risk=self.risk.snapshot())

    # ------------------------------------------------------------------
    # State flush
    # ------------------------------------------------------------------
    def flush_state(self) -> None:
        self.state.flush()
