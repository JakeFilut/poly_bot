"""
risk.py – Risk management: exposure caps, cash reserve, error cooldowns.

Provides gate functions for strategy to call before placing orders.

LiveRiskGuard (LIVE-mode only):
  - Hard daily kill switch (persisted in SQLite)
  - Intraday drawdown pause
  - Capital deployment limit (global + per-asset)
  - Cross/taker discipline (hourly notional cap)
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Dict, Tuple
from zoneinfo import ZoneInfo

from config import Config
from logger import Logger
from state import StateManager

_ET = ZoneInfo("America/New_York")


class RiskManager:
    """Enforces position limits, cash reserve, and error cooldowns."""

    def __init__(self, cfg: Config, state: StateManager, logger: Logger):
        self.cfg = cfg
        self.state = state
        self.log = logger

        # Error cooldown state
        self._consecutive_errors = 0
        self._cooldown_until = 0.0

        # Cash tracking (estimated; updated from fills)
        self.cash_usd: float = 0.0

    # ------------------------------------------------------------------
    # Gate functions (called by strategy)
    # ------------------------------------------------------------------
    def _pending_buy_usd(self) -> float:
        """Sum of USD committed to open BUY orders (not yet filled)."""
        return sum(
            o.price * o.size
            for o in self.state.open_orders.values()
            if o.side == "BUY"
        )

    def allows_buy(self, slug: str, outcome: str,
                   desired_usd: float) -> Tuple[bool, str]:
        """Check if a BUY is allowed.  Returns (ok, reason).

        Exposure = inventory cost basis + pending open buy orders.
        Cash reserve accounts for pending buy orders too.
        """
        now = time.time()

        # Cooldown check
        if now < self._cooldown_until:
            remaining = self._cooldown_until - now
            return False, f"error_cooldown({remaining:.1f}s)"

        # Total exposure cap (includes pending open buy orders)
        pending_usd = self._pending_buy_usd()
        total_exp = self.state.total_exposure_usd() + pending_usd
        if total_exp + desired_usd > self.cfg.MAX_TOTAL_EXPOSURE_USD:
            self.log.risk(
                check="total_exposure", total=total_exp,
                pending_buy_usd=pending_usd,
                desired=desired_usd, cap=self.cfg.MAX_TOTAL_EXPOSURE_USD,
            )
            return False, f"total_exposure({total_exp:.2f}+{desired_usd:.2f}>{self.cfg.MAX_TOTAL_EXPOSURE_USD})"

        # Per-outcome exposure cap
        outcome_exp = self.state.outcome_exposure_usd(slug, outcome)
        if outcome_exp + desired_usd > self.cfg.MAX_POSITION_USD_PER_OUTCOME:
            return False, f"outcome_exposure({outcome_exp:.2f}+{desired_usd:.2f}>{self.cfg.MAX_POSITION_USD_PER_OUTCOME})"

        # Cash reserve (accounts for pending buy orders)
        available_cash = self.cash_usd - pending_usd
        if available_cash - desired_usd < self.cfg.MIN_CASH_USD:
            return False, f"cash_reserve({available_cash:.2f}-{desired_usd:.2f}<{self.cfg.MIN_CASH_USD})"

        return True, ""

    def allows_sell(self, slug: str, outcome: str) -> Tuple[bool, str]:
        """Check if a SELL is allowed (almost always yes if we have inventory)."""
        inv = self.state.get_inventory(slug, outcome)
        if inv is None or inv.shares <= 0:
            return False, "no_inventory"
        return True, ""

    # ------------------------------------------------------------------
    # Error cooldown
    # ------------------------------------------------------------------
    def record_error(self) -> None:
        """Record an API/execution error.  Triggers exponential cooldown."""
        self._consecutive_errors += 1
        wait = min(
            self.cfg.ERROR_COOLDOWN_BASE_SEC * (2 ** (self._consecutive_errors - 1)),
            self.cfg.ERROR_COOLDOWN_MAX_SEC,
        )
        self._cooldown_until = time.time() + wait
        self.log.risk(
            trigger="error_cooldown_triggered",
            consecutive=self._consecutive_errors,
            cooldown_sec=wait,
        )

    def clear_errors(self) -> None:
        """Reset error counter after a successful cycle."""
        if self._consecutive_errors > 0:
            self._consecutive_errors = 0
            self._cooldown_until = 0.0

    # ------------------------------------------------------------------
    # Cash management
    # ------------------------------------------------------------------
    def update_cash(self, new_cash: float) -> None:
        """Update estimated cash balance."""
        self.cash_usd = new_cash

    def estimate_cash_after_buys(self, pending_buy_usd: float) -> float:
        """Estimate cash after pending buys."""
        return self.cash_usd - pending_buy_usd

    # ------------------------------------------------------------------
    # Summary for logging
    # ------------------------------------------------------------------
    def snapshot(self) -> dict:
        pending = self._pending_buy_usd()
        return {
            "cash_usd": round(self.cash_usd, 2),
            "total_exposure_usd": round(self.state.total_exposure_usd(), 2),
            "pending_buy_usd": round(pending, 2),
            "effective_exposure_usd": round(self.state.total_exposure_usd() + pending, 2),
            "open_orders": len(self.state.open_orders),
            "inventory_positions": len(self.state.inventory),
            "consecutive_errors": self._consecutive_errors,
            "in_cooldown": time.time() < self._cooldown_until,
        }


# ═══════════════════════════════════════════════════════════════════════════
# LiveRiskGuard – LIVE-mode only hard risk controls
# ═══════════════════════════════════════════════════════════════════════════

def _et_date_str(dt_utc: datetime | None = None) -> str:
    """Return current ET date as 'YYYY-MM-DD' string."""
    if dt_utc is None:
        dt_utc = datetime.now(timezone.utc)
    return dt_utc.astimezone(_ET).strftime("%Y-%m-%d")


def _et_hour_key(dt_utc: datetime | None = None) -> str:
    """Return current ET hour as 'YYYY-MM-DD-HH' string."""
    if dt_utc is None:
        dt_utc = datetime.now(timezone.utc)
    return dt_utc.astimezone(_ET).strftime("%Y-%m-%d-%H")


def _extract_asset_from_slug(slug: str) -> str:
    """Best-effort asset extraction from slug string."""
    s = slug.lower()
    for asset in ("btc", "eth", "sol", "xrp"):
        if asset in s:
            return asset.upper()
    return "OTHER"


class LiveRiskGuard:
    """Hard risk controls for LIVE mode.  Overrides strategy decisions.

    Must be checked BEFORE every order placement.  Persists kill-switch
    state to SQLite so restarts cannot bypass the daily loss limit.
    """

    def __init__(self, cfg: Config, state: StateManager, logger: Logger):
        self.cfg = cfg
        self.state = state
        self.log = logger

        # --- Daily kill switch ---
        self._halted: bool = False
        self._halt_date: str = ""  # ET date string when halt was triggered

        # Track realized PnL baseline at start of ET day
        self._et_day: str = _et_date_str()
        self._realized_at_day_start: float = state.realized_pnl

        # --- Intraday drawdown pause ---
        self._equity_peak_today: float = 0.0
        self._equity_peak_initialized: bool = False
        self._dd_pause_until: float = 0.0  # epoch seconds

        # --- Cross discipline ---
        self._cross_notional_hour: float = 0.0
        self._cross_hour_key: str = _et_hour_key()

        # --- Restore persisted halt state ---
        self._restore_halt()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _restore_halt(self) -> None:
        """Restore daily halt from SQLite if it was set today (ET)."""
        persisted_date = self.state.get_risk_halt("halt_date")
        if persisted_date and persisted_date == _et_date_str():
            self._halted = True
            self._halt_date = persisted_date
            self.log.warn(
                "daily_kill_switch_restored_from_db",
                halt_date=persisted_date,
            )
        elif persisted_date:
            # Old halt from a previous day — clear it
            self.state.clear_risk_halt("halt_date")

        # Restore realized baseline
        saved_baseline = self.state.get_risk_halt("realized_at_day_start")
        if saved_baseline is not None:
            saved_day = self.state.get_risk_halt("baseline_day")
            if saved_day == _et_date_str():
                self._realized_at_day_start = float(saved_baseline)
            else:
                # New day — reset baseline
                self._realized_at_day_start = self.state.realized_pnl
                self._persist_baseline()

        # Restore equity peak
        saved_peak = self.state.get_risk_halt("equity_peak_today")
        saved_peak_day = self.state.get_risk_halt("peak_day")
        if saved_peak is not None and saved_peak_day == _et_date_str():
            self._equity_peak_today = float(saved_peak)
            self._equity_peak_initialized = True

        # Restore drawdown pause
        saved_pause = self.state.get_risk_halt("dd_pause_until")
        if saved_pause:
            self._dd_pause_until = float(saved_pause)

    def _persist_halt(self) -> None:
        """Persist halt state to SQLite."""
        self.state.set_risk_halt("halt_date", self._halt_date)

    def _persist_baseline(self) -> None:
        """Persist the realized PnL baseline for today."""
        self.state.set_risk_halt("realized_at_day_start",
                                 str(self._realized_at_day_start))
        self.state.set_risk_halt("baseline_day", self._et_day)

    def _persist_peak(self) -> None:
        """Persist equity peak for today."""
        self.state.set_risk_halt("equity_peak_today",
                                 str(self._equity_peak_today))
        self.state.set_risk_halt("peak_day", _et_date_str())

    def _persist_dd_pause(self) -> None:
        self.state.set_risk_halt("dd_pause_until", str(self._dd_pause_until))

    # ------------------------------------------------------------------
    # Day rollover
    # ------------------------------------------------------------------
    def _check_day_rollover(self) -> None:
        """Detect ET day boundary and reset daily state."""
        today = _et_date_str()
        if today != self._et_day:
            self.log.info(
                "live_risk_day_rollover",
                old_day=self._et_day, new_day=today,
                realized_pnl_yesterday=round(
                    self.state.realized_pnl - self._realized_at_day_start, 4),
            )
            self._et_day = today
            self._realized_at_day_start = self.state.realized_pnl
            self._persist_baseline()
            self._equity_peak_today = 0.0
            self._equity_peak_initialized = False
            self._dd_pause_until = 0.0
            self.state.clear_risk_halt("dd_pause_until")
            # Clear halt if it was from a previous day
            if self._halted and self._halt_date != today:
                self._halted = False
                self._halt_date = ""
                self.state.clear_risk_halt("halt_date")
                self.log.info("daily_kill_switch_cleared_new_day")

    # ------------------------------------------------------------------
    # Equity update (call every tick from main loop)
    # ------------------------------------------------------------------
    def update_equity(self, realized_pnl: float,
                      unrealized_pnl: float) -> None:
        """Update equity tracking.  Must be called every tick.

        Args:
            realized_pnl: cumulative realized PnL (from state.realized_pnl)
            unrealized_pnl: current mark-to-market unrealized PnL
        """
        self._check_day_rollover()

        realized_today = realized_pnl - self._realized_at_day_start
        equity_today = realized_today + unrealized_pnl

        # Track peak for drawdown
        if not self._equity_peak_initialized:
            self._equity_peak_today = equity_today
            self._equity_peak_initialized = True
            self._persist_peak()
        elif equity_today > self._equity_peak_today:
            self._equity_peak_today = equity_today
            self._persist_peak()

        # --- 2. DAILY KILL SWITCH ---
        if not self._halted and equity_today <= -self.cfg.MAX_DAILY_LOSS_USD:
            self._halted = True
            self._halt_date = _et_date_str()
            self._persist_halt()
            self.log.log(
                "DAILY_KILL_SWITCH",
                equity_today=round(equity_today, 4),
                limit=self.cfg.MAX_DAILY_LOSS_USD,
                realized_today=round(realized_today, 4),
                unrealized=round(unrealized_pnl, 4),
                halt_date=self._halt_date,
            )
            return  # caller should cancel all orders

        # --- 3. INTRADAY DRAWDOWN PAUSE ---
        drawdown = self._equity_peak_today - equity_today
        if drawdown >= self.cfg.MAX_INTRADAY_DRAWDOWN_USD:
            now = time.time()
            if now >= self._dd_pause_until:
                # Trigger new pause
                self._dd_pause_until = now + self.cfg.PAUSE_MINUTES_ON_DD * 60
                self._persist_dd_pause()
                self.log.log(
                    "DRAWDOWN_PAUSE",
                    drawdown=round(drawdown, 4),
                    equity_peak_today=round(self._equity_peak_today, 4),
                    equity_today=round(equity_today, 4),
                    pause_until_epoch=self._dd_pause_until,
                    pause_minutes=self.cfg.PAUSE_MINUTES_ON_DD,
                )

    # ------------------------------------------------------------------
    # Gate checks (called before every order placement)
    # ------------------------------------------------------------------
    @property
    def is_halted(self) -> bool:
        """True if daily kill switch is active."""
        return self._halted

    @property
    def is_paused(self) -> bool:
        """True if intraday drawdown pause is active."""
        if self._halted:
            return True
        return time.time() < self._dd_pause_until

    @property
    def trading_allowed(self) -> bool:
        """True if neither halted nor paused."""
        return not self.is_halted and not self.is_paused

    def check_order_allowed(self, side: str, slug: str, outcome: str,
                            price: float, size_shares: float,
                            best_bid: float, best_ask: float,
                            ) -> Tuple[bool, str]:
        """Master gate for LIVE order placement.

        Returns (allowed, reason).  If not allowed, the order MUST be skipped.
        """
        # Daily kill switch
        if self._halted:
            return False, "daily_kill_switch"

        # Drawdown pause
        if time.time() < self._dd_pause_until:
            remaining = self._dd_pause_until - time.time()
            return False, f"drawdown_pause({remaining:.0f}s remaining)"

        notional = price * size_shares

        if side == "BUY":
            # --- 4. CAPITAL DEPLOYMENT LIMIT ---
            deployed = self.state.total_exposure_usd()
            pending_buy = sum(
                o.price * o.size
                for o in self.state.open_orders.values()
                if o.side == "BUY"
            )
            if deployed + pending_buy + notional > self.cfg.LIVE_MAX_CAPITAL_USD:
                return False, (
                    f"capital_limit(deployed={deployed:.2f}+"
                    f"pending={pending_buy:.2f}+"
                    f"new={notional:.2f}>{self.cfg.LIVE_MAX_CAPITAL_USD})"
                )

            # Per-asset capital limit
            asset = _extract_asset_from_slug(slug)
            asset_basis = self._asset_cost_basis(asset)
            if asset_basis + notional > self.cfg.MAX_CAPITAL_PER_ASSET_USD:
                return False, (
                    f"asset_limit({asset}={asset_basis:.2f}+"
                    f"{notional:.2f}>{self.cfg.MAX_CAPITAL_PER_ASSET_USD})"
                )

        # --- 5. CROSS DISCIPLINE ---
        is_crossing = False
        if side == "BUY" and price >= best_ask:
            is_crossing = True
        elif side == "SELL" and price <= best_bid:
            is_crossing = True

        if is_crossing:
            self._maybe_reset_cross_hour()
            if self._cross_notional_hour + notional > self.cfg.MAX_CROSS_NOTIONAL_PER_HOUR_USD:
                return False, (
                    f"cross_budget_exhausted("
                    f"used={self._cross_notional_hour:.2f}+"
                    f"{notional:.2f}>{self.cfg.MAX_CROSS_NOTIONAL_PER_HOUR_USD})"
                )

        return True, ""

    def record_cross(self, notional: float) -> None:
        """Record cross/taker notional after a crossing order is placed."""
        self._maybe_reset_cross_hour()
        self._cross_notional_hour += notional

    # ------------------------------------------------------------------
    # Per-asset cost basis
    # ------------------------------------------------------------------
    def _asset_cost_basis(self, asset: str) -> float:
        """Sum cost basis across all inventory entries for a given asset."""
        total = 0.0
        for (slug, outcome), inv in self.state.inventory.items():
            if _extract_asset_from_slug(slug) == asset:
                total += inv.cost_basis_usd
        return total

    # ------------------------------------------------------------------
    # Cross hour reset
    # ------------------------------------------------------------------
    def _maybe_reset_cross_hour(self) -> None:
        """Reset cross notional counter on ET hour boundary."""
        current = _et_hour_key()
        if current != self._cross_hour_key:
            self._cross_hour_key = current
            self._cross_notional_hour = 0.0

    # ------------------------------------------------------------------
    # Snapshot for hourly rollup logging
    # ------------------------------------------------------------------
    def snapshot(self) -> dict:
        realized_today = self.state.realized_pnl - self._realized_at_day_start
        return {
            "halted": self._halted,
            "paused": self.is_paused,
            "realized_today": round(realized_today, 4),
            "equity_peak_today": round(self._equity_peak_today, 4),
            "drawdown": round(
                self._equity_peak_today - realized_today, 4)
                if self._equity_peak_initialized else 0.0,
            "deployed_usd": round(self.state.total_exposure_usd(), 2),
            "cross_notional_hour": round(self._cross_notional_hour, 2),
            "dd_pause_remaining_sec": round(
                max(0, self._dd_pause_until - time.time()), 0),
        }
