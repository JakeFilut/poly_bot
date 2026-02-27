"""
risk.py – Risk management: exposure caps, cash reserve, error cooldowns.

Provides gate functions for strategy to call before placing orders.
"""
from __future__ import annotations

import time
from typing import Tuple

from config import Config
from logger import Logger
from state import StateManager


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
    def allows_buy(self, slug: str, outcome: str,
                   desired_usd: float) -> Tuple[bool, str]:
        """Check if a BUY is allowed.  Returns (ok, reason)."""
        now = time.time()

        # Cooldown check
        if now < self._cooldown_until:
            remaining = self._cooldown_until - now
            return False, f"error_cooldown({remaining:.1f}s)"

        # Total exposure cap
        total_exp = self.state.total_exposure_usd()
        if total_exp + desired_usd > self.cfg.MAX_TOTAL_EXPOSURE_USD:
            self.log.risk(
                check="total_exposure", total=total_exp,
                desired=desired_usd, cap=self.cfg.MAX_TOTAL_EXPOSURE_USD,
            )
            return False, f"total_exposure({total_exp:.2f}+{desired_usd:.2f}>{self.cfg.MAX_TOTAL_EXPOSURE_USD})"

        # Per-outcome exposure cap
        outcome_exp = self.state.outcome_exposure_usd(slug, outcome)
        if outcome_exp + desired_usd > self.cfg.MAX_POSITION_USD_PER_OUTCOME:
            return False, f"outcome_exposure({outcome_exp:.2f}+{desired_usd:.2f}>{self.cfg.MAX_POSITION_USD_PER_OUTCOME})"

        # Cash reserve
        if self.cash_usd - desired_usd < self.cfg.MIN_CASH_USD:
            return False, f"cash_reserve({self.cash_usd:.2f}-{desired_usd:.2f}<{self.cfg.MIN_CASH_USD})"

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
            event="error_cooldown_triggered",
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
        return {
            "cash_usd": round(self.cash_usd, 2),
            "total_exposure_usd": round(self.state.total_exposure_usd(), 2),
            "open_orders": len(self.state.open_orders),
            "inventory_positions": len(self.state.inventory),
            "consecutive_errors": self._consecutive_errors,
            "in_cooldown": time.time() < self._cooldown_until,
        }
