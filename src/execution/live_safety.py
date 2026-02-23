"""Live trading safety rules — 9-rule defense layer for LIVE and LIVE_SAFE modes.

Rules:
  1. Atomic Hedge Rule: time-based kill switch for unpaired legs
     Timer starts at fill confirmation (not submission).
     Pauses if leg2 is actively filling (partial progress).
  2. Max Slippage Protection: abort hedge if expected PnL < threshold
     Checks effective price including maker/taker fees, not just raw drift.
  3. Partial Fill Handling: hedge only what filled, cancel remainder
  4. Spread Explosion Filter: disable trading when spread > 3c (5c in final 2 min)
  5. Order Book Depth Check: require 2x order size at price level
  6. Consecutive Hedge Failure Circuit Breaker: 3 failures in 10 min → 15 min pause
  7. Max Loss Per Window: stop trading if hourly PnL < -$25
  8. Last 90 Seconds Rule: no resting orders, IOC-only behavior
  9. Cancel All Before Resolution: cancel everything 30s before close
     Sets CLOSE_IMMINENT flag to prevent re-entry on same tick.

Sell guard: sells must be monotonic risk-reducing. No sell that would
increase net exposure (e.g., selling YES when already net short YES).

All rules are active in LIVE and LIVE_SAFE modes. LOG mode bypasses all checks.
Exit-only sells are always allowed; exposure-increasing sells are blocked.
"""
from __future__ import annotations

import time
from typing import Callable, Dict, List, Optional

from src.config.settings import (
    MODE,
    HEDGE_KILL_TIMEOUT_SEC,
    HEDGE_KILL_TIMEOUT_FINAL_SEC,
    HEDGE_KILL_FINAL_WINDOW_MIN,
    HEDGE_MAX_SLIPPAGE_CENTS,
    SPREAD_EXPLOSION_CENTS_NORMAL,
    SPREAD_EXPLOSION_CENTS_FINAL,
    SPREAD_EXPLOSION_PAUSE_SEC,
    DEPTH_CHECK_MULTIPLIER,
    HEDGE_FAIL_MAX_COUNT,
    HEDGE_FAIL_WINDOW_SEC,
    HEDGE_FAIL_PAUSE_SEC,
    MAX_LOSS_PER_HOUR_USD,
    LAST_SECONDS_IOC_ONLY,
    CANCEL_ALL_BEFORE_CLOSE_SEC,
)


class LiveSafety:
    """Centralized live trading safety checks.

    Wire into bot:
      - check_pre_entry(slug, book, order_qty, t_min) → gate before ANY buy
      - check_pre_hedge(intended_price, current_book) → gate before 2nd leg
      - on_hedge_fail(slug) → record hedge failure
      - on_hedge_success(slug) → record hedge success
      - tick(t_min, equity, hour_start_equity) → periodic checks
      - cancel_all_check(seconds_to_close) → Rule 9
      - is_ioc_only(seconds_to_close) → Rule 8
    """

    def __init__(self, write_jsonl_fn: Callable[[dict], None]):
        self._write_jsonl = write_jsonl_fn

        # Rule 4: Spread explosion pause per slug
        self._spread_paused_until: Dict[str, float] = {}

        # Rule 6: Hedge failure tracking
        self._hedge_fail_timestamps: List[float] = []
        self._hedge_fail_paused_until: float = 0.0

        # Rule 7: Loss stop state
        self._loss_stop_active: bool = False
        self._loss_stop_hour_key: str = ""  # reset on new hour

        # Rule 9: Track if cancel-all already fired this hour + CLOSE_IMMINENT flag
        self._cancel_all_fired_hour_key: str = ""
        self._close_imminent: bool = False
        self._close_imminent_hour_key: str = ""

        # Sell churn guard: track recent sells per slug to prevent flip-flop
        self.diag_sell_churn_blocks = 0

        # Diagnostics
        self.diag_hedge_fail_exits = 0
        self.diag_slippage_aborts = 0
        self.diag_spread_blocks = 0
        self.diag_depth_blocks = 0
        self.diag_loss_stop_blocks = 0
        self.diag_ioc_enforced = 0
        self.diag_cancel_all_fired = 0

    # ──────────────────────────────────────────────────────────────────────
    # Rule 1: Atomic Hedge Rule
    # Timer starts at leg1 FILL CONFIRMATION, not submission.
    # If leg2 is actively filling (partial progress), pause the timer.
    # ──────────────────────────────────────────────────────────────────────

    def hedge_timeout_sec(self, t_min: float) -> float:
        """Return max seconds to wait for hedge fill based on time in hour."""
        if t_min >= HEDGE_KILL_FINAL_WINDOW_MIN:
            return HEDGE_KILL_TIMEOUT_FINAL_SEC
        return HEDGE_KILL_TIMEOUT_SEC

    def should_kill_hedge(self, leg1_fill_ts: float, t_min: float,
                          leg2_partial_qty: float = 0.0,
                          leg2_requested_qty: float = 0.0) -> bool:
        """Check if hedge has exceeded its time limit.
        Args:
            leg1_fill_ts: timestamp when leg1 fill was CONFIRMED (not submitted)
            t_min: minutes into hour
            leg2_partial_qty: how much of leg2 has filled so far
            leg2_requested_qty: total leg2 size requested
        Returns True if the hedge should be killed and the filled side exited.
        If leg2 is actively filling (>25% done), DON'T kill — let it complete."""
        # If leg2 has made significant progress, don't kill it
        if leg2_requested_qty > 0 and leg2_partial_qty > 0:
            fill_pct = leg2_partial_qty / leg2_requested_qty
            if fill_pct >= 0.25:
                return False  # actively filling — let it finish
        elapsed = time.time() - leg1_fill_ts
        timeout = self.hedge_timeout_sec(t_min)
        return elapsed >= timeout

    # ──────────────────────────────────────────────────────────────────────
    # Rule 2: Max Slippage Protection (PnL-based, fee-aware)
    # Compares expected hedge PnL including fees, not just raw price drift.
    # ──────────────────────────────────────────────────────────────────────

    def check_slippage_ok(self, intended_price: float,
                          current_price: float,
                          fee_bps: float = 0.0) -> bool:
        """Check if hedge is still viable after price movement + fees.
        Args:
            intended_price: the ask price we planned to buy at for leg2
            current_price: the actual current ask for leg2
            fee_bps: estimated fee in basis points (e.g., 0.5 for maker, 2.0 for taker)
        Returns True if slippage is acceptable, False if hedge should be aborted.
        Uses effective cost (price + fee) to compute true slippage."""
        fee_adj = intended_price * fee_bps / 10000.0
        effective_intended = intended_price + fee_adj
        effective_current = current_price + current_price * fee_bps / 10000.0
        slippage_cents = abs(effective_current - effective_intended) * 100.0
        if slippage_cents > HEDGE_MAX_SLIPPAGE_CENTS:
            self.diag_slippage_aborts += 1
            return False
        return True

    # ──────────────────────────────────────────────────────────────────────
    # Rule 3: Partial Fill Handling
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def clamp_hedge_qty(filled_qty: float, intended_qty: float) -> float:
        """Return the qty to hedge: never more than what actually filled.
        Rule 3: hedge only what filled, don't try to complete size."""
        return min(filled_qty, intended_qty)

    # ──────────────────────────────────────────────────────────────────────
    # Rule 4: Spread Explosion Filter
    # ──────────────────────────────────────────────────────────────────────

    def check_spread_ok(self, slug: str, spread_cents: float,
                        t_min: float) -> bool:
        """Check if spread is within acceptable limits.
        Returns True if spread is OK, False if trading should be blocked."""
        # Check if slug is currently paused from prior explosion
        now = time.time()
        paused_until = self._spread_paused_until.get(slug, 0.0)
        if now < paused_until:
            self.diag_spread_blocks += 1
            return False

        # Determine threshold based on time
        threshold = (SPREAD_EXPLOSION_CENTS_FINAL
                     if t_min >= HEDGE_KILL_FINAL_WINDOW_MIN
                     else SPREAD_EXPLOSION_CENTS_NORMAL)

        if spread_cents > threshold:
            self._spread_paused_until[slug] = now + SPREAD_EXPLOSION_PAUSE_SEC
            self.diag_spread_blocks += 1
            self._write_jsonl({
                "event_type": "SPREAD_TOO_WIDE",
                "slug": slug,
                "spread_cents": round(spread_cents, 2),
                "threshold_cents": threshold,
                "pause_sec": SPREAD_EXPLOSION_PAUSE_SEC,
                "t_min": round(t_min, 3),
                "ts_ms": int(now * 1000),
            })
            return False
        return True

    # ──────────────────────────────────────────────────────────────────────
    # Rule 5: Order Book Depth Check
    # ──────────────────────────────────────────────────────────────────────

    def check_depth_ok(self, order_qty: float, book_depth_at_price: float,
                       slug: str = "") -> bool:
        """Check that sufficient depth exists at our price level.
        book_depth_at_price: cumulative size within 1c of best bid/ask.
        Returns True if depth is sufficient, False to skip/reduce."""
        required = order_qty * DEPTH_CHECK_MULTIPLIER
        if book_depth_at_price < required:
            self.diag_depth_blocks += 1
            return False
        return True

    # ──────────────────────────────────────────────────────────────────────
    # Rule 6: Consecutive Hedge Failure Circuit Breaker
    # ──────────────────────────────────────────────────────────────────────

    def on_hedge_fail(self, slug: str, reason: str = ""):
        """Record a hedge failure. If threshold exceeded, pause all trading."""
        now = time.time()
        self._hedge_fail_timestamps.append(now)
        self.diag_hedge_fail_exits += 1

        self._write_jsonl({
            "event_type": "HEDGE_FAIL_EXIT",
            "slug": slug,
            "reason": reason,
            "ts_ms": int(now * 1000),
        })

        # Prune old timestamps outside window
        cutoff = now - HEDGE_FAIL_WINDOW_SEC
        self._hedge_fail_timestamps = [
            ts for ts in self._hedge_fail_timestamps if ts >= cutoff
        ]

        if len(self._hedge_fail_timestamps) >= HEDGE_FAIL_MAX_COUNT:
            self._hedge_fail_paused_until = now + HEDGE_FAIL_PAUSE_SEC
            self._write_jsonl({
                "event_type": "HEDGE_FAIL_CIRCUIT_BREAKER",
                "failures_in_window": len(self._hedge_fail_timestamps),
                "window_sec": HEDGE_FAIL_WINDOW_SEC,
                "pause_sec": HEDGE_FAIL_PAUSE_SEC,
                "ts_ms": int(now * 1000),
            })
            print(f"  [CIRCUIT_BREAKER] {len(self._hedge_fail_timestamps)} hedge "
                  f"failures in {HEDGE_FAIL_WINDOW_SEC:.0f}s — "
                  f"pausing entries for {HEDGE_FAIL_PAUSE_SEC:.0f}s")

    def is_hedge_breaker_active(self) -> bool:
        """Check if hedge failure circuit breaker is pausing entries."""
        if self._hedge_fail_paused_until <= 0:
            return False
        if time.time() >= self._hedge_fail_paused_until:
            self._hedge_fail_paused_until = 0.0
            self._write_jsonl({
                "event_type": "HEDGE_FAIL_CIRCUIT_BREAKER_EXPIRED",
                "ts_ms": int(time.time() * 1000),
            })
            return False
        return True

    # ──────────────────────────────────────────────────────────────────────
    # Rule 7: Max Loss Per Window
    # ──────────────────────────────────────────────────────────────────────

    def check_loss_limit(self, equity: float, hour_start_equity: float,
                         hour_key: str, hour_pnl: float | None = None) -> bool:
        """Check if hourly loss limit has been hit.
        Returns True if trading is allowed, False if loss limit exceeded.

        If hour_pnl is provided (realized + unrealized P&L for the hour),
        use that directly so the check matches the displayed Hour P&L.
        Otherwise fall back to equity - hour_start_equity."""
        # Reset on new hour
        if hour_key != self._loss_stop_hour_key:
            self._loss_stop_active = False
            self._loss_stop_hour_key = hour_key

        if self._loss_stop_active:
            self.diag_loss_stop_blocks += 1
            return False

        # Use hour_pnl if provided; otherwise fall back to equity delta
        if hour_pnl is not None:
            loss = -hour_pnl  # hour_pnl is negative when losing
        else:
            if hour_start_equity <= 0:
                return True
            loss = hour_start_equity - equity

        if loss >= MAX_LOSS_PER_HOUR_USD:
            self._loss_stop_active = True
            self.diag_loss_stop_blocks += 1
            self._write_jsonl({
                "event_type": "MAX_LOSS_STOP",
                "loss_usd": round(loss, 2),
                "threshold_usd": MAX_LOSS_PER_HOUR_USD,
                "equity": round(equity, 2),
                "hour_start_equity": round(hour_start_equity, 2),
                "hour_pnl": round(hour_pnl, 2) if hour_pnl is not None else None,
                "hour_key": hour_key,
                "ts_ms": int(time.time() * 1000),
            })
            print(f"  [MAX_LOSS_STOP] Loss ${loss:.2f} >= "
                  f"${MAX_LOSS_PER_HOUR_USD:.2f} — entries disabled for this hour")
            return False
        return True

    # ──────────────────────────────────────────────────────────────────────
    # Rule 8: Last 90 Seconds Rule
    # ──────────────────────────────────────────────────────────────────────

    def is_ioc_only(self, seconds_to_close: float) -> bool:
        """Returns True if only IOC-style (immediate) orders are allowed.
        No resting limit orders in the last 90 seconds."""
        if seconds_to_close <= LAST_SECONDS_IOC_ONLY:
            self.diag_ioc_enforced += 1
            return True
        return False

    def should_block_new_entry(self, seconds_to_close: float) -> bool:
        """In the last 90 seconds, block new entries unless both legs
        are immediately marketable. Callers should check this BEFORE placing."""
        return seconds_to_close <= LAST_SECONDS_IOC_ONLY

    # ──────────────────────────────────────────────────────────────────────
    # Rule 9: Cancel All Before Resolution
    # ──────────────────────────────────────────────────────────────────────

    def should_cancel_all(self, seconds_to_close: float,
                          hour_key: str) -> bool:
        """Returns True if all open orders should be cancelled (30s before close).
        Only fires once per hour."""
        if seconds_to_close > CANCEL_ALL_BEFORE_CLOSE_SEC:
            return False
        if hour_key == self._cancel_all_fired_hour_key:
            return False  # already fired this hour
        self._cancel_all_fired_hour_key = hour_key
        self.diag_cancel_all_fired += 1
        return True

    # ──────────────────────────────────────────────────────────────────────
    # Sell Churn Guard: sells must be monotonic risk-reducing
    # ──────────────────────────────────────────────────────────────────────

    def check_sell_reduces_risk(self, outcome: str, sell_qty: float,
                                pos_qty: float, slug: str = "") -> bool:
        """Verify that a sell order reduces risk (does not increase exposure).
        A sell is only allowed if:
          - We actually hold a position in this outcome
          - The sell qty does not exceed our position (would create a short)
        Returns True if sell is risk-reducing, False if it would increase exposure."""
        if pos_qty < 0.5:
            # No position to sell — this would be exposure-increasing
            self.diag_sell_churn_blocks += 1
            self._write_jsonl({
                "event_type": "SELL_CHURN_BLOCKED",
                "slug": slug,
                "outcome": outcome,
                "sell_qty": round(sell_qty, 1),
                "pos_qty": round(pos_qty, 1),
                "reason": "no_position",
                "ts_ms": int(time.time() * 1000),
            })
            return False
        if sell_qty > pos_qty + 0.5:
            # Sell exceeds position — would flip to short
            self.diag_sell_churn_blocks += 1
            self._write_jsonl({
                "event_type": "SELL_CHURN_BLOCKED",
                "slug": slug,
                "outcome": outcome,
                "sell_qty": round(sell_qty, 1),
                "pos_qty": round(pos_qty, 1),
                "reason": "would_exceed_position",
                "ts_ms": int(time.time() * 1000),
            })
            return False
        return True

    # ──────────────────────────────────────────────────────────────────────
    # CLOSE_IMMINENT flag (Fix 4): prevent re-entry after cancel-all
    # ──────────────────────────────────────────────────────────────────────

    def set_close_imminent(self, hour_key: str):
        """Set the CLOSE_IMMINENT flag — prevents ANY new orders."""
        self._close_imminent = True
        self._close_imminent_hour_key = hour_key
        self._write_jsonl({
            "event_type": "CLOSE_IMMINENT_SET",
            "hour_key": hour_key,
            "ts_ms": int(time.time() * 1000),
        })

    def is_close_imminent(self, hour_key: str = "") -> bool:
        """Check if CLOSE_IMMINENT flag is set. Resets on new hour."""
        if hour_key and hour_key != self._close_imminent_hour_key:
            self._close_imminent = False
        return self._close_imminent

    # ──────────────────────────────────────────────────────────────────────
    # Composite gate: check all pre-entry rules at once
    # ──────────────────────────────────────────────────────────────────────

    def check_pre_entry(self, slug: str, spread_cents: float,
                        order_qty: float, book_depth: float,
                        t_min: float, seconds_to_close: float,
                        equity: float, hour_start_equity: float,
                        hour_key: str, hour_pnl: float | None = None) -> tuple:
        """Run all pre-entry safety checks. Returns (allowed: bool, reason: str).
        Exit-only sells are always allowed; only new entries are gated."""
        # Fix 4: CLOSE_IMMINENT — hard block after cancel-all fired
        if self.is_close_imminent(hour_key):
            return False, "CLOSE_IMMINENT"

        # Rule 4: Spread explosion
        if not self.check_spread_ok(slug, spread_cents, t_min):
            return False, "SPREAD_TOO_WIDE"

        # Rule 5: Depth check
        if not self.check_depth_ok(order_qty, book_depth, slug):
            return False, "INSUFFICIENT_DEPTH"

        # Rule 6: Hedge failure circuit breaker
        if self.is_hedge_breaker_active():
            return False, "HEDGE_FAIL_CIRCUIT_BREAKER"

        # Rule 7: Max loss per window (use hour_pnl for consistency with display)
        if not self.check_loss_limit(equity, hour_start_equity, hour_key, hour_pnl=hour_pnl):
            return False, "MAX_LOSS_STOP"

        # Rule 8: Last 90 seconds — block new entries
        if self.should_block_new_entry(seconds_to_close):
            return False, "LAST_90S_NO_ENTRY"

        return True, ""

    # ──────────────────────────────────────────────────────────────────────
    # Diagnostics
    # ──────────────────────────────────────────────────────────────────────

    def get_diag(self) -> dict:
        return {
            "hedge_fail_exits": self.diag_hedge_fail_exits,
            "slippage_aborts": self.diag_slippage_aborts,
            "spread_blocks": self.diag_spread_blocks,
            "depth_blocks": self.diag_depth_blocks,
            "loss_stop_blocks": self.diag_loss_stop_blocks,
            "ioc_enforced": self.diag_ioc_enforced,
            "cancel_all_fired": self.diag_cancel_all_fired,
            "sell_churn_blocks": self.diag_sell_churn_blocks,
            "hedge_breaker_active": self.is_hedge_breaker_active(),
            "loss_stop_active": self._loss_stop_active,
            "close_imminent": self._close_imminent,
            "spread_paused_slugs": len(self._spread_paused_until),
        }

    def reset_minute_counters(self):
        self.diag_spread_blocks = 0
        self.diag_depth_blocks = 0
        self.diag_loss_stop_blocks = 0
        self.diag_ioc_enforced = 0
