"""Safety caps and kill-switch — execution-only guards.

Responsibilities (items 6, 8 from spec):
  - Kill-switch: pause entries if API errors or orphan cancels exceed thresholds
  - Global caps: MAX_OPEN_ORDERS, MAX_TOTAL_USD, per-slug USD
  - Sells ALWAYS remain allowed regardless of kill-switch state

Does NOT alter normal trading logic when within limits.
"""
from __future__ import annotations

import time
from typing import Callable, Dict

from src.config.settings import (
    OM_KILL_API_ERROR_THRESHOLD_PER_MIN,
    OM_KILL_ORPHAN_THRESHOLD_PER_MIN,
    OM_KILL_PAUSE_SEC,
    OM_MAX_OPEN_ORDERS,
    OM_MAX_TOTAL_USD,
    OM_MAX_PER_SLUG_USD,
)


class SafetyCaps:
    """Execution-layer safety caps and kill-switch.

    Wire into bot:
      - record_api_error()           → after any CLOB API error
      - can_enter(slug, open_count, slug_usd, total_usd) → gate before entries
      - tick()                       → call every main loop iteration
    """

    def __init__(self, write_jsonl_fn: Callable[[dict], None]):
        self._write_jsonl = write_jsonl_fn
        # Error counters (per-minute)
        self.api_errors_this_min = 0
        self.orphan_cancels_this_min = 0  # updated externally from orphan scanner
        # Kill-switch state
        self._kill_active = False
        self._kill_until_ts = 0.0
        self._kill_reason = ""
        # Lifetime stats
        self.total_api_errors = 0
        self.total_kill_activations = 0

    def record_api_error(self):
        """Record a CLOB API error."""
        self.api_errors_this_min += 1
        self.total_api_errors += 1

    def update_orphan_count(self, orphan_cancels_this_min: int):
        """Sync orphan cancel count from OrphanScanner."""
        self.orphan_cancels_this_min = orphan_cancels_this_min

    def is_kill_active(self) -> bool:
        """Check if kill-switch is currently pausing entries."""
        if self._kill_active and time.time() >= self._kill_until_ts:
            self._kill_active = False
            self._write_jsonl({
                "event_type": "KILL_SWITCH_EXPIRED",
                "reason": self._kill_reason,
                "ts_ms": int(time.time() * 1000),
            })
            self._kill_reason = ""
        return self._kill_active

    def can_enter(self, slug: str, open_order_count: int,
                  slug_usd: float, total_usd: float) -> tuple:
        """Check if a new entry is allowed. Returns (allowed: bool, reason: str).

        This ONLY blocks entries — sells are always allowed.
        """
        # Kill-switch blocks entries
        if self.is_kill_active():
            return False, f"KILL_SWITCH({self._kill_reason})"

        # Global open order cap
        if open_order_count >= OM_MAX_OPEN_ORDERS:
            return False, f"MAX_OPEN_ORDERS({open_order_count}>={OM_MAX_OPEN_ORDERS})"

        # Total USD exposure cap
        if total_usd >= OM_MAX_TOTAL_USD:
            return False, f"MAX_TOTAL_USD(${total_usd:.0f}>=${OM_MAX_TOTAL_USD:.0f})"

        # Per-slug USD cap
        if slug_usd >= OM_MAX_PER_SLUG_USD:
            return False, f"MAX_PER_SLUG_USD({slug}:${slug_usd:.0f}>=${OM_MAX_PER_SLUG_USD:.0f})"

        return True, ""

    def tick(self):
        """Evaluate kill-switch thresholds. Call every main loop iteration."""
        if self._kill_active:
            return  # already paused

        reason = ""
        if self.api_errors_this_min >= OM_KILL_API_ERROR_THRESHOLD_PER_MIN:
            reason = f"API_ERRORS({self.api_errors_this_min})"
        elif self.orphan_cancels_this_min >= OM_KILL_ORPHAN_THRESHOLD_PER_MIN:
            reason = f"ORPHAN_CANCELS({self.orphan_cancels_this_min})"

        if reason:
            self._kill_active = True
            self._kill_until_ts = time.time() + OM_KILL_PAUSE_SEC
            self._kill_reason = reason
            self.total_kill_activations += 1
            self._write_jsonl({
                "event_type": "KILL_SWITCH_ACTIVATED",
                "reason": reason,
                "pause_sec": OM_KILL_PAUSE_SEC,
                "api_errors": self.api_errors_this_min,
                "orphan_cancels": self.orphan_cancels_this_min,
                "ts_ms": int(time.time() * 1000),
            })

    def reset_minute_counters(self):
        """Reset per-minute error counters."""
        self.api_errors_this_min = 0
        self.orphan_cancels_this_min = 0
