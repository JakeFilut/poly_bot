"""Safety caps and kill-switch — execution-only guards.

Responsibilities:
  - Kill-switch: pause entries if API errors or orphan cancels exceed thresholds
  - Global caps: MAX_OPEN_ORDERS, MAX_TOTAL_USD, per-slug USD
  - Per-slug no-progress pause: submits>=40 with fills==0 in 60s -> pause 120s
  - State-drift pause: if drift checker detects mismatch -> block entries
  - Sells ALWAYS remain allowed regardless of any pause state

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
    OM_NO_PROGRESS_SUBMITS,
    OM_NO_PROGRESS_PAUSE_SEC,
)


class SafetyCaps:
    """Execution-layer safety caps and kill-switch.

    Wire into bot:
      - record_api_error()           → after any CLOB API error
      - record_slug_submit(slug)     → after order submit for a slug
      - record_slug_fill(slug)       → after fill for a slug
      - can_enter(slug, ...)         → gate before entries
      - set_drift_paused(bool)       → set by drift checker
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
        # Drift pause (set externally by StateDriftChecker)
        self._drift_paused = False
        # Per-slug no-progress tracking
        self._slug_submits: Dict[str, int] = {}   # slug -> submits in window
        self._slug_fills: Dict[str, int] = {}     # slug -> fills in window
        self._slug_paused_until: Dict[str, float] = {}  # slug -> pause_until_ts
        # Lifetime / per-minute stats
        self.total_api_errors = 0
        self.total_kill_activations = 0
        self.cap_blocks_this_min = 0
        self.no_progress_pauses_total = 0

    def record_api_error(self):
        """Record a CLOB API error."""
        self.api_errors_this_min += 1
        self.total_api_errors += 1

    def record_slug_submit(self, slug: str):
        """Record an order submit for no-progress tracking."""
        self._slug_submits[slug] = self._slug_submits.get(slug, 0) + 1

    def record_slug_fill(self, slug: str):
        """Record a fill for no-progress tracking."""
        self._slug_fills[slug] = self._slug_fills.get(slug, 0) + 1

    def update_orphan_count(self, orphan_cancels_this_min: int):
        """Sync orphan cancel count from OrphanScanner."""
        self.orphan_cancels_this_min = orphan_cancels_this_min

    def set_drift_paused(self, paused: bool):
        """Set drift-pause state (called by StateDriftChecker integration)."""
        self._drift_paused = paused

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

    def is_slug_paused(self, slug: str) -> bool:
        """Check if a slug is paused due to no-progress."""
        until = self._slug_paused_until.get(slug, 0.0)
        if until > 0 and time.time() >= until:
            self._slug_paused_until.pop(slug, None)
            self._write_jsonl({
                "event_type": "NO_PROGRESS_PAUSE_EXPIRED",
                "slug": slug,
                "ts_ms": int(time.time() * 1000),
            })
            return False
        return until > 0

    def can_enter(self, slug: str, open_order_count: int,
                  slug_usd: float, total_usd: float) -> tuple:
        """Check if a new entry is allowed. Returns (allowed: bool, reason: str).

        This ONLY blocks entries — sells are always allowed.
        """
        # Kill-switch blocks entries
        if self.is_kill_active():
            self.cap_blocks_this_min += 1
            return False, f"KILL_SWITCH({self._kill_reason})"

        # Drift pause blocks entries
        if self._drift_paused:
            self.cap_blocks_this_min += 1
            return False, "STATE_DRIFT"

        # Per-slug no-progress pause
        if self.is_slug_paused(slug):
            self.cap_blocks_this_min += 1
            return False, f"NO_PROGRESS_PAUSE({slug})"

        # Global open order cap
        if open_order_count >= OM_MAX_OPEN_ORDERS:
            self.cap_blocks_this_min += 1
            return False, f"MAX_OPEN_ORDERS({open_order_count}>={OM_MAX_OPEN_ORDERS})"

        # Total USD exposure cap
        if total_usd >= OM_MAX_TOTAL_USD:
            self.cap_blocks_this_min += 1
            return False, f"MAX_TOTAL_USD(${total_usd:.0f}>=${OM_MAX_TOTAL_USD:.0f})"

        # Per-slug USD cap
        if slug_usd >= OM_MAX_PER_SLUG_USD:
            self.cap_blocks_this_min += 1
            return False, f"MAX_PER_SLUG_USD({slug}:${slug_usd:.0f}>=${OM_MAX_PER_SLUG_USD:.0f})"

        return True, ""

    def tick(self):
        """Evaluate kill-switch thresholds and no-progress detection."""
        if not self._kill_active:
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

    def evaluate_no_progress(self):
        """Check for slugs with many submits but zero fills (call at minute boundary).

        Pauses slugs where submits >= threshold and fills == 0.
        """
        for slug, submits in self._slug_submits.items():
            fills = self._slug_fills.get(slug, 0)
            if submits >= OM_NO_PROGRESS_SUBMITS and fills == 0:
                if not self.is_slug_paused(slug):
                    self._slug_paused_until[slug] = (
                        time.time() + OM_NO_PROGRESS_PAUSE_SEC)
                    self.no_progress_pauses_total += 1
                    self._write_jsonl({
                        "event_type": "NO_PROGRESS_PAUSE",
                        "slug": slug,
                        "submits_60s": submits,
                        "fills_60s": fills,
                        "pause_sec": OM_NO_PROGRESS_PAUSE_SEC,
                        "ts_ms": int(time.time() * 1000),
                    })
                    print(f"  [NO_PROGRESS] {slug}: {submits} submits, "
                          f"0 fills -> paused {OM_NO_PROGRESS_PAUSE_SEC:.0f}s")

    def reset_minute_counters(self):
        """Reset per-minute error counters and no-progress tracking."""
        # Evaluate no-progress BEFORE resetting slug counters
        self.evaluate_no_progress()
        self.api_errors_this_min = 0
        self.orphan_cancels_this_min = 0
        self.cap_blocks_this_min = 0
        self._slug_submits.clear()
        self._slug_fills.clear()
