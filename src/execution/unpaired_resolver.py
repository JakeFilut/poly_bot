"""Unpaired (one-sided fill) resolution — deterministic escalation.

Responsibility (item 1 from spec):
  - When one leg fills and the other does not, ensure the position is resolved
  - Escalation runs deterministically: tick1 -> tick2 -> cross -> unwind
  - Never allows a naked position to persist without state tracking
  - Logs: UNPAIRED_START, UNPAIRED_ESCALATE, UNPAIRED_CROSS, UNPAIRED_UNWIND

Uses the EXISTING hedge timing constants from settings — does NOT change them.
Does NOT alter entry rules or hedge thresholds.
"""
from __future__ import annotations

import time
from typing import Dict, Optional, Callable

from src.config.settings import (
    HEDGE_TICK1_MS,
    HEDGE_TICK2_MS,
    HEDGE_EARLY_CROSS_MS,
    HEDGE_CROSS_MS,
    HEDGE_MAX_DURATION_MS,
)


class UnpairedPosition:
    """Tracks a single one-sided fill awaiting resolution."""
    __slots__ = (
        "slug", "filled_outcome", "filled_qty", "filled_price",
        "pending_outcome", "start_ts", "state", "escalation_ts",
    )

    # States
    STARTED = "STARTED"
    TICK1 = "TICK1"
    TICK2 = "TICK2"
    CROSSING = "CROSSING"
    UNWINDING = "UNWINDING"
    RESOLVED = "RESOLVED"

    def __init__(self, slug: str, filled_outcome: str, filled_qty: float,
                 filled_price: float, pending_outcome: str):
        self.slug = slug
        self.filled_outcome = filled_outcome
        self.filled_qty = filled_qty
        self.filled_price = filled_price
        self.pending_outcome = pending_outcome
        self.start_ts = time.time()
        self.state = self.STARTED
        self.escalation_ts = self.start_ts

    @property
    def age_ms(self) -> float:
        return (time.time() - self.start_ts) * 1000.0


class UnpairedResolver:
    """Tracks and resolves one-sided fills via deterministic escalation.

    This mirrors the existing hedge timing logic but adds explicit state
    tracking and logging. It does NOT change any hedge thresholds.

    Wire into bot:
      - on_one_sided_fill(slug, ...) → when one leg fills, other doesn't
      - on_second_fill(slug)         → when the second leg fills
      - tick()                       → call every main loop iteration
    """

    def __init__(self, write_jsonl_fn: Callable[[dict], None]):
        self._active: Dict[str, UnpairedPosition] = {}
        self._write_jsonl = write_jsonl_fn
        # Counters
        self.events_this_min = 0
        self.total_events = 0
        self.total_crosses = 0
        self.total_unwinds = 0

    def on_one_sided_fill(self, slug: str, filled_outcome: str,
                          filled_qty: float, filled_price: float,
                          pending_outcome: str):
        """Record that one leg filled and the other did not."""
        if slug in self._active:
            return  # already tracking this slug
        pos = UnpairedPosition(slug, filled_outcome, filled_qty,
                               filled_price, pending_outcome)
        self._active[slug] = pos
        self.events_this_min += 1
        self.total_events += 1
        self._write_jsonl({
            "event_type": "UNPAIRED_START",
            "slug": slug,
            "filled_outcome": filled_outcome,
            "filled_qty": filled_qty,
            "filled_price": filled_price,
            "pending_outcome": pending_outcome,
            "ts_ms": int(time.time() * 1000),
        })

    def on_second_fill(self, slug: str):
        """Record that the second leg has now filled — resolve."""
        pos = self._active.pop(slug, None)
        if pos:
            pos.state = UnpairedPosition.RESOLVED
            self._write_jsonl({
                "event_type": "UNPAIRED_RESOLVED",
                "slug": slug,
                "age_ms": round(pos.age_ms, 1),
                "final_state": pos.state,
                "ts_ms": int(time.time() * 1000),
            })

    def is_unpaired(self, slug: str) -> bool:
        return slug in self._active

    def get_state(self, slug: str) -> Optional[str]:
        pos = self._active.get(slug)
        return pos.state if pos else None

    def get_active_count(self) -> int:
        return len(self._active)

    def tick(self) -> list:
        """Check all unpaired positions and return escalation actions.

        Returns list of dicts: [{slug, action, ...}, ...]
        Actions: "ESCALATE_TICK1", "ESCALATE_TICK2", "CROSS", "UNWIND"

        The caller (bot) executes the actual trades — this module only
        determines what should happen based on timing.
        """
        actions = []
        for slug, pos in list(self._active.items()):
            age = pos.age_ms

            # Hard deadline: force unwind if unpaired > HEDGE_MAX_DURATION_MS
            if age >= HEDGE_MAX_DURATION_MS and pos.state != UnpairedPosition.UNWINDING:
                pos.state = UnpairedPosition.UNWINDING
                self.total_unwinds += 1
                actions.append({
                    "slug": slug, "action": "UNWIND",
                    "filled_outcome": pos.filled_outcome,
                    "filled_qty": pos.filled_qty,
                })
                self._write_jsonl({
                    "event_type": "UNPAIRED_UNWIND",
                    "slug": slug,
                    "age_ms": round(age, 1),
                    "reason": "HEDGE_MAX_DURATION",
                    "ts_ms": int(time.time() * 1000),
                })
                self._active.pop(slug, None)
                continue

            if pos.state == UnpairedPosition.STARTED and age >= HEDGE_TICK1_MS:
                pos.state = UnpairedPosition.TICK1
                pos.escalation_ts = time.time()
                actions.append({
                    "slug": slug, "action": "ESCALATE_TICK1",
                    "pending_outcome": pos.pending_outcome,
                    "filled_outcome": pos.filled_outcome,
                    "filled_qty": pos.filled_qty,
                })
                self._write_jsonl({
                    "event_type": "UNPAIRED_ESCALATE",
                    "slug": slug, "state": pos.state,
                    "age_ms": round(age, 1),
                    "ts_ms": int(time.time() * 1000),
                })

            elif pos.state == UnpairedPosition.TICK1 and age >= HEDGE_TICK2_MS:
                pos.state = UnpairedPosition.TICK2
                pos.escalation_ts = time.time()
                actions.append({
                    "slug": slug, "action": "ESCALATE_TICK2",
                    "pending_outcome": pos.pending_outcome,
                    "filled_outcome": pos.filled_outcome,
                    "filled_qty": pos.filled_qty,
                })
                self._write_jsonl({
                    "event_type": "UNPAIRED_ESCALATE",
                    "slug": slug, "state": pos.state,
                    "age_ms": round(age, 1),
                    "ts_ms": int(time.time() * 1000),
                })

            elif pos.state == UnpairedPosition.TICK2 and age >= HEDGE_CROSS_MS:
                pos.state = UnpairedPosition.CROSSING
                pos.escalation_ts = time.time()
                self.total_crosses += 1
                actions.append({
                    "slug": slug, "action": "CROSS",
                    "pending_outcome": pos.pending_outcome,
                    "filled_outcome": pos.filled_outcome,
                    "filled_qty": pos.filled_qty,
                })
                self._write_jsonl({
                    "event_type": "UNPAIRED_CROSS",
                    "slug": slug,
                    "age_ms": round(age, 1),
                    "ts_ms": int(time.time() * 1000),
                })

            elif pos.state == UnpairedPosition.CROSSING:
                # If cross didn't resolve within another HEDGE_CROSS_MS, unwind
                if (time.time() - pos.escalation_ts) * 1000.0 >= HEDGE_CROSS_MS:
                    pos.state = UnpairedPosition.UNWINDING
                    self.total_unwinds += 1
                    actions.append({
                        "slug": slug, "action": "UNWIND",
                        "filled_outcome": pos.filled_outcome,
                        "filled_qty": pos.filled_qty,
                    })
                    self._write_jsonl({
                        "event_type": "UNPAIRED_UNWIND",
                        "slug": slug,
                        "age_ms": round(age, 1),
                        "ts_ms": int(time.time() * 1000),
                    })
                    self._active.pop(slug, None)

        return actions

    def reset_minute_counters(self):
        self.events_this_min = 0
