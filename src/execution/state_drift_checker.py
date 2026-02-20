"""State drift checker — compares API state vs internal state.

Responsibilities:
  - Every OM_DRIFT_CHECK_INTERVAL_SEC: compare CLOB open orders and
    positions against internal tracking. If mismatch detected: pause
    entries and reconcile/flatten.
  - Log: STATE_DRIFT_DETECTED, STATE_DRIFT_RESOLVED

Does NOT alter any trading logic — only pauses entries on mismatch.
"""
from __future__ import annotations

import time
from typing import Callable, Dict, List, Optional, Set

from src.config.settings import OM_DRIFT_CHECK_INTERVAL_SEC


class StateDriftChecker:
    """Periodic comparison of API state vs internal tracking.

    Wire into bot:
      - tick(internal_orders, internal_positions, token_map) → every main loop
      - is_drifted()  → check before entries
      - clear_drift() → after reconciliation
    """

    def __init__(self,
                 get_open_orders_fn: Callable[[], List[dict]],
                 get_balances_fn: Callable[[], List[dict]],
                 write_jsonl_fn: Callable[[dict], None]):
        self._get_open_orders = get_open_orders_fn
        self._get_balances = get_balances_fn
        self._write_jsonl = write_jsonl_fn
        self._last_check_ts = 0.0
        self._drifted = False
        self._drift_reason = ""
        self._drift_details: dict = {}
        # Counters
        self.drift_events_total = 0
        self.drift_events_this_min = 0

    def is_drifted(self) -> bool:
        """True if a state mismatch has been detected and not yet resolved."""
        return self._drifted

    def drift_reason(self) -> str:
        return self._drift_reason

    def clear_drift(self):
        """Mark drift as resolved (call after reconciliation/flattening)."""
        if self._drifted:
            self._drifted = False
            self._write_jsonl({
                "event_type": "STATE_DRIFT_RESOLVED",
                "previous_reason": self._drift_reason,
                "ts_ms": int(time.time() * 1000),
            })
            self._drift_reason = ""
            self._drift_details = {}

    def tick(self, tracked_order_ids: Set[str],
             internal_positions: Dict[str, Dict[str, float]],
             token_to_slug_outcome: Dict[str, tuple]):
        """Periodic state comparison.

        Args:
            tracked_order_ids: set of order IDs tracked by LiveOrderTracker
            internal_positions: {slug: {"Up": qty, "Down": qty}} from bot state
            token_to_slug_outcome: {token_id: (slug, outcome)} for balance mapping
        """
        now = time.time()
        if now - self._last_check_ts < OM_DRIFT_CHECK_INTERVAL_SEC:
            return
        self._last_check_ts = now

        if self._drifted:
            return  # already in drift state, waiting for resolution

        drift_reasons = []

        # ── 1. Order drift: CLOB orders not in tracker (or vice versa) ──
        try:
            api_orders = self._get_open_orders()
        except Exception as e:
            self._write_jsonl({
                "event_type": "DRIFT_CHECK_ORDER_ERROR",
                "err": str(e)[:200],
                "ts_ms": int(now * 1000),
            })
            return

        api_order_ids = set()
        for order in api_orders:
            oid = (order.get("id") or order.get("orderID")
                   or order.get("order_id", ""))
            if oid:
                api_order_ids.add(oid)

        # Orders on CLOB but not tracked internally
        untracked = api_order_ids - tracked_order_ids
        # Orders tracked internally but gone from CLOB (stale tracking)
        ghost = tracked_order_ids - api_order_ids

        if untracked:
            drift_reasons.append(
                f"untracked_orders({len(untracked)}): "
                f"{list(untracked)[:3]}")
        if ghost and len(ghost) > 3:
            # A few ghosts are normal (just-filled orders), only flag many
            drift_reasons.append(
                f"ghost_tracked({len(ghost)}): "
                f"{list(ghost)[:3]}")

        # ── 2. Position drift: API balances vs internal quantities ──
        try:
            api_balances = self._get_balances()
        except Exception:
            api_balances = []  # non-fatal — skip position check

        if api_balances and token_to_slug_outcome:
            api_positions: Dict[str, Dict[str, float]] = {}
            for bal in api_balances:
                tid = bal.get("token_id") or bal.get("asset_id", "")
                amount = float(bal.get("balance", 0) or 0)
                if tid in token_to_slug_outcome and amount > 0.0:
                    slug, outcome = token_to_slug_outcome[tid]
                    api_positions.setdefault(slug, {})[outcome] = amount

            for slug in set(list(internal_positions.keys()) +
                            list(api_positions.keys())):
                for outcome in ["Up", "Down"]:
                    internal_qty = internal_positions.get(
                        slug, {}).get(outcome, 0.0)
                    api_qty = api_positions.get(
                        slug, {}).get(outcome, 0.0)
                    diff = abs(internal_qty - api_qty)
                    # Flag if difference > 1 share (tighter tolerance)
                    if diff > 1.0:
                        drift_reasons.append(
                            f"pos_mismatch({slug}/{outcome}: "
                            f"internal={internal_qty:.6f} "
                            f"api={api_qty:.6f} "
                            f"diff={diff:.6f})")

        if drift_reasons:
            self._drifted = True
            self._drift_reason = "; ".join(drift_reasons)
            self._drift_details = {
                "untracked_orders": list(untracked)[:10],
                "ghost_orders": list(ghost)[:10],
            }
            self.drift_events_total += 1
            self.drift_events_this_min += 1
            self._write_jsonl({
                "event_type": "STATE_DRIFT_DETECTED",
                "reasons": drift_reasons,
                "untracked_count": len(untracked),
                "ghost_count": len(ghost),
                "ts_ms": int(now * 1000),
            })
            print(f"  [STATE_DRIFT] DETECTED: {self._drift_reason}")

    def get_untracked_order_ids(self) -> list:
        """Return untracked CLOB order IDs from last drift detection."""
        return self._drift_details.get("untracked_orders", [])

    def reset_minute_counters(self):
        self.drift_events_this_min = 0
