"""Order lifecycle tracker — TTL enforcement, cancel retry, partial fill accounting.

Responsibilities (items 2, 3, 7 from spec):
  - Track every LIVE order from submit to fill/cancel
  - Enforce TTL: cancel unfilled orders after OM_MAKER_ORDER_TTL_MS
  - Retry cancels up to OM_CANCEL_MAX_RETRIES
  - Handle partial fills: update position by delta only, keep order tracked
  - Increment tx_count only after confirmed submit, fill_count only after confirmed fill

Does NOT alter any trading signals, thresholds, or paper-mode behaviour.
"""
from __future__ import annotations

import time
import logging
from typing import Dict, Optional, Callable

from src.config.settings import (
    OM_MAKER_ORDER_TTL_MS,
    OM_CANCEL_MAX_RETRIES,
    OM_CANCEL_RETRY_DELAY_MS,
)

logger = logging.getLogger(__name__)


class TrackedOrder:
    """State for a single in-flight order."""
    __slots__ = (
        "order_id", "slug", "outcome", "side", "price",
        "requested_qty", "filled_qty", "submit_ts",
        "reason", "cancel_pending", "cancel_attempts",
    )

    def __init__(self, order_id: str, slug: str, outcome: str,
                 side: str, price: float, qty: float, reason: str):
        self.order_id = order_id
        self.slug = slug
        self.outcome = outcome
        self.side = side
        self.price = price
        self.requested_qty = qty
        self.filled_qty = 0.0
        self.submit_ts = time.time()
        self.reason = reason
        self.cancel_pending = False
        self.cancel_attempts = 0

    @property
    def remaining_qty(self) -> float:
        return max(0.0, self.requested_qty - self.filled_qty)

    @property
    def is_fully_filled(self) -> bool:
        return self.remaining_qty < 1.0  # < 1 share = dust

    @property
    def age_ms(self) -> float:
        return (time.time() - self.submit_ts) * 1000.0


class LiveOrderTracker:
    """Tracks all LIVE orders and enforces TTL / cancel-retry / partial fills.

    Wire into bot:
      - on_submit(order_id, info)  → after confirmed API submit
      - on_fill(order_id, fill_qty) → after confirmed fill
      - tick()                      → call every main-loop iteration
    """

    def __init__(self, cancel_fn: Callable[[str], None],
                 write_jsonl_fn: Callable[[dict], None]):
        self._orders: Dict[str, TrackedOrder] = {}
        self._cancel_fn = cancel_fn
        self._write_jsonl = write_jsonl_fn
        # Counters — only incremented on confirmed events
        self.confirmed_submits = 0
        self.confirmed_fills = 0
        self.ttl_cancels = 0
        self.cancel_retries = 0
        self.partial_fill_count = 0

    # ── Public API ──────────────────────────────────────────────────────

    def on_submit(self, order_id: str, slug: str, outcome: str,
                  side: str, price: float, qty: float, reason: str):
        """Record a confirmed order submission."""
        if not order_id or order_id.startswith("paper_"):
            return  # paper mode — skip
        self._orders[order_id] = TrackedOrder(
            order_id, slug, outcome, side, price, qty, reason,
        )
        self.confirmed_submits += 1

    def on_fill(self, order_id: str, fill_qty: float) -> float:
        """Record a confirmed fill (possibly partial). Returns the delta qty applied.

        - If fill_qty < requested_qty: order stays tracked (partial fill)
        - If fill_qty >= remaining: order removed (fully filled)
        """
        tracked = self._orders.get(order_id)
        if tracked is None:
            # Unknown order — still count the fill for accuracy
            self.confirmed_fills += 1
            return fill_qty

        delta = min(fill_qty, tracked.remaining_qty)
        tracked.filled_qty += delta
        self.confirmed_fills += 1

        if delta < tracked.requested_qty and delta > 0:
            self.partial_fill_count += 1

        if tracked.is_fully_filled:
            self._orders.pop(order_id, None)
        return delta

    def on_cancel(self, order_id: str):
        """Record a confirmed cancellation."""
        self._orders.pop(order_id, None)

    def is_tracked(self, order_id: str) -> bool:
        return order_id in self._orders

    def open_order_count(self) -> int:
        return len(self._orders)

    def get_tracked_orders(self) -> Dict[str, TrackedOrder]:
        return dict(self._orders)

    def get_tracked_order_ids(self) -> set:
        return set(self._orders.keys())

    # ── TTL enforcement (call every tick) ───────────────────────────────

    def tick(self):
        """Enforce TTL on all tracked orders. Call from main loop."""
        expired = []
        for oid, order in self._orders.items():
            if order.cancel_pending:
                continue  # already trying to cancel
            if order.age_ms >= OM_MAKER_ORDER_TTL_MS and not order.is_fully_filled:
                expired.append(oid)

        for oid in expired:
            self._try_cancel(oid, reason="TTL_EXPIRED")

        # Retry any pending cancels
        pending = [oid for oid, o in self._orders.items() if o.cancel_pending]
        for oid in pending:
            order = self._orders.get(oid)
            if order and order.cancel_attempts < OM_CANCEL_MAX_RETRIES:
                self._try_cancel(oid, reason="CANCEL_RETRY")

    # ── Internal ────────────────────────────────────────────────────────

    def _try_cancel(self, order_id: str, reason: str):
        order = self._orders.get(order_id)
        if order is None:
            return
        order.cancel_pending = True
        order.cancel_attempts += 1
        try:
            self._cancel_fn(order_id)
            self._write_jsonl({
                "event_type": "OM_TTL_CANCEL",
                "order_id": order_id,
                "slug": order.slug,
                "outcome": order.outcome,
                "side": order.side,
                "age_ms": round(order.age_ms, 1),
                "filled_qty": order.filled_qty,
                "remaining_qty": order.remaining_qty,
                "reason": reason,
                "attempt": order.cancel_attempts,
                "ts_ms": int(time.time() * 1000),
            })
            # Successful cancel — remove
            self._orders.pop(order_id, None)
            self.ttl_cancels += 1
        except Exception as e:
            self.cancel_retries += 1
            if order.cancel_attempts >= OM_CANCEL_MAX_RETRIES:
                self._write_jsonl({
                    "event_type": "OM_CANCEL_FAILED",
                    "order_id": order_id,
                    "slug": order.slug,
                    "attempts": order.cancel_attempts,
                    "err": str(e)[:120],
                    "ts_ms": int(time.time() * 1000),
                })
                # Leave cancel_pending=True — reconciliation will pick it up
            # else: will retry next tick due to cancel_pending flag

    def reset_minute_counters(self):
        """Reset per-minute counters (call from minute reset)."""
        self.ttl_cancels = 0
        self.cancel_retries = 0
