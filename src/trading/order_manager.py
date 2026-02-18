from __future__ import annotations
from typing import Dict, Optional, List


class OrderManager:
    """Wraps all submit/cancel/fill tracking so main loop doesn't care about order mechanics."""

    def __init__(self):
        # Active orders: order_id -> {slug, outcome, side, price, qty, submit_ts, reason}
        self.active_orders: Dict[str, dict] = {}
        # Diagnostics
        self.submit_count = 0
        self.cancel_count = 0
        self.fill_count = 0
        self.replace_count = 0

    def track_submit(self, order_id: str, info: dict):
        """Record a new order submission."""
        self.active_orders[order_id] = info
        self.submit_count += 1

    def track_fill(self, order_id: str):
        """Record an order fill."""
        self.active_orders.pop(order_id, None)
        self.fill_count += 1

    def track_cancel(self, order_id: str):
        """Record an order cancellation."""
        self.active_orders.pop(order_id, None)
        self.cancel_count += 1

    def track_replace(self, old_order_id: str, new_order_id: str, info: dict):
        """Record a cancel+replace."""
        self.active_orders.pop(old_order_id, None)
        self.active_orders[new_order_id] = info
        self.replace_count += 1

    def get_active(self, slug: str = None) -> Dict[str, dict]:
        """Get active orders, optionally filtered by slug."""
        if slug is None:
            return dict(self.active_orders)
        return {k: v for k, v in self.active_orders.items() if v.get("slug") == slug}

    def reset_minute_counters(self):
        """Reset per-minute diagnostic counters."""
        self.submit_count = 0
        self.cancel_count = 0
        self.fill_count = 0
        self.replace_count = 0
