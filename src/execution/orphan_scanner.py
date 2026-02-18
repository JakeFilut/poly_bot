"""Orphan order scanner — detects and cancels stale CLOB orders.

Responsibilities:
  - Every OM_ORPHAN_SCAN_INTERVAL_SEC: query CLOB open orders
  - If an order is not in our tracked set -> cancel it (orphan)
  - On startup in LIVE mode: cancel ALL open orders, then re-fetch
    to confirm 0 remain (retries up to OM_STARTUP_VERIFY_RETRIES)
  - Log: ORPHAN_CANCEL, STARTUP_ORPHAN_CANCEL, STARTUP_VERIFY_*

Does NOT alter any trading behaviour.
"""
from __future__ import annotations

import time
import logging
from typing import Callable, List, Set

from src.config.settings import (
    OM_ORPHAN_SCAN_INTERVAL_SEC,
    OM_STARTUP_VERIFY_RETRIES,
)

logger = logging.getLogger(__name__)


def _extract_oid(order: dict) -> str:
    """Extract order ID from a CLOB order dict (handles multiple key names)."""
    return (order.get("id") or order.get("orderID")
            or order.get("order_id", ""))


class OrphanScanner:
    """Periodic CLOB orphan order detection and cleanup.

    Wire into bot:
      - startup_cleanup()            → call once on LIVE startup
      - tick(tracked_order_ids)      → call every main loop iteration
    """

    def __init__(self,
                 get_open_orders_fn: Callable[[], List[dict]],
                 cancel_order_fn: Callable[[str], None],
                 write_jsonl_fn: Callable[[dict], None]):
        self._get_open_orders = get_open_orders_fn
        self._cancel_order = cancel_order_fn
        self._write_jsonl = write_jsonl_fn
        self._last_scan_ts = 0.0
        # Counters
        self.orphan_cancels_this_min = 0
        self.total_orphan_cancels = 0
        self.startup_cancels = 0
        self.startup_verified = False

    def startup_cleanup(self):
        """Cancel all open CLOB orders on startup, then verify 0 remain.

        Call once during bot.run() initialization when MODE == "LIVE".
        Retries verification up to OM_STARTUP_VERIFY_RETRIES times.
        """
        # Phase 1: cancel everything
        try:
            open_orders = self._get_open_orders()
        except Exception as e:
            self._write_jsonl({
                "event_type": "STARTUP_CLEANUP_ERROR",
                "err": str(e)[:200],
                "ts_ms": int(time.time() * 1000),
            })
            return

        for order in open_orders:
            oid = _extract_oid(order)
            if not oid:
                continue
            try:
                self._cancel_order(oid)
                self.startup_cancels += 1
                self._write_jsonl({
                    "event_type": "STARTUP_ORPHAN_CANCEL",
                    "order_id": oid,
                    "status": order.get("status", ""),
                    "ts_ms": int(time.time() * 1000),
                })
            except Exception as e:
                self._write_jsonl({
                    "event_type": "STARTUP_CANCEL_ERROR",
                    "order_id": oid,
                    "err": str(e)[:120],
                    "ts_ms": int(time.time() * 1000),
                })

        if self.startup_cancels > 0:
            logger.info("STARTUP: cancelled %d orphan orders", self.startup_cancels)
            print(f"  [STARTUP] Cancelled {self.startup_cancels} stale orders")

        # Phase 2: verify 0 orders remain
        for attempt in range(1, OM_STARTUP_VERIFY_RETRIES + 1):
            time.sleep(1.0)  # brief delay for cancels to propagate
            try:
                remaining = self._get_open_orders()
            except Exception as e:
                self._write_jsonl({
                    "event_type": "STARTUP_VERIFY_ERROR",
                    "attempt": attempt,
                    "err": str(e)[:200],
                    "ts_ms": int(time.time() * 1000),
                })
                continue

            if not remaining:
                self.startup_verified = True
                self._write_jsonl({
                    "event_type": "STARTUP_VERIFY_OK",
                    "attempt": attempt,
                    "cancelled": self.startup_cancels,
                    "ts_ms": int(time.time() * 1000),
                })
                print(f"  [STARTUP] Verified 0 open orders (attempt {attempt})")
                return

            # Still have orders — cancel stragglers
            for order in remaining:
                oid = _extract_oid(order)
                if oid:
                    try:
                        self._cancel_order(oid)
                        self.startup_cancels += 1
                    except Exception:
                        pass

            self._write_jsonl({
                "event_type": "STARTUP_VERIFY_RETRY",
                "attempt": attempt,
                "remaining": len(remaining),
                "ts_ms": int(time.time() * 1000),
            })

        # Exhausted retries
        self._write_jsonl({
            "event_type": "STARTUP_VERIFY_FAILED",
            "attempts": OM_STARTUP_VERIFY_RETRIES,
            "ts_ms": int(time.time() * 1000),
        })
        print(f"  [STARTUP] WARNING: could not verify 0 orders after "
              f"{OM_STARTUP_VERIFY_RETRIES} retries")

    def tick(self, tracked_order_ids: Set[str]):
        """Periodic scan: compare CLOB open orders vs tracked set, cancel orphans.

        Args:
            tracked_order_ids: set of order IDs currently tracked by LiveOrderTracker
        """
        now = time.time()
        if now - self._last_scan_ts < OM_ORPHAN_SCAN_INTERVAL_SEC:
            return
        self._last_scan_ts = now

        try:
            open_orders = self._get_open_orders()
        except Exception as e:
            self._write_jsonl({
                "event_type": "ORPHAN_SCAN_ERROR",
                "err": str(e)[:200],
                "ts_ms": int(now * 1000),
            })
            return

        for order in open_orders:
            oid = _extract_oid(order)
            if not oid:
                continue
            if oid not in tracked_order_ids:
                try:
                    self._cancel_order(oid)
                    self.orphan_cancels_this_min += 1
                    self.total_orphan_cancels += 1
                    self._write_jsonl({
                        "event_type": "ORPHAN_CANCEL",
                        "order_id": oid,
                        "status": order.get("status", ""),
                        "ts_ms": int(now * 1000),
                    })
                except Exception as e:
                    self._write_jsonl({
                        "event_type": "ORPHAN_CANCEL_ERROR",
                        "order_id": oid,
                        "err": str(e)[:120],
                        "ts_ms": int(now * 1000),
                    })

    def reset_minute_counters(self):
        self.orphan_cancels_this_min = 0
