"""Order lifecycle tracker — TTL enforcement, cancel retry, partial fill accounting.

Responsibilities (items 2, 3, 7 from spec):
  - Track every LIVE order from submit to fill/cancel
  - Enforce TTL: cancel unfilled orders after OM_MAKER_ORDER_TTL_MS
  - Retry cancels up to OM_CANCEL_MAX_RETRIES
  - Handle partial fills: update position by delta only, keep order tracked
  - Increment tx_count only after confirmed submit, fill_count only after confirmed fill
  - Order Lifecycle Guard: poll pending orders until filled/partially filled/canceled
  - PENDING_CONFIRMATION state: reserve capital for matched-but-unconfirmed orders
  - Multi-source fill confirmation: order status -> fills endpoint
  - Duplicate fill prevention via deterministic fill_key
  - Aggressive initial polling (0.5-1s) with exponential backoff (2s, 4s, 8s, max 30s)

Does NOT alter any trading signals, thresholds, or paper-mode behaviour.
"""
from __future__ import annotations

import json
import os
import time
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from src.config.settings import (
    OM_MAKER_ORDER_TTL_MS,
    OM_CANCEL_MAX_RETRIES,
    OM_CANCEL_RETRY_DELAY_MS,
)

logger = logging.getLogger(__name__)

# ── Lifecycle watcher schedule ────────────────────────────────────
# Phase 1: aggressive polling (first 15 seconds after order placement)
FAST_POLL_INTERVAL_SEC = 0.5
FAST_POLL_DURATION_SEC = 15.0
# Phase 2: exponential backoff (after fast phase)
BACKOFF_INITIAL_SEC = 2.0
BACKOFF_MAX_SEC = 30.0
# Max age before terminal timeout (rely on reconciliation after this)
LIFECYCLE_MAX_AGE_SEC = 300.0   # 5 minutes

# Terminal CLOB statuses
_TERMINAL_FILLED = frozenset({"matched", "filled"})
_TERMINAL_CANCELED = frozenset({"cancelled", "canceled", "expired", "rejected"})
_TERMINAL_ALL = _TERMINAL_FILLED | _TERMINAL_CANCELED


class TrackedOrder:
    """State for a single in-flight order."""
    __slots__ = (
        "order_id", "slug", "outcome", "side", "price", "token_id",
        "requested_qty", "filled_qty", "submit_ts",
        "reason", "cancel_pending", "cancel_attempts",
        "pending_confirmation", "last_poll_ts", "poll_backoff_sec",
        "confirmed_terminal", "clob_status",
    )

    def __init__(self, order_id: str, slug: str, outcome: str,
                 side: str, price: float, qty: float, reason: str,
                 token_id: str = "", pending_confirmation: bool = False):
        self.order_id = order_id
        self.slug = slug
        self.outcome = outcome
        self.side = side
        self.price = price
        self.token_id = token_id
        self.requested_qty = qty
        self.filled_qty = 0.0
        self.submit_ts = time.time()
        self.reason = reason
        self.cancel_pending = False
        self.cancel_attempts = 0
        # PENDING_CONFIRMATION: CLOB said "matched" but fill qty unknown
        self.pending_confirmation = pending_confirmation
        # Per-order adaptive polling
        self.last_poll_ts: float = 0.0
        self.poll_backoff_sec: float = FAST_POLL_INTERVAL_SEC
        # Terminal state tracking
        self.confirmed_terminal: bool = False
        self.clob_status: str = ""

    @property
    def remaining_qty(self) -> float:
        return max(0.0, self.requested_qty - self.filled_qty)

    @property
    def is_fully_filled(self) -> bool:
        return self.remaining_qty < 1.0  # < 1 share = dust

    @property
    def age_sec(self) -> float:
        return time.time() - self.submit_ts

    @property
    def age_ms(self) -> float:
        return self.age_sec * 1000.0

    @property
    def in_fast_phase(self) -> bool:
        """True if order is still in aggressive-polling phase."""
        return self.age_sec < FAST_POLL_DURATION_SEC

    def next_poll_due(self) -> bool:
        """Check if this order is due for its next poll."""
        now = time.time()
        return (now - self.last_poll_ts) >= self.poll_backoff_sec

    def advance_backoff(self) -> None:
        """Advance to next backoff interval after a poll."""
        self.last_poll_ts = time.time()
        if self.in_fast_phase:
            self.poll_backoff_sec = FAST_POLL_INTERVAL_SEC
        else:
            # Exponential backoff: 2s -> 4s -> 8s -> ... -> 30s
            self.poll_backoff_sec = min(
                self.poll_backoff_sec * 2,
                BACKOFF_MAX_SEC,
            )
            if self.poll_backoff_sec < BACKOFF_INITIAL_SEC:
                self.poll_backoff_sec = BACKOFF_INITIAL_SEC


# ══════════════════════════════════════════════════════════════════
#  FILL DEDUP SET — in-memory + hourly on-disk persistence
# ══════════════════════════════════════════════════════════════════

class FillDedup:
    """Deterministic fill deduplication.

    fill_key = f"{order_id}:{token_id}:{fill_price}:{fill_qty}:{fill_ts}"

    Maintains an in-memory set + hourly on-disk persistence.
    """

    def __init__(self, persist_dir: str = "./logs"):
        self._seen: Set[str] = set()
        self._persist_dir = persist_dir
        self._last_persist_ts: float = 0.0
        self._persist_interval: float = 3600.0  # hourly

    @staticmethod
    def make_key(order_id: str, token_id: str = "",
                 fill_price: float = 0.0, fill_qty: float = 0.0,
                 fill_ts: float = 0.0) -> str:
        """Build deterministic fill key."""
        return (f"{order_id}:{token_id[-12:] if token_id else ''}:"
                f"{fill_price:.4f}:{fill_qty:.2f}:{int(fill_ts)}")

    def is_duplicate(self, key: str) -> bool:
        return key in self._seen

    def record(self, key: str) -> bool:
        """Record a fill key.  Returns True if new, False if duplicate."""
        if key in self._seen:
            return False
        self._seen.add(key)
        return True

    def maybe_persist(self) -> None:
        """Persist to disk if interval elapsed."""
        now = time.time()
        if now - self._last_persist_ts < self._persist_interval:
            return
        self._last_persist_ts = now
        try:
            os.makedirs(self._persist_dir, exist_ok=True)
            path = os.path.join(self._persist_dir, "fill_dedup.json")
            with open(path, "w") as f:
                json.dump(sorted(self._seen), f)
        except Exception:
            pass

    def load_from_disk(self) -> int:
        """Load persisted fill keys on startup."""
        path = os.path.join(self._persist_dir, "fill_dedup.json")
        if not os.path.exists(path):
            return 0
        try:
            with open(path, "r") as f:
                keys = json.load(f)
            if isinstance(keys, list):
                self._seen.update(keys)
                return len(keys)
        except Exception:
            pass
        return 0

    def clear(self) -> None:
        """Clear all seen keys (e.g. on hour rotation)."""
        self._seen.clear()

    @property
    def size(self) -> int:
        return len(self._seen)


# ══════════════════════════════════════════════════════════════════
#  LiveOrderTracker
# ══════════════════════════════════════════════════════════════════

class LiveOrderTracker:
    """Tracks all LIVE orders and enforces TTL / cancel-retry / partial fills.

    Lifecycle watcher runs every tick — no 10s global interval.
    Each order has its own adaptive poll schedule:
      - Phase 1 (0-15s): poll every 0.5s (aggressive)
      - Phase 2 (15s+):  exponential backoff 2s -> 4s -> 8s -> ... -> 30s max
      - Terminal at 5 min: give up, rely on reconciliation

    Multi-source confirmation priority:
      B) Order status endpoint (check size_matched / filled_size fields)
      C) Fills endpoint filtered by order_id

    Wire into bot:
      - on_submit(order_id, info)  → after confirmed API submit
      - on_fill(order_id, fill_qty) → after confirmed fill
      - tick()                      → call every main-loop iteration
      - poll_pending_orders()       → lifecycle guard (called from tick)
    """

    def __init__(self, cancel_fn: Callable[[str], None],
                 write_jsonl_fn: Callable[[dict], None],
                 get_order_status_fn: Optional[Callable[[str], Optional[dict]]] = None,
                 get_order_fills_fn: Optional[Callable[[str], list]] = None):
        self._orders: Dict[str, TrackedOrder] = {}
        self._cancel_fn = cancel_fn
        self._write_jsonl = write_jsonl_fn
        self._get_order_status = get_order_status_fn
        self._get_order_fills = get_order_fills_fn
        # Fill deduplication
        self._fill_dedup = FillDedup()
        self._fill_dedup.load_from_disk()
        # Counters — only incremented on confirmed events
        self.confirmed_submits = 0
        self.confirmed_fills = 0
        self.ttl_cancels = 0
        self.cancel_retries = 0
        self.partial_fill_count = 0
        self.lifecycle_fills_recovered = 0
        # No more global poll interval — each order polls independently

    # ── Public API ──────────────────────────────────────────────────────

    def on_submit(self, order_id: str, slug: str, outcome: str,
                  side: str, price: float, qty: float, reason: str,
                  token_id: str = "",
                  pending_confirmation: bool = False):
        """Record a confirmed order submission.

        If pending_confirmation=True, the order is marked as
        PENDING_CONFIRMATION (CLOB said matched but qty unknown).
        Capital should be reserved as if fully filled.
        """
        if not order_id or order_id.startswith("paper_"):
            return  # paper mode — skip
        self._orders[order_id] = TrackedOrder(
            order_id, slug, outcome, side, price, qty, reason,
            token_id=token_id,
            pending_confirmation=pending_confirmation,
        )
        self.confirmed_submits += 1
        if pending_confirmation:
            self._write_jsonl({
                "event_type": "ORDER_PENDING_CONFIRMATION",
                "order_id": order_id,
                "slug": slug, "outcome": outcome,
                "side": side, "qty": qty, "price": price,
                "note": "CLOB matched but size unknown — capital reserved",
                "ts_ms": int(time.time() * 1000),
            })

    def on_fill(self, order_id: str, fill_qty: float,
                fill_price: float = 0.0, token_id: str = "") -> float:
        """Record a confirmed fill (possibly partial). Returns the delta qty applied.

        - If fill_qty < requested_qty: order stays tracked (partial fill)
        - If fill_qty >= remaining: order removed (fully filled)
        - Checks fill_key dedup to prevent double-counting.
        """
        tracked = self._orders.get(order_id)

        # Dedup check
        _tid = token_id or (tracked.token_id if tracked else "")
        _price = fill_price or (tracked.price if tracked else 0.0)
        fill_key = FillDedup.make_key(order_id, _tid, _price, fill_qty)
        if not self._fill_dedup.record(fill_key):
            self._write_jsonl({
                "event_type": "FILL_DEDUP_SKIP",
                "order_id": order_id,
                "fill_key": fill_key,
                "fill_qty": fill_qty,
                "ts_ms": int(time.time() * 1000),
            })
            return 0.0

        if tracked is None:
            # Unknown order — still count the fill for accuracy
            self.confirmed_fills += 1
            return fill_qty

        delta = min(fill_qty, tracked.remaining_qty)
        tracked.filled_qty += delta
        self.confirmed_fills += 1

        if delta < tracked.requested_qty and delta > 0:
            self.partial_fill_count += 1

        # Clear pending_confirmation once we have confirmed qty
        if tracked.pending_confirmation and tracked.filled_qty > 0:
            tracked.pending_confirmation = False

        if tracked.is_fully_filled:
            tracked.confirmed_terminal = True
            self._orders.pop(order_id, None)
        return delta

    def on_cancel(self, order_id: str):
        """Record a confirmed cancellation."""
        self._orders.pop(order_id, None)

    def is_tracked(self, order_id: str) -> bool:
        return order_id in self._orders

    def is_pending_confirmation(self, order_id: str) -> bool:
        """Check if order is in PENDING_CONFIRMATION state."""
        tracked = self._orders.get(order_id)
        return tracked is not None and tracked.pending_confirmation

    def get_pending_confirmation_orders(self) -> List[TrackedOrder]:
        """Return all orders in PENDING_CONFIRMATION state."""
        return [o for o in self._orders.values() if o.pending_confirmation]

    def get_reserved_capital(self) -> float:
        """Return total capital reserved by PENDING_CONFIRMATION orders."""
        return sum(o.requested_qty * o.price
                   for o in self._orders.values()
                   if o.pending_confirmation)

    def open_order_count(self) -> int:
        return len(self._orders)

    def get_tracked_orders(self) -> Dict[str, TrackedOrder]:
        return dict(self._orders)

    def get_tracked_order_ids(self) -> set:
        return set(self._orders.keys())

    @property
    def fill_dedup(self) -> FillDedup:
        return self._fill_dedup

    # ── TTL enforcement (call every tick) ───────────────────────────────

    def tick(self):
        """Enforce TTL on all tracked orders. Call from main loop."""
        expired = []
        for oid, order in self._orders.items():
            if order.cancel_pending:
                continue  # already trying to cancel
            if order.pending_confirmation:
                continue  # don't cancel orders pending confirmation
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

        # Persist fill dedup hourly
        self._fill_dedup.maybe_persist()

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

    # ── Multi-source fill qty extraction ──────────────────────────────

    @staticmethod
    def _extract_fill_qty(resp: dict) -> float:
        """Extract fill quantity from CLOB order response."""
        for key in ("size_matched", "sizeMatched", "filled_size",
                     "filledSize", "matchedSize", "executedQty"):
            val = resp.get(key)
            if val:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue
        return 0.0

    def _confirm_via_fills_endpoint(self, order_id: str) -> float:
        """Confirmation source C: query fills endpoint for this order.
        Returns total filled qty across all fills for this order."""
        if self._get_order_fills is None:
            return 0.0
        try:
            fills = self._get_order_fills(order_id)
            if not fills or not isinstance(fills, list):
                return 0.0
            total = 0.0
            for f in fills:
                qty = f.get("size") or f.get("amount") or f.get("qty") or 0
                total += float(qty)
            return total
        except Exception:
            return 0.0

    # ── Order Lifecycle Guard ─────────────────────────────────────────

    def poll_pending_orders(self) -> List[dict]:
        """Poll tracked orders using per-order adaptive scheduling.

        Each order has its own poll timer:
          - Phase 1 (0-15s): every 0.5s
          - Phase 2 (15s+):  exponential backoff 2s -> 4s -> 8s -> ... -> 30s
          - Terminal at 5min: timeout + cleanup

        Checks multiple confirmation sources:
          B) Order status endpoint (size_matched / filled_size)
          C) Fills endpoint filtered by order_id

        Returns list of newly discovered fills: [{order_id, slug, outcome,
        side, fill_qty, fill_price}, ...]

        This is called every tick from the main loop — no global interval.
        Individual orders pace themselves via advance_backoff().
        """
        if self._get_order_status is None:
            return []

        if not self._orders:
            return []

        now = time.time()
        recovered_fills: List[dict] = []
        orders_to_remove: List[str] = []

        for oid, order in list(self._orders.items()):
            # Skip orders already fully filled
            if order.is_fully_filled:
                orders_to_remove.append(oid)
                continue

            # Terminal timeout
            if order.age_sec > LIFECYCLE_MAX_AGE_SEC:
                if order.pending_confirmation:
                    self._write_jsonl({
                        "event_type": "PENDING_CONFIRMATION_TIMEOUT",
                        "order_id": oid,
                        "slug": order.slug,
                        "outcome": order.outcome,
                        "side": order.side,
                        "age_sec": round(order.age_sec, 1),
                        "filled_qty": order.filled_qty,
                        "note": "PENDING_CONFIRMATION order timed out — "
                                "reconciliation must resolve",
                        "ts_ms": int(now * 1000),
                    })
                orders_to_remove.append(oid)
                continue

            # Per-order adaptive poll timer
            if not order.next_poll_due():
                continue

            try:
                # ── Source B: Order status endpoint ──
                status_resp = self._get_order_status(oid)
                if not status_resp or not isinstance(status_resp, dict):
                    order.advance_backoff()
                    continue

                clob_status = (status_resp.get("status", "")
                               or status_resp.get("order_status", "")).lower()
                fill_qty = self._extract_fill_qty(status_resp)

                if clob_status in _TERMINAL_FILLED:
                    # ── Source C fallback: fills endpoint ──
                    if fill_qty <= 0:
                        fill_qty = self._confirm_via_fills_endpoint(oid)
                    # Last resort: if CLOB says matched and we still have
                    # no qty, use requested_qty (best effort)
                    if fill_qty <= 0:
                        fill_qty = order.requested_qty
                        self._write_jsonl({
                            "event_type": "FILL_QTY_FROM_REQUEST",
                            "order_id": oid,
                            "slug": order.slug,
                            "fill_qty": fill_qty,
                            "note": "All confirmation sources exhausted; "
                                    "using requested_qty",
                            "ts_ms": int(now * 1000),
                        })

                    delta = fill_qty - order.filled_qty
                    if delta > 0:
                        # Dedup check
                        fill_price = float(
                            status_resp.get("price", order.price))
                        fill_key = FillDedup.make_key(
                            oid, order.token_id, fill_price, delta)
                        if self._fill_dedup.is_duplicate(fill_key):
                            order.advance_backoff()
                            continue
                        self._fill_dedup.record(fill_key)

                        order.filled_qty += delta
                        self.confirmed_fills += 1
                        self.lifecycle_fills_recovered += 1
                        order.pending_confirmation = False
                        order.confirmed_terminal = True
                        order.clob_status = clob_status
                        recovered_fills.append({
                            "order_id": oid,
                            "slug": order.slug,
                            "outcome": order.outcome,
                            "side": order.side,
                            "fill_qty": delta,
                            "fill_price": fill_price,
                            "reason": order.reason,
                            "token_id": order.token_id,
                        })
                        self._write_jsonl({
                            "event_type": "ORDER_LIFECYCLE_FILL_RECOVERED",
                            "order_id": oid,
                            "slug": order.slug,
                            "outcome": order.outcome,
                            "side": order.side,
                            "fill_qty": delta,
                            "fill_price": fill_price,
                            "age_sec": round(order.age_sec, 1),
                            "was_pending_confirmation": True,
                            "ts_ms": int(now * 1000),
                        })
                        print(f"  [ORDER_LIFECYCLE] FILL RECOVERED: "
                              f"{order.slug} {order.outcome} {order.side} "
                              f"qty={delta:.2f} @ {fill_price:.4f} "
                              f"(order_id={oid[:16]}...)")
                    if order.is_fully_filled:
                        orders_to_remove.append(oid)

                elif clob_status in _TERMINAL_CANCELED:
                    # Check for partial fill on canceled order
                    if fill_qty > 0:
                        delta = fill_qty - order.filled_qty
                        if delta > 0:
                            fill_price = float(
                                status_resp.get("price", order.price))
                            fill_key = FillDedup.make_key(
                                oid, order.token_id, fill_price, delta)
                            if not self._fill_dedup.is_duplicate(fill_key):
                                self._fill_dedup.record(fill_key)
                                order.filled_qty += delta
                                recovered_fills.append({
                                    "order_id": oid,
                                    "slug": order.slug,
                                    "outcome": order.outcome,
                                    "side": order.side,
                                    "fill_qty": delta,
                                    "fill_price": fill_price,
                                    "reason": order.reason,
                                    "partial": True,
                                    "token_id": order.token_id,
                                })
                    order.pending_confirmation = False
                    order.confirmed_terminal = True
                    order.clob_status = clob_status
                    orders_to_remove.append(oid)
                    self._write_jsonl({
                        "event_type": "ORDER_LIFECYCLE_CANCELED",
                        "order_id": oid,
                        "slug": order.slug,
                        "outcome": order.outcome,
                        "clob_status": clob_status,
                        "partial_filled": order.filled_qty,
                        "was_pending_confirmation":
                            order.pending_confirmation,
                        "ts_ms": int(now * 1000),
                    })

                else:
                    # Non-terminal (e.g. "live", "open") — keep watching
                    pass

                order.advance_backoff()

            except Exception as e:
                order.advance_backoff()
                self._write_jsonl({
                    "event_type": "ORDER_LIFECYCLE_POLL_ERROR",
                    "order_id": oid,
                    "err": str(e)[:120],
                    "ts_ms": int(now * 1000),
                })

        for oid in orders_to_remove:
            self._orders.pop(oid, None)

        return recovered_fills

    def reset_minute_counters(self):
        """Reset per-minute counters (call from minute reset)."""
        self.ttl_cancels = 0
        self.cancel_retries = 0
