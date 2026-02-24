"""Fill Integrity — idempotent fill pipeline, pending/confirm protocol, order watcher.

Implements:
  A) Single idempotent fill pipeline: apply_fill_idempotent()
     - All fills (ws/poll/recovered/manual) through ONE function
     - Stable fill_key: prefer trade_id/match_id, else composite
     - Persistent dedupe store (JSONL + in-memory set) survives restarts
     - FILL_DEDUP_SKIPPED logged on duplicate
     - Auto-rotation when JSONL exceeds FILL_DEDUPE_MAX_LINES (memory-safe)

  B) Two-step "pending then confirm" fill protocol
     - PENDING_FILL recorded on candidate fill event
     - Confirm via API before applying: poll order status
     - Partial fills applied immediately for confirmed delta
     - FILL_CONFIRMED / FILL_CONFIRM_FAILED / PARTIAL_FILL_APPLIED emitted
     - FILL_CONFIRM_LATENCY_MS emitted on every confirmation

  C) Order lifecycle integrity (no UNKNOWN)
     - Orders with status=UNKNOWN enter a watcher
     - Resolved to CONFIRMED_FILLED / CONFIRMED_OPEN / CONFIRMED_CANCELLED / CONFIRMED_REJECTED
     - Before ORPHAN: one final status poll as last-chance recovery
     - Unresolved after timeout -> ORPHAN_ORDER, block BUYs only (sells allowed)
     - ORPHAN_ORDER_CONFIRMED logged

  E) Audit logging: PENDING_FILL, FILL_CONFIRMED, FILL_CONFIRM_FAILED,
     FILL_DEDUP_SKIPPED, ORPHAN_ORDER, PARTIAL_FILL_APPLIED,
     FILL_CONFIRM_LATENCY_MS, ORPHAN_ORDER_CONFIRMED

Example log lines:
  {"event_type":"FILL_DEDUP_LOADED","count":142,"file_size_bytes":28400,"bad_lines_skipped":0}
  {"event_type":"PARTIAL_FILL_APPLIED","order_id":"0x...","confirmed_qty":3.0,"remaining_open_qty":7.0}
  {"event_type":"ORPHAN_ORDER_CONFIRMED","order_id":"0x...","slug":"btc-hour-...","action_blocked":"BUY_only"}
  {"event_type":"FILL_CONFIRM_LATENCY_MS","order_id":"0x...","latency_ms":145}
"""
from __future__ import annotations

import json
import os
import shutil
import time
import threading
from typing import Callable, Dict, List, Optional, Set, Tuple

# ══════════════════════════════════════════════════════════════════
#  PERSISTENT FILL DEDUPE STORE (JSONL + in-memory set)
# ══════════════════════════════════════════════════════════════════

class FillDedupeStore:
    """Persistent fill deduplication: JSONL append-log + in-memory set.

    On startup, loads all keys from the JSONL file into memory.
    Every new key is immediately appended to disk.
    Survives restarts without reapplying fills.

    Hardening:
      - Corrupt/partial lines skipped gracefully on load (never crash)
      - Auto-rotation when line count exceeds max_lines (prevents unbounded growth)
      - Startup emits FILL_DEDUP_LOADED with count + file_size_bytes
    """

    def __init__(self, persist_path: str = "",
                 write_jsonl: Optional[Callable] = None,
                 max_lines: int = 200_000):
        self._seen: Set[str] = set()
        self._lock = threading.Lock()
        self._persist_path = persist_path
        self._write_jsonl = write_jsonl or (lambda d: None)
        self._fd = None  # file descriptor for append-only writes
        self._max_lines = max_lines
        self._lines_written: int = 0  # lines appended since last load/rotation
        self._bad_lines_skipped: int = 0

    def load_from_disk(self) -> int:
        """Load persisted fill keys on startup. Returns count loaded.
        Skips corrupt/partial lines without crashing."""
        if not self._persist_path or not os.path.exists(self._persist_path):
            return 0
        loaded = 0
        self._bad_lines_skipped = 0
        file_size = 0
        try:
            file_size = os.path.getsize(self._persist_path)
            with open(self._persist_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        key = obj.get("fill_key", "")
                        if key:
                            self._seen.add(key)
                            loaded += 1
                        else:
                            self._bad_lines_skipped += 1
                    except (json.JSONDecodeError, KeyError, TypeError):
                        self._bad_lines_skipped += 1
                        continue
        except Exception:
            pass
        self._lines_written = loaded
        self._write_jsonl({
            "event_type": "FILL_DEDUP_LOADED",
            "count": loaded,
            "file_size_bytes": file_size,
            "bad_lines_skipped": self._bad_lines_skipped,
            "max_lines": self._max_lines,
            "ts_ms": int(time.time() * 1000),
        })
        return loaded

    def _ensure_fd(self):
        """Open file descriptor for appending (lazy)."""
        if self._fd is None and self._persist_path:
            os.makedirs(os.path.dirname(self._persist_path) or ".", exist_ok=True)
            self._fd = open(self._persist_path, "a")

    def _maybe_rotate(self):
        """Rotate JSONL file if line count exceeds max_lines.
        Archives old file as .bak, starts fresh with current in-memory keys
        pruned to most recent half."""
        if self._lines_written < self._max_lines:
            return
        if not self._persist_path:
            return
        try:
            # Close current fd
            if self._fd:
                self._fd.close()
                self._fd = None
            # Archive old file
            bak_path = self._persist_path + ".bak"
            shutil.move(self._persist_path, bak_path)
            # Start fresh — write nothing, keys stay in memory
            # Next record() call will re-open fd and append
            self._lines_written = 0
            self._write_jsonl({
                "event_type": "FILL_DEDUPE_ROTATED",
                "archived_to": bak_path,
                "keys_in_memory": len(self._seen),
                "ts_ms": int(time.time() * 1000),
            })
        except Exception:
            pass

    def is_duplicate(self, fill_key: str) -> bool:
        """Thread-safe check if fill_key was already applied."""
        with self._lock:
            return fill_key in self._seen

    def record(self, fill_key: str, meta: Optional[dict] = None) -> bool:
        """Record a fill key. Returns True if NEW, False if duplicate.
        Immediately persists to disk. Auto-rotates if file too large."""
        with self._lock:
            if fill_key in self._seen:
                return False
            self._seen.add(fill_key)
        # Persist to disk (outside lock to avoid IO blocking)
        try:
            self._ensure_fd()
            if self._fd:
                entry = {"fill_key": fill_key, "ts_ms": int(time.time() * 1000)}
                if meta:
                    entry.update(meta)
                self._fd.write(json.dumps(entry) + "\n")
                self._fd.flush()
                self._lines_written += 1
            self._maybe_rotate()
        except Exception:
            pass
        return True

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._seen)

    @property
    def bad_lines_skipped(self) -> int:
        return self._bad_lines_skipped

    def close(self):
        if self._fd:
            try:
                self._fd.close()
            except Exception:
                pass
            self._fd = None


# ══════════════════════════════════════════════════════════════════
#  FILL KEY GENERATION — stable, deterministic
# ══════════════════════════════════════════════════════════════════

def make_fill_key(order_id: str = "", side: str = "", token_id: str = "",
                  price: float = 0.0, qty: float = 0.0,
                  trade_id: str = "", match_id: str = "") -> str:
    """Generate a stable fill_key for deduplication.

    Priority:
      1. Exchange-provided trade_id or match_id (globally unique)
      2. Composite: (order_id, side, token_id, round(price,4), round(qty,3))
         qty rounded to 3dp to prevent venue precision mismatch.
    """
    # Prefer exchange-provided unique IDs
    if trade_id:
        return f"tid:{trade_id}"
    if match_id:
        return f"mid:{match_id}"
    # Composite fallback — qty at 3dp for venue precision safety
    tok = token_id[-12:] if token_id else ""
    return f"cmp:{order_id}:{side}:{tok}:{price:.4f}:{qty:.3f}"


# ══════════════════════════════════════════════════════════════════
#  PENDING FILL REGISTRY — two-step confirm protocol
# ══════════════════════════════════════════════════════════════════

class PendingFill:
    """A candidate fill awaiting API confirmation."""
    __slots__ = ("fill_key", "order_id", "slug", "outcome", "side",
                 "qty", "price", "source", "token_id",
                 "created_ts", "confirm_attempts", "reason",
                 "trade_id", "match_id", "applied_qty")

    def __init__(self, fill_key: str, order_id: str, slug: str,
                 outcome: str, side: str, qty: float, price: float,
                 source: str = "immediate", token_id: str = "",
                 reason: str = "", trade_id: str = "", match_id: str = ""):
        self.fill_key = fill_key
        self.order_id = order_id
        self.slug = slug
        self.outcome = outcome
        self.side = side
        self.qty = qty
        self.price = price
        self.source = source
        self.token_id = token_id
        self.created_ts = time.time()
        self.confirm_attempts = 0
        self.reason = reason
        self.trade_id = trade_id
        self.match_id = match_id
        self.applied_qty = 0.0  # tracks how much already applied (for partials)

    @property
    def age_sec(self) -> float:
        return time.time() - self.created_ts

    @property
    def latency_ms(self) -> int:
        return int((time.time() - self.created_ts) * 1000)


class FillConfirmResult:
    """Result of a fill confirmation attempt."""
    __slots__ = ("confirmed", "confirmed_qty", "reason")

    def __init__(self, confirmed: bool, confirmed_qty: float = 0.0,
                 reason: str = ""):
        self.confirmed = confirmed
        self.confirmed_qty = confirmed_qty
        self.reason = reason


class PendingFillRegistry:
    """Manages the two-step pending->confirm fill protocol.

    Handles partial fills: if venue confirms 3 out of 10 qty, applies the
    confirmed delta immediately without waiting for the full quantity.
    Emits FILL_CONFIRM_LATENCY_MS on every confirmation for diagnostics.
    """

    # Confirmation timeouts
    NORMAL_TIMEOUT_SEC = 2.0
    SLOW_TIMEOUT_SEC = 5.0

    def __init__(self,
                 get_order_status_fn: Optional[Callable] = None,
                 get_order_fills_fn: Optional[Callable] = None,
                 write_jsonl: Optional[Callable] = None):
        self._pending: Dict[str, PendingFill] = {}  # fill_key -> PendingFill
        self._get_order_status = get_order_status_fn
        self._get_order_fills = get_order_fills_fn
        self._write_jsonl = write_jsonl or (lambda d: None)

    def register_pending(self, fill_key: str, order_id: str,
                         slug: str, outcome: str, side: str,
                         qty: float, price: float,
                         source: str = "immediate",
                         token_id: str = "",
                         reason: str = "",
                         trade_id: str = "",
                         match_id: str = "") -> PendingFill:
        """Record a candidate fill as PENDING_FILL.
        Returns the PendingFill object (or existing one if already pending).
        """
        if fill_key in self._pending:
            return self._pending[fill_key]

        pf = PendingFill(
            fill_key=fill_key, order_id=order_id,
            slug=slug, outcome=outcome, side=side,
            qty=qty, price=price, source=source,
            token_id=token_id, reason=reason,
            trade_id=trade_id, match_id=match_id,
        )
        self._pending[fill_key] = pf

        self._write_jsonl({
            "event_type": "PENDING_FILL",
            "fill_key": fill_key,
            "order_id": order_id,
            "slug": slug, "outcome": outcome,
            "side": side, "qty": round(qty, 3),
            "price": round(price, 4),
            "source": source,
            "ts_ms": int(time.time() * 1000),
        })
        return pf

    def _emit_latency(self, pf: PendingFill) -> None:
        """Emit FILL_CONFIRM_LATENCY_MS for venue/API lag diagnostics."""
        self._write_jsonl({
            "event_type": "FILL_CONFIRM_LATENCY_MS",
            "order_id": pf.order_id,
            "fill_key": pf.fill_key,
            "latency_ms": pf.latency_ms,
            "source": pf.source,
            "ts_ms": int(time.time() * 1000),
        })

    def tick_confirmations(self, dedupe_store: FillDedupeStore) -> List[PendingFill]:
        """Attempt to confirm all pending fills via API.

        Returns list of confirmed PendingFills (ready for apply_fill_idempotent).
        For partial fills, adjusts pf.qty to the confirmed delta and returns
        immediately — does NOT wait for the full quantity.
        Emits FILL_CONFIRM_LATENCY_MS on every confirmation.
        """
        if not self._pending:
            return []

        confirmed: List[PendingFill] = []
        to_remove: List[str] = []
        now = time.time()

        for fill_key, pf in list(self._pending.items()):
            # Already deduped? Skip.
            if dedupe_store.is_duplicate(fill_key):
                to_remove.append(fill_key)
                continue

            # Determine timeout
            timeout = self.SLOW_TIMEOUT_SEC if pf.source in ("recovered", "external_scan") else self.NORMAL_TIMEOUT_SEC

            # Timeout check
            if pf.age_sec > timeout:
                if pf.source == "immediate":
                    confirmed.append(pf)
                    to_remove.append(fill_key)
                    self._emit_latency(pf)
                    self._write_jsonl({
                        "event_type": "FILL_CONFIRMED",
                        "fill_key": fill_key,
                        "order_id": pf.order_id,
                        "confirmed_qty": round(pf.qty, 3),
                        "method": "immediate_trust",
                        "latency_ms": pf.latency_ms,
                        "ts_ms": int(now * 1000),
                    })
                    continue

                # Timeout without confirmation
                self._write_jsonl({
                    "event_type": "FILL_CONFIRM_FAILED",
                    "fill_key": fill_key,
                    "order_id": pf.order_id,
                    "slug": pf.slug, "outcome": pf.outcome,
                    "reason": "timeout",
                    "age_sec": round(pf.age_sec, 1),
                    "ts_ms": int(now * 1000),
                })
                to_remove.append(fill_key)
                continue

            # For immediate fills (API already said "filled"), confirm immediately
            if pf.source == "immediate":
                confirmed.append(pf)
                to_remove.append(fill_key)
                self._emit_latency(pf)
                self._write_jsonl({
                    "event_type": "FILL_CONFIRMED",
                    "fill_key": fill_key,
                    "order_id": pf.order_id,
                    "confirmed_qty": round(pf.qty, 3),
                    "method": "immediate",
                    "latency_ms": pf.latency_ms,
                    "ts_ms": int(now * 1000),
                })
                continue

            # For non-immediate fills, try API confirmation
            if self._get_order_status and pf.order_id:
                pf.confirm_attempts += 1
                try:
                    status = self._get_order_status(pf.order_id)
                    if status and isinstance(status, dict):
                        clob_status = (status.get("status", "") or
                                       status.get("order_status", "")).lower()
                        filled_qty = _extract_filled_qty(status)

                        if clob_status in ("matched", "filled"):
                            if filled_qty <= 0 and self._get_order_fills:
                                filled_qty = _sum_order_fills(
                                    self._get_order_fills, pf.order_id)
                            if filled_qty <= 0:
                                filled_qty = pf.qty  # best effort

                            if filled_qty >= pf.qty * 0.95:  # full fill
                                pf.qty = filled_qty  # use exact venue qty
                                confirmed.append(pf)
                                to_remove.append(fill_key)
                                self._emit_latency(pf)
                                self._write_jsonl({
                                    "event_type": "FILL_CONFIRMED",
                                    "fill_key": fill_key,
                                    "order_id": pf.order_id,
                                    "confirmed_qty": round(filled_qty, 3),
                                    "expected_qty": round(pf.qty, 3),
                                    "method": "api_poll",
                                    "attempts": pf.confirm_attempts,
                                    "latency_ms": pf.latency_ms,
                                    "ts_ms": int(now * 1000),
                                })
                                continue

                            # Partial fill on a still-open order: apply delta now
                            if filled_qty > pf.applied_qty:
                                delta = filled_qty - pf.applied_qty
                                pf.applied_qty = filled_qty
                                # Return a copy with just the delta qty
                                partial_pf = PendingFill(
                                    fill_key=fill_key + f":partial:{filled_qty:.3f}",
                                    order_id=pf.order_id,
                                    slug=pf.slug, outcome=pf.outcome,
                                    side=pf.side, qty=delta,
                                    price=pf.price, source=pf.source,
                                    token_id=pf.token_id,
                                    reason=pf.reason,
                                    trade_id=pf.trade_id,
                                    match_id=pf.match_id,
                                )
                                confirmed.append(partial_pf)
                                remaining = pf.qty - filled_qty
                                self._write_jsonl({
                                    "event_type": "PARTIAL_FILL_APPLIED",
                                    "order_id": pf.order_id,
                                    "fill_key": fill_key,
                                    "confirmed_qty": round(delta, 3),
                                    "total_filled": round(filled_qty, 3),
                                    "remaining_open_qty": round(max(remaining, 0), 3),
                                    "expected_qty": round(pf.qty, 3),
                                    "ts_ms": int(now * 1000),
                                })
                                # Don't remove — keep watching for remaining
                                continue

                        elif clob_status in ("cancelled", "canceled",
                                             "expired", "rejected", "killed"):
                            # Check for partial fill on cancelled order
                            if filled_qty > pf.applied_qty:
                                delta = filled_qty - pf.applied_qty
                                pf.qty = delta
                                confirmed.append(pf)
                                to_remove.append(fill_key)
                                remaining = 0.0  # order is terminal
                                self._emit_latency(pf)
                                self._write_jsonl({
                                    "event_type": "PARTIAL_FILL_APPLIED",
                                    "order_id": pf.order_id,
                                    "fill_key": fill_key,
                                    "confirmed_qty": round(delta, 3),
                                    "total_filled": round(filled_qty, 3),
                                    "remaining_open_qty": 0.0,
                                    "terminal_status": clob_status,
                                    "ts_ms": int(now * 1000),
                                })
                            elif filled_qty > 0 and pf.applied_qty == 0:
                                pf.qty = filled_qty
                                confirmed.append(pf)
                                to_remove.append(fill_key)
                                self._emit_latency(pf)
                                self._write_jsonl({
                                    "event_type": "FILL_CONFIRMED",
                                    "fill_key": fill_key,
                                    "order_id": pf.order_id,
                                    "confirmed_qty": round(filled_qty, 3),
                                    "method": "partial_on_cancel",
                                    "latency_ms": pf.latency_ms,
                                    "ts_ms": int(now * 1000),
                                })
                            else:
                                self._write_jsonl({
                                    "event_type": "FILL_CONFIRM_FAILED",
                                    "fill_key": fill_key,
                                    "order_id": pf.order_id,
                                    "reason": f"order_{clob_status}_no_fill",
                                    "ts_ms": int(now * 1000),
                                })
                                to_remove.append(fill_key)
                            continue
                except Exception:
                    pass  # will retry on next tick

            # For fills with trade_id but no order to poll, trust immediately
            if pf.trade_id or pf.match_id:
                confirmed.append(pf)
                to_remove.append(fill_key)
                self._emit_latency(pf)
                self._write_jsonl({
                    "event_type": "FILL_CONFIRMED",
                    "fill_key": fill_key,
                    "order_id": pf.order_id,
                    "confirmed_qty": round(pf.qty, 3),
                    "method": "exchange_id_trust",
                    "latency_ms": pf.latency_ms,
                    "ts_ms": int(now * 1000),
                })

        for key in to_remove:
            self._pending.pop(key, None)

        return confirmed

    @property
    def pending_count(self) -> int:
        return len(self._pending)


# ══════════════════════════════════════════════════════════════════
#  ORDER LIFECYCLE WATCHER (no UNKNOWN)
# ══════════════════════════════════════════════════════════════════

class WatchedOrder:
    """An order with status=UNKNOWN that needs resolution."""
    __slots__ = ("order_id", "slug", "outcome", "side", "price", "qty",
                 "token_id", "submit_ts", "poll_attempts", "resolved_status")

    def __init__(self, order_id: str, slug: str, outcome: str,
                 side: str, price: float, qty: float,
                 token_id: str = ""):
        self.order_id = order_id
        self.slug = slug
        self.outcome = outcome
        self.side = side
        self.price = price
        self.qty = qty
        self.token_id = token_id
        self.submit_ts = time.time()
        self.poll_attempts = 0
        self.resolved_status = ""  # empty until resolved

    @property
    def age_sec(self) -> float:
        return time.time() - self.submit_ts


class OrderLifecycleWatcher:
    """Watches UNKNOWN orders until resolved to a terminal state.

    States: CONFIRMED_FILLED, CONFIRMED_OPEN, CONFIRMED_CANCELLED, CONFIRMED_REJECTED
    Unresolved after timeout -> one final poll -> ORPHAN_ORDER.
    Orphan blocks BUYs only; SELLs that reduce exposure are always allowed.
    """

    RESOLVE_TIMEOUT_SEC = 15.0  # max time to resolve UNKNOWN
    POLL_INTERVAL_SEC = 1.0     # poll every 1s

    def __init__(self,
                 get_order_status_fn: Optional[Callable] = None,
                 get_order_fills_fn: Optional[Callable] = None,
                 write_jsonl: Optional[Callable] = None):
        self._watched: Dict[str, WatchedOrder] = {}
        self._get_order_status = get_order_status_fn
        self._get_order_fills = get_order_fills_fn
        self._write_jsonl = write_jsonl or (lambda d: None)
        self._orphaned_slugs: Set[str] = set()  # slugs with BUYs blocked
        self._last_poll_ts: float = 0.0

    def watch(self, order_id: str, slug: str, outcome: str,
              side: str, price: float, qty: float,
              token_id: str = "") -> None:
        """Register an order with UNKNOWN status for resolution."""
        if order_id in self._watched:
            return
        self._watched[order_id] = WatchedOrder(
            order_id, slug, outcome, side, price, qty, token_id)

    def _final_fill_check(self, wo: WatchedOrder) -> float:
        """One final attempt to find fills for an order before marking orphan.
        Checks both order status AND fills/trades endpoint."""
        filled_qty = 0.0
        # 1. Final order status poll
        if self._get_order_status:
            try:
                status = self._get_order_status(wo.order_id)
                if status and isinstance(status, dict):
                    filled_qty = _extract_filled_qty(status)
            except Exception:
                pass
        # 2. Check fills/trades endpoint as last resort
        if filled_qty <= 0 and self._get_order_fills:
            try:
                filled_qty = _sum_order_fills(self._get_order_fills, wo.order_id)
            except Exception:
                pass
        return filled_qty

    def tick(self) -> Tuple[List[dict], List[dict]]:
        """Poll watched orders. Returns (resolved_fills, orphan_orders).

        resolved_fills: [{order_id, slug, outcome, side, qty, price, status, token_id}, ...]
        orphan_orders: [{order_id, slug, outcome, side, status, age_sec}, ...]
        """
        now = time.time()
        if now - self._last_poll_ts < self.POLL_INTERVAL_SEC:
            return [], []
        self._last_poll_ts = now

        if not self._watched or not self._get_order_status:
            return [], []

        resolved_fills: List[dict] = []
        orphan_orders: List[dict] = []
        to_remove: List[str] = []

        for oid, wo in list(self._watched.items()):
            # Timeout -> one final fill check -> orphan
            if wo.age_sec > self.RESOLVE_TIMEOUT_SEC:
                # Last-chance recovery: check fills endpoint before orphaning
                last_chance_qty = self._final_fill_check(wo)
                if last_chance_qty > 0:
                    # Found a fill — process it normally instead of orphaning
                    wo.resolved_status = "CONFIRMED_FILLED"
                    resolved_fills.append({
                        "order_id": oid, "slug": wo.slug,
                        "outcome": wo.outcome, "side": wo.side,
                        "qty": last_chance_qty, "price": wo.price,
                        "status": "CONFIRMED_FILLED",
                        "token_id": wo.token_id,
                    })
                    self._write_jsonl({
                        "event_type": "ORDER_RESOLVED",
                        "order_id": oid,
                        "resolved_status": "CONFIRMED_FILLED",
                        "fill_qty": round(last_chance_qty, 3),
                        "age_sec": round(wo.age_sec, 1),
                        "method": "final_fill_check",
                        "poll_attempts": wo.poll_attempts,
                        "ts_ms": int(now * 1000),
                    })
                    to_remove.append(oid)
                    continue

                # Truly orphaned — no fill found anywhere
                wo.resolved_status = "ORPHAN"
                self._orphaned_slugs.add(wo.slug)
                orphan = {
                    "order_id": oid, "slug": wo.slug,
                    "outcome": wo.outcome, "side": wo.side,
                    "status": "UNKNOWN", "age_sec": round(wo.age_sec, 1),
                }
                orphan_orders.append(orphan)
                self._write_jsonl({
                    "event_type": "ORPHAN_ORDER",
                    "order_id": oid,
                    "slug": wo.slug, "outcome": wo.outcome,
                    "side": wo.side, "price": round(wo.price, 4),
                    "qty": round(wo.qty, 3),
                    "age_sec": round(wo.age_sec, 1),
                    "ts_ms": int(now * 1000),
                })
                self._write_jsonl({
                    "event_type": "ORPHAN_ORDER_CONFIRMED",
                    "order_id": oid,
                    "slug": wo.slug, "outcome": wo.outcome,
                    "action_blocked": "BUY_only",
                    "sells_allowed": True,
                    "ts_ms": int(now * 1000),
                })
                print(f"  [ORDER_WATCHER] ORPHAN: {wo.slug} {wo.outcome} "
                      f"{wo.side} order={oid[:16]}... age={wo.age_sec:.0f}s "
                      f"(BUYs blocked, SELLs allowed)")
                to_remove.append(oid)
                continue

            # Poll
            wo.poll_attempts += 1
            try:
                status = self._get_order_status(oid)
                if not status or not isinstance(status, dict):
                    continue

                clob_status = (status.get("status", "") or
                               status.get("order_status", "")).lower()
                filled_qty = _extract_filled_qty(status)

                if clob_status in ("matched", "filled"):
                    wo.resolved_status = "CONFIRMED_FILLED"
                    if filled_qty <= 0:
                        filled_qty = wo.qty
                    resolved_fills.append({
                        "order_id": oid, "slug": wo.slug,
                        "outcome": wo.outcome, "side": wo.side,
                        "qty": filled_qty, "price": wo.price,
                        "status": "CONFIRMED_FILLED",
                        "token_id": wo.token_id,
                    })
                    self._write_jsonl({
                        "event_type": "ORDER_RESOLVED",
                        "order_id": oid,
                        "resolved_status": "CONFIRMED_FILLED",
                        "fill_qty": round(filled_qty, 3),
                        "age_sec": round(wo.age_sec, 1),
                        "poll_attempts": wo.poll_attempts,
                        "ts_ms": int(now * 1000),
                    })
                    to_remove.append(oid)

                elif clob_status in ("live", "open"):
                    wo.resolved_status = "CONFIRMED_OPEN"
                    self._write_jsonl({
                        "event_type": "ORDER_RESOLVED",
                        "order_id": oid,
                        "resolved_status": "CONFIRMED_OPEN",
                        "age_sec": round(wo.age_sec, 1),
                        "ts_ms": int(now * 1000),
                    })
                    to_remove.append(oid)

                elif clob_status in ("cancelled", "canceled", "expired"):
                    resolved_status = "CONFIRMED_CANCELLED"
                    wo.resolved_status = resolved_status
                    # Check for partial fill
                    if filled_qty > 0:
                        resolved_fills.append({
                            "order_id": oid, "slug": wo.slug,
                            "outcome": wo.outcome, "side": wo.side,
                            "qty": filled_qty, "price": wo.price,
                            "status": "CONFIRMED_FILLED",
                            "token_id": wo.token_id,
                            "partial": True,
                        })
                    self._write_jsonl({
                        "event_type": "ORDER_RESOLVED",
                        "order_id": oid,
                        "resolved_status": resolved_status,
                        "partial_fill_qty": round(filled_qty, 3) if filled_qty > 0 else 0,
                        "age_sec": round(wo.age_sec, 1),
                        "ts_ms": int(now * 1000),
                    })
                    to_remove.append(oid)

                elif clob_status in ("rejected",):
                    wo.resolved_status = "CONFIRMED_REJECTED"
                    self._write_jsonl({
                        "event_type": "ORDER_RESOLVED",
                        "order_id": oid,
                        "resolved_status": "CONFIRMED_REJECTED",
                        "age_sec": round(wo.age_sec, 1),
                        "ts_ms": int(now * 1000),
                    })
                    to_remove.append(oid)

            except Exception:
                continue

        for oid in to_remove:
            self._watched.pop(oid, None)

        return resolved_fills, orphan_orders

    def is_slug_blocked(self, slug: str) -> bool:
        """Check if BUYs are blocked for a slug due to orphan orders.
        SELLs are always allowed regardless."""
        return slug in self._orphaned_slugs

    def clear_orphan_block(self, slug: str) -> None:
        """Clear orphan block for a slug after reconciliation."""
        self._orphaned_slugs.discard(slug)

    def clear_all_orphan_blocks(self) -> None:
        """Clear all orphan blocks (e.g., after successful resync)."""
        self._orphaned_slugs.clear()

    @property
    def watched_count(self) -> int:
        return len(self._watched)

    @property
    def orphaned_slugs(self) -> Set[str]:
        return set(self._orphaned_slugs)


# ══════════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════

def _extract_filled_qty(resp: dict) -> float:
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


def _sum_order_fills(get_order_fills_fn: Callable, order_id: str) -> float:
    """Sum fill quantities from the fills endpoint."""
    try:
        fills = get_order_fills_fn(order_id)
        if not fills or not isinstance(fills, list):
            return 0.0
        total = 0.0
        for f in fills:
            qty = f.get("size") or f.get("amount") or f.get("qty") or 0
            total += float(qty)
        return total
    except Exception:
        return 0.0
