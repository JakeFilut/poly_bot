"""Truth Capture — authoritative fill recording and position derivation.

Design goals:
    1. Every live fill MUST be persisted to disk (append-only JSONL).
    2. Positions derived ONLY from persisted fills, never from in-memory state.
    3. Dual-path capture: immediate record on fill event + order-watcher polling.
    4. Periodic wallet truth scan catches anything both paths missed.
    5. Reconciliation against wallet balances with SAFE MODE on desync.

Ledger format (one JSON object per line in ``./logs/fills_ledger.jsonl``):
    {
      "ts_iso": "2026-02-20T15:30:01.123456Z",
      "ts_ms": 1771512601123,
      "wallet": "0xabc...",
      "order_id": "...",
      "trade_id": "...",
      "token_id": "...",
      "slug": "...",
      "outcome": "Up"|"Down",
      "action": "BUY"|"SELL",
      "qty": "10.5",          # string for exact precision
      "price": "0.95",        # string for exact precision
      "fees": "0",
      "source": "ws"|"poll"|"external_scan"
    }
"""
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Decimal helpers
# ---------------------------------------------------------------------------
_ZERO = Decimal("0")
_PRECISION = Decimal("0.000000001")
_DUST = Decimal("0.001")


def _to_dec(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v is None:
        return _ZERO
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return _ZERO


def _dec_str(d: Decimal) -> str:
    return str(d.quantize(_PRECISION))


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _ts_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class TruthFill:
    """One confirmed fill in the append-only ledger."""
    ts_iso: str
    ts_ms: int
    wallet: str
    order_id: str
    trade_id: str
    token_id: str
    slug: str
    outcome: str          # "Up" or "Down"
    action: str           # "BUY" or "SELL"
    qty: Decimal
    price: Decimal
    fees: Decimal
    source: str           # "ws", "poll", "external_scan"


@dataclass
class TruthPosition:
    """Derived position for one (token_id, outcome)."""
    token_id: str
    slug: str
    outcome: str
    net_qty: Decimal = _ZERO
    avg_price: Decimal = _ZERO       # weighted-average cost
    total_cost: Decimal = _ZERO
    total_buy_qty: Decimal = _ZERO
    total_sell_qty: Decimal = _ZERO
    realized_pnl: Decimal = _ZERO
    fill_count: int = 0


# ---------------------------------------------------------------------------
# Order Watcher — polls a single order until terminal
# ---------------------------------------------------------------------------
@dataclass
class _WatchedOrder:
    """Internal state for one order being watched."""
    order_id: str
    token_id: str
    slug: str
    outcome: str
    side: str             # "BUY" or "SELL"
    requested_qty: float
    price: float
    started_ts: float
    last_poll_ts: float = 0.0
    cumulative_filled: float = 0.0
    terminal: bool = False


# ---------------------------------------------------------------------------
# Cursor persistence for wallet truth scan
# ---------------------------------------------------------------------------
_CURSOR_FILE = "./logs/truth_scan_cursor.json"


def _load_cursor(path: str = _CURSOR_FILE) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_scan_ts_ms": 0, "last_trade_id": ""}


def _save_cursor(data: dict, path: str = _CURSOR_FILE) -> None:
    try:
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════
# TruthCapture
# ═══════════════════════════════════════════════════════════════════════════
class TruthCapture:
    """Authoritative fill capture + position derivation.

    Parameters
    ----------
    ledger_path : str
        Path to the append-only JSONL ledger file.
    wallet_address : str
        Hex wallet address (for ledger records).
    get_order_status_fn : callable
        ``(order_id: str) -> Optional[dict]``  — returns CLOB order dict.
    get_trades_fn : callable
        ``() -> List[dict]``  — returns recent trades for the wallet.
    get_balances_fn : callable
        ``() -> List[dict]``  — returns wallet token balances.
    write_jsonl_fn : callable
        ``(event: dict) -> None``  — emit structured log event.
    poll_interval_sec : float
        Order watcher poll interval (default 1.0 s).
    poll_timeout_sec : float
        Max time to watch a single order (default 30 s).
    scan_interval_sec : float
        Wallet truth scan interval (default 60 s).
    reconcile_interval_sec : float
        Reconciliation interval (default 60 s).
    desync_tolerance : float
        Max qty diff before CRITICAL (default 0.01 shares).
    """

    def __init__(
        self,
        ledger_path: str = "./logs/fills_ledger.jsonl",
        wallet_address: str = "",
        get_order_status_fn: Optional[Callable] = None,
        get_trades_fn: Optional[Callable] = None,
        get_balances_fn: Optional[Callable] = None,
        write_jsonl_fn: Optional[Callable] = None,
        token_meta_fn: Optional[Callable] = None,
        poll_interval_sec: float = 1.0,
        poll_timeout_sec: float = 30.0,
        scan_interval_sec: float = 60.0,
        reconcile_interval_sec: float = 60.0,
        desync_tolerance: float = 0.01,
    ):
        self._ledger_path = ledger_path
        self._wallet = wallet_address
        self._get_order_status = get_order_status_fn
        self._get_trades = get_trades_fn
        self._get_balances = get_balances_fn
        self._write_jsonl = write_jsonl_fn or (lambda d: None)
        self._token_meta_fn = token_meta_fn  # () -> {token_id: (slug, outcome)}
        self._poll_interval = poll_interval_sec
        self._poll_timeout = poll_timeout_sec
        self._scan_interval = scan_interval_sec
        self._reconcile_interval = reconcile_interval_sec
        self._desync_tolerance = desync_tolerance

        self._lock = threading.Lock()

        # All fills in memory (loaded from disk + runtime)
        self._fills: List[TruthFill] = []
        self._seen_ids: Set[str] = set()

        # Positions: token_id -> TruthPosition
        self._positions: Dict[str, TruthPosition] = {}

        # Token metadata: token_id -> (slug, outcome)
        self._token_meta: Dict[str, Tuple[str, str]] = {}

        # Order watchers: order_id -> _WatchedOrder
        self._watchers: Dict[str, _WatchedOrder] = {}

        # SAFE MODE
        self._safe_mode: bool = False
        self._safe_mode_reason: str = ""
        self._safe_mode_mismatches: List[dict] = []

        # Timers
        self._last_scan_ts: float = 0.0
        self._last_reconcile_ts: float = 0.0
        self._last_positions_print_ts: float = 0.0

        # Counters
        self.fills_from_ws: int = 0
        self.fills_from_poll: int = 0
        self.fills_from_scan: int = 0
        self.orders_watched: int = 0
        self.reconcile_runs: int = 0
        self.desync_count: int = 0

        # Ensure ledger directory exists
        d = os.path.dirname(os.path.abspath(self._ledger_path))
        if d:
            os.makedirs(d, exist_ok=True)

    # ══════════════════════════════════════════════════════════════════
    #  DISK I/O
    # ══════════════════════════════════════════════════════════════════

    def load_from_disk(self) -> int:
        """Load existing fills from JSONL ledger. Returns count loaded."""
        if not os.path.exists(self._ledger_path):
            print(f"  [TRUTH] No ledger file at {self._ledger_path}, starting fresh")
            return 0
        loaded = 0
        with self._lock:
            with open(self._ledger_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                        fill = self._parse_fill(raw)
                        if fill is None:
                            continue
                        dk = self._dedup_key(fill)
                        if dk in self._seen_ids:
                            continue
                        self._seen_ids.add(dk)
                        self._fills.append(fill)
                        self._update_token_meta_from_fill(fill)
                        loaded += 1
                    except Exception as e:
                        print(f"  [TRUTH] WARN: skip line {line_num}: {e}")
            self._recompute_positions()
        active = sum(1 for p in self._positions.values() if p.net_qty > _ZERO)
        print(f"  [TRUTH] Loaded {loaded} fills -> {active} active positions "
              f"from {self._ledger_path}")
        self._write_jsonl({
            "event_type": "TRUTH_LOADED",
            "fills": loaded,
            "positions": active,
            "ts_ms": _ts_ms(),
        })
        return loaded

    def _append_to_disk(self, fill: TruthFill) -> None:
        row = {
            "ts_iso": fill.ts_iso,
            "ts_ms": fill.ts_ms,
            "wallet": fill.wallet,
            "order_id": fill.order_id,
            "trade_id": fill.trade_id,
            "token_id": fill.token_id,
            "slug": fill.slug,
            "outcome": fill.outcome,
            "action": fill.action,
            "qty": _dec_str(fill.qty),
            "price": _dec_str(fill.price),
            "fees": _dec_str(fill.fees),
            "source": fill.source,
        }
        try:
            with open(self._ledger_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, separators=(",", ":")) + "\n")
        except Exception as e:
            print(f"  [TRUTH] ERROR writing ledger: {e}")

    def _parse_fill(self, raw: dict) -> Optional[TruthFill]:
        action = (raw.get("action") or "").upper()
        if action not in ("BUY", "SELL"):
            return None
        qty = _to_dec(raw.get("qty", raw.get("fill_qty", 0)))
        if qty <= _ZERO:
            return None
        return TruthFill(
            ts_iso=raw.get("ts_iso", raw.get("timestamp", "")),
            ts_ms=int(raw.get("ts_ms", 0)),
            wallet=raw.get("wallet", self._wallet),
            order_id=str(raw.get("order_id", "")),
            trade_id=str(raw.get("trade_id", "")),
            token_id=str(raw.get("token_id", "")),
            slug=raw.get("slug", ""),
            outcome=raw.get("outcome", raw.get("side", "")),
            action=action,
            qty=qty,
            price=_to_dec(raw.get("price", raw.get("fill_price", 0))),
            fees=_to_dec(raw.get("fees", 0)),
            source=raw.get("source", "unknown"),
        )

    # ══════════════════════════════════════════════════════════════════
    #  DEDUP
    # ══════════════════════════════════════════════════════════════════

    @staticmethod
    def _dedup_key(fill: TruthFill) -> str:
        if fill.trade_id:
            return f"tid:{fill.trade_id}"
        return (f"oid:{fill.order_id}|{fill.action}|"
                f"{_dec_str(fill.qty)}|{_dec_str(fill.price)}|"
                f"{fill.ts_iso}")

    # ══════════════════════════════════════════════════════════════════
    #  TOKEN METADATA
    # ══════════════════════════════════════════════════════════════════

    def register_token(self, token_id: str, slug: str, outcome: str) -> None:
        with self._lock:
            self._token_meta[str(token_id)] = (slug, outcome)

    def _update_token_meta_from_fill(self, fill: TruthFill) -> None:
        if fill.token_id and fill.slug:
            self._token_meta[fill.token_id] = (fill.slug, fill.outcome)

    def _resolve_meta(self, token_id: str) -> Tuple[str, str]:
        """Return (slug, outcome) for a token_id."""
        meta = self._token_meta.get(str(token_id))
        if meta:
            return meta
        # Try the external meta function
        if self._token_meta_fn:
            try:
                ext = self._token_meta_fn()
                if isinstance(ext, dict):
                    m = ext.get(str(token_id))
                    if m:
                        self._token_meta[str(token_id)] = m
                        return m
            except Exception:
                pass
        return ("", "")

    # ══════════════════════════════════════════════════════════════════
    #  PATH A: RECORD FILL (immediate — from WS/event or bot logic)
    # ══════════════════════════════════════════════════════════════════

    def record_fill(
        self,
        token_id: str,
        slug: str,
        outcome: str,
        action: str,
        qty: float,
        price: float,
        order_id: str = "",
        trade_id: str = "",
        fees: float = 0.0,
        source: str = "ws",
    ) -> Optional[TruthPosition]:
        """Record a CONFIRMED fill. Persists to disk immediately.

        Returns the updated TruthPosition, or None if duplicate/invalid.
        """
        qty_d = _to_dec(qty)
        price_d = _to_dec(price)
        if qty_d <= _ZERO:
            return None

        fill = TruthFill(
            ts_iso=_utc_iso(),
            ts_ms=_ts_ms(),
            wallet=self._wallet,
            order_id=str(order_id),
            trade_id=str(trade_id),
            token_id=str(token_id),
            slug=slug,
            outcome=outcome,
            action=action.upper(),
            qty=qty_d,
            price=price_d,
            fees=_to_dec(fees),
            source=source,
        )

        with self._lock:
            dk = self._dedup_key(fill)
            if dk in self._seen_ids:
                return None
            self._seen_ids.add(dk)
            self._fills.append(fill)
            self._update_token_meta_from_fill(fill)
            self._append_to_disk(fill)
            pos = self._apply_fill_to_position(fill)

        # Track source
        if source == "ws":
            self.fills_from_ws += 1
        elif source == "poll":
            self.fills_from_poll += 1
        elif source == "external_scan":
            self.fills_from_scan += 1

        # Log
        self._write_jsonl({
            "event_type": "TRUTH_FILL",
            "token_id": token_id, "slug": slug, "outcome": outcome,
            "action": action.upper(),
            "qty": float(qty_d), "price": float(price_d),
            "order_id": order_id, "trade_id": trade_id,
            "source": source,
            "net_qty": float(pos.net_qty),
            "avg_price": float(pos.avg_price),
            "ts_ms": _ts_ms(),
        })
        print(f"  [TRUTH] FILL {action.upper()} {slug} {outcome}: "
              f"qty={float(qty_d):.4f} @ {float(price_d):.4f}  "
              f"net={float(pos.net_qty):.4f}  avg={float(pos.avg_price):.4f}  "
              f"src={source}")
        return pos

    # ══════════════════════════════════════════════════════════════════
    #  PATH B: ORDER WATCHER (poll order_id until terminal)
    # ══════════════════════════════════════════════════════════════════

    def watch_order(
        self,
        order_id: str,
        token_id: str,
        slug: str,
        outcome: str,
        side: str,
        qty: float,
        price: float,
    ) -> None:
        """Start watching an order for fills. Call after every ORDER_PLACED."""
        if not order_id or order_id.startswith("paper_"):
            return
        with self._lock:
            if order_id in self._watchers:
                return
            self._watchers[order_id] = _WatchedOrder(
                order_id=order_id,
                token_id=str(token_id),
                slug=slug,
                outcome=outcome,
                side=side.upper(),
                requested_qty=float(qty),
                price=float(price),
                started_ts=time.time(),
            )
        self.orders_watched += 1
        self._write_jsonl({
            "event_type": "TRUTH_WATCHER_START",
            "order_id": order_id, "slug": slug, "outcome": outcome,
            "side": side, "qty": qty, "price": price,
            "ts_ms": _ts_ms(),
        })
        print(f"  [TRUTH] WATCHER started: {slug} {outcome} {side} "
              f"qty={qty:.1f} @ {price:.4f} order={order_id[:16]}...")

    def notify_fill(self, order_id: str, fill_qty: float) -> None:
        """Notify watcher that a fill was already recorded (e.g. from immediate
        fill response).  Updates cumulative_filled so the watcher doesn't
        double-record."""
        with self._lock:
            w = self._watchers.get(order_id)
            if w:
                w.cumulative_filled += fill_qty
                if w.cumulative_filled >= w.requested_qty - 0.5:
                    w.terminal = True

    def poll_watchers(self) -> int:
        """Poll all active order watchers. Call from main loop tick.

        Returns number of new fills discovered.
        """
        if self._get_order_status is None:
            return 0

        now = time.time()
        new_fills = 0
        to_remove: List[str] = []

        with self._lock:
            watcher_snapshot = list(self._watchers.items())

        for oid, w in watcher_snapshot:
            if w.terminal:
                to_remove.append(oid)
                continue

            age = now - w.started_ts
            if age > self._poll_timeout:
                to_remove.append(oid)
                self._write_jsonl({
                    "event_type": "TRUTH_WATCHER_TIMEOUT",
                    "order_id": oid, "slug": w.slug,
                    "age_sec": round(age, 1),
                    "filled_so_far": w.cumulative_filled,
                    "ts_ms": _ts_ms(),
                })
                continue

            if now - w.last_poll_ts < self._poll_interval:
                continue
            w.last_poll_ts = now

            try:
                resp = self._get_order_status(oid)
                if not resp or not isinstance(resp, dict):
                    continue

                status = (resp.get("status") or resp.get("order_status") or "").lower()
                size_matched = float(resp.get("size_matched") or 0)

                if status in ("matched", "filled"):
                    if size_matched <= 0:
                        size_matched = w.requested_qty
                    delta = size_matched - w.cumulative_filled
                    if delta > 0.5:  # at least 0.5 share new fill
                        w.cumulative_filled = size_matched
                        fill_price = float(resp.get("price") or w.price)
                        # Record via the primary path
                        trade_id = resp.get("id") or resp.get("trade_id") or ""
                        pos = self.record_fill(
                            token_id=w.token_id,
                            slug=w.slug,
                            outcome=w.outcome,
                            action=w.side,
                            qty=delta,
                            price=fill_price,
                            order_id=oid,
                            trade_id=str(trade_id),
                            source="poll",
                        )
                        if pos is not None:
                            new_fills += 1
                    w.terminal = True
                    to_remove.append(oid)

                elif status in ("cancelled", "canceled", "expired"):
                    # Check for partial fill on cancelled order
                    if size_matched > 0:
                        delta = size_matched - w.cumulative_filled
                        if delta > 0.5:
                            fill_price = float(resp.get("price") or w.price)
                            trade_id = resp.get("id") or resp.get("trade_id") or ""
                            pos = self.record_fill(
                                token_id=w.token_id,
                                slug=w.slug,
                                outcome=w.outcome,
                                action=w.side,
                                qty=delta,
                                price=fill_price,
                                order_id=oid,
                                trade_id=str(trade_id),
                                source="poll",
                            )
                            if pos is not None:
                                new_fills += 1
                    w.terminal = True
                    to_remove.append(oid)

            except Exception as e:
                self._write_jsonl({
                    "event_type": "TRUTH_WATCHER_POLL_ERROR",
                    "order_id": oid, "err": str(e)[:120],
                    "ts_ms": _ts_ms(),
                })

        with self._lock:
            for oid in to_remove:
                self._watchers.pop(oid, None)

        return new_fills

    # ══════════════════════════════════════════════════════════════════
    #  WALLET TRUTH SCAN (periodic catch-all)
    # ══════════════════════════════════════════════════════════════════

    def maybe_run_wallet_scan(self) -> int:
        """Run wallet truth scan if enough time has elapsed. Returns new fills."""
        now = time.time()
        if now - self._last_scan_ts < self._scan_interval:
            return 0
        self._last_scan_ts = now
        return self.run_wallet_scan()

    def run_wallet_scan(self) -> int:
        """Fetch recent trades for the wallet and ingest any missing fills.

        Uses cursor persistence so we don't re-process old trades.
        Returns number of NEW fills discovered.
        """
        if self._get_trades is None:
            return 0

        try:
            raw_trades = self._get_trades()
            if not isinstance(raw_trades, list) or not raw_trades:
                return 0
        except Exception as e:
            self._write_jsonl({
                "event_type": "TRUTH_SCAN_ERROR",
                "err": str(e)[:200],
                "ts_ms": _ts_ms(),
            })
            return 0

        cursor = _load_cursor()
        new_count = 0

        for raw in raw_trades:
            trade_id = str(raw.get("trade_id") or raw.get("id") or "")
            if not trade_id:
                continue

            # Skip already-seen trades
            with self._lock:
                dk = f"tid:{trade_id}"
                if dk in self._seen_ids:
                    continue

            token_id = str(raw.get("token_id") or raw.get("asset_id") or "")
            action = (raw.get("side") or raw.get("action") or "").upper()
            if action not in ("BUY", "SELL"):
                continue

            qty = raw.get("size") or raw.get("amount") or 0
            price = raw.get("price") or 0

            slug, outcome = self._resolve_meta(token_id)
            if not slug:
                slug = raw.get("slug", "")
            if not outcome:
                outcome = raw.get("outcome", "")

            order_id = str(raw.get("order_id") or "")

            pos = self.record_fill(
                token_id=token_id,
                slug=slug,
                outcome=outcome,
                action=action,
                qty=qty,
                price=price,
                order_id=order_id,
                trade_id=trade_id,
                fees=raw.get("fee") or raw.get("fees") or 0,
                source="external_scan",
            )
            if pos is not None:
                new_count += 1

        # Update cursor
        if raw_trades:
            last = raw_trades[-1]
            cursor["last_scan_ts_ms"] = _ts_ms()
            cursor["last_trade_id"] = str(
                last.get("trade_id") or last.get("id") or "")
            _save_cursor(cursor)

        self._write_jsonl({
            "event_type": "TRUTH_SCAN_DONE",
            "total_trades": len(raw_trades),
            "new_fills": new_count,
            "ts_ms": _ts_ms(),
        })
        if new_count > 0:
            print(f"  [TRUTH] Wallet scan: {new_count} new fills from "
                  f"{len(raw_trades)} trades")

        return new_count

    # ══════════════════════════════════════════════════════════════════
    #  POSITION COMPUTATION (Decimal, WAC)
    # ══════════════════════════════════════════════════════════════════

    def _apply_fill_to_position(self, fill: TruthFill) -> TruthPosition:
        """Apply a single fill to positions. Must hold self._lock."""
        tid = fill.token_id
        if tid not in self._positions:
            self._positions[tid] = TruthPosition(
                token_id=tid, slug=fill.slug, outcome=fill.outcome)
        pos = self._positions[tid]
        pos.slug = fill.slug or pos.slug
        pos.outcome = fill.outcome or pos.outcome

        if fill.action == "BUY":
            old_cost = pos.avg_price * pos.net_qty
            new_cost = fill.qty * fill.price
            new_qty = pos.net_qty + fill.qty
            if new_qty > _ZERO:
                pos.avg_price = (old_cost + new_cost) / new_qty
            pos.net_qty = new_qty
            pos.total_cost = pos.avg_price * pos.net_qty
            pos.total_buy_qty += fill.qty

        elif fill.action == "SELL":
            sell_qty = min(fill.qty, pos.net_qty)
            if sell_qty > _ZERO:
                pnl = (fill.price - pos.avg_price) * sell_qty
                pos.realized_pnl += pnl - fill.fees
                pos.net_qty -= sell_qty
                pos.total_cost = pos.avg_price * pos.net_qty
            pos.total_sell_qty += fill.qty

        if pos.net_qty < _DUST:
            pos.net_qty = _ZERO
            pos.total_cost = _ZERO
            pos.avg_price = _ZERO

        pos.fill_count += 1
        return pos

    def _recompute_positions(self) -> None:
        """Full recompute from all fills. Must hold self._lock."""
        self._positions.clear()
        for fill in self._fills:
            self._apply_fill_to_position(fill)

    def recompute_positions_from_ledger(self) -> Dict[str, TruthPosition]:
        """Public: full recompute. Returns {token_id: TruthPosition}."""
        with self._lock:
            self._recompute_positions()
            return dict(self._positions)

    # ══════════════════════════════════════════════════════════════════
    #  POSITION QUERIES
    # ══════════════════════════════════════════════════════════════════

    def get_position(self, token_id: str) -> TruthPosition:
        with self._lock:
            tid = str(token_id)
            if tid in self._positions:
                return self._positions[tid]
            slug, outcome = self._resolve_meta(tid)
            return TruthPosition(token_id=tid, slug=slug, outcome=outcome)

    def get_all_active(self) -> Dict[str, TruthPosition]:
        with self._lock:
            return {k: v for k, v in self._positions.items()
                    if v.net_qty > _ZERO}

    def get_sellable_qty(self, token_id: str) -> Decimal:
        return max(self.get_position(token_id).net_qty, _ZERO)

    def validate_sell(self, token_id: str, requested_qty: float,
                      slug: str = "") -> Tuple[bool, float, str]:
        """Validate sell against truth positions.
        Returns (allowed, capped_qty, reason)."""
        req = _to_dec(requested_qty)
        owned = self.get_sellable_qty(token_id)
        display = slug or token_id[-12:]

        if owned <= _ZERO:
            msg = f"SELL BLOCKED: no position for {display} (truth net_qty=0)"
            print(f"  [TRUTH] {msg}")
            return False, 0.0, msg

        if req > owned:
            capped = float(owned)
            if req - owned <= Decimal("0.000001"):
                return True, capped, "snapped"
            msg = (f"SELL CAPPED: {display}: req={float(req):.6f} > "
                   f"owned={float(owned):.6f}")
            print(f"  [TRUTH] {msg}")
            return True, capped, msg

        return True, float(req), "ok"

    # ══════════════════════════════════════════════════════════════════
    #  RECONCILIATION (truth vs wallet)
    # ══════════════════════════════════════════════════════════════════

    def maybe_run_reconciliation(self,
                                 active_token_ids: Optional[Set[str]] = None,
                                 ) -> Optional[dict]:
        """Run reconciliation if interval elapsed. Returns result or None."""
        now = time.time()
        if now - self._last_reconcile_ts < self._reconcile_interval:
            return None
        self._last_reconcile_ts = now
        return self.run_reconciliation(active_token_ids)

    def run_reconciliation(self,
                           active_token_ids: Optional[Set[str]] = None,
                           ) -> dict:
        """Compare truth positions vs actual wallet balances.

        Returns {"matches": [...], "mismatches": [...], "critical": bool}.
        If critical mismatch: enter SAFE MODE (no new buys).
        """
        self.reconcile_runs += 1
        result = {"matches": [], "mismatches": [], "critical": False,
                  "skipped": 0}

        if self._get_balances is None:
            return result

        # Fetch wallet balances
        try:
            raw_balances = self._get_balances()
            if not isinstance(raw_balances, list):
                return result
        except Exception as e:
            self._write_jsonl({
                "event_type": "TRUTH_RECONCILE_FETCH_ERROR",
                "err": str(e)[:200], "ts_ms": _ts_ms(),
            })
            return result

        # Build wallet truth: {token_id: qty}
        wallet: Dict[str, float] = {}
        for b in raw_balances:
            tid = str(b.get("token_id") or b.get("asset_id") or "")
            bal = float(b.get("balance") or b.get("size") or 0)
            if tid and bal > 0.001:
                wallet[tid] = bal

        if not wallet and not raw_balances:
            # Empty response — don't compare against empty
            return result

        # Collect all token_ids to check
        all_tids: Set[str] = set(wallet.keys())
        with self._lock:
            for tid, pos in self._positions.items():
                if pos.net_qty > _ZERO:
                    if active_token_ids is not None and tid not in active_token_ids:
                        result["skipped"] += 1
                        continue
                    all_tids.add(tid)

        mismatches = []
        tol = self._desync_tolerance
        crit_tol = tol * 10

        for tid in all_tids:
            truth_qty = float(self.get_position(tid).net_qty)
            wallet_qty = wallet.get(tid, 0.0)
            diff = wallet_qty - truth_qty
            abs_diff = abs(diff)

            if abs_diff < tol:
                result["matches"].append({
                    "token_id": tid, "truth": truth_qty,
                    "wallet": wallet_qty})
            else:
                is_crit = (abs_diff > crit_tol
                           or (truth_qty < tol and wallet_qty >= tol)
                           or (wallet_qty < tol and truth_qty >= tol))
                sev = "CRITICAL" if is_crit else "WARNING"
                slug, outcome = self._resolve_meta(tid)
                entry = {
                    "token_id": tid, "slug": slug, "outcome": outcome,
                    "truth": truth_qty, "wallet": wallet_qty,
                    "diff": diff, "severity": sev,
                }
                mismatches.append(entry)
                print(f"  [TRUTH] RECONCILE {sev}: {slug} {outcome}: "
                      f"truth={truth_qty:.4f} wallet={wallet_qty:.4f} "
                      f"diff={diff:+.4f}")

        result["mismatches"] = mismatches
        critical = any(m["severity"] == "CRITICAL" for m in mismatches)
        result["critical"] = critical

        self._write_jsonl({
            "event_type": "TRUTH_RECONCILE",
            "matches": len(result["matches"]),
            "mismatches": len(mismatches),
            "critical": critical,
            "skipped": result["skipped"],
            "details": mismatches[:20],
            "ts_ms": _ts_ms(),
        })

        if critical:
            self.desync_count += 1
            n_crit = sum(1 for m in mismatches if m["severity"] == "CRITICAL")
            self.enter_safe_mode(
                f"STATE DESYNC DETECTED: {n_crit} critical mismatches",
                mismatches)
        elif self._safe_mode and not mismatches:
            self.exit_safe_mode()

        return result

    # ══════════════════════════════════════════════════════════════════
    #  SAFE MODE
    # ══════════════════════════════════════════════════════════════════

    @property
    def safe_mode(self) -> bool:
        return self._safe_mode

    @property
    def safe_mode_reason(self) -> str:
        return self._safe_mode_reason

    def enter_safe_mode(self, reason: str,
                        mismatches: Optional[List[dict]] = None) -> None:
        self._safe_mode = True
        self._safe_mode_reason = reason
        self._safe_mode_mismatches = mismatches or []
        self._write_jsonl({
            "event_type": "TRUTH_SAFE_MODE_ENTER",
            "reason": reason,
            "mismatches": (mismatches or [])[:10],
            "ts_ms": _ts_ms(),
        })
        print(f"  [TRUTH] *** SAFE MODE: {reason} ***")
        print(f"  [TRUTH]     No new buys. Sells that reduce exposure allowed.")

    def exit_safe_mode(self) -> None:
        if self._safe_mode:
            self._safe_mode = False
            self._safe_mode_reason = ""
            self._safe_mode_mismatches = []
            self._write_jsonl({
                "event_type": "TRUTH_SAFE_MODE_EXIT", "ts_ms": _ts_ms()})
            print("  [TRUTH] SAFE MODE resolved — trading allowed")

    # ══════════════════════════════════════════════════════════════════
    #  MAIN TICK — call from bot main loop every iteration
    # ══════════════════════════════════════════════════════════════════

    def tick(self, active_token_ids: Optional[Set[str]] = None) -> None:
        """Run all periodic tasks: poll watchers, wallet scan, reconcile,
        and per-minute position print."""
        # 1. Poll order watchers (fast — every tick)
        self.poll_watchers()

        # 2. Wallet truth scan (every scan_interval_sec)
        self.maybe_run_wallet_scan()

        # 3. Reconciliation (every reconcile_interval_sec)
        self.maybe_run_reconciliation(active_token_ids)

        # 4. Per-minute position print
        now = time.time()
        if now - self._last_positions_print_ts >= 60.0:
            self._last_positions_print_ts = now
            self.print_positions()

    # ══════════════════════════════════════════════════════════════════
    #  REPORTING
    # ══════════════════════════════════════════════════════════════════

    def print_positions(self) -> None:
        """Print truth-derived positions + any wallet mismatches."""
        active = self.get_all_active()
        if not active:
            print("  [TRUTH] Positions: (none)")
            return
        print(f"  [TRUTH] ── Positions ({len(active)}) "
              f"{'[SAFE MODE]' if self._safe_mode else ''} ──")
        total_cost = _ZERO
        total_rpnl = _ZERO
        for tid, pos in sorted(active.items(), key=lambda x: x[1].slug):
            cost = float(pos.total_cost)
            total_cost += pos.total_cost
            total_rpnl += pos.realized_pnl
            print(f"  [TRUTH]   {pos.slug:<35s} {pos.outcome:<5s} "
                  f"qty={float(pos.net_qty):8.2f}  "
                  f"avg={float(pos.avg_price):.4f}  "
                  f"cost=${cost:7.2f}  "
                  f"rpnl={float(pos.realized_pnl):+.4f}")
        print(f"  [TRUTH]   Total cost=${float(total_cost):.2f}  "
              f"rpnl={float(total_rpnl):+.4f}  "
              f"fills_ws={self.fills_from_ws} poll={self.fills_from_poll} "
              f"scan={self.fills_from_scan}")

    def summary(self) -> dict:
        active = self.get_all_active()
        total_rpnl = sum((p.realized_pnl for p in self._positions.values()),
                         _ZERO)
        total_cost = sum((p.total_cost for p in active.values()), _ZERO)
        return {
            "total_fills": len(self._fills),
            "active_positions": len(active),
            "total_realized_pnl": float(total_rpnl),
            "total_cost_basis": float(total_cost),
            "safe_mode": self._safe_mode,
            "fills_ws": self.fills_from_ws,
            "fills_poll": self.fills_from_poll,
            "fills_scan": self.fills_from_scan,
            "watchers_active": len(self._watchers),
            "orders_watched": self.orders_watched,
            "reconcile_runs": self.reconcile_runs,
            "desync_count": self.desync_count,
        }
