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
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
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
# Market Window State Machine
# ---------------------------------------------------------------------------
class WindowState(Enum):
    PRE_WINDOW = "PRE_WINDOW"
    ACTIVE = "ACTIVE"
    SETTLEMENT = "SETTLEMENT"
    CLOSED = "CLOSED"


def _hour_boundaries(dt: Optional[datetime] = None):
    """Return (hour_start_utc, hour_end_utc) for the given or current time."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    start = dt.replace(minute=0, second=0, microsecond=0)
    end = start + timedelta(hours=1)
    return start, end


def _hour_file_suffix(dt: datetime) -> str:
    """Return YYYYMMDD_HH string for per-hour file naming."""
    return dt.strftime("%Y%m%d_%H")


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

        # ── Hour window tracking ──
        now = datetime.now(timezone.utc)
        self._hour_start, self._hour_end = _hour_boundaries(now)
        self._hour_suffix = _hour_file_suffix(self._hour_start)
        self._ledger_base_dir = os.path.dirname(os.path.abspath(self._ledger_path))
        self._ledger_base_name = os.path.splitext(
            os.path.basename(self._ledger_path))[0]

        # ── State machine ──
        self._window_state: WindowState = WindowState.ACTIVE
        self._window_state_since: float = time.time()

        # All fills in memory (CURRENT HOUR ONLY after rotation)
        self._fills: List[TruthFill] = []
        self._seen_ids: Set[str] = set()

        # Positions: token_id -> TruthPosition (derived from current-hour fills ONLY)
        self._positions: Dict[str, TruthPosition] = {}

        # Token metadata: token_id -> (slug, outcome)
        self._token_meta: Dict[str, Tuple[str, str]] = {}

        # Active hour slugs: set of slugs belonging to current hour
        self._active_hour_slugs: Set[str] = set()

        # Order watchers: order_id -> _WatchedOrder
        self._watchers: Dict[str, _WatchedOrder] = {}

        # SAFE MODE
        self._safe_mode: bool = False
        self._safe_mode_reason: str = ""
        self._safe_mode_mismatches: List[dict] = []

        # Active window filter: set of token_ids belonging to current hour
        self._active_window_tokens: Set[str] = set()
        self._active_window_only: bool = False
        self._strict_window_mode: bool = False

        # ── Incremental scan cursor (persisted in state.json) ──
        self._scan_cursor_ts_ms: int = 0        # last_seen_ts_ms
        self._scan_cursor_tid: str = ""          # last_seen_tid
        self._scan_recent_tids: List[str] = []   # ring buffer for same-ts dedup
        self._scan_recent_tids_max: int = 200

        # ── Cursor proof counters ──
        # Lifetime (never reset — proves nothing leaked since boot)
        self.cursor_accepted_fills: int = 0
        self.cursor_dropped_old_ts: int = 0
        self.cursor_dropped_outside_window: int = 0
        # Per-hour (reset on hour boundary — localises spikes)
        self.cursor_hour_accepted: int = 0
        self.cursor_hour_dropped_old: int = 0
        self.cursor_hour_dropped_window: int = 0

        # Timers
        self._last_scan_ts: float = 0.0
        self._last_reconcile_ts: float = 0.0
        self._last_positions_print_ts: float = 0.0
        self._last_loop_log_ts: float = 0.0

        # Counters
        self.fills_from_ws: int = 0
        self.fills_from_poll: int = 0
        self.fills_from_scan: int = 0
        self.fills_dedup_skipped: int = 0
        self.orders_watched: int = 0
        self.reconcile_runs: int = 0
        self.desync_count: int = 0

        # Ensure ledger directory exists
        d = os.path.dirname(os.path.abspath(self._ledger_path))
        if d:
            os.makedirs(d, exist_ok=True)

    # ══════════════════════════════════════════════════════════════════
    #  PER-HOUR FILE MANAGEMENT
    # ══════════════════════════════════════════════════════════════════

    def _current_hour_path(self) -> str:
        """Return path to the current hour's ledger JSONL."""
        return os.path.join(
            self._ledger_base_dir,
            f"{self._ledger_base_name}_{self._hour_suffix}.jsonl")

    # ── Cursor get/set (integrated into state.json by the bot) ──

    def get_scan_cursor(self) -> dict:
        """Return the current incremental scan cursor for state persistence."""
        return {
            "last_seen_ts_ms": self._scan_cursor_ts_ms,
            "last_seen_tid": self._scan_cursor_tid,
            "recent_tids": self._scan_recent_tids[-self._scan_recent_tids_max:],
        }

    def set_scan_cursor(self, cursor: dict) -> None:
        """Restore scan cursor from state.json on startup."""
        self._scan_cursor_ts_ms = int(cursor.get("last_seen_ts_ms", 0))
        self._scan_cursor_tid = str(cursor.get("last_seen_tid", ""))
        tids = cursor.get("recent_tids", [])
        self._scan_recent_tids = list(tids)[-self._scan_recent_tids_max:]

    def skip_history_now(self) -> None:
        """Set cursor to NOW — skip all earlier fills on next scan."""
        self._scan_cursor_ts_ms = _ts_ms()
        self._scan_cursor_tid = ""
        self._scan_recent_tids.clear()
        print(f"  [TRUTH] SKIP_HISTORY: cursor set to now "
              f"({self._scan_cursor_ts_ms}), no historical fills will be ingested")

    def clamp_cursor_to_hour(self, hour_start_ms: int) -> bool:
        """If boot cursor < hour_start, clamp up and warn. Returns True if clamped."""
        if self._scan_cursor_ts_ms > 0 and self._scan_cursor_ts_ms < hour_start_ms:
            old = self._scan_cursor_ts_ms
            self._scan_cursor_ts_ms = hour_start_ms
            self._scan_recent_tids.clear()
            self._write_jsonl({
                "event_type": "CURSOR_CLAMPED",
                "old_cursor_ms": old,
                "new_cursor_ms": hour_start_ms,
                "delta_ms": hour_start_ms - old,
                "reason": "boot_cursor < hour_start — clamped to prevent earlier-in-hour ingestion",
                "ts_ms": _ts_ms(),
            })
            print(f"  [TRUTH] WARNING: boot_cursor_ms ({old}) < hour_start_ms ({hour_start_ms})  "
                  f"→ clamped cursor up by {hour_start_ms - old}ms")
            return True
        return False

    def reset_hour_counters(self) -> None:
        """Reset per-hour cursor proof counters. Call on hour boundary."""
        self.cursor_hour_accepted = 0
        self.cursor_hour_dropped_old = 0
        self.cursor_hour_dropped_window = 0

    # ══════════════════════════════════════════════════════════════════
    #  STATE MACHINE
    # ══════════════════════════════════════════════════════════════════

    @property
    def window_state(self) -> WindowState:
        return self._window_state

    def _transition_state(self, new_state: WindowState, reason: str = "") -> None:
        old = self._window_state
        if old == new_state:
            return
        now = time.time()
        self._window_state = new_state
        self._window_state_since = now
        msg = (f"  [TRUTH] STATE: {old.value} -> {new_state.value}"
               f"  reason={reason}")
        print(msg)
        self._write_jsonl({
            "event_type": "WINDOW_STATE_TRANSITION",
            "from": old.value,
            "to": new_state.value,
            "reason": reason,
            "hour_start": self._hour_start.isoformat(),
            "ts_ms": _ts_ms(),
        })

    # ══════════════════════════════════════════════════════════════════
    #  HOUR ROTATION
    # ══════════════════════════════════════════════════════════════════

    def rotate_hour(self, new_hour_start: Optional[datetime] = None) -> None:
        """Rotate to a new hour window. Resets all in-memory state.

        Called by the bot at each hour boundary.
        """
        with self._lock:
            old_suffix = self._hour_suffix

            # 1. Transition state machine: old hour -> CLOSED
            self._transition_state(WindowState.CLOSED,
                                   reason=f"hour_end_{old_suffix}")

            # 2. Compute new hour boundaries
            if new_hour_start is None:
                new_hour_start = datetime.now(timezone.utc).replace(
                    minute=0, second=0, microsecond=0)
            self._hour_start, self._hour_end = _hour_boundaries(new_hour_start)
            self._hour_suffix = _hour_file_suffix(self._hour_start)

            # 3. Reset in-memory state
            old_fills = len(self._fills)
            self._fills.clear()
            self._seen_ids.clear()
            self._positions.clear()
            self._watchers.clear()
            self._active_hour_slugs.clear()

            # 4. Reset counters
            self.fills_from_ws = 0
            self.fills_from_poll = 0
            self.fills_from_scan = 0
            self.fills_dedup_skipped = 0

            # 5. Reset SAFE MODE for new hour
            if self._safe_mode:
                self._safe_mode = False
                self._safe_mode_reason = ""
                self._safe_mode_mismatches.clear()

            # 6. Set scan cursor to new hour start (skip all before this)
            self._scan_cursor_ts_ms = int(self._hour_start.timestamp() * 1000)
            self._scan_cursor_tid = ""
            self._scan_recent_tids.clear()

            # 7. Load current hour file if it exists (restart mid-hour)
            loaded = self._load_hour_file()

            # 8. Transition to ACTIVE
            self._transition_state(WindowState.ACTIVE,
                                   reason=f"hour_start_{self._hour_suffix}")

        print(f"  [TRUTH] HOUR ROTATION: {old_suffix} -> {self._hour_suffix}  "
              f"cleared={old_fills} fills  loaded={loaded}")
        self._write_jsonl({
            "event_type": "HOUR_ROTATION",
            "old_hour": old_suffix,
            "new_hour": self._hour_suffix,
            "cleared_fills": old_fills,
            "loaded_fills": loaded,
            "ts_ms": _ts_ms(),
        })

    def enter_settlement(self) -> None:
        """Transition to SETTLEMENT state. No new buys, reduce-only sells."""
        self._transition_state(WindowState.SETTLEMENT,
                               reason="hour_ending")

    # ══════════════════════════════════════════════════════════════════
    #  DISK I/O
    # ══════════════════════════════════════════════════════════════════

    def _load_hour_file(self) -> int:
        """Load fills from the current hour's JSONL file. Returns count.
        MUST hold self._lock."""
        path = self._current_hour_path()
        if not os.path.exists(path):
            return 0
        loaded = 0
        with open(path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                    fill = self._parse_fill(raw)
                    if fill is None:
                        continue
                    # Time-based filter: only accept fills within this hour
                    if not self._is_current_hour_fill(fill):
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
        if loaded > 0:
            self._recompute_positions()
        return loaded

    def load_from_disk(self) -> int:
        """Load fills for the CURRENT HOUR from per-hour JSONL file.

        If per-hour file doesn't exist, falls back to global ledger and
        filters by timestamp.  Returns count loaded.
        """
        hour_path = self._current_hour_path()

        with self._lock:
            # 1. Try per-hour file first
            if os.path.exists(hour_path):
                loaded = self._load_hour_file()
                active = sum(1 for p in self._positions.values()
                             if p.net_qty > _ZERO)
                print(f"  [TRUTH] Loaded {loaded} fills (hour={self._hour_suffix}) "
                      f"-> {active} active positions")
                self._write_jsonl({
                    "event_type": "TRUTH_LOADED",
                    "fills": loaded,
                    "positions": active,
                    "hour": self._hour_suffix,
                    "source": "hour_file",
                    "ts_ms": _ts_ms(),
                })
                self._transition_state(WindowState.ACTIVE,
                                       reason="loaded_from_hour_file")
                return loaded

            # 2. Fallback: load from global ledger, filter by hour
            if os.path.exists(self._ledger_path):
                loaded = 0
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
                            if not self._is_current_hour_fill(fill):
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
                active = sum(1 for p in self._positions.values()
                             if p.net_qty > _ZERO)
                print(f"  [TRUTH] Loaded {loaded} fills (filtered to hour "
                      f"{self._hour_suffix}) from global ledger "
                      f"-> {active} active positions")
                self._write_jsonl({
                    "event_type": "TRUTH_LOADED",
                    "fills": loaded,
                    "positions": active,
                    "hour": self._hour_suffix,
                    "source": "global_ledger_filtered",
                    "ts_ms": _ts_ms(),
                })
                self._transition_state(WindowState.ACTIVE,
                                       reason="loaded_from_global_filtered")
                return loaded

        print(f"  [TRUTH] No ledger files found, starting fresh for hour "
              f"{self._hour_suffix}")
        self._transition_state(WindowState.ACTIVE, reason="fresh_start")
        return 0

    def _is_current_hour_fill(self, fill: TruthFill) -> bool:
        """Return True if this fill belongs to the current hour window.

        Uses BOTH timestamp check AND slug check when active_hour_slugs is set.
        """
        # Timestamp check: hour_start <= fill.ts < hour_end
        if fill.ts_ms > 0:
            fill_ts = fill.ts_ms / 1000.0
            hour_start_ts = self._hour_start.timestamp()
            hour_end_ts = self._hour_end.timestamp()
            if not (hour_start_ts <= fill_ts < hour_end_ts):
                return False
        elif fill.ts_iso:
            try:
                fill_dt = datetime.fromisoformat(
                    fill.ts_iso.replace("Z", "+00:00"))
                if not (self._hour_start <= fill_dt < self._hour_end):
                    return False
            except (ValueError, TypeError):
                return False
        else:
            return False  # no timestamp = cannot verify

        # Slug check (if active slugs are registered)
        if self._active_hour_slugs and fill.slug:
            if fill.slug not in self._active_hour_slugs:
                return False

        return True

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
        # Write to BOTH per-hour file AND global ledger (append-only)
        for path in [self._current_hour_path(), self._ledger_path]:
            try:
                with open(path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(row, separators=(",", ":")) + "\n")
            except Exception as e:
                print(f"  [TRUTH] ERROR writing {path}: {e}")

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
            if slug:
                self._active_hour_slugs.add(slug)

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
    #  ACTIVE WINDOW FILTER
    # ══════════════════════════════════════════════════════════════════

    def set_active_window(self, token_ids: Set[str],
                          active_only: bool = True,
                          strict: bool = True) -> None:
        """Set which token_ids belong to the current active 1-hour market.

        Parameters
        ----------
        token_ids : set of str
            Token IDs for the current hour's markets (Up + Down for each slug).
        active_only : bool
            When True, trading logic only sees active-window positions.
        strict : bool
            When True, block ALL trades on non-active tokens (buys AND sells).
        """
        with self._lock:
            self._active_window_tokens = set(token_ids)
            self._active_window_only = active_only
            self._strict_window_mode = strict

    def is_active_window_token(self, token_id: str) -> bool:
        """Return True if token_id belongs to the current active hour window."""
        if not self._active_window_only:
            return True
        if not self._active_window_tokens:
            return True  # no window set yet — allow everything
        return str(token_id) in self._active_window_tokens

    def check_trade_allowed(self, token_id: str, action: str,
                            slug: str = "") -> Tuple[bool, str]:
        """Check if a trade on this token_id is allowed by the window filter.

        Returns (allowed, reason).
        """
        if not self._active_window_only or not self._strict_window_mode:
            return True, "ok"
        if not self._active_window_tokens:
            return True, "ok"  # window not set yet
        if str(token_id) in self._active_window_tokens:
            return True, "ok"
        # Non-active window token
        slug_display = slug or token_id[-12:]
        reason = (f"NON_ACTIVE_WINDOW: {action} blocked for {slug_display} — "
                  f"token not in current hour window")
        self._write_jsonl({
            "event_type": "WINDOW_TRADE_BLOCKED",
            "token_id": token_id, "slug": slug, "action": action,
            "reason": "non_active_window",
            "ts_ms": _ts_ms(),
        })
        print(f"  [TRUTH] WARN: Attempt to trade non-active window token: "
              f"{slug_display} {action}")
        return False, reason

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
                self.fills_dedup_skipped += 1
                self._write_jsonl({
                    "event_type": "DUP_SKIP",
                    "key": dk[:80],
                    "reason": "already_seen",
                    "ts_ms": _ts_ms(),
                })
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
        """Incremental wallet scan using persistent cursor.

        Algorithm:
        1. Fetch newest page of trades from API.
        2. Walk trades newest-first.  Keep only those where:
           a) ts_ms > _scan_cursor_ts_ms, OR
           b) ts_ms == _scan_cursor_ts_ms AND trade_id not in recent_tids.
        3. Stop paging once we hit ts_ms <= _scan_cursor_ts_ms (all older).
        4. After ingest, update cursor:
           _scan_cursor_ts_ms = max(ts_ms of ingested trades)
           _scan_recent_tids  = ring buffer of trade_ids at max ts_ms.
        5. Hour window filter still applied: reject fills outside current hour.

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

        new_count = 0
        skipped_old_cursor = 0
        skipped_old_hour = 0
        skipped_no_identity = 0
        hour_start_epoch = self._hour_start.timestamp()
        hour_end_epoch = self._hour_end.timestamp()
        cursor_ts = self._scan_cursor_ts_ms
        recent_tids_set = set(self._scan_recent_tids)

        # Track max ts_ms among ingested fills for cursor update
        max_ingested_ts_ms = cursor_ts
        new_recent_tids: List[str] = []

        for raw in raw_trades:
            trade_id = str(raw.get("trade_id") or raw.get("id") or "")
            if not trade_id:
                continue

            # ── Parse trade timestamp (ms) ──
            fill_ts_ms = 0
            fill_ts_str = (raw.get("match_time") or raw.get("created_at")
                           or raw.get("timestamp") or "")
            if fill_ts_str:
                try:
                    fill_dt = datetime.fromisoformat(
                        str(fill_ts_str).replace("Z", "+00:00"))
                    fill_ts_ms = int(fill_dt.timestamp() * 1000)
                except (ValueError, TypeError):
                    pass

            # ── Cursor filter: skip already-processed trades ──
            # Predicate: REJECT if ts_ms < cursor (old fill).
            if fill_ts_ms > 0 and cursor_ts > 0:
                if fill_ts_ms < cursor_ts:
                    skipped_old_cursor += 1
                    self.cursor_dropped_old_ts += 1
                    self.cursor_hour_dropped_old += 1
                    continue
                if fill_ts_ms == cursor_ts and trade_id in recent_tids_set:
                    skipped_old_cursor += 1
                    self.cursor_dropped_old_ts += 1
                    self.cursor_hour_dropped_old += 1
                    continue

            # Skip already-seen trades (in-memory dedup)
            with self._lock:
                dk = f"tid:{trade_id}"
                if dk in self._seen_ids:
                    continue

            token_id = str(raw.get("token_id") or raw.get("asset_id") or "")
            action = (raw.get("side") or raw.get("action") or "").upper()
            if action not in ("BUY", "SELL"):
                continue

            slug, outcome = self._resolve_meta(token_id)
            if not slug:
                slug = raw.get("slug", "")
            if not outcome:
                outcome = raw.get("outcome", "")

            # ── HOUR WINDOW FILTER (strict) ──
            fill_epoch = fill_ts_ms / 1000.0 if fill_ts_ms > 0 else 0.0

            # Slug filter
            slug_ok = True
            if self._active_hour_slugs and slug:
                if slug not in self._active_hour_slugs:
                    slug_ok = False

            # Timestamp filter
            ts_ok = True
            if fill_epoch > 0:
                if not (hour_start_epoch <= fill_epoch < hour_end_epoch):
                    ts_ok = False

            if not slug_ok or not ts_ok:
                skipped_old_hour += 1
                self.cursor_dropped_outside_window += 1
                self.cursor_hour_dropped_window += 1
                continue
            if not slug and fill_epoch == 0:
                skipped_no_identity += 1
                continue

            qty = raw.get("size") or raw.get("amount") or 0
            price = raw.get("price") or 0
            order_id = str(raw.get("order_id") or "")

            # ── ACCEPTANCE: fill passed BOTH cursor gate AND hour-window gate ──
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
                self.cursor_accepted_fills += 1
                self.cursor_hour_accepted += 1

            # ── Update cursor tracking ──
            if fill_ts_ms > 0:
                if fill_ts_ms > max_ingested_ts_ms:
                    max_ingested_ts_ms = fill_ts_ms
                    new_recent_tids = [trade_id]
                elif fill_ts_ms == max_ingested_ts_ms:
                    new_recent_tids.append(trade_id)

        # ── Persist cursor ──
        if max_ingested_ts_ms > cursor_ts:
            self._scan_cursor_ts_ms = max_ingested_ts_ms
            self._scan_cursor_tid = new_recent_tids[-1] if new_recent_tids else ""
            self._scan_recent_tids = new_recent_tids[-self._scan_recent_tids_max:]
        elif max_ingested_ts_ms == cursor_ts and new_recent_tids:
            # Same timestamp — extend ring buffer
            combined = list(self._scan_recent_tids) + new_recent_tids
            self._scan_recent_tids = combined[-self._scan_recent_tids_max:]

        self._write_jsonl({
            "event_type": "TRUTH_SCAN_DONE",
            "total_api_trades": len(raw_trades),
            "new_fills": new_count,
            "skipped_old_cursor": skipped_old_cursor,
            "skipped_old_hour": skipped_old_hour,
            "skipped_no_identity": skipped_no_identity,
            "cursor_ts_ms": self._scan_cursor_ts_ms,
            # Acceptance predicate (explicit AND — not OR):
            #   ACCEPT iff (ts_ms >= cursor_ts_ms) AND (hour_start <= ts < hour_end)
            #   Both conditions must be true; either failure → drop.
            "acceptance_predicate": "(ts_ms >= cursor_ts_ms) AND (hour_start <= ts < hour_end)",
            "cumulative_accepted": self.cursor_accepted_fills,
            "cumulative_dropped_old_ts": self.cursor_dropped_old_ts,
            "cumulative_dropped_outside_window": self.cursor_dropped_outside_window,
            "ts_ms": _ts_ms(),
        })
        if new_count > 0 or skipped_old_cursor > 0 or skipped_old_hour > 0:
            print(f"  [TRUTH] Wallet scan: {new_count} new fills from "
                  f"{len(raw_trades)} API trades  "
                  f"(cursor_skip={skipped_old_cursor} "
                  f"hour_skip={skipped_old_hour} "
                  f"no_id={skipped_no_identity})  "
                  f"[cum: accept={self.cursor_accepted_fills} "
                  f"drop_old={self.cursor_dropped_old_ts} "
                  f"drop_win={self.cursor_dropped_outside_window}]")

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

    def recompute_positions_from_ledger(self,
                                       active_only: bool = False,
                                       ) -> Dict[str, TruthPosition]:
        """Public: recompute positions from all fills.

        Parameters
        ----------
        active_only : bool
            When True, only return positions for tokens in the active window.
            When False, return wallet-wide positions (for audit).
            In BOTH cases, the full internal positions dict is recomputed
            from ALL fills (the ledger is never filtered).

        Returns {token_id: TruthPosition}.
        """
        with self._lock:
            self._recompute_positions()
            if active_only and self._active_window_only and self._active_window_tokens:
                return {k: v for k, v in self._positions.items()
                        if k in self._active_window_tokens}
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
        """Return all positions with net_qty > 0 (wallet-wide, unfiltered)."""
        with self._lock:
            return {k: v for k, v in self._positions.items()
                    if v.net_qty > _ZERO}

    def get_active_window_positions(self) -> Dict[str, TruthPosition]:
        """Return only positions for tokens in the current active hour window."""
        with self._lock:
            if not self._active_window_only or not self._active_window_tokens:
                return {k: v for k, v in self._positions.items()
                        if v.net_qty > _ZERO}
            return {k: v for k, v in self._positions.items()
                    if v.net_qty > _ZERO and k in self._active_window_tokens}

    def get_other_positions(self) -> Dict[str, TruthPosition]:
        """Return positions NOT in the current active window (non-tradable)."""
        with self._lock:
            if not self._active_window_only or not self._active_window_tokens:
                return {}
            return {k: v for k, v in self._positions.items()
                    if v.net_qty > _ZERO and k not in self._active_window_tokens}

    def get_sellable_qty(self, token_id: str) -> Decimal:
        return max(self.get_position(token_id).net_qty, _ZERO)

    def validate_sell(self, token_id: str, requested_qty: float,
                      slug: str = "") -> Tuple[bool, float, str]:
        """Validate sell against truth positions.

        When active_window_only + strict_window_mode are set, sells on
        non-active-window tokens are BLOCKED.

        Returns (allowed, capped_qty, reason).
        """
        display = slug or token_id[-12:]

        # Active window enforcement: block sells on old-hour tokens
        if self._active_window_only and self._strict_window_mode:
            if self._active_window_tokens and str(token_id) not in self._active_window_tokens:
                msg = (f"SELL BLOCKED: {display} not in active window "
                       f"(old-hour position, non-tradable)")
                print(f"  [TRUTH] {msg}")
                self._write_jsonl({
                    "event_type": "WINDOW_SELL_BLOCKED",
                    "token_id": token_id, "slug": slug,
                    "reason": "non_active_window",
                    "ts_ms": _ts_ms(),
                })
                return False, 0.0, msg

        req = _to_dec(requested_qty)
        owned = self.get_sellable_qty(token_id)

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
        active_diffs = []
        non_active_diffs = []
        tol = self._desync_tolerance
        crit_tol = tol * 10

        for tid in all_tids:
            truth_qty = float(self.get_position(tid).net_qty)
            wallet_qty = wallet.get(tid, 0.0)
            diff = wallet_qty - truth_qty
            abs_diff = abs(diff)
            is_active = self.is_active_window_token(tid)

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
                    "active_window": is_active,
                }
                mismatches.append(entry)
                if is_active:
                    active_diffs.append(entry)
                else:
                    non_active_diffs.append(entry)
                tag = "ACTIVE" if is_active else "NON_ACTIVE"
                print(f"  [TRUTH] RECONCILE {sev} [{tag}]: {slug} {outcome}: "
                      f"ledger={truth_qty:.4f} wallet={wallet_qty:.4f} "
                      f"diff={diff:+.4f}")

        # Print reconciliation diff tables
        if active_diffs:
            print(f"  [RECON] ACTIVE_WINDOW_DIFF ({len(active_diffs)}):")
            for d in active_diffs:
                print(f"  [RECON]   {d['slug']:<30s} "
                      f"wallet={d['wallet']:.4f}  ledger={d['truth']:.4f}  "
                      f"diff={d['diff']:+.4f}")
        if non_active_diffs:
            print(f"  [RECON] NON_ACTIVE_DIFF ({len(non_active_diffs)}):")
            for d in non_active_diffs:
                print(f"  [RECON]   {d['slug']:<30s} "
                      f"wallet={d['wallet']:.4f}  ledger={d['truth']:.4f}  "
                      f"diff={d['diff']:+.4f}")
        recon_status = "OK" if not mismatches else "FAIL"
        max_delta = max((abs(m["diff"]) for m in mismatches), default=0.0)
        print(f"  [RECON] status={recon_status} delta_max={max_delta:.4f}")

        result["mismatches"] = mismatches
        result["active_diffs"] = active_diffs
        result["non_active_diffs"] = non_active_diffs
        critical = any(m["severity"] == "CRITICAL" and m["active_window"]
                       for m in mismatches)
        result["critical"] = critical

        self._write_jsonl({
            "event_type": "TRUTH_RECONCILE",
            "matches": len(result["matches"]),
            "mismatches": len(mismatches),
            "active_diffs": len(active_diffs),
            "non_active_diffs": len(non_active_diffs),
            "critical": critical,
            "skipped": result["skipped"],
            "max_delta": round(max_delta, 6),
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

    def tick(self, active_token_ids: Optional[Set[str]] = None,
             position_log_interval: float = 600.0) -> None:
        """Run all periodic tasks: poll watchers, wallet scan, reconcile,
        and periodic position print + structured logging.

        Parameters
        ----------
        position_log_interval : float
            Seconds between position prints (default 600 = 10 min).
        """
        now_ts = time.time()

        # State machine guard: skip most work if CLOSED
        if self._window_state == WindowState.CLOSED:
            return

        # 1. Poll order watchers (fast — every tick)
        new_fills = self.poll_watchers()

        # 2. Wallet truth scan — DISABLED: positions tracked via verified fills
        #    The external scan iterated 4600+ old API trades every cycle,
        #    causing duplicate fills and confusing position tracking.
        # self.maybe_run_wallet_scan()

        # 3. Reconciliation (every reconcile_interval_sec)
        self.maybe_run_reconciliation(active_token_ids)

        # 4. Position print at configurable interval (default 10 min)
        if now_ts - self._last_positions_print_ts >= position_log_interval:
            self._last_positions_print_ts = now_ts
            self.print_positions()

        # 5. Enhanced per-loop structured log (every 30s)
        if now_ts - self._last_loop_log_ts >= 30.0:
            self._last_loop_log_ts = now_ts
            active = self.get_active_window_positions()
            other = self.get_other_positions()
            active_slugs = sorted(set(
                p.slug for p in active.values()))
            pos_summary = ", ".join(
                f"{p.slug[-15:]} {p.outcome}:{float(p.net_qty):.0f}"
                for p in sorted(active.values(), key=lambda x: x.slug)
            ) if active else "none"
            self._write_jsonl({
                "event_type": "TRUTH_LOOP",
                "state": self._window_state.value,
                "hour": self._hour_suffix,
                "hour_start": self._hour_start.isoformat(),
                "hour_end": self._hour_end.isoformat(),
                "active_slugs": active_slugs,
                "fills_total": len(self._fills),
                "fills_ws": self.fills_from_ws,
                "fills_poll": self.fills_from_poll,
                "fills_scan": self.fills_from_scan,
                "dedup_skipped": self.fills_dedup_skipped,
                "active_positions": len(active),
                "other_positions": len(other),
                "watchers": len(self._watchers),
                "safe_mode": self._safe_mode,
                "ts_ms": _ts_ms(),
            })
            print(f"  [TRUTH] [LOOP] state={self._window_state.value}  "
                  f"hour={self._hour_suffix}  "
                  f"fills={len(self._fills)} "
                  f"(ws={self.fills_from_ws} poll={self.fills_from_poll} "
                  f"scan={self.fills_from_scan} dup={self.fills_dedup_skipped})  "
                  f"active_pos={len(active)} other={len(other)}  "
                  f"watchers={len(self._watchers)}")
            # Position details printed by bot's print_positions() with crypto names

    # ══════════════════════════════════════════════════════════════════
    #  REPORTING
    # ══════════════════════════════════════════════════════════════════

    def print_positions(self, max_display: int = 10) -> None:
        """Print truth-derived positions for current hour only (top N by cost).

        Parameters
        ----------
        max_display : int
            Maximum positions to display (default 10).
        """
        window_pos = self.get_active_window_positions()

        if not window_pos:
            safe_tag = " [SAFE MODE]" if self._safe_mode else ""
            print(f"  [TRUTH] ── LEDGER_POSITIONS — current hour: (none){safe_tag} ──")
            return

        safe_tag = " [SAFE MODE]" if self._safe_mode else ""

        # Sort by cost descending, show top N
        sorted_pos = sorted(window_pos.items(),
                            key=lambda x: float(x[1].total_cost),
                            reverse=True)
        shown = sorted_pos[:max_display]
        hidden = len(sorted_pos) - len(shown)

        print(f"  [TRUTH] ── LEDGER_POSITIONS — current hour "
              f"({len(window_pos)} positions, top {min(max_display, len(sorted_pos))})"
              f"{safe_tag} ──")
        total_cost = _ZERO
        total_rpnl = _ZERO
        for tid, pos in shown:
            cost = float(pos.total_cost)
            total_cost += pos.total_cost
            total_rpnl += pos.realized_pnl
            print(f"  [TRUTH]   {pos.slug:<35s} {pos.outcome:<5s} "
                  f"qty={float(pos.net_qty):8.2f}  "
                  f"avg={float(pos.avg_price):.4f}  "
                  f"cost=${cost:7.2f}  "
                  f"rpnl={float(pos.realized_pnl):+.4f}")
        # Add hidden positions to totals
        for tid, pos in sorted_pos[max_display:]:
            total_cost += pos.total_cost
            total_rpnl += pos.realized_pnl
        if hidden > 0:
            print(f"  [TRUTH]   ... and {hidden} more positions")
        print(f"  [TRUTH]   Total cost=${float(total_cost):.2f}  "
              f"rpnl={float(total_rpnl):+.4f}  "
              f"fills in current hour={len(self._fills)} "
              f"(ws={self.fills_from_ws} poll={self.fills_from_poll} "
              f"scan={self.fills_from_scan})")

    def summary(self) -> dict:
        active = self.get_all_active()
        total_rpnl = sum((p.realized_pnl for p in self._positions.values()),
                         _ZERO)
        total_cost = sum((p.total_cost for p in active.values()), _ZERO)
        return {
            "fills_in_current_hour": len(self._fills),
            "total_fills": len(self._fills),  # backward compat alias
            "active_positions": len(active),
            "total_realized_pnl": float(total_rpnl),
            "total_cost_basis": float(total_cost),
            "hour": self._hour_suffix,
            "safe_mode": self._safe_mode,
            "fills_ws": self.fills_from_ws,
            "fills_poll": self.fills_from_poll,
            "fills_scan": self.fills_from_scan,
            "watchers_active": len(self._watchers),
            "orders_watched": self.orders_watched,
            "reconcile_runs": self.reconcile_runs,
            "desync_count": self.desync_count,
        }
