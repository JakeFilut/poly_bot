"""Fills ledger — append-only CONFIRMED fill accounting with position derivation.

The ledger is the SOURCE OF TRUTH for inventory.  Positions are derived
ONLY from confirmed fills, never from order intents or internal guesses.

Provides:
  - Append-only JSONL ledger of every CONFIRMED BUY/SELL fill
  - Position calculator: net_qty, avg_entry_price (WAC), realized_pnl
  - Reconciliation against wallet balances + open orders with SAFE_MODE
  - Sell validation: blocks over-selling, caps to owned qty
  - Fetch + de-dup of recent fills from Polymarket API

Precision: Decimal internally, string serialization in JSONL.
Cost method: weighted-average cost (WAC) for avg_entry_price.
"""
from __future__ import annotations

import json
import os
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_ZERO = Decimal("0")
_PRECISION = Decimal("0.000000001")  # 9 dp for storage
_DUST = Decimal("0.001")             # below this = dust (zero out)
_EPSILON = Decimal("0.000001")       # rounding tolerance for sell cap


def _to_dec(v: Any) -> Decimal:
    """Safely convert a value to Decimal."""
    if isinstance(v, Decimal):
        return v
    if v is None:
        return _ZERO
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return _ZERO


def _dec_str(d: Decimal) -> str:
    """Serialize Decimal to string with full precision."""
    return str(d.quantize(_PRECISION))


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class FillRecord:
    """One CONFIRMED fill in the append-only ledger."""
    timestamp: str
    slug: str                    # e.g. "BTC_2025_02_20_14"
    crypto: str                  # e.g. "BTC"
    token_id: str                # on-chain token ID (stable identifier)
    market_id: str               # condition_id or market_id
    side: str                    # "Up" or "Down"
    action: str                  # "BUY" or "SELL"
    fill_qty: Decimal
    fill_price: Decimal
    fees: Decimal
    order_id: str
    trade_id: str                # tx_hash or trade_id for dedup
    source: str                  # "bot" or "polymarket"
    run_id: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PositionInfo:
    """Derived position state for one (token_id, side)."""
    slug: str
    crypto: str
    token_id: str
    side: str
    net_qty: Decimal = _ZERO
    avg_entry_price: Decimal = _ZERO
    total_cost: Decimal = _ZERO        # WAC cost basis of remaining inventory
    total_buy_qty: Decimal = _ZERO
    total_sell_qty: Decimal = _ZERO
    realized_pnl: Decimal = _ZERO
    unrealized_pnl: Decimal = _ZERO    # updated externally with current price
    fill_count: int = 0


@dataclass
class OrderIntent:
    """Logged when an order is SUBMITTED (before fill confirmation)."""
    timestamp: str
    slug: str
    token_id: str
    side: str
    action: str
    requested_qty: Decimal
    requested_price: Decimal
    reason: str
    order_id: str


# ---------------------------------------------------------------------------
# FillsLedger
# ---------------------------------------------------------------------------
class FillsLedger:
    """Append-only CONFIRMED fills ledger with position calculator.

    IMPORTANT: record_fill() must ONLY be called after the exchange confirms
    an actual execution.  Use record_order_intent() at submit time.

    Thread-safe: all mutations go through a lock.
    """

    def __init__(
        self,
        ledger_path: str = "./fills_ledger.jsonl",
        run_id: str = "",
        write_jsonl_fn: Optional[Callable[[dict], None]] = None,
    ):
        self._path = ledger_path
        self._run_id = run_id
        self._write_jsonl = write_jsonl_fn or (lambda d: None)
        self._lock = threading.Lock()

        # ── Hour window tracking ──
        now = datetime.now(timezone.utc)
        self._hour_start = now.replace(minute=0, second=0, microsecond=0)
        self._hour_end = self._hour_start + timedelta(hours=1)
        self._hour_suffix = self._hour_start.strftime("%Y%m%d_%H")
        self._base_dir = os.path.dirname(os.path.abspath(ledger_path))
        self._base_name = os.path.splitext(os.path.basename(ledger_path))[0]

        # All fills in memory (CURRENT HOUR ONLY after rotation)
        self._fills: List[FillRecord] = []

        # Dedup set: dedup_key -> True
        self._seen_ids: Set[str] = set()
        self.fills_dedup_skipped: int = 0

        # Derived positions: (token_id, side) -> PositionInfo
        self._positions: Dict[Tuple[str, str], PositionInfo] = {}

        # Slug/token mapping: token_id -> (slug, crypto, side)
        self._token_meta: Dict[str, Tuple[str, str, str]] = {}

        # Active hour slugs
        self._active_hour_slugs: Set[str] = set()

        # SAFE MODE: no new buys when True (sells that reduce exposure still ok)
        self._safe_mode: bool = False
        self._safe_mode_reason: str = ""
        self._safe_mode_details: List[dict] = []

        # Active window filter: set of token_ids in current hour
        self._active_window_tokens: Set[str] = set()
        self._active_window_only: bool = False
        self._strict_window_mode: bool = False

        # Order intent log (in-memory ring buffer, not persisted to ledger)
        self._order_intents: List[OrderIntent] = []

        # Ensure ledger directory exists
        ledger_dir = os.path.dirname(os.path.abspath(self._path))
        if ledger_dir:
            os.makedirs(ledger_dir, exist_ok=True)

    # ══════════════════════════════════════════════════════════════════
    #  DISK I/O
    # ══════════════════════════════════════════════════════════════════

    def load_from_disk(self) -> int:
        """Load fills for the CURRENT HOUR.

        Tries per-hour file first, falls back to global ledger filtered
        by timestamp.  Returns count loaded.
        """
        hour_path = self._current_hour_path()

        with self._lock:
            # 1. Try per-hour file first
            if os.path.exists(hour_path):
                loaded = self._load_hour_file()
                print(f"  [LEDGER] Loaded {loaded} fills (hour={self._hour_suffix}) "
                      f"-> {len(self._positions)} positions")
                self._write_jsonl({
                    "event_type": "LEDGER_LOADED",
                    "fills_count": loaded,
                    "positions_count": len(self._positions),
                    "hour": self._hour_suffix,
                    "source": "hour_file",
                    "ts_ms": int(time.time() * 1000),
                })
                return loaded

            # 2. Fallback: load from global ledger, filter by hour
            if not os.path.exists(self._path):
                print(f"  [LEDGER] No ledger files found, starting fresh "
                      f"for hour {self._hour_suffix}")
                return 0
            loaded = 0
            with open(self._path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                        fill = self._parse_fill(raw)
                        if fill is None:
                            continue
                        # TIME FILTER: only current hour fills
                        if not self._is_current_hour_fill(fill):
                            continue
                        dedup_key = self._dedup_key(fill)
                        if dedup_key in self._seen_ids:
                            continue
                        self._seen_ids.add(dedup_key)
                        self._fills.append(fill)
                        self._update_token_meta(fill)
                        loaded += 1
                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        print(f"  [LEDGER] WARN: skip line {line_num}: {e}")
            self._recompute_all()

        self._write_jsonl({
            "event_type": "LEDGER_LOADED",
            "fills_count": loaded,
            "positions_count": len(self._positions),
            "hour": self._hour_suffix,
            "source": "global_ledger_filtered",
            "ts_ms": int(time.time() * 1000),
        })
        print(f"  [LEDGER] Loaded {loaded} fills (filtered to hour "
              f"{self._hour_suffix}) -> {len(self._positions)} positions")
        return loaded

    def _parse_raw_fill(self, raw: dict) -> Optional[FillRecord]:
        """Alias for _parse_fill (used by _load_hour_file)."""
        return self._parse_fill(raw)

    def _update_meta_from_fill(self, fill: FillRecord) -> None:
        """Alias for _update_token_meta (used by _load_hour_file)."""
        self._update_token_meta(fill)

    def _append_to_disk(self, fill: FillRecord) -> None:
        """Append a fill to BOTH per-hour file AND global ledger."""
        row = {
            "timestamp": fill.timestamp,
            "slug": fill.slug,
            "crypto": fill.crypto,
            "token_id": fill.token_id,
            "market_id": fill.market_id,
            "side": fill.side,
            "action": fill.action,
            "fill_qty": _dec_str(fill.fill_qty),
            "fill_price": _dec_str(fill.fill_price),
            "fees": _dec_str(fill.fees),
            "order_id": fill.order_id,
            "trade_id": fill.trade_id,
            "source": fill.source,
            "run_id": fill.run_id or self._run_id,
        }
        if fill.extra:
            row["extra"] = fill.extra
        line = json.dumps(row, separators=(",", ":")) + "\n"
        # Write to both per-hour and global ledger (append-only)
        for path in [self._current_hour_path(), self._path]:
            try:
                with open(path, "a", encoding="utf-8") as f:
                    f.write(line)
            except Exception as e:
                print(f"  [LEDGER] ERROR writing to {path}: {e}")

    def _parse_fill(self, raw: dict) -> Optional[FillRecord]:
        """Parse a raw dict from JSONL into a FillRecord."""
        action = raw.get("action", "").upper()
        if action not in ("BUY", "SELL"):
            return None
        return FillRecord(
            timestamp=raw.get("timestamp", ""),
            slug=raw.get("slug", ""),
            crypto=raw.get("crypto", ""),
            token_id=str(raw.get("token_id", "")),
            market_id=str(raw.get("market_id", "")),
            side=raw.get("side", ""),
            action=action,
            fill_qty=_to_dec(raw.get("fill_qty", 0)),
            fill_price=_to_dec(raw.get("fill_price", 0)),
            fees=_to_dec(raw.get("fees", 0)),
            order_id=str(raw.get("order_id", "")),
            trade_id=str(raw.get("trade_id", "")),
            source=raw.get("source", "unknown"),
            run_id=raw.get("run_id", ""),
            extra=raw.get("extra", {}),
        )

    # ══════════════════════════════════════════════════════════════════
    #  DEDUPLICATION
    # ══════════════════════════════════════════════════════════════════

    @staticmethod
    def _dedup_key(fill: FillRecord) -> str:
        """Build a dedup key from trade_id, or fallback to order_id composite."""
        if fill.trade_id:
            return f"tid:{fill.trade_id}"
        # Fallback composite (order_id can appear for both legs of a pair,
        # so include action + qty + price to distinguish)
        return (f"oid:{fill.order_id}|{fill.action}|"
                f"{_dec_str(fill.fill_qty)}|{_dec_str(fill.fill_price)}|"
                f"{fill.timestamp}")

    # ══════════════════════════════════════════════════════════════════
    #  TOKEN METADATA
    # ══════════════════════════════════════════════════════════════════

    def _update_token_meta(self, fill: FillRecord) -> None:
        if fill.token_id:
            self._token_meta[fill.token_id] = (fill.slug, fill.crypto, fill.side)

    def register_token(self, token_id: str, slug: str, crypto: str, side: str) -> None:
        """Register a token_id mapping (called during market discovery)."""
        with self._lock:
            self._token_meta[token_id] = (slug, crypto, side)
            if slug:
                self._active_hour_slugs.add(slug)

    # ══════════════════════════════════════════════════════════════════
    #  ACTIVE WINDOW FILTER
    # ══════════════════════════════════════════════════════════════════

    def set_active_window(self, token_ids: Set[str],
                          active_only: bool = True,
                          strict: bool = True) -> None:
        """Set which token_ids belong to the current active 1-hour market."""
        with self._lock:
            self._active_window_tokens = set(token_ids)
            self._active_window_only = active_only
            self._strict_window_mode = strict

    def is_active_window_token(self, token_id: str) -> bool:
        if not self._active_window_only:
            return True
        if not self._active_window_tokens:
            return True
        return str(token_id) in self._active_window_tokens

    def get_active_window_positions(self) -> Dict[Tuple[str, str], PositionInfo]:
        """Return only positions for tokens in the current active window."""
        with self._lock:
            if not self._active_window_only or not self._active_window_tokens:
                return {k: v for k, v in self._positions.items()
                        if v.net_qty > _ZERO}
            return {k: v for k, v in self._positions.items()
                    if v.net_qty > _ZERO and k[0] in self._active_window_tokens}

    def get_other_positions(self) -> Dict[Tuple[str, str], PositionInfo]:
        """Return positions NOT in the current active window."""
        with self._lock:
            if not self._active_window_only or not self._active_window_tokens:
                return {}
            return {k: v for k, v in self._positions.items()
                    if v.net_qty > _ZERO and k[0] not in self._active_window_tokens}

    # ══════════════════════════════════════════════════════════════════
    #  PER-HOUR ROTATION
    # ══════════════════════════════════════════════════════════════════

    def _current_hour_path(self) -> str:
        return os.path.join(
            self._base_dir,
            f"{self._base_name}_{self._hour_suffix}.jsonl")

    def _is_current_hour_fill(self, fill: FillRecord) -> bool:
        """Return True if fill timestamp is within current hour window."""
        if not fill.timestamp:
            return False
        try:
            fill_dt = datetime.fromisoformat(
                fill.timestamp.replace("Z", "+00:00"))
            return self._hour_start <= fill_dt < self._hour_end
        except (ValueError, TypeError):
            return False

    def rotate_hour(self, new_hour_start: Optional[datetime] = None) -> None:
        """Rotate to a new hour window. Clears in-memory state."""
        with self._lock:
            old_suffix = self._hour_suffix
            old_fills = len(self._fills)

            # Compute new hour
            if new_hour_start is None:
                new_hour_start = datetime.now(timezone.utc).replace(
                    minute=0, second=0, microsecond=0)
            self._hour_start = new_hour_start
            self._hour_end = new_hour_start + timedelta(hours=1)
            self._hour_suffix = new_hour_start.strftime("%Y%m%d_%H")

            # Reset in-memory state
            self._fills.clear()
            self._seen_ids.clear()
            self._positions.clear()
            self._active_hour_slugs.clear()
            self.fills_dedup_skipped = 0

            # Reset SAFE MODE
            self._safe_mode = False
            self._safe_mode_reason = ""
            self._safe_mode_details.clear()

            # Load per-hour file if it exists (restart mid-hour)
            loaded = self._load_hour_file()

        print(f"  [LEDGER] HOUR ROTATION: {old_suffix} -> {self._hour_suffix}  "
              f"cleared={old_fills} fills  loaded={loaded}")
        self._write_jsonl({
            "event_type": "LEDGER_HOUR_ROTATION",
            "old_hour": old_suffix,
            "new_hour": self._hour_suffix,
            "cleared_fills": old_fills,
            "loaded_fills": loaded,
            "ts_ms": int(time.time() * 1000),
        })

    def _load_hour_file(self) -> int:
        """Load fills from current hour's file. Must hold self._lock."""
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
                    fill = self._parse_raw_fill(raw)
                    if fill is None:
                        continue
                    if not self._is_current_hour_fill(fill):
                        continue
                    dk = self._dedup_key(fill)
                    if dk in self._seen_ids:
                        continue
                    self._seen_ids.add(dk)
                    self._fills.append(fill)
                    self._update_meta_from_fill(fill)
                    loaded += 1
                except Exception:
                    pass
        if loaded > 0:
            self._recompute_all()
        return loaded

    # ══════════════════════════════════════════════════════════════════
    #  ORDER INTENT (submit-time, NOT a fill)
    # ══════════════════════════════════════════════════════════════════

    def record_order_intent(
        self,
        slug: str,
        token_id: str,
        side: str,
        action: str,
        requested_qty: float,
        requested_price: float,
        reason: str,
        order_id: str = "",
    ) -> None:
        """Log an order intent at SUBMIT time (before fill confirmation).

        NOT written to the ledger file — only emitted as a JSONL event
        and kept in an in-memory ring buffer.
        """
        intent = OrderIntent(
            timestamp=_utc_iso(),
            slug=slug,
            token_id=str(token_id),
            side=side,
            action=action.upper(),
            requested_qty=_to_dec(requested_qty),
            requested_price=_to_dec(requested_price),
            reason=reason,
            order_id=str(order_id),
        )
        with self._lock:
            self._order_intents.append(intent)
            if len(self._order_intents) > 1000:
                self._order_intents = self._order_intents[-500:]

        self._write_jsonl({
            "event_type": "LEDGER_ORDER_INTENT",
            "slug": slug,
            "token_id": str(token_id),
            "side": side,
            "action": action.upper(),
            "requested_qty": float(requested_qty),
            "requested_price": float(requested_price),
            "reason": reason,
            "order_id": str(order_id),
            "ts_ms": int(time.time() * 1000),
        })

    # ══════════════════════════════════════════════════════════════════
    #  RECORD CONFIRMED FILL  (the ONLY way data enters the ledger)
    # ══════════════════════════════════════════════════════════════════

    def record_fill(
        self,
        slug: str,
        crypto: str,
        token_id: str,
        market_id: str,
        side: str,
        action: str,
        fill_qty: float,
        fill_price: float,
        fees: float = 0.0,
        order_id: str = "",
        trade_id: str = "",
        source: str = "bot",
        extra: Optional[dict] = None,
    ) -> Optional[PositionInfo]:
        """Record a CONFIRMED fill and update positions.

        MUST be called ONLY after the exchange confirms an actual execution
        (status == "matched", transactionsHashes present, etc.).

        Returns updated PositionInfo, or None if duplicate.
        """
        qty_d = _to_dec(fill_qty)
        price_d = _to_dec(fill_price)
        fees_d = _to_dec(fees)

        if qty_d <= _ZERO:
            return None

        fill = FillRecord(
            timestamp=_utc_iso(),
            slug=slug,
            crypto=crypto,
            token_id=str(token_id),
            market_id=str(market_id),
            side=side,
            action=action.upper(),
            fill_qty=qty_d,
            fill_price=price_d,
            fees=fees_d,
            order_id=str(order_id),
            trade_id=str(trade_id),
            source=source,
            run_id=self._run_id,
            extra=extra or {},
        )

        with self._lock:
            dedup_key = self._dedup_key(fill)
            if dedup_key in self._seen_ids:
                return None  # duplicate — already recorded

            self._seen_ids.add(dedup_key)
            self._fills.append(fill)
            self._update_token_meta(fill)
            self._append_to_disk(fill)

            # Incremental position update
            pos = self._update_position_incremental(fill)

        # Log
        self._write_jsonl({
            "event_type": "LEDGER_FILL_RECORDED",
            "slug": slug, "crypto": crypto,
            "token_id": token_id, "side": side,
            "action": action.upper(),
            "fill_qty": float(qty_d),
            "fill_price": float(price_d),
            "fees": float(fees_d),
            "order_id": order_id, "trade_id": trade_id,
            "source": source,
            "net_qty": float(pos.net_qty),
            "avg_entry_price": float(pos.avg_entry_price),
            "realized_pnl": float(pos.realized_pnl),
            "ts_ms": int(time.time() * 1000),
        })

        print(f"  [LEDGER] FILL {action.upper()} {slug} {side}: "
              f"qty={float(qty_d):.6f} @ {float(price_d):.4f}  →  "
              f"net_qty={float(pos.net_qty):.6f}  "
              f"avg_price={float(pos.avg_entry_price):.4f}  "
              f"realized_pnl={float(pos.realized_pnl):.4f}")

        return pos

    # ══════════════════════════════════════════════════════════════════
    #  POSITION CALCULATION (WAC)
    # ══════════════════════════════════════════════════════════════════

    def _update_position_incremental(self, fill: FillRecord) -> PositionInfo:
        """Update one position from a new fill.  Must hold self._lock."""
        key = (fill.token_id, fill.side)

        if key not in self._positions:
            self._positions[key] = PositionInfo(
                slug=fill.slug, crypto=fill.crypto,
                token_id=fill.token_id, side=fill.side,
            )

        pos = self._positions[key]
        pos.slug = fill.slug  # track latest slug (hour rotation)

        if fill.action == "BUY":
            new_cost = fill.fill_qty * fill.fill_price
            old_total_cost = pos.avg_entry_price * pos.net_qty
            new_qty = pos.net_qty + fill.fill_qty
            if new_qty > _ZERO:
                pos.avg_entry_price = (old_total_cost + new_cost) / new_qty
            pos.net_qty = new_qty
            pos.total_cost = pos.avg_entry_price * pos.net_qty
            pos.total_buy_qty += fill.fill_qty

        elif fill.action == "SELL":
            sell_qty = min(fill.fill_qty, pos.net_qty)
            if sell_qty > _ZERO:
                pnl = (fill.fill_price - pos.avg_entry_price) * sell_qty
                pos.realized_pnl += pnl - fill.fees
                pos.net_qty -= sell_qty
                pos.total_cost = pos.avg_entry_price * pos.net_qty
            pos.total_sell_qty += fill.fill_qty

        # Dust cleanup
        if pos.net_qty < _DUST:
            pos.net_qty = _ZERO
            pos.total_cost = _ZERO
            pos.avg_entry_price = _ZERO

        pos.fill_count += 1
        return pos

    def _recompute_all(self) -> None:
        """Recompute ALL positions from the full fills list.  Must hold self._lock."""
        self._positions.clear()
        for fill in self._fills:
            self._update_position_incremental(fill)

    def recompute_positions_from_ledger(self,
                                       active_only: bool = False,
                                       ) -> Dict[Tuple[str, str], PositionInfo]:
        """Public: full recompute.  Returns positions dict.

        Parameters
        ----------
        active_only : bool
            When True, only return positions for tokens in the active window.
            When False, return wallet-wide positions (for audit).
            The full internal positions dict is always recomputed from ALL fills.
        """
        with self._lock:
            self._recompute_all()
            if active_only and self._active_window_only and self._active_window_tokens:
                return {k: v for k, v in self._positions.items()
                        if k[0] in self._active_window_tokens}
            return dict(self._positions)

    # ══════════════════════════════════════════════════════════════════
    #  POSITION QUERIES
    # ══════════════════════════════════════════════════════════════════

    def get_position(self, token_id: str, side: str) -> PositionInfo:
        """Get position for (token_id, side).  Returns zero if none."""
        with self._lock:
            key = (str(token_id), side)
            if key in self._positions:
                return self._positions[key]
            meta = self._token_meta.get(str(token_id), ("", "", side))
            return PositionInfo(
                slug=meta[0], crypto=meta[1],
                token_id=str(token_id), side=side,
            )

    def get_position_by_slug(self, slug: str, side: str) -> Optional[PositionInfo]:
        """Get position by slug + side (scans; O(n) but n is small)."""
        with self._lock:
            for _, pos in self._positions.items():
                if pos.slug == slug and pos.side == side:
                    return pos
        return None

    def get_sellable_qty(self, token_id: str, side: str) -> Decimal:
        """Net qty available to sell for (token_id, side)."""
        pos = self.get_position(token_id, side)
        return max(pos.net_qty, _ZERO)

    def get_sellable_qty_by_slug(self, slug: str, side: str) -> Decimal:
        """Net qty available to sell by slug + side."""
        pos = self.get_position_by_slug(slug, side)
        if pos is None:
            return _ZERO
        return max(pos.net_qty, _ZERO)

    def get_all_positions(self) -> Dict[Tuple[str, str], PositionInfo]:
        with self._lock:
            return dict(self._positions)

    def get_all_active_positions(self) -> Dict[Tuple[str, str], PositionInfo]:
        with self._lock:
            return {k: v for k, v in self._positions.items() if v.net_qty > _ZERO}

    def get_total_realized_pnl(self) -> Decimal:
        with self._lock:
            return sum((p.realized_pnl for p in self._positions.values()), _ZERO)

    # ══════════════════════════════════════════════════════════════════
    #  UNREALIZED PNL
    # ══════════════════════════════════════════════════════════════════

    def update_unrealized_pnl(
        self,
        price_lookup: Dict[Tuple[str, str], float],
    ) -> None:
        """Update unrealized PnL using current mid prices."""
        with self._lock:
            for key, pos in self._positions.items():
                if pos.net_qty <= _ZERO:
                    pos.unrealized_pnl = _ZERO
                    continue
                current = price_lookup.get(key)
                if current is not None:
                    pos.unrealized_pnl = (
                        (_to_dec(current) - pos.avg_entry_price) * pos.net_qty
                    )

    # ══════════════════════════════════════════════════════════════════
    #  SELL VALIDATION
    # ══════════════════════════════════════════════════════════════════

    def validate_sell(
        self, token_id: str, side: str, requested_qty: float, slug: str = ""
    ) -> Tuple[bool, float, str]:
        """Validate a sell against ledger-derived position.

        Returns (allowed, capped_qty, reason).
          - block if token not in active window (when strict mode)
          - block if owned <= 0
          - cap qty to owned if within tiny epsilon of owned
          - log: attempting X, owned Y, delta Z
        """
        display = slug or token_id

        # Active window enforcement: block sells on old-hour tokens
        if self._active_window_only and self._strict_window_mode:
            if self._active_window_tokens and str(token_id) not in self._active_window_tokens:
                msg = (f"SELL BLOCKED: {display} {side} not in active window "
                       f"(old-hour position, non-tradable)")
                print(f"  [LEDGER] {msg}")
                self._write_jsonl({
                    "event_type": "WINDOW_SELL_BLOCKED",
                    "token_id": token_id, "slug": slug, "side": side,
                    "reason": "non_active_window",
                    "ts_ms": int(time.time() * 1000),
                })
                return False, 0.0, msg

        req = _to_dec(requested_qty)
        owned = self.get_sellable_qty(token_id, side)
        delta = req - owned

        if owned <= _ZERO:
            msg = (f"SELL BLOCKED: no position for {display} {side} "
                   f"(ledger net_qty=0)")
            print(f"  [LEDGER] {msg}")
            self._write_jsonl({
                "event_type": "LEDGER_SELL_BLOCKED",
                "slug": slug, "token_id": token_id, "side": side,
                "requested_qty": float(req), "owned_qty": 0.0,
                "reason": "no_position",
                "ts_ms": int(time.time() * 1000),
            })
            return False, 0.0, msg

        if req > owned:
            # If within epsilon, snap to owned (float rounding)
            if delta <= _EPSILON:
                capped = float(owned)
                return True, capped, "snapped to owned (epsilon)"
            # Otherwise, hard cap to owned and warn
            capped = float(owned)
            msg = (f"SELL CAPPED: {display} {side}: "
                   f"requested={float(req):.6f} > owned={float(owned):.6f}, "
                   f"capping to {capped:.6f}")
            print(f"  [LEDGER] {msg}")
            self._write_jsonl({
                "event_type": "LEDGER_SELL_CAPPED",
                "slug": slug, "token_id": token_id, "side": side,
                "requested_qty": float(req), "owned_qty": float(owned),
                "capped_qty": capped,
                "ts_ms": int(time.time() * 1000),
            })
            return True, capped, msg

        return True, float(req), "ok"

    # ══════════════════════════════════════════════════════════════════
    #  SAFE MODE
    # ══════════════════════════════════════════════════════════════════

    @property
    def safe_mode(self) -> bool:
        return self._safe_mode

    @property
    def safe_mode_reason(self) -> str:
        return self._safe_mode_reason

    def enter_safe_mode(self, reason: str, details: Optional[List[dict]] = None) -> None:
        """Enter SAFE MODE — no new BUYS until resolved.
        Sells that reduce exposure + pass validate_sell are still allowed."""
        self._safe_mode = True
        self._safe_mode_reason = reason
        self._safe_mode_details = details or []
        self._write_jsonl({
            "event_type": "LEDGER_SAFE_MODE_ENTER",
            "reason": reason,
            "details": self._safe_mode_details[:10],
            "ts_ms": int(time.time() * 1000),
        })
        print(f"  [LEDGER] *** SAFE MODE ACTIVATED: {reason} ***")
        print(f"  [LEDGER]     No new buys until mismatch resolved.")
        print(f"  [LEDGER]     Sells that reduce exposure still allowed.")

    def exit_safe_mode(self) -> None:
        if self._safe_mode:
            self._safe_mode = False
            self._safe_mode_reason = ""
            self._safe_mode_details = []
            self._write_jsonl({
                "event_type": "LEDGER_SAFE_MODE_EXIT",
                "ts_ms": int(time.time() * 1000),
            })
            print("  [LEDGER] SAFE MODE resolved — trading allowed")

    # ══════════════════════════════════════════════════════════════════
    #  RECONCILIATION  (ledger vs wallet + open orders)
    # ══════════════════════════════════════════════════════════════════

    def reconcile_positions(
        self,
        wallet_balances: Dict[Tuple[str, str], float],
        open_orders: Optional[List[dict]] = None,
        tolerance_shares: float = 0.01,
        tolerance_usdc: float = 0.05,
        active_token_ids: Optional[Set[str]] = None,
    ) -> Dict[str, Any]:
        """Compare ledger-derived positions vs actual wallet positions + open orders.

        Args:
            wallet_balances: {(token_id, side): qty} from CLOB API / on-chain
            open_orders: list of open order dicts (for context logging)
            tolerance_shares: max qty mismatch before WARNING (CRITICAL = 10x)
            tolerance_usdc: max USD mismatch before WARNING
            active_token_ids: if provided, only reconcile positions whose
                token_id is in this set (current-hour markets).  Expired
                hourly positions are settled on-chain and naturally go to 0;
                comparing them would trigger false CRITICAL alerts.

        Returns:
            {"matches": [...], "mismatches": [...], "critical": bool,
             "open_orders_count": int, "skipped_expired": int}
        """
        result: Dict[str, Any] = {
            "matches": [],
            "mismatches": [],
            "critical": False,
            "open_orders_count": len(open_orders) if open_orders else 0,
            "skipped_expired": 0,
        }

        # Collect all relevant (token_id, side) keys
        all_keys: Set[Tuple[str, str]] = set(wallet_balances.keys())
        skipped_expired = 0
        with self._lock:
            for key, pos in self._positions.items():
                if pos.net_qty > _ZERO:
                    token_id = key[0]
                    # Skip expired positions not in current active markets
                    if active_token_ids is not None and token_id not in active_token_ids:
                        skipped_expired += 1
                        continue
                    all_keys.add(key)
        result["skipped_expired"] = skipped_expired

        mismatches = []
        critical_threshold = tolerance_shares * 10  # 10x tolerance = CRITICAL

        for token_id, side in all_keys:
            ledger_qty = float(self.get_position(token_id, side).net_qty)
            wallet_qty = wallet_balances.get((token_id, side), 0.0)
            diff = wallet_qty - ledger_qty
            abs_diff = abs(diff)

            if abs_diff < tolerance_shares:
                result["matches"].append({
                    "token_id": token_id, "side": side,
                    "ledger_qty": ledger_qty, "wallet_qty": wallet_qty,
                })
            else:
                # CRITICAL if: diff > 10x tolerance, OR ledger=0 but wallet>0,
                # OR wallet=0 but ledger>0
                is_critical = (
                    abs_diff > critical_threshold
                    or (ledger_qty < tolerance_shares and wallet_qty >= tolerance_shares)
                    or (wallet_qty < tolerance_shares and ledger_qty >= tolerance_shares)
                )
                severity = "CRITICAL" if is_critical else "WARNING"

                entry = {
                    "token_id": token_id, "side": side,
                    "ledger_qty": ledger_qty, "wallet_qty": wallet_qty,
                    "diff": diff, "severity": severity,
                }
                mismatches.append(entry)

                meta = self._token_meta.get(token_id, ("?", "?", side))
                print(f"  [LEDGER] RECONCILE {severity}: "
                      f"{meta[0]} {side}: ledger={ledger_qty:.6f} "
                      f"wallet={wallet_qty:.6f} diff={diff:+.6f}")

        result["mismatches"] = mismatches
        critical = any(m["severity"] == "CRITICAL" for m in mismatches)
        result["critical"] = critical

        self._write_jsonl({
            "event_type": "LEDGER_RECONCILE_DONE",
            "matches": len(result["matches"]),
            "mismatches": len(mismatches),
            "critical": critical,
            "open_orders_count": result["open_orders_count"],
            "skipped_expired": skipped_expired,
            "details": mismatches[:20],
            "ts_ms": int(time.time() * 1000),
        })

        if skipped_expired > 0:
            print(f"  [LEDGER] Reconcile: skipped {skipped_expired} expired "
                  f"hourly positions (settled on-chain)")

        if critical:
            print(f"  [LEDGER] *** CRITICAL MISMATCH: "
                  f"{len([m for m in mismatches if m['severity'] == 'CRITICAL'])} "
                  f"positions differ beyond tolerance ***")

        return result

    # ══════════════════════════════════════════════════════════════════
    #  FETCH + INGEST EXTERNAL FILLS (from Polymarket API)
    # ══════════════════════════════════════════════════════════════════

    def fetch_and_ingest_fills(
        self,
        clob_client: Any,
        token_to_slug: Optional[Dict[str, Tuple[str, str, str]]] = None,
    ) -> int:
        """Fetch recent fills/trades from Polymarket CLOB API for this wallet,
        de-duplicate by trade_id/tx_hash, and append missing fills to ledger.

        Args:
            clob_client: the py_clob_client ClobClient instance (has .get_trades()
                         or we fall back to REST GET /data/trades)
            token_to_slug: {token_id: (slug, crypto, side)} mapping

        Returns: number of NEW fills ingested.
        """
        raw_trades: List[dict] = []

        # Try the SDK method first
        try:
            if hasattr(clob_client, 'get_trades'):
                trades = clob_client.get_trades()
                if isinstance(trades, list):
                    raw_trades = trades
            # Fallback: direct REST call
            if not raw_trades and hasattr(clob_client, 'host'):
                import requests
                addr = ""
                if hasattr(clob_client, 'get_address'):
                    addr = clob_client.get_address()
                if addr:
                    url = f"{clob_client.host}/data/trades"
                    resp = requests.get(url, params={"maker_address": addr},
                                        timeout=10)
                    if resp.status_code == 200:
                        data = resp.json()
                        if isinstance(data, list):
                            raw_trades = data
                        elif isinstance(data, dict) and "data" in data:
                            raw_trades = data["data"]
        except Exception as e:
            self._write_jsonl({
                "event_type": "LEDGER_FETCH_FILLS_ERROR",
                "err": str(e)[:200],
                "ts_ms": int(time.time() * 1000),
            })
            return 0

        if not raw_trades:
            return 0

        return self.ingest_external_fills(raw_trades, token_to_slug)

    def ingest_external_fills(
        self,
        fills: List[dict],
        token_to_slug: Optional[Dict[str, Tuple[str, str, str]]] = None,
    ) -> int:
        """Ingest fills fetched from the Polymarket API.

        Each fill dict should have at minimum:
            trade_id (or id), token_id (or asset_id), side (BUY/SELL),
            size (or amount), price

        HOUR FILTER: Only ingests fills whose slug belongs to the current
        active hour window AND whose timestamp falls within the current hour.
        Old-hour fills are silently skipped (not counted as dedup).

        Returns number of NEW fills appended (after dedup).
        """
        new_count = 0
        skipped_action = 0
        skipped_dedup = 0
        skipped_no_meta = 0
        skipped_old_hour = 0

        for i, raw_fill in enumerate(fills):
            trade_id = str(raw_fill.get("trade_id", raw_fill.get("id", "")))
            token_id = str(raw_fill.get("token_id", raw_fill.get("asset_id", "")))
            action = raw_fill.get("side", raw_fill.get("action", "")).upper()
            qty = raw_fill.get("size", raw_fill.get("amount", 0))
            price = raw_fill.get("price", 0)
            order_id = str(raw_fill.get("order_id", ""))

            if action not in ("BUY", "SELL"):
                skipped_action += 1
                continue

            # Look up metadata
            meta = (token_to_slug or {}).get(token_id)
            if meta is None:
                meta = self._token_meta.get(token_id)
            slug = meta[0] if meta else raw_fill.get("slug", "")
            crypto = meta[1] if meta else raw_fill.get("crypto", "")
            outcome = meta[2] if meta else raw_fill.get("outcome", "")

            if not slug and not outcome:
                skipped_no_meta += 1
                continue  # No metadata = cannot identify or filter this fill

            # ── HOUR WINDOW FILTER (strict) ──
            # Parse the fill's actual timestamp
            fill_ts_str = raw_fill.get("match_time") or raw_fill.get("created_at") or raw_fill.get("timestamp") or ""
            fill_epoch = 0.0
            if fill_ts_str:
                try:
                    fill_dt = datetime.fromisoformat(
                        str(fill_ts_str).replace("Z", "+00:00"))
                    fill_epoch = fill_dt.timestamp()
                except (ValueError, TypeError):
                    pass

            # Slug filter: if slug known and not in current hour, skip
            slug_ok = True
            if self._active_hour_slugs and slug:
                if slug not in self._active_hour_slugs:
                    slug_ok = False

            # Timestamp filter: if parseable timestamp, enforce hour boundary
            ts_ok = True
            if fill_epoch > 0:
                if not (self._hour_start.timestamp() <= fill_epoch < self._hour_end.timestamp()):
                    ts_ok = False

            # REJECT if either check explicitly fails, or if NEITHER is available
            if not slug_ok or not ts_ok:
                skipped_old_hour += 1
                continue
            if not slug and fill_epoch == 0:
                skipped_no_meta += 1
                continue  # unverifiable — no slug, no timestamp

            pos = self.record_fill(
                slug=slug, crypto=crypto,
                token_id=token_id,
                market_id=raw_fill.get("market_id", raw_fill.get("condition_id", "")),
                side=outcome, action=action,
                fill_qty=qty, fill_price=price,
                fees=raw_fill.get("fee", raw_fill.get("fees", 0)),
                order_id=order_id,
                trade_id=trade_id,
                source="polymarket",
            )
            if pos is not None:
                new_count += 1
                # Always log newly discovered fills
                print(f"  [LEDGER] EXTERNAL_FILL NEW: "
                      f"{slug} {outcome} {action} qty={qty} @ {price}  "
                      f"order_id={order_id[:16] if order_id else '?'}  "
                      f"token_id={token_id[-12:] if token_id else '?'}")
            else:
                skipped_dedup += 1

        self._write_jsonl({
            "event_type": "LEDGER_EXTERNAL_FILLS_INGESTED",
            "new_fills": new_count,
            "total_submitted": len(fills),
            "skipped_action": skipped_action,
            "skipped_dedup": skipped_dedup,
            "skipped_no_meta": skipped_no_meta,
            "skipped_old_hour": skipped_old_hour,
            "ts_ms": int(time.time() * 1000),
        })
        # One-line summary: always print so operator sees the cycle ran
        print(f"  [LEDGER] External fills: {new_count} new / "
              f"{len(fills)} total ({skipped_dedup} dedup"
              f"{f', {skipped_old_hour} old_hour' if skipped_old_hour else ''}"
              f"{f', {skipped_no_meta} no_meta' if skipped_no_meta else ''}"
              f"{f', {skipped_action} bad_action' if skipped_action else ''})")

        return new_count

    # ══════════════════════════════════════════════════════════════════
    #  SUMMARY / REPORTING
    # ══════════════════════════════════════════════════════════════════

    def summary(self) -> dict:
        with self._lock:
            active = {k: v for k, v in self._positions.items() if v.net_qty > _ZERO}
            total_realized = sum((p.realized_pnl for p in self._positions.values()), _ZERO)
            total_unrealized = sum((p.unrealized_pnl for p in active.values()), _ZERO)
            total_cost = sum((p.total_cost for p in active.values()), _ZERO)
        return {
            "fills_in_current_hour": len(self._fills),
            "total_fills": len(self._fills),  # backward compat alias
            "active_positions": len(active),
            "total_positions": len(self._positions),
            "total_realized_pnl": float(total_realized),
            "total_unrealized_pnl": float(total_unrealized),
            "total_cost_basis": float(total_cost),
            "hour": self._hour_suffix,
            "safe_mode": self._safe_mode,
            "safe_mode_reason": self._safe_mode_reason,
        }

    def print_positions(self) -> None:
        """Print positions table split by active window vs other."""
        window_pos = self.get_active_window_positions()
        other_pos = self.get_other_positions()

        if not window_pos and not other_pos:
            print("  [LEDGER] No active positions")
            return

        # ── LEDGER_POSITIONS — current hour ──
        if window_pos:
            print(f"  [LEDGER] ╔══ LEDGER_POSITIONS — current hour "
                  f"══════════════════════════════════════════════════════╗")
            print(f"  [LEDGER] ║  {'Slug':<30s} {'Side':<6s} {'Net Qty':>12s} "
                  f"{'Avg Price':>10s} {'Cost $':>10s} {'Real PnL':>10s} "
                  f"{'Unrl PnL':>10s} ║")
            print(f"  [LEDGER] ╠══════════════════════════════════════════════════"
                  f"════════════════════════════════════════════╣")
            for (token_id, side), pos in sorted(window_pos.items(),
                                                key=lambda x: x[1].slug):
                print(f"  [LEDGER] ║  {pos.slug:<30s} {side:<6s} "
                      f"{float(pos.net_qty):12.6f} "
                      f"{float(pos.avg_entry_price):10.4f} "
                      f"{float(pos.total_cost):10.2f} "
                      f"{float(pos.realized_pnl):10.4f} "
                      f"{float(pos.unrealized_pnl):10.4f} ║")
            print(f"  [LEDGER] ╚══════════════════════════════════════════════════"
                  f"════════════════════════════════════════════╝")
        else:
            print("  [LEDGER] LEDGER_POSITIONS — current hour: (none)")

        # ── OTHER (non-tradable, old-hour) ──
        if other_pos:
            print(f"  [LEDGER] ── OTHER [old-hour, non-tradable] ({len(other_pos)}) ──")
            for (token_id, side), pos in sorted(other_pos.items(),
                                                key=lambda x: x[1].slug):
                print(f"  [LEDGER]   {pos.slug:<30s} {side:<6s} "
                      f"qty={float(pos.net_qty):.6f}  (old hour)")

        totals = self.summary()
        print(f"  [LEDGER] Totals: {totals['active_positions']} positions  "
              f"| fills in current hour={totals['fills_in_current_hour']}  "
              f"| realized=${totals['total_realized_pnl']:.4f}  "
              f"| unrealized=${totals['total_unrealized_pnl']:.4f}  "
              f"| cost_basis=${totals['total_cost_basis']:.2f}")
