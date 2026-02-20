"""Fills ledger — append-only fill accounting with position derivation.

Provides:
  - Append-only JSONL ledger of every BUY/SELL fill (disk-persistent)
  - Position calculator that derives net_qty, avg_entry_price, realized_pnl
    ONLY from the ledger (not from internal guesses)
  - Reconciliation against on-chain / wallet balances with SAFE_MODE
  - Helper functions: get_position, get_sellable_qty, record_fill, etc.

Precision: all quantities and prices stored internally as Decimal.
Cost method: weighted-average cost (WAC) for avg_entry_price.
"""
from __future__ import annotations

import json
import os
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_ZERO = Decimal("0")
_PRECISION = Decimal("0.000000001")  # 9 decimal places for storage


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
    """One fill in the append-only ledger."""
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
    """Logged when an order is requested (before fill)."""
    timestamp: str
    slug: str
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
    """Append-only fills ledger with position calculator.

    Thread-safe: all mutations go through a lock.

    Usage:
        ledger = FillsLedger(ledger_path="./fills_ledger.jsonl", run_id="abc123")
        ledger.load_from_disk()  # replay existing fills
        ledger.record_fill(...)  # after each fill
        pos = ledger.get_position(token_id, "Up")
        sellable = ledger.get_sellable_qty(token_id, "Up")
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

        # All fills in memory (loaded from disk + new)
        self._fills: List[FillRecord] = []

        # Dedup set: trade_id/order_id -> True
        self._seen_ids: Set[str] = set()

        # Derived positions: (token_id, side) -> PositionInfo
        self._positions: Dict[Tuple[str, str], PositionInfo] = {}

        # Slug/token mapping: token_id -> (slug, crypto, side)
        self._token_meta: Dict[str, Tuple[str, str, str]] = {}

        # SAFE MODE: no new trades when True
        self._safe_mode: bool = False
        self._safe_mode_reason: str = ""
        self._safe_mode_details: List[dict] = []

        # Order intent log (in-memory only, not persisted)
        self._order_intents: List[OrderIntent] = []

        # Ensure ledger directory exists
        ledger_dir = os.path.dirname(os.path.abspath(self._path))
        if ledger_dir:
            os.makedirs(ledger_dir, exist_ok=True)

    # ── Disk I/O ──────────────────────────────────────────────────────

    def load_from_disk(self) -> int:
        """Load existing fills from the JSONL ledger file.
        Returns number of fills loaded."""
        if not os.path.exists(self._path):
            return 0

        loaded = 0
        with self._lock:
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
                        dedup_key = self._dedup_key(fill)
                        if dedup_key in self._seen_ids:
                            continue
                        self._seen_ids.add(dedup_key)
                        self._fills.append(fill)
                        self._update_token_meta(fill)
                        loaded += 1
                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        print(f"  [LEDGER] WARN: skipping malformed line {line_num}: {e}")
            # Recompute positions from all loaded fills
            self._recompute_all()

        self._write_jsonl({
            "event_type": "LEDGER_LOADED",
            "fills_count": loaded,
            "positions_count": len(self._positions),
            "path": self._path,
            "ts_ms": int(time.time() * 1000),
        })
        print(f"  [LEDGER] Loaded {loaded} fills from {self._path}, "
              f"{len(self._positions)} positions derived")
        return loaded

    def _append_to_disk(self, fill: FillRecord) -> None:
        """Append a single fill record to the JSONL file."""
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
        try:
            with open(self._path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, separators=(",", ":")) + "\n")
        except Exception as e:
            print(f"  [LEDGER] ERROR writing to disk: {e}")

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

    # ── Deduplication ─────────────────────────────────────────────────

    @staticmethod
    def _dedup_key(fill: FillRecord) -> str:
        """Build a dedup key from trade_id or order_id+action+qty+price."""
        if fill.trade_id:
            return f"tid:{fill.trade_id}"
        # Fallback: composite key
        return f"oid:{fill.order_id}|{fill.action}|{fill.fill_qty}|{fill.fill_price}|{fill.timestamp}"

    # ── Token metadata ────────────────────────────────────────────────

    def _update_token_meta(self, fill: FillRecord) -> None:
        """Cache token_id -> (slug, crypto, side) mapping."""
        if fill.token_id:
            self._token_meta[fill.token_id] = (fill.slug, fill.crypto, fill.side)

    def register_token(self, token_id: str, slug: str, crypto: str, side: str) -> None:
        """Register a token_id mapping (called during market discovery)."""
        with self._lock:
            self._token_meta[token_id] = (slug, crypto, side)

    # ── Record fills ──────────────────────────────────────────────────

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
        """Record a fill and update positions. Returns updated PositionInfo or None if duplicate.

        This is the MAIN entry point for recording fills from the bot.
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
                return None  # duplicate

            self._seen_ids.add(dedup_key)
            self._fills.append(fill)
            self._update_token_meta(fill)
            self._append_to_disk(fill)

            # Update position incrementally
            pos = self._update_position_incremental(fill)

        # Log the fill
        self._write_jsonl({
            "event_type": "LEDGER_FILL_RECORDED",
            "slug": slug,
            "crypto": crypto,
            "token_id": token_id,
            "side": side,
            "action": action.upper(),
            "fill_qty": float(qty_d),
            "fill_price": float(price_d),
            "fees": float(fees_d),
            "order_id": order_id,
            "trade_id": trade_id,
            "source": source,
            "net_qty": float(pos.net_qty),
            "avg_entry_price": float(pos.avg_entry_price),
            "realized_pnl": float(pos.realized_pnl),
            "ts_ms": int(time.time() * 1000),
        })

        print(f"  [LEDGER] {action.upper()} {slug} {side}: "
              f"qty={float(qty_d):.6f} @ {float(price_d):.4f}  →  "
              f"net_qty={float(pos.net_qty):.6f}  "
              f"avg_price={float(pos.avg_entry_price):.4f}  "
              f"realized_pnl={float(pos.realized_pnl):.4f}")

        return pos

    def record_order_intent(
        self,
        slug: str,
        side: str,
        action: str,
        requested_qty: float,
        requested_price: float,
        reason: str,
        order_id: str = "",
    ) -> None:
        """Log an order intent (pre-fill). Not persisted to ledger, just JSONL event log."""
        intent = OrderIntent(
            timestamp=_utc_iso(),
            slug=slug,
            side=side,
            action=action.upper(),
            requested_qty=_to_dec(requested_qty),
            requested_price=_to_dec(requested_price),
            reason=reason,
            order_id=order_id,
        )
        with self._lock:
            self._order_intents.append(intent)
            # Keep only last 1000 intents in memory
            if len(self._order_intents) > 1000:
                self._order_intents = self._order_intents[-500:]

        self._write_jsonl({
            "event_type": "LEDGER_ORDER_INTENT",
            "slug": slug,
            "side": side,
            "action": action.upper(),
            "requested_qty": float(requested_qty),
            "requested_price": float(requested_price),
            "reason": reason,
            "order_id": order_id,
            "ts_ms": int(time.time() * 1000),
        })

    # ── Position calculation ──────────────────────────────────────────

    def _update_position_incremental(self, fill: FillRecord) -> PositionInfo:
        """Update a single position based on a new fill (WAC method).
        Must be called under self._lock."""
        key = (fill.token_id, fill.side)

        if key not in self._positions:
            self._positions[key] = PositionInfo(
                slug=fill.slug,
                crypto=fill.crypto,
                token_id=fill.token_id,
                side=fill.side,
            )

        pos = self._positions[key]
        # Update slug in case it changed (new hour)
        pos.slug = fill.slug

        if fill.action == "BUY":
            # Weighted average cost: new_avg = (old_cost + new_cost) / new_qty
            new_cost = fill.fill_qty * fill.fill_price
            old_total_cost = pos.avg_entry_price * pos.net_qty
            new_qty = pos.net_qty + fill.fill_qty

            if new_qty > _ZERO:
                pos.avg_entry_price = (old_total_cost + new_cost) / new_qty
            pos.net_qty = new_qty
            pos.total_cost = pos.avg_entry_price * pos.net_qty
            pos.total_buy_qty += fill.fill_qty

        elif fill.action == "SELL":
            sell_qty = min(fill.fill_qty, pos.net_qty)  # can't sell more than owned
            if sell_qty > _ZERO:
                # Realized PnL = (sell_price - avg_entry) * sell_qty
                pnl = (fill.fill_price - pos.avg_entry_price) * sell_qty
                pos.realized_pnl += pnl - fill.fees

                # Reduce inventory (avg_entry stays the same under WAC)
                pos.net_qty -= sell_qty
                pos.total_cost = pos.avg_entry_price * pos.net_qty
            pos.total_sell_qty += fill.fill_qty

        # Clean dust
        if pos.net_qty < Decimal("0.001"):
            pos.net_qty = _ZERO
            pos.total_cost = _ZERO
            pos.avg_entry_price = _ZERO

        pos.fill_count += 1
        return pos

    def _recompute_all(self) -> None:
        """Recompute ALL positions from scratch using the full fills list.
        Must be called under self._lock."""
        self._positions.clear()

        for fill in self._fills:
            self._update_position_incremental(fill)

    def recompute_positions_from_ledger(self) -> Dict[Tuple[str, str], PositionInfo]:
        """Public: recompute all positions from ledger. Returns positions dict."""
        with self._lock:
            self._recompute_all()
            return dict(self._positions)

    # ── Position queries ──────────────────────────────────────────────

    def get_position(self, token_id: str, side: str) -> PositionInfo:
        """Get position info for a (token_id, side). Returns zero position if none."""
        with self._lock:
            key = (str(token_id), side)
            if key in self._positions:
                return self._positions[key]
            # Return empty position
            meta = self._token_meta.get(str(token_id), ("", "", side))
            return PositionInfo(
                slug=meta[0], crypto=meta[1],
                token_id=str(token_id), side=side,
            )

    def get_position_by_slug(self, slug: str, side: str) -> Optional[PositionInfo]:
        """Get position info by slug + side. Scans positions for matching slug."""
        with self._lock:
            for key, pos in self._positions.items():
                if pos.slug == slug and pos.side == side:
                    return pos
        return None

    def get_sellable_qty(self, token_id: str, side: str) -> Decimal:
        """Max quantity available to sell for a (token_id, side)."""
        pos = self.get_position(token_id, side)
        return max(pos.net_qty, _ZERO)

    def get_sellable_qty_by_slug(self, slug: str, side: str) -> Decimal:
        """Max quantity available to sell by slug + side."""
        pos = self.get_position_by_slug(slug, side)
        if pos is None:
            return _ZERO
        return max(pos.net_qty, _ZERO)

    def get_all_positions(self) -> Dict[Tuple[str, str], PositionInfo]:
        """Get a snapshot of all positions."""
        with self._lock:
            return dict(self._positions)

    def get_all_active_positions(self) -> Dict[Tuple[str, str], PositionInfo]:
        """Get all positions with net_qty > 0."""
        with self._lock:
            return {k: v for k, v in self._positions.items() if v.net_qty > _ZERO}

    def get_total_realized_pnl(self) -> Decimal:
        """Sum of realized PnL across all positions."""
        with self._lock:
            return sum((p.realized_pnl for p in self._positions.values()), _ZERO)

    # ── Unrealized PnL update ─────────────────────────────────────────

    def update_unrealized_pnl(
        self,
        price_lookup: Dict[Tuple[str, str], float],
    ) -> None:
        """Update unrealized PnL for all positions using current market prices.

        Args:
            price_lookup: {(token_id, side): current_mid_price}
        """
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

    # ── Sell validation ───────────────────────────────────────────────

    def validate_sell(
        self, token_id: str, side: str, requested_qty: float, slug: str = ""
    ) -> Tuple[bool, float, str]:
        """Validate a sell order against ledger-derived position.

        Returns:
            (allowed, capped_qty, reason)
            - allowed: True if sell is ok
            - capped_qty: quantity capped to owned amount
            - reason: explanation if blocked
        """
        req = _to_dec(requested_qty)
        owned = self.get_sellable_qty(token_id, side)

        if owned <= _ZERO:
            display = slug or token_id
            msg = f"SELL BLOCKED: no position for {display} {side} (ledger net_qty=0)"
            print(f"  [LEDGER] {msg}")
            return False, 0.0, msg

        if req > owned:
            capped = float(owned)
            display = slug or token_id
            print(f"  [LEDGER] SELL CAPPED: {display} {side}: "
                  f"requested={float(req):.6f} > owned={float(owned):.6f}, "
                  f"capping to {capped:.6f}")
            return True, capped, f"capped from {float(req):.6f} to {capped:.6f}"

        return True, float(req), "ok"

    # ── SAFE MODE ─────────────────────────────────────────────────────

    @property
    def safe_mode(self) -> bool:
        return self._safe_mode

    @property
    def safe_mode_reason(self) -> str:
        return self._safe_mode_reason

    def enter_safe_mode(self, reason: str, details: Optional[List[dict]] = None) -> None:
        """Enter SAFE MODE — no new trades until resolved."""
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

    def exit_safe_mode(self) -> None:
        """Exit SAFE MODE — trading allowed again."""
        if self._safe_mode:
            self._safe_mode = False
            self._safe_mode_reason = ""
            self._safe_mode_details = []
            self._write_jsonl({
                "event_type": "LEDGER_SAFE_MODE_EXIT",
                "ts_ms": int(time.time() * 1000),
            })
            print("  [LEDGER] SAFE MODE resolved — trading allowed")

    # ── Reconciliation ────────────────────────────────────────────────

    def reconcile_positions(
        self,
        wallet_balances: Dict[Tuple[str, str], float],
        tolerance: float = 0.5,
    ) -> Dict[str, List[dict]]:
        """Compare ledger-derived positions against wallet/on-chain balances.

        Args:
            wallet_balances: {(token_id, side): qty} from on-chain or CLOB API
            tolerance: max acceptable qty mismatch before flagging

        Returns:
            {"matches": [...], "mismatches": [...], "critical": bool}
        """
        result: Dict[str, Any] = {
            "matches": [],
            "mismatches": [],
            "critical": False,
        }

        # Collect all keys
        all_keys: Set[Tuple[str, str]] = set(wallet_balances.keys())
        with self._lock:
            for key, pos in self._positions.items():
                if pos.net_qty > _ZERO:
                    all_keys.add(key)

        mismatches = []
        for token_id, side in all_keys:
            ledger_qty = float(self.get_position(token_id, side).net_qty)
            wallet_qty = wallet_balances.get((token_id, side), 0.0)
            diff = wallet_qty - ledger_qty

            if abs(diff) < 0.001:  # negligible
                result["matches"].append({
                    "token_id": token_id,
                    "side": side,
                    "ledger_qty": ledger_qty,
                    "wallet_qty": wallet_qty,
                })
            else:
                severity = "CRITICAL" if abs(diff) > tolerance else "WARNING"
                entry = {
                    "token_id": token_id,
                    "side": side,
                    "ledger_qty": ledger_qty,
                    "wallet_qty": wallet_qty,
                    "diff": diff,
                    "severity": severity,
                }
                mismatches.append(entry)

                meta = self._token_meta.get(token_id, ("?", "?", side))
                print(f"  [LEDGER] RECONCILE {severity}: "
                      f"{meta[0]} {side}: ledger={ledger_qty:.6f} "
                      f"wallet={wallet_qty:.6f} diff={diff:+.6f}")

        result["mismatches"] = mismatches

        # Check for critical mismatches
        critical = any(m["severity"] == "CRITICAL" for m in mismatches)
        result["critical"] = critical

        self._write_jsonl({
            "event_type": "LEDGER_RECONCILE_DONE",
            "matches": len(result["matches"]),
            "mismatches": len(mismatches),
            "critical": critical,
            "details": mismatches[:20],
            "ts_ms": int(time.time() * 1000),
        })

        if critical:
            print(f"  [LEDGER] *** CRITICAL MISMATCH: {len(mismatches)} positions differ ***")

        return result

    # ── Ingest external fills (from API) ──────────────────────────────

    def ingest_external_fills(
        self,
        fills: List[dict],
        token_to_slug: Optional[Dict[str, Tuple[str, str, str]]] = None,
    ) -> int:
        """Ingest fills fetched from the Polymarket API.

        Each fill dict should have at minimum:
            trade_id, token_id, side (BUY/SELL), size, price, timestamp

        Returns number of NEW fills appended (after dedup).
        """
        new_count = 0
        for raw_fill in fills:
            trade_id = str(raw_fill.get("trade_id", raw_fill.get("id", "")))
            token_id = str(raw_fill.get("token_id", raw_fill.get("asset_id", "")))
            action = raw_fill.get("side", raw_fill.get("action", "")).upper()
            if action not in ("BUY", "SELL"):
                continue

            qty = raw_fill.get("size", raw_fill.get("amount", 0))
            price = raw_fill.get("price", 0)

            # Look up metadata
            meta = (token_to_slug or {}).get(token_id)
            if meta is None:
                meta = self._token_meta.get(token_id)
            slug = meta[0] if meta else raw_fill.get("slug", "")
            crypto = meta[1] if meta else raw_fill.get("crypto", "")
            outcome = meta[2] if meta else raw_fill.get("outcome", "")

            result = self.record_fill(
                slug=slug,
                crypto=crypto,
                token_id=token_id,
                market_id=raw_fill.get("market_id", raw_fill.get("condition_id", "")),
                side=outcome,
                action=action,
                fill_qty=qty,
                fill_price=price,
                fees=raw_fill.get("fee", raw_fill.get("fees", 0)),
                order_id=str(raw_fill.get("order_id", "")),
                trade_id=trade_id,
                source="polymarket",
            )
            if result is not None:
                new_count += 1

        if new_count > 0:
            self._write_jsonl({
                "event_type": "LEDGER_EXTERNAL_FILLS_INGESTED",
                "new_fills": new_count,
                "total_submitted": len(fills),
                "ts_ms": int(time.time() * 1000),
            })
            print(f"  [LEDGER] Ingested {new_count} new external fills "
                  f"(of {len(fills)} total)")

        return new_count

    # ── Summary / reporting ───────────────────────────────────────────

    def summary(self) -> dict:
        """Return a summary dict of ledger state."""
        with self._lock:
            active = {k: v for k, v in self._positions.items() if v.net_qty > _ZERO}
            total_realized = sum((p.realized_pnl for p in self._positions.values()), _ZERO)
            total_unrealized = sum((p.unrealized_pnl for p in active.values()), _ZERO)
            total_cost = sum((p.total_cost for p in active.values()), _ZERO)

        return {
            "total_fills": len(self._fills),
            "active_positions": len(active),
            "total_positions": len(self._positions),
            "total_realized_pnl": float(total_realized),
            "total_unrealized_pnl": float(total_unrealized),
            "total_cost_basis": float(total_cost),
            "safe_mode": self._safe_mode,
            "safe_mode_reason": self._safe_mode_reason,
        }

    def print_positions(self) -> None:
        """Print all active positions to console."""
        active = self.get_all_active_positions()
        if not active:
            print("  [LEDGER] No active positions")
            return
        print(f"  [LEDGER] Active positions ({len(active)}):")
        for (token_id, side), pos in sorted(active.items(), key=lambda x: x[1].slug):
            print(f"    {pos.slug:30s} {side:5s}: "
                  f"qty={float(pos.net_qty):10.6f}  "
                  f"avg={float(pos.avg_entry_price):.4f}  "
                  f"cost=${float(pos.total_cost):.2f}  "
                  f"realized=${float(pos.realized_pnl):.4f}  "
                  f"unrealized=${float(pos.unrealized_pnl):.4f}")
