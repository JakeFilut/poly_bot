"""
ExposureTracker — shared real-time exposure accounting for HYBRID_COPYWALLET.

Two engine sub-ledgers (DIR / SCALP) track their own positions independently,
but a single shared tracker enforces MAX_TOTAL_EXPOSURE_USD at all times.

Exposure = sum(abs(qty) * avg_price) for filled positions + open order notional.
Sells FREE exposure immediately.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Tuple, Optional, Callable

from src.config import settings

# Type alias for sub-ledger key
_PosKey = Tuple[str, str]  # (slug, outcome)


@dataclass
class SubPosition:
    """Single position entry in an engine sub-ledger."""
    qty: float = 0.0
    vwap: float = 0.0
    cost_usd: float = 0.0
    realized_pnl: float = 0.0
    trade_count: int = 0


class ExposureTracker:
    """Shared exposure tracker with two engine sub-ledgers.

    Usage:
        tracker = ExposureTracker(write_jsonl_fn)
        tracker.record_buy("DIR", slug, outcome, qty, price)
        tracker.record_sell("SCALP", slug, outcome, qty, price)
        ok = tracker.can_open("SCALP", 1.50)
        exp = tracker.total_exposure_usd()
    """

    def __init__(self, write_jsonl: Optional[Callable] = None):
        self._write_jsonl = write_jsonl or (lambda d: None)

        # Engine sub-ledgers: engine -> {(slug, outcome): SubPosition}
        self._positions: Dict[str, Dict[_PosKey, SubPosition]] = {
            "DIR": {},
            "SCALP": {},
        }

        # Open order notional tracking: order_id -> {engine, usd}
        self._open_orders: Dict[str, dict] = {}

        # Engine-level realized PnL accumulators
        self._engine_rpnl: Dict[str, float] = {"DIR": 0.0, "SCALP": 0.0}

        # Scalp trade stats
        self.scalp_trades: int = 0
        self.scalp_wins: int = 0
        self.scalp_hold_times: list = []

        # Order-to-engine mapping (persisted for fill attribution)
        self._order_engine_map: Dict[str, str] = {}

    # ──────────────────────────────────────────────────────────────────
    # Exposure queries
    # ──────────────────────────────────────────────────────────────────

    def total_exposure_usd(self) -> float:
        """Current exposure = positions cost + open orders notional."""
        return self._positions_cost_usd() + self._open_orders_usd()

    def engine_exposure_usd(self, engine: str) -> float:
        """Exposure for a specific engine (DIR or SCALP)."""
        total = 0.0
        for pos in self._positions.get(engine, {}).values():
            if pos.qty > 0:
                total += pos.cost_usd
        # Add open orders for this engine
        for info in self._open_orders.values():
            if info.get("engine") == engine:
                total += info.get("usd", 0.0)
        return total

    def can_open(self, engine: str, additional_usd: float) -> bool:
        """Check if opening a new position of additional_usd is allowed.

        Checks:
        1. Total exposure + new order <= MAX_TOTAL_EXPOSURE_USD
        2. Engine-specific budget not exceeded
        """
        current = self.total_exposure_usd()
        if current + additional_usd > settings.MAX_TOTAL_EXPOSURE_USD:
            return False

        engine_exp = self.engine_exposure_usd(engine)
        budget = (settings.DIR_BUDGET_USD if engine == "DIR"
                  else settings.SCALP_BUDGET_USD)
        if engine_exp + additional_usd > budget:
            return False

        return True

    def exposure_room(self) -> float:
        """How much more USD can be deployed across all engines."""
        return max(0.0, settings.MAX_TOTAL_EXPOSURE_USD - self.total_exposure_usd())

    def _positions_cost_usd(self) -> float:
        total = 0.0
        for engine_positions in self._positions.values():
            for pos in engine_positions.values():
                if pos.qty > 0:
                    total += pos.cost_usd
        return total

    def _open_orders_usd(self) -> float:
        return sum(info.get("usd", 0.0) for info in self._open_orders.values())

    # ──────────────────────────────────────────────────────────────────
    # Order-engine mapping
    # ──────────────────────────────────────────────────────────────────

    def tag_order(self, order_id: str, engine: str) -> None:
        """Associate an order with an engine for fill attribution."""
        self._order_engine_map[order_id] = engine

    def get_order_engine(self, order_id: str) -> str:
        """Look up which engine placed an order. Defaults to 'DIR'."""
        return self._order_engine_map.get(order_id, "DIR")

    def register_open_order(self, order_id: str, engine: str, usd: float) -> None:
        """Track an open order's notional for exposure calculation."""
        self._open_orders[order_id] = {"engine": engine, "usd": usd}
        self._order_engine_map[order_id] = engine

    def release_open_order(self, order_id: str) -> float:
        """Remove open order from tracking. Returns notional freed."""
        info = self._open_orders.pop(order_id, None)
        return info.get("usd", 0.0) if info else 0.0

    # ──────────────────────────────────────────────────────────────────
    # Fill recording (updates sub-ledger)
    # ──────────────────────────────────────────────────────────────────

    def record_buy(self, engine: str, slug: str, outcome: str,
                   qty: float, price: float, order_id: str = "") -> None:
        """Record a buy fill in the engine's sub-ledger."""
        key: _PosKey = (slug, outcome)
        ledger = self._positions.setdefault(engine, {})
        pos = ledger.setdefault(key, SubPosition())

        new_qty = pos.qty + qty
        if new_qty > 0:
            pos.vwap = (pos.vwap * pos.qty + price * qty) / new_qty
        pos.qty = new_qty
        pos.cost_usd = pos.vwap * pos.qty
        pos.trade_count += 1

        # Release open order reservation if tracked
        if order_id:
            self.release_open_order(order_id)

        self._write_jsonl({
            "event_type": "ENGINE_FILL",
            "engine": engine, "side": "BUY",
            "slug": slug, "outcome": outcome,
            "qty": round(qty, 2), "price": round(price, 4),
            "pos_qty": round(pos.qty, 2), "pos_vwap": round(pos.vwap, 4),
            "engine_exposure": round(self.engine_exposure_usd(engine), 2),
            "total_exposure": round(self.total_exposure_usd(), 2),
            "ts_ms": int(time.time() * 1000),
        })

    def record_sell(self, engine: str, slug: str, outcome: str,
                    qty: float, price: float, order_id: str = "") -> float:
        """Record a sell fill. Returns realized PnL for this fill."""
        key: _PosKey = (slug, outcome)
        ledger = self._positions.setdefault(engine, {})
        pos = ledger.get(key)

        if pos is None or pos.qty <= 0:
            return 0.0

        sell_qty = min(qty, pos.qty)
        if sell_qty <= 0:
            return 0.0

        proceeds = sell_qty * price
        cost_basis = sell_qty * pos.vwap
        pnl = proceeds - cost_basis

        pos.qty -= sell_qty
        pos.cost_usd = max(0.0, pos.vwap * pos.qty)
        pos.realized_pnl += pnl
        pos.trade_count += 1
        self._engine_rpnl[engine] = self._engine_rpnl.get(engine, 0.0) + pnl

        # Clean up dust
        if pos.qty < 0.5:
            pos.qty = 0.0
            pos.cost_usd = 0.0
            pos.vwap = 0.0

        if order_id:
            self.release_open_order(order_id)

        self._write_jsonl({
            "event_type": "ENGINE_FILL",
            "engine": engine, "side": "SELL",
            "slug": slug, "outcome": outcome,
            "qty": round(sell_qty, 2), "price": round(price, 4),
            "pnl": round(pnl, 4),
            "pos_qty": round(pos.qty, 2),
            "engine_exposure": round(self.engine_exposure_usd(engine), 2),
            "total_exposure": round(self.total_exposure_usd(), 2),
            "ts_ms": int(time.time() * 1000),
        })

        return pnl

    # ──────────────────────────────────────────────────────────────────
    # Position queries
    # ──────────────────────────────────────────────────────────────────

    def get_position(self, engine: str, slug: str, outcome: str) -> SubPosition:
        """Get position for an engine/slug/outcome. Returns empty if none."""
        key = (slug, outcome)
        return self._positions.get(engine, {}).get(key, SubPosition())

    def has_scalp_position(self, slug: str) -> bool:
        """Check if scalp engine has any open position for this slug."""
        for outcome in ("Up", "Down"):
            pos = self.get_position("SCALP", slug, outcome)
            if pos.qty >= 0.5:
                return True
        return False

    def get_scalp_position_outcome(self, slug: str) -> Optional[str]:
        """Return outcome of open scalp position, or None."""
        for outcome in ("Up", "Down"):
            pos = self.get_position("SCALP", slug, outcome)
            if pos.qty >= 0.5:
                return outcome
        return None

    def all_positions(self, engine: str = None) -> Dict[_PosKey, SubPosition]:
        """Return all non-zero positions, optionally filtered by engine."""
        result = {}
        engines = [engine] if engine else ["DIR", "SCALP"]
        for eng in engines:
            for key, pos in self._positions.get(eng, {}).items():
                if pos.qty >= 0.5:
                    result[key] = pos
        return result

    # ──────────────────────────────────────────────────────────────────
    # Summary / diagnostics
    # ──────────────────────────────────────────────────────────────────

    def summary(self) -> dict:
        """Return summary dict for logging / end-of-hour report."""
        return {
            "total_exposure_usd": round(self.total_exposure_usd(), 2),
            "dir_exposure_usd": round(self.engine_exposure_usd("DIR"), 2),
            "scalp_exposure_usd": round(self.engine_exposure_usd("SCALP"), 2),
            "dir_rpnl": round(self._engine_rpnl.get("DIR", 0.0), 4),
            "scalp_rpnl": round(self._engine_rpnl.get("SCALP", 0.0), 4),
            "scalp_trades": self.scalp_trades,
            "scalp_wins": self.scalp_wins,
            "scalp_winrate": round(self.scalp_wins / max(1, self.scalp_trades), 3),
            "open_orders_usd": round(self._open_orders_usd(), 2),
        }

    def flatten_all(self) -> list:
        """Return list of (engine, slug, outcome, qty) for all positions to flatten."""
        to_flatten = []
        for engine in ("DIR", "SCALP"):
            for (slug, outcome), pos in self._positions.get(engine, {}).items():
                if pos.qty >= 0.5:
                    to_flatten.append((engine, slug, outcome, pos.qty))
        return to_flatten
