"""
state.py – SQLite-backed persistent state for inventory and open orders.

Survives restarts.  In-memory rolling tape buffers are NOT persisted
(rebuilt from live data after restart).
"""
from __future__ import annotations

import json
import sqlite3
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------
@dataclass
class InventoryEntry:
    slug: str
    outcome: str         # "Up" or "Down"
    token_id: str
    shares: float
    avg_cost: float      # VWAP per share (0.00 – 1.00)
    last_update_ts: str  # ISO-8601

    @property
    def cost_basis_usd(self) -> float:
        return self.shares * self.avg_cost

    def apply_buy(self, qty: float, price: float) -> None:
        """Update VWAP and shares after a buy fill."""
        if qty <= 0:
            return
        total_cost = self.shares * self.avg_cost + qty * price
        self.shares += qty
        self.avg_cost = total_cost / self.shares if self.shares > 0 else 0.0
        self.last_update_ts = _utc_iso()

    def apply_sell(self, qty: float) -> None:
        """Reduce shares after a sell fill.  VWAP unchanged."""
        self.shares = max(0.0, self.shares - qty)
        if self.shares < 0.01:
            self.shares = 0.0
        self.last_update_ts = _utc_iso()


@dataclass
class OpenOrder:
    order_id: str
    client_order_id: str
    slug: str
    outcome: str
    token_id: str
    side: str            # "BUY" or "SELL"
    price: float
    size: float          # shares
    created_ts: float    # epoch seconds


@dataclass
class ShadowOrder:
    """A DRY_RUN limit order waiting for the market to 'touch' its price."""
    order_id: str
    slug: str
    outcome: str
    token_id: str
    side: str            # "BUY" or "SELL"
    price: float
    qty: float           # shares
    timestamp: float     # epoch seconds – when the order was placed
    expiry_ts: float     # epoch seconds – when the order expires (timestamp + ORDER_TTL_MS/1000)
    client_order_id: str
    was_crossing: bool = False  # True if price crossed the spread at placement


# ---------------------------------------------------------------------------
# In-memory rolling tape buffers (not persisted)
# ---------------------------------------------------------------------------
class RollingTape:
    """Fixed-window deque of (epoch_sec, value) tuples."""

    def __init__(self, window_sec: float = 60.0):
        self.window_sec = window_sec
        self._buf: Deque[Tuple[float, float]] = deque()

    def add(self, ts: float, value: float) -> None:
        self._buf.append((ts, value))
        self._trim(ts)

    def _trim(self, now: float) -> None:
        cutoff = now - self.window_sec
        while self._buf and self._buf[0][0] < cutoff:
            self._buf.popleft()

    def values(self, now: float | None = None) -> List[float]:
        if now is not None:
            self._trim(now)
        return [v for _, v in self._buf]

    def latest(self) -> float | None:
        return self._buf[-1][1] if self._buf else None

    def oldest(self) -> Tuple[float, float] | None:
        return self._buf[0] if self._buf else None

    def __len__(self) -> int:
        return len(self._buf)


# ---------------------------------------------------------------------------
# State manager (SQLite-backed)
# ---------------------------------------------------------------------------
class StateManager:
    """Manages persistent inventory + open orders in SQLite, plus in-memory tapes."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()

        # In-memory caches (loaded from DB at init)
        self.inventory: Dict[Tuple[str, str], InventoryEntry] = {}
        self.open_orders: Dict[str, OpenOrder] = {}
        self.shadow_orders: Dict[str, ShadowOrder] = {}

        # Rolling tapes (in-memory only)
        self.spread_tapes: Dict[str, RollingTape] = defaultdict(
            lambda: RollingTape(60.0)
        )
        self.binance_tapes: Dict[str, RollingTape] = defaultdict(
            lambda: RollingTape(300.0)  # 5 min for ret_120s
        )

        # Used client_order_ids (idempotency guard)
        self._used_client_ids: set = set()

        # Cumulative realized PnL (in-memory, for logging)
        self.realized_pnl: float = 0.0

        # Load persisted data
        self._load_inventory()
        self._load_open_orders()
        self._load_shadow_orders()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def _create_tables(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS inventory (
                slug TEXT NOT NULL,
                outcome TEXT NOT NULL,
                token_id TEXT NOT NULL,
                shares REAL NOT NULL DEFAULT 0,
                avg_cost REAL NOT NULL DEFAULT 0,
                last_update_ts TEXT NOT NULL,
                PRIMARY KEY (slug, outcome)
            );
            CREATE TABLE IF NOT EXISTS open_orders (
                order_id TEXT PRIMARY KEY,
                client_order_id TEXT NOT NULL,
                slug TEXT NOT NULL,
                outcome TEXT NOT NULL,
                token_id TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                size REAL NOT NULL,
                created_ts REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS used_client_ids (
                client_id TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS shadow_orders (
                order_id TEXT PRIMARY KEY,
                slug TEXT NOT NULL,
                outcome TEXT NOT NULL,
                token_id TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                qty REAL NOT NULL,
                timestamp REAL NOT NULL,
                expiry_ts REAL NOT NULL,
                client_order_id TEXT NOT NULL,
                was_crossing INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS live_risk_halt (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS diag_hourly_pnl (
                hour_et TEXT PRIMARY KEY,
                realized_pnl REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS diag_counters (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS diag_copywallet_hourly_pnl (
                hour_et TEXT PRIMARY KEY,
                realized_pnl REAL NOT NULL DEFAULT 0
            );
        """)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------
    def _load_inventory(self) -> None:
        for row in self._conn.execute(
            "SELECT slug, outcome, token_id, shares, avg_cost, last_update_ts FROM inventory"
        ):
            slug, outcome, token_id, shares, avg_cost, ts = row
            if shares > 0:
                self.inventory[(slug, outcome)] = InventoryEntry(
                    slug=slug, outcome=outcome, token_id=token_id,
                    shares=shares, avg_cost=avg_cost, last_update_ts=ts,
                )

    def _load_open_orders(self) -> None:
        for row in self._conn.execute(
            "SELECT order_id, client_order_id, slug, outcome, token_id, side, price, size, created_ts FROM open_orders"
        ):
            oid, cid, slug, outcome, tid, side, price, size, cts = row
            self.open_orders[oid] = OpenOrder(
                order_id=oid, client_order_id=cid,
                slug=slug, outcome=outcome, token_id=tid,
                side=side, price=price, size=size, created_ts=cts,
            )
        # Load used client IDs
        for (cid,) in self._conn.execute("SELECT client_id FROM used_client_ids"):
            self._used_client_ids.add(cid)

    # ------------------------------------------------------------------
    # Inventory operations
    # ------------------------------------------------------------------
    def get_inventory(self, slug: str, outcome: str) -> InventoryEntry | None:
        return self.inventory.get((slug, outcome))

    def apply_buy_fill(self, slug: str, outcome: str, token_id: str,
                       qty: float, price: float) -> InventoryEntry:
        key = (slug, outcome)
        inv = self.inventory.get(key)
        if inv is None:
            inv = InventoryEntry(
                slug=slug, outcome=outcome, token_id=token_id,
                shares=0.0, avg_cost=0.0, last_update_ts=_utc_iso(),
            )
            self.inventory[key] = inv
        inv.apply_buy(qty, price)
        self._persist_inventory(inv)
        return inv

    def apply_sell_fill(self, slug: str, outcome: str, qty: float,
                        sell_price: float = 0.0,
                        fee_bps: float = 0.0) -> InventoryEntry | None:
        key = (slug, outcome)
        inv = self.inventory.get(key)
        if inv is None:
            return None
        # Compute realized PnL: (sell_price - avg_cost) * qty minus fees
        if sell_price > 0 and inv.avg_cost > 0:
            raw_realized = (sell_price - inv.avg_cost) * qty
            if fee_bps > 0:
                # Deduct round-trip fee: sell-side + proportional buy-side
                fee_rate = fee_bps / 10_000.0
                fee = fee_rate * qty * (sell_price + inv.avg_cost)
                raw_realized -= fee
            self.realized_pnl += raw_realized
        inv.apply_sell(qty)
        if inv.shares <= 0:
            self._delete_inventory(slug, outcome)
            self.inventory.pop(key, None)
            return None
        self._persist_inventory(inv)
        return inv

    def _persist_inventory(self, inv: InventoryEntry) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO inventory
               (slug, outcome, token_id, shares, avg_cost, last_update_ts)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (inv.slug, inv.outcome, inv.token_id,
             inv.shares, inv.avg_cost, inv.last_update_ts),
        )
        self._conn.commit()

    def _delete_inventory(self, slug: str, outcome: str) -> None:
        self._conn.execute(
            "DELETE FROM inventory WHERE slug=? AND outcome=?", (slug, outcome)
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Open order operations
    # ------------------------------------------------------------------
    def track_order(self, order: OpenOrder) -> None:
        self.open_orders[order.order_id] = order
        self._used_client_ids.add(order.client_order_id)
        self._conn.execute(
            """INSERT OR REPLACE INTO open_orders
               (order_id, client_order_id, slug, outcome, token_id, side, price, size, created_ts)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (order.order_id, order.client_order_id, order.slug, order.outcome,
             order.token_id, order.side, order.price, order.size, order.created_ts),
        )
        self._conn.execute(
            "INSERT OR IGNORE INTO used_client_ids (client_id) VALUES (?)",
            (order.client_order_id,),
        )
        self._conn.commit()

    def remove_order(self, order_id: str) -> OpenOrder | None:
        order = self.open_orders.pop(order_id, None)
        if order is not None:
            self._conn.execute("DELETE FROM open_orders WHERE order_id=?", (order_id,))
            self._conn.commit()
        return order

    def is_client_id_used(self, client_id: str) -> bool:
        return client_id in self._used_client_ids

    # ------------------------------------------------------------------
    # Shadow order operations (for touch fill mode)
    # ------------------------------------------------------------------
    def _load_shadow_orders(self) -> None:
        try:
            for row in self._conn.execute(
                "SELECT order_id, slug, outcome, token_id, side, price, qty, "
                "timestamp, expiry_ts, client_order_id, was_crossing FROM shadow_orders"
            ):
                oid, slug, outcome, tid, side, price, qty, ts, exp, cid, crossing = row
                self.shadow_orders[oid] = ShadowOrder(
                    order_id=oid, slug=slug, outcome=outcome, token_id=tid,
                    side=side, price=price, qty=qty, timestamp=ts,
                    expiry_ts=exp, client_order_id=cid, was_crossing=bool(crossing),
                )
        except sqlite3.OperationalError:
            # Table may not exist yet on first run with older DB
            pass

    def track_shadow_order(self, shadow: ShadowOrder) -> None:
        self.shadow_orders[shadow.order_id] = shadow
        self._conn.execute(
            """INSERT OR REPLACE INTO shadow_orders
               (order_id, slug, outcome, token_id, side, price, qty,
                timestamp, expiry_ts, client_order_id, was_crossing)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (shadow.order_id, shadow.slug, shadow.outcome, shadow.token_id,
             shadow.side, shadow.price, shadow.qty, shadow.timestamp,
             shadow.expiry_ts, shadow.client_order_id, int(shadow.was_crossing)),
        )
        self._conn.commit()

    def remove_shadow_order(self, order_id: str) -> ShadowOrder | None:
        shadow = self.shadow_orders.pop(order_id, None)
        if shadow is not None:
            self._conn.execute("DELETE FROM shadow_orders WHERE order_id=?", (order_id,))
            self._conn.commit()
        return shadow

    def get_orders_for_token(self, token_id: str) -> List[OpenOrder]:
        return [o for o in self.open_orders.values() if o.token_id == token_id]

    def get_expired_orders(self, ttl_ms: int) -> List[OpenOrder]:
        cutoff = time.time() - ttl_ms / 1000.0
        return [o for o in self.open_orders.values() if o.created_ts < cutoff]

    # ------------------------------------------------------------------
    # Aggregate queries
    # ------------------------------------------------------------------
    def total_exposure_usd(self) -> float:
        return sum(inv.cost_basis_usd for inv in self.inventory.values())

    def outcome_exposure_usd(self, slug: str, outcome: str) -> float:
        inv = self.inventory.get((slug, outcome))
        return inv.cost_basis_usd if inv else 0.0

    def inventory_snapshot(self) -> dict:
        """Return dict of (slug, outcome) -> {shares, avg_cost, usd_value}."""
        snap = {}
        for (slug, outcome), inv in self.inventory.items():
            snap[f"{slug}:{outcome}"] = {
                "shares": round(inv.shares, 2),
                "avg_cost": round(inv.avg_cost, 4),
                "usd_value": round(inv.cost_basis_usd, 4),
            }
        return snap

    # ------------------------------------------------------------------
    # LIVE risk halt persistence
    # ------------------------------------------------------------------
    def set_risk_halt(self, key: str, value: str) -> None:
        """Persist a risk halt key-value pair to SQLite."""
        self._conn.execute(
            "INSERT OR REPLACE INTO live_risk_halt (key, value) VALUES (?, ?)",
            (key, value),
        )
        self._conn.commit()

    def get_risk_halt(self, key: str) -> str | None:
        """Read a persisted risk halt value."""
        try:
            row = self._conn.execute(
                "SELECT value FROM live_risk_halt WHERE key=?", (key,)
            ).fetchone()
            return row[0] if row else None
        except sqlite3.OperationalError:
            return None

    def clear_risk_halt(self, key: str) -> None:
        """Remove a persisted risk halt key."""
        self._conn.execute("DELETE FROM live_risk_halt WHERE key=?", (key,))
        self._conn.commit()

    # ------------------------------------------------------------------
    # Diagnostics persistence (hourly PnL buckets + counters)
    # ------------------------------------------------------------------
    def diag_set_hourly_pnl(self, hour_et: str, pnl: float) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO diag_hourly_pnl (hour_et, realized_pnl) VALUES (?, ?)",
            (hour_et, pnl),
        )
        self._conn.commit()

    def diag_load_hourly_pnl(self) -> Dict[str, float]:
        result: Dict[str, float] = {}
        try:
            for row in self._conn.execute(
                "SELECT hour_et, realized_pnl FROM diag_hourly_pnl ORDER BY hour_et"
            ):
                result[row[0]] = row[1]
        except sqlite3.OperationalError:
            pass
        return result

    def diag_set_counter(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO diag_counters (key, value) VALUES (?, ?)",
            (key, value),
        )
        self._conn.commit()

    def diag_get_counter(self, key: str) -> str | None:
        try:
            row = self._conn.execute(
                "SELECT value FROM diag_counters WHERE key=?", (key,)
            ).fetchone()
            return row[0] if row else None
        except sqlite3.OperationalError:
            return None

    # Copywallet hourly PnL persistence
    def diag_set_copywallet_hourly_pnl(self, hour_et: str, pnl: float) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO diag_copywallet_hourly_pnl "
            "(hour_et, realized_pnl) VALUES (?, ?)",
            (hour_et, pnl),
        )
        self._conn.commit()

    def diag_load_copywallet_hourly_pnl(self) -> Dict[str, float]:
        result: Dict[str, float] = {}
        try:
            for row in self._conn.execute(
                "SELECT hour_et, realized_pnl FROM diag_copywallet_hourly_pnl "
                "ORDER BY hour_et"
            ):
                result[row[0]] = row[1]
        except sqlite3.OperationalError:
            pass
        return result

    # ------------------------------------------------------------------
    # Flush / close
    # ------------------------------------------------------------------
    def flush(self) -> None:
        """Explicit flush (WAL mode auto-commits, but this ensures sync)."""
        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

    def close(self) -> None:
        self.flush()
        self._conn.close()
