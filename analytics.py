"""
analytics.py – LIVE-trading structured event logging + analytics.

Emits structured JSON events for:
  - ORDER_INTENT, ORDER_PLACED, ORDER_CANCELED, FILL
  - MINUTE_ROLLUP, HOURLY_PNL (enhanced)

Tracks per-order metadata (entry_style, placement timestamps, book snapshots)
so that FILL events can report passive-vs-crossing, time-to-fill, spread
capture, and realized PnL.

No external dependencies beyond stdlib.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Per-order metadata persisted from intent → fill/cancel
# ---------------------------------------------------------------------------
@dataclass
class OrderMeta:
    """Metadata captured at ORDER_INTENT time, carried through the order lifecycle."""
    client_order_id: str
    order_id: str = ""
    slug: str = ""
    outcome: str = ""
    token_id: str = ""
    side: str = ""

    # Pricing at intent time
    desired_price: float = 0.0
    desired_shares: float = 0.0
    desired_usd: float = 0.0

    # Book snapshot at intent time
    best_bid: float = 0.0
    best_ask: float = 0.0
    mid: float = 0.0
    spread: float = 0.0

    # Binance features at intent
    bin_ret_30s: float = 0.0
    bin_ret_120s: float = 0.0

    # Cadence
    cadence_sec_from_quarter_et: int = 0
    buy_weight: float = 0.0
    sell_weight: float = 0.0

    # Classification
    entry_style: str = ""  # "PASSIVE" or "CROSS"
    reason: str = ""

    # Timestamps
    intent_ts: float = 0.0      # epoch seconds – when intent was created
    placed_ts: float = 0.0      # epoch seconds – when API returned

    # Spread at intent
    spread_cents_at_intent: float = 0.0


# ---------------------------------------------------------------------------
# Analytics tracker
# ---------------------------------------------------------------------------
class AnalyticsTracker:
    """Tracks order metadata and accumulates stats for rollups.

    Designed to be used alongside the existing Logger; emits events
    through a logger reference passed at construction.
    """

    def __init__(self):
        # Live order metadata: client_order_id -> OrderMeta
        self._order_meta: Dict[str, OrderMeta] = {}
        # Also index by order_id (set after placement)
        self._order_meta_by_oid: Dict[str, OrderMeta] = {}

        # Per-position first-open timestamp: (slug, outcome) -> epoch
        self._position_opened_ts: Dict[Tuple[str, str], float] = {}

        # --- Minute rollup accumulators (reset every 60s) ---
        self._minute_orders_placed: int = 0
        self._minute_orders_canceled: int = 0
        self._minute_orders_expired: int = 0
        self._minute_fills_buy: int = 0
        self._minute_fills_sell: int = 0
        self._minute_passive_fills: int = 0
        self._minute_cross_fills: int = 0
        self._minute_realized_pnl: float = 0.0
        self._minute_errors: int = 0
        self._minute_rate_limit_hits: int = 0
        self._last_minute_rollup_ts: float = time.time()

        # --- Hourly PnL accumulators (reset every hour) ---
        self._hour_realized_pnl: float = 0.0
        self._hour_max_equity: float = 0.0
        self._hour_min_equity: float = 0.0
        self._hour_equity_initialized: bool = False

    # ------------------------------------------------------------------
    # ORDER_INTENT
    # ------------------------------------------------------------------
    def record_intent(self, *,
                      client_order_id: str,
                      slug: str, outcome: str, token_id: str, side: str,
                      desired_price: float, desired_shares: float,
                      desired_usd: float,
                      best_bid: float, best_ask: float, mid: float,
                      spread: float,
                      bin_ret_30s: float, bin_ret_120s: float,
                      cadence_sec: int, buy_weight: float, sell_weight: float,
                      reason: str) -> OrderMeta:
        """Record intent and classify PASSIVE vs CROSS."""
        now = time.time()

        # Classify entry style
        if side == "BUY":
            entry_style = "CROSS" if desired_price >= best_ask else "PASSIVE"
        elif side == "SELL":
            entry_style = "CROSS" if desired_price <= best_bid else "PASSIVE"
        else:
            entry_style = "PASSIVE"

        meta = OrderMeta(
            client_order_id=client_order_id,
            slug=slug, outcome=outcome, token_id=token_id, side=side,
            desired_price=desired_price, desired_shares=desired_shares,
            desired_usd=desired_usd,
            best_bid=best_bid, best_ask=best_ask, mid=mid, spread=spread,
            bin_ret_30s=bin_ret_30s, bin_ret_120s=bin_ret_120s,
            cadence_sec_from_quarter_et=cadence_sec,
            buy_weight=buy_weight, sell_weight=sell_weight,
            entry_style=entry_style, reason=reason,
            intent_ts=now,
            spread_cents_at_intent=round(spread * 100, 2),
        )
        self._order_meta[client_order_id] = meta
        return meta

    def intent_dict(self, meta: OrderMeta) -> dict:
        """Build the ORDER_INTENT log payload."""
        return {
            "slug": meta.slug,
            "outcome": meta.outcome,
            "token_id": meta.token_id,
            "side": meta.side,
            "desired_price": meta.desired_price,
            "desired_shares": meta.desired_shares,
            "desired_usd": round(meta.desired_usd, 4),
            "best_bid": meta.best_bid,
            "best_ask": meta.best_ask,
            "mid": round(meta.mid, 4),
            "spread": round(meta.spread, 4),
            "spread_cents": meta.spread_cents_at_intent,
            "bin_ret_30s": round(meta.bin_ret_30s, 6),
            "bin_ret_120s": round(meta.bin_ret_120s, 6),
            "cadence_sec_from_quarter_et": meta.cadence_sec_from_quarter_et,
            "buy_weight": round(meta.buy_weight, 2),
            "sell_weight": round(meta.sell_weight, 2),
            "entry_style": meta.entry_style,
            "reason": meta.reason,
            "client_order_id": meta.client_order_id,
        }

    # ------------------------------------------------------------------
    # ORDER_PLACED
    # ------------------------------------------------------------------
    def record_placed(self, *, client_order_id: str, order_id: str,
                      posted_price: float, posted_shares: float,
                      api_response: dict | None = None) -> dict:
        """Record placement and return ORDER_PLACED payload."""
        now = time.time()
        meta = self._order_meta.get(client_order_id)
        latency_ms = 0.0
        if meta:
            meta.order_id = order_id
            meta.placed_ts = now
            latency_ms = round((now - meta.intent_ts) * 1000, 1)
            self._order_meta_by_oid[order_id] = meta

        self._minute_orders_placed += 1

        payload = {
            "order_id": order_id,
            "client_order_id": client_order_id,
            "posted_price": posted_price,
            "posted_shares": posted_shares,
            "placement_latency_ms": latency_ms,
        }
        if meta:
            payload["slug"] = meta.slug
            payload["outcome"] = meta.outcome
            payload["side"] = meta.side
            payload["entry_style"] = meta.entry_style
        if api_response:
            # Include select API debug fields
            for k in ("status", "orderID", "transactionsHash"):
                if k in api_response:
                    payload[f"api_{k}"] = api_response[k]
        return payload

    # ------------------------------------------------------------------
    # ORDER_CANCELED / ORDER_EXPIRED
    # ------------------------------------------------------------------
    def record_cancel(self, *, order_id: str, client_order_id: str,
                      cancel_reason: str, created_ts: float,
                      best_bid: float = 0.0, best_ask: float = 0.0) -> dict:
        """Record cancellation and return payload."""
        now = time.time()
        time_alive_ms = round((now - created_ts) * 1000, 1)

        if cancel_reason == "ttl_expired":
            self._minute_orders_expired += 1
        else:
            self._minute_orders_canceled += 1

        # Clean up meta
        meta = self._order_meta_by_oid.pop(order_id, None)
        entry_style = ""
        if meta:
            entry_style = meta.entry_style
            self._order_meta.pop(meta.client_order_id, None)

        return {
            "order_id": order_id,
            "client_order_id": client_order_id,
            "cancel_reason": cancel_reason,
            "time_alive_ms": time_alive_ms,
            "last_seen_best_bid": best_bid,
            "last_seen_best_ask": best_ask,
            "entry_style": entry_style,
        }

    # ------------------------------------------------------------------
    # FILL
    # ------------------------------------------------------------------
    def record_fill(self, *, order_id: str, client_order_id: str,
                    slug: str, outcome: str, token_id: str, side: str,
                    fill_price: float, fill_shares: float,
                    best_bid: float = 0.0, best_ask: float = 0.0,
                    mid: float = 0.0, spread: float = 0.0,
                    bin_ret_30s: float = 0.0,
                    avg_cost_before_sell: float = 0.0,
                    position_shares_after: float = 0.0,
                    realized_pnl: float = 0.0) -> dict:
        """Record fill event and return FILL payload with analytics fields."""
        now = time.time()
        fill_usd = round(fill_shares * fill_price, 4)

        # Retrieve intent metadata
        meta = self._order_meta_by_oid.get(order_id)
        if meta is None:
            meta = self._order_meta.get(client_order_id)

        entry_style = meta.entry_style if meta else "UNKNOWN"
        time_to_fill_ms = 0.0
        spread_cents_at_placement = 0.0
        if meta and meta.placed_ts > 0:
            time_to_fill_ms = round((now - meta.placed_ts) * 1000, 1)
            spread_cents_at_placement = meta.spread_cents_at_intent

        # Edge vs mid
        if side == "BUY":
            edge_vs_mid = round(mid - fill_price, 4) if mid > 0 else 0.0
            self._minute_fills_buy += 1
        else:
            edge_vs_mid = round(fill_price - mid, 4) if mid > 0 else 0.0
            self._minute_fills_sell += 1

        if entry_style == "PASSIVE":
            self._minute_passive_fills += 1
        elif entry_style == "CROSS":
            self._minute_cross_fills += 1

        spread_cents_at_fill = round(spread * 100, 2)

        payload: dict = {
            "order_id": order_id,
            "client_order_id": client_order_id,
            "slug": slug,
            "outcome": outcome,
            "token_id": token_id,
            "side": side,
            "fill_price": fill_price,
            "fill_shares": fill_shares,
            "fill_usd": fill_usd,
            "fill_source": "live_sync",
            "time_to_fill_ms": time_to_fill_ms,
            "entry_style": entry_style,
            "edge_vs_mid": edge_vs_mid,
            "spread_cents_at_placement": spread_cents_at_placement,
            "spread_cents_at_fill": spread_cents_at_fill,
            "snapshot_at_fill": {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "mid": round(mid, 4),
                "spread": round(spread, 4),
            },
            "bin_ret_30s": round(bin_ret_30s, 6),
        }

        # Sell-specific fields: realized PnL and holding time
        if side == "SELL":
            payload["avg_cost_before_sell"] = round(avg_cost_before_sell, 4)
            payload["realized_pnl"] = round(realized_pnl, 4)
            payload["position_shares_after"] = round(position_shares_after, 2)
            self._minute_realized_pnl += realized_pnl
            self._hour_realized_pnl += realized_pnl

            # Holding time approximation
            key = (slug, outcome)
            opened_ts = self._position_opened_ts.get(key)
            if opened_ts:
                payload["holding_time_sec"] = round(now - opened_ts, 1)
            if position_shares_after <= 0:
                self._position_opened_ts.pop(key, None)

        elif side == "BUY":
            # Track position open time
            key = (slug, outcome)
            if key not in self._position_opened_ts:
                self._position_opened_ts[key] = now

        # Clean up meta
        if meta:
            self._order_meta_by_oid.pop(order_id, None)
            self._order_meta.pop(meta.client_order_id, None)

        return payload

    # ------------------------------------------------------------------
    # Error / rate-limit tracking
    # ------------------------------------------------------------------
    def record_error(self) -> None:
        self._minute_errors += 1

    def record_rate_limit_hit(self) -> None:
        self._minute_rate_limit_hits += 1

    # ------------------------------------------------------------------
    # Equity tracking (for drawdown)
    # ------------------------------------------------------------------
    def update_equity(self, equity: float) -> None:
        """Track equity high/low for hourly drawdown calculation."""
        if not self._hour_equity_initialized:
            self._hour_max_equity = equity
            self._hour_min_equity = equity
            self._hour_equity_initialized = True
        else:
            self._hour_max_equity = max(self._hour_max_equity, equity)
            self._hour_min_equity = min(self._hour_min_equity, equity)

    # ------------------------------------------------------------------
    # MINUTE_ROLLUP
    # ------------------------------------------------------------------
    def maybe_minute_rollup(self, *, inventory_notional: float = 0.0,
                            top_positions: list | None = None,
                            now: float | None = None) -> dict | None:
        """Return MINUTE_ROLLUP payload if 60s elapsed, else None."""
        now = now or time.time()
        elapsed = now - self._last_minute_rollup_ts
        if elapsed < 60.0:
            return None

        total_fills = self._minute_fills_buy + self._minute_fills_sell
        passive_pct = 0.0
        cross_pct = 0.0
        if total_fills > 0:
            passive_pct = round(self._minute_passive_fills / total_fills * 100, 1)
            cross_pct = round(self._minute_cross_fills / total_fills * 100, 1)

        payload = {
            "period_sec": round(elapsed, 1),
            "orders_placed": self._minute_orders_placed,
            "orders_canceled": self._minute_orders_canceled,
            "orders_expired": self._minute_orders_expired,
            "cancels_per_min": self._minute_orders_canceled + self._minute_orders_expired,
            "fills_buy": self._minute_fills_buy,
            "fills_sell": self._minute_fills_sell,
            "passive_fills": self._minute_passive_fills,
            "cross_fills": self._minute_cross_fills,
            "passive_fill_pct": passive_pct,
            "cross_fill_pct": cross_pct,
            "realized_pnl_1m": round(self._minute_realized_pnl, 4),
            "inventory_notional": round(inventory_notional, 4),
            "error_count": self._minute_errors,
            "rate_limit_hits": self._minute_rate_limit_hits,
        }
        if top_positions:
            payload["top_positions"] = top_positions

        # Reset accumulators
        self._minute_orders_placed = 0
        self._minute_orders_canceled = 0
        self._minute_orders_expired = 0
        self._minute_fills_buy = 0
        self._minute_fills_sell = 0
        self._minute_passive_fills = 0
        self._minute_cross_fills = 0
        self._minute_realized_pnl = 0.0
        self._minute_errors = 0
        self._minute_rate_limit_hits = 0
        self._last_minute_rollup_ts = now

        return payload

    # ------------------------------------------------------------------
    # HOURLY_PNL (enhanced)
    # ------------------------------------------------------------------
    def get_and_reset_hourly_analytics(self, *,
                                       unrealized_start: float,
                                       unrealized_end: float) -> dict:
        """Return enhanced hourly analytics fields and reset."""
        net = self._hour_realized_pnl + (unrealized_end - unrealized_start)
        drawdown = 0.0
        if self._hour_equity_initialized and self._hour_max_equity > 0:
            drawdown = round(self._hour_max_equity - self._hour_min_equity, 4)

        result = {
            "analytics_realized_pnl_hour": round(self._hour_realized_pnl, 4),
            "drawdown_in_hour": drawdown,
        }

        # Reset
        self._hour_realized_pnl = 0.0
        self._hour_max_equity = 0.0
        self._hour_min_equity = 0.0
        self._hour_equity_initialized = False

        return result

    # ------------------------------------------------------------------
    # Lookup helpers
    # ------------------------------------------------------------------
    def get_meta(self, order_id: str = "",
                 client_order_id: str = "") -> OrderMeta | None:
        """Retrieve OrderMeta by order_id or client_order_id."""
        if order_id:
            meta = self._order_meta_by_oid.get(order_id)
            if meta:
                return meta
        if client_order_id:
            return self._order_meta.get(client_order_id)
        return None

    def cleanup_stale(self, max_age_sec: float = 300.0) -> int:
        """Remove metadata for orders older than max_age_sec. Returns count removed."""
        now = time.time()
        stale_cids = [
            cid for cid, m in self._order_meta.items()
            if now - m.intent_ts > max_age_sec
        ]
        for cid in stale_cids:
            meta = self._order_meta.pop(cid, None)
            if meta and meta.order_id:
                self._order_meta_by_oid.pop(meta.order_id, None)
        return len(stale_cids)
