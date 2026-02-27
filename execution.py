"""
execution.py – Order management with TTL-based cancel/replace.

Responsibilities:
  - Submit orders (BUY/SELL) via PolymarketAPI
  - Track open orders with TTL; cancel stale orders
  - Enforce MAX_OPEN_ORDERS_PER_MARKET
  - Ensure idempotent client_order_id
  - Process fills to update state
  - Support DRY_RUN (log-only) and LIVE modes
  - Rate-limit total order ops per loop
"""
from __future__ import annotations

import time
import uuid
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

from config import Config
from logger import Logger
from polymarket_api import PolymarketAPI
from state import OpenOrder, StateManager
from strategy import TradeAction


class ExecutionEngine:
    """Manages the order lifecycle: submit, track, cancel/replace, fill."""

    def __init__(self, cfg: Config, api: PolymarketAPI,
                 state: StateManager, logger: Logger):
        self.cfg = cfg
        self.api = api
        self.state = state
        self.log = logger

        # Track ops this loop iteration (reset each tick)
        self._ops_this_tick = 0

        # Per-token cooldown: (slug, outcome) -> epoch of last fill
        self._last_fill_by_token: Dict[Tuple[str, str], float] = {}

        # Cancel/replace rate limiter: sliding window of cancel timestamps
        self._cancel_timestamps: Deque[float] = deque()
        self._last_cancel_ts: float = 0.0

        # Fills cursor for incremental polling
        self._last_fill_ts: float = time.time() - 300  # start 5 min ago

    # ------------------------------------------------------------------
    # Main entry point: process a batch of actions
    # ------------------------------------------------------------------
    def execute_actions(self, actions: List[TradeAction]) -> None:
        """Execute a batch of trade actions, respecting rate limits."""
        self._ops_this_tick = 0

        # 1. Cancel expired orders first
        self._cancel_expired()

        # 2. Process sells before buys (risk reduction first)
        sells = [a for a in actions if a.action == "SELL"]
        buys = [a for a in actions if a.action == "BUY"]

        for action in sells:
            if self._ops_this_tick >= self.cfg.MAX_ORDER_OPS_PER_LOOP:
                break
            self._execute_one(action)

        for action in buys:
            if self._ops_this_tick >= self.cfg.MAX_ORDER_OPS_PER_LOOP:
                break
            self._execute_one(action)

    def _execute_one(self, action: TradeAction) -> None:
        """Execute a single trade action."""
        token_id = action.token_id
        now = time.time()

        # Per-token cooldown: skip if recently filled on this (slug, outcome)
        key = (action.slug, action.outcome)
        last_fill = self._last_fill_by_token.get(key, 0.0)
        if now - last_fill < self.cfg.PER_TOKEN_COOLDOWN_SEC:
            self.log.decision(
                action="SKIP", reason="per_token_cooldown",
                slug=action.slug, outcome=action.outcome,
                cooldown_remaining=round(self.cfg.PER_TOKEN_COOLDOWN_SEC - (now - last_fill), 2),
            )
            return

        # Check open order limit for this token
        existing = self.state.get_orders_for_token(token_id)
        if len(existing) >= self.cfg.MAX_OPEN_ORDERS_PER_MARKET:
            self.log.decision(
                action="SKIP", reason="max_open_orders",
                slug=action.slug, outcome=action.outcome,
                existing_orders=len(existing),
            )
            return

        # Skip if existing order at nearly the same price (avoids churn)
        for ex_order in existing:
            if ex_order.side == action.action:
                price_diff = abs(ex_order.price - action.price)
                if price_diff < self.cfg.MIN_PRICE_CHANGE_FOR_REPLACE:
                    self.log.decision(
                        action="SKIP", reason="price_unchanged_for_replace",
                        slug=action.slug, outcome=action.outcome,
                        existing_price=ex_order.price,
                        desired_price=action.price,
                        diff=round(price_diff, 4),
                        threshold=self.cfg.MIN_PRICE_CHANGE_FOR_REPLACE,
                    )
                    return

        # Prevent sell-to-open: verify inventory before selling
        if action.action == "SELL":
            inv = self.state.get_inventory(action.slug, action.outcome)
            if inv is None or inv.shares < action.size_shares:
                self.log.decision(
                    action="SKIP", reason="no_inventory_for_sell",
                    slug=action.slug, outcome=action.outcome,
                    requested=action.size_shares,
                    available=inv.shares if inv else 0,
                )
                return

        # Clamp price to 2 decimals within [PRICE_MIN, PRICE_MAX]
        action.price = round(action.price, 2)
        action.price = max(self.cfg.PRICE_MIN, min(action.price, self.cfg.PRICE_MAX))

        # Round shares down to integer; reject if below minimum
        action.size_shares = int(action.size_shares)
        if action.size_shares < self.cfg.MIN_ORDER_SHARES:
            self.log.decision(
                action="SKIP", reason="below_min_order_size",
                slug=action.slug, outcome=action.outcome,
                size_shares=action.size_shares,
                min_required=self.cfg.MIN_ORDER_SHARES,
            )
            return

        # Generate idempotent client order ID
        client_id = str(uuid.uuid4())
        if self.state.is_client_id_used(client_id):
            client_id = str(uuid.uuid4())  # extremely unlikely collision

        # Place order
        try:
            if action.action == "BUY":
                result = self.api.place_limit_buy(
                    token_id=token_id,
                    price=action.price,
                    size_shares=action.size_shares,
                    client_order_id=client_id,
                )
            elif action.action == "SELL":
                result = self.api.place_limit_sell(
                    token_id=token_id,
                    price=action.price,
                    size_shares=action.size_shares,
                    client_order_id=client_id,
                )
            else:
                return
        except Exception as e:
            self.log.error(
                f"order_submit_failed: {e}",
                slug=action.slug, outcome=action.outcome,
                action=action.action,
            )
            self._ops_this_tick += 1
            return

        order_id = result.get("order_id", "")
        self._ops_this_tick += 1

        # Track the order in state
        order = OpenOrder(
            order_id=order_id,
            client_order_id=client_id,
            slug=action.slug,
            outcome=action.outcome,
            token_id=token_id,
            side=action.action,
            price=action.price,
            size=action.size_shares,
            created_ts=time.time(),
        )
        self.state.track_order(order)

        self.log.order_place(
            order_id=order_id,
            client_order_id=client_id,
            slug=action.slug,
            outcome=action.outcome,
            side=action.action,
            price=action.price,
            size=action.size_shares,
            usd=action.size_usd,
            reason=action.reason,
            mode=self.cfg.MODE,
        )

        # In DRY_RUN, respect fill mode setting
        if self.cfg.MODE == "DRY_RUN" and self.cfg.DRY_RUN_FILL_MODE == "instant":
            self._simulate_fill(order, action)
        elif self.cfg.MODE == "DRY_RUN" and self.cfg.DRY_RUN_FILL_MODE == "none":
            # Log-only: track order but do NOT mutate inventory
            self.log.info(
                "dry_run_no_fill",
                order_id=order.order_id,
                slug=action.slug,
                outcome=action.outcome,
                side=action.action,
                msg="DRY_RUN_FILL_MODE=none; order tracked but no inventory mutation",
            )

    def _simulate_fill(self, order: OpenOrder, action: TradeAction) -> None:
        """In DRY_RUN mode, immediately simulate a fill."""
        if action.action == "BUY":
            inv = self.state.apply_buy_fill(
                slug=order.slug, outcome=order.outcome,
                token_id=order.token_id,
                qty=order.size, price=order.price,
            )
            self.log.fill(
                order_id=order.order_id, slug=order.slug,
                outcome=order.outcome, side="BUY",
                qty=order.size, price=order.price,
                avg_cost=inv.avg_cost if inv else 0,
                total_shares=inv.shares if inv else 0,
                simulated=True,
            )
        elif action.action == "SELL":
            inv = self.state.apply_sell_fill(order.slug, order.outcome, order.size,
                                             sell_price=order.price)
            self.log.fill(
                order_id=order.order_id, slug=order.slug,
                outcome=order.outcome, side="SELL",
                qty=order.size, price=order.price,
                remaining_shares=inv.shares if inv else 0,
                simulated=True,
            )
        # Record fill timestamp for per-token cooldown
        self._last_fill_by_token[(order.slug, order.outcome)] = time.time()
        self.state.remove_order(order.order_id)

    # ------------------------------------------------------------------
    # Cancel expired orders
    # ------------------------------------------------------------------
    def _cancel_rate_ok(self) -> bool:
        """Check if we can perform another cancel/replace within rate limits."""
        now = time.time()
        # Enforce min interval between cancels
        if now - self._last_cancel_ts < self.cfg.MIN_CANCEL_REPLACE_INTERVAL_SEC:
            return False
        # Enforce global cancels-per-second cap (sliding 1s window)
        cutoff = now - 1.0
        while self._cancel_timestamps and self._cancel_timestamps[0] < cutoff:
            self._cancel_timestamps.popleft()
        if len(self._cancel_timestamps) >= self.cfg.MAX_CANCEL_REPLACE_PER_SEC:
            return False
        return True

    def _record_cancel(self) -> None:
        """Record a cancel/replace operation for rate limiting."""
        now = time.time()
        self._cancel_timestamps.append(now)
        self._last_cancel_ts = now

    def _cancel_expired(self) -> None:
        """Cancel orders older than ORDER_TTL_MS, respecting rate limits."""
        expired = self.state.get_expired_orders(self.cfg.ORDER_TTL_MS)
        for order in expired:
            if self._ops_this_tick >= self.cfg.MAX_ORDER_OPS_PER_LOOP:
                break
            if not self._cancel_rate_ok():
                self.log.info(
                    "cancel_rate_limited",
                    order_id=order.order_id,
                    msg="cancel/replace rate limit hit, deferring",
                )
                break
            try:
                success = self.api.cancel_order(order.order_id)
                if success:
                    self.state.remove_order(order.order_id)
                    self.log.order_cancel(
                        order_id=order.order_id,
                        slug=order.slug,
                        outcome=order.outcome,
                        reason="TTL_expired",
                        age_ms=round((time.time() - order.created_ts) * 1000),
                    )
                self._record_cancel()
                self._ops_this_tick += 1
            except Exception as e:
                self.log.error(
                    f"cancel_failed: {e}",
                    order_id=order.order_id,
                )
                self._ops_this_tick += 1

    # ------------------------------------------------------------------
    # Fill sync (LIVE mode: poll API for fills)
    # ------------------------------------------------------------------
    def sync_fills(self) -> int:
        """Poll for new fills and update inventory.  Returns fill count."""
        if self.cfg.MODE == "DRY_RUN":
            return 0  # fills are simulated immediately

        fills = self.api.get_fills(since_ts=self._last_fill_ts)
        count = 0

        for fill in fills:
            order_id = fill.get("order_id", "") or fill.get("orderId", "")
            side = (fill.get("side", "") or "").upper()
            qty = float(fill.get("size", 0) or fill.get("amount", 0) or 0)
            price = float(fill.get("price", 0) or 0)
            token_id = fill.get("asset_id", "") or fill.get("token_id", "")

            if qty <= 0 or not order_id:
                continue

            # Look up which slug/outcome this fill belongs to
            order = self.state.open_orders.get(order_id)
            if order is None:
                # Try token_id lookup from universe (set by caller)
                continue

            slug = order.slug
            outcome = order.outcome

            if side == "BUY":
                inv = self.state.apply_buy_fill(slug, outcome, token_id, qty, price)
                self.log.fill(
                    order_id=order_id, slug=slug, outcome=outcome,
                    side="BUY", qty=qty, price=price,
                    avg_cost=inv.avg_cost, total_shares=inv.shares,
                )
            elif side == "SELL":
                inv = self.state.apply_sell_fill(slug, outcome, qty, sell_price=price)
                self.log.fill(
                    order_id=order_id, slug=slug, outcome=outcome,
                    side="SELL", qty=qty, price=price,
                    remaining_shares=inv.shares if inv else 0,
                    realized_pnl=round(self.state.realized_pnl, 4),
                )

            # Record fill timestamp for per-token cooldown
            self._last_fill_by_token[(slug, outcome)] = time.time()

            # Remove filled order from tracking
            self.state.remove_order(order_id)
            count += 1

        if fills:
            # Update cursor to most recent fill timestamp
            try:
                latest_ts = max(
                    float(f.get("timestamp", 0) or f.get("matchTime", 0) or 0)
                    for f in fills if f
                )
                if latest_ts > 0:
                    self._last_fill_ts = latest_ts
            except (ValueError, TypeError):
                pass

        return count

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------
    def cancel_all_open(self) -> int:
        """Cancel all tracked open orders.  For graceful shutdown."""
        count = 0
        for order_id in list(self.state.open_orders.keys()):
            try:
                self.api.cancel_order(order_id)
                self.state.remove_order(order_id)
                count += 1
            except Exception:
                pass
        if self.cfg.MODE == "LIVE":
            self.api.cancel_all()  # belt-and-suspenders
        return count
