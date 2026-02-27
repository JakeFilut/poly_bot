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

import random
import time
import uuid
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

from config import Config
from logger import Logger
from polymarket_api import BookSnapshot, PolymarketAPI
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

        # Features engine reference (set externally after construction)
        self._features = None  # type: Optional[object]

        # Self-test: force-fill counter (decremented on each forced fill)
        self._selftest_remaining: int = 0

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

        # Cancel/replace: check existing same-side orders
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

                # Price changed enough — cancel the stale order before replacing
                if not self._cancel_rate_ok():
                    self.log.decision(
                        action="SKIP", reason="cancel_rate_limited_for_replace",
                        slug=action.slug, outcome=action.outcome,
                        stale_order_id=ex_order.order_id,
                    )
                    return

                try:
                    success = self.api.cancel_order(ex_order.order_id)
                    now_cancel = time.time()
                    if success:
                        self.state.remove_order(ex_order.order_id)
                        self.log.order_cancel(
                            order_id=ex_order.order_id,
                            slug=action.slug,
                            outcome=action.outcome,
                            reason="replace_stale_price",
                            old_price=ex_order.price,
                            new_price=action.price,
                        )
                    else:
                        self.log.error(
                            "cancel_for_replace_rejected",
                            order_id=ex_order.order_id,
                            slug=action.slug,
                        )
                    # Always update rate-limit trackers (even on failure,
                    # the API call was attempted and counts toward limits)
                    self._last_cancel_ts = now_cancel
                    self._cancel_timestamps.append(now_cancel)
                    self._ops_this_tick += 1
                except Exception as e:
                    now_cancel = time.time()
                    self.log.error(
                        f"cancel_for_replace_failed: {e}",
                        order_id=ex_order.order_id,
                        slug=action.slug,
                    )
                    # Rate-limit even on exception — the attempt was made
                    self._last_cancel_ts = now_cancel
                    self._cancel_timestamps.append(now_cancel)
                    self._ops_this_tick += 1
                    return  # don't place replacement if cancel errored

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
        if self.cfg.MODE == "DRY_RUN":
            if self.cfg.DRY_RUN_FILL_MODE == "instant":
                self._simulate_fill(order, reason="instant_fill",
                                    fill_mode="instant")
            elif self.cfg.DRY_RUN_FILL_MODE == "probabilistic":
                if self._selftest_remaining > 0:
                    self._simulate_fill(order, reason="selftest_forced",
                                        fill_mode="selftest")
                    self._selftest_remaining -= 1
                else:
                    self._try_probabilistic_fill_at_placement(order)
            elif self.cfg.DRY_RUN_FILL_MODE == "none":
                # Log-only: track order but do NOT mutate inventory
                self.log.info(
                    "DRY_RUN_FILL_MODE=none; order tracked but no inventory mutation",
                    order_id=order.order_id,
                    slug=action.slug,
                    outcome=action.outcome,
                    side=action.action,
                )

    # ------------------------------------------------------------------
    # DRY_RUN fill simulation
    # ------------------------------------------------------------------
    def _simulate_fill(self, order: OpenOrder, reason: str = "simulated",
                       fill_mode: str = "instant") -> None:
        """Simulate a fill in DRY_RUN mode.

        Calls the SAME state pipeline as real fills:
          - state.apply_buy_fill / apply_sell_fill  (inventory + SQLite)
          - state.remove_order                       (open orders cleanup)
          - per-token cooldown update
        Emits a DRY_FILL log event with all required fields.
        """
        usd = round(order.size * order.price, 4)

        if order.side == "BUY":
            inv = self.state.apply_buy_fill(
                slug=order.slug, outcome=order.outcome,
                token_id=order.token_id,
                qty=order.size, price=order.price,
            )
            self.log.dry_fill(
                slug=order.slug, outcome=order.outcome,
                token_id=order.token_id, side="BUY",
                price=order.price, qty_shares=order.size, usd=usd,
                reason=reason, fill_mode=fill_mode,
                order_id=order.order_id,
                avg_cost=inv.avg_cost if inv else 0,
                total_shares=inv.shares if inv else 0,
            )
        elif order.side == "SELL":
            inv = self.state.apply_sell_fill(
                order.slug, order.outcome, order.size,
                sell_price=order.price,
            )
            self.log.dry_fill(
                slug=order.slug, outcome=order.outcome,
                token_id=order.token_id, side="SELL",
                price=order.price, qty_shares=order.size, usd=usd,
                reason=reason, fill_mode=fill_mode,
                order_id=order.order_id,
                remaining_shares=inv.shares if inv else 0,
                realized_pnl=round(self.state.realized_pnl, 4),
            )

        # Record fill timestamp for per-token cooldown
        self._last_fill_by_token[(order.slug, order.outcome)] = time.time()
        self.state.remove_order(order.order_id)

    # ------------------------------------------------------------------
    # Probabilistic fill logic
    # ------------------------------------------------------------------
    def _try_probabilistic_fill_at_placement(self, order: OpenOrder) -> None:
        """At placement time, fill crossing orders with high probability."""
        book = self._get_book_for_order(order)
        if book is None:
            return

        is_crossing = False
        if order.side == "BUY" and order.price >= book.best_ask:
            is_crossing = True
        elif order.side == "SELL" and order.price <= book.best_bid:
            is_crossing = True

        if is_crossing:
            prob = self._compute_fill_probability(order, book)
            if random.random() < prob:
                self._simulate_fill(order, reason="crossing_spread",
                                    fill_mode="probabilistic")

    def _check_probabilistic_fills(self) -> int:
        """Check all pending DRY_RUN orders for probabilistic fills.

        Called each tick from sync_fills().  Passive orders get a low but
        non-zero chance each tick; crossing orders that survived placement
        get a high chance.
        """
        count = 0
        for order_id in list(self.state.open_orders.keys()):
            order = self.state.open_orders.get(order_id)
            if order is None:
                continue

            # Self-test: force-fill unconditionally
            if self._selftest_remaining > 0:
                self._simulate_fill(order, reason="selftest_forced",
                                    fill_mode="selftest")
                self._selftest_remaining -= 1
                count += 1
                continue

            book = self._get_book_for_order(order)
            if book is None:
                continue

            prob = self._compute_fill_probability(order, book)
            if random.random() < prob:
                reason = "passive_filled"
                if order.side == "BUY" and order.price >= book.best_ask:
                    reason = "crossing_spread"
                elif order.side == "SELL" and order.price <= book.best_bid:
                    reason = "crossing_spread"
                self._simulate_fill(order, reason=reason,
                                    fill_mode="probabilistic")
                count += 1

        return count

    def _get_book_for_order(self, order: OpenOrder) -> Optional[BookSnapshot]:
        """Get order book snapshot for an order's token.

        Uses the features-engine cache (sub-second freshness) when available,
        otherwise falls back to a direct API fetch.
        """
        if self._features is not None:
            book = self._features._book_cache.get(order.token_id)
            if book is not None:
                return book
        return self.api.get_orderbook(order.token_id)

    def _compute_fill_probability(self, order: OpenOrder,
                                  book: BookSnapshot) -> float:
        """Per-tick fill probability based on order vs. book.

        Crossing orders  (BUY >= best_ask, SELL <= best_bid):  0.85 – 0.95
        Passive at touch (BUY ≈ best_bid, SELL ≈ best_ask):   0.05 – 0.25
          - tighter spread  → higher prob
          - imbalance favoring fill → higher prob
        Deep passive     (away from touch):                    0.02
        """
        spread = book.spread

        if order.side == "BUY":
            if order.price >= book.best_ask:
                # Crossing the spread — very high fill probability
                return 0.90
            elif abs(order.price - book.best_bid) < 0.005:
                # At or near top-of-book bid — passive fill
                base = 0.10
                if spread <= 0.01:
                    base = 0.20
                elif spread <= 0.02:
                    base = 0.14
                # Imbalance: more ask-side size means sellers willing to cross
                if book.bid_size > 0 and book.ask_size > 0:
                    imbalance = book.ask_size / (book.bid_size + book.ask_size)
                    base *= (0.5 + imbalance)
                return min(base, 0.25)
            else:
                return 0.02

        elif order.side == "SELL":
            if order.price <= book.best_bid:
                # Crossing — very high fill probability
                return 0.90
            elif abs(order.price - book.best_ask) < 0.005:
                # At or near top-of-book ask — passive fill
                base = 0.10
                if spread <= 0.01:
                    base = 0.20
                elif spread <= 0.02:
                    base = 0.14
                # Imbalance: more bid-side size means buyers willing to cross
                if book.bid_size > 0 and book.ask_size > 0:
                    imbalance = book.bid_size / (book.bid_size + book.ask_size)
                    base *= (0.5 + imbalance)
                return min(base, 0.25)
            else:
                return 0.02

        return 0.0

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
                # Still record the cancel attempt for rate limiting
                self._record_cancel()
                self._ops_this_tick += 1

    # ------------------------------------------------------------------
    # Fill sync (LIVE mode: poll API for fills)
    # ------------------------------------------------------------------
    def sync_fills(self) -> int:
        """Poll for new fills and update inventory.  Returns fill count."""
        if self.cfg.MODE == "DRY_RUN":
            if self.cfg.DRY_RUN_FILL_MODE == "probabilistic":
                return self._check_probabilistic_fills()
            return 0  # instant fills handled at placement; none = no fills

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
