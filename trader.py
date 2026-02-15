"""
Simplified arbitrage trader - fast execution with smart buffers.
"""

import math
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Tuple

from config import (
    MIN_BET_DOLLARS,
    MAX_TRADE_QTY,
    ORDER_CHECK_INTERVAL,
    MAX_WAIT_FOR_FILL,
    MIN_EXECUTE_EDGE_PCT,
    PRICE_BUFFER_START,
    PRICE_BUFFER_INCREMENT,
    PRICE_BUFFER_MAX,
    POLY_BUFFER_START,
    calc_kalshi_fee,
    calc_poly_fee,
    GAS_FEE_DOLLARS,
    MAX_LOSS_PCT,
    MAX_LOSS_PCT_TESTING,
    # TESTING mode position sizing
    TESTING_MAX_COST_PER_TRADE,
    TESTING_BALANCE_RISK_PCT,
    TESTING_SOL_SIZE_MULTIPLIER,
    TESTING_SIZE_SCALE,
)
from kalshi_client import KalshiClient
from polymarket_client import PolymarketClient
from models import Snapshot
from logging_utils import append_pnl_summary, get_session_stats, log_trade_row
from utils import now_iso, now_mst


@dataclass
class TradeStats:
    """Track trading statistics."""
    scans: int = 0
    trade_attempts: int = 0
    successful_trades: int = 0
    partial_fills: int = 0
    failed_attempts: int = 0
    total_invested: float = 0.0
    realized_pnl: float = 0.0
    unwind_losses: float = 0.0
    kalshi_start_balance: float = 0.0
    poly_start_balance: float = 0.0
    cached_kalshi_balance: float = 0.0  # Cached balance (updated after trades)
    cached_poly_balance: float = 0.0    # Cached balance (updated after trades)
    redeemed_amount: float = 0.0
    redemption_gas_fees: float = 0.0
    opportunities_found: int = 0  # Count of logged opportunities (for logging modes)
    last_filled_qty: int = 0      # Contracts filled in the most recent trade


class ArbTrader:
    """Simple, fast arbitrage execution."""

    def __init__(self, kalshi: KalshiClient, poly: PolymarketClient):
        self.kalshi = kalshi
        self.poly = poly
        self.stats = TradeStats()
        # Fetch balances once at startup and cache them
        self.stats.kalshi_start_balance = kalshi.get_balance() or 0.0
        self.stats.poly_start_balance = poly.get_balance() or 0.0
        self.stats.cached_kalshi_balance = self.stats.kalshi_start_balance
        self.stats.cached_poly_balance = self.stats.poly_start_balance

    def _calc_qty(self, k_price: float, p_price: float) -> int:
        """Calculate quantity ensuring minimum $1 per side."""
        k_min = math.ceil(MIN_BET_DOLLARS / k_price) if k_price > 0 else 1
        p_min = math.ceil(MIN_BET_DOLLARS / p_price) if p_price > 0 else 1
        return min(max(k_min, p_min), MAX_TRADE_QTY)

    def _calc_testing_qty(self, k_price: float, p_price: float, crypto: str, raw_edge: float) -> int:
        """
        Calculate position size for TESTING mode with risk management.

        Rules:
        - Max cost per trade: $25
        - Max risk: 2% of balance
        - SOL trades at 70% of BTC size
        - Edge-based scaling: small (<4%) = 1x, medium (4-8%) = 1.5x, large (8%+) = 2x
        """
        # Get current balance for risk calculation
        k_bal, p_bal = self.get_balances()
        total_balance = k_bal + p_bal

        # Calculate cost per contract
        cost_per_contract = k_price + p_price

        if cost_per_contract <= 0:
            return 1

        # Step 1: Calculate base quantity from max cost per trade
        max_cost_qty = int(TESTING_MAX_COST_PER_TRADE / cost_per_contract)

        # Step 2: Calculate quantity from balance risk (2% of total)
        max_risk_dollars = total_balance * TESTING_BALANCE_RISK_PCT
        risk_qty = int(max_risk_dollars / cost_per_contract)

        # Step 3: Use the smaller of the two (conservative)
        base_qty = min(max_cost_qty, risk_qty)

        # Step 4: Apply edge-based scaling
        if raw_edge >= 8.0:
            scale = TESTING_SIZE_SCALE.get("large", 2.0)
        elif raw_edge >= 4.0:
            scale = TESTING_SIZE_SCALE.get("medium", 1.5)
        else:
            scale = TESTING_SIZE_SCALE.get("small", 1.0)

        scaled_qty = int(base_qty * scale)

        # Step 5: Apply SOL size reduction
        if crypto in ("SOL", "XRP"):
            scaled_qty = int(scaled_qty * TESTING_SOL_SIZE_MULTIPLIER)

        # Step 6: Ensure minimum viable quantity (need at least $1 per side for each platform)
        min_qty = self._calc_qty(k_price, p_price)

        # Step 7: Apply absolute max and ensure at least minimum
        final_qty = max(min_qty, min(scaled_qty, MAX_TRADE_QTY))

        return final_qty

    def _calc_edge(self, k_price: float, p_price: float, qty: int, buffer: float = 0.0) -> Tuple[float, float, Dict]:
        """Calculate edge and profit at given prices with detailed breakdown.

        Returns:
            Tuple of (edge_pct, profit, breakdown_dict)
            breakdown_dict contains edge_no_fees, edge_with_fees_gas, edge_with_buffer
        """
        cost = (k_price + p_price) * qty
        # Kalshi: fee = 0.07 × contracts × price × (1-price)
        k_fee = calc_kalshi_fee(k_price, qty)
        # Polymarket: fee curve (lower at extremes, max 1.56% at 50%)
        p_fee = calc_poly_fee(p_price, qty)
        fees_total = k_fee + p_fee
        payout = qty * 1.0

        # Edge without fees
        profit_no_fees = payout - cost
        edge_no_fees = (profit_no_fees / cost) * 100 if cost > 0 else 0

        # Edge with fees and gas
        profit_with_fees_gas = payout - cost - fees_total - GAS_FEE_DOLLARS
        edge_with_fees_gas = (profit_with_fees_gas / cost) * 100 if cost > 0 else 0

        # Edge with fees, gas, and buffer (buffer only for Kalshi, not Poly)
        buffer_cost = buffer * qty  # Only Kalshi gets buffer
        profit_with_buffer = payout - cost - fees_total - GAS_FEE_DOLLARS - buffer_cost
        edge_with_buffer = (profit_with_buffer / cost) * 100 if cost > 0 else 0

        breakdown = {
            "edge_no_fees": edge_no_fees,
            "edge_with_fees_gas": edge_with_fees_gas,
            "edge_with_buffer": edge_with_buffer,
        }

        return edge_with_buffer, profit_with_buffer, breakdown

    def get_balances(self) -> Tuple[float, float]:
        """Get cached balances (no API call)."""
        return self.stats.cached_kalshi_balance, self.stats.cached_poly_balance

    def update_cached_balances(self, k_spent: float = 0.0, p_spent: float = 0.0):
        """Update cached balances after a trade."""
        self.stats.cached_kalshi_balance -= k_spent
        self.stats.cached_poly_balance -= p_spent

    def _check_orphaned_position(self, ticker: str, side: str, expected_qty: int, buy_price: float, pos_before: int = 0) -> Tuple[bool, int, float]:
        """
        Check if we have new fills after a 'NO FILL' that actually filled.

        Args:
            pos_before: Position count BEFORE placing the order (to calculate new fills only)

        Returns:
            Tuple of (has_new_position, new_qty, estimated_price)
        """
        pos = self.kalshi.get_position_for_ticker(ticker)
        if not pos:
            return False, 0, 0.0

        # Check position on our side
        position_val = pos.get("position", 0)

        # Kalshi returns positive for YES, negative for NO
        if side.lower() == "yes":
            pos_now = position_val if position_val > 0 else 0
        else:
            pos_now = abs(position_val) if position_val < 0 else 0

        # Calculate NEW fills only (subtract baseline)
        new_qty = pos_now - pos_before

        if new_qty >= expected_qty:
            print(f"    [VERIFY] Found new fills: {new_qty} {side.upper()} (was {pos_before}, now {pos_now})")
            return True, new_qty, buy_price
        elif new_qty > 0:
            print(f"    [VERIFY] Partial new fills: {new_qty}/{expected_qty} {side.upper()} (was {pos_before}, now {pos_now})")
            return True, new_qty, buy_price

        return False, 0, 0.0

    def _wait_kalshi(self, order_id: str, requested_qty: int, side: str) -> Tuple[bool, Optional[float], int]:
        """Wait for Kalshi order to FULLY fill. Returns (filled, actual_fill_price, actual_fill_qty).

        Only returns True if ALL requested contracts fill (no partial fills).
        """
        start = time.time()
        poll_num = 0
        last_remaining = requested_qty  # Track last known remaining for timeout detection
        last_order_data = None

        while time.time() - start < MAX_WAIT_FOR_FILL:
            poll_num += 1
            resp = self.kalshi.get_order_status(order_id)
            if resp:
                order_data = resp.get("order", resp)
                last_order_data = order_data
                status = order_data.get("status", "").lower()

                # Check fill indicators
                count_filled = order_data.get("count_filled", 0) or 0
                remaining = order_data.get("remaining_count", -1)  # -1 means field not present

                # Calculate filled from remaining (more reliable than count_filled)
                if remaining >= 0:
                    filled_from_remaining = requested_qty - remaining
                    last_remaining = remaining
                else:
                    filled_from_remaining = 0

                # Use whichever is higher - count_filled or calculated from remaining
                actual_filled = max(count_filled, filled_from_remaining)

                # Debug: show what API returned
                elapsed_ms = (time.time() - start) * 1000
                print(f"\n      [poll#{poll_num} {elapsed_ms:.0f}ms] status={status}, filled={count_filled}/{requested_qty}, remaining={remaining}", end="", flush=True)

                is_executed = status in ["executed", "filled", "complete", "closed"]

                # FULL FILL: Either count_filled == requested OR (executed AND remaining == 0) OR remaining == 0
                full_fill_by_count = count_filled >= requested_qty
                full_fill_by_remaining = (is_executed and remaining == 0) or remaining == 0

                if full_fill_by_count or full_fill_by_remaining:
                    actual_qty = actual_filled if actual_filled > 0 else requested_qty

                    # Get actual fill price
                    avg_fill = order_data.get("average_fill_price")
                    yes_price = order_data.get("yes_price")
                    no_price = order_data.get("no_price")

                    if avg_fill:
                        fill_price_cents = avg_fill
                    elif side.lower() == "yes":
                        fill_price_cents = yes_price or order_data.get("price")
                    else:
                        fill_price_cents = no_price or order_data.get("price")

                    fill_price = float(fill_price_cents) / 100.0 if fill_price_cents else None

                    print(f" -> FULL FILL!")
                    return True, fill_price, int(actual_qty)

                # PARTIAL FILL: Some filled but not all - reject and cancel remaining
                # Check both count_filled AND remaining to detect partial fills
                if actual_filled > 0 and actual_filled < requested_qty:
                    print(f" -> PARTIAL ({actual_filled}/{requested_qty}) - rejecting")
                    # Cancel remaining
                    self.kalshi.cancel_order(order_id)
                    return False, None, actual_filled  # Return partial qty for potential unwind

                if status in ["canceled", "cancelled", "expired"]:
                    print(f" -> {status}")
                    # Even if cancelled, check if any filled
                    if actual_filled > 0:
                        return False, None, actual_filled
                    return False, None, 0

            time.sleep(ORDER_CHECK_INTERVAL)

        # TIMEOUT - check if any were filled before giving up
        partial_filled = requested_qty - last_remaining if last_remaining < requested_qty else 0
        if partial_filled > 0:
            print(f" -> TIMEOUT (partial {partial_filled}/{requested_qty})")
            return False, None, partial_filled

        print(f" -> TIMEOUT after {poll_num} polls")
        return False, None, 0

    def _poly_filled(self, order, expected_qty: int = 0) -> Tuple[bool, float]:
        """Check if Polymarket order filled. Accepts fractional fills.

        Returns:
            Tuple of (filled_ok, actual_fill_qty)
            filled_ok is True if we got a fill (even fractional)
        """
        if not order or not isinstance(order, dict):
            return False, 0.0

        # PRIMARY CHECK: Transaction hashes indicate the order filled
        tx_hashes = order.get("transactionsHashes", []) or order.get("transactionHashes", [])
        has_tx_hashes = tx_hashes and len(tx_hashes) > 0

        # Get size_matched if available, otherwise use expected_qty if we have tx hashes
        size_matched = float(order.get("size_matched", 0) or 0)
        if has_tx_hashes and size_matched == 0:
            # FOK orders with tx hashes - assume filled for expected qty
            size_matched = float(expected_qty) if expected_qty > 0 else 1.0

        # Check status
        status = order.get("status", "").upper()
        success = order.get("success", False)

        # Order filled if: has tx hashes OR (success=True AND status=MATCHED)
        if has_tx_hashes or (success and status == "MATCHED"):
            # Accept any fill - fractional is OK
            if expected_qty > 0 and size_matched != expected_qty:
                print(f" ({size_matched:.1f}/{expected_qty})", end="")
            return True, size_matched

        return False, size_matched

    def _verify_unwind_fill(self, order_response: Optional[Dict], qty: int, max_wait: float = 2.0) -> bool:
        """
        Verify if an unwind order actually filled.
        Returns True if filled, False if not filled or timed out.
        """
        if not order_response:
            return False

        # Get order ID from response
        order_data = order_response.get("order", order_response)
        order_id = order_data.get("order_id")
        if not order_id:
            return False

        # Check if immediately filled
        status = order_data.get("status", "").lower()
        remaining = order_data.get("remaining_count", -1)
        count_filled = order_data.get("count_filled", 0) or 0

        if status == "executed" or remaining == 0 or count_filled >= qty:
            return True

        # Poll for fill
        start = time.time()
        poll_num = 0
        while time.time() - start < max_wait:
            poll_num += 1
            time.sleep(0.3)
            resp = self.kalshi.get_order_status(order_id)
            if resp:
                check_data = resp.get("order", resp)
                check_status = check_data.get("status", "").lower()
                check_remaining = check_data.get("remaining_count", -1)
                check_filled = check_data.get("count_filled", 0) or 0

                print(f" [poll#{poll_num}: {check_status}, filled={check_filled}/{qty}]", end="", flush=True)

                if check_status == "executed" or check_remaining == 0 or check_filled >= qty:
                    return True

                if check_status in ["canceled", "cancelled"]:
                    return False

        return False

    def _unwind_kalshi(self, ticker: str, side: str, qty: int, buy_price: float) -> float:
        """
        Unwind Kalshi position. Strategy:
        1. Check both options: sell current side OR buy opposite side
        2. Pick whichever has lower loss
        3. Max 3% loss on the position, then just sell off all contracts
        """
        MAX_LOSS_PCT = 3.0  # Max 3% loss then sell off all contracts
        buy_price_cents = int(buy_price * 100)
        original_cost = buy_price * qty
        max_loss_dollars = original_cost * (MAX_LOSS_PCT / 100)

        # Get orderbook to check both sides
        ob_response = self.kalshi.get_orderbook(ticker)

        # Current side: what we can sell at (bid)
        current_bid_cents = None
        # Opposite side: what we'd pay to buy (ask)
        opposite_ask_cents = None
        opposite_side = "no" if side.lower() == "yes" else "yes"

        if ob_response:
            # Unwrap the orderbook from response (Kalshi nests it under "orderbook" key)
            orderbook = ob_response.get("orderbook", {}) or {}

            if side.lower() == "yes":
                # We have YES, check YES bid (to sell) and NO ask (to buy opposite)
                yes_bids = orderbook.get("yes", []) or []
                if yes_bids:
                    current_bid_cents = max(b[0] for b in yes_bids)
                # NO ask = 100 - YES bid (approximately)
                no_bids = orderbook.get("no", []) or []
                if no_bids:
                    best_no_bid = max(b[0] for b in no_bids)
                    opposite_ask_cents = 100 - best_no_bid  # This is what we'd pay for NO
            else:
                # We have NO, check NO bid (to sell) and YES ask (to buy opposite)
                no_bids = orderbook.get("no", []) or []
                if no_bids:
                    current_bid_cents = max(b[0] for b in no_bids)
                # YES ask = 100 - NO bid (approximately)
                yes_bids = orderbook.get("yes", []) or []
                if yes_bids:
                    best_yes_bid = max(b[0] for b in yes_bids)
                    opposite_ask_cents = 100 - best_yes_bid  # This is what we'd pay for YES

        # Calculate losses for each option
        sell_loss = None
        buy_opposite_loss = None

        if current_bid_cents:
            # Loss from selling current position
            sell_loss = (buy_price_cents - current_bid_cents) * qty / 100

        if opposite_ask_cents:
            # Loss from buying opposite (total cost - $1 payout per contract)
            total_cost_cents = buy_price_cents + opposite_ask_cents
            buy_opposite_loss = (total_cost_cents - 100) * qty / 100

        # Calculate loss percentages for logging/analysis
        sell_loss_pct = (sell_loss / original_cost * 100) if sell_loss is not None and original_cost > 0 else None
        buy_opp_loss_pct = (buy_opposite_loss / original_cost * 100) if buy_opposite_loss is not None and original_cost > 0 else None

        print(f"    [UNWIND] Bought {side.upper()} @ {buy_price_cents}c, qty={qty}, cost=${original_cost:.2f}")
        if sell_loss is not None:
            print(f"    [UNWIND] Option A (sell {side.upper()}): bid={current_bid_cents}c -> loss=${sell_loss:.2f} ({sell_loss_pct:.1f}%)")
        else:
            print(f"    [UNWIND] Option A (sell {side.upper()}): No bid available")
        if buy_opposite_loss is not None:
            total_arb_cost = (buy_price_cents + opposite_ask_cents) / 100 * qty
            # If negative, it's actually a profit (total cost < $1 payout)
            if buy_opposite_loss < 0:
                print(f"    [UNWIND] Option B (buy {opposite_side.upper()}): ask={opposite_ask_cents}c -> total cost=${total_arb_cost:.2f}, payout=${qty:.2f}, profit=${-buy_opposite_loss:.2f}")
            else:
                print(f"    [UNWIND] Option B (buy {opposite_side.upper()}): ask={opposite_ask_cents}c -> loss=${buy_opposite_loss:.2f} ({buy_opp_loss_pct:.1f}%)")
        else:
            print(f"    [UNWIND] Option B (buy {opposite_side.upper()}): No ask available")
        print(f"    [UNWIND] Max loss allowed: ${max_loss_dollars:.2f} (3%)")

        # Pick the better option - PRIORITY: profit first, then lowest loss
        best_option = None
        best_loss = float('inf')

        # Check if either option gives a PROFIT (negative loss)
        sell_is_profit = sell_loss is not None and sell_loss < 0
        buy_opp_is_profit = buy_opposite_loss is not None and buy_opposite_loss < 0

        if sell_is_profit or buy_opp_is_profit:
            # At least one option is profitable - pick the best profit
            if sell_is_profit and buy_opp_is_profit:
                # Both profitable - pick the one with more profit (more negative loss)
                if sell_loss < buy_opposite_loss:
                    best_option = "sell"
                    best_loss = sell_loss
                else:
                    best_option = "buy_opposite"
                    best_loss = buy_opposite_loss
            elif sell_is_profit:
                best_option = "sell"
                best_loss = sell_loss
            else:
                best_option = "buy_opposite"
                best_loss = buy_opposite_loss
            print(f"    [UNWIND] 💰 PROFIT available! Taking it.")
        else:
            # No profit - pick lowest loss within max allowed
            if sell_loss is not None and sell_loss <= max_loss_dollars:
                best_option = "sell"
                best_loss = sell_loss

            if buy_opposite_loss is not None and buy_opposite_loss <= max_loss_dollars:
                if buy_opposite_loss < best_loss:
                    best_option = "buy_opposite"
                    best_loss = buy_opposite_loss

        # Execute the best option with fill verification
        if best_option == "sell":
            print(f"    [UNWIND] -> Trying: Sell {side.upper()} @ {current_bid_cents}c...", end="", flush=True)
            sell_order = self.kalshi.sell_position(ticker, side, qty, current_bid_cents, gtc=False)
            if sell_order and self._verify_unwind_fill(sell_order, qty):
                print()  # New line after verification
                if sell_loss < 0:
                    print(f"    [UNWIND] ✅ SOLD @ {current_bid_cents}c - PROFIT: ${-sell_loss:.2f} ({-sell_loss_pct:.1f}%)")
                else:
                    print(f"    [UNWIND] ✅ SOLD @ {current_bid_cents}c - Loss: ${sell_loss:.2f} ({sell_loss_pct:.1f}%)")
                # Log what the other option would have been
                if buy_opposite_loss is not None:
                    print(f"    [UNWIND] 📊 (Alt: buy {opposite_side.upper()} would have been ${buy_opposite_loss:.2f} / {buy_opp_loss_pct:.1f}%)")
                return sell_loss
            else:
                print(" NO FILL")
                print(f"    [UNWIND] Sell didn't fill, trying buy opposite...")
                best_option = "buy_opposite" if buy_opposite_loss is not None else None

        if best_option == "buy_opposite":
            print(f"    [UNWIND] -> Trying: Buy {opposite_side.upper()} @ {opposite_ask_cents}c...", end="", flush=True)
            # Buy the opposite side to lock in guaranteed payout
            buy_order = self.kalshi.place_order(ticker, opposite_side, qty, opposite_ask_cents)
            if buy_order and self._verify_unwind_fill(buy_order, qty):
                print()  # New line after verification
                if buy_opposite_loss < 0:
                    print(f"    [UNWIND] ✅ BOUGHT {opposite_side.upper()} @ {opposite_ask_cents}c - PROFIT: ${-buy_opposite_loss:.2f} ({-buy_opp_loss_pct:.1f}%)")
                else:
                    print(f"    [UNWIND] ✅ BOUGHT {opposite_side.upper()} @ {opposite_ask_cents}c - Loss: ${buy_opposite_loss:.2f} ({buy_opp_loss_pct:.1f}%)")
                # Log what the other option would have been
                if sell_loss is not None:
                    print(f"    [UNWIND] 📊 (Alt: sell {side.upper()} would have been ${sell_loss:.2f} / {sell_loss_pct:.1f}%)")
                return buy_opposite_loss
            else:
                print(" NO FILL")
                print(f"    [UNWIND] Buy opposite didn't fill...")

        # Fallback: Aggressive instant sell - keep lowering price until filled
        print(f"    [UNWIND] Instant sell - finding fill price...")

        # Start at current bid (or estimate), go down aggressively until filled
        start_price = current_bid_cents if current_bid_cents else max(1, buy_price_cents - 20)

        for attempt in range(10):  # Max 10 attempts
            sell_price = max(1, start_price - (attempt * 5))  # Drop 5c each attempt

            print(f"    [UNWIND] -> Sell {side.upper()} @ {sell_price}c (attempt {attempt+1})...", end="", flush=True)
            sell_order = self.kalshi.sell_position(ticker, side, qty, sell_price, gtc=False)

            if sell_order and self._verify_unwind_fill(sell_order, qty, max_wait=1.5):
                actual_loss = (buy_price_cents - sell_price) * qty / 100
                loss_pct = (actual_loss / original_cost * 100) if original_cost > 0 else 0
                print()
                if actual_loss < 0:
                    print(f"    [UNWIND] ✅ SOLD @ {sell_price}c - PROFIT: ${-actual_loss:.2f} ({-loss_pct:.1f}%)")
                else:
                    print(f"    [UNWIND] ✅ SOLD @ {sell_price}c - Loss: ${actual_loss:.2f} ({loss_pct:.1f}%)")
                return actual_loss
            else:
                print(" NO FILL")
                # Cancel any resting order before trying lower price
                if sell_order:
                    order_id = sell_order.get("order", {}).get("order_id")
                    if order_id:
                        self.kalshi.cancel_order(order_id)
                time.sleep(0.3)

        # Last resort: sell at 1 cent (basically giving away the position)
        print(f"    [UNWIND] -> LAST RESORT: Sell @ 1c...")
        sell_order = self.kalshi.sell_position(ticker, side, qty, 1, gtc=False)
        if sell_order:
            actual_loss = (buy_price_cents - 1) * qty / 100
            loss_pct = (actual_loss / original_cost * 100) if original_cost > 0 else 0
            print(f"    [UNWIND] ✅ SOLD @ 1c - Loss: ${actual_loss:.2f} ({loss_pct:.1f}%)")
            return actual_loss

        print("    [UNWIND] ❌ Could not sell - position will resolve at expiration")
        return original_cost  # Assume total loss

    def try_trade(self, snap: Snapshot, direction: str, edge_pct: float, breakdown: Dict, start_p_buffer: float = 0.0, testing_mode: bool = False, final_window: bool = False) -> Tuple[bool, float, float]:
        """
        Trade execution with Polymarket retry logic.
        - Use 1.5 cent buffer for edge >= 10%, 1 cent buffer otherwise
        - Kalshi first, then Polymarket

        Args:
            start_p_buffer: Starting Poly buffer (for repeat trades, start where last one filled)
            testing_mode: If True, use instant Poly fills (best ask) and 10% stop loss
            final_window: If True (last 3 min), use half quantity and tighter stop-loss (5%)

        Returns:
            Tuple of (success, actual_edge_pct, final_p_buffer)
            success=True and actual_edge_pct > 0 means profitable trade
            final_p_buffer = the buffer at which Poly filled (for repeating)
        - If Poly doesn't fill, keep increasing buffer (+0.005) while edge > 0
        - Only unwind when no more positive edge available
        """
        self.stats.trade_attempts += 1

        # Setup prices
        if direction == "K_UP+P_DOWN":
            k_side, k_price, p_token, p_price = "yes", snap.k_up, snap.poly_down_token, snap.p_down
        else:
            k_side, k_price, p_token, p_price = "no", snap.k_down, snap.poly_up_token, snap.p_up

        # === MARKET EXPIRATION SAFETY: Skip expensive trades near market close ===
        # Markets expire at :00, :15, :30, :45 - avoid >85c trades within 1 minute of close
        now = datetime.utcnow()
        mins_in_window = now.minute % 15
        secs_until_close = (15 - mins_in_window) * 60 - now.second
        if secs_until_close <= 60 and p_price > 0.85:
            print(f"\n  {snap.crypto} | {direction}")
            print(f"    [SKIP] Near market close ({secs_until_close}s) + expensive Poly (${p_price:.2f} > $0.85)")
            self.stats.failed_attempts += 1
            return False, 0.0, 0.0

        # Calculate quantity - use TESTING mode sizing if enabled
        raw_edge = breakdown.get("raw_edge_pct", edge_pct)
        if testing_mode:
            qty = self._calc_testing_qty(k_price, p_price, snap.crypto, raw_edge)
            # Final window: half quantity
            if final_window:
                qty = max(1, int(qty * 0.5))
                print(f"    [FINAL WINDOW] Half qty: {qty} contracts")
        else:
            qty = self._calc_qty(k_price, p_price)

        # Calculate edge breakdown at raw prices first
        _, _, edge_breakdown = self._calc_edge(k_price, p_price, qty, buffer=0.0)

        # Check if trade is profitable at MAX buffer (worst case) before attempting
        # Determine Kalshi buffer: +1c default, +2c if edge > 10%
        k_buffer = 0.02 if edge_pct > 10.0 else 0.01
        k_buf_display = "+2c" if k_buffer > 0.01 else "+1c"

        # Calculate edge with actual buffer we'll use
        actual_edge, actual_profit, _ = self._calc_edge(k_price, p_price, qty, buffer=k_buffer)

        print(f"\n  {snap.crypto} | {direction} | {qty}x")
        print(f"    Edge (no fees):       {edge_breakdown['edge_no_fees']:.1f}%")
        print(f"    Edge (fees+gas):      {edge_breakdown['edge_with_fees_gas']:.1f}%")
        print(f"    Edge (K{k_buf_display}, P base):  {actual_edge:.1f}% | P/L: ${actual_profit:.2f}")
        print(f"    K: ${k_price:.2f} | P: ${p_price:.2f}")

        if actual_edge < MIN_EXECUTE_EDGE_PCT:
            print(f"    [SKIP] Edge @ {k_buf_display} = {actual_edge:.1f}% < {MIN_EXECUTE_EDGE_PCT}%")
            self.stats.failed_attempts += 1
            return False, 0.0, 0.0

        # Check we have enough funds at max buffer prices
        raw_edge = edge_breakdown.get('edge_no_fees', edge_pct)
        k_max_buffer = 0.025 if raw_edge >= 15.0 else 0.02  # Max we'll pay above scan price (+2c)
        k_max_buy = min(k_price + k_max_buffer, 0.99)
        p_max_buy = min(p_price + 0.20, 0.99)  # Poly: up to 20c buffer
        k_bal, p_bal = self.get_balances()
        if k_bal < k_max_buy * qty or p_bal < p_max_buy * qty:
            print(f"    [SKIP] Insufficient funds")
            self.stats.failed_attempts += 1
            return False, 0.0, 0.0

        # === EXECUTE KALSHI IMMEDIATELY ===
        k_buy = min(k_price + k_buffer, 0.99)
        k_filled = False
        k_actual_price = None
        k_actual_qty = 0

        # Get position BEFORE order to calculate actual fills
        pos_before = 0
        pre_pos = self.kalshi.get_position_for_ticker(snap.kalshi_ticker)
        if pre_pos:
            pos_val = pre_pos.get("position", 0)
            if k_side.lower() == "yes":
                pos_before = pos_val if pos_val > 0 else 0
            else:
                pos_before = abs(pos_val) if pos_val < 0 else 0

        print(f"    [K] BUY {qty} {k_side.upper()} @ ${k_buy:.2f} ({k_buf_display})...", end=" ", flush=True)
        k_order = self.kalshi.place_order(snap.kalshi_ticker, k_side, qty, int(k_buy * 100))

        # Start Poly orderbook prefetch in parallel while Kalshi processes
        poly_prefetch_future = None
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            poly_prefetch_future = executor.submit(self.poly.get_fillable_ask_price, p_token, int(qty))
        except Exception:
            pass  # If prefetch fails, we'll fetch later

        if not k_order:
            print("FAILED")
            executor.shutdown(wait=False)
            self.stats.failed_attempts += 1
            return False, 0.0, 0.0

        # Check if order was immediately filled
        order_data = k_order.get("order", {})
        k_order_id = order_data.get("order_id", "")
        status = order_data.get("status", "").lower()
        remaining = order_data.get("remaining_count", -1)
        count_filled = order_data.get("count_filled", 0) or 0

        is_executed = status in ["executed", "filled", "complete", "closed"]
        full_fill_immediate = (is_executed and remaining == 0) or count_filled >= qty

        if full_fill_immediate:
            k_actual_qty = count_filled if count_filled > 0 else qty
            avg_fill = order_data.get("average_fill_price")
            k_actual_price = float(avg_fill) / 100.0 if avg_fill else k_buy
            k_filled = True
            print(f"FILLED {k_actual_qty}x @ ${k_actual_price:.4f}")
        else:
            # Not immediately filled - wait briefly and re-check before canceling
            if count_filled > 0:
                print(f"PARTIAL ({count_filled}/{qty}) - checking...", end="", flush=True)
            else:
                print(f"RESTING - checking...", end="", flush=True)

            # Wait briefly for potential fill
            time.sleep(0.05)  # 50ms (was 100ms)

            # Re-check order status before canceling
            recheck = self.kalshi.get_order_status(k_order_id)
            if recheck:
                recheck_data = recheck.get("order", recheck)
                recheck_status = recheck_data.get("status", "").lower()
                recheck_filled = recheck_data.get("count_filled", 0) or 0
                recheck_remaining = recheck_data.get("remaining_count", -1)

                recheck_executed = recheck_status in ["executed", "filled", "complete", "closed"]
                recheck_full = (recheck_executed and recheck_remaining == 0) or recheck_filled >= qty

                if recheck_full:
                    # Order filled during the wait!
                    k_actual_qty = recheck_filled if recheck_filled > 0 else qty
                    avg_fill = recheck_data.get("average_fill_price")
                    k_actual_price = float(avg_fill) / 100.0 if avg_fill else k_buy
                    k_filled = True
                    print(f" FILLED {k_actual_qty}x @ ${k_actual_price:.4f}")
                else:
                    # Still not filled - cancel
                    print(f" still pending", end="", flush=True)
                    self.kalshi.cancel_order(k_order_id)

                    # Wait and verify cancel - check if it filled before cancel took effect
                    time.sleep(0.05)  # 50ms (was 100ms)
                    verify = self.kalshi.get_order_status(k_order_id)
                    if verify:
                        verify_data = verify.get("order", verify)
                        verify_filled = verify_data.get("count_filled", 0) or 0
                        verify_status = verify_data.get("status", "").lower()

                        if verify_filled >= qty:
                            # Actually filled before cancel!
                            k_actual_qty = verify_filled
                            avg_fill = verify_data.get("average_fill_price")
                            k_actual_price = float(avg_fill) / 100.0 if avg_fill else k_buy
                            k_filled = True
                            print(f" -> actually FILLED {k_actual_qty}x @ ${k_actual_price:.4f}")
                        elif verify_filled > 0:
                            # Partial fill - check if position exists anyway
                            print(f" -> partial {verify_filled}/{qty}")
                            has_pos, pos_qty, pos_price = self._check_orphaned_position(snap.kalshi_ticker, k_side, qty, k_buy, pos_before)
                            if has_pos and pos_qty >= qty:
                                k_actual_qty = pos_qty
                                k_actual_price = pos_price
                                k_filled = True
                            elif has_pos and pos_qty > 0:
                                # Partial fill - check if Poly cost > $1 to proceed
                                poly_cost = pos_qty * p_price
                                if poly_cost > 1.0:
                                    print(f"\n    [PARTIAL] {pos_qty}x filled, Poly cost ${poly_cost:.2f} > $1 - proceeding")
                                    k_actual_qty = pos_qty
                                    k_actual_price = pos_price
                                    k_filled = True
                                else:
                                    # Poly cost too low, unwind
                                    print(f"    [UNWIND PARTIAL] Unwinding {pos_qty} contracts (Poly cost ${poly_cost:.2f} < $1)...")
                                    loss = self._unwind_kalshi(snap.kalshi_ticker, k_side, pos_qty, pos_price)
                                    self.stats.partial_fills += 1
                                    self.stats.unwind_losses += loss
                                    print(f"    [K] NO FILL at {k_buf_display} (partial {verify_filled} unwound)")
                                    self.stats.failed_attempts += 1
                                    return False, 0.0, 0.0
                            else:
                                print(f"    [K] NO FILL at {k_buf_display} (partial {verify_filled})")
                                self.stats.failed_attempts += 1
                                return False, 0.0, 0.0
                        else:
                            # Order cancelled - but verify no position exists
                            print(f" -> cancelled", end="", flush=True)
                            time.sleep(0.05)  # Brief pause for position to settle
                            has_pos, pos_qty, pos_price = self._check_orphaned_position(snap.kalshi_ticker, k_side, qty, k_buy, pos_before)
                            if has_pos and pos_qty >= qty:
                                k_actual_qty = pos_qty
                                k_actual_price = pos_price
                                k_filled = True
                                print(f" -> POSITION FOUND! {pos_qty}x")
                            elif has_pos and pos_qty > 0:
                                # Partial fill - check if Poly cost > $1 to proceed
                                poly_cost = pos_qty * p_price
                                if poly_cost > 1.0:
                                    print(f"\n    [PARTIAL] {pos_qty}x filled, Poly cost ${poly_cost:.2f} > $1 - proceeding")
                                    k_actual_qty = pos_qty
                                    k_actual_price = pos_price
                                    k_filled = True
                                else:
                                    # Poly cost too low, unwind
                                    print(f"\n    [UNWIND PARTIAL] Unwinding {pos_qty} contracts (Poly cost ${poly_cost:.2f} < $1)...")
                                    loss = self._unwind_kalshi(snap.kalshi_ticker, k_side, pos_qty, pos_price)
                                    self.stats.partial_fills += 1
                                    self.stats.unwind_losses += loss
                                    print(f"    [K] NO FILL at {k_buf_display} (partial unwound)")
                                    self.stats.failed_attempts += 1
                                    return False, 0.0, 0.0
                            else:
                                print()
                                print(f"    [K] NO FILL at {k_buf_display}")
                                self.stats.failed_attempts += 1
                                return False, 0.0, 0.0
                    else:
                        # Verify request failed - check position as safety
                        print(f" -> cancelled", end="", flush=True)
                        time.sleep(0.05)
                        has_pos, pos_qty, pos_price = self._check_orphaned_position(snap.kalshi_ticker, k_side, qty, k_buy, pos_before)
                        if has_pos and pos_qty >= qty:
                            k_actual_qty = pos_qty
                            k_actual_price = pos_price
                            k_filled = True
                            print(f" -> POSITION FOUND! {pos_qty}x")
                        elif has_pos and pos_qty > 0:
                            # Partial fill - check if Poly cost > $1 to proceed
                            poly_cost = pos_qty * p_price
                            if poly_cost > 1.0:
                                print(f"\n    [PARTIAL] {pos_qty}x filled, Poly cost ${poly_cost:.2f} > $1 - proceeding")
                                k_actual_qty = pos_qty
                                k_actual_price = pos_price
                                k_filled = True
                            else:
                                # Poly cost too low, unwind
                                print(f"\n    [UNWIND PARTIAL] Unwinding {pos_qty} contracts (Poly cost ${poly_cost:.2f} < $1)...")
                                loss = self._unwind_kalshi(snap.kalshi_ticker, k_side, pos_qty, pos_price)
                                self.stats.partial_fills += 1
                                self.stats.unwind_losses += loss
                                print(f"    [K] NO FILL at {k_buf_display} (partial unwound)")
                                self.stats.failed_attempts += 1
                                return False, 0.0, 0.0
                        else:
                            print()
                            print(f"    [K] NO FILL at {k_buf_display}")
                            self.stats.failed_attempts += 1
                            return False, 0.0, 0.0
            else:
                # Couldn't recheck - cancel and verify position
                self.kalshi.cancel_order(k_order_id)
                time.sleep(0.05)
                has_pos, pos_qty, pos_price = self._check_orphaned_position(snap.kalshi_ticker, k_side, qty, k_buy, pos_before)
                if has_pos and pos_qty >= qty:
                    k_actual_qty = pos_qty
                    k_actual_price = pos_price
                    k_filled = True
                    print(f"    [VERIFY] POSITION FOUND despite recheck fail! {pos_qty}x")
                elif has_pos and pos_qty > 0:
                    # Partial fill - check if Poly cost > $1 to proceed
                    poly_cost = pos_qty * p_price
                    if poly_cost > 1.0:
                        print(f"    [PARTIAL] {pos_qty}x filled, Poly cost ${poly_cost:.2f} > $1 - proceeding")
                        k_actual_qty = pos_qty
                        k_actual_price = pos_price
                        k_filled = True
                    else:
                        # Poly cost too low, unwind
                        print(f"    [UNWIND PARTIAL] Unwinding {pos_qty} contracts (Poly cost ${poly_cost:.2f} < $1)...")
                        loss = self._unwind_kalshi(snap.kalshi_ticker, k_side, pos_qty, pos_price)
                        self.stats.partial_fills += 1
                        self.stats.unwind_losses += loss
                        print(f"    [K] NO FILL at {k_buf_display} (partial unwound)")
                        self.stats.failed_attempts += 1
                        return False, 0.0, 0.0
                else:
                    print(f"    [K] NO FILL at {k_buf_display} (recheck failed)")
                    self.stats.failed_attempts += 1
                    return False, 0.0, 0.0

        k_actual = k_actual_price if k_actual_price else k_price
        trade_qty = k_actual_qty if k_actual_qty > 0 else qty

        # === EXECUTE POLYMARKET - START AT GIVEN BUFFER, DYNAMIC INCREMENTS ===
        current_p_buffer = start_p_buffer  # Start at given buffer (0 for first trade, or last fill price for repeats)
        max_safety_buffer = 0.50  # Safety limit - don't go above 50 cents (loss limit will usually hit first)
        if start_p_buffer > 0:
            print(f"    [P] Starting at buffer ${start_p_buffer:.3f} (from previous fill)")

        # Max loss we're willing to take on the trade before giving up and unwinding Kalshi
        # Use config values for max loss percentage per mode
        # Final window uses same 3% stop-loss, then sell off all contracts
        expected_total_cost = (k_actual + p_price) * trade_qty
        if final_window:
            max_loss_pct = 0.03  # 3% stop-loss for final window
        elif testing_mode:
            max_loss_pct = MAX_LOSS_PCT_TESTING
        else:
            max_loss_pct = MAX_LOSS_PCT
        MAX_LOSS_BEFORE_UNWIND = expected_total_cost * max_loss_pct

        if final_window:
            print(f"    [STOP LOSS] {max_loss_pct*100:.0f}% max loss (${MAX_LOSS_BEFORE_UNWIND:.2f}) [FINAL - faster unwind]")
        else:
            print(f"    [STOP LOSS] {max_loss_pct*100:.0f}% max loss (${MAX_LOSS_BEFORE_UNWIND:.2f})")

        # Clean up executor from Kalshi prefetch
        try:
            executor.shutdown(wait=False)
        except Exception:
            pass

        # === LEVEL-BY-LEVEL POLY FILL - GET BEST PRICES AT EACH LEVEL ===
        # Calculate max price we're willing to pay (based on loss threshold)
        # max_poly_price where (k_actual + max_poly_price) * qty - fees - gas <= -MAX_LOSS_BEFORE_UNWIND
        k_cost = k_actual * trade_qty
        k_fee = calc_kalshi_fee(k_actual, trade_qty)
        payout = trade_qty * 1.0
        # Solve: payout - k_cost - max_p_cost - k_fee - p_fee - gas = -MAX_LOSS_BEFORE_UNWIND
        # Approximating p_fee as ~1.5% of p_cost
        max_p_cost = payout - k_cost - k_fee - GAS_FEE_DOLLARS + MAX_LOSS_BEFORE_UNWIND
        max_poly_price = (max_p_cost / trade_qty) / 1.015 if trade_qty > 0 else 0.99  # Account for ~1.5% Poly fee
        max_poly_price = min(max(0.01, max_poly_price), 0.99)

        print(f"    [P] Level-by-level fill, max price: ${max_poly_price:.2f}")

        # Create loss check callback
        def check_loss(avg_price, filled_qty):
            total_p_cost = avg_price * filled_qty
            p_fee = calc_poly_fee(avg_price, filled_qty)
            total_cost = k_cost + total_p_cost
            profit = payout - total_cost - k_fee - p_fee - GAS_FEE_DOLLARS
            return profit < -MAX_LOSS_BEFORE_UNWIND

        # Get prefetched best ask price (if available)
        prefetched_best_ask = None
        if poly_prefetch_future:
            try:
                prefetch_result = poly_prefetch_future.result(timeout=0.1)
                if prefetch_result and prefetch_result[0]:
                    prefetched_best_ask = prefetch_result[0]
            except Exception:
                pass  # Prefetch failed, will fetch in fill_level_by_level

        # Fill level by level with 1 cent buffer for faster fills
        fill_result = self.poly.fill_level_by_level(
            token_id=p_token,
            target_qty=int(trade_qty),
            max_price=max_poly_price,
            max_loss_check=check_loss,
            price_buffer=POLY_BUFFER_START,
            prefetched_best_ask=prefetched_best_ask,
        )

        if fill_result["success"] and fill_result["total_filled"] > 0:
            poly_fill_qty = fill_result["total_filled"]
            p_avg_price = fill_result["avg_price"]
            p_total_cost = fill_result["total_cost"]

            matched_qty = min(trade_qty, poly_fill_qty)
            position_gap = trade_qty - poly_fill_qty

            final_cost = k_cost + p_total_cost
            final_k_fee = k_fee
            final_p_fee = calc_poly_fee(p_avg_price, poly_fill_qty)
            final_payout = matched_qty * 1.0
            final_profit = final_payout - final_cost - final_k_fee - final_p_fee - GAS_FEE_DOLLARS

            self.stats.successful_trades += 1
            self.stats.total_invested += final_cost
            self.stats.realized_pnl += final_profit

            # Update cached balances (subtract what we spent)
            self.update_cached_balances(k_spent=k_cost, p_spent=p_total_cost)

            final_edge = (final_profit / final_cost) * 100 if final_cost > 0 else 0
            raw_edge_val = edge_breakdown.get('edge_no_fees', edge_pct)

            append_pnl_summary(
                timestamp=now_mst(),
                crypto=snap.crypto,
                direction=direction,
                status="SUCCESS",
                qty=matched_qty,
                cost=final_cost,
                pnl=final_profit,
                running_total=self.stats.realized_pnl - self.stats.unwind_losses,
                raw_edge=raw_edge_val,
                actual_edge=final_edge,
                kalshi_strike=snap.kalshi_strike,
                poly_strike=snap.poly_strike,
            )

            self.stats.last_filled_qty = matched_qty

            levels_note = f" ({fill_result['levels_filled']} levels)"
            gap_note = f" (gap: {position_gap:.1f})" if position_gap > 0 else ""
            print(f"    [OK] Trade #{self.stats.successful_trades} complete! K:{trade_qty} P:{poly_fill_qty}{gap_note}{levels_note} | Avg: ${p_avg_price:.4f} | P/L: ${final_profit:.2f} | Edge: {final_edge:.1f}%")
            return True, final_edge, p_avg_price

        # Handle partial fills or no fills
        poly_filled = fill_result["total_filled"]

        if poly_filled > 0 and poly_filled < trade_qty:
            # Partial fill - sell ONLY the excess Kalshi shares, keep the matched position
            excess_kalshi = trade_qty - poly_filled
            p_avg_price = fill_result["avg_price"]
            p_total_cost = fill_result["total_cost"]

            print(f"    [!] Partial fill: Got {poly_filled}/{trade_qty} on Poly")
            print(f"    [!] Selling {excess_kalshi}x excess on Kalshi (keeping {poly_filled}x matched)...")

            # Sell the excess Kalshi shares at market price
            excess_loss = self._unwind_kalshi(snap.kalshi_ticker, k_side, excess_kalshi, k_actual)

            # Calculate profit on the matched portion
            matched_k_cost = k_actual * poly_filled
            matched_k_fee = calc_kalshi_fee(k_actual, poly_filled)
            matched_p_fee = calc_poly_fee(p_avg_price, poly_filled)
            matched_payout = poly_filled * 1.0
            matched_profit = matched_payout - matched_k_cost - p_total_cost - matched_k_fee - matched_p_fee - GAS_FEE_DOLLARS

            # Net result = matched profit - excess loss
            net_profit = matched_profit - excess_loss

            self.stats.successful_trades += 1
            self.stats.partial_fills += 1
            self.stats.total_invested += matched_k_cost + p_total_cost
            self.stats.realized_pnl += net_profit

            # Update cached balances
            self.update_cached_balances(k_spent=k_cost, p_spent=p_total_cost)

            final_edge = (net_profit / (matched_k_cost + p_total_cost)) * 100 if (matched_k_cost + p_total_cost) > 0 else 0
            raw_edge_val = edge_breakdown.get('edge_no_fees', edge_pct)

            append_pnl_summary(
                timestamp=now_mst(),
                crypto=snap.crypto,
                direction=direction,
                status="PARTIAL",
                qty=poly_filled,
                cost=matched_k_cost + p_total_cost,
                pnl=net_profit,
                loss_reason=f"Partial {poly_filled}/{trade_qty}, sold {excess_kalshi} excess",
                running_total=self.stats.realized_pnl - self.stats.unwind_losses,
                raw_edge=raw_edge_val,
                actual_edge=final_edge,
                kalshi_strike=snap.kalshi_strike,
                poly_strike=snap.poly_strike,
            )

            self.stats.last_filled_qty = poly_filled
            print(f"    [PARTIAL] Matched {poly_filled}x | Excess loss: ${excess_loss:.2f} | Net P/L: ${net_profit:.2f}")
            return True, final_edge, p_avg_price

        # No fills at all - unwind everything on Kalshi
        print(f"    [!] No Poly fills - unwinding all {trade_qty}x on Kalshi...")
        print(f"    [!] Unwinding Kalshi {trade_qty}x (bought @ ${k_actual:.4f})...")
        loss = self._unwind_kalshi(snap.kalshi_ticker, k_side, trade_qty, k_actual)
        self.stats.partial_fills += 1
        self.stats.unwind_losses += loss
        raw_edge_val = edge_breakdown.get('edge_no_fees', edge_pct)
        append_pnl_summary(
            timestamp=now_mst(),
            crypto=snap.crypto,
            direction=direction,
            status="UNWIND",
            qty=trade_qty,
            cost=k_cost,
            pnl=-loss,
            loss_reason="Poly fill failed completely",
            running_total=self.stats.realized_pnl - self.stats.unwind_losses,
            raw_edge=raw_edge_val,
            actual_edge=0.0,
            kalshi_strike=snap.kalshi_strike,
            poly_strike=snap.poly_strike,
        )
        return False, 0.0, fill_result.get("avg_price", p_price)

    def log_opportunity(self, snap: Snapshot, direction: str, edge_pct: float, breakdown: Dict) -> None:
        """Log opportunity without trading (for LOGGING_ONLY mode)."""
        if direction == "K_UP+P_DOWN":
            k_price, p_price = snap.k_up, snap.p_down
        else:
            k_price, p_price = snap.k_down, snap.p_up

        qty = self._calc_qty(k_price, p_price)

        # Calculate edge breakdown at raw prices first
        _, _, edge_breakdown = self._calc_edge(k_price, p_price, qty, buffer=0.0)

        # Use starting buffer for logging
        buffer = PRICE_BUFFER_START

        # Buffered prices
        k_buy = min(k_price + buffer, 0.99)
        p_buy = min(p_price + buffer, 0.99)

        # Check edge at buffered prices
        edge, profit, _ = self._calc_edge(k_price, p_price, qty, buffer=buffer)

        print(f"\n  {snap.crypto} | {direction} | {qty}x | Buffer: ${buffer:.3f}")
        print(f"    Edge (no fees):       {edge_breakdown['edge_no_fees']:.1f}%")
        print(f"    Edge (fees+gas):      {edge_breakdown['edge_with_fees_gas']:.1f}%")
        print(f"    Edge (fees+gas+buf):  {edge:.1f}% | P/L: ${profit:.2f}")
        print(f"    K: ${k_price:.2f} -> ${k_buy:.2f} | P: ${p_price:.2f} -> ${p_buy:.2f}")

        # Log to CSV file (arb_trades_log.csv in logging mode)
        k_bal, p_bal = self.get_balances()
        log_trade_row({
            "timestamp": now_mst(),
            "mode": "OPPORTUNITY",
            "crypto": snap.crypto,
            "direction": direction,
            "qty": qty,
            "k_price": k_price,
            "p_price": p_price,
            "k_actual_price": "",
            "p_actual_price": "",
            "k_cost": k_price * qty,
            "p_cost": p_price * qty,
            "total_cost": (k_price + p_price) * qty,
            "k_fee": calc_kalshi_fee(k_price, qty),
            "p_fee": calc_poly_fee(p_price, qty),
            "gas_fee": GAS_FEE_DOLLARS,
            "total_fees": edge_breakdown.get("total_fees", 0),
            "expected_profit": profit,
            "worst_case_profit": "",
            "realized_pnl": "",
            "edge_pct": edge_breakdown.get("edge_no_fees", edge_pct),
            "k_balance": k_bal,
            "p_balance": p_bal,
            "total_balance": k_bal + p_bal,
            "cumulative_pnl": "",
            "unwind_losses": "",
            "successful_trades": "",
            "status": "LOGGED",
            "k_order_id": "",
            "p_order_id": "",
        })

        self.stats.opportunities_found += 1

    def print_summary(self):
        """Print session summary with hourly rate."""
        k_bal, p_bal = self.get_balances()
        net = self.stats.realized_pnl - self.stats.unwind_losses

        # Get session stats for hourly rate
        session = get_session_stats()
        hours = session.get("hours", 0)
        hourly_rate = session.get("hourly_rate", 0)

        print(f"\n{'='*50}")
        print(f"  SESSION SUMMARY")
        print(f"{'='*50}")
        print(f"  Trades: {self.stats.successful_trades} success, {self.stats.partial_fills} unwinds")
        print(f"  Net P&L: ${net:.2f}")
        if hours >= 0.01:
            print(f"  Hourly Rate: ${hourly_rate:.2f}/hr")
            print(f"  Duration: {int(hours)}h {int((hours * 60) % 60)}m")
        print(f"  Balances: K=${k_bal:.2f}, P=${p_bal:.2f}")
