"""
Edge calculation functions for arbitrage opportunities.
"""

from typing import Dict, Tuple

from config import (
    calc_kalshi_fee,
    calc_poly_fee,
    GAS_FEE_DOLLARS,
    PAYOUT_PER_UNIT,
    PRICE_BUFFER_START,
)
from models import Snapshot

# Default quantity for edge estimation (matches MAX_TRADE_QTY in config)
DEFAULT_QTY_FOR_EDGE = 7


def calculate_edge(qty: int, k_price: float, p_price: float, buffer: float = PRICE_BUFFER_START) -> Tuple[float, Dict[str, float]]:
    """
    Calculate net edge percentage and breakdown for an arbitrage trade.

    Args:
        qty: Number of contracts
        k_price: Kalshi leg price (can be None if unavailable)
        p_price: Polymarket leg price (can be None if unavailable)
        buffer: Price buffer to add to EACH side (default: PRICE_BUFFER_START = 1 cent)

    Returns:
        Tuple of (edge_pct, breakdown_dict)
        breakdown_dict includes raw_edge (no fees/gas) and edge_with_all (fees+gas+buffer)
    """
    # Handle None prices - return very negative edge to indicate not tradeable
    if k_price is None or p_price is None:
        return -999.0, {
            "raw_edge_pct": -999.0,
            "edge_with_fees_pct": -999.0,
            "edge_with_all_pct": -999.0,
            "k_price": k_price,
            "p_price": p_price,
            "not_tradeable": True,
        }

    k_leg_cost = qty * k_price
    p_leg_cost = qty * p_price
    cost_total = k_leg_cost + p_leg_cost

    # Calculate fees using actual fee curves
    # Kalshi: ceil(0.07 × C × P × (1-P)) - peaks at 50%, drops at extremes
    kalshi_fee = calc_kalshi_fee(k_price, qty, is_maker=False)
    # Polymarket: fee curve (lower at extremes, max 1.56% at 50%)
    poly_fee = calc_poly_fee(p_price, qty)
    fees_total = kalshi_fee + poly_fee

    # Buffer cost - 1 cent on EACH side (Kalshi + Polymarket)
    kalshi_buffer_cost = buffer * qty  # 1 cent per contract on Kalshi
    poly_buffer_cost = buffer * qty    # 1 cent per contract on Polymarket
    buffer_cost = kalshi_buffer_cost + poly_buffer_cost  # Total: 2 cents per contract

    gas = GAS_FEE_DOLLARS
    payout_total = qty * PAYOUT_PER_UNIT

    # Raw profit (no fees/gas/buffer)
    raw_profit = payout_total - cost_total
    # Net profit with fees and gas (no buffer)
    net_profit_fees_gas = payout_total - cost_total - fees_total - gas
    # Net profit with fees, gas, AND buffer on both sides (the REAL edge)
    net_profit_all = payout_total - cost_total - fees_total - gas - buffer_cost

    if cost_total <= 0:
        raw_edge_pct = -999.0
        edge_fees_gas_pct = -999.0
        edge_with_all_pct = -999.0
    else:
        raw_edge_pct = (raw_profit / cost_total) * 100.0
        edge_fees_gas_pct = (net_profit_fees_gas / cost_total) * 100.0
        edge_with_all_pct = (net_profit_all / cost_total) * 100.0

    breakdown = {
        "k_leg_cost": k_leg_cost,
        "p_leg_cost": p_leg_cost,
        "cost_total": cost_total,
        "kalshi_fee": kalshi_fee,
        "poly_fee": poly_fee,
        "fees_total": fees_total,
        "kalshi_buffer": kalshi_buffer_cost,
        "poly_buffer": poly_buffer_cost,
        "buffer_cost": buffer_cost,
        "gas_fee": gas,
        "payout_total": payout_total,
        "raw_profit": raw_profit,
        "net_profit": net_profit_all,
        "raw_edge_pct": raw_edge_pct,        # Edge without fees/gas/buffer
        "edge_fees_gas_pct": edge_fees_gas_pct,  # Edge with fees+gas (no buffer)
        "edge_pct": edge_with_all_pct,       # Edge with fees+gas+buffer on BOTH sides
    }

    return edge_with_all_pct, breakdown


def pick_best_direction(s: Snapshot) -> Tuple[str, float, float, Dict]:
    """
    Pick the best arbitrage direction for a given snapshot.

    Args:
        s: Market snapshot with prices from both exchanges

    Returns:
        Tuple of (direction, edge_pct, other_edge_pct, breakdown)
        Direction is either "K_UP+P_DOWN" or "K_DOWN+P_UP"
    """
    edgeA, breakdownA = calculate_edge(DEFAULT_QTY_FOR_EDGE, s.k_up, s.p_down)
    edgeB, breakdownB = calculate_edge(DEFAULT_QTY_FOR_EDGE, s.k_down, s.p_up)

    if edgeA >= edgeB:
        return "K_UP+P_DOWN", edgeA, edgeB, breakdownA
    else:
        return "K_DOWN+P_UP", edgeB, edgeA, breakdownB
