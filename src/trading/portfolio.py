from __future__ import annotations
from src.config.settings import MIN_QTY


def compute_equity(cash_usdc: float, market_states: dict, last_book: dict) -> float:
    """Mark-to-market equity: cash + sum(pos.qty * mid_price) for all open positions."""
    mtm = 0.0
    for slug, st in market_states.items():
        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty < MIN_QTY:
                continue
            book = last_book.get(slug, {}).get(outcome)
            mid = book.mid if book else pos.vwap
            mtm += pos.qty * mid
    return cash_usdc + mtm


def clean_dust(pos):
    """Zero out positions that are dust (< MIN_QTY)."""
    if abs(pos.qty) < MIN_QTY:
        pos.qty = 0.0
        pos.cost_usdc = 0.0
        pos.vwap = 0.0
        pos.opened_at = None
        pos.last_trade_ts = None
        pos.tp1_done = False
        pos.tp2_done = False
        pos.tp3_done = False
        pos.scalp_mode = False
        pos.scalp_open_ts = None
        pos.position_id = None
        pos.trade_id = None
        pos.entry_decision_id = None
        pos.parent_order_id = None
        pos.entry_mid = 0.0
        pos.max_favorable_mid = 0.0
        pos.max_adverse_mid = 1.0
        pos.last_derisk_ts = None
        pos.last_derisk_mid = 0.0
        pos.fast_tp_done = False
