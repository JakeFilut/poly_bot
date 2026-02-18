from __future__ import annotations
from src.config.settings import (
    ENFORCE_STOP_LOSS, STOP_LOSS_PCT_PER_HOUR, STOP_LOSS_PCT_PER_DAY,
    MAX_COST_PER_MARKET_PCT, MAX_COST_PER_CRYPTO_PCT, MIN_QTY,
    SHADOW_STOP_SIM,
)


def market_cost_usdc(market_state) -> float:
    """Total cost across all positions in a single market."""
    return sum(p.cost_usdc for p in market_state.positions.values())


def crypto_cost_usdc(crypto: str, market_states: dict) -> float:
    """Total cost across all markets for a given crypto."""
    s = 0.0
    for st in market_states.values():
        if st.crypto == crypto:
            s += sum(p.cost_usdc for p in st.positions.values())
    return s


def risk_ok(market_state, cash_usdc: float, hour_start_equity: float, equity_fn) -> bool:
    """Position-sizing risk caps. Stop-loss is log-only when ENFORCE_STOP_LOSS=False."""
    if ENFORCE_STOP_LOSS:
        equity = equity_fn()
        hse = hour_start_equity if hour_start_equity > 0 else equity
        if (equity - hse) / max(hse, 1e-9) <= -STOP_LOSS_PCT_PER_HOUR:
            return False
    if market_cost_usdc(market_state) > cash_usdc * MAX_COST_PER_MARKET_PCT:
        return False
    if crypto_cost_usdc(market_state.crypto, {}) > cash_usdc * MAX_COST_PER_CRYPTO_PCT:
        return False
    return True
