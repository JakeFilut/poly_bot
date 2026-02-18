"""Spread / liquidity check re-exports for the gating layer.

These functions are the canonical implementations living in
``src.strategy.f247_like``; this module re-exports them so that gating
consumers can do::

    from src.gating.spread import spread_limit, parity_liquidity_ok, taker_gate_allows

without reaching into the strategy package directly.
"""

from src.strategy.f247_like import (  # noqa: F401
    spread_limit,
    parity_liquidity_ok,
    taker_gate_allows,
)
