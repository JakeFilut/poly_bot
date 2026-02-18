"""Math helpers extracted from pm_hourly_clone_bot.py (Pass 1)."""
from __future__ import annotations

import math

from src.config.settings import EDGE_K


# ---------------------------------------------------------------------------
# Clamping
# ---------------------------------------------------------------------------
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def clamp_to_tick(price: float, tick: float = 0.001) -> float:
    """Round price DOWN to nearest tick (Polymarket uses $0.001 ticks)."""
    return math.floor(price / tick) * tick


# ---------------------------------------------------------------------------
# Safe conversions
# ---------------------------------------------------------------------------
def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Probability model
# ---------------------------------------------------------------------------
def _p_up_model(delta_bps: float) -> float:
    """Implied probability of Up outcome via sigmoid on delta_bps."""
    return 1.0 / (1.0 + math.exp(-EDGE_K * delta_bps))
