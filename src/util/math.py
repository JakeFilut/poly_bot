"""Math helpers extracted from pm_hourly_clone_bot.py (Pass 1)."""
from __future__ import annotations

import math

# EDGE_K is a trading parameter; import from the canonical config location
# so the value stays in one place.  During the refactor the constant lives in
# the original monolith — we reference it lazily to avoid circular imports.
_EDGE_K_DEFAULT = 0.05  # fallback: matches the monolith's hard-coded value


def _get_edge_k() -> float:
    """Return EDGE_K, trying the config module first."""
    try:
        from src.util.config import EDGE_K  # type: ignore[import-untyped]
        return EDGE_K
    except Exception:
        return _EDGE_K_DEFAULT


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
    return 1.0 / (1.0 + math.exp(-_get_edge_k() * delta_bps))
