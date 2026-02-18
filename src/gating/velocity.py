"""Velocity estimation for trade-entry gating.

Wraps the existing ``delta_velocity_bps_per_min`` helper so that callers
can work with a stateful estimator object rather than a bare function.
"""

from __future__ import annotations

from typing import List, Tuple


class VelocityEstimator:
    """Tracks rolling window of spot/delta moves and returns velocity."""

    def __init__(self, lookback_sec: float = 30.0):
        self.lookback_sec = lookback_sec

    def vel_bps_per_min(
        self, delta_series: List[Tuple[str, float]]
    ) -> float:
        """Compute delta velocity in bps/min from a delta history series.

        Delegates to ``delta_velocity_bps_per_min`` from
        ``src.strategy.sizing``.
        """
        from src.strategy.sizing import delta_velocity_bps_per_min

        return delta_velocity_bps_per_min(delta_series, self.lookback_sec)
