"""Gating module -- centralised trade-entry gates for poly_bot.

Pass 2 refactoring: new wrapper classes that encapsulate the gating logic
previously embedded in pm_hourly_clone_bot.py (low-vol detection, spread
checks, whipsaw filters, velocity estimation, rate limiting, and the
unified GateEvaluator).
"""

from src.gating.velocity import VelocityEstimator
from src.gating.low_vol import LowVolDetector, LowVolThrottler, LowVolResult
from src.gating.gates import GateEvaluator, GateResult
from src.gating.report import GateReporter

__all__ = [
    "VelocityEstimator",
    "LowVolDetector",
    "LowVolThrottler",
    "LowVolResult",
    "GateEvaluator",
    "GateResult",
    "GateReporter",
]
