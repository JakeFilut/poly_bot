"""
BotApp — top-level wiring for poly_bot.

Instantiates all subsystems (feeds, strategy, gating, trading) and
exposes them to the Bot class.  In Pass 1 this is a thin shell; the
Bot class in pm_hourly_clone_bot.py still owns all logic.  Over time
the Bot methods will migrate here.
"""
from __future__ import annotations

from src.config.settings import (
    REGIME_LOW_VOL_THRESHOLD,
    REGIME_VOL_LOOKBACK_SEC,
    REGIME_LOW_VOL_REDUCTION,
    MIN_DELTA_VEL_BPS_PER_MIN,
)
from src.gating.velocity import VelocityEstimator
from src.gating.low_vol import LowVolDetector, LowVolThrottler
from src.gating.gates import GateEvaluator
from src.gating.report import GateReporter
from src.trading.order_manager import OrderManager


class BotApp:
    """Wires all subsystems together.  The main Bot inherits or holds this."""

    def __init__(self):
        # ── Gating subsystem ──
        self.velocity_estimator = VelocityEstimator(
            lookback_sec=30.0,
        )
        self.low_vol_detector = LowVolDetector(
            threshold_bps=REGIME_LOW_VOL_THRESHOLD,
            lookback_sec=REGIME_VOL_LOOKBACK_SEC,
        )
        self.low_vol_throttler = LowVolThrottler(
            mode="THROTTLE",
            size_mult=REGIME_LOW_VOL_REDUCTION,
            edge_bump_cents=0.0,
        )
        self.gate_evaluator = GateEvaluator(
            low_vol_detector=self.low_vol_detector,
            low_vol_throttler=self.low_vol_throttler,
            velocity_estimator=self.velocity_estimator,
        )
        self.gate_reporter = GateReporter()

        # ── Trading subsystem ──
        self.order_manager = OrderManager()
