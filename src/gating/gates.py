"""Unified gate evaluator -- single entry-point for all pre-trade checks.

Consolidates the scattered gating logic from pm_hourly_clone_bot.py into
one composable class:

* Low-vol detection / throttle
* Spread limit check  (delegates to ``src.strategy.f247_like.spread_limit``)
* Whipsaw filter       (delegates to ``src.strategy.f247_like.whipsaw_ok``)

Additional gates (rate-limit, warmup, risk caps, parity suppression, etc.)
will be wired in during later passes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from src.gating.low_vol import LowVolDetector, LowVolThrottler
from src.gating.velocity import VelocityEstimator


@dataclass
class GateResult:
    """Outcome of running all pre-trade gates."""

    allowed: bool
    reasons_blocked: List[str] = field(default_factory=list)
    reasons_throttled: List[str] = field(default_factory=list)
    size_mult: float = 1.0
    edge_bump_cents: float = 0.0
    diagnostics: Dict = field(default_factory=dict)


class GateEvaluator:
    """Centralised gating: LOW_VOL, spread, velocity, warmup, risk caps, rate limits.

    Parameters
    ----------
    low_vol_detector : LowVolDetector
        Answers "is the market in a low-vol regime?"
    low_vol_throttler : LowVolThrottler
        Decides what to do when low-vol is detected.
    velocity_estimator : VelocityEstimator
        Provides rolling velocity (bps/min) from delta history.
    """

    def __init__(
        self,
        low_vol_detector: LowVolDetector,
        low_vol_throttler: LowVolThrottler,
        velocity_estimator: VelocityEstimator,
    ):
        self.low_vol_detector = low_vol_detector
        self.low_vol_throttler = low_vol_throttler
        self.velocity_estimator = velocity_estimator

    # ------------------------------------------------------------------
    def evaluate(
        self,
        *,
        symbol: str,
        side: str,
        price: float,
        size: float,
        spread_cents: float,
        abs_edge_bps: float,
        thr_bps: float,
        vol_bps: float,
        t_min: float,
        delta_bps: float,
        vel: float,
        edge_sign_since: Optional[float] = None,
        **kwargs,
    ) -> GateResult:
        """Run all gates and return a single :class:`GateResult`.

        Parameters
        ----------
        symbol : str
            Market symbol / slug.
        side : str
            ``"BUY"`` or ``"SELL"``.
        price, size : float
            Intended order price and size.
        spread_cents : float
            Current top-of-book spread in cents.
        abs_edge_bps : float
            Absolute edge in basis points.
        thr_bps : float
            Entry threshold in basis points.
        vol_bps : float
            Rolling volatility (std-dev of returns, bps).
        t_min : float
            Minutes past the hour.
        delta_bps : float
            Current delta in basis points.
        vel : float
            Current velocity in bps/min.
        edge_sign_since : float or None
            Epoch timestamp when the current edge sign became stable.
        **kwargs
            Reserved for future gate inputs.
        """
        result = GateResult(allowed=True)

        # 1. Low-vol check --------------------------------------------------
        is_lv = self.low_vol_detector.is_low_vol(vol_bps)
        lv_result = self.low_vol_throttler.apply(is_lv, vol_bps)

        if lv_result.action == "BLOCK":
            result.allowed = False
            result.reasons_blocked.append(
                f"LOW_VOL(vol={vol_bps:.1f}bps)"
            )
        elif lv_result.action == "THROTTLE":
            result.size_mult *= lv_result.size_mult
            result.edge_bump_cents += lv_result.edge_bump_cents
            result.reasons_throttled.append(
                f"LOW_VOL_THROTTLE(vol={vol_bps:.1f}bps,mult={lv_result.size_mult})"
            )

        # 2. Spread check ---------------------------------------------------
        from src.strategy.f247_like import spread_limit as _spread_limit

        max_spread = _spread_limit(t_min, abs_edge_bps, symbol)
        if spread_cents / 100.0 > max_spread:
            result.allowed = False
            result.reasons_blocked.append(
                f"SPREAD({spread_cents:.1f}c>{max_spread * 100:.1f}c)"
            )

        # 3. Whipsaw check --------------------------------------------------
        from src.strategy.f247_like import whipsaw_ok

        ws_ok, ws_reason = whipsaw_ok(delta_bps, vel, edge_sign_since)
        if not ws_ok:
            result.allowed = False
            result.reasons_blocked.append(f"WHIPSAW({ws_reason})")

        # Diagnostics -------------------------------------------------------
        result.diagnostics = {
            "low_vol": is_lv,
            "vol_bps": vol_bps,
            "spread_cents": spread_cents,
            "ws_ok": ws_ok,
        }

        return result
