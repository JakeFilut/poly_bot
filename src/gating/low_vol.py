"""Low-volatility detection and throttling.

Extracted from the regime-awareness logic in pm_hourly_clone_bot.py
(REGIME_LOW_VOL_THRESHOLD, REGIME_LOW_VOL_REDUCTION, etc.).

* ``LowVolDetector``  -- answers "is vol below the threshold?"
* ``LowVolThrottler`` -- decides what to *do* about it (BLOCK / THROTTLE / PASS)
* ``LowVolResult``    -- immutable result object returned by the throttler
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class LowVolResult:
    """Outcome of the low-vol throttle decision."""

    is_low_vol: bool
    vol_bps: float
    action: str  # "PASS", "BLOCK", "THROTTLE"
    size_mult: float  # 1.0 if no throttle
    edge_bump_cents: float  # 0.0 if no throttle


class LowVolDetector:
    """Simple threshold check on rolling volatility (bps std-dev)."""

    def __init__(
        self,
        threshold_bps: float = 3.0,
        lookback_sec: float = 60.0,
    ):
        self.threshold_bps = threshold_bps
        self.lookback_sec = lookback_sec

    def is_low_vol(self, vol_bps: float) -> bool:
        """Return ``True`` when *vol_bps* is below the configured threshold."""
        return vol_bps < self.threshold_bps


class LowVolThrottler:
    """Policy layer that converts a low-vol flag into an actionable result.

    Parameters
    ----------
    mode : str
        One of ``"BLOCK"``, ``"THROTTLE"``, or ``"PASS"``.
    size_mult : float
        Size multiplier applied when mode is ``"THROTTLE"`` (e.g. 0.50).
    edge_bump_cents : float
        Additional edge required (in cents) when throttling.
    """

    def __init__(
        self,
        mode: str = "THROTTLE",
        size_mult: float = 0.50,
        edge_bump_cents: float = 0.0,
    ):
        self.mode = mode  # "BLOCK", "THROTTLE", "PASS"
        self.size_mult = size_mult
        self.edge_bump_cents = edge_bump_cents

    def apply(self, is_low_vol: bool, vol_bps: float) -> LowVolResult:
        """Return a :class:`LowVolResult` describing the throttle decision."""
        if not is_low_vol:
            return LowVolResult(
                is_low_vol=False,
                vol_bps=vol_bps,
                action="PASS",
                size_mult=1.0,
                edge_bump_cents=0.0,
            )

        if self.mode == "BLOCK":
            return LowVolResult(
                is_low_vol=True,
                vol_bps=vol_bps,
                action="BLOCK",
                size_mult=0.0,
                edge_bump_cents=0.0,
            )

        if self.mode == "THROTTLE":
            return LowVolResult(
                is_low_vol=True,
                vol_bps=vol_bps,
                action="THROTTLE",
                size_mult=self.size_mult,
                edge_bump_cents=self.edge_bump_cents,
            )

        # mode == "PASS" (or anything else) -- no action
        return LowVolResult(
            is_low_vol=True,
            vol_bps=vol_bps,
            action="PASS",
            size_mult=1.0,
            edge_bump_cents=0.0,
        )
