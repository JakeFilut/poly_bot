"""
Position sizing helpers extracted from pm_hourly_clone_bot.py (Pass 1).

sizing_mult, zscore, delta_velocity_bps_per_min.

No behavior changes -- exact same logic as the monolith.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from src.config.settings import (
    SIZING_MULTIPLIERS,
    Z_WINDOW_SEC,
)
from src.util.time import utc_now


# =============================================================================
# Size multiplier by absolute delta
# =============================================================================

def sizing_mult(abs_delta_bps: float) -> float:
    """Return clip-size multiplier for a given absolute delta (bps)."""
    for lo, hi, mult in SIZING_MULTIPLIERS:
        if lo <= abs_delta_bps < hi:
            return mult
    return 0.0


# =============================================================================
# Z-score of recent delta series
# =============================================================================

def zscore(delta_series: List[Tuple[str, float]]) -> float:
    """Z-score of latest delta vs last ~Z_WINDOW_SEC seconds."""
    if len(delta_series) < 10:
        return 0.0
    now = utc_now()
    cutoff = now - timedelta(seconds=Z_WINDOW_SEC)
    vals: List[float] = []
    for ts_iso, d in reversed(delta_series):
        try:
            t = datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if t < cutoff:
            break
        vals.append(d)
    if len(vals) < 10:
        return 0.0
    mu = sum(vals) / len(vals)
    var = sum((x - mu) ** 2 for x in vals) / max(1, len(vals) - 1)
    sd = math.sqrt(var) if var > 1e-12 else 0.0
    if sd <= 1e-12:
        return 0.0
    return (vals[0] - mu) / sd  # vals[0] is latest because we appended reversed


# =============================================================================
# Delta velocity (bps / min)
# =============================================================================

_VEL_MIN_DT_SEC = 3.0  # minimum dt to avoid noise from sub-second jitter


def delta_velocity_bps_per_min(delta_series: List[Tuple[str, float]],
                               lookback_sec: float = 30.0) -> float:
    """Estimate how fast delta is changing (bps per minute).

    Anchors to latest entry timestamp (not wall-clock) to avoid sub-second
    precision mismatch with iso_z() second-truncated timestamps.
    Falls back to oldest available point with dt >= 3 s when no point
    reaches the full lookback window.
    """
    if len(delta_series) < 2:
        return 0.0
    latest_ts, latest_d = delta_series[-1]
    latest_t = datetime.strptime(latest_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    target = latest_t - timedelta(seconds=lookback_sec)

    # Walk newest→oldest (excluding latest) looking for a point at the
    # lookback boundary.  Keep a fallback = oldest point with dt >= 3 s.
    prior_d = None
    prior_t = None
    for ts_iso, d in reversed(delta_series[:-1]):
        t = datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        dt_sec = (latest_t - t).total_seconds()
        if dt_sec < _VEL_MIN_DT_SEC:
            continue  # too close to latest — skip
        # Always update fallback (gets progressively older as we walk back)
        prior_d = d
        prior_t = t
        if t <= target:
            break  # at or past lookback — ideal point

    if prior_d is None or prior_t is None:
        return 0.0
    dt_sec = (latest_t - prior_t).total_seconds()
    if dt_sec <= 0.0:
        return 0.0
    dt_min = dt_sec / 60.0
    return (latest_d - prior_d) / dt_min


def delta_velocity_debug(delta_series: List[Tuple[str, float]],
                         lookback_sec: float = 30.0) -> dict:
    """Like delta_velocity_bps_per_min but returns full diagnostics dict."""
    result = {"vel": 0.0, "delta_now": 0.0, "delta_prev": 0.0,
              "dt_sec": 0.0, "n_points": len(delta_series), "status": "ok"}
    if len(delta_series) < 2:
        result["status"] = "too_few_points"
        return result
    latest_ts, latest_d = delta_series[-1]
    latest_t = datetime.strptime(latest_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    target = latest_t - timedelta(seconds=lookback_sec)
    result["delta_now"] = latest_d

    prior_d = None
    prior_t = None
    for ts_iso, d in reversed(delta_series[:-1]):
        t = datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        dt_sec = (latest_t - t).total_seconds()
        if dt_sec < _VEL_MIN_DT_SEC:
            continue
        prior_d = d
        prior_t = t
        if t <= target:
            break

    if prior_d is None or prior_t is None:
        result["status"] = "no_prior_point"
        return result
    dt_sec = (latest_t - prior_t).total_seconds()
    if dt_sec <= 0.0:
        result["status"] = "dt_zero"
        return result
    dt_min = dt_sec / 60.0
    vel = (latest_d - prior_d) / dt_min
    result.update({"vel": vel, "delta_prev": prior_d, "dt_sec": dt_sec})
    return result
