"""
Utility helper functions for the arbitrage bot.
"""

import csv
import os
from datetime import datetime, timezone
from typing import Any, List, Optional


def now_mst() -> str:
    """Return current Mountain Standard Time in format: Feb 06 2026 03 10"""
    from datetime import timedelta
    mst = datetime.now(timezone.utc) - timedelta(hours=7)
    return mst.strftime("%b %d %Y %H %M")


def now_iso() -> str:
    """Return current UTC time in ISO format."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def safe_float(x: Any) -> Optional[float]:
    """Safely convert a value to float, returning None on failure."""
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def parse_iso_dt(s: str) -> Optional[datetime]:
    """Parse an ISO datetime string, handling 'Z' suffix."""
    try:
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def ensure_csv_header(path: str, header: List[str]) -> None:
    """Ensure a CSV file exists with the given header row."""
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(header)


def get_window_progress() -> float:
    """
    Calculate progress through current 15-minute market window.

    Returns:
        float: Progress from 0.0 (start) to 1.0 (end of window)

    Example: At 10:07:30, we're 50% through the 10:00-10:15 window
    """
    now = datetime.now(timezone.utc)
    mins_in_window = now.minute % 15
    secs_in_window = mins_in_window * 60 + now.second
    total_secs = 15 * 60  # 900 seconds
    return secs_in_window / total_secs


def calc_spread_pct(kalshi_strike: float, poly_strike: float) -> float:
    """
    Calculate spread percentage between Kalshi and Poly strikes.

    Formula: abs(kalshi_strike - poly_strike) / min(kalshi_strike, poly_strike)

    Returns:
        float: Spread as a decimal (e.g., 0.0006 = 0.06%)
    """
    if not kalshi_strike or not poly_strike:
        return float('inf')
    min_strike = min(kalshi_strike, poly_strike)
    if min_strike <= 0:
        return float('inf')
    return abs(kalshi_strike - poly_strike) / min_strike
