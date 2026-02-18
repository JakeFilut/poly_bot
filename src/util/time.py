"""Time / parsing helpers extracted from pm_hourly_clone_bot.py (Pass 1)."""
from __future__ import annotations

import re
from datetime import datetime, timezone

import pytz

# ---------------------------------------------------------------------------
# Month lookup (used by slug parser)
# ---------------------------------------------------------------------------
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


# ---------------------------------------------------------------------------
# Core time helpers
# ---------------------------------------------------------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Slug parsing
# ---------------------------------------------------------------------------
def parse_hour_start_from_slug(slug: str, year: int = None) -> datetime:
    """
    Parse slug like: bitcoin-up-or-down-february-14-9pm-et
    Returns hour start UTC.
    """
    et = pytz.timezone("US/Eastern")
    if year is None:
        year = utc_now().year
    m = re.search(
        r"-(january|february|march|april|may|june|july|august|september"
        r"|october|november|december)-(\d{1,2})-(\d{1,2})(am|pm)-et$",
        slug,
    )
    if not m:
        raise ValueError(f"Cannot parse hour from slug: {slug}")
    month = MONTHS[m.group(1)]
    day = int(m.group(2))
    hour12 = int(m.group(3))
    ampm = m.group(4)
    hour = hour12 % 12 + (12 if ampm == "pm" else 0)
    dt_local = et.localize(datetime(year, month, day, hour, 0, 0))
    return dt_local.astimezone(timezone.utc)


def minutes_into_hour(hour_start_utc: datetime, now_utc: datetime) -> float:
    return (now_utc - hour_start_utc).total_seconds() / 60.0


# ---------------------------------------------------------------------------
# Phase / display helpers
# ---------------------------------------------------------------------------
def _phase(t_min: float) -> str:
    """Time band within the hour window."""
    if t_min < 10.0:
        return "OPENING"
    if t_min < 50.0:
        return "MID"
    return "CLOSING"


def _hour_label_et(hour_start_utc_str: str) -> str:
    """Convert '2026-02-14T18:00:00Z' -> '2026-02-14 13:00 ET'."""
    try:
        dt = datetime.strptime(hour_start_utc_str, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
        et = pytz.timezone("US/Eastern")
        dt_et = dt.astimezone(et)
        return dt_et.strftime("%Y-%m-%d %H:%M ET")
    except Exception:
        return hour_start_utc_str
