#!/usr/bin/env python3
"""
trade_quality_report.py – Trade-quality analytics from bot JSONL logs.

Parses DRY_FILL events, reconstructs VWAP inventory per (slug, outcome),
and computes realized PnL on each SELL fill.

Metrics:
  A) Win Rate            – fraction of sell fills with positive PnL
  B) Average Win         – mean PnL of winning sells
  C) Average Loss        – mean PnL of losing sells
  D) Profit Per Sell     – total realized / total sell fills
  E) Hourly Sharpe-like  – mean(hourly_pnl) / std(hourly_pnl)

Also prints: total buy/sell fills, total realized, max/min hour.

Outputs:
  - Console summary table
  - CSV file (trade_quality.csv)

No external dependencies (stdlib only).

Usage:
    python trade_quality_report.py bot_log.jsonl
    python trade_quality_report.py bot_log.jsonl --tz ET
    python trade_quality_report.py bot_log.jsonl --csv output.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


# ──────────────────────────────────────────────────────────────────────
# Timezone handling (stdlib only)
# ──────────────────────────────────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from datetime import timedelta, tzinfo as _tzinfo

    class _ET(_tzinfo):
        def utcoffset(self, dt):
            return timedelta(hours=-5)

        def tzname(self, dt):
            return "ET"

        def dst(self, dt):
            return timedelta(0)

    class ZoneInfo:  # type: ignore[no-redef]
        def __init__(self, name: str):
            if "New_York" in name or name == "ET":
                self._tz = _ET()
            else:
                self._tz = timezone.utc

        def utcoffset(self, dt):
            return self._tz.utcoffset(dt)

        def tzname(self, dt):
            return self._tz.tzname(dt)

        def dst(self, dt):
            return self._tz.dst(dt)


_ET_TZ = ZoneInfo("America/New_York")
_UTC = timezone.utc


# ──────────────────────────────────────────────────────────────────────
# Timestamp parsing
# ──────────────────────────────────────────────────────────────────────
def _parse_ts(ts_str: str) -> Optional[datetime]:
    """Parse ISO-8601 timestamp to datetime (UTC)."""
    if not ts_str:
        return None
    try:
        ts_str = ts_str.rstrip("Z")
        return datetime.fromisoformat(ts_str).replace(tzinfo=_UTC)
    except (ValueError, TypeError):
        return None


def _hour_key_et(dt: datetime) -> str:
    """Return ET hour key like '2025-05-28 14:00'."""
    dt_et = dt.astimezone(_ET_TZ)
    return dt_et.strftime("%Y-%m-%d %H:00")


# ──────────────────────────────────────────────────────────────────────
# VWAP Inventory tracker
# ──────────────────────────────────────────────────────────────────────
class VWAPInventory:
    """Tracks per-(slug, outcome) VWAP cost basis."""

    def __init__(self):
        # (slug, outcome) -> {shares: float, avg_cost: float}
        self.positions: Dict[Tuple[str, str], dict] = {}

    def apply_buy(self, slug: str, outcome: str, qty: float,
                  price: float) -> None:
        key = (slug, outcome)
        pos = self.positions.get(key)
        if pos is None:
            self.positions[key] = {"shares": qty, "avg_cost": price}
        else:
            total_cost = pos["shares"] * pos["avg_cost"] + qty * price
            pos["shares"] += qty
            pos["avg_cost"] = (total_cost / pos["shares"]
                               if pos["shares"] > 0 else 0.0)

    def get_avg_cost(self, slug: str, outcome: str) -> float:
        pos = self.positions.get((slug, outcome))
        if pos is None or pos["shares"] <= 0:
            return 0.0
        return pos["avg_cost"]

    def apply_sell(self, slug: str, outcome: str, qty: float) -> None:
        key = (slug, outcome)
        pos = self.positions.get(key)
        if pos is None:
            return
        pos["shares"] = max(0.0, pos["shares"] - qty)
        if pos["shares"] < 0.01:
            pos["shares"] = 0.0


# ──────────────────────────────────────────────────────────────────────
# Log parsing
# ──────────────────────────────────────────────────────────────────────
def parse_log(path: str) -> List[Dict[str, Any]]:
    """Read JSONL log, return only DRY_FILL events."""
    records = []
    opener = open(path, "r") if path != "-" else sys.stdin
    try:
        for line in opener:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("event") == "DRY_FILL":
                records.append(rec)
    finally:
        if path != "-":
            opener.close()
    return records


# ──────────────────────────────────────────────────────────────────────
# Core computation
# ──────────────────────────────────────────────────────────────────────
def compute_metrics(fills: List[dict]) -> dict:
    """Process DRY_FILL events and compute all trade quality metrics.

    Returns a dict with all computed values.
    """
    inv = VWAPInventory()

    total_buy_fills = 0
    total_sell_fills = 0
    total_realized_pnl = 0.0
    sell_pnls: List[float] = []

    # hourly_key (ET) -> list of realized pnl from sells in that hour
    hourly_pnl: Dict[str, float] = defaultdict(float)

    # Sort by timestamp to process chronologically
    timed_fills = []
    for rec in fills:
        ts = _parse_ts(rec.get("ts", ""))
        if ts is None:
            continue
        timed_fills.append((ts, rec))
    timed_fills.sort(key=lambda x: x[0])

    for ts, rec in timed_fills:
        slug = rec.get("slug", "")
        outcome = rec.get("outcome", "")
        side = rec.get("side", "")
        price = float(rec.get("price", 0) or 0)
        qty = float(rec.get("qty_shares", 0) or rec.get("qty", 0) or 0)

        if qty <= 0 or price <= 0 or not slug or not outcome:
            continue

        if side == "BUY":
            inv.apply_buy(slug, outcome, qty, price)
            total_buy_fills += 1

        elif side == "SELL":
            avg_cost = inv.get_avg_cost(slug, outcome)
            pnl = (price - avg_cost) * qty
            inv.apply_sell(slug, outcome, qty)

            total_sell_fills += 1
            total_realized_pnl += pnl
            sell_pnls.append(pnl)

            hk = _hour_key_et(ts)
            hourly_pnl[hk] += pnl

    # ── A) Win Rate ──
    wins = [p for p in sell_pnls if p > 0]
    losses = [p for p in sell_pnls if p < 0]
    win_rate = len(wins) / total_sell_fills if total_sell_fills > 0 else 0.0

    # ── B) Average Win ──
    avg_win = sum(wins) / len(wins) if wins else 0.0

    # ── C) Average Loss ──
    avg_loss = sum(losses) / len(losses) if losses else 0.0

    # ── D) Profit Per Sell Fill ──
    profit_per_sell = (total_realized_pnl / total_sell_fills
                       if total_sell_fills > 0 else 0.0)

    # ── E) Hourly Sharpe-like ──
    hourly_vals = list(hourly_pnl.values())
    if hourly_vals:
        mean_hourly = sum(hourly_vals) / len(hourly_vals)
        variance = sum((v - mean_hourly) ** 2 for v in hourly_vals) / len(hourly_vals)
        std_hourly = math.sqrt(variance)
        sharpe_like = mean_hourly / std_hourly if std_hourly > 0 else 0.0
        max_hour_val = max(hourly_vals)
        min_hour_val = min(hourly_vals)
        max_hour_key = [k for k, v in hourly_pnl.items() if v == max_hour_val][0]
        min_hour_key = [k for k, v in hourly_pnl.items() if v == min_hour_val][0]
    else:
        mean_hourly = 0.0
        std_hourly = 0.0
        sharpe_like = 0.0
        max_hour_val = 0.0
        min_hour_val = 0.0
        max_hour_key = "N/A"
        min_hour_key = "N/A"

    return {
        "total_buy_fills": total_buy_fills,
        "total_sell_fills": total_sell_fills,
        "total_realized_pnl": total_realized_pnl,
        "win_rate": win_rate,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "profit_per_sell": profit_per_sell,
        "mean_hourly": mean_hourly,
        "std_hourly": std_hourly,
        "sharpe_like": sharpe_like,
        "max_hour_val": max_hour_val,
        "max_hour_key": max_hour_key,
        "min_hour_val": min_hour_val,
        "min_hour_key": min_hour_key,
        "sell_pnls": sell_pnls,
        "hourly_pnl": dict(hourly_pnl),
    }


# ──────────────────────────────────────────────────────────────────────
# Console output
# ──────────────────────────────────────────────────────────────────────
def print_report(m: dict) -> None:
    """Print the trade quality summary to console."""
    sep = "=" * 50
    print(f"\n{sep}")
    print("  TRADE QUALITY REPORT")
    print(f"{sep}")
    print()
    print(f"  Total Buy Fills:    {m['total_buy_fills']}")
    print(f"  Total Sell Fills:   {m['total_sell_fills']}")
    print(f"  Total Realized:     ${m['total_realized_pnl']:+.4f}")
    print()
    print(f"  Win Rate:           {m['win_rate'] * 100:.1f}%")
    print(f"  Avg Win:            ${m['avg_win']:+.4f}")
    print(f"  Avg Loss:           ${m['avg_loss']:+.4f}")
    print(f"  Profit per Sell:    ${m['profit_per_sell']:+.4f}")
    print()
    print(f"  Hourly Mean:        ${m['mean_hourly']:+.4f}")
    print(f"  Hourly Std:         ${m['std_hourly']:.4f}")
    print(f"  Sharpe-like:        {m['sharpe_like']:.4f}")
    print()
    print(f"  Max Hour:           ${m['max_hour_val']:+.4f}  ({m['max_hour_key']})")
    print(f"  Min Hour:           ${m['min_hour_val']:+.4f}  ({m['min_hour_key']})")
    print(f"\n{sep}\n")


# ──────────────────────────────────────────────────────────────────────
# CSV output
# ──────────────────────────────────────────────────────────────────────
def write_csv(path: str, m: dict) -> None:
    """Write per-sell-fill detail and hourly summary to CSV."""
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    fieldnames = [
        "metric", "value",
    ]

    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        rows = [
            ("total_buy_fills", m["total_buy_fills"]),
            ("total_sell_fills", m["total_sell_fills"]),
            ("total_realized_pnl", round(m["total_realized_pnl"], 6)),
            ("win_rate", round(m["win_rate"], 6)),
            ("avg_win", round(m["avg_win"], 6)),
            ("avg_loss", round(m["avg_loss"], 6)),
            ("profit_per_sell", round(m["profit_per_sell"], 6)),
            ("hourly_mean", round(m["mean_hourly"], 6)),
            ("hourly_std", round(m["std_hourly"], 6)),
            ("sharpe_like", round(m["sharpe_like"], 6)),
            ("max_hour", round(m["max_hour_val"], 6)),
            ("max_hour_label", m["max_hour_key"]),
            ("min_hour", round(m["min_hour_val"], 6)),
            ("min_hour_label", m["min_hour_key"]),
        ]
        for metric, value in rows:
            w.writerow({"metric": metric, "value": value})

        # Blank separator row then hourly breakdown
        w.writerow({"metric": "", "value": ""})
        w.writerow({"metric": "--- HOURLY BREAKDOWN ---", "value": ""})

        for hk in sorted(m["hourly_pnl"].keys()):
            w.writerow({
                "metric": f"hour_{hk}",
                "value": round(m["hourly_pnl"][hk], 6),
            })


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Trade Quality Report – per-fill PnL analytics from bot JSONL logs.",
    )
    parser.add_argument(
        "logfile",
        help='Path to JSONL log file (or "-" for stdin)',
    )
    parser.add_argument(
        "--csv",
        default="trade_quality.csv",
        help="Output CSV path (default: trade_quality.csv)",
    )
    args = parser.parse_args()

    fills = parse_log(args.logfile)
    if not fills:
        print("ERROR: No DRY_FILL events found in log file.", file=sys.stderr)
        sys.exit(1)

    m = compute_metrics(fills)

    if m["total_sell_fills"] == 0:
        print("WARNING: No SELL fills found. Only BUY fills present.",
              file=sys.stderr)

    print_report(m)
    write_csv(args.csv, m)
    print(f"  CSV written to: {args.csv}")


if __name__ == "__main__":
    main()
