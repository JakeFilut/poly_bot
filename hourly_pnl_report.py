#!/usr/bin/env python3
"""
hourly_pnl_report.py – Standalone hourly PnL analysis from bot JSONL logs.

Parses DRY_FILL and FILL events to reconstruct:
  - Realized PnL per hour (from SELL fills)
  - Inventory changes (buys/sells)
  - Unrealized PnL per hour (mark-to-market delta)
  - Net PnL = realized + unrealized delta

Works for DRY_RUN (probabilistic + instant) and LIVE modes.

Outputs:
  - Console table
  - CSV file (hourly_pnl.csv)

No external dependencies beyond stdlib.

Usage:
    python hourly_pnl_report.py bot_log.jsonl
    python hourly_pnl_report.py bot_log.jsonl --tz ET
    python hourly_pnl_report.py bot_log.jsonl --fee-bps 5
    python hourly_pnl_report.py bot_log.jsonl --csv output.csv
"""
from __future__ import annotations

import argparse
import csv
import json
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
    # Python 3.8 fallback: basic ET offset (no DST handling)
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

        def __repr__(self):
            return "ET"

        # Allow use with .astimezone()
        def localize(self, dt):
            return dt.replace(tzinfo=self._tz)

        # Make it work as a tzinfo
        def utcoffset(self, dt):
            return self._tz.utcoffset(dt)

        def tzname(self, dt):
            return self._tz.tzname(dt)

        def dst(self, dt):
            return self._tz.dst(dt)


_ET_TZ = ZoneInfo("America/New_York")
_UTC = timezone.utc


# ──────────────────────────────────────────────────────────────────────
# Inventory tracker (reconstructed from fills)
# ──────────────────────────────────────────────────────────────────────
class InventoryTracker:
    """Tracks inventory positions from fill events."""

    def __init__(self, fee_bps: float = 0.0):
        # (slug, outcome) -> {shares, avg_cost}
        self.positions: Dict[Tuple[str, str], dict] = {}
        self.fee_bps = fee_bps
        self.fee_rate = fee_bps / 10_000.0 if fee_bps > 0 else 0.0

    def apply_buy(self, slug: str, outcome: str, qty: float,
                  price: float) -> None:
        key = (slug, outcome)
        pos = self.positions.get(key)
        if pos is None:
            self.positions[key] = {"shares": qty, "avg_cost": price}
        else:
            total_cost = pos["shares"] * pos["avg_cost"] + qty * price
            pos["shares"] += qty
            pos["avg_cost"] = total_cost / pos["shares"] if pos["shares"] > 0 else 0.0

    def apply_sell(self, slug: str, outcome: str, qty: float,
                   sell_price: float) -> float:
        """Apply sell, return realized PnL for this fill (fee-adjusted)."""
        key = (slug, outcome)
        pos = self.positions.get(key)
        if pos is None or pos["shares"] <= 0:
            return 0.0

        avg_cost = pos["avg_cost"]
        sell_qty = min(qty, pos["shares"])
        raw_realized = (sell_price - avg_cost) * sell_qty

        if self.fee_rate > 0:
            fee = self.fee_rate * sell_qty * (sell_price + avg_cost)
            raw_realized -= fee

        pos["shares"] -= sell_qty
        if pos["shares"] < 0.01:
            pos["shares"] = 0.0
            del self.positions[key]

        return raw_realized

    def unrealized_pnl(self, prices: Dict[Tuple[str, str], float]) -> float:
        """Compute total unrealized PnL given current mark prices.

        prices: (slug, outcome) -> mark_price (best_bid or mid)
        """
        total = 0.0
        for (slug, outcome), pos in self.positions.items():
            if pos["shares"] <= 0:
                continue
            mark = prices.get((slug, outcome))
            if mark is None:
                continue
            pnl = pos["shares"] * (mark - pos["avg_cost"])
            if self.fee_rate > 0:
                pnl -= self.fee_rate * pos["shares"] * (mark + pos["avg_cost"])
            total += pnl
        return total

    def total_notional(self) -> float:
        return sum(
            p["shares"] * p["avg_cost"]
            for p in self.positions.values()
            if p["shares"] > 0
        )


# ──────────────────────────────────────────────────────────────────────
# Log parsing
# ──────────────────────────────────────────────────────────────────────
def _parse_ts(ts_str: str) -> Optional[datetime]:
    """Parse ISO-8601 timestamp to datetime (UTC)."""
    if not ts_str:
        return None
    try:
        ts_str = ts_str.rstrip("Z")
        if "+" in ts_str or ts_str.endswith("Z"):
            return datetime.fromisoformat(ts_str)
        return datetime.fromisoformat(ts_str).replace(tzinfo=_UTC)
    except (ValueError, TypeError):
        return None


def _hour_key(dt: datetime, tz_name: str) -> str:
    """Return hour key string in the requested timezone."""
    if tz_name == "ET":
        dt_local = dt.astimezone(_ET_TZ)
    else:
        dt_local = dt.astimezone(_UTC)
    return dt_local.strftime("%Y-%m-%d %H:00")


def _hour_display(dt: datetime, tz_name: str) -> str:
    """Return human-readable hour display."""
    if tz_name == "ET":
        dt_local = dt.astimezone(_ET_TZ)
        return dt_local.strftime("%H:%M ET")
    else:
        return dt.astimezone(_UTC).strftime("%H:%M UTC")


def parse_log(path: str) -> List[Dict[str, Any]]:
    """Read JSONL log, skip unparseable lines."""
    records = []
    opener = open(path, "r") if path != "-" else sys.stdin
    try:
        for line in opener:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    finally:
        if path != "-":
            opener.close()
    return records


# ──────────────────────────────────────────────────────────────────────
# Price extraction from log events
# ──────────────────────────────────────────────────────────────────────
def _extract_prices_per_hour(records: List[dict], tz_name: str
                             ) -> Dict[str, Dict[Tuple[str, str], float]]:
    """Extract last known mark price per (slug, outcome) per hour.

    Uses ROLLUP top_positions, FILL/DRY_FILL prices, and DECISION events
    to reconstruct approximate mark prices.
    """
    # hour_key -> {(slug, outcome) -> last_price}
    prices: Dict[str, Dict[Tuple[str, str], float]] = defaultdict(dict)

    for rec in records:
        ts = _parse_ts(rec.get("ts", ""))
        if ts is None:
            continue
        hk = _hour_key(ts, tz_name)
        event = rec.get("event", "")
        slug = rec.get("slug", "")
        outcome = rec.get("outcome", "")

        if event in ("FILL", "DRY_FILL") and slug and outcome:
            price = rec.get("price", 0)
            if price > 0:
                prices[hk][(slug, outcome)] = price

        elif event == "DECISION" and slug and outcome:
            # Use mid or price from decisions as mark reference
            mid = rec.get("mid")
            price = rec.get("price")
            best_bid = rec.get("best_bid")
            if best_bid and best_bid > 0:
                prices[hk][(slug, outcome)] = best_bid
            elif mid and mid > 0:
                prices[hk][(slug, outcome)] = mid
            elif price and price > 0:
                prices[hk][(slug, outcome)] = price

    return prices


# ──────────────────────────────────────────────────────────────────────
# Main computation
# ──────────────────────────────────────────────────────────────────────
def compute_hourly_pnl(records: List[dict], tz_name: str = "UTC",
                       fee_bps: float = 0.0) -> List[dict]:
    """Compute hourly PnL from log records.

    Returns list of dicts, one per hour, sorted chronologically.
    """
    # 1. Check if HOURLY_PNL events exist (emitted by bot in real-time)
    hourly_events = [r for r in records if r.get("event") == "HOURLY_PNL"]
    if hourly_events:
        return _from_hourly_events(hourly_events, tz_name)

    # 2. Reconstruct from fill events
    return _from_fills(records, tz_name, fee_bps)


def _from_hourly_events(events: List[dict], tz_name: str) -> List[dict]:
    """Use pre-computed HOURLY_PNL events from the log."""
    rows = []
    for rec in events:
        ts = _parse_ts(rec.get("ts", ""))
        hour_utc = rec.get("hour_start_utc", "")
        hour_et = rec.get("hour_start_et", "")

        if tz_name == "ET":
            hour_label = hour_et or (ts.astimezone(_ET_TZ).strftime("%H:%M ET")
                                     if ts else "?")
        else:
            hour_label = hour_utc or (ts.strftime("%H:%M UTC") if ts else "?")

        rows.append({
            "hour": hour_label,
            "hour_sort": hour_utc or (ts.isoformat() if ts else ""),
            "realized": rec.get("realized_usd", 0.0),
            "unrealized_start": rec.get("unrealized_start_usd", 0.0),
            "unrealized_end": rec.get("unrealized_end_usd", 0.0),
            "unrealized_delta": (rec.get("unrealized_end_usd", 0.0)
                                 - rec.get("unrealized_start_usd", 0.0)),
            "net_pnl": rec.get("net_pnl_usd", 0.0),
            "buys": rec.get("total_buy_fills", 0),
            "sells": rec.get("total_sell_fills", 0),
            "gross_buy_usd": rec.get("gross_buy_usd", 0.0),
            "gross_sell_usd": rec.get("gross_sell_usd", 0.0),
            "inv_notional": rec.get("inventory_notional_end_usd", 0.0),
        })

    rows.sort(key=lambda r: r["hour_sort"])
    return rows


def _from_fills(records: List[dict], tz_name: str,
                fee_bps: float) -> List[dict]:
    """Reconstruct hourly PnL from individual fill events."""
    inv = InventoryTracker(fee_bps=fee_bps)
    prices_by_hour = _extract_prices_per_hour(records, tz_name)

    # Per-hour accumulators
    hour_data: Dict[str, dict] = {}  # hour_key -> accumulator

    # Collect all fill events
    fill_events = []
    for rec in records:
        event = rec.get("event", "")
        if event not in ("FILL", "DRY_FILL"):
            continue
        ts = _parse_ts(rec.get("ts", ""))
        if ts is None:
            continue
        fill_events.append((ts, rec))

    fill_events.sort(key=lambda x: x[0])

    if not fill_events:
        return []

    # Determine hour boundaries
    first_ts = fill_events[0][0]
    last_ts = fill_events[-1][0]
    first_hour = first_ts.replace(minute=0, second=0, microsecond=0)

    # Process fills chronologically
    prev_hour_key = None
    prev_unrealized = 0.0

    for ts, rec in fill_events:
        hk = _hour_key(ts, tz_name)

        if hk not in hour_data:
            # Initialize new hour
            mark_prices = prices_by_hour.get(hk, {})
            # Use prices from previous hour as start-of-hour marks
            start_unrealized = inv.unrealized_pnl(mark_prices) if mark_prices else prev_unrealized
            hour_data[hk] = {
                "hour": hk,
                "realized": 0.0,
                "unrealized_start": start_unrealized,
                "unrealized_end": 0.0,
                "buys": 0,
                "sells": 0,
                "gross_buy_usd": 0.0,
                "gross_sell_usd": 0.0,
            }

        slug = rec.get("slug", "")
        outcome = rec.get("outcome", "")
        side = rec.get("side", "")
        qty = float(rec.get("qty", 0) or rec.get("qty_shares", 0) or 0)
        price = float(rec.get("price", 0) or 0)

        if qty <= 0 or price <= 0:
            continue

        usd = qty * price

        if side == "BUY":
            inv.apply_buy(slug, outcome, qty, price)
            hour_data[hk]["buys"] += 1
            hour_data[hk]["gross_buy_usd"] += usd

        elif side == "SELL":
            realized = inv.apply_sell(slug, outcome, qty, price)
            hour_data[hk]["realized"] += realized
            hour_data[hk]["sells"] += 1
            hour_data[hk]["gross_sell_usd"] += usd

        prev_hour_key = hk

    # Compute end-of-hour unrealized for each hour
    # We need to replay in order and snapshot unrealized at hour transitions
    # Since we've already processed all fills, we need a second pass
    # to compute unrealized at end of each hour

    # Sort hours chronologically
    sorted_hours = sorted(hour_data.keys())

    # Re-process fills to compute unrealized at each hour boundary
    inv2 = InventoryTracker(fee_bps=fee_bps)
    hour_idx = 0
    current_hour = sorted_hours[0] if sorted_hours else None

    for ts, rec in fill_events:
        hk = _hour_key(ts, tz_name)
        slug = rec.get("slug", "")
        outcome = rec.get("outcome", "")
        side = rec.get("side", "")
        qty = float(rec.get("qty", 0) or rec.get("qty_shares", 0) or 0)
        price = float(rec.get("price", 0) or 0)

        if qty <= 0 or price <= 0:
            continue

        # If we've moved to a new hour, snapshot unrealized for previous hour
        if hk != current_hour and current_hour in hour_data:
            mark_prices = prices_by_hour.get(current_hour, {})
            hour_data[current_hour]["unrealized_end"] = inv2.unrealized_pnl(mark_prices)
            # Set start unrealized for new hour
            new_marks = prices_by_hour.get(hk, mark_prices)
            if hk in hour_data:
                hour_data[hk]["unrealized_start"] = inv2.unrealized_pnl(new_marks)
            current_hour = hk

        if side == "BUY":
            inv2.apply_buy(slug, outcome, qty, price)
        elif side == "SELL":
            inv2.apply_sell(slug, outcome, qty, price)

    # Snapshot last hour
    if current_hour and current_hour in hour_data:
        mark_prices = prices_by_hour.get(current_hour, {})
        hour_data[current_hour]["unrealized_end"] = inv2.unrealized_pnl(mark_prices)

    # Build output rows
    rows = []
    for hk in sorted_hours:
        hd = hour_data[hk]
        u_delta = hd["unrealized_end"] - hd["unrealized_start"]
        net = hd["realized"] + u_delta
        rows.append({
            "hour": hd["hour"],
            "hour_sort": hk,
            "realized": round(hd["realized"], 4),
            "unrealized_start": round(hd["unrealized_start"], 4),
            "unrealized_end": round(hd["unrealized_end"], 4),
            "unrealized_delta": round(u_delta, 4),
            "net_pnl": round(net, 4),
            "buys": hd["buys"],
            "sells": hd["sells"],
            "gross_buy_usd": round(hd["gross_buy_usd"], 4),
            "gross_sell_usd": round(hd["gross_sell_usd"], 4),
            "inv_notional": round(inv2.total_notional(), 4),
        })

    return rows


# ──────────────────────────────────────────────────────────────────────
# Console output
# ──────────────────────────────────────────────────────────────────────
def print_table(rows: List[dict], tz_name: str) -> None:
    """Print formatted hourly PnL table to console."""
    sep = "=" * 78
    print(f"\n{sep}")
    print("  HOURLY PnL REPORT")
    print(f"{sep}")
    print(f"  Timezone: {tz_name}    Hours: {len(rows)}")
    print()

    # Header
    hdr = (f"  {'Hour':>14s} | {'Realized':>10s} | {'Unreal Δ':>10s} | "
           f"{'Net PnL':>10s} | {'Buys':>5s} | {'Sells':>5s}")
    print(hdr)
    print(f"  {'-' * 14}-+-{'-' * 10}-+-{'-' * 10}-+-{'-' * 10}-+-{'-' * 5}-+-{'-' * 5}")

    total_realized = 0.0
    total_unreal_delta = 0.0
    total_net = 0.0
    total_buys = 0
    total_sells = 0

    for row in rows:
        realized = row["realized"]
        u_delta = row["unrealized_delta"]
        net = row["net_pnl"]
        buys = row["buys"]
        sells = row["sells"]

        total_realized += realized
        total_unreal_delta += u_delta
        total_net += net
        total_buys += buys
        total_sells += sells

        # Format with sign
        r_str = f"{realized:+.2f}"
        u_str = f"{u_delta:+.2f}"
        n_str = f"{net:+.2f}"

        hour_label = row["hour"]
        if len(hour_label) > 14:
            hour_label = hour_label[:14]

        print(f"  {hour_label:>14s} | {r_str:>10s} | {u_str:>10s} | "
              f"{n_str:>10s} | {buys:5d} | {sells:5d}")

    # Totals
    print(f"  {'-' * 14}-+-{'-' * 10}-+-{'-' * 10}-+-{'-' * 10}-+-{'-' * 5}-+-{'-' * 5}")
    print(f"  {'TOTAL':>14s} | {total_realized:+10.2f} | {total_unreal_delta:+10.2f} | "
          f"{total_net:+10.2f} | {total_buys:5d} | {total_sells:5d}")
    print(f"\n{sep}\n")


# ──────────────────────────────────────────────────────────────────────
# CSV output
# ──────────────────────────────────────────────────────────────────────
def write_csv(path: str, rows: List[dict]) -> None:
    """Write hourly PnL data to CSV."""
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    fieldnames = [
        "hour", "realized_usd", "unrealized_start_usd",
        "unrealized_end_usd", "unrealized_delta_usd", "net_pnl_usd",
        "buy_fills", "sell_fills", "gross_buy_usd", "gross_sell_usd",
        "inventory_notional_usd",
    ]

    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({
                "hour": row["hour"],
                "realized_usd": row["realized"],
                "unrealized_start_usd": row.get("unrealized_start", 0),
                "unrealized_end_usd": row.get("unrealized_end", 0),
                "unrealized_delta_usd": row["unrealized_delta"],
                "net_pnl_usd": row["net_pnl"],
                "buy_fills": row["buys"],
                "sell_fills": row["sells"],
                "gross_buy_usd": row.get("gross_buy_usd", 0),
                "gross_sell_usd": row.get("gross_sell_usd", 0),
                "inventory_notional_usd": row.get("inv_notional", 0),
            })


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hourly PnL Report – parse bot JSONL logs and compute per-hour PnL.",
    )
    parser.add_argument(
        "logfile",
        help='Path to JSONL log file (or "-" for stdin)',
    )
    parser.add_argument(
        "--tz",
        default="UTC",
        choices=["UTC", "ET"],
        help="Timezone for hour display (default: UTC)",
    )
    parser.add_argument(
        "--fee-bps",
        type=float,
        default=0.0,
        help="Fee in basis points to deduct from PnL (default: 0)",
    )
    parser.add_argument(
        "--csv",
        default="hourly_pnl.csv",
        help="Output CSV path (default: hourly_pnl.csv)",
    )
    args = parser.parse_args()

    # Read log
    records = parse_log(args.logfile)
    if not records:
        print("ERROR: No valid JSON records found in log file.", file=sys.stderr)
        sys.exit(1)

    # Check for SIM_FEE_BPS in config log event
    fee_bps = args.fee_bps
    if fee_bps == 0.0:
        for rec in records:
            if rec.get("event") == "CONFIG":
                cfg_fee = rec.get("SIM_FEE_BPS")
                if cfg_fee is not None and cfg_fee > 0:
                    fee_bps = float(cfg_fee)
                    print(f"  Using SIM_FEE_BPS={fee_bps} from log CONFIG event")
                break

    # Compute
    rows = compute_hourly_pnl(records, tz_name=args.tz, fee_bps=fee_bps)

    if not rows:
        print("No fill events found in log. Cannot compute hourly PnL.",
              file=sys.stderr)
        sys.exit(1)

    # Output
    print_table(rows, args.tz)
    write_csv(args.csv, rows)
    print(f"  CSV written to: {args.csv}")


if __name__ == "__main__":
    main()
