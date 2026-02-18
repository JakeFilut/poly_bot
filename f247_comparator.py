#!/usr/bin/env python3
"""F247 Comparator Report — offline side-by-side analysis.

Reads f247_trades_live_raw.csv (F247 reference trades) and our bot_events JSONL,
then outputs a side-by-side comparison table of key behavioral metrics.

Usage:
    python f247_comparator.py [--f247 f247_trades_live_raw.csv] [--bot bot_events*.jsonl]

If files are not found, prints placeholder metrics with instructions.
"""
import argparse
import csv
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ── Defaults ──
_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.join(os.path.dirname(_DIR), "logs", "poly_bot")
DEFAULT_F247_CSV = os.path.join(_DIR, "f247_trades_live_raw.csv")


def _find_bot_jsonl():
    """Find the most recent bot_events JSONL file."""
    candidates = sorted(Path(_LOG_DIR).glob("bot_events_*.jsonl"), key=os.path.getmtime, reverse=True)
    if candidates:
        return str(candidates[0])
    # Also check local dir
    local = sorted(Path(_DIR).glob("bot_events_*.jsonl"), key=os.path.getmtime, reverse=True)
    return str(local[0]) if local else None


def parse_f247_csv(path: str) -> dict:
    """Parse F247 trades CSV and compute behavioral metrics."""
    if not os.path.exists(path):
        return None

    trades = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trades.append(row)

    if not trades:
        return {"error": "empty CSV"}

    # Detect column names (flexible)
    sample = trades[0]
    cols = list(sample.keys())

    # Try to find timestamp, side, price, size columns
    ts_col = next((c for c in cols if c.lower() in ("timestamp", "ts", "time", "created_at", "createdAt")), None)
    side_col = next((c for c in cols if c.lower() in ("side", "direction", "type", "tradeType")), None)
    price_col = next((c for c in cols if c.lower() in ("price", "fill_price", "avg_price", "avgPrice")), None)
    size_col = next((c for c in cols if c.lower() in ("size", "qty", "amount", "quantity", "usdcSize")), None)
    market_col = next((c for c in cols if c.lower() in ("market", "slug", "market_slug", "conditionId", "condition_id")), None)
    outcome_col = next((c for c in cols if c.lower() in ("outcome", "asset", "asset_id", "assetId")), None)

    buys = []
    sells = []
    all_trades = []

    for t in trades:
        try:
            ts_raw = t.get(ts_col, "") if ts_col else ""
            side = t.get(side_col, "").upper() if side_col else ""
            price = float(t.get(price_col, 0)) if price_col else 0.0
            size_usd = float(t.get(size_col, 0)) if size_col else 0.0
            market = t.get(market_col, "") if market_col else ""
            outcome = t.get(outcome_col, "") if outcome_col else ""

            # Parse timestamp
            ts = 0.0
            if ts_raw:
                for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                            "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                    try:
                        ts = datetime.strptime(ts_raw, fmt).replace(tzinfo=timezone.utc).timestamp()
                        break
                    except ValueError:
                        continue
                if ts == 0.0:
                    try:
                        ts = float(ts_raw) / 1000 if float(ts_raw) > 1e12 else float(ts_raw)
                    except (ValueError, TypeError):
                        pass

            entry = {"ts": ts, "side": side, "price": price, "size_usd": size_usd,
                     "market": market, "outcome": outcome}
            all_trades.append(entry)
            if "BUY" in side:
                buys.append(entry)
            elif "SELL" in side:
                sells.append(entry)
        except (ValueError, TypeError):
            continue

    if not all_trades:
        return {"error": "no parseable trades", "columns": cols}

    # Sort by timestamp
    all_trades.sort(key=lambda x: x["ts"])
    buys.sort(key=lambda x: x["ts"])
    sells.sort(key=lambda x: x["ts"])

    # Time range
    ts_min = all_trades[0]["ts"]
    ts_max = all_trades[-1]["ts"]
    duration_min = max(1, (ts_max - ts_min) / 60.0)

    # Trades/min
    trades_per_min = len(all_trades) / duration_min

    # Avg buy size
    buy_sizes = [t["size_usd"] for t in buys if t["size_usd"] > 0]
    avg_buy_size = statistics.mean(buy_sizes) if buy_sizes else 0.0
    med_buy_size = statistics.median(buy_sizes) if buy_sizes else 0.0

    # Paired straddle detection: buys within 10s on same market, different outcomes
    paired_buys = 0
    for i, b in enumerate(buys):
        for j in range(i + 1, min(i + 50, len(buys))):
            other = buys[j]
            if abs(other["ts"] - b["ts"]) > 10:
                break
            if (other["market"] == b["market"] and other["outcome"] != b["outcome"]
                    and b["outcome"] and other["outcome"]):
                paired_buys += 2
                break
    paired_ratio = paired_buys / max(1, len(buys))

    # FIFO hold time: match sells to buys by market+outcome
    buy_queue = defaultdict(list)  # (market, outcome) -> [buy entries]
    hold_times = []
    exit_improvements = []

    for t in all_trades:
        key = (t["market"], t["outcome"])
        if "BUY" in t["side"]:
            buy_queue[key].append(t)
        elif "SELL" in t["side"] and buy_queue[key]:
            matched_buy = buy_queue[key].pop(0)
            if matched_buy["ts"] > 0 and t["ts"] > 0:
                hold_sec = t["ts"] - matched_buy["ts"]
                if 0 < hold_sec < 86400:
                    hold_times.append(hold_sec)
                    improvement = (t["price"] - matched_buy["price"]) * 100
                    exit_improvements.append(improvement)

    med_hold_sec = statistics.median(hold_times) if hold_times else 0.0
    med_exit_cents = statistics.median(exit_improvements) if exit_improvements else 0.0

    return {
        "source": "f247",
        "total_trades": len(all_trades),
        "buys": len(buys),
        "sells": len(sells),
        "duration_min": round(duration_min, 1),
        "trades_per_min": round(trades_per_min, 2),
        "avg_buy_size_usd": round(avg_buy_size, 2),
        "med_buy_size_usd": round(med_buy_size, 2),
        "median_hold_sec": round(med_hold_sec, 1),
        "median_exit_cents": round(med_exit_cents, 2),
        "paired_straddle_ratio": round(paired_ratio, 3),
        "columns_found": cols,
    }


def parse_bot_jsonl(path: str) -> dict:
    """Parse our bot_events JSONL and compute behavioral metrics."""
    if not path or not os.path.exists(path):
        return None

    fills = []
    all_events = []

    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except json.JSONDecodeError:
                continue
            all_events.append(evt)
            if evt.get("event_type") == "ORDER_FILL":
                fills.append(evt)

    if not fills:
        return {"error": "no ORDER_FILL events", "total_events": len(all_events)}

    buys = [f for f in fills if f.get("side", "").upper() == "BUY"]
    sells = [f for f in fills if f.get("side", "").upper() in ("SELL", "EXIT")]

    # Timestamps
    def get_ts(evt):
        if "ts_ms" in evt:
            return evt["ts_ms"] / 1000
        ts_raw = evt.get("ts", "")
        if isinstance(ts_raw, (int, float)):
            return ts_raw if ts_raw < 1e12 else ts_raw / 1000
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return datetime.strptime(ts_raw, fmt).replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                continue
        return 0.0

    fill_ts = sorted([get_ts(f) for f in fills if get_ts(f) > 0])
    if len(fill_ts) < 2:
        return {"error": "too few fills", "total_fills": len(fills)}

    duration_min = max(1, (fill_ts[-1] - fill_ts[0]) / 60.0)
    trades_per_min = len(fills) / duration_min

    # Buy sizes
    buy_sizes = []
    for b in buys:
        cost = b.get("usdc_cost", 0)
        if cost:
            buy_sizes.append(abs(float(cost)))
        else:
            qty = float(b.get("qty", 0))
            price = float(b.get("fill_price", b.get("price", 0)))
            if qty > 0 and price > 0:
                buy_sizes.append(qty * price)

    avg_buy_size = statistics.mean(buy_sizes) if buy_sizes else 0.0
    med_buy_size = statistics.median(buy_sizes) if buy_sizes else 0.0

    # Hold time from ROUND_TRIP_CLOSE events
    hold_times = []
    exit_cents = []
    for evt in all_events:
        if evt.get("event_type") == "ROUND_TRIP_CLOSE":
            ht = evt.get("hold_sec", 0)
            if ht and 0 < float(ht) < 86400:
                hold_times.append(float(ht))
            pnl = evt.get("pnl_cents", evt.get("net_pnl_usdc", 0))
            if pnl:
                exit_cents.append(float(pnl) * 100 if abs(float(pnl)) < 1 else float(pnl))

    med_hold_sec = statistics.median(hold_times) if hold_times else 0.0
    med_exit_cents = statistics.median(exit_cents) if exit_cents else 0.0

    # Paired straddle ratio from PAIR_COMPLETED
    pair_events = [e for e in all_events if e.get("event_type") == "PAIR_COMPLETED"]
    paired_ratio = len(pair_events) * 2 / max(1, len(buys))

    # Submits
    submits = sum(1 for e in all_events if e.get("event_type") == "ORDER_SUBMIT")
    submits_per_min = submits / duration_min

    return {
        "source": "bot",
        "total_fills": len(fills),
        "buys": len(buys),
        "sells": len(sells),
        "submits": submits,
        "duration_min": round(duration_min, 1),
        "trades_per_min": round(trades_per_min, 2),
        "submits_per_min": round(submits_per_min, 2),
        "avg_buy_size_usd": round(avg_buy_size, 2),
        "med_buy_size_usd": round(med_buy_size, 2),
        "median_hold_sec": round(med_hold_sec, 1),
        "median_exit_cents": round(med_exit_cents, 2),
        "paired_straddle_ratio": round(paired_ratio, 3),
    }


def print_comparison(f247: dict, bot: dict):
    """Print side-by-side comparison table."""
    print("=" * 72)
    print("  F247 COMPARATOR REPORT — Side-by-Side Behavioral Metrics")
    print("=" * 72)
    print()

    rows = [
        ("Trades/min", "trades_per_min", "{:.1f}", "{:.1f}"),
        ("Avg buy size ($)", "avg_buy_size_usd", "${:.2f}", "${:.2f}"),
        ("Med buy size ($)", "med_buy_size_usd", "${:.2f}", "${:.2f}"),
        ("Median hold (sec)", "median_hold_sec", "{:.0f}s", "{:.0f}s"),
        ("Median exit (cents)", "median_exit_cents", "{:+.1f}c", "{:+.1f}c"),
        ("Paired-straddle ratio", "paired_straddle_ratio", "{:.1%}", "{:.1%}"),
    ]

    extra_bot_rows = [
        ("Submits/min", "submits_per_min", "{:.1f}"),
    ]

    print(f"  {'Metric':<25s}  {'F247':>12s}  {'Bot':>12s}  {'Gap':>12s}")
    print(f"  {'-'*25}  {'-'*12}  {'-'*12}  {'-'*12}")

    for label, key, f247_fmt, bot_fmt in rows:
        f247_val = f247.get(key, 0) if f247 else None
        bot_val = bot.get(key, 0) if bot else None

        f247_str = f247_fmt.format(f247_val) if f247_val is not None else "—"
        bot_str = bot_fmt.format(bot_val) if bot_val is not None else "—"

        if f247_val is not None and bot_val is not None and f247_val != 0:
            ratio = bot_val / f247_val
            gap_str = f"{ratio:.1f}x"
        else:
            gap_str = "—"

        print(f"  {label:<25s}  {f247_str:>12s}  {bot_str:>12s}  {gap_str:>12s}")

    if bot:
        print()
        print("  Bot-only metrics:")
        for label, key, fmt in extra_bot_rows:
            val = bot.get(key, 0)
            print(f"    {label:<25s}  {fmt.format(val)}")
        if "submits" in bot and "total_fills" in bot:
            fill_rate = bot["total_fills"] / max(1, bot["submits"])
            print(f"    {'Fill rate':<25s}  {fill_rate:.1%}")

    print()
    print("=" * 72)

    # Actionable summary
    if f247 and bot:
        issues = []
        if bot.get("trades_per_min", 0) > (f247.get("trades_per_min", 999) * 5):
            issues.append(f"CHURN: bot {bot['trades_per_min']:.0f} trades/min vs F247 {f247['trades_per_min']:.0f} ({bot['trades_per_min']/max(1,f247['trades_per_min']):.0f}x)")
        if bot.get("avg_buy_size_usd", 0) < (f247.get("avg_buy_size_usd", 0) * 0.3):
            issues.append(f"SIZE: bot avg ${bot['avg_buy_size_usd']:.2f} vs F247 ${f247['avg_buy_size_usd']:.2f}")
        if bot.get("median_hold_sec", 0) < (f247.get("median_hold_sec", 999) * 0.1):
            issues.append(f"HOLD: bot {bot['median_hold_sec']:.0f}s vs F247 {f247['median_hold_sec']:.0f}s")
        if bot.get("paired_straddle_ratio", 0) > 0.3 and f247.get("paired_straddle_ratio", 0) < 0.15:
            issues.append(f"PARITY: bot paired {bot['paired_straddle_ratio']:.0%} vs F247 {f247['paired_straddle_ratio']:.0%} — F247 is directional")

        if issues:
            print("  ACTIONABLE GAPS:")
            for iss in issues:
                print(f"    - {iss}")
            print()


def main():
    parser = argparse.ArgumentParser(description="F247 Comparator Report")
    parser.add_argument("--f247", default=DEFAULT_F247_CSV, help="Path to F247 trades CSV")
    parser.add_argument("--bot", default=None, help="Path to bot_events JSONL (auto-detect if omitted)")
    args = parser.parse_args()

    bot_path = args.bot or _find_bot_jsonl()

    print(f"  F247 CSV: {args.f247} ({'found' if os.path.exists(args.f247) else 'NOT FOUND'})")
    print(f"  Bot JSONL: {bot_path or 'NOT FOUND'}")
    print()

    f247_data = parse_f247_csv(args.f247)
    bot_data = parse_bot_jsonl(bot_path)

    if f247_data and "error" in f247_data:
        print(f"  F247 parse error: {f247_data['error']}")
        if "columns" in f247_data:
            print(f"  Columns found: {f247_data['columns']}")
        f247_data = None

    if bot_data and "error" in bot_data:
        print(f"  Bot parse error: {bot_data['error']}")
        bot_data = None

    if not f247_data and not bot_data:
        print("  No data files found. Place f247_trades_live_raw.csv in this directory")
        print("  and/or run the bot to generate bot_events JSONL files.")
        print()
        print("  Expected F247 CSV columns (any of):")
        print("    timestamp/ts, side/direction, price/fill_price, size/qty, market/slug, outcome/asset")
        sys.exit(1)

    print_comparison(f247_data, bot_data)


if __name__ == "__main__":
    main()
