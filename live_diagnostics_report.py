#!/usr/bin/env python3
"""
live_diagnostics_report.py – Offline analysis of bot_log.jsonl.

Reads structured JSON log lines and outputs:
  - Passive vs cross fill rates
  - Avg time-to-fill
  - Cancel/replace rates
  - Profit per sell (overall + passive-only + cross-only)
  - Realized PnL by asset
  - Profit per sell by volatility bucket
  - Hourly realized mean/std and Sharpe-like ratio

Usage:
    python live_diagnostics_report.py [path/to/bot_log.jsonl]

No external dependencies (stdlib only).
"""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from datetime import datetime


def load_events(path: str) -> list[dict]:
    events = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def parse_ts(ts_str: str) -> float:
    """Parse ISO-8601 timestamp to epoch seconds."""
    try:
        # Handle "2026-03-02T15:30:01.123Z" format
        ts_str = ts_str.rstrip("Z")
        if "." in ts_str:
            dt = datetime.fromisoformat(ts_str)
        else:
            dt = datetime.fromisoformat(ts_str)
        return dt.timestamp()
    except Exception:
        return 0.0


def extract_slug_asset(slug: str) -> str:
    """Extract asset name from slug like 'will-btc-go-up-...' """
    s = slug.lower()
    for asset in ("btc", "eth", "sol", "xrp"):
        if asset in s:
            return asset.upper()
    return "OTHER"


def analyze(events: list[dict]) -> None:
    # Collect events by type
    intents = [e for e in events if e.get("event") == "ORDER_INTENT"]
    fills = [e for e in events if e.get("event") == "FILL"]
    cancels = [e for e in events if e.get("event") == "ORDER_CANCELED"]
    placed = [e for e in events if e.get("event") == "ORDER_PLACED"]
    minute_rollups = [e for e in events if e.get("event") == "MINUTE_ROLLUP"]
    hourly_pnls = [e for e in events if e.get("event") == "HOURLY_PNL"]

    # Also count legacy fill events
    legacy_fills = [e for e in events if e.get("event") in ("DRY_FILL",)]

    total_fills = fills + legacy_fills

    print("=" * 70)
    print("  LIVE DIAGNOSTICS REPORT")
    print("=" * 70)

    # Time range
    if events:
        first_ts = events[0].get("ts", "")
        last_ts = events[-1].get("ts", "")
        print(f"\n  Time range: {first_ts} → {last_ts}")

    print(f"\n  Events: {len(events)} total")
    print(f"    ORDER_INTENT:   {len(intents)}")
    print(f"    ORDER_PLACED:   {len(placed)}")
    print(f"    ORDER_CANCELED: {len(cancels)}")
    print(f"    FILL:           {len(fills)}")
    if legacy_fills:
        print(f"    DRY_FILL:       {len(legacy_fills)}")
    print(f"    MINUTE_ROLLUP:  {len(minute_rollups)}")
    print(f"    HOURLY_PNL:     {len(hourly_pnls)}")

    # ---------------------------------------------------------------
    # 1. Passive vs Cross fill rates
    # ---------------------------------------------------------------
    print("\n" + "-" * 70)
    print("  1. PASSIVE vs CROSS FILL RATES")
    print("-" * 70)

    passive_fills = [f for f in fills if f.get("entry_style") == "PASSIVE"]
    cross_fills = [f for f in fills if f.get("entry_style") == "CROSS"]
    unknown_fills = [f for f in fills if f.get("entry_style") not in ("PASSIVE", "CROSS")]

    passive_intents = [i for i in intents if i.get("entry_style") == "PASSIVE"]
    cross_intents = [i for i in intents if i.get("entry_style") == "CROSS"]

    print(f"\n  Intents:  {len(passive_intents)} passive, {len(cross_intents)} cross")
    print(f"  Fills:    {len(passive_fills)} passive, {len(cross_fills)} cross, {len(unknown_fills)} unknown")

    if len(passive_intents) > 0:
        print(f"  Passive fill rate: {len(passive_fills)/len(passive_intents)*100:.1f}%")
    if len(cross_intents) > 0:
        print(f"  Cross fill rate:   {len(cross_fills)/len(cross_intents)*100:.1f}%")

    # ---------------------------------------------------------------
    # 2. Avg time-to-fill
    # ---------------------------------------------------------------
    print("\n" + "-" * 70)
    print("  2. TIME-TO-FILL")
    print("-" * 70)

    ttfs = [f.get("time_to_fill_ms", 0) for f in fills if f.get("time_to_fill_ms", 0) > 0]
    passive_ttfs = [f.get("time_to_fill_ms", 0) for f in passive_fills if f.get("time_to_fill_ms", 0) > 0]
    cross_ttfs = [f.get("time_to_fill_ms", 0) for f in cross_fills if f.get("time_to_fill_ms", 0) > 0]

    if ttfs:
        print(f"\n  All fills:     avg={_mean(ttfs):.0f}ms  median={_median(ttfs):.0f}ms  n={len(ttfs)}")
    if passive_ttfs:
        print(f"  Passive fills: avg={_mean(passive_ttfs):.0f}ms  median={_median(passive_ttfs):.0f}ms  n={len(passive_ttfs)}")
    if cross_ttfs:
        print(f"  Cross fills:   avg={_mean(cross_ttfs):.0f}ms  median={_median(cross_ttfs):.0f}ms  n={len(cross_ttfs)}")

    if not ttfs:
        print("\n  No time-to-fill data available.")

    # ---------------------------------------------------------------
    # 3. Cancel/replace rates
    # ---------------------------------------------------------------
    print("\n" + "-" * 70)
    print("  3. CANCEL / REPLACE RATES")
    print("-" * 70)

    ttl_cancels = [c for c in cancels if c.get("cancel_reason") == "ttl_expired"]
    replace_cancels = [c for c in cancels if c.get("cancel_reason") == "replace"]
    shutdown_cancels = [c for c in cancels if c.get("cancel_reason") == "shutdown"]
    other_cancels = [c for c in cancels if c.get("cancel_reason") not in ("ttl_expired", "replace", "shutdown")]

    total_placed = len(placed) or len(intents) or 1  # avoid div/0

    print(f"\n  Total placed:    {len(placed)}")
    print(f"  TTL expired:     {len(ttl_cancels)}  ({len(ttl_cancels)/total_placed*100:.1f}%)")
    print(f"  Replace:         {len(replace_cancels)}  ({len(replace_cancels)/total_placed*100:.1f}%)")
    print(f"  Shutdown:        {len(shutdown_cancels)}")
    if other_cancels:
        print(f"  Other:           {len(other_cancels)}")

    cancel_alive_ms = [c.get("time_alive_ms", 0) for c in cancels if c.get("time_alive_ms", 0) > 0]
    if cancel_alive_ms:
        print(f"  Avg time alive before cancel: {_mean(cancel_alive_ms):.0f}ms")

    # ---------------------------------------------------------------
    # 4. Profit per sell
    # ---------------------------------------------------------------
    print("\n" + "-" * 70)
    print("  4. PROFIT PER SELL")
    print("-" * 70)

    sell_fills = [f for f in fills if f.get("side") == "SELL"]
    sell_pnls = [f.get("realized_pnl", 0) for f in sell_fills if "realized_pnl" in f]
    passive_sell_pnls = [f.get("realized_pnl", 0) for f in sell_fills
                         if f.get("entry_style") == "PASSIVE" and "realized_pnl" in f]
    cross_sell_pnls = [f.get("realized_pnl", 0) for f in sell_fills
                       if f.get("entry_style") == "CROSS" and "realized_pnl" in f]

    if sell_pnls:
        print(f"\n  Overall:      n={len(sell_pnls)}  total=${sum(sell_pnls):.4f}  avg=${_mean(sell_pnls):.4f}")
    if passive_sell_pnls:
        print(f"  Passive-only: n={len(passive_sell_pnls)}  total=${sum(passive_sell_pnls):.4f}  avg=${_mean(passive_sell_pnls):.4f}")
    if cross_sell_pnls:
        print(f"  Cross-only:   n={len(cross_sell_pnls)}  total=${sum(cross_sell_pnls):.4f}  avg=${_mean(cross_sell_pnls):.4f}")

    if not sell_pnls:
        print("\n  No sell fill PnL data available.")

    # ---------------------------------------------------------------
    # 5. Realized PnL by asset
    # ---------------------------------------------------------------
    print("\n" + "-" * 70)
    print("  5. REALIZED PnL BY ASSET")
    print("-" * 70)

    pnl_by_asset: dict[str, list[float]] = defaultdict(list)
    for f in sell_fills:
        if "realized_pnl" not in f:
            continue
        slug = f.get("slug", "")
        asset = extract_slug_asset(slug)
        pnl_by_asset[asset].append(f["realized_pnl"])

    if pnl_by_asset:
        print(f"\n  {'Asset':<8} {'Sells':>6} {'Total PnL':>12} {'Avg PnL':>10}")
        print(f"  {'-'*8} {'-'*6} {'-'*12} {'-'*10}")
        for asset in sorted(pnl_by_asset.keys()):
            pnls = pnl_by_asset[asset]
            print(f"  {asset:<8} {len(pnls):>6} ${sum(pnls):>11.4f} ${_mean(pnls):>9.4f}")
    else:
        print("\n  No per-asset PnL data available.")

    # ---------------------------------------------------------------
    # 6. Profit per sell by volatility bucket
    # ---------------------------------------------------------------
    print("\n" + "-" * 70)
    print("  6. PROFIT PER SELL BY VOLATILITY BUCKET")
    print("-" * 70)

    buckets: dict[str, list[float]] = {
        "<0.0005": [],
        "0.0005-0.001": [],
        ">0.001": [],
    }

    for f in sell_fills:
        if "realized_pnl" not in f or "bin_ret_30s" not in f:
            continue
        ret = abs(f["bin_ret_30s"])
        pnl = f["realized_pnl"]
        if ret < 0.0005:
            buckets["<0.0005"].append(pnl)
        elif ret <= 0.001:
            buckets["0.0005-0.001"].append(pnl)
        else:
            buckets[">0.001"].append(pnl)

    has_vol_data = any(len(v) > 0 for v in buckets.values())
    if has_vol_data:
        print(f"\n  {'Vol Bucket':<16} {'Sells':>6} {'Total PnL':>12} {'Avg PnL':>10}")
        print(f"  {'-'*16} {'-'*6} {'-'*12} {'-'*10}")
        for bucket_name in ["<0.0005", "0.0005-0.001", ">0.001"]:
            pnls = buckets[bucket_name]
            if pnls:
                print(f"  {bucket_name:<16} {len(pnls):>6} ${sum(pnls):>11.4f} ${_mean(pnls):>9.4f}")
            else:
                print(f"  {bucket_name:<16} {0:>6} {'—':>12} {'—':>10}")
    else:
        print("\n  No volatility-bucketed PnL data (need bin_ret_30s in FILL events).")

    # ---------------------------------------------------------------
    # 7. Hourly realized mean/std and Sharpe-like
    # ---------------------------------------------------------------
    print("\n" + "-" * 70)
    print("  7. HOURLY PnL STATISTICS")
    print("-" * 70)

    hourly_realized = [h.get("realized_usd", 0) for h in hourly_pnls]
    hourly_net = [h.get("net_pnl_usd", 0) for h in hourly_pnls]
    hourly_drawdowns = [h.get("drawdown_in_hour", 0) for h in hourly_pnls if "drawdown_in_hour" in h]

    if hourly_realized and len(hourly_realized) >= 2:
        mean_r = _mean(hourly_realized)
        std_r = _std(hourly_realized)
        sharpe = mean_r / std_r if std_r > 0 else 0.0
        print(f"\n  Hours: {len(hourly_realized)}")
        print(f"  Realized PnL/hr:  mean=${mean_r:.4f}  std=${std_r:.4f}")
        print(f"  Sharpe-like (hourly): {sharpe:.3f}")
        print(f"  Total realized:   ${sum(hourly_realized):.4f}")
        if hourly_net:
            mean_n = _mean(hourly_net)
            std_n = _std(hourly_net)
            net_sharpe = mean_n / std_n if std_n > 0 else 0.0
            print(f"  Net PnL/hr:       mean=${mean_n:.4f}  std=${std_n:.4f}")
            print(f"  Net Sharpe-like:  {net_sharpe:.3f}")
        if hourly_drawdowns:
            print(f"  Avg hourly drawdown: ${_mean(hourly_drawdowns):.4f}")
            print(f"  Max hourly drawdown: ${max(hourly_drawdowns):.4f}")
    elif hourly_realized:
        print(f"\n  Only 1 hour of data. Realized: ${hourly_realized[0]:.4f}")
    else:
        print("\n  No HOURLY_PNL data available.")

    # ---------------------------------------------------------------
    # 8. Edge vs Mid (spread capture)
    # ---------------------------------------------------------------
    print("\n" + "-" * 70)
    print("  8. SPREAD CAPTURE (EDGE vs MID)")
    print("-" * 70)

    buy_edges = [f.get("edge_vs_mid", 0) for f in fills
                 if f.get("side") == "BUY" and "edge_vs_mid" in f]
    sell_edges = [f.get("edge_vs_mid", 0) for f in fills
                  if f.get("side") == "SELL" and "edge_vs_mid" in f]

    if buy_edges:
        print(f"\n  BUY edge vs mid:  avg={_mean(buy_edges):.4f}  n={len(buy_edges)}")
    if sell_edges:
        print(f"  SELL edge vs mid: avg={_mean(sell_edges):.4f}  n={len(sell_edges)}")
    if not buy_edges and not sell_edges:
        print("\n  No edge_vs_mid data available.")

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    print("\n" + "=" * 70)
    total_pnl = sum(sell_pnls) if sell_pnls else 0.0
    total_buy_fills = len([f for f in fills if f.get("side") == "BUY"])
    total_sell_fills = len(sell_fills)
    print(f"  SUMMARY: {total_buy_fills} buy fills, {total_sell_fills} sell fills, "
          f"realized PnL = ${total_pnl:.4f}")
    print("=" * 70)


# ---------------------------------------------------------------------------
# Stats helpers (stdlib only)
# ---------------------------------------------------------------------------
def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _median(xs: list[float]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    n = len(s)
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2


def _std(xs: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    var = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(var)


def main():
    if len(sys.argv) < 2:
        path = "bot_log.jsonl"
    else:
        path = sys.argv[1]

    try:
        events = load_events(path)
    except FileNotFoundError:
        print(f"Error: file not found: {path}")
        sys.exit(1)

    if not events:
        print(f"No events found in {path}")
        sys.exit(1)

    analyze(events)


if __name__ == "__main__":
    main()
