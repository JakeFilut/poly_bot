#!/usr/bin/env python3
"""
Independent strategy backtest — convergence + dip-buying on Polymarket binary options.

Strategy:
  These are hourly "Up or Down" binary markets on BTC/ETH/SOL/XRP.
  They resolve to $1 or $0 at a specific hour.

  BUY signal (on "Up" token):
    - mid > threshold  (market trending toward $1 → "Up" is likely)
    - price dipped in last N seconds (buy the dip)
    - orderbook imbalance surging (buyers stepping in)
    → Buy at bestBid (passive limit order), sell 30-60s later at mid

  SELL signal (on "Up" token — i.e. buy the "Down" token):
    - mid < threshold  (market trending toward $0 → "Down" is likely)
    - price rose in last N seconds (sell the rip)
    - orderbook imbalance dropping (sellers stepping in)
    → Sell at bestAsk, buy back 30-60s later at mid

  Also supports: spread-widening filter, Binance confirmation, max imbalance cap.

Usage:
    python independent_backtest.py [--data-dir DIR] [--sweep]
"""

import argparse
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from itertools import product
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Strategy parameters
# ---------------------------------------------------------------------------

@dataclass
class ConvergenceParams:
    """Tunable parameters for the convergence/dip-buy strategy."""
    # --- Entry thresholds ---
    mid_buy_thresh: float = 0.65       # buy Up when mid > this
    mid_sell_thresh: float = 0.35      # sell Up (buy Down) when mid < this
    # --- Dip detection ---
    lookback_sec: int = 5              # seconds to look back for dip/rip
    min_dip_cents: float = 0.5         # minimum dip in cents to trigger (0 = disabled)
    # --- Imbalance surge ---
    min_imb_change: float = 0.03       # imbalance must have risen by this (buy) / fallen (sell)
    # --- Spread filter ---
    require_spread_widening: bool = False  # require spread to be widening
    # --- Execution ---
    entry_mode: str = "passive"        # "passive" (buy at bid) or "cross" (buy at ask)
    hold_sec: int = 30                 # seconds to hold position
    position_size: float = 10.0        # shares per trade
    # --- Cooldown ---
    cooldown_sec: float = 30.0         # min seconds between trades on same market
    # --- Fee ---
    fee_pct: float = 0.0              # one-way fee as decimal (0.005 = 0.5%)


# ---------------------------------------------------------------------------
# Data loading (reuse MarketReplayEngine from replay_engine.py)
# ---------------------------------------------------------------------------

def load_data(data_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load book tape and binance tape, return as DataFrames."""
    data_path = Path(data_dir)
    book = pd.read_csv(data_path / "f247_copywallet_book_tape.csv")
    binance = pd.read_csv(data_path / "f247_copywallet_binance_tape.csv")
    return book, binance


def find_data_dir() -> str:
    candidates = ["logs/poly_bot", "/home/ubuntu/github/logs/poly_bot", "."]
    for d in candidates:
        if (Path(d) / "f247_copywallet_book_tape.csv").exists():
            return d
    return candidates[0]


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def build_features(book: pd.DataFrame, binance: pd.DataFrame) -> pd.DataFrame:
    """Enrich book tape with lagged features needed for strategy signals."""

    book = book.sort_values(["slug", "outcome", "timestamp_epoch"]).copy()

    # Mid price
    if "mid" not in book.columns:
        book["mid"] = (book["bestBid"] + book["bestAsk"]) / 2
    book["spread"] = book["bestAsk"] - book["bestBid"]

    # Lagged mid / imbalance / spread (per market-outcome)
    grp = book.groupby(["slug", "outcome"])
    for lag in [5, 10]:
        book[f"mid_lag_{lag}s"] = grp["mid"].shift(lag)
        book[f"mid_chg_{lag}s"] = book["mid"] - book[f"mid_lag_{lag}s"]

    book["imb_lag_5s"] = grp["imbalance_topN"].shift(5)
    book["imb_chg_5s"] = book["imbalance_topN"] - book["imb_lag_5s"]

    book["spread_lag_5s"] = grp["spread"].shift(5)
    book["spread_chg_5s"] = book["spread"] - book["spread_lag_5s"]

    # Forward mid (for PnL calculation)
    for fwd in [10, 30, 60, 90, 120]:
        book[f"mid_fwd_{fwd}s"] = grp["mid"].shift(-fwd)

    # Extract asset from slug
    book["asset"] = book["slug"].str.extract(r"(bitcoin|ethereum|solana|xrp)", expand=False)
    asset_map = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL", "xrp": "XRP"}
    book["symbol"] = book["asset"].map(asset_map)

    # Hour in ET
    ET = timedelta(hours=-4)
    book["dt_et"] = pd.to_datetime(book["timestamp_epoch"], unit="s", utc=True) + ET
    book["hour_et"] = book["dt_et"].dt.hour

    # Merge Binance 5s return for optional confirmation
    binance = binance.sort_values(["symbol", "timestamp_epoch"]).copy()
    for sym in binance["symbol"].unique():
        mask = binance["symbol"] == sym
        binance.loc[mask, "bret_5s"] = binance.loc[mask, "price"].pct_change(5)

    bret = binance[["timestamp_epoch", "symbol", "bret_5s"]].dropna()
    book = book.merge(bret, left_on=["timestamp_epoch", "symbol"],
                      right_on=["timestamp_epoch", "symbol"], how="left")

    return book


# ---------------------------------------------------------------------------
# Strategy simulation
# ---------------------------------------------------------------------------

@dataclass
class Trade:
    """A single simulated trade."""
    timestamp: float
    slug: str
    outcome: str
    asset: str
    side: str          # "BUY_UP" or "SELL_UP" (sell Up = buy Down)
    entry_price: float
    exit_price: float
    size: float
    pnl_gross: float
    pnl_net: float     # after fees
    hour_et: int
    signal: str        # description of why we entered


def run_backtest(df: pd.DataFrame, params: ConvergenceParams) -> List[Trade]:
    """Run the convergence strategy on enriched book data (vectorized)."""

    mid_chg_col = f"mid_chg_{params.lookback_sec}s"
    fwd_col = f"mid_fwd_{params.hold_sec}s"

    # Only rows with all needed columns
    required = ["mid", mid_chg_col, "imb_chg_5s", fwd_col, "bestBid", "bestAsk"]
    valid = df[df[required].notna().all(axis=1)].copy()

    dip_thresh = -params.min_dip_cents / 100  # negative for dips
    rip_thresh = params.min_dip_cents / 100   # positive for rips

    # --- Vectorized signal detection ---
    buy_mask = (
        (valid["outcome"] == "Up")
        & (valid["mid"] > params.mid_buy_thresh)
        & (valid[mid_chg_col] < dip_thresh)
        & (valid["imb_chg_5s"] > params.min_imb_change)
    )
    sell_mask = (
        (valid["outcome"] == "Up")
        & (valid["mid"] < params.mid_sell_thresh)
        & (valid[mid_chg_col] > rip_thresh)
        & (valid["imb_chg_5s"] < -params.min_imb_change)
    )

    if params.require_spread_widening:
        spread_ok = valid["spread_chg_5s"] > 0
        buy_mask = buy_mask & spread_ok
        sell_mask = sell_mask & spread_ok

    # Combine signals
    signals = valid[buy_mask | sell_mask].copy()
    signals["side"] = np.where(buy_mask[signals.index], "BUY_UP", "SELL_UP")

    if signals.empty:
        return []

    # --- Apply cooldown (sequential, but only on small signal set) ---
    signals = signals.sort_values("timestamp_epoch")
    last_ts: Dict[str, float] = {}
    keep_idx = []
    for idx, row in signals.iterrows():
        key = f"{row['slug']}|{row['outcome']}"
        ts = row["timestamp_epoch"]
        if key not in last_ts or ts - last_ts[key] >= params.cooldown_sec:
            keep_idx.append(idx)
            last_ts[key] = ts

    signals = signals.loc[keep_idx]

    if signals.empty:
        return []

    # --- Vectorized PnL calculation ---
    is_buy = signals["side"] == "BUY_UP"
    if params.entry_mode == "passive":
        entry = np.where(is_buy, signals["bestBid"], signals["bestAsk"])
    else:
        entry = np.where(is_buy, signals["bestAsk"], signals["bestBid"])

    exit_price = signals[fwd_col].values
    size = params.position_size
    pnl_gross = np.where(is_buy, (exit_price - entry) * size, (entry - exit_price) * size)
    fee = (entry + exit_price) * size * params.fee_pct
    pnl_net = pnl_gross - fee

    # --- Build trade objects ---
    trades = []
    for i, (idx, row) in enumerate(signals.iterrows()):
        mid_chg = row[mid_chg_col]
        trades.append(Trade(
            timestamp=row["timestamp_epoch"], slug=row["slug"],
            outcome=row["outcome"], asset=row.get("symbol", ""),
            side=row["side"], entry_price=entry[i], exit_price=exit_price[i],
            size=size, pnl_gross=pnl_gross[i], pnl_net=pnl_net[i],
            hour_et=int(row.get("hour_et", 0)),
            signal=f"mid={row['mid']:.3f} chg={mid_chg*100:.1f}c imb_chg={row['imb_chg_5s']:.3f}"
        ))

    return trades


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_results(trades: List[Trade], params: ConvergenceParams, label: str = "") -> Dict:
    """Print PnL summary and per-hour breakdown. Returns summary dict."""

    if not trades:
        print(f"\n  {'[' + label + '] ' if label else ''}No trades generated.\n")
        return {}

    tdf = pd.DataFrame([t.__dict__ for t in trades])

    total_pnl = tdf["pnl_net"].sum()
    total_gross = tdf["pnl_gross"].sum()
    n_trades = len(tdf)
    win_rate = (tdf["pnl_net"] > 0).mean()
    avg_pnl = tdf["pnl_net"].mean()
    winners = tdf[tdf["pnl_net"] > 0]["pnl_net"]
    losers = tdf[tdf["pnl_net"] < 0]["pnl_net"]

    ts_min = tdf["timestamp"].min()
    ts_max = tdf["timestamp"].max()
    duration_hours = max((ts_max - ts_min) / 3600, 0.01)

    print()
    print("=" * 72)
    if label:
        print(f"  INDEPENDENT STRATEGY BACKTEST — {label}")
    else:
        print("  INDEPENDENT STRATEGY BACKTEST RESULTS")
    print("=" * 72)
    print(f"  Strategy:  Convergence dip-buy / rip-sell")
    print(f"  Entry:     {params.entry_mode}  |  Hold: {params.hold_sec}s  |  Size: {params.position_size} shares")
    print(f"  Fee:       {params.fee_pct * 100:.2f}% per side")
    print(f"  Cooldown:  {params.cooldown_sec:.0f}s per market")
    print(f"  Filters:   mid_buy>{params.mid_buy_thresh}, mid_sell<{params.mid_sell_thresh}, "
          f"dip>={params.min_dip_cents}c, imb_chg>={params.min_imb_change}")
    print("─" * 72)
    print(f"  Total trades:    {n_trades}")
    print(f"  BUY_UP:          {(tdf['side'] == 'BUY_UP').sum()}")
    print(f"  SELL_UP:         {(tdf['side'] == 'SELL_UP').sum()}")
    print(f"  Duration:        {duration_hours:.1f} hours")
    print(f"  Trades/hour:     {n_trades / duration_hours:.1f}")
    print("─" * 72)
    print(f"  Gross PnL:       ${total_gross:>10.2f}")
    print(f"  Fees:            ${total_gross - total_pnl:>10.2f}")
    print(f"  Net PnL:         ${total_pnl:>10.2f}")
    print(f"  PnL / hour:      ${total_pnl / duration_hours:>10.2f}")
    print(f"  Avg PnL/trade:   ${avg_pnl:>10.4f}")
    print(f"  Win rate:        {win_rate:>10.1%}")
    if len(winners) > 0:
        print(f"  Avg winner:      ${winners.mean():>10.4f}")
    if len(losers) > 0:
        print(f"  Avg loser:       ${losers.mean():>10.4f}")

    # --- Per-hour breakdown ---
    print()
    print("─" * 72)
    print("  PnL BY HOUR (ET)")
    print("─" * 72)
    print(f"  {'Hour':>6}  {'Trades':>7}  {'Gross':>10}  {'Net':>10}  {'Win%':>7}  {'Avg PnL':>10}")
    print("  " + "─" * 60)

    hourly = tdf.groupby("hour_et").agg(
        trades=("pnl_net", "count"),
        gross=("pnl_gross", "sum"),
        net=("pnl_net", "sum"),
        win_rate=("pnl_net", lambda x: (x > 0).mean()),
        avg_pnl=("pnl_net", "mean"),
    )
    for hour in sorted(hourly.index):
        r = hourly.loc[hour]
        print(f"  {hour:>6}  {int(r['trades']):>7}  ${r['gross']:>9.2f}  ${r['net']:>9.2f}  "
              f"{r['win_rate']:>6.1%}  ${r['avg_pnl']:>9.4f}")

    print("  " + "─" * 60)
    print(f"  {'TOTAL':>6}  {n_trades:>7}  ${total_gross:>9.2f}  ${total_pnl:>9.2f}  "
          f"{win_rate:>6.1%}  ${avg_pnl:>9.4f}")

    # --- Per-asset breakdown ---
    print()
    print("─" * 72)
    print("  PnL BY ASSET")
    print("─" * 72)
    print(f"  {'Asset':>6}  {'Trades':>7}  {'Net PnL':>10}  {'Win%':>7}  {'Avg PnL':>10}")
    print("  " + "─" * 50)

    by_asset = tdf.groupby("asset").agg(
        trades=("pnl_net", "count"),
        net=("pnl_net", "sum"),
        win_rate=("pnl_net", lambda x: (x > 0).mean()),
        avg_pnl=("pnl_net", "mean"),
    )
    for asset in sorted(by_asset.index):
        r = by_asset.loc[asset]
        print(f"  {asset:>6}  {int(r['trades']):>7}  ${r['net']:>9.2f}  "
              f"{r['win_rate']:>6.1%}  ${r['avg_pnl']:>9.4f}")

    # --- Sample trades ---
    print()
    print("─" * 72)
    print("  SAMPLE TRADES (first 10)")
    print("─" * 72)
    ET = timedelta(hours=-4)
    for _, t in tdf.head(10).iterrows():
        ts_et = datetime.fromtimestamp(t["timestamp"], tz=timezone.utc) + ET
        print(f"  {ts_et.strftime('%H:%M:%S')} {t['asset']:>4} {t['side']:<8} "
              f"entry={t['entry_price']:.3f} exit={t['exit_price']:.3f} "
              f"pnl=${t['pnl_net']:.4f}  [{t['signal']}]")

    print("=" * 72)

    return {
        "n_trades": n_trades, "total_pnl": total_pnl, "pnl_per_hour": total_pnl / duration_hours,
        "win_rate": win_rate, "avg_pnl": avg_pnl,
    }


# ---------------------------------------------------------------------------
# Parameter sweep
# ---------------------------------------------------------------------------

SWEEP_RANGES = {
    "mid_buy_thresh":  [0.55, 0.60, 0.62, 0.65, 0.68, 0.70, 0.75],
    "mid_sell_thresh": [0.40, 0.35, 0.32, 0.30, 0.25],
    "min_dip_cents":   [0.0],
    "min_imb_change":  [0.0],
    "hold_sec":        [30, 60, 90, 120],
    "cooldown_sec":    [5, 10, 15],
    "entry_mode":      ["passive"],
    "lookback_sec":    [5, 10],
    "fee_pct":         [0.0],       # run at 0 fee; add 0.005 separately to see impact
}


def run_sweep(df: pd.DataFrame) -> pd.DataFrame:
    """Grid search over strategy parameters."""
    keys = list(SWEEP_RANGES.keys())
    values = list(SWEEP_RANGES.values())
    combos = list(product(*values))
    print(f"\n  Parameter sweep: {len(combos)} combinations")

    results = []
    for i, combo in enumerate(combos):
        kv = dict(zip(keys, combo))

        # hold_sec must match a forward column we computed
        hold = kv["hold_sec"]
        fwd_col = f"mid_fwd_{hold}s"
        if fwd_col not in df.columns:
            continue

        # mid_sell_thresh must be < mid_buy_thresh
        if kv["mid_sell_thresh"] >= kv["mid_buy_thresh"]:
            continue

        p = ConvergenceParams(**kv)
        trades = run_backtest(df, p)

        if trades:
            tdf = pd.DataFrame([t.__dict__ for t in trades])
            total_pnl = tdf["pnl_net"].sum()
            duration_h = max((tdf["timestamp"].max() - tdf["timestamp"].min()) / 3600, 0.01)
            win_rate = (tdf["pnl_net"] > 0).mean()
        else:
            total_pnl = 0.0
            duration_h = 1.0
            win_rate = 0.0

        results.append({
            **kv,
            "n_trades": len(trades),
            "total_pnl": total_pnl,
            "pnl_per_hour": total_pnl / duration_h,
            "win_rate": win_rate,
        })

        if (i + 1) % 50 == 0:
            print(f"    ... {i + 1}/{len(combos)} done")

    rdf = pd.DataFrame(results).sort_values("pnl_per_hour", ascending=False)

    print("\n" + "=" * 72)
    print("  TOP 15 PARAMETER SETS (by PnL/hour)")
    print("=" * 72)
    display_cols = keys + ["n_trades", "total_pnl", "pnl_per_hour", "win_rate"]
    print(rdf[display_cols].head(15).to_string(index=False))

    print("\n  WORST 5:")
    print(rdf[display_cols].tail(5).to_string(index=False))

    return rdf


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Independent Convergence Strategy Backtest")
    parser.add_argument("--data-dir", type=str, default=None)
    parser.add_argument("--sweep", action="store_true", help="Run parameter sweep")
    parser.add_argument("--fee", type=float, default=None, help="Override fee pct (e.g. 0.005 for 0.5%%)")
    parser.add_argument("--entry-mode", choices=["passive", "cross"], default="passive")
    parser.add_argument("--hold", type=int, default=30, choices=[10, 30, 60, 90, 120])
    parser.add_argument("--cooldown", type=float, default=30.0)
    args = parser.parse_args()

    data_dir = args.data_dir or find_data_dir()
    print(f"\n  Data directory: {data_dir}")

    print("\n[1] Loading data ...")
    book, binance = load_data(data_dir)
    print(f"  book_tape:    {len(book):>8,} rows")
    print(f"  binance_tape: {len(binance):>8,} rows")

    print("\n[2] Building features ...")
    df = build_features(book, binance)
    print(f"  Enriched rows: {len(df):,}")

    if args.sweep:
        print("\n[3] Running parameter sweep ...")
        rdf = run_sweep(df)

        # Run the best config with full report
        best = rdf.iloc[0]
        print(f"\n[4] Detailed report for best config ...")
        best_kv = {k: best[k] for k in SWEEP_RANGES.keys()}
        best_kv["hold_sec"] = int(best_kv["hold_sec"])
        if "lookback_sec" in best_kv:
            best_kv["lookback_sec"] = int(best_kv["lookback_sec"])
        best_params = ConvergenceParams(**best_kv)
        trades = run_backtest(df, best_params)
        print_results(trades, best_params, label="BEST SWEEP")

        # Also show with 0.5% fee
        print(f"\n[5] Same config with 0.5% fee ...")
        best_params.fee_pct = 0.005
        trades = run_backtest(df, best_params)
        print_results(trades, best_params, label="BEST SWEEP + 0.5% FEE")
    else:
        print("\n[3] Running backtest ...")
        params = ConvergenceParams(
            entry_mode=args.entry_mode,
            hold_sec=args.hold,
            cooldown_sec=args.cooldown,
        )
        if args.fee is not None:
            params.fee_pct = args.fee

        trades = run_backtest(df, params)
        print_results(trades, params, label="BASELINE")

        # Also run with 0.5% fee to show impact
        if params.fee_pct == 0.0:
            print("\n  --- Re-running with 0.5% fee for comparison ---")
            params_fee = ConvergenceParams(
                mid_buy_thresh=params.mid_buy_thresh,
                mid_sell_thresh=params.mid_sell_thresh,
                min_dip_cents=params.min_dip_cents,
                min_imb_change=params.min_imb_change,
                hold_sec=params.hold_sec,
                cooldown_sec=params.cooldown_sec,
                entry_mode=params.entry_mode,
                fee_pct=0.005,
                position_size=params.position_size,
            )
            trades_fee = run_backtest(df, params_fee)
            print_results(trades_fee, params_fee, label="WITH 0.5% FEE")


if __name__ == "__main__":
    main()
