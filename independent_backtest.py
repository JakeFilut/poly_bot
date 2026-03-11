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
import io
import sys
from contextlib import redirect_stdout
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
    mid_buy_thresh: float = 0.55       # buy Up when mid > this
    mid_sell_thresh: float = 0.35      # sell Up (buy Down) when mid < this
    # --- Dip detection ---
    lookback_sec: int = 5              # seconds to look back for dip/rip
    min_dip_cents: float = 0.0         # minimum dip in cents to trigger (0 = disabled)
    # --- Imbalance surge ---
    min_imb_change: float = 0.0        # imbalance must have risen by this (buy) / fallen (sell)
    # --- Spread filter ---
    require_spread_widening: bool = False  # require spread to be widening
    # --- Execution ---
    entry_mode: str = "passive"        # "passive" (buy at bid) or "cross" (buy at ask)
    hold_sec: int = 120                # seconds to hold position
    position_size: float = 10.0        # shares per trade
    # --- Stop loss / take profit (cents, 0 = disabled) ---
    stop_loss_cents: float = 0.0       # sell if position down this many cents
    take_profit_cents: float = 0.0     # sell if position up this many cents
    # --- Cooldown ---
    cooldown_sec: float = 5.0          # min seconds between trades on same market
    # --- Fee ---
    fee_pct: float = 0.0              # one-way fee as decimal (0.005 = 0.5%)
    # --- Filters ---
    exclude_assets: tuple = ()        # assets to skip (e.g. ("ETH",))


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
    for fwd in [10, 30, 60, 90, 120, 300]:
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


def _find_exit_price(ts_series: np.ndarray, bid_series: np.ndarray,
                     entry_ts: float, entry_price: float, is_buy: bool,
                     hold_sec: int, stop_loss: float, take_profit: float
                     ) -> Tuple[float, str]:
    """Walk tick-by-tick through the hold window to find the exit price.

    Returns (exit_price, exit_reason).
    Checks stop loss / take profit at each tick; falls back to time exit.
    """
    end_ts = entry_ts + hold_sec
    sl_price = stop_loss / 100 if stop_loss > 0 else 0
    tp_price = take_profit / 100 if take_profit > 0 else 0

    # Find ticks within the hold window
    mask = (ts_series > entry_ts) & (ts_series <= end_ts)
    window_bids = bid_series[mask]
    window_ts = ts_series[mask]

    for i in range(len(window_bids)):
        bid = window_bids[i]
        if is_buy:
            edge = bid - entry_price
        else:
            edge = entry_price - bid

        # Stop loss: edge is negative and exceeds threshold
        if sl_price > 0 and edge <= -sl_price:
            return bid, "STOP_LOSS"

        # Take profit: edge is positive and exceeds threshold
        if tp_price > 0 and edge >= tp_price:
            return bid, "TAKE_PROFIT"

    # Time exit: use the last bid at or after end_ts
    after_mask = ts_series >= end_ts
    if after_mask.any():
        return bid_series[after_mask][0], "TIME_EXIT"

    # Fallback: use last available bid in window
    if len(window_bids) > 0:
        return window_bids[-1], "TIME_EXIT"

    return entry_price, "NO_DATA"


def run_backtest(df: pd.DataFrame, params: ConvergenceParams) -> List[Trade]:
    """Run the convergence strategy on enriched book data.

    When stop_loss_cents or take_profit_cents are set, walks through ticks
    during the hold window to find early exits. Otherwise uses vectorized
    forward-looking mid for speed.
    """

    mid_chg_col = f"mid_chg_{params.lookback_sec}s"
    use_pathwise = params.stop_loss_cents > 0 or params.take_profit_cents > 0
    fwd_col = f"mid_fwd_{params.hold_sec}s"

    # Only rows with all needed columns
    required = ["mid", mid_chg_col, "imb_chg_5s", "bestBid", "bestAsk"]
    if not use_pathwise:
        required.append(fwd_col)
    valid = df[df[required].notna().all(axis=1)].copy()

    # Asset exclusion
    if params.exclude_assets:
        valid = valid[~valid["symbol"].isin(params.exclude_assets)]

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

    # --- Entry prices ---
    is_buy = signals["side"] == "BUY_UP"
    if params.entry_mode == "passive":
        entry = np.where(is_buy, signals["bestBid"], signals["bestAsk"])
    else:
        entry = np.where(is_buy, signals["bestAsk"], signals["bestBid"])

    size = params.position_size

    if use_pathwise:
        # --- Path-wise exit: walk ticks to check SL/TP ---
        # Pre-index bid series per (slug, outcome) for fast lookup
        bid_cache: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
        for (slug, outcome), grp in df.groupby(["slug", "outcome"]):
            sorted_grp = grp.sort_values("timestamp_epoch")
            bid_cache[f"{slug}|{outcome}"] = (
                sorted_grp["timestamp_epoch"].values,
                sorted_grp["bestBid"].values,
            )

        exit_prices = np.empty(len(signals))
        exit_reasons = []
        for i, (idx, row) in enumerate(signals.iterrows()):
            cache_key = f"{row['slug']}|{row['outcome']}"
            ts_arr, bid_arr = bid_cache.get(cache_key, (np.array([]), np.array([])))
            ep, reason = _find_exit_price(
                ts_arr, bid_arr,
                entry_ts=row["timestamp_epoch"],
                entry_price=entry[i],
                is_buy=(row["side"] == "BUY_UP"),
                hold_sec=params.hold_sec,
                stop_loss=params.stop_loss_cents,
                take_profit=params.take_profit_cents,
            )
            exit_prices[i] = ep
            exit_reasons.append(reason)
    else:
        # --- Vectorized exit (original fast path) ---
        exit_prices = signals[fwd_col].values
        exit_reasons = ["TIME_EXIT"] * len(signals)

    pnl_gross = np.where(is_buy, (exit_prices - entry) * size, (entry - exit_prices) * size)
    fee = (entry + exit_prices) * size * params.fee_pct
    pnl_net = pnl_gross - fee

    # --- Build trade objects ---
    trades = []
    for i, (idx, row) in enumerate(signals.iterrows()):
        mid_chg = row[mid_chg_col]
        trades.append(Trade(
            timestamp=row["timestamp_epoch"], slug=row["slug"],
            outcome=row["outcome"], asset=row.get("symbol", ""),
            side=row["side"], entry_price=entry[i], exit_price=exit_prices[i],
            size=size, pnl_gross=pnl_gross[i], pnl_net=pnl_net[i],
            hour_et=int(row.get("hour_et", 0)),
            signal=f"mid={row['mid']:.3f} chg={mid_chg*100:.1f}c imb_chg={row['imb_chg_5s']:.3f} exit={exit_reasons[i]}"
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
    sl_str = f"{params.stop_loss_cents:.0f}c" if params.stop_loss_cents > 0 else "off"
    tp_str = f"{params.take_profit_cents:.0f}c" if params.take_profit_cents > 0 else "off"
    print(f"  Stop loss: {sl_str}  |  Take profit: {tp_str}")
    print(f"  Fee:       {params.fee_pct * 100:.2f}% per side")
    print(f"  Cooldown:  {params.cooldown_sec:.0f}s per market")
    print(f"  Filters:   mid_buy>{params.mid_buy_thresh}, mid_sell<{params.mid_sell_thresh}, "
          f"dip>={params.min_dip_cents}c, imb_chg>={params.min_imb_change}")
    if params.exclude_assets:
        print(f"  Skip assets: {params.exclude_assets}")
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

    # --- Exit reason breakdown ---
    if params.stop_loss_cents > 0 or params.take_profit_cents > 0:
        print()
        print("─" * 72)
        print("  EXIT REASONS")
        print("─" * 72)
        for reason in ["STOP_LOSS", "TAKE_PROFIT", "TIME_EXIT", "NO_DATA"]:
            reason_trades = tdf[tdf["signal"].str.contains(f"exit={reason}")]
            if len(reason_trades) > 0:
                r_pnl = reason_trades["pnl_net"].sum()
                r_wr = (reason_trades["pnl_net"] > 0).mean()
                print(f"  {reason:<15} {len(reason_trades):>5} trades  "
                      f"PnL=${r_pnl:>8.2f}  WR={r_wr:.1%}  "
                      f"Avg=${reason_trades['pnl_net'].mean():>8.4f}")

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
    "mid_buy_thresh":   [0.48, 0.50, 0.52, 0.55, 0.60, 0.65, 0.70],
    "mid_sell_thresh":  [0.50, 0.48, 0.45, 0.40, 0.35, 0.30],
    "stop_loss_cents":  [3, 5, 7, 10, 0],           # 0 = disabled
    "take_profit_cents":[5, 8, 12, 15, 20, 0],      # 0 = disabled
    "hold_sec":         [30, 60, 90, 120, 300],
    "cooldown_sec":     [5, 10, 15],
    "entry_mode":       ["cross"],
    "lookback_sec":     [5, 10],
    "min_dip_cents":    [0.0],
    "min_imb_change":   [0.0],
    "fee_pct":          [0.0],
}


def _eval_params(df, params):
    """Run backtest and return summary dict."""
    trades = run_backtest(df, params)
    if trades:
        tdf = pd.DataFrame([t.__dict__ for t in trades])
        total_pnl = tdf["pnl_net"].sum()
        duration_h = max((tdf["timestamp"].max() - tdf["timestamp"].min()) / 3600, 0.01)
        win_rate = (tdf["pnl_net"] > 0).mean()
    else:
        total_pnl = 0.0
        duration_h = 1.0
        win_rate = 0.0
    return {
        "n_trades": len(trades),
        "total_pnl": total_pnl,
        "pnl_per_hour": total_pnl / duration_h,
        "win_rate": win_rate,
    }


# Asset-exclusion sets to test in phase 2
ASSET_EXCLUSION_COMBOS = [
    (),
    ("ETH",),
    ("BTC",),
    ("ETH", "BTC"),
]


def run_sweep(df: pd.DataFrame) -> pd.DataFrame:
    """Two-phase grid search: base params then hour/asset exclusions."""

    # --- Phase 1: base parameter sweep ---
    keys = list(SWEEP_RANGES.keys())
    values = list(SWEEP_RANGES.values())
    combos = list(product(*values))
    print(f"\n  Phase 1: {len(combos)} base parameter combinations")

    results = []
    for i, combo in enumerate(combos):
        kv = dict(zip(keys, combo))

        if kv["mid_sell_thresh"] >= kv["mid_buy_thresh"]:
            continue
        # When no SL/TP, we need the forward-looking column
        has_sl_tp = kv.get("stop_loss_cents", 0) > 0 or kv.get("take_profit_cents", 0) > 0
        if not has_sl_tp:
            hold = kv["hold_sec"]
            fwd_col = f"mid_fwd_{hold}s"
            if fwd_col not in df.columns:
                continue

        p = ConvergenceParams(**kv)
        res = _eval_params(df, p)
        results.append({**kv, **res})

        if (i + 1) % 50 == 0:
            print(f"    ... {i + 1}/{len(combos)} done")

    rdf = pd.DataFrame(results).sort_values("total_pnl", ascending=False)

    print("\n" + "=" * 72)
    print("  PHASE 1 — TOP 15 BASE CONFIGS (by total PnL)")
    print("=" * 72)
    display_cols = keys + ["n_trades", "total_pnl", "pnl_per_hour", "win_rate"]
    print(rdf[display_cols].head(15).to_string(index=False))

    # --- Phase 2: test hour/asset exclusions on top 10 base configs ---
    top_n = min(10, len(rdf))
    top_bases = rdf.head(top_n)
    print(f"\n  Phase 2: testing {len(ASSET_EXCLUSION_COMBOS)} asset-exclusion sets on top {top_n} configs")

    phase2_results = []
    for _, base_row in top_bases.iterrows():
        base_kv = {k: base_row[k] for k in keys}
        base_kv["hold_sec"] = int(base_kv["hold_sec"])
        if "lookback_sec" in base_kv:
            base_kv["lookback_sec"] = int(base_kv["lookback_sec"])

        for exc_assets in ASSET_EXCLUSION_COMBOS:
            p = ConvergenceParams(**base_kv,
                                  exclude_assets=exc_assets)
            res = _eval_params(df, p)
            phase2_results.append({
                **base_kv,
                "exclude_assets": str(exc_assets) if exc_assets else "none",
                **res,
            })

    p2df = pd.DataFrame(phase2_results).sort_values("total_pnl", ascending=False)

    print("\n" + "=" * 72)
    print("  PHASE 2 — TOP 20 WITH ASSET EXCLUSIONS (by total PnL)")
    print("=" * 72)
    p2_cols = keys + ["exclude_assets", "n_trades", "total_pnl", "pnl_per_hour", "win_rate"]
    print(p2df[p2_cols].head(20).to_string(index=False))

    return p2df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Independent Convergence Strategy Backtest")
    parser.add_argument("--data-dir", type=str, default=None)
    parser.add_argument("--sweep", action="store_true", help="Run parameter sweep")
    parser.add_argument("--fee", type=float, default=None, help="Override fee pct (e.g. 0.005 for 0.5%%)")
    parser.add_argument("--entry-mode", choices=["passive", "cross"], default="cross")
    parser.add_argument("--hold", type=int, default=120)
    parser.add_argument("--cooldown", type=float, default=5.0)
    parser.add_argument("--stop-loss", type=float, default=5.0, help="Stop loss in cents (0=off)")
    parser.add_argument("--take-profit", type=float, default=12.0, help="Take profit in cents (0=off)")
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

    # Capture all results output to also write to file
    results_buf = io.StringIO()

    def tee_print_results(trades, params, label=""):
        """Print results to console AND capture to buffer."""
        capture = io.StringIO()
        with redirect_stdout(capture):
            result = print_results(trades, params, label=label)
        text = capture.getvalue()
        sys.stdout.write(text)  # still show on console
        results_buf.write(text)
        return result

    if args.sweep:
        print("\n[3] Running parameter sweep ...")
        rdf = run_sweep(df)

        # Run the best config with full report
        best = rdf.iloc[0]
        print(f"\n[4] Detailed report for best config ...")
        best_kv = {k: best[k] for k in SWEEP_RANGES.keys()}
        for int_key in ("hold_sec", "lookback_sec"):
            if int_key in best_kv:
                best_kv[int_key] = int(best_kv[int_key])
        # Restore exclude params from phase 2
        exc_a = best.get("exclude_assets", "none")
        if exc_a != "none" and exc_a:
            best_kv["exclude_assets"] = tuple(x.strip().strip("'\"") for x in exc_a.strip("()").split(",") if x.strip())
        best_params = ConvergenceParams(**best_kv)
        trades = run_backtest(df, best_params)
        tee_print_results(trades, best_params, label="BEST SWEEP")

        # Also show with 0.5% fee
        print(f"\n[5] Same config with 0.5% fee ...")
        best_params.fee_pct = 0.005
        trades = run_backtest(df, best_params)
        tee_print_results(trades, best_params, label="BEST SWEEP + 0.5% FEE")
    else:
        print("\n[3] Running backtest ...")
        params = ConvergenceParams(
            entry_mode=args.entry_mode,
            hold_sec=args.hold,
            cooldown_sec=args.cooldown,
            stop_loss_cents=args.stop_loss,
            take_profit_cents=args.take_profit,
        )
        if args.fee is not None:
            params.fee_pct = args.fee

        trades = run_backtest(df, params)
        tee_print_results(trades, params, label="BASELINE")

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
            tee_print_results(trades_fee, params_fee, label="WITH 0.5% FEE")

    # Write results to file
    outdir = Path("reports")
    outdir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = outdir / f"backtest_results_{ts}.txt"
    outfile.write_text(results_buf.getvalue())
    print(f"\n  Results saved to {outfile}")


if __name__ == "__main__":
    main()
