#!/usr/bin/env python3
"""
replay_engine.py -- Market Replay & Strategy Comparison Framework.

Replays historical market conditions from logged datasets and compares
bot entry decisions against wallet trades to tune strategy parameters.

Usage:
    python replay_engine.py [--data-dir LOGS_DIR] [--optimize] [--no-charts]
"""
from __future__ import annotations

import argparse
import itertools
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Try importing matplotlib -- optional for charts
# ---------------------------------------------------------------------------
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 -- DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class MarketState:
    """Snapshot of market conditions at a single point in time."""
    timestamp: float          # epoch seconds
    asset: str                # BTC / ETH / SOL / XRP
    slug: str
    outcome: str              # Up / Down
    token_id: str
    best_bid: float
    best_ask: float
    spread_cents: float
    spread_percentile: float
    orderbook_imbalance: float
    binance_ret_5s: float
    binance_ret_30s: float
    binance_ret_60s: float


@dataclass
class BotDecision:
    """A decision made by the strategy during replay."""
    timestamp: float
    asset: str
    slug: str
    outcome: str
    action: str               # BUY / SELL / NO_ACTION
    reason: str
    spread_cents: float
    spread_percentile: float
    orderbook_imbalance: float
    binance_ret_30s: float


@dataclass
class ComparisonResult:
    """Outcome of comparing bot decisions to wallet trades."""
    wallet_trades_total: int
    wallet_trades_matched: int
    wallet_trades_missed: int
    bot_false_entries: int
    similarity_score: float
    entry_lag_median: float = 0.0   # median (bot_ts - wallet_ts) for matched trades
    entry_lag_p90: float = 0.0      # p90 entry lag


@dataclass
class StrategyParams:
    """Tunable strategy parameters for the sweep."""
    entry_min_spread_pctl: float = 0.90
    entry_min_spread_cents: float = 2.0
    entry_min_ret_30s: float = 0.0015
    entry_min_imbalance: float = 0.60


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 -- MARKET REPLAY ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class MarketReplayEngine:
    """Loads and replays historical market data in chronological order."""

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.book_tape: pd.DataFrame = pd.DataFrame()
        self.binance_tape: pd.DataFrame = pd.DataFrame()
        self.wallet_trades: pd.DataFrame = pd.DataFrame()

    def load(self) -> None:
        """Load all three datasets."""
        book_path = self.data_dir / "f247_copywallet_book_tape.csv"
        binance_path = self.data_dir / "f247_copywallet_binance_tape.csv"
        fills_path = self.data_dir / "f247_copywallet_fills_enriched.csv"

        if book_path.exists():
            self.book_tape = pd.read_csv(book_path)
            self._normalize_ts(self.book_tape)
            print(f"  book_tape:    {len(self.book_tape):>8,} rows")
        else:
            print(f"  WARNING: {book_path} not found")

        if binance_path.exists():
            self.binance_tape = pd.read_csv(binance_path)
            self._normalize_ts(self.binance_tape)
            print(f"  binance_tape: {len(self.binance_tape):>8,} rows")
        else:
            print(f"  WARNING: {binance_path} not found")

        if fills_path.exists():
            self.wallet_trades = pd.read_csv(fills_path)
            self._normalize_ts(self.wallet_trades)
            print(f"  wallet_fills: {len(self.wallet_trades):>8,} rows")
        else:
            print(f"  WARNING: {fills_path} not found")

    @staticmethod
    def _normalize_ts(df: pd.DataFrame) -> None:
        """Ensure a numeric 'ts' column exists (epoch seconds)."""
        if "timestamp_epoch" in df.columns:
            df["ts"] = pd.to_numeric(df["timestamp_epoch"], errors="coerce")
        elif "timestamp_iso" in df.columns:
            df["ts"] = pd.to_datetime(df["timestamp_iso"], utc=True).astype(int) / 1e9
        else:
            # fallback: first column that looks epoch-ish
            for col in df.columns:
                if "time" in col.lower() or "epoch" in col.lower():
                    df["ts"] = pd.to_numeric(df[col], errors="coerce")
                    break

    def build_binance_returns(self) -> Dict[str, pd.DataFrame]:
        """Pre-compute rolling Binance returns per symbol.

        Returns dict: symbol -> DataFrame with columns [ts, price, ret_5s, ret_30s, ret_60s].
        """
        result: Dict[str, pd.DataFrame] = {}
        if self.binance_tape.empty:
            return result

        sym_col = "symbol" if "symbol" in self.binance_tape.columns else "asset"
        for sym, grp in self.binance_tape.groupby(sym_col):
            g = grp.sort_values("ts").copy()
            g = g.dropna(subset=["ts", "price"])
            g["price"] = pd.to_numeric(g["price"], errors="coerce")
            g = g.dropna(subset=["price"])
            if g.empty:
                continue

            # Approximate returns using time-based lookback on 1Hz tape
            g = g.set_index("ts")
            prices = g["price"]

            ret_5 = []
            ret_30 = []
            ret_60 = []
            ts_list = prices.index.values

            for i, t in enumerate(ts_list):
                px = prices.iloc[i]

                # 5s lookback
                mask_5 = ts_list[max(0, i - 10):i + 1]
                past_5 = [prices.loc[tt] for tt in mask_5 if t - tt >= 4.5 and t - tt <= 6.0]
                ret_5.append((px / past_5[0] - 1) if past_5 else 0.0)

                # 30s lookback
                mask_30 = ts_list[max(0, i - 40):i + 1]
                past_30 = [prices.loc[tt] for tt in mask_30 if t - tt >= 28.0 and t - tt <= 32.0]
                ret_30.append((px / past_30[0] - 1) if past_30 else 0.0)

                # 60s lookback
                mask_60 = ts_list[max(0, i - 70):i + 1]
                past_60 = [prices.loc[tt] for tt in mask_60 if t - tt >= 58.0 and t - tt <= 62.0]
                ret_60.append((px / past_60[0] - 1) if past_60 else 0.0)

            g["ret_5s"] = ret_5
            g["ret_30s"] = ret_30
            g["ret_60s"] = ret_60
            g = g.reset_index()
            result[str(sym)] = g

        return result

    def generate_market_states(self) -> List[MarketState]:
        """Walk through the book tape chronologically, enriching each
        snapshot with Binance returns to produce MarketState objects."""
        if self.book_tape.empty:
            print("  No book_tape data -- cannot generate market states.")
            return []

        # Pre-compute binance returns
        binance_rets = self.build_binance_returns()

        # Determine column mappings for book_tape
        bt = self.book_tape.sort_values("ts").copy()
        states: List[MarketState] = []

        # Map crypto asset from slug/token_id if available
        asset_col = None
        for c in ["crypto", "asset"]:
            if c in bt.columns:
                asset_col = c
                break

        for _, row in bt.iterrows():
            ts = row.get("ts", 0.0)
            if pd.isna(ts) or ts == 0:
                continue

            slug = str(row.get("slug", ""))
            outcome = str(row.get("outcome", ""))
            token_id = str(row.get("token_id", ""))
            asset = str(row.get(asset_col, "BTC")) if asset_col else "BTC"

            best_bid = float(row.get("bestBid", 0))
            best_ask = float(row.get("bestAsk", 0))
            spread = float(row.get("spread", 0))
            spread_cents = spread * 100 if spread < 1.0 else spread  # handle both formats
            spread_pctl = float(row.get("spread_percentile_60s", 0))
            imbalance = float(row.get("imbalance_topN", 0.5))

            # Look up Binance returns at this timestamp
            r5 = r30 = r60 = 0.0
            sym_key = asset + "USDT" if asset else None
            # Try exact symbol match first, then asset name
            bdf = binance_rets.get(sym_key) or binance_rets.get(asset, pd.DataFrame())
            if not bdf.empty:
                idx = bdf["ts"].searchsorted(ts) - 1
                if 0 <= idx < len(bdf):
                    r5 = float(bdf.iloc[idx].get("ret_5s", 0))
                    r30 = float(bdf.iloc[idx].get("ret_30s", 0))
                    r60 = float(bdf.iloc[idx].get("ret_60s", 0))

            states.append(MarketState(
                timestamp=ts,
                asset=asset,
                slug=slug,
                outcome=outcome,
                token_id=token_id,
                best_bid=best_bid,
                best_ask=best_ask,
                spread_cents=spread_cents,
                spread_percentile=spread_pctl,
                orderbook_imbalance=imbalance,
                binance_ret_5s=r5,
                binance_ret_30s=r30,
                binance_ret_60s=r60,
            ))

        print(f"  Generated {len(states):,} market state snapshots.")
        return states


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 -- BOT DECISION REPLAY (Strategy Evaluator)
# ═══════════════════════════════════════════════════════════════════════════

class Strategy:
    """Simplified strategy that mirrors the live bot's entry gates."""

    def __init__(self, params: StrategyParams):
        self.params = params

    def evaluate_market_state(self, ms: MarketState) -> BotDecision:
        """Evaluate a single market state snapshot and return a decision."""
        p = self.params

        # --- Gate checks ---
        if ms.spread_cents < p.entry_min_spread_cents:
            return self._no_action(ms, "spread_cents below threshold")

        if ms.spread_percentile < p.entry_min_spread_pctl:
            return self._no_action(ms, "spread_pctl below threshold")

        if abs(ms.binance_ret_30s) < p.entry_min_ret_30s:
            return self._no_action(ms, "ret_30s below threshold")

        if ms.orderbook_imbalance < p.entry_min_imbalance:
            return self._no_action(ms, "imbalance below threshold")

        # All gates passed -- direction from momentum
        action = "BUY" if ms.binance_ret_30s > 0 else "SELL"

        return BotDecision(
            timestamp=ms.timestamp,
            asset=ms.asset,
            slug=ms.slug,
            outcome=ms.outcome,
            action=action,
            reason="all_gates_passed",
            spread_cents=ms.spread_cents,
            spread_percentile=ms.spread_percentile,
            orderbook_imbalance=ms.orderbook_imbalance,
            binance_ret_30s=ms.binance_ret_30s,
        )

    @staticmethod
    def _no_action(ms: MarketState, reason: str) -> BotDecision:
        return BotDecision(
            timestamp=ms.timestamp,
            asset=ms.asset,
            slug=ms.slug,
            outcome=ms.outcome,
            action="NO_ACTION",
            reason=reason,
            spread_cents=ms.spread_cents,
            spread_percentile=ms.spread_percentile,
            orderbook_imbalance=ms.orderbook_imbalance,
            binance_ret_30s=ms.binance_ret_30s,
        )


COOLDOWN_SECONDS = 20  # suppress duplicate bot entries per market


def run_replay(states: List[MarketState], params: StrategyParams) -> List[BotDecision]:
    """Run the strategy over all market states and return decisions.

    Applies a per-market cooldown: after the bot triggers an entry for a
    (slug, outcome) pair, further entries for that pair are suppressed for
    COOLDOWN_SECONDS to prevent overfitting during parameter sweeps.
    """
    strategy = Strategy(params)
    last_entry_ts: Dict[Tuple[str, str], float] = {}  # (slug, outcome) -> epoch
    decisions: List[BotDecision] = []

    for ms in states:
        decision = strategy.evaluate_market_state(ms)

        if decision.action in ("BUY", "SELL"):
            key = (ms.slug, ms.outcome)
            prev = last_entry_ts.get(key, 0.0)
            if ms.timestamp - prev < COOLDOWN_SECONDS:
                decision = Strategy._no_action(ms, "cooldown_active")
            else:
                last_entry_ts[key] = ms.timestamp

        decisions.append(decision)

    return decisions


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 -- WALLET COMPARISON
# ═══════════════════════════════════════════════════════════════════════════

def _resolve_wallet_side(row: pd.Series) -> str:
    """Normalise wallet trade side to BUY / SELL."""
    for col in ("side", "action", "direction"):
        if col in row.index and pd.notna(row[col]):
            return str(row[col]).strip().upper()
    return ""


def _resolve_wallet_asset(row: pd.Series) -> str:
    """Normalise wallet trade asset."""
    for col in ("crypto", "asset"):
        if col in row.index and pd.notna(row[col]):
            return str(row[col]).strip().upper()
    return ""


def compare_to_wallet(
    decisions: List[BotDecision],
    wallet_trades: pd.DataFrame,
    tolerance_sec: float = 3.0,
) -> ComparisonResult:
    """Compare bot decisions against wallet trades.

    Matching requires:
      1. Timestamp within +/- tolerance_sec
      2. Same trade direction (BUY == BUY, SELL == SELL)
      3. Same asset (BTC == BTC, etc.)

    Also computes entry_lag_seconds = bot_ts - wallet_ts for every match.
    """
    if wallet_trades.empty or not decisions:
        return ComparisonResult(0, 0, 0, 0, 0.0)

    wt = wallet_trades.copy()
    if "ts" not in wt.columns:
        return ComparisonResult(0, 0, 0, 0, 0.0)

    # --- Build bot entry index keyed by (asset, side) ---
    # Each value is a sorted array of timestamps for that group.
    bot_entries_by_key: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for d in decisions:
        if d.action in ("BUY", "SELL"):
            key = (d.asset.upper(), d.action)
            bot_entries_by_key[key].append(d.timestamp)

    bot_arrays: Dict[Tuple[str, str], np.ndarray] = {
        k: np.array(sorted(v)) for k, v in bot_entries_by_key.items()
    }

    # --- Walk wallet trades and attempt matching ---
    total = 0
    matched = 0
    entry_lags: List[float] = []

    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue

        w_side = _resolve_wallet_side(row)
        w_asset = _resolve_wallet_asset(row)
        if not w_side:
            continue

        total += 1
        key = (w_asset, w_side)
        arr = bot_arrays.get(key)
        if arr is None or len(arr) == 0:
            continue

        idx = np.searchsorted(arr, wts)
        best_lag = None
        for i in range(max(0, idx - 1), min(len(arr), idx + 2)):
            lag = arr[i] - wts
            if abs(lag) <= tolerance_sec:
                if best_lag is None or abs(lag) < abs(best_lag):
                    best_lag = lag
        if best_lag is not None:
            matched += 1
            entry_lags.append(best_lag)

    missed = total - matched

    # --- False entries: bot entries with no matching wallet trade ---
    # Build wallet index keyed by (asset, side) for reverse lookup.
    wallet_by_key: Dict[Tuple[str, str], np.ndarray] = defaultdict(list)
    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        w_asset = _resolve_wallet_asset(row)
        if w_side:
            wallet_by_key[(w_asset, w_side)].append(float(wts))

    wallet_arrays: Dict[Tuple[str, str], np.ndarray] = {
        k: np.array(sorted(v)) for k, v in wallet_by_key.items()
    }

    bot_false = 0
    for d in decisions:
        if d.action not in ("BUY", "SELL"):
            continue
        key = (d.asset.upper(), d.action)
        warr = wallet_arrays.get(key)
        if warr is None or len(warr) == 0:
            bot_false += 1
            continue
        idx = np.searchsorted(warr, d.timestamp)
        found = False
        for i in range(max(0, idx - 1), min(len(warr), idx + 2)):
            if abs(warr[i] - d.timestamp) <= tolerance_sec:
                found = True
                break
        if not found:
            bot_false += 1

    similarity = matched / total if total > 0 else 0.0

    # --- Latency stats ---
    lag_median = float(np.median(entry_lags)) if entry_lags else 0.0
    lag_p90 = float(np.percentile(entry_lags, 90)) if entry_lags else 0.0

    return ComparisonResult(
        wallet_trades_total=total,
        wallet_trades_matched=matched,
        wallet_trades_missed=missed,
        bot_false_entries=bot_false,
        similarity_score=similarity,
        entry_lag_median=lag_median,
        entry_lag_p90=lag_p90,
    )


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 -- SIGNAL DISTRIBUTION ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

def analyze_signal_distributions(wallet_trades: pd.DataFrame) -> pd.DataFrame:
    """Compute distributions of key features at wallet trade time.

    Returns a DataFrame with rows = metrics, columns = [median, p25, p75, p90].
    """
    feature_cols = {
        "spread_cents_at_trade": ["spread", "spread_cents"],
        "spread_percentile_at_trade": ["spread_percentile_60s", "spread_percentile_60s_at_trade"],
        "orderbook_imbalance_at_trade": ["imbalance_topN", "orderbook_imbalance_at_trade"],
        "binance_ret_5s_at_trade": ["binance_ret_5s_at_trade", "ret_5s"],
        "binance_ret_30s_at_trade": ["binance_ret_30s_at_trade", "ret_30s"],
        "binance_ret_60s_at_trade": ["binance_ret_60s_at_trade", "ret_60s"],
    }

    rows = []
    for label, candidates in feature_cols.items():
        col = None
        for c in candidates:
            if c in wallet_trades.columns:
                col = c
                break
        if col is None:
            continue

        vals = pd.to_numeric(wallet_trades[col], errors="coerce").dropna()
        # Convert spread from decimal to cents if needed
        if "spread_cents" in label and col == "spread":
            vals = vals * 100

        if vals.empty:
            continue

        rows.append({
            "metric": label,
            "median": vals.median(),
            "p25": vals.quantile(0.25),
            "p75": vals.quantile(0.75),
            "p90": vals.quantile(0.90),
        })

    return pd.DataFrame(rows).set_index("metric") if rows else pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5 -- PARAMETER OPTIMIZATION (Sweep)
# ═══════════════════════════════════════════════════════════════════════════

DEFAULT_SWEEP_RANGES = {
    "entry_min_spread_pctl": np.arange(0.80, 0.96, 0.02),
    "entry_min_spread_cents": [2.0],  # keep fixed
    "entry_min_ret_30s": np.arange(0.0005, 0.0035, 0.0005),
    "entry_min_imbalance": np.arange(0.55, 0.76, 0.05),
}


def parameter_sweep(
    states: List[MarketState],
    wallet_trades: pd.DataFrame,
    sweep_ranges: Optional[Dict] = None,
    tolerance_sec: float = 3.0,
) -> pd.DataFrame:
    """Exhaustive grid search over strategy parameters.

    Returns DataFrame of all combinations ranked by similarity_score descending.
    """
    ranges = sweep_ranges or DEFAULT_SWEEP_RANGES

    keys = list(ranges.keys())
    grid = list(itertools.product(*(ranges[k] for k in keys)))
    total = len(grid)
    print(f"\n  Parameter sweep: {total} combinations ...")

    results = []
    for idx, combo in enumerate(grid):
        params = StrategyParams(**dict(zip(keys, combo)))
        decisions = run_replay(states, params)
        cr = compare_to_wallet(decisions, wallet_trades, tolerance_sec)

        entry = {k: round(float(v), 6) for k, v in zip(keys, combo)}
        entry["similarity"] = round(cr.similarity_score, 4)
        entry["matched"] = cr.wallet_trades_matched
        entry["missed"] = cr.wallet_trades_missed
        entry["false_entries"] = cr.bot_false_entries
        entry["lag_median"] = round(cr.entry_lag_median, 3)
        entry["lag_p90"] = round(cr.entry_lag_p90, 3)
        results.append(entry)

        if (idx + 1) % max(1, total // 10) == 0:
            print(f"    ... {idx + 1}/{total}")

    df = pd.DataFrame(results).sort_values("similarity", ascending=False).reset_index(drop=True)
    return df


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6 -- OUTPUT REPORT
# ═══════════════════════════════════════════════════════════════════════════

def print_report(
    comparison: ComparisonResult,
    signal_dist: pd.DataFrame,
    sweep_results: Optional[pd.DataFrame] = None,
) -> None:
    """Print the full analysis report."""
    print("\n" + "=" * 70)
    print("  MARKET REPLAY -- COMPARISON REPORT")
    print("=" * 70)

    print(f"\n  Wallet trades total:   {comparison.wallet_trades_total}")
    print(f"  Wallet trades matched: {comparison.wallet_trades_matched}")
    print(f"  Wallet trades missed:  {comparison.wallet_trades_missed}")
    print(f"  Bot false entries:     {comparison.bot_false_entries}")
    print(f"  Similarity score:      {comparison.similarity_score:.4f}")
    print(f"\n  Entry lag (bot - wallet):")
    print(f"    median:  {comparison.entry_lag_median:+.3f}s")
    print(f"    p90:     {comparison.entry_lag_p90:+.3f}s")

    if not signal_dist.empty:
        print(f"\n{'─' * 70}")
        print("  SIGNAL DISTRIBUTIONS AT WALLET ENTRY")
        print(f"{'─' * 70}")
        print(signal_dist.to_string(float_format=lambda x: f"{x:.6f}"))

    if sweep_results is not None and not sweep_results.empty:
        print(f"\n{'─' * 70}")
        print("  TOP 10 PARAMETER SETS (by similarity)")
        print(f"{'─' * 70}")
        top = sweep_results.head(10)
        print(top.to_string(index=False))

    print("\n" + "=" * 70)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 7 -- VISUAL DEBUGGING
# ═══════════════════════════════════════════════════════════════════════════

def generate_charts(
    decisions: List[BotDecision],
    wallet_trades: pd.DataFrame,
    signal_dist: pd.DataFrame,
    output_dir: str = "replay_charts",
) -> None:
    """Generate visual debugging charts."""
    if not HAS_MPL:
        print("  matplotlib not available -- skipping charts.")
        return

    out = Path(output_dir)
    out.mkdir(exist_ok=True)

    # 1. Wallet trades vs bot trades over time
    bot_entries = [(d.timestamp, d.action) for d in decisions if d.action in ("BUY", "SELL")]
    if bot_entries and not wallet_trades.empty and "ts" in wallet_trades.columns:
        fig, ax = plt.subplots(figsize=(14, 4))
        wts = wallet_trades["ts"].dropna().values
        bot_ts = [t for t, _ in bot_entries]

        ax.scatter(wts, [1] * len(wts), alpha=0.5, s=10, label="Wallet trades", color="blue")
        ax.scatter(bot_ts, [0] * len(bot_ts), alpha=0.3, s=6, label="Bot entries", color="red")
        ax.set_yticks([0, 1])
        ax.set_yticklabels(["Bot", "Wallet"])
        ax.set_xlabel("Epoch time")
        ax.set_title("Wallet Trades vs Bot Entries Over Time")
        ax.legend(loc="upper right")
        fig.tight_layout()
        fig.savefig(out / "trades_timeline.png", dpi=120)
        plt.close(fig)
        print(f"  Chart saved: {out / 'trades_timeline.png'}")

    # 2-4. Distribution histograms at wallet entry
    dist_features = {
        "spread": (["spread", "spread_cents"], "Spread (cents) at Wallet Entry"),
        "imbalance": (["imbalance_topN", "orderbook_imbalance_at_trade"], "Imbalance at Wallet Entry"),
        "momentum": (["binance_ret_30s_at_trade", "ret_30s"], "Binance ret_30s at Wallet Entry"),
    }

    if not wallet_trades.empty:
        for fname, (candidates, title) in dist_features.items():
            col = None
            for c in candidates:
                if c in wallet_trades.columns:
                    col = c
                    break
            if col is None:
                continue

            vals = pd.to_numeric(wallet_trades[col], errors="coerce").dropna()
            if "spread" in fname and col == "spread":
                vals = vals * 100

            if vals.empty:
                continue

            fig, ax = plt.subplots(figsize=(8, 4))
            ax.hist(vals, bins=40, edgecolor="black", alpha=0.7)
            ax.axvline(vals.median(), color="red", linestyle="--", label=f"median={vals.median():.4f}")
            ax.set_title(title)
            ax.set_xlabel(col)
            ax.set_ylabel("Count")
            ax.legend()
            fig.tight_layout()
            fig.savefig(out / f"dist_{fname}.png", dpi=120)
            plt.close(fig)
            print(f"  Chart saved: {out / f'dist_{fname}.png'}")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 8 -- MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def find_data_dir() -> str:
    """Try common locations for the CSV log files."""
    candidates = [
        "logs/poly_bot",
        "/home/ubuntu/github/logs/poly_bot",
        ".",
    ]
    for d in candidates:
        p = Path(d)
        if (p / "f247_copywallet_book_tape.csv").exists():
            return str(p)
    return candidates[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Market Replay & Strategy Comparison")
    parser.add_argument("--data-dir", type=str, default=None,
                        help="Directory containing the CSV log files")
    parser.add_argument("--optimize", action="store_true",
                        help="Run parameter sweep optimization")
    parser.add_argument("--no-charts", action="store_true",
                        help="Skip chart generation")
    parser.add_argument("--tolerance", type=float, default=3.0,
                        help="Matching tolerance in seconds (default: 3.0)")
    args = parser.parse_args()

    data_dir = args.data_dir or find_data_dir()
    print(f"\n  Data directory: {data_dir}")

    # --- Step 1: Load datasets ---
    print("\n[1/6] Loading datasets ...")
    engine = MarketReplayEngine(data_dir)
    engine.load()

    # --- Step 2: Replay market conditions ---
    print("\n[2/6] Generating market state snapshots ...")
    states = engine.generate_market_states()
    if not states:
        print("  No market states generated. Check your data files.")
        sys.exit(1)

    # --- Step 3: Run bot strategy with default params ---
    print("\n[3/6] Running bot strategy replay (default params) ...")
    default_params = StrategyParams()
    decisions = run_replay(states, default_params)
    entry_count = sum(1 for d in decisions if d.action in ("BUY", "SELL"))
    print(f"  Total decisions: {len(decisions):,}  |  Entries: {entry_count:,}")

    # --- Step 4: Compare to wallet trades ---
    print("\n[4/6] Comparing to wallet trades ...")
    comparison = compare_to_wallet(decisions, engine.wallet_trades, args.tolerance)
    print(f"  Similarity score: {comparison.similarity_score:.4f}")
    print(f"  Entry lag median: {comparison.entry_lag_median:+.3f}s  |  p90: {comparison.entry_lag_p90:+.3f}s")

    # --- Step 5: Signal distribution analysis ---
    print("\n[5/6] Analyzing signal distributions at wallet entry ...")
    signal_dist = analyze_signal_distributions(engine.wallet_trades)

    # --- Step 6: Parameter optimization (optional) ---
    sweep_results = None
    if args.optimize:
        print("\n[6/6] Running parameter sweep optimization ...")
        sweep_results = parameter_sweep(states, engine.wallet_trades, tolerance_sec=args.tolerance)
    else:
        print("\n[6/6] Skipping parameter sweep (use --optimize to enable)")

    # --- Report ---
    print_report(comparison, signal_dist, sweep_results)

    # --- Charts ---
    if not args.no_charts:
        print("\n  Generating charts ...")
        generate_charts(decisions, engine.wallet_trades, signal_dist)

    print("\n  Done.\n")


if __name__ == "__main__":
    main()
