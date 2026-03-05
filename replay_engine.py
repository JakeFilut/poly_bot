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

import re

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

_SLUG_ASSET_PATTERNS = [
    (re.compile(r"\bbitcoin\b", re.I), "BTC"),
    (re.compile(r"\bethereum\b", re.I), "ETH"),
    (re.compile(r"\bsolana\b", re.I), "SOL"),
    (re.compile(r"\bxrp\b", re.I), "XRP"),
]


def asset_from_slug(slug: str) -> str:
    """Derive crypto asset ticker from a Polymarket slug string.

    E.g. 'bitcoin-up-or-down-...' -> 'BTC', 'xrp-up-or-down-...' -> 'XRP'.
    Returns '' if no known asset is found.
    """
    for pat, ticker in _SLUG_ASSET_PATTERNS:
        if pat.search(slug):
            return ticker
    return ""


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
    binance_ret_120s: float = 0.0   # 120s return for ret_accel
    spread_pctl_prev: float = 0.0   # spread_pctl ~60s ago for delta


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
    entry_lag_median: float = float("nan")  # median (bot_ts - wallet_ts) for matched trades
    entry_lag_p90: float = float("nan")    # p90 entry lag


@dataclass
class StrategyParams:
    """Tunable strategy parameters for the sweep.

    Defaults are set to the BEST SWEEP params discovered during optimization
    so the baseline comparison is meaningful (not zero matches).
    """
    entry_min_spread_pctl: float = 0.94
    entry_min_spread_cents: float = 1.0
    entry_min_ret_30s: float = 0.0005
    entry_min_imbalance: float = 0.45
    # -- New one-shot conditions (task 4) --
    spread_pctl_delta_min: float = 0.0      # pctl_now - pctl_60s_ago >= this
    ret_accel_min: float = 0.0              # |ret_30s - ret_120s| >= this
    entry_cooldown_sec: float = 0.0         # per (slug, outcome, side) cooldown


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
            ret_120 = []
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

                # 120s lookback
                mask_120 = ts_list[max(0, i - 140):i + 1]
                past_120 = [prices.loc[tt] for tt in mask_120 if t - tt >= 118.0 and t - tt <= 122.0]
                ret_120.append((px / past_120[0] - 1) if past_120 else 0.0)

            g["ret_5s"] = ret_5
            g["ret_30s"] = ret_30
            g["ret_60s"] = ret_60
            g["ret_120s"] = ret_120
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
            # Always derive asset from slug to avoid wrong-asset bugs
            asset = asset_from_slug(slug)
            if not asset:
                # Fallback to CSV column only if slug doesn't contain a known asset
                asset = str(row.get(asset_col, "BTC")) if asset_col else "BTC"

            best_bid = float(row.get("bestBid", 0))
            best_ask = float(row.get("bestAsk", 0))
            spread = float(row.get("spread", 0))
            spread_cents = round((best_ask - best_bid) * 100, 1) if best_ask > 0 and best_bid > 0 else (spread * 100 if spread < 1.0 else spread)
            spread_pctl = float(row.get("spread_percentile_60s", 0))

            # Compute imbalance as bid/(bid+ask), guaranteed [0,1]
            bid_depth = max(0.0, float(row.get("bid_depth_topN", 0)))
            ask_depth = max(0.0, float(row.get("ask_depth_topN", 0)))
            imbalance = bid_depth / max(1e-9, bid_depth + ask_depth) if (bid_depth + ask_depth) > 0 else 0.5

            # Look up Binance returns at this timestamp
            r5 = r30 = r60 = r120 = 0.0
            sym_key = asset + "USDT" if asset else None
            # Try exact symbol match first, then asset name
            bdf = binance_rets.get(sym_key) or binance_rets.get(asset, pd.DataFrame())
            if not bdf.empty:
                idx = bdf["ts"].searchsorted(ts) - 1
                if 0 <= idx < len(bdf):
                    binance_ts = float(bdf.iloc[idx]["ts"])
                    if abs(binance_ts - ts) <= 10.0:  # align within ±10s
                        r5 = float(bdf.iloc[idx].get("ret_5s", 0))
                        r30 = float(bdf.iloc[idx].get("ret_30s", 0))
                        r60 = float(bdf.iloc[idx].get("ret_60s", 0))
                        r120 = float(bdf.iloc[idx].get("ret_120s", 0))

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
                binance_ret_120s=r120,
            ))

        # Backfill spread_pctl_prev: for each (slug, outcome) track pctl ~60s ago
        slug_pctl_history: Dict[Tuple[str, str], List[Tuple[float, float]]] = defaultdict(list)
        for ms in states:
            key = (ms.slug, ms.outcome)
            hist = slug_pctl_history[key]
            # Find pctl value ~60s ago
            prev_pctl = 0.0
            for ht, hp in reversed(hist):
                dt = ms.timestamp - ht
                if 55.0 <= dt <= 65.0:
                    prev_pctl = hp
                    break
                if dt > 70.0:
                    break
            ms.spread_pctl_prev = prev_pctl
            hist.append((ms.timestamp, ms.spread_percentile))
            # Trim old entries beyond 120s
            while hist and ms.timestamp - hist[0][0] > 120.0:
                hist.pop(0)

        print(f"  Generated {len(states):,} market state snapshots.")
        return states


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 -- BOT DECISION REPLAY (Strategy Evaluator)
# ═══════════════════════════════════════════════════════════════════════════

class Strategy:
    """Simplified strategy that mirrors the live bot's entry gates."""

    def __init__(self, params: StrategyParams):
        self.params = params

    def evaluate_market_state(self, ms: MarketState) -> Tuple[BotDecision, str]:
        """Evaluate a single market state snapshot and return (decision, gate_that_failed).

        gate_that_failed is '' if all gates pass, otherwise the name of the first failing gate.
        """
        p = self.params

        # --- Gate checks ---
        if ms.spread_cents < p.entry_min_spread_cents:
            return self._no_action(ms, "spread_cents below threshold"), "spread_cents"

        if ms.spread_percentile < p.entry_min_spread_pctl:
            return self._no_action(ms, "spread_pctl below threshold"), "spread_pctl"

        if abs(ms.binance_ret_30s) < p.entry_min_ret_30s:
            return self._no_action(ms, "ret_30s below threshold"), "ret_30s"

        if ms.orderbook_imbalance < p.entry_min_imbalance:
            return self._no_action(ms, "imbalance below threshold"), "imbalance"

        # --- New condition: spread percentile delta ---
        if p.spread_pctl_delta_min > 0:
            pctl_delta = ms.spread_percentile - ms.spread_pctl_prev
            if pctl_delta < p.spread_pctl_delta_min:
                return self._no_action(ms, "spread_pctl_delta below threshold"), "spread_pctl_delta"

        # --- New condition: return acceleration ---
        if p.ret_accel_min > 0:
            ret_accel = abs(ms.binance_ret_30s - ms.binance_ret_120s)
            if ret_accel < p.ret_accel_min:
                return self._no_action(ms, "ret_accel below threshold"), "ret_accel"

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
        ), ""

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

    Applies a per-(slug, outcome, side) cooldown: after the bot triggers an
    entry, further entries for that triple are suppressed for
    max(COOLDOWN_SECONDS, params.entry_cooldown_sec) to prevent overfitting.
    """
    strategy = Strategy(params)
    cooldown = max(COOLDOWN_SECONDS, params.entry_cooldown_sec)
    last_entry_ts: Dict[Tuple[str, str, str], float] = {}  # (slug, outcome, side) -> epoch
    decisions: List[BotDecision] = []

    for ms in states:
        decision, _gate = strategy.evaluate_market_state(ms)

        if decision.action in ("BUY", "SELL"):
            key = (ms.slug, ms.outcome, decision.action)
            prev = last_entry_ts.get(key, 0.0)
            if ms.timestamp - prev < cooldown:
                decision = Strategy._no_action(ms, "cooldown_active")
            else:
                last_entry_ts[key] = ms.timestamp

        decisions.append(decision)

    return decisions


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2b -- BOT ENTRY DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════════════════

def diagnose_no_entries(
    states: List[MarketState],
    wallet_trades: pd.DataFrame,
    params: StrategyParams,
    tolerance_sec: float = 10.0,
) -> None:
    """Explain WHY the bot produced no/few entries.

    Prints:
      - Snapshot counts per asset
      - Gate failure breakdown (which gate blocked the most)
      - First 20 wallet trades' market conditions at trade time + which gate fails
    """
    print(f"\n  {'═' * 70}")
    print("  BOT ENTRY DIAGNOSTICS -- Why did the bot produce no entries?")
    print(f"  {'═' * 70}")

    # --- 1) Snapshot counts per asset ---
    asset_counts: Dict[str, int] = defaultdict(int)
    for ms in states:
        asset_counts[ms.asset] += 1
    print(f"\n  Snapshots per asset:")
    for asset in sorted(asset_counts):
        print(f"    {asset:>4}: {asset_counts[asset]:>8,}")
    print(f"    {'TOTAL':>4}: {len(states):>8,}")

    # --- 2) Gate failure breakdown across ALL snapshots ---
    strategy = Strategy(params)
    gate_fail_counts: Dict[str, int] = defaultdict(int)
    gate_pass_count = 0
    for ms in states:
        _decision, gate = strategy.evaluate_market_state(ms)
        if gate:
            gate_fail_counts[gate + "_gate"] += 1
        else:
            gate_pass_count += 1

    all_gates = ["spread_cents_gate", "spread_pctl_gate", "ret_30s_gate", "imbalance_gate",
                 "spread_pctl_delta_gate", "ret_accel_gate"]
    print(f"\n  Gate failure breakdown (sequential, first-fail):")
    print(f"    {'Gate':<25} {'Blocked':>10}  {'Pct':>7}")
    total_snap = len(states)
    for gate in all_gates:
        cnt = gate_fail_counts.get(gate, 0)
        if cnt == 0:
            continue
        pct = 100.0 * cnt / total_snap if total_snap else 0
        print(f"    {gate:<25} {cnt:>10,}  {pct:>6.1f}%")
    pct_pass = 100.0 * gate_pass_count / total_snap if total_snap else 0
    print(f"    {'ALL_GATES_PASSED':<25} {gate_pass_count:>10,}  {pct_pass:>6.1f}%")

    # --- 2b) Gate failures per asset ---
    asset_gate_fails: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    asset_gate_pass: Dict[str, int] = defaultdict(int)
    for ms in states:
        a = ms.asset
        _decision, gate = strategy.evaluate_market_state(ms)
        if gate:
            asset_gate_fails[a][gate + "_gate"] += 1
        else:
            asset_gate_pass[a] += 1

    print(f"\n  Gate failures per asset:")
    for asset in sorted(set(list(asset_gate_fails.keys()) + list(asset_gate_pass.keys()))):
        total_a = asset_counts.get(asset, 0)
        passed = asset_gate_pass.get(asset, 0)
        print(f"    {asset}: total={total_a}, passed={passed}", end="")
        fails = asset_gate_fails.get(asset, {})
        if fails:
            parts = [f"{g}={c}" for g, c in sorted(fails.items(), key=lambda x: -x[1])]
            print(f", blocked: {', '.join(parts)}")
        else:
            print()

    # --- 3) Current param thresholds vs units ---
    print(f"\n  Current strategy thresholds:")
    print(f"    entry_min_spread_cents  = {params.entry_min_spread_cents}  (cents)")
    print(f"    entry_min_spread_pctl   = {params.entry_min_spread_pctl}  (0-1 fractional)")
    print(f"    entry_min_ret_30s       = {params.entry_min_ret_30s}  (decimal, 0.001 = 0.1%)")
    print(f"    entry_min_imbalance     = {params.entry_min_imbalance}  (0-1 fractional)")

    # --- 3b) Verify units: check if ret_30s values look like percent vs decimal ---
    ret_vals = [abs(ms.binance_ret_30s) for ms in states if ms.binance_ret_30s != 0.0]
    if ret_vals:
        med_ret = float(np.median(ret_vals))
        max_ret = float(np.max(ret_vals))
        print(f"\n  binance_ret_30s unit check:")
        print(f"    median(|ret_30s|) = {med_ret:.6f}")
        print(f"    max(|ret_30s|)    = {max_ret:.6f}")
        if med_ret > 0.1:
            print(f"    WARNING: median > 0.1 suggests PERCENT units, but thresholds are DECIMAL.")
            print(f"    If data is in percent, divide by 100 or multiply thresholds by 100.")
        else:
            print(f"    OK: values look like decimal (0.001 = 0.1%). Thresholds are consistent.")

    # --- 4) First 20 wallet trades: show conditions + which gate fails ---
    if wallet_trades.empty or "ts" not in wallet_trades.columns:
        print(f"\n  No wallet trades to analyze.")
        return

    wt = wallet_trades.sort_values("ts").head(20)
    # Build a time-sorted index of states for quick lookup
    state_ts = np.array([ms.timestamp for ms in states])
    state_by_idx = states

    print(f"\n  First {len(wt)} wallet trades -- market conditions & gate analysis:")
    print(f"  {'─' * 100}")
    print(f"  {'#':>3} {'Asset':>5} {'Side':>4} {'spread_c':>9} {'spd_pctl':>9} "
          f"{'|ret_30s|':>10} {'imbal':>7} {'Gate Result':<30}")
    print(f"  {'─' * 100}")

    for i, (_, row) in enumerate(wt.iterrows()):
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        w_slug = _resolve_wallet_slug(row)
        w_asset = _resolve_wallet_asset(row)

        # Find nearest market state snapshot
        idx = np.searchsorted(state_ts, float(wts))
        best_ms = None
        best_dt = float("inf")
        for si in range(max(0, idx - 5), min(len(states), idx + 5)):
            dt = abs(state_ts[si] - float(wts))
            # Also require same slug for relevance
            if dt < best_dt and states[si].slug.casefold() == w_slug:
                best_dt = dt
                best_ms = states[si]

        if best_ms is None:
            # Try without slug filter
            for si in range(max(0, idx - 5), min(len(states), idx + 5)):
                dt = abs(state_ts[si] - float(wts))
                if dt < best_dt:
                    best_dt = dt
                    best_ms = states[si]

        if best_ms is None or best_dt > tolerance_sec:
            print(f"  {i+1:>3} {w_asset:>5} {w_side:>4}   (no snapshot within {tolerance_sec}s)")
            continue

        ms = best_ms
        abs_ret = abs(ms.binance_ret_30s)

        # Determine which gate fails using the strategy evaluator
        _decision, gate = strategy.evaluate_market_state(ms)
        if gate:
            gate_result = f"FAIL: {gate}"
        else:
            gate_result = "PASS"

        print(f"  {i+1:>3} {w_asset:>5} {w_side:>4} {ms.spread_cents:>9.1f} {ms.spread_percentile:>9.3f} "
              f"{abs_ret:>10.6f} {ms.orderbook_imbalance:>7.3f} {gate_result}")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 -- WALLET COMPARISON
# ═══════════════════════════════════════════════════════════════════════════

def _normalize_outcome(outcome: str) -> str:
    """Normalise outcome string: casefold + map Yes/No <-> Up/Down.

    Polymarket crypto markets may label outcomes as Yes/No while the bot
    uses Up/Down (or vice-versa).  Canonicalise to Yes/No.
    """
    s = outcome.strip().casefold()
    mapping = {
        "up": "yes",
        "down": "no",
        "yes": "yes",
        "no": "no",
    }
    return mapping.get(s, s)


def _resolve_wallet_side(row: pd.Series) -> str:
    """Normalise wallet trade side to BUY / SELL."""
    for col in ("side", "action", "direction"):
        if col in row.index and pd.notna(row[col]):
            return str(row[col]).strip().upper()
    return ""


def _resolve_wallet_asset(row: pd.Series) -> str:
    """Normalise wallet trade asset, preferring slug-derived asset."""
    # Prefer deriving asset from slug to stay consistent with bot keys
    slug = str(row.get("slug", "")) if "slug" in row.index else ""
    if slug:
        derived = asset_from_slug(slug)
        if derived:
            return derived
    for col in ("crypto", "asset"):
        if col in row.index and pd.notna(row[col]):
            return str(row[col]).strip().upper()
    return ""


def _resolve_wallet_slug(row: pd.Series) -> str:
    """Normalise wallet trade slug."""
    for col in ("slug",):
        if col in row.index and pd.notna(row[col]):
            return str(row[col]).strip().casefold()
    return ""


def _resolve_wallet_outcome(row: pd.Series) -> str:
    """Normalise wallet trade outcome."""
    for col in ("outcome",):
        if col in row.index and pd.notna(row[col]):
            return _normalize_outcome(str(row[col]))
    return ""


def _make_decision_key(d) -> tuple:
    """Build normalised matching key from a BotDecision."""
    slug_norm = d.slug.strip().casefold()
    # Always derive asset from slug to stay consistent with wallet keys
    derived_asset = asset_from_slug(slug_norm)
    asset = derived_asset if derived_asset else d.asset.strip().upper()
    return (
        asset,
        slug_norm,
        _normalize_outcome(d.outcome),
        d.action.strip().upper(),
    )


def _nearest_ts(arr: np.ndarray, target: float) -> Optional[float]:
    """Return the nearest timestamp in arr to target, or None if arr is empty."""
    if len(arr) == 0:
        return None
    idx = np.searchsorted(arr, target)
    best = None
    for i in range(max(0, idx - 1), min(len(arr), idx + 2)):
        if best is None or abs(arr[i] - target) < abs(best - target):
            best = float(arr[i])
    return best


def compare_to_wallet(
    decisions: List[BotDecision],
    wallet_trades: pd.DataFrame,
    tolerance_sec: float = 3.0,
    debug: bool = False,
    offset_sec: float = 0.0,
) -> ComparisonResult:
    """Compare bot decisions against wallet trades.

    Matching requires:
      1. Timestamp within +/- tolerance_sec
      2. Same trade direction (BUY == BUY, SELL == SELL)
      3. Same asset (BTC == BTC, etc.)
      4. Same slug (market identifier)

    For each wallet trade, the closest bot decision in time (within tolerance)
    with matching (asset, slug, side) is selected.  Lag is computed from the
    original (non-rounded) timestamps.

    offset_sec: added to wallet timestamps before matching (auto-fit alignment).
    """
    if wallet_trades.empty or not decisions:
        return ComparisonResult(0, 0, 0, 0, 0.0)

    wt = wallet_trades.copy()
    if "ts" not in wt.columns:
        return ComparisonResult(0, 0, 0, 0, 0.0)

    # --- Build bot entry index keyed by (asset, slug, outcome, side) ---
    # Using normalised 4-tuple keys to avoid slug/outcome/side mismatches
    bot_entries_by_key: Dict[Tuple[str, str, str, str], List[float]] = defaultdict(list)
    for d in decisions:
        if d.action in ("BUY", "SELL"):
            key = _make_decision_key(d)
            bot_entries_by_key[key].append(d.timestamp)

    bot_arrays: Dict[Tuple[str, str, str, str], np.ndarray] = {
        k: np.array(sorted(v)) for k, v in bot_entries_by_key.items()
    }

    # --- Build ALL bot decision timestamps for proximity check ---
    all_bot_ts = np.array(sorted(d.timestamp for d in decisions if d.action in ("BUY", "SELL")))

    # --- Build a time-sorted index of all bot entry decisions for debug dump ---
    bot_entries_sorted: List[BotDecision] = sorted(
        [d for d in decisions if d.action in ("BUY", "SELL")],
        key=lambda d: d.timestamp,
    )
    bot_entry_ts_arr = np.array([d.timestamp for d in bot_entries_sorted]) if bot_entries_sorted else np.array([])

    # --- Walk wallet trades and attempt matching ---
    total = 0
    matched = 0
    entry_lags: List[float] = []
    mismatch_reasons: Dict[str, int] = defaultdict(int)
    debug_examples: List[dict] = []
    wrong_key_examples: List[dict] = []  # detailed dump for wrong-key mismatches

    for _, row in wt.iterrows():
        wts_raw = row.get("ts")
        if pd.isna(wts_raw):
            continue
        wts = float(wts_raw) + offset_sec  # apply timestamp alignment offset

        w_side = _resolve_wallet_side(row)
        w_asset = _resolve_wallet_asset(row)
        w_slug = _resolve_wallet_slug(row)
        w_outcome = _resolve_wallet_outcome(row)
        w_token_id = str(row.get("token_id", "")) if "token_id" in row.index else ""
        if not w_side:
            continue

        total += 1
        key = (w_asset, w_slug, w_outcome, w_side)
        arr = bot_arrays.get(key)

        if arr is None or len(arr) == 0:
            # Check if ANY bot entry exists within tolerance (ignoring key)
            if len(all_bot_ts) > 0:
                idx_any = np.searchsorted(all_bot_ts, float(wts))
                nearest_lag = float("inf")
                for i in range(max(0, idx_any - 1), min(len(all_bot_ts), idx_any + 2)):
                    lag = abs(all_bot_ts[i] - float(wts))
                    if lag < nearest_lag:
                        nearest_lag = lag
                if nearest_lag <= tolerance_sec:
                    mismatch_reasons["no_bot_entry_for_key (but bot decision within tolerance)"] += 1
                    # --- Collect wrong-key debug examples ---
                    if len(wrong_key_examples) < 50:
                        # Find all bot entries within tolerance window
                        nearby_bot_decisions = []
                        lo = np.searchsorted(bot_entry_ts_arr, float(wts) - tolerance_sec)
                        hi = np.searchsorted(bot_entry_ts_arr, float(wts) + tolerance_sec, side="right")
                        for bi in range(lo, min(hi, len(bot_entries_sorted))):
                            bd = bot_entries_sorted[bi]
                            bkey = _make_decision_key(bd)
                            nearby_bot_decisions.append({
                                "snapshot_ts": bd.timestamp,
                                "bot_key": bkey,
                                "action": bd.action,
                                "asset": bd.asset,
                                "slug_raw": bd.slug,
                                "outcome_raw": bd.outcome,
                                "side_raw": bd.action,
                            })
                        wrong_key_examples.append({
                            "wallet_ts": float(wts),
                            "wallet_key": key,
                            "wallet_side": w_side,
                            "wallet_asset": w_asset,
                            "wallet_slug_raw": str(row.get("slug", "")) if "slug" in row.index else "",
                            "wallet_outcome_raw": str(row.get("outcome", "")) if "outcome" in row.index else "",
                            "wallet_token_id": w_token_id,
                            "wallet_slug_norm": w_slug,
                            "wallet_outcome_norm": w_outcome,
                            "nearby_bot_decisions": nearby_bot_decisions,
                        })
                else:
                    mismatch_reasons[f"no_bot_entry_for_key (nearest bot decision {nearest_lag:.1f}s away)"] += 1
            else:
                mismatch_reasons["no_bot_entries_at_all"] += 1

            if debug and len(debug_examples) < 5:
                debug_examples.append({
                    "wallet_ts": float(wts), "side": w_side, "asset": w_asset,
                    "slug": w_slug, "outcome": w_outcome,
                    "reason": "no_bot_entry_for_key",
                    "nearest_any_bot_ts": _nearest_ts(all_bot_ts, float(wts)),
                })
            continue

        idx = np.searchsorted(arr, float(wts))
        best_lag = None
        for i in range(max(0, idx - 1), min(len(arr), idx + 2)):
            lag = float(arr[i]) - float(wts)
            if abs(lag) <= tolerance_sec:
                if best_lag is None or abs(lag) < abs(best_lag):
                    best_lag = lag
        if best_lag is not None:
            matched += 1
            entry_lags.append(best_lag)
        else:
            # Key exists but no timestamp match
            nearest = float("inf")
            for i in range(max(0, idx - 1), min(len(arr), idx + 2)):
                d_lag = abs(float(arr[i]) - float(wts))
                if d_lag < nearest:
                    nearest = d_lag
            mismatch_reasons[f"key_match_but_outside_tolerance (nearest {nearest:.1f}s)"] += 1
            if debug and len(debug_examples) < 5:
                debug_examples.append({
                    "wallet_ts": float(wts), "side": w_side, "asset": w_asset,
                    "slug": w_slug, "outcome": w_outcome,
                    "reason": f"outside_tolerance (nearest={nearest:.1f}s)",
                    "nearest_any_bot_ts": _nearest_ts(all_bot_ts, float(wts)),
                })

    missed = total - matched

    # --- Matching diagnostics ---
    if mismatch_reasons:
        print(f"\n  {'─' * 60}")
        print("  MATCHING DIAGNOSTICS -- Top 10 mismatch reasons")
        print(f"  {'─' * 60}")
        sorted_reasons = sorted(mismatch_reasons.items(), key=lambda x: -x[1])
        for reason, count in sorted_reasons[:10]:
            print(f"    {count:>5}  {reason}")
        # Count wallet trades with no bot decision within tolerance at all
        no_nearby = sum(v for k, v in mismatch_reasons.items()
                        if "nearest bot decision" in k and "away" in k)
        nearby_but_wrong = sum(v for k, v in mismatch_reasons.items()
                               if "but bot decision within tolerance" in k)
        print(f"\n    Wallet trades with NO bot decision within {tolerance_sec}s (any key): {no_nearby}")
        print(f"    Wallet trades with bot decision within {tolerance_sec}s but wrong key:  {nearby_but_wrong}")

    # --- Wrong-key debug dump (first 50) ---
    if wrong_key_examples:
        print(f"\n  {'═' * 70}")
        print(f"  WRONG-KEY MISMATCH DEBUG DUMP  (first {len(wrong_key_examples)} of "
              f"{mismatch_reasons.get('no_bot_entry_for_key (but bot decision within tolerance)', 0)})")
        print(f"  {'═' * 70}")
        for i, ex in enumerate(wrong_key_examples):
            print(f"\n  ── Mismatch #{i + 1} ──")
            print(f"  WALLET:")
            print(f"    trade_ts       = {ex['wallet_ts']:.3f}")
            print(f"    slug (raw)     = {ex['wallet_slug_raw']!r}")
            print(f"    outcome (raw)  = {ex['wallet_outcome_raw']!r}")
            print(f"    side           = {ex['wallet_side']!r}")
            print(f"    token_id       = {ex['wallet_token_id']!r}")
            print(f"    normalized_wallet_key = {ex['wallet_key']}")
            print(f"  NEAREST BOT ENTRIES (within {tolerance_sec}s):")
            if not ex["nearby_bot_decisions"]:
                print(f"    (none found)")
            for bd in ex["nearby_bot_decisions"]:
                delta = bd["snapshot_ts"] - ex["wallet_ts"]
                print(f"    snapshot_ts    = {bd['snapshot_ts']:.3f}  (delta={delta:+.3f}s)")
                print(f"      slug (raw)   = {bd['slug_raw']!r}")
                print(f"      outcome (raw)= {bd['outcome_raw']!r}")
                print(f"      action       = {bd['action']!r}")
                print(f"      normalized_bot_key = {bd['bot_key']}")
                # Show per-field comparison
                wk = ex["wallet_key"]
                bk = bd["bot_key"]
                diffs = []
                labels = ("asset", "slug", "outcome", "side")
                for li, lb in enumerate(labels):
                    if wk[li] != bk[li]:
                        diffs.append(f"{lb}: wallet={wk[li]!r} vs bot={bk[li]!r}")
                if diffs:
                    print(f"      KEY DIFFS: {'; '.join(diffs)}")
                else:
                    print(f"      KEY DIFFS: (none -- keys match, possible timestamp issue)")

    if debug and debug_examples:
        print(f"\n  {'─' * 60}")
        print("  DEBUG -- Example unmatched wallet trades")
        print(f"  {'─' * 60}")
        for ex in debug_examples:
            nearest_str = f"{ex['nearest_any_bot_ts']:.3f}" if ex['nearest_any_bot_ts'] is not None else "N/A"
            lag_str = f"{abs(ex['nearest_any_bot_ts'] - ex['wallet_ts']):.1f}s" if ex['nearest_any_bot_ts'] is not None else "N/A"
            print(f"    wallet_ts={ex['wallet_ts']:.3f}  {ex['side']:>4} {ex['asset']:>4} slug={ex['slug']} outcome={ex['outcome']}")
            print(f"      reason: {ex['reason']}  |  nearest_any_bot_ts={nearest_str} ({lag_str} away)")

    # --- Print all unique bot entry keys for cross-reference ---
    if wrong_key_examples:
        print(f"\n  {'─' * 60}")
        print("  ALL UNIQUE BOT ENTRY KEYS (normalised)")
        print(f"  {'─' * 60}")
        unique_bot_keys = sorted(set(bot_entries_by_key.keys()))
        for bk in unique_bot_keys[:50]:
            print(f"    {bk}  ({len(bot_entries_by_key[bk])} entries)")
        if len(unique_bot_keys) > 50:
            print(f"    ... and {len(unique_bot_keys) - 50} more")
        print(f"\n  ALL UNIQUE WALLET KEYS (normalised, from mismatched trades)")
        unique_wallet_keys = sorted(set(ex["wallet_key"] for ex in wrong_key_examples))
        for wk in unique_wallet_keys[:50]:
            print(f"    {wk}")
        if len(unique_wallet_keys) > 50:
            print(f"    ... and {len(unique_wallet_keys) - 50} more")

    # --- False entries: bot entries with no matching wallet trade ---
    wallet_by_key: Dict[Tuple[str, str, str, str], List[float]] = defaultdict(list)
    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        w_asset = _resolve_wallet_asset(row)
        w_slug = _resolve_wallet_slug(row)
        w_outcome = _resolve_wallet_outcome(row)
        if w_side:
            wallet_by_key[(w_asset, w_slug, w_outcome, w_side)].append(float(wts))

    wallet_arrays: Dict[Tuple[str, str, str, str], np.ndarray] = {
        k: np.array(sorted(v)) for k, v in wallet_by_key.items()
    }

    bot_false = 0
    for d in decisions:
        if d.action not in ("BUY", "SELL"):
            continue
        key = _make_decision_key(d)
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

    # --- Latency stats (ms precision) ---
    lag_median = float(np.median(entry_lags)) if entry_lags else float("nan")
    lag_p90 = float(np.percentile(entry_lags, 90)) if entry_lags else float("nan")

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
# SECTION 3b -- DEBUG DUMPS: MISSED WALLET TRADES & FALSE BOT ENTRIES
# ═══════════════════════════════════════════════════════════════════════════

def dump_missed_wallet_trades(
    states: List[MarketState],
    wallet_trades: pd.DataFrame,
    decisions: List[BotDecision],
    params: StrategyParams,
    tolerance_sec: float = 3.0,
    max_rows: int = 50,
) -> None:
    """Print top N wallet trades that the bot MISSED (no matching bot entry).

    For each missed trade, show: ts, slug, outcome, side, spread_cents, pctl,
    imbalance, ret_30s at trade time, and which gate failed (or if bot didn't
    evaluate that slug).
    """
    if wallet_trades.empty or "ts" not in wallet_trades.columns or not decisions:
        return

    strategy = Strategy(params)

    # Build bot entries index
    bot_entries_by_key: Dict[Tuple[str, str, str, str], np.ndarray] = {}
    tmp: Dict[Tuple[str, str, str, str], List[float]] = defaultdict(list)
    for d in decisions:
        if d.action in ("BUY", "SELL"):
            key = _make_decision_key(d)
            tmp[key].append(d.timestamp)
    bot_entries_by_key = {k: np.array(sorted(v)) for k, v in tmp.items()}

    # Build state lookup
    state_ts = np.array([ms.timestamp for ms in states])
    # Index states by (slug_casefold, outcome)
    state_by_slug: Dict[str, List[int]] = defaultdict(list)
    for si, ms in enumerate(states):
        state_by_slug[ms.slug.casefold()].append(si)

    wt = wallet_trades.sort_values("ts")

    print(f"\n  {'═' * 110}")
    print(f"  MISSED WALLET TRADES (top {max_rows})")
    print(f"  {'═' * 110}")
    print(f"  {'#':>3} {'ts':>14} {'Asset':>5} {'Side':>4} {'Slug':<35} {'Outcome':>7} "
          f"{'spd_c':>6} {'pctl':>6} {'imbal':>6} {'|ret30|':>9} {'Gate Failed':<25}")
    print(f"  {'─' * 110}")

    missed_count = 0
    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        w_asset = _resolve_wallet_asset(row)
        w_slug = _resolve_wallet_slug(row)
        w_outcome = _resolve_wallet_outcome(row)
        if not w_side:
            continue

        key = (w_asset, w_slug, w_outcome, w_side)
        arr = bot_entries_by_key.get(key)
        is_matched = False
        if arr is not None and len(arr) > 0:
            idx = np.searchsorted(arr, float(wts))
            for i in range(max(0, idx - 1), min(len(arr), idx + 2)):
                if abs(arr[i] - float(wts)) <= tolerance_sec:
                    is_matched = True
                    break
        if is_matched:
            continue

        missed_count += 1
        if missed_count > max_rows:
            break

        # Find nearest market state for this slug
        best_ms = None
        best_dt = float("inf")
        slug_indices = state_by_slug.get(w_slug, [])
        if slug_indices:
            # Binary search approx
            idx = np.searchsorted(state_ts, float(wts))
            for si in range(max(0, idx - 20), min(len(states), idx + 20)):
                if states[si].slug.casefold() != w_slug:
                    continue
                dt = abs(states[si].timestamp - float(wts))
                if dt < best_dt:
                    best_dt = dt
                    best_ms = states[si]

        if best_ms is not None and best_dt <= 30.0:
            _dec, gate = strategy.evaluate_market_state(best_ms)
            gate_str = gate if gate else "PASS (cooldown/key mismatch?)"
            print(f"  {missed_count:>3} {float(wts):>14.1f} {w_asset:>5} {w_side:>4} "
                  f"{w_slug:<35.35} {w_outcome:>7} "
                  f"{best_ms.spread_cents:>6.1f} {best_ms.spread_percentile:>6.3f} "
                  f"{best_ms.orderbook_imbalance:>6.3f} {abs(best_ms.binance_ret_30s):>9.6f} "
                  f"{gate_str}")
        else:
            print(f"  {missed_count:>3} {float(wts):>14.1f} {w_asset:>5} {w_side:>4} "
                  f"{w_slug:<35.35} {w_outcome:>7} "
                  f"{'--':>6} {'--':>6} {'--':>6} {'--':>9} "
                  f"no_snapshot_within_30s")

    total_missed = 0
    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        w_asset = _resolve_wallet_asset(row)
        w_slug = _resolve_wallet_slug(row)
        w_outcome = _resolve_wallet_outcome(row)
        if not w_side:
            continue
        key = (w_asset, w_slug, w_outcome, w_side)
        arr = bot_entries_by_key.get(key)
        is_matched = False
        if arr is not None and len(arr) > 0:
            idx = np.searchsorted(arr, float(wts))
            for i in range(max(0, idx - 1), min(len(arr), idx + 2)):
                if abs(arr[i] - float(wts)) <= tolerance_sec:
                    is_matched = True
                    break
        if not is_matched:
            total_missed += 1
    print(f"\n  Total missed: {total_missed}")


def dump_false_bot_entries(
    states: List[MarketState],
    wallet_trades: pd.DataFrame,
    decisions: List[BotDecision],
    tolerance_sec: float = 3.0,
    max_rows: int = 50,
) -> None:
    """Print top N bot entries that had NO matching wallet trade (false entries).

    For each false entry, show: ts, slug, outcome, side, features at entry time,
    plus nearest wallet trade within wider tolerance and delta seconds.
    """
    if wallet_trades.empty or "ts" not in wallet_trades.columns or not decisions:
        return

    # Build wallet trade index
    wallet_by_key: Dict[Tuple[str, str, str, str], List[float]] = defaultdict(list)
    wt = wallet_trades.copy()
    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        w_asset = _resolve_wallet_asset(row)
        w_slug = _resolve_wallet_slug(row)
        w_outcome = _resolve_wallet_outcome(row)
        if w_side:
            wallet_by_key[(w_asset, w_slug, w_outcome, w_side)].append(float(wts))
    wallet_arrays: Dict[Tuple[str, str, str, str], np.ndarray] = {
        k: np.array(sorted(v)) for k, v in wallet_by_key.items()
    }
    # Also build a flat array of all wallet trade timestamps for nearest search
    all_wallet_ts = np.array(sorted(
        float(row.get("ts")) for _, row in wt.iterrows()
        if not pd.isna(row.get("ts"))
    )) if not wt.empty else np.array([])

    print(f"\n  {'═' * 130}")
    print(f"  FALSE BOT ENTRIES (top {max_rows}) -- bot entry with no matching wallet trade")
    print(f"  {'═' * 130}")
    print(f"  {'#':>3} {'ts':>14} {'Asset':>5} {'Side':>4} {'Slug':<35} {'Outcome':>7} "
          f"{'spd_c':>6} {'pctl':>6} {'imbal':>6} {'ret30':>9} "
          f"{'nearest_w':>10} {'delta_s':>8}")
    print(f"  {'─' * 130}")

    false_count = 0
    total_false = 0
    for d in decisions:
        if d.action not in ("BUY", "SELL"):
            continue
        key = _make_decision_key(d)
        warr = wallet_arrays.get(key)
        is_matched = False
        if warr is not None and len(warr) > 0:
            idx = np.searchsorted(warr, d.timestamp)
            for i in range(max(0, idx - 1), min(len(warr), idx + 2)):
                if abs(warr[i] - d.timestamp) <= tolerance_sec:
                    is_matched = True
                    break
        if is_matched:
            continue

        total_false += 1
        if total_false > max_rows:
            continue  # keep counting

        false_count += 1

        # Find nearest wallet trade (any key) for context
        nearest_wt_str = "--"
        delta_str = "--"
        if len(all_wallet_ts) > 0:
            idx = np.searchsorted(all_wallet_ts, d.timestamp)
            best_delta = float("inf")
            for i in range(max(0, idx - 1), min(len(all_wallet_ts), idx + 2)):
                dt = all_wallet_ts[i] - d.timestamp
                if abs(dt) < abs(best_delta):
                    best_delta = dt
            if abs(best_delta) < 300:
                nearest_wt_str = f"{best_delta:+.1f}s"
                delta_str = f"{abs(best_delta):.1f}"

        print(f"  {false_count:>3} {d.timestamp:>14.1f} {d.asset:>5} {d.action:>4} "
              f"{d.slug:<35.35} {d.outcome:>7} "
              f"{d.spread_cents:>6.1f} {d.spread_percentile:>6.3f} "
              f"{d.orderbook_imbalance:>6.3f} {d.binance_ret_30s:>9.6f} "
              f"{nearest_wt_str:>10} {delta_str:>8}")

    print(f"\n  Total false bot entries: {total_false}")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3c -- TIMESTAMP LAG INVESTIGATION & AUTO-FIT OFFSET
# ═══════════════════════════════════════════════════════════════════════════

def auto_fit_offset(
    states: List[MarketState],
    wallet_trades: pd.DataFrame,
    params: StrategyParams,
    tolerance_sec: float = 10.0,
) -> float:
    """Auto-fit a timestamp OFFSET_SEC that minimizes median |lag| on matched trades.

    Tests offsets from -10s to +10s in 0.5s increments.  Returns the best offset.
    The offset is applied as: adjusted_wallet_ts = wallet_ts + offset.
    """
    if wallet_trades.empty or "ts" not in wallet_trades.columns or not states:
        return 0.0

    decisions = run_replay(states, params)
    bot_entries: List[BotDecision] = [d for d in decisions if d.action in ("BUY", "SELL")]
    if not bot_entries:
        return 0.0

    # Build bot entries by key
    bot_by_key: Dict[Tuple[str, str, str, str], np.ndarray] = {}
    tmp: Dict[Tuple[str, str, str, str], List[float]] = defaultdict(list)
    for d in bot_entries:
        key = _make_decision_key(d)
        tmp[key].append(d.timestamp)
    bot_by_key = {k: np.array(sorted(v)) for k, v in tmp.items()}

    wt = wallet_trades.copy()

    best_offset = 0.0
    best_median_abs_lag = float("inf")

    offsets = [x * 0.5 for x in range(-20, 21)]  # -10s to +10s
    for offset in offsets:
        lags = []
        for _, row in wt.iterrows():
            wts = row.get("ts")
            if pd.isna(wts):
                continue
            adjusted_ts = float(wts) + offset
            w_side = _resolve_wallet_side(row)
            w_asset = _resolve_wallet_asset(row)
            w_slug = _resolve_wallet_slug(row)
            w_outcome = _resolve_wallet_outcome(row)
            if not w_side:
                continue
            key = (w_asset, w_slug, w_outcome, w_side)
            arr = bot_by_key.get(key)
            if arr is None or len(arr) == 0:
                continue
            idx = np.searchsorted(arr, adjusted_ts)
            best_lag = None
            for i in range(max(0, idx - 1), min(len(arr), idx + 2)):
                lag = float(arr[i]) - adjusted_ts
                if abs(lag) <= tolerance_sec:
                    if best_lag is None or abs(lag) < abs(best_lag):
                        best_lag = lag
            if best_lag is not None:
                lags.append(best_lag)
        if lags:
            med = float(np.median([abs(l) for l in lags]))
            if med < best_median_abs_lag:
                best_median_abs_lag = med
                best_offset = offset

    return best_offset


def print_lag_investigation(
    states: List[MarketState],
    wallet_trades: pd.DataFrame,
    params: StrategyParams,
    tolerance_sec: float = 10.0,
) -> float:
    """Investigate timestamp lag and print findings. Returns recommended offset."""
    print(f"\n  {'═' * 70}")
    print("  TIMESTAMP LAG INVESTIGATION")
    print(f"  {'═' * 70}")

    if wallet_trades.empty or "ts" not in wallet_trades.columns:
        print("  No wallet trades to analyze.")
        return 0.0

    # Check unit consistency
    wt_ts = wallet_trades["ts"].dropna()
    state_ts_vals = [ms.timestamp for ms in states[:100]] if states else []
    if not wt_ts.empty and state_ts_vals:
        wt_sample = wt_ts.iloc[0]
        st_sample = state_ts_vals[0]
        print(f"\n  Clock/units check:")
        print(f"    First wallet_trade ts: {wt_sample:.3f}")
        print(f"    First snapshot ts:     {st_sample:.3f}")
        if abs(wt_sample - st_sample) > 1e9:
            print(f"    WARNING: timestamps differ by >1e9 -- likely different units (ms vs s)!")
        else:
            print(f"    OK: timestamps appear to be in same units (epoch seconds).")

    offset = auto_fit_offset(states, wallet_trades, params, tolerance_sec)
    print(f"\n  Auto-fit offset: {offset:+.1f}s")
    print(f"    Interpretation: wallet_ts + {offset:+.1f}s best aligns with bot snapshot_ts")
    if abs(offset) > 1.0:
        print(f"    RECOMMENDATION: Apply offset={offset:+.1f}s to wallet timestamps during matching.")
    else:
        print(f"    No significant systematic lag detected.")

    return offset


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
        "orderbook_imbalance_at_trade": ["orderbook_imbalance_at_trade", "imbalance_topN"],
        "binance_ret_5s_at_trade (dec)": ["binance_ret_5s_at_trade", "ret_5s"],
        "binance_ret_30s_at_trade (dec)": ["binance_ret_30s_at_trade", "ret_30s"],
        "binance_ret_60s_at_trade (dec)": ["binance_ret_60s_at_trade", "ret_60s"],
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

        # Recompute imbalance as bid/(bid+ask) from raw depths if available
        if "imbalance" in label and col == "imbalance_topN":
            if "bid_depth_topN" in wallet_trades.columns and "ask_depth_topN" in wallet_trades.columns:
                bid_d = pd.to_numeric(wallet_trades["bid_depth_topN"], errors="coerce").clip(lower=0).fillna(0)
                ask_d = pd.to_numeric(wallet_trades["ask_depth_topN"], errors="coerce").clip(lower=0).fillna(0)
                denom = (bid_d + ask_d).replace(0, np.nan)
                vals = (bid_d / denom).dropna()

        # Convert percent returns to decimal if values look like percents
        # (abs median > 0.1 means likely percent-scaled)
        if "ret_" in label and not vals.empty:
            if vals.abs().median() > 0.1:
                vals = vals / 100.0

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
    "entry_min_spread_cents": [0.0, 1.0, 1.5, 2.0],
    "entry_min_ret_30s": np.arange(0.0005, 0.0035, 0.0005),
    "entry_min_imbalance": [0.45, 0.50, 0.55, 0.60, 0.65],
    "spread_pctl_delta_min": [0.0, 0.02, 0.05, 0.10],
    "ret_accel_min": [0.0, 0.0002, 0.0005, 0.001],
    "entry_cooldown_sec": [0.0, 30.0, 60.0],
}


def parameter_sweep(
    states: List[MarketState],
    wallet_trades: pd.DataFrame,
    sweep_ranges: Optional[Dict] = None,
    tolerance_sec: float = 3.0,
    offset_sec: float = 0.0,
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
        cr = compare_to_wallet(decisions, wallet_trades, tolerance_sec, offset_sec=offset_sec)

        entry = {k: round(float(v), 6) for k, v in zip(keys, combo)}
        entry["similarity"] = round(cr.similarity_score, 4)
        entry["matched"] = cr.wallet_trades_matched
        entry["missed"] = cr.wallet_trades_missed
        entry["false_entries"] = cr.bot_false_entries
        entry["lag_median_ms"] = round(cr.entry_lag_median * 1000, 1) if not np.isnan(cr.entry_lag_median) else "N/A"
        entry["lag_p90_ms"] = round(cr.entry_lag_p90 * 1000, 1) if not np.isnan(cr.entry_lag_p90) else "N/A"
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
    if comparison.wallet_trades_matched == 0 or np.isnan(comparison.entry_lag_median):
        print(f"    median:  N/A  (no matched trades)")
        print(f"    p90:     N/A")
    else:
        print(f"    median:  {comparison.entry_lag_median * 1000:+.1f}ms  ({comparison.entry_lag_median:+.4f}s)")
        print(f"    p90:     {comparison.entry_lag_p90 * 1000:+.1f}ms  ({comparison.entry_lag_p90:+.4f}s)")

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

    # --- Summary: baseline vs best sweep ---
    print(f"\n{'═' * 70}")
    print("  FINAL SUMMARY")
    print(f"{'═' * 70}")
    print(f"\n  BASELINE (default params):")
    print(f"    similarity  = {comparison.similarity_score:.4f}")
    print(f"    matched     = {comparison.wallet_trades_matched}")
    print(f"    missed      = {comparison.wallet_trades_missed}")
    print(f"    false       = {comparison.bot_false_entries}")
    if not np.isnan(comparison.entry_lag_median):
        print(f"    lag median  = {comparison.entry_lag_median * 1000:+.1f}ms")

    if sweep_results is not None and not sweep_results.empty:
        best = sweep_results.iloc[0]
        print(f"\n  BEST SWEEP:")
        print(f"    similarity  = {best['similarity']:.4f}")
        print(f"    matched     = {int(best['matched'])}")
        print(f"    missed      = {int(best['missed'])}")
        print(f"    false       = {int(best['false_entries'])}")
        # Print the parameter values
        param_cols = [c for c in sweep_results.columns
                      if c not in ("similarity", "matched", "missed", "false_entries",
                                   "lag_median_ms", "lag_p90_ms")]
        params_str = ", ".join(f"{c}={best[c]}" for c in param_cols)
        print(f"    params      = {params_str}")
        if best.get("lag_median_ms") != "N/A":
            print(f"    lag median  = {best['lag_median_ms']}ms")
    else:
        print(f"\n  BEST SWEEP: (not run -- use --optimize)")

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
        "imbalance": (["orderbook_imbalance_at_trade", "imbalance_topN"], "Imbalance bid/(bid+ask) at Wallet Entry"),
        "momentum": (["binance_ret_30s_at_trade", "ret_30s"], "Binance ret_30s (decimal) at Wallet Entry"),
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
            # Recompute imbalance from raw depths
            if "imbalance" in fname and col == "imbalance_topN":
                if "bid_depth_topN" in wallet_trades.columns and "ask_depth_topN" in wallet_trades.columns:
                    bid_d = pd.to_numeric(wallet_trades["bid_depth_topN"], errors="coerce").clip(lower=0).fillna(0)
                    ask_d = pd.to_numeric(wallet_trades["ask_depth_topN"], errors="coerce").clip(lower=0).fillna(0)
                    denom = (bid_d + ask_d).replace(0, np.nan)
                    vals = (bid_d / denom).dropna()
            # Convert percent returns to decimal
            if "momentum" in fname and not vals.empty and vals.abs().median() > 0.1:
                vals = vals / 100.0

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
    parser.add_argument("--tolerance-sec", type=float, default=3.0,
                        help="Matching tolerance in seconds (default: 3.0)")
    parser.add_argument("--debug", action="store_true",
                        help="Print debug examples of unmatched wallet trades")
    parser.add_argument("--offset-sec", type=float, default=None,
                        help="Manual timestamp offset (auto-fit if not provided)")
    args = parser.parse_args()

    data_dir = args.data_dir or find_data_dir()
    print(f"\n  Data directory: {data_dir}")

    # --- Step 1: Load datasets ---
    print("\n[1/8] Loading datasets ...")
    engine = MarketReplayEngine(data_dir)
    engine.load()

    # --- Step 2: Replay market conditions ---
    print("\n[2/8] Generating market state snapshots ...")
    states = engine.generate_market_states()
    if not states:
        print("  No market states generated. Check your data files.")
        sys.exit(1)

    # --- Step 3: Run bot strategy with default (best-sweep) params ---
    print("\n[3/8] Running bot strategy replay (baseline = best-sweep params) ...")
    default_params = StrategyParams()
    print(f"  Baseline params: spread_pctl={default_params.entry_min_spread_pctl}, "
          f"spread_cents={default_params.entry_min_spread_cents}, "
          f"ret_30s={default_params.entry_min_ret_30s}, "
          f"imbalance={default_params.entry_min_imbalance}")
    decisions = run_replay(states, default_params)
    entry_count = sum(1 for d in decisions if d.action in ("BUY", "SELL"))
    print(f"  Total decisions: {len(decisions):,}  |  Entries: {entry_count:,}")

    # --- Step 3b: Diagnose why bot may produce no entries ---
    print("\n[3b/8] Diagnosing bot entry gate failures ...")
    diagnose_no_entries(states, engine.wallet_trades, default_params, tolerance_sec=args.tolerance_sec)

    # --- Step 4: Investigate timestamp lag ---
    print("\n[4/8] Investigating timestamp lag ...")
    if args.offset_sec is not None:
        offset_sec = args.offset_sec
        print(f"  Using manual offset: {offset_sec:+.1f}s")
    else:
        offset_sec = print_lag_investigation(
            states, engine.wallet_trades, default_params,
            tolerance_sec=max(args.tolerance_sec, 10.0),
        )

    # --- Step 5: Compare to wallet trades (with offset) ---
    print("\n[5/8] Comparing to wallet trades ...")
    comparison = compare_to_wallet(
        decisions, engine.wallet_trades, args.tolerance_sec,
        debug=args.debug, offset_sec=offset_sec,
    )
    print(f"  Similarity score: {comparison.similarity_score:.4f}")
    if comparison.wallet_trades_matched == 0 or np.isnan(comparison.entry_lag_median):
        print(f"  Entry lag median: N/A  |  p90: N/A  (no matched trades)")
    else:
        print(f"  Entry lag median: {comparison.entry_lag_median * 1000:+.1f}ms  |  p90: {comparison.entry_lag_p90 * 1000:+.1f}ms")
    if offset_sec != 0:
        print(f"  (with offset={offset_sec:+.1f}s applied to wallet timestamps)")

    # --- Step 5b: Debug dumps ---
    print("\n[5b/8] Generating debug dumps ...")
    dump_missed_wallet_trades(
        states, engine.wallet_trades, decisions, default_params,
        tolerance_sec=args.tolerance_sec, max_rows=50,
    )
    dump_false_bot_entries(
        states, engine.wallet_trades, decisions,
        tolerance_sec=args.tolerance_sec, max_rows=50,
    )

    # --- Step 6: Signal distribution analysis ---
    print("\n[6/8] Analyzing signal distributions at wallet entry ...")
    signal_dist = analyze_signal_distributions(engine.wallet_trades)

    # --- Step 7: Parameter optimization (optional) ---
    sweep_results = None
    if args.optimize:
        print("\n[7/8] Running parameter sweep optimization ...")
        sweep_results = parameter_sweep(
            states, engine.wallet_trades,
            tolerance_sec=args.tolerance_sec, offset_sec=offset_sec,
        )
    else:
        print("\n[7/8] Skipping parameter sweep (use --optimize to enable)")

    # --- Step 8: Report ---
    print_report(comparison, signal_dist, sweep_results)

    # --- Charts ---
    if not args.no_charts:
        print("\n  Generating charts ...")
        generate_charts(decisions, engine.wallet_trades, signal_dist)

    print("\n  Done.\n")


if __name__ == "__main__":
    main()
