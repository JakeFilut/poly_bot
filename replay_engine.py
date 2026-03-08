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
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

_ET = ZoneInfo("America/New_York")

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
    # Timing-inside-window features
    sec_from_quarter_et: int = 0
    sec_from_hour_et: int = 0
    seconds_to_resolution: int = 900
    hour_et: int = 0              # hour of day in ET (0-23) for quiet-hour gate
    # Market type (derived from slug)
    market_type: str = ""     # "hourly" or "15m" or ""
    # --- v3: Extended market data ---
    binance_ret_300s: float = 0.0   # 5-minute return
    binance_ret_900s: float = 0.0   # 15-minute return
    realized_vol_5m: float = 0.0    # rolling realized volatility (σ of 30s rets over 5min)
    binance_spread_bps: float = 0.0 # Binance bid-ask spread in bps
    binance_volume_24h: float = 0.0 # 24h quote volume on Binance
    bid_depth_delta_1s: float = 0.0 # book bid depth change vs 1s ago
    ask_depth_delta_1s: float = 0.0 # book ask depth change vs 1s ago
    imbalance_delta_1s: float = 0.0 # imbalance change vs 1s ago
    bid_depth_near_mid: float = 0.0 # cumulative bid depth within 2¢ of mid
    ask_depth_near_mid: float = 0.0 # cumulative ask depth within 2¢ of mid
    poly_binance_price: float = 0.0 # raw Binance price at this tick (for divergence)


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
    token_id: str = ""        # token_id for primary matching
    binance_ret_120s: float = 0.0
    spread_pctl_prev: float = 0.0
    accepted_reason_codes: List[str] = field(default_factory=list)
    sec_from_quarter_et: int = 0
    sec_from_hour_et: int = 0
    seconds_to_resolution: int = 900
    market_type: str = ""


@dataclass
class ComparisonResult:
    """Outcome of comparing bot decisions to wallet trades."""
    wallet_trades_total: int
    wallet_trades_matched: int
    wallet_trades_missed: int
    bot_false_entries: int
    similarity_score: float          # F1 score (harmonic mean of precision & recall)
    precision: float = 0.0           # matched / (matched + false)
    recall: float = 0.0              # matched / total_wallet_trades
    entry_lag_median: float = float("nan")  # median (bot_ts - wallet_ts) for matched trades
    entry_lag_p90: float = float("nan")    # p90 entry lag


@dataclass
class StrategyParams:
    """Tunable strategy parameters for the sweep.

    Defaults aligned to f247 wallet signal distributions (2,637 trades, 5h session).
    """
    entry_min_spread_pctl: float = 0.85      # original default; wallet p25=78%
    entry_min_spread_cents: float = 1.0     # wallet median spread=1¢
    entry_min_ret_30s: float = 0.0          # disabled: wallet median ret_30s=0.0
    entry_min_imbalance: float = 0.1        # original: light imbalance filter
    entry_max_imbalance: float = 0.70       # f247: imb>0.7 = chasing (-0.3c markout)
    imbalance_band_enabled: bool = True     # use min/max band
    # -- Directional imbalance --
    imbalance_directional: bool = False     # wallet doesn't strongly filter on directional imbalance
    # -- One-shot conditions --
    spread_pctl_delta_min: float = 0.0      # pctl_now - pctl_60s_ago >= this
    ret_accel_min: float = 0.0              # |ret_30s - ret_120s| >= this
    entry_cooldown_sec: float = 5.0         # original default
    # -- Time-of-day filter --
    quiet_hours_et: tuple = (3, 5, 7, 8)   # hours in ET to suppress entries (<16% match rate)
    # -- Time-to-resolution filter --
    entry_max_seconds_to_resolution: float = 0.0  # 0=disabled
    entry_min_seconds_to_resolution: float = 120.0  # original: skip entries >2min from resolution
    # -- 60s return filter --
    entry_min_ret_60s: float = 0.0004      # original: require 0.04% 60s return
    # -- Crossing control --
    cross_enabled: bool = False             # f247: crossing = -2.5c markout; disabled
    # -- Size cap --
    entry_max_size_shares: float = 30.0     # f247: 30-60 marginal, 60+ negative

    # -- Per-asset overrides --
    # Keys are asset names (e.g. "BTC", "ETH"), values are dicts of param overrides.
    # Example: {"BTC": {"entry_min_spread_pctl": 0.95, "entry_min_ret_60s": 0.0}}
    asset_overrides: dict = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.asset_overrides is None:
            # Per-asset overrides disabled by default — the global sweep optimum
            # (spread_pctl=0.95, cooldown=15s) outperforms per-asset tuning when
            # combined, because lower per-asset thresholds flood the system.
            # Use --per-asset-sweep to discover per-asset params, then set manually.
            self.asset_overrides = {}

    def for_asset(self, asset: str) -> "StrategyParams":
        """Return a copy of this StrategyParams with asset-specific overrides applied."""
        overrides = self.asset_overrides.get(asset)
        if not overrides:
            return self
        import copy
        p = copy.copy(self)
        for k, v in overrides.items():
            if hasattr(p, k):
                object.__setattr__(p, k, v)
        # Clear asset_overrides on the copy to avoid nested lookups
        object.__setattr__(p, 'asset_overrides', {})
        return p


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
        """Pre-compute rolling Binance returns per symbol (vectorized).

        Returns dict: symbol -> DataFrame with columns
        [ts, price, ret_5s, ret_30s, ret_60s, ret_120s, ret_300s, ret_900s,
         realized_vol_5m, binance_spread_bps, volume_24h, quote_volume_24h].
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

            ts_arr = g["ts"].values
            px_arr = g["price"].values

            # Returns at multiple horizons (vectorized)
            for lag, col, tol in [(5, "ret_5s", 1.5), (30, "ret_30s", 2.0),
                                   (60, "ret_60s", 2.0), (120, "ret_120s", 2.0),
                                   (300, "ret_300s", 5.0), (900, "ret_900s", 10.0)]:
                target_ts = ts_arr - lag
                idx = np.searchsorted(ts_arr, target_ts, side="right") - 1
                idx = np.clip(idx, 0, len(ts_arr) - 1)
                past_ts = ts_arr[idx]
                past_px = px_arr[idx]
                dt = np.abs(past_ts - target_ts)
                valid = (dt <= tol) & (past_px > 0)
                ret = np.where(valid, px_arr / past_px - 1, 0.0)
                g[col] = ret

            # Realized volatility: rolling σ of 30s log returns over 5min window
            # For each row, compute std of log(px[t]/px[t-30]) over last 300s
            log_ret_30 = np.zeros(len(g))
            target_30 = ts_arr - 30
            idx_30 = np.searchsorted(ts_arr, target_30, side="right") - 1
            idx_30 = np.clip(idx_30, 0, len(ts_arr) - 1)
            valid_30 = (np.abs(ts_arr[idx_30] - target_30) <= 2.0) & (px_arr[idx_30] > 0)
            log_ret_30 = np.where(valid_30, np.log(np.maximum(px_arr, 1e-12) / np.maximum(px_arr[idx_30], 1e-12)), 0.0)
            # Rolling std with ~10 samples (300s / 30s)
            rvol = pd.Series(log_ret_30).rolling(window=10, min_periods=3).std().fillna(0).values
            g["realized_vol_5m"] = rvol

            # Pass through existing tape columns if available
            for col_name in ["binance_spread_bps", "volume_24h", "quote_volume_24h"]:
                if col_name in g.columns:
                    g[col_name] = pd.to_numeric(g[col_name], errors="coerce").fillna(0)
                else:
                    g[col_name] = 0.0

            result[str(sym)] = g

        return result

    def generate_market_states(self) -> List[MarketState]:
        """Walk through the book tape chronologically, enriching each
        snapshot with Binance returns to produce MarketState objects.
        Vectorized for performance on large datasets."""
        if self.book_tape.empty:
            print("  No book_tape data -- cannot generate market states.")
            return []

        # Pre-compute binance returns
        binance_rets = self.build_binance_returns()

        # Prepare book_tape
        bt = self.book_tape.sort_values("ts").copy()
        bt = bt[bt["ts"].notna() & (bt["ts"] != 0)].copy()
        if bt.empty:
            print("  No valid book_tape rows after filtering.")
            return []

        # --- Vectorized column preparation ---

        # Slugs, outcomes, token_ids
        slugs = bt["slug"].fillna("").astype(str).values
        outcomes = bt["outcome"].fillna("").astype(str).values if "outcome" in bt.columns else np.full(len(bt), "")
        token_ids = bt["token_id"].fillna("").astype(str).values if "token_id" in bt.columns else np.full(len(bt), "")

        # Derive assets from slugs (vectorized via map)
        asset_col_name = None
        for c in ["crypto", "asset"]:
            if c in bt.columns:
                asset_col_name = c
                break
        assets = np.array([asset_from_slug(s) for s in slugs])
        # Fallback for empty assets
        if asset_col_name:
            fallback = bt[asset_col_name].fillna("BTC").astype(str).values
        else:
            fallback = np.full(len(bt), "BTC")
        empty_mask = assets == ""
        assets[empty_mask] = fallback[empty_mask]

        # Spread calculations (vectorized)
        best_bids = pd.to_numeric(bt.get("bestBid", pd.Series(0, index=bt.index)), errors="coerce").fillna(0).values
        best_asks = pd.to_numeric(bt.get("bestAsk", pd.Series(0, index=bt.index)), errors="coerce").fillna(0).values
        raw_spread = pd.to_numeric(bt.get("spread", pd.Series(0, index=bt.index)), errors="coerce").fillna(0).values

        valid_ba = (best_asks > 0) & (best_bids > 0)
        spread_cents = np.where(valid_ba,
                                np.round((best_asks - best_bids) * 100, 1),
                                np.where(raw_spread < 1.0, raw_spread * 100, raw_spread))

        spread_pctls = pd.to_numeric(bt.get("spread_percentile_60s", pd.Series(0, index=bt.index)), errors="coerce").fillna(0).values

        # Imbalance (vectorized)
        bid_depth = np.maximum(0.0, pd.to_numeric(bt.get("bid_depth_topN", pd.Series(0, index=bt.index)), errors="coerce").fillna(0).values)
        ask_depth = np.maximum(0.0, pd.to_numeric(bt.get("ask_depth_topN", pd.Series(0, index=bt.index)), errors="coerce").fillna(0).values)
        total_depth = bid_depth + ask_depth
        imbalances = np.where(total_depth > 0, bid_depth / np.maximum(1e-9, total_depth), 0.5)

        # Timestamps
        ts_arr = bt["ts"].values.astype(np.float64)

        # --- v3: New book tape columns (graceful fallback if missing) ---
        def _safe_col(df, col_name):
            if col_name in df.columns:
                return pd.to_numeric(df[col_name], errors="coerce").fillna(0).values
            return np.zeros(len(df))

        bid_delta_1s_arr = _safe_col(bt, "bid_depth_delta_1s")
        ask_delta_1s_arr = _safe_col(bt, "ask_depth_delta_1s")
        imb_delta_1s_arr = _safe_col(bt, "imbalance_delta_1s")
        bid_near_mid_arr = _safe_col(bt, "bid_depth_within_2c")
        ask_near_mid_arr = _safe_col(bt, "ask_depth_within_2c")
        poly_bn_price_arr = _safe_col(bt, "poly_binance_divergence_bps")

        # --- Binance return lookups (vectorized per asset) ---
        r5_arr = np.zeros(len(bt))
        r30_arr = np.zeros(len(bt))
        r60_arr = np.zeros(len(bt))
        r120_arr = np.zeros(len(bt))
        r300_arr = np.zeros(len(bt))
        r900_arr = np.zeros(len(bt))
        rvol_arr = np.zeros(len(bt))
        bn_spread_arr = np.zeros(len(bt))
        bn_vol_arr = np.zeros(len(bt))

        unique_assets = np.unique(assets)
        for ua in unique_assets:
            if not ua:
                continue
            sym_key = ua + "USDT"
            bdf = binance_rets.get(sym_key) or binance_rets.get(ua)
            if bdf is None or bdf.empty:
                continue

            mask = assets == ua
            asset_ts = ts_arr[mask]
            b_ts = bdf["ts"].values

            def _get_col(name):
                return bdf[name].values if name in bdf.columns else np.zeros(len(bdf))

            b_r5 = _get_col("ret_5s")
            b_r30 = _get_col("ret_30s")
            b_r60 = _get_col("ret_60s")
            b_r120 = _get_col("ret_120s")
            b_r300 = _get_col("ret_300s")
            b_r900 = _get_col("ret_900s")
            b_rvol = _get_col("realized_vol_5m")
            b_spread = _get_col("binance_spread_bps")
            b_vol = _get_col("quote_volume_24h")

            idx = np.searchsorted(b_ts, asset_ts, side="right") - 1
            idx = np.clip(idx, 0, len(b_ts) - 1)
            aligned_ts = b_ts[idx]
            within_tol = np.abs(aligned_ts - asset_ts) <= 10.0

            r5_arr[mask] = np.where(within_tol, b_r5[idx], 0.0)
            r30_arr[mask] = np.where(within_tol, b_r30[idx], 0.0)
            r60_arr[mask] = np.where(within_tol, b_r60[idx], 0.0)
            r120_arr[mask] = np.where(within_tol, b_r120[idx], 0.0)
            r300_arr[mask] = np.where(within_tol, b_r300[idx], 0.0)
            r900_arr[mask] = np.where(within_tol, b_r900[idx], 0.0)
            rvol_arr[mask] = np.where(within_tol, b_rvol[idx], 0.0)
            bn_spread_arr[mask] = np.where(within_tol, b_spread[idx], 0.0)
            bn_vol_arr[mask] = np.where(within_tol, b_vol[idx], 0.0)

        # --- Timing-inside-window features (vectorized) ---
        # Convert epoch seconds to ET minute/second
        ts_dt = pd.to_datetime(ts_arr, unit="s", utc=True).tz_convert(_ET)
        minutes = ts_dt.minute.values.astype(np.int32)
        seconds = ts_dt.second.values.astype(np.int32)
        total_sec = minutes * 60 + seconds
        sfq_arr = total_sec % 900
        sfh_arr = total_sec
        str_arr = 900 - sfq_arr
        hour_et_arr = ts_dt.hour.values.astype(np.int32)

        # --- Market type from slug (vectorized) ---
        slugs_lower = np.char.lower(slugs.astype(str))
        is_hourly = np.char.find(slugs_lower, "hourly") >= 0
        is_1hour = np.char.find(slugs_lower, "1-hour") >= 0
        is_15 = np.char.find(slugs_lower, "15") >= 0
        is_fifteen = np.char.find(slugs_lower, "fifteen") >= 0
        mtypes = np.where(is_hourly | is_1hour, "hourly",
                          np.where(is_15 | is_fifteen, "15m", ""))

        # --- Build MarketState list ---
        print(f"  Building {len(bt):,} MarketState objects ...")
        states: List[MarketState] = []
        for i in range(len(bt)):
            states.append(MarketState(
                timestamp=ts_arr[i],
                asset=assets[i],
                slug=slugs[i],
                outcome=outcomes[i],
                token_id=token_ids[i],
                best_bid=best_bids[i],
                best_ask=best_asks[i],
                spread_cents=float(spread_cents[i]),
                spread_percentile=float(spread_pctls[i]),
                orderbook_imbalance=float(imbalances[i]),
                binance_ret_5s=float(r5_arr[i]),
                binance_ret_30s=float(r30_arr[i]),
                binance_ret_60s=float(r60_arr[i]),
                binance_ret_120s=float(r120_arr[i]),
                sec_from_quarter_et=int(sfq_arr[i]),
                sec_from_hour_et=int(sfh_arr[i]),
                seconds_to_resolution=int(str_arr[i]),
                hour_et=int(hour_et_arr[i]),
                market_type=str(mtypes[i]),
                # v3: extended fields
                binance_ret_300s=float(r300_arr[i]),
                binance_ret_900s=float(r900_arr[i]),
                realized_vol_5m=float(rvol_arr[i]),
                binance_spread_bps=float(bn_spread_arr[i]),
                binance_volume_24h=float(bn_vol_arr[i]),
                bid_depth_delta_1s=float(bid_delta_1s_arr[i]),
                ask_depth_delta_1s=float(ask_delta_1s_arr[i]),
                imbalance_delta_1s=float(imb_delta_1s_arr[i]),
                bid_depth_near_mid=float(bid_near_mid_arr[i]),
                ask_depth_near_mid=float(ask_near_mid_arr[i]),
                poly_binance_price=float(poly_bn_price_arr[i]),
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


def _pick_direction_cascade(ms: MarketState) -> Optional[str]:
    """Pick preferred outcome ('Up' or 'Down') using momentum cascade.

    Mirrors the live strategy's _pick_buy_direction():
      ret_30s → ret_5s → ret_120s → orderbook imbalance.
    Returns None if no signal at all.
    """
    # Primary: 30s return
    if ms.binance_ret_30s > 0:
        return "Up"
    if ms.binance_ret_30s < 0:
        return "Down"

    # Fallback 1: 5s return
    if ms.binance_ret_5s > 0:
        return "Up"
    if ms.binance_ret_5s < 0:
        return "Down"

    # Fallback 2: 120s return
    if ms.binance_ret_120s > 0:
        return "Up"
    if ms.binance_ret_120s < 0:
        return "Down"

    # Fallback 3: orderbook imbalance (>0.5 means bids dominate → Up)
    if ms.orderbook_imbalance > 0.5:
        return "Up"
    if ms.orderbook_imbalance < 0.5:
        return "Down"

    # Perfectly balanced -- no signal
    return None


class Strategy:
    """Simplified strategy that mirrors the live bot's entry gates."""

    def __init__(self, params: StrategyParams):
        self.params = params

    def evaluate_market_state(self, ms: MarketState) -> Tuple[BotDecision, str]:
        """Evaluate a single market state snapshot and return (decision, gate_that_failed).

        gate_that_failed is '' if all gates pass, otherwise the name of the first failing gate.
        """
        # Apply per-asset overrides if available
        p = self.params.for_asset(ms.asset) if self.params.asset_overrides else self.params
        accepted_reasons: List[str] = []

        # --- Gate 0: quiet hours ---
        if p.quiet_hours_et and ms.hour_et in p.quiet_hours_et:
            return self._no_action(ms, f"quiet_hour(hour_et={ms.hour_et})"), "quiet_hour"

        # --- Gate checks ---
        if ms.spread_cents < p.entry_min_spread_cents:
            return self._no_action(ms, "spread_cents below threshold"), "spread_cents"
        accepted_reasons.append("spread_cents_ok")

        if ms.spread_percentile < p.entry_min_spread_pctl:
            return self._no_action(ms, "spread_pctl below threshold"), "spread_pctl"
        accepted_reasons.append("spread_pctl_ok")

        if p.entry_min_ret_30s > 0 and abs(ms.binance_ret_30s) < p.entry_min_ret_30s:
            return self._no_action(ms, "ret_30s below threshold"), "ret_30s"
        accepted_reasons.append("momentum_ok")

        # --- Directional imbalance gate ---
        if p.imbalance_directional:
            direction = _pick_direction_cascade(ms) or "Up"
            if direction == "Up" and ms.orderbook_imbalance < p.entry_min_imbalance:
                return self._no_action(ms, "imbalance_dir below threshold (Up)"), "imbalance"
            if direction == "Down" and ms.orderbook_imbalance > (1.0 - p.entry_min_imbalance):
                return self._no_action(ms, "imbalance_dir above threshold (Down)"), "imbalance"
        else:
            if ms.orderbook_imbalance < p.entry_min_imbalance:
                return self._no_action(ms, "imbalance below threshold"), "imbalance"
        # f247 data: imbalance > 0.7 = chasing (-0.3c markout at 10s)
        if p.imbalance_band_enabled and ms.orderbook_imbalance > p.entry_max_imbalance:
            return self._no_action(ms, "imbalance too high (chasing)"), "imbalance_high"
        accepted_reasons.append("imbalance_ok")

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

        # --- Gate: seconds to resolution ---
        if p.entry_max_seconds_to_resolution > 0 and ms.seconds_to_resolution > p.entry_max_seconds_to_resolution:
            return self._no_action(ms, f"too_far_from_resolution({ms.seconds_to_resolution}s)"), "sec_to_resolution"
        if p.entry_min_seconds_to_resolution > 0 and ms.seconds_to_resolution < p.entry_min_seconds_to_resolution:
            return self._no_action(ms, f"too_close_to_resolution({ms.seconds_to_resolution}s)"), "sec_to_resolution"
        accepted_reasons.append("resolution_window_ok")

        # --- Gate: 60s return magnitude ---
        if p.entry_min_ret_60s > 0 and abs(ms.binance_ret_60s) < p.entry_min_ret_60s:
            return self._no_action(ms, "ret_60s below threshold"), "ret_60s"
        accepted_reasons.append("ret_60s_ok")

        accepted_reasons.append("time_window_ok")

        # All gates passed -- pick direction via momentum cascade
        # (mirrors live _pick_buy_direction: ret_30s → ret_5s → ret_120s → imbalance)
        preferred_outcome = _pick_direction_cascade(ms)
        if preferred_outcome is None:
            return self._no_action(ms, "no_direction_signal"), "direction"

        # Only enter on the outcome matching the chosen direction
        if ms.outcome != preferred_outcome:
            return self._no_action(ms, f"wrong_outcome({ms.outcome}!={preferred_outcome})"), "direction"

        # Wallet-copy: entries are always BUYs (buying outcome tokens)
        action = "BUY"

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
            token_id=ms.token_id,
            binance_ret_120s=ms.binance_ret_120s,
            spread_pctl_prev=ms.spread_pctl_prev,
            accepted_reason_codes=accepted_reasons,
            sec_from_quarter_et=ms.sec_from_quarter_et,
            sec_from_hour_et=ms.sec_from_hour_et,
            seconds_to_resolution=ms.seconds_to_resolution,
            market_type=ms.market_type,
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
            token_id=ms.token_id,
            binance_ret_120s=ms.binance_ret_120s,
            spread_pctl_prev=ms.spread_pctl_prev,
        )

    def evaluate_gate_detail(self, ms: MarketState) -> List[Tuple[str, float, float, bool]]:
        """Return ALL gate checks as (gate_code, value, threshold, passed).

        Unlike evaluate_market_state which short-circuits on first failure,
        this evaluates every gate for full attribution.
        """
        p = self.params.for_asset(ms.asset) if self.params.asset_overrides else self.params
        results = []
        quiet_ok = not p.quiet_hours_et or ms.hour_et not in p.quiet_hours_et
        results.append(("quiet_hour", float(ms.hour_et), 0.0, quiet_ok))
        results.append(("spread_cents", ms.spread_cents, p.entry_min_spread_cents,
                         ms.spread_cents >= p.entry_min_spread_cents))
        results.append(("spread_pctl", ms.spread_percentile, p.entry_min_spread_pctl,
                         ms.spread_percentile >= p.entry_min_spread_pctl))
        if p.entry_min_ret_30s > 0:
            results.append(("|ret_30s|", abs(ms.binance_ret_30s), p.entry_min_ret_30s,
                             abs(ms.binance_ret_30s) >= p.entry_min_ret_30s))
        else:
            results.append(("|ret_30s|", abs(ms.binance_ret_30s), 0.0, True))

        # Directional imbalance check
        if p.imbalance_directional:
            direction = _pick_direction_cascade(ms) or "Up"
            if direction == "Up":
                results.append(("imbalance_dir", ms.orderbook_imbalance, p.entry_min_imbalance,
                                 ms.orderbook_imbalance >= p.entry_min_imbalance))
            else:
                effective_thr = 1.0 - p.entry_min_imbalance
                results.append(("imbalance_dir", ms.orderbook_imbalance, effective_thr,
                                 ms.orderbook_imbalance <= effective_thr))
        else:
            results.append(("imbalance", ms.orderbook_imbalance, p.entry_min_imbalance,
                             ms.orderbook_imbalance >= p.entry_min_imbalance))

        if p.spread_pctl_delta_min > 0:
            pctl_delta = ms.spread_percentile - ms.spread_pctl_prev
            results.append(("pctl_delta", pctl_delta, p.spread_pctl_delta_min,
                             pctl_delta >= p.spread_pctl_delta_min))
        if p.ret_accel_min > 0:
            ret_accel = abs(ms.binance_ret_30s - ms.binance_ret_120s)
            results.append(("ret_accel", ret_accel, p.ret_accel_min,
                             ret_accel >= p.ret_accel_min))

        # seconds_to_resolution gates
        if p.entry_max_seconds_to_resolution > 0:
            results.append(("max_sec_to_res", float(ms.seconds_to_resolution),
                             p.entry_max_seconds_to_resolution,
                             ms.seconds_to_resolution <= p.entry_max_seconds_to_resolution))
        if p.entry_min_seconds_to_resolution > 0:
            results.append(("min_sec_to_res", float(ms.seconds_to_resolution),
                             p.entry_min_seconds_to_resolution,
                             ms.seconds_to_resolution >= p.entry_min_seconds_to_resolution))

        # 60s return gate
        if p.entry_min_ret_60s > 0:
            results.append(("|ret_60s|", abs(ms.binance_ret_60s), p.entry_min_ret_60s,
                             abs(ms.binance_ret_60s) >= p.entry_min_ret_60s))
        else:
            results.append(("|ret_60s|", abs(ms.binance_ret_60s), 0.0, True))

        return results


COOLDOWN_SECONDS = 5  # suppress duplicate bot entries per market (lowered from 20; wallet re-enters same market quickly)


def run_replay(states: List[MarketState], params: StrategyParams) -> List[BotDecision]:
    """Run the strategy over all market states and return decisions.

    Applies a per-(slug, outcome) cooldown: after the bot triggers an entry,
    further entries for that pair are suppressed for
    max(COOLDOWN_SECONDS, params.entry_cooldown_sec) to prevent overfitting.
    """
    strategy = Strategy(params)
    cooldown = max(COOLDOWN_SECONDS, params.entry_cooldown_sec)
    last_entry_ts: Dict[Tuple[str, str], float] = {}  # (slug, outcome) -> epoch
    decisions: List[BotDecision] = []

    for ms in states:
        decision, _gate = strategy.evaluate_market_state(ms)

        if decision.action in ("BUY", "SELL"):
            key = (ms.slug, ms.outcome)
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

    NOTE: outcome is a market property (which token), NOT trade direction.
    Do NOT flip outcome based on BUY/SELL side.
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
    """Normalise wallet trade asset, ALWAYS deriving from slug first."""
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
    """Normalise wallet trade outcome (market property, NOT direction)."""
    for col in ("outcome",):
        if col in row.index and pd.notna(row[col]):
            return _normalize_outcome(str(row[col]))
    return ""


def _resolve_wallet_token_id(row: pd.Series) -> str:
    """Extract token_id from wallet trade row."""
    if "token_id" in row.index and pd.notna(row.get("token_id")):
        return str(row["token_id"]).strip()
    return ""


def _make_decision_key(d) -> tuple:
    """Build normalised matching key from a BotDecision.

    Uses token_id as primary key if available; falls back to
    (asset, slug, outcome, side) tuple.
    """
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


def _make_wallet_key(row: pd.Series) -> tuple:
    """Build normalised matching key from a wallet trade row."""
    return (
        _resolve_wallet_asset(row),
        _resolve_wallet_slug(row),
        _resolve_wallet_outcome(row),
        _resolve_wallet_side(row),
    )


def _build_token_id_index(
    decisions: List[BotDecision],
) -> Dict[str, List[float]]:
    """Build token_id -> [timestamps] index for bot decisions."""
    idx: Dict[str, List[float]] = defaultdict(list)
    for d in decisions:
        if d.action in ("BUY", "SELL") and d.token_id:
            idx[d.token_id].append(d.timestamp)
    return {k: sorted(v) for k, v in idx.items()}


def _match_by_token_id_or_key(
    wts: float,
    w_token_id: str,
    key: tuple,
    bot_token_idx: Dict[str, np.ndarray],
    bot_key_arrays: Dict[tuple, np.ndarray],
    tolerance_sec: float,
) -> Optional[float]:
    """Try matching a wallet trade to a bot decision.

    Priority: token_id match first, then (asset, slug, outcome, side) fallback.
    Returns best lag (bot_ts - wallet_ts) or None if no match.
    """
    # 1) Try token_id match
    if w_token_id:
        arr = bot_token_idx.get(w_token_id)
        if arr is not None and len(arr) > 0:
            lag = _find_best_lag(arr, wts, tolerance_sec)
            if lag is not None:
                return lag

    # 2) Fallback to key-based match
    arr = bot_key_arrays.get(key)
    if arr is not None and len(arr) > 0:
        return _find_best_lag(arr, wts, tolerance_sec)

    return None


def _find_best_lag(arr: np.ndarray, target: float, tolerance: float) -> Optional[float]:
    """Find best lag (arr[i] - target) within tolerance."""
    idx = np.searchsorted(arr, target)
    best_lag = None
    for i in range(max(0, idx - 1), min(len(arr), idx + 2)):
        lag = float(arr[i]) - target
        if abs(lag) <= tolerance:
            if best_lag is None or abs(lag) < abs(best_lag):
                best_lag = lag
    return best_lag


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
    buy_only: bool = True,
    quiet: bool = False,
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
    buy_only: if True, exclude wallet SELL trades from comparison (replay only
              models entries/BUYs, so SELL trades can never match).
    """
    if wallet_trades.empty or not decisions:
        return ComparisonResult(0, 0, 0, 0, 0.0)

    wt = wallet_trades.copy()
    if "ts" not in wt.columns:
        return ComparisonResult(0, 0, 0, 0, 0.0)

    # --- Build bot entry index keyed by (asset, slug, outcome, side) ---
    bot_entries_by_key: Dict[Tuple[str, str, str, str], List[float]] = defaultdict(list)
    for d in decisions:
        if d.action in ("BUY", "SELL"):
            key = _make_decision_key(d)
            bot_entries_by_key[key].append(d.timestamp)

    bot_arrays: Dict[Tuple[str, str, str, str], np.ndarray] = {
        k: np.array(sorted(v)) for k, v in bot_entries_by_key.items()
    }

    # --- Build token_id index for primary matching ---
    bot_token_raw = _build_token_id_index(decisions)
    bot_token_idx: Dict[str, np.ndarray] = {
        k: np.array(v) for k, v in bot_token_raw.items()
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
        # Skip wallet SELL trades: replay only models entries (BUYs)
        if buy_only and w_side == "SELL":
            continue

        total += 1
        key = (w_asset, w_slug, w_outcome, w_side)

        # Try token_id-primary match first, then fallback to key
        best_lag = _match_by_token_id_or_key(
            wts, w_token_id, key, bot_token_idx, bot_arrays, tolerance_sec,
        )
        if best_lag is not None:
            matched += 1
            entry_lags.append(best_lag)
            continue

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
                    "slug": w_slug, "outcome": w_outcome, "token_id": w_token_id,
                    "reason": "no_bot_entry_for_key",
                    "nearest_any_bot_ts": _nearest_ts(all_bot_ts, float(wts)),
                })
            continue

        # Key exists but _match_by_token_id_or_key found no match within tolerance
        nearest = float("inf")
        idx = np.searchsorted(arr, float(wts))
        for i in range(max(0, idx - 1), min(len(arr), idx + 2)):
            d_lag = abs(float(arr[i]) - float(wts))
            if d_lag < nearest:
                nearest = d_lag
        mismatch_reasons[f"key_match_but_outside_tolerance (nearest {nearest:.1f}s)"] += 1
        if debug and len(debug_examples) < 5:
            debug_examples.append({
                "wallet_ts": float(wts), "side": w_side, "asset": w_asset,
                "slug": w_slug, "outcome": w_outcome, "token_id": w_token_id,
                "reason": f"outside_tolerance (nearest={nearest:.1f}s)",
                "nearest_any_bot_ts": _nearest_ts(all_bot_ts, float(wts)),
            })

    missed = total - matched

    # --- Matching diagnostics ---
    if mismatch_reasons and not quiet:
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
    if wrong_key_examples and not quiet:
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

    if debug and debug_examples and not quiet:
        print(f"\n  {'─' * 60}")
        print("  DEBUG -- Example unmatched wallet trades")
        print(f"  {'─' * 60}")
        for ex in debug_examples:
            nearest_str = f"{ex['nearest_any_bot_ts']:.3f}" if ex['nearest_any_bot_ts'] is not None else "N/A"
            lag_str = f"{abs(ex['nearest_any_bot_ts'] - ex['wallet_ts']):.1f}s" if ex['nearest_any_bot_ts'] is not None else "N/A"
            print(f"    wallet_ts={ex['wallet_ts']:.3f}  {ex['side']:>4} {ex['asset']:>4} slug={ex['slug']} outcome={ex['outcome']}")
            print(f"      reason: {ex['reason']}  |  nearest_any_bot_ts={nearest_str} ({lag_str} away)")

    # --- Print all unique bot entry keys for cross-reference ---
    if wrong_key_examples and not quiet:
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
        if not w_side:
            continue
        if buy_only and w_side == "SELL":
            continue
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

    # F1 score: harmonic mean of precision and recall
    recall = matched / total if total > 0 else 0.0
    bot_total = matched + bot_false
    precision = matched / bot_total if bot_total > 0 else 0.0
    if precision + recall > 0:
        similarity = 2 * precision * recall / (precision + recall)
    else:
        similarity = 0.0

    # --- Latency stats (ms precision) ---
    lag_median = float(np.median(entry_lags)) if entry_lags else float("nan")
    lag_p90 = float(np.percentile(entry_lags, 90)) if entry_lags else float("nan")

    return ComparisonResult(
        wallet_trades_total=total,
        wallet_trades_matched=matched,
        wallet_trades_missed=missed,
        bot_false_entries=bot_false,
        similarity_score=similarity,
        precision=precision,
        recall=recall,
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
) -> Dict[str, int]:
    """Print top N wallet trades that the bot MISSED, with gate-fail attribution.

    For each missed trade finds the nearest MarketState and shows EXACTLY which
    gate(s) failed: gate_code, gate_value, gate_threshold.

    Returns a dict of miss_reason -> count for use by recommend_config().
    """
    miss_reasons: Dict[str, int] = defaultdict(int)
    if wallet_trades.empty or "ts" not in wallet_trades.columns or not decisions:
        return dict(miss_reasons)

    strategy = Strategy(params)

    # Build bot entries index -- token_id primary, key fallback
    bot_token_idx: Dict[str, np.ndarray] = {}
    bot_key_arrays: Dict[tuple, np.ndarray] = {}
    tmp_tok: Dict[str, List[float]] = defaultdict(list)
    tmp_key: Dict[tuple, List[float]] = defaultdict(list)
    for d in decisions:
        if d.action in ("BUY", "SELL"):
            if d.token_id:
                tmp_tok[d.token_id].append(d.timestamp)
            key = _make_decision_key(d)
            tmp_key[key].append(d.timestamp)
    bot_token_idx = {k: np.array(sorted(v)) for k, v in tmp_tok.items()}
    bot_key_arrays = {k: np.array(sorted(v)) for k, v in tmp_key.items()}

    # Build state lookup by (slug_casefold, outcome_norm) and by token_id
    state_ts = np.array([ms.timestamp for ms in states])
    state_by_slug_outcome: Dict[Tuple[str, str], List[int]] = defaultdict(list)
    state_by_token: Dict[str, List[int]] = defaultdict(list)
    for si, ms in enumerate(states):
        state_by_slug_outcome[(ms.slug.casefold(), _normalize_outcome(ms.outcome))].append(si)
        if ms.token_id:
            state_by_token[ms.token_id].append(si)

    wt = wallet_trades.sort_values("ts")

    print(f"\n  {'═' * 140}")
    print(f"  MISSED WALLET TRADES (top {max_rows}) -- gate-fail attribution")
    print(f"  {'═' * 140}")
    print(f"  {'#':>3} {'ts':>14} {'Asset':>5} {'Side':>4} {'Slug':<30} {'Out':>4} "
          f"{'Miss Reason':<70}")
    print(f"  {'─' * 140}")

    missed_count = 0
    total_missed = 0
    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        w_asset = _resolve_wallet_asset(row)
        w_slug = _resolve_wallet_slug(row)
        w_outcome = _resolve_wallet_outcome(row)
        w_token_id = _resolve_wallet_token_id(row)
        if not w_side:
            continue

        key = (w_asset, w_slug, w_outcome, w_side)
        lag = _match_by_token_id_or_key(
            float(wts), w_token_id, key, bot_token_idx, bot_key_arrays, tolerance_sec
        )
        if lag is not None:
            continue  # matched

        total_missed += 1
        missed_count += 1

        # Find nearest market state for this (token_id or slug+outcome)
        best_ms = None
        best_dt = float("inf")
        # Try token_id first
        candidate_indices = []
        if w_token_id and w_token_id in state_by_token:
            candidate_indices = state_by_token[w_token_id]
        if not candidate_indices:
            candidate_indices = state_by_slug_outcome.get((w_slug, w_outcome), [])

        if candidate_indices:
            target = float(wts)
            # Use binary search on state_ts then filter by candidates
            idx = np.searchsorted(state_ts, target)
            for si in range(max(0, idx - 30), min(len(states), idx + 30)):
                if si not in candidate_indices and states[si].slug.casefold() != w_slug:
                    continue
                # Check token or slug+outcome match
                ms = states[si]
                ok = False
                if w_token_id and ms.token_id == w_token_id:
                    ok = True
                elif ms.slug.casefold() == w_slug and _normalize_outcome(ms.outcome) == w_outcome:
                    ok = True
                if not ok:
                    continue
                dt = abs(ms.timestamp - target)
                if dt < best_dt:
                    best_dt = dt
                    best_ms = ms

        # Build miss reason string
        if best_ms is None or best_dt > 30.0:
            reason = "no_market_snapshot (no data within 30s)"
            miss_reasons["no_snapshot"] += 1
        else:
            gate_details = strategy.evaluate_gate_detail(best_ms)
            failed_gates = [(gc, val, thr) for gc, val, thr, passed in gate_details if not passed]
            if failed_gates:
                parts = []
                for gc, val, thr in failed_gates:
                    parts.append(f"missed_gate={gc} value={val:.6f} threshold={thr:.6f}")
                    miss_reasons[gc] += 1
                reason = " | ".join(parts)
            else:
                # All gates passed -- missed due to cooldown or key mismatch
                reason = "all_gates_PASS (cooldown or key-mapping mismatch)"
                miss_reasons["cooldown_or_key"] += 1

        if missed_count <= max_rows:
            print(f"  {missed_count:>3} {float(wts):>14.1f} {w_asset:>5} {w_side:>4} "
                  f"{w_slug:<30.30} {w_outcome:>4} "
                  f"{reason}")

    # Summary by reason
    print(f"\n  Total missed: {total_missed}")
    if miss_reasons:
        print(f"  Miss breakdown:")
        for reason, cnt in sorted(miss_reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason:<30} {cnt:>5} ({100*cnt/max(1,total_missed):.1f}%)")

    return dict(miss_reasons)


def dump_false_bot_entries(
    states: List[MarketState],
    wallet_trades: pd.DataFrame,
    decisions: List[BotDecision],
    params: StrategyParams,
    tolerance_sec: float = 3.0,
    max_rows: int = 50,
) -> None:
    """Print top N bot entries with NO matching wallet trade (false entries).

    For each false entry shows which gates ALLOWED it (all passed) and the
    feature values that caused it to fire, so users can tighten thresholds.
    """
    if wallet_trades.empty or "ts" not in wallet_trades.columns or not decisions:
        return

    strategy = Strategy(params)

    # Build wallet trade index -- token_id primary, key fallback
    wallet_token_idx: Dict[str, np.ndarray] = {}
    wallet_key_arrays: Dict[tuple, np.ndarray] = {}
    tmp_tok: Dict[str, List[float]] = defaultdict(list)
    tmp_key: Dict[tuple, List[float]] = defaultdict(list)
    wt = wallet_trades.copy()
    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        w_asset = _resolve_wallet_asset(row)
        w_slug = _resolve_wallet_slug(row)
        w_outcome = _resolve_wallet_outcome(row)
        w_token_id = _resolve_wallet_token_id(row)
        if not w_side:
            continue
        if w_token_id:
            tmp_tok[w_token_id].append(float(wts))
        tmp_key[(w_asset, w_slug, w_outcome, w_side)].append(float(wts))
    wallet_token_idx = {k: np.array(sorted(v)) for k, v in tmp_tok.items()}
    wallet_key_arrays = {k: np.array(sorted(v)) for k, v in tmp_key.items()}

    # Flat wallet ts for nearest search
    all_wallet_ts = np.array(sorted(
        float(row.get("ts")) for _, row in wt.iterrows()
        if not pd.isna(row.get("ts"))
    )) if not wt.empty else np.array([])

    print(f"\n  {'═' * 150}")
    print(f"  FALSE BOT ENTRIES (top {max_rows}) -- bot entry with no matching wallet trade")
    print(f"  {'═' * 150}")
    print(f"  {'#':>3} {'ts':>14} {'Asset':>5} {'Side':>4} {'Slug':<28} {'Out':>4} "
          f"{'nearest_w':>10} {'Gates that ALLOWED entry (gate=value/threshold)':<75}")
    print(f"  {'─' * 150}")

    false_count = 0
    total_false = 0
    # Collect feature values for false entries to help with recommendations
    false_features: List[List[Tuple[str, float, float, bool]]] = []

    for d in decisions:
        if d.action not in ("BUY", "SELL"):
            continue

        key = _make_decision_key(d)
        # Try token_id match, then key match
        lag = None
        if d.token_id:
            warr = wallet_token_idx.get(d.token_id)
            if warr is not None and len(warr) > 0:
                lag = _find_best_lag(warr, d.timestamp, tolerance_sec)
        if lag is None:
            warr = wallet_key_arrays.get(key)
            if warr is not None and len(warr) > 0:
                lag = _find_best_lag(warr, d.timestamp, tolerance_sec)
        if lag is not None:
            continue  # matched

        total_false += 1

        # Build a MarketState-like view from the BotDecision fields for gate detail
        ms_proxy = MarketState(
            timestamp=d.timestamp, asset=d.asset, slug=d.slug,
            outcome=d.outcome, token_id=d.token_id,
            best_bid=0.0, best_ask=0.0,
            spread_cents=d.spread_cents,
            spread_percentile=d.spread_percentile,
            orderbook_imbalance=d.orderbook_imbalance,
            binance_ret_5s=0.0, binance_ret_30s=d.binance_ret_30s,
            binance_ret_60s=0.0, binance_ret_120s=d.binance_ret_120s,
            spread_pctl_prev=d.spread_pctl_prev,
        )
        gate_details = strategy.evaluate_gate_detail(ms_proxy)
        false_features.append(gate_details)

        if total_false <= max_rows:
            false_count += 1

            # Nearest wallet trade for context
            nearest_wt_str = "--"
            if len(all_wallet_ts) > 0:
                idx = np.searchsorted(all_wallet_ts, d.timestamp)
                best_delta = float("inf")
                for i in range(max(0, idx - 1), min(len(all_wallet_ts), idx + 2)):
                    dt = all_wallet_ts[i] - d.timestamp
                    if abs(dt) < abs(best_delta):
                        best_delta = dt
                if abs(best_delta) < 300:
                    nearest_wt_str = f"{best_delta:+.1f}s"

            # Show all gates with their values
            gate_parts = []
            for gc, val, thr, passed in gate_details:
                marker = "OK" if passed else "FAIL"
                gate_parts.append(f"{gc}={val:.4f}/{thr:.4f}({marker})")
            gate_str = " | ".join(gate_parts)

            print(f"  {false_count:>3} {d.timestamp:>14.1f} {d.asset:>5} {d.action:>4} "
                  f"{d.slug:<28.28} {d.outcome:>4} "
                  f"{nearest_wt_str:>10} {gate_str}")

    print(f"\n  Total false bot entries: {total_false}")

    # Summary: which gates are most permissive on false entries
    if false_features:
        print(f"\n  False-entry gate value distributions (what allowed them in):")
        gate_vals: Dict[str, List[float]] = defaultdict(list)
        for details in false_features:
            for gc, val, _thr, _passed in details:
                gate_vals[gc].append(val)
        for gc, vals in sorted(gate_vals.items()):
            varr = np.array(vals)
            print(f"    {gc:<15} median={np.median(varr):.6f}  p10={np.percentile(varr,10):.6f}  "
                  f"p90={np.percentile(varr,90):.6f}  (n={len(varr)})")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3b2 -- DEEP DIAGNOSTICS: direction, matched-vs-false features,
#                per-slug breakdown, cooldown impact, time-in-window
# ═══════════════════════════════════════════════════════════════════════════

def deep_diagnostics(
    states: List[MarketState],
    wallet_trades: pd.DataFrame,
    decisions: List[BotDecision],
    params: StrategyParams,
    tolerance_sec: float = 3.0,
    offset_sec: float = 0.0,
) -> None:
    """Run comprehensive diagnostics to understand WHY similarity is low.

    Prints:
      A) Direction correctness: does the bot pick the same outcome as wallet?
      B) Matched vs false entry feature distributions (what separates them)
      C) Per-slug breakdown (which markets have worst false entry rates)
      D) Cooldown impact (how many would-be matches were suppressed)
      E) Time-in-window analysis (when in 15-min window does wallet vs bot enter)
      F) Wallet-side analysis (BUY vs SELL breakdown of missed trades)
      G) Feature distributions of MISSED trades vs ALL wallet trades
    """
    if wallet_trades.empty or "ts" not in wallet_trades.columns or not decisions:
        print("\n  No data for deep diagnostics.")
        return

    print(f"\n{'═' * 100}")
    print("  DEEP DIAGNOSTICS -- Understanding the similarity gap")
    print(f"{'═' * 100}")

    strategy = Strategy(params)
    wt = wallet_trades.copy()

    # Build bot entry structures
    bot_entries = [d for d in decisions if d.action in ("BUY", "SELL")]
    bot_token_raw = _build_token_id_index(decisions)
    bot_token_idx = {k: np.array(v) for k, v in bot_token_raw.items()}
    tmp_key: Dict[tuple, List[float]] = defaultdict(list)
    for d in decisions:
        if d.action in ("BUY", "SELL"):
            tmp_key[_make_decision_key(d)].append(d.timestamp)
    bot_key_arrays = {k: np.array(sorted(v)) for k, v in tmp_key.items()}

    # Build wallet structures
    wallet_token_raw: Dict[str, List[float]] = defaultdict(list)
    wallet_key_raw: Dict[tuple, List[float]] = defaultdict(list)
    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        w_token_id = _resolve_wallet_token_id(row)
        if not w_side:
            continue
        if w_side == "SELL":
            continue
        key = _make_wallet_key(row)
        if w_token_id:
            wallet_token_raw[w_token_id].append(float(wts) + offset_sec)
        wallet_key_raw[key].append(float(wts) + offset_sec)
    wallet_token_idx = {k: np.array(sorted(v)) for k, v in wallet_token_raw.items()}
    wallet_key_arrays = {k: np.array(sorted(v)) for k, v in wallet_key_raw.items()}

    # Build state lookup
    state_ts = np.array([ms.timestamp for ms in states])

    # ── A) DIRECTION CORRECTNESS ──────────────────────────────────────────
    print(f"\n  {'─' * 90}")
    print("  A) DIRECTION CORRECTNESS -- Does the bot pick the same outcome as the wallet?")
    print(f"  {'─' * 90}")

    direction_correct = 0
    direction_wrong = 0
    direction_no_signal = 0
    direction_wrong_examples: List[dict] = []

    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        if w_side != "BUY":
            continue
        w_outcome = _resolve_wallet_outcome(row)
        w_slug = _resolve_wallet_slug(row)
        w_token_id = _resolve_wallet_token_id(row)
        if not w_outcome:
            continue

        # Find nearest market state for this trade
        target = float(wts)
        idx = np.searchsorted(state_ts, target)
        best_ms = None
        best_dt = float("inf")
        for si in range(max(0, idx - 10), min(len(states), idx + 10)):
            ms = states[si]
            if ms.token_id and w_token_id and ms.token_id == w_token_id:
                dt = abs(ms.timestamp - target)
                if dt < best_dt:
                    best_dt = dt
                    best_ms = ms
            elif ms.slug.casefold() == w_slug and _normalize_outcome(ms.outcome) == w_outcome:
                dt = abs(ms.timestamp - target)
                if dt < best_dt:
                    best_dt = dt
                    best_ms = ms
        if best_ms is None or best_dt > 10.0:
            continue

        # What direction does the bot pick?
        bot_direction = _pick_direction_cascade(best_ms)
        wallet_outcome_norm = w_outcome  # already normalized

        # Map bot direction to normalized outcome
        if bot_direction is None:
            direction_no_signal += 1
        else:
            bot_outcome_norm = _normalize_outcome(bot_direction)
            if bot_outcome_norm == wallet_outcome_norm:
                direction_correct += 1
            else:
                direction_wrong += 1
                if len(direction_wrong_examples) < 10:
                    direction_wrong_examples.append({
                        "ts": target,
                        "wallet_outcome": wallet_outcome_norm,
                        "bot_direction": bot_direction,
                        "ret_30s": best_ms.binance_ret_30s,
                        "ret_5s": best_ms.binance_ret_5s,
                        "ret_120s": best_ms.binance_ret_120s,
                        "imbalance": best_ms.orderbook_imbalance,
                        "asset": best_ms.asset,
                    })

    total_dir = direction_correct + direction_wrong + direction_no_signal
    if total_dir > 0:
        print(f"    Direction CORRECT:   {direction_correct:>6} ({100*direction_correct/total_dir:.1f}%)")
        print(f"    Direction WRONG:     {direction_wrong:>6} ({100*direction_wrong/total_dir:.1f}%)")
        print(f"    No signal:          {direction_no_signal:>6} ({100*direction_no_signal/total_dir:.1f}%)")
        print(f"    Total wallet BUYs:  {total_dir:>6}")
        if direction_wrong > 0 and direction_wrong_examples:
            print(f"\n    First {len(direction_wrong_examples)} wrong-direction examples:")
            print(f"    {'ts':>14} {'Asset':>5} {'Wallet':>7} {'Bot':>5} {'ret_30s':>10} {'ret_5s':>10} {'ret_120s':>10} {'imbal':>7}")
            for ex in direction_wrong_examples:
                print(f"    {ex['ts']:>14.1f} {ex['asset']:>5} {ex['wallet_outcome']:>7} {ex['bot_direction']:>5} "
                      f"{ex['ret_30s']:>10.6f} {ex['ret_5s']:>10.6f} {ex['ret_120s']:>10.6f} {ex['imbalance']:>7.3f}")

    # ── B) MATCHED vs FALSE ENTRY FEATURE DISTRIBUTIONS ───────────────────
    print(f"\n  {'─' * 90}")
    print("  B) MATCHED vs FALSE entry feature distributions")
    print(f"  {'─' * 90}")

    matched_feats = {"spread_cents": [], "spread_pctl": [], "|ret_30s|": [],
                     "imbalance": [], "hour_et": [], "sec_from_quarter": []}
    false_feats = {"spread_cents": [], "spread_pctl": [], "|ret_30s|": [],
                   "imbalance": [], "hour_et": [], "sec_from_quarter": []}

    for d in bot_entries:
        key = _make_decision_key(d)
        # Check if matched
        lag = None
        if d.token_id:
            warr = wallet_token_idx.get(d.token_id)
            if warr is not None and len(warr) > 0:
                lag = _find_best_lag(warr, d.timestamp, tolerance_sec)
        if lag is None:
            warr = wallet_key_arrays.get(key)
            if warr is not None and len(warr) > 0:
                lag = _find_best_lag(warr, d.timestamp, tolerance_sec)

        target = matched_feats if lag is not None else false_feats
        target["spread_cents"].append(d.spread_cents)
        target["spread_pctl"].append(d.spread_percentile)
        target["|ret_30s|"].append(abs(d.binance_ret_30s))
        target["imbalance"].append(d.orderbook_imbalance)
        # Compute hour from timestamp
        try:
            dt_utc = datetime.fromtimestamp(d.timestamp, tz=timezone.utc)
            target["hour_et"].append(dt_utc.astimezone(_ET).hour)
        except (OSError, OverflowError, ValueError):
            pass
        target["sec_from_quarter"].append(d.sec_from_quarter_et)

    print(f"\n    Matched bot entries: {len(matched_feats['spread_cents'])}")
    print(f"    False bot entries:   {len(false_feats['spread_cents'])}")

    print(f"\n    {'Feature':<20} {'Matched median':>16} {'False median':>14} {'Matched p25':>13} {'False p25':>11} {'Matched p75':>13} {'False p75':>11}")
    print(f"    {'─' * 98}")
    for feat in ["spread_cents", "spread_pctl", "|ret_30s|", "imbalance", "sec_from_quarter"]:
        m_vals = np.array(matched_feats[feat]) if matched_feats[feat] else np.array([0])
        f_vals = np.array(false_feats[feat]) if false_feats[feat] else np.array([0])
        print(f"    {feat:<20} {np.median(m_vals):>16.6f} {np.median(f_vals):>14.6f} "
              f"{np.percentile(m_vals, 25):>13.6f} {np.percentile(f_vals, 25):>11.6f} "
              f"{np.percentile(m_vals, 75):>13.6f} {np.percentile(f_vals, 75):>11.6f}")

    # Hour distribution comparison
    if matched_feats["hour_et"] and false_feats["hour_et"]:
        print(f"\n    Hour distribution (matched vs false entry rate):")
        m_hours = np.array(matched_feats["hour_et"])
        f_hours = np.array(false_feats["hour_et"])
        print(f"    {'Hour':>6} {'Matched':>9} {'False':>9} {'False/Match ratio':>18}")
        for h in range(24):
            mc = np.sum(m_hours == h)
            fc = np.sum(f_hours == h)
            if mc > 0 or fc > 0:
                ratio = f"{fc/max(1,mc):.1f}x" if mc > 0 else "inf"
                print(f"    {h:>6} {mc:>9} {fc:>9} {ratio:>18}")

    # ── C) PER-SLUG BREAKDOWN ─────────────────────────────────────────────
    print(f"\n  {'─' * 90}")
    print("  C) PER-SLUG BREAKDOWN -- Which specific markets have worst false entry rates?")
    print(f"  {'─' * 90}")

    slug_matched: Dict[str, int] = defaultdict(int)
    slug_false: Dict[str, int] = defaultdict(int)
    slug_missed: Dict[str, int] = defaultdict(int)

    # Count matched/false for bot entries
    for d in bot_entries:
        key = _make_decision_key(d)
        slug_norm = d.slug.strip().casefold()
        lag = None
        if d.token_id:
            warr = wallet_token_idx.get(d.token_id)
            if warr is not None and len(warr) > 0:
                lag = _find_best_lag(warr, d.timestamp, tolerance_sec)
        if lag is None:
            warr = wallet_key_arrays.get(key)
            if warr is not None and len(warr) > 0:
                lag = _find_best_lag(warr, d.timestamp, tolerance_sec)
        if lag is not None:
            slug_matched[slug_norm] += 1
        else:
            slug_false[slug_norm] += 1

    # Count missed wallet trades
    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        if w_side != "BUY":
            continue
        w_slug = _resolve_wallet_slug(row)
        w_token_id = _resolve_wallet_token_id(row)
        key = _make_wallet_key(row)
        wts_adj = float(wts) + offset_sec
        lag = _match_by_token_id_or_key(
            wts_adj, w_token_id, key, bot_token_idx, bot_key_arrays, tolerance_sec,
        )
        if lag is None:
            slug_missed[w_slug] += 1

    all_slugs = sorted(set(list(slug_matched.keys()) + list(slug_false.keys()) + list(slug_missed.keys())))
    print(f"\n    {'Slug':<55} {'Matched':>8} {'False':>8} {'Missed':>8} {'F/M ratio':>10}")
    print(f"    {'─' * 89}")
    # Sort by false count descending
    slug_data = [(s, slug_matched.get(s, 0), slug_false.get(s, 0), slug_missed.get(s, 0)) for s in all_slugs]
    slug_data.sort(key=lambda x: -x[2])
    for slug, mc, fc, mi in slug_data[:30]:
        ratio = f"{fc/max(1,mc):.1f}x" if mc > 0 else ("inf" if fc > 0 else "--")
        print(f"    {slug:<55.55} {mc:>8} {fc:>8} {mi:>8} {ratio:>10}")
    if len(slug_data) > 30:
        print(f"    ... and {len(slug_data) - 30} more slugs")

    # ── D) COOLDOWN IMPACT ANALYSIS ───────────────────────────────────────
    print(f"\n  {'─' * 90}")
    print("  D) COOLDOWN IMPACT -- How many entries were suppressed, and would they have matched?")
    print(f"  {'─' * 90}")

    # Re-run replay WITHOUT cooldown to see what cooldown suppresses
    no_cd_params = StrategyParams(
        entry_min_spread_pctl=params.entry_min_spread_pctl,
        entry_min_spread_cents=params.entry_min_spread_cents,
        entry_min_ret_30s=params.entry_min_ret_30s,
        entry_min_imbalance=params.entry_min_imbalance,
        imbalance_directional=params.imbalance_directional,
        quiet_hours_et=params.quiet_hours_et,
        entry_cooldown_sec=0.0,  # No cooldown
    )
    no_cd_decisions = run_replay(states, no_cd_params)
    no_cd_entries = sum(1 for d in no_cd_decisions if d.action in ("BUY", "SELL"))
    cd_entries = len(bot_entries)
    suppressed = no_cd_entries - cd_entries

    # Compare matches with vs without cooldown
    no_cd_cr = compare_to_wallet(no_cd_decisions, wt, tolerance_sec, offset_sec=offset_sec)

    print(f"    With cooldown ({params.entry_cooldown_sec}s):    entries={cd_entries:>7,}  matched={sum(1 for d in bot_entries if True):>6}")
    print(f"    Without cooldown:         entries={no_cd_entries:>7,}")
    print(f"    Suppressed by cooldown:   {suppressed:>7,} entries")
    print(f"    Match comparison:")
    print(f"      WITH cooldown:    similarity={0:.4f}  matched=~{len(matched_feats['spread_cents'])}  false=~{len(false_feats['spread_cents'])}")
    print(f"      WITHOUT cooldown: similarity={no_cd_cr.similarity_score:.4f}  matched={no_cd_cr.wallet_trades_matched}  false={no_cd_cr.bot_false_entries}")
    gained = no_cd_cr.wallet_trades_matched - len(matched_feats['spread_cents'])
    print(f"    Cooldown costs ~{max(0,gained)} matches but saves {suppressed - max(0,gained)} false entries")

    # ── E) TIME-IN-WINDOW ANALYSIS ────────────────────────────────────────
    print(f"\n  {'─' * 90}")
    print("  E) TIME-IN-WINDOW -- When in the 15-min window does the wallet vs bot enter?")
    print(f"  {'─' * 90}")

    wallet_sfq: List[int] = []
    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        if w_side != "BUY":
            continue
        try:
            dt_utc = datetime.fromtimestamp(float(wts), tz=timezone.utc)
            dt_et = dt_utc.astimezone(_ET)
            sfq = (dt_et.minute * 60 + dt_et.second) % 900
            wallet_sfq.append(sfq)
        except (OSError, OverflowError, ValueError):
            pass

    bot_sfq: List[int] = [d.sec_from_quarter_et for d in bot_entries]

    if wallet_sfq and bot_sfq:
        w_arr = np.array(wallet_sfq)
        b_arr = np.array(bot_sfq)
        # Bucket into 3-minute bins
        bins = list(range(0, 901, 180))
        print(f"\n    {'Window (sec)':>14} {'Wallet BUYs':>12} {'Bot entries':>12} {'Wallet%':>8} {'Bot%':>6}")
        print(f"    {'─' * 52}")
        for i in range(len(bins) - 1):
            lo, hi = bins[i], bins[i+1]
            wc = np.sum((w_arr >= lo) & (w_arr < hi))
            bc = np.sum((b_arr >= lo) & (b_arr < hi))
            wp = 100 * wc / len(w_arr) if len(w_arr) > 0 else 0
            bp = 100 * bc / len(b_arr) if len(b_arr) > 0 else 0
            print(f"    {lo:>4}-{hi:>4}s       {wc:>12} {bc:>12} {wp:>7.1f}% {bp:>5.1f}%")

        print(f"\n    Wallet median sec_from_quarter: {np.median(w_arr):.0f}s")
        print(f"    Bot median sec_from_quarter:    {np.median(b_arr):.0f}s")

    # ── F) WALLET SELL TRADES (potential missed signals) ──────────────────
    print(f"\n  {'─' * 90}")
    print("  F) WALLET TRADE SIDE BREAKDOWN")
    print(f"  {'─' * 90}")

    buy_count = 0
    sell_count = 0
    for _, row in wt.iterrows():
        w_side = _resolve_wallet_side(row)
        if w_side == "BUY":
            buy_count += 1
        elif w_side == "SELL":
            sell_count += 1

    print(f"    Total wallet BUYs:   {buy_count:>6}")
    print(f"    Total wallet SELLs:  {sell_count:>6}")
    print(f"    (Replay only models BUYs -- SELLs are excluded from similarity)")

    # ── G) MISSED TRADE FEATURE DISTRIBUTIONS ─────────────────────────────
    print(f"\n  {'─' * 90}")
    print("  G) MISSED TRADES -- feature distributions at wallet entry time")
    print(f"  {'─' * 90}")
    print("  (Compares features of MATCHED wallet trades vs MISSED wallet trades)")

    matched_wallet_feats: Dict[str, List[float]] = defaultdict(list)
    missed_wallet_feats: Dict[str, List[float]] = defaultdict(list)

    feat_col_map = {
        "spread_cents": ["spread", "spread_cents"],
        "spread_pctl": ["spread_percentile_60s", "spread_percentile_60s_at_trade"],
        "imbalance": ["orderbook_imbalance_at_trade", "imbalance_topN"],
        "|ret_30s|": ["binance_ret_30s_at_trade", "ret_30s"],
        "|ret_5s|": ["binance_ret_5s_at_trade", "ret_5s"],
    }

    # Resolve columns
    resolved_cols: Dict[str, Optional[str]] = {}
    for feat, candidates in feat_col_map.items():
        resolved_cols[feat] = None
        for c in candidates:
            if c in wt.columns:
                resolved_cols[feat] = c
                break

    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        if w_side != "BUY":
            continue
        w_token_id = _resolve_wallet_token_id(row)
        key = _make_wallet_key(row)
        wts_adj = float(wts) + offset_sec
        lag = _match_by_token_id_or_key(
            wts_adj, w_token_id, key, bot_token_idx, bot_key_arrays, tolerance_sec,
        )
        target = matched_wallet_feats if lag is not None else missed_wallet_feats

        for feat, col in resolved_cols.items():
            if col is None:
                continue
            val = row.get(col)
            if pd.notna(val):
                v = float(val)
                if "spread_cents" == feat and col == "spread":
                    v *= 100
                if "ret_" in feat:
                    v = abs(v)
                    if v > 0.1:  # likely percent
                        v /= 100
                target[feat].append(v)

    if matched_wallet_feats or missed_wallet_feats:
        print(f"\n    Matched wallet trades: {len(next(iter(matched_wallet_feats.values()), []))}")
        print(f"    Missed wallet trades:  {len(next(iter(missed_wallet_feats.values()), []))}")
        print(f"\n    {'Feature':<15} {'Match med':>11} {'Miss med':>10} {'Match p25':>11} {'Miss p25':>10} {'Match p75':>11} {'Miss p75':>10}")
        print(f"    {'─' * 78}")
        for feat in ["spread_cents", "spread_pctl", "imbalance", "|ret_30s|", "|ret_5s|"]:
            m_vals = np.array(matched_wallet_feats.get(feat, [0]))
            mi_vals = np.array(missed_wallet_feats.get(feat, [0]))
            if len(m_vals) == 0:
                m_vals = np.array([0])
            if len(mi_vals) == 0:
                mi_vals = np.array([0])
            print(f"    {feat:<15} {np.median(m_vals):>11.6f} {np.median(mi_vals):>10.6f} "
                  f"{np.percentile(m_vals, 25):>11.6f} {np.percentile(mi_vals, 25):>10.6f} "
                  f"{np.percentile(m_vals, 75):>11.6f} {np.percentile(mi_vals, 75):>10.6f}")

    # ── H) ASSET-SPECIFIC GATE FAILURE BREAKDOWN ──────────────────────────
    print(f"\n  {'─' * 90}")
    print("  H) ASSET-SPECIFIC missed-trade gate failures")
    print(f"  {'─' * 90}")

    asset_miss_gates: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    asset_miss_totals: Dict[str, int] = defaultdict(int)

    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        if w_side != "BUY":
            continue
        w_asset = _resolve_wallet_asset(row)
        w_slug = _resolve_wallet_slug(row)
        w_outcome = _resolve_wallet_outcome(row)
        w_token_id = _resolve_wallet_token_id(row)
        key = _make_wallet_key(row)
        wts_adj = float(wts) + offset_sec
        lag = _match_by_token_id_or_key(
            wts_adj, w_token_id, key, bot_token_idx, bot_key_arrays, tolerance_sec,
        )
        if lag is not None:
            continue  # matched

        asset_miss_totals[w_asset] += 1

        # Find nearest market state
        target_ts = float(wts)
        idx = np.searchsorted(state_ts, target_ts)
        best_ms = None
        best_dt = float("inf")
        for si in range(max(0, idx - 20), min(len(states), idx + 20)):
            ms = states[si]
            ok = False
            if w_token_id and ms.token_id == w_token_id:
                ok = True
            elif ms.slug.casefold() == w_slug and _normalize_outcome(ms.outcome) == w_outcome:
                ok = True
            if ok:
                dt = abs(ms.timestamp - target_ts)
                if dt < best_dt:
                    best_dt = dt
                    best_ms = ms

        if best_ms is None or best_dt > 30.0:
            asset_miss_gates[w_asset]["no_snapshot"] += 1
        else:
            gate_details = strategy.evaluate_gate_detail(best_ms)
            failed = [gc for gc, val, thr, passed in gate_details if not passed]
            if failed:
                for gc in failed:
                    asset_miss_gates[w_asset][gc] += 1
            else:
                asset_miss_gates[w_asset]["cooldown_or_key"] += 1

    for asset in sorted(asset_miss_totals.keys()):
        total = asset_miss_totals[asset]
        gates = asset_miss_gates[asset]
        print(f"\n    {asset}: {total} missed trades")
        for gate, cnt in sorted(gates.items(), key=lambda x: -x[1]):
            print(f"      {gate:<25} {cnt:>5} ({100*cnt/max(1,total):.1f}%)")

    print(f"\n{'═' * 100}")
    print("  END DEEP DIAGNOSTICS")
    print(f"{'═' * 100}\n")


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
    quiet: bool = False,
) -> float:
    """Investigate timestamp lag and print findings. Returns recommended offset."""
    if not quiet:
        print(f"\n  {'═' * 70}")
        print("  TIMESTAMP LAG INVESTIGATION")
        print(f"  {'═' * 70}")

    if wallet_trades.empty or "ts" not in wallet_trades.columns:
        if not quiet:
            print("  No wallet trades to analyze.")
        return 0.0

    # Check unit consistency
    wt_ts = wallet_trades["ts"].dropna()
    state_ts_vals = [ms.timestamp for ms in states[:100]] if states else []
    if not quiet and not wt_ts.empty and state_ts_vals:
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
    if not quiet:
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
    "entry_min_spread_pctl": [0.75, 0.80, 0.85, 0.90],
    "entry_min_spread_cents": [0.5, 1.0, 2.0],
    "entry_min_ret_30s": [0.0, 0.0005, 0.0015],
    "entry_min_imbalance": [0.0, 0.40, 0.45, 0.55],
    "spread_pctl_delta_min": [0.0, 0.05, 0.10],
    "ret_accel_min": [0.0, 0.0005, 0.001],
    "entry_cooldown_sec": [0.0, 5.0, 15.0],
    "entry_max_seconds_to_resolution": [0.0, 600],
    "entry_min_seconds_to_resolution": [0.0, 60],
    "entry_min_ret_60s": [0.0, 0.0005],
    "tolerance_sec": [3.0, 5.0, 7.0],
}

# Focused sweep ranges based on replay analysis:
#   - wallet ret_30s median=0.0 → momentum not needed
#   - wallet spread_pctl p25=83% → need to test lower pctl values
#   - wallet imbalance p25=0.432 → need lower imbalance thresholds
#   - wallet doesn't use directional imbalance strongly
#   - hours 3,5,7,8 ET have <16% match rate → quiet hours help
FOCUSED_SWEEP_RANGES = {
    # --- Locked from previous sweep (converged) ---
    "entry_min_ret_30s": [0.0],
    "entry_min_spread_pctl": [0.85],
    "entry_min_spread_cents": [1.0],
    "imbalance_directional": [False],
    "quiet_hours_et": [(3, 5, 7, 8)],
    "entry_cooldown_sec": [10.0],
    "entry_max_seconds_to_resolution": [0.0],
    # --- Locked from focused sweep (converged) ---
    "entry_min_imbalance": [0.1],
    "entry_min_seconds_to_resolution": [120],
    "entry_min_ret_60s": [0.0004],
    # Comparison tolerance
    "tolerance_sec": [7.0],
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

    # Separate tolerance_sec from strategy params (it's a comparison param, not a gate)
    tolerance_values = ranges.pop("tolerance_sec", [tolerance_sec])
    if not isinstance(tolerance_values, list):
        tolerance_values = [tolerance_values]

    keys = list(ranges.keys())
    strategy_grid = list(itertools.product(*(ranges[k] for k in keys)))
    # Full grid = strategy combos × tolerance values
    total = len(strategy_grid) * len(tolerance_values)
    print(f"\n  Parameter sweep: {total} combinations ({len(strategy_grid)} strategy × {len(tolerance_values)} tolerance) ...")

    import time as _time
    results = []
    t_start = _time.monotonic()
    done = 0
    for combo in strategy_grid:
        params = StrategyParams(**dict(zip(keys, combo)), asset_overrides={})
        decisions = run_replay(states, params)
        for tol in tolerance_values:
            cr = compare_to_wallet(decisions, wallet_trades, tol, offset_sec=offset_sec)

            entry = {}
            for k, v in zip(keys, combo):
                if isinstance(v, bool):
                    entry[k] = v
                elif isinstance(v, (tuple, list)):
                    entry[k] = str(v)
                else:
                    entry[k] = round(float(v), 6)
            entry["tolerance_sec"] = tol
            entry["similarity"] = round(cr.similarity_score, 4)
            entry["matched"] = cr.wallet_trades_matched
            entry["missed"] = cr.wallet_trades_missed
            entry["false_entries"] = cr.bot_false_entries
            entry["lag_median_ms"] = round(cr.entry_lag_median * 1000, 1) if not np.isnan(cr.entry_lag_median) else "N/A"
            entry["lag_p90_ms"] = round(cr.entry_lag_p90 * 1000, 1) if not np.isnan(cr.entry_lag_p90) else "N/A"
            results.append(entry)

            done += 1
            if done % max(1, total // 20) == 0 or done == 5:
                elapsed = _time.monotonic() - t_start
                eta = elapsed / done * (total - done)
                print(f"    ... {done}/{total}  ({elapsed:.0f}s elapsed, ~{eta:.0f}s remaining)")

    df = pd.DataFrame(results).sort_values("similarity", ascending=False).reset_index(drop=True)
    return df


# Per-asset sweep: wider grid since each asset runs independently (fewer states → faster)
PER_ASSET_SWEEP_RANGES = {
    "entry_min_spread_pctl": [0.75, 0.80, 0.85, 0.90, 0.95],
    "entry_min_spread_cents": [0.5, 1.0, 2.0],
    "entry_min_ret_30s": [0.0, 0.0003],
    "entry_min_imbalance": [0.0, 0.2, 0.42],
    "imbalance_directional": [False],
    "entry_cooldown_sec": [5.0, 10.0, 15.0],
    "quiet_hours_et": [(), (3, 5, 7, 8)],
    "entry_min_ret_60s": [0.0, 0.0003, 0.0005, 0.0008],
    "entry_min_seconds_to_resolution": [0.0, 60.0],
    "entry_max_seconds_to_resolution": [0.0],
    "tolerance_sec": [7.0],
}


def per_asset_sweep(
    states: List[MarketState],
    wallet_trades: pd.DataFrame,
    sweep_ranges: Optional[Dict] = None,
    tolerance_sec: float = 7.0,
    offset_sec: float = 0.0,
) -> Dict[str, pd.DataFrame]:
    """Run independent parameter sweeps for each crypto asset.

    Filters states and wallet trades by asset, then runs a full grid search
    for each. Returns dict of asset -> results DataFrame.
    """
    ranges = sweep_ranges or dict(PER_ASSET_SWEEP_RANGES)

    # Identify unique assets
    all_assets = sorted(set(ms.asset for ms in states if ms.asset))
    print(f"\n  Per-asset sweep for: {', '.join(all_assets)}")

    # Pre-compute wallet asset column for filtering
    wt = wallet_trades.copy()
    wt["_resolved_asset"] = wt.apply(_resolve_wallet_asset, axis=1)

    asset_results: Dict[str, pd.DataFrame] = {}

    for asset in all_assets:
        # Filter states for this asset
        asset_states = [ms for ms in states if ms.asset == asset]
        # Filter wallet trades for this asset
        asset_wallet = wt[wt["_resolved_asset"] == asset].drop(columns=["_resolved_asset"])

        if not asset_states:
            print(f"\n  {asset}: no market states, skipping")
            continue

        wallet_count = len(asset_wallet)
        print(f"\n  {'═' * 60}")
        print(f"  {asset}: {len(asset_states):,} states, {wallet_count} wallet trades")
        print(f"  {'═' * 60}")

        if wallet_count == 0:
            print(f"  {asset}: no wallet trades to match against, skipping")
            continue

        # Run sweep with a copy of ranges (parameter_sweep pops tolerance_sec)
        asset_ranges = dict(ranges)
        df = parameter_sweep(
            asset_states, asset_wallet,
            sweep_ranges=asset_ranges,
            tolerance_sec=tolerance_sec,
            offset_sec=offset_sec,
        )
        asset_results[asset] = df

        # Print top 5 for this asset
        if not df.empty:
            print(f"\n  TOP 5 for {asset}:")
            print(df.head(5).to_string(index=False))
            best = df.iloc[0]
            print(f"\n  {asset} BEST: F1={best['similarity']:.4f}  "
                  f"matched={best['matched']}  missed={best['missed']}  "
                  f"false={best['false_entries']}")

    # Print combined summary
    print(f"\n\n{'═' * 70}")
    print("  PER-ASSET SWEEP SUMMARY")
    print(f"{'═' * 70}")
    print(f"  {'Asset':<6} {'Best F1':>8} {'Matched':>8} {'Missed':>8} {'False':>8}  Best Params")
    print(f"  {'─' * 90}")
    for asset in all_assets:
        if asset not in asset_results or asset_results[asset].empty:
            print(f"  {asset:<6} {'N/A':>8}")
            continue
        best = asset_results[asset].iloc[0]
        # Collect non-default params
        param_strs = []
        for col in best.index:
            if col in ("tolerance_sec", "similarity", "matched", "missed",
                       "false_entries", "lag_median_ms", "lag_p90_ms"):
                continue
            param_strs.append(f"{col}={best[col]}")
        print(f"  {asset:<6} {best['similarity']:>8.4f} {best['matched']:>8} "
              f"{best['missed']:>8} {best['false_entries']:>8}  "
              f"{', '.join(param_strs)}")

    return asset_results


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
    print(f"  Precision:             {comparison.precision:.4f}  (matched / (matched + false))")
    print(f"  Recall:                {comparison.recall:.4f}  (matched / wallet_total)")
    print(f"  F1 score:              {comparison.similarity_score:.4f}  (harmonic mean)")
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
        print("  TOP 10 PARAMETER SETS (by F1 score)")
        print(f"{'─' * 70}")
        top = sweep_results.head(10)
        print(top.to_string(index=False))

    # --- Summary: baseline vs best sweep ---
    print(f"\n{'═' * 70}")
    print("  FINAL SUMMARY")
    print(f"{'═' * 70}")
    print(f"\n  BASELINE (default params):")
    print(f"    F1 score    = {comparison.similarity_score:.4f}")
    print(f"    precision   = {comparison.precision:.4f}")
    print(f"    recall      = {comparison.recall:.4f}")
    print(f"    matched     = {comparison.wallet_trades_matched}")
    print(f"    missed      = {comparison.wallet_trades_missed}")
    print(f"    false       = {comparison.bot_false_entries}")
    if not np.isnan(comparison.entry_lag_median):
        print(f"    lag median  = {comparison.entry_lag_median * 1000:+.1f}ms")

    if sweep_results is not None and not sweep_results.empty:
        best = sweep_results.iloc[0]
        print(f"\n  BEST SWEEP:")
        print(f"    F1 score    = {best['similarity']:.4f}")
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

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 8b -- DETAILED BREAKDOWN REPORT
# ═══════════════════════════════════════════════════════════════════════════

def print_breakdown_report(
    decisions: List[BotDecision],
    wallet_trades: pd.DataFrame,
    states: List[MarketState],
    params: StrategyParams,
    tolerance_sec: float = 3.0,
    offset_sec: float = 0.0,
) -> None:
    """Print detailed breakdown report by hour, asset, and market type.

    Sections:
      - match/miss/false rate by hour of day (ET)
      - similarity by asset (BTC/ETH/SOL/XRP)
      - similarity by market type (hourly vs 15m)
    """
    if wallet_trades.empty or "ts" not in wallet_trades.columns or not decisions:
        print("\n  No data for breakdown report.")
        return

    wt = wallet_trades.copy()
    bot_entries = [d for d in decisions if d.action in ("BUY", "SELL")]

    # Build bot entry lookup structures
    bot_token_raw = _build_token_id_index(decisions)
    bot_token_idx = {k: np.array(v) for k, v in bot_token_raw.items()}
    tmp_key: Dict[tuple, List[float]] = defaultdict(list)
    for d in decisions:
        if d.action in ("BUY", "SELL"):
            tmp_key[_make_decision_key(d)].append(d.timestamp)
    bot_key_arrays = {k: np.array(sorted(v)) for k, v in tmp_key.items()}

    # Classify each wallet trade: matched or missed, by hour/asset/market_type
    hour_stats: Dict[int, Dict[str, int]] = defaultdict(lambda: {"matched": 0, "missed": 0})
    asset_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"matched": 0, "missed": 0, "total": 0})
    mtype_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"matched": 0, "missed": 0, "total": 0})

    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        wts_adj = float(wts) + offset_sec
        w_side = _resolve_wallet_side(row)
        w_asset = _resolve_wallet_asset(row)
        w_slug = _resolve_wallet_slug(row)
        w_outcome = _resolve_wallet_outcome(row)
        w_token_id = _resolve_wallet_token_id(row)
        if not w_side:
            continue

        key = (w_asset, w_slug, w_outcome, w_side)
        lag = _match_by_token_id_or_key(
            wts_adj, w_token_id, key, bot_token_idx, bot_key_arrays, tolerance_sec,
        )
        is_matched = lag is not None

        # Hour (ET)
        try:
            dt_utc = datetime.fromtimestamp(float(wts), tz=timezone.utc)
            hour_et = dt_utc.astimezone(_ET).hour
        except (OSError, OverflowError, ValueError):
            hour_et = -1

        if is_matched:
            hour_stats[hour_et]["matched"] += 1
        else:
            hour_stats[hour_et]["missed"] += 1

        # Asset
        asset_stats[w_asset]["total"] += 1
        if is_matched:
            asset_stats[w_asset]["matched"] += 1
        else:
            asset_stats[w_asset]["missed"] += 1

        # Market type
        slug_lower = w_slug.lower()
        if "hourly" in slug_lower or "1-hour" in slug_lower:
            mtype = "hourly"
        elif "15" in slug_lower or "fifteen" in slug_lower:
            mtype = "15m"
        else:
            mtype = "other"
        mtype_stats[mtype]["total"] += 1
        if is_matched:
            mtype_stats[mtype]["matched"] += 1
        else:
            mtype_stats[mtype]["missed"] += 1

    # False bot entries by hour
    wallet_token_idx_r: Dict[str, np.ndarray] = {}
    wallet_key_arrays_r: Dict[tuple, np.ndarray] = {}
    tmp_tok2: Dict[str, List[float]] = defaultdict(list)
    tmp_key2: Dict[tuple, List[float]] = defaultdict(list)
    for _, row in wt.iterrows():
        wts = row.get("ts")
        if pd.isna(wts):
            continue
        w_side = _resolve_wallet_side(row)
        w_asset = _resolve_wallet_asset(row)
        w_slug = _resolve_wallet_slug(row)
        w_outcome = _resolve_wallet_outcome(row)
        w_token_id = _resolve_wallet_token_id(row)
        if not w_side:
            continue
        if w_token_id:
            tmp_tok2[w_token_id].append(float(wts) + offset_sec)
        tmp_key2[(w_asset, w_slug, w_outcome, w_side)].append(float(wts) + offset_sec)
    wallet_token_idx_r = {k: np.array(sorted(v)) for k, v in tmp_tok2.items()}
    wallet_key_arrays_r = {k: np.array(sorted(v)) for k, v in tmp_key2.items()}

    hour_false: Dict[int, int] = defaultdict(int)
    for d in bot_entries:
        key = _make_decision_key(d)
        lag = None
        if d.token_id:
            warr = wallet_token_idx_r.get(d.token_id)
            if warr is not None and len(warr) > 0:
                lag = _find_best_lag(warr, d.timestamp, tolerance_sec)
        if lag is None:
            warr = wallet_key_arrays_r.get(key)
            if warr is not None and len(warr) > 0:
                lag = _find_best_lag(warr, d.timestamp, tolerance_sec)
        if lag is None:
            try:
                dt_utc = datetime.fromtimestamp(d.timestamp, tz=timezone.utc)
                hour_et = dt_utc.astimezone(_ET).hour
            except (OSError, OverflowError, ValueError):
                hour_et = -1
            hour_false[hour_et] += 1

    # --- Print report ---
    print(f"\n{'═' * 80}")
    print("  BREAKDOWN REPORT")
    print(f"{'═' * 80}")

    # By hour
    print(f"\n  {'─' * 70}")
    print("  MATCH / MISS / FALSE RATE BY HOUR (ET)")
    print(f"  {'─' * 70}")
    print(f"  {'Hour':>6} {'Matched':>9} {'Missed':>9} {'False':>9} {'Match%':>8}")
    print(f"  {'─' * 50}")
    for h in range(24):
        m = hour_stats[h]["matched"]
        mi = hour_stats[h]["missed"]
        f = hour_false.get(h, 0)
        total_h = m + mi
        pct = f"{100 * m / total_h:.1f}%" if total_h > 0 else "N/A"
        if total_h > 0 or f > 0:
            print(f"  {h:>6} {m:>9} {mi:>9} {f:>9} {pct:>8}")

    # By asset
    print(f"\n  {'─' * 70}")
    print("  SIMILARITY BY ASSET")
    print(f"  {'─' * 70}")
    print(f"  {'Asset':>6} {'Matched':>9} {'Missed':>9} {'Total':>9} {'Similarity':>11}")
    print(f"  {'─' * 50}")
    for asset in sorted(asset_stats.keys()):
        s = asset_stats[asset]
        sim = s["matched"] / s["total"] if s["total"] > 0 else 0
        print(f"  {asset:>6} {s['matched']:>9} {s['missed']:>9} {s['total']:>9} {sim:>11.4f}")

    # By market type
    print(f"\n  {'─' * 70}")
    print("  SIMILARITY BY MARKET TYPE")
    print(f"  {'─' * 70}")
    print(f"  {'Type':>8} {'Matched':>9} {'Missed':>9} {'Total':>9} {'Similarity':>11}")
    print(f"  {'─' * 50}")
    for mtype in sorted(mtype_stats.keys()):
        s = mtype_stats[mtype]
        sim = s["matched"] / s["total"] if s["total"] > 0 else 0
        print(f"  {mtype:>8} {s['matched']:>9} {s['missed']:>9} {s['total']:>9} {sim:>11.4f}")

    print(f"\n{'═' * 80}")


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


def recommend_config(
    signal_dist: pd.DataFrame,
    miss_reasons: Dict[str, int],
    params: StrategyParams,
) -> None:
    """Print a RECOMMENDED_CONFIG block based on wallet signal distributions
    and missed-trade attribution.

    Uses wallet entry quantiles to suggest gate thresholds that would have
    captured more wallet trades while still filtering noise.
    """
    print(f"\n  {'═' * 80}")
    print(f"  RECOMMENDED_CONFIG  (auto-suggested from wallet signal quantiles)")
    print(f"  {'═' * 80}")

    recs: List[Tuple[str, str, str]] = []  # (env_var, value, rationale)

    # Map signal_dist metric names to gate params
    # signal_dist index: "spread_cents_at_trade", "spread_percentile_at_trade",
    #   "orderbook_imbalance_at_trade", "binance_ret_30s_at_trade (dec)"
    metric_map = {
        "spread_cents_at_trade": ("ENTRY_MIN_SPREAD_CENTS", "entry_min_spread_cents", "p25"),
        "spread_percentile_at_trade": ("SPREAD_PCTL_MIN", "entry_min_spread_pctl", "p25"),
        "orderbook_imbalance_at_trade": ("OB_IMBALANCE_MIN", "entry_min_imbalance", "p25"),
        "binance_ret_30s_at_trade (dec)": ("VOL_MIN_RET_30S", "entry_min_ret_30s", "p25"),
    }

    if not signal_dist.empty:
        for metric, (env_var, param_attr, quantile) in metric_map.items():
            if metric not in signal_dist.index:
                continue
            row = signal_dist.loc[metric]
            wallet_q = float(row[quantile])
            current_val = getattr(params, param_attr)

            # For |ret_30s|, use abs of the p25
            if "ret_30s" in metric:
                wallet_q = abs(wallet_q)

            # Recommend the lower of current and wallet p25 (to capture more trades)
            if wallet_q < current_val:
                recs.append((env_var, f"{wallet_q:.6f}",
                             f"wallet p25={wallet_q:.6f} < current={current_val:.6f}  "
                             f"(lowering to capture more wallet trades)"))
            else:
                recs.append((env_var, f"{current_val:.6f}",
                             f"current={current_val:.6f} already <= wallet p25={wallet_q:.6f}  "
                             f"(keep current)"))

    # Cooldown recommendation based on miss_reasons
    cooldown_misses = miss_reasons.get("cooldown_or_key", 0)
    total_misses = sum(miss_reasons.values())
    if total_misses > 0 and cooldown_misses / max(1, total_misses) > 0.2:
        new_cd = max(0, params.entry_cooldown_sec - 10)
        recs.append(("ENTRY_COOLDOWN_SEC", f"{new_cd:.0f}",
                     f"{cooldown_misses}/{total_misses} misses from cooldown/key  "
                     f"(reduce cooldown from {params.entry_cooldown_sec:.0f}s)"))

    # Gate-specific: if a gate causes >30% of misses, suggest lowering threshold
    for gate_name, param_attr, env_var in [
        ("spread_cents", "entry_min_spread_cents", "ENTRY_MIN_SPREAD_CENTS"),
        ("spread_pctl", "entry_min_spread_pctl", "SPREAD_PCTL_MIN"),
        ("|ret_30s|", "entry_min_ret_30s", "VOL_MIN_RET_30S"),
        ("imbalance", "entry_min_imbalance", "OB_IMBALANCE_MIN"),
    ]:
        gate_misses = miss_reasons.get(gate_name, 0)
        if total_misses > 0 and gate_misses / max(1, total_misses) > 0.3:
            current = getattr(params, param_attr)
            suggested = current * 0.8  # 20% lower
            recs.append((env_var, f"{suggested:.6f}",
                         f"gate '{gate_name}' blocked {gate_misses}/{total_misses} misses  "
                         f"(reduce from {current:.6f} by ~20%)"))

    if not recs:
        print("  No recommendations available (insufficient data).")
    else:
        # Deduplicate by env_var, keeping the most aggressive recommendation
        seen: Dict[str, Tuple[str, str]] = {}
        for env_var, val, rationale in recs:
            if env_var not in seen:
                seen[env_var] = (val, rationale)
            else:
                # Keep the lower (more permissive) value
                if float(val) < float(seen[env_var][0]):
                    seen[env_var] = (val, rationale)

        print(f"\n  # Paste into .env or export:")
        for env_var, (val, rationale) in sorted(seen.items()):
            print(f"  {env_var}={val}    # {rationale}")

    print(f"  {'═' * 80}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Market Replay & Strategy Comparison")
    parser.add_argument("--data-dir", type=str, default=None,
                        help="Directory containing the CSV log files")
    parser.add_argument("--optimize", action="store_true",
                        help="Run parameter sweep optimization")
    parser.add_argument("--no-charts", action="store_true",
                        help="Skip chart generation")
    parser.add_argument("--tolerance-sec", type=float, default=7.0,
                        help="Matching tolerance in seconds (default: 7.0)")
    parser.add_argument("--debug", action="store_true",
                        help="Print debug examples of unmatched wallet trades")
    parser.add_argument("--offset-sec", type=float, default=None,
                        help="Manual timestamp offset (auto-fit if not provided)")
    parser.add_argument("--focused-sweep", action="store_true",
                        help="Run focused sweep (momentum + spread + imbalance grid)")
    parser.add_argument("--no-deep-diag", action="store_true",
                        help="Skip deep diagnostics (saves time)")
    parser.add_argument("--per-asset-sweep", action="store_true",
                        help="Run independent parameter sweep per crypto asset (BTC/ETH/SOL/XRP)")
    parser.add_argument("--concise", action="store_true",
                        help="Only print final summary, asset breakdown, and gate failures")
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

    concise = getattr(args, 'concise', False)

    # --- Step 3b: Diagnose why bot may produce no entries ---
    if not concise:
        print("\n[3b/8] Diagnosing bot entry gate failures ...")
        diagnose_no_entries(states, engine.wallet_trades, default_params, tolerance_sec=args.tolerance_sec)

    # --- Step 4: Investigate timestamp lag ---
    if args.offset_sec is not None:
        offset_sec = args.offset_sec
        if not concise:
            print(f"\n[4/8] Using manual offset: {offset_sec:+.1f}s")
    else:
        if not concise:
            print("\n[4/8] Investigating timestamp lag ...")
        offset_sec = print_lag_investigation(
            states, engine.wallet_trades, default_params,
            tolerance_sec=max(args.tolerance_sec, 10.0),
            quiet=concise,
        )

    # --- Step 5: Compare to wallet trades (with offset) ---
    if not concise:
        print("\n[5/8] Comparing to wallet trades ...")
    comparison = compare_to_wallet(
        decisions, engine.wallet_trades, args.tolerance_sec,
        debug=args.debug, offset_sec=offset_sec, quiet=concise,
    )
    if not concise:
        print(f"  F1 score: {comparison.similarity_score:.4f}  (precision={comparison.precision:.4f}, recall={comparison.recall:.4f})")
        if comparison.wallet_trades_matched == 0 or np.isnan(comparison.entry_lag_median):
            print(f"  Entry lag median: N/A  |  p90: N/A  (no matched trades)")
        else:
            print(f"  Entry lag median: {comparison.entry_lag_median * 1000:+.1f}ms  |  p90: {comparison.entry_lag_p90 * 1000:+.1f}ms")
        if offset_sec != 0:
            print(f"  (with offset={offset_sec:+.1f}s applied to wallet timestamps)")

    # --- Step 5b: Debug dumps ---
    miss_reasons = {}
    if not concise:
        print("\n[5b/8] Generating debug dumps ...")
        miss_reasons = dump_missed_wallet_trades(
            states, engine.wallet_trades, decisions, default_params,
            tolerance_sec=args.tolerance_sec, max_rows=50,
        )
        dump_false_bot_entries(
            states, engine.wallet_trades, decisions, default_params,
            tolerance_sec=args.tolerance_sec, max_rows=50,
        )

    # --- Step 5c: Deep diagnostics ---
    if not concise and not args.no_deep_diag:
        print("\n[5c/8] Running deep diagnostics ...")
        deep_diagnostics(
            states, engine.wallet_trades, decisions, default_params,
            tolerance_sec=args.tolerance_sec, offset_sec=offset_sec,
        )
    elif not concise:
        print("\n[5c/8] Skipping deep diagnostics (--no-deep-diag)")

    # --- Step 6: Signal distribution analysis ---
    if not concise:
        print("\n[6/8] Analyzing signal distributions at wallet entry ...")
    signal_dist = analyze_signal_distributions(engine.wallet_trades)

    # --- Step 6b: Auto-suggest config ---
    if not concise:
        print("\n[6b/9] Computing recommended config ...")
        recommend_config(signal_dist, miss_reasons, default_params)

    # --- Step 7: Parameter optimization (optional) ---
    sweep_results = None
    if args.optimize:
        print("\n[7/9] Running parameter sweep optimization ...")
        sweep_results = parameter_sweep(
            states, engine.wallet_trades,
            tolerance_sec=args.tolerance_sec, offset_sec=offset_sec,
        )
    elif getattr(args, 'focused_sweep', False):
        print("\n[7/9] Running FOCUSED parameter sweep ...")
        sweep_results = parameter_sweep(
            states, engine.wallet_trades,
            sweep_ranges=FOCUSED_SWEEP_RANGES,
            tolerance_sec=args.tolerance_sec, offset_sec=offset_sec,
        )
    elif getattr(args, 'per_asset_sweep', False):
        print("\n[7/9] Running PER-ASSET parameter sweep ...")
        asset_results = per_asset_sweep(
            states, engine.wallet_trades,
            tolerance_sec=args.tolerance_sec, offset_sec=offset_sec,
        )
        # Use the first non-empty result as sweep_results for the report
        for _a, _df in asset_results.items():
            if not _df.empty:
                sweep_results = _df
                break
    else:
        print("\n[7/9] Skipping parameter sweep (use --optimize, --focused-sweep, or --per-asset-sweep)")

    # --- Step 8: Report ---
    if concise:
        # Concise mode: just the key numbers
        print("\n" + "=" * 60)
        print("  RESULTS")
        print("=" * 60)
        print(f"  F1={comparison.similarity_score:.4f}  precision={comparison.precision:.4f}  recall={comparison.recall:.4f}")
        print(f"  matched={comparison.wallet_trades_matched}  missed={comparison.wallet_trades_missed}  false={comparison.bot_false_entries}")
        if not np.isnan(comparison.entry_lag_median):
            print(f"  lag: median={comparison.entry_lag_median*1000:+.0f}ms  p90={comparison.entry_lag_p90*1000:+.0f}ms")
    else:
        print_report(comparison, signal_dist, sweep_results)

    # --- Step 8b: Breakdown report ---
    print("\n[8b] Generating breakdown report (by hour, asset, market type) ...")
    print_breakdown_report(
        decisions, engine.wallet_trades, states, default_params,
        tolerance_sec=args.tolerance_sec, offset_sec=offset_sec,
    )

    # --- Charts ---
    if not args.no_charts and not concise:
        print("\n  Generating charts ...")
        generate_charts(decisions, engine.wallet_trades, signal_dist)

    print("\n  Done.\n")


if __name__ == "__main__":
    main()
