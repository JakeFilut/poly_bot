"""
config.py – All configuration for the F247-style Polymarket scalper.

Every knob is an env-var with a safe default.  Validated once at startup.
Secrets are redacted when printed.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field, fields
from typing import List


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_float(key: str, default: float) -> float:
    return float(_env(key, str(default)))


def _env_int(key: str, default: int) -> int:
    return int(_env(key, str(default)))


def _env_bool(key: str, default: bool) -> bool:
    return _env(key, str(default)).lower() in ("1", "true", "yes")


def _env_list_float(key: str, default: List[float]) -> List[float]:
    raw = _env(key, "")
    if not raw:
        return list(default)
    return [float(x.strip()) for x in raw.split(",")]


def _env_list_int(key: str, default: List[int]) -> List[int]:
    raw = _env(key, "")
    if not raw:
        return list(default)
    return [int(x.strip()) for x in raw.split(",")]


# ---------------------------------------------------------------------------
# Dataclass config – every field is populated from env
# ---------------------------------------------------------------------------
@dataclass
class Config:
    # -- Mode --
    MODE: str = ""  # DRY_RUN | LIVE
    DRY_RUN_FILL_MODE: str = "probabilistic"  # none | probabilistic | instant | touch
    DRY_RUN_SELFTEST: bool = False   # Force-fill next N orders to prove pipeline
    DRY_RUN_SELFTEST_N: int = 10     # Number of orders to force-fill in selftest

    # -- Polymarket credentials --
    POLYMARKET_API_KEY: str = ""
    POLYMARKET_API_SECRET: str = ""
    POLYMARKET_API_PASSPHRASE: str = ""
    POLYMARKET_PRIVATE_KEY: str = ""  # Wallet private key for signing
    POLYMARKET_FUNDER: str = ""  # Funder/proxy wallet address (optional)
    WALLET_ADDRESS: str = ""  # Optional: your wallet address

    # -- CLOB endpoint --
    CLOB_API_URL: str = "https://clob.polymarket.com"
    GAMMA_API_URL: str = "https://gamma-api.polymarket.com"

    # -- Universe --
    MAX_ACTIVE_SLUGS: int = 60
    ASSETS: List[str] = field(default_factory=lambda: ["BTC", "ETH", "SOL", "XRP"])

    # -- Loop timing --
    TARGET_LOOP_MS: int = 10  # decision loop tick (ms); was 500 before hot-snapshot arch

    # -- Background task intervals (ms) --
    BINANCE_POLL_MS: int = 500    # Binance price refresh cadence
    BOOK_POLL_MS: int = 300       # Order-book refresh cadence (all tokens, concurrent)
    FILLS_POLL_MS: int = 1000     # Fill sync cadence (LIVE only)
    BOOK_MAX_STALE_MS: int = 5000 # Refuse order-book data older than this
    BINANCE_MAX_STALE_MS: int = 3000   # Refuse Binance data older than this
    UNIVERSE_MAX_STALE_MS: int = 300000 # Refuse universe older than 5 min
    BOOK_MAX_INFLIGHT: int = 10   # Max concurrent order-book HTTP requests

    # -- Order management --
    MAX_OPEN_ORDERS_PER_MARKET: int = 3
    ORDER_TTL_MS: int = 2500
    MAX_ORDER_OPS_PER_LOOP: int = 20

    # -- Position limits --
    MAX_POSITION_USD_PER_OUTCOME: float = 150.0
    MAX_TOTAL_EXPOSURE_USD: float = 1500.0
    MIN_CASH_USD: float = 50.0

    # -- Spread / gating --
    SPREAD_PCTL_MIN: float = 0.85     # tightened from 0.80
    SPREAD_MAX_SANE: float = 0.08  # 8 cents max (reject illiquid tokens earlier)
    BIN_RET30_THRESHOLD: float = 0.0

    # -- Volatility filter (entry gate) --
    VOL_MIN_RET_30S: float = 0.0      # disabled; wallet median ret_30s=0.0
    VOL_MIN_RET_120S: float = 0.0     # disabled; wallet doesn't require momentum

    # -- Orderbook imbalance gate --
    OB_IMBALANCE_MIN: float = 0.40    # f247 data: BUY sweet spot is 0.4-0.7; lowered from 0.50
    OB_IMBALANCE_MAX: float = 0.70    # f247 data: imb>0.7 = chasing (-0.3c markout); cap it
    OB_IMBALANCE_BAND_ENABLED: bool = True  # use min/max band instead of just min

    # -- Take-profit / stop-loss (in cents i.e. 0.06 = 6¢) --
    # f247 data: wallet markout grows from +0.9c(2s) to +1.1c(30s) to +1.0c(60s)
    # wallet median hold=64s, mean=190s — it lets winners run
    TP_CENTS_MIN: float = 0.08         # lowered: start taking profit earlier (was 0.10)
    TP_CENTS_TARGET: float = 0.12      # lowered: wallet avg markout is ~1c (was 0.14)
    TP_CENTS_MAX: float = 0.20         # slightly lower max (was 0.22)
    SL_CENTS: float = 0.04
    # Per-asset TP adjustments (multiplier on TP_CENTS_TARGET)
    TP_MULT_BTC: float = 0.8   # f247: BTC fades at 60s, take profit faster
    TP_MULT_ETH: float = 1.3   # f247: ETH markout grows to +1.25c at 60s, let it run
    TP_MULT_SOL: float = 1.1   # f247: SOL steady +1.4c, slight premium
    TP_MULT_XRP: float = 1.0   # f247: XRP steady +1.0c, baseline

    # -- Sell sizing --
    # f247: wallet sells are 64% passive, avg reduce size=15.9 shares
    # REDUCE trades have +1.2c markout — wallet is good at timing exits
    SELL_FRAC_MED: float = 0.05       # bumped: wallet shaves more aggressively (was 0.036)
    SELL_FRAC_MAX: float = 0.20       # bumped: allow larger exit clips (was 0.15)
    SELL_MIN_SHARES: float = 5.0  # Minimum sell clip to avoid dust

    # -- Buy sizing --
    CLIP_UNIT_USD: float = 1.10
    CLIP_LADDER_MULTS: List[int] = field(default_factory=lambda: [1, 3, 8, 10, 12])
    # f247 data: wallet avg size=16 shares, sweet spot 5-15 shares
    # SOL/XRP have +1.3c markout vs BTC +0.7c at 60s → bump SOL/XRP allocation
    CLIP_SIZE_MULT_BTC: float = 0.9    # BTC fades at 60s (+0.7c), slightly less
    CLIP_SIZE_MULT_ETH: float = 1.0    # ETH needs longer hold but decent
    CLIP_SIZE_MULT_SOL: float = 1.3    # SOL best performer (+1.4c), more size
    CLIP_SIZE_MULT_XRP: float = 1.2    # XRP solid (+1.0c at 60s), bump up

    # -- Inventory laddering (BUY side) --
    LADDER_LEVELS: int = 2                       # 2-level passive ladder (wallet style)
    LADDER_STEP_CENTS: float = 1.0               # spacing between levels (1 cent)
    LADDER_SPLIT: List[float] = field(default_factory=lambda: [0.625, 0.375])
    LADDER_MAX_TOTAL_ORDERS_PER_TOKEN: int = 3   # safety: max open BUY orders per (slug,outcome)
    LADDER_ONLY_IF_SPREAD_CENTS_GTE: float = 2.0 # only ladder when spread >= 2c
    LADDER_ONLY_IF_SPREAD_PCTL_GTE: float = 0.90 # wallet only ladders at high spread pctl
    LADDER_LEVEL_CAP_BPS_FROM_MID: float = 150.0 # don't bid too far below mid

    # -- Order precision (Polymarket rules) --
    PRICE_MIN: float = 0.01
    PRICE_MAX: float = 0.99
    MIN_ORDER_SHARES: float = 1.0  # minimum shares per order

    # -- Aggression --
    CROSS_PROB_1C: float = 0.0   # never cross at 1¢ spread (wallet is passive)
    CROSS_MIN_RET_30S: float = 0.003   # only cross on very strong momentum
    CROSS_MIN_SPREAD_PCTL: float = 0.95  # only cross when spread pctl >= 0.95
    CROSS_ENABLED: bool = False  # f247 data: crossing trades are -2.5c markout; disable by default
    # When CROSS_ENABLED=True, only cross if size <= this (big crosses are worst)
    CROSS_MAX_SIZE_SHARES: float = 15.0  # f247: 60+ share trades = -3.8c markout

    # -- Entry quality gates --
    ENTRY_MIN_EDGE_CENTS: float = 0.04   # require at least 4¢ expected edge
    ENTRY_MIN_EDGE_BPS: float = 400.0    # 4% edge vs cost (informational)
    ENTRY_MAX_OFFSET_FROM_BID: float = 0.005  # max 0.5¢ above best_bid
    ENTRY_MIN_SPREAD_CENTS: float = 1.0  # aligned: wallet median spread=1¢ (was 1.5)
    ENTRY_MIN_SPREAD_PCTL: float = 0.85  # wallet p25=83%, keep 85% as floor
    ENTRY_PREFER_SPREAD_PCTL: float = 0.92  # preferred spread pctl (soft bonus)
    ENTRY_MIN_ABS_RET_30S: float = 0.0   # disabled: wallet median ret_30s=0.0 (was 0.0002)
    ENTRY_MIN_RET_30S: float = 0.0       # legacy alias (synced)
    ENTRY_MIN_RET_60S: float = 0.0004    # focused sweep best: require |ret_60s| >= 0.04%
    ENTRY_MIN_SECONDS_TO_RESOLUTION: float = 120.0  # focused sweep best: require 2min+ to resolution
    ENTRY_LATE_CUTOFF_SEC: int = 600     # stop entering after 10 min
    ENTRY_AVG_UP_TOLERANCE: float = 0.005 # max 0.5¢ above avg cost
    ENTRY_MAX_SIZE_SHARES: float = 30.0  # f247: 30-60 marginal, 60+ negative; cap at 30

    # -- Directional imbalance gate --
    ENTRY_MIN_IMBALANCE: float = 0.1     # focused sweep best: light imbalance filter cuts false entries
    ENTRY_IMBALANCE_DIRECTIONAL: bool = False  # disabled: sweep confirmed non-directional is better
    ENTRY_IMBALANCE_ENABLED: bool = True  # set False to disable imbalance gate entirely

    # -- Time-of-day entry filter (ET hours where entries are suppressed) --
    # Hours 3,5,7,8 ET have <16% match rate in replay; skip them
    ENTRY_QUIET_HOURS_ET: List[int] = field(default_factory=lambda: [3, 5, 7, 8])

    # -- Book depth gate (new) --
    ENTRY_MIN_BOOK_DEPTH: float = 50.0    # minimum bid+ask size (shares) to enter
    ENTRY_MIN_BID_SIZE: float = 20.0      # minimum bid-side depth (shares)

    # -- Momentum acceleration gate (new) --
    ENTRY_MIN_RET_ACCEL: float = 0.0      # min (ret_30s - ret_120s); 0 = disabled

    # -- Cadence (15-min) --
    BUY_HEAVY_SEC: int = 60  # first N seconds: heavy buy
    BUY_MED_SEC: int = 120  # 60-120s: medium buy
    SELL_START_SEC: int = 300  # minute 5
    SELL_END_SEC: int = 360  # minute 6
    SELL_BURST2_START: int = 600  # minute 10
    SELL_BURST2_END: int = 660
    SELL_BURST3_START: int = 780  # minute 13
    SELL_BURST3_END: int = 840

    # -- Universe refresh --
    UNIVERSE_REFRESH_SEC: int = 120
    UNIVERSE_DEBUG: bool = False  # Log every candidate market with pass/reject reason

    # -- State persistence --
    STATE_DB_PATH: str = "/home/ubuntu/github/logs/poly_bot/state.db"
    STATE_FLUSH_SEC: int = 10

    # -- Per-token cooldown (after a fill, wait before placing another order) --
    # f247: 56% of trades within 2s of previous (burst trading), median gap=2s
    # but wallet bursts are on SAME slug; cross-slug cooldown should be shorter
    PER_TOKEN_COOLDOWN_SEC: float = 5.0   # lowered: wallet trades every 2s in bursts (was 10.0)
    MIN_CANCEL_REPLACE_INTERVAL_SEC: float = 0.5  # min time between cancel/replace ops
    MAX_CANCEL_REPLACE_PER_SEC: int = 4  # global cap on cancel/replace ops per second
    MIN_PRICE_CHANGE_FOR_REPLACE: float = 0.01  # 1¢ minimum price change to justify replace

    # -- Risk cooldown --
    ERROR_COOLDOWN_BASE_SEC: float = 2.0
    ERROR_COOLDOWN_MAX_SEC: float = 60.0

    # -- Retry --
    RETRY_MAX: int = 3
    RETRY_BACKOFF_BASE: float = 0.5

    # -- Logging --
    LOG_FILE: str = "/home/ubuntu/github/logs/poly_bot/bot_log.jsonl"
    LOG_ROLLUP_SEC: int = 60
    DEBUG_LOG_ORDERS: bool = False  # Emit DRY_RUN_ORDER events (verbose; off by default)

    # -- Decision log throttling --
    LOG_DECISIONS: bool = False  # If True, log every DECISION tick (old behaviour)
    DECISION_LOG_SAMPLE_EVERY_N: int = 200  # Log 1 out of N SKIP ticks per asset
    DECISION_LOG_MIN_INTERVAL_MS: int = 2000  # Time-based throttle per (slug, reason)

    # -- Heartbeat --
    HEARTBEAT_LOG_MS: int = 5000  # Heartbeat log interval (ms)

    # -- Log rotation --
    LOG_ROTATE_MAX_BYTES: int = 50_000_000  # 50 MB per log file
    LOG_ROTATE_BACKUP_COUNT: int = 5  # Keep 5 rotated backups

    # -- LIVE risk controls (only enforced when MODE=LIVE) --
    LIVE_MAX_CAPITAL_USD: float = 400.0          # Max deployed cost basis + pending buys
    MAX_DAILY_LOSS_USD: float = 100.0            # Hard daily kill switch threshold
    MAX_INTRADAY_DRAWDOWN_USD: float = 60.0      # Drawdown pause threshold
    MAX_CAPITAL_PER_ASSET_USD: float = 150.0     # Per-asset cost basis cap
    MAX_CROSS_NOTIONAL_PER_HOUR_USD: float = 100.0  # Hourly crossing/taker budget
    PAUSE_MINUTES_ON_DD: int = 45                # Minutes to pause on drawdown breach

    # -- F247 copy-wallet tracker --
    TRACK_F247_WALLET: bool = False  # Disabled: no longer using wallet-copy strategy

    # -- Convergence strategy params (from independent_backtest sweep) --
    CONV_MID_BUY_THRESH: float = 0.60    # buy Up when Up-token mid > this
    CONV_MID_SELL_THRESH: float = 0.48   # sell Up (buy Down) when Up-token mid < this
    CONV_LOOKBACK_SEC: int = 5           # seconds to look back for dip/rip detection
    CONV_MIN_DIP_CENTS: float = 0.0      # minimum dip in cents to trigger (0 = disabled)
    CONV_MIN_IMB_CHANGE: float = 0.0     # imbalance change threshold (0 = disabled)
    CONV_HOLD_SEC: int = 300             # seconds to hold before time-based exit
    CONV_STOP_LOSS_CENTS: float = 0.0    # disabled — best sweep had no SL
    CONV_TAKE_PROFIT_CENTS: float = 0.0  # disabled — best sweep had no TP
    CONV_TRAILING_STOP_CENTS: float = 0.0  # disabled — best sweep had no trailing stop
    CONV_POSITION_SIZE: float = 10.0     # shares per trade
    CONV_HOURLY_BUDGET_USD: float = 500.0  # max USD to invest per hour (sells refund budget)
    CONV_EXCLUDE_ASSETS: tuple = ("BTC",)  # exclude BTC (Phase 2: $700 vs $567 without)

    # -- Fee simulation --
    SIM_FEE_BPS: float = 0.0  # Simulated fee in basis points (e.g., 5.0 = 5 bps = 0.05%)

    # -- Secrets to redact --
    _SECRETS: List[str] = field(
        default_factory=lambda: [
            "POLYMARKET_API_KEY",
            "POLYMARKET_API_SECRET",
            "POLYMARKET_API_PASSPHRASE",
            "POLYMARKET_PRIVATE_KEY",
        ],
        repr=False,
    )

    def redacted_dict(self) -> dict:
        """Return config as dict with secrets redacted."""
        d = {}
        for f in fields(self):
            if f.name.startswith("_"):
                continue
            val = getattr(self, f.name)
            if f.name in self._SECRETS and val:
                d[f.name] = val[:4] + "****"
            else:
                d[f.name] = val
        return d


def load_config() -> Config:
    """Load config from env vars, validate, return frozen Config."""
    cfg = Config(
        MODE=_env("MODE", "DRY_RUN").upper(),
        DRY_RUN_FILL_MODE=_env("DRY_RUN_FILL_MODE", "probabilistic").lower(),
        DRY_RUN_SELFTEST=_env_bool("DRY_RUN_SELFTEST", False),
        DRY_RUN_SELFTEST_N=_env_int("DRY_RUN_SELFTEST_N", 10),
        POLYMARKET_API_KEY=_env("POLYMARKET_API_KEY", ""),
        POLYMARKET_API_SECRET=_env("POLYMARKET_API_SECRET", ""),
        POLYMARKET_API_PASSPHRASE=_env("POLYMARKET_API_PASSPHRASE", ""),
        POLYMARKET_PRIVATE_KEY=_env("POLYMARKET_PRIVATE_KEY", ""),
        POLYMARKET_FUNDER=_env("POLYMARKET_FUNDER", ""),
        WALLET_ADDRESS=_env("WALLET_ADDRESS", ""),
        CLOB_API_URL=_env("CLOB_API_URL", "https://clob.polymarket.com"),
        GAMMA_API_URL=_env("GAMMA_API_URL", "https://gamma-api.polymarket.com"),
        MAX_ACTIVE_SLUGS=_env_int("MAX_ACTIVE_SLUGS", 60),
        ASSETS=_env("ASSETS", "BTC,ETH,SOL,XRP").split(","),
        TARGET_LOOP_MS=_env_int("TARGET_LOOP_MS", 10),
        BINANCE_POLL_MS=_env_int("BINANCE_POLL_MS", 500),
        BOOK_POLL_MS=_env_int("BOOK_POLL_MS", 300),
        FILLS_POLL_MS=_env_int("FILLS_POLL_MS", 1000),
        BOOK_MAX_STALE_MS=_env_int("BOOK_MAX_STALE_MS", 5000),
        BINANCE_MAX_STALE_MS=_env_int("BINANCE_MAX_STALE_MS", 3000),
        UNIVERSE_MAX_STALE_MS=_env_int("UNIVERSE_MAX_STALE_MS", 300000),
        BOOK_MAX_INFLIGHT=_env_int("BOOK_MAX_INFLIGHT", 10),
        MAX_OPEN_ORDERS_PER_MARKET=_env_int("MAX_OPEN_ORDERS_PER_MARKET", 3),
        ORDER_TTL_MS=_env_int("ORDER_TTL_MS", 2500),
        MAX_ORDER_OPS_PER_LOOP=_env_int("MAX_ORDER_OPS_PER_LOOP", 20),
        MAX_POSITION_USD_PER_OUTCOME=_env_float("MAX_POSITION_USD_PER_OUTCOME", 150.0),
        MAX_TOTAL_EXPOSURE_USD=_env_float("MAX_TOTAL_EXPOSURE_USD", 1500.0),
        MIN_CASH_USD=_env_float("MIN_CASH_USD", 50.0),
        SPREAD_PCTL_MIN=_env_float("SPREAD_PCTL_MIN", 0.85),
        SPREAD_MAX_SANE=_env_float("SPREAD_MAX_SANE", 0.08),
        BIN_RET30_THRESHOLD=_env_float("BIN_RET30_THRESHOLD", 0.0),
        VOL_MIN_RET_30S=_env_float("VOL_MIN_RET_30S", 0.0),
        OB_IMBALANCE_MIN=_env_float("OB_IMBALANCE_MIN", 0.40),
        OB_IMBALANCE_MAX=_env_float("OB_IMBALANCE_MAX", 0.70),
        OB_IMBALANCE_BAND_ENABLED=_env_bool("OB_IMBALANCE_BAND_ENABLED", True),
        VOL_MIN_RET_120S=_env_float("VOL_MIN_RET_120S", 0.0),
        TP_CENTS_MIN=_env_float("TP_CENTS_MIN", 0.08),
        TP_CENTS_TARGET=_env_float("TP_CENTS_TARGET", 0.12),
        TP_CENTS_MAX=_env_float("TP_CENTS_MAX", 0.20),
        SL_CENTS=_env_float("SL_CENTS", 0.04),
        TP_MULT_BTC=_env_float("TP_MULT_BTC", 0.8),
        TP_MULT_ETH=_env_float("TP_MULT_ETH", 1.3),
        TP_MULT_SOL=_env_float("TP_MULT_SOL", 1.1),
        TP_MULT_XRP=_env_float("TP_MULT_XRP", 1.0),
        SELL_FRAC_MED=_env_float("SELL_FRAC_MED", 0.05),
        SELL_FRAC_MAX=_env_float("SELL_FRAC_MAX", 0.20),
        SELL_MIN_SHARES=_env_float("SELL_MIN_SHARES", 5.0),
        CLIP_UNIT_USD=_env_float("CLIP_UNIT_USD", 1.10),
        CLIP_LADDER_MULTS=_env_list_int("CLIP_LADDER_MULTS", [1, 3, 8, 10, 12]),
        CLIP_SIZE_MULT_BTC=_env_float("CLIP_SIZE_MULT_BTC", 0.9),
        CLIP_SIZE_MULT_ETH=_env_float("CLIP_SIZE_MULT_ETH", 1.0),
        CLIP_SIZE_MULT_SOL=_env_float("CLIP_SIZE_MULT_SOL", 1.3),
        CLIP_SIZE_MULT_XRP=_env_float("CLIP_SIZE_MULT_XRP", 1.2),
        LADDER_LEVELS=_env_int("LADDER_LEVELS", 2),
        LADDER_STEP_CENTS=_env_float("LADDER_STEP_CENTS", 1.0),
        LADDER_SPLIT=_env_list_float("LADDER_SPLIT", [0.625, 0.375]),
        LADDER_MAX_TOTAL_ORDERS_PER_TOKEN=_env_int("LADDER_MAX_TOTAL_ORDERS_PER_TOKEN", 3),
        LADDER_ONLY_IF_SPREAD_CENTS_GTE=_env_float("LADDER_ONLY_IF_SPREAD_CENTS_GTE", 2.0),
        LADDER_ONLY_IF_SPREAD_PCTL_GTE=_env_float("LADDER_ONLY_IF_SPREAD_PCTL_GTE", 0.90),
        LADDER_LEVEL_CAP_BPS_FROM_MID=_env_float("LADDER_LEVEL_CAP_BPS_FROM_MID", 150.0),
        CROSS_PROB_1C=_env_float("CROSS_PROB_1C", 0.0),
        CROSS_MIN_RET_30S=_env_float("CROSS_MIN_RET_30S", 0.003),
        CROSS_MIN_SPREAD_PCTL=_env_float("CROSS_MIN_SPREAD_PCTL", 0.95),
        CROSS_ENABLED=_env_bool("CROSS_ENABLED", False),
        CROSS_MAX_SIZE_SHARES=_env_float("CROSS_MAX_SIZE_SHARES", 15.0),
        ENTRY_MIN_EDGE_CENTS=_env_float("ENTRY_MIN_EDGE_CENTS", 0.04),
        ENTRY_MIN_EDGE_BPS=_env_float("ENTRY_MIN_EDGE_BPS", 400.0),
        ENTRY_MAX_OFFSET_FROM_BID=_env_float("ENTRY_MAX_OFFSET_FROM_BID", 0.005),
        ENTRY_MIN_SPREAD_CENTS=_env_float("ENTRY_MIN_SPREAD_CENTS", 1.0),
        ENTRY_MIN_SPREAD_PCTL=_env_float("ENTRY_MIN_SPREAD_PCTL", 0.85),
        ENTRY_PREFER_SPREAD_PCTL=_env_float("ENTRY_PREFER_SPREAD_PCTL", 0.92),
        ENTRY_MIN_ABS_RET_30S=_env_float("ENTRY_MIN_ABS_RET_30S", 0.0),
        ENTRY_MIN_RET_30S=_env_float("ENTRY_MIN_RET_30S", 0.0),
        ENTRY_MIN_RET_60S=_env_float("ENTRY_MIN_RET_60S", 0.0004),
        ENTRY_MIN_SECONDS_TO_RESOLUTION=_env_float("ENTRY_MIN_SECONDS_TO_RESOLUTION", 120.0),
        ENTRY_LATE_CUTOFF_SEC=_env_int("ENTRY_LATE_CUTOFF_SEC", 600),
        ENTRY_AVG_UP_TOLERANCE=_env_float("ENTRY_AVG_UP_TOLERANCE", 0.005),
        ENTRY_MAX_SIZE_SHARES=_env_float("ENTRY_MAX_SIZE_SHARES", 30.0),
        ENTRY_MIN_IMBALANCE=_env_float("ENTRY_MIN_IMBALANCE", 0.1),
        ENTRY_IMBALANCE_DIRECTIONAL=_env_bool("ENTRY_IMBALANCE_DIRECTIONAL", False),
        ENTRY_IMBALANCE_ENABLED=_env_bool("ENTRY_IMBALANCE_ENABLED", True),
        ENTRY_MIN_BOOK_DEPTH=_env_float("ENTRY_MIN_BOOK_DEPTH", 50.0),
        ENTRY_MIN_BID_SIZE=_env_float("ENTRY_MIN_BID_SIZE", 20.0),
        ENTRY_MIN_RET_ACCEL=_env_float("ENTRY_MIN_RET_ACCEL", 0.0),
        ENTRY_QUIET_HOURS_ET=_env_list_int("ENTRY_QUIET_HOURS_ET", [3, 5, 7, 8]),
        BUY_HEAVY_SEC=_env_int("BUY_HEAVY_SEC", 60),
        BUY_MED_SEC=_env_int("BUY_MED_SEC", 120),
        SELL_START_SEC=_env_int("SELL_START_SEC", 300),
        SELL_END_SEC=_env_int("SELL_END_SEC", 360),
        SELL_BURST2_START=_env_int("SELL_BURST2_START", 600),
        SELL_BURST2_END=_env_int("SELL_BURST2_END", 660),
        SELL_BURST3_START=_env_int("SELL_BURST3_START", 780),
        SELL_BURST3_END=_env_int("SELL_BURST3_END", 840),
        UNIVERSE_REFRESH_SEC=_env_int("UNIVERSE_REFRESH_SEC", 120),
        UNIVERSE_DEBUG=_env_bool("UNIVERSE_DEBUG", False),
        STATE_DB_PATH=_env("STATE_DB_PATH", "/home/ubuntu/github/logs/poly_bot/state.db"),
        STATE_FLUSH_SEC=_env_int("STATE_FLUSH_SEC", 10),
        PER_TOKEN_COOLDOWN_SEC=_env_float("PER_TOKEN_COOLDOWN_SEC", 5.0),
        MIN_CANCEL_REPLACE_INTERVAL_SEC=_env_float("MIN_CANCEL_REPLACE_INTERVAL_SEC", 0.5),
        MAX_CANCEL_REPLACE_PER_SEC=_env_int("MAX_CANCEL_REPLACE_PER_SEC", 4),
        MIN_PRICE_CHANGE_FOR_REPLACE=_env_float("MIN_PRICE_CHANGE_FOR_REPLACE", 0.01),
        ERROR_COOLDOWN_BASE_SEC=_env_float("ERROR_COOLDOWN_BASE_SEC", 2.0),
        ERROR_COOLDOWN_MAX_SEC=_env_float("ERROR_COOLDOWN_MAX_SEC", 60.0),
        RETRY_MAX=_env_int("RETRY_MAX", 3),
        RETRY_BACKOFF_BASE=_env_float("RETRY_BACKOFF_BASE", 0.5),
        LOG_FILE=_env("LOG_FILE", "/home/ubuntu/github/logs/poly_bot/bot_log.jsonl"),
        LOG_ROLLUP_SEC=_env_int("LOG_ROLLUP_SEC", 60),
        LIVE_MAX_CAPITAL_USD=_env_float("LIVE_MAX_CAPITAL_USD", 400.0),
        MAX_DAILY_LOSS_USD=_env_float("MAX_DAILY_LOSS_USD", 100.0),
        MAX_INTRADAY_DRAWDOWN_USD=_env_float("MAX_INTRADAY_DRAWDOWN_USD", 60.0),
        MAX_CAPITAL_PER_ASSET_USD=_env_float("MAX_CAPITAL_PER_ASSET_USD", 150.0),
        MAX_CROSS_NOTIONAL_PER_HOUR_USD=_env_float("MAX_CROSS_NOTIONAL_PER_HOUR_USD", 100.0),
        PAUSE_MINUTES_ON_DD=_env_int("PAUSE_MINUTES_ON_DD", 45),
        CONV_MID_BUY_THRESH=_env_float("CONV_MID_BUY_THRESH", 0.60),
        CONV_MID_SELL_THRESH=_env_float("CONV_MID_SELL_THRESH", 0.48),
        CONV_LOOKBACK_SEC=_env_int("CONV_LOOKBACK_SEC", 5),
        CONV_MIN_DIP_CENTS=_env_float("CONV_MIN_DIP_CENTS", 0.0),
        CONV_MIN_IMB_CHANGE=_env_float("CONV_MIN_IMB_CHANGE", 0.0),
        CONV_HOLD_SEC=_env_int("CONV_HOLD_SEC", 300),
        CONV_STOP_LOSS_CENTS=_env_float("CONV_STOP_LOSS_CENTS", 0.0),
        CONV_TAKE_PROFIT_CENTS=_env_float("CONV_TAKE_PROFIT_CENTS", 0.0),
        CONV_TRAILING_STOP_CENTS=_env_float("CONV_TRAILING_STOP_CENTS", 0.0),
        CONV_POSITION_SIZE=_env_float("CONV_POSITION_SIZE", 10.0),
        CONV_HOURLY_BUDGET_USD=_env_float("CONV_HOURLY_BUDGET_USD", 500.0),
        CONV_EXCLUDE_ASSETS=tuple(x.strip() for x in os.environ.get("CONV_EXCLUDE_ASSETS", "BTC").split(",") if x.strip()),
        SIM_FEE_BPS=_env_float("SIM_FEE_BPS", 0.0),
        TRACK_F247_WALLET=_env_bool("TRACK_F247_WALLET", False),
        DEBUG_LOG_ORDERS=_env_bool("DEBUG_LOG_ORDERS", False),
        LOG_DECISIONS=_env_bool("LOG_DECISIONS", False),
        DECISION_LOG_SAMPLE_EVERY_N=_env_int("DECISION_LOG_SAMPLE_EVERY_N", 200),
        DECISION_LOG_MIN_INTERVAL_MS=_env_int("DECISION_LOG_MIN_INTERVAL_MS", 2000),
        HEARTBEAT_LOG_MS=_env_int("HEARTBEAT_LOG_MS", 5000),
        LOG_ROTATE_MAX_BYTES=_env_int("LOG_ROTATE_MAX_BYTES", 50_000_000),
        LOG_ROTATE_BACKUP_COUNT=_env_int("LOG_ROTATE_BACKUP_COUNT", 5),
    )
    _validate(cfg)
    # Ensure log/state directories exist
    for path in (cfg.LOG_FILE, cfg.STATE_DB_PATH):
        if path:
            os.makedirs(os.path.dirname(path), exist_ok=True)
    return cfg


def _validate(cfg: Config) -> None:
    """Validate config; abort on fatal errors."""
    errors: List[str] = []

    if cfg.MODE not in ("DRY_RUN", "LIVE"):
        errors.append(f"MODE must be DRY_RUN or LIVE, got '{cfg.MODE}'")
    if cfg.DRY_RUN_FILL_MODE not in ("none", "probabilistic", "instant", "touch"):
        errors.append(f"DRY_RUN_FILL_MODE must be none|probabilistic|instant|touch, got '{cfg.DRY_RUN_FILL_MODE}'")

    if cfg.MODE == "LIVE":
        if not cfg.POLYMARKET_API_KEY:
            errors.append("LIVE mode requires POLYMARKET_API_KEY")
        if not cfg.POLYMARKET_API_SECRET:
            errors.append("LIVE mode requires POLYMARKET_API_SECRET")
        if not cfg.POLYMARKET_PRIVATE_KEY:
            errors.append("LIVE mode requires POLYMARKET_PRIVATE_KEY")

    if cfg.MAX_TOTAL_EXPOSURE_USD <= 0:
        errors.append("MAX_TOTAL_EXPOSURE_USD must be > 0")
    if cfg.MAX_POSITION_USD_PER_OUTCOME <= 0:
        errors.append("MAX_POSITION_USD_PER_OUTCOME must be > 0")
    if cfg.TARGET_LOOP_MS < 1:
        errors.append("TARGET_LOOP_MS must be >= 1")
    if not (0.0 <= cfg.SPREAD_PCTL_MIN <= 1.0):
        errors.append("SPREAD_PCTL_MIN must be in [0, 1]")
    if cfg.TP_CENTS_MIN > cfg.TP_CENTS_TARGET:
        errors.append("TP_CENTS_MIN must be <= TP_CENTS_TARGET")
    if cfg.TP_CENTS_TARGET > cfg.TP_CENTS_MAX:
        errors.append("TP_CENTS_TARGET must be <= TP_CENTS_MAX")

    # Ladder validation
    if cfg.LADDER_LEVELS > 0:
        if len(cfg.LADDER_SPLIT) != cfg.LADDER_LEVELS:
            errors.append(f"LADDER_SPLIT length ({len(cfg.LADDER_SPLIT)}) must equal LADDER_LEVELS ({cfg.LADDER_LEVELS})")
        split_sum = sum(cfg.LADDER_SPLIT)
        if abs(split_sum - 1.0) > 0.01:
            errors.append(f"LADDER_SPLIT must sum to ~1.0, got {split_sum:.4f}")

    if errors:
        for e in errors:
            print(f"[CONFIG_ERROR] {e}", file=sys.stderr)
        sys.exit(1)
