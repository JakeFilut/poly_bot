#!/usr/bin/env python3
"""
pm_hourly_clone_bot.py
Two-mode Polymarket hourly crypto bot:
- MODE=LOG  : paper mode, starts with $1000, runs until stopped, logs every action
- MODE=LIVE : same logic, submits orders
Implements a close behavioral clone of the high-frequency hourly wallet you analyzed:
- Drift-direction "core" engine + scale-in/out inventory recycling
- Late-hour cheap-side scalp engine
- Delta velocity / persistence / volatility normalization
- Orderbook imbalance gating
- Pullback entries
- Cooldown + layered limit orders
- Risk caps per market/crypto + hourly/daily stop-loss alerts (log-only, shadow sim)
- Stops adding risk after minute 57; cleanup near minute 59
Logging:
- CSV trade-intent + order-intent logs
- JSONL event logs (full state snapshots)
"""
from __future__ import annotations
import os
import sys
import time
import json
import math
import signal
import random
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import requests
# =============================================================================
# CONFIG
# =============================================================================
MODE = os.getenv("MODE", "LOG").upper()         # LOG or LIVE
BANKROLL_START_USDC = float(os.getenv("BANKROLL_START_USDC", "1000.0"))  # only used in LOG
RUN_ID = uuid.uuid4().hex[:12]  # unique per run — included in all logs + file names

# ---------------------------------------------------------------------------
# Paths — resolved relative to this file's location
#   poly_bot/          <- _PROJECT_DIR
#   ../keys/.env       <- where your private key lives
#   ../logs/poly_bot/  <- where all logs go
# ---------------------------------------------------------------------------
_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
_KEYS_DIR    = os.path.join(os.path.dirname(_PROJECT_DIR), "keys")
_LOG_DIR     = os.path.join(os.path.dirname(_PROJECT_DIR), "logs", "poly_bot")
os.makedirs(_LOG_DIR, exist_ok=True)

STATE_FILE = os.getenv("STATE_FILE", os.path.join(_LOG_DIR, "state.json"))
# Import the new Logger (replaces old write_jsonl / log_csv)
from logger import (
    Logger, SCHEMA_VERSION, BOT_VERSION, MIN_QTY as _LOG_MIN_QTY,
    STATE_HIST_MAX,
    build_book_fields, new_decision_id, new_order_id, new_position_id,
    infer_maker_taker, spread_capture_fields,
)
# Markets / coins
CRYPTOS = ["BTC", "ETH", "SOL", "XRP"]
# Polling / evaluation
EVAL_EVERY_SEC = float(os.getenv("EVAL_EVERY_SEC", "0.0"))
ORDER_REPRICE_SEC = float(os.getenv("ORDER_REPRICE_SEC", "10.0"))
# Time window within each hour (minutes)
TRADE_START_MIN = 2.0
TRADE_STOP_ADD_MIN = 57.0
TRADE_HARD_STOP_MIN = 59.25
# -----------------------------------------------------------------------------
# Entry thresholds (bps) — coin-specific, time-varying
# -----------------------------------------------------------------------------
PROFILE = "F247_LIKE"
# Coin-specific threshold tables: coin -> {early, mid, late}
_THR_TABLE = {
    "BTC": {"early": 7, "mid": 10, "late": 4},
    "SOL": {"early": 7, "mid": 10, "late": 4},
    "ETH": {"early": 6, "mid":  7, "late": 6},
    "XRP": {"early": 6, "mid":  7, "late": 6},
}
# Price cap curve (max price you will pay to BUY), piecewise by time bucket
CAP_0_5   = 0.67
CAP_5_15  = 0.82
CAP_15_30 = 0.90
CAP_30_45 = 0.96
CAP_45_60 = 0.97
# Drift persistence & velocity (hidden edge)
PERSISTENCE_SEC = 0.0           # no persistence delay (f247 parity)
MIN_DELTA_VEL_BPS_PER_MIN = 1.0 # require some "push" to scale size (not to enter)
# Volatility normalization
Z_WINDOW_SEC = 300.0            # 5 minutes for zscore
Z_ENTRY_MIN = 1.0               # only enter if zscore >= 1.0 (optional gate)
Z_ENTRY_ENABLED = False
# Orderbook imbalance
IMB_ENABLED = False
IMB_LEVELS = 5
IMB_MIN = 1.15                  # bidDepth/askDepth must exceed this for with-drift buys
IMB_MAX_SPREAD = 0.02           # cross only when spread <= 2c; maker posting ok wider
MAKER_MAX_SPREAD = 0.06         # allow maker posting up to 6c spread
# Pullback entry
PULLBACK_ENABLED = False
PULLBACK_CENTS = 0.02           # wait for 2c pullback from recent extreme
PULLBACK_LOOKBACK_SEC = 90.0
# Cooldowns — ultra-low, just anti-spam (F247 mode)
def entry_cooldown_sec(coin: str, t_min: float) -> float:
    """F247-parity cooldown: 0.5s uniform. Fast re-entry after burst."""
    return 0.5
REENTRY_COOLDOWN_SEC = 1.0        # fast re-entry (f247 parity)
# Base clip sizing (USDC cost) as % of bankroll
BASE_CLIP_PCT = 0.0035  # 0.35% bankroll per tick (~$3.50 on $1k)
EARLY_SIZE_MULT = 0.80   # less timid in first 10 min
# Size multipliers by abs_delta_bps
SIZING_MULTIPLIERS = [
    (8,   15, 1.25),
    (15,  25, 1.75),
    (25,  40, 2.25),
    (40,  75, 2.75),
    (75,  10_000, 3.50),
]
# Exit ladder (scale out)
TP1 = 0.04; TP1_SELL_FRAC = 0.30          # +4c (raised from +3c)
TP2 = 0.06; TP2_SELL_FRAC = 0.30          # +6c (raised from +5c)
TP3 = 0.08; TP3_SELL_FRAC = 0.40          # +8c (raised from +7c)
CORE_KEEP_FRAC = 0.00                     # no remainder — full exit at TP3
# De-risk on drift reversal (bps)
DERISK_CROSS_BPS = 5.0
DERISK_SELL_FRAC_PER_TICK = 0.35
DERISK_COOLDOWN_SEC = 10.0      # min seconds between DERISK actions on same position
DERISK_MID_CHANGE_CENTS = 0.01  # or mid must move >= 1c since last derisk
MAX_DERISK_PER_HOUR = int(os.getenv("MAX_DERISK_PER_HOUR", "10"))  # cap derisks to prevent bleed
# Maker-first DERISK — stop panic taker sells
DERISK_MAKER_REFRESH_MS = 250          # cancel/replace maker every 250ms
DERISK_TAKER_EMERGENCY_ONLY = True     # only taker derisk in emergency
INVENTORY_EMERGENCY_SHARES = 300       # above this = emergency taker derisk
DERISK_TAKER_EDGE_EXTRA_BPS = 25      # edge must exceed thr+25 for taker derisk
DERISK_TAKER_EDGE_WORSEN_SEC = 1.0    # edge must be worsening for 1s
# ---------------------------------------------------------------------------
# Taker gating — ONLY cross if BOTH conditions met (entry + exit)
# ---------------------------------------------------------------------------
TAKER_MAX_SPREAD_CENTS = 1.0           # spread <= 1c
TAKER_MIN_EDGE_EXTRA_BPS = 12         # abs(edge_bps) >= thr + 12
# ---------------------------------------------------------------------------
# Whipsaw / anti-chop filter
# ---------------------------------------------------------------------------
ENTRY_MIN_STABLE_SIGN_MS = 400         # delta sign must be stable 400ms
BLOCK_IF_VEL_OPPOSES = True            # block if velocity opposes delta
VEL_OPPOSE_THRESHOLD = 2.0            # bps/min threshold for opposition
# ---------------------------------------------------------------------------
# No-flip rule — prevent immediate direction reversal
# ---------------------------------------------------------------------------
NO_FLIP_COOLDOWN_SEC = 3.0            # don't reverse direction within 3s
NO_FLIP_OVERRIDE_EXTRA_BPS = 20       # unless edge >= thr + 20
# Late scalp engine
LATE_SCALP_ENABLED = bool(os.getenv("PARITY_ENABLED", "False") not in ("", "0", "False", "false"))  # OFF unless parity on
LATE_SCALP_T_START = 40.0
LATE_SCALP_T_END   = 58.0
LATE_SCALP_PRICE_MAX = 0.80
LATE_SCALP_ABSDELTA_MIN = 5.0
LATE_SCALP_ABSDELTA_MAX = 20.0
LATE_SCALP_TP_CENTS = 0.04      # aim +4c (raised from +3c)
LATE_SCALP_MAX_HOLD_MIN = 6.0   # aggressive F247 — flip fast
# Risk caps
MAX_COST_PER_MARKET_PCT = 0.015   # 1.5% bankroll per market-hour
MAX_COST_PER_CRYPTO_PCT = 0.035   # 3.5% bankroll per crypto across markets
# ---------------------------------------------------------------------------
# Risk / stop-loss configuration (log-only mode)
# ---------------------------------------------------------------------------
LOG_MODE = True                       # paper / logging mode — no real orders
ENFORCE_STOP_LOSS = False             # MUST remain False in LOG mode
STOP_LOSS_PCT_PER_HOUR = 0.02        # 2% equity drawdown per 1-hour window
STOP_LOSS_PCT_PER_DAY  = 0.06        # 6% equity drawdown per calendar day
SHADOW_STOP_SIM = True                # simulate what would have happened if stop was enforced
# Execution policy
POST_ONLY_WHEN_POSSIBLE = True
MAX_CROSS_SLIPPAGE = 0.01         # cross at most 1c if absolutely needed
LAYER_ORDERS = True
LAYER_COUNT = 3
LAYER_STEP = 0.01                 # 1c ladder
MIN_ORDER_USDC = float(os.getenv("MIN_ORDER_USDC", "5.0"))  # raised from $0.25 — tiny clips amplify noise+churn
MIN_QTY = _LOG_MIN_QTY  # from logger — below this, position is dust
EDGE_K = 0.05    # sigmoid steepness: delta_bps -> P(Up)
# -----------------------------------------------------------------------------
# Probe → Scale state machine
# -----------------------------------------------------------------------------
PROBE_SIZE_FRAC = 0.25        # probe = max($1, clip * 0.25)
PROBE_CONFIRM_SEC = 0.3       # 300ms — near-instant confirmation (F247)
# Count-based burst engine (f247-tuned: less spam, bigger steps)
BURST_ORDERS = 8                       # max micro-orders per burst
BURST_INTERVAL_MS = 180                # micro-order every 180ms
BURST_STEP_USD_MIN = 0.75             # micro-order floor
BURST_STEP_USD_MAX = 6.00             # micro-order ceiling
BURST_MIN_EDGE_EXTRA_BPS = 6          # only burst if edge >= thr + 6, else probe only
BURST_STOP_IF_PRICE_MOVES_CENTS = 0.02  # stop if price moves 2c against us
BURST_STOP_IF_EDGE_DROPS_BPS = 6.0     # hard edge collapse
BURST_EDGE_BELOW_HOLD_MS = 500         # edge below threshold must persist 500ms to stop
BURST_SPREAD_HARD_LIMIT = 0.12         # absolute max spread for any order type
BURST_CROSS_MAX_SPREAD = 0.01          # cross at ask ONLY when spread <= 1c (used by taker gate)
# Dynamic price cap boost
CAP_BOOST_EDGE_THRESHOLD = 10.0  # edge_bps above which cap starts boosting
CAP_BOOST_MAX = 0.08             # max +8 cents boost (aggressive chase)
CAP_BOOST_EDGE_FULL = 30.0       # edge_bps at which full boost is applied
# ---------------------------------------------------------------------------
# Parity (straddle) arbitrage engine — Up + Down ≈ 1.000
# ---------------------------------------------------------------------------
PARITY_ENABLED = bool(os.getenv("PARITY_ENABLED", "False") not in ("", "0", "False", "false"))  # OFF by default — dscalp only
PARITY_BUY_ENABLED = PARITY_ENABLED      # buy cheap straddle (up_ask + dn_ask < 1)
PARITY_SELL_ENABLED = PARITY_ENABLED     # sell rich straddle (up_bid + dn_bid > 1)
PARITY_MAX_USD_PER_SLUG = 40.0          # max total straddle investment per slug
PARITY_STEP_USD = 2.00                   # per-leg size per parity order
PARITY_COOLDOWN_MS = 250                 # min time between parity orders per slug
PARITY_MAKER_REFRESH_MS = 200            # cancel/replace maker every 200ms
PARITY_TAKER_ALLOWED_SPREAD_CENTS = 1.0  # allow taker only when spread <= 1c
# Fee-aware parity edge (CRITICAL)
MAKER_FEE_BPS = float(os.getenv("MAKER_FEE_BPS", "0.5"))   # configurable: Poly CLOB ≈0-0.5 bps maker
TAKER_FEE_BPS = float(os.getenv("TAKER_FEE_BPS", "2.0"))   # configurable: Poly CLOB ≈2 bps taker
PARITY_BUY_MIN_EDGE_NET_CENTS = 1.0     # min NET edge after fees/slippage to buy straddle
PARITY_SELL_MIN_EDGE_NET_CENTS = 1.0    # min NET edge after fees/slippage to sell straddle
PARITY_EDGE_BUFFER_CENTS = 0.25         # safety buffer on top of min edge thresholds
# Partial-fill protection
PAIR_FILL_TIMEOUT_MS = 1200              # max time to wait for second leg fill
# Maker queue discipline (reduce cancel spam)
MIN_REPLACE_INTERVAL_MS = 200            # min time between cancel/replace on same order
MAKER_ORDER_TIMEOUT_MS = 3000            # cancel maker order if unfilled after 3s
# Locked inventory recycle
LOCKED_MAX_HOLD_SEC = 180                # max seconds to hold locked straddle before recycling
RECYCLE_MIN_PROFIT_NET_CENTS = 0.5      # min net-of-fee profit to trigger recycle sell
RECYCLE_STEP_USD = 2.0                   # per-leg sell size during recycle
# Liquidity + staleness guards
MAX_SPREAD_FOR_PARITY_CENTS = 10.0      # block parity if either leg spread > 10c
MIN_TOP_LIQ_USD = 1.0                   # block parity if best bid/ask size < $1 (F247 trades tiny clips)
PARITY_MAX_CACHE_AGE_MS = 600           # block parity if cache > 600ms stale
# End-of-hour parity flattening
PARITY_STOP_NEW_MIN = 57.0              # stop opening NEW parity trades after minute 57
PARITY_FLATTEN_START_MIN = 59.0         # begin flattening locked + unpaired parity inventory
PARITY_HARD_FLATTEN_MIN = 59.25         # force taker flatten (if time_to_close<20s or emergency)
# ---------------------------------------------------------------------------
# Parity QUOTING mode — continuously post maker bids on BOTH legs
# ---------------------------------------------------------------------------
PARITY_QUOTE_ENABLED = PARITY_ENABLED    # OFF unless PARITY_ENABLED=true
PARITY_QUOTE_TARGET_EDGE_NET_CENTS_BASE = 1.0  # min edge target (aggressive — pay up)
PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX  = 2.0  # max edge target (selective)
PARITY_QUOTE_STEP_USD = 2.0              # per-leg bid size (equal USD both legs)
PARITY_QUOTE_MAX_USD_PER_SLUG = 40.0    # max total quoting investment per slug
PARITY_QUOTE_REFRESH_MS = 250           # refresh interval for quote repricing
PARITY_QUOTE_ONLY_IF_LIQ_OK = True      # require liquidity guards for quoting
# Unpaired quote management
QUOTE_UNPAIRED_ESCALATE_AFTER_MS = 800  # raise missing leg bid by 1 tick if still net edge ok
QUOTE_UNPAIRED_MAX_SEC = 6.0            # after this, unwind or pause quoting this slug
QUOTE_PAUSE_AFTER_UNPAIRED_SEC = 15.0   # pause quoting this slug after forced unpaired unwind
# Adverse selection guard
ADVERSE_SPOT_MOVE_BPS_THRESHOLD = 20.0  # degrade quoting if spot moved > 20 bps in 10s
ADVERSE_LOOKBACK_SEC = 10.0             # lookback window for spot move detection
ADVERSE_PAUSE_SEC = 10.0                # hard-pause quoting only if move is accelerating
ADVERSE_DEGRADE_SEC = 10.0              # degrade duration: MAX target + 50% step reduction
ADVERSE_ACCEL_BPS_PER_MIN = 40.0        # velocity threshold to escalate degrade -> hard-pause
# ---------------------------------------------------------------------------
# FAST_CLONE mode — tighter timing to match F247 speed
# ---------------------------------------------------------------------------
FAST_CLONE = bool(os.getenv("FAST_CLONE", "True") not in ("", "0", "False", "false"))
# One-sided auto-hedge — faster escalation than unpaired management
HEDGE_TICK_ESCALATION_ENABLED = False   # DISABLED: no micro-neutralizing
HEDGE_TICK1_MS = 200                    # +1 tick after 200ms (disabled by flag above)
HEDGE_TICK2_MS = 350                    # +3 ticks after 350ms (disabled by flag above)
HEDGE_EARLY_CROSS_MS = 1500            # taker cross after 1500ms (no rushing)
HEDGE_CROSS_MS = 2000                   # taker cross after 2000ms (fallback)
HEDGE_MIN_CROSS_EDGE_CENTS = 0.5       # min net edge for taker cross (both early+late)
HEDGE_EARLY_CROSS_EDGE_CENTS = 0.5     # min net edge for early taker cross (500ms)
HEDGE_MAX_CROSS_SPREAD_CENTS = 2.0     # max spread for taker cross (cents)
HEDGE_STALE_CACHE_MS = 450.0           # block hedge actions if cache > 450ms
# ---------------------------------------------------------------------------
# Imbalance caps — keep net exposure near neutral (F247 style)
# ---------------------------------------------------------------------------
IMBALANCE_CAP_SHARES = 30              # hard cap: abs(up_qty - dn_qty) per slug
IMBALANCE_SOFT_CAP_SHARES = 20         # soft cap: start reducing new orders above this
# ---------------------------------------------------------------------------
# Derisk RESCUE-TO-STRADDLE — convert losing one-sided to straddle
# ---------------------------------------------------------------------------
DERISK_RESCUE_TO_STRADDLE = PARITY_ENABLED  # rescue adds parity-like exposure — off when parity off
RESCUE_MIN_EDGE_NET_CENTS = 0.5         # min net edge for straddle completion to be worth it
RESCUE_MAX_USD_PER_SLUG = 20.0          # max USD to spend completing straddle per slug
RESCUE_STEP_USD = 2.0                    # per-order size for rescue buys
MIN_PAIR_QTY = 5.0                       # both legs must exceed this to count as "already paired"
# ---------------------------------------------------------------------------
# Directional lean overlay (on top of parity, for exits)
# ---------------------------------------------------------------------------
LEAN_EXIT_PRIORITY = True                # prioritize exits on "wrong" side
LEAN_MAX_IMBALANCE_SHARES = 30           # cap how unbalanced Up vs Down can get (align with IMBALANCE_CAP_SHARES)
# Spread rule relaxation
SPREAD_RELAXED_MAX = 0.12         # 12 cents during burst (F247 tolerant)
# Fast take-profit — skim faster than before
FAST_TP_AFTER_SEC = 60.0              # raised: don't skim early (was 25s)
FAST_TP_CENTS = 0.04                  # raised: +4c min (was +2c)
FAST_TP_SELL_PCT = 0.20              # reduced: sell 20% (was 30%)
# Inventory pressure controls
INVENTORY_CAP_SHARES_PER_MARKET = 250
# Correlation exposure scaling (reduces correlated stacking)
CORR_SCALE_ENABLED = True
BTC_LEAD = True
BTC_EXPOSURE_REDUCE_OTHERS = 0.50  # up to 50% size reduction if BTC exposure high
# ---------------------------------------------------------------------------
# Background data refresh — sub-second loop architecture
# ---------------------------------------------------------------------------
MARKET_DISCOVERY_INTERVAL_SEC = 10.0   # re-discover markets via Gamma API every 10s
BOOK_REFRESH_PRIORITY_MS = 100         # active markets: 100ms (positions / probing / scaling)
BOOK_REFRESH_IDLE_MS = 400             # idle markets: 400ms (no positions, IDLE state)
BOOK_STALE_MS = 1500                   # data older than this is stale — skip processing
STATE_SAVE_INTERVAL_SEC = 5.0          # flush state.json every 5s (not every loop)
BG_POOL_WORKERS = 16                   # bg threads — covers priority + idle markets
BG_POOL_MIN_WORKERS = 12               # minimum pool size
MAIN_LOOP_TARGET_MS = 75              # 75ms decision loop target (f247 parity)
BG_REFRESH_STARVE_CYCLES = 2           # if pending for > N cycles, force-submit
# Burst freshness gate — micro-orders must have fresh data
BURST_FRESHNESS_MAX_MS = 500           # max cache age to place a micro-order
BURST_FRESHNESS_WAIT_MS = 250          # max time to wait for fresh data if stale
# ---------------------------------------------------------------------------
# FAST_CLONE speed overrides — tighter loops, faster hedging
# ---------------------------------------------------------------------------
if FAST_CLONE:
    MAIN_LOOP_TARGET_MS = 80
    BOOK_REFRESH_PRIORITY_MS = 80
    BOOK_REFRESH_IDLE_MS = 150
    BOOK_STALE_MS = 700
    MIN_REPLACE_INTERVAL_MS = 100
    PARITY_MAKER_REFRESH_MS = 120
    QUOTE_UNPAIRED_ESCALATE_AFTER_MS = 250
    QUOTE_UNPAIRED_MAX_SEC = 2.0
    HEDGE_TICK1_MS = 200
    HEDGE_TICK2_MS = 350
    HEDGE_EARLY_CROSS_MS = 1500
    HEDGE_CROSS_MS = 2000
    # Fast-start: trade immediately, minimal persistence delay, lower early threshold
    TRADE_START_MIN = 0.1
    PERSISTENCE_SEC = 1.5
    for _coin in list(_THR_TABLE.keys()):
        _THR_TABLE[_coin]["early"] = 3
# ---------------------------------------------------------------------------
# RATE LIMITING / CHURN CONTROL — hard caps per slug (F247 cadence)
# ---------------------------------------------------------------------------
RATE_LIMIT_ENABLED = bool(os.getenv("RATE_LIMIT_ENABLED", "True") not in ("", "0", "False", "false"))
MIN_ORDER_INTERVAL_MS = float(os.getenv("MIN_ORDER_INTERVAL_MS", "500"))       # min ms between ANY orders on same slug (raised from 400)
MAX_ORDER_SUBMITS_PER_MIN = int(os.getenv("MAX_ORDER_SUBMITS_PER_MIN", "120")) # hard cap submits/min globally
MAX_SUBMITS_PER_MIN_PER_SLUG = int(os.getenv("MAX_SUBMITS_PER_MIN_PER_SLUG", "120"))  # per-slug cap
QUOTE_REFRESH_SKIP_IF_SAME = True                                               # skip refresh if price unchanged
QUOTE_REFRESH_MIN_TICK_MOVE = 0.001                                              # require >= 1 tick move to refresh
QUOTE_REFRESH_MIN_ELAPSED_MS = float(os.getenv("QUOTE_REFRESH_MIN_ELAPSED_MS", "500"))  # min ms between refreshes
# Reprice guard: only reprice when justified (outbid OR price moved >= 1 tick OR TTL expired)
REPRICE_MIN_PRICE_MOVE = float(os.getenv("REPRICE_MIN_PRICE_MOVE", "0.001"))     # 1 tick minimum to trigger reprice
REPRICE_REQUIRE_OUTBID = bool(os.getenv("REPRICE_REQUIRE_OUTBID", "True") not in ("", "0", "False", "false"))
# ---------------------------------------------------------------------------
# DIRECTIONAL SCALP MODE — PRIMARY engine (F247-style, priority #1)
# Strategy stack: 1) Directional Scalp  2) Inventory Repair  3) Parity (throttled)
# ---------------------------------------------------------------------------
DIRECTIONAL_SCALP_ENABLED = bool(os.getenv("DIRECTIONAL_SCALP_ENABLED", "True") not in ("", "0", "False", "false"))
# Entry gates (explicit — must meet delta OR spot_move condition + edge_cents)
DSCALP_DELTA_MIN_BPS = float(os.getenv("DSCALP_DELTA_MIN_BPS", "15.0"))        # min abs_delta_bps for entry (raised for conviction)
DSCALP_SPOT_MOVE_10S_BPS = float(os.getenv("DSCALP_SPOT_MOVE_10S_BPS", "8.0"))  # OR: spot moved >= 8bps in last 10s
DSCALP_VEL_MIN_BPS_PER_MIN = float(os.getenv("DSCALP_VEL_MIN_BPS_PER_MIN", "1.0"))  # min velocity (supportive, not hard gate)
DSCALP_MAX_SPREAD_CENTS = float(os.getenv("DSCALP_MAX_SPREAD_CENTS", "2.0"))   # max spread for entry
DSCALP_MAX_CACHE_AGE_MS = float(os.getenv("DSCALP_MAX_CACHE_AGE_MS", "250"))   # max cache age for entry
# Edge filter: require meaningful gap between expected exit and entry
DSCALP_MIN_EDGE_CENTS = float(os.getenv("DSCALP_MIN_EDGE_CENTS", "4.0"))       # min edge (delta_bps * price / 100) for entry
DSCALP_MIN_EDGE_CENTS_SOL = float(os.getenv("DSCALP_MIN_EDGE_CENTS_SOL", "5.0"))   # SOL: more volatile, need wider edge
DSCALP_MIN_EDGE_CENTS_XRP = float(os.getenv("DSCALP_MIN_EDGE_CENTS_XRP", "5.0"))   # XRP: same
DSCALP_MIN_EDGE_CENTS_BTC = float(os.getenv("DSCALP_MIN_EDGE_CENTS_BTC", "4.0"))   # BTC: tighter spreads
# No-trade zone: block entry if data feeds disagree or are stale
DSCALP_FEED_DISAGREE_BPS = float(os.getenv("DSCALP_FEED_DISAGREE_BPS", "30.0"))    # block if Binance vs Chainlink > 30bps apart
DSCALP_FEED_STALE_SEC = float(os.getenv("DSCALP_FEED_STALE_SEC", "5.0"))           # block if any feed > 5s stale
# Sizing — one entry = one meaningful position, no micro-splits
DSCALP_STEP_USD = float(os.getenv("DSCALP_STEP_USD", "8.0"))                   # per-order size (target avg $7-8)
DSCALP_STEP_USD_MIN = float(os.getenv("DSCALP_STEP_USD_MIN", "6.0"))           # minimum entry size (no $1 clips)
DSCALP_MAX_USD_PER_SLUG = float(os.getenv("DSCALP_MAX_USD_PER_SLUG", "30.0"))  # max directional per slug
DSCALP_COOLDOWN_MS = float(os.getenv("DSCALP_COOLDOWN_MS", "4000"))            # 4s between entries (target ~15 trades/min)
if FAST_CLONE:
    DSCALP_COOLDOWN_MS = float(os.getenv("DSCALP_COOLDOWN_MS", "1000"))       # FAST_CLONE: 1s cooldown
# Exit ladder
DSCALP_TP1_CENTS = float(os.getenv("DSCALP_TP1_CENTS", "4.0"))                 # +4c: sell 30% (raised from +3c)
DSCALP_TP1_FRAC = float(os.getenv("DSCALP_TP1_FRAC", "0.30"))
DSCALP_TP2_CENTS = float(os.getenv("DSCALP_TP2_CENTS", "6.0"))                 # +6c: sell 30%
DSCALP_TP2_FRAC = float(os.getenv("DSCALP_TP2_FRAC", "0.30"))
DSCALP_TP3_CENTS = float(os.getenv("DSCALP_TP3_CENTS", "8.0"))                 # +8c: sell 40% (remainder)
DSCALP_TP3_FRAC = float(os.getenv("DSCALP_TP3_FRAC", "0.40"))
DSCALP_MIN_HOLD_SEC = float(os.getenv("DSCALP_MIN_HOLD_SEC", "120"))           # 120s min hold — allow real directional exposure
DSCALP_MAX_HOLD_SEC = float(os.getenv("DSCALP_MAX_HOLD_SEC", "600"))           # 10 min max hold
DSCALP_STOP_LOSS_CENTS = float(os.getenv("DSCALP_STOP_LOSS_CENTS", "5.0"))     # -5c stop loss (emergency only)
# Breakeven exit: after MAX_HOLD_SEC/2 (300s), try maker exit at entry+1c before timeout
DSCALP_BREAKEVEN_AFTER_SEC = float(os.getenv("DSCALP_BREAKEVEN_AFTER_SEC", "300"))  # try breakeven after 5 min
DSCALP_BREAKEVEN_CENTS = float(os.getenv("DSCALP_BREAKEVEN_CENTS", "1.0"))     # exit at entry + 1c
# ---------------------------------------------------------------------------
# PARITY SUPPRESSION — parity is #3 priority, hard-capped
# ---------------------------------------------------------------------------
PARITY_DEFER_TO_DIRECTIONAL = True                                               # always defer when directional active
PARITY_BLOCK_IF_ADVERSE = True                                                   # block parity when adverse guard active
PARITY_STANDDOWN_AFTER_DSCALP_SEC = float(os.getenv("PARITY_STANDDOWN_AFTER_DSCALP_SEC", "30"))  # parity stands down X sec after dscalp fires
PARITY_IMBALANCE_BLOCK_SHARES = float(os.getenv("PARITY_IMBALANCE_BLOCK_SHARES", "5.0"))  # block parity if net imbal >= this
PARITY_DSCALP_INV_BLOCK_USD = float(os.getenv("PARITY_DSCALP_INV_BLOCK_USD", "25.0"))  # block parity if directional invested > $25 on slug
PARITY_MAX_FILL_PCT = float(os.getenv("PARITY_MAX_FILL_PCT", "0.30"))           # target: parity < 30% of total fills
PARITY_MAX_WHEN_DIRECTIONAL_USD = float(os.getenv("PARITY_MAX_WHEN_DIRECTIONAL_USD", "0.0"))  # $0 parity when directional active
# ---------------------------------------------------------------------------
# GLOBAL THROTTLE — target trades/min
# ---------------------------------------------------------------------------
TARGET_TRADES_PER_MIN = float(os.getenv("TARGET_TRADES_PER_MIN", "15"))          # target ~15 trades/min (F247 = ~12)
THROTTLE_LOOKBACK_SEC = float(os.getenv("THROTTLE_LOOKBACK_SEC", "60"))          # rolling window for trades/min calc
# ---------------------------------------------------------------------------
# REGIME AWARENESS — volatility-adaptive activity
# ---------------------------------------------------------------------------
REGIME_VOL_LOOKBACK_SEC = float(os.getenv("REGIME_VOL_LOOKBACK_SEC", "60"))      # 60s rolling window
REGIME_LOW_VOL_THRESHOLD = float(os.getenv("REGIME_LOW_VOL_THRESHOLD", "3.0"))   # bps std_dev below this = low vol
REGIME_LOW_VOL_REDUCTION = float(os.getenv("REGIME_LOW_VOL_REDUCTION", "0.50"))  # reduce activity 50% in low vol
# ---------------------------------------------------------------------------
# TRUE COST TRACKER — tx counting and fee estimation
# ---------------------------------------------------------------------------
TRUE_COST_ENABLED = True
TRUE_COST_EST_GAS_PER_TX_USD = float(os.getenv("TRUE_COST_EST_GAS_PER_TX_USD", "0.001"))  # est gas per tx
TRUE_COST_EST_FEE_BPS = float(os.getenv("TRUE_COST_EST_FEE_BPS", "2.0"))                  # avg fee bps per fill
# ---------------------------------------------------------------------------
# ORDER MANAGER — live order lifecycle (Phase 1-7 hardening)
# ---------------------------------------------------------------------------
OM_MAKER_ORDER_TTL_MS = float(os.getenv("OM_MAKER_ORDER_TTL_MS", "3000"))               # cancel maker if no new fills after TTL
OM_MAX_ACTIVE_PER_SLUG_SIDE = int(os.getenv("OM_MAX_ACTIVE_PER_SLUG_SIDE", "1"))        # max concurrent orders per (slug, side)
OM_MAX_OPEN_ORDERS = int(os.getenv("OM_MAX_OPEN_ORDERS", "50"))                          # global cap on open orders
OM_ORPHAN_SCAN_INTERVAL_SEC = float(os.getenv("OM_ORPHAN_SCAN_INTERVAL_SEC", "15"))      # scan CLOB for orphans every N sec
OM_SUBMIT_MAX_RETRIES = int(os.getenv("OM_SUBMIT_MAX_RETRIES", "3"))                     # retry submit on transient error
OM_SUBMIT_BACKOFF_MS = [100, 250, 500]                                                    # backoff per retry
OM_CANCEL_MAX_RETRIES = int(os.getenv("OM_CANCEL_MAX_RETRIES", "3"))
OM_CANCEL_BACKOFF_MS = [100, 250, 500]
OM_RECONCILE_MAX_PER_TICK = int(os.getenv("OM_RECONCILE_MAX_PER_TICK", "10"))            # max orders to poll per main-loop tick
OM_KILL_ORPHAN_THRESHOLD_PER_MIN = int(os.getenv("OM_KILL_ORPHAN_THRESHOLD_PER_MIN", "10"))  # kill-switch: orphans/min
OM_KILL_API_ERROR_THRESHOLD_PER_MIN = int(os.getenv("OM_KILL_API_ERROR_THRESHOLD_PER_MIN", "20"))  # kill-switch: errors/min
OM_KILL_COOLDOWN_SEC = float(os.getenv("OM_KILL_COOLDOWN_SEC", "60"))                    # disable entries for N sec after kill-switch
# Hedge escalation in LIVE mode (Phase 5)
OM_HEDGE_ESCALATION_LIVE = bool(os.getenv("OM_HEDGE_ESCALATION_LIVE", "True") not in ("", "0", "False", "false"))
OM_HEDGE_TICK1_MS = 200       # +1 tick after 200ms
OM_HEDGE_TICK2_MS = 350       # +3 ticks after 350ms
OM_HEDGE_CROSS_MS = 500       # taker cross after 500ms
OM_HEDGE_CROSS_MIN_EDGE_CENTS = 0.5
OM_HEDGE_CROSS_MAX_SPREAD_CENTS = 2.0
# Restart recovery (Safety Item 1)
OM_STARTUP_RECONCILE = bool(os.getenv("OM_STARTUP_RECONCILE", "True") not in ("", "0", "False", "false"))
OM_OPEN_ORDERS_FILE = os.getenv("OM_OPEN_ORDERS_FILE", os.path.join(_LOG_DIR, "om_open_orders.json"))
# Per-slug no-progress circuit breaker (Safety Item 2)
OM_SLUG_NOPROGRESS_SUBMITS = int(os.getenv("OM_SLUG_NOPROGRESS_SUBMITS", "20"))   # submits in 60s threshold
OM_SLUG_PAUSE_SEC = float(os.getenv("OM_SLUG_PAUSE_SEC", "120"))                   # pause duration
# Cancel/replace rate limits (Safety Item 3)
OM_MIN_REPLACE_INTERVAL_MS = float(os.getenv("OM_MIN_REPLACE_INTERVAL_MS", "250")) # min ms between replaces
OM_MAX_CANCELS_PER_MIN_GLOBAL = int(os.getenv("OM_MAX_CANCELS_PER_MIN_GLOBAL", "200"))    # global cancel cap
OM_MAX_CANCELS_PER_MIN_SLUG = int(os.getenv("OM_MAX_CANCELS_PER_MIN_SLUG", "40"))         # per-slug cancel cap
OM_CANCEL_FREEZE_SEC = float(os.getenv("OM_CANCEL_FREEZE_SEC", "30"))              # freeze quoting after exceed
# Flatten verification (Safety Item 4)
OM_FLATTEN_VERIFY_ENABLED = bool(os.getenv("OM_FLATTEN_VERIFY_ENABLED", "True") not in ("", "0", "False", "false"))
OM_FLATTEN_CROSS_MAX_RETRIES = int(os.getenv("OM_FLATTEN_CROSS_MAX_RETRIES", "3"))
# State drift detector (Safety Item 5) — enhanced: API positions vs internal
OM_DRIFT_CHECK_INTERVAL_SEC = float(os.getenv("OM_DRIFT_CHECK_INTERVAL_SEC", "60"))
OM_DRIFT_QTY_TOLERANCE = float(os.getenv("OM_DRIFT_QTY_TOLERANCE", "5.0"))         # shares tolerance
OM_DRIFT_PAUSE_SEC = float(os.getenv("OM_DRIFT_PAUSE_SEC", "120"))                 # pause entries on drift
OM_DRIFT_POSITION_CHECK = bool(os.getenv("OM_DRIFT_POSITION_CHECK", "True") not in ("", "0", "False", "false"))
# PnL attribution reporting interval
PNL_REPORT_INTERVAL_SEC = float(os.getenv("PNL_REPORT_INTERVAL_SEC", "900"))       # every 15 min
# Auto-disable slug if pnl_30m < -$X
SLUG_AUTO_DISABLE_ENABLED = bool(os.getenv("SLUG_AUTO_DISABLE_ENABLED", "True") not in ("", "0", "False", "false"))
SLUG_AUTO_DISABLE_LOSS_USD = float(os.getenv("SLUG_AUTO_DISABLE_LOSS_USD", "15.0"))   # -$15 triggers disable
SLUG_AUTO_DISABLE_WINDOW_SEC = float(os.getenv("SLUG_AUTO_DISABLE_WINDOW_SEC", "1800"))  # 30 min rolling window
SLUG_AUTO_DISABLE_DURATION_SEC = float(os.getenv("SLUG_AUTO_DISABLE_DURATION_SEC", "7200"))  # 2 hour disable
# =============================================================================
# UTIL / LOGGING — thin wrappers around Logger instance
# =============================================================================
# The global `_LOGGER` is initialised in Bot.__init__().
_LOGGER: Optional["Logger"] = None

def utc_now() -> datetime:
    return datetime.now(timezone.utc)
def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))
def clamp_to_tick(price: float, tick: float = 0.001) -> float:
    """Round price DOWN to nearest tick (Polymarket uses $0.001 ticks)."""
    return math.floor(price / tick) * tick

def write_jsonl(event: dict) -> None:
    """Legacy shim — delegates to _LOGGER._write_jsonl if available."""
    if _LOGGER is not None:
        _LOGGER._write_jsonl(event)
    else:
        event["ts"] = iso_z(utc_now())
        event["run_id"] = RUN_ID
        print(f"[{event.get('event_type','')}] (pre-logger)")

def _hour_label_et(hour_start_utc_str: str) -> str:
    """Convert '2026-02-14T18:00:00Z' -> '2026-02-14 13:00 ET'."""
    try:
        import pytz
        dt = datetime.strptime(hour_start_utc_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        et = pytz.timezone("US/Eastern")
        dt_et = dt.astimezone(et)
        return dt_et.strftime("%Y-%m-%d %H:%M ET")
    except Exception:
        return hour_start_utc_str
def _p_up_model(delta_bps: float) -> float:
    """Implied probability of Up outcome via sigmoid on delta_bps."""
    return 1.0 / (1.0 + math.exp(-EDGE_K * delta_bps))
def _phase(t_min: float) -> str:
    """Time band within the hour window."""
    if t_min < 10.0:
        return "OPENING"
    if t_min < 50.0:
        return "MID"
    return "CLOSING"
def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default
# =============================================================================
# DATA STRUCTURES
# =============================================================================
@dataclass
class MarketRef:
    crypto: str
    slug: str
    market_id: str              # polymarket market identifier (token / condition id)
    outcome_up_id: str          # token id for UP
    outcome_down_id: str        # token id for DOWN
    hour_open: float            # open reference price for the hour
    hour_start_utc: datetime    # hour start timestamp
@dataclass
class BookTop:
    bid: float
    ask: float
    bid_sz: float
    ask_sz: float
    spread: float
    imb: float                 # bid_depth/ask_depth over N levels (approx)
    mid: float
    # Depth at price increments (cumulative size within Xc of best)
    depth_1c_bid: float = 0.0
    depth_1c_ask: float = 0.0
    depth_2c_bid: float = 0.0
    depth_2c_ask: float = 0.0
    depth_5c_bid: float = 0.0
    depth_5c_ask: float = 0.0
@dataclass
class Position:
    qty: float = 0.0
    cost_usdc: float = 0.0      # total cost spent (for paper)
    vwap: float = 0.0
    tp1_done: bool = False
    tp2_done: bool = False
    tp3_done: bool = False
    opened_at: Optional[str] = None
    last_trade_ts: Optional[str] = None
    scalp_mode: bool = False
    scalp_open_ts: Optional[str] = None
    position_id: Optional[str] = None       # UUID lifecycle: first entry → fully flat
    trade_id: Optional[str] = None          # persistent across entry → TP1 → TP2 → cleanup
    entry_decision_id: Optional[str] = None # decision_id of original entry (parent)
    parent_order_id: Optional[str] = None   # client_order_id of entry order (for exit legs)
    entry_mid: float = 0.0                  # mid price at entry time
    max_favorable_mid: float = 0.0          # best mid seen while holding
    max_adverse_mid: float = 1.0            # worst mid seen while holding
    last_derisk_ts: Optional[str] = None    # ISO timestamp of last DERISK sell
    last_derisk_mid: float = 0.0            # mid price at last DERISK action
    fast_tp_done: bool = False              # FAST_TP fires only once per position
@dataclass
class MarketState:
    slug: str
    crypto: str
    hour_open: float
    hour_start_utc: str
    last_entry_ts: Optional[str] = None
    last_reentry_ts: Optional[str] = None
    peak_abs_delta_bps: float = 0.0
    hour_index: int = 0                          # monotonic counter per crypto
    delta_hist: List[Tuple[str, float]] = None   # (iso, delta_bps)
    price_hist: List[Tuple[str, float]] = None   # (iso, binance_spot)
    positions: Dict[str, Position] = None        # "Up" / "Down"
    def __post_init__(self):
        if self.delta_hist is None: self.delta_hist = []
        if self.price_hist is None: self.price_hist = []
        if self.positions is None: self.positions = {"Up": Position(), "Down": Position()}
# =============================================================================
# TIME / PARSING HELPERS
# =============================================================================
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}
def parse_hour_start_from_slug(slug: str, year: int = None) -> datetime:
    """
    Parse slug like: bitcoin-up-or-down-february-14-9pm-et
    Returns hour start UTC.
    """
    import re
    import pytz
    et = pytz.timezone("US/Eastern")
    if year is None:
        year = utc_now().year
    m = re.search(r"-(january|february|march|april|may|june|july|august|september|october|november|december)-(\d{1,2})-(\d{1,2})(am|pm)-et$", slug)
    if not m:
        raise ValueError(f"Cannot parse hour from slug: {slug}")
    month = MONTHS[m.group(1)]
    day = int(m.group(2))
    hour12 = int(m.group(3))
    ampm = m.group(4)
    hour = hour12 % 12 + (12 if ampm == "pm" else 0)
    dt_local = et.localize(datetime(year, month, day, hour, 0, 0))
    return dt_local.astimezone(timezone.utc)
def minutes_into_hour(hour_start_utc: datetime, now_utc: datetime) -> float:
    return (now_utc - hour_start_utc).total_seconds() / 60.0
# =============================================================================
# STRATEGY FUNCTIONS (the exact logic)
# =============================================================================
def entry_threshold_bps(coin: str, t_min: float) -> float:
    # F247_LIKE: coin-specific thresholds only
    tbl = _THR_TABLE.get(coin, {"early": 8, "mid": 10, "late": 6})
    if TRADE_START_MIN <= t_min < 15:
        thr = tbl["early"]
    elif 15 <= t_min < 45:
        thr = tbl["mid"]
    elif 45 <= t_min <= 57:
        thr = tbl["late"]
    else:
        return 10_000
    # Special rule: XRP min 30-40 reduce by 2 bps (F247 aggressive)
    if coin == "XRP" and 30 <= t_min < 40:
        thr = max(1, thr - 2)
    return thr
def price_cap(t_min: float) -> float:
    if 0 <= t_min < 5:   return CAP_0_5
    if 5 <= t_min < 15:  return CAP_5_15
    if 15 <= t_min < 30: return CAP_15_30
    if 30 <= t_min < 45: return CAP_30_45
    if 45 <= t_min < 60: return CAP_45_60
    return 0.0
def dynamic_cap(t_min: float, abs_edge_bps: float) -> float:
    """Price cap with dynamic boost based on edge strength."""
    base = price_cap(t_min)
    if abs_edge_bps <= CAP_BOOST_EDGE_THRESHOLD:
        return base
    frac = min(1.0, (abs_edge_bps - CAP_BOOST_EDGE_THRESHOLD) /
               max(1.0, CAP_BOOST_EDGE_FULL - CAP_BOOST_EDGE_THRESHOLD))
    boost = frac * CAP_BOOST_MAX
    return min(0.99, base + boost)
def spread_limit(t_min: float, abs_edge_bps: float, coin: str, in_burst: bool = False) -> float:
    """Return max allowed spread for entry gating.
    Crossing only when spread <= 2c (IMB_MAX_SPREAD).
    Maker posting allowed up to MAKER_MAX_SPREAD (6c).
    During burst: controlled by burst engine's own maker/taker logic."""
    if in_burst:
        return BURST_SPREAD_HARD_LIMIT  # burst engine manages its own spread logic
    # Allow entry up to MAKER_MAX_SPREAD — burst engine will decide maker vs taker
    thr = entry_threshold_bps(coin, t_min)
    if 45 <= t_min <= 57 or abs_edge_bps >= thr + 10:
        return SPREAD_RELAXED_MAX
    return MAKER_MAX_SPREAD
def sizing_mult(abs_delta_bps: float) -> float:
    for lo, hi, mult in SIZING_MULTIPLIERS:
        if lo <= abs_delta_bps < hi:
            return mult
    return 0.0
def zscore(delta_series: List[Tuple[str, float]]) -> float:
    """
    Z-score of latest delta vs last ~Z_WINDOW_SEC.
    """
    if len(delta_series) < 10:
        return 0.0
    now = utc_now()
    cutoff = now - timedelta(seconds=Z_WINDOW_SEC)
    vals = []
    for ts_iso, d in reversed(delta_series):
        try:
            t = datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if t < cutoff:
            break
        vals.append(d)
    if len(vals) < 10:
        return 0.0
    mu = sum(vals) / len(vals)
    var = sum((x - mu) ** 2 for x in vals) / max(1, len(vals) - 1)
    sd = math.sqrt(var) if var > 1e-12 else 0.0
    if sd <= 1e-12:
        return 0.0
    return (vals[0] - mu) / sd  # vals[0] is latest because we appended reversed
def delta_velocity_bps_per_min(delta_series: List[Tuple[str, float]], lookback_sec: float = 30.0) -> float:
    """Legacy wrapper — returns 0.0 for back-compat with call sites that don't expect None."""
    if len(delta_series) < 2:
        return 0.0
    now = utc_now()
    target = now - timedelta(seconds=lookback_sec)
    latest_ts, latest_d = delta_series[-1]
    latest_t = datetime.strptime(latest_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    prior_d = None
    prior_t = None
    for ts_iso, d in reversed(delta_series[:-1]):
        t = datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if t <= target:
            prior_d = d
            prior_t = t
            break
    if prior_d is None or prior_t is None:
        return 0.0
    dt_min = (latest_t - prior_t).total_seconds() / 60.0
    if dt_min <= 1e-6:
        return 0.0
    return (latest_d - prior_d) / dt_min

def spot_velocity_bps_per_min(spot_hist: List[Tuple[float, float]], lookback_sec: float = 30.0) -> Optional[float]:
    """Compute velocity from spot history: vel_bps_per_min = ((spot_now - spot_old)/spot_old)*10000 / (dt/60s).
    Returns None if insufficient history (<2 points or dt < 200ms)."""
    if len(spot_hist) < 2:
        return None
    ts_now, spot_now = spot_hist[-1]
    # Find oldest point within lookback window
    cutoff = ts_now - lookback_sec
    ts_old, spot_old = spot_hist[0]
    for ts, sp in spot_hist:
        if ts >= cutoff:
            ts_old, spot_old = ts, sp
            break
    # If the "oldest within window" IS the latest point, use oldest overall
    if ts_old >= ts_now - 0.001:
        ts_old, spot_old = spot_hist[0]
    dt_ms = (ts_now - ts_old) * 1000.0
    if dt_ms < 200.0 or spot_old <= 0:
        return None
    dt_min = (ts_now - ts_old) / 60.0
    return ((spot_now - spot_old) / spot_old) * 10000.0 / dt_min
def persistence_ok(signal_series: List[Tuple[str, bool]]) -> bool:
    """
    signal_series holds (ts_iso, signal_bool) for last entries.
    True if signal has been continuously True for >= PERSISTENCE_SEC.
    """
    if not signal_series:
        return False
    now = utc_now()
    cutoff = now - timedelta(seconds=PERSISTENCE_SEC)
    # Must find that all points since cutoff are True
    for ts_iso, s in reversed(signal_series):
        t = datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if t < cutoff:
            break
        if not s:
            return False
    # Also ensure we have coverage back to cutoff
    oldest_ts = signal_series[0][0]
    try:
        oldest_t = datetime.strptime(oldest_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return False
    return oldest_t <= cutoff or len(signal_series) > 10

def taker_gate_allows(spread_cents: float, abs_edge_bps: float, thr_bps: float) -> bool:
    """Return True only if taker (crossing) is permitted.
    BOTH conditions must be true: spread <= 1c AND edge >= thr + 12."""
    return (spread_cents <= TAKER_MAX_SPREAD_CENTS and
            abs_edge_bps >= thr_bps + TAKER_MIN_EDGE_EXTRA_BPS)

def whipsaw_ok(delta_bps: float, vel: Optional[float],
               edge_sign_since: Optional[float]) -> Tuple[bool, str]:
    """Return (allowed, block_reason). Blocks entry in chop conditions."""
    # 1. Sign stability: delta sign must be unchanged for >= ENTRY_MIN_STABLE_SIGN_MS
    if edge_sign_since is None:
        return False, "sign_no_history"
    elapsed_ms = (time.time() - edge_sign_since) * 1000
    if elapsed_ms < ENTRY_MIN_STABLE_SIGN_MS:
        return False, f"sign_unstable({elapsed_ms:.0f}ms<{ENTRY_MIN_STABLE_SIGN_MS}ms)"
    # 2. Velocity alignment: vel must not oppose delta_bps
    #    If vel is None (unknown), skip this check — allow entry
    if vel is not None and BLOCK_IF_VEL_OPPOSES and abs(vel) >= VEL_OPPOSE_THRESHOLD:
        delta_sign = 1 if delta_bps > 0 else -1
        vel_sign = 1 if vel > 0 else -1
        if delta_sign != vel_sign:
            return False, f"vel_opposes(delta={delta_bps:+.1f},vel={vel:+.1f})"
    return True, ""

def parity_net_edge_cents(raw_edge_cents: float, up_book: "BookTop", dn_book: "BookTop",
                          is_buy: bool) -> Tuple[float, float, float]:
    """Compute net parity edge after estimated fees and slippage.
    Returns (net_edge_cents, total_fee_cents, total_slippage_cents).
    For BUY straddle: we cross both asks (taker) or post bids (maker).
    For SELL straddle: we cross both bids (taker) or post asks (maker)."""
    total_fee_cents = 0.0
    total_slippage_cents = 0.0
    for book in (up_book, dn_book):
        spread_cents = book.spread * 100
        use_taker = spread_cents <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
        if use_taker:
            # Taker: fee + half-spread slippage
            fee = TAKER_FEE_BPS / 100.0  # bps -> cents (approx: 1 bps on $0.50 ~ 0.005c)
            slippage = spread_cents / 2.0
        else:
            # Maker: fee only, no slippage (we're at best price)
            fee = MAKER_FEE_BPS / 100.0
            slippage = 0.0
        total_fee_cents += fee
        total_slippage_cents += slippage
    net = raw_edge_cents - total_fee_cents - total_slippage_cents
    return net, total_fee_cents, total_slippage_cents

def compute_fee_usdc(notional_usdc: float, maker_taker: str) -> float:
    """Compute fee in USDC for a given notional and maker/taker type."""
    if maker_taker == "maker":
        return notional_usdc * MAKER_FEE_BPS / 10000.0
    elif maker_taker == "taker":
        return notional_usdc * TAKER_FEE_BPS / 10000.0
    # If unknown, assume taker (conservative)
    return notional_usdc * TAKER_FEE_BPS / 10000.0

def parity_liquidity_ok(up_book: "BookTop", dn_book: "BookTop") -> Tuple[bool, str]:
    """Check liquidity and spread guards for parity entry.
    Returns (ok, block_reason)."""
    for label, book in [("up", up_book), ("dn", dn_book)]:
        spread_cents = book.spread * 100
        if spread_cents > MAX_SPREAD_FOR_PARITY_CENTS:
            return False, f"{label}_spread({spread_cents:.1f}c>{MAX_SPREAD_FOR_PARITY_CENTS}c)"
        # Check top-of-book liquidity in USD
        bid_usd = book.bid_sz * book.bid if book.bid > 0 else 0.0
        ask_usd = book.ask_sz * book.ask if book.ask > 0 else 0.0
        if bid_usd < MIN_TOP_LIQ_USD:
            return False, f"{label}_bid_liq(${bid_usd:.1f}<${MIN_TOP_LIQ_USD})"
        if ask_usd < MIN_TOP_LIQ_USD:
            return False, f"{label}_ask_liq(${ask_usd:.1f}<${MIN_TOP_LIQ_USD})"
    return True, ""

# =============================================================================
# POLYMARKET ADAPTER — wired to real CLOB via py_clob_client + Binance spot
# =============================================================================
class PolymarketClient:
    """
    Live Polymarket CLOB client.
    Uses POLYMARKET_PRIVATE_KEY from .env (same key as the pruned repo).
    Market discovery via Gamma API, spot/open via Binance public API,
    orderbook via CLOB /book endpoint, orders via py_clob_client SDK.

    In LOG mode order placement returns paper ids; everything else is live data.
    """

    CRYPTO_FULL_NAMES = {
        "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "xrp",
    }
    BINANCE_SYMBOLS = {
        "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT",
    }

    def __init__(self):
        from dotenv import load_dotenv
        # Try keys dir first (../keys/.env), then repo root (.env)
        _env_keys = os.path.join(_KEYS_DIR, ".env")
        if os.path.exists(_env_keys):
            load_dotenv(_env_keys)
        else:
            load_dotenv()  # fallback: searches CWD and parents

        self.session = requests.Session()
        self.gamma_url = "https://gamma-api.polymarket.com"
        self.clob_host = "https://clob.polymarket.com"
        self._clob = None           # ClobClient (lazy — only needed for LIVE orders)
        self._wallet_address = None

        # Market cache: slug -> {up_id, down_id, market_id, ...}
        self._market_cache: Dict[str, dict] = {}
        self._market_cache_ts: Dict[str, float] = {}
        self._cache_ttl = 45  # seconds

        # Binance hour-open cache: crypto -> (open, fetched_hour_start_ts)
        self._hour_open_cache: Dict[str, Tuple[float, int]] = {}

        private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
        if private_key:
            self._init_clob(private_key)
        elif MODE == "LIVE":
            print("ERROR: POLYMARKET_PRIVATE_KEY required for LIVE mode")
            sys.exit(1)

    # ------------------------------------------------------------------ #
    # CLOB client initialisation (same pattern as src/clients)
    # ------------------------------------------------------------------ #
    def _init_clob(self, private_key: str):
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.constants import POLYGON
        except ImportError:
            print("ERROR: pip install py-clob-client")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: Failed to import py_clob_client: {e}")
            print("Try: pip install --upgrade py-clob-client httpx click")
            sys.exit(1)

        self._clob = ClobClient(
            host=self.clob_host,
            key=private_key,
            chain_id=POLYGON,
            signature_type=0,       # EOA wallet
        )
        self._wallet_address = self._clob.get_address()
        print(f"[pm] Wallet: {self._wallet_address}")

        # Derive API creds for order status / cancel
        try:
            creds = self._clob.create_or_derive_api_creds()
            if creds:
                self._clob = ClobClient(
                    host=self.clob_host,
                    key=private_key,
                    chain_id=POLYGON,
                    creds=creds,
                    signature_type=0,
                    funder=self._wallet_address,
                )
                print(f"[pm] API Key: {creds.api_key[:8]}...")
        except Exception as e:
            print(f"[pm] WARN: API credential derivation failed: {e}")

    # ================================================================== #
    #  1.  MARKET DISCOVERY
    # ================================================================== #
    def get_current_hour_markets(self) -> List[MarketRef]:
        """
        Discover current-hour crypto Up/Down markets on Polymarket.
        Builds the slug in ET time, hits Gamma API /events/slug/{slug},
        extracts token IDs for Up and Down outcomes.
        """
        import pytz

        now_utc = utc_now()
        et = pytz.timezone("US/Eastern")
        now_et = now_utc.astimezone(et)

        hour_start_utc = now_utc.replace(minute=0, second=0, microsecond=0)

        month_name = now_et.strftime("%B").lower()   # "february"
        day_str    = str(now_et.day)                 # "14" (no zero-pad)
        hour12     = now_et.hour % 12 or 12          # 1-12
        ampm       = "am" if now_et.hour < 12 else "pm"

        markets: List[MarketRef] = []

        for crypto in CRYPTOS:
            full = self.CRYPTO_FULL_NAMES[crypto]
            slug = f"{full}-up-or-down-{month_name}-{day_str}-{hour12}{ampm}-et"

            try:
                data = self._resolve_market(slug)
                if data is None:
                    continue

                _, hour_open = self.get_binance_spot_and_hour_open(crypto)

                markets.append(MarketRef(
                    crypto=crypto,
                    slug=slug,
                    market_id=data["market_id"],
                    outcome_up_id=data["up_id"],
                    outcome_down_id=data["down_id"],
                    hour_open=hour_open,
                    hour_start_utc=hour_start_utc,
                ))
            except Exception as e:
                write_jsonl({"event_type":"MARKET_DISCOVERY_ERROR", "crypto": crypto,
                             "slug": slug, "err": str(e)})
        return markets

    def _resolve_market(self, slug: str) -> Optional[dict]:
        """Fetch event by slug from Gamma API; cache results."""
        now = time.time()
        if slug in self._market_cache:
            if now - self._market_cache_ts.get(slug, 0) < self._cache_ttl:
                return self._market_cache[slug]

        event = self._gamma_event_by_slug(slug)
        if event is None:
            return None

        market_list = event.get("markets", [])
        if not isinstance(market_list, list) or not market_list:
            market_list = [event]
        market = market_list[0]

        # Parse outcomes / token IDs (may be JSON strings or lists)
        outcomes_raw = market.get("outcomes")
        tokens_raw   = market.get("clobTokenIds")
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
        tokens   = json.loads(tokens_raw)   if isinstance(tokens_raw, str)   else (tokens_raw or [])

        if len(outcomes) < 2 or len(tokens) < 2:
            return None

        up_id, down_id = None, None
        for i, out in enumerate(outcomes):
            if i >= len(tokens):
                continue
            label = str(out).upper()
            if "UP" in label or "YES" in label:
                up_id = str(tokens[i])
            elif "DOWN" in label or "NO" in label:
                down_id = str(tokens[i])
        if not up_id or not down_id:
            up_id, down_id = str(tokens[0]), str(tokens[1])

        market_id = str(market.get("id") or market.get("conditionId") or slug)

        result = {"up_id": up_id, "down_id": down_id, "market_id": market_id}
        self._market_cache[slug] = result
        self._market_cache_ts[slug] = now
        return result

    def _gamma_event_by_slug(self, slug: str) -> Optional[dict]:
        url = f"{self.gamma_url}/events/slug/{slug}"
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return None

    # ================================================================== #
    #  2.  BINANCE SPOT & HOUR-OPEN
    # ================================================================== #
    # CoinGecko IDs for fallback price fetching
    COINGECKO_IDS = {
        "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "ripple",
    }

    # Binance data API — the public mirror Polymarket hourly markets resolve against.
    # api.binance.com is geo-blocked in the US; data-api.binance.vision is not.
    BINANCE_BASE = "https://data-api.binance.vision"

    def get_binance_spot_and_hour_open(self, crypto: str) -> Tuple[float, float]:
        """
        Return (spot, hour_open) from Binance data API.
        Uses data-api.binance.vision (same source Polymarket resolves against).
        Falls back to CoinGecko if Binance is unreachable.
        """
        sym = self.BINANCE_SYMBOLS[crypto]

        # --- spot ---
        spot = 0.0
        try:
            r = self.session.get(
                f"{self.BINANCE_BASE}/api/v3/ticker/price",
                params={"symbol": sym}, timeout=5,
            )
            if r.status_code == 200:
                spot = float(r.json()["price"])
        except Exception:
            pass

        # --- CoinGecko fallback ---
        if spot == 0.0:
            cg_id = self.COINGECKO_IDS.get(crypto)
            if cg_id:
                try:
                    r = self.session.get(
                        "https://api.coingecko.com/api/v3/simple/price",
                        params={"ids": cg_id, "vs_currencies": "usd"},
                        timeout=5,
                    )
                    if r.status_code == 200:
                        spot = float(r.json()[cg_id]["usd"])
                except Exception:
                    pass

        # --- hour open (cached per hour boundary) ---
        current_hour_ts = int(utc_now().timestamp()) // 3600 * 3600
        cached = self._hour_open_cache.get(crypto)
        if cached and cached[1] == current_hour_ts:
            return (spot if spot > 0 else cached[0], cached[0])

        hour_open = spot  # fallback if kline fetch fails
        try:
            hour_ms = current_hour_ts * 1000
            r = self.session.get(
                f"{self.BINANCE_BASE}/api/v3/klines",
                params={"symbol": sym, "interval": "1h",
                        "startTime": hour_ms, "limit": 1},
                timeout=5,
            )
            if r.status_code == 200:
                kline = r.json()[0]
                hour_open = float(kline[1])  # index 1 = candle open
        except Exception:
            pass

        if hour_open > 0:
            self._hour_open_cache[crypto] = (hour_open, current_hour_ts)
        return (spot, hour_open)

    # ================================================================== #
    #  3.  ORDERBOOK — top-of-book with depth imbalance
    # ================================================================== #
    def get_top_of_book(self, token_id: str, levels: int = 5) -> BookTop:
        """
        Fetch CLOB orderbook and return BookTop with best bid/ask,
        sizes, spread, mid-price, and N-level depth imbalance ratio.
        """
        empty = BookTop(bid=0.0, ask=1.0, bid_sz=0.0, ask_sz=0.0,
                        spread=1.0, imb=0.0, mid=0.5,
                        depth_1c_bid=0.0, depth_1c_ask=0.0,
                        depth_2c_bid=0.0, depth_2c_ask=0.0,
                        depth_5c_bid=0.0, depth_5c_ask=0.0)
        url = f"{self.clob_host}/book"
        try:
            r = self.session.get(url, params={"token_id": token_id}, timeout=2)
            if r.status_code != 200:
                return empty
            data = r.json()
        except Exception:
            return empty

        raw_bids = data.get("bids") or []
        raw_asks = data.get("asks") or []

        # Sort: bids descending by price, asks ascending
        bids = sorted(raw_bids, key=lambda x: float(x.get("price", 0)), reverse=True)
        asks = sorted(raw_asks, key=lambda x: float(x.get("price", 999)))

        best_bid = float(bids[0]["price"]) if bids else 0.0
        best_ask = float(asks[0]["price"]) if asks else 1.0
        bid_sz   = float(bids[0]["size"])  if bids else 0.0
        ask_sz   = float(asks[0]["size"])  if asks else 0.0

        spread = best_ask - best_bid
        mid    = (best_bid + best_ask) / 2.0

        # N-level depth imbalance = total_bid_depth / total_ask_depth
        bid_depth = sum(float(b.get("size", 0)) for b in bids[:levels])
        ask_depth = sum(float(a.get("size", 0)) for a in asks[:levels])
        imb = bid_depth / ask_depth if ask_depth > 0 else 999.0

        # Depth at price increments (cumulative size within Xc of best)
        def _cum_depth(orders, ref_price, cents, side_is_bid):
            total = 0.0
            for o in orders:
                px = float(o.get("price", 0))
                if side_is_bid and px >= ref_price - cents:
                    total += float(o.get("size", 0))
                elif not side_is_bid and px <= ref_price + cents:
                    total += float(o.get("size", 0))
            return total

        d1b = _cum_depth(bids, best_bid, 0.01, True)
        d1a = _cum_depth(asks, best_ask, 0.01, False)
        d2b = _cum_depth(bids, best_bid, 0.02, True)
        d2a = _cum_depth(asks, best_ask, 0.02, False)
        d5b = _cum_depth(bids, best_bid, 0.05, True)
        d5a = _cum_depth(asks, best_ask, 0.05, False)

        return BookTop(
            bid=best_bid, ask=best_ask,
            bid_sz=bid_sz, ask_sz=ask_sz,
            spread=spread, imb=imb, mid=mid,
            depth_1c_bid=d1b, depth_1c_ask=d1a,
            depth_2c_bid=d2b, depth_2c_ask=d2a,
            depth_5c_bid=d5b, depth_5c_ask=d5a,
        )

    # ================================================================== #
    #  4.  ORDER PLACEMENT / CANCEL
    # ================================================================== #
    def place_limit_order(self, token_id: str, side: str, price: float,
                          size: float, post_only: bool = True) -> dict:
        """
        Place a limit order on the CLOB.
        Returns dict with fill info: {order_id, filled, fill_qty, fill_price, status}.
        In LOG mode returns a paper result.
        """
        if MODE == "LOG":
            pid = f"paper_{int(time.time()*1000)}_{random.randint(100,999)}"
            return {"order_id": pid, "filled": True, "fill_qty": int(float(size)),
                    "fill_price": price, "status": "matched"}

        if not self._clob:
            raise RuntimeError("CLOB client not initialised (missing POLYMARKET_PRIVATE_KEY)")

        from py_clob_client.clob_types import OrderArgs, OrderType

        price = max(0.01, min(0.99, price))
        qty   = int(float(size))
        if qty < 1:
            return {"order_id": "", "filled": False, "fill_qty": 0,
                    "fill_price": 0.0, "status": "rejected"}

        order_type = OrderType.GTC  # limit / resting order

        try:
            args = OrderArgs(
                price=price,
                size=qty,
                side=side.upper(),
                token_id=token_id,
            )
            signed   = self._clob.create_order(args)
            response = self._clob.post_order(signed, order_type)

            if response and isinstance(response, dict):
                oid = response.get("orderID", "")
                status = response.get("status", "").lower()
                size_matched = response.get("size_matched") or 0
                tx_hashes = (response.get("transactionsHashes", [])
                             or response.get("transactionHashes", []))

                filled = False
                fill_qty = 0
                if status == "matched" or tx_hashes:
                    filled = True
                    fill_qty = int(size_matched) if size_matched else qty
                elif status == "live" and oid:
                    fill_qty = self._poll_order_fill(oid, qty, timeout=5)
                    filled = fill_qty > 0

                write_jsonl({"event_type":"ORDER_PLACED", "order_id": oid,
                             "token_id": token_id[-12:], "side": side,
                             "price": price, "qty": qty,
                             "status": status, "filled": filled,
                             "fill_qty": fill_qty})
                return {"order_id": oid, "filled": filled, "fill_qty": fill_qty,
                        "fill_price": price, "status": status}
        except Exception as e:
            write_jsonl({"event_type":"ORDER_ERROR", "err": str(e)[:200],
                         "token_id": token_id[-12:], "side": side,
                         "price": price, "qty": qty})
        return {"order_id": "", "filled": False, "fill_qty": 0,
                "fill_price": 0.0, "status": "error"}

    def _poll_order_fill(self, order_id: str, expected_qty: int,
                         timeout: int = 5) -> int:
        """Poll CLOB briefly for GTC order fill. Returns filled qty (0 if not filled)."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                order = self._clob.get_order(order_id)
                if order and isinstance(order, dict):
                    status = order.get("status", "").lower()
                    if status in ("matched", "filled"):
                        return int(order.get("size_matched", expected_qty)
                                   or expected_qty)
                    if status in ("cancelled", "canceled", "expired"):
                        return 0
            except Exception:
                pass
            time.sleep(1)
        return 0

    def cancel_order(self, order_id: str) -> None:
        """Cancel a single order by id."""
        if MODE == "LOG":
            return
        if not self._clob:
            return
        try:
            self._clob.cancel(order_id)
        except Exception as e:
            write_jsonl({"event_type":"CANCEL_ERROR", "order_id": order_id, "err": str(e)[:120]})

    def get_open_orders(self) -> List[dict]:
        """Return currently open/live orders."""
        if not self._clob:
            return []
        try:
            result = self._clob.get_orders()
            if isinstance(result, list):
                return [o for o in result
                        if isinstance(o, dict) and o.get("status", "").upper() in ("LIVE", "OPEN")]
            return []
        except Exception:
            return []

    def get_live_positions(self) -> Dict[str, Dict[str, float]]:
        """Fetch live token positions from the CLOB / conditional-tokens API.
        Returns: {token_id: {"size": float, "avg_price": float}} or empty dict."""
        if not self._clob or not self._wallet_address:
            return {}
        try:
            # py_clob_client >=0.15 has get_balances / get_complement
            if hasattr(self._clob, 'get_balances'):
                balances = self._clob.get_balances()
                if isinstance(balances, list):
                    return {b.get("asset_id", ""): {
                        "size": float(b.get("size", 0) or 0),
                        "avg_price": float(b.get("avg_price", 0) or 0),
                    } for b in balances if b.get("asset_id")}
            # Fallback: REST call to CLOB positions endpoint
            url = f"{self.clob_host}/positions?user={self._wallet_address}"
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    return {p.get("asset_id", p.get("token_id", "")): {
                        "size": float(p.get("size", 0) or 0),
                        "avg_price": float(p.get("avg_price", 0) or 0),
                    } for p in data if p.get("asset_id") or p.get("token_id")}
            return {}
        except Exception:
            return {}

    def get_usdc_balance(self) -> Optional[float]:
        """Fetch USDC balance from CLOB API. Returns None if unavailable."""
        if not self._clob or not self._wallet_address:
            return None
        try:
            if hasattr(self._clob, 'get_balance_allowance'):
                ba = self._clob.get_balance_allowance()
                if isinstance(ba, dict):
                    return float(ba.get("balance", 0) or 0) / 1e6  # USDC has 6 decimals
            return None
        except Exception:
            return None

# =============================================================================
# BOT CORE
# =============================================================================
class Bot:
    def __init__(self):
        global _LOGGER
        self.client = PolymarketClient()
        self.running = True
        self.cash_usdc = BANKROLL_START_USDC
        self.realized_pnl_usdc = 0.0  # cumulative realized P&L (sells + settlements)
        self.day_start = utc_now().date()
        self.day_start_equity = BANKROLL_START_USDC
        self.hour_start_equity = BANKROLL_START_USDC
        self.hourly_pnl_usdc = 0.0
        self._hour_window = utc_now().replace(minute=0, second=0, microsecond=0)
        # Risk-alert tracking (log-only, never enforced)
        self._hour_risk_stop_hit = False   # single flag per hour window (portfolio-wide)
        self._day_risk_stop_hit = False
        # Hourly stats for HOUR_SUMMARY
        self._hour_trade_count = 0
        self._hour_net_pnl = 0.0
        self._hour_edges: List[float] = []
        # Shadow stop-loss simulation
        self._shadow_active = False
        self._shadow_cash = 0.0
        self._shadow_positions: Dict[str, Dict[str, dict]] = {}  # slug->{outcome->pos}
        self._shadow_equity_at_trigger = 0.0
        self._shadow_trades_blocked = 0
        self.market_states: Dict[str, MarketState] = {}   # slug -> MarketState
        self.signal_hist: Dict[str, List[Tuple[str, bool]]] = {}  # slug -> [(ts, valid_signal)]
        self.last_book: Dict[str, Dict[str, BookTop]] = {}  # slug -> outcome -> BookTop
        self.recent_extreme_price: Dict[str, Dict[str, float]] = {} # slug->outcome->extreme
        # Whipsaw filter: slug -> (current_sign, since_ts)
        self._edge_sign_state: Dict[str, Tuple[int, float]] = {}
        # No-flip rule: slug -> (last_outcome, last_ts)
        self._last_trade_direction: Dict[str, Tuple[str, float]] = {}
        # Derisk edge worsening tracker: (slug, outcome) -> first_worsen_ts
        self._derisk_edge_worsen_since: Dict[Tuple[str, str], float] = {}
        # Per-minute diagnostic counters
        self._diag_taker_count = 0
        self._diag_maker_count = 0
        self._diag_derisk_count = 0
        self._diag_derisk_count_hour = 0     # per-hour cap for MAX_DERISK_PER_HOUR
        self._diag_derisk_reasons: Dict[str, int] = {}  # reason -> count (for PnL attribution)
        self._diag_derisk_taker_count = 0
        self._diag_blocked_whipsaw = 0
        self._diag_blocked_taker_gate = 0
        self._diag_blocked_noflip = 0
        # Parity arb tracking
        self._parity_last_order_ts: Dict[str, float] = {}    # slug -> last parity order timestamp
        self._parity_invested_usd: Dict[str, float] = {}     # slug -> total parity investment
        # Partial-fill tracking: list of pending pairs awaiting second leg
        # Each entry: {pair_id, slug, filled_outcome, filled_usd, filled_qty, filled_price,
        #              pending_outcome, ts, edge_net_cents}
        self._parity_pending_pairs: List[dict] = []
        # Maker queue discipline: slug -> {outcome -> {order_id, price, last_replace_ts}}
        self._parity_maker_orders: Dict[str, Dict[str, dict]] = {}
        # Locked straddle first-open timestamps: slug -> first_locked_ts
        self._parity_locked_since: Dict[str, float] = {}
        # Per-minute parity diagnostics
        self._diag_parity_buy_signals = 0
        self._diag_parity_sell_signals = 0
        self._diag_parity_trades = 0
        self._diag_parity_edges: List[float] = []            # NET cents captured per trade
        self._diag_parity_maker_count = 0
        self._diag_parity_taker_count = 0
        self._diag_pair_partial_count = 0
        self._diag_pair_fill_delays: List[float] = []        # ms per pair fill
        self._diag_unpaired_unwind_usd = 0.0
        self._diag_maker_orders_placed = 0
        self._diag_maker_fills = 0
        self._diag_cancel_replace_count = 0
        self._diag_parity_blocked_spread = 0
        self._diag_parity_blocked_liq = 0
        self._diag_parity_blocked_stale = 0
        self._diag_parity_blocked_fee = 0
        self._diag_recycle_count = 0
        # Maker fill-quality metrics
        self._diag_maker_fill_latencies: List[float] = []   # ms from place->fill per maker order
        self._diag_maker_timeout_cancel_count = 0
        self._diag_maker_lost_best_count = 0
        # End-of-hour flatten counters
        self._diag_flatten_actions = 0
        self._diag_flatten_taker = 0
        # Rescue-to-straddle counters
        self._diag_rescue_attempts = 0
        self._diag_rescue_success = 0
        self._diag_rescue_fallback_sells = 0
        # Parity quoting counters
        self._diag_quote_orders_placed = 0
        self._diag_quote_fills = 0
        self._diag_quote_unpaired_events = 0
        self._diag_quote_unpaired_escalations = 0
        self._diag_quote_pause_count = 0
        # Parity quoting: active quotes state {slug -> {outcome -> {price, ts, pair_id}}}
        self._parity_quotes: Dict[str, Dict[str, dict]] = {}
        # Per-slug dynamic quote target: slug -> current effective edge target (cents)
        self._quote_dynamic_target: Dict[str, float] = {}
        # Unpaired quote tracking: slug -> {outcome, fill_ts, pair_id, escalated}
        self._quote_unpaired: Dict[str, dict] = {}
        # Quote pause tracking: slug -> pause_until_ts
        self._quote_paused_until: Dict[str, float] = {}
        # Adverse selection guard
        self._diag_adverse_guard_events = 0
        self._diag_adverse_guard_pauses = 0
        self._diag_adverse_guard_degrades = 0
        # Per-slug degrade state: slug -> degrade_until_ts (soft mode: MAX target + 50% step)
        self._quote_degraded_until: Dict[str, float] = {}
        # Per-slug spot history for adverse selection + velocity + regime: slug -> [(epoch_ts, spot)]
        self._spot_history: Dict[str, List[Tuple[float, float]]] = {}
        # VELOCITY_DIAG: last emit ts per slug (1/min)
        self._vel_diag_last_ts: Dict[str, float] = {}
        # Warm-up: timestamp of bootstrap completion + last hour roll
        self._bootstrap_done_ts: float = 0.0
        self._last_hour_roll_ts: float = 0.0
        # GATE_BREAKDOWN: per-slug per-minute counters, reset every 60s
        self._gate_counters: Dict[str, Dict[str, int]] = {}  # slug -> {reason: count}
        self._gate_report_last_ts: float = 0.0
        # Rescue invested tracking: slug -> USD spent on rescue buys
        self._rescue_invested_usd: Dict[str, float] = {}
        # Similarity/tempo stats: timestamps of all parity trades this minute
        self._diag_parity_trade_timestamps: List[float] = []
        # ── Order lifecycle tracking ──
        # Active orders: order_id -> {slug, outcome, side, price, qty, submit_ts, reason}
        self._active_orders: Dict[str, dict] = {}
        # ── Central Order Manager (LIVE hardening) ──
        # Tracked open orders: order_id -> {slug, outcome, side, price, qty, filled_qty,
        #   reason, maker, created_ms, last_check_ms, status, st_ref, token_id}
        self._om_open_orders: Dict[str, dict] = {}
        self._om_last_orphan_scan_ts: float = 0.0
        self._om_orphan_canceled_count: int = 0
        self._om_orphan_canceled_min: int = 0
        self._om_submit_fail_count: int = 0
        self._om_submit_fail_min: int = 0
        self._om_cancel_fail_count: int = 0
        self._om_cancel_fail_min: int = 0
        self._om_partial_fill_events: int = 0
        self._om_partial_fill_events_min: int = 0
        self._om_api_errors_min: int = 0
        self._om_kill_switch_until: float = 0.0  # entries disabled until this ts
        self._om_last_sanity_ts: float = 0.0
        self._om_sanity_interval_sec: float = 60.0
        # Safety Item 2: per-slug no-progress circuit
        self._om_slug_submits_60s: Dict[str, List[float]] = {}   # slug -> [submit_ts, ...]
        self._om_slug_fills_60s: Dict[str, List[float]] = {}     # slug -> [fill_ts, ...]
        self._om_slug_paused_until: Dict[str, float] = {}        # slug -> pause_until_ts
        # Safety Item 3: cancel rate limits
        self._om_cancel_ts_global: List[float] = []              # timestamps of global cancels
        self._om_cancel_ts_slug: Dict[str, List[float]] = {}     # slug -> [cancel_ts, ...]
        self._om_cancel_freeze_until: float = 0.0                # global freeze on quoting
        self._om_last_replace_ts: Dict[str, float] = {}          # (slug+outcome+side) -> last replace ts
        # Safety Item 5: state drift detector — enhanced with position compare
        self._om_last_drift_check_ts: float = 0.0
        self._om_drift_pause_until: float = 0.0
        self._om_drift_count: int = 0                        # total drift events
        self._om_drift_position_mismatches: int = 0          # position-level mismatches
        # PnL attribution: 15m interval reporting
        self._pnl_report_last_ts: float = 0.0
        self._pnl_total_slug_pause_sec: float = 0.0          # cumulative slug pause time
        self._pnl_total_drift_pause_sec: float = 0.0         # cumulative drift pause time
        # Auto-disable slug state
        self._slug_realized_pnl_window: Dict[str, List[Tuple[float, float]]] = {}  # slug -> [(ts, pnl_usd)]
        self._slug_auto_disabled_until: Dict[str, float] = {}  # slug -> disable expiry ts
        self._diag_quote_submit_count = 0
        self._diag_quote_cancel_count = 0
        self._diag_quote_replace_count = 0
        # Clone-report dedicated counters (independent of tempo report reset cycle)
        self._clone_quote_submit_count = 0
        self._clone_quote_cancel_count = 0
        self._clone_quote_replace_count = 0
        self._clone_quote_fill_count = 0
        # Maker queue time tracking: list of (submit_ts, fill_ts) for filled maker orders
        self._diag_maker_queue_times: List[float] = []  # ms from submit to fill
        # Top-of-book tracking: slug -> {outcome -> {is_best: bool, best_since_ts: float}}
        self._top_of_book_state: Dict[str, Dict[str, dict]] = {}
        self._diag_top_of_book_time_ms = 0.0
        self._diag_top_of_book_total_ms = 0.0
        # ── Auto-hedge tracking ──
        # Overrides unpaired management with faster escalation
        # slug -> {outcome, fill_ts, tick1_done, tick2_done, cross_done}
        self._hedge_state: Dict[str, dict] = {}
        self._diag_hedge_tick1 = 0
        self._diag_hedge_tick2 = 0
        self._diag_hedge_cross = 0
        self._diag_hedge_cross_early = 0
        self._diag_hedge_cross_late = 0
        self._diag_hedge_skipped_stale = 0
        self._diag_hedge_unwind = 0
        # ── Pair fill tracker: pair_id -> {slug, crypto, fills: {outcome: fill_ts}} ──
        self._pair_tracker: Dict[str, dict] = {}
        self._diag_pairs_completed = 0
        self._diag_pairs_completed_500ms = 0
        self._diag_pairs_completed_1500ms = 0
        self._diag_pairs_completed_10s = 0
        # ── Per-slug pair completion KPI ──
        self._diag_slug_unpaired_events: Dict[str, int] = {}
        self._diag_slug_paired_500ms: Dict[str, int] = {}
        self._diag_slug_paired_1500ms: Dict[str, int] = {}
        self._diag_slug_timeouts: Dict[str, int] = {}
        # ── Pending fetch streak tracking ──
        self._diag_pending_fetch_streak: Dict[str, int] = {}
        self._diag_pending_fetch_streak_max: int = 0
        # ── Rate limiter state ──
        self._rate_last_order_ts: Dict[str, float] = {}     # slug -> last order submit timestamp
        self._rate_submit_count: Dict[str, int] = {}        # slug -> submits this minute
        self._rate_submit_window_start: Dict[str, float] = {}  # slug -> minute window start ts
        self._rate_blocked_interval = 0                      # diag: blocked by MIN_ORDER_INTERVAL_MS
        self._rate_blocked_cap = 0                           # diag: blocked by MAX_ORDER_SUBMITS_PER_MIN
        # ── Directional scalp state ──
        self._dscalp_positions: Dict[str, dict] = {}  # slug -> {outcome, entry_price, entry_ts, qty, tp1_done, tp2_done}
        self._dscalp_last_entry_ts: Dict[str, float] = {}   # slug -> last entry timestamp
        self._dscalp_invested_usd: Dict[str, float] = {}    # slug -> total invested USD
        # Directional scalp diagnostics (per-minute, reset in DIAG report)
        self._diag_dscalp_entries = 0
        self._diag_dscalp_exits = 0                          # all exits (TP + timeout + stop)
        self._diag_dscalp_tp1 = 0
        self._diag_dscalp_tp2 = 0
        self._diag_dscalp_tp3 = 0
        self._diag_dscalp_timeout_exits = 0
        self._diag_dscalp_stop_exits = 0
        self._diag_dscalp_hold_times: List[float] = []    # seconds
        self._diag_dscalp_exit_cents: List[float] = []     # profit/loss in cents
        self._diag_dscalp_breakeven_exits = 0               # breakeven exits count
        # Per-reason PnL attribution: reason -> [(pnl_cents, hold_sec, usdc_size)]
        self._diag_exit_by_reason: Dict[str, List[Tuple[float, float, float]]] = {}
        # Parity fill tracking (for parity_fill_pct)
        self._diag_parity_fills_min = 0                      # parity fills this minute
        self._diag_directional_fills_min = 0                 # directional fills this minute
        self._diag_total_fills_min = 0                       # all fills this minute
        # Trade size tracking
        self._diag_trade_sizes: List[float] = []             # USD per fill this minute
        # Rolling trade timestamps for trades/min throttle
        self._rolling_trade_ts: List[float] = []             # epoch timestamps of recent trades
        # Regime awareness state
        self._regime_vol_bps: Dict[str, float] = {}          # slug -> rolling 60s vol (bps std dev)
        self._regime_is_low_vol: Dict[str, bool] = {}        # slug -> True if low vol regime
        # ── True cost tracker ──
        self._true_cost_tx_count = 0                         # total tx count (fills + cancels)
        self._true_cost_fill_count = 0                       # fills this hour
        self._true_cost_fill_count_min = 0                   # fills this minute
        self._true_cost_submit_count = 0                     # submits this minute
        self._true_cost_cancel_count = 0                     # cancels this minute
        self._true_cost_hour_start_ts = time.time()
        # ── DIAG report tracking ──
        self._diag_report_last_ts = time.time()
        self._diag_derisk_count_min = 0
        self._diag_unpaired_count_min = 0
        # ── F247 similarity / CLONE_REPORT tracking ──
        # pair delays: list of ms between Up and Down fills for same slug
        self._clone_pair_delays: List[float] = []
        # inter-pair gaps: list of ms between consecutive pair completions
        self._clone_inter_pair_gaps: List[float] = []
        self._clone_last_pair_ts: float = 0.0
        # signal-to-fill: ms from significant spot move to first fill
        self._clone_signal_to_fill: List[float] = []
        # Track last significant spot move per slug: slug -> (ts, spot)
        self._clone_last_spot_move: Dict[str, Tuple[float, float]] = {}
        # Hold time tracking: list of hold durations (seconds) for completed straddle cycles
        self._clone_hold_times: List[float] = []
        # Per-slug max imbalance tracker for analytics: slug -> max_abs_imbalance this minute
        self._diag_max_imbalance: Dict[str, float] = {}
        # Per-slug imbalance x delta samples: list of (imbalance_shares, delta_bps)
        self._diag_imbalance_delta_samples: List[Tuple[float, float]] = []
        # Probe → Scale state machine: slug -> {state, probe_ts, probe_ask, initial_edge_bps}
        self.entry_sm: Dict[str, dict] = {}   # IDLE / PROBING / SCALING / COOLDOWN
        self.hour_index_counters: Dict[str, int] = {c: 0 for c in CRYPTOS}  # crypto -> monotonic index
        # Background data refresh infrastructure (sub-second loop)
        # Dynamic pool: max(BG_POOL_MIN_WORKERS, 3 * markets_count)
        dynamic_workers = max(BG_POOL_MIN_WORKERS, BG_POOL_WORKERS)
        self._bg_executor = ThreadPoolExecutor(max_workers=dynamic_workers)
        self._bg_running = True
        self._data_cache: Dict[str, dict] = {}   # slug -> {market, spot, hour_open, up_book, dn_book, ts}
        self._cached_markets: List[MarketRef] = []
        self._last_market_discovery_ts = 0.0
        self._last_save_ts = 0.0
        self._pending_fetches: Dict[str, bool] = {}  # slug -> True if fetch in-flight
        self._high_priority_slugs: set = set()       # markets needing fast refresh
        self._stale_skip_total = 0                   # counter: stale cache skips
        self._loop_count = 0
        # Deadline scheduling: slug -> next_due_ts
        self._bg_next_due: Dict[str, float] = {}
        # Per-slug refresh diagnostics
        self._bg_fetch_durations: Dict[str, List[float]] = {}  # slug -> [duration_ms, ...]
        self._bg_refresh_miss_count: Dict[str, int] = {}       # slug -> miss count
        self._bg_pending_cycles: Dict[str, int] = {}           # slug -> cycles pending
        # Tempo parity diagnostics
        self._tempo_fills: Dict[str, int] = {}       # slug -> fills this minute
        self._tempo_intents: Dict[str, int] = {}     # slug -> intents this minute
        self._tempo_cache_ages: Dict[str, List[float]] = {}  # slug -> cache ages this minute
        self._tempo_loop_times: List[float] = []     # loop durations this minute (ms)
        self._tempo_last_report_ts = 0.0
        # Initialise new Logger (replaces old write_jsonl / log_csv)
        self.logger = Logger(
            run_id=RUN_ID,
            log_dir=_LOG_DIR,
            mode=MODE,
            profile=PROFILE,
        )
        _LOGGER = self.logger  # expose globally for write_jsonl shim
        self._load_state()
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)
    def _handle_stop(self, *_):
        self.running = False
        self._bg_running = False
        write_jsonl({"event_type":"STOP_SIGNAL"})
    def _load_state(self):
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            # Support both old ("bankroll_usdc") and new ("cash_usdc") state files
            self.cash_usdc = float(raw.get("cash_usdc", raw.get("bankroll_usdc", self.cash_usdc)))
            self.realized_pnl_usdc = float(raw.get("realized_pnl_usdc", self.realized_pnl_usdc))
            self.hourly_pnl_usdc = float(raw.get("hourly_pnl_usdc", raw.get("daily_pnl_usdc", self.hourly_pnl_usdc)))
            self.day_start_equity = float(raw.get("day_start_equity", self.day_start_equity))
            self.hour_start_equity = float(raw.get("hour_start_equity", self.hour_start_equity))
            saved_day = raw.get("day_start")
            if saved_day:
                self.day_start = datetime.strptime(saved_day, "%Y-%m-%d").date()
            saved_hour = raw.get("hour_window")
            if saved_hour:
                self._hour_window = datetime.strptime(saved_hour, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            ms = raw.get("market_states", {})
            pos_fields = {f.name for f in Position.__dataclass_fields__.values()}
            ms_fields = {f.name for f in MarketState.__dataclass_fields__.values()}
            for slug, st_dict in ms.items():
                # Reconstruct Position objects from raw dicts (filter unknown keys)
                pos_raw = st_dict.get("positions")
                if isinstance(pos_raw, dict):
                    st_dict["positions"] = {
                        k: Position(**{pk: pv for pk, pv in v.items() if pk in pos_fields})
                            if isinstance(v, dict) else v
                        for k, v in pos_raw.items()
                    }
                # Filter unknown keys from MarketState too
                st_dict = {k: v for k, v in st_dict.items() if k in ms_fields}
                self.market_states[slug] = MarketState(**st_dict)
            # Hard-zero any dust positions loaded from state
            for st in self.market_states.values():
                for outcome in ["Up", "Down"]:
                    self._clean_dust(st.positions[outcome])
            loaded_schema = raw.get("schema_version", "unknown")
            write_jsonl({"event_type":"STATE_LOADED", "cash": self.cash_usdc,
                         "realized_pnl": self.realized_pnl_usdc,
                         "loaded_schema_version": loaded_schema,
                         "current_schema_version": SCHEMA_VERSION})
            # Safety Item 1: load persisted open orders
            self._om_load_open_orders()
        except Exception as e:
            write_jsonl({"event_type":"STATE_LOAD_ERROR", "err": str(e)})
    def _save_state(self):
        try:
            ms = {}
            for slug, st in self.market_states.items():
                d = asdict(st)
                # Trim histories for state file (full history is in JSONL)
                d["delta_hist"] = d.get("delta_hist", [])[-STATE_HIST_MAX:]
                d["price_hist"] = d.get("price_hist", [])[-STATE_HIST_MAX:]
                ms[slug] = d
            raw = {
                "schema_version": SCHEMA_VERSION,
                "run_id": RUN_ID,
                "cash_usdc": self.cash_usdc,
                "realized_pnl_usdc": self.realized_pnl_usdc,
                "hourly_pnl_usdc": self.hourly_pnl_usdc,
                "day_start_equity": self.day_start_equity,
                "hour_start_equity": self.hour_start_equity,
                "day_start": self.day_start.isoformat(),
                "hour_window": iso_z(self._hour_window),
                "equity_usdc": self._equity(),
                "market_states": ms,
            }
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(raw, f, separators=(",", ":"))
        except Exception as e:
            write_jsonl({"event_type":"STATE_SAVE_ERROR", "err": str(e)})
        # Safety Item 1: persist open orders separately (every save cycle)
        self._om_save_open_orders()
    def _equity(self) -> float:
        """Mark-to-market equity: cash + sum(pos.qty * mid_price) for all open positions."""
        mtm = 0.0
        for slug, st in self.market_states.items():
            for outcome in ["Up", "Down"]:
                pos = st.positions[outcome]
                if pos.qty < MIN_QTY:
                    continue
                book = self.last_book.get(slug, {}).get(outcome)
                mid = book.mid if book else pos.vwap  # fallback to vwap if no book yet
                mtm += pos.qty * mid
        return self.cash_usdc + mtm
    def _reset_daily_if_needed(self):
        today = utc_now().date()
        if today != self.day_start:
            self.day_start = today
            self.day_start_equity = self._equity()
            self._day_risk_stop_hit = False
            write_jsonl({"event_type":"NEW_DAY_RESET", "day_start_equity": round(self.day_start_equity, 2)})
        # Reset hourly state at each UTC hour boundary
        current_hour = utc_now().replace(minute=0, second=0, microsecond=0)
        if current_hour != self._hour_window:
            equity_now = self._equity()
            # ── HOUR_SUMMARY for the hour that just ended ──
            any_stop = self._hour_risk_stop_hit
            shadow_delta = 0.0
            if self._shadow_active and SHADOW_STOP_SIM:
                shadow_eq = self._shadow_equity()
                shadow_delta = equity_now - shadow_eq
                # ── RISK_STOP_SHADOW_RESULT ──
                write_jsonl({
                    "event_type": "RISK_STOP_SHADOW_RESULT",
                    "hour_window": iso_z(self._hour_window),
                    "hour_start_equity": round(self.hour_start_equity, 4),
                    "actual_equity": round(equity_now, 4),
                    "shadow_equity": round(shadow_eq, 4),
                    "actual_equity_change": round(equity_now - self.hour_start_equity, 4),
                    "shadow_equity_change": round(shadow_eq - self.hour_start_equity, 4),
                    "trades_blocked": self._shadow_trades_blocked,
                    "shadow_delta": round(shadow_delta, 4),
                })
            avg_edge = (sum(self._hour_edges) / len(self._hour_edges)) if self._hour_edges else 0.0
            write_jsonl({
                "event_type": "HOUR_SUMMARY",
                "hour_window": iso_z(self._hour_window),
                "hour_start_equity": round(self.hour_start_equity, 4),
                "equity_now": round(equity_now, 4),
                "trades": self._hour_trade_count,
                "net_pnl": round(self._hour_net_pnl, 4),
                "fees_estimate": 0.0,
                "avg_edge": round(avg_edge, 6),
                "stop_triggered": any_stop,
                "shadow_delta": round(shadow_delta, 4),
            })
            # ── Reset for new hour ──
            self._hour_window = current_hour
            self._last_hour_roll_ts = time.time()
            self.hourly_pnl_usdc = 0.0
            self.hour_start_equity = equity_now
            self._hour_risk_stop_hit = False
            self._hour_trade_count = 0
            self._hour_net_pnl = 0.0
            self._hour_edges = []
            # Reset shadow
            self._shadow_active = False
            self._shadow_cash = 0.0
            self._shadow_positions = {}
            self._shadow_equity_at_trigger = 0.0
            self._shadow_trades_blocked = 0
    # -----------------------------
    # Position helpers (paper-mode)
    # -----------------------------
    def _paper_buy(self, st: MarketState, outcome: str, price: float, qty: float, usdc_cost: float):
        pos = st.positions[outcome]
        new_cost = pos.cost_usdc + usdc_cost
        new_qty = pos.qty + qty
        pos.vwap = (pos.vwap * pos.qty + price * qty) / max(1e-12, new_qty)
        pos.qty = new_qty
        pos.cost_usdc = new_cost
        pos.last_trade_ts = iso_z(utc_now())
        if pos.opened_at is None:
            pos.opened_at = pos.last_trade_ts
        self.cash_usdc -= usdc_cost
        # Shadow: new entries are BLOCKED after stop trigger
        if self._shadow_active:
            self._shadow_trades_blocked += 1
        # Hourly stats
        self._hour_trade_count += 1
    def _paper_sell(self, st: MarketState, outcome: str, price: float, qty: float):
        pos = st.positions[outcome]
        qty = min(qty, pos.qty)
        if qty <= 0:
            return 0.0
        proceeds = price * qty
        cost_basis = pos.vwap * qty
        pnl = proceeds - cost_basis
        pos.qty -= qty
        pos.cost_usdc = max(0.0, pos.cost_usdc - cost_basis)
        pos.last_trade_ts = iso_z(utc_now())
        self.cash_usdc += proceeds
        self.realized_pnl_usdc += pnl
        self.hourly_pnl_usdc += pnl
        self._clean_dust(pos)
        # Shadow: exits still apply to existing shadow positions
        if self._shadow_active:
            sp = self._shadow_positions.get(st.slug, {}).get(outcome)
            if sp and sp["qty"] >= MIN_QTY:
                sell_qty = min(qty, sp["qty"])  # never sell more than shadow holds
                shadow_proceeds = price * sell_qty
                sp["qty"] = max(0.0, sp["qty"] - sell_qty)
                sp["cost_usdc"] = max(0.0, sp["cost_usdc"] - sp["vwap"] * sell_qty)
                self._shadow_cash += shadow_proceeds
        # Hourly stats
        self._hour_trade_count += 1
        self._hour_net_pnl += pnl
        return pnl
    def _live_buy(self, st: MarketState, outcome: str, price: float,
                  qty: float, usdc_cost: float):
        """Update position state after a real buy fill (mirrors _paper_buy)."""
        pos = st.positions[outcome]
        new_cost = pos.cost_usdc + usdc_cost
        new_qty = pos.qty + qty
        pos.vwap = (pos.vwap * pos.qty + price * qty) / max(1e-12, new_qty)
        pos.qty = new_qty
        pos.cost_usdc = new_cost
        pos.last_trade_ts = iso_z(utc_now())
        if pos.opened_at is None:
            pos.opened_at = pos.last_trade_ts
        self.cash_usdc -= usdc_cost
        self._hour_trade_count += 1
    def _live_sell(self, st: MarketState, outcome: str, price: float,
                   qty: float) -> float:
        """Update position state after a real sell fill (mirrors _paper_sell). Returns pnl."""
        pos = st.positions[outcome]
        qty = min(qty, pos.qty)
        if qty <= 0:
            return 0.0
        proceeds = price * qty
        cost_basis = pos.vwap * qty
        pnl = proceeds - cost_basis
        pos.qty -= qty
        pos.cost_usdc = max(0.0, pos.cost_usdc - cost_basis)
        pos.last_trade_ts = iso_z(utc_now())
        self.cash_usdc += proceeds
        self.realized_pnl_usdc += pnl
        self.hourly_pnl_usdc += pnl
        self._clean_dust(pos)
        self._hour_trade_count += 1
        self._hour_net_pnl += pnl
        return pnl
    @staticmethod
    def _clean_dust(pos: Position):
        """Zero out positions that are dust (< MIN_QTY)."""
        if abs(pos.qty) < MIN_QTY:
            pos.qty = 0.0
            pos.cost_usdc = 0.0
            pos.vwap = 0.0
            pos.opened_at = None
            pos.last_trade_ts = None
            pos.tp1_done = False
            pos.tp2_done = False
            pos.tp3_done = False
            pos.scalp_mode = False
            pos.scalp_open_ts = None
            pos.position_id = None
            pos.trade_id = None
            pos.entry_decision_id = None
            pos.parent_order_id = None
            pos.entry_mid = 0.0
            pos.max_favorable_mid = 0.0
            pos.max_adverse_mid = 1.0
            pos.last_derisk_ts = None
            pos.last_derisk_mid = 0.0
            pos.fast_tp_done = False

    # =========================================================================
    # ORDER MANAGER — Central live-order lifecycle (Phases 0-7)
    # =========================================================================

    def _om_kill_switch_active(self) -> bool:
        """True if kill-switch has disabled entries."""
        return time.time() < self._om_kill_switch_until

    def _om_has_open_order(self, slug: str, outcome: str, side: str) -> Optional[str]:
        """Return order_id if there is already an open order for (slug, outcome, side), else None."""
        for oid, o in self._om_open_orders.items():
            if o["slug"] == slug and o["outcome"] == outcome and o["side"] == side and o["status"] == "open":
                return oid
        return None

    def _om_open_count(self) -> int:
        return len(self._om_open_orders)

    def _om_submit_order(self, token_id: str, slug: str, outcome: str, side: str,
                         price: float, qty: float, reason: str, maker: bool = True,
                         st: Optional[MarketState] = None) -> dict:
        """Submit an order to the CLOB with retry+backoff. Track in _om_open_orders.
        Returns {order_id, filled, fill_qty, fill_price, status}.
        Counters: tx_count++ on successful submit, fill_count++ only on confirmed fill."""
        if MODE == "LOG":
            pid = f"paper_{int(time.time()*1000)}_{random.randint(100,999)}"
            return {"order_id": pid, "filled": True, "fill_qty": int(float(qty)),
                    "fill_price": price, "status": "matched"}

        # Global cap check
        if self._om_open_count() >= OM_MAX_OPEN_ORDERS:
            write_jsonl({"event_type": "OM_SUBMIT_BLOCKED", "reason": "max_open_orders",
                          "slug": slug, "side": side, "open_count": self._om_open_count()})
            return {"order_id": "", "filled": False, "fill_qty": 0, "fill_price": 0.0, "status": "blocked"}

        # Per-slug-side cap check
        existing_oid = self._om_has_open_order(slug, outcome, side)
        if existing_oid:
            write_jsonl({"event_type": "OM_SUBMIT_BLOCKED", "reason": "duplicate_order",
                          "slug": slug, "outcome": outcome, "side": side,
                          "existing_oid": existing_oid})
            return {"order_id": "", "filled": False, "fill_qty": 0, "fill_price": 0.0, "status": "blocked"}

        # Submit with retry
        last_err = ""
        for attempt in range(OM_SUBMIT_MAX_RETRIES):
            try:
                result = self.client.place_limit_order(token_id, side, price, qty, post_only=maker)
                oid = result.get("order_id", "")
                status = result.get("status", "")
                filled = result.get("filled", False)
                fill_qty = result.get("fill_qty", 0)
                fill_price = result.get("fill_price", 0.0)

                # Successful submit — count tx + record for slug progress
                if oid:
                    self._true_cost_tx_count += 1
                    self._true_cost_submit_count += 1
                    self._om_record_slug_submit(slug)

                # If fully filled immediately, count fill and do NOT track as open
                if filled and fill_qty >= int(float(qty)):
                    self._true_cost_fill_count += 1
                    self._true_cost_fill_count_min += 1
                    self._om_record_slug_fill(slug)
                    write_jsonl({"event_type": "OM_ORDER_FILLED_IMMEDIATE",
                                  "order_id": oid, "slug": slug, "outcome": outcome,
                                  "side": side, "price": fill_price, "qty": fill_qty,
                                  "reason": reason})
                    return result

                # Partial fill or resting — track in open orders
                if oid:
                    now_ms = int(time.time() * 1000)
                    self._om_open_orders[oid] = {
                        "slug": slug, "outcome": outcome, "side": side,
                        "price": price, "qty": int(float(qty)),
                        "filled_qty": fill_qty if fill_qty else 0,
                        "reason": reason, "maker": maker,
                        "created_ms": now_ms, "last_check_ms": now_ms,
                        "status": "open", "token_id": token_id,
                        "st_slug": st.slug if st else slug,
                        "cancel_pending": False,
                    }
                    # If partial fill happened, count that fill
                    if fill_qty and fill_qty > 0:
                        self._true_cost_fill_count += 1
                        self._true_cost_fill_count_min += 1
                        self._om_partial_fill_events += 1
                        self._om_partial_fill_events_min += 1
                        self._om_record_slug_fill(slug)
                    write_jsonl({"event_type": "OM_ORDER_TRACKED",
                                  "order_id": oid, "slug": slug, "outcome": outcome,
                                  "side": side, "price": price, "qty": int(float(qty)),
                                  "filled_qty": fill_qty or 0,
                                  "status": status, "reason": reason})
                    # Return with actual fill state (may be partial or unfilled)
                    return {"order_id": oid, "filled": filled, "fill_qty": fill_qty,
                            "fill_price": fill_price, "status": status}

                # No order_id returned but no exception — API rejected silently
                if status == "error":
                    self._om_api_errors_min += 1
                    last_err = f"api_rejected: {status}"
                    # Fall through to retry
                else:
                    return result

            except Exception as e:
                last_err = str(e)[:200]
                self._om_api_errors_min += 1
                write_jsonl({"event_type": "OM_SUBMIT_ERROR", "attempt": attempt + 1,
                              "slug": slug, "side": side, "err": last_err})

            # Backoff before retry
            if attempt < OM_SUBMIT_MAX_RETRIES - 1:
                backoff_ms = OM_SUBMIT_BACKOFF_MS[min(attempt, len(OM_SUBMIT_BACKOFF_MS) - 1)]
                time.sleep(backoff_ms / 1000.0)

        # All retries exhausted
        self._om_submit_fail_count += 1
        self._om_submit_fail_min += 1
        write_jsonl({"event_type": "OM_SUBMIT_FAILED", "slug": slug, "outcome": outcome,
                      "side": side, "price": price, "qty": qty, "reason": reason,
                      "last_err": last_err, "retries": OM_SUBMIT_MAX_RETRIES})
        return {"order_id": "", "filled": False, "fill_qty": 0, "fill_price": 0.0, "status": "submit_failed"}

    def _om_poll_and_reconcile(self, order_id: str) -> dict:
        """Poll a single open order and reconcile fills. Returns updated order info.
        Applies position state changes for any new fills detected."""
        entry = self._om_open_orders.get(order_id)
        if not entry:
            return {"status": "not_tracked"}

        if MODE == "LOG":
            # Paper mode: orders fill instantly, nothing to reconcile
            self._om_open_orders.pop(order_id, None)
            return {"status": "matched", "filled_qty": entry["qty"]}

        try:
            order = self.client._clob.get_order(order_id) if self.client._clob else None
            if not order or not isinstance(order, dict):
                entry["last_check_ms"] = int(time.time() * 1000)
                return {"status": "unknown"}

            clob_status = order.get("status", "").lower()
            size_matched = int(order.get("size_matched", 0) or 0)
            old_filled = entry["filled_qty"]
            delta_qty = max(0, size_matched - old_filled)

            if delta_qty > 0:
                # New fills detected — apply to position state
                entry["filled_qty"] = size_matched
                self._true_cost_fill_count += 1
                self._true_cost_fill_count_min += 1
                if delta_qty < (entry["qty"] - old_filled):
                    self._om_partial_fill_events += 1
                    self._om_partial_fill_events_min += 1

                # Apply fill to position
                slug = entry["slug"]
                outcome = entry["outcome"]
                fill_price = entry["price"]  # best available; CLOB may not give avg
                usdc_delta = fill_price * delta_qty
                self._om_record_slug_fill(slug)

                st = self.market_states.get(slug)
                if st:
                    if entry["side"] == "BUY":
                        self._live_buy(st, outcome, fill_price, delta_qty, usdc_delta)
                    else:
                        self._live_sell(st, outcome, fill_price, delta_qty)

                write_jsonl({"event_type": "OM_RECONCILE_FILL",
                              "order_id": order_id, "slug": slug, "outcome": outcome,
                              "side": entry["side"], "delta_qty": delta_qty,
                              "total_filled": size_matched, "total_qty": entry["qty"],
                              "fill_price": fill_price})

            if clob_status in ("matched", "filled"):
                # Fully filled — remove from tracking
                self._om_open_orders.pop(order_id, None)
                return {"status": "filled", "filled_qty": size_matched}
            elif clob_status in ("cancelled", "canceled", "expired"):
                self._om_open_orders.pop(order_id, None)
                return {"status": "cancelled", "filled_qty": size_matched}

            entry["last_check_ms"] = int(time.time() * 1000)
            return {"status": clob_status, "filled_qty": size_matched}

        except Exception as e:
            self._om_api_errors_min += 1
            write_jsonl({"event_type": "OM_RECONCILE_ERROR", "order_id": order_id,
                          "err": str(e)[:200]})
            entry["last_check_ms"] = int(time.time() * 1000)
            return {"status": "error"}

    def _om_cancel_order(self, order_id: str, reason: str = "ttl") -> bool:
        """Cancel an order with retry+backoff. Returns True if cancel succeeded or order gone."""
        entry = self._om_open_orders.get(order_id)
        if not entry:
            return True
        if MODE == "LOG":
            self._om_open_orders.pop(order_id, None)
            return True

        # Safety Item 3: Cancel rate-limit check (skip for hard_flatten/shutdown)
        slug = entry.get("slug", "")
        if reason not in ("hard_flatten", "shutdown", "kill_switch") and not self._om_cancel_rate_ok(slug):
            write_jsonl({"event_type": "OM_CANCEL_RATE_LIMITED", "order_id": order_id,
                          "slug": slug, "reason": reason})
            entry["cancel_pending"] = True
            return False

        for attempt in range(OM_CANCEL_MAX_RETRIES):
            try:
                self.client._clob.cancel(order_id)
                self._true_cost_cancel_count += 1
                self._om_record_cancel(slug)
                self._om_open_orders.pop(order_id, None)
                write_jsonl({"event_type": "OM_ORDER_CANCELED", "order_id": order_id,
                              "slug": entry["slug"], "outcome": entry["outcome"],
                              "side": entry["side"], "reason": reason,
                              "filled_qty": entry["filled_qty"], "total_qty": entry["qty"]})
                return True
            except Exception as e:
                self._om_api_errors_min += 1
                write_jsonl({"event_type": "OM_CANCEL_ERROR", "order_id": order_id,
                              "attempt": attempt + 1, "err": str(e)[:200]})
                if attempt < OM_CANCEL_MAX_RETRIES - 1:
                    backoff_ms = OM_CANCEL_BACKOFF_MS[min(attempt, len(OM_CANCEL_BACKOFF_MS) - 1)]
                    time.sleep(backoff_ms / 1000.0)

        # Mark cancel_pending for retry next tick
        entry["cancel_pending"] = True
        self._om_cancel_fail_count += 1
        self._om_cancel_fail_min += 1
        write_jsonl({"event_type": "OM_CANCEL_FAILED", "order_id": order_id,
                      "slug": entry["slug"], "reason": reason})
        return False

    def _om_reprice_order(self, order_id: str, new_price: float, reason: str = "reprice") -> Optional[str]:
        """Cancel old order and submit replacement at new_price. Returns new order_id or None."""
        entry = self._om_open_orders.get(order_id)
        if not entry:
            return None

        # Snapshot unfilled remainder
        remaining_qty = max(0, entry["qty"] - entry["filled_qty"])
        if remaining_qty < 1:
            self._om_cancel_order(order_id, "reprice_no_remainder")
            return None

        old_price = entry["price"]
        slug = entry["slug"]
        outcome = entry["outcome"]
        side = entry["side"]

        # Reprice guard: only reprice when justified
        price_move = abs(new_price - old_price)
        if REPRICE_REQUIRE_OUTBID and price_move < REPRICE_MIN_PRICE_MOVE:
            return None  # price hasn't moved enough — skip

        # Safety Item 3: Replace interval throttle
        if not self._om_replace_interval_ok(slug, outcome, side):
            return None  # too soon, skip this tick

        # Cancel old
        if not self._om_cancel_order(order_id, f"reprice:{reason}"):
            return None  # will retry next tick

        self._om_record_replace(slug, outcome, side)

        # Submit new with remaining qty
        st = self.market_states.get(slug)
        result = self._om_submit_order(
            token_id=entry["token_id"], slug=slug, outcome=outcome,
            side=side, price=new_price, qty=remaining_qty,
            reason=entry["reason"], maker=entry["maker"], st=st)

        new_oid = result.get("order_id", "")
        if new_oid:
            write_jsonl({"event_type": "OM_ORDER_REPLACED",
                          "old_id": order_id, "new_id": new_oid,
                          "slug": slug, "outcome": outcome, "side": side,
                          "old_price": old_price, "new_price": new_price,
                          "remaining_qty": remaining_qty})
        return new_oid if new_oid else None

    def _om_reconcile_all(self):
        """Main-loop maintenance: reconcile open orders, cancel stale, detect orphans."""
        if MODE == "LOG":
            return

        now_ms = int(time.time() * 1000)
        now_ts = time.time()
        checked = 0

        # 1. Reconcile + TTL enforcement on tracked orders
        stale_to_cancel = []
        cancel_pending_retry = []
        for oid, entry in list(self._om_open_orders.items()):
            if checked >= OM_RECONCILE_MAX_PER_TICK:
                break

            # Retry pending cancels first
            if entry.get("cancel_pending"):
                cancel_pending_retry.append(oid)
                continue

            # Poll for fill updates
            self._om_poll_and_reconcile(oid)
            checked += 1

            # TTL check: if no new fills for TTL period, mark for cancel
            if oid in self._om_open_orders:  # still tracked (not fully filled)
                age_ms = now_ms - entry["created_ms"]
                since_last_check = now_ms - entry["last_check_ms"]
                if age_ms > OM_MAKER_ORDER_TTL_MS and entry["maker"]:
                    stale_to_cancel.append(oid)

        # Cancel stale orders
        for oid in stale_to_cancel:
            self._om_cancel_order(oid, "ttl_expired")

        # Retry pending cancels
        for oid in cancel_pending_retry:
            self._om_cancel_order(oid, "cancel_retry")

        # 2. Periodic orphan scan (every OM_ORPHAN_SCAN_INTERVAL_SEC)
        if now_ts - self._om_last_orphan_scan_ts >= OM_ORPHAN_SCAN_INTERVAL_SEC:
            self._om_scan_orphans()
            self._om_last_orphan_scan_ts = now_ts

        # 3. Kill-switch evaluation
        if (self._om_orphan_canceled_min > OM_KILL_ORPHAN_THRESHOLD_PER_MIN or
                self._om_api_errors_min > OM_KILL_API_ERROR_THRESHOLD_PER_MIN):
            if not self._om_kill_switch_active():
                self._om_kill_switch_until = now_ts + OM_KILL_COOLDOWN_SEC
                write_jsonl({"event_type": "OM_KILL_SWITCH_TRIGGERED",
                              "orphans_min": self._om_orphan_canceled_min,
                              "api_errors_min": self._om_api_errors_min,
                              "cooldown_sec": OM_KILL_COOLDOWN_SEC})
                # Cancel all open orders and flatten
                for oid in list(self._om_open_orders.keys()):
                    self._om_cancel_order(oid, "kill_switch")

    def _om_scan_orphans(self):
        """Scan CLOB for orders not in our tracking — cancel them."""
        if MODE == "LOG" or not self.client._clob:
            return
        try:
            clob_orders = self.client.get_open_orders()
            tracked_ids = set(self._om_open_orders.keys())
            for o in clob_orders:
                oid = o.get("id") or o.get("orderID") or o.get("order_id", "")
                if oid and oid not in tracked_ids:
                    # Orphan detected — cancel it
                    try:
                        self.client._clob.cancel(oid)
                        self._om_orphan_canceled_count += 1
                        self._om_orphan_canceled_min += 1
                        write_jsonl({"event_type": "OM_ORPHAN_CANCELED",
                                      "order_id": oid,
                                      "status": o.get("status", ""),
                                      "side": o.get("side", ""),
                                      "price": o.get("price", ""),
                                      "size": o.get("size", "")})
                    except Exception as e:
                        write_jsonl({"event_type": "OM_ORPHAN_CANCEL_ERROR",
                                      "order_id": oid, "err": str(e)[:120]})
        except Exception as e:
            write_jsonl({"event_type": "OM_ORPHAN_SCAN_ERROR", "err": str(e)[:200]})

    # ── Phase 0: Unified buy execution ──

    def _exec_buy(self, st: MarketState, m, outcome: str, price: float,
                  qty: float, reason: str, prefer_maker: bool = True,
                  ctx: Optional[dict] = None) -> dict:
        """Unified buy execution for ALL engines. Paper or live.
        Returns {filled: bool, fill_qty, fill_price, usdc_cost, order_id}."""
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        usdc_cost = price * qty

        if MODE == "LOG":
            self._paper_buy(st, outcome, price, qty, usdc_cost)
            return {"filled": True, "fill_qty": int(float(qty)), "fill_price": price,
                    "usdc_cost": usdc_cost, "order_id": f"paper_{int(time.time()*1000)}"}

        # LIVE mode — use order manager
        if self._om_kill_switch_active():
            write_jsonl({"event_type": "OM_BUY_BLOCKED_KILLSWITCH",
                          "slug": m.slug, "outcome": outcome, "reason": reason})
            return {"filled": False, "fill_qty": 0, "fill_price": 0.0,
                    "usdc_cost": 0.0, "order_id": ""}

        # Safety Item 2: Per-slug no-progress circuit breaker
        if self._om_slug_paused(m.slug):
            write_jsonl({"event_type": "OM_BUY_BLOCKED_SLUG_PAUSED",
                          "slug": m.slug, "outcome": outcome, "reason": reason})
            return {"filled": False, "fill_qty": 0, "fill_price": 0.0,
                    "usdc_cost": 0.0, "order_id": ""}

        # Safety Item 5: State drift pause
        if self._om_drift_paused():
            write_jsonl({"event_type": "OM_BUY_BLOCKED_DRIFT_PAUSE",
                          "slug": m.slug, "outcome": outcome, "reason": reason})
            return {"filled": False, "fill_qty": 0, "fill_price": 0.0,
                    "usdc_cost": 0.0, "order_id": ""}

        # Slug auto-disable: block entries for slugs with bad rolling PnL
        if self._slug_auto_disabled(m.slug):
            write_jsonl({"event_type": "OM_BUY_BLOCKED_SLUG_AUTO_DISABLED",
                          "slug": m.slug, "outcome": outcome, "reason": reason})
            return {"filled": False, "fill_qty": 0, "fill_price": 0.0,
                    "usdc_cost": 0.0, "order_id": ""}

        result = self._om_submit_order(
            token_id=token_id, slug=m.slug, outcome=outcome,
            side="BUY", price=price, qty=qty, reason=reason,
            maker=prefer_maker, st=st)

        oid = result.get("order_id", "")
        filled = result.get("filled", False)
        fill_qty = result.get("fill_qty", 0)
        fill_price = result.get("fill_price", price)

        if filled and fill_qty > 0:
            actual_cost = fill_price * fill_qty
            self._live_buy(st, outcome, fill_price, fill_qty, actual_cost)
            return {"filled": True, "fill_qty": fill_qty, "fill_price": fill_price,
                    "usdc_cost": actual_cost, "order_id": oid}

        # Order resting or partially filled — position update happens via reconciliation
        return {"filled": False, "fill_qty": fill_qty, "fill_price": fill_price,
                "usdc_cost": fill_price * fill_qty if fill_qty else 0.0, "order_id": oid}

    def _exec_sell(self, st: MarketState, m, outcome: str, price: float,
                   qty: float, reason: str, prefer_maker: bool = True,
                   ctx: Optional[dict] = None) -> dict:
        """Unified sell execution. Paper or live.
        Returns {filled: bool, fill_qty, fill_price, pnl, order_id}."""
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id

        if MODE == "LOG":
            pnl = self._paper_sell(st, outcome, price, qty)
            return {"filled": True, "fill_qty": int(float(qty)), "fill_price": price,
                    "pnl": pnl, "order_id": f"paper_{int(time.time()*1000)}"}

        # LIVE mode — use order manager
        result = self._om_submit_order(
            token_id=token_id, slug=m.slug, outcome=outcome,
            side="SELL", price=price, qty=qty, reason=reason,
            maker=prefer_maker, st=st)

        oid = result.get("order_id", "")
        filled = result.get("filled", False)
        fill_qty = result.get("fill_qty", 0)
        fill_price = result.get("fill_price", price)

        if filled and fill_qty > 0:
            pnl = self._live_sell(st, outcome, fill_price, fill_qty)
            return {"filled": True, "fill_qty": fill_qty, "fill_price": fill_price,
                    "pnl": pnl, "order_id": oid}

        # Order resting — reconciliation handles fills
        return {"filled": False, "fill_qty": fill_qty, "fill_price": fill_price,
                "pnl": 0.0, "order_id": oid}

    def _om_emit_live_sanity(self):
        """Emit LIVE_SANITY report every minute — order manager health metrics."""
        now_ts = time.time()
        if now_ts - self._om_last_sanity_ts < self._om_sanity_interval_sec:
            return
        self._om_last_sanity_ts = now_ts

        # Calculate avg order age
        ages = []
        for entry in self._om_open_orders.values():
            age_ms = int(now_ts * 1000) - entry["created_ms"]
            ages.append(age_ms)
        avg_age_ms = sum(ages) / len(ages) if ages else 0.0

        # Safety Item 2: Count slugs currently paused
        slugs_paused = [s for s, t in self._om_slug_paused_until.items() if now_ts < t]

        # Safety Item 3: Cancel rate metrics
        cutoff_60 = now_ts - 60.0
        cancels_global_min = len([t for t in self._om_cancel_ts_global if t > cutoff_60])
        cancel_freeze_remaining = max(0.0, self._om_cancel_freeze_until - now_ts)

        # Safety Item 5: Drift pause remaining
        drift_pause_remaining = max(0.0, self._om_drift_pause_until - now_ts)

        write_jsonl({
            "event_type": "LIVE_SANITY",
            "open_orders_count": len(self._om_open_orders),
            "orphan_canceled_count": self._om_orphan_canceled_count,
            "orphan_canceled_min": self._om_orphan_canceled_min,
            "partial_fill_events": self._om_partial_fill_events,
            "partial_fill_events_min": self._om_partial_fill_events_min,
            "submit_fail_count": self._om_submit_fail_count,
            "submit_fail_min": self._om_submit_fail_min,
            "cancel_fail_count": self._om_cancel_fail_count,
            "cancel_fail_min": self._om_cancel_fail_min,
            "api_errors_min": self._om_api_errors_min,
            "avg_order_age_ms": round(avg_age_ms, 1),
            "tx_count": self._true_cost_tx_count,
            "fill_count": self._true_cost_fill_count,
            "fill_count_min": self._true_cost_fill_count_min,
            "kill_switch_active": self._om_kill_switch_active(),
            "kill_switch_until": round(max(0, self._om_kill_switch_until - now_ts), 1),
            # Safety Item 2: Slug progress
            "slugs_paused": slugs_paused,
            "slugs_paused_count": len(slugs_paused),
            # Safety Item 3: Cancel rate
            "cancels_global_min": cancels_global_min,
            "cancel_freeze_remaining_sec": round(cancel_freeze_remaining, 1),
            # Safety Item 5: Drift
            "drift_pause_remaining_sec": round(drift_pause_remaining, 1),
        })

        # Reset per-minute counters
        self._om_orphan_canceled_min = 0
        self._om_partial_fill_events_min = 0
        self._om_submit_fail_min = 0
        self._om_cancel_fail_min = 0
        self._om_api_errors_min = 0

    # =========================================================================
    # Safety Item 1: Restart Recovery — persist + reconcile open orders
    # =========================================================================

    def _om_save_open_orders(self):
        """Persist _om_open_orders to disk for crash recovery."""
        if not self._om_open_orders:
            # Remove stale file if no open orders
            try:
                if os.path.exists(OM_OPEN_ORDERS_FILE):
                    os.remove(OM_OPEN_ORDERS_FILE)
            except Exception:
                pass
            return
        try:
            with open(OM_OPEN_ORDERS_FILE, "w", encoding="utf-8") as f:
                json.dump(self._om_open_orders, f, separators=(",", ":"))
        except Exception as e:
            write_jsonl({"event_type": "OM_SAVE_ORDERS_ERROR", "err": str(e)[:200]})

    def _om_load_open_orders(self):
        """Load persisted open orders from disk (called during _load_state)."""
        if not os.path.exists(OM_OPEN_ORDERS_FILE):
            return
        try:
            with open(OM_OPEN_ORDERS_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                self._om_open_orders = loaded
                write_jsonl({"event_type": "OM_ORDERS_LOADED",
                              "count": len(loaded),
                              "order_ids": list(loaded.keys())[:20]})
        except Exception as e:
            write_jsonl({"event_type": "OM_LOAD_ORDERS_ERROR", "err": str(e)[:200]})

    def _om_startup_reconcile(self):
        """On startup: reconcile orders + positions + balance against live API state.
        1. List CLOB orders: adopt tracked, cancel unknown orphans
        2. Fetch live positions: compare qty per token to internal state
        3. Fetch USDC balance: compare to internal cash_usdc
        4. If mismatch -> pause entries, emit STARTUP_RECONCILE_REPORT"""
        if not self.client._clob:
            return

        startup_pause = False
        position_mismatches = []
        balance_mismatch = None

        # ── Phase 1: Order reconciliation ──
        try:
            clob_orders = self.client.get_open_orders()
        except Exception as e:
            write_jsonl({"event_type": "OM_STARTUP_RECONCILE_ERROR", "err": str(e)[:200]})
            return

        clob_ids = {}
        for o in clob_orders:
            oid = o.get("id") or o.get("orderID") or o.get("order_id", "")
            if oid:
                clob_ids[oid] = o

        adopted = 0
        canceled_unknown = 0
        reconciled_fills = 0

        # 1a. For tracked orders that still exist on CLOB: reconcile fills
        for oid in list(self._om_open_orders.keys()):
            if oid in clob_ids:
                result = self._om_poll_and_reconcile(oid)
                if result.get("filled_qty", 0) > 0:
                    reconciled_fills += 1
                adopted += 1
            else:
                entry = self._om_open_orders[oid]
                write_jsonl({"event_type": "OM_STARTUP_ORDER_GONE",
                              "order_id": oid, "slug": entry["slug"],
                              "outcome": entry["outcome"], "side": entry["side"],
                              "filled_qty": entry["filled_qty"], "total_qty": entry["qty"]})
                self._om_open_orders.pop(oid, None)

        # 1b. For CLOB orders NOT in our tracking: cancel them (orphans)
        tracked = set(self._om_open_orders.keys())
        for oid, o in clob_ids.items():
            if oid not in tracked:
                try:
                    self.client._clob.cancel(oid)
                    canceled_unknown += 1
                    write_jsonl({"event_type": "OM_STARTUP_ORPHAN_CANCELED",
                                  "order_id": oid,
                                  "side": o.get("side", ""),
                                  "price": o.get("price", ""),
                                  "size": o.get("size", "")})
                except Exception as e:
                    write_jsonl({"event_type": "OM_STARTUP_ORPHAN_CANCEL_ERROR",
                                  "order_id": oid, "err": str(e)[:120]})

        # ── Phase 2: Position reconciliation ──
        live_positions = self.client.get_live_positions()
        if live_positions:
            # Build internal position map: token_id -> qty
            internal_qty_by_token: Dict[str, float] = {}
            for slug, st in self.market_states.items():
                m_data = self.client._market_cache.get(slug, {})
                up_id = m_data.get("up_id", "")
                dn_id = m_data.get("down_id", "")
                for outcome, tid in [("Up", up_id), ("Down", dn_id)]:
                    if tid:
                        internal_qty_by_token[tid] = st.positions[outcome].qty

            # Compare
            for tid, live_data in live_positions.items():
                live_qty = live_data.get("size", 0.0)
                internal_qty = internal_qty_by_token.get(tid, 0.0)
                diff = abs(live_qty - internal_qty)
                if diff > OM_DRIFT_QTY_TOLERANCE:
                    position_mismatches.append({
                        "token_id": tid[-12:],
                        "live_qty": round(live_qty, 1),
                        "internal_qty": round(internal_qty, 1),
                        "diff": round(diff, 1),
                    })
                    startup_pause = True

            # Also check: internal positions that have no live counterpart
            for tid, iqty in internal_qty_by_token.items():
                if iqty > OM_DRIFT_QTY_TOLERANCE and tid not in live_positions:
                    position_mismatches.append({
                        "token_id": tid[-12:],
                        "live_qty": 0.0,
                        "internal_qty": round(iqty, 1),
                        "diff": round(iqty, 1),
                        "note": "internal_only",
                    })
                    startup_pause = True

        # ── Phase 3: Balance check ──
        live_balance = self.client.get_usdc_balance()
        if live_balance is not None:
            balance_diff = abs(live_balance - self.cash_usdc)
            if balance_diff > 10.0:  # >$10 discrepancy
                balance_mismatch = {
                    "live_usdc": round(live_balance, 2),
                    "internal_usdc": round(self.cash_usdc, 2),
                    "diff": round(balance_diff, 2),
                }
                startup_pause = True
                # Adopt live balance as truth
                self.cash_usdc = live_balance

        # ── Phase 4: Pause if mismatch ──
        if startup_pause:
            self._om_drift_pause_until = time.time() + OM_DRIFT_PAUSE_SEC
            write_jsonl({"event_type": "STARTUP_RECONCILE_MISMATCH",
                          "position_mismatches": position_mismatches,
                          "balance_mismatch": balance_mismatch,
                          "pause_sec": OM_DRIFT_PAUSE_SEC})

        # ── Emit STARTUP_RECONCILE_REPORT ──
        write_jsonl({
            "event_type": "STARTUP_RECONCILE_REPORT",
            "clob_orders": len(clob_ids),
            "adopted": adopted,
            "canceled_unknown": canceled_unknown,
            "reconciled_fills": reconciled_fills,
            "tracked_remaining": len(self._om_open_orders),
            "live_positions_fetched": len(live_positions),
            "position_mismatches": len(position_mismatches),
            "balance_mismatch": balance_mismatch is not None,
            "startup_pause": startup_pause,
        })
        print(f"  STARTUP RECONCILE: orders={len(clob_ids)} adopted={adopted} "
              f"orphans_canceled={canceled_unknown} fills_reconciled={reconciled_fills}")
        if position_mismatches:
            print(f"  !! POSITION MISMATCH: {len(position_mismatches)} tokens — "
                  f"entries paused {OM_DRIFT_PAUSE_SEC}s")
        if balance_mismatch:
            print(f"  !! BALANCE MISMATCH: live=${balance_mismatch['live_usdc']:.2f} "
                  f"internal=${balance_mismatch['internal_usdc']:.2f} — adopted live")

    # =========================================================================
    # Safety Item 2: Per-Slug No-Progress Circuit Breaker
    # =========================================================================

    def _om_record_slug_submit(self, slug: str):
        """Record a submit for the no-progress circuit breaker."""
        now = time.time()
        self._om_slug_submits_60s.setdefault(slug, []).append(now)

    def _om_record_slug_fill(self, slug: str):
        """Record a fill for the no-progress circuit breaker."""
        now = time.time()
        self._om_slug_fills_60s.setdefault(slug, []).append(now)

    def _om_slug_paused(self, slug: str) -> bool:
        """True if slug is paused due to no-progress circuit breaker."""
        return time.time() < self._om_slug_paused_until.get(slug, 0.0)

    def _om_check_slug_progress(self):
        """Check all slugs for no-progress condition. Pause offenders."""
        now = time.time()
        cutoff = now - 60.0

        for slug in list(self._om_slug_submits_60s.keys()):
            # Trim old entries
            self._om_slug_submits_60s[slug] = [
                t for t in self._om_slug_submits_60s[slug] if t > cutoff]
            fills = self._om_slug_fills_60s.get(slug, [])
            self._om_slug_fills_60s[slug] = [t for t in fills if t > cutoff]

            submits_60s = len(self._om_slug_submits_60s[slug])
            fills_60s = len(self._om_slug_fills_60s.get(slug, []))

            if (submits_60s >= OM_SLUG_NOPROGRESS_SUBMITS and fills_60s == 0
                    and not self._om_slug_paused(slug)):
                self._om_slug_paused_until[slug] = now + OM_SLUG_PAUSE_SEC
                # Cancel all open orders for this slug
                for oid, entry in list(self._om_open_orders.items()):
                    if entry["slug"] == slug:
                        self._om_cancel_order(oid, "noprogress_pause")
                write_jsonl({"event_type": "OM_SLUG_NOPROGRESS_PAUSE",
                              "slug": slug,
                              "submits_60s": submits_60s, "fills_60s": fills_60s,
                              "pause_sec": OM_SLUG_PAUSE_SEC})

    # =========================================================================
    # Safety Item 3: Cancel/Replace Rate Limits
    # =========================================================================

    def _om_cancel_rate_ok(self, slug: str = "") -> bool:
        """Check if we can cancel/replace without exceeding rate limits."""
        if time.time() < self._om_cancel_freeze_until:
            return False

        now = time.time()
        cutoff = now - 60.0

        # Global check
        self._om_cancel_ts_global = [t for t in self._om_cancel_ts_global if t > cutoff]
        if len(self._om_cancel_ts_global) >= OM_MAX_CANCELS_PER_MIN_GLOBAL:
            self._om_cancel_freeze_until = now + OM_CANCEL_FREEZE_SEC
            write_jsonl({"event_type": "OM_CANCEL_RATE_FREEZE",
                          "reason": "global_limit",
                          "cancels_min": len(self._om_cancel_ts_global),
                          "freeze_sec": OM_CANCEL_FREEZE_SEC})
            return False

        # Per-slug check
        if slug:
            slug_ts = self._om_cancel_ts_slug.get(slug, [])
            self._om_cancel_ts_slug[slug] = [t for t in slug_ts if t > cutoff]
            if len(self._om_cancel_ts_slug[slug]) >= OM_MAX_CANCELS_PER_MIN_SLUG:
                self._om_cancel_freeze_until = now + OM_CANCEL_FREEZE_SEC
                write_jsonl({"event_type": "OM_CANCEL_RATE_FREEZE",
                              "reason": "slug_limit", "slug": slug,
                              "cancels_min": len(self._om_cancel_ts_slug[slug]),
                              "freeze_sec": OM_CANCEL_FREEZE_SEC})
                return False

        return True

    def _om_record_cancel(self, slug: str = ""):
        """Record a cancel for rate-limiting purposes."""
        now = time.time()
        self._om_cancel_ts_global.append(now)
        if slug:
            self._om_cancel_ts_slug.setdefault(slug, []).append(now)

    def _om_replace_interval_ok(self, slug: str, outcome: str, side: str) -> bool:
        """Check if enough time has passed since last replace for this key."""
        key = f"{slug}:{outcome}:{side}"
        last = self._om_last_replace_ts.get(key, 0.0)
        elapsed_ms = (time.time() - last) * 1000
        return elapsed_ms >= OM_MIN_REPLACE_INTERVAL_MS

    def _om_record_replace(self, slug: str, outcome: str, side: str):
        """Record a replace timestamp."""
        key = f"{slug}:{outcome}:{side}"
        self._om_last_replace_ts[key] = time.time()

    # =========================================================================
    # Safety Item 4: Flatten Guarantees + Verify
    # =========================================================================

    def _om_flatten_hard(self, m, st: MarketState, ctx: dict):
        """Hard flatten: cancel ALL orders, cross positions with retries, verify flat."""
        slug = m.slug
        up_book = ctx.get("up_book")
        dn_book = ctx.get("dn_book")

        # 1. Cancel ALL tracked open orders for this slug
        for oid, entry in list(self._om_open_orders.items()):
            if entry["slug"] == slug:
                self._om_cancel_order(oid, "hard_flatten")

        # 2. Orphan scan for this slug specifically
        if MODE != "LOG" and self.client._clob:
            try:
                clob_orders = self.client.get_open_orders()
                for o in clob_orders:
                    # Filter to this slug's token_ids
                    o_token = o.get("asset_id") or o.get("token_id", "")
                    if o_token in (m.outcome_up_id, m.outcome_down_id):
                        oid = o.get("id") or o.get("orderID") or o.get("order_id", "")
                        if oid:
                            try:
                                self.client._clob.cancel(oid)
                                self._om_orphan_canceled_count += 1
                                write_jsonl({"event_type": "OM_FLATTEN_ORPHAN_CANCELED",
                                              "slug": slug, "order_id": oid})
                            except Exception:
                                pass
            except Exception:
                pass

        # 3. Cross remaining positions with retries
        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty < MIN_QTY:
                continue
            book = up_book if outcome == "Up" else dn_book
            if not book or book.bid <= 0:
                continue

            for attempt in range(OM_FLATTEN_CROSS_MAX_RETRIES):
                sell_price = book.bid  # taker cross at bid
                sell_result = self._exec_sell(
                    st, m, outcome, sell_price, pos.qty,
                    reason="HARD_FLATTEN_CROSS", prefer_maker=False, ctx=ctx)
                if sell_result.get("filled"):
                    break
                # Brief pause before retry
                time.sleep(0.2)
                # Refresh book if possible
                cached = self._data_cache.get(slug)
                if cached:
                    book = cached.get(f"{outcome.lower()}_book") or cached.get(f"{outcome}_book") or book

        # 4. Verify flat (Safety Item 4 — assert flat)
        if not OM_FLATTEN_VERIFY_ENABLED or MODE == "LOG":
            return

        # Brief delay for fills to settle
        time.sleep(0.5)

        any_remaining = False
        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty >= MIN_QTY:
                any_remaining = True
                write_jsonl({"event_type": "FLATTEN_VERIFY_FAIL",
                              "slug": slug, "outcome": outcome,
                              "remaining_qty": round(pos.qty, 1),
                              "vwap": round(pos.vwap, 4)})

        if any_remaining:
            # Disable trading for this slug
            self._om_slug_paused_until[slug] = time.time() + 300.0  # 5 min pause
            write_jsonl({"event_type": "FLATTEN_VERIFY_FAIL_PAUSED",
                          "slug": slug, "pause_sec": 300})

    # =========================================================================
    # Safety Item 5: State Drift Detector
    # =========================================================================

    def _om_check_state_drift(self):
        """Compare internal state against live API: orders + positions.
        If mismatch beyond tolerance -> log STATE_DRIFT, disable entries, reconcile."""
        if MODE == "LOG" or not self.client._clob:
            return

        now_ts = time.time()
        if now_ts - self._om_last_drift_check_ts < OM_DRIFT_CHECK_INTERVAL_SEC:
            return
        self._om_last_drift_check_ts = now_ts

        drift_detected = False
        position_drifts = []

        # ── Phase 1: Compare tracked open orders vs CLOB reality ──
        try:
            clob_orders = self.client.get_open_orders()
        except Exception:
            return  # can't check, skip this cycle

        clob_ids = set()
        for o in clob_orders:
            oid = o.get("id") or o.get("orderID") or o.get("order_id", "")
            if oid:
                clob_ids.add(oid)

        tracked_ids = set(self._om_open_orders.keys())
        ghost_orders = tracked_ids - clob_ids
        orphan_orders = clob_ids - tracked_ids

        if ghost_orders:
            for oid in ghost_orders:
                entry = self._om_open_orders.get(oid, {})
                write_jsonl({"event_type": "STATE_DRIFT_GHOST_ORDER",
                              "order_id": oid,
                              "slug": entry.get("slug", ""),
                              "outcome": entry.get("outcome", ""),
                              "side": entry.get("side", ""),
                              "filled_qty": entry.get("filled_qty", 0),
                              "total_qty": entry.get("qty", 0)})
                self._om_poll_and_reconcile(oid)
                if oid in self._om_open_orders:
                    self._om_open_orders.pop(oid, None)
            drift_detected = True

        # ── Phase 2: Compare API positions vs internal qty by slug/outcome ──
        if OM_DRIFT_POSITION_CHECK:
            live_positions = self.client.get_live_positions()
            if live_positions:
                # Build internal map: token_id -> (slug, outcome, internal_qty)
                internal_map: Dict[str, Tuple[str, str, float]] = {}
                for slug, st in self.market_states.items():
                    m_data = self.client._market_cache.get(slug, {})
                    up_id = m_data.get("up_id", "")
                    dn_id = m_data.get("down_id", "")
                    for outcome, tid in [("Up", up_id), ("Down", dn_id)]:
                        if tid:
                            internal_map[tid] = (slug, outcome, st.positions[outcome].qty)

                for tid, live_data in live_positions.items():
                    live_qty = live_data.get("size", 0.0)
                    if tid in internal_map:
                        slug, outcome, internal_qty = internal_map[tid]
                        diff = abs(live_qty - internal_qty)
                        if diff > OM_DRIFT_QTY_TOLERANCE:
                            position_drifts.append({
                                "slug": slug, "outcome": outcome,
                                "live_qty": round(live_qty, 1),
                                "internal_qty": round(internal_qty, 1),
                                "diff": round(diff, 1),
                            })
                            # Reconcile: adopt live qty if larger (fills we missed)
                            if live_qty > internal_qty:
                                st = self.market_states.get(slug)
                                if st:
                                    st.positions[outcome].qty = live_qty
                                    write_jsonl({"event_type": "STATE_DRIFT_QTY_ADOPTED",
                                                  "slug": slug, "outcome": outcome,
                                                  "old_qty": round(internal_qty, 1),
                                                  "new_qty": round(live_qty, 1)})

                # Check internal positions with no live counterpart
                for tid, (slug, outcome, iqty) in internal_map.items():
                    if iqty > OM_DRIFT_QTY_TOLERANCE and tid not in live_positions:
                        position_drifts.append({
                            "slug": slug, "outcome": outcome,
                            "live_qty": 0.0,
                            "internal_qty": round(iqty, 1),
                            "diff": round(iqty, 1),
                            "note": "phantom_internal",
                        })

                if position_drifts:
                    drift_detected = True
                    self._om_drift_position_mismatches += len(position_drifts)

        # ── Phase 3: React to drift ──
        if drift_detected:
            self._om_drift_count += 1
            write_jsonl({
                "event_type": "STATE_DRIFT",
                "ghost_orders": len(ghost_orders),
                "orphan_orders": len(orphan_orders),
                "position_drifts": position_drifts,
                "position_drift_count": len(position_drifts),
                "total_drift_events": self._om_drift_count,
            })
            # Disable entries until reconciliation settles
            self._om_drift_pause_until = now_ts + OM_DRIFT_PAUSE_SEC
            self._pnl_total_drift_pause_sec += OM_DRIFT_PAUSE_SEC
            write_jsonl({"event_type": "STATE_DRIFT_PAUSE",
                          "pause_sec": OM_DRIFT_PAUSE_SEC,
                          "total_drift_pauses": self._om_drift_count})

    def _om_drift_paused(self) -> bool:
        """True if entries are paused due to state drift."""
        return time.time() < self._om_drift_pause_until

    # -----------------------------
    # Risk checks (log-only alerts)
    # -----------------------------
    def _market_cost_usdc(self, st: MarketState) -> float:
        return sum(p.cost_usdc for p in st.positions.values())
    def _crypto_cost_usdc(self, crypto: str) -> float:
        s = 0.0
        for st in self.market_states.values():
            if st.crypto == crypto:
                s += sum(p.cost_usdc for p in st.positions.values())
        return s
    def _shadow_equity(self) -> float:
        """MTM equity for the shadow portfolio."""
        mtm = 0.0
        for slug, outcomes in self._shadow_positions.items():
            for outcome, sp in outcomes.items():
                if sp["qty"] < MIN_QTY:
                    continue
                book = self.last_book.get(slug, {}).get(outcome)
                mid = book.mid if book else sp["vwap"]
                mtm += sp["qty"] * mid
        return self._shadow_cash + mtm
    def _activate_shadow(self):
        """Snapshot current state into shadow portfolio (called once per trigger)."""
        self._shadow_active = True
        self._shadow_cash = self.cash_usdc
        self._shadow_equity_at_trigger = self._equity()
        self._shadow_trades_blocked = 0
        self._shadow_positions = {}
        for slug, st in self.market_states.items():
            sp = {}
            for outcome in ["Up", "Down"]:
                pos = st.positions[outcome]
                if pos.qty >= MIN_QTY:
                    sp[outcome] = {"qty": pos.qty, "cost_usdc": pos.cost_usdc, "vwap": pos.vwap}
            if sp:
                self._shadow_positions[slug] = sp
    def _check_risk_drawdown(self, ctx: dict) -> dict:
        """
        Compute equity drawdown, emit RISK_STOP_TRIGGERED when thresholds
        are breached. Returns dict with fields for CSV/ctx enrichment.
        Never stops the bot — log only.
        """
        equity_now = self._equity()
        hse = self.hour_start_equity if self.hour_start_equity > 0 else equity_now
        dse = self.day_start_equity if self.day_start_equity > 0 else equity_now
        hour_dd_pct = (equity_now - hse) / hse if hse > 0 else 0.0
        day_dd_pct  = (equity_now - dse) / dse if dse > 0 else 0.0
        st = ctx["st"]
        m = ctx["m"]
        hour_triggered = self._hour_risk_stop_hit
        day_triggered = self._day_risk_stop_hit
        # ── Hourly stop-loss check (single global flag per hour) ──
        if hour_dd_pct <= -STOP_LOSS_PCT_PER_HOUR and not hour_triggered:
            self._hour_risk_stop_hit = True
            hour_triggered = True
            # Build open-position summary across ALL markets
            pos_summary = []
            for s_slug, s_st in self.market_states.items():
                for outcome in ["Up", "Down"]:
                    pos = s_st.positions[outcome]
                    if pos.qty >= MIN_QTY:
                        bk = self.last_book.get(s_slug, {}).get(outcome)
                        pos_summary.append({
                            "slug": s_slug, "crypto": s_st.crypto,
                            "outcome": outcome, "qty": round(pos.qty, 2),
                            "vwap": round(pos.vwap, 4),
                            "mid": round(bk.mid, 4) if bk else None,
                        })
            up_book, dn_book = ctx["up_book"], ctx["dn_book"]
            ref_book = up_book if ctx.get("drift_dir") == "Up" else dn_book
            write_jsonl({
                "event_type": "RISK_STOP_TRIGGERED",
                "trigger": "HOURLY",
                "detected_on_slug": m.slug, "detected_on_crypto": m.crypto,
                "t_min": round(ctx["t_min"], 3),
                "hour_start_equity": round(hse, 4),
                "equity_now": round(equity_now, 4),
                "hour_dd_pct": round(hour_dd_pct, 6),
                "day_dd_pct": round(day_dd_pct, 6),
                "open_positions": pos_summary,
                "spread": round(ref_book.spread, 4),
                "delta_bps": round(ctx["delta_bps"], 3),
                "zscore": round(ctx.get("z", 0), 3),
            })
            if SHADOW_STOP_SIM and not self._shadow_active:
                self._activate_shadow()
        # ── Daily stop-loss check ──
        if day_dd_pct <= -STOP_LOSS_PCT_PER_DAY and not day_triggered:
            self._day_risk_stop_hit = True
            day_triggered = True
            write_jsonl({
                "event_type": "RISK_STOP_TRIGGERED",
                "trigger": "DAILY",
                "detected_on_slug": m.slug, "detected_on_crypto": m.crypto,
                "t_min": round(ctx["t_min"], 3),
                "day_start_equity": round(dse, 4),
                "equity_now": round(equity_now, 4),
                "day_dd_pct": round(day_dd_pct, 6),
                "hour_dd_pct": round(hour_dd_pct, 6),
            })
        return {
            "hour_start_equity": round(hse, 4),
            "hour_dd_pct": round(hour_dd_pct, 6),
            "day_dd_pct": round(day_dd_pct, 6),
            "risk_stop_triggered": hour_triggered or day_triggered,
            "shadow_blocked": self._shadow_active,
        }
    def _risk_ok(self, st: MarketState) -> bool:
        """Position-sizing risk caps. Stop-loss is log-only when ENFORCE_STOP_LOSS=False."""
        if ENFORCE_STOP_LOSS:
            equity = self._equity()
            hse = self.hour_start_equity if self.hour_start_equity > 0 else equity
            if (equity - hse) / max(hse, 1e-9) <= -STOP_LOSS_PCT_PER_HOUR:
                return False
        if self._market_cost_usdc(st) > self.cash_usdc * MAX_COST_PER_MARKET_PCT:
            return False
        if self._crypto_cost_usdc(st.crypto) > self.cash_usdc * MAX_COST_PER_CRYPTO_PCT:
            return False
        return True
    # -----------------------------
    # Background data refresh
    # -----------------------------
    def _submit_market_discovery(self):
        """Submit market discovery to background pool (non-blocking)."""
        if self._pending_fetches.get("__markets__"):
            return  # already in-flight
        self._pending_fetches["__markets__"] = True
        def _do_discovery():
            try:
                markets = self._get_markets()
                for m in markets:
                    self._ensure_market_state(m)
                self._cached_markets = markets  # atomic list assignment
                self._last_market_discovery_ts = time.time()
            except Exception as e:
                self.logger.log_event({"event_type": "BG_DISCOVERY_ERROR", "err": str(e)})
            finally:
                self._pending_fetches.pop("__markets__", None)
        self._bg_executor.submit(_do_discovery)

    def _submit_market_refresh(self, m: MarketRef):
        """Submit a single market's data refresh to background pool (non-blocking)."""
        slug = m.slug
        if self._pending_fetches.get(slug):
            return  # already in-flight for this market
        self._pending_fetches[slug] = True
        self._bg_pending_cycles[slug] = 0  # reset starvation counter
        start_ts = time.time()
        def _do_refresh():
            try:
                data = self._prefetch_market_data(m)
                end_ts = time.time()
                data["ts"] = end_ts
                self._data_cache[slug] = data  # atomic dict assignment
                # Track fetch duration
                dur_ms = (end_ts - start_ts) * 1000
                durations = self._bg_fetch_durations.setdefault(slug, [])
                durations.append(dur_ms)
                # Keep last 100 for rolling stats
                if len(durations) > 100:
                    self._bg_fetch_durations[slug] = durations[-100:]
            except Exception as e:
                self.logger.log_event({"event_type": "BG_REFRESH_ERROR",
                                       "slug": slug, "err": str(e)})
            finally:
                self._pending_fetches.pop(slug, None)
        self._bg_executor.submit(_do_refresh)

    def _cache_age_ms(self, slug: str) -> float:
        """Return age of cached data in milliseconds, or inf if not cached."""
        cached = self._data_cache.get(slug)
        if not cached or "ts" not in cached:
            return float('inf')
        return (time.time() - cached["ts"]) * 1000

    def _update_priority_slugs(self):
        """Recompute which markets need fast refresh (positions or active state machine)."""
        priority = set()
        for slug, st in self.market_states.items():
            sm = self.entry_sm.get(slug, {})
            if sm.get("state") in ("PROBING", "SCALING"):
                priority.add(slug)
                continue
            for outcome in ("Up", "Down"):
                if st.positions[outcome].qty >= MIN_QTY:
                    priority.add(slug)
                    break
        self._high_priority_slugs = priority  # atomic set assignment

    # -----------------------------
    # Main loop
    # -----------------------------
    def _prefetch_market_data(self, m: MarketRef) -> dict:
        """Fetch all HTTP data for one market (runs in thread)."""
        spot, hour_open = self.client.get_binance_spot_and_hour_open(m.crypto)
        up_book = self.client.get_top_of_book(m.outcome_up_id, levels=IMB_LEVELS)
        dn_book = self.client.get_top_of_book(m.outcome_down_id, levels=IMB_LEVELS)
        return {"market": m, "spot": spot, "hour_open": hour_open,
                "up_book": up_book, "dn_book": dn_book}

    def run(self):
        # Set hour_start_equity to actual equity so the first (partial) hour
        # measures drawdown correctly — not from BANKROLL_START_USDC.
        actual_equity = self._equity()
        self.hour_start_equity = actual_equity
        self.day_start_equity = actual_equity
        self.logger.log_event({
            "event_type": "START",
            "run_id": RUN_ID, "schema_version": SCHEMA_VERSION,
            "bot_version": BOT_VERSION,
            "mode": MODE, "profile": PROFILE,
            "cash": self.cash_usdc, "realized_pnl": self.realized_pnl_usdc,
            "hour_start_equity": round(actual_equity, 4),
            "csv_path": self.logger._csv_path,
            "jsonl_path": self.logger._jsonl_path,
            # Fee model config (logged at startup for audit trail)
            "maker_fee_bps": MAKER_FEE_BPS,
            "taker_fee_bps": TAKER_FEE_BPS,
            "parity_buy_min_edge_net_cents": PARITY_BUY_MIN_EDGE_NET_CENTS,
            "parity_sell_min_edge_net_cents": PARITY_SELL_MIN_EDGE_NET_CENTS,
            "parity_edge_buffer_cents": PARITY_EDGE_BUFFER_CENTS,
            "parity_stop_new_min": PARITY_STOP_NEW_MIN,
            "parity_flatten_start_min": PARITY_FLATTEN_START_MIN,
            "parity_hard_flatten_min": PARITY_HARD_FLATTEN_MIN,
            "quote_target_edge_base": PARITY_QUOTE_TARGET_EDGE_NET_CENTS_BASE,
            "quote_target_edge_max": PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX,
            "quote_unpaired_escalate_ms": QUOTE_UNPAIRED_ESCALATE_AFTER_MS,
            "quote_unpaired_max_sec": QUOTE_UNPAIRED_MAX_SEC,
            "quote_pause_sec": QUOTE_PAUSE_AFTER_UNPAIRED_SEC,
            "rescue_min_edge": RESCUE_MIN_EDGE_NET_CENTS,
        })
        print(f"  FEE MODEL: maker={MAKER_FEE_BPS}bps taker={TAKER_FEE_BPS}bps "
              f"buffer={PARITY_EDGE_BUFFER_CENTS}c "
              f"buy_min_net={PARITY_BUY_MIN_EDGE_NET_CENTS}c "
              f"sell_min_net={PARITY_SELL_MIN_EDGE_NET_CENTS}c")
        print(f"  QUOTE: target_edge={PARITY_QUOTE_TARGET_EDGE_NET_CENTS_BASE}-"
              f"{PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX}c "
              f"step=${PARITY_QUOTE_STEP_USD} max=${PARITY_QUOTE_MAX_USD_PER_SLUG} "
              f"unpaired_esc={QUOTE_UNPAIRED_ESCALATE_AFTER_MS}ms "
              f"max_unpaired={QUOTE_UNPAIRED_MAX_SEC}s pause={QUOTE_PAUSE_AFTER_UNPAIRED_SEC}s")
        self._last_balance_print = 0.0
        self._last_save_ts = time.time()

        # ── Initial sync bootstrap: discover markets + prefetch (blocking, once) ──
        try:
            self._cached_markets = self._get_markets()
            for m in self._cached_markets:
                self._ensure_market_state(m)
            self._last_market_discovery_ts = time.time()
            if self._cached_markets:
                with ThreadPoolExecutor(max_workers=len(self._cached_markets)) as pool:
                    futures = {pool.submit(self._prefetch_market_data, m): m
                               for m in self._cached_markets}
                    for fut in as_completed(futures):
                        try:
                            data = fut.result()
                            data["ts"] = time.time()
                            self._data_cache[data["market"].slug] = data
                        except Exception as e:
                            m_ref = futures[fut]
                            self.logger.log_event({"event_type": "INIT_PREFETCH_ERROR",
                                                   "slug": m_ref.slug, "err": str(e)})
            self._bootstrap_done_ts = time.time()
            self._last_hour_roll_ts = self._bootstrap_done_ts
            write_jsonl({"event_type": "INIT_BOOTSTRAP_DONE",
                          "markets": len(self._cached_markets),
                          "cached": len(self._data_cache)})
        except Exception as e:
            self.logger.log_event({"event_type": "INIT_BOOTSTRAP_ERROR", "err": str(e)})

        # ── Safety Item 1: Startup reconcile — adopt or cancel leftover CLOB orders ──
        if MODE != "LOG" and OM_STARTUP_RECONCILE:
            self._om_startup_reconcile()

        # ══════════════════════════════════════════════════════════════════
        # NON-BLOCKING MAIN LOOP — reads from cache, never blocks on HTTP
        # Background pool continuously refreshes _data_cache
        # ══════════════════════════════════════════════════════════════════
        while self.running:
            loop_start = time.time()
            self._reset_daily_if_needed()
            try:
                # 1. Market discovery (background, every MARKET_DISCOVERY_INTERVAL_SEC)
                if time.time() - self._last_market_discovery_ts >= MARKET_DISCOVERY_INTERVAL_SEC:
                    self._submit_market_discovery()

                # 2. Snapshot current markets & resolve ended hours
                markets = list(self._cached_markets)  # atomic snapshot
                self._resolve_ended_hours(markets)

                # 3. Update priority set + deadline-based refresh scheduling
                self._update_priority_slugs()
                now_ts = time.time()
                for m in markets:
                    slug = m.slug
                    interval_ms = (BOOK_REFRESH_PRIORITY_MS
                                   if slug in self._high_priority_slugs
                                   else BOOK_REFRESH_IDLE_MS)
                    interval_sec = interval_ms / 1000.0

                    # Initialize deadline if not set
                    if slug not in self._bg_next_due:
                        self._bg_next_due[slug] = now_ts

                    # Track starvation: if pending for > BG_REFRESH_STARVE_CYCLES
                    if self._pending_fetches.get(slug):
                        self._bg_pending_cycles[slug] = self._bg_pending_cycles.get(slug, 0) + 1
                        pending_streak = self._bg_pending_cycles.get(slug, 0)
                        # Track pending fetch streak per slug and global max
                        self._diag_pending_fetch_streak[slug] = max(
                            self._diag_pending_fetch_streak.get(slug, 0), pending_streak)
                        self._diag_pending_fetch_streak_max = max(
                            self._diag_pending_fetch_streak_max, pending_streak)
                        if pending_streak > BG_REFRESH_STARVE_CYCLES:
                            # Starved: previous fetch is still in-flight after N cycles
                            self._bg_refresh_miss_count[slug] = self._bg_refresh_miss_count.get(slug, 0) + 1
                            # Priority resubmit: if pending > 2 cycles, force re-submit
                            # (cancel stale future, re-queue with priority)
                            self._pending_fetches.pop(slug, None)
                            self._bg_pending_cycles[slug] = 0
                            self._submit_market_refresh(m)
                            self._bg_next_due[slug] = now_ts + interval_sec
                        continue

                    # Deadline scheduling: submit if past due
                    if now_ts >= self._bg_next_due[slug]:
                        self._submit_market_refresh(m)
                        # Advance deadline by interval (not from now — prevents drift)
                        self._bg_next_due[slug] = max(now_ts, self._bg_next_due[slug] + interval_sec)

                    # Also track cache_age misses (cache too old = 3x interval)
                    cache_age = self._cache_age_ms(slug)
                    if cache_age > interval_ms * 3:
                        self._bg_refresh_miss_count[slug] = self._bg_refresh_miss_count.get(slug, 0) + 1

                # 4. Process each market with cached data (ZERO HTTP, pure logic)
                stale_skips = 0
                for m in markets:
                    cached = self._data_cache.get(m.slug)
                    age = self._cache_age_ms(m.slug)
                    if cached and age < BOOK_STALE_MS:
                        self._step_market_with_data(cached)
                    elif cached:
                        stale_skips += 1
                if stale_skips > 0:
                    self._stale_skip_total += stale_skips

                # 4b. Track cache ages for tempo diagnostics
                for m in markets:
                    age = self._cache_age_ms(m.slug)
                    if age < float('inf'):
                        self._tempo_cache_ages.setdefault(m.slug, []).append(age)

                # 4c. Order Manager: reconcile open orders, TTL, orphan scan
                self._om_reconcile_all()

                # 4d. Safety checks: no-progress, state drift, LIVE_SANITY, PnL attribution
                self._om_check_slug_progress()
                self._om_check_state_drift()
                self._om_emit_live_sanity()
                self._emit_pnl_attribution()  # self-timed at PNL_REPORT_INTERVAL_SEC (15m)
                self._check_slug_auto_disable()  # auto-disable slugs with bad PnL

                # 5. Save state periodically (every STATE_SAVE_INTERVAL_SEC)
                now = time.time()
                if now - self._last_save_ts >= STATE_SAVE_INTERVAL_SEC:
                    self._save_state()
                    self._last_save_ts = now

                # 6. Log rotation
                self.logger.rotate_files_if_needed()

                # 7. Console balance summary
                if now - self._last_balance_print >= 30.0:
                    self._print_balance_summary()
                    self._last_balance_print = now

                # 8. Tempo parity diagnostics (every 60s)
                if now - self._tempo_last_report_ts >= 60.0:
                    self._emit_clone_report()   # must run BEFORE tempo_report resets counters
                    self._emit_diag_report()    # F247-style behavioral diagnostics
                    self._emit_pnl_attribution()  # per-hour PnL attribution + expectancy
                    self._emit_tempo_report()
                    self._maybe_emit_gate_report()  # GATE_BREAKDOWN
                    self._tempo_last_report_ts = now
            except Exception as e:
                self.logger.log_event({"event_type": "LOOP_ERROR", "err": str(e)})

            # Enforce target loop interval — sleep only the remaining time
            loop_elapsed_ms = (time.time() - loop_start) * 1000
            self._tempo_loop_times.append(loop_elapsed_ms)
            sleep_ms = max(0, MAIN_LOOP_TARGET_MS - loop_elapsed_ms)
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)

            # Log loop latency periodically (every 50 loops)
            self._loop_count += 1
            if self._loop_count % 50 == 0:
                actual_loop_ms = (time.time() - loop_start) * 1000
                cache_ages = {slug: round(self._cache_age_ms(slug), 0)
                              for slug in self._data_cache}
                pending = list(self._pending_fetches.keys())
                write_jsonl({"event_type": "LOOP_LATENCY",
                              "loop_elapsed_ms": round(loop_elapsed_ms, 1),
                              "actual_loop_ms": round(actual_loop_ms, 1),
                              "target_ms": MAIN_LOOP_TARGET_MS,
                              "cache_ages_ms": cache_ages,
                              "pending_fetches": pending,
                              "markets_count": len(markets)})

        # ── Shutdown ──
        self._bg_running = False
        # Cancel all tracked open orders before shutdown
        if MODE != "LOG" and self._om_open_orders:
            write_jsonl({"event_type": "OM_SHUTDOWN_CANCEL_ALL",
                          "open_orders": len(self._om_open_orders)})
            for oid in list(self._om_open_orders.keys()):
                self._om_cancel_order(oid, "shutdown")
        self._bg_executor.shutdown(wait=False)
        self.logger.log_event({"event_type": "STOPPED", "cash": self.cash_usdc,
                               "realized_pnl": self.realized_pnl_usdc,
                               "equity": round(self._equity(), 2),
                               "hourly_pnl": self.hourly_pnl_usdc})
        self._save_state()
        self.logger.close()

    def _emit_tempo_report(self):
        """Emit per-minute tempo parity diagnostics for f247 comparison."""
        import statistics
        # Per-market metrics
        per_market = {}
        all_slugs = set(self._tempo_fills.keys()) | set(self._tempo_intents.keys()) | set(self._tempo_cache_ages.keys())
        for slug in all_slugs:
            fills = self._tempo_fills.get(slug, 0)
            intents = self._tempo_intents.get(slug, 0)
            ages = self._tempo_cache_ages.get(slug, [])
            median_age = round(statistics.median(ages), 0) if ages else 0
            per_market[slug] = {
                "fills_per_min": fills,
                "intents_per_min": intents,
                "median_cache_age_ms": median_age,
            }
        # Decision loop p95
        loop_p95 = 0.0
        if self._tempo_loop_times:
            sorted_loops = sorted(self._tempo_loop_times)
            p95_idx = int(len(sorted_loops) * 0.95)
            loop_p95 = round(sorted_loops[min(p95_idx, len(sorted_loops) - 1)], 1)
        # Diagnostic counters snapshot
        avg_parity_edge = (sum(self._diag_parity_edges) / len(self._diag_parity_edges)
                           if self._diag_parity_edges else 0.0)
        avg_pair_delay = (sum(self._diag_pair_fill_delays) / len(self._diag_pair_fill_delays)
                          if self._diag_pair_fill_delays else 0.0)
        maker_fill_rate = (self._diag_maker_fills / max(1, self._diag_maker_orders_placed)
                           if self._diag_maker_orders_placed > 0 else 0.0)
        # Maker fill latency percentiles
        maker_lat_p50 = 0.0
        maker_lat_p90 = 0.0
        if self._diag_maker_fill_latencies:
            sorted_lat = sorted(self._diag_maker_fill_latencies)
            p50_idx = len(sorted_lat) // 2
            p90_idx = int(len(sorted_lat) * 0.9)
            maker_lat_p50 = sorted_lat[min(p50_idx, len(sorted_lat) - 1)]
            maker_lat_p90 = sorted_lat[min(p90_idx, len(sorted_lat) - 1)]
        # Similarity/tempo stats
        trade_ts = sorted(self._diag_parity_trade_timestamps)
        paired_trades_per_min = len(trade_ts)
        max_trades_in_one_sec = 0
        if trade_ts:
            # Count max trades in any 1-second window
            for j, t0 in enumerate(trade_ts):
                cnt = sum(1 for t1 in trade_ts[j:] if t1 - t0 <= 1.0)
                max_trades_in_one_sec = max(max_trades_in_one_sec, cnt)
        # Median time between trades
        inter_trade_ms = []
        for j in range(1, len(trade_ts)):
            inter_trade_ms.append((trade_ts[j] - trade_ts[j - 1]) * 1000)
        median_inter_trade_ms = (sorted(inter_trade_ms)[len(inter_trade_ms) // 2]
                                  if inter_trade_ms else 0.0)
        # Paired trade ratio = pair completions / total individual fills
        total_fills_all = sum(v.get("fills_per_min", 0) for v in per_market.values())
        # Each pair completion = 2 fills, so ratio = 2*pairs / total_fills
        paired_ratio = (2 * self._diag_pairs_completed / max(1, total_fills_all)
                        if total_fills_all > 0 else 0.0)
        # Maker ratio = maker / (maker + taker) across parity
        total_parity_mt = self._diag_parity_maker_count + self._diag_parity_taker_count
        maker_ratio = (self._diag_parity_maker_count / max(1, total_parity_mt))

        diag = {
            "taker_count": self._diag_taker_count,
            "maker_count": self._diag_maker_count,
            "derisk_count": self._diag_derisk_count,
            "derisk_taker_count": self._diag_derisk_taker_count,
            "blocked_whipsaw": self._diag_blocked_whipsaw,
            "blocked_taker_gate": self._diag_blocked_taker_gate,
            "blocked_noflip": self._diag_blocked_noflip,
            "parity_buy_signals": self._diag_parity_buy_signals,
            "parity_sell_signals": self._diag_parity_sell_signals,
            "parity_trades": self._diag_parity_trades,
            "parity_avg_edge_net_cents": round(avg_parity_edge, 3),
            "parity_maker_count": self._diag_parity_maker_count,
            "parity_taker_count": self._diag_parity_taker_count,
            "pair_partial_count": self._diag_pair_partial_count,
            "avg_pair_fill_delay_ms": round(avg_pair_delay, 1),
            "unpaired_unwind_usd": round(self._diag_unpaired_unwind_usd, 2),
            "maker_orders_placed": self._diag_maker_orders_placed,
            "maker_fills": self._diag_maker_fills,
            "maker_fill_rate": round(maker_fill_rate, 3),
            "cancel_replace_per_min": self._diag_cancel_replace_count,
            "blocked_spread": self._diag_parity_blocked_spread,
            "blocked_liq": self._diag_parity_blocked_liq,
            "blocked_stale": self._diag_parity_blocked_stale,
            "recycle_count": self._diag_recycle_count,
            "maker_fill_latency_ms_p50": round(maker_lat_p50, 1),
            "maker_fill_latency_ms_p90": round(maker_lat_p90, 1),
            "maker_timeout_cancel_count": self._diag_maker_timeout_cancel_count,
            "maker_top_of_book_lost_count": self._diag_maker_lost_best_count,
            "flatten_actions_count": self._diag_flatten_actions,
            "flatten_taker_count": self._diag_flatten_taker,
            "rescue_attempts": self._diag_rescue_attempts,
            "rescue_success": self._diag_rescue_success,
            "rescue_fallback_sells": self._diag_rescue_fallback_sells,
            "quote_orders_placed": self._diag_quote_orders_placed,
            "quote_fills": self._diag_quote_fills,
            "quote_fill_rate": round(self._diag_quote_fills / max(1, self._diag_quote_orders_placed), 3),
            "quote_unpaired_events": self._diag_quote_unpaired_events,
            "quote_unpaired_escalations": self._diag_quote_unpaired_escalations,
            "quote_pause_count": self._diag_quote_pause_count,
            "adverse_guard_events": self._diag_adverse_guard_events,
            "adverse_guard_degrades": self._diag_adverse_guard_degrades,
            "adverse_guard_pauses": self._diag_adverse_guard_pauses,
            "quote_submit_count": self._diag_quote_submit_count,
            "quote_cancel_count": self._diag_quote_cancel_count,
            "quote_replace_count": self._diag_quote_replace_count,
            "hedge_tick1": self._diag_hedge_tick1,
            "hedge_tick2": self._diag_hedge_tick2,
            "hedge_cross": self._diag_hedge_cross,
            "hedge_unwind": self._diag_hedge_unwind,
        }
        tempo_stats = {
            "paired_trade_ratio": round(paired_ratio, 3),
            "paired_trades_per_min": paired_trades_per_min,
            "max_trades_in_one_sec": max_trades_in_one_sec,
            "median_inter_trade_ms": round(median_inter_trade_ms, 1),
            "maker_ratio": round(maker_ratio, 3),
        }
        # Per-slug inventory imbalance + straddle metrics
        inv_imbalance = {}
        total_straddle_locked_usd = 0.0
        locked_hold_secs = []
        for slug, st in self.market_states.items():
            up_qty = st.positions["Up"].qty
            dn_qty = st.positions["Down"].qty
            if up_qty >= MIN_QTY or dn_qty >= MIN_QTY:
                locked_age = 0.0
                ls = self._parity_locked_since.get(slug)
                if ls:
                    locked_age = time.time() - ls
                    locked_hold_secs.append(locked_age)
                locked_shares = min(up_qty, dn_qty)
                locked_usd = locked_shares * (st.positions["Up"].vwap + st.positions["Down"].vwap)
                total_straddle_locked_usd += locked_usd
                inv_imbalance[slug] = {
                    "up_qty": round(up_qty, 1),
                    "dn_qty": round(dn_qty, 1),
                    "up_vwap": round(st.positions["Up"].vwap, 4),
                    "dn_vwap": round(st.positions["Down"].vwap, 4),
                    "imbalance": round(up_qty - dn_qty, 1),
                    "straddle_locked": round(locked_shares, 1),
                    "locked_usd": round(locked_usd, 2),
                    "locked_age_sec": round(locked_age, 1),
                    "unpaired_usd": round(self._parity_invested_usd.get(slug, 0), 2),
                }
        avg_locked_hold_sec = (sum(locked_hold_secs) / len(locked_hold_secs)
                               if locked_hold_secs else 0.0)
        write_jsonl({
            "event_type": "TEMPO_REPORT",
            "per_market": per_market,
            "loop_p95_ms": loop_p95,
            "loop_count": len(self._tempo_loop_times),
            "stale_skip_total": self._stale_skip_total,
            "priority_slugs": list(self._high_priority_slugs),
            "diagnostics": diag,
            "tempo_stats": tempo_stats,
            "inventory_imbalance": inv_imbalance,
            "straddle_locked_usd": round(total_straddle_locked_usd, 2),
            "avg_locked_hold_sec": round(avg_locked_hold_sec, 1),
        })
        # Print summary to console
        total_fills = sum(v.get("fills_per_min", 0) for v in per_market.values())
        total_intents = sum(v.get("intents_per_min", 0) for v in per_market.values())
        print(f"\n  TEMPO: fills/min={total_fills}  intents/min={total_intents}  "
              f"loop_p95={loop_p95:.1f}ms  stale_skips={self._stale_skip_total}")
        print(f"  DIAG:  taker={diag['taker_count']}  maker={diag['maker_count']}  "
              f"derisk={diag['derisk_count']}(taker={diag['derisk_taker_count']})  "
              f"blocked: whipsaw={diag['blocked_whipsaw']} "
              f"taker_gate={diag['blocked_taker_gate']} "
              f"noflip={diag['blocked_noflip']}")
        print(f"  PARITY: buy_sig={diag['parity_buy_signals']}  "
              f"sell_sig={diag['parity_sell_signals']}  "
              f"trades={diag['parity_trades']}  "
              f"avg_net_edge={diag['parity_avg_edge_net_cents']:.2f}c  "
              f"maker={diag['parity_maker_count']}  "
              f"taker={diag['parity_taker_count']}")
        print(f"  PAIRS: partial={diag['pair_partial_count']}  "
              f"avg_delay={diag['avg_pair_fill_delay_ms']:.0f}ms  "
              f"unwind=${diag['unpaired_unwind_usd']:.2f}  "
              f"recycle={diag['recycle_count']}")
        print(f"  MAKER: placed={diag['maker_orders_placed']}  "
              f"fills={diag['maker_fills']}  "
              f"rate={diag['maker_fill_rate']:.1%}  "
              f"lat_p50={diag['maker_fill_latency_ms_p50']:.0f}ms  "
              f"lat_p90={diag['maker_fill_latency_ms_p90']:.0f}ms  "
              f"timeout={diag['maker_timeout_cancel_count']}  "
              f"lost_best={diag['maker_top_of_book_lost_count']}  "
              f"cancel_rep={diag['cancel_replace_per_min']}")
        if diag['flatten_actions_count'] > 0:
            print(f"  FLATTEN: actions={diag['flatten_actions_count']}  "
                  f"taker={diag['flatten_taker_count']}")
        if diag['rescue_attempts'] > 0 or diag['rescue_success'] > 0:
            print(f"  RESCUE: attempts={diag['rescue_attempts']}  "
                  f"success={diag['rescue_success']}  "
                  f"fallback_sells={diag['rescue_fallback_sells']}")
        if diag['quote_orders_placed'] > 0 or diag['quote_unpaired_events'] > 0:
            print(f"  QUOTE: placed={diag['quote_orders_placed']}  "
                  f"fills={diag['quote_fills']}  "
                  f"rate={diag['quote_fill_rate']:.1%}  "
                  f"unpaired={diag['quote_unpaired_events']}  "
                  f"escalations={diag['quote_unpaired_escalations']}  "
                  f"pauses={diag['quote_pause_count']}")
        if total_straddle_locked_usd > 0:
            print(f"  STRADDLE: locked_usd=${total_straddle_locked_usd:.2f}  "
                  f"avg_hold={avg_locked_hold_sec:.0f}s")
        print(f"  TEMPO: paired_ratio={tempo_stats['paired_trade_ratio']:.1%}  "
              f"paired/min={tempo_stats['paired_trades_per_min']}  "
              f"max_burst={tempo_stats['max_trades_in_one_sec']}/s  "
              f"med_gap={tempo_stats['median_inter_trade_ms']:.0f}ms  "
              f"mkr_ratio={tempo_stats['maker_ratio']:.1%}")
        print(f"  GUARD: spread_blk={diag['blocked_spread']}  "
              f"liq_blk={diag['blocked_liq']}  "
              f"stale_blk={diag['blocked_stale']}  "
              f"adverse={diag['adverse_guard_events']}  "
              f"degrades={diag['adverse_guard_degrades']}  "
              f"hard_pauses={diag['adverse_guard_pauses']}")
        if inv_imbalance:
            for slug, inv in inv_imbalance.items():
                print(f"    INV {slug[:30]:30s}  Up={inv['up_qty']:5.0f}  "
                      f"Dn={inv['dn_qty']:5.0f}  "
                      f"imbal={inv['imbalance']:+5.0f}  "
                      f"locked={inv['straddle_locked']:.0f}({inv['locked_age_sec']:.0f}s)")
        for slug, info in per_market.items():
            if info["fills_per_min"] > 0 or info["intents_per_min"] > 0:
                print(f"    {slug[:30]:30s}  fills={info['fills_per_min']:3d}  "
                      f"intents={info['intents_per_min']:3d}  "
                      f"cache_age_med={info['median_cache_age_ms']:.0f}ms")
        # Reset counters for next minute
        self._tempo_fills.clear()
        self._tempo_intents.clear()
        self._tempo_cache_ages.clear()
        self._tempo_loop_times.clear()
        self._stale_skip_total = 0
        self._diag_taker_count = 0
        self._diag_maker_count = 0
        self._diag_derisk_count = 0
        self._diag_derisk_count_hour = 0     # reset per-hour derisk cap
        self._diag_derisk_reasons.clear()    # reset per-hour derisk reason distribution
        self._diag_derisk_taker_count = 0
        self._diag_blocked_whipsaw = 0
        self._diag_blocked_taker_gate = 0
        self._diag_blocked_noflip = 0
        self._diag_parity_buy_signals = 0
        self._diag_parity_sell_signals = 0
        self._diag_parity_trades = 0
        self._diag_parity_edges.clear()
        self._diag_parity_maker_count = 0
        self._diag_parity_taker_count = 0
        self._diag_pair_partial_count = 0
        self._diag_pair_fill_delays.clear()
        self._diag_unpaired_unwind_usd = 0.0
        self._diag_maker_orders_placed = 0
        self._diag_maker_fills = 0
        self._diag_cancel_replace_count = 0
        self._diag_parity_blocked_spread = 0
        self._diag_parity_blocked_liq = 0
        self._diag_parity_blocked_stale = 0
        self._diag_recycle_count = 0
        self._diag_maker_fill_latencies.clear()
        self._diag_maker_timeout_cancel_count = 0
        self._diag_maker_lost_best_count = 0
        self._diag_flatten_actions = 0
        self._diag_flatten_taker = 0
        self._diag_rescue_attempts = 0
        self._diag_rescue_success = 0
        self._diag_rescue_fallback_sells = 0
        self._diag_quote_orders_placed = 0
        self._diag_quote_fills = 0
        self._diag_quote_unpaired_events = 0
        self._diag_quote_unpaired_escalations = 0
        self._diag_quote_pause_count = 0
        self._diag_adverse_guard_events = 0
        self._diag_adverse_guard_pauses = 0
        self._diag_adverse_guard_degrades = 0
        self._diag_parity_trade_timestamps.clear()
        self._diag_quote_submit_count = 0
        self._diag_quote_cancel_count = 0
        self._diag_quote_replace_count = 0
        self._diag_maker_queue_times.clear()
        self._diag_hedge_tick1 = 0
        self._diag_hedge_tick2 = 0
        self._diag_hedge_cross = 0
        self._diag_hedge_cross_early = 0
        self._diag_hedge_cross_late = 0
        self._diag_hedge_skipped_stale = 0
        self._diag_hedge_unwind = 0

    def _emit_clone_report(self):
        """Emit F247 similarity metrics + 4 analytics every minute."""
        import statistics

        # ── 1. Paired straddle metrics (from _pair_tracker via _record_pair_fill) ──
        pairs_within_500ms = self._diag_pairs_completed_500ms
        pairs_within_1500ms = self._diag_pairs_completed_1500ms
        pairs_within_10s = self._diag_pairs_completed_10s
        total_pairs = self._diag_pairs_completed
        paired_500ms_ratio = (pairs_within_500ms / max(1, total_pairs)
                              if total_pairs > 0 else 0.0)
        paired_straddle_ratio = (pairs_within_1500ms / max(1, total_pairs)
                                 if total_pairs > 0 else 0.0)
        paired_10s_ratio = (pairs_within_10s / max(1, total_pairs)
                            if total_pairs > 0 else 0.0)

        # median pair fill delay (Up vs Down fill timestamps)
        med_pair_delay_ms = (statistics.median(self._clone_pair_delays)
                             if self._clone_pair_delays else 0.0)
        # median time between pairs (burstiness)
        med_inter_pair_ms = (statistics.median(self._clone_inter_pair_gaps)
                             if self._clone_inter_pair_gaps else 0.0)
        # signal-to-fill latency
        med_signal_to_fill_ms = (statistics.median(self._clone_signal_to_fill)
                                 if self._clone_signal_to_fill else 0.0)
        # maker queue time percentiles (submit_ts -> fill_ts)
        queue_p50 = 0.0
        queue_p90 = 0.0
        if self._diag_maker_queue_times:
            sorted_qt = sorted(self._diag_maker_queue_times)
            p50_idx = len(sorted_qt) // 2
            p90_idx = int(len(sorted_qt) * 0.9)
            queue_p50 = sorted_qt[min(p50_idx, len(sorted_qt) - 1)]
            queue_p90 = sorted_qt[min(p90_idx, len(sorted_qt) - 1)]

        # ── 2. Hold time distribution for paired inventory ──
        hold_p50 = 0.0
        hold_p90 = 0.0
        if self._clone_hold_times:
            sorted_ht = sorted(self._clone_hold_times)
            hold_p50 = sorted_ht[len(sorted_ht) // 2]
            hold_p90 = sorted_ht[int(len(sorted_ht) * 0.9)]
        # Also compute current hold times from active locked positions
        active_hold_secs = []
        now_t = time.time()
        for slug, ls_ts in self._parity_locked_since.items():
            active_hold_secs.append(now_t - ls_ts)
        active_hold_p50 = (statistics.median(active_hold_secs)
                           if active_hold_secs else 0.0)

        # ── 3. Net imbalance per slug and max abs imbalance ──
        per_slug_imbalance = {}
        global_max_imbalance = 0.0
        for slug, st in self.market_states.items():
            up_q = st.positions["Up"].qty
            dn_q = st.positions["Down"].qty
            imbal = up_q - dn_q
            max_imbal = self._diag_max_imbalance.get(slug, abs(imbal))
            global_max_imbalance = max(global_max_imbalance, max_imbal)
            if abs(imbal) >= MIN_QTY or max_imbal >= MIN_QTY:
                per_slug_imbalance[slug] = {
                    "current_imbalance": round(imbal, 1),
                    "max_abs_imbalance": round(max_imbal, 1),
                }

        # ── 4. Correlation between net exposure and spot trend ──
        imbal_delta_corr = 0.0
        if len(self._diag_imbalance_delta_samples) >= 10:
            imbals = [s[0] for s in self._diag_imbalance_delta_samples]
            deltas = [s[1] for s in self._diag_imbalance_delta_samples]
            mean_i = sum(imbals) / len(imbals)
            mean_d = sum(deltas) / len(deltas)
            cov = sum((i - mean_i) * (d - mean_d) for i, d in zip(imbals, deltas)) / len(imbals)
            var_i = sum((i - mean_i) ** 2 for i in imbals) / len(imbals)
            var_d = sum((d - mean_d) ** 2 for d in deltas) / len(deltas)
            denom = (var_i * var_d) ** 0.5
            imbal_delta_corr = cov / denom if denom > 1e-9 else 0.0

        # ── 5. Per-slug F247 clone KPI ──
        per_slug_kpi = {}
        for slug in set(list(self._tempo_cache_ages.keys()) + list(self._bg_fetch_durations.keys())
                        + [s for s in self.market_states]):
            # Cache age p50/p90
            ages = self._tempo_cache_ages.get(slug, [])
            ca_p50 = 0.0
            ca_p90 = 0.0
            if ages:
                sa = sorted(ages)
                ca_p50 = sa[len(sa) // 2]
                ca_p90 = sa[min(int(len(sa) * 0.9), len(sa) - 1)]
            # BG fetch duration p50/p90
            fetch_durs = self._bg_fetch_durations.get(slug, [])
            bf_p50 = 0.0
            bf_p90 = 0.0
            if fetch_durs:
                sf = sorted(fetch_durs)
                bf_p50 = sf[len(sf) // 2]
                bf_p90 = sf[min(int(len(sf) * 0.9), len(sf) - 1)]
            # Per-slug pair completion KPI
            slug_unpaired = self._diag_slug_unpaired_events.get(slug, 0)
            slug_p500 = self._diag_slug_paired_500ms.get(slug, 0)
            slug_p1500 = self._diag_slug_paired_1500ms.get(slug, 0)
            slug_timeouts = self._diag_slug_timeouts.get(slug, 0)
            # Refresh misses
            refresh_misses = self._bg_refresh_miss_count.get(slug, 0)
            # Pending fetch streak
            slug_fetch_streak = self._diag_pending_fetch_streak.get(slug, 0)
            # Hedge cross rate: crosses / (crosses + unwinds) for this window
            hedge_total = self._diag_hedge_cross + self._diag_hedge_unwind
            hedge_cross_rate = self._diag_hedge_cross / max(1, hedge_total)

            if ca_p90 > 0 or bf_p90 > 0 or slug_p500 > 0 or slug_p1500 > 0 or refresh_misses > 0:
                per_slug_kpi[slug] = {
                    "cache_age_p50_ms": round(ca_p50, 0),
                    "cache_age_p90_ms": round(ca_p90, 0),
                    "bg_fetch_p50_ms": round(bf_p50, 0),
                    "bg_fetch_p90_ms": round(bf_p90, 0),
                    "refresh_miss_count": refresh_misses,
                    "unpaired_events": slug_unpaired,
                    "paired_within_500ms": slug_p500,
                    "paired_within_1500ms": slug_p1500,
                    "timeouts": slug_timeouts,
                    "pending_fetch_streak": slug_fetch_streak,
                    "hedge_cross_rate": round(hedge_cross_rate, 3),
                }

        # ── Build report (use clone-dedicated counters for lifecycle) ──
        clone_fills = self._clone_quote_fill_count
        clone_submits = self._clone_quote_submit_count
        clone_cancels = self._clone_quote_cancel_count
        clone_replaces = self._clone_quote_replace_count
        fill_rate = clone_fills / max(1, clone_submits)
        clone_data = {
            "event_type": "CLONE_REPORT",
            "ts_ms": int(time.time() * 1000),
            # Pair metrics
            "pairs_completed": total_pairs,
            "pairs_within_500ms": pairs_within_500ms,
            "pairs_within_1500ms": pairs_within_1500ms,
            "pairs_within_10s": pairs_within_10s,
            "paired_within_500ms_ratio": round(paired_500ms_ratio, 3),
            "paired_straddle_ratio_1500ms": round(paired_straddle_ratio, 3),
            "paired_straddle_ratio_10s": round(paired_10s_ratio, 3),
            "median_pair_fill_delay_ms": round(med_pair_delay_ms, 1),
            "median_time_between_pairs_ms": round(med_inter_pair_ms, 1),
            "median_signal_to_fill_ms": round(med_signal_to_fill_ms, 1),
            # Queue/latency
            "maker_queue_time_p50_ms": round(queue_p50, 1),
            "maker_queue_time_p90_ms": round(queue_p90, 1),
            # Lifecycle
            "quote_submit_count": clone_submits,
            "quote_cancel_count": clone_cancels,
            "quote_replace_count": clone_replaces,
            "quote_fill_count": clone_fills,
            "quote_fill_rate": round(fill_rate, 3),
            "active_quote_orders": len(self._active_orders),
            "pending_unpaired": len(self._quote_unpaired),
            # Hedge
            "hedge_tick1": self._diag_hedge_tick1,
            "hedge_tick2": self._diag_hedge_tick2,
            "hedge_cross": self._diag_hedge_cross,
            "hedge_cross_early_count": self._diag_hedge_cross_early,
            "hedge_cross_late_count": self._diag_hedge_cross_late,
            "hedge_skipped_stale_count": self._diag_hedge_skipped_stale,
            "hedge_unwind": self._diag_hedge_unwind,
            "pending_fetch_streak_max": self._diag_pending_fetch_streak_max,
            # Hold time
            "hold_time_p50_sec": round(hold_p50, 1),
            "hold_time_p90_sec": round(hold_p90, 1),
            "active_hold_p50_sec": round(active_hold_p50, 1),
            "active_locked_count": len(active_hold_secs),
            # Imbalance
            "max_abs_imbalance": round(global_max_imbalance, 1),
            "per_slug_imbalance": per_slug_imbalance,
            # Correlation
            "imbalance_delta_correlation": round(imbal_delta_corr, 3),
            # Adverse guard
            "adverse_events": self._diag_adverse_guard_events,
            "adverse_degrades": self._diag_adverse_guard_degrades,
            "adverse_hard_pauses": self._diag_adverse_guard_pauses,
            "fast_clone": FAST_CLONE,
            # Per-slug KPIs
            "per_slug_kpi": per_slug_kpi,
        }
        write_jsonl(clone_data)

        # Console print
        print(f"  CLONE: pairs={total_pairs}  "
              f"r500={paired_500ms_ratio:.0%}  "
              f"r1.5s={paired_straddle_ratio:.0%}  "
              f"r10s={paired_10s_ratio:.0%}  "
              f"delay={med_pair_delay_ms:.0f}ms  "
              f"gap={med_inter_pair_ms:.0f}ms  "
              f"sig2fill={med_signal_to_fill_ms:.0f}ms")
        print(f"  LIFECYCLE: submit={clone_submits}  "
              f"cancel={clone_cancels}  "
              f"replace={clone_replaces}  "
              f"fills={clone_fills}  "
              f"fill_rate={fill_rate:.0%}  "
              f"queue_p50={queue_p50:.0f}ms  "
              f"queue_p90={queue_p90:.0f}ms")
        print(f"  HEDGE: tick1={self._diag_hedge_tick1}  "
              f"tick2={self._diag_hedge_tick2}  "
              f"cross_early={self._diag_hedge_cross_early}  "
              f"cross_late={self._diag_hedge_cross_late}  "
              f"stale_skip={self._diag_hedge_skipped_stale}  "
              f"unwind={self._diag_hedge_unwind}  "
              f"fetch_streak={self._diag_pending_fetch_streak_max}")
        print(f"  HOLD: p50={hold_p50:.0f}s  p90={hold_p90:.0f}s  "
              f"active_p50={active_hold_p50:.0f}s  locked={len(active_hold_secs)}")
        print(f"  IMBAL: max={global_max_imbalance:.0f}  "
              f"corr(imbal,delta)={imbal_delta_corr:.2f}")
        if per_slug_imbalance:
            for slug, info in per_slug_imbalance.items():
                print(f"    {slug[:30]:30s}  imbal={info['current_imbalance']:+5.0f}  "
                      f"max={info['max_abs_imbalance']:.0f}")
        # Per-slug KPI line
        if per_slug_kpi:
            for slug, kpi in per_slug_kpi.items():
                if kpi["cache_age_p90_ms"] > 0 or kpi.get("paired_within_500ms", 0) > 0 or kpi.get("paired_within_1500ms", 0) > 0:
                    print(f"    KPI {slug[:25]:25s}  "
                          f"ca_p90={kpi['cache_age_p90_ms']:.0f}ms  "
                          f"bg_p90={kpi['bg_fetch_p90_ms']:.0f}ms  "
                          f"p500={kpi.get('paired_within_500ms', 0)}  "
                          f"p1500={kpi.get('paired_within_1500ms', 0)}  "
                          f"tout={kpi.get('timeouts', 0)}  "
                          f"miss={kpi['refresh_miss_count']}  "
                          f"hcross={kpi['hedge_cross_rate']:.0%}")
        # Reset clone-specific counters (per-minute)
        self._clone_pair_delays.clear()
        self._clone_inter_pair_gaps.clear()
        self._clone_signal_to_fill.clear()
        self._clone_hold_times.clear()
        self._diag_top_of_book_time_ms = 0.0
        self._diag_top_of_book_total_ms = 0.0
        self._diag_pairs_completed = 0
        self._diag_pairs_completed_500ms = 0
        self._diag_pairs_completed_1500ms = 0
        self._diag_pairs_completed_10s = 0
        self._diag_slug_unpaired_events.clear()
        self._diag_slug_paired_500ms.clear()
        self._diag_slug_paired_1500ms.clear()
        self._diag_slug_timeouts.clear()
        self._diag_pending_fetch_streak.clear()
        self._diag_pending_fetch_streak_max = 0
        self._diag_max_imbalance.clear()
        self._diag_imbalance_delta_samples.clear()
        # Reset clone-dedicated lifecycle counters
        self._clone_quote_submit_count = 0
        self._clone_quote_cancel_count = 0
        self._clone_quote_replace_count = 0
        self._clone_quote_fill_count = 0
        # Reset per-slug refresh miss counts
        self._bg_refresh_miss_count.clear()
        # Clean up stale pair tracker entries (older than 30s)
        stale_cutoff = time.time() - 30.0
        stale_ids = [pid for pid, info in self._pair_tracker.items()
                     if all(f["ts"] < stale_cutoff for f in info["fills"].values())]
        for pid in stale_ids:
            self._pair_tracker.pop(pid, None)

    def _emit_diag_report(self):
        """Emit behavioral diagnostics every 60s — measures success vs F247 targets.
        Targets: trades/min ~10-20, avg_trade_size ~$7+, median_hold >=120s, exit >= +3c."""
        import statistics as _stats
        now_t = time.time()
        elapsed_min = max(0.1, (now_t - self._diag_report_last_ts) / 60.0)

        # ── Core metrics ──
        total_fills = self._diag_total_fills_min
        dir_fills = self._diag_directional_fills_min
        par_fills = self._diag_parity_fills_min
        trades_per_min = total_fills / elapsed_min
        parity_fill_pct = par_fills / max(1, total_fills)
        dir_entry_count = self._diag_dscalp_entries
        dir_exit_count = self._diag_dscalp_exits
        avg_trade_size = (_stats.mean(self._diag_trade_sizes)
                          if self._diag_trade_sizes else 0.0)
        med_hold = (_stats.median(self._diag_dscalp_hold_times)
                    if self._diag_dscalp_hold_times else 0.0)
        med_exit = (_stats.median(self._diag_dscalp_exit_cents)
                    if self._diag_dscalp_exit_cents else 0.0)
        unpaired_rate = self._diag_unpaired_count_min / max(1, total_fills)
        derisk_rate = self._diag_derisk_count_min / max(1, total_fills)

        # ── SOL/XRP cache age ──
        sol_ages = []
        xrp_ages = []
        for slug, ages_list in self._tempo_cache_ages.items():
            if "sol" in slug.lower():
                sol_ages = ages_list
            if "xrp" in slug.lower():
                xrp_ages = ages_list

        def _percentiles(vals):
            if not vals:
                return 0.0, 0.0
            s = sorted(vals)
            return s[len(s) // 2], s[min(int(len(s) * 0.9), len(s) - 1)]

        sol_p50, sol_p90 = _percentiles(sol_ages)
        xrp_p50, xrp_p90 = _percentiles(xrp_ages)

        # ── True cost ──
        hour_elapsed = max(0.01, (now_t - self._true_cost_hour_start_ts) / 3600.0)
        fills_per_hour = self._true_cost_fill_count / hour_elapsed
        tx_per_hour = self._true_cost_tx_count / hour_elapsed
        est_cost_per_hour = (self._true_cost_tx_count * TRUE_COST_EST_GAS_PER_TX_USD
                             + self._true_cost_fill_count * TRUE_COST_EST_FEE_BPS / 10000.0 * avg_trade_size)

        # ── Regime ──
        low_vol_slugs = sum(1 for v in self._regime_is_low_vol.values() if v)
        total_slugs = max(1, len(self._regime_is_low_vol))

        diag_data = {
            "event_type": "DIAG_REPORT",
            "ts_ms": int(now_t * 1000),
            # F247 target metrics
            "trades_per_min": round(trades_per_min, 1),
            "avg_trade_size_usd": round(avg_trade_size, 2),
            "median_directional_hold_sec": round(med_hold, 1),
            "median_directional_exit_cents": round(med_exit, 2),
            # Fill composition
            "directional_entry_count": dir_entry_count,
            "directional_exit_count": dir_exit_count,
            "parity_fills": par_fills,
            "parity_fill_pct": round(parity_fill_pct, 3),
            "total_fills": total_fills,
            # Quality
            "unpaired_rate": round(unpaired_rate, 3),
            "derisk_rate": round(derisk_rate, 3),
            # Cache freshness
            "sol_cache_p50_ms": round(sol_p50, 0),
            "sol_cache_p90_ms": round(sol_p90, 0),
            "xrp_cache_p50_ms": round(xrp_p50, 0),
            "xrp_cache_p90_ms": round(xrp_p90, 0),
            # True cost
            "fills_per_hour": round(fills_per_hour, 0),
            "tx_per_hour": round(tx_per_hour, 0),
            "est_cost_per_hour_usd": round(est_cost_per_hour, 4),
            # Regime
            "low_vol_slugs": low_vol_slugs,
            "total_slugs": total_slugs,
            # Rate limiter
            "rate_blocked_interval": self._rate_blocked_interval,
            "rate_blocked_cap": self._rate_blocked_cap,
            # Directional detail
            "dscalp_tp1": self._diag_dscalp_tp1,
            "dscalp_tp2": self._diag_dscalp_tp2,
            "dscalp_tp3": self._diag_dscalp_tp3,
            "dscalp_timeouts": self._diag_dscalp_timeout_exits,
            "dscalp_stops": self._diag_dscalp_stop_exits,
            "dscalp_active_positions": len(self._dscalp_positions),
        }
        write_jsonl(diag_data)

        # ── Console output (F247 target comparison) ──
        # Status indicators: check vs target
        tpm_ok = "OK" if 10 <= trades_per_min <= 20 else "!!"
        size_ok = "OK" if avg_trade_size >= 7.0 else "!!"
        hold_ok = "OK" if med_hold >= 120 else ("--" if med_hold == 0 else "!!")
        exit_ok = "OK" if med_exit >= 5.0 else ("--" if med_exit == 0 else "!!")
        par_ok = "OK" if parity_fill_pct <= 0.30 else "!!"

        print(f"  DIAG [{tpm_ok}] trades/min={trades_per_min:.1f}  "
              f"[{size_ok}] avg_size=${avg_trade_size:.1f}  "
              f"[{hold_ok}] hold={med_hold:.0f}s  "
              f"[{exit_ok}] exit={med_exit:+.1f}c")
        print(f"  FILL: dir_entry={dir_entry_count}  dir_exit={dir_exit_count}  "
              f"parity={par_fills}  [{par_ok}] par_pct={parity_fill_pct:.0%}  "
              f"unpaired={unpaired_rate:.0%}  derisk={derisk_rate:.0%}")
        print(f"  CACHE: SOL={sol_p50:.0f}/{sol_p90:.0f}ms  XRP={xrp_p50:.0f}/{xrp_p90:.0f}ms  "
              f"regime: {low_vol_slugs}/{total_slugs} low-vol")
        print(f"  COST: fills/hr={fills_per_hour:.0f}  tx/hr={tx_per_hour:.0f}  "
              f"est=${est_cost_per_hour:.4f}/hr  "
              f"rate_block={self._rate_blocked_interval}+{self._rate_blocked_cap}")
        if self._dscalp_positions:
            print(f"  DSCALP: tp1={self._diag_dscalp_tp1} tp2={self._diag_dscalp_tp2} "
                  f"tp3={self._diag_dscalp_tp3} "
                  f"timeout={self._diag_dscalp_timeout_exits} stop={self._diag_dscalp_stop_exits} "
                  f"active={len(self._dscalp_positions)}")

        # Reset per-minute counters
        self._diag_report_last_ts = now_t
        self._true_cost_fill_count_min = 0
        self._true_cost_submit_count = 0
        self._true_cost_cancel_count = 0
        self._diag_derisk_count_min = 0
        self._diag_unpaired_count_min = 0
        self._diag_parity_fills_min = 0
        self._diag_directional_fills_min = 0
        self._diag_total_fills_min = 0
        self._diag_trade_sizes.clear()
        self._rate_blocked_interval = 0
        self._rate_blocked_cap = 0
        self._diag_dscalp_entries = 0
        self._diag_dscalp_exits = 0
        self._diag_dscalp_tp1 = 0
        self._diag_dscalp_tp2 = 0
        self._diag_dscalp_tp3 = 0
        self._diag_dscalp_timeout_exits = 0
        self._diag_dscalp_stop_exits = 0
        self._diag_dscalp_breakeven_exits = 0
        self._diag_dscalp_hold_times.clear()
        self._diag_dscalp_exit_cents.clear()

    def _emit_pnl_attribution(self):
        """PnL attribution report — runs every PNL_REPORT_INTERVAL_SEC (15m default) + hourly.
        Emits: realized_pnl by reason (TP/rescue/recycle/derisk/flatten),
        winrate, median_exit_cents, median_hold_sec, derisk detail, pause totals."""
        import statistics as _stats
        now_t = time.time()

        # Self-timed: run every 15 min (plus called at hourly diag)
        if now_t - self._pnl_report_last_ts < PNL_REPORT_INTERVAL_SEC:
            return
        self._pnl_report_last_ts = now_t

        all_exit_cents = list(self._diag_dscalp_exit_cents)  # snapshot
        all_hold_times = list(self._diag_dscalp_hold_times)
        n_exits = len(all_exit_cents)

        # PnL by reason
        pnl_by_reason = {}
        for reason, entries in self._diag_exit_by_reason.items():
            pnl_list = [e[0] for e in entries]
            hold_list = [e[1] for e in entries]
            usd_list = [e[2] for e in entries if e[2] > 0]
            n = len(pnl_list)
            pnl_by_reason[reason] = {
                "count": n,
                "total_cents": round(sum(pnl_list), 2),
                "avg_cents": round(_stats.mean(pnl_list), 2) if pnl_list else 0.0,
                "median_cents": round(_stats.median(pnl_list), 2) if pnl_list else 0.0,
                "median_hold_sec": round(_stats.median(hold_list), 1) if hold_list else 0.0,
                "total_usd": round(sum(usd_list), 2) if usd_list else 0.0,
            }

        # Derisk detail
        derisk_reasons = pnl_by_reason.copy()
        derisk_entries = []
        for reason, entries in self._diag_exit_by_reason.items():
            if "derisk" in reason:
                derisk_entries.extend(entries)
        derisk_loss_cents = [e[0] for e in derisk_entries]
        avg_derisk_loss = _stats.mean(derisk_loss_cents) if derisk_loss_cents else 0.0

        # Pause time totals
        slug_pause_sec = sum(
            max(0, t - now_t) for t in self._om_slug_paused_until.values()
        ) + self._pnl_total_slug_pause_sec
        drift_pause_sec = self._pnl_total_drift_pause_sec

        # Core metrics
        if n_exits > 0:
            gross_exit_cents = sum(all_exit_cents)
            avg_exit_cents = _stats.mean(all_exit_cents)
            med_exit_cents = _stats.median(all_exit_cents)
            winners = [c for c in all_exit_cents if c > 0]
            losers = [c for c in all_exit_cents if c <= 0]
            winrate = len(winners) / n_exits
            avg_win = _stats.mean(winners) if winners else 0.0
            avg_loss = _stats.mean(losers) if losers else 0.0
            med_hold = _stats.median(all_hold_times) if all_hold_times else 0.0
            expectancy = avg_exit_cents
        else:
            gross_exit_cents = avg_exit_cents = med_exit_cents = 0.0
            winrate = avg_win = avg_loss = med_hold = expectancy = 0.0

        report = {
            "event_type": "PNL_ATTRIBUTION",
            "ts_ms": int(now_t * 1000),
            "n_exits": n_exits,
            "gross_exit_cents": round(gross_exit_cents, 2),
            "avg_exit_cents": round(avg_exit_cents, 2),
            "median_exit_cents": round(med_exit_cents, 2),
            "winrate": round(winrate, 3),
            "avg_win_cents": round(avg_win, 2),
            "avg_loss_cents": round(avg_loss, 2),
            "expectancy_cents": round(expectancy, 2),
            "median_hold_sec": round(med_hold, 1),
            # Derisk detail
            "derisk_count": self._diag_derisk_count_hour,
            "avg_derisk_loss_cents": round(avg_derisk_loss, 2),
            "derisk_reasons": dict(self._diag_derisk_reasons),
            # Exit breakdown
            "orphan_cancel_count": self._om_orphan_canceled_count,
            "tp1_count": self._diag_dscalp_tp1,
            "tp2_count": self._diag_dscalp_tp2,
            "tp3_count": self._diag_dscalp_tp3,
            "breakeven_count": self._diag_dscalp_breakeven_exits,
            "timeout_count": self._diag_dscalp_timeout_exits,
            "stop_count": self._diag_dscalp_stop_exits,
            # PnL by reason (the knob-tuning data)
            "pnl_by_reason": pnl_by_reason,
            # Pause time totals
            "slug_pause_total_sec": round(slug_pause_sec, 1),
            "drift_pause_total_sec": round(drift_pause_sec, 1),
            "drift_events": self._om_drift_count,
            "position_mismatches": self._om_drift_position_mismatches,
            # Auto-disabled slugs
            "auto_disabled_slugs": [s for s, t in self._slug_auto_disabled_until.items() if now_t < t],
        }
        write_jsonl(report)

        # Console summary
        print(f"  PNL: {n_exits} exits  gross={gross_exit_cents:+.1f}c  "
              f"med={med_exit_cents:+.1f}c  win={winrate:.0%}  "
              f"exp={expectancy:+.2f}c/trade  hold={med_hold:.0f}s")
        print(f"  EXIT: tp1={self._diag_dscalp_tp1} tp2={self._diag_dscalp_tp2} "
              f"tp3={self._diag_dscalp_tp3} be={self._diag_dscalp_breakeven_exits} "
              f"timeout={self._diag_dscalp_timeout_exits} stop={self._diag_dscalp_stop_exits} "
              f"derisk={self._diag_derisk_count_hour} avg_drsk={avg_derisk_loss:+.1f}c")
        if pnl_by_reason:
            parts = [f"{r}={d['total_cents']:+.1f}c({d['count']})"
                     for r, d in sorted(pnl_by_reason.items(), key=lambda x: -abs(x[1]['total_cents']))]
            print(f"  BY_REASON: {' '.join(parts)}")
        paused_slugs = [s for s, t in self._slug_auto_disabled_until.items() if now_t < t]
        if paused_slugs:
            print(f"  AUTO_DISABLED: {', '.join(paused_slugs)}")
        print(f"  PAUSES: slug={slug_pause_sec:.0f}s  drift={drift_pause_sec:.0f}s  "
              f"drift_events={self._om_drift_count}")

        # Do NOT clear _diag_exit_by_reason here — let it accumulate for hourly rollup
        # It gets cleared at hour boundary by the hourly reset

    # ── Slug Auto-Disable ────────────────────────────────────────────
    def _check_slug_auto_disable(self):
        """Auto-disable slugs with bad rolling 30m PnL.
        If a slug's realized PnL over the last SLUG_AUTO_DISABLE_WINDOW_SEC < -SLUG_AUTO_DISABLE_LOSS_USD,
        disable entries for that slug for SLUG_AUTO_DISABLE_DURATION_SEC."""
        if not SLUG_AUTO_DISABLE_ENABLED:
            return
        now_t = time.time()
        cutoff = now_t - SLUG_AUTO_DISABLE_WINDOW_SEC

        for slug in list(self._slug_realized_pnl_window.keys()):
            # Prune old entries
            entries = self._slug_realized_pnl_window[slug]
            entries[:] = [(ts, pnl) for ts, pnl in entries if ts >= cutoff]
            if not entries:
                self._slug_realized_pnl_window.pop(slug, None)
                continue

            # Already disabled — skip
            if self._slug_auto_disabled_until.get(slug, 0) > now_t:
                continue

            rolling_pnl = sum(pnl for _, pnl in entries)
            if rolling_pnl < -SLUG_AUTO_DISABLE_LOSS_USD:
                disable_until = now_t + SLUG_AUTO_DISABLE_DURATION_SEC
                self._slug_auto_disabled_until[slug] = disable_until
                self._pnl_total_slug_pause_sec += SLUG_AUTO_DISABLE_DURATION_SEC
                write_jsonl({
                    "event_type": "SLUG_AUTO_DISABLED",
                    "slug": slug,
                    "rolling_pnl_usd": round(rolling_pnl, 2),
                    "window_sec": SLUG_AUTO_DISABLE_WINDOW_SEC,
                    "disable_duration_sec": SLUG_AUTO_DISABLE_DURATION_SEC,
                    "disable_until": disable_until,
                    "n_exits": len(entries),
                })
                print(f"  ⚠ SLUG_AUTO_DISABLED {slug}  "
                      f"rolling_pnl=${rolling_pnl:+.2f} over {SLUG_AUTO_DISABLE_WINDOW_SEC:.0f}s  "
                      f"disabled for {SLUG_AUTO_DISABLE_DURATION_SEC/3600:.1f}h")

    def _slug_auto_disabled(self, slug: str) -> bool:
        """Return True if slug is currently auto-disabled due to bad PnL."""
        if not SLUG_AUTO_DISABLE_ENABLED:
            return False
        return self._slug_auto_disabled_until.get(slug, 0) > time.time()

    def _print_balance_summary(self):
        """Print balance and open positions to console."""
        total_cost = 0.0
        pos_parts = []
        for slug, st in self.market_states.items():
            for outcome in ["Up", "Down"]:
                pos = st.positions[outcome]
                if pos.qty >= MIN_QTY:
                    total_cost += pos.cost_usdc
                    pos_parts.append(f"{st.crypto} {outcome}:{pos.qty:.0f}@{pos.vwap:.3f}")
        pos_str = "  ".join(pos_parts) if pos_parts else "none"
        equity = self._equity()
        daily_pnl = equity - self.day_start_equity
        pnl_str = f"+${daily_pnl:.2f}" if daily_pnl >= 0 else f"-${abs(daily_pnl):.2f}"
        rpnl_str = f"+${self.realized_pnl_usdc:.2f}" if self.realized_pnl_usdc >= 0 else f"-${abs(self.realized_pnl_usdc):.2f}"
        print(f"\n  --- Cash: ${self.cash_usdc:.2f}  |  Equity: ${equity:.2f}  |  Invested: ${total_cost:.2f}"
              f"  |  Day P&L: {pnl_str}  |  Total P&L: {rpnl_str}  |  Positions: {pos_str} ---\n")
    def _resolve_ended_hours(self, current_markets: List[MarketRef]):
        """
        Resolve positions for hours that have ended.
        Winner pays $1/share, loser pays $0. Then remove old market state.
        """
        current_slugs = {m.slug for m in current_markets}
        ended = [slug for slug in list(self.market_states.keys()) if slug not in current_slugs]
        for slug in ended:
            st = self.market_states[slug]
            has_positions = any(st.positions[o].qty >= MIN_QTY for o in ["Up", "Down"])
            if not has_positions:
                # No positions — still log settlement for truth tracking, then clean up
                try:
                    sp, nho = self.client.get_binance_spot_and_hour_open(st.crypto)
                    sd = (sp - st.hour_open) / max(st.hour_open, 1e-9) * 10000.0
                    w = "Up" if sp >= st.hour_open else "Down"
                    write_jsonl({
                        "event_type": "HOUR_LABEL", "slug": slug, "crypto": st.crypto,
                        "hour_start_utc": st.hour_start_utc,
                        "hour_label_et": _hour_label_et(st.hour_start_utc),
                        "hour_index": st.hour_index,
                        "hour_open": round(st.hour_open, 2),
                        "hour_final_price": round(sp, 2),
                        "effective_hour_close": round(nho, 2),
                        "hour_close_source": "NEXT_HOUR_OPEN",
                        "hour_direction": w,
                        "settlement_delta_bps": round(sd, 3),
                        "won_up": w == "Up", "won_down": w == "Down",
                        "position_direction_at_entry": "NONE",
                        "would_win_if_held_to_settle": None,
                    })
                except Exception:
                    pass
                self.market_states.pop(slug, None)
                self.signal_hist.pop(slug, None)
                self.last_book.pop(slug, None)
                self.recent_extreme_price.pop(slug, None)
                self.entry_sm.pop(slug, None)
                self._data_cache.pop(slug, None)
                self._pending_fetches.pop(slug, None)
                self._parity_last_order_ts.pop(slug, None)
                self._parity_invested_usd.pop(slug, None)
                self._parity_locked_since.pop(slug, None)
                self._parity_maker_orders.pop(slug, None)
                self._parity_pending_pairs[:] = [p for p in self._parity_pending_pairs if p["slug"] != slug]
                continue
            # Determine winner by checking final Binance price vs hour open
            # spot here = next hour's opening price (close proxy for the prior hour)
            spot, next_hour_open = self.client.get_binance_spot_and_hour_open(st.crypto)
            close_proxy = spot  # spot at transition ≈ open(H+1) ≈ close(H)
            winner = "Up" if close_proxy >= st.hour_open else "Down"
            settlement_delta_bps = (close_proxy - st.hour_open) / max(st.hour_open, 1e-9) * 10000.0
            self.logger.log_event({
                "event_type": "WINDOW_SETTLED", "slug": slug, "crypto": st.crypto,
                "hour_start_utc": st.hour_start_utc,
                "settlement_price": round(close_proxy, 2),
                "hour_open": round(st.hour_open, 2),
                "settlement_delta_bps": round(settlement_delta_bps, 3),
                "winner": winner,
                "next_hour_open": round(next_hour_open, 2),
            }, also_csv=True)
            pos_details = []
            for outcome in ["Up", "Down"]:
                pos = st.positions[outcome]
                if pos.qty < MIN_QTY:
                    self._clean_dust(pos)
                    continue
                payout_price = 1.0 if outcome == winner else 0.0
                payout = payout_price * pos.qty
                pnl = payout - pos.cost_usdc
                self.cash_usdc += payout
                self.realized_pnl_usdc += pnl
                self.hourly_pnl_usdc += pnl
                self._hour_net_pnl += pnl
                pos_details.append({"outcome": outcome, "qty": pos.qty,
                                    "payout": payout, "pnl": pnl})
                pos.qty = 0.0
                pos.cost_usdc = 0.0
            # Shadow settlement: settle shadow positions for this slug
            if self._shadow_active and slug in self._shadow_positions:
                for outcome in ["Up", "Down"]:
                    sp = self._shadow_positions.get(slug, {}).get(outcome)
                    if sp and sp["qty"] >= MIN_QTY:
                        s_payout = (1.0 if outcome == winner else 0.0) * sp["qty"]
                        self._shadow_cash += s_payout
                        sp["qty"] = 0.0
                        sp["cost_usdc"] = 0.0
                self._shadow_positions.pop(slug, None)
            self.logger.log_event({
                "event_type": "HOUR_RESOLVED", "slug": slug, "crypto": st.crypto,
                "hour_start_utc": st.hour_start_utc,
                "winner": winner,
                "close_proxy": round(close_proxy, 2),
                "hour_open": round(st.hour_open, 2),
                "positions": pos_details,
                "cash": round(self.cash_usdc, 2),
                "equity": round(self._equity(), 2),
            })
            # HOUR_LABEL — truth labels for settlement analysis
            # What position direction did we actually hold?
            held_up = any(d["outcome"] == "Up" for d in pos_details)
            held_down = any(d["outcome"] == "Down" for d in pos_details)
            position_direction = "Up" if (held_up and not held_down) else ("Down" if (held_down and not held_up) else ("BOTH" if (held_up and held_down) else "NONE"))
            hour_direction = "Up" if close_proxy >= st.hour_open else "Down"
            would_win = position_direction == hour_direction if position_direction in ("Up", "Down") else None
            write_jsonl({
                "event_type": "HOUR_LABEL", "slug": slug, "crypto": st.crypto,
                "hour_start_utc": st.hour_start_utc,
                "hour_label_et": _hour_label_et(st.hour_start_utc),
                "hour_index": st.hour_index,
                "hour_open": round(st.hour_open, 2),
                "hour_final_price": round(close_proxy, 2),
                "effective_hour_close": round(next_hour_open, 2),
                "hour_close_source": "NEXT_HOUR_OPEN",
                "hour_direction": hour_direction,
                "settlement_delta_bps": round(settlement_delta_bps, 3),
                "won_up": hour_direction == "Up",
                "won_down": hour_direction == "Down",
                "position_direction_at_entry": position_direction,
                "would_win_if_held_to_settle": would_win,
                "positions_held": pos_details,
            })
            # Clean up ended market
            self.market_states.pop(slug, None)
            self.signal_hist.pop(slug, None)
            self.last_book.pop(slug, None)
            self.recent_extreme_price.pop(slug, None)
            self.entry_sm.pop(slug, None)
            self._data_cache.pop(slug, None)
            self._pending_fetches.pop(slug, None)
            self._parity_last_order_ts.pop(slug, None)
            self._parity_invested_usd.pop(slug, None)
            self._parity_locked_since.pop(slug, None)
            self._parity_maker_orders.pop(slug, None)
            self._parity_pending_pairs[:] = [p for p in self._parity_pending_pairs if p["slug"] != slug]
    def _get_markets(self) -> List[MarketRef]:
        # In practice: discover the current hour markets
        return self.client.get_current_hour_markets()
    def _ensure_market_state(self, m: MarketRef):
        if m.slug not in self.market_states:
            idx = self.hour_index_counters.get(m.crypto, 0)
            self.hour_index_counters[m.crypto] = idx + 1
            self.market_states[m.slug] = MarketState(
                slug=m.slug,
                crypto=m.crypto,
                hour_open=m.hour_open,
                hour_start_utc=iso_z(m.hour_start_utc),
                hour_index=idx,
            )
            write_jsonl({"event_type":"NEW_MARKET_STATE", "slug": m.slug, "crypto": m.crypto,
                         "hour_index": idx, "hour_start_utc": iso_z(m.hour_start_utc)})
        # Always ensure companion dicts exist (may be missing after state reload)
        self.signal_hist.setdefault(m.slug, [])
        self.last_book.setdefault(m.slug, {})
        self.recent_extreme_price.setdefault(m.slug, {"Up": None, "Down": None})
    def _make_tick_ctx(self, m: MarketRef, st: MarketState, spot: float, hour_open: float,
                       t_min: float, delta_bps: float, abs_delta_bps: float,
                       vel: Optional[float], z: float, up_book: BookTop, dn_book: BookTop) -> dict:
        """Build shared per-tick context dict used by entries, exits, and logging."""
        effective_delta = abs_delta_bps * (t_min / 60.0)
        normalized_delta = abs_delta_bps / max(st.peak_abs_delta_bps, 1.0)
        # Edge model
        p_up = _p_up_model(delta_bps)
        edge_up = p_up - up_book.mid
        edge_down = (1.0 - p_up) - dn_book.mid
        # Phase / timing
        phase = _phase(t_min)
        seconds_to_close = max(0.0, (60.0 - t_min) * 60.0)
        return {
            "m": m, "st": st, "spot": spot, "hour_open": hour_open,
            "t_min": t_min, "delta_bps": delta_bps, "abs_delta_bps": abs_delta_bps,
            "vel": vel, "z": z,
            "effective_delta": effective_delta, "normalized_delta": normalized_delta,
            "p_up_model": p_up, "edge_up": edge_up, "edge_down": edge_down,
            "phase": phase, "seconds_to_close": seconds_to_close,
            "up_book": up_book, "dn_book": dn_book,
            "drift_dir": "Up" if delta_bps >= 0 else "Down",
            # Hour identity
            "hour_start_utc": st.hour_start_utc,
            "hour_label_et": _hour_label_et(st.hour_start_utc),
            "hour_index": st.hour_index,
        }
    def _book_fields(self, up_book: BookTop, dn_book: BookTop, outcome: Optional[str] = None) -> dict:
        """Return flat dict with up_*/dn_*/ref_* book fields for CSV/JSONL."""
        return build_book_fields(up_book, dn_book, outcome)
    def _step_market_with_data(self, data: dict):
        """Process a market using pre-fetched HTTP data."""
        ts_snapshot = time.time()
        m = data["market"]
        spot, hour_open = data["spot"], data["hour_open"]
        up_book, dn_book = data["up_book"], data["dn_book"]
        st = self.market_states[m.slug]
        now = utc_now()
        hour_start = datetime.strptime(st.hour_start_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        t_min = minutes_into_hour(hour_start, now)
        if t_min >= TRADE_HARD_STOP_MIN:
            self.last_book[m.slug]["Up"] = up_book
            self.last_book[m.slug]["Down"] = dn_book
            self._cleanup_market(m, st, t_min)
            return
        if hour_open <= 0 or spot <= 0:
            self.logger.log_event({"event_type": "SKIP_NO_PRICE", "slug": m.slug,
                                   "crypto": m.crypto, "spot": spot, "hour_open": hour_open})
            return
        st.hour_open = hour_open
        delta_bps = (spot - hour_open) / hour_open * 10000.0
        abs_delta_bps = abs(delta_bps)
        st.peak_abs_delta_bps = max(st.peak_abs_delta_bps, abs_delta_bps)
        # Track edge sign stability for whipsaw filter
        cur_sign = 1 if delta_bps > 0 else (-1 if delta_bps < 0 else 0)
        prev = self._edge_sign_state.get(m.slug)
        if prev is None or prev[0] != cur_sign:
            self._edge_sign_state[m.slug] = (cur_sign, time.time())
        st.delta_hist.append((iso_z(now), delta_bps))
        st.delta_hist = st.delta_hist[-STATE_HIST_MAX:]
        st.price_hist.append((iso_z(now), spot))
        st.price_hist = st.price_hist[-STATE_HIST_MAX:]
        # Lightweight spot history for adverse selection + velocity + regime (epoch-based)
        spot_ts = time.time()
        slug_spot_hist = self._spot_history.setdefault(m.slug, [])
        slug_spot_hist.append((spot_ts, spot))
        # Trim to last 120s (supports 60s regime lookback + 30s velocity lookback)
        cutoff = spot_ts - 120.0
        while slug_spot_hist and slug_spot_hist[0][0] < cutoff:
            slug_spot_hist.pop(0)
        # Track significant spot moves for signal-to-fill latency (clone metrics)
        last_move = self._clone_last_spot_move.get(m.slug)
        if last_move:
            move_bps = abs((spot - last_move[1]) / last_move[1]) * 10000.0 if last_move[1] > 0 else 0.0
            if move_bps >= 5.0:  # significant move = 5+ bps
                self._clone_last_spot_move[m.slug] = (spot_ts, spot)
        else:
            self._clone_last_spot_move[m.slug] = (spot_ts, spot)
        vel = spot_velocity_bps_per_min(slug_spot_hist, lookback_sec=30.0)  # None if insufficient data
        z = zscore(st.delta_hist) if Z_ENTRY_ENABLED else 0.0

        # ── VELOCITY_DIAG: 1/min per slug (temporary debug) ──
        _vd_last = self._vel_diag_last_ts.get(m.slug, 0.0)
        if spot_ts - _vd_last >= 60.0:
            self._vel_diag_last_ts[m.slug] = spot_ts
            _oldest_ts = slug_spot_hist[0][0] if slug_spot_hist else 0.0
            _newest_ts = slug_spot_hist[-1][0] if slug_spot_hist else 0.0
            _spot_old = slug_spot_hist[0][1] if slug_spot_hist else 0.0
            _spot_new = slug_spot_hist[-1][1] if slug_spot_hist else 0.0
            write_jsonl({
                "event_type": "VELOCITY_DIAG",
                "slug": m.slug, "crypto": m.crypto,
                "sample_count": len(slug_spot_hist),
                "oldest_ts": round(_oldest_ts, 3),
                "newest_ts": round(_newest_ts, 3),
                "dt_ms": round((_newest_ts - _oldest_ts) * 1000, 0),
                "spot_old": round(_spot_old, 4),
                "spot_new": round(_spot_new, 4),
                "computed_vel": round(vel, 3) if vel is not None else None,
            })

        self.last_book[m.slug]["Up"] = up_book
        self.last_book[m.slug]["Down"] = dn_book
        self._update_extremes(m.slug, up_book, dn_book)
        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty >= MIN_QTY and pos.entry_mid > 0:
                bk = up_book if outcome == "Up" else dn_book
                pos.max_favorable_mid = max(pos.max_favorable_mid, bk.mid)
                pos.max_adverse_mid = min(pos.max_adverse_mid, bk.mid)
        ctx = self._make_tick_ctx(m, st, spot, hour_open, t_min, delta_bps, abs_delta_bps,
                                  vel, z, up_book, dn_book)
        ctx["ts_snapshot"] = ts_snapshot
        # ── Analytics: track imbalance per slug for CLONE_REPORT ──
        up_q = st.positions["Up"].qty
        dn_q = st.positions["Down"].qty
        abs_imbal = abs(up_q - dn_q)
        cur_max = self._diag_max_imbalance.get(m.slug, 0.0)
        self._diag_max_imbalance[m.slug] = max(cur_max, abs_imbal)
        self._diag_imbalance_delta_samples.append((up_q - dn_q, delta_bps))
        # ── Parity metrics (computed every tick, used by parity arb engine) ──
        ctx["straddle_buy_cost"] = up_book.ask + dn_book.ask
        ctx["straddle_sell_value"] = up_book.bid + dn_book.bid
        ctx["parity_edge_buy_cents"] = (1.000 - ctx["straddle_buy_cost"]) * 100
        ctx["parity_edge_sell_cents"] = (ctx["straddle_sell_value"] - 1.000) * 100
        # Fee-aware net edges (pre-computed for logging even when parity doesn't trade)
        ctx["parity_raw_buy_cents"] = ctx["parity_edge_buy_cents"]
        ctx["parity_raw_sell_cents"] = ctx["parity_edge_sell_cents"]
        _net_buy, _, _ = parity_net_edge_cents(ctx["parity_raw_buy_cents"], up_book, dn_book, True)
        _net_sell, _, _ = parity_net_edge_cents(ctx["parity_raw_sell_cents"], up_book, dn_book, False)
        ctx["parity_net_buy_cents"] = _net_buy
        ctx["parity_net_sell_cents"] = _net_sell
        # ── SNAPSHOT_COMPACT (every SNAPSHOT_INTERVAL_SEC per market) ──
        pos_up_qty = st.positions["Up"].qty
        pos_dn_qty = st.positions["Down"].qty
        equity = self._equity()
        if self.logger.should_log_snapshot_compact(ts_snapshot, m.slug):
            self.logger.log_snapshot_compact(
                slug=m.slug, crypto=m.crypto, t_min=t_min,
                spot=spot, hour_open=hour_open,
                delta_bps=delta_bps, abs_delta_bps=abs_delta_bps,
                vel=vel, z=z,
                up_bid=up_book.bid, up_ask=up_book.ask, up_mid=up_book.mid,
                up_spread=up_book.spread, up_imb=up_book.imb,
                dn_bid=dn_book.bid, dn_ask=dn_book.ask, dn_mid=dn_book.mid,
                dn_spread=dn_book.spread, dn_imb=dn_book.imb,
                pos_qty_up=pos_up_qty, pos_qty_down=pos_dn_qty,
                cash_usdc=self.cash_usdc, equity_usdc=equity,
                parity_edge_buy_cents=ctx["parity_edge_buy_cents"],
                parity_edge_sell_cents=ctx["parity_edge_sell_cents"],
            )
        # ── SNAPSHOT_ON_CHANGE (only when significant changes happen) ──
        snap_dict = {
            "up_mid": up_book.mid, "dn_mid": dn_book.mid,
            "up_spread": up_book.spread, "dn_spread": dn_book.spread,
            "up_imb": up_book.imb, "dn_imb": dn_book.imb,
            "delta_bps": delta_bps,
            "entry_thr_bps": entry_threshold_bps(m.crypto, t_min),
            "pos_qty_up": pos_up_qty, "pos_qty_down": pos_dn_qty,
        }
        should_change, trigger = self.logger.should_log_snapshot_on_change(m.slug, snap_dict)
        if should_change:
            self.logger.log_snapshot_on_change(
                slug=m.slug, crypto=m.crypto, trigger=trigger,
                t_min=t_min, delta_bps=delta_bps,
                up_mid=up_book.mid, up_spread=up_book.spread, up_imb=up_book.imb,
                dn_mid=dn_book.mid, dn_spread=dn_book.spread, dn_imb=dn_book.imb,
                pos_qty_up=pos_up_qty, pos_qty_down=pos_dn_qty,
                cash_usdc=self.cash_usdc, equity_usdc=equity,
            )
        # ── Risk drawdown check (log-only) ──
        risk_fields = self._check_risk_drawdown(ctx)
        ctx.update(risk_fields)
        # ── Update regime awareness ──
        self._update_regime(m.slug)

        # ── EXITS FIRST (all engines) ──
        self._manage_exits(m, st, t_min, delta_bps, ctx)
        if DIRECTIONAL_SCALP_ENABLED:
            self._dscalp_manage_exits(m, st, ctx)
        self._directional_lean_exits(m, st, t_min, delta_bps, ctx)

        # ── PARITY: priority #3 — OFF by default, only runs if PARITY_ENABLED=true ──
        if PARITY_ENABLED:
            parity_blocked = self._should_block_parity(m, st)
            if parity_blocked:
                # Only run parity for pending pairs + recycle + flatten — NO new quotes
                self._parity_arb(ctx, new_quotes_blocked=True)
            else:
                self._parity_arb(ctx)
        else:
            # Even with parity off, still flatten any existing parity inventory
            if any(st.positions[o].qty >= MIN_QTY for o in ["Up", "Down"]):
                if t_min >= PARITY_FLATTEN_START_MIN:
                    self._parity_arb(ctx, new_quotes_blocked=True)

        # stop adding risk after minute 57
        if t_min > TRADE_STOP_ADD_MIN:
            return
        # risk gate
        if not self._risk_ok(st):
            return

        # ── Global trades/min throttle ──
        if self._throttle_exceeded():
            return

        # ── PRIMARY: Directional scalp entries (priority #1) ──
        if DIRECTIONAL_SCALP_ENABLED:
            self._dscalp_entries(ctx)
        # Core engine: drift-direction entries (secondary — only if parity enabled)
        if PARITY_ENABLED and m.slug not in self._dscalp_positions:
            self._core_entries(ctx)
        # Late scalp engine
        if LATE_SCALP_ENABLED and m.slug not in self._dscalp_positions:
            self._late_scalps(ctx)
    def _update_extremes(self, slug: str, up_book: BookTop, dn_book: BookTop):
        # Use mid price extremes to detect pullbacks
        for outcome, book in [("Up", up_book), ("Down", dn_book)]:
            mid = book.mid
            prev = self.recent_extreme_price[slug].get(outcome)
            if prev is None:
                self.recent_extreme_price[slug][outcome] = mid
            else:
                # track extreme in direction of "most likely" (we just keep max for simplicity)
                self.recent_extreme_price[slug][outcome] = max(prev, mid)
    # -----------------------------------------------------------------
    # State machine helpers
    # -----------------------------------------------------------------
    def _get_sm(self, slug: str) -> dict:
        if slug not in self.entry_sm:
            self.entry_sm[slug] = {"state": "IDLE", "probe_ts": None,
                                   "probe_ask": None, "initial_edge_bps": None}
        return self.entry_sm[slug]

    def _sm_transition(self, slug: str, new_state: str, reason: str = "", ctx: dict = None):
        sm = self._get_sm(slug)
        old = sm["state"]
        sm["state"] = new_state
        write_jsonl({"event_type": "SM_TRANSITION", "slug": slug,
                      "from": old, "to": new_state, "reason": reason,
                      "t_min": round(ctx["t_min"], 3) if ctx else 0})

    # =================================================================
    # PARITY SUPPRESSION — priority #3, hard-gated
    # =================================================================
    def _should_block_parity(self, m: MarketRef, st: MarketState) -> bool:
        """Determine if parity quoting should be blocked for this slug.
        Parity is blocked when:
        1. Directional inventory > $25 on this slug
        2. Directional scalp fired recently (standdown timer)
        3. Net imbalance >= threshold
        4. Adverse guard active
        5. Parity fill % exceeds target cap"""
        slug = m.slug

        # 1. Directional inventory exceeds USD threshold
        dscalp_inv = self._dscalp_invested_usd.get(slug, 0.0)
        if dscalp_inv >= PARITY_DSCALP_INV_BLOCK_USD:
            return True

        # 2. Standdown timer: dscalp fired within last N seconds
        last_dscalp = self._dscalp_last_entry_ts.get(slug, 0.0)
        if time.time() - last_dscalp < PARITY_STANDDOWN_AFTER_DSCALP_SEC:
            return True

        # 3. Net imbalance exceeds threshold
        up_q = st.positions["Up"].qty
        dn_q = st.positions["Down"].qty
        if abs(up_q - dn_q) >= PARITY_IMBALANCE_BLOCK_SHARES:
            return True

        # 4. Adverse guard active
        if PARITY_BLOCK_IF_ADVERSE and self._adverse_guard_active(slug):
            return True

        # 5. Parity fill % exceeds target
        total = max(1, self._diag_total_fills_min)
        parity_pct = self._diag_parity_fills_min / total
        if parity_pct > PARITY_MAX_FILL_PCT and self._diag_total_fills_min > 5:
            return True

        return False

    # =================================================================
    # GLOBAL THROTTLE — target trades/min
    # =================================================================
    def _throttle_exceeded(self) -> bool:
        """Check if rolling trades/min exceeds target. If so, block new entries."""
        now = time.time()
        cutoff = now - THROTTLE_LOOKBACK_SEC
        # Trim old entries
        self._rolling_trade_ts = [ts for ts in self._rolling_trade_ts if ts > cutoff]
        trades_in_window = len(self._rolling_trade_ts)
        trades_per_min = trades_in_window / (THROTTLE_LOOKBACK_SEC / 60.0)
        return trades_per_min > TARGET_TRADES_PER_MIN

    def _throttle_record_trade(self):
        """Record a trade for the rolling trades/min window."""
        self._rolling_trade_ts.append(time.time())

    # =================================================================
    # REGIME AWARENESS — volatility-adaptive activity
    # =================================================================
    WARMUP_BYPASS_SEC = 20.0  # seconds after bootstrap/hour-roll to bypass low-vol gating

    def _in_warmup(self) -> bool:
        """Return True if we're within WARMUP_BYPASS_SEC of bootstrap or hour roll."""
        now = time.time()
        since_bootstrap = now - self._bootstrap_done_ts if self._bootstrap_done_ts > 0 else 999.0
        since_hour_roll = now - self._last_hour_roll_ts if self._last_hour_roll_ts > 0 else 999.0
        return min(since_bootstrap, since_hour_roll) < self.WARMUP_BYPASS_SEC

    def _update_regime(self, slug: str):
        """Compute rolling 60s spot volatility and determine regime."""
        # During warm-up, force non-low-vol so quoting/entries can start
        if self._in_warmup():
            self._regime_is_low_vol[slug] = False
            return
        spot_hist = self._spot_history.get(slug, [])
        if len(spot_hist) < 5:
            self._regime_is_low_vol[slug] = False
            return

        now = time.time()
        cutoff = now - REGIME_VOL_LOOKBACK_SEC
        recent = [s for ts, s in spot_hist if ts > cutoff]
        if len(recent) < 3:
            self._regime_is_low_vol[slug] = False
            return

        # Compute returns in bps
        returns_bps = []
        for i in range(1, len(recent)):
            if recent[i - 1] > 0:
                ret = (recent[i] - recent[i - 1]) / recent[i - 1] * 10000.0
                returns_bps.append(ret)

        if len(returns_bps) < 2:
            self._regime_is_low_vol[slug] = False
            return

        mean_r = sum(returns_bps) / len(returns_bps)
        var_r = sum((r - mean_r) ** 2 for r in returns_bps) / len(returns_bps)
        std_bps = var_r ** 0.5
        self._regime_vol_bps[slug] = std_bps
        self._regime_is_low_vol[slug] = std_bps < REGIME_LOW_VOL_THRESHOLD

    def _regime_activity_mult(self, slug: str) -> float:
        """Return activity multiplier based on regime. 1.0 = full, 0.5 = reduced."""
        if self._regime_is_low_vol.get(slug, False):
            return REGIME_LOW_VOL_REDUCTION
        return 1.0

    # =================================================================
    # RANGE PERCENTILE — bottom 30% of 5m range
    # =================================================================
    def _is_price_in_cheap_range(self, slug: str, outcome: str, ask_price: float) -> bool:
        """Check if current ask is in bottom DSCALP_RANGE_PERCENTILE of 5-min range."""
        spot_hist = self._spot_history.get(slug, [])
        if len(spot_hist) < 3:
            return False
        now = time.time()
        cutoff = now - DSCALP_RANGE_LOOKBACK_SEC
        recent = [s for ts, s in spot_hist if ts > cutoff]
        if len(recent) < 3:
            return False
        hi = max(recent)
        lo = min(recent)
        rng = hi - lo
        if rng < 0.001:
            return False  # flat market
        # For "Up" outcome, cheap = low price. For "Down", cheap = high spot (inverse).
        # But on Polymarket binary, price of outcome moves with spot.
        # Up price is low when spot is low (cheap entry for Up = bottom of range)
        # Down price is low when spot is high (cheap entry for Down = top of range)
        if outcome == "Up":
            threshold = lo + rng * DSCALP_RANGE_PERCENTILE
            return ask_price <= threshold
        else:
            # For Down, spot being high means Down is cheap
            threshold = hi - rng * DSCALP_RANGE_PERCENTILE
            # Current spot
            cur_spot = recent[-1] if recent else 0
            return cur_spot >= threshold

    def _spot_move_10s_bps(self, slug: str) -> float:
        """Absolute spot move over the last 10 seconds, in bps."""
        spot_hist = self._spot_history.get(slug, [])
        if len(spot_hist) < 2:
            return 0.0
        now = time.time()
        cutoff = now - 10.0
        recent = [(ts, s) for ts, s in spot_hist if ts > cutoff]
        if len(recent) < 2:
            return 0.0
        first_spot = recent[0][1]
        last_spot = recent[-1][1]
        if first_spot <= 0:
            return 0.0
        return abs(last_spot - first_spot) / first_spot * 10000.0

    def _feed_disagreement_bps(self, slug: str) -> float:
        """Disagreement between Binance and Chainlink feeds in bps.
        Returns 0 if feeds not available (allows trading)."""
        cached = self._data_cache.get(slug)
        if not cached:
            return 0.0
        binance_spot = cached.get("binance_spot") or cached.get("spot", 0.0)
        chainlink_spot = cached.get("chainlink_spot", 0.0)
        if binance_spot <= 0 or chainlink_spot <= 0:
            return 0.0  # missing feed — don't block
        return abs(binance_spot - chainlink_spot) / binance_spot * 10000.0

    def _max_feed_age_sec(self, slug: str) -> float:
        """Max age of any data feed for this slug, in seconds.
        Returns 0 if no staleness info available (allows trading)."""
        cached = self._data_cache.get(slug)
        if not cached:
            return 0.0
        feed_ts = cached.get("feed_ts") or cached.get("last_update_ts", 0.0)
        if feed_ts <= 0:
            return 0.0
        return max(0.0, time.time() - feed_ts)

    # =================================================================
    # RATE LIMITER — hard per-slug throttling
    # =================================================================
    def _rate_limit_ok(self, slug: str) -> bool:
        """Check if we can submit another order for this slug.
        Returns True if OK, False if blocked."""
        if not RATE_LIMIT_ENABLED:
            return True
        now = time.time()
        now_ms = now * 1000

        # Check MIN_ORDER_INTERVAL_MS
        last_ts = self._rate_last_order_ts.get(slug, 0.0)
        if (now_ms - last_ts * 1000) < MIN_ORDER_INTERVAL_MS:
            self._rate_blocked_interval += 1
            return False

        # Check MAX_ORDER_SUBMITS_PER_MIN
        window_start = self._rate_submit_window_start.get(slug, now)
        if now - window_start > 60.0:
            # Reset window
            self._rate_submit_window_start[slug] = now
            self._rate_submit_count[slug] = 0
        count = self._rate_submit_count.get(slug, 0)
        if count >= MAX_ORDER_SUBMITS_PER_MIN:
            self._rate_blocked_cap += 1
            return False

        return True

    def _rate_limit_record(self, slug: str):
        """Record an order submission for rate limiting."""
        now = time.time()
        self._rate_last_order_ts[slug] = now
        self._rate_submit_count[slug] = self._rate_submit_count.get(slug, 0) + 1
        self._true_cost_submit_count += 1

    def _adverse_guard_active(self, slug: str) -> bool:
        """Check if adverse selection guard is currently blocking for this slug."""
        paused_until = self._quote_paused_until.get(slug, 0.0)
        degraded_until = self._quote_degraded_until.get(slug, 0.0)
        now = time.time()
        return now < paused_until or now < degraded_until

    # =================================================================
    # GATE_BREAKDOWN — per-slug gate counters + GATE_REPORT
    # =================================================================
    def _gate_inc(self, slug: str, reason: str):
        """Increment gate-blocked counter for a slug."""
        bucket = self._gate_counters.setdefault(slug, {})
        bucket[reason] = bucket.get(reason, 0) + 1

    def _maybe_emit_gate_report(self):
        """Emit GATE_REPORT every 60s with per-slug gate breakdown."""
        now = time.time()
        if now - self._gate_report_last_ts < 60.0:
            return
        self._gate_report_last_ts = now
        if not self._gate_counters:
            return
        per_slug = {}
        for slug, reasons in self._gate_counters.items():
            total = sum(reasons.values())
            top3 = sorted(reasons.items(), key=lambda x: -x[1])[:3]
            per_slug[slug] = {"total_blocked": total,
                              "top3": {k: v for k, v in top3}}
        write_jsonl({"event_type": "GATE_REPORT",
                      "ts_ms": int(now * 1000),
                      "per_slug": per_slug})
        # Console summary
        top_slugs = sorted(per_slug.items(), key=lambda x: -x[1]["total_blocked"])[:3]
        parts = []
        for s, d in top_slugs:
            reasons_s = ",".join(f"{k}={v}" for k, v in d["top3"].items())
            parts.append(f"{s}({d['total_blocked']}): {reasons_s}")
        if parts:
            print(f"  GATE: {' | '.join(parts)}")
        # Reset for next window
        self._gate_counters = {}

    # =================================================================
    # DIRECTIONAL SCALP MODE — F247-style momentum entries
    # =================================================================
    def _dscalp_entries(self, ctx: dict):
        """Directional scalp entry (PRIMARY engine, F247-style).
        Entry requires: (delta >= 15bps OR spot_move_10s >= 8bps)
        AND spread <= 2c AND cache_age <= 250ms. Min $6 per entry."""
        m, st = ctx["m"], ctx["st"]
        t_min = ctx["t_min"]
        delta_bps = ctx["delta_bps"]
        abs_delta_bps = ctx["abs_delta_bps"]
        vel = ctx["vel"]
        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        now_t = time.time()

        # Cooldown (4s default = ~15 trades/min max across all slugs)
        last_entry = self._dscalp_last_entry_ts.get(m.slug, 0.0)
        if (now_t - last_entry) * 1000 < DSCALP_COOLDOWN_MS:
            self._gate_inc(m.slug, "cooldown")
            return

        # Already at max position for this slug
        invested = self._dscalp_invested_usd.get(m.slug, 0.0)
        if invested >= DSCALP_MAX_USD_PER_SLUG:
            self._gate_inc(m.slug, "max_position")
            return

        # Rate limit
        if not self._rate_limit_ok(m.slug):
            self._gate_inc(m.slug, "rate_limit")
            return

        # Regime awareness: reduce activity in low-vol
        activity_mult = self._regime_activity_mult(m.slug)
        if activity_mult < 1.0:
            # In low-vol, randomly skip entries proportional to reduction
            if random.random() > activity_mult:
                self._gate_inc(m.slug, "low_vol")
                return

        # Direction: follow the drift
        outcome = ctx["drift_dir"]
        book = up_book if outcome == "Up" else dn_book

        # ── HARD GATES (must pass ALL) ──

        # Cache freshness
        cache_age = self._cache_age_ms(m.slug)
        if cache_age > DSCALP_MAX_CACHE_AGE_MS:
            self._gate_inc(m.slug, "stale_cache")
            return

        # Spread gate
        spread_cents = book.spread * 100
        if spread_cents > DSCALP_MAX_SPREAD_CENTS:
            self._gate_inc(m.slug, "spread")
            return

        # Edge filter: require minimum directional edge (cents) for this coin
        # edge = abs_delta_bps * mid_price / 100  (convert bps of price to cents of token)
        # More practically: half-spread + expected move must justify TP target
        crypto_lower = (m.crypto or "").lower()
        if "sol" in crypto_lower:
            min_edge = DSCALP_MIN_EDGE_CENTS_SOL
        elif "xrp" in crypto_lower:
            min_edge = DSCALP_MIN_EDGE_CENTS_XRP
        elif "btc" in crypto_lower:
            min_edge = DSCALP_MIN_EDGE_CENTS_BTC
        else:
            min_edge = DSCALP_MIN_EDGE_CENTS
        # Edge estimate: directional signal strength in token cents
        # For tokens priced 0.30-0.70, 1 bps of price ≈ 0.005c, so 15 bps ≈ 0.75c
        # We use a more direct measure: abs_delta_bps as the signal strength
        # and also factor in the book: if bid is well below mid, more edge
        half_spread_cents = spread_cents / 2.0
        signal_edge_cents = abs_delta_bps * book.mid / 100.0  # convert bps to cents
        effective_edge_cents = signal_edge_cents - half_spread_cents  # net of half-spread cost
        if effective_edge_cents < min_edge:
            self._gate_inc(m.slug, "edge")
            return

        # No-trade zone: block if data feeds disagree or are stale
        _feed_delta = self._feed_disagreement_bps(m.slug) if hasattr(self, '_feed_disagreement_bps') else 0.0
        if _feed_delta > DSCALP_FEED_DISAGREE_BPS:
            self._gate_inc(m.slug, "feed_disagree")
            return
        _feed_age = self._max_feed_age_sec(m.slug) if hasattr(self, '_max_feed_age_sec') else 0.0
        if _feed_age > DSCALP_FEED_STALE_SEC:
            self._gate_inc(m.slug, "feed_stale")
            return

        # ── ENTRY SIGNAL: delta >= 15bps OR spot moved >= 8bps in 10s ──
        delta_ok = abs_delta_bps >= DSCALP_DELTA_MIN_BPS
        spot_move_ok = self._spot_move_10s_bps(m.slug) >= DSCALP_SPOT_MOVE_10S_BPS
        if not delta_ok and not spot_move_ok:
            self._gate_inc(m.slug, "delta")
            return

        # Velocity must be supportive (agree with direction)
        # If vel is None (unknown/warming up), skip this gate — allow entry
        if vel is not None:
            if outcome == "Up" and vel < DSCALP_VEL_MIN_BPS_PER_MIN:
                self._gate_inc(m.slug, "velocity")
                return
            if outcome == "Down" and vel > -DSCALP_VEL_MIN_BPS_PER_MIN:
                self._gate_inc(m.slug, "velocity")
                return

        # Size: $5-10 per entry, no micro-splits
        remaining = DSCALP_MAX_USD_PER_SLUG - invested
        step_usd = min(DSCALP_STEP_USD, remaining)
        if step_usd < DSCALP_STEP_USD_MIN:
            self._gate_inc(m.slug, "size")
            return  # don't enter with less than minimum size

        # Place maker buy
        buy_price = book.bid  # maker at best bid
        if buy_price <= 0.01:
            return

        order_qty = step_usd / buy_price
        if order_qty < 1:
            return

        # Use existing buy infrastructure
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        pos = st.positions[outcome]
        decision_id = new_decision_id()
        client_oid = new_order_id()
        if pos.position_id is None:
            pos.position_id = new_position_id()
            pos.trade_id = uuid.uuid4().hex
            pos.entry_decision_id = decision_id
            pos.parent_order_id = client_oid
            pos.entry_mid = book.mid
            pos.max_favorable_mid = book.mid

        # Log intent with signal type
        entry_signal = "delta" if delta_ok else "spot_move"
        regime = "low_vol" if self._regime_is_low_vol.get(m.slug, False) else "normal"
        write_jsonl({"event_type": "DSCALP_ENTRY",
                      "ts_ms": int(now_t * 1000),
                      "slug": m.slug, "crypto": m.crypto,
                      "outcome": outcome,
                      "entry_signal": entry_signal,
                      "regime": regime,
                      "price": round(buy_price, 4),
                      "qty": round(order_qty, 1),
                      "step_usd": round(step_usd, 2),
                      "delta_bps": round(delta_bps, 1),
                      "vel": round(vel, 2) if vel is not None else None,
                      "cache_age_ms": round(cache_age, 0),
                      "spread_cents": round(spread_cents, 2),
                      "edge_cents": round(effective_edge_cents, 2),
                      "min_edge_cents": min_edge})

        # CSV: ORDER_INTENT + ORDER_SUBMIT
        _bk_fields = build_book_fields(ctx["up_book"], ctx["dn_book"], outcome)
        self.logger.log_order_intent(
            engine="DSCALP", reason="DSCALP_ENTRY",
            decision_id=decision_id, position_id=pos.position_id,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=order_qty, target_price=buy_price,
            usdc_cost=step_usd, ctx=ctx, book_fields=_bk_fields,
        )
        self.logger.log_order_submit(
            engine="DSCALP", reason="DSCALP_ENTRY",
            decision_id=decision_id, position_id=pos.position_id,
            client_order_id=client_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=order_qty, target_price=buy_price,
            usdc_cost=step_usd, ctx=ctx, book_fields=_bk_fields,
        )

        # Execute buy (unified: paper in LOG, live order manager in LIVE)
        buy_result = self._exec_buy(st, m, outcome, buy_price, order_qty,
                                     reason="DSCALP_ENTRY", prefer_maker=True, ctx=ctx)
        if not buy_result.get("filled"):
            # Order resting or failed — reconciliation loop handles fills later
            # Still record rate-limit to prevent spam
            self._rate_limit_record(m.slug)
            if buy_result.get("order_id"):
                write_jsonl({"event_type": "DSCALP_ENTRY_RESTING",
                              "slug": m.slug, "outcome": outcome,
                              "order_id": buy_result["order_id"],
                              "price": buy_price, "qty": round(order_qty, 1)})
            return
        actual_cost = buy_result["usdc_cost"]
        actual_qty = buy_result["fill_qty"]
        actual_price = buy_result["fill_price"]
        self._rate_limit_record(m.slug)
        self._throttle_record_trade()
        # Counters updated by _om_submit_order/_exec_buy for LIVE; manual for LOG
        if MODE == "LOG":
            self._true_cost_fill_count += 1
            self._true_cost_fill_count_min += 1
            self._true_cost_tx_count += 1
        self._diag_directional_fills_min += 1
        self._diag_total_fills_min += 1
        self._diag_trade_sizes.append(actual_cost)

        # Track directional scalp position
        existing_qty = self._dscalp_positions.get(m.slug, {}).get("qty", 0)
        existing_entry = self._dscalp_positions.get(m.slug, {}).get("entry_price", actual_price)
        # VWAP the entry price if adding to existing position
        total_qty = actual_qty + existing_qty
        vwap_price = ((existing_entry * existing_qty + actual_price * actual_qty) / total_qty
                      if total_qty > 0 else actual_price)
        self._dscalp_positions[m.slug] = {
            "outcome": outcome,
            "entry_price": vwap_price,
            "entry_ts": self._dscalp_positions.get(m.slug, {}).get("entry_ts", now_t),
            "qty": total_qty,
            "tp1_done": self._dscalp_positions.get(m.slug, {}).get("tp1_done", False),
            "tp2_done": self._dscalp_positions.get(m.slug, {}).get("tp2_done", False),
            "tp3_done": self._dscalp_positions.get(m.slug, {}).get("tp3_done", False),
        }
        self._dscalp_invested_usd[m.slug] = invested + actual_cost
        self._dscalp_last_entry_ts[m.slug] = now_t
        self._diag_dscalp_entries += 1
        st.last_entry_ts = iso_z(utc_now())

        _vel_s = f"{vel:.1f}" if vel is not None else "None"
        self.logger.log_order_fill(
            engine="DSCALP", reason="DSCALP_ENTRY",
            decision_id=decision_id, client_order_id=client_oid,
            position_id=pos.position_id,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=actual_qty,
            fill_price=actual_price, usdc_cost=actual_cost,
            maker_taker="maker",
            ctx=ctx,
            book_fields=build_book_fields(ctx["up_book"], ctx["dn_book"], outcome),
            extra={"notes": f"dscalp_entry vel={_vel_s}"},
        )

    def _record_exit_reason(self, reason: str, pnl_cents: float, hold_sec: float,
                            usdc_size: float = 0.0, slug: str = ""):
        """Record an exit for per-hour PnL attribution + per-slug rolling PnL."""
        self._diag_exit_by_reason.setdefault(reason, []).append((pnl_cents, hold_sec, usdc_size))
        # Track per-slug realized PnL for auto-disable
        if slug and SLUG_AUTO_DISABLE_ENABLED:
            pnl_usd = pnl_cents * usdc_size / 100.0 if usdc_size > 0 else 0.0
            self._slug_realized_pnl_window.setdefault(slug, []).append((time.time(), pnl_usd))

    def _dscalp_manage_exits(self, m: MarketRef, st: MarketState, ctx: dict):
        """Manage directional scalp exits: TP ladder + timeout + stop loss.
        ENFORCES 120s minimum hold unless emergency (stop loss).
        TP ladder: +3c/+6c/+9c at 25% each, remainder for timeout/trailing."""
        dpos = self._dscalp_positions.get(m.slug)
        if dpos is None:
            return

        now_t = time.time()
        outcome = dpos["outcome"]
        entry_price = dpos["entry_price"]
        entry_ts = dpos["entry_ts"]
        hold_sec = now_t - entry_ts

        book = ctx["up_book"] if outcome == "Up" else ctx["dn_book"]
        pos = st.positions[outcome]
        if pos.qty < MIN_QTY:
            # Position gone — cleanup
            self._dscalp_positions.pop(m.slug, None)
            self._dscalp_invested_usd.pop(m.slug, None)
            return

        current_mid = book.mid
        pnl_cents = (current_mid - entry_price) * 100

        # ── Stop loss (EMERGENCY ONLY — bypasses min hold) ──
        if pnl_cents <= -DSCALP_STOP_LOSS_CENTS and hold_sec > 10:
            sell_qty = pos.qty
            sell_price = max(book.bid, 0.01)
            self._do_sell(m, st, outcome, sell_qty, sell_price,
                          reason="DSCALP_STOP", leg="DSCALP", ctx=ctx, use_maker=False)
            self._diag_dscalp_stop_exits += 1
            self._diag_dscalp_hold_times.append(hold_sec)
            self._diag_dscalp_exit_cents.append(pnl_cents)
            self._diag_dscalp_exits += 1
            self._diag_directional_fills_min += 1
            self._diag_total_fills_min += 1
            # tx/fill counters now handled by _do_sell -> _exec_sell -> _om_submit_order
            self._throttle_record_trade()
            self._dscalp_positions.pop(m.slug, None)
            self._dscalp_invested_usd.pop(m.slug, None)
            self._record_exit_reason("stop_loss", pnl_cents, hold_sec, sell_qty * sell_price, slug=m.slug)
            write_jsonl({"event_type": "DSCALP_STOP", "ts_ms": int(now_t * 1000),
                          "slug": m.slug, "outcome": outcome,
                          "pnl_cents": round(pnl_cents, 2), "hold_sec": round(hold_sec, 1)})
            return

        # ── MINIMUM HOLD FLOOR: 120s unless emergency (stop loss above) ──
        if hold_sec < DSCALP_MIN_HOLD_SEC:
            return  # allow real directional exposure — no early exit

        # ── Breakeven exit: after 300s if not green, try maker at entry+1c ──
        if (hold_sec >= DSCALP_BREAKEVEN_AFTER_SEC
                and pnl_cents < DSCALP_TP1_CENTS
                and pnl_cents > -DSCALP_STOP_LOSS_CENTS
                and not dpos.get("breakeven_attempted")):
            be_price = entry_price + DSCALP_BREAKEVEN_CENTS / 100.0
            if book.bid >= be_price:
                # Can exit at breakeven+1c — do it
                sell_qty = pos.qty
                self._do_sell(m, st, outcome, sell_qty, be_price,
                              reason="DSCALP_BREAKEVEN", leg="DSCALP", ctx=ctx, use_maker=True)
                self._diag_dscalp_exits += 1
                self._diag_dscalp_hold_times.append(hold_sec)
                self._diag_dscalp_exit_cents.append(pnl_cents)
                self._diag_directional_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._dscalp_positions.pop(m.slug, None)
                self._dscalp_invested_usd.pop(m.slug, None)
                self._diag_dscalp_breakeven_exits += 1
                self._record_exit_reason("breakeven", pnl_cents, hold_sec, sell_qty * be_price, slug=m.slug)
                write_jsonl({"event_type": "DSCALP_BREAKEVEN", "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "outcome": outcome,
                              "pnl_cents": round(pnl_cents, 2), "hold_sec": round(hold_sec, 1),
                              "be_price": round(be_price, 4)})
                return
            # Mark attempted so we don't keep trying every tick
            dpos["breakeven_attempted"] = True

        # ── Timeout exit ──
        if hold_sec >= DSCALP_MAX_HOLD_SEC:
            sell_qty = pos.qty
            sell_price = max(book.bid, 0.01)
            self._do_sell(m, st, outcome, sell_qty, sell_price,
                          reason="DSCALP_TIMEOUT", leg="DSCALP", ctx=ctx, use_maker=True)
            self._diag_dscalp_timeout_exits += 1
            self._diag_dscalp_exits += 1
            self._diag_dscalp_hold_times.append(hold_sec)
            self._diag_dscalp_exit_cents.append(pnl_cents)
            self._diag_directional_fills_min += 1
            self._diag_total_fills_min += 1
            # tx/fill counters now handled by _do_sell -> _exec_sell -> _om_submit_order
            self._throttle_record_trade()
            self._dscalp_positions.pop(m.slug, None)
            self._dscalp_invested_usd.pop(m.slug, None)
            self._record_exit_reason("timeout", pnl_cents, hold_sec, sell_qty * sell_price, slug=m.slug)
            write_jsonl({"event_type": "DSCALP_TIMEOUT", "ts_ms": int(now_t * 1000),
                          "slug": m.slug, "outcome": outcome,
                          "pnl_cents": round(pnl_cents, 2), "hold_sec": round(hold_sec, 1)})
            return

        # ── TP1: +3c, sell 25% ──
        if pnl_cents >= DSCALP_TP1_CENTS and not dpos["tp1_done"]:
            sell_qty = min(pos.qty * DSCALP_TP1_FRAC, pos.qty)
            if sell_qty >= MIN_QTY:
                sell_price = book.bid
                self._do_sell(m, st, outcome, sell_qty, sell_price,
                              reason="DSCALP_TP1", leg="DSCALP", ctx=ctx, use_maker=True)
                self._diag_dscalp_tp1 += 1
                self._diag_dscalp_exits += 1
                self._diag_directional_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._diag_trade_sizes.append(sell_qty * sell_price)
                self._record_exit_reason("scalp_tp1", pnl_cents, hold_sec, sell_qty * sell_price, slug=m.slug)
                write_jsonl({"event_type": "DSCALP_TP1", "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "outcome": outcome,
                              "pnl_cents": round(pnl_cents, 2), "sell_qty": round(sell_qty, 1),
                              "hold_sec": round(hold_sec, 1)})
            dpos["tp1_done"] = True

        # ── TP2: +6c, sell 30% ──
        if pnl_cents >= DSCALP_TP2_CENTS and not dpos["tp2_done"]:
            sell_qty = min(pos.qty * DSCALP_TP2_FRAC, pos.qty)
            if sell_qty >= MIN_QTY:
                sell_price = book.bid
                self._do_sell(m, st, outcome, sell_qty, sell_price,
                              reason="DSCALP_TP2", leg="DSCALP", ctx=ctx, use_maker=True)
                self._diag_dscalp_tp2 += 1
                self._diag_dscalp_exits += 1
                self._diag_directional_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._diag_trade_sizes.append(sell_qty * sell_price)
                self._record_exit_reason("scalp_tp2", pnl_cents, hold_sec, sell_qty * sell_price, slug=m.slug)
                write_jsonl({"event_type": "DSCALP_TP2", "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "outcome": outcome,
                              "pnl_cents": round(pnl_cents, 2), "sell_qty": round(sell_qty, 1),
                              "hold_sec": round(hold_sec, 1)})
            dpos["tp2_done"] = True

        # ── TP3: +8c, sell 40% — full exit ──
        if pnl_cents >= DSCALP_TP3_CENTS and not dpos.get("tp3_done"):
            sell_qty = min(pos.qty * DSCALP_TP3_FRAC, pos.qty)
            if sell_qty >= MIN_QTY:
                sell_price = book.bid
                self._do_sell(m, st, outcome, sell_qty, sell_price,
                              reason="DSCALP_TP3", leg="DSCALP", ctx=ctx, use_maker=True)
                self._diag_dscalp_tp3 += 1
                self._diag_dscalp_exits += 1
                self._diag_directional_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._diag_trade_sizes.append(sell_qty * sell_price)
                self._record_exit_reason("scalp_tp3", pnl_cents, hold_sec, sell_qty * sell_price, slug=m.slug)
                write_jsonl({"event_type": "DSCALP_TP3", "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "outcome": outcome,
                              "pnl_cents": round(pnl_cents, 2), "sell_qty": round(sell_qty, 1),
                              "hold_sec": round(hold_sec, 1)})
            dpos["tp3_done"] = True

    def _core_entries(self, ctx: dict):
        m, st = ctx["m"], ctx["st"]
        t_min = ctx["t_min"]
        delta_bps, abs_delta_bps = ctx["delta_bps"], ctx["abs_delta_bps"]
        vel, z = ctx["vel"], ctx["z"]
        up_book, dn_book = ctx["up_book"], ctx["dn_book"]
        spot = ctx["spot"]
        sm = self._get_sm(m.slug)

        # --- Build valid signal (time + coin-specific threshold + dynamic cap) ---
        thr = entry_threshold_bps(m.crypto, t_min)
        edge_bps = abs(ctx["edge_up"] if ctx["drift_dir"] == "Up" else ctx["edge_down"]) * 10000.0
        cap = dynamic_cap(t_min, edge_bps)
        valid_time = (t_min >= TRADE_START_MIN)
        valid_delta = (abs_delta_bps >= thr)
        valid_z = (not Z_ENTRY_ENABLED) or (abs(z) >= Z_ENTRY_MIN)
        outcome = ctx["drift_dir"]
        book = up_book if outcome == "Up" else dn_book
        valid_price = (book.ask <= cap)
        # Spread: relaxed during PROBING/SCALING
        in_burst = sm["state"] in ("PROBING", "SCALING")
        max_spread = spread_limit(t_min, edge_bps, m.crypto, in_burst=in_burst)
        valid_spread = (book.spread <= max_spread)
        valid_imb = (not IMB_ENABLED) or (book.imb >= IMB_MIN)
        valid_pullback = True
        if PULLBACK_ENABLED:
            extreme = self.recent_extreme_price[m.slug].get(outcome)
            if extreme is not None:
                valid_pullback = (book.mid <= extreme - PULLBACK_CENTS) or (t_min > 45)
        sig = bool(valid_time and valid_delta and valid_z and valid_price and valid_spread and valid_imb and valid_pullback)

        # Persistence tracking
        sh = self.signal_hist.setdefault(m.slug, [])
        sh.append((iso_z(utc_now()), sig))
        sh[:] = sh[-500:]
        persist_ok = persistence_ok(sh)

        # Cooldown check — dynamic per coin/time
        cooldown_active = False
        if sm["state"] == "COOLDOWN":
            cooldown_active = True
        elif st.last_entry_ts:
            last = datetime.strptime(st.last_entry_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            cd = entry_cooldown_sec(m.crypto, t_min)
            cooldown_active = (utc_now() - last).total_seconds() < cd
        # Exit COOLDOWN state when cooldown expires
        if sm["state"] == "COOLDOWN" and st.last_entry_ts:
            last = datetime.strptime(st.last_entry_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            cd = entry_cooldown_sec(m.crypto, t_min)
            if (utc_now() - last).total_seconds() >= cd:
                self._sm_transition(m.slug, "IDLE", "cooldown_expired", ctx)
                cooldown_active = False

        risk_blocked = not self._risk_ok(st)

        # Sizing
        mult = sizing_mult(abs_delta_bps)
        clip = self._calc_clip(m.crypto, t_min, abs_delta_bps)
        if vel is not None and mult >= 2.0 and vel < MIN_DELTA_VEL_BPS_PER_MIN:
            clip *= 0.6
        if CORR_SCALE_ENABLED and BTC_LEAD and m.crypto != "BTC":
            btc_cost = self._crypto_cost_usdc("BTC")
            if btc_cost > 0:
                reduce = clamp(btc_cost / (self.cash_usdc * MAX_COST_PER_CRYPTO_PCT), 0.0, 1.0)
                clip *= (1.0 - BTC_EXPOSURE_REDUCE_OTHERS * reduce)

        # ---- Whipsaw / anti-chop filter ----
        whipsaw_blocked = False
        whipsaw_reason = ""
        if sig and persist_ok and not cooldown_active and not risk_blocked:
            edge_sign_entry = self._edge_sign_state.get(m.slug)
            ws_since = edge_sign_entry[1] if edge_sign_entry else None
            ws_ok, ws_reason = whipsaw_ok(delta_bps, vel, ws_since)
            if not ws_ok:
                whipsaw_blocked = True
                whipsaw_reason = ws_reason
                self._diag_blocked_whipsaw += 1

        # ---- No-flip rule: block immediate direction reversal ----
        noflip_blocked = False
        noflip_reason = ""
        if sig and persist_ok and not cooldown_active and not risk_blocked and not whipsaw_blocked:
            prev_dir = self._last_trade_direction.get(m.slug)
            if prev_dir is not None:
                prev_outcome, prev_ts = prev_dir
                if prev_outcome != outcome and (time.time() - prev_ts) < NO_FLIP_COOLDOWN_SEC:
                    if abs_delta_bps < thr + NO_FLIP_OVERRIDE_EXTRA_BPS:
                        noflip_blocked = True
                        noflip_reason = f"noflip({prev_outcome}->{outcome},{time.time()-prev_ts:.1f}s)"
                        self._diag_blocked_noflip += 1

        # ---- DECISION event ----
        will_trade = (sig and persist_ok and not cooldown_active and not risk_blocked
                      and not whipsaw_blocked and not noflip_blocked and clip >= MIN_ORDER_USDC)
        if will_trade:
            self._tempo_intents[m.slug] = self._tempo_intents.get(m.slug, 0) + 1
        skip_reason = ""
        if not will_trade:
            reasons = []
            # Track gate breakdown for _core_entries too
            if not valid_time: self._gate_inc(m.slug, "time_gate")
            if not valid_delta: self._gate_inc(m.slug, "delta")
            if sig and not persist_ok: self._gate_inc(m.slug, "persistence")
            if sig and cooldown_active: self._gate_inc(m.slug, "cooldown")
            if sig and risk_blocked: self._gate_inc(m.slug, "risk")
            if whipsaw_blocked: self._gate_inc(m.slug, "whipsaw")
            if noflip_blocked: self._gate_inc(m.slug, "noflip")
            if not valid_time: reasons.append("time")
            if not valid_delta: reasons.append(f"delta({abs_delta_bps:.1f}<{thr})")
            if not valid_z: reasons.append("zscore")
            if not valid_price: reasons.append(f"price({book.ask:.3f}>{cap:.3f})")
            if not valid_spread: reasons.append(f"spread({book.spread:.3f}>{max_spread:.3f})")
            if not valid_imb: reasons.append("imb")
            if not valid_pullback: reasons.append("pullback")
            if sig and not persist_ok: reasons.append("persistence")
            if sig and cooldown_active: reasons.append(f"cooldown({entry_cooldown_sec(m.crypto, t_min):.0f}s)")
            if sig and risk_blocked: reasons.append("risk_cap")
            if whipsaw_blocked: reasons.append(f"whipsaw({whipsaw_reason})")
            if noflip_blocked: reasons.append(noflip_reason)
            if sig and persist_ok and not cooldown_active and not risk_blocked and clip < MIN_ORDER_USDC:
                reasons.append(f"clip_too_small({clip:.2f})")
            skip_reason = "|".join(reasons)

        # ENTRY_INTENT: signal present but trade blocked — show exact gate
        if sig and not will_trade:
            _cd_remaining = 0.0
            if st.last_entry_ts:
                try:
                    _last = datetime.strptime(st.last_entry_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    _cd_remaining = max(0, entry_cooldown_sec(m.crypto, t_min) - (utc_now() - _last).total_seconds()) * 1000
                except Exception:
                    pass
            write_jsonl({
                "event_type": "ENTRY_INTENT",
                "slug": m.slug, "crypto": m.crypto, "outcome": outcome,
                "delta_bps": round(delta_bps, 2),
                "abs_delta_bps": round(abs_delta_bps, 2),
                "thr_used": round(thr, 1),
                "persist_ok": persist_ok,
                "persist_sec": PERSISTENCE_SEC,
                "cooldown_active": cooldown_active,
                "cooldown_remaining_ms": round(_cd_remaining, 0),
                "risk_blocked": risk_blocked,
                "whipsaw_blocked": whipsaw_blocked,
                "noflip_blocked": noflip_blocked,
                "cache_age_ms": round(self._cache_age_ms(m.slug), 0),
                "spread_cents": round(book.spread * 100, 2),
                "vel": round(vel, 2) if vel is not None else None,
                "clip": round(clip, 2),
                "skip_reason": skip_reason,
            })

        sig_dict = {
            "outcome": outcome, "will_trade": will_trade,
            "valid_time": valid_time, "valid_delta": valid_delta,
            "valid_price": valid_price, "valid_spread": valid_spread,
            "valid_imb": valid_imb, "persist_ok": persist_ok,
            "cooldown": cooldown_active, "risk": risk_blocked,
            "sm_state": sm["state"],
        }
        if self.logger.should_log_decision(m.slug, ctx["hour_start_utc"], sig_dict):
            self.logger.log_decision({
                "engine": "CORE", "slug": m.slug, "crypto": m.crypto,
                "hour_start_utc": ctx["hour_start_utc"],
                "t_min": round(t_min, 3),
                "phase": ctx["phase"], "seconds_to_close": round(ctx["seconds_to_close"], 1),
                "selected_outcome": outcome,
                "valid_time": valid_time, "valid_delta": valid_delta, "valid_z": valid_z,
                "valid_price": valid_price, "valid_spread": valid_spread,
                "valid_imb": valid_imb, "valid_pullback": valid_pullback,
                "persistence_ok": persist_ok, "cooldown_active": cooldown_active,
                "risk_blocked": risk_blocked,
                "cap_used": cap, "cap_boost": round(cap - price_cap(t_min), 4),
                "thr_used": thr, "size_mult": mult,
                "spread_limit_used": max_spread,
                "clip_usdc": round(clip, 4),
                "will_trade": will_trade, "skip_reason": skip_reason,
                "spot": spot, "hour_open": ctx["hour_open"],
                "delta_bps": round(delta_bps, 3), "abs_delta_bps": round(abs_delta_bps, 3),
                "vel": round(vel, 3) if vel is not None else None, "z": round(z, 3),
                "sm_state": sm["state"],
            })
        # ── BOUNDARY events ──
        boundary_state = {
            "abs_delta_bps": abs_delta_bps,
            "entry_thr_bps": thr,
            "valid_price": valid_price,
            "ask": book.ask, "price_cap": cap,
            "persistence_ok": persist_ok,
            "peak_abs_delta_bps": st.peak_abs_delta_bps,
        }
        for bev in self.logger.check_boundaries(m.slug, ctx["hour_start_utc"], boundary_state):
            self.logger.log_boundary(bev)

        # --- State machine: IDLE → PROBING → SCALING → COOLDOWN ---
        if sm["state"] == "IDLE":
            if not sig:
                print(f"  [GATE_FAIL] {m.crypto:5s} {skip_reason}")
                return
            if not persist_ok:
                print(f"  [PERSIST_FAIL] {m.crypto:5s} sig=True but persistence not met (hist={len(sh)})")
                return
            if cooldown_active or risk_blocked:
                return
            if clip < MIN_ORDER_USDC:
                print(f"  [CLIP_FAIL] {m.crypto:5s} clip=${clip:.2f} < min=${MIN_ORDER_USDC}")
                return
            # ---- Place PROBE order (taker-gated) ----
            signal_detect_ts = time.time()
            sm["signal_detect_ts"] = signal_detect_ts
            # Record trade direction for no-flip rule
            self._last_trade_direction[m.slug] = (outcome, time.time())
            # Taker gate: only cross if spread <= 1c AND edge >= thr + 12
            probe_use_taker = taker_gate_allows(book.spread * 100, abs_delta_bps, thr)
            if not probe_use_taker:
                self._diag_blocked_taker_gate += 1
            probe_price = book.ask if probe_use_taker else book.bid
            probe_usd = max(MIN_ORDER_USDC, clip * PROBE_SIZE_FRAC)
            probe_qty = probe_usd / max(1e-9, probe_price)
            edge = ctx["edge_up"] if outcome == "Up" else ctx["edge_down"]
            self._hour_edges.append(edge)
            decision_id = new_decision_id()
            client_oid = new_order_id()
            pos = st.positions[outcome]
            if pos.position_id is None:
                pos.position_id = new_position_id()
                pos.trade_id = uuid.uuid4().hex
                pos.entry_decision_id = decision_id
                pos.parent_order_id = client_oid
                pos.entry_mid = book.mid
                pos.max_favorable_mid = book.mid
                pos.max_adverse_mid = book.mid
            ctx["cap_used"] = cap
            ctx["thr_used"] = thr
            ctx["size_mult"] = mult
            bk_fields = self._book_fields(up_book, dn_book, outcome)
            order_place_ts = time.time()
            signal_to_order_ms = (order_place_ts - signal_detect_ts) * 1000
            print(f"  [PROBE] {m.crypto:5s} {outcome} probe=${probe_usd:.2f} ask={book.ask:.3f} latency={signal_to_order_ms:.0f}ms")
            write_jsonl({"event_type": "SIGNAL_LATENCY", "slug": m.slug,
                          "crypto": m.crypto, "outcome": outcome,
                          "signal_detect_ts": signal_detect_ts,
                          "order_place_ts": order_place_ts,
                          "signal_to_order_ms": round(signal_to_order_ms, 1),
                          "t_min": round(t_min, 3)})
            probe_mt = "taker" if probe_use_taker else "maker"
            if probe_use_taker:
                self._diag_taker_count += 1
            else:
                self._diag_maker_count += 1
            self.logger.log_order_intent(
                engine="CORE", reason="ENTRY_PROBE",
                decision_id=decision_id, position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=probe_qty, target_price=probe_price,
                usdc_cost=probe_usd, ctx=ctx, book_fields=bk_fields,
            )
            self.logger.log_order_submit(
                engine="CORE", reason="ENTRY_PROBE",
                decision_id=decision_id, position_id=pos.position_id,
                client_order_id=client_oid,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=probe_qty, target_price=probe_price,
                usdc_cost=probe_usd, ctx=ctx, book_fields=bk_fields,
            )
            if MODE == "LOG":
                self._paper_buy(st, outcome, probe_price, probe_qty, probe_usd)
                mt = probe_mt
                sc = spread_capture_fields("BUY", probe_price, book)
                _fee = compute_fee_usdc(probe_usd, mt)
                self.logger.log_order_fill(
                    engine="CORE", reason="ENTRY_PROBE",
                    decision_id=decision_id, client_order_id=client_oid,
                    position_id=pos.position_id,
                    crypto=m.crypto, slug=m.slug, outcome=outcome,
                    side="BUY", qty=probe_qty, fill_price=probe_price,
                    usdc_cost=probe_usd, fees_usdc=_fee,
                    maker_taker=mt, did_cross=sc.get("did_cross", ""),
                    vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                )
            else:
                fill = self._place_layered_buy(m, outcome, probe_qty, probe_price)
                if fill["total_filled"] > 0:
                    self._live_buy(st, outcome, fill["avg_price"], fill["total_filled"], fill["total_cost"])
                    mt = infer_maker_taker("BUY", fill["avg_price"], book)
                    sc = spread_capture_fields("BUY", fill["avg_price"], book)
                    _fee = compute_fee_usdc(fill["total_cost"], mt)
                    self.logger.log_order_fill(
                        engine="CORE", reason="ENTRY_PROBE",
                        decision_id=decision_id, client_order_id=client_oid,
                        position_id=pos.position_id,
                        crypto=m.crypto, slug=m.slug, outcome=outcome,
                        side="BUY", qty=fill["total_filled"], fill_price=fill["avg_price"],
                        usdc_cost=fill["total_cost"], fees_usdc=_fee,
                        maker_taker=mt, did_cross=sc.get("did_cross", ""),
                        vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                    )
            sm["probe_ts"] = time.time()
            sm["probe_ask"] = probe_price
            sm["initial_edge_bps"] = edge_bps
            self._sm_transition(m.slug, "PROBING", "probe_placed", ctx)
            return

        elif sm["state"] == "PROBING":
            # Wait PROBE_CONFIRM_SEC (300ms), then check signal still valid
            elapsed = time.time() - (sm.get("probe_ts") or time.time())
            if elapsed < PROBE_CONFIRM_SEC:
                return  # still waiting
            if not sig:
                self._sm_transition(m.slug, "IDLE", "signal_lost_after_probe", ctx)
                return
            # Edge gate: only burst if edge >= thr + BURST_MIN_EDGE_EXTRA_BPS
            if abs_delta_bps < thr + BURST_MIN_EDGE_EXTRA_BPS:
                # Edge not strong enough for burst — stay with probe only
                st.last_entry_ts = iso_z(utc_now())
                self._sm_transition(m.slug, "COOLDOWN", "probe_only_weak_edge", ctx)
                return
            # Signal still valid + strong edge → burst scale in background thread
            self._sm_transition(m.slug, "SCALING", "signal_confirmed", ctx)
            burst_ctx = dict(ctx)  # snapshot context
            burst_clip = clip
            burst_thr = thr  # capture threshold for taker gating inside burst
            def _run_burst():
                try:
                    self._execute_burst_buy(m, st, outcome, burst_clip, burst_ctx, burst_thr)
                finally:
                    st.last_entry_ts = iso_z(utc_now())
                    self._sm_transition(m.slug, "COOLDOWN", "burst_complete", burst_ctx)
            t = threading.Thread(target=_run_burst, daemon=True)
            t.start()
            return

        elif sm["state"] == "SCALING":
            # Burst is running in background thread — don't interrupt
            return

        elif sm["state"] == "COOLDOWN":
            # Already handled above (cooldown expiry → IDLE)
            return

    # -----------------------------------------------------------------
    # Burst execution engine
    # -----------------------------------------------------------------
    def _execute_burst_buy(self, m: MarketRef, st: MarketState, outcome: str,
                           base_clip_usd: float, ctx: dict, thr_bps: float = 0):
        """Count-based burst engine (f247-tuned: less spam, bigger steps).
        Places up to BURST_ORDERS micro-orders every BURST_INTERVAL_MS.
        Taker-gated: only crosses if spread <= 1c AND abs(edge) >= thr + 12.
        Otherwise posts maker at best bid. Stops on edge collapse (sustained 500ms),
        price move 2c against, inventory cap, or hard spread limit."""
        burst_start_ts = time.time()
        up_book, dn_book = ctx["up_book"], ctx["dn_book"]
        book = up_book if outcome == "Up" else dn_book
        initial_mid = book.mid
        pos = st.positions[outcome]
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        inv_cap = INVENTORY_CAP_SHARES_PER_MARKET
        total_filled_usd = 0.0
        burst_count = 0
        skipped_stale = 0
        maker_count = 0
        taker_count = 0
        stop_reason = ""
        edge_below_since: Optional[float] = None

        write_jsonl({"event_type": "BURST_START", "slug": m.slug, "crypto": m.crypto,
                      "outcome": outcome, "burst_orders": BURST_ORDERS,
                      "step_usd_range": f"{BURST_STEP_USD_MIN}-{BURST_STEP_USD_MAX}",
                      "initial_mid": initial_mid, "thr_bps": thr_bps,
                      "t_min": round(ctx["t_min"], 3)})

        for i in range(BURST_ORDERS):
            # ── Freshness gate ──
            cache_age = self._cache_age_ms(m.slug)
            if cache_age > BURST_FRESHNESS_MAX_MS:
                self._submit_market_refresh(m)
                waited = 0.0
                while waited < BURST_FRESHNESS_WAIT_MS:
                    time.sleep(0.025)
                    waited += 25.0
                    if self._cache_age_ms(m.slug) <= BURST_FRESHNESS_MAX_MS:
                        break
                if self._cache_age_ms(m.slug) > BURST_FRESHNESS_MAX_MS:
                    write_jsonl({"event_type": "BURST_SKIP_STALE", "slug": m.slug,
                                  "burst_idx": i,
                                  "cache_age_ms": round(self._cache_age_ms(m.slug), 0)})
                    skipped_stale += 1
                    continue

            # ── Read fresh data from background cache ──
            fresh_cache = self._data_cache.get(m.slug)
            if not fresh_cache:
                stop_reason = "no_cache"
                break
            fresh_book = fresh_cache["up_book"] if outcome == "Up" else fresh_cache["dn_book"]
            fresh_up_book = fresh_cache["up_book"]
            fresh_dn_book = fresh_cache["dn_book"]

            if fresh_book.ask <= 0 or fresh_book.bid <= 0:
                stop_reason = "bad_book"
                break

            # ── Stop: price moved 2c against us ──
            if fresh_book.mid - initial_mid <= -BURST_STOP_IF_PRICE_MOVES_CENTS:
                stop_reason = f"price_adverse({fresh_book.mid - initial_mid:.3f}c)"
                break

            # ── Stop: per-market max position ──
            if pos.qty >= inv_cap:
                stop_reason = f"inventory_cap({pos.qty:.0f}>={inv_cap})"
                break

            # ── Edge computation from live spot ──
            fresh_spot = fresh_cache.get("spot", ctx["spot"])
            fresh_hour_open = fresh_cache.get("hour_open", ctx["hour_open"])
            if fresh_hour_open > 0:
                live_delta_bps = (fresh_spot - fresh_hour_open) / fresh_hour_open * 10000.0
            else:
                live_delta_bps = ctx["delta_bps"]
            abs_live_delta = abs(live_delta_bps)
            p_up = _p_up_model(live_delta_bps)
            if outcome == "Up":
                cur_edge = (p_up - fresh_book.mid) * 10000.0
            else:
                cur_edge = ((1.0 - p_up) - fresh_book.mid) * 10000.0

            # ── Stop: edge below threshold for sustained 500ms ──
            sm = self._get_sm(m.slug)
            initial_edge = sm.get("initial_edge_bps") or cur_edge
            edge_drop = initial_edge - cur_edge
            if edge_drop >= BURST_STOP_IF_EDGE_DROPS_BPS:
                now_t = time.time()
                if edge_below_since is None:
                    edge_below_since = now_t
                elif (now_t - edge_below_since) * 1000 >= BURST_EDGE_BELOW_HOLD_MS:
                    stop_reason = f"edge_sustained_drop({edge_drop:.1f}bps,{(now_t-edge_below_since)*1000:.0f}ms)"
                    break
            else:
                edge_below_since = None

            # ── Stop: hard spread limit ──
            if fresh_book.spread > BURST_SPREAD_HARD_LIMIT:
                stop_reason = f"spread_hard({fresh_book.spread:.3f}>{BURST_SPREAD_HARD_LIMIT})"
                break

            # ── Size micro-order: 0.75–6.00 USDC ──
            this_usd = clamp(base_clip_usd * 0.15, BURST_STEP_USD_MIN, BURST_STEP_USD_MAX)
            decision_id = new_decision_id()
            client_oid = new_order_id()
            bk_fields = self._book_fields(fresh_up_book, fresh_dn_book, outcome)

            # ── Taker gate: only cross if spread <= 1c AND edge >= thr + 12 ──
            use_taker = taker_gate_allows(fresh_book.spread * 100, abs_live_delta, thr_bps)
            order_price = fresh_book.ask if use_taker else fresh_book.bid
            this_qty = this_usd / max(1e-9, order_price)
            order_type = "taker" if use_taker else "maker"

            write_jsonl({"event_type": "BURST_MICRO_ORDER", "slug": m.slug,
                          "burst_idx": i, "usd": round(this_usd, 2),
                          "qty": round(this_qty, 2), "price": order_price,
                          "order_type": order_type,
                          "edge_bps": round(cur_edge, 2), "spread": round(fresh_book.spread, 4),
                          "cache_age_ms": round(self._cache_age_ms(m.slug), 0),
                          "live_delta_bps": round(live_delta_bps, 2)})

            if MODE == "LOG":
                self._paper_buy(st, outcome, order_price, this_qty, this_usd)
                mt = order_type
                sc = spread_capture_fields("BUY", order_price, fresh_book)
                _fee = compute_fee_usdc(this_usd, mt)
                self.logger.log_order_fill(
                    engine="CORE", reason="ENTRY_BURST",
                    decision_id=decision_id, client_order_id=client_oid,
                    position_id=pos.position_id or "",
                    crypto=m.crypto, slug=m.slug, outcome=outcome,
                    side="BUY", qty=this_qty, fill_price=order_price,
                    usdc_cost=this_usd, fees_usdc=_fee,
                    maker_taker=mt, did_cross=sc.get("did_cross", ""),
                    vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                )
                total_filled_usd += this_usd
                burst_count += 1
            else:
                post_only = not use_taker
                fill = self.client.place_limit_order(token_id, "BUY", order_price,
                                                     this_qty, post_only=post_only)
                if fill.get("filled"):
                    self._live_buy(st, outcome, fill["fill_price"], fill["fill_qty"],
                                   fill["fill_price"] * fill["fill_qty"])
                    mt = infer_maker_taker("BUY", fill["fill_price"], fresh_book)
                    sc = spread_capture_fields("BUY", fill["fill_price"], fresh_book)
                    _burst_notional = fill["fill_price"] * fill["fill_qty"]
                    _fee = compute_fee_usdc(_burst_notional, mt)
                    self.logger.log_order_fill(
                        engine="CORE", reason="ENTRY_BURST",
                        decision_id=decision_id, client_order_id=client_oid,
                        position_id=pos.position_id or "",
                        crypto=m.crypto, slug=m.slug, outcome=outcome,
                        side="BUY", qty=fill["fill_qty"], fill_price=fill["fill_price"],
                        usdc_cost=_burst_notional, fees_usdc=_fee,
                        maker_taker=mt, did_cross=sc.get("did_cross", ""),
                        vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                    )
                    total_filled_usd += fill["fill_price"] * fill["fill_qty"]
                    burst_count += 1
                elif fill.get("order_id"):
                    # Track unfilled burst order in order manager
                    oid = fill["order_id"]
                    now_ms = int(time.time() * 1000)
                    self._om_open_orders[oid] = {
                        "slug": m.slug, "outcome": outcome, "side": "BUY",
                        "price": order_price, "qty": int(float(this_qty)),
                        "filled_qty": fill.get("fill_qty", 0) or 0,
                        "reason": "ENTRY_BURST", "maker": post_only,
                        "created_ms": now_ms, "last_check_ms": now_ms,
                        "status": "open", "token_id": token_id,
                        "st_slug": m.slug, "cancel_pending": False,
                    }

            # Track diagnostics
            if use_taker:
                taker_count += 1
                self._diag_taker_count += 1
            else:
                maker_count += 1
                self._diag_maker_count += 1
            self._tempo_fills[m.slug] = self._tempo_fills.get(m.slug, 0) + 1
            # Sleep between micro-orders
            time.sleep(BURST_INTERVAL_MS / 1000.0)

        burst_duration_ms = (time.time() - burst_start_ts) * 1000
        write_jsonl({"event_type": "BURST_STOP", "slug": m.slug, "crypto": m.crypto,
                      "outcome": outcome, "burst_count": burst_count,
                      "total_filled_usd": round(total_filled_usd, 2),
                      "stop_reason": stop_reason or "all_orders_done",
                      "burst_duration_ms": round(burst_duration_ms, 1),
                      "skipped_stale": skipped_stale,
                      "maker_count": maker_count, "taker_count": taker_count,
                      "t_min": round(ctx["t_min"], 3)})

    # =================================================================
    # PARITY (STRADDLE) ARBITRAGE ENGINE  (v2: fee-aware, partial-fill
    # protected, maker-disciplined, with locked recycle)
    # =================================================================
    def _parity_arb(self, ctx: dict, new_quotes_blocked: bool = False):
        """Fee-aware parity arbitrage with partial-fill protection.
        1. Compute raw + net (after fees/slippage) edges
        2. Liquidity/staleness guards
        3. Execute paired orders with pair_id tracking
        4. Handle pending partial fills from prior ticks
        If new_quotes_blocked=True, only handle pending, recycle, flatten — no new quotes/orders."""
        m, st = ctx["m"], ctx["st"]
        t_min = ctx["t_min"]
        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        spot, hour_open = ctx["spot"], ctx["hour_open"]

        # ── Handle pending partial fills first (always, even after stop-new time) ──
        self._parity_handle_pending_pairs(m, st, ctx)

        # ── Recycle locked inventory if stale ──
        self._parity_recycle_locked(m, st, ctx)

        # ── Parity QUOTING mode (continuously post maker bids on both legs) ──
        if PARITY_QUOTE_ENABLED and t_min < PARITY_STOP_NEW_MIN and not new_quotes_blocked:
            self._parity_quote(m, st, ctx)

        # ── End-of-hour flattening (runs even after PARITY_STOP_NEW_MIN) ──
        if t_min >= PARITY_FLATTEN_START_MIN:
            self._parity_flatten_eoh(m, st, t_min, ctx)
            return  # don't open new parity trades while flattening

        # ── Compute raw parity metrics ──
        straddle_buy_cost = up_book.ask + dn_book.ask
        straddle_sell_value = up_book.bid + dn_book.bid
        raw_buy_edge = (1.000 - straddle_buy_cost) * 100
        raw_sell_edge = (straddle_sell_value - 1.000) * 100

        # ── Fee-aware net edges ──
        buy_net, buy_fee, buy_slip = parity_net_edge_cents(raw_buy_edge, up_book, dn_book, True)
        sell_net, sell_fee, sell_slip = parity_net_edge_cents(raw_sell_edge, up_book, dn_book, False)

        # Store in ctx for logging
        ctx["parity_raw_buy_cents"] = raw_buy_edge
        ctx["parity_raw_sell_cents"] = raw_sell_edge
        ctx["parity_net_buy_cents"] = buy_net
        ctx["parity_net_sell_cents"] = sell_net

        # ── Stop new parity trades after PARITY_STOP_NEW_MIN or when blocked ──
        if t_min >= PARITY_STOP_NEW_MIN or new_quotes_blocked:
            return

        # ── Cooldown gate ──
        now_t = time.time()
        last_ts = self._parity_last_order_ts.get(m.slug, 0.0)
        if (now_t - last_ts) * 1000 < PARITY_COOLDOWN_MS:
            return

        # ── Staleness guard ──
        cache_age = self._cache_age_ms(m.slug)
        if cache_age > PARITY_MAX_CACHE_AGE_MS:
            self._diag_parity_blocked_stale += 1
            return

        # ── Liquidity guard ──
        liq_ok, liq_reason = parity_liquidity_ok(up_book, dn_book)
        if not liq_ok:
            if "spread" in liq_reason:
                self._diag_parity_blocked_spread += 1
            else:
                self._diag_parity_blocked_liq += 1
            return

        # ── Investment cap gate ──
        invested = self._parity_invested_usd.get(m.slug, 0.0)

        # ── Directional lean ──
        lean_up = spot >= hour_open

        # ── BUY CHEAP STRADDLE (with edge buffer) ──
        buy_threshold = PARITY_BUY_MIN_EDGE_NET_CENTS + PARITY_EDGE_BUFFER_CENTS
        if PARITY_BUY_ENABLED and buy_net >= buy_threshold:
            self._diag_parity_buy_signals += 1

            if invested >= PARITY_MAX_USD_PER_SLUG:
                return

            if up_book.ask <= 0 or dn_book.ask <= 0 or up_book.bid <= 0 or dn_book.bid <= 0:
                return

            leg_usd = min(PARITY_STEP_USD, (PARITY_MAX_USD_PER_SLUG - invested) / 2.0)
            if leg_usd < MIN_ORDER_USDC:
                return

            # Generate pair_id for partial-fill tracking
            pair_id = uuid.uuid4().hex[:16]

            # Execute leg 1: BUY Up
            up_filled = self._parity_buy_leg(m, st, "Up", up_book, leg_usd, ctx, pair_id)

            # Execute leg 2: BUY Down
            dn_filled = self._parity_buy_leg(m, st, "Down", dn_book, leg_usd, ctx, pair_id)

            total_cost = up_filled + dn_filled
            both_filled = up_filled > 0 and dn_filled > 0
            one_filled = (up_filled > 0) != (dn_filled > 0)

            if both_filled:
                self._parity_invested_usd[m.slug] = invested + total_cost
                self._parity_last_order_ts[m.slug] = time.time()
                self._diag_parity_trades += 1
                self._diag_parity_edges.append(buy_net)
                self._diag_parity_trade_timestamps.append(time.time())
                # Track locked straddle start time
                if m.slug not in self._parity_locked_since:
                    self._parity_locked_since[m.slug] = time.time()

                write_jsonl({"event_type": "PARITY_BUY_STRADDLE",
                              "slug": m.slug, "crypto": m.crypto,
                              "pair_id": pair_id,
                              "straddle_buy_cost": round(straddle_buy_cost, 4),
                              "raw_edge_cents": round(raw_buy_edge, 3),
                              "net_edge_cents": round(buy_net, 3),
                              "fee_cents": round(buy_fee, 3),
                              "slippage_cents": round(buy_slip, 3),
                              "up_cost": round(up_filled, 4),
                              "dn_cost": round(dn_filled, 4),
                              "total_invested": round(invested + total_cost, 2),
                              "t_min": round(t_min, 3)})

            elif one_filled:
                # Partial fill — queue for resolution
                self._diag_pair_partial_count += 1
                filled_outcome = "Up" if up_filled > 0 else "Down"
                pending_outcome = "Down" if filled_outcome == "Up" else "Up"
                filled_usd = up_filled if up_filled > 0 else dn_filled
                filled_book = up_book if filled_outcome == "Up" else dn_book
                self._parity_pending_pairs.append({
                    "pair_id": pair_id,
                    "slug": m.slug,
                    "filled_outcome": filled_outcome,
                    "filled_usd": filled_usd,
                    "filled_qty": filled_usd / max(1e-9, filled_book.ask),
                    "filled_price": filled_book.ask,
                    "pending_outcome": pending_outcome,
                    "ts": time.time(),
                    "edge_net_cents": buy_net,
                })
                self._parity_invested_usd[m.slug] = invested + filled_usd
                self._parity_last_order_ts[m.slug] = time.time()

                write_jsonl({"event_type": "PARITY_PARTIAL_FILL",
                              "slug": m.slug, "pair_id": pair_id,
                              "filled_outcome": filled_outcome,
                              "filled_usd": round(filled_usd, 4),
                              "pending_outcome": pending_outcome,
                              "net_edge_cents": round(buy_net, 3)})
            return

        # ── SELL RICH STRADDLE ──
        # ── SELL RICH STRADDLE (with edge buffer) ──
        sell_threshold = PARITY_SELL_MIN_EDGE_NET_CENTS + PARITY_EDGE_BUFFER_CENTS
        if PARITY_SELL_ENABLED and sell_net >= sell_threshold:
            self._diag_parity_sell_signals += 1

            pos_up = st.positions["Up"]
            pos_dn = st.positions["Down"]

            if pos_up.qty < MIN_QTY or pos_dn.qty < MIN_QTY:
                return
            if up_book.bid <= 0 or dn_book.bid <= 0:
                return

            sell_up_usd = min(PARITY_STEP_USD, pos_up.qty * up_book.bid)
            sell_dn_usd = min(PARITY_STEP_USD, pos_dn.qty * dn_book.bid)
            sell_usd = min(sell_up_usd, sell_dn_usd)
            if sell_usd < MIN_ORDER_USDC:
                return

            pair_id = uuid.uuid4().hex[:16]

            if LEAN_EXIT_PRIORITY:
                sell_order = [("Down", dn_book), ("Up", up_book)] if lean_up else [("Up", up_book), ("Down", dn_book)]
            else:
                sell_order = [("Up", up_book), ("Down", dn_book)]

            total_proceeds = 0.0
            legs_filled = 0
            for outcome, book in sell_order:
                sell_qty = sell_usd / max(1e-9, book.bid)
                pos = st.positions[outcome]
                sell_qty = min(sell_qty, pos.qty)
                if sell_qty < MIN_QTY:
                    continue

                use_taker = (book.spread * 100) <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
                sell_price = book.bid if use_taker else max(book.bid, book.ask - 0.001)
                if use_taker:
                    self._diag_parity_taker_count += 1
                else:
                    self._diag_parity_maker_count += 1

                self._do_sell(m, st, outcome, sell_qty, sell_price,
                              reason="PARITY_SELL_STRADDLE", leg="PARITY_SELL",
                              ctx=ctx, use_maker=not use_taker)
                total_proceeds += sell_qty * sell_price
                legs_filled += 1

            if total_proceeds > 0:
                self._parity_last_order_ts[m.slug] = time.time()
                self._diag_parity_trades += 1
                self._diag_parity_edges.append(sell_net)
                self._diag_parity_trade_timestamps.append(time.time())
                self._parity_invested_usd[m.slug] = max(0.0, invested - total_proceeds)

                # Update locked tracking
                up_q = st.positions["Up"].qty
                dn_q = st.positions["Down"].qty
                if min(up_q, dn_q) < MIN_QTY:
                    self._parity_locked_since.pop(m.slug, None)

                write_jsonl({"event_type": "PARITY_SELL_STRADDLE",
                              "slug": m.slug, "crypto": m.crypto,
                              "pair_id": pair_id,
                              "straddle_sell_value": round(straddle_sell_value, 4),
                              "raw_edge_cents": round(raw_sell_edge, 3),
                              "net_edge_cents": round(sell_net, 3),
                              "fee_cents": round(sell_fee, 3),
                              "total_proceeds": round(total_proceeds, 4),
                              "legs_filled": legs_filled,
                              "remaining_invested": round(self._parity_invested_usd.get(m.slug, 0), 2),
                              "t_min": round(t_min, 3)})

    def _parity_handle_pending_pairs(self, m: MarketRef, st: MarketState, ctx: dict):
        """Resolve partial fills from prior ticks. If timeout exceeded:
        - edge still good → cross remaining leg (taker if spread<=1c)
        - edge gone → unwind filled leg to flatten."""
        now_t = time.time()
        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        resolved = []

        for i, pair in enumerate(self._parity_pending_pairs):
            if pair["slug"] != m.slug:
                continue
            elapsed_ms = (now_t - pair["ts"]) * 1000

            if elapsed_ms < PAIR_FILL_TIMEOUT_MS:
                # Still within timeout — try to fill the pending leg
                pending_book = up_book if pair["pending_outcome"] == "Up" else dn_book
                if pending_book.ask <= 0:
                    continue
                # Recompute net edge
                raw_buy_edge = ctx.get("parity_raw_buy_cents", 0.0)
                net_buy, _, _ = parity_net_edge_cents(raw_buy_edge, up_book, dn_book, True)

                if net_buy >= PARITY_BUY_MIN_EDGE_NET_CENTS:
                    # Edge still good — try to fill pending leg
                    filled = self._parity_buy_leg(m, st, pair["pending_outcome"],
                                                   pending_book, pair["filled_usd"],
                                                   ctx, pair["pair_id"])
                    if filled > 0:
                        # Pair complete
                        self._diag_parity_trades += 1
                        self._diag_parity_edges.append(net_buy)
                        self._diag_pair_fill_delays.append(elapsed_ms)
                        self._diag_parity_trade_timestamps.append(time.time())
                        self._parity_invested_usd[m.slug] = (
                            self._parity_invested_usd.get(m.slug, 0.0) + filled)
                        if m.slug not in self._parity_locked_since:
                            self._parity_locked_since[m.slug] = time.time()
                        write_jsonl({"event_type": "PARITY_PAIR_COMPLETED",
                                      "ts_ms": int(time.time() * 1000),
                                      "slug": m.slug, "pair_id": pair["pair_id"],
                                      "delay_ms": round(elapsed_ms, 1),
                                      "net_edge_cents": round(net_buy, 3)})
                        resolved.append(i)
                continue

            # Timeout exceeded — decide: cross or unwind
            pending_book = up_book if pair["pending_outcome"] == "Up" else dn_book
            raw_buy_edge = ctx.get("parity_raw_buy_cents", 0.0)
            net_buy, _, _ = parity_net_edge_cents(raw_buy_edge, up_book, dn_book, True)

            if net_buy >= PARITY_BUY_MIN_EDGE_NET_CENTS and pending_book.ask > 0:
                # Edge still good — force cross remaining leg (taker if spread<=1c)
                filled = self._parity_buy_leg(m, st, pair["pending_outcome"],
                                               pending_book, pair["filled_usd"],
                                               ctx, pair["pair_id"])
                if filled > 0:
                    self._diag_parity_trades += 1
                    self._diag_parity_edges.append(net_buy)
                    self._diag_pair_fill_delays.append(elapsed_ms)
                    self._diag_parity_trade_timestamps.append(time.time())
                    self._parity_invested_usd[m.slug] = (
                        self._parity_invested_usd.get(m.slug, 0.0) + filled)
                    if m.slug not in self._parity_locked_since:
                        self._parity_locked_since[m.slug] = time.time()
                    write_jsonl({"event_type": "PARITY_PAIR_COMPLETED_LATE",
                                  "ts_ms": int(time.time() * 1000),
                                  "slug": m.slug, "pair_id": pair["pair_id"],
                                  "delay_ms": round(elapsed_ms, 1)})
                    resolved.append(i)
                    continue

            # Edge gone or fill failed — unwind the filled leg
            filled_book = up_book if pair["filled_outcome"] == "Up" else dn_book
            pos = st.positions[pair["filled_outcome"]]
            unwind_qty = min(pair["filled_qty"], pos.qty)
            if unwind_qty >= MIN_QTY and filled_book.bid > 0:
                use_taker = (filled_book.spread * 100) <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
                unwind_price = filled_book.bid if use_taker else max(filled_book.bid, filled_book.ask - 0.001)
                self._do_sell(m, st, pair["filled_outcome"], unwind_qty, unwind_price,
                              reason="PARITY_UNWIND_PARTIAL", leg="PARITY_UNWIND",
                              ctx=ctx, use_maker=not use_taker)
                self._diag_unpaired_unwind_usd += unwind_qty * unwind_price
                self._parity_invested_usd[m.slug] = max(
                    0.0, self._parity_invested_usd.get(m.slug, 0.0) - unwind_qty * unwind_price)
                write_jsonl({"event_type": "PARITY_UNWIND_PARTIAL",
                              "slug": m.slug, "pair_id": pair["pair_id"],
                              "unwound_outcome": pair["filled_outcome"],
                              "unwind_qty": round(unwind_qty, 1),
                              "delay_ms": round(elapsed_ms, 1)})
            resolved.append(i)

        # Remove resolved pairs (reverse order to preserve indices)
        for idx in sorted(resolved, reverse=True):
            self._parity_pending_pairs.pop(idx)

    def _parity_recycle_locked(self, m: MarketRef, st: MarketState, ctx: dict):
        """Recycle locked straddle inventory that has been held too long.
        Tries to sell both legs at a small net profit (maker-first)."""
        locked_since = self._parity_locked_since.get(m.slug)
        if locked_since is None:
            return
        pos_up = st.positions["Up"]
        pos_dn = st.positions["Down"]
        locked_qty = min(pos_up.qty, pos_dn.qty)
        if locked_qty < MIN_QTY:
            self._parity_locked_since.pop(m.slug, None)
            return

        hold_sec = time.time() - locked_since
        if hold_sec < LOCKED_MAX_HOLD_SEC:
            return

        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]

        # Check if we can sell both legs at net profit
        up_vwap = pos_up.vwap
        dn_vwap = pos_dn.vwap
        straddle_vwap = up_vwap + dn_vwap
        straddle_sell_value = up_book.bid + dn_book.bid

        # Net profit after fees
        raw_profit_cents = (straddle_sell_value - straddle_vwap) * 100
        _, sell_fee, sell_slip = parity_net_edge_cents(raw_profit_cents, up_book, dn_book, False)
        net_profit_cents = raw_profit_cents - sell_fee - sell_slip

        if net_profit_cents < RECYCLE_MIN_PROFIT_NET_CENTS:
            return

        # Sell both legs
        sell_usd = min(RECYCLE_STEP_USD, locked_qty * min(up_book.bid, dn_book.bid))
        if sell_usd < MIN_ORDER_USDC:
            return

        pair_id = uuid.uuid4().hex[:16]
        for outcome, book in [("Up", up_book), ("Down", dn_book)]:
            pos = st.positions[outcome]
            sell_qty = min(sell_usd / max(1e-9, book.bid), pos.qty)
            if sell_qty < MIN_QTY:
                continue
            use_taker = (book.spread * 100) <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
            sell_price = book.bid if use_taker else max(book.bid, book.ask - 0.001)
            if use_taker:
                self._diag_parity_taker_count += 1
            else:
                self._diag_parity_maker_count += 1
            self._do_sell(m, st, outcome, sell_qty, sell_price,
                          reason="PARITY_RECYCLE", leg="PARITY_RECYCLE",
                          ctx=ctx, use_maker=not use_taker)

        self._diag_recycle_count += 1
        self._clone_hold_times.append(hold_sec)
        self._parity_invested_usd[m.slug] = max(
            0.0, self._parity_invested_usd.get(m.slug, 0.0) - sell_usd * 2)

        # Update locked tracking
        if min(st.positions["Up"].qty, st.positions["Down"].qty) < MIN_QTY:
            self._parity_locked_since.pop(m.slug, None)
        else:
            self._parity_locked_since[m.slug] = time.time()  # reset timer

        write_jsonl({"event_type": "PARITY_RECYCLE",
                      "slug": m.slug, "crypto": m.crypto,
                      "pair_id": pair_id,
                      "hold_sec": round(hold_sec, 1),
                      "raw_profit_cents": round(raw_profit_cents, 3),
                      "net_profit_cents": round(net_profit_cents, 3),
                      "sell_usd": round(sell_usd, 2),
                      "t_min": round(ctx["t_min"], 3)})

    # =================================================================
    # PAIR FILL TRACKER — thread pair_id through all quote/arb fills
    # =================================================================
    def _record_pair_fill(self, pair_id: str, slug: str, crypto: str,
                          outcome: str, fill_ts: float, maker_taker: str):
        """Record a fill for a pair_id. When both legs filled, compute pair metrics."""
        if not pair_id:
            return
        entry = self._pair_tracker.setdefault(pair_id, {
            "slug": slug, "crypto": crypto, "fills": {},
        })
        entry["fills"][outcome] = {"ts": fill_ts, "maker_taker": maker_taker}

        # Track maker vs taker for parity
        if maker_taker == "maker":
            self._diag_parity_maker_count += 1
        elif maker_taker == "taker":
            self._diag_parity_taker_count += 1

        # Check if pair is now complete (both Up and Down filled)
        if "Up" in entry["fills"] and "Down" in entry["fills"]:
            up_ts = entry["fills"]["Up"]["ts"]
            dn_ts = entry["fills"]["Down"]["ts"]
            delay_ms = abs(up_ts - dn_ts) * 1000
            self._clone_pair_delays.append(delay_ms)
            self._diag_pairs_completed += 1
            if delay_ms <= 500:
                self._diag_pairs_completed_500ms += 1
                self._diag_slug_paired_500ms[slug] = self._diag_slug_paired_500ms.get(slug, 0) + 1
            if delay_ms <= 1500:
                self._diag_pairs_completed_1500ms += 1
                self._diag_slug_paired_1500ms[slug] = self._diag_slug_paired_1500ms.get(slug, 0) + 1
            if delay_ms <= 10000:
                self._diag_pairs_completed_10s += 1
            # Inter-pair gap
            completion_ts = max(up_ts, dn_ts)
            if self._clone_last_pair_ts > 0:
                gap_ms = (completion_ts - self._clone_last_pair_ts) * 1000
                self._clone_inter_pair_gaps.append(gap_ms)
            self._clone_last_pair_ts = completion_ts
            # Straddle edge at completion
            self._diag_parity_trades += 1
            self._diag_parity_trade_timestamps.append(completion_ts)
            write_jsonl({
                "event_type": "PAIR_COMPLETED",
                "ts_ms": int(completion_ts * 1000),
                "pair_id": pair_id, "slug": slug, "crypto": crypto,
                "delay_ms": round(delay_ms, 1),
                "up_ts_ms": int(up_ts * 1000), "dn_ts_ms": int(dn_ts * 1000),
            })
            # Clean up tracker (keep last 100 for reference)
            self._pair_tracker.pop(pair_id, None)

    # =================================================================
    # PARITY QUOTING MODE — continuously post maker bids on both legs
    # =================================================================
    def _quote_get_dynamic_target(self, slug: str) -> float:
        """Return current dynamic quote target edge (cents) for this slug.
        Adjusts each minute based on fill rate and locked inventory."""
        current = self._quote_dynamic_target.get(slug,
                    PARITY_QUOTE_TARGET_EDGE_NET_CENTS_BASE)
        # Compute slug-level fill rate: quote_fills / quote_orders for this slug
        # (Use global rate as proxy — per-slug tracking is too granular for now)
        fill_rate = (self._diag_quote_fills / max(1, self._diag_quote_orders_placed)
                     if self._diag_quote_orders_placed > 0 else 0.5)
        locked_usd = 0.0
        st = self.market_states.get(slug)
        if st:
            up_q = st.positions["Up"].qty
            dn_q = st.positions["Down"].qty
            locked_shares = min(up_q, dn_q)
            if locked_shares >= MIN_QTY:
                locked_usd = locked_shares * (st.positions["Up"].vwap + st.positions["Down"].vwap)
        imbalance = 0.0
        if st:
            imbalance = abs(st.positions["Up"].qty - st.positions["Down"].qty)

        # Adjust: low fill rate + low locked -> pay up (decrease edge toward base)
        #         high locked or rising imbalance -> be selective (increase toward max)
        step = 0.05  # adjust by 0.05c per tick
        if fill_rate < 0.30 and locked_usd < PARITY_QUOTE_MAX_USD_PER_SLUG * 0.3:
            current = max(PARITY_QUOTE_TARGET_EDGE_NET_CENTS_BASE, current - step)
        elif locked_usd > PARITY_QUOTE_MAX_USD_PER_SLUG * 0.6 or imbalance > 20:
            current = min(PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX, current + step)

        self._quote_dynamic_target[slug] = current
        return current

    def _quote_get_dynamic_step(self, slug: str) -> float:
        """Return inventory-aware quote step size (USD) for this slug.
        Reduced by 50% during adverse degrade window."""
        fill_rate = (self._diag_quote_fills / max(1, self._diag_quote_orders_placed)
                     if self._diag_quote_orders_placed > 0 else 0.5)
        locked_usd = 0.0
        imbalance = 0.0
        st = self.market_states.get(slug)
        if st:
            up_q = st.positions["Up"].qty
            dn_q = st.positions["Down"].qty
            locked_shares = min(up_q, dn_q)
            if locked_shares >= MIN_QTY:
                locked_usd = locked_shares * (st.positions["Up"].vwap + st.positions["Down"].vwap)
            imbalance = abs(up_q - dn_q)
        # High inventory or imbalance -> reduce step (less risk per order)
        if locked_usd > 0.6 * PARITY_QUOTE_MAX_USD_PER_SLUG or imbalance > 20:
            base = max(0.75, PARITY_QUOTE_STEP_USD * 0.5)
        # Low fill rate + low locked -> increase step (be more aggressive)
        elif fill_rate < 0.30 and locked_usd < PARITY_QUOTE_MAX_USD_PER_SLUG * 0.3:
            base = min(3.0, PARITY_QUOTE_STEP_USD * 1.25)
        else:
            base = PARITY_QUOTE_STEP_USD
        # Adverse degrade: reduce step by 50%
        if time.time() < self._quote_degraded_until.get(slug, 0.0):
            base *= 0.5
        return base

    def _quote_check_adverse(self, slug: str) -> Tuple[bool, float, float]:
        """Check adverse selection: large spot move in last ADVERSE_LOOKBACK_SEC.
        Returns (is_adverse, spot_move_bps, velocity_bps_per_min)."""
        hist = self._spot_history.get(slug, [])
        if len(hist) < 2:
            return False, 0.0, 0.0
        now_t = time.time()
        cutoff = now_t - ADVERSE_LOOKBACK_SEC
        # Find oldest spot within lookback window
        oldest_spot = None
        oldest_ts = None
        for ts, sp in hist:
            if ts >= cutoff:
                oldest_spot = sp
                oldest_ts = ts
                break
        if oldest_spot is None or oldest_spot <= 0:
            return False, 0.0, 0.0
        latest_spot = hist[-1][1]
        latest_ts = hist[-1][0]
        if latest_spot <= 0:
            return False, 0.0, 0.0
        move_bps = abs((latest_spot - oldest_spot) / oldest_spot) * 10000.0
        # Compute velocity: bps per minute
        elapsed_sec = max(0.1, latest_ts - oldest_ts)
        vel_bps_per_min = move_bps / elapsed_sec * 60.0
        return move_bps > ADVERSE_SPOT_MOVE_BPS_THRESHOLD, move_bps, vel_bps_per_min

    def _parity_quote(self, m: MarketRef, st: MarketState, ctx: dict):
        """Post maker bids on both Up and Down to 'manufacture' cheap straddles.
        Dynamic target edge, anchor-based pricing, unpaired management."""
        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        now_t = time.time()

        # ── Unpaired quote management (runs first, always) ──
        self._quote_manage_unpaired(m, st, ctx, now_t)

        # ── Pause check ──
        pause_until = self._quote_paused_until.get(m.slug, 0.0)
        if now_t < pause_until:
            return

        # ── Adverse selection guard (degrade-first, hard-pause only if accelerating) ──
        is_adverse, spot_move_bps, vel_bps_per_min = self._quote_check_adverse(m.slug)
        if is_adverse:
            self._diag_adverse_guard_events += 1
            ts_ms = int(time.time() * 1000)
            if vel_bps_per_min >= ADVERSE_ACCEL_BPS_PER_MIN:
                # Move is accelerating — hard pause
                self._quote_paused_until[m.slug] = now_t + ADVERSE_PAUSE_SEC
                self._diag_adverse_guard_pauses += 1
                self._quote_dynamic_target[m.slug] = PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX
                write_jsonl({"event_type": "ADVERSE_GUARD_HARD_PAUSE",
                              "ts_ms": ts_ms,
                              "slug": m.slug, "crypto": m.crypto,
                              "spot_move_bps": round(spot_move_bps, 2),
                              "vel_bps_per_min": round(vel_bps_per_min, 1),
                              "threshold_bps": ADVERSE_SPOT_MOVE_BPS_THRESHOLD,
                              "accel_threshold": ADVERSE_ACCEL_BPS_PER_MIN,
                              "pause_sec": ADVERSE_PAUSE_SEC})
                return
            else:
                # Move is significant but not accelerating — degrade (don't pause)
                self._quote_degraded_until[m.slug] = now_t + ADVERSE_DEGRADE_SEC
                self._diag_adverse_guard_degrades += 1
                self._quote_dynamic_target[m.slug] = PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX
                write_jsonl({"event_type": "ADVERSE_GUARD_DEGRADE",
                              "ts_ms": ts_ms,
                              "slug": m.slug, "crypto": m.crypto,
                              "spot_move_bps": round(spot_move_bps, 2),
                              "vel_bps_per_min": round(vel_bps_per_min, 1),
                              "threshold_bps": ADVERSE_SPOT_MOVE_BPS_THRESHOLD,
                              "degrade_sec": ADVERSE_DEGRADE_SEC})
                # Don't return — continue quoting but with degraded parameters

        # ── Liquidity guard (optional) ──
        if PARITY_QUOTE_ONLY_IF_LIQ_OK:
            liq_ok, _ = parity_liquidity_ok(up_book, dn_book)
            if not liq_ok:
                return

        # ── Staleness guard ──
        cache_age = self._cache_age_ms(m.slug)
        if cache_age > PARITY_MAX_CACHE_AGE_MS:
            return

        # ── Rate limit check ──
        if not self._rate_limit_ok(m.slug):
            return

        # ── Investment cap (reduced when directional scalp active) ──
        invested = self._parity_invested_usd.get(m.slug, 0.0)
        cap = PARITY_QUOTE_MAX_USD_PER_SLUG
        if PARITY_DEFER_TO_DIRECTIONAL and m.slug in self._dscalp_positions:
            cap = min(cap, PARITY_MAX_WHEN_DIRECTIONAL_USD)
        if invested >= cap:
            return

        # ── Dynamic target edge ──
        target_edge_cents = self._quote_get_dynamic_target(m.slug)
        target_combined = 1.00 - target_edge_cents / 100.0
        # Deduct maker fees from target (both legs are maker)
        maker_fee_per_leg = MAKER_FEE_BPS / 10000.0
        target_combined_net = target_combined - 2 * maker_fee_per_leg

        if up_book.bid <= 0 or dn_book.bid <= 0:
            return
        if up_book.ask <= 0 or dn_book.ask <= 0:
            return

        # ── Anchor pricing: equal USD on both legs ──
        # Try Up as anchor first, then Down if Up doesn't work
        target_up, target_dn = self._quote_compute_anchor_prices(
            up_book, dn_book, target_combined_net)
        if target_up is None or target_dn is None:
            return

        # ── Dynamic step sizing ──
        dynamic_step_usd = self._quote_get_dynamic_step(m.slug)

        # ── Post/refresh quotes on each leg ──
        slug_quotes = self._parity_quotes.get(m.slug, {})
        pair_id = uuid.uuid4().hex[:16]

        for outcome, target_price, book in [("Up", target_up, up_book),
                                             ("Down", target_dn, dn_book)]:
            existing = slug_quotes.get(outcome)
            effective_price = target_price
            if existing:
                price_diff = abs(target_price - existing.get("price", 0))
                elapsed_ms = (now_t - existing.get("ts", 0)) * 1000
                our_price = existing.get("price", 0)
                outbid = book.bid > our_price + 0.0005

                # Queue position heuristic: step up when outbid to regain best
                if outbid and elapsed_ms >= HEDGE_TICK1_MS:
                    # 250ms+ outbid: step +1 tick above best_bid
                    step_up = clamp_to_tick(book.bid + 0.001)
                    if step_up < book.ask:
                        effective_price = step_up
                elif outbid and elapsed_ms >= 100:
                    # 100ms+ outbid: match best_bid
                    effective_price = clamp_to_tick(book.bid)
                elif not outbid and elapsed_ms < QUOTE_REFRESH_MIN_ELAPSED_MS:
                    continue  # rate-limited: not outbid + too soon
                elif (not outbid and QUOTE_REFRESH_SKIP_IF_SAME
                      and price_diff < QUOTE_REFRESH_MIN_TICK_MOVE
                      and elapsed_ms < PARITY_QUOTE_REFRESH_MS):
                    continue  # no need to refresh: price unchanged

            # Check that effective_price still yields net edge
            other_price = target_dn if outcome == "Up" else target_up
            combined_check = effective_price + other_price
            if combined_check > target_combined_net + 0.002:
                effective_price = target_price  # revert to conservative price

            # Imbalance check: don't add to over-weighted side (TASK C)
            up_q = st.positions["Up"].qty
            dn_q = st.positions["Down"].qty
            imbalance = up_q - dn_q
            if outcome == "Up" and imbalance > IMBALANCE_CAP_SHARES:
                continue  # too much Up, skip
            if outcome == "Down" and imbalance < -IMBALANCE_CAP_SHARES:
                continue  # too much Down, skip

            leg_usd = min(dynamic_step_usd,
                          (PARITY_QUOTE_MAX_USD_PER_SLUG - invested) / 2.0)
            if leg_usd < MIN_ORDER_USDC:
                continue

            cost = self._parity_quote_buy(m, st, outcome, book, effective_price,
                                           leg_usd, ctx, pair_id,
                                           quote_step_usd_used=dynamic_step_usd)
            if cost > 0:
                self._diag_quote_orders_placed += 1
                self._parity_quotes.setdefault(m.slug, {})[outcome] = {
                    "price": target_price,
                    "ts": now_t,
                    "pair_id": pair_id,
                    "order_id": getattr(self, '_last_quote_oid', ''),
                }
                # Track unpaired state: if one leg filled but the other hasn't yet
                invested = self._parity_invested_usd.get(m.slug, 0.0)

        # ── Check for straddle completion (both legs balanced) ──
        up_q = st.positions["Up"].qty
        dn_q = st.positions["Down"].qty
        imbal = abs(up_q - dn_q)
        both_have = up_q >= MIN_PAIR_QTY and dn_q >= MIN_PAIR_QTY
        if both_have and imbal < MIN_PAIR_QTY:
            # Balanced straddle — clear any hedge state
            if m.slug not in self._parity_locked_since:
                self._parity_locked_since[m.slug] = time.time()
            self._quote_unpaired.pop(m.slug, None)
            self._hedge_state.pop(m.slug, None)

        # ── Detect one-sided or imbalanced fills → trigger hedge ──
        # Case 1: one leg missing entirely
        # Case 2: significant imbalance (one leg much larger than other)
        up_has = up_q >= MIN_QTY
        dn_has = dn_q >= MIN_QTY
        one_sided = (up_has != dn_has)
        imbalanced = (up_has and dn_has and imbal >= MIN_PAIR_QTY
                      and max(up_q, dn_q) > min(up_q, dn_q) * 2.0)
        if (one_sided or imbalanced) and m.slug not in self._quote_unpaired:
            filled_outcome = "Up" if up_q > dn_q else "Down"
            self._quote_unpaired[m.slug] = {
                "outcome": filled_outcome,
                "fill_ts": now_t,
                "escalated": False,
            }
            self._diag_quote_unpaired_events += 1

    @staticmethod
    def _quote_compute_anchor_prices(up_book: BookTop, dn_book: BookTop,
                                      target_combined: float):
        """Compute anchor-based quote prices for both legs.
        Set one leg at best_bid, compute other = target_combined - anchor.
        Try both anchors, pick the one where both prices are competitive.
        Allow derived price up to best_bid + 2 ticks (more aggressive fills)."""
        min_tick = 0.01  # minimum price to bid
        max_above_best = 0.002  # allow up to 2 ticks above best_bid

        # Try anchor=Up (set Up at best_bid, compute Down)
        anchor_up = clamp_to_tick(up_book.bid)
        derived_dn = clamp_to_tick(target_combined - anchor_up)
        up_ok = anchor_up >= min_tick and anchor_up < up_book.ask
        dn_ok = (derived_dn >= min_tick
                 and derived_dn <= dn_book.bid + max_above_best
                 and derived_dn < dn_book.ask)

        # Try anchor=Down (set Down at best_bid, compute Up)
        anchor_dn = clamp_to_tick(dn_book.bid)
        derived_up = clamp_to_tick(target_combined - anchor_dn)
        up_ok2 = (derived_up >= min_tick
                  and derived_up <= up_book.bid + max_above_best
                  and derived_up < up_book.ask)
        dn_ok2 = anchor_dn >= min_tick and anchor_dn < dn_book.ask

        # Pick the anchor where the derived leg is most competitive
        # (closest to best bid without exceeding it)
        score_up_anchor = 0.0
        if up_ok and dn_ok:
            score_up_anchor = derived_dn  # higher = more competitive

        score_dn_anchor = 0.0
        if up_ok2 and dn_ok2:
            score_dn_anchor = derived_up

        if score_up_anchor <= 0 and score_dn_anchor <= 0:
            return None, None

        if score_up_anchor >= score_dn_anchor:
            # Anchor Up
            final_up = anchor_up
            final_dn = derived_dn
        else:
            # Anchor Down
            final_up = derived_up
            final_dn = anchor_dn

        # Final sanity: combined must not exceed 1.00
        if final_up + final_dn > 1.00:
            return None, None
        if final_up <= 0.01 or final_dn <= 0.01:
            return None, None
        return final_up, final_dn

    def _quote_manage_unpaired(self, m: MarketRef, st: MarketState,
                                ctx: dict, now_t: float):
        """Auto-hedge unpaired fills: fast escalation (+1 tick at 200ms,
        +3 ticks at 350ms, early cross at 500ms, late cross at 800ms),
        then unwind+pause. Stale cache (>450ms) blocks all hedge actions."""
        unpaired = self._quote_unpaired.get(m.slug)
        if unpaired is None:
            return

        # Track unpaired event per slug
        self._diag_slug_unpaired_events[m.slug] = self._diag_slug_unpaired_events.get(m.slug, 0) + 1

        # If both legs now balanced, clear hedge state
        up_q = st.positions["Up"].qty
        dn_q = st.positions["Down"].qty
        imbal = abs(up_q - dn_q)
        if (up_q >= MIN_PAIR_QTY and dn_q >= MIN_PAIR_QTY
                and imbal < MIN_PAIR_QTY):
            self._quote_unpaired.pop(m.slug, None)
            self._hedge_state.pop(m.slug, None)
            return

        filled_outcome = unpaired["outcome"]
        fill_ts = unpaired["fill_ts"]
        age_ms = (now_t - fill_ts) * 1000
        age_sec = age_ms / 1000.0

        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        missing_outcome = "Down" if filled_outcome == "Up" else "Up"
        missing_book = dn_book if missing_outcome == "Down" else up_book

        # Initialize hedge state if needed
        hedge = self._hedge_state.setdefault(m.slug, {
            "tick1_done": False, "tick2_done": False,
            "early_cross_done": False, "cross_done": False,
        })

        # ── Stale cache guard: block all hedge actions if cache > 450ms ──
        cache_age = self._cache_age_ms(m.slug)
        if cache_age > HEDGE_STALE_CACHE_MS:
            self._diag_hedge_skipped_stale += 1
            # Force immediate refresh for this slug
            if not self._pending_fetches.get(m.slug):
                for mkt in self._cached_markets:
                    if mkt.slug == m.slug:
                        self._submit_market_refresh(mkt)
                        self._bg_next_due[m.slug] = now_t + 0.1
                        break
            write_jsonl({"event_type": "HEDGE_SKIPPED_STALE",
                          "ts_ms": int(now_t * 1000),
                          "slug": m.slug, "crypto": m.crypto,
                          "cache_age_ms": round(cache_age, 0),
                          "age_ms": round(age_ms, 0)})
            return  # Do NOT escalate/cross on stale data

        # Compute net edge for escalation decisions
        our_filled_price = st.positions[filled_outcome].vwap
        fee_cents = 2 * MAKER_FEE_BPS / 100.0
        target_edge = self._quote_get_dynamic_target(m.slug)
        dynamic_step_usd = self._quote_get_dynamic_step(m.slug)

        def _edge_ok(price: float) -> Tuple[bool, float]:
            est_combined = our_filled_price + price
            edge_c = (1.000 - est_combined) * 100 - fee_cents
            return edge_c >= target_edge * 0.3, edge_c

        # ── Tick 1 escalation: +1 tick at HEDGE_TICK1_MS ──
        _hedge_esc_enabled = HEDGE_TICK_ESCALATION_ENABLED or (MODE == "LIVE" and OM_HEDGE_ESCALATION_LIVE)
        if _hedge_esc_enabled and age_ms >= OM_HEDGE_TICK1_MS and not hedge["tick1_done"] and missing_book.bid > 0:
            esc_price = clamp_to_tick(missing_book.bid + 0.001)
            ok, edge_c = _edge_ok(esc_price)
            if ok and esc_price < missing_book.ask:
                leg_usd = min(dynamic_step_usd,
                              (PARITY_QUOTE_MAX_USD_PER_SLUG -
                               self._parity_invested_usd.get(m.slug, 0.0)) / 2.0)
                if leg_usd >= MIN_ORDER_USDC:
                    cost = self._parity_quote_buy(
                        m, st, missing_outcome, missing_book,
                        esc_price, leg_usd, ctx,
                        uuid.uuid4().hex[:16],
                        quote_step_usd_used=dynamic_step_usd)
                    if cost > 0:
                        self._diag_hedge_tick1 += 1
                        self._diag_quote_unpaired_escalations += 1
                        self._diag_quote_orders_placed += 1
            hedge["tick1_done"] = True
            write_jsonl({"event_type": "HEDGE_TICK1",
                          "ts_ms": int(now_t * 1000),
                          "slug": m.slug, "crypto": m.crypto,
                          "missing_outcome": missing_outcome,
                          "age_ms": round(age_ms, 0),
                          "cache_age_ms": round(cache_age, 0),
                          "edge_cents": round(edge_c, 3)})

        # ── Tick 2 escalation: +3 ticks at HEDGE_TICK2_MS ──
        if _hedge_esc_enabled and age_ms >= OM_HEDGE_TICK2_MS and not hedge["tick2_done"] and missing_book.bid > 0:
            esc_price = clamp_to_tick(missing_book.bid + 0.003)
            ok, edge_c = _edge_ok(esc_price)
            if ok and esc_price < missing_book.ask:
                leg_usd = min(dynamic_step_usd,
                              (PARITY_QUOTE_MAX_USD_PER_SLUG -
                               self._parity_invested_usd.get(m.slug, 0.0)) / 2.0)
                if leg_usd >= MIN_ORDER_USDC:
                    cost = self._parity_quote_buy(
                        m, st, missing_outcome, missing_book,
                        esc_price, leg_usd, ctx,
                        uuid.uuid4().hex[:16],
                        quote_step_usd_used=dynamic_step_usd)
                    if cost > 0:
                        self._diag_hedge_tick2 += 1
                        self._diag_quote_unpaired_escalations += 1
                        self._diag_quote_orders_placed += 1
            hedge["tick2_done"] = True
            write_jsonl({"event_type": "HEDGE_TICK2",
                          "ts_ms": int(now_t * 1000),
                          "slug": m.slug, "crypto": m.crypto,
                          "missing_outcome": missing_outcome,
                          "age_ms": round(age_ms, 0),
                          "cache_age_ms": round(cache_age, 0),
                          "edge_cents": round(edge_c, 3)})

        # ── Early taker cross: at OM_HEDGE_CROSS_MS (500ms in LIVE) — primary completion ──
        _cross_ms = OM_HEDGE_CROSS_MS if (MODE == "LIVE" and OM_HEDGE_ESCALATION_LIVE) else HEDGE_EARLY_CROSS_MS
        _cross_min_edge = OM_HEDGE_CROSS_MIN_EDGE_CENTS if (MODE == "LIVE" and OM_HEDGE_ESCALATION_LIVE) else HEDGE_EARLY_CROSS_EDGE_CENTS
        _cross_max_spread = OM_HEDGE_CROSS_MAX_SPREAD_CENTS if (MODE == "LIVE" and OM_HEDGE_ESCALATION_LIVE) else HEDGE_MAX_CROSS_SPREAD_CENTS
        if (age_ms >= _cross_ms and not hedge.get("early_cross_done")
                and not hedge["cross_done"] and missing_book.ask > 0):
            cross_price = missing_book.ask
            spread_cents = (missing_book.ask - missing_book.bid) * 100 if missing_book.bid > 0 else 999
            ok, edge_c = _edge_ok(cross_price)
            if edge_c >= _cross_min_edge and spread_cents <= _cross_max_spread:
                leg_usd = min(dynamic_step_usd,
                              (PARITY_QUOTE_MAX_USD_PER_SLUG -
                               self._parity_invested_usd.get(m.slug, 0.0)) / 2.0)
                if leg_usd >= MIN_ORDER_USDC:
                    cost = self._parity_buy_leg(
                        m, st, missing_outcome, missing_book,
                        leg_usd, ctx,
                        pair_id=uuid.uuid4().hex[:16])
                    if cost > 0:
                        self._diag_hedge_cross += 1
                        self._diag_hedge_cross_early += 1
                        self._diag_quote_orders_placed += 1
                        hedge["cross_done"] = True  # skip late cross too
                        write_jsonl({"event_type": "HEDGE_CROSS_EARLY",
                                      "ts_ms": int(now_t * 1000),
                                      "slug": m.slug, "crypto": m.crypto,
                                      "missing_outcome": missing_outcome,
                                      "cross_price": round(cross_price, 4),
                                      "edge_cents": round(edge_c, 3),
                                      "spread_cents": round(spread_cents, 2),
                                      "cache_age_ms": round(cache_age, 0),
                                      "age_ms": round(age_ms, 0)})
            hedge["early_cross_done"] = True

        # ── Late taker cross: at HEDGE_CROSS_MS (800ms) — fallback completion ──
        if age_ms >= HEDGE_CROSS_MS and not hedge["cross_done"] and missing_book.ask > 0:
            cross_price = missing_book.ask
            spread_cents = (missing_book.ask - missing_book.bid) * 100 if missing_book.bid > 0 else 999
            ok, edge_c = _edge_ok(cross_price)
            if edge_c >= HEDGE_MIN_CROSS_EDGE_CENTS and spread_cents <= HEDGE_MAX_CROSS_SPREAD_CENTS:
                leg_usd = min(dynamic_step_usd,
                              (PARITY_QUOTE_MAX_USD_PER_SLUG -
                               self._parity_invested_usd.get(m.slug, 0.0)) / 2.0)
                if leg_usd >= MIN_ORDER_USDC:
                    cost = self._parity_buy_leg(
                        m, st, missing_outcome, missing_book,
                        leg_usd, ctx,
                        pair_id=uuid.uuid4().hex[:16])
                    if cost > 0:
                        self._diag_hedge_cross += 1
                        self._diag_hedge_cross_late += 1
                        self._diag_quote_orders_placed += 1
                        write_jsonl({"event_type": "HEDGE_CROSS_LATE",
                                      "ts_ms": int(now_t * 1000),
                                      "slug": m.slug, "crypto": m.crypto,
                                      "missing_outcome": missing_outcome,
                                      "cross_price": round(cross_price, 4),
                                      "edge_cents": round(edge_c, 3),
                                      "spread_cents": round(spread_cents, 2),
                                      "cache_age_ms": round(cache_age, 0),
                                      "age_ms": round(age_ms, 0)})
            else:
                # Edge collapsed — unwind the filled leg (maker-first with TTL)
                filled_pos = st.positions[filled_outcome]
                filled_book = up_book if filled_outcome == "Up" else dn_book
                if filled_pos.qty >= MIN_QTY and filled_book and filled_book.bid > 0:
                    unwind_price = max(filled_book.bid, filled_book.ask - 0.001)
                    self._do_sell(m, st, filled_outcome, filled_pos.qty, unwind_price,
                                  reason="HEDGE_EDGE_COLLAPSE_UNWIND", leg="HEDGE",
                                  ctx=ctx, use_maker=True)
                    self._diag_hedge_unwind += 1
                # Also cancel any resting missing-leg order
                for oid, oentry in list(self._om_open_orders.items()):
                    if (oentry["slug"] == m.slug and oentry["outcome"] == missing_outcome
                            and oentry["side"] == "BUY"):
                        self._om_cancel_order(oid, "hedge_edge_collapse")
                write_jsonl({"event_type": "HEDGE_EDGE_COLLAPSE",
                              "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "crypto": m.crypto,
                              "filled_outcome": filled_outcome,
                              "edge_cents": round(edge_c, 3),
                              "spread_cents": round(spread_cents, 2),
                              "cache_age_ms": round(cache_age, 0),
                              "age_ms": round(age_ms, 0)})
            hedge["cross_done"] = True

        # ── Timeout: after QUOTE_UNPAIRED_MAX_SEC, unwind and pause ──
        if age_sec >= QUOTE_UNPAIRED_MAX_SEC:
            pos = st.positions[filled_outcome]
            if pos.qty >= MIN_QTY:
                filled_book = up_book if filled_outcome == "Up" else dn_book
                if filled_book.bid > 0:
                    unwind_price = max(filled_book.bid, filled_book.ask - 0.001)
                    self._do_sell(m, st, filled_outcome, pos.qty, unwind_price,
                                  reason="QUOTE_UNPAIRED_UNWIND", leg="PARITY_QUOTE",
                                  ctx=ctx, use_maker=True)
                    self._diag_unpaired_unwind_usd += unwind_price * pos.qty
                    self._diag_hedge_unwind += 1
                    self._diag_unpaired_count_min += 1
            self._quote_paused_until[m.slug] = now_t + QUOTE_PAUSE_AFTER_UNPAIRED_SEC
            self._diag_quote_pause_count += 1
            self._diag_slug_timeouts[m.slug] = self._diag_slug_timeouts.get(m.slug, 0) + 1
            self._quote_unpaired.pop(m.slug, None)
            self._hedge_state.pop(m.slug, None)
            write_jsonl({"event_type": "QUOTE_UNPAIRED_TIMEOUT",
                          "slug": m.slug, "crypto": m.crypto,
                          "filled_outcome": filled_outcome,
                          "age_sec": round(age_sec, 1),
                          "pause_sec": QUOTE_PAUSE_AFTER_UNPAIRED_SEC})

    def _parity_quote_buy(self, m: MarketRef, st: MarketState,
                           outcome: str, book: BookTop, bid_price: float,
                           leg_usd: float, ctx: dict, pair_id: str,
                           quote_step_usd_used: float = 0.0) -> float:
        """Place a maker buy at bid_price for quoting mode. Returns cost if filled."""
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        pos = st.positions[outcome]
        placed_ts = time.time()

        order_qty = leg_usd / max(1e-9, bid_price)
        if order_qty < 1:
            return 0.0

        decision_id = new_decision_id()
        client_oid = new_order_id()
        if pos.position_id is None:
            pos.position_id = new_position_id()
            pos.trade_id = uuid.uuid4().hex
            pos.entry_decision_id = decision_id
            pos.parent_order_id = client_oid
            pos.entry_mid = book.mid
            pos.max_favorable_mid = book.mid
            pos.max_adverse_mid = book.mid

        up_book = ctx["up_book"]
        dn_book = ctx["dn_book"]
        bk_fields = self._book_fields(up_book, dn_book, outcome)

        self.logger.log_order_intent(
            engine="PARITY", reason="PARITY_QUOTE",
            decision_id=decision_id, position_id=pos.position_id,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=order_qty, target_price=bid_price,
            usdc_cost=leg_usd, ctx=ctx, book_fields=bk_fields,
        )
        self.logger.log_order_submit(
            engine="PARITY", reason="PARITY_QUOTE",
            decision_id=decision_id, position_id=pos.position_id,
            client_order_id=client_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=order_qty, target_price=bid_price,
            usdc_cost=leg_usd, ctx=ctx, book_fields=bk_fields,
            extra={"pair_id": pair_id, "placed_ts": placed_ts,
                   "quote_step_usd_used": round(quote_step_usd_used, 2)},
        )
        # ── Order lifecycle: track submit ──
        self._rate_limit_record(m.slug)
        self._true_cost_tx_count += 1
        placed_ts_ms = int(placed_ts * 1000)
        self._last_quote_oid = client_oid
        self._diag_quote_submit_count += 1
        self._clone_quote_submit_count += 1
        self._active_orders[client_oid] = {
            "slug": m.slug, "outcome": outcome, "side": "BUY",
            "price": bid_price, "qty": order_qty,
            "submit_ts": placed_ts, "submit_ts_ms": placed_ts_ms,
            "reason": "PARITY_QUOTE",
        }
        write_jsonl({"event_type": "ORDER_SUBMIT",
                      "ts_ms": placed_ts_ms,
                      "slug": m.slug, "outcome": outcome,
                      "order_id": client_oid, "pair_id": pair_id,
                      "price": round(bid_price, 4), "qty": round(order_qty, 1),
                      "reason": "PARITY_QUOTE"})
        # Cancel any previous quote for this slug+outcome (ORDER_REPLACE)
        slug_quotes = self._parity_quotes.get(m.slug, {})
        prev_quote = slug_quotes.get(outcome)
        if prev_quote and prev_quote.get("order_id"):
            old_oid = prev_quote["order_id"]
            if old_oid in self._active_orders:
                old_info = self._active_orders.pop(old_oid)
                self._diag_quote_cancel_count += 1
                self._diag_quote_replace_count += 1
                self._clone_quote_cancel_count += 1
                self._clone_quote_replace_count += 1
                write_jsonl({"event_type": "ORDER_REPLACE",
                              "ts_ms": placed_ts_ms,
                              "slug": m.slug, "outcome": outcome,
                              "old_id": old_oid, "new_id": client_oid,
                              "old_px": round(old_info["price"], 4),
                              "new_px": round(bid_price, 4),
                              "reason": "refresh"})

        if MODE == "LOG":
            fill_ts = time.time()
            fill_ts_ms = int(fill_ts * 1000)
            # In paper mode: maker fill simulated — fill at our bid_price
            # Only fill if our bid >= current best bid (we'd be at top of book)
            if bid_price < book.bid - 0.001:
                # Our bid is below best bid — unlikely to fill, skip
                return 0.0
            actual_cost = bid_price * order_qty
            self._paper_buy(st, outcome, bid_price, order_qty, actual_cost)
            fill_latency_ms = (fill_ts - placed_ts) * 1000
            notional = bid_price * order_qty
            fee = compute_fee_usdc(notional, "maker")
            self.logger.log_order_fill(
                engine="PARITY", reason="PARITY_QUOTE",
                decision_id=decision_id, client_order_id=client_oid,
                position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=order_qty, fill_price=bid_price,
                usdc_cost=actual_cost, fees_usdc=fee,
                maker_taker="maker", did_cross="",
                vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                extra={"placed_ts_ms": placed_ts_ms, "fill_ts_ms": fill_ts_ms,
                       "fill_latency_ms": round(fill_latency_ms, 1),
                       "quote_step_usd_used": round(quote_step_usd_used, 2)},
            )
            write_jsonl({"event_type": "ORDER_FILL",
                          "ts_ms": fill_ts_ms,
                          "slug": m.slug, "outcome": outcome,
                          "order_id": client_oid, "pair_id": pair_id,
                          "price": round(bid_price, 4), "qty": round(order_qty, 1),
                          "maker_taker": "maker",
                          "fill_latency_ms": round(fill_latency_ms, 1)})
            self._tempo_fills[m.slug] = self._tempo_fills.get(m.slug, 0) + 1
            self._diag_maker_fills += 1
            self._diag_quote_fills += 1
            self._clone_quote_fill_count += 1
            self._diag_parity_fills_min += 1
            self._diag_total_fills_min += 1
            self._throttle_record_trade()
            self._diag_maker_fill_latencies.append(fill_latency_ms)
            self._active_orders.pop(client_oid, None)
            self._diag_maker_queue_times.append(fill_latency_ms)
            self._record_pair_fill(pair_id, m.slug, m.crypto, outcome, fill_ts, "maker")
            # Signal-to-fill tracking for clone metrics
            last_move = self._clone_last_spot_move.get(m.slug)
            if last_move:
                s2f = (fill_ts - last_move[0]) * 1000
                if s2f > 0 and s2f < 30000:
                    self._clone_signal_to_fill.append(s2f)
            self._parity_invested_usd[m.slug] = self._parity_invested_usd.get(m.slug, 0.0) + actual_cost
            self._parity_last_order_ts[m.slug] = time.time()

            # If both legs now filled, log straddle completion
            if (st.positions["Up"].qty >= MIN_QTY
                    and st.positions["Down"].qty >= MIN_QTY):
                up_vwap = st.positions["Up"].vwap
                dn_vwap = st.positions["Down"].vwap
                straddle_cost = up_vwap + dn_vwap
                net_edge = (1.000 - straddle_cost) * 100
                self._diag_parity_edges.append(net_edge)
                write_jsonl({"event_type": "PARITY_QUOTE_STRADDLE",
                              "ts_ms": fill_ts_ms,
                              "slug": m.slug, "crypto": m.crypto,
                              "pair_id": pair_id,
                              "straddle_cost": round(straddle_cost, 4),
                              "net_edge_cents": round(net_edge, 3),
                              "up_vwap": round(up_vwap, 4),
                              "dn_vwap": round(dn_vwap, 4)})
            else:
                # One leg filled but not balanced — immediately trigger hedge
                up_q = st.positions["Up"].qty
                dn_q = st.positions["Down"].qty
                if (up_q >= MIN_QTY or dn_q >= MIN_QTY) and m.slug not in self._quote_unpaired:
                    imbal = abs(up_q - dn_q)
                    one_sided = (up_q < MIN_QTY) != (dn_q < MIN_QTY)
                    big_imbal = imbal >= MIN_PAIR_QTY and max(up_q, dn_q) > min(up_q, dn_q) * 2.0
                    if one_sided or big_imbal:
                        heavier = "Up" if up_q > dn_q else "Down"
                        self._quote_unpaired[m.slug] = {
                            "outcome": heavier,
                            "fill_ts": fill_ts,
                            "escalated": False,
                        }
                        self._diag_quote_unpaired_events += 1

            return actual_cost
        else:
            # LIVE mode: post maker bid
            fill = self.client.place_limit_order(token_id, "BUY", bid_price,
                                                  order_qty, post_only=True)
            fill_ts = time.time()
            if fill.get("filled"):
                fill_latency_ms = (fill_ts - placed_ts) * 1000
                actual_cost = fill["fill_price"] * fill["fill_qty"]
                self._live_buy(st, outcome, fill["fill_price"], fill["fill_qty"], actual_cost)
                notional = fill["fill_price"] * fill["fill_qty"]
                fee = compute_fee_usdc(notional, "maker")
                self.logger.log_order_fill(
                    engine="PARITY", reason="PARITY_QUOTE",
                    decision_id=decision_id, client_order_id=client_oid,
                    position_id=pos.position_id,
                    crypto=m.crypto, slug=m.slug, outcome=outcome,
                    side="BUY", qty=fill["fill_qty"], fill_price=fill["fill_price"],
                    usdc_cost=actual_cost, fees_usdc=fee,
                    maker_taker="maker", did_cross="",
                    vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                    extra={"placed_ts": placed_ts, "fill_ts": fill_ts,
                           "fill_latency_ms": round(fill_latency_ms, 1),
                           "quote_step_usd_used": round(quote_step_usd_used, 2)},
                )
                self._tempo_fills[m.slug] = self._tempo_fills.get(m.slug, 0) + 1
                self._diag_maker_fills += 1
                self._diag_quote_fills += 1
                self._clone_quote_fill_count += 1
                self._diag_parity_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._diag_maker_fill_latencies.append(fill_latency_ms)
                self._active_orders.pop(client_oid, None)
                self._diag_maker_queue_times.append(fill_latency_ms)
                self._record_pair_fill(pair_id, m.slug, m.crypto, outcome, fill_ts, "maker")
                last_move = self._clone_last_spot_move.get(m.slug)
                if last_move:
                    s2f = (fill_ts - last_move[0]) * 1000
                    if s2f > 0 and s2f < 30000:
                        self._clone_signal_to_fill.append(s2f)
                self._parity_invested_usd[m.slug] = self._parity_invested_usd.get(m.slug, 0.0) + actual_cost
                self._parity_last_order_ts[m.slug] = time.time()
                return actual_cost
            # Order not filled — track in order manager so it doesn't become orphan
            oid = fill.get("order_id", "")
            if oid:
                now_ms = int(time.time() * 1000)
                self._om_open_orders[oid] = {
                    "slug": m.slug, "outcome": outcome, "side": "BUY",
                    "price": bid_price, "qty": int(float(order_qty)),
                    "filled_qty": fill.get("fill_qty", 0) or 0,
                    "reason": "PARITY_QUOTE", "maker": True,
                    "created_ms": now_ms, "last_check_ms": now_ms,
                    "status": "open", "token_id": token_id,
                    "st_slug": m.slug, "cancel_pending": False,
                }
                write_jsonl({"event_type": "OM_PARITY_QUOTE_RESTING",
                              "order_id": oid, "slug": m.slug, "outcome": outcome,
                              "price": bid_price, "qty": int(float(order_qty))})
            return 0.0

    def _parity_buy_leg(self, m: MarketRef, st: MarketState,
                        outcome: str, book: BookTop,
                        leg_usd: float, ctx: dict,
                        pair_id: str = "") -> float:
        """Execute one leg of a parity buy. Returns cost (USDC) of filled order.
        Uses maker-first: taker only if spread <= 1c. Tracks maker queue discipline
        including fill latency, timeout cancels, and lost-best-price detection."""
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        pos = st.positions[outcome]
        placed_ts = time.time()

        # Maker/taker decision
        use_taker = (book.spread * 100) <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
        if use_taker:
            order_price = book.ask
            self._diag_parity_taker_count += 1
        else:
            # Maker queue discipline: only replace if price changed by >= 1 tick
            order_price = book.bid
            maker_state = self._parity_maker_orders.get(m.slug, {}).get(outcome)
            if maker_state:
                price_diff = abs(order_price - maker_state.get("price", 0))
                elapsed_ms = (placed_ts - maker_state.get("last_replace_ts", 0)) * 1000

                # Detect: our order is no longer best price (price moved away)
                if price_diff >= 0.005 and order_price > maker_state.get("price", 0):
                    self._diag_maker_lost_best_count += 1

                # Detect: maker timeout (unfilled after MAKER_ORDER_TIMEOUT_MS)
                if elapsed_ms >= MAKER_ORDER_TIMEOUT_MS and not maker_state.get("filled"):
                    self._diag_maker_timeout_cancel_count += 1

                if price_diff < 0.005 and elapsed_ms < MIN_REPLACE_INTERVAL_MS:
                    # Price hasn't moved enough and interval not met — skip replace
                    return 0.0
                if price_diff >= 0.005 or elapsed_ms >= MIN_REPLACE_INTERVAL_MS:
                    self._diag_cancel_replace_count += 1
            self._diag_parity_maker_count += 1
            self._diag_maker_orders_placed += 1
            # Track maker order state with timestamps
            self._parity_maker_orders.setdefault(m.slug, {})[outcome] = {
                "price": order_price,
                "last_replace_ts": placed_ts,
                "placed_ts": placed_ts,
                "first_not_best_ts": None,
                "filled": False,
            }

        order_qty = leg_usd / max(1e-9, order_price)
        if order_qty < 1:
            return 0.0

        decision_id = new_decision_id()
        client_oid = new_order_id()
        if pos.position_id is None:
            pos.position_id = new_position_id()
            pos.trade_id = uuid.uuid4().hex
            pos.entry_decision_id = decision_id
            pos.parent_order_id = client_oid
            pos.entry_mid = book.mid
            pos.max_favorable_mid = book.mid
            pos.max_adverse_mid = book.mid

        up_book = ctx["up_book"]
        dn_book = ctx["dn_book"]
        bk_fields = self._book_fields(up_book, dn_book, outcome)
        mt = "taker" if use_taker else "maker"

        self.logger.log_order_intent(
            engine="PARITY", reason="PARITY_BUY",
            decision_id=decision_id, position_id=pos.position_id,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=order_qty, target_price=order_price,
            usdc_cost=leg_usd, ctx=ctx, book_fields=bk_fields,
        )
        self.logger.log_order_submit(
            engine="PARITY", reason="PARITY_BUY",
            decision_id=decision_id, position_id=pos.position_id,
            client_order_id=client_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=order_qty, target_price=order_price,
            usdc_cost=leg_usd, ctx=ctx, book_fields=bk_fields,
            extra={"pair_id": pair_id, "placed_ts": placed_ts} if pair_id else {"placed_ts": placed_ts},
        )

        if MODE == "LOG":
            fill_ts = time.time()
            self._paper_buy(st, outcome, order_price, order_qty, leg_usd)
            sc = spread_capture_fields("BUY", order_price, book)
            fill_latency_ms = (fill_ts - placed_ts) * 1000
            _fee = compute_fee_usdc(leg_usd, mt)
            self.logger.log_order_fill(
                engine="PARITY", reason="PARITY_BUY",
                decision_id=decision_id, client_order_id=client_oid,
                position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=order_qty, fill_price=order_price,
                usdc_cost=leg_usd, fees_usdc=_fee,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                extra={"placed_ts": placed_ts, "fill_ts": fill_ts,
                       "fill_latency_ms": round(fill_latency_ms, 1)},
            )
            self._tempo_fills[m.slug] = self._tempo_fills.get(m.slug, 0) + 1
            if mt == "maker":
                self._diag_maker_fills += 1
                self._diag_maker_fill_latencies.append(fill_latency_ms)
                # Mark maker order as filled
                ms = self._parity_maker_orders.get(m.slug, {}).get(outcome)
                if ms:
                    ms["filled"] = True
            return leg_usd
        else:
            post_only = not use_taker
            fill = self.client.place_limit_order(token_id, "BUY", order_price,
                                                  order_qty, post_only=post_only)
            fill_ts = time.time()
            if fill.get("filled"):
                fill_latency_ms = (fill_ts - placed_ts) * 1000
                actual_cost = fill["fill_price"] * fill["fill_qty"]
                self._live_buy(st, outcome, fill["fill_price"], fill["fill_qty"], actual_cost)
                sc = spread_capture_fields("BUY", fill["fill_price"], book)
                actual_mt = infer_maker_taker("BUY", fill["fill_price"], book)
                _fee = compute_fee_usdc(actual_cost, actual_mt)
                self.logger.log_order_fill(
                    engine="PARITY", reason="PARITY_BUY",
                    decision_id=decision_id, client_order_id=client_oid,
                    position_id=pos.position_id,
                    crypto=m.crypto, slug=m.slug, outcome=outcome,
                    side="BUY", qty=fill["fill_qty"], fill_price=fill["fill_price"],
                    usdc_cost=actual_cost, fees_usdc=_fee,
                    maker_taker=actual_mt, did_cross=sc.get("did_cross", ""),
                    vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                    extra={"placed_ts": placed_ts, "fill_ts": fill_ts,
                           "fill_latency_ms": round(fill_latency_ms, 1)},
                )
                self._tempo_fills[m.slug] = self._tempo_fills.get(m.slug, 0) + 1
                if actual_mt == "maker":
                    self._diag_maker_fills += 1
                    self._diag_maker_fill_latencies.append(fill_latency_ms)
                    ms = self._parity_maker_orders.get(m.slug, {}).get(outcome)
                    if ms:
                        ms["filled"] = True
                return actual_cost
            # Order not filled — track in order manager so it doesn't become orphan
            oid = fill.get("order_id", "")
            if oid:
                now_ms = int(time.time() * 1000)
                self._om_open_orders[oid] = {
                    "slug": m.slug, "outcome": outcome, "side": "BUY",
                    "price": order_price, "qty": int(float(order_qty)),
                    "filled_qty": fill.get("fill_qty", 0) or 0,
                    "reason": "PARITY_BUY", "maker": not use_taker,
                    "created_ms": now_ms, "last_check_ms": now_ms,
                    "status": "open", "token_id": token_id,
                    "st_slug": m.slug, "cancel_pending": False,
                }
                write_jsonl({"event_type": "OM_PARITY_LEG_RESTING",
                              "order_id": oid, "slug": m.slug, "outcome": outcome,
                              "price": order_price, "qty": int(float(order_qty))})
            return 0.0

    # =================================================================
    # END-OF-HOUR PARITY FLATTENING
    # =================================================================
    def _parity_flatten_eoh(self, m: MarketRef, st: MarketState,
                             t_min: float, ctx: dict):
        """Flatten all parity (locked + unpaired) inventory as hour ends.
        PARITY_FLATTEN_START_MIN..PARITY_HARD_FLATTEN_MIN: maker-first with 200ms refresh.
        After PARITY_HARD_FLATTEN_MIN: taker if time_to_close<20s or emergency inventory."""
        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        seconds_to_close = ctx.get("seconds_to_close", 999.0)
        hard_mode = t_min >= PARITY_HARD_FLATTEN_MIN

        # Safety Item 4: In LIVE hard mode, use _om_flatten_hard for guaranteed flatten
        if hard_mode and MODE != "LOG" and seconds_to_close < 20.0:
            self._om_flatten_hard(m, st, ctx)
            return

        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty < MIN_QTY:
                continue
            book = up_book if outcome == "Up" else dn_book
            if book.bid <= 0:
                continue

            # Determine maker vs taker
            allow_taker = False
            if hard_mode:
                if seconds_to_close < 20.0:
                    allow_taker = True
                elif pos.qty >= INVENTORY_EMERGENCY_SHARES:
                    allow_taker = True
            spread_ok = (book.spread * 100) <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
            use_taker = allow_taker and spread_ok

            # Maker queue discipline: respect MIN_REPLACE_INTERVAL_MS
            if not use_taker:
                maker_state = self._parity_maker_orders.get(m.slug, {}).get(outcome)
                if maker_state:
                    elapsed_ms = (time.time() - maker_state.get("last_replace_ts", 0)) * 1000
                    price_diff = abs(book.bid - maker_state.get("price", 0))
                    if price_diff < 0.005 and elapsed_ms < PARITY_MAKER_REFRESH_MS:
                        continue  # not time to replace yet

            # Sell quantity: in hard mode, sell everything; otherwise step-based
            if hard_mode:
                sell_qty = pos.qty
            else:
                sell_qty = min(pos.qty, RECYCLE_STEP_USD / max(1e-9, book.bid))
            if sell_qty < MIN_QTY:
                continue

            if use_taker:
                sell_price = book.bid
                self._diag_flatten_taker += 1
                self._diag_parity_taker_count += 1
            else:
                sell_price = max(book.bid, book.ask - 0.001)
                self._diag_parity_maker_count += 1
                # Update maker order tracking
                self._parity_maker_orders.setdefault(m.slug, {})[outcome] = {
                    "price": book.bid,
                    "last_replace_ts": time.time(),
                    "placed_ts": time.time(),
                    "first_not_best_ts": None,
                    "filled": False,
                }

            self._diag_flatten_actions += 1
            flatten_reason = "PARITY_HARD_FLATTEN" if hard_mode else "PARITY_FLATTEN"

            self._do_sell(m, st, outcome, sell_qty, sell_price,
                          reason=flatten_reason, leg="PARITY_FLATTEN",
                          ctx=ctx, use_maker=not use_taker)

            write_jsonl({"event_type": flatten_reason,
                          "slug": m.slug, "crypto": m.crypto,
                          "outcome": outcome, "sell_qty": round(sell_qty, 1),
                          "sell_price": round(sell_price, 4),
                          "use_taker": use_taker,
                          "seconds_to_close": round(seconds_to_close, 1),
                          "t_min": round(t_min, 3)})

        # Also flatten any pending partial fills by unwinding
        resolved = []
        for i, pair in enumerate(self._parity_pending_pairs):
            if pair["slug"] != m.slug:
                continue
            filled_book = up_book if pair["filled_outcome"] == "Up" else dn_book
            pos = st.positions[pair["filled_outcome"]]
            unwind_qty = min(pair.get("filled_qty", 0), pos.qty)
            if unwind_qty >= MIN_QTY and filled_book.bid > 0:
                use_taker_unwind = hard_mode and (seconds_to_close < 20.0 or spread_ok)
                unwind_price = filled_book.bid if use_taker_unwind else max(filled_book.bid, filled_book.ask - 0.001)
                self._do_sell(m, st, pair["filled_outcome"], unwind_qty, unwind_price,
                              reason="PARITY_FLATTEN_UNPAIRED", leg="PARITY_FLATTEN",
                              ctx=ctx, use_maker=not use_taker_unwind)
                self._diag_flatten_actions += 1
                if use_taker_unwind:
                    self._diag_flatten_taker += 1
            resolved.append(i)
        for idx in sorted(resolved, reverse=True):
            self._parity_pending_pairs.pop(idx)

        # Clear locked tracking if fully flat
        up_q = st.positions["Up"].qty
        dn_q = st.positions["Down"].qty
        if up_q < MIN_QTY and dn_q < MIN_QTY:
            self._parity_locked_since.pop(m.slug, None)
            self._parity_invested_usd.pop(m.slug, None)

    # =================================================================
    # DIRECTIONAL LEAN EXIT OVERLAY
    # =================================================================
    def _directional_lean_exits(self, m: MarketRef, st: MarketState,
                                 t_min: float, delta_bps: float, ctx: dict):
        """Directional lean: prioritize exits on the "wrong" side of spot vs hour_open.
        If spot > hour_open (up trend): unload Down inventory first.
        If spot < hour_open (down trend): unload Up inventory first.
        Only applies to imbalanced positions."""
        if not LEAN_EXIT_PRIORITY:
            return
        spot, hour_open = ctx["spot"], ctx["hour_open"]
        lean_up = spot >= hour_open

        pos_up = st.positions["Up"]
        pos_dn = st.positions["Down"]
        imbalance = pos_up.qty - pos_dn.qty  # positive = more Up than Down

        # Determine which side to preferentially exit
        if lean_up:
            # Up trend: prefer holding Up, unload excess Down
            wrong_side = "Down"
            wrong_pos = pos_dn
            right_pos = pos_up
        else:
            # Down trend: prefer holding Down, unload excess Up
            wrong_side = "Up"
            wrong_pos = pos_up
            right_pos = pos_dn

        # Only act if we have "wrong side" inventory exceeding the right side
        if wrong_pos.qty < MIN_QTY:
            return

        book = self.last_book.get(m.slug, {}).get(wrong_side)
        if not book or book.bid <= 0:
            return

        # Sell excess on wrong side to bring closer to balance
        excess = wrong_pos.qty - right_pos.qty
        if excess <= 0:
            return  # already balanced or right-side heavy

        # Sell up to LEAN_MAX_IMBALANCE_SHARES reduction, but at most 25% of wrong-side
        sell_qty = min(excess, wrong_pos.qty * 0.25, float(LEAN_MAX_IMBALANCE_SHARES))
        if sell_qty < MIN_QTY:
            return

        # Only sell if we're profitable on this side (don't force a loss)
        if book.bid < wrong_pos.vwap:
            return

        # Use maker when spread > 1c
        use_taker = (book.spread * 100) <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
        sell_price = book.bid if use_taker else max(book.bid, book.ask - 0.001)

        write_jsonl({"event_type": "LEAN_EXIT", "slug": m.slug, "crypto": m.crypto,
                      "wrong_side": wrong_side, "sell_qty": round(sell_qty, 1),
                      "imbalance": round(pos_up.qty - pos_dn.qty, 1),
                      "lean": "Up" if lean_up else "Down",
                      "t_min": round(t_min, 3)})

        self._do_sell(m, st, wrong_side, sell_qty, sell_price,
                      reason="LEAN_EXIT", leg="LEAN_EXIT",
                      ctx=ctx, use_maker=not use_taker)

    def _late_scalps(self, ctx: dict):
        m, st = ctx["m"], ctx["st"]
        t_min = ctx["t_min"]
        delta_bps, abs_delta_bps = ctx["delta_bps"], ctx["abs_delta_bps"]
        up_book, dn_book = ctx["up_book"], ctx["dn_book"]
        spot = ctx["spot"]
        if not (LATE_SCALP_T_START <= t_min <= LATE_SCALP_T_END):
            return
        if abs_delta_bps < LATE_SCALP_ABSDELTA_MIN or abs_delta_bps > LATE_SCALP_ABSDELTA_MAX:
            return
        if min(up_book.ask, dn_book.ask) > LATE_SCALP_PRICE_MAX:
            return
        outcome = "Up" if up_book.ask < dn_book.ask else "Down"
        book = up_book if outcome == "Up" else dn_book
        if book.spread > IMB_MAX_SPREAD:
            return
        now_iso = iso_z(utc_now())
        if st.last_reentry_ts:
            last = datetime.strptime(st.last_reentry_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if (utc_now() - last).total_seconds() < REENTRY_COOLDOWN_SEC:
                return
        clip = self._calc_clip(m.crypto, t_min, abs_delta_bps) * 0.50
        if clip < MIN_ORDER_USDC:
            return
        qty = clip / max(1e-9, book.ask)
        target_sell = min(0.999, book.ask + LATE_SCALP_TP_CENTS)
        decision_id = new_decision_id()
        client_oid = new_order_id()
        mult = sizing_mult(abs_delta_bps)
        pos = st.positions[outcome]
        if pos.position_id is None:
            pos.position_id = new_position_id()
            pos.trade_id = uuid.uuid4().hex
            pos.entry_decision_id = decision_id
            pos.parent_order_id = client_oid
            pos.entry_mid = book.mid
            pos.max_favorable_mid = book.mid
            pos.max_adverse_mid = book.mid
        ctx["size_mult"] = mult
        bk_fields = self._book_fields(up_book, dn_book, outcome)
        self.logger.log_order_intent(
            engine="LATE_SCALP", reason="ENTRY_SCALP",
            decision_id=decision_id, position_id=pos.position_id,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=qty, target_price=book.ask,
            usdc_cost=clip, ctx=ctx, book_fields=bk_fields,
            extra={"notes": f"target_sell={target_sell:.3f}"},
        )
        self.logger.log_order_submit(
            engine="LATE_SCALP", reason="ENTRY_SCALP",
            decision_id=decision_id, position_id=pos.position_id,
            client_order_id=client_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=qty, target_price=book.ask,
            usdc_cost=clip, ctx=ctx, book_fields=bk_fields,
        )
        st.last_reentry_ts = now_iso
        if MODE == "LOG":
            self._paper_buy(st, outcome, book.ask, qty, clip)
            mt = infer_maker_taker("BUY", book.ask, book)
            sc = spread_capture_fields("BUY", book.ask, book)
            _fee = compute_fee_usdc(clip, mt)
            self.logger.log_order_fill(
                engine="LATE_SCALP", reason="ENTRY_SCALP",
                decision_id=decision_id, client_order_id=client_oid,
                position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=qty, fill_price=book.ask,
                usdc_cost=clip, fees_usdc=_fee,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
            )
            pos.scalp_mode = True
            pos.scalp_open_ts = now_iso
            return
        fill = self._place_layered_buy(m, outcome, qty, book.ask)
        if fill["total_filled"] > 0:
            actual_qty = fill["total_filled"]
            actual_price = fill["avg_price"]
            actual_cost = fill["total_cost"]
            self._live_buy(st, outcome, actual_price, actual_qty, actual_cost)
            mt = infer_maker_taker("BUY", actual_price, book)
            sc = spread_capture_fields("BUY", actual_price, book)
            _fee = compute_fee_usdc(actual_cost, mt)
            self.logger.log_order_fill(
                engine="LATE_SCALP", reason="ENTRY_SCALP",
                decision_id=decision_id, client_order_id=client_oid,
                position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=actual_qty, fill_price=actual_price,
                usdc_cost=actual_cost, fees_usdc=_fee,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
            )
        pos.scalp_mode = True
        pos.scalp_open_ts = now_iso
    def _manage_exits(self, m: MarketRef, st: MarketState, t_min: float, delta_bps: float, ctx: dict):
        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty < MIN_QTY:
                self._clean_dust(pos)  # hard-zero any ghost positions
                continue
            book = self.last_book[m.slug].get(outcome)
            if not book:
                continue
            if pos.scalp_mode and pos.scalp_open_ts:
                opened = datetime.strptime(pos.scalp_open_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                if (utc_now() - opened).total_seconds() / 60.0 > LATE_SCALP_MAX_HOLD_MIN:
                    self._do_sell(m, st, outcome, pos.qty, book.bid,
                                  reason="SCALP_TIMEOUT", leg="SCALP_TIMEOUT", ctx=ctx)
                    pos.scalp_mode = False
                    continue
            if t_min >= TRADE_HARD_STOP_MIN:
                self._do_sell(m, st, outcome, pos.qty,
                              max(book.bid, book.ask - MAX_CROSS_SLIPPAGE),
                              reason="HARD_STOP", leg="STOP", ctx=ctx)
                continue

            # --- Inventory pressure: hard cap on shares per market ---
            inv_cap = INVENTORY_CAP_SHARES_PER_MARKET
            if pos.qty > inv_cap:
                # Immediately sell 50% (respecting derisk cooldown)
                sell_qty = pos.qty * 0.50
                if sell_qty >= MIN_QTY:
                    should_inv_sell = False
                    now_t = utc_now()
                    if pos.last_derisk_ts is None:
                        should_inv_sell = True
                    else:
                        try:
                            last_dt = datetime.strptime(pos.last_derisk_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                            elapsed = (now_t - last_dt).total_seconds()
                        except Exception:
                            elapsed = 999.0
                        if elapsed >= DERISK_COOLDOWN_SEC:
                            should_inv_sell = True
                    if should_inv_sell:
                        write_jsonl({"event_type": "INVENTORY_PRESSURE_SELL",
                                      "slug": m.slug, "crypto": m.crypto,
                                      "outcome": outcome, "qty": round(pos.qty, 1),
                                      "cap": inv_cap, "sell_qty": round(sell_qty, 1)})
                        self._do_sell(m, st, outcome, sell_qty, book.bid,
                                      reason="INVENTORY_CAP", leg="INVENTORY_CAP", ctx=ctx)
                        pos.last_derisk_ts = iso_z(now_t)
                        pos.last_derisk_mid = book.mid
                continue

            # Inventory pressure: tighten TP ladder if > 70% cap
            tp_tighten = 0.0
            if pos.qty > inv_cap * 0.70:
                tp_tighten = 0.01  # tighten by 1 cent
                write_jsonl({"event_type": "INVENTORY_TP_TIGHTEN",
                              "slug": m.slug, "outcome": outcome,
                              "qty": round(pos.qty, 1), "tp_tighten": tp_tighten})

            # --- DERISK with RESCUE-TO-STRADDLE (cooldown + change detection) ---
            # Skip derisk entirely during directional hold floor
            _dpos_derisk = self._dscalp_positions.get(m.slug)
            if _dpos_derisk and (time.time() - _dpos_derisk["entry_ts"]) < DSCALP_MIN_HOLD_SEC:
                continue  # protect directional exposure during hold floor
            derisk_triggered = False
            if outcome == "Up" and delta_bps < +DERISK_CROSS_BPS:
                derisk_triggered = True
            elif outcome == "Down" and delta_bps > -DERISK_CROSS_BPS:
                derisk_triggered = True
            if derisk_triggered:
                sell_qty = pos.qty * DERISK_SELL_FRAC_PER_TICK
                if sell_qty >= MIN_QTY:
                    # Check cooldown: only fire if enough time passed OR mid moved meaningfully
                    should_derisk = False
                    now_t = utc_now()
                    if pos.last_derisk_ts is None:
                        should_derisk = True  # first derisk for this position
                    else:
                        try:
                            last_dt = datetime.strptime(pos.last_derisk_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                            elapsed = (now_t - last_dt).total_seconds()
                        except Exception:
                            elapsed = 999.0
                        mid_moved = abs(book.mid - pos.last_derisk_mid) >= DERISK_MID_CHANGE_CENTS
                        if elapsed >= DERISK_COOLDOWN_SEC or mid_moved:
                            should_derisk = True
                    if should_derisk:
                        # MAX_DERISK_PER_HOUR cap — prevent bleed from excessive derisking
                        if self._diag_derisk_count_hour >= MAX_DERISK_PER_HOUR:
                            write_jsonl({"event_type": "DERISK_HOUR_CAP_HIT",
                                          "slug": m.slug, "outcome": outcome,
                                          "derisk_count_hour": self._diag_derisk_count_hour,
                                          "cap": MAX_DERISK_PER_HOUR})
                            pos.last_derisk_ts = iso_z(now_t)
                            pos.last_derisk_mid = book.mid
                            continue  # skip this derisk entirely
                        self._diag_derisk_count += 1
                        self._diag_derisk_count_min += 1
                        self._diag_derisk_count_hour += 1
                        thr = entry_threshold_bps(m.crypto, t_min)
                        abs_edge = abs(ctx.get("delta_bps", 0))
                        seconds_to_close = ctx.get("seconds_to_close", 999.0)
                        emergency = False
                        if pos.qty >= INVENTORY_EMERGENCY_SHARES:
                            emergency = True
                        elif seconds_to_close < 45:
                            emergency = True

                        # ── RESCUE-TO-STRADDLE ──
                        opposite = "Down" if outcome == "Up" else "Up"
                        opp_pos = st.positions[opposite]
                        rescue_done = False

                        # Skip rescue entirely if position is already a balanced straddle
                        _up_q = st.positions["Up"].qty
                        _dn_q = st.positions["Down"].qty
                        _already_balanced = (_up_q >= MIN_PAIR_QTY and _dn_q >= MIN_PAIR_QTY
                                             and abs(_up_q - _dn_q) < MIN_PAIR_QTY)
                        if _already_balanced and not emergency:
                            # Balanced straddle — no rescue needed, no derisk sell
                            pos.last_derisk_ts = iso_z(now_t)
                            pos.last_derisk_mid = book.mid
                            continue

                        # Count every one-sided + drift reversal as a rescue attempt
                        self._diag_rescue_attempts += 1

                        # Compute rescue feasibility
                        up_book_r = self.last_book.get(m.slug, {}).get("Up")
                        dn_book_r = self.last_book.get(m.slug, {}).get("Down")
                        opp_book = (dn_book_r if opposite == "Down" else up_book_r) if (up_book_r and dn_book_r) else None
                        our_leg_price = clamp_to_tick(book.bid) if book.bid > 0 else 0.0
                        rescue_bid = clamp_to_tick(opp_book.bid) if (opp_book and opp_book.bid > 0) else 0.0
                        est_straddle_cost = our_leg_price + rescue_bid if (our_leg_price > 0 and rescue_bid > 0) else 0.0
                        rescue_edge_cents = (1.000 - est_straddle_cost) * 100 if est_straddle_cost > 0 else -999.0
                        fee_cents = 2 * MAKER_FEE_BPS / 100.0
                        rescue_net_edge = rescue_edge_cents - fee_cents
                        dyn_target = self._quote_get_dynamic_target(m.slug)
                        min_edge = max(RESCUE_MIN_EDGE_NET_CENTS, dyn_target * 0.5)
                        rescue_invested = self._rescue_invested_usd.get(m.slug, 0.0)

                        # Log RESCUE_CHECK for every attempt
                        write_jsonl({
                            "event_type": "RESCUE_CHECK",
                            "slug": m.slug, "crypto": m.crypto,
                            "outcome": outcome, "opposite": opposite,
                            "t_min": round(t_min, 3),
                            "our_qty": round(pos.qty, 1),
                            "opp_qty": round(opp_pos.qty, 1),
                            "our_bid": round(our_leg_price, 4),
                            "opp_bid": round(rescue_bid, 4),
                            "net_edge_cents": round(rescue_net_edge, 3),
                            "dyn_target_cents": round(dyn_target, 3),
                            "min_edge_used": round(min_edge, 3),
                            "emergency": emergency,
                            "rescue_invested": round(rescue_invested, 2),
                        })

                        # Determine rescue block reason (if any)
                        # "already_paired" = BOTH legs have substantial inventory
                        up_qty = st.positions["Up"].qty
                        dn_qty = st.positions["Down"].qty
                        straddle_locked = min(up_qty, dn_qty)
                        pending_pairs_count = len([p for p in self._parity_pending_pairs
                                                    if p.get("slug") == m.slug]) if hasattr(self, '_parity_pending_pairs') else 0
                        rescue_block_reason = None
                        # Block rescue during directional hold floor
                        _dpos = self._dscalp_positions.get(m.slug)
                        if _dpos and (time.time() - _dpos["entry_ts"]) < DSCALP_MIN_HOLD_SEC:
                            rescue_block_reason = "dscalp_hold_floor"
                        elif emergency:
                            rescue_block_reason = "emergency"
                        elif not DERISK_RESCUE_TO_STRADDLE:
                            rescue_block_reason = "disabled"
                        elif up_qty >= MIN_PAIR_QTY and dn_qty >= MIN_PAIR_QTY:
                            rescue_block_reason = "already_paired"
                        elif not opp_book or opp_book.bid <= 0:
                            rescue_block_reason = "no_liquidity"
                        elif not up_book_r or not dn_book_r or up_book_r.bid <= 0 or dn_book_r.bid <= 0:
                            rescue_block_reason = "stale_book"
                        elif rescue_invested >= RESCUE_MAX_USD_PER_SLUG:
                            rescue_block_reason = "cap_reached"
                        elif rescue_net_edge < min_edge:
                            rescue_block_reason = "threshold"
                        elif t_min >= PARITY_STOP_NEW_MIN:
                            rescue_block_reason = "near_close"

                        if rescue_block_reason is None:
                            # ── Execute rescue buy ──
                            rescue_usd = min(RESCUE_STEP_USD,
                                             RESCUE_MAX_USD_PER_SLUG - rescue_invested)
                            rescue_qty = rescue_usd / max(1e-9, rescue_bid)
                            if rescue_qty >= 1:
                                pair_id = uuid.uuid4().hex[:16]
                                cost = self._parity_buy_leg(
                                    m, st, opposite, opp_book,
                                    rescue_usd, ctx, pair_id=pair_id)
                                if cost > 0:
                                    self._rescue_invested_usd[m.slug] = rescue_invested + cost
                                    self._diag_rescue_success += 1
                                    rescue_done = True
                                    if (st.positions["Up"].qty >= MIN_QTY
                                            and st.positions["Down"].qty >= MIN_QTY):
                                        if m.slug not in self._parity_locked_since:
                                            self._parity_locked_since[m.slug] = time.time()
                                    write_jsonl({
                                        "event_type": "RESCUE_TRIGGERED",
                                        "slug": m.slug, "crypto": m.crypto,
                                        "pair_id": pair_id,
                                        "losing_outcome": outcome,
                                        "rescue_outcome": opposite,
                                        "step_usd": round(cost, 4),
                                        "rescue_price": round(rescue_bid, 4),
                                        "our_leg_price": round(our_leg_price, 4),
                                        "est_straddle_cost": round(est_straddle_cost, 4),
                                        "net_edge_cents": round(rescue_net_edge, 3),
                                        "t_min": round(t_min, 3),
                                    })
                            else:
                                rescue_block_reason = "qty_too_small"

                        if rescue_block_reason is not None:
                            write_jsonl({
                                "event_type": "RESCUE_BLOCKED",
                                "slug": m.slug, "crypto": m.crypto,
                                "reason": rescue_block_reason,
                                "outcome": outcome,
                                "up_qty": round(up_qty, 1),
                                "dn_qty": round(dn_qty, 1),
                                "min_pair_qty": MIN_PAIR_QTY,
                                "straddle_locked": round(straddle_locked, 1),
                                "pending_pairs_count": pending_pairs_count,
                                "net_edge_cents": round(rescue_net_edge, 3),
                                "min_edge_used": round(min_edge, 3),
                                "emergency": emergency,
                                "t_min": round(t_min, 3),
                            })

                        # ── FALLBACK: derisk sell ONLY as last resort ──
                        # Determine if derisk sell is warranted:
                        #   - emergency (inventory/time)
                        #   - near_close (< 45s to close)
                        #   - stale_book (no live data)
                        #   - rescue explicitly failed with hard block
                        seconds_to_close_now = ctx.get("seconds_to_close", 999.0)
                        near_close = seconds_to_close_now < 45 or t_min >= PARITY_STOP_NEW_MIN
                        stale_data = rescue_block_reason in ("stale_book", "no_liquidity")
                        hard_rescue_fail = rescue_block_reason in ("cap_reached", "threshold",
                                                                     "qty_too_small", "disabled")

                        derisk_decision = None
                        if not rescue_done:
                            if emergency:
                                derisk_decision = "emergency"
                            elif near_close:
                                derisk_decision = "near_close"
                            elif stale_data:
                                derisk_decision = "stale_data"
                            elif hard_rescue_fail:
                                derisk_decision = "rescue_failed"
                            else:
                                # Rescue blocked as "already_paired" — don't sell,
                                # let hedge/parity rebalance instead
                                derisk_decision = "defer_to_hedge"

                        write_jsonl({
                            "event_type": "DERISK_DECISION",
                            "ts_ms": int(time.time() * 1000),
                            "slug": m.slug, "crypto": m.crypto,
                            "outcome": outcome,
                            "decision": derisk_decision if derisk_decision else "rescue_success",
                            "rescue_done": rescue_done,
                            "rescue_block_reason": rescue_block_reason,
                            "emergency": emergency,
                            "near_close": near_close,
                            "sell_qty": round(sell_qty, 1),
                            "up_qty": round(st.positions["Up"].qty, 1),
                            "dn_qty": round(st.positions["Down"].qty, 1),
                        })

                        # Track derisk reason distribution
                        _dreason = derisk_decision if derisk_decision else "rescue_success"
                        self._diag_derisk_reasons[_dreason] = self._diag_derisk_reasons.get(_dreason, 0) + 1

                        if not rescue_done and derisk_decision != "defer_to_hedge":
                            # Check for emergency taker conditions
                            emergency_taker = emergency
                            if not emergency_taker and abs_edge >= thr + DERISK_TAKER_EDGE_EXTRA_BPS:
                                wk = (m.slug, outcome)
                                wt = self._derisk_edge_worsen_since.get(wk)
                                if wt is None:
                                    self._derisk_edge_worsen_since[wk] = time.time()
                                elif (time.time() - wt) >= DERISK_TAKER_EDGE_WORSEN_SEC:
                                    emergency_taker = True
                            else:
                                self._derisk_edge_worsen_since.pop((m.slug, outcome), None)

                            if emergency_taker and DERISK_TAKER_EMERGENCY_ONLY:
                                self._diag_derisk_taker_count += 1
                                self._diag_taker_count += 1
                                self._diag_rescue_fallback_sells += 1
                                self._do_sell(m, st, outcome, sell_qty, book.bid,
                                              reason="DERISK_EMERGENCY", leg="DERISK", ctx=ctx)
                                _pnl_est = (book.bid - pos.vwap) * 100 if pos.vwap > 0 else 0.0
                                self._record_exit_reason("derisk_emergency", _pnl_est, 0.0, sell_qty * book.bid, slug=m.slug)
                            else:
                                self._diag_maker_count += 1
                                self._diag_rescue_fallback_sells += 1
                                maker_price = min(book.ask, book.bid + 0.001) if book.bid > 0 else book.bid
                                self._do_sell(m, st, outcome, sell_qty, maker_price,
                                              reason="DERISK_MAKER", leg="DERISK", ctx=ctx,
                                              use_maker=True)
                                _pnl_est = (maker_price - pos.vwap) * 100 if pos.vwap > 0 else 0.0
                                self._record_exit_reason("derisk_maker", _pnl_est, 0.0, sell_qty * maker_price, slug=m.slug)
                        elif not rescue_done and derisk_decision == "defer_to_hedge":
                            # Trigger hedge state machine for opposite leg instead of selling
                            if m.slug not in self._quote_unpaired:
                                self._quote_unpaired[m.slug] = {
                                    "outcome": outcome,  # the side we HAVE
                                    "fill_ts": time.time(),
                                    "escalated": False,
                                }
                                self._diag_quote_unpaired_events += 1

                        pos.last_derisk_ts = iso_z(now_t)
                        pos.last_derisk_mid = book.mid
                continue

            # --- FAST_TP: early partial take-profit (once per position) ---
            if not pos.fast_tp_done and pos.opened_at:
                try:
                    opened_dt = datetime.strptime(pos.opened_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    pos_age_sec = (utc_now() - opened_dt).total_seconds()
                except Exception:
                    pos_age_sec = 0.0
                if pos_age_sec >= FAST_TP_AFTER_SEC and book.bid >= pos.vwap + FAST_TP_CENTS:
                    ftp_qty = pos.qty * FAST_TP_SELL_PCT
                    if ftp_qty >= MIN_QTY:
                        write_jsonl({"event_type": "FAST_TP_FIRE", "slug": m.slug,
                                      "outcome": outcome, "pos_age_sec": round(pos_age_sec, 1),
                                      "bid": book.bid, "vwap": pos.vwap,
                                      "ftp_qty": round(ftp_qty, 1)})
                        self._do_sell(m, st, outcome, ftp_qty, book.bid,
                                      reason="FAST_TP", leg="FAST_TP",
                                      target_price=pos.vwap + FAST_TP_CENTS, ctx=ctx)
                    pos.fast_tp_done = True

            # --- Take-profit ladder (with optional tightening) ---
            tp1_adj = TP1 - tp_tighten
            tp2_adj = TP2 - tp_tighten
            tp3_adj = TP3 - tp_tighten
            if (not pos.tp1_done) and book.bid >= pos.vwap + tp1_adj:
                tp_qty = pos.qty * TP1_SELL_FRAC
                if tp_qty >= MIN_QTY:
                    tp_target = pos.vwap + tp1_adj
                    self._do_sell(m, st, outcome, tp_qty, book.bid,
                                  reason="TP1", leg="TP1", target_price=tp_target, ctx=ctx)
                pos.tp1_done = True
            if (not pos.tp2_done) and book.bid >= pos.vwap + tp2_adj:
                tp_qty = pos.qty * TP2_SELL_FRAC
                if tp_qty >= MIN_QTY:
                    tp_target = pos.vwap + tp2_adj
                    self._do_sell(m, st, outcome, tp_qty, book.bid,
                                  reason="TP2", leg="TP2", target_price=tp_target, ctx=ctx)
                pos.tp2_done = True
            if (not pos.tp3_done) and book.bid >= pos.vwap + tp3_adj:
                tp_qty = pos.qty * TP3_SELL_FRAC
                if tp_qty >= MIN_QTY:
                    tp_target = pos.vwap + tp3_adj
                    self._do_sell(m, st, outcome, tp_qty, book.bid,
                                  reason="TP3", leg="TP3", target_price=tp_target, ctx=ctx)
                pos.tp3_done = True
            if pos.scalp_mode:
                target = min(0.999, pos.vwap + LATE_SCALP_TP_CENTS)
                if book.bid >= target and pos.qty >= MIN_QTY:
                    self._do_sell(m, st, outcome, pos.qty, book.bid,
                                  reason="SCALP_TP", leg="SCALP_TP", target_price=target, ctx=ctx)
                    pos.scalp_mode = False
    def _do_sell(self, m: MarketRef, st: MarketState, outcome: str, qty: float, price: float,
                 reason: str, leg: str = "EXIT", target_price: Optional[float] = None,
                 ctx: Optional[dict] = None, use_maker: bool = False):
        qty = max(0.0, qty)
        if qty < MIN_QTY:
            return  # don't sell dust — don't log it either
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        pos = st.positions[outcome]
        up_book = self.last_book.get(m.slug, {}).get("Up")
        dn_book = self.last_book.get(m.slug, {}).get("Down")
        ref_book = up_book if outcome == "Up" else dn_book
        decision_id = new_decision_id()
        client_oid = new_order_id()
        position_id = pos.position_id or ""
        parent_oid = pos.parent_order_id or ""
        usdc_cost = pos.vwap * qty
        unrealized_pnl = (ref_book.bid - pos.vwap) * pos.qty if ref_book and pos.vwap > 0 else 0.0
        # Build a minimal ctx if caller didn't pass one
        if ctx is None:
            t_min = minutes_into_hour(
                datetime.strptime(st.hour_start_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc),
                utc_now())
            ctx = {"hour_start_utc": st.hour_start_utc, "hour_open": st.hour_open,
                   "t_min": t_min, "phase": _phase(t_min),
                   "seconds_to_close": max(0.0, (60.0 - t_min) * 60.0)}
        bk_fields = self._book_fields(up_book, dn_book, outcome) if (up_book and dn_book) else {}
        # ORDER_INTENT
        self.logger.log_order_intent(
            engine="EXIT", reason=reason,
            decision_id=decision_id, position_id=position_id,
            parent_order_id=parent_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="SELL", qty=qty, target_price=target_price or price,
            usdc_cost=usdc_cost, ctx=ctx, book_fields=bk_fields,
            extra={"vwap": pos.vwap, "unrealized_pnl_usdc": unrealized_pnl},
        )
        # ORDER_SUBMIT
        self.logger.log_order_submit(
            engine="EXIT", reason=reason,
            decision_id=decision_id, position_id=position_id,
            client_order_id=client_oid, parent_order_id=parent_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="SELL", qty=qty, target_price=target_price or price,
            usdc_cost=usdc_cost, ctx=ctx, book_fields=bk_fields,
        )
        # Execute sell via unified path (handles paper + live)
        sell_result = self._exec_sell(st, m, outcome, price, qty,
                                       reason=reason, prefer_maker=use_maker, ctx=ctx)
        filled = sell_result.get("filled", False)
        actual_qty = sell_result.get("fill_qty", 0)
        actual_price = sell_result.get("fill_price", price)
        pnl = sell_result.get("pnl", 0.0)

        # Counters: only increment on confirmed fill
        if filled and actual_qty > 0:
            if MODE == "LOG":
                self._true_cost_tx_count += 1
                self._true_cost_fill_count += 1
                self._true_cost_fill_count_min += 1
            # LIVE counters handled by _om_submit_order

            mt = infer_maker_taker("SELL", actual_price, ref_book) if ref_book else ""
            sc = spread_capture_fields("SELL", actual_price, ref_book) if ref_book else {}
            sell_notional = actual_price * actual_qty
            fee = compute_fee_usdc(sell_notional, mt if mt else ("maker" if use_maker else "taker"))
            net_pnl = pnl - fee
            # ORDER_FILL
            self.logger.log_order_fill(
                engine="EXIT", reason=reason,
                decision_id=decision_id, client_order_id=client_oid,
                position_id=position_id, parent_order_id=parent_oid,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="SELL", qty=actual_qty, fill_price=actual_price,
                usdc_cost=usdc_cost, fees_usdc=fee,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                realized_pnl_usdc=pnl, net_pnl_usdc=net_pnl,
                unrealized_pnl_usdc=0.0, vwap=pos.vwap,
                ctx=ctx, book_fields=bk_fields,
            )
            # ROUND_TRIP_CLOSE when position goes flat
            if pos.qty < MIN_QTY and pos.entry_mid > 0:
                time_held = 0.0
                if pos.opened_at:
                    try:
                        opened_dt = datetime.strptime(pos.opened_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                        time_held = (utc_now() - opened_dt).total_seconds()
                    except Exception:
                        pass
                mfe = (pos.max_favorable_mid - pos.entry_mid) * 10000.0 / max(pos.entry_mid, 1e-9)
                mae = (pos.entry_mid - pos.max_adverse_mid) * 10000.0 / max(pos.entry_mid, 1e-9)
                self.logger.log_event({
                    "event_type": "ROUND_TRIP_CLOSE",
                    "position_id": position_id,
                    "slug": m.slug, "crypto": m.crypto, "outcome": outcome,
                    "hour_start_utc": st.hour_start_utc,
                    "gross_pnl_usdc": round(pnl, 4), "net_pnl_usdc": round(net_pnl, 4),
                    "time_in_position_sec": round(time_held, 1),
                    "max_favorable_bps": round(mfe, 3), "max_adverse_bps": round(mae, 3),
                    "exit_reason": reason,
                }, also_csv=True)
        elif not filled and sell_result.get("order_id"):
            # Order resting — will be reconciled by _om_reconcile_all
            write_jsonl({"event_type": "SELL_ORDER_RESTING",
                          "order_id": sell_result["order_id"],
                          "slug": m.slug, "outcome": outcome,
                          "reason": reason, "price": price, "qty": qty})
    def _place_layered_buy(self, m: MarketRef, outcome: str, qty: float, ask: float) -> dict:
        """Place layered buy orders. Returns {total_filled, total_cost, avg_price}.
        Unfilled orders are tracked in _om_open_orders for reconciliation."""
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        result = {"total_filled": 0, "total_cost": 0.0, "avg_price": 0.0}
        if not LAYER_ORDERS:
            r = self.client.place_limit_order(token_id, "BUY", ask, qty, post_only=POST_ONLY_WHEN_POSSIBLE)
            if r.get("filled"):
                result["total_filled"] = r["fill_qty"]
                result["total_cost"] = r["fill_price"] * r["fill_qty"]
                result["avg_price"] = r["fill_price"]
            elif r.get("order_id") and MODE != "LOG":
                # Track unfilled GTC order
                oid = r["order_id"]
                now_ms = int(time.time() * 1000)
                self._om_open_orders[oid] = {
                    "slug": m.slug, "outcome": outcome, "side": "BUY",
                    "price": ask, "qty": int(float(qty)),
                    "filled_qty": r.get("fill_qty", 0) or 0,
                    "reason": "LAYERED_BUY", "maker": POST_ONLY_WHEN_POSSIBLE,
                    "created_ms": now_ms, "last_check_ms": now_ms,
                    "status": "open", "token_id": token_id,
                    "st_slug": m.slug, "cancel_pending": False,
                }
            return result
        # Split qty across layers around ask and slightly below
        per = qty / LAYER_COUNT
        for i in range(LAYER_COUNT):
            px = max(0.01, ask - i * LAYER_STEP)
            r = self.client.place_limit_order(token_id, "BUY", px, per, post_only=POST_ONLY_WHEN_POSSIBLE)
            if r.get("filled"):
                result["total_filled"] += r["fill_qty"]
                result["total_cost"] += r["fill_price"] * r["fill_qty"]
            elif r.get("order_id") and MODE != "LOG":
                oid = r["order_id"]
                now_ms = int(time.time() * 1000)
                self._om_open_orders[oid] = {
                    "slug": m.slug, "outcome": outcome, "side": "BUY",
                    "price": px, "qty": int(float(per)),
                    "filled_qty": r.get("fill_qty", 0) or 0,
                    "reason": "LAYERED_BUY", "maker": POST_ONLY_WHEN_POSSIBLE,
                    "created_ms": now_ms, "last_check_ms": now_ms,
                    "status": "open", "token_id": token_id,
                    "st_slug": m.slug, "cancel_pending": False,
                }
        if result["total_filled"] > 0:
            result["avg_price"] = result["total_cost"] / result["total_filled"]
        return result
    def _calc_clip(self, crypto: str, t_min: float, abs_delta_bps: float) -> float:
        base = self.cash_usdc * BASE_CLIP_PCT
        mult = sizing_mult(abs_delta_bps)
        if t_min < 10:
            mult *= EARLY_SIZE_MULT
        clip = base * mult
        return max(0.0, clip)
    def _cleanup_market(self, m: MarketRef, st: MarketState, t_min: float):
        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty < MIN_QTY:
                continue
            book = self.last_book.get(m.slug, {}).get(outcome)
            if book:
                self._do_sell(m, st, outcome, pos.qty,
                              max(book.bid, book.ask - MAX_CROSS_SLIPPAGE),
                              reason="CLEANUP", leg="CLEANUP")
# =============================================================================
# MAIN
# =============================================================================
def main():
    if MODE not in ("LOG", "LIVE"):
        print("MODE must be LOG or LIVE")
        sys.exit(1)
    bot = Bot()
    bot.run()
if __name__ == "__main__":
    main()
