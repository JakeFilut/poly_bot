"""
settings.py
All configuration constants for the Polymarket hourly clone bot.

Extracted from pm_hourly_clone_bot.py (lines 36-389) -- Pass 1 refactoring.
Every variable name, value, and logic block is preserved exactly.
"""
from __future__ import annotations

import os
import uuid
import math

# =============================================================================
# CONFIG
# =============================================================================
MODE = os.getenv("MODE", "LOG").upper()         # LOG, LIVE_SAFE, or LIVE
BANKROLL_START_USDC = float(os.getenv("BANKROLL_START_USDC", "1000.0"))  # only used in LOG
RUN_ID = uuid.uuid4().hex[:12]  # unique per run -- included in all logs + file names

# LIVE/TEST symbol filter — only these symbols allowed for new entries in LIVE/TEST modes
# Sells/flattening always allowed regardless. LOG mode is unaffected.
LIVE_ALLOWED_SYMBOLS = [s.strip() for s in os.getenv("LIVE_ALLOWED_SYMBOLS", "BTC,ETH").split(",")]

# ---------------------------------------------------------------------------
# Paths -- resolved relative to this file's location
#   poly_bot/          <- _PROJECT_DIR
#   ../keys/.env       <- where your private key lives
#   ../logs/poly_bot/  <- where all logs go
# ---------------------------------------------------------------------------
_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_KEYS_DIR    = os.path.join(os.path.dirname(_PROJECT_DIR), "keys")
_LOG_DIR     = os.path.join(os.path.dirname(_PROJECT_DIR), "logs", "poly_bot")
os.makedirs(_LOG_DIR, exist_ok=True)

STATE_FILE = os.getenv("STATE_FILE", os.path.join(_LOG_DIR, "state.json"))

# ---------------------------------------------------------------------------
# LOG SIZE / READABILITY — COMPACT mode suppresses verbose per-tick events
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "COMPACT").upper()                           # COMPACT | DEBUG
LOG_GATES_EVERY_N_SEC = float(os.getenv("LOG_GATES_EVERY_N_SEC", "30"))         # GATE_REPORT summary interval
LOG_SNAPSHOT_EVERY_N_SEC = float(os.getenv("LOG_SNAPSHOT_EVERY_N_SEC", "60"))   # SNAPSHOT_COMPACT per slug interval
LOG_SPREAD_BLOCK_SAMPLER = float(os.getenv("LOG_SPREAD_BLOCK_SAMPLER", "0.02")) # 2% sampling for GATE_SPREAD_BLOCK
CONSOLE_LEVEL = os.getenv("CONSOLE_LEVEL", "QUIET").upper()                     # NORMAL | QUIET

# Markets / coins
CRYPTOS = ["BTC", "ETH", "SOL", "XRP"]
ENABLE_XRP = bool(os.getenv("ENABLE_XRP", "False") not in ("", "0", "False", "false"))   # default OFF
XRP_MAX_USD_PER_SLUG = float(os.getenv("XRP_MAX_USD_PER_SLUG", "15.0"))
XRP_MAX_IMBALANCE_SHARES = float(os.getenv("XRP_MAX_IMBALANCE_SHARES", "10"))
XRP_PARITY_QUOTE_ENABLED = bool(os.getenv("XRP_PARITY_QUOTE_ENABLED", "False") not in ("", "0", "False", "false"))
XRP_PARITY_BUY_ENABLED = bool(os.getenv("XRP_PARITY_BUY_ENABLED", "False") not in ("", "0", "False", "false"))

# Polling / evaluation
EVAL_EVERY_SEC = float(os.getenv("EVAL_EVERY_SEC", "0.0"))
ORDER_REPRICE_SEC = float(os.getenv("ORDER_REPRICE_SEC", "10.0"))

# Time window within each hour (minutes)
TRADE_START_MIN = 2.0
TRADE_STOP_ADD_MIN = 55.0                                                                  # was 57 — stop entries 2 min earlier
TRADE_HARD_STOP_MIN = 59.25
BURST_EARLY_WINDOW_MIN = 3.0                                                               # burst only in minutes 0-3; after that probe-only
NO_NEW_ENTRIES_SEC_TO_CLOSE = float(os.getenv("NO_NEW_ENTRIES_SEC_TO_CLOSE", "180"))       # 3 min to close → block new entries

# -----------------------------------------------------------------------------
# Entry thresholds (bps) -- coin-specific, time-varying
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

# Cooldowns -- ultra-low, just anti-spam (F247 mode)
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

# Exit ladder (scale out) -- TP1=35%, TP2=25%, TP3=25%, TP4=15% runner
TP1 = 0.04; TP1_SELL_FRAC = 0.35
TP2 = 0.07; TP2_SELL_FRAC = 0.25
TP3 = 0.10; TP3_SELL_FRAC = 0.25
TP4 = 0.12; TP4_SELL_FRAC = 0.15
CORE_KEEP_FRAC = 0.0

# De-risk on drift reversal (bps)
DERISK_CROSS_BPS = 5.0
DERISK_SELL_FRAC_PER_TICK = 0.35
DERISK_COOLDOWN_SEC = 10.0      # min seconds between DERISK actions on same position
DERISK_MID_CHANGE_CENTS = 0.01  # or mid must move >= 1c since last derisk

# Maker-first DERISK -- stop panic taker sells
DERISK_MAKER_REFRESH_MS = 250          # cancel/replace maker every 250ms
DERISK_TAKER_EMERGENCY_ONLY = True     # only taker derisk in emergency
INVENTORY_EMERGENCY_SHARES = 300       # above this = emergency taker derisk
DERISK_TAKER_EDGE_EXTRA_BPS = 25      # edge must exceed thr+25 for taker derisk
DERISK_TAKER_EDGE_WORSEN_SEC = 1.0    # edge must be worsening for 1s

# ---------------------------------------------------------------------------
# Taker gating -- ONLY cross if BOTH conditions met (entry + exit)
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
# No-flip rule -- prevent immediate direction reversal
# ---------------------------------------------------------------------------
NO_FLIP_COOLDOWN_SEC = 3.0            # don't reverse direction within 3s
NO_FLIP_OVERRIDE_EXTRA_BPS = 20       # unless edge >= thr + 20

# Late scalp engine
LATE_SCALP_ENABLED = True
LATE_SCALP_T_START = 40.0
LATE_SCALP_T_END   = 58.0
LATE_SCALP_PRICE_MAX = 0.80
LATE_SCALP_ABSDELTA_MIN = 5.0
LATE_SCALP_ABSDELTA_MAX = 20.0
LATE_SCALP_TP_CENTS = 0.03      # aim +3c
LATE_SCALP_MAX_HOLD_MIN = 6.0   # aggressive F247 -- flip fast

# Risk caps
MAX_COST_PER_MARKET_PCT = 0.015   # 1.5% bankroll per market-hour
MAX_COST_PER_CRYPTO_PCT = 0.035   # 3.5% bankroll per crypto across markets

# ---------------------------------------------------------------------------
# Risk / stop-loss configuration (log-only mode)
# ---------------------------------------------------------------------------
LOG_MODE = True                       # paper / logging mode -- no real orders
ENFORCE_STOP_LOSS = False             # MUST remain False in LOG mode
STOP_LOSS_PCT_PER_HOUR = 0.02        # 2% equity drawdown per 1-hour window
STOP_LOSS_PCT_PER_DAY  = 0.06        # 6% equity drawdown per calendar day
SHADOW_STOP_SIM = True                # simulate what would have happened if stop was enforced

# Execution policy
MAX_CROSS_SLIPPAGE = 0.01         # cross at most 1c if absolutely needed
MIN_ORDER_USDC = 0.25             # f247 does tiny prints
MIN_QTY = float(os.getenv("MIN_QTY", "0.001"))  # below this, position is dust
EDGE_K = 0.05    # sigmoid steepness: delta_bps -> P(Up)

# -----------------------------------------------------------------------------
# Probe -> Scale state machine
# -----------------------------------------------------------------------------
PROBE_SIZE_FRAC = 0.25        # probe = max($1, clip * 0.25)
PROBE_CONFIRM_SEC = 0.3       # 300ms -- near-instant confirmation (F247)

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

# Dynamic price cap boost
CAP_BOOST_EDGE_THRESHOLD = 10.0  # edge_bps above which cap starts boosting
CAP_BOOST_MAX = 0.08             # max +8 cents boost (aggressive chase)
CAP_BOOST_EDGE_FULL = 30.0       # edge_bps at which full boost is applied

# ---------------------------------------------------------------------------
# Parity (straddle) arbitrage engine -- Up + Down ~= 1.000
# ---------------------------------------------------------------------------
PARITY_BUY_ENABLED = True                # buy cheap straddle (up_ask + dn_ask < 1)
PARITY_SELL_ENABLED = True               # sell rich straddle (up_bid + dn_bid > 1)
PARITY_MAX_USD_PER_SLUG = 25.0          # max total straddle investment per slug
PARITY_STEP_USD = 2.50                   # per-leg size per parity order
PARITY_COOLDOWN_MS = 250                 # min time between parity orders per slug
PARITY_MAKER_REFRESH_MS = 200            # cancel/replace maker every 200ms
PARITY_TAKER_ALLOWED_SPREAD_CENTS = 1.0  # allow taker only when spread <= 1c

# Fee-aware parity edge (CRITICAL)
MAKER_FEE_BPS = float(os.getenv("MAKER_FEE_BPS", "0.5"))   # configurable: Poly CLOB ~0-0.5 bps maker
TAKER_FEE_BPS = float(os.getenv("TAKER_FEE_BPS", "2.0"))   # configurable: Poly CLOB ~2 bps taker
PARITY_BUY_MIN_EDGE_NET_CENTS = 1.75    # min NET edge after fees/slippage to buy straddle
PARITY_SELL_MIN_EDGE_NET_CENTS = 1.75   # min NET edge after fees/slippage to sell straddle
PARITY_EDGE_BUFFER_CENTS = 0.25         # safety buffer on top of min edge thresholds

# Partial-fill protection
PAIR_FILL_TIMEOUT_MS = 1200              # max time to wait for second leg fill

# Locked inventory recycle
LOCKED_MAX_HOLD_SEC = 180                # max seconds to hold locked straddle before recycling
RECYCLE_MIN_PROFIT_NET_CENTS = 0.5      # min net-of-fee profit to trigger recycle sell
RECYCLE_STEP_USD = 2.5                   # per-leg sell size during recycle

# Liquidity + staleness guards
MAX_SPREAD_FOR_PARITY_CENTS = 10.0      # block parity if either leg spread > 10c
MIN_TOP_LIQ_USD = 1.0                   # block parity if best bid/ask size < $1 (F247 trades tiny clips)
PARITY_MAX_CACHE_AGE_MS = 600           # block parity if cache > 600ms stale

# End-of-hour parity flattening
PARITY_STOP_NEW_MIN = 55.0              # stop opening NEW parity trades (aligned with TRADE_STOP_ADD_MIN)
PARITY_QUOTE_STOP_MIN = float(os.getenv("PARITY_QUOTE_STOP_MIN", "50.0"))   # stop quoting at minute 50, exits/recycle continue
PARITY_FLATTEN_START_MIN = 58.5         # begin flattening locked + unpaired parity inventory (was 59.0)
PARITY_HARD_FLATTEN_MIN = 59.1          # force taker flatten (was 59.25)

# ---------------------------------------------------------------------------
# Parity QUOTING mode -- continuously post maker bids on BOTH legs
# ---------------------------------------------------------------------------
PARITY_QUOTE_ENABLED = True
PARITY_QUOTE_TARGET_EDGE_NET_CENTS_BASE = 1.0  # min edge target (aggressive -- pay up)
PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX  = 2.0  # max edge target (selective)
PARITY_QUOTE_STEP_USD = 2.5              # per-leg bid size (equal USD both legs)
PARITY_QUOTE_MAX_USD_PER_SLUG = 25.0    # max total quoting investment per slug
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
# FAST_CLONE mode -- tighter timing to match F247 speed
# ---------------------------------------------------------------------------
FAST_CLONE = bool(os.getenv("FAST_CLONE", "True") not in ("", "0", "False", "false"))

# One-sided auto-hedge -- faster escalation than unpaired management
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
# Imbalance caps -- keep net exposure near neutral (F247 style)
# ---------------------------------------------------------------------------
IMBALANCE_CAP_SHARES = 30              # hard cap: abs(up_qty - dn_qty) per slug
IMBALANCE_SOFT_CAP_SHARES = 20         # soft cap: start reducing new orders above this

# ---------------------------------------------------------------------------
# Derisk RESCUE-TO-STRADDLE -- convert losing one-sided to straddle
# ---------------------------------------------------------------------------
DERISK_RESCUE_TO_STRADDLE = True
RESCUE_MIN_EDGE_NET_CENTS = 0.5         # min net edge for straddle completion to be worth it
RESCUE_MAX_USD_PER_SLUG = 10.0          # max USD to spend completing straddle per slug
RESCUE_STEP_USD = 2.5                    # per-order size for rescue buys
MIN_PAIR_QTY = 5.0                       # both legs must exceed this to count as "already paired"

# ---------------------------------------------------------------------------
# Directional lean overlay (on top of parity, for exits)
# ---------------------------------------------------------------------------
LEAN_EXIT_PRIORITY = True                # prioritize exits on "wrong" side
LEAN_MAX_IMBALANCE_SHARES = 30           # cap how unbalanced Up vs Down can get (align with IMBALANCE_CAP_SHARES)

# Spread rule relaxation
SPREAD_RELAXED_MAX = 0.12         # 12 cents during burst (F247 tolerant)

# Fast take-profit -- skim faster than before
FAST_TP_AFTER_SEC = 25.0
FAST_TP_CENTS = 0.02
FAST_TP_SELL_PCT = 0.30           # sell 30%

# Inventory pressure controls
INVENTORY_CAP_SHARES_PER_MARKET = 250

# Correlation exposure scaling (reduces correlated stacking)
CORR_SCALE_ENABLED = True
BTC_LEAD = True
BTC_EXPOSURE_REDUCE_OTHERS = 0.50  # up to 50% size reduction if BTC exposure high

# ---------------------------------------------------------------------------
# Background data refresh -- sub-second loop architecture
# ---------------------------------------------------------------------------
MARKET_DISCOVERY_INTERVAL_SEC = 10.0   # re-discover markets via Gamma API every 10s
BOOK_REFRESH_PRIORITY_MS = 100         # active markets: 100ms (positions / probing / scaling)
BOOK_REFRESH_IDLE_MS = 400             # idle markets: 400ms (no positions, IDLE state)
BOOK_STALE_MS = 1500                   # data older than this is stale -- skip processing
STATE_SAVE_INTERVAL_SEC = 5.0          # flush state.json every 5s (not every loop)
BG_POOL_WORKERS = 16                   # bg threads -- covers priority + idle markets
BG_POOL_MIN_WORKERS = 12               # minimum pool size
MAIN_LOOP_TARGET_MS = 75              # 75ms decision loop target (f247 parity)
BG_REFRESH_STARVE_CYCLES = 2           # if pending for > N cycles, force-submit

# Burst freshness gate -- micro-orders must have fresh data
BURST_FRESHNESS_MAX_MS = 500           # max cache age to place a micro-order
BURST_FRESHNESS_WAIT_MS = 250          # max time to wait for fresh data if stale

# ---------------------------------------------------------------------------
# FAST_CLONE speed overrides -- tighter loops, faster hedging
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

# ---------------------------------------------------------------------------
# RATE LIMITING / CHURN CONTROL -- hard caps per slug (F247 cadence)
# ---------------------------------------------------------------------------
RATE_LIMIT_ENABLED = bool(os.getenv("RATE_LIMIT_ENABLED", "True") not in ("", "0", "False", "false"))
MIN_ORDER_INTERVAL_MS = float(os.getenv("MIN_ORDER_INTERVAL_MS", "600"))       # min ms between ANY orders on same slug
MAX_ORDER_SUBMITS_PER_MIN = int(os.getenv("MAX_ORDER_SUBMITS_PER_MIN", "60"))  # hard cap submits/min per slug -- no bursting
QUOTE_REFRESH_SKIP_IF_SAME = True                                               # skip refresh if price unchanged
QUOTE_REFRESH_MIN_TICK_MOVE = 0.001                                              # require >= 1 tick move to refresh
QUOTE_REFRESH_MIN_ELAPSED_MS = float(os.getenv("QUOTE_REFRESH_MIN_ELAPSED_MS", "500"))  # min ms between refreshes

# ---------------------------------------------------------------------------
# DIRECTIONAL SCALP MODE -- PRIMARY engine (F247-style, priority #1)
# Strategy stack: 1) Directional Scalp  2) Inventory Repair  3) Parity (throttled)
# ---------------------------------------------------------------------------
DIRECTIONAL_SCALP_ENABLED = bool(os.getenv("DIRECTIONAL_SCALP_ENABLED", "True") not in ("", "0", "False", "false"))

# Entry gates (explicit -- must meet delta OR spot_move condition)
DSCALP_DELTA_MIN_BPS = float(os.getenv("DSCALP_DELTA_MIN_BPS", "15.0"))        # min abs_delta_bps for entry (raised for conviction)
DSCALP_SPOT_MOVE_10S_BPS = float(os.getenv("DSCALP_SPOT_MOVE_10S_BPS", "8.0"))  # OR: spot moved >= 8bps in last 10s
DSCALP_VEL_MIN_BPS_PER_MIN = float(os.getenv("DSCALP_VEL_MIN_BPS_PER_MIN", "2.5"))  # min velocity (tightened from 2.0)
DSCALP_MAX_SPREAD_CENTS = float(os.getenv("DSCALP_MAX_SPREAD_CENTS", "2.0"))   # max spread for entry
DSCALP_MIN_ENTRY_EDGE_CENTS = float(os.getenv("DSCALP_MIN_ENTRY_EDGE_CENTS", "3.0"))  # min edge: outcome mid must be >=3c above 50c neutral (tightened from 2.5)

# Stronger entry quality gates — PER-COIN GROUP (BTC/ETH vs SOL/XRP)
# SOL/XRP require stronger momentum + better edge + fresher data due to higher latency/noise
MIN_DIRECTIONAL_EDGE_CENTS_BTCETH = float(os.getenv("MIN_DIRECTIONAL_EDGE_CENTS_BTCETH", "3.0"))
MIN_DIRECTIONAL_EDGE_CENTS_SOLXRP = float(os.getenv("MIN_DIRECTIONAL_EDGE_CENTS_SOLXRP", "4.0"))
MIN_VEL_BPS_PER_MIN_BTCETH = float(os.getenv("MIN_VEL_BPS_PER_MIN_BTCETH", "2.5"))
MIN_VEL_BPS_PER_MIN_SOLXRP = float(os.getenv("MIN_VEL_BPS_PER_MIN_SOLXRP", "3.5"))
CACHE_AGE_MAX_MS_BTCETH = float(os.getenv("CACHE_AGE_MAX_MS_BTCETH", "300"))
CACHE_AGE_MAX_MS_SOLXRP = float(os.getenv("CACHE_AGE_MAX_MS_SOLXRP", "200"))        # SOL/XRP must be fresher
# Per-coin group cooldown between new directional entries
BTCETH_COOLDOWN_MS = float(os.getenv("BTCETH_COOLDOWN_MS", "1500"))
SOLXRP_COOLDOWN_MS = float(os.getenv("SOLXRP_COOLDOWN_MS", "2500"))                  # slower markets get longer cooldown
# Late-move filter (SOL/XRP only): block entry if spot moved too fast on stale data
SOLXRP_LATE_MOVE_BPS = float(os.getenv("SOLXRP_LATE_MOVE_BPS", "8.0"))               # max abs spot move in last 2s
SOLXRP_LATE_MOVE_CACHE_MS = float(os.getenv("SOLXRP_LATE_MOVE_CACHE_MS", "120"))     # only block if cache_age > 120ms
# Legacy globals (kept for backward compat, overridden by per-coin values above)
MIN_DIRECTIONAL_EDGE_CENTS = float(os.getenv("MIN_DIRECTIONAL_EDGE_CENTS", "3.0"))
MIN_VEL_BPS_PER_MIN = float(os.getenv("MIN_VEL_BPS_PER_MIN", "2.5"))
DSCALP_MAX_CACHE_AGE_MS = float(os.getenv("DSCALP_MAX_CACHE_AGE_MS", "250"))   # max cache age for entry

# Sizing -- one entry = one meaningful position, no micro-splits
DSCALP_STEP_USD = float(os.getenv("DSCALP_STEP_USD", "7.0"))                   # per-order size (~F247's $7.4 avg)
DSCALP_STEP_USD_MIN = float(os.getenv("DSCALP_STEP_USD_MIN", "6.0"))           # minimum entry size (no $1 clips)
DSCALP_MAX_USD_PER_SLUG = float(os.getenv("DSCALP_MAX_USD_PER_SLUG", "25.0"))  # max directional per slug
DSCALP_COOLDOWN_MS = float(os.getenv("DSCALP_COOLDOWN_MS", "4000"))            # 4s between entries (target ~15 trades/min)

# Exit ladder (TP1=35%, TP2=25%, TP3=25%, TP4=15% runner)
DSCALP_TP1_CENTS = float(os.getenv("DSCALP_TP1_CENTS", "4.0"))                 # +4c: sell 35%
DSCALP_TP1_FRAC = float(os.getenv("DSCALP_TP1_FRAC", "0.35"))
DSCALP_TP2_CENTS = float(os.getenv("DSCALP_TP2_CENTS", "7.0"))                 # +7c: sell 25%
DSCALP_TP2_FRAC = float(os.getenv("DSCALP_TP2_FRAC", "0.25"))
DSCALP_TP3_CENTS = float(os.getenv("DSCALP_TP3_CENTS", "10.0"))                # +10c: sell 25%
DSCALP_TP3_FRAC = float(os.getenv("DSCALP_TP3_FRAC", "0.25"))
DSCALP_TP4_CENTS = float(os.getenv("DSCALP_TP4_CENTS", "12.0"))                # +12c: sell remaining 15% (runner tier)
DSCALP_TP4_FRAC = float(os.getenv("DSCALP_TP4_FRAC", "0.15"))

# Runner protection -- exit runner if held too long or velocity reverses hard
RUNNER_MAX_HOLD_SEC = float(os.getenv("RUNNER_MAX_HOLD_SEC", "600"))            # max 600s for runner portion
RUNNER_VEL_REVERSAL_BPS = float(os.getenv("RUNNER_VEL_REVERSAL_BPS", "8.0"))   # vel reversal >= 8 bps/min triggers runner fallback

# Early exit — requires ALL three conditions: profit >= 4c AND vel reversal >= 6 bps AND spread >= 5c
EARLY_EXIT_ENABLED = bool(os.getenv("EARLY_EXIT_ENABLED", "True") not in ("", "0", "False", "false"))
EARLY_EXIT_MIN_PROFIT_CENTS = float(os.getenv("EARLY_EXIT_MIN_PROFIT_CENTS", "4.0"))       # min profit to allow early exit
EARLY_EXIT_VEL_REVERSAL_BPS = float(os.getenv("EARLY_EXIT_VEL_REVERSAL_BPS", "6.0"))       # min velocity reversal (bps/min against position)
EARLY_EXIT_SPREAD_THRESHOLD_CENTS = float(os.getenv("EARLY_EXIT_SPREAD_THRESHOLD_CENTS", "5.0"))  # min spread (cents) for early exit
DSCALP_VEL_REVERSAL_BPS = float(os.getenv("DSCALP_VEL_REVERSAL_BPS", "6.0"))   # velocity reversal threshold (bps/min against position)

DSCALP_MIN_HOLD_SEC = float(os.getenv("DSCALP_MIN_HOLD_SEC", "60"))            # 60s min hold -- TP/STOP can override
DSCALP_MAX_HOLD_SEC = float(os.getenv("DSCALP_MAX_HOLD_SEC", "600"))           # 10 min max hold
DSCALP_STOP_LOSS_CENTS = float(os.getenv("DSCALP_STOP_LOSS_CENTS", "4.0"))     # -4c hard stop loss — overrides min hold timer

# Per-position loss cap (tail-loss killer) -- prevents rare huge losses from wiping many wins
POS_LOSS_CAP_MULT = float(os.getenv("POS_LOSS_CAP_MULT", "1.25"))              # trigger stop if unrealized <= -1.25 * clip_usdc
POS_LOSS_CAP_ENABLED = bool(os.getenv("POS_LOSS_CAP_ENABLED", "True") not in ("", "0", "False", "false"))

# Post-STOP entry cooldown per slug
STOP_COOLDOWN_SEC = float(os.getenv("STOP_COOLDOWN_SEC", "90"))                 # 90s cooldown after STOP on same slug

# TIME_STOP_EXIT loss prevention -- don't realize losses on time stop
TIME_STOP_MIN_PNL_CENTS = float(os.getenv("TIME_STOP_MIN_PNL_CENTS", "1.0"))   # min +1c to allow TIME_STOP_EXIT
TIME_STOP_SOFT_EXIT_FRAC = float(os.getenv("TIME_STOP_SOFT_EXIT_FRAC", "0.25"))  # soft-exit 25% when conditions met
TIME_STOP_SOFT_EXIT_MAX_SPREAD_CENTS = float(os.getenv("TIME_STOP_SOFT_EXIT_MAX_SPREAD_CENTS", "2.0"))  # max spread for soft-exit

# DERISK_MAKER emergency-only gating
DERISK_MAKER_EMERGENCY_ONLY = bool(os.getenv("DERISK_MAKER_EMERGENCY_ONLY", "True") not in ("", "0", "False", "false"))

# ---------------------------------------------------------------------------
# PARITY SUPPRESSION -- parity is #3 priority, hard-capped
# ---------------------------------------------------------------------------
PARITY_DEFER_TO_DIRECTIONAL = True                                               # always defer when directional active
PARITY_BLOCK_IF_ADVERSE = True                                                   # block parity when adverse guard active
PARITY_STANDDOWN_AFTER_DSCALP_SEC = float(os.getenv("PARITY_STANDDOWN_AFTER_DSCALP_SEC", "30"))  # parity stands down X sec after dscalp fires
PARITY_IMBALANCE_BLOCK_SHARES = float(os.getenv("PARITY_IMBALANCE_BLOCK_SHARES", "5.0"))  # block parity if net imbal >= this
PARITY_MAX_FILL_PCT = float(os.getenv("PARITY_MAX_FILL_PCT", "0.30"))           # target: parity < 30% of total fills
PARITY_MAX_WHEN_DIRECTIONAL_USD = float(os.getenv("PARITY_MAX_WHEN_DIRECTIONAL_USD", "0.0"))  # $0 parity when directional active

# ---------------------------------------------------------------------------
# GLOBAL THROTTLE -- target trades/min
# ---------------------------------------------------------------------------
TARGET_TRADES_PER_MIN = float(os.getenv("TARGET_TRADES_PER_MIN", "15"))          # target ~15 trades/min (F247 = ~12)
THROTTLE_LOOKBACK_SEC = float(os.getenv("THROTTLE_LOOKBACK_SEC", "60"))          # rolling window for trades/min calc
# Per-slug entry throttle (prevents over-trading any single market)
MIN_ENTRY_INTERVAL_MS = float(os.getenv("MIN_ENTRY_INTERVAL_MS", "350"))         # min ms between entries on same slug
MAX_ENTRIES_PER_MIN_PER_SLUG = int(os.getenv("MAX_ENTRIES_PER_MIN_PER_SLUG", "20"))  # hard cap per slug per minute (was 40)
# Anti-stacking: prevent rapid entries after a fill
MIN_TIME_BETWEEN_NEW_ENTRIES_MS = float(os.getenv("MIN_TIME_BETWEEN_NEW_ENTRIES_MS", "2000"))  # 2s cooldown after fill before new entry (tightened from 1.5s)

# Noisy-spread chop guard (entry-only): block if spread wide AND velocity low
NOISY_SPREAD_BLOCK_CENTS = float(os.getenv("NOISY_SPREAD_BLOCK_CENTS", "4"))    # spread > 4c
NOISY_SPREAD_BLOCK_VEL = float(os.getenv("NOISY_SPREAD_BLOCK_VEL", "4"))        # AND vel < 4 bps/min → chop, block entry

# ---------------------------------------------------------------------------
# REGIME AWARENESS -- volatility-adaptive activity
# ---------------------------------------------------------------------------
REGIME_VOL_LOOKBACK_SEC = float(os.getenv("REGIME_VOL_LOOKBACK_SEC", "60"))      # 60s rolling window
REGIME_LOW_VOL_THRESHOLD = float(os.getenv("REGIME_LOW_VOL_THRESHOLD", "3.0"))   # bps std_dev below this = low vol
REGIME_LOW_VOL_REDUCTION = float(os.getenv("REGIME_LOW_VOL_REDUCTION", "0.50"))  # reduce activity 50% in low vol

# ---------------------------------------------------------------------------
# TRUE COST TRACKER -- tx counting and fee estimation
# ---------------------------------------------------------------------------
TRUE_COST_ENABLED = True
TRUE_COST_EST_GAS_PER_TX_USD = float(os.getenv("TRUE_COST_EST_GAS_PER_TX_USD", "0.001"))  # est gas per tx
TRUE_COST_EST_FEE_BPS = float(os.getenv("TRUE_COST_EST_FEE_BPS", "2.0"))                  # avg fee bps per fill

# ---------------------------------------------------------------------------
# LIVE EXECUTION SAFETY (execution-layer only — NO strategy changes)
# ---------------------------------------------------------------------------

# Order TTL & cancel retry
OM_MAKER_ORDER_TTL_MS = float(os.getenv("OM_MAKER_ORDER_TTL_MS", "3000"))          # cancel unfilled limit after 3s (hourly markets move fast)
OM_CANCEL_MAX_RETRIES = int(os.getenv("OM_CANCEL_MAX_RETRIES", "3"))               # retry cancel up to 3x
OM_CANCEL_RETRY_DELAY_MS = float(os.getenv("OM_CANCEL_RETRY_DELAY_MS", "500"))    # delay between cancel retries

# Orphan scanner
OM_ORPHAN_SCAN_INTERVAL_SEC = float(os.getenv("OM_ORPHAN_SCAN_INTERVAL_SEC", "12"))  # scan CLOB every 12s

# State drift checker
OM_DRIFT_CHECK_INTERVAL_SEC = float(os.getenv("OM_DRIFT_CHECK_INTERVAL_SEC", "60"))  # compare API vs internal every 60s
OM_STARTUP_VERIFY_RETRIES = int(os.getenv("OM_STARTUP_VERIFY_RETRIES", "3"))        # re-fetch after cancel to confirm 0

# ---------------------------------------------------------------------------
# POSITION RECONCILIATION — wallet-verified position tracking
# ---------------------------------------------------------------------------
POSITION_RECONCILE_ENABLED = bool(os.getenv("POSITION_RECONCILE_ENABLED", "True") not in ("", "0", "False", "false"))
POSITION_RECONCILE_INTERVAL_SEC = float(os.getenv("POSITION_RECONCILE_INTERVAL_SEC", "60"))  # reconcile every N seconds
POSITION_RECONCILE_ON_STARTUP = bool(os.getenv("POSITION_RECONCILE_ON_STARTUP", "True") not in ("", "0", "False", "false"))
POSITION_RECONCILE_AFTER_FILL = bool(os.getenv("POSITION_RECONCILE_AFTER_FILL", "True") not in ("", "0", "False", "false"))
POSITION_RECONCILE_BLOCK_ON_DESYNC = bool(os.getenv("POSITION_RECONCILE_BLOCK_ON_DESYNC", "True") not in ("", "0", "False", "false"))

# ---------------------------------------------------------------------------
# FILLS LEDGER — append-only fill accounting with position derivation
# ---------------------------------------------------------------------------
LEDGER_ENABLED = bool(os.getenv("LEDGER_ENABLED", "True") not in ("", "0", "False", "false"))
LEDGER_PATH = os.getenv("LEDGER_PATH", os.path.join(_LOG_DIR, "fills_ledger.jsonl"))
RECONCILE_INTERVAL_SECONDS = float(os.getenv("RECONCILE_INTERVAL_SECONDS", "60"))
SAFE_MODE_ON_MISMATCH = bool(os.getenv("SAFE_MODE_ON_MISMATCH", "True") not in ("", "0", "False", "false"))
LEDGER_RECONCILE_TOLERANCE = float(os.getenv("LEDGER_RECONCILE_TOLERANCE", "0.01"))  # qty mismatch tolerance before CRITICAL

# ---------------------------------------------------------------------------
# ACTIVE WINDOW FILTER — restrict trading logic to current 1-hour market only
# ---------------------------------------------------------------------------
ACTIVE_WINDOW_ONLY = bool(os.getenv("ACTIVE_WINDOW_ONLY", "True") not in ("", "0", "False", "false"))
STRICT_WINDOW_MODE = bool(os.getenv("STRICT_WINDOW_MODE", "True") not in ("", "0", "False", "false"))
POST_WINDOW_UNWIND = bool(os.getenv("POST_WINDOW_UNWIND", "True") not in ("", "0", "False", "false"))  # allow reduce-only sells after window

# ---------------------------------------------------------------------------
# PER-HOUR LEDGER ROTATION — isolate each 1-hour window
# ---------------------------------------------------------------------------
HOURLY_LEDGER_ROTATION = bool(os.getenv("HOURLY_LEDGER_ROTATION", "True") not in ("", "0", "False", "false"))
REQUIRE_CONSISTENT_STATE_ON_RESTART = bool(os.getenv("REQUIRE_CONSISTENT_STATE_ON_RESTART", "True") not in ("", "0", "False", "false"))

# ---------------------------------------------------------------------------
# SKIP_HISTORY — on startup, skip all historical fills and start fresh
# ---------------------------------------------------------------------------
SKIP_HISTORY = bool(os.getenv("SKIP_HISTORY", "True") not in ("", "0", "False", "false"))
# Max recent trade IDs to keep in ring buffer for same-timestamp dedup
SCAN_RECENT_TIDS_MAX = int(os.getenv("SCAN_RECENT_TIDS_MAX", "200"))
# Position logging: only every N seconds (default 600 = 10 min)
POSITION_LOG_INTERVAL_SEC = float(os.getenv("POSITION_LOG_INTERVAL_SEC", "600"))

# ---------------------------------------------------------------------------
# ORDER TRACKING + OPEN ORDER HYGIENE
# ---------------------------------------------------------------------------
ORDER_TTL_SECONDS = float(os.getenv("ORDER_TTL_SECONDS", "20"))
CANCEL_ON_STALE = bool(os.getenv("CANCEL_ON_STALE", "True") not in ("", "0", "False", "false"))
REPRICE_ON_STALE = bool(os.getenv("REPRICE_ON_STALE", "False") not in ("", "0", "False", "false"))

# ---------------------------------------------------------------------------
# RECONCILIATION — wallet vs ledger comparison
# ---------------------------------------------------------------------------
RECONCILE_EVERY_SECONDS = float(os.getenv("RECONCILE_EVERY_SECONDS", "120"))
RECONCILE_DELTA_THRESHOLD = float(os.getenv("RECONCILE_DELTA_THRESHOLD", "0.5"))
SAFETY_PAUSE_ON_RECONCILE_FAIL = bool(os.getenv("SAFETY_PAUSE_ON_RECONCILE_FAIL", "True") not in ("", "0", "False", "false"))

# ---------------------------------------------------------------------------
# HEDGE MISMATCH POLICY — one-side-fill safety
# ---------------------------------------------------------------------------
HEDGE_MISMATCH_POLICY = os.getenv("HEDGE_MISMATCH_POLICY", "IMMEDIATE_FLATTEN").upper()  # IMMEDIATE_FLATTEN | HOLD_AND_HEDGE | PAUSE
MAX_NET_EXPOSURE_SHARES = float(os.getenv("MAX_NET_EXPOSURE_SHARES", "50"))
MAX_UNHEDGED_SECONDS = float(os.getenv("MAX_UNHEDGED_SECONDS", "10"))

# No-progress pause (per-slug)
OM_NO_PROGRESS_SUBMITS = int(os.getenv("OM_NO_PROGRESS_SUBMITS", "40"))             # submits in 60s with 0 fills -> pause
OM_NO_PROGRESS_PAUSE_SEC = float(os.getenv("OM_NO_PROGRESS_PAUSE_SEC", "120"))     # pause slug for 120s

# Kill-switch thresholds (execution errors only — sells always allowed)
OM_KILL_API_ERROR_THRESHOLD_PER_MIN = int(os.getenv("OM_KILL_API_ERROR_THRESHOLD_PER_MIN", "10"))
OM_KILL_ORPHAN_THRESHOLD_PER_MIN = int(os.getenv("OM_KILL_ORPHAN_THRESHOLD_PER_MIN", "5"))
OM_KILL_PAUSE_SEC = float(os.getenv("OM_KILL_PAUSE_SEC", "60"))                    # pause entries for 60s

# Global safety caps
OM_MAX_OPEN_ORDERS = int(os.getenv("OM_MAX_OPEN_ORDERS", "50"))                    # hard cap on tracked open orders
OM_MAX_TOTAL_USD = float(os.getenv("OM_MAX_TOTAL_USD", "50"))                      # max total exposure across all slugs
OM_MAX_PER_SLUG_USD = float(os.getenv("OM_MAX_PER_SLUG_USD", "25"))                # max exposure per slug

# Sanity report
OM_SANITY_REPORT_INTERVAL_SEC = float(os.getenv("OM_SANITY_REPORT_INTERVAL_SEC", "60"))  # report every 60s

# ---------------------------------------------------------------------------
# LIVE_SAFE mode — time-limited entry window + trade-size limiter
# ---------------------------------------------------------------------------
LIVE_SAFE_ENTRY_WINDOW_SEC = float(os.getenv("LIVE_SAFE_ENTRY_WINDOW_SEC", "300"))        # buys allowed for 300s after start
LIVE_SAFE_NUM_HOURS = int(os.getenv("LIVE_SAFE_NUM_HOURS", "3"))                         # run for N hourly windows before auto-exit
LIVE_SAFE_MAX_ORDER_USD = float(os.getenv("LIVE_SAFE_MAX_ORDER_USD", "5.00"))             # max buy order size in LIVE_SAFE
LIVE_SAFE_MAX_TOTAL_INVESTED_USD = float(os.getenv("LIVE_SAFE_MAX_TOTAL_INVESTED_USD", "50.0"))  # max total invested at any time in LIVE_SAFE
CLOB_MIN_ORDER_SIZE = 5                                                                   # Polymarket CLOB minimum order size (shares)
CLOB_MIN_ORDER_USD = 1.0                                                                   # Polymarket CLOB minimum order value ($1)

# ---------------------------------------------------------------------------
# LOSS-TAIL REDUCTION GUARD — auto-pause slugs with repeated negative exits
# ---------------------------------------------------------------------------
LOSS_TAIL_NEGATIVE_EXIT_REASONS = {"DERISK_MAKER", "PARITY_FLATTEN", "PARITY_RECYCLE"}
LOSS_TAIL_LOOKBACK_SEC = float(os.getenv("LOSS_TAIL_LOOKBACK_SEC", "600"))                # 10 min rolling window
LOSS_TAIL_NEG_EXIT_THRESHOLD = int(os.getenv("LOSS_TAIL_NEG_EXIT_THRESHOLD", "3"))        # X negative exits in window → pause
LOSS_TAIL_PAUSE_SEC = float(os.getenv("LOSS_TAIL_PAUSE_SEC", "600"))                      # pause entries for 10 min

# ---------------------------------------------------------------------------
# PER-SLUG PnL REPORT — emitted every 60s alongside TEMPO_REPORT
# ---------------------------------------------------------------------------
SLUG_PNL_REPORT_INTERVAL_SEC = float(os.getenv("SLUG_PNL_REPORT_INTERVAL_SEC", "60"))

# ---------------------------------------------------------------------------
# PRE-8HR SAFETY CAPS — inventory, cooldown, exit reliability, time stops
# ---------------------------------------------------------------------------

# 1) Inventory caps — block new BUYS when exceeded, sells always allowed
MAX_POSITION_USD_PER_SLUG = float(os.getenv("MAX_POSITION_USD_PER_SLUG", "25"))
MAX_POSITION_SHARES_PER_OUTCOME = float(os.getenv("MAX_POSITION_SHARES_PER_OUTCOME", "25"))
MAX_NET_IMBALANCE_SHARES = float(os.getenv("MAX_NET_IMBALANCE_SHARES", "12"))

# 2) Post-fill cooldown — skip new BUY attempts after any fill on slug
POST_FILL_COOLDOWN_MS = float(os.getenv("POST_FILL_COOLDOWN_MS", "800"))

# 3) Exit reliability — maker-first TP with taker fallback
TP_MAKER_GRACE_MS = float(os.getenv("TP_MAKER_GRACE_MS", "2000"))

# 4) Time stop exits — force exit if position held too long
MAX_HOLD_SEC_SCALP = float(os.getenv("MAX_HOLD_SEC_SCALP", "600"))
MAX_HOLD_SEC_PARITY = float(os.getenv("MAX_HOLD_SEC_PARITY", "900"))

# 5) Coin-specific spread limits (cents) — directional entries only
MAX_SPREAD_ENTRY_CENTS_BTCETH = float(os.getenv("MAX_SPREAD_ENTRY_CENTS_BTCETH", "2"))   # BTC/ETH: max 2c spread for entry
MAX_SPREAD_ENTRY_CENTS_SOLXRP = float(os.getenv("MAX_SPREAD_ENTRY_CENTS_SOLXRP", "3"))   # SOL/XRP: max 3c spread for entry (tightened from 4c)
MAX_SPREAD_EXIT_CENTS_SOLXRP = float(os.getenv("MAX_SPREAD_EXIT_CENTS_SOLXRP", "6"))

# 6) Module priority — directional suppresses parity BUYs
DIRECTIONAL_PARITY_SUPPRESS_SEC = float(os.getenv("DIRECTIONAL_PARITY_SUPPRESS_SEC", "120"))

# ---------------------------------------------------------------------------
# LIVE TRADING SAFETY RULES (9 rules — active in LIVE and LIVE_SAFE modes)
# ---------------------------------------------------------------------------

# Rule 1: Atomic Hedge Rule — time-based kill switch for unpaired legs
HEDGE_KILL_TIMEOUT_SEC = float(os.getenv("HEDGE_KILL_TIMEOUT_SEC", "2.0"))           # max wait for hedge fill (normal)
HEDGE_KILL_TIMEOUT_FINAL_SEC = float(os.getenv("HEDGE_KILL_TIMEOUT_FINAL_SEC", "1.0"))  # max wait in last 2 min
HEDGE_KILL_FINAL_WINDOW_MIN = float(os.getenv("HEDGE_KILL_FINAL_WINDOW_MIN", "58.0"))   # t_min threshold for "final" mode

# Rule 2: Max Slippage Protection — abort hedge if price moved too far
HEDGE_MAX_SLIPPAGE_CENTS = float(os.getenv("HEDGE_MAX_SLIPPAGE_CENTS", "1.5"))       # max price move before aborting 2nd leg

# Rule 3: Partial Fill Handling — hedge only what actually filled
# (no config needed — logic enforced in code: hedge filled_qty, cancel remainder)

# Rule 4: Spread Explosion Filter — disable trading when spread too wide
SPREAD_EXPLOSION_CENTS_NORMAL = float(os.getenv("SPREAD_EXPLOSION_CENTS_NORMAL", "3.0"))   # max spread (normal)
SPREAD_EXPLOSION_CENTS_FINAL = float(os.getenv("SPREAD_EXPLOSION_CENTS_FINAL", "5.0"))     # max spread (last 2 min)
SPREAD_EXPLOSION_PAUSE_SEC = float(os.getenv("SPREAD_EXPLOSION_PAUSE_SEC", "10.0"))        # pause trading this slug

# Rule 5: Order Book Depth Check — require 2x order size at price level
DEPTH_CHECK_MULTIPLIER = float(os.getenv("DEPTH_CHECK_MULTIPLIER", "2.0"))           # min depth = multiplier * order_size

# Rule 6: Consecutive Hedge Failure Circuit Breaker
HEDGE_FAIL_MAX_COUNT = int(os.getenv("HEDGE_FAIL_MAX_COUNT", "3"))                   # X failures in window → pause
HEDGE_FAIL_WINDOW_SEC = float(os.getenv("HEDGE_FAIL_WINDOW_SEC", "600"))             # 10 min window
HEDGE_FAIL_PAUSE_SEC = float(os.getenv("HEDGE_FAIL_PAUSE_SEC", "900"))               # 15 min pause

# Rule 7: Max Loss Per Window — stop trading if PnL drops below threshold
MAX_LOSS_PER_HOUR_USD = float(os.getenv("MAX_LOSS_PER_HOUR_USD", "25.0"))            # max loss per hour

# Hourly budget: max dollars that can be invested per hour.
# Sells return proceeds to the budget so net spend = buys - sell proceeds.
HOURLY_BUDGET_USD = float(os.getenv("HOURLY_BUDGET_USD", "50.0"))

# Rule 8: Last 90 Seconds Rule — no resting orders, IOC-only
LAST_SECONDS_IOC_ONLY = float(os.getenv("LAST_SECONDS_IOC_ONLY", "90.0"))           # seconds before close

# Rule 9: Cancel All Before Resolution — cancel everything N seconds before close
CANCEL_ALL_BEFORE_CLOSE_SEC = float(os.getenv("CANCEL_ALL_BEFORE_CLOSE_SEC", "30.0"))  # cancel all open orders

# ---------------------------------------------------------------------------
# IOC / TAKER ENTRY — immediate-fill for entry orders
# ---------------------------------------------------------------------------
IOC_ENTRY_ENABLED = bool(os.getenv("IOC_ENTRY_ENABLED", "True") not in ("", "0", "False", "false"))
ENTRY_USE_FOK = bool(os.getenv("ENTRY_USE_FOK", "False") not in ("", "0", "False", "false"))  # False=IOC (partial OK), True=FOK (all-or-nothing)
IOC_MAX_RETRIES = int(os.getenv("IOC_MAX_RETRIES", "2"))             # quick_buy attempts (2-3)
IOC_RETRY_DELAY_MS = float(os.getenv("IOC_RETRY_DELAY_MS", "100"))  # 75-150ms between attempts
IOC_TICK_STEP = float(os.getenv("IOC_TICK_STEP", "0.01"))           # +1 tick per retry
IOC_MAX_SLIPPAGE_CENTS = float(os.getenv("IOC_MAX_SLIPPAGE_CENTS", "1.5"))  # max above ask for buys

# ---------------------------------------------------------------------------
# BUDGET RESERVATION — reserve capital for pending/open orders
# ---------------------------------------------------------------------------
BUDGET_RESERVE_ENABLED = bool(os.getenv("BUDGET_RESERVE_ENABLED", "True") not in ("", "0", "False", "false"))
RESERVATION_STUCK_TIMEOUT_SEC = float(os.getenv("RESERVATION_STUCK_TIMEOUT_SEC", "25"))   # release stuck reservations — short for fast hourly windows

# ---------------------------------------------------------------------------
# IN-FLIGHT ORDER GUARD — prevent duplicate entries per token/outcome
# ---------------------------------------------------------------------------
ONE_INFLIGHT_PER_TOKEN = bool(os.getenv("ONE_INFLIGHT_PER_TOKEN", "True") not in ("", "0", "False", "false"))
PER_TOKEN_COOLDOWN_MS = float(os.getenv("PER_TOKEN_COOLDOWN_MS", "350"))        # min ms between orders on same token
GLOBAL_ORDER_RATE_LIMIT_PER_SEC = float(os.getenv("GLOBAL_ORDER_RATE_LIMIT_PER_SEC", "5"))  # max new orders/sec

# ---------------------------------------------------------------------------
# PER-TOKEN ATTEMPT BREAKER — prevent runaway loops on one token
# ---------------------------------------------------------------------------
TOKEN_ATTEMPT_MAX_PER_MIN = int(os.getenv("TOKEN_ATTEMPT_MAX_PER_MIN", "10"))     # max attempts per token per 60s
TOKEN_ATTEMPT_COOLDOWN_SEC = float(os.getenv("TOKEN_ATTEMPT_COOLDOWN_SEC", "45"))  # cooldown when breaker trips (30-60s)

# ---------------------------------------------------------------------------
# END-OF-HOUR FLATTEN — wind down positions before hour close
# ---------------------------------------------------------------------------
FLATTEN_START_MIN = float(os.getenv("FLATTEN_START_MIN", "57.0"))       # stop new entries, start trimming
HARD_FLATTEN_MIN = float(os.getenv("HARD_FLATTEN_MIN", "59.0"))         # force-close ALL with IOC sells
FLATTEN_IOC_SLIPPAGE_CENTS = float(os.getenv("FLATTEN_IOC_SLIPPAGE_CENTS", "2.0"))  # max slippage on flatten IOC sells
