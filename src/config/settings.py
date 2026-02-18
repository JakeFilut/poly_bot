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

# Exit ladder (scale out)
TP1 = 0.03; TP1_SELL_FRAC = 0.25
TP2 = 0.05; TP2_SELL_FRAC = 0.25
TP3 = 0.07; TP3_SELL_FRAC = 0.25
CORE_KEEP_FRAC = 0.25

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
POST_ONLY_WHEN_POSSIBLE = True
MAX_CROSS_SLIPPAGE = 0.01         # cross at most 1c if absolutely needed
LAYER_ORDERS = True
LAYER_COUNT = 3
LAYER_STEP = 0.01                 # 1c ladder
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
BURST_CROSS_MAX_SPREAD = 0.01          # cross at ask ONLY when spread <= 1c (used by taker gate)

# Dynamic price cap boost
CAP_BOOST_EDGE_THRESHOLD = 10.0  # edge_bps above which cap starts boosting
CAP_BOOST_MAX = 0.08             # max +8 cents boost (aggressive chase)
CAP_BOOST_EDGE_FULL = 30.0       # edge_bps at which full boost is applied

# ---------------------------------------------------------------------------
# Parity (straddle) arbitrage engine -- Up + Down ~= 1.000
# ---------------------------------------------------------------------------
PARITY_BUY_ENABLED = True                # buy cheap straddle (up_ask + dn_ask < 1)
PARITY_SELL_ENABLED = True               # sell rich straddle (up_bid + dn_bid > 1)
PARITY_MAX_USD_PER_SLUG = 40.0          # max total straddle investment per slug
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

# Maker queue discipline (reduce cancel spam)
MIN_REPLACE_INTERVAL_MS = 200            # min time between cancel/replace on same order
MAKER_ORDER_TIMEOUT_MS = 3000            # cancel maker order if unfilled after 3s

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
RESCUE_MAX_USD_PER_SLUG = 20.0          # max USD to spend completing straddle per slug
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
MIN_ORDER_INTERVAL_MS = float(os.getenv("MIN_ORDER_INTERVAL_MS", "500"))       # min ms between ANY orders on same slug
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
DSCALP_VEL_MIN_BPS_PER_MIN = float(os.getenv("DSCALP_VEL_MIN_BPS_PER_MIN", "1.0"))  # min velocity (supportive, not hard gate)
DSCALP_MAX_SPREAD_CENTS = float(os.getenv("DSCALP_MAX_SPREAD_CENTS", "2.0"))   # max spread for entry
DSCALP_MIN_ENTRY_EDGE_CENTS = float(os.getenv("DSCALP_MIN_ENTRY_EDGE_CENTS", "2.0"))  # min edge: outcome mid must be >=2c above 50c neutral
DSCALP_MAX_CACHE_AGE_MS = float(os.getenv("DSCALP_MAX_CACHE_AGE_MS", "250"))   # max cache age for entry

# Sizing -- one entry = one meaningful position, no micro-splits
DSCALP_STEP_USD = float(os.getenv("DSCALP_STEP_USD", "7.0"))                   # per-order size (~F247's $7.4 avg)
DSCALP_STEP_USD_MIN = float(os.getenv("DSCALP_STEP_USD_MIN", "6.0"))           # minimum entry size (no $1 clips)
DSCALP_MAX_USD_PER_SLUG = float(os.getenv("DSCALP_MAX_USD_PER_SLUG", "30.0"))  # max directional per slug
DSCALP_COOLDOWN_MS = float(os.getenv("DSCALP_COOLDOWN_MS", "4000"))            # 4s between entries (target ~15 trades/min)

# Exit ladder
DSCALP_TP1_CENTS = float(os.getenv("DSCALP_TP1_CENTS", "3.0"))                 # +3c: sell 25%
DSCALP_TP1_FRAC = float(os.getenv("DSCALP_TP1_FRAC", "0.25"))
DSCALP_TP2_CENTS = float(os.getenv("DSCALP_TP2_CENTS", "6.0"))                 # +6c: sell 25%
DSCALP_TP2_FRAC = float(os.getenv("DSCALP_TP2_FRAC", "0.25"))
DSCALP_TP3_CENTS = float(os.getenv("DSCALP_TP3_CENTS", "8.0"))                 # +8c: sell 25%, remainder for timeout/trailing
DSCALP_TP3_FRAC = float(os.getenv("DSCALP_TP3_FRAC", "0.25"))
DSCALP_EARLY_TP_CENTS = float(os.getenv("DSCALP_EARLY_TP_CENTS", "2.0"))       # +2c early exit (taker) — fires only on real danger
DSCALP_EARLY_TP_SPREAD_THRESH = float(os.getenv("DSCALP_EARLY_TP_SPREAD_THRESH", "6.0"))  # spread blowout threshold for early exit (cents)
DSCALP_VEL_REVERSAL_BPS = float(os.getenv("DSCALP_VEL_REVERSAL_BPS", "4.0"))   # velocity reversal threshold (bps/min against position)
DSCALP_MIN_HOLD_SEC = float(os.getenv("DSCALP_MIN_HOLD_SEC", "90"))            # 90s min hold -- bypass for emergency/spread-collapse/reversal
DSCALP_MAX_HOLD_SEC = float(os.getenv("DSCALP_MAX_HOLD_SEC", "600"))           # 10 min max hold
DSCALP_STOP_LOSS_CENTS = float(os.getenv("DSCALP_STOP_LOSS_CENTS", "5.0"))     # -5c stop loss (emergency only)

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
MAX_ENTRIES_PER_MIN_PER_SLUG = int(os.getenv("MAX_ENTRIES_PER_MIN_PER_SLUG", "40"))  # hard cap per slug per minute

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

# No-progress pause (per-slug)
OM_NO_PROGRESS_SUBMITS = int(os.getenv("OM_NO_PROGRESS_SUBMITS", "40"))             # submits in 60s with 0 fills -> pause
OM_NO_PROGRESS_PAUSE_SEC = float(os.getenv("OM_NO_PROGRESS_PAUSE_SEC", "120"))     # pause slug for 120s

# Kill-switch thresholds (execution errors only — sells always allowed)
OM_KILL_API_ERROR_THRESHOLD_PER_MIN = int(os.getenv("OM_KILL_API_ERROR_THRESHOLD_PER_MIN", "10"))
OM_KILL_ORPHAN_THRESHOLD_PER_MIN = int(os.getenv("OM_KILL_ORPHAN_THRESHOLD_PER_MIN", "5"))
OM_KILL_PAUSE_SEC = float(os.getenv("OM_KILL_PAUSE_SEC", "60"))                    # pause entries for 60s

# Global safety caps
OM_MAX_OPEN_ORDERS = int(os.getenv("OM_MAX_OPEN_ORDERS", "50"))                    # hard cap on tracked open orders
OM_MAX_TOTAL_USD = float(os.getenv("OM_MAX_TOTAL_USD", "500"))                     # max total exposure across all slugs
OM_MAX_PER_SLUG_USD = float(os.getenv("OM_MAX_PER_SLUG_USD", "60"))                # max exposure per slug

# Sanity report
OM_SANITY_REPORT_INTERVAL_SEC = float(os.getenv("OM_SANITY_REPORT_INTERVAL_SEC", "60"))  # report every 60s

# ---------------------------------------------------------------------------
# LIVE_SAFE mode — time-limited entry window + trade-size limiter
# ---------------------------------------------------------------------------
LIVE_SAFE_ENTRY_WINDOW_SEC = float(os.getenv("LIVE_SAFE_ENTRY_WINDOW_SEC", "300"))        # buys allowed for 300s after start
LIVE_SAFE_MAX_ORDER_USD = float(os.getenv("LIVE_SAFE_MAX_ORDER_USD", "2.00"))             # max buy order size in LIVE_SAFE

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
