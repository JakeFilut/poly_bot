"""
Configuration constants for the arbitrage bot.
"""

import os
from pathlib import Path

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
# Paths are relative to the github folder (parent of arb_repo)
# Expected structure:
#   <anywhere>/github/
#     ├── keys/         <- .env and kalshi_private_key.pem
#     ├── logs/         <- Log files created here
#     └── arb_repo/     <- This codebase
#
# Can override with environment variables: ARB_LOG_DIR, ARB_KEYS_DIR

_SCRIPT_DIR = Path(__file__).resolve().parent  # arb_repo folder
_GITHUB_DIR = _SCRIPT_DIR.parent  # github folder (parent of arb_repo)

# =============================================================================
# MODE SETTINGS
# =============================================================================

# Set to True to only log profitable opportunities without placing real trades
LOGGING_ONLY = True

# Debug mode - set to True for detailed logging
DEBUG = False

# Keys directory - where .env and kalshi_private_key.pem are stored
# Defaults to ../keys relative to arb_repo, can override with ARB_KEYS_DIR env var
KEYS_DIR = os.getenv("ARB_KEYS_DIR", str(_GITHUB_DIR / "keys"))

# Log file paths - separate logs for live trades vs logging mode
# Defaults to ../logs relative to arb_repo, can override with ARB_LOG_DIR env var
LOG_DIR = os.getenv("ARB_LOG_DIR", str(_GITHUB_DIR / "logs"))
TRADES_LOG_LIVE = os.path.join(LOG_DIR, "arb_trades_live.csv")   # Actual trades
TRADES_LOG_LOG = os.path.join(LOG_DIR, "arb_trades_log.csv")     # Logging mode only
TRADES_LOG = TRADES_LOG_LIVE  # Default - will be overridden based on mode
SUMMARY_LOG = os.path.join(LOG_DIR, "arb_summary.txt")
POTENTIAL_TRADES_LOG = os.path.join(LOG_DIR, "potential_trades.csv")  # 2%-4.9% edge opportunities
CONSOLE_HIGHLIGHTS_LOG = os.path.join(LOG_DIR, "console_highlights.log")  # Key events: strikes, scans, trades

# =============================================================================
# TRADE LIMITS
# =============================================================================

# Target number of SUCCESSFUL trades (both legs filled) before stopping
# Only counts fully completed arbitrage trades, not attempts or partial fills
TARGET_SUCCESSFUL_TRADES = 5

# Minimum average profit percentage (after N trades) - stops if average drops below this
MIN_AVG_PROFIT_PCT = 4.0          # Stop if average profit per trade < 4%
MIN_TRADES_BEFORE_AVG_CHECK = 3   # Only check average after this many trades

# In LOGGING_ONLY mode: run forever (0 = infinite)
MAX_LOG_OPPORTUNITIES = 0  # 0 = run forever until Ctrl+C

# Scan interval
SCAN_INTERVAL_SECONDS = 0.5  # How often to check for opportunities

# =============================================================================
# TRADE SIZING
# =============================================================================

# Minimum dollar amount per side (e.g., if price is $0.30, need 10 contracts)
# Increased to spread gas costs over more contracts for better margins
MIN_BET_DOLLARS = 3.00  # Doubled for better gas efficiency

# Maximum contracts per trade (safety limit)
MAX_TRADE_QTY = 50

# 1-hour market trading (disabled — strike price mismatch issues)
ENABLE_1H_TRADING = False

# 5-minute cross-strike trading (disabled — Kalshi and Polymarket use different
# resolution prices, so cross-strike arb can double-lose)
ENABLE_5M_CROSS_STRIKE = False

# =============================================================================
# EDGE THRESHOLDS (by mode)
# =============================================================================

# Minimum edge WITH fees+gas+buffer to consider a trade
# This is the "real" edge after all costs are accounted for
MIN_EDGE_PCT = 3.5              # Default (used by regular trading mode)
MIN_EDGE_PCT_FOREVER = 7.0      # Forever mode / TESTING LOGS edge threshold
MIN_EDGE_PCT_TESTING = 5.0      # Testing mode edge threshold (lower for more trades)

# Minimum edge WITH fees+gas+buffer to actually execute
# Only execute if we still have at least 1% edge after all costs
MIN_EXECUTE_EDGE_PCT = 1.0

# Maximum edge to consider (skip outliers - likely data errors)
MAX_NET_EDGE_TO_TRADE_PCT = 15.0

# Balance thresholds for forever mode
MIN_KALSHI_BALANCE_FOREVER = 20.0   # Stop program if Kalshi below this
MIN_POLY_BALANCE_FOREVER = 20.0     # Redeem if Poly below this

# =============================================================================
# STOP LOSS THRESHOLDS (by mode)
# =============================================================================

# Maximum loss as percentage of trade cost before unwinding on Kalshi
MAX_LOSS_PCT = 0.03             # Default: 3% max loss (regular trading mode)
MAX_LOSS_PCT_FOREVER = 0.03     # Forever mode: 3% max loss
MAX_LOSS_PCT_TESTING = 0.03     # Testing mode: 3% max loss

# =============================================================================
# FEE ESTIMATES
# =============================================================================

# Kalshi fee formula (effective Feb 5, 2026):
# Taker fee = ceil(0.07 × C × P × (1-P))
# Maker fee = ceil(0.0175 × C × P × (1-P))
# Where C = contracts, P = price (0.50 = 50 cents)
# Max fee at P=0.50, drops to near zero at extremes
import math

def calc_kalshi_fee(price: float, contracts: int, is_maker: bool = False) -> float:
    """
    Calculate Kalshi transaction fee.

    Formula: ceil(rate × C × P × (1-P))
    - Taker rate: 0.07 (7%)
    - Maker rate: 0.0175 (1.75%)

    Examples (100 contracts):
    - price $0.50: taker fee = $1.75, maker fee = $0.44
    - price $0.20: taker fee = $1.12, maker fee = $0.28
    - price $0.90: taker fee = $0.63, maker fee = $0.16
    """
    if price <= 0 or price >= 1:
        return 0.0
    rate = 0.0175 if is_maker else 0.07
    fee = rate * contracts * price * (1 - price)
    return math.ceil(fee * 100) / 100  # Round up to nearest cent

# Legacy constants (kept for compatibility, but use calc_kalshi_fee instead)
KALSHI_FEE_PER_CONTRACT = 0.02  # Approximate average, use calc_kalshi_fee for accuracy
KALSHI_FEE_PCT = 0.0

# Polymarket fee curve: fee = shares * price * 0.25 * (price * (1 - price))^2
# Max fee is 1.56% at 50% probability, drops to ~0% at extremes
# See: https://docs.polymarket.com/#trading
def calc_poly_fee(price: float, shares: float) -> float:
    """
    Calculate Polymarket taker fee using actual fee curve.

    Formula: fee = shares * price * 0.25 * (price * (1 - price))^2

    Examples (100 shares):
    - price $0.50: fee = $0.78 (1.56% effective)
    - price $0.75: fee = $0.66 (0.88% effective)
    - price $0.90: fee = $0.18 (0.20% effective)
    """
    if price <= 0 or price >= 1:
        return 0.0
    return shares * price * 0.25 * (price * (1 - price)) ** 2

def calc_poly_fee_pct(price: float) -> float:
    """Get effective fee percentage at a given price."""
    if price <= 0 or price >= 1:
        return 0.0
    return 0.25 * (price * (1 - price)) ** 2 * 100  # Return as percentage

GAS_FEE_DOLLARS = 0.0025        # Polygon gas ~fractions of a cent per trade

# Payout per fully-hedged contract
PAYOUT_PER_UNIT = 1.00

# =============================================================================
# ORDER EXECUTION
# =============================================================================

# Time to wait for order fills
ORDER_CHECK_INTERVAL = 0.05  # Check order status every 50ms (was 100ms)
MAX_WAIT_FOR_FILL = 0.5      # Max 500ms to wait for Kalshi fill (was 1s)
POLY_WAIT_FOR_FILL = 0.175   # VPS in Ireland ~88ms RTT, 175ms is 2x safe margin

# Price buffer settings - added to limit price for faster fills
PRICE_BUFFER_START = 0.01    # 1 cent buffer on Kalshi
PRICE_BUFFER_INCREMENT = 0.002  # Increase by 0.2 cents each retry
PRICE_BUFFER_MAX = 0.02      # Max 2 cent buffer (stop retrying after this)

# Polymarket buffer - added to best ask for faster fills
POLY_BUFFER_START = 0.01     # 1 cent buffer on Polymarket orders

# Number of retry attempts when unwinding a position
UNWIND_RETRY_ATTEMPTS = 3
UNWIND_RETRY_DELAY = 0.25    # Seconds between unwind retries

# =============================================================================
# RATE LIMITING (to avoid Cloudflare blocks)
# =============================================================================

# Delay between Polymarket API requests (seconds)
POLY_REQUEST_DELAY = 0.01     # 10ms between requests (was 15ms)

# Delay after placing an order before checking status
POST_ORDER_DELAY = 0.005      # 5ms after order placement (fast VPS connection)

# If rate limited, wait this long before retrying
RATE_LIMIT_BACKOFF = 30     # 30 seconds on 403 error

# =============================================================================
# STRIKE PRICE TIMING
# =============================================================================

# Chainlink price offset: fetch price this many seconds AFTER window start
# Polymarket's "price to beat" is the Chainlink price at the exact boundary
# payload.timestamp == windowStart * 1000 (the t+0s price)
STRIKE_PRICE_OFFSET_SECONDS = 0  # t+0s: price at exact boundary

# 2-Phase startup timing (when thresholds are ON):
#   Phase 1 at 60s: Set up Kalshi connections + get Kalshi strike prices
#   Phase 2: Immediately after Phase 1 (1s delay) scrape Polymarket strike prices
KALSHI_SETUP_SECONDS = 60  # Set up Kalshi connections at 60s into window

# Wait time before fetching strikes: Poly scrape starts right after Kalshi
# (dynamically set after Phase 1 completes)
WAIT_BEFORE_TRADING_SECONDS = 3  # Price arrives at ~t+0s, trade at t+3s

# =============================================================================
# RTDS (Real-Time Data Stream) - Polymarket's Chainlink WebSocket
# =============================================================================
# Polymarket relays Chainlink prices via wss://ws-live-data.polymarket.com/ws
# This is the EXACT price shown as "price to beat" in their UI.
# We capture this at window start and log for a delay window to verify accuracy.

# How long (seconds) after window start to keep logging RTDS prices for comparison
# This helps detect any delay between when the window opens and when the price settles
RTDS_LOG_DURATION_SECONDS = 30  # Log RTDS prices for 30s after window start

# =============================================================================
# TESTING MODE RULES (ChatGPT-optimized strategy)
# =============================================================================

# Minimum spread thresholds (strike gap) to consider trading
# spread_pct = abs(kalshi_strike - poly_strike) / min(kalshi_strike, poly_strike)
TESTING_MIN_SPREAD_PCT = {
    "BTC": 0.0005,  # 0.05%
    "ETH": 0.0007,  # 0.07%
    "SOL": 0.0012,  # 0.12%
    "XRP": 0.0012,  # 0.12%
}

# Late window spread thresholds (add 0.01% when 70-80% through window)
TESTING_LATE_SPREAD_PCT = {
    "BTC": 0.0007,  # 0.07%
    "ETH": 0.0008,  # 0.08%
    "SOL": 0.0009,  # 0.09%
    "XRP": 0.0009,  # 0.09%
}

# Minimum actual edge (after fees+gas) to trade
TESTING_MIN_EDGE_NORMAL = 2.0     # +2% in normal window (0-70%)
TESTING_MIN_EDGE_LATE = 3.0       # +3% in late window (70-80% progress)
TESTING_MIN_EDGE_FINAL = 5.0      # +5% in final 3 minutes (80%+ progress)

# Window progress thresholds
TESTING_LATE_WINDOW_START = 0.70  # Start applying late rules at 70%
TESTING_FINAL_WINDOW_START = 0.90 # Final 1.5 minutes - no trading, just wait
TESTING_FORCE_FLATTEN = 0.90      # Force flatten positions at 90%

# Final window adjustments (last 1.5 minutes)
TESTING_FINAL_QTY_MULTIPLIER = 0.5  # Half quantity in final window
TESTING_FINAL_UNWIND_FASTER = True  # Faster unwind for final window trades
TESTING_NO_TRADE_FINAL_WINDOW = True  # No new trades in last 1.5 minutes (90%+ progress)

# Position sizing
TESTING_MAX_COST_PER_TRADE = 25.0  # Max $25 per trade
TESTING_MAX_COST_PER_CRYPTO_MARKET = 50.0  # Max $50 per crypto per 15-min market window
TESTING_BALANCE_RISK_PCT = 0.02    # Max 2% of balance per trade
TESTING_SOL_SIZE_MULTIPLIER = 0.7  # SOL trades at 70% of BTC size

# Edge-based size scaling (multipliers)
TESTING_SIZE_SCALE = {
    "small": 1.0,    # actual_edge < 4%
    "medium": 1.5,   # 4% <= actual_edge < 8%
    "large": 2.0,    # actual_edge >= 8%
}

# Exit thresholds
TESTING_EXIT_EDGE_THRESHOLD = -1.0  # Exit if edge drops below -1%
TESTING_DEAD_LEG_THRESHOLD = 0.10   # Close leg if value < $0.10

# Kill-switch thresholds
TESTING_MAX_DRAWDOWN = 50.0        # Stop if down $50 from session peak
TESTING_MAX_UNWINDS = 2            # Stop after 2 unwinds in session
TESTING_MAX_CONSECUTIVE_LOSSES = 5 # Stop after 5 consecutive losers
TESTING_COOLDOWN_MINUTES = 15      # Cooldown after hitting consecutive loss limit

# Fill safety
TESTING_FILL_TIMEOUT_SEC = 2.0     # Wait max 2s for fill
TESTING_MAX_RETRIES_POLY = 2       # Max Poly retries
TESTING_MAX_RETRIES_KALSHI = 1     # Max Kalshi retries
TESTING_UNHEDGED_MAX_SEC = 3.0     # Max time unhedged before forced unwind

# Minimum liquidity (contracts) on both sides to trade
TESTING_MIN_LIQUIDITY = 30         # Require 30 contracts available

# =============================================================================
# NO-THRESHOLD MODE SETTINGS (when user selects thresholds = no)
# =============================================================================
# These override various settings when strike thresholds are disabled.
# The bot skips strike price lookups and uses simpler trading rules.

NO_THRESHOLD_WAIT_SECONDS = 60              # Wait 1 minute at window start (vs 2 min with thresholds)
NO_THRESHOLD_MIN_EDGE_PCT = 12.0            # Require 12% edge to trade
NO_THRESHOLD_MAX_TRADES_PER_CRYPTO = 1      # Max 1 trade per crypto per 15-min window
NO_THRESHOLD_MAX_QTY = 100                  # Max 100 contracts per trade
NO_THRESHOLD_FIRST_MINUTE_PROGRESS = 60 / 900   # First 60s of window = ~0.0667 progress
NO_THRESHOLD_LAST_MINUTE_PROGRESS = 0.90    # No trading in last 1.5 min (90% of 15-min window)

# =============================================================================
# LAST 2 MINUTES CONFIRMATION (trade direction confirmation via external price)
# =============================================================================
# During the final 2 minutes of the 15-minute window, confirm trade direction
# using a live external price before placing a trade.
# Only works when exactly ONE is true; if both true, use existing logic.
BINANCE_LAST_TWO_MIN_ENABLED = False
CHAINLINK_LAST_TWO_MIN_ENABLED = True

# =============================================================================
# MAX CONTRACTS PER WINDOW (per-crypto cap)
# =============================================================================
# Limits contracts traded PER CRYPTO in a single 15-min window.
# E.g., if set to 33, each crypto (BTC, ETH, SOL, XRP) can trade up to 33
# contracts independently per window.
MAX_CONTRACTS_PER_WINDOW_ENABLED = True
MAX_CONTRACTS_PER_WINDOW = 33

