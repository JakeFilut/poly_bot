"""
Configuration loader for Polymarket trader bot.
Loads credentials from .env and exposes trading parameters.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

_SRC_DIR = Path(__file__).resolve().parent.parent  # src/
_PROJECT_DIR = _SRC_DIR.parent  # poly_bot/

# Keys directory - where .env is stored
# Defaults to ../keys relative to project root, override with KEYS_DIR env var
KEYS_DIR = os.getenv("KEYS_DIR", str(_PROJECT_DIR.parent / "keys"))

# Log directory
LOG_DIR = os.getenv("LOG_DIR", str(_PROJECT_DIR / "logs"))

# =============================================================================
# LOAD .env
# =============================================================================

def load_config():
    """
    Load .env file and return validated config dict.
    Raises SystemExit if required variables are missing.
    """
    # Try multiple .env locations
    env_candidates = [
        os.path.join(KEYS_DIR, ".env"),
        str(_PROJECT_DIR / ".env"),
    ]

    loaded = False
    for env_path in env_candidates:
        if os.path.exists(env_path):
            load_dotenv(env_path)
            loaded = True
            break

    if not loaded:
        # Still try to load from environment (systemd, docker, etc.)
        pass

    poly_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
    if not poly_key:
        print("ERROR: POLYMARKET_PRIVATE_KEY not set.")
        print(f"  Searched: {env_candidates}")
        print("  Set it in .env or as an environment variable.")
        raise SystemExit(1)

    config = {
        "polymarket_private_key": poly_key,
        "log_dir": LOG_DIR,
        "debug": os.getenv("DEBUG", "false").lower() in ("true", "1", "yes"),
    }
    return config


# =============================================================================
# DEBUG FLAG (importable)
# =============================================================================

DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")

# =============================================================================
# POLYMARKET TRADING PARAMETERS
# =============================================================================

# Scan interval - how often to check markets
SCAN_INTERVAL_SECONDS = float(os.getenv("SCAN_INTERVAL_SECONDS", "5.0"))

# Order execution
ORDER_CHECK_INTERVAL = 0.05       # Check order status every 50ms
MAX_WAIT_FOR_FILL = 2.0           # Max seconds to wait for fill
PRICE_BUFFER = 0.01               # Buffer added to ask for faster fills

# Polymarket fee curve: fee = shares * price * 0.25 * (price * (1 - price))^2
def calc_poly_fee(price: float, shares: float) -> float:
    """Calculate Polymarket taker fee."""
    if price <= 0 or price >= 1:
        return 0.0
    return shares * price * 0.25 * (price * (1 - price)) ** 2

def calc_poly_fee_pct(price: float) -> float:
    """Get effective fee percentage at a given price."""
    if price <= 0 or price >= 1:
        return 0.0
    return 0.25 * (price * (1 - price)) ** 2 * 100

GAS_FEE_DOLLARS = 0.0025  # Polygon gas per trade

# Rate limiting
POLY_REQUEST_DELAY = 0.01    # 10ms between requests
RATE_LIMIT_BACKOFF = 30      # seconds on 403

# Trade sizing
MIN_BET_DOLLARS = float(os.getenv("MIN_BET_DOLLARS", "3.00"))
MAX_TRADE_QTY = int(os.getenv("MAX_TRADE_QTY", "50"))
