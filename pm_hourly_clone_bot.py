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
- Risk caps per market/crypto + daily stop loss
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
# Entry thresholds (bps) — dynamic time-varying threshold
# These match the "clone frequency" profile by default.
# Switch to MAX_EV by setting PROFILE=MAX_EV.
# -----------------------------------------------------------------------------
PROFILE = os.getenv("PROFILE", "CLONE").upper()   # CLONE or MAX_EV
THR_EARLY_5_15 = 6
THR_MID_15_45  = 10
THR_LATE_45_57 = 6
THR_EARLY_5_15_MAXEV = 12
THR_MID_15_45_MAXEV  = 18
THR_LATE_45_57_MAXEV = 10
# Price cap curve (max price you will pay to BUY), piecewise by time bucket
CAP_0_5   = 0.67
CAP_5_15  = 0.82
CAP_15_30 = 0.90
CAP_30_45 = 0.96
CAP_45_60 = 0.97
# Drift persistence & velocity (hidden edge)
PERSISTENCE_SEC = 6.0           # signal must hold for >= 6s (one snapshot)
MIN_DELTA_VEL_BPS_PER_MIN = 1.0 # require some "push" to scale size (not to enter)
# Volatility normalization
Z_WINDOW_SEC = 300.0            # 5 minutes for zscore
Z_ENTRY_MIN = 1.0               # only enter if zscore >= 1.0 (optional gate)
Z_ENTRY_ENABLED = False
# Orderbook imbalance
IMB_ENABLED = False
IMB_LEVELS = 5
IMB_MIN = 1.15                  # bidDepth/askDepth must exceed this for with-drift buys
IMB_MAX_SPREAD = 0.06           # skip if spread too wide (6c)
# Pullback entry
PULLBACK_ENABLED = False
PULLBACK_CENTS = 0.02           # wait for 2c pullback from recent extreme
PULLBACK_LOOKBACK_SEC = 90.0
# Cooldowns
ENTRY_COOLDOWN_SEC = 20.0
REENTRY_COOLDOWN_SEC = 10.0
# Base clip sizing (USDC cost) as % of bankroll
BASE_CLIP_PCT = 0.0020  # 0.2% bankroll per tick
EARLY_SIZE_MULT = 0.60   # reduce size when t < 10
# Size multipliers by abs_delta_bps
SIZING_MULTIPLIERS = [
    (8,   15, 1.00),
    (15,  25, 1.50),
    (25,  40, 2.00),
    (40,  75, 2.50),
    (75,  10_000, 3.00),
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
# Late scalp engine
LATE_SCALP_ENABLED = True
LATE_SCALP_T_START = 40.0
LATE_SCALP_T_END   = 58.0
LATE_SCALP_PRICE_MAX = 0.80
LATE_SCALP_ABSDELTA_MIN = 5.0
LATE_SCALP_ABSDELTA_MAX = 20.0
LATE_SCALP_TP_CENTS = 0.03      # aim +3c
LATE_SCALP_MAX_HOLD_MIN = 10.0
# Risk caps
MAX_COST_PER_MARKET_PCT = 0.008   # 0.8% bankroll per market-hour
MAX_COST_PER_CRYPTO_PCT = 0.020   # 2.0% bankroll per crypto across markets
DAILY_STOP_LOSS_PCT = 0.020       # 2% bankroll
# Execution policy
POST_ONLY_WHEN_POSSIBLE = True
MAX_CROSS_SLIPPAGE = 0.01         # cross at most 1c if absolutely needed
LAYER_ORDERS = True
LAYER_COUNT = 3
LAYER_STEP = 0.01                 # 1c ladder
MIN_ORDER_USDC = 1.0
MIN_QTY = _LOG_MIN_QTY  # from logger — below this, position is dust
EDGE_K = 0.05    # sigmoid steepness: delta_bps -> P(Up)
# Correlation exposure scaling (reduces correlated stacking)
CORR_SCALE_ENABLED = True
BTC_LEAD = True
BTC_EXPOSURE_REDUCE_OTHERS = 0.50  # up to 50% size reduction if BTC exposure high
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

def write_jsonl(event: dict) -> None:
    """Legacy shim — delegates to _LOGGER.log_jsonl if available."""
    if _LOGGER is not None:
        _LOGGER.log_jsonl(event)
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
def entry_threshold_bps(t_min: float) -> float:
    if PROFILE == "MAX_EV":
        if 5 <= t_min < 15: return THR_EARLY_5_15_MAXEV
        if 15 <= t_min < 45: return THR_MID_15_45_MAXEV
        if 45 <= t_min <= 57: return THR_LATE_45_57_MAXEV
        return 10_000
    else:  # CLONE
        if TRADE_START_MIN <= t_min < 5: return THR_EARLY_5_15
        if 5 <= t_min < 15: return THR_EARLY_5_15
        if 15 <= t_min < 45: return THR_MID_15_45
        if 45 <= t_min <= 57: return THR_LATE_45_57
        return 10_000
def price_cap(t_min: float) -> float:
    if 0 <= t_min < 5:   return CAP_0_5
    if 5 <= t_min < 15:  return CAP_5_15
    if 15 <= t_min < 30: return CAP_15_30
    if 30 <= t_min < 45: return CAP_30_45
    if 45 <= t_min < 60: return CAP_45_60
    return 0.0
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
    if len(delta_series) < 2:
        return 0.0
    now = utc_now()
    target = now - timedelta(seconds=lookback_sec)
    latest_ts, latest_d = delta_series[-1]
    latest_t = datetime.strptime(latest_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    # find nearest prior point
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
            r = self.session.get(url, params={"token_id": token_id}, timeout=5)
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
                          size: float, post_only: bool = True) -> str:
        """
        Place a limit order on the CLOB.
        Returns order_id (str).  In LOG mode returns a paper id.
        """
        if MODE == "LOG":
            return f"paper_{int(time.time()*1000)}_{random.randint(100,999)}"

        if not self._clob:
            raise RuntimeError("CLOB client not initialised (missing POLYMARKET_PRIVATE_KEY)")

        from py_clob_client.clob_types import OrderArgs, OrderType

        price = max(0.01, min(0.99, price))
        qty   = int(float(size))
        if qty < 1:
            return ""

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
                write_jsonl({"event_type":"ORDER_PLACED", "order_id": oid,
                             "token_id": token_id[-12:], "side": side,
                             "price": price, "qty": qty,
                             "status": response.get("status")})
                return oid
        except Exception as e:
            write_jsonl({"event_type":"ORDER_ERROR", "err": str(e)[:200],
                         "token_id": token_id[-12:], "side": side,
                         "price": price, "qty": qty})
        return ""

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
        self.day_start_equity = BANKROLL_START_USDC  # equity at start of day for daily P&L
        self.daily_pnl_usdc = 0.0  # daily P&L = current equity - day_start_equity
        self.market_states: Dict[str, MarketState] = {}   # slug -> MarketState
        self.signal_hist: Dict[str, List[Tuple[str, bool]]] = {}  # slug -> [(ts, valid_signal)]
        self.last_book: Dict[str, Dict[str, BookTop]] = {}  # slug -> outcome -> BookTop
        self.recent_extreme_price: Dict[str, Dict[str, float]] = {} # slug->outcome->extreme
        self.hour_index_counters: Dict[str, int] = {c: 0 for c in CRYPTOS}  # crypto -> monotonic index
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
            self.daily_pnl_usdc = float(raw.get("daily_pnl_usdc", self.daily_pnl_usdc))
            self.day_start_equity = float(raw.get("day_start_equity", self.day_start_equity))
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
                "daily_pnl_usdc": self.daily_pnl_usdc,
                "day_start_equity": self.day_start_equity,
                "equity_usdc": self._equity(),
                "market_states": ms,
            }
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(raw, f, indent=2)
        except Exception as e:
            write_jsonl({"event_type":"STATE_SAVE_ERROR", "err": str(e)})
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
            self.daily_pnl_usdc = 0.0
            write_jsonl({"event_type":"NEW_DAY_RESET", "day_start_equity": round(self.day_start_equity, 2)})
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
        self.daily_pnl_usdc += pnl
        self._clean_dust(pos)
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
    # -----------------------------
    # Risk checks
    # -----------------------------
    def _market_cost_usdc(self, st: MarketState) -> float:
        # paper approximation: sum pos.cost_usdc
        return sum(p.cost_usdc for p in st.positions.values())
    def _crypto_cost_usdc(self, crypto: str) -> float:
        s = 0.0
        for st in self.market_states.values():
            if st.crypto == crypto:
                s += sum(p.cost_usdc for p in st.positions.values())
        return s
    def _risk_ok(self, st: MarketState) -> bool:
        if self.daily_pnl_usdc < -self.cash_usdc * DAILY_STOP_LOSS_PCT:
            return False
        if self._market_cost_usdc(st) > self.cash_usdc * MAX_COST_PER_MARKET_PCT:
            return False
        if self._crypto_cost_usdc(st.crypto) > self.cash_usdc * MAX_COST_PER_CRYPTO_PCT:
            return False
        return True
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
        self.logger.log_event({"event_type": "START", "mode": MODE, "profile": PROFILE,
                               "cash": self.cash_usdc, "realized_pnl": self.realized_pnl_usdc,
                               "csv_path": self.logger._csv_path,
                               "jsonl_path": self.logger._jsonl_path})
        self._last_balance_print = 0.0
        while self.running:
            self._reset_daily_if_needed()
            try:
                markets = self._get_markets()
                for m in markets:
                    self._ensure_market_state(m)
                self._resolve_ended_hours(markets)
                prefetched = []
                if markets:
                    with ThreadPoolExecutor(max_workers=len(markets)) as pool:
                        futures = {pool.submit(self._prefetch_market_data, m): m for m in markets}
                        for fut in as_completed(futures):
                            try:
                                prefetched.append(fut.result())
                            except Exception as e:
                                m = futures[fut]
                                self.logger.log_event({"event_type": "PREFETCH_ERROR",
                                                       "slug": m.slug, "err": str(e)})
                for data in prefetched:
                    self._step_market_with_data(data)
                self._save_state()
                # Rotate log files if they exceed size limits
                self.logger.rotate_files_if_needed()
                now = time.time()
                if now - self._last_balance_print >= 30.0:
                    self._print_balance_summary()
                    self._last_balance_print = now
            except Exception as e:
                self.logger.log_event({"event_type": "LOOP_ERROR", "err": str(e)})
            time.sleep(EVAL_EVERY_SEC)
        self.logger.log_event({"event_type": "STOPPED", "cash": self.cash_usdc,
                               "realized_pnl": self.realized_pnl_usdc,
                               "equity": round(self._equity(), 2),
                               "daily_pnl": self.daily_pnl_usdc})
        self._save_state()
        self.logger.close()
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
                self.daily_pnl_usdc += pnl
                pos_details.append({"outcome": outcome, "qty": pos.qty,
                                    "payout": payout, "pnl": pnl})
                pos.qty = 0.0
                pos.cost_usdc = 0.0
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
                       vel: float, z: float, up_book: BookTop, dn_book: BookTop) -> dict:
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
        st.delta_hist.append((iso_z(now), delta_bps))
        st.delta_hist = st.delta_hist[-2000:]
        st.price_hist.append((iso_z(now), spot))
        st.price_hist = st.price_hist[-2000:]
        vel = delta_velocity_bps_per_min(st.delta_hist, lookback_sec=30.0)
        z = zscore(st.delta_hist) if Z_ENTRY_ENABLED else 0.0
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
            )
        # ── SNAPSHOT_ON_CHANGE (only when significant changes happen) ──
        snap_dict = {
            "up_mid": up_book.mid, "dn_mid": dn_book.mid,
            "up_spread": up_book.spread, "dn_spread": dn_book.spread,
            "up_imb": up_book.imb, "dn_imb": dn_book.imb,
            "delta_bps": delta_bps,
            "entry_thr_bps": entry_threshold_bps(t_min),
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
        # manage exits first (inventory recycling)
        self._manage_exits(m, st, t_min, delta_bps, ctx)
        # stop adding risk after minute 57
        if t_min > TRADE_STOP_ADD_MIN:
            return
        # risk gate
        if not self._risk_ok(st):
            return
        # Core engine: drift-direction entries
        self._core_entries(ctx)
        # Late scalp engine: cheap side scalps
        if LATE_SCALP_ENABLED:
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
    def _core_entries(self, ctx: dict):
        m, st = ctx["m"], ctx["st"]
        t_min = ctx["t_min"]
        delta_bps, abs_delta_bps = ctx["delta_bps"], ctx["abs_delta_bps"]
        vel, z = ctx["vel"], ctx["z"]
        up_book, dn_book = ctx["up_book"], ctx["dn_book"]
        spot = ctx["spot"]
        # Build valid signal (time + threshold)
        thr = entry_threshold_bps(t_min)
        cap = price_cap(t_min)
        valid_time = (t_min >= TRADE_START_MIN)
        valid_delta = (abs_delta_bps >= thr)
        valid_z = (not Z_ENTRY_ENABLED) or (abs(z) >= Z_ENTRY_MIN)
        outcome = ctx["drift_dir"]
        book = up_book if outcome == "Up" else dn_book
        valid_price = (book.ask <= cap)
        valid_spread = (book.spread <= IMB_MAX_SPREAD)
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
        # Cooldown check
        cooldown_active = False
        if st.last_entry_ts:
            last = datetime.strptime(st.last_entry_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            cooldown_active = (utc_now() - last).total_seconds() < ENTRY_COOLDOWN_SEC
        risk_blocked = not self._risk_ok(st)
        # Sizing
        mult = sizing_mult(abs_delta_bps)
        clip = self._calc_clip(m.crypto, t_min, abs_delta_bps)
        if mult >= 2.0 and vel < MIN_DELTA_VEL_BPS_PER_MIN:
            clip *= 0.6
        if CORR_SCALE_ENABLED and BTC_LEAD and m.crypto != "BTC":
            btc_cost = self._crypto_cost_usdc("BTC")
            if btc_cost > 0:
                reduce = clamp(btc_cost / (self.cash_usdc * MAX_COST_PER_CRYPTO_PCT), 0.0, 1.0)
                clip *= (1.0 - BTC_EXPOSURE_REDUCE_OTHERS * reduce)
        # ---- DECISION event (JSONL only — every tick, for tuning) ----
        will_trade = sig and persist_ok and not cooldown_active and not risk_blocked and clip >= MIN_ORDER_USDC
        # Build skip_reason for non-trades
        skip_reason = ""
        if not will_trade:
            reasons = []
            if not valid_time: reasons.append("time")
            if not valid_delta: reasons.append(f"delta({abs_delta_bps:.1f}<{thr})")
            if not valid_z: reasons.append("zscore")
            if not valid_price: reasons.append(f"price({book.ask:.3f}>{cap})")
            if not valid_spread: reasons.append(f"spread({book.spread:.3f})")
            if not valid_imb: reasons.append("imb")
            if not valid_pullback: reasons.append("pullback")
            if sig and not persist_ok: reasons.append("persistence")
            if sig and cooldown_active: reasons.append("cooldown")
            if sig and risk_blocked: reasons.append("risk_cap")
            if sig and persist_ok and not cooldown_active and not risk_blocked and clip < MIN_ORDER_USDC:
                reasons.append(f"clip_too_small({clip:.2f})")
            skip_reason = "|".join(reasons)
        # Favored outcome = direction model favors
        favored_outcome = "Up" if ctx["edge_up"] > ctx["edge_down"] else "Down"
        favored_strength_bps = abs_delta_bps
        self.logger.log_decision({
            "engine": "CORE", "slug": m.slug, "crypto": m.crypto,
            "hour_start_utc": ctx["hour_start_utc"], "hour_index": ctx["hour_index"],
            "t_min": round(t_min, 3),
            "phase": ctx["phase"], "seconds_to_close": round(ctx["seconds_to_close"], 1),
            "selected_outcome": outcome,
            "valid_time": valid_time, "valid_delta": valid_delta, "valid_z": valid_z,
            "valid_price": valid_price, "valid_spread": valid_spread,
            "valid_imb": valid_imb, "valid_pullback": valid_pullback,
            "persistence_ok": persist_ok, "cooldown_active": cooldown_active,
            "risk_blocked": risk_blocked,
            "cap_used": cap, "thr_used": thr, "size_mult": mult,
            "clip_usdc": round(clip, 4),
            "will_trade": will_trade, "did_trade": will_trade,
            "skip_reason": skip_reason,
            "spot": spot, "hour_open": ctx["hour_open"],
            "delta_bps": round(delta_bps, 3), "abs_delta_bps": round(abs_delta_bps, 3),
            "vel": round(vel, 3), "z": round(z, 3),
        })
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
        # ---- Proceed to trade ----
        now_iso = iso_z(utc_now())
        qty = clip / max(1e-9, book.ask)
        decision_id = new_decision_id()
        client_oid = new_order_id()
        pos = st.positions[outcome]
        # Assign position_id + trade_id on first entry for this position lifecycle
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
        print(f"  [TRADE!] {m.crypto:5s} {outcome} clip=${clip:.2f} ask={book.ask:.3f}")
        # ORDER_INTENT
        self.logger.log_order_intent(
            engine="CORE", reason="ENTRY_DELTA",
            decision_id=decision_id, position_id=pos.position_id,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=qty, target_price=book.ask,
            usdc_cost=clip, ctx=ctx, book_fields=bk_fields,
        )
        # ORDER_SUBMIT
        self.logger.log_order_submit(
            engine="CORE", reason="ENTRY_DELTA",
            decision_id=decision_id, position_id=pos.position_id,
            client_order_id=client_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=qty, target_price=book.ask,
            usdc_cost=clip, ctx=ctx, book_fields=bk_fields,
        )
        st.last_entry_ts = now_iso
        if MODE == "LOG":
            self._paper_buy(st, outcome, book.ask, qty, clip)
            mt = infer_maker_taker("BUY", book.ask, book)
            sc = spread_capture_fields("BUY", book.ask, book)
            self.logger.log_order_fill(
                engine="CORE", reason="ENTRY_DELTA",
                decision_id=decision_id, client_order_id=client_oid,
                position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=qty, fill_price=book.ask,
                usdc_cost=clip, fees_usdc=0.0,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
            )
            return
        self._place_layered_buy(m, outcome, qty, book.ask)
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
            self.logger.log_order_fill(
                engine="LATE_SCALP", reason="ENTRY_SCALP",
                decision_id=decision_id, client_order_id=client_oid,
                position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=qty, fill_price=book.ask,
                usdc_cost=clip, fees_usdc=0.0,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
            )
            pos.scalp_mode = True
            pos.scalp_open_ts = now_iso
            return
        self._place_layered_buy(m, outcome, qty, book.ask)
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
            # --- DERISK with cooldown + change detection ---
            derisk_triggered = False
            if outcome == "Up" and delta_bps < +DERISK_CROSS_BPS:
                derisk_triggered = True
            elif outcome == "Down" and delta_bps > -DERISK_CROSS_BPS:
                derisk_triggered = True
            if derisk_triggered:
                qty = pos.qty * DERISK_SELL_FRAC_PER_TICK
                if qty >= MIN_QTY:
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
                        self._do_sell(m, st, outcome, qty, book.bid,
                                      reason="DERISK_REVERSAL", leg="DERISK", ctx=ctx)
                        pos.last_derisk_ts = iso_z(now_t)
                        pos.last_derisk_mid = book.mid
                continue
            # --- Take-profit ladder (only if qty after TP is meaningful) ---
            if (not pos.tp1_done) and book.bid >= pos.vwap + TP1:
                tp_qty = pos.qty * TP1_SELL_FRAC
                if tp_qty >= MIN_QTY:
                    tp_target = pos.vwap + TP1
                    self._do_sell(m, st, outcome, tp_qty, book.bid,
                                  reason="TP1", leg="TP1", target_price=tp_target, ctx=ctx)
                pos.tp1_done = True
            if (not pos.tp2_done) and book.bid >= pos.vwap + TP2:
                tp_qty = pos.qty * TP2_SELL_FRAC
                if tp_qty >= MIN_QTY:
                    tp_target = pos.vwap + TP2
                    self._do_sell(m, st, outcome, tp_qty, book.bid,
                                  reason="TP2", leg="TP2", target_price=tp_target, ctx=ctx)
                pos.tp2_done = True
            if (not pos.tp3_done) and book.bid >= pos.vwap + TP3:
                tp_qty = pos.qty * TP3_SELL_FRAC
                if tp_qty >= MIN_QTY:
                    tp_target = pos.vwap + TP3
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
                 ctx: Optional[dict] = None):
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
        if MODE == "LOG":
            pnl = self._paper_sell(st, outcome, price, qty)
            mt = infer_maker_taker("SELL", price, ref_book) if ref_book else ""
            sc = spread_capture_fields("SELL", price, ref_book) if ref_book else {}
            # ORDER_FILL
            self.logger.log_order_fill(
                engine="EXIT", reason=reason,
                decision_id=decision_id, client_order_id=client_oid,
                position_id=position_id, parent_order_id=parent_oid,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="SELL", qty=qty, fill_price=price,
                usdc_cost=usdc_cost, fees_usdc=0.0,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                realized_pnl_usdc=pnl, net_pnl_usdc=pnl,
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
                    "gross_pnl_usdc": round(pnl, 4), "net_pnl_usdc": round(pnl, 4),
                    "time_in_position_sec": round(time_held, 1),
                    "max_favorable_bps": round(mfe, 3), "max_adverse_bps": round(mae, 3),
                    "exit_reason": reason,
                }, also_csv=True)
            return
        self.client.place_limit_order(token_id, "SELL", price, qty, post_only=False)
    def _place_layered_buy(self, m: MarketRef, outcome: str, qty: float, ask: float):
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        if not LAYER_ORDERS:
            self.client.place_limit_order(token_id, "BUY", ask, qty, post_only=POST_ONLY_WHEN_POSSIBLE)
            return
        # Split qty across layers around ask and slightly below
        per = qty / LAYER_COUNT
        for i in range(LAYER_COUNT):
            px = max(0.01, ask - i * LAYER_STEP)
            self.client.place_limit_order(token_id, "BUY", px, per, post_only=POST_ONLY_WHEN_POSSIBLE)
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
