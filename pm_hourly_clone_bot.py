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
LOG_CSV    = os.getenv("LOG_CSV",    os.path.join(_LOG_DIR, "bot_log.csv"))
LOG_JSONL  = os.getenv("LOG_JSONL",  os.path.join(_LOG_DIR, "bot_events.jsonl"))
# Markets / coins
CRYPTOS = ["BTC", "ETH", "SOL", "XRP"]
# Polling / evaluation
EVAL_EVERY_SEC = float(os.getenv("EVAL_EVERY_SEC", "6.0"))
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
THR_EARLY_5_15 = 10
THR_MID_15_45  = 15
THR_LATE_45_57 = 8
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
PERSISTENCE_SEC = 20.0          # signal must hold for >= 20s
MIN_DELTA_VEL_BPS_PER_MIN = 1.0 # require some "push" to scale size (not to enter)
# Volatility normalization
Z_WINDOW_SEC = 300.0            # 5 minutes for zscore
Z_ENTRY_MIN = 1.0               # only enter if zscore >= 1.0 (optional gate)
Z_ENTRY_ENABLED = True
# Orderbook imbalance
IMB_ENABLED = True
IMB_LEVELS = 5
IMB_MIN = 1.15                  # bidDepth/askDepth must exceed this for with-drift buys
IMB_MAX_SPREAD = 0.03           # skip if spread too wide (3c)
# Pullback entry
PULLBACK_ENABLED = True
PULLBACK_CENTS = 0.02           # wait for 2c pullback from recent extreme
PULLBACK_LOOKBACK_SEC = 90.0
# Cooldowns
ENTRY_COOLDOWN_SEC = 20.0
REENTRY_COOLDOWN_SEC = 10.0
# Base clip sizing (USDC cost) as % of bankroll
BASE_CLIP_PCT = 0.00020  # 0.02% bankroll per tick
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
# Correlation exposure scaling (reduces correlated stacking)
CORR_SCALE_ENABLED = True
BTC_LEAD = True
BTC_EXPOSURE_REDUCE_OTHERS = 0.50  # up to 50% size reduction if BTC exposure high
# =============================================================================
# UTIL / LOGGING
# =============================================================================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)
def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))
def write_jsonl(event: dict) -> None:
    event["ts"] = iso_z(utc_now())
    line = json.dumps(event, ensure_ascii=False)
    with open(LOG_JSONL, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    # Console output — show key events in a readable format
    etype = event.get("type", "")
    if etype == "SNAPSHOT":
        print(f"  {event.get('crypto',''):4s}  delta={event.get('delta_bps',0):+7.1f}bps  "
              f"vel={event.get('vel_bps_per_min',0):+6.1f}  "
              f"up_ask={event.get('up',{}).get('ask',0):.3f}  "
              f"dn_ask={event.get('down',{}).get('ask',0):.3f}  "
              f"t={event.get('t_min',0):.1f}m")
    elif etype == "ORDER_INTENT":
        print(f"  >>> {event.get('engine','')} {event.get('action','')} {event.get('crypto','')}"
              f" {event.get('outcome','')} qty={event.get('qty',0):.1f}"
              f" px={event.get('price', event.get('ask',0)):.3f}"
              f" clip=${event.get('clip_usdc',0):.2f}")
    elif etype == "PAPER_BUY":
        print(f"  $$$ BUY  {event.get('crypto',''):4s} {event.get('outcome',''):4s}"
              f" qty={event.get('qty',0):.1f} @ ${event.get('price',0):.3f}"
              f"  cost=${event.get('cost',0):.2f}"
              f"  bal=${event.get('balance',0):.2f}")
    elif etype == "PAPER_SELL":
        pnl = event.get('pnl', 0)
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        print(f"  $$$ SELL {event.get('crypto', event.get('slug','')):4s}"
              f" {event.get('outcome',''):4s}"
              f" qty={event.get('qty',0):.1f} @ ${event.get('price',0):.3f}"
              f"  pnl={pnl_str}  reason={event.get('reason','')}"
              f"  bal=${event.get('balance',0):.2f}")
    elif etype == "HOUR_RESOLVED":
        print(f"\n  === HOUR RESOLVED: {event.get('crypto','')} {event.get('slug','')} ===")
        print(f"      Winner: {event.get('winner','')}  |  spot={event.get('spot',0):.2f}  open={event.get('hour_open',0):.2f}")
        for pos_info in event.get("positions", []):
            print(f"      {pos_info['outcome']}: qty={pos_info['qty']:.1f}  payout=${pos_info['payout']:.2f}  pnl={'+' if pos_info['pnl']>=0 else ''}{pos_info['pnl']:.2f}")
        print(f"      Balance: ${event.get('balance',0):.2f}\n")
    elif etype == "LOOP_ERROR":
        print(f"  ERROR: {event.get('err','')}")
    elif etype == "SKIP_NO_PRICE":
        print(f"  SKIP {event.get('crypto','')}: no price data")
    elif etype not in ("SNAPSHOT",):  # all other events (START, NEW_MARKET_STATE, etc.)
        print(f"[{etype}] {json.dumps({k:v for k,v in event.items() if k not in ('type','ts')})}")
def append_csv_row(row: dict) -> None:
    exists = os.path.exists(LOG_CSV)
    with open(LOG_CSV, "a", encoding="utf-8") as f:
        if not exists:
            f.write(",".join(row.keys()) + "\n")
        f.write(",".join(str(row[k]) for k in row.keys()) + "\n")
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
@dataclass
class MarketState:
    slug: str
    crypto: str
    hour_open: float
    hour_start_utc: str
    last_entry_ts: Optional[str] = None
    last_reentry_ts: Optional[str] = None
    peak_abs_delta_bps: float = 0.0
    delta_hist: List[Tuple[str, float]] = None   # (iso, delta_bps)
    price_hist: List[Tuple[str, float]] = None   # (iso, price_up_mid or something)
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
                write_jsonl({"type": "MARKET_DISCOVERY_ERROR", "crypto": crypto,
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
                        spread=1.0, imb=0.0, mid=0.5)
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

        return BookTop(
            bid=best_bid, ask=best_ask,
            bid_sz=bid_sz, ask_sz=ask_sz,
            spread=spread, imb=imb, mid=mid,
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
                write_jsonl({"type": "ORDER_PLACED", "order_id": oid,
                             "token_id": token_id[-12:], "side": side,
                             "price": price, "qty": qty,
                             "status": response.get("status")})
                return oid
        except Exception as e:
            write_jsonl({"type": "ORDER_ERROR", "err": str(e)[:200],
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
            write_jsonl({"type": "CANCEL_ERROR", "order_id": order_id, "err": str(e)[:120]})

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
        self.client = PolymarketClient()
        self.running = True
        self.bankroll_usdc = BANKROLL_START_USDC
        self.day_start = utc_now().date()
        self.daily_pnl_usdc = 0.0  # paper-mode tracked; live mode you can compute from fills
        self.market_states: Dict[str, MarketState] = {}   # slug -> MarketState
        self.signal_hist: Dict[str, List[Tuple[str, bool]]] = {}  # slug -> [(ts, valid_signal)]
        self.last_book: Dict[str, Dict[str, BookTop]] = {}  # slug -> outcome -> BookTop
        self.recent_extreme_price: Dict[str, Dict[str, float]] = {} # slug->outcome->extreme
        self._load_state()
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)
    def _handle_stop(self, *_):
        self.running = False
        write_jsonl({"type": "STOP_SIGNAL"})
    def _load_state(self):
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            self.bankroll_usdc = float(raw.get("bankroll_usdc", self.bankroll_usdc))
            self.daily_pnl_usdc = float(raw.get("daily_pnl_usdc", self.daily_pnl_usdc))
            ms = raw.get("market_states", {})
            for slug, st_dict in ms.items():
                # Reconstruct Position objects from raw dicts
                pos_raw = st_dict.get("positions")
                if isinstance(pos_raw, dict):
                    st_dict["positions"] = {
                        k: Position(**v) if isinstance(v, dict) else v
                        for k, v in pos_raw.items()
                    }
                self.market_states[slug] = MarketState(**st_dict)
            write_jsonl({"type": "STATE_LOADED", "bankroll": self.bankroll_usdc})
        except Exception as e:
            write_jsonl({"type": "STATE_LOAD_ERROR", "err": str(e)})
    def _save_state(self):
        try:
            ms = {slug: asdict(st) for slug, st in self.market_states.items()}
            raw = {
                "bankroll_usdc": self.bankroll_usdc,
                "daily_pnl_usdc": self.daily_pnl_usdc,
                "market_states": ms,
            }
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(raw, f, indent=2)
        except Exception as e:
            write_jsonl({"type": "STATE_SAVE_ERROR", "err": str(e)})
    def _reset_daily_if_needed(self):
        today = utc_now().date()
        if today != self.day_start:
            self.day_start = today
            self.daily_pnl_usdc = 0.0
            write_jsonl({"type": "NEW_DAY_RESET"})
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
        self.bankroll_usdc -= usdc_cost
        write_jsonl({"type": "PAPER_BUY", "slug": st.slug, "crypto": st.crypto,
                     "outcome": outcome, "qty": round(qty, 2), "price": round(price, 3),
                     "cost": round(usdc_cost, 2), "balance": round(self.bankroll_usdc, 2)})
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
        self.bankroll_usdc += proceeds
        self.daily_pnl_usdc += pnl
        return pnl
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
        if self.daily_pnl_usdc < -self.bankroll_usdc * DAILY_STOP_LOSS_PCT:
            return False
        if self._market_cost_usdc(st) > self.bankroll_usdc * MAX_COST_PER_MARKET_PCT:
            return False
        if self._crypto_cost_usdc(st.crypto) > self.bankroll_usdc * MAX_COST_PER_CRYPTO_PCT:
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
        write_jsonl({"type": "START", "mode": MODE, "profile": PROFILE, "bankroll": self.bankroll_usdc})
        self._last_balance_print = 0.0
        while self.running:
            self._reset_daily_if_needed()
            try:
                markets = self._get_markets()
                for m in markets:
                    self._ensure_market_state(m)
                # Check for hour-end resolution on stale markets
                self._resolve_ended_hours(markets)
                # Fetch all market data in parallel (all HTTP calls at once)
                prefetched = []
                if markets:
                    with ThreadPoolExecutor(max_workers=len(markets)) as pool:
                        futures = {pool.submit(self._prefetch_market_data, m): m for m in markets}
                        for fut in as_completed(futures):
                            try:
                                prefetched.append(fut.result())
                            except Exception as e:
                                m = futures[fut]
                                write_jsonl({"type": "PREFETCH_ERROR", "slug": m.slug, "err": str(e)})
                # Process results sequentially (safe for shared state)
                for data in prefetched:
                    self._step_market_with_data(data)
                self._save_state()
                # Print balance summary periodically (every 30s)
                now = time.time()
                if now - self._last_balance_print >= 30.0:
                    self._print_balance_summary()
                    self._last_balance_print = now
            except Exception as e:
                write_jsonl({"type": "LOOP_ERROR", "err": str(e)})
            time.sleep(EVAL_EVERY_SEC)
        write_jsonl({"type": "STOPPED", "bankroll": self.bankroll_usdc, "daily_pnl": self.daily_pnl_usdc})
        self._save_state()
    def _print_balance_summary(self):
        """Print balance and open positions to console."""
        total_cost = 0.0
        pos_parts = []
        for slug, st in self.market_states.items():
            for outcome in ["Up", "Down"]:
                pos = st.positions[outcome]
                if pos.qty > 0:
                    total_cost += pos.cost_usdc
                    pos_parts.append(f"{st.crypto} {outcome}:{pos.qty:.0f}@{pos.vwap:.3f}")
        pos_str = "  ".join(pos_parts) if pos_parts else "none"
        pnl_str = f"+${self.daily_pnl_usdc:.2f}" if self.daily_pnl_usdc >= 0 else f"-${abs(self.daily_pnl_usdc):.2f}"
        print(f"\n  --- Balance: ${self.bankroll_usdc:.2f}  |  Invested: ${total_cost:.2f}"
              f"  |  Day P&L: {pnl_str}  |  Positions: {pos_str} ---\n")
    def _resolve_ended_hours(self, current_markets: List[MarketRef]):
        """
        Resolve positions for hours that have ended.
        Winner pays $1/share, loser pays $0. Then remove old market state.
        """
        current_slugs = {m.slug for m in current_markets}
        ended = [slug for slug in list(self.market_states.keys()) if slug not in current_slugs]
        for slug in ended:
            st = self.market_states[slug]
            has_positions = any(st.positions[o].qty > 0 for o in ["Up", "Down"])
            if not has_positions:
                # No positions — just clean up
                self.market_states.pop(slug, None)
                self.signal_hist.pop(slug, None)
                self.last_book.pop(slug, None)
                self.recent_extreme_price.pop(slug, None)
                continue
            # Determine winner by checking final Binance price vs hour open
            spot, _ = self.client.get_binance_spot_and_hour_open(st.crypto)
            winner = "Up" if spot >= st.hour_open else "Down"
            pos_details = []
            for outcome in ["Up", "Down"]:
                pos = st.positions[outcome]
                if pos.qty <= 0:
                    continue
                # Winner resolves at $1, loser at $0
                payout_price = 1.0 if outcome == winner else 0.0
                payout = payout_price * pos.qty
                pnl = payout - pos.cost_usdc
                self.bankroll_usdc += payout
                self.daily_pnl_usdc += pnl
                pos_details.append({"outcome": outcome, "qty": round(pos.qty, 2),
                                    "payout": round(payout, 2), "pnl": round(pnl, 2)})
                pos.qty = 0.0
                pos.cost_usdc = 0.0
            write_jsonl({"type": "HOUR_RESOLVED", "slug": slug, "crypto": st.crypto,
                         "winner": winner, "spot": round(spot, 2),
                         "hour_open": round(st.hour_open, 2),
                         "positions": pos_details,
                         "balance": round(self.bankroll_usdc, 2)})
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
            self.market_states[m.slug] = MarketState(
                slug=m.slug,
                crypto=m.crypto,
                hour_open=m.hour_open,
                hour_start_utc=iso_z(m.hour_start_utc),
            )
            write_jsonl({"type": "NEW_MARKET_STATE", "slug": m.slug, "crypto": m.crypto})
        # Always ensure companion dicts exist (may be missing after state reload)
        self.signal_hist.setdefault(m.slug, [])
        self.last_book.setdefault(m.slug, {})
        self.recent_extreme_price.setdefault(m.slug, {"Up": None, "Down": None})
    def _step_market_with_data(self, data: dict):
        """Process a market using pre-fetched HTTP data."""
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
            write_jsonl({"type": "SKIP_NO_PRICE", "slug": m.slug, "crypto": m.crypto,
                         "spot": spot, "hour_open": hour_open})
            return
        st.hour_open = hour_open
        delta_bps = (spot - hour_open) / hour_open * 10000.0
        abs_delta_bps = abs(delta_bps)
        st.peak_abs_delta_bps = max(st.peak_abs_delta_bps, abs_delta_bps)
        st.delta_hist.append((iso_z(now), delta_bps))
        st.delta_hist = st.delta_hist[-2000:]
        vel = delta_velocity_bps_per_min(st.delta_hist, lookback_sec=30.0)
        z = zscore(st.delta_hist) if Z_ENTRY_ENABLED else 0.0
        drift_dir = "Up" if delta_bps >= 0 else "Down"
        self.last_book[m.slug]["Up"] = up_book
        self.last_book[m.slug]["Down"] = dn_book
        # update recent extreme prices (for pullbacks)
        self._update_extremes(m.slug, up_book, dn_book)
        # log core snapshot
        write_jsonl({
            "type": "SNAPSHOT",
            "slug": m.slug,
            "crypto": m.crypto,
            "t_min": round(t_min, 3),
            "spot": spot,
            "hour_open": hour_open,
            "delta_bps": round(delta_bps, 3),
            "vel_bps_per_min": round(vel, 3),
            "z": round(z, 3),
            "up": asdict(up_book),
            "down": asdict(dn_book),
            "market_cost": self._market_cost_usdc(st),
            "crypto_cost": self._crypto_cost_usdc(m.crypto),
            "bankroll": self.bankroll_usdc,
            "daily_pnl": self.daily_pnl_usdc,
        })
        # manage exits first (inventory recycling)
        self._manage_exits(m, st, t_min, delta_bps)
        # stop adding risk after minute 57
        if t_min > TRADE_STOP_ADD_MIN:
            return
        # risk gate
        if not self._risk_ok(st):
            return
        # Core engine: drift-direction entries
        self._core_entries(m, st, t_min, delta_bps, abs_delta_bps, vel, z, drift_dir, up_book, dn_book)
        # Late scalp engine: cheap side scalps
        if LATE_SCALP_ENABLED:
            self._late_scalps(m, st, t_min, delta_bps, abs_delta_bps, up_book, dn_book)
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
    def _core_entries(self, m: MarketRef, st: MarketState, t_min: float, delta_bps: float, abs_delta_bps: float,
                     vel_bps_per_min: float, z: float, drift_dir: str,
                     up_book: BookTop, dn_book: BookTop):
        # Build valid signal (time + threshold)
        thr = entry_threshold_bps(t_min)
        cap = price_cap(t_min)
        valid_time = (t_min >= TRADE_START_MIN)
        valid_delta = (abs_delta_bps >= thr)
        # Optional z-score gate
        valid_z = (not Z_ENTRY_ENABLED) or (abs(z) >= Z_ENTRY_MIN)
        # Which outcome to buy
        outcome = drift_dir
        book = up_book if outcome == "Up" else dn_book
        # price cap gate
        valid_price = (book.ask <= cap)
        # orderbook gates
        valid_spread = (book.spread <= IMB_MAX_SPREAD)
        valid_imb = (not IMB_ENABLED) or (book.imb >= IMB_MIN)
        # pullback gate (optional): require small retrace from recent extreme to avoid chasing
        valid_pullback = True
        if PULLBACK_ENABLED:
            extreme = self.recent_extreme_price[m.slug].get(outcome)
            if extreme is not None:
                valid_pullback = (book.mid <= extreme - PULLBACK_CENTS) or (t_min > 45)  # allow chasing late
        # persistence tracking
        sig = bool(valid_time and valid_delta and valid_z and valid_price and valid_spread and valid_imb and valid_pullback)
        sh = self.signal_hist.setdefault(m.slug, [])
        sh.append((iso_z(utc_now()), sig))
        sh[:] = sh[-500:]
        if not persistence_ok(sh):
            return
        # cooldown
        now_iso = iso_z(utc_now())
        if st.last_entry_ts:
            last = datetime.strptime(st.last_entry_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if (utc_now() - last).total_seconds() < ENTRY_COOLDOWN_SEC:
                return
        # sizing
        clip = self._calc_clip(m.crypto, t_min, abs_delta_bps)
        # acceleration scaling (hidden): only scale >1x when velocity confirms
        mult = sizing_mult(abs_delta_bps)
        if mult >= 2.0 and vel_bps_per_min < MIN_DELTA_VEL_BPS_PER_MIN:
            # still allow entry but reduce size if momentum is fading
            clip *= 0.6
        # correlation scaling (if BTC is heavy, reduce others)
        if CORR_SCALE_ENABLED and BTC_LEAD and m.crypto != "BTC":
            btc_cost = self._crypto_cost_usdc("BTC")
            if btc_cost > 0:
                # reduce others proportionally up to 50%
                reduce = clamp(btc_cost / (self.bankroll_usdc * MAX_COST_PER_CRYPTO_PCT), 0.0, 1.0)
                clip *= (1.0 - BTC_EXPOSURE_REDUCE_OTHERS * reduce)
        if clip < MIN_ORDER_USDC:
            return
        # Convert cost clip to contracts at ask price
        qty = clip / max(1e-9, book.ask)
        # Log intent (LOG mode stops right here, per your request)
        intent = {
            "mode": MODE,
            "engine": "CORE",
            "action": "BUY_INTENT",
            "ts": now_iso,
            "crypto": m.crypto,
            "slug": m.slug,
            "t_min": round(t_min, 3),
            "hour_open": st.hour_open,
            "delta_bps": round(delta_bps, 3),
            "abs_delta_bps": round(abs_delta_bps, 3),
            "vel_bps_per_min": round(vel_bps_per_min, 3),
            "z": round(z, 3),
            "outcome": outcome,
            "ask": book.ask,
            "bid": book.bid,
            "spread": book.spread,
            "imb": book.imb,
            "cap": cap,
            "thr": thr,
            "clip_usdc": round(clip, 6),
            "qty": round(qty, 6),
            "layered": LAYER_ORDERS,
        }
        append_csv_row({k: intent[k] for k in intent.keys()})
        write_jsonl({"type": "ORDER_INTENT", **intent})
        # Execute order(s)
        st.last_entry_ts = now_iso
        if MODE == "LOG":
            # paper-fill assumption: fill at ask
            self._paper_buy(st, outcome, book.ask, qty, clip)
            return
        self._place_layered_buy(m, outcome, qty, book.ask)
    def _late_scalps(self, m: MarketRef, st: MarketState, t_min: float, delta_bps: float, abs_delta_bps: float,
                     up_book: BookTop, dn_book: BookTop):
        if not (LATE_SCALP_T_START <= t_min <= LATE_SCALP_T_END):
            return
        if abs_delta_bps < LATE_SCALP_ABSDELTA_MIN or abs_delta_bps > LATE_SCALP_ABSDELTA_MAX:
            return
        # choose "cheap side" (underdog) late
        # cheap side = the outcome with lower mid/ask
        up_price = up_book.ask
        dn_price = dn_book.ask
        if min(up_price, dn_price) > LATE_SCALP_PRICE_MAX:
            return
        outcome = "Up" if up_price < dn_price else "Down"
        book = up_book if outcome == "Up" else dn_book
        # liquidity gates
        if book.spread > IMB_MAX_SPREAD:
            return
        # cooldown per market
        now_iso = iso_z(utc_now())
        if st.last_reentry_ts:
            last = datetime.strptime(st.last_reentry_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if (utc_now() - last).total_seconds() < REENTRY_COOLDOWN_SEC:
                return
        # small size only
        clip = self._calc_clip(m.crypto, t_min, abs_delta_bps) * 0.50
        if clip < MIN_ORDER_USDC:
            return
        qty = clip / max(1e-9, book.ask)
        intent = {
            "mode": MODE,
            "engine": "LATE_SCALP",
            "action": "BUY_INTENT",
            "ts": now_iso,
            "crypto": m.crypto,
            "slug": m.slug,
            "t_min": round(t_min, 3),
            "delta_bps": round(delta_bps, 3),
            "abs_delta_bps": round(abs_delta_bps, 3),
            "outcome": outcome,
            "ask": book.ask,
            "bid": book.bid,
            "spread": book.spread,
            "clip_usdc": round(clip, 6),
            "qty": round(qty, 6),
            "target_sell": round(min(0.999, book.ask + LATE_SCALP_TP_CENTS), 3),
        }
        append_csv_row(intent)
        write_jsonl({"type": "ORDER_INTENT", **intent})
        st.last_reentry_ts = now_iso
        # Execute
        if MODE == "LOG":
            self._paper_buy(st, outcome, book.ask, qty, clip)
            pos = st.positions[outcome]
            pos.scalp_mode = True
            pos.scalp_open_ts = now_iso
            return
        # Live: place layered or single
        self._place_layered_buy(m, outcome, qty, book.ask)
        pos = st.positions[outcome]
        pos.scalp_mode = True
        pos.scalp_open_ts = now_iso
    def _manage_exits(self, m: MarketRef, st: MarketState, t_min: float, delta_bps: float):
        # For each outcome, apply ladder and de-risk rules
        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty <= 0:
                continue
            book = self.last_book[m.slug].get(outcome)
            if not book:
                continue
            # Scalp timeout
            if pos.scalp_mode and pos.scalp_open_ts:
                opened = datetime.strptime(pos.scalp_open_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                if (utc_now() - opened).total_seconds() / 60.0 > LATE_SCALP_MAX_HOLD_MIN:
                    # force exit
                    self._do_sell(m, st, outcome, pos.qty, book.bid, reason="SCALP_TIMEOUT")
                    pos.scalp_mode = False
                    continue
            # Hard stop near end
            if t_min >= TRADE_HARD_STOP_MIN:
                self._do_sell(m, st, outcome, pos.qty, max(book.bid, book.ask - MAX_CROSS_SLIPPAGE), reason="HARD_STOP")
                continue
            # De-risk on drift reversal
            if outcome == "Up" and delta_bps < +DERISK_CROSS_BPS:
                qty = pos.qty * DERISK_SELL_FRAC_PER_TICK
                self._do_sell(m, st, outcome, qty, book.bid, reason="DERISK_REVERSAL")
                continue
            if outcome == "Down" and delta_bps > -DERISK_CROSS_BPS:
                qty = pos.qty * DERISK_SELL_FRAC_PER_TICK
                self._do_sell(m, st, outcome, qty, book.bid, reason="DERISK_REVERSAL")
                continue
            # Profit ladder based on VWAP
            if (not pos.tp1_done) and book.bid >= pos.vwap + TP1:
                self._do_sell(m, st, outcome, pos.qty * TP1_SELL_FRAC, book.bid, reason="TP1")
                pos.tp1_done = True
            if (not pos.tp2_done) and book.bid >= pos.vwap + TP2:
                self._do_sell(m, st, outcome, pos.qty * TP2_SELL_FRAC, book.bid, reason="TP2")
                pos.tp2_done = True
            if (not pos.tp3_done) and book.bid >= pos.vwap + TP3:
                self._do_sell(m, st, outcome, pos.qty * TP3_SELL_FRAC, book.bid, reason="TP3")
                pos.tp3_done = True
            # For scalp mode, exit at target faster
            if pos.scalp_mode:
                target = min(0.999, pos.vwap + LATE_SCALP_TP_CENTS)
                if book.bid >= target:
                    self._do_sell(m, st, outcome, pos.qty, book.bid, reason="SCALP_TP")
                    pos.scalp_mode = False
    def _do_sell(self, m: MarketRef, st: MarketState, outcome: str, qty: float, price: float, reason: str):
        qty = max(0.0, qty)
        if qty <= 0:
            return
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        intent = {
            "mode": MODE,
            "engine": "EXIT",
            "action": "SELL_INTENT",
            "ts": iso_z(utc_now()),
            "crypto": m.crypto,
            "slug": m.slug,
            "t_min": round(minutes_into_hour(datetime.strptime(st.hour_start_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc), utc_now()), 3),
            "outcome": outcome,
            "qty": round(qty, 6),
            "price": round(price, 3),
            "reason": reason,
            "vwap": round(st.positions[outcome].vwap, 6),
        }
        append_csv_row(intent)
        write_jsonl({"type": "ORDER_INTENT", **intent})
        if MODE == "LOG":
            pnl = self._paper_sell(st, outcome, price, qty)
            write_jsonl({"type": "PAPER_SELL", "slug": m.slug, "outcome": outcome,
                         "qty": round(qty, 2), "price": round(price, 3),
                         "pnl": round(pnl, 2), "balance": round(self.bankroll_usdc, 2),
                         "reason": reason})
            return
        # live sell
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
        base = self.bankroll_usdc * BASE_CLIP_PCT
        mult = sizing_mult(abs_delta_bps)
        if t_min < 10:
            mult *= EARLY_SIZE_MULT
        clip = base * mult
        return max(0.0, clip)
    def _cleanup_market(self, m: MarketRef, st: MarketState, t_min: float):
        # Close everything near end
        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty <= 0:
                continue
            book = self.last_book.get(m.slug, {}).get(outcome)
            if book:
                self._do_sell(m, st, outcome, pos.qty, max(book.bid, book.ask - MAX_CROSS_SLIPPAGE), reason="CLEANUP")
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
