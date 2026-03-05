#!/usr/bin/env python3
"""
F247 Copy-Wallet Trade Tracker (Start-From-Now + MAX FORENSICS v2)
===================================================================
Standalone script: watches the F247 wallet on Polymarket, logs every
new trade with full orderbook + Binance enrichment.

Logs to the SAME directory as the main poly_bot (../logs/poly_bot/)
so all outputs sit beside bot_log / bot_events files.

Key behaviors
  - Start-from-now: ignores any trades with timestamp < program start
  - Robust pagination: walks offset pages, stops on already-seen timestamps,
    hard-caps offsets to avoid API 400s
  - Dedup: in-memory set + SQLite primary key (txHash)
  - Enrichment: Binance spot (cached), 1h open, 1h close backfill
  - CLOB orderbook snapshot: bid/ask, spread, mid, microprice, imbalance,
    depth + top ladders JSON
  - Markouts: +2 / +10 / +30 / +60 seconds (mid/spread + optional spot)
  - Rollups: per-minute (buys/sells/usdc/spread/crossing%), 60s fingerprint

MAX FORENSICS v2 additions (42 new columns per trade):
  A) Velocity features: spread/imbalance/mid/binance deltas from 1Hz tapes
  B) Execution style: aggressiveness classification + edge metrics
  C) Roundtrip PnL: FIFO lot matching with holding time
  D) Trade clustering: burstiness signals (counts, USDC in last 10s/60s)
  E) Market selection: cross-market spread/imbalance ranks
  F) Latency: API-to-local delay + book/binance data freshness

Outputs (all inside LOG_DIR):
  f247_copywallet_trades.db              SQLite source of truth
  f247_copywallet_fills_raw.csv          append-only raw fills (96 columns)
  f247_copywallet_fills_enriched.csv     periodic full snapshot from DB
  f247_copywallet_markouts.csv           append-only markout registry
  f247_copywallet_markout_stats.csv      periodic markout stats
  f247_copywallet_minute_rollup.csv      periodic snapshot
  f247_copywallet_fingerprint.csv        append-only
  f247_copywallet_inventory_snapshot.csv periodic inventory state
  f247_copywallet_book_tape.csv          1Hz orderbook tape (append-only)
  f247_copywallet_binance_tape.csv       1Hz Binance spot tape (append-only)
  f247_copywallet_realized_pnl_summary.csv  periodic FIFO PnL summary
  f247_copywallet_events.jsonl           append-only event log

Run:
  python f247_copywallet_tracker.py
"""
import os
import re
import csv
import time
import json
import math
import sqlite3
import signal
import hashlib
import statistics
import requests
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Tuple, List, Any

# ======================== LOG DIRECTORY ========================
# Match the main bot: go up one level from this file's dir, then logs/poly_bot
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(os.path.dirname(_SCRIPT_DIR), "logs", "poly_bot")
os.makedirs(LOG_DIR, exist_ok=True)

# ======================== CONFIG ========================
WALLET = "0xf247584e41117bbBe4Cc06E4d2C95741792a5216"

# All output files live in LOG_DIR
DB_PATH = os.path.join(LOG_DIR, "f247_copywallet_trades.db")
RAW_CSV = os.path.join(LOG_DIR, "f247_copywallet_fills_raw.csv")
OUT_CSV = os.path.join(LOG_DIR, "f247_copywallet_fills_enriched.csv")
MARKOUTS_CSV = os.path.join(LOG_DIR, "f247_copywallet_markouts.csv")
MINUTE_ROLLUP_CSV = os.path.join(LOG_DIR, "f247_copywallet_minute_rollup.csv")
FINGERPRINT_CSV = os.path.join(LOG_DIR, "f247_copywallet_fingerprint.csv")
INVENTORY_CSV = os.path.join(LOG_DIR, "f247_copywallet_inventory_snapshot.csv")
MARKOUT_STATS_CSV = os.path.join(LOG_DIR, "f247_copywallet_markout_stats.csv")
ENTRY_MODEL_CSV = os.path.join(LOG_DIR, "f247_copywallet_entry_model.csv")
BOOK_TAPE_CSV = os.path.join(LOG_DIR, "f247_copywallet_book_tape.csv")
BINANCE_TAPE_CSV = os.path.join(LOG_DIR, "f247_copywallet_binance_tape.csv")
REALIZED_PNL_SUMMARY_CSV = os.path.join(LOG_DIR, "f247_copywallet_realized_pnl_summary.csv")
JSONL_LOG = os.path.join(LOG_DIR, "f247_copywallet_events.jsonl")

POLL_SECONDS = 1.0

# Activity API pagination
LIMIT = 200
MAX_PAGES_PER_POLL = 12
MAX_OFFSET = 2400

# Write cadences
EXPORT_EVERY_SECONDS = 120
BACKFILL_EVERY_SECONDS = 30
ROLLUP_EVERY_SECONDS = 60
FINGERPRINT_EVERY_SECONDS = 60
INVENTORY_EVERY_SECONDS = 60
MARKOUT_STATS_EVERY_SECONDS = 60
BEHAVIOR_SUMMARY_EVERY_SECONDS = 60

# Spread percentile
SPREAD_WINDOW_SEC = 60

# Markout stats rolling window
MARKOUT_STATS_WINDOW = 200

# Polymarket fee schedule (public)
TAKER_FEE_BPS = 2.0
MAKER_FEE_BPS = 0.0

# Book tape: continuous 1Hz orderbook recording
BOOK_TAPE_ENABLED = True
BOOK_TAPE_MAX_TOKENS = 10
BOOK_TAPE_MEMORY_SEC = 120  # In-memory tape window for velocity lookups

# Binance tape: 1Hz spot recording for velocity
BINANCE_TAPE_MEMORY_SEC = 120  # In-memory tape window

# Write cadences for new v2 features
LATENCY_ROLLUP_EVERY_SECONDS = 60
REALIZED_PNL_SUMMARY_EVERY_SECONDS = 120
MARKET_RANK_EVERY_SECONDS = 10

# Binance
BINANCE_SPOT_TTL_SEC = 1.0
BINANCE_CLOSE_READY_BUFFER_MS = 5_000

# CLOB orderbook snapshot
ORDERBOOK_ENABLED = True
ORDERBOOK_TTL_SEC = 0.35
ORDERBOOK_DEPTH_LEVELS = 10
ORDERBOOK_TOTAL_DEPTH_LEVELS = 25

# Markouts
MARKOUT_HORIZONS_SEC = [2, 10, 30, 60]
MARKOUT_MAX_PER_TICK = 10
MARKOUT_MAX_AGE_SEC = 300  # Abandon unresolved markouts after 5 minutes

# Backoff
HTTP_BACKOFF_SEC = 6

# =======================================================
POLYMARKET_ACTIVITY = "https://data-api.polymarket.com/activity"
CLOB_BOOK = "https://clob.polymarket.com/book"
CLOB_PRICE = "https://clob.polymarket.com/price"
CLOB_LIVE_ACTIVITY = "https://clob.polymarket.com/live-activity/events/"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
BINANCE_BASE = "https://data-api.binance.vision"
BINANCE_SPOT = f"{BINANCE_BASE}/api/v3/ticker/price"
BINANCE_KLINES = f"{BINANCE_BASE}/api/v3/klines"

CRYPTO_KEYWORDS = {
    "bitcoin": "BTC", "btc": "BTC",
    "ethereum": "ETH", "eth": "ETH",
    "solana": "SOL", "sol": "SOL",
    "xrp": "XRP",
}

STOP = False

# Callback for bridging fills to the bot's main logger.
# Set by wallet_tracker.py before starting the main loop.
# Signature: ON_FILL_CB(trade_dict) where trade_dict has:
#   tx_hash, ts_epoch, ts_iso, slug, asset, outcome, side, price,
#   qty_shares, notional_usd, token_id
ON_FILL_CB = None


# -------------------- signals --------------------
def _handle_stop(signum, frame):
    global STOP
    STOP = True
    print("\n[stop] Finishing current loop...")


# -------------------- JSONL event log --------------------
_jsonl_fh = None


def _open_jsonl():
    global _jsonl_fh
    if _jsonl_fh is None:
        _jsonl_fh = open(JSONL_LOG, "a", encoding="utf-8")


def write_jsonl(event: dict):
    """Append a JSON event line to the JSONL log."""
    _open_jsonl()
    event.setdefault("ts_ms", int(time.time() * 1000))
    _jsonl_fh.write(json.dumps(event, separators=(",", ":")) + "\n")
    _jsonl_fh.flush()


# -------------------- helpers --------------------
def ts_to_iso(ts: int) -> str:
    if ts > 1_000_000_000_000:
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    else:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def now_ms() -> int:
    return int(time.time() * 1000)


def epoch_sec() -> int:
    return int(time.time())


def ts_to_hour_ms(ts: int) -> int:
    ms = ts * 1000 if ts < 1_000_000_000_000 else ts
    return (ms // 3_600_000) * 3_600_000


def clean_number(x) -> str:
    try:
        f = float(x)
        if not math.isfinite(f):
            return ""
        if f.is_integer():
            return str(int(f))
        return f"{f:.10f}".rstrip("0").rstrip(".")
    except Exception:
        return ""


def ffloat(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def detect_crypto(slug: str, title: str = "") -> str:
    text = f"{slug} {title}".lower()
    for kw in sorted(CRYPTO_KEYWORDS, key=len, reverse=True):
        if re.search(rf'(?:^|[^a-z]){re.escape(kw)}(?:$|[^a-z])', text):
            return CRYPTO_KEYWORDS[kw]
    return ""


def safe_get(d: dict, keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def sha1_short(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:10]


def minute_bucket(ts_epoch: int) -> int:
    return (ts_epoch // 60) * 60


# -------------------- HTTP session --------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "pm-copywallet-tracker/1.0"})
    return s


SESSION = make_session()


# -------------------- Token ID resolver cache --------------------
_token_cache: Dict[Tuple[str, str], str] = {}   # (slug, outcome) -> token_id

# Outcome normalization: Activity API says "Up"/"Down", Gamma may say "Yes"/"No"
_OUTCOME_ALIASES: Dict[str, List[str]] = {
    "Up": ["Up", "Yes"],
    "Down": ["Down", "No"],
    "Yes": ["Yes", "Up"],
    "No": ["No", "Down"],
}

# -------------------- Binance caching --------------------
_spot_cache: Dict[str, Tuple[float, str]] = {}
_hour_open_cache: Dict[Tuple[str, int], str] = {}
_hour_close_cache: Dict[Tuple[str, int], str] = {}


def binance_spot_price(ticker: str) -> str:
    now = time.monotonic()
    cached = _spot_cache.get(ticker)
    if cached and (now - cached[0]) <= BINANCE_SPOT_TTL_SEC:
        return cached[1]
    try:
        r = SESSION.get(BINANCE_SPOT, params={"symbol": f"{ticker}USDT"}, timeout=5)
        r.raise_for_status()
        price = r.json().get("price", "")
        _spot_cache[ticker] = (now, str(price))
        return str(price)
    except Exception:
        return ""


def binance_hour_open(ticker: str, hour_ms: int) -> str:
    key = (ticker, hour_ms)
    if key in _hour_open_cache:
        return _hour_open_cache[key]
    try:
        r = SESSION.get(BINANCE_KLINES, params={
            "symbol": f"{ticker}USDT",
            "interval": "1h",
            "startTime": hour_ms,
            "limit": 1,
        }, timeout=5)
        r.raise_for_status()
        klines = r.json()
        if klines:
            open_price = str(klines[0][1])
            _hour_open_cache[key] = open_price
            return open_price
    except Exception:
        return ""
    return ""


def binance_hour_close_if_final(ticker: str, hour_ms: int) -> Optional[str]:
    key = (ticker, hour_ms)
    if key in _hour_close_cache:
        return _hour_close_cache[key]
    try:
        r = SESSION.get(BINANCE_KLINES, params={
            "symbol": f"{ticker}USDT",
            "interval": "1h",
            "startTime": hour_ms,
            "limit": 1,
        }, timeout=5)
        r.raise_for_status()
        klines = r.json()
        if not klines:
            return None
        k = klines[0]
        close_time_ms = int(k[6])
        if now_ms() >= close_time_ms + BINANCE_CLOSE_READY_BUFFER_MS:
            close_price = str(k[4])
            _hour_close_cache[key] = close_price
            return close_price
        return None
    except Exception:
        return None


# -------------------- CLOB orderbook snapshot --------------------
@dataclass
class BookSnap:
    best_bid: str = ""
    best_ask: str = ""
    spread: str = ""
    mid: str = ""
    microprice: str = ""
    imbalance_topN: str = ""
    bid_depth_topN: str = ""
    ask_depth_topN: str = ""
    bid_depth_total: str = ""
    ask_depth_total: str = ""
    bids_json: str = ""
    asks_json: str = ""
    book_ts: str = ""
    book_hash: str = ""
    suspect: str = ""          # "SUSPECT" if spread > 0.20 or extreme bid/ask


def _is_book_suspect(bb: Optional[float], ba: Optional[float], spread_f: Optional[float]) -> bool:
    """Detect pathological books: spread > 0.20 or bid<=0.02 and ask>=0.98."""
    if spread_f is not None and spread_f > 0.20:
        return True
    if bb is not None and ba is not None and bb <= 0.02 and ba >= 0.98:
        return True
    return False


_book_cache: Dict[str, Tuple[float, BookSnap]] = {}


def _levels_depth(levels: List[dict], n: int) -> float:
    total = 0.0
    for lvl in levels[:n]:
        sz = ffloat(lvl.get("size"))
        if sz is not None:
            total += sz
    return total


def _microprice(best_bid: float, best_ask: float, bid_sz: float, ask_sz: float) -> float:
    denom = bid_sz + ask_sz
    if denom <= 0:
        return (best_bid + best_ask) / 2.0
    return (best_ask * bid_sz + best_bid * ask_sz) / denom


def fetch_orderbook(token_id: str) -> Optional[BookSnap]:
    if not ORDERBOOK_ENABLED or not token_id:
        return None
    now = time.monotonic()
    cached = _book_cache.get(token_id)
    if cached and (now - cached[0]) <= ORDERBOOK_TTL_SEC:
        return cached[1]
    try:
        r = SESSION.get(CLOB_BOOK, params={"token_id": token_id}, timeout=5)
        if r.status_code != 200:
            write_jsonl({"event_type": "BOOK_FETCH_FAIL",
                         "token_id": token_id, "status": r.status_code})
            return None
        data = r.json()
        if not isinstance(data, dict):
            write_jsonl({"event_type": "BOOK_FETCH_BAD_RESPONSE",
                         "token_id": token_id, "type": str(type(data).__name__)})
            return None
        raw_bids = data.get("bids") or []
        raw_asks = data.get("asks") or []
        # CRITICAL: CLOB API returns unsorted levels — sort properly.
        # Bids: highest price first (DESC). Asks: lowest price first (ASC).
        bids = sorted(raw_bids, key=lambda l: float(l.get("price", 0)), reverse=True)
        asks = sorted(raw_asks, key=lambda l: float(l.get("price", 0)))
        best_bid = str(bids[0]["price"]) if bids else ""
        best_ask = str(asks[0]["price"]) if asks else ""
        bb = ffloat(best_bid)
        ba = ffloat(best_ask)
        spread = ""
        mid = ""
        micro = ""
        bid_sz1 = ffloat(bids[0].get("size")) if bids else 0.0
        ask_sz1 = ffloat(asks[0].get("size")) if asks else 0.0
        if bid_sz1 is None:
            bid_sz1 = 0.0
        if ask_sz1 is None:
            ask_sz1 = 0.0
        if bb is not None and ba is not None:
            spread = clean_number(ba - bb)
            mid_val = (ba + bb) / 2.0
            mid = clean_number(mid_val)
            micro_val = _microprice(bb, ba, bid_sz1, ask_sz1)
            micro = clean_number(micro_val)
        # Suspect detection
        spread_f = ffloat(spread)
        suspect = "SUSPECT" if _is_book_suspect(bb, ba, spread_f) else ""
        if suspect:
            write_jsonl({"event_type": "BOOK_SUSPECT", "token_id": token_id,
                         "best_bid": best_bid, "best_ask": best_ask,
                         "spread": spread,
                         "bids_top3": json.dumps(bids[:3], separators=(",", ":")),
                         "asks_top3": json.dumps(asks[:3], separators=(",", ":"))})

        bid_depth_top = _levels_depth(bids, ORDERBOOK_DEPTH_LEVELS)
        ask_depth_top = _levels_depth(asks, ORDERBOOK_DEPTH_LEVELS)
        bid_depth_tot = _levels_depth(bids, ORDERBOOK_TOTAL_DEPTH_LEVELS)
        ask_depth_tot = _levels_depth(asks, ORDERBOOK_TOTAL_DEPTH_LEVELS)
        imb = ""
        if bid_depth_top + ask_depth_top > 0:
            imb = clean_number(bid_depth_top / (bid_depth_top + ask_depth_top))
        snap = BookSnap(
            best_bid=best_bid,
            best_ask=best_ask,
            spread=spread,
            mid=mid,
            microprice=micro,
            imbalance_topN=imb,
            bid_depth_topN=clean_number(bid_depth_top),
            ask_depth_topN=clean_number(ask_depth_top),
            bid_depth_total=clean_number(bid_depth_tot),
            ask_depth_total=clean_number(ask_depth_tot),
            bids_json=json.dumps(bids[:ORDERBOOK_DEPTH_LEVELS], separators=(",", ":")),
            asks_json=json.dumps(asks[:ORDERBOOK_DEPTH_LEVELS], separators=(",", ":")),
            book_ts=str(data.get("timestamp", "")),
            book_hash=str(data.get("hash", "")),
            suspect=suspect,
        )
        _book_cache[token_id] = (now, snap)
        return snap
    except Exception:
        return None


def fetch_clob_midprice(token_id: str) -> Optional[Tuple[str, str]]:
    """Fallback: use CLOB /price endpoint when /book is suspect.
    Returns (buy_price, sell_price) or None.
    The /price endpoint reportedly returns correct live data even when
    /book serves stale snapshots (Polymarket known issue #180).
    """
    if not token_id:
        return None
    try:
        buy_r = SESSION.get(CLOB_PRICE, params={
            "token_id": token_id, "side": "BUY"}, timeout=5)
        sell_r = SESSION.get(CLOB_PRICE, params={
            "token_id": token_id, "side": "SELL"}, timeout=5)
        if buy_r.status_code == 200 and sell_r.status_code == 200:
            buy_px = str(buy_r.json().get("price", ""))
            sell_px = str(sell_r.json().get("price", ""))
            return (buy_px, sell_px)
    except Exception:
        pass
    return None


# -------------------- Execution enrichment (taker/maker + fee) --------------------
def fetch_clob_trade_info(condition_id: str, tx_hash: str = "",
                          match_id: str = "") -> Optional[dict]:
    """Query CLOB live-activity endpoint for trade details.

    Returns dict with taker_maker, fee, fee_rate_bps, or None on failure.
    """
    if not condition_id:
        return None
    try:
        url = f"{CLOB_LIVE_ACTIVITY}{condition_id}"
        r = SESSION.get(url, params={"limit": 50}, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, list):
            return None
        for event in data:
            event_tx = (event.get("transactionHash", "")
                        or event.get("txHash", ""))
            event_match = (event.get("matchId", "")
                           or event.get("match_id", ""))
            if ((tx_hash and event_tx == tx_hash)
                    or (match_id and event_match == match_id)):
                tm_raw = (event.get("matchType", "")
                          or event.get("type", "")
                          or event.get("tradeType", ""))
                fee_raw = (event.get("fee", "")
                           or event.get("feeAmount", ""))
                fbps_raw = (event.get("feeRateBps", "")
                            or event.get("fee_rate_bps", ""))
                return {
                    "taker_maker": str(tm_raw).upper() if tm_raw else "",
                    "fee": str(fee_raw) if fee_raw else "",
                    "fee_rate_bps": str(fbps_raw) if fbps_raw else "",
                }
        return None
    except Exception:
        return None


def infer_execution_fields(side: str, price_f: float,
                           bb_f: float, ba_f: float,
                           usdc_f: float) -> dict:
    """Infer taker/maker + fee from orderbook position.

    Polymarket fee schedule: taker ~2 bps, maker 0 bps.
    """
    if side == "BUY":
        is_taker = price_f >= ba_f - 1e-6
    elif side == "SELL":
        is_taker = price_f <= bb_f + 1e-6
    else:
        return {"taker_maker": "", "fee": "", "fee_rate_bps": ""}
    tm = "TAKER" if is_taker else "MAKER"
    fee_bps = TAKER_FEE_BPS if is_taker else MAKER_FEE_BPS
    fee_usdc = usdc_f * fee_bps / 10000.0 if usdc_f else 0.0
    return {
        "taker_maker": tm,
        "fee": clean_number(fee_usdc),
        "fee_rate_bps": clean_number(fee_bps),
    }


# -------------------- Book tape: continuous 1Hz recording --------------------
_active_tokens: Dict[str, dict] = {}   # token_id -> {slug, outcome, last_trade_ts}

_BOOK_TAPE_HEADER = [
    "timestamp_iso", "timestamp_epoch", "token_id", "slug", "outcome",
    "bestBid", "bestAsk", "spread", "mid", "microprice",
    "imbalance_topN", "bid_depth_topN", "ask_depth_topN",
    "spread_percentile_60s",
    # v2 additions
    "bid_depth_total", "ask_depth_total",
    "tape_ts_epoch", "tape_source",
]


def register_active_token(token_id: str, slug: str, outcome: str, ts: int):
    """Register a token_id for book tape recording."""
    if not token_id:
        return
    _active_tokens[token_id] = {
        "slug": slug, "outcome": outcome, "last_trade_ts": ts}
    if len(_active_tokens) > BOOK_TAPE_MAX_TOKENS:
        oldest = min(_active_tokens,
                     key=lambda k: _active_tokens[k]["last_trade_ts"])
        del _active_tokens[oldest]


def write_book_tape_tick():
    """Record one book tape row per active token_id (called every ~1s).
    Also stores in-memory for velocity feature lookups.
    """
    if not BOOK_TAPE_ENABLED or not _active_tokens:
        return
    now_epoch = epoch_sec()
    now_iso = ts_to_iso(now_epoch)
    for token_id, info in list(_active_tokens.items()):
        snap = fetch_orderbook(token_id)
        if snap is None or snap.suspect:
            continue
        spread_f = ffloat(snap.spread)
        imb_f = ffloat(snap.imbalance_topN)
        mid_f = ffloat(snap.mid)
        micro_f = ffloat(snap.microprice)
        sp_stats: Dict[str, str] = {}
        if spread_f is not None:
            spread_tracker_record(token_id, now_epoch, spread_f)
            sp_stats = spread_tracker_stats(token_id, spread_f)
        # Store in-memory for velocity lookups
        book_tape_mem_record(token_id, now_epoch, spread_f, imb_f, mid_f, micro_f)
        append_row_csv(BOOK_TAPE_CSV, _BOOK_TAPE_HEADER, {
            "timestamp_iso": now_iso,
            "timestamp_epoch": str(now_epoch),
            "token_id": token_id,
            "slug": info["slug"],
            "outcome": info["outcome"],
            "bestBid": snap.best_bid,
            "bestAsk": snap.best_ask,
            "spread": snap.spread,
            "mid": snap.mid,
            "microprice": snap.microprice,
            "imbalance_topN": snap.imbalance_topN,
            "bid_depth_topN": snap.bid_depth_topN,
            "ask_depth_topN": snap.ask_depth_topN,
            "spread_percentile_60s": sp_stats.get(
                "spread_percentile_60s", ""),
            "bid_depth_total": snap.bid_depth_total,
            "ask_depth_total": snap.ask_depth_total,
            "tape_ts_epoch": str(now_epoch),
            "tape_source": "CLOB_BOOK",
        })


# -------------------- Token ID resolver --------------------
def _normalize_outcome_label(label: str) -> str:
    """Normalize Gamma outcome labels: Yes->Up, No->Down, passthrough others."""
    u = label.strip().upper()
    if "UP" in u or "YES" in u:
        return "Up"
    if "DOWN" in u or "NO" in u:
        return "Down"
    return label


def _fetch_gamma_slug_tokens(slug: str) -> Dict[str, str]:
    """Fetch event by slug from Gamma API, return {normalized_outcome: clob_token_id}.

    Tries the path-parameter endpoint first, then query-parameter fallback.
    Normalizes outcome labels (Yes->Up, No->Down) so activity API outcomes match.
    """
    result: Dict[str, str] = {}
    event = None

    # Try path-parameter endpoint (what the main bot uses)
    url = f"{GAMMA_API_BASE}/events/slug/{slug}"
    try:
        r = SESSION.get(url, timeout=10)
        if r.status_code == 200:
            event = r.json()
    except Exception:
        pass

    # Fallback: query-parameter endpoint
    if event is None:
        try:
            r = SESSION.get(f"{GAMMA_API_BASE}/events", params={"slug": slug}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and data:
                    event = data[0]
        except Exception:
            pass

    if event is None:
        write_jsonl({"event_type": "GAMMA_FETCH_FAILED", "slug": slug})
        return {}

    market_list = event.get("markets", [])
    if not isinstance(market_list, list) or not market_list:
        market_list = [event]

    # Iterate all markets in the event (usually just 1 for hourly crypto)
    for market in market_list:
        outcomes_raw = market.get("outcomes")
        tokens_raw = market.get("clobTokenIds")
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
        tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else (tokens_raw or [])

        for i, out in enumerate(outcomes):
            if i >= len(tokens):
                continue
            raw_label = str(out)
            tid = str(tokens[i])
            # Store under the normalized label (Up/Down)
            norm = _normalize_outcome_label(raw_label)
            result[norm] = tid
            # Also store under the raw label if different
            if raw_label != norm:
                result[raw_label] = tid

    write_jsonl({"event_type": "GAMMA_FETCH_OK", "slug": slug,
                 "outcomes": list(result.keys()),
                 "market_count": len(market_list)})
    return result


def _cache_lookup_with_aliases(slug: str, outcome: str) -> Optional[str]:
    """Look up token in memory cache, trying outcome aliases (Up↔Yes, Down↔No)."""
    direct = _token_cache.get((slug, outcome))
    if direct:
        return direct
    for alias in _OUTCOME_ALIASES.get(outcome, []):
        hit = _token_cache.get((slug, alias))
        if hit:
            # Populate the primary key too so future lookups are instant
            _token_cache[(slug, outcome)] = hit
            return hit
    return None


def resolve_token_id(conn: sqlite3.Connection, slug: str, outcome: str) -> Optional[str]:
    """Resolve token_id for slug+outcome.  3-tier: memory cache → SQLite → Gamma API.
    Tries outcome aliases (Up↔Yes, Down↔No) at each tier.
    """
    if not slug or not outcome:
        return None

    # --- tier 1: in-memory cache (with aliases) ---
    cached = _cache_lookup_with_aliases(slug, outcome)
    if cached:
        write_jsonl({"event_type": "TOKEN_RESOLUTION", "slug": slug,
                     "outcome": outcome, "resolved_token_id": cached,
                     "source": "cache", "cache_hit": True})
        return cached

    # --- tier 2: SQLite lookup (try outcome + aliases) ---
    aliases = [outcome] + [a for a in _OUTCOME_ALIASES.get(outcome, []) if a != outcome]
    for alias in aliases:
        cur = conn.execute(
            "SELECT token_id FROM slug_tokens WHERE slug = ? AND outcome = ?",
            (slug, alias),
        )
        row = cur.fetchone()
        if row:
            tid = row[0]
            _token_cache[(slug, outcome)] = tid
            _token_cache[(slug, alias)] = tid
            write_jsonl({"event_type": "TOKEN_RESOLUTION", "slug": slug,
                         "outcome": outcome, "resolved_token_id": tid,
                         "source": "sqlite", "cache_hit": False,
                         "matched_alias": alias})
            return tid

    # --- tier 3: Gamma API ---
    write_jsonl({"event_type": "TOKEN_RESOLUTION", "slug": slug,
                 "outcome": outcome, "source": "gamma_api",
                 "cache_hit": False, "status": "fetching"})
    outcome_map = _fetch_gamma_slug_tokens(slug)
    if not outcome_map:
        write_jsonl({"event_type": "TOKEN_RESOLUTION_FAILED", "slug": slug,
                     "outcome": outcome, "reason": "gamma_api_empty"})
        return None

    # Persist ALL outcomes for this slug
    now_ts = epoch_sec()
    for out_label, tid in outcome_map.items():
        try:
            conn.execute(
                "INSERT OR REPLACE INTO slug_tokens (slug, outcome, token_id, updated_epoch) "
                "VALUES (?, ?, ?, ?)",
                (slug, out_label, tid, now_ts),
            )
        except Exception:
            pass
        _token_cache[(slug, out_label)] = tid
    conn.commit()

    # Now try cache again (with aliases)
    resolved = _cache_lookup_with_aliases(slug, outcome)
    if resolved:
        write_jsonl({"event_type": "TOKEN_RESOLUTION", "slug": slug,
                     "outcome": outcome, "resolved_token_id": resolved,
                     "source": "gamma_api", "cache_hit": False})
    else:
        write_jsonl({"event_type": "TOKEN_RESOLUTION_FAILED", "slug": slug,
                     "outcome": outcome, "reason": "outcome_not_in_gamma",
                     "gamma_outcomes": list(outcome_map.keys())})
    return resolved


def preload_token_cache(conn: sqlite3.Connection):
    """Load all slug_tokens rows into _token_cache at startup and log stats."""
    cur = conn.execute("SELECT slug, outcome, token_id FROM slug_tokens")
    rows = cur.fetchall()
    for slug, outcome, tid in rows:
        _token_cache[(slug, outcome)] = tid
    write_jsonl({"event_type": "TOKEN_RESOLVER", "action": "startup_preload",
                 "cache_size": len(_token_cache), "db_rows": len(rows)})
    print(f"  Token resolver: preloaded {len(rows)} rows from slug_tokens")


# -------------------- SQLite --------------------
def db_connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def db_init(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        txHash TEXT PRIMARY KEY,
        timestamp_epoch INTEGER,
        timestamp_iso TEXT,
        slug TEXT,
        title TEXT,
        outcome TEXT,
        side TEXT,
        size TEXT,
        price TEXT,
        usdcSize TEXT,
        clob_token_id TEXT,
        erc1155_token_id TEXT,
        order_id TEXT,
        match_id TEXT,
        crypto TEXT,
        binanceSpot TEXT,
        hour_ms INTEGER,
        hourOpen TEXT,
        hourClose TEXT,
        bestBid TEXT,
        bestAsk TEXT,
        spread TEXT,
        mid TEXT,
        microprice TEXT,
        imbalance_topN TEXT,
        bid_depth_topN TEXT,
        ask_depth_topN TEXT,
        bid_depth_total TEXT,
        ask_depth_total TEXT,
        bidsJson TEXT,
        asksJson TEXT,
        bookTs TEXT,
        bookHash TEXT,
        crossing_estimate TEXT,
        trade_vs_mid TEXT,
        trade_vs_micro TEXT,
        book_suspect TEXT,
        spread_percentile_60s TEXT,
        spread_mean_60s TEXT,
        spread_std_60s TEXT,
        net_shares_after TEXT,
        avg_cost_after TEXT,
        realized_pnl_cumulative TEXT,
        trade_id TEXT,
        market_id TEXT,
        event_id TEXT,
        outcome_id TEXT,
        taker_maker TEXT,
        fee TEXT,
        fee_rate_bps TEXT,
        trade_status TEXT,
        inv_net_shares_before TEXT,
        inv_avg_cost_before TEXT,
        time_since_last_trade TEXT
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS markouts (
        markout_id TEXT PRIMARY KEY,
        txHash TEXT,
        clob_token_id TEXT,
        t0_epoch INTEGER,
        horizon_sec INTEGER,
        due_epoch INTEGER,
        done INTEGER DEFAULT 0,
        mid_t0 TEXT,
        spread_t0 TEXT,
        binance_t0 TEXT,
        mid_t1 TEXT,
        spread_t1 TEXT,
        binance_t1 TEXT,
        trade_price TEXT,
        side TEXT,
        markout_pnl_per_share TEXT
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS slug_tokens (
        slug TEXT NOT NULL,
        outcome TEXT NOT NULL,
        token_id TEXT NOT NULL,
        updated_epoch INTEGER,
        PRIMARY KEY (slug, outcome)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(timestamp_epoch);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_markouts_due ON markouts(done, due_epoch);")

    # Migrations: rename token_id → clob_token_id in trades table
    try:
        conn.execute("SELECT clob_token_id FROM trades LIMIT 1")
    except sqlite3.OperationalError:
        try:
            conn.execute("ALTER TABLE trades RENAME COLUMN token_id TO clob_token_id")
        except Exception:
            conn.execute("ALTER TABLE trades ADD COLUMN clob_token_id TEXT DEFAULT ''")
            conn.execute("UPDATE trades SET clob_token_id = token_id WHERE token_id IS NOT NULL AND token_id != ''")

    # Migrations: rename token_id → clob_token_id in markouts table
    try:
        conn.execute("SELECT clob_token_id FROM markouts LIMIT 1")
    except sqlite3.OperationalError:
        try:
            conn.execute("ALTER TABLE markouts RENAME COLUMN token_id TO clob_token_id")
        except Exception:
            conn.execute("ALTER TABLE markouts ADD COLUMN clob_token_id TEXT DEFAULT ''")
            conn.execute("UPDATE markouts SET clob_token_id = token_id WHERE token_id IS NOT NULL AND token_id != ''")

    # Migrations: add columns if missing (existing DBs)
    _migrate_cols = [
        ("erc1155_token_id", "TEXT DEFAULT ''"),
        ("book_suspect", "TEXT DEFAULT ''"),
        ("spread_percentile_60s", "TEXT DEFAULT ''"),
        ("spread_mean_60s", "TEXT DEFAULT ''"),
        ("spread_std_60s", "TEXT DEFAULT ''"),
        ("net_shares_after", "TEXT DEFAULT ''"),
        ("avg_cost_after", "TEXT DEFAULT ''"),
        ("realized_pnl_cumulative", "TEXT DEFAULT ''"),
        ("trade_id", "TEXT DEFAULT ''"),
        ("market_id", "TEXT DEFAULT ''"),
        ("event_id", "TEXT DEFAULT ''"),
        ("outcome_id", "TEXT DEFAULT ''"),
        ("taker_maker", "TEXT DEFAULT ''"),
        ("fee", "TEXT DEFAULT ''"),
        ("fee_rate_bps", "TEXT DEFAULT ''"),
        ("trade_status", "TEXT DEFAULT ''"),
        ("inv_net_shares_before", "TEXT DEFAULT ''"),
        ("inv_avg_cost_before", "TEXT DEFAULT ''"),
        ("time_since_last_trade", "TEXT DEFAULT ''"),
        # === MAX FORENSICS v2 columns ===
        # A) Velocity features
        ("spread_prev_1s", "TEXT DEFAULT ''"),
        ("spread_prev_5s_avg", "TEXT DEFAULT ''"),
        ("spread_change_1s", "TEXT DEFAULT ''"),
        ("spread_change_5s", "TEXT DEFAULT ''"),
        ("imb_prev_1s", "TEXT DEFAULT ''"),
        ("imb_prev_5s_avg", "TEXT DEFAULT ''"),
        ("imb_change_1s", "TEXT DEFAULT ''"),
        ("imb_change_5s", "TEXT DEFAULT ''"),
        ("mid_prev_1s", "TEXT DEFAULT ''"),
        ("mid_prev_5s_avg", "TEXT DEFAULT ''"),
        ("mid_change_1s", "TEXT DEFAULT ''"),
        ("mid_change_5s", "TEXT DEFAULT ''"),
        ("mid_change_10s", "TEXT DEFAULT ''"),
        ("micro_prev_1s", "TEXT DEFAULT ''"),
        ("micro_change_1s", "TEXT DEFAULT ''"),
        ("time_since_spread_p90", "TEXT DEFAULT ''"),
        ("binance_prev_1s", "TEXT DEFAULT ''"),
        ("binance_change_1s", "TEXT DEFAULT ''"),
        ("binance_prev_5s", "TEXT DEFAULT ''"),
        ("binance_change_5s", "TEXT DEFAULT ''"),
        ("binance_prev_10s", "TEXT DEFAULT ''"),
        ("binance_change_10s", "TEXT DEFAULT ''"),
        # B) Execution style / aggressiveness
        ("aggressiveness", "TEXT DEFAULT ''"),
        ("edge_to_micro", "TEXT DEFAULT ''"),
        ("edge_to_mid", "TEXT DEFAULT ''"),
        ("spread_paid_est", "TEXT DEFAULT ''"),
        ("queue_position_proxy", "TEXT DEFAULT ''"),
        # C) Roundtrip PnL (FIFO)
        ("realized_pnl_usdc", "TEXT DEFAULT ''"),
        ("holding_time_sec_wavg", "TEXT DEFAULT ''"),
        ("avg_entry_price_matched", "TEXT DEFAULT ''"),
        # D) Trade clustering
        ("trades_last_10s", "TEXT DEFAULT ''"),
        ("trades_last_60s", "TEXT DEFAULT ''"),
        ("buys_last_60s", "TEXT DEFAULT ''"),
        ("sells_last_60s", "TEXT DEFAULT ''"),
        ("usdc_last_60s", "TEXT DEFAULT ''"),
        # E) Market selection
        ("spread_rank_at_trade", "TEXT DEFAULT ''"),
        ("imbalance_rank_at_trade", "TEXT DEFAULT ''"),
        ("spread_percentile_60s_at_trade", "TEXT DEFAULT ''"),
        ("orderbook_imbalance_at_trade", "TEXT DEFAULT ''"),
        ("binance_ret_5s_at_trade", "TEXT DEFAULT ''"),
        ("binance_ret_30s_at_trade", "TEXT DEFAULT ''"),
        ("binance_ret_60s_at_trade", "TEXT DEFAULT ''"),
        # F) Latency
        ("local_received_ms", "TEXT DEFAULT ''"),
        ("api_to_local_delay_ms", "TEXT DEFAULT ''"),
        ("book_age_ms_at_trade", "TEXT DEFAULT ''"),
        ("binance_age_ms_at_trade", "TEXT DEFAULT ''"),
    ]
    for col_name, col_type in _migrate_cols:
        try:
            conn.execute(f"SELECT {col_name} FROM trades LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute(f"ALTER TABLE trades ADD COLUMN {col_name} {col_type}")

    # Migrations for markouts table: add new columns
    _markout_migrate = [
        ("trade_price", "TEXT DEFAULT ''"),
        ("side", "TEXT DEFAULT ''"),
        ("markout_pnl_per_share", "TEXT DEFAULT ''"),
    ]
    for col_name, col_type in _markout_migrate:
        try:
            conn.execute(f"SELECT {col_name} FROM markouts LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute(f"ALTER TABLE markouts ADD COLUMN {col_name} {col_type}")

    conn.commit()


_TRADE_INSERT_COLS = [
    "txHash", "timestamp_epoch", "timestamp_iso", "slug", "title", "outcome", "side", "size", "price", "usdcSize",
    "clob_token_id", "erc1155_token_id", "order_id", "match_id",
    "crypto", "binanceSpot", "hour_ms", "hourOpen", "hourClose",
    "bestBid", "bestAsk", "spread", "mid", "microprice", "imbalance_topN", "bid_depth_topN", "ask_depth_topN",
    "bid_depth_total", "ask_depth_total", "bidsJson", "asksJson", "bookTs", "bookHash",
    "crossing_estimate", "trade_vs_mid", "trade_vs_micro", "book_suspect",
    "spread_percentile_60s", "spread_mean_60s", "spread_std_60s",
    "net_shares_after", "avg_cost_after", "realized_pnl_cumulative",
    "trade_id", "market_id", "event_id", "outcome_id", "taker_maker", "fee", "fee_rate_bps", "trade_status",
    "inv_net_shares_before", "inv_avg_cost_before", "time_since_last_trade",
    # v2 columns
    "spread_prev_1s", "spread_prev_5s_avg", "spread_change_1s", "spread_change_5s",
    "imb_prev_1s", "imb_prev_5s_avg", "imb_change_1s", "imb_change_5s",
    "mid_prev_1s", "mid_prev_5s_avg", "mid_change_1s", "mid_change_5s", "mid_change_10s",
    "micro_prev_1s", "micro_change_1s",
    "time_since_spread_p90",
    "binance_prev_1s", "binance_change_1s", "binance_prev_5s", "binance_change_5s",
    "binance_prev_10s", "binance_change_10s",
    "aggressiveness", "edge_to_micro", "edge_to_mid", "spread_paid_est", "queue_position_proxy",
    "realized_pnl_usdc", "holding_time_sec_wavg", "avg_entry_price_matched",
    "trades_last_10s", "trades_last_60s", "buys_last_60s", "sells_last_60s", "usdc_last_60s",
    "spread_rank_at_trade", "imbalance_rank_at_trade", "spread_percentile_60s_at_trade",
    "orderbook_imbalance_at_trade",
    "binance_ret_5s_at_trade", "binance_ret_30s_at_trade", "binance_ret_60s_at_trade",
    "local_received_ms", "api_to_local_delay_ms", "book_age_ms_at_trade", "binance_age_ms_at_trade",
]


def db_insert_trade(conn: sqlite3.Connection, row: dict) -> bool:
    cols_str = ", ".join(_TRADE_INSERT_COLS)
    placeholders = ", ".join(["?"] * len(_TRADE_INSERT_COLS))
    vals = tuple(row.get(c, "") for c in _TRADE_INSERT_COLS)
    try:
        conn.execute(f"INSERT INTO trades ({cols_str}) VALUES ({placeholders})", vals)
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def db_export_csv(conn: sqlite3.Connection, path: str):
    tmp = path + ".tmp"
    cur = conn.execute("SELECT * FROM trades ORDER BY timestamp_epoch ASC")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    os.replace(tmp, path)


def db_pending_hours(conn: sqlite3.Connection) -> List[Tuple[str, int]]:
    cur = conn.execute("""
        SELECT DISTINCT crypto, hour_ms
        FROM trades
        WHERE crypto IS NOT NULL AND crypto != ''
          AND hour_ms IS NOT NULL
          AND (hourClose IS NULL OR hourClose = '')
    """)
    return [(r[0], int(r[1])) for r in cur.fetchall()]


def db_set_hour_close(conn: sqlite3.Connection, crypto: str, hour_ms: int, close_price: str) -> int:
    cur = conn.execute("""
        UPDATE trades
        SET hourClose = ?
        WHERE crypto = ?
          AND hour_ms = ?
          AND (hourClose IS NULL OR hourClose = '')
    """, (close_price, crypto, hour_ms))
    conn.commit()
    return cur.rowcount


def db_add_markout_jobs(conn: sqlite3.Connection, txHash: str, token_id: str, t0_epoch: int,
                        mid_t0: str, spread_t0: str, binance_t0: str,
                        trade_price: str = "", side: str = ""):
    for h in MARKOUT_HORIZONS_SEC:
        due = t0_epoch + h
        markout_id = sha1_short(f"{txHash}:{h}")
        try:
            conn.execute("""
                INSERT INTO markouts (
                    markout_id, txHash, clob_token_id, t0_epoch, horizon_sec, due_epoch,
                    mid_t0, spread_t0, binance_t0, trade_price, side
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (markout_id, txHash, token_id, t0_epoch, h, due,
                  mid_t0, spread_t0, binance_t0, trade_price, side))
        except sqlite3.IntegrityError:
            pass
    conn.commit()


def db_fetch_due_markouts(conn: sqlite3.Connection, now_epoch: int, limit: int) -> List[tuple]:
    cur = conn.execute("""
        SELECT markout_id, txHash, clob_token_id, t0_epoch, horizon_sec, due_epoch,
               mid_t0, spread_t0, binance_t0, trade_price, side
        FROM markouts
        WHERE done = 0 AND due_epoch <= ?
        ORDER BY due_epoch ASC
        LIMIT ?
    """, (now_epoch, limit))
    return cur.fetchall()


def db_set_markout_done(conn: sqlite3.Connection, markout_id: str, mid_t1: str, spread_t1: str,
                        binance_t1: str, markout_pnl_per_share: str = ""):
    conn.execute("""
        UPDATE markouts
        SET done = 1, mid_t1 = ?, spread_t1 = ?, binance_t1 = ?, markout_pnl_per_share = ?
        WHERE markout_id = ?
    """, (mid_t1, spread_t1, binance_t1, markout_pnl_per_share, markout_id))
    conn.commit()


# -------------------- CSV append-only --------------------
def init_csv(path: str, header: List[str]):
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(header)


def append_row_csv(path: str, header: List[str], row: dict):
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        w.writerow(row)


# -------------------- Activity fetch (robust pagination) --------------------
def fetch_trades_page(wallet: str, limit: int, offset: int) -> List[dict]:
    r = SESSION.get(POLYMARKET_ACTIVITY, params={
        "user": wallet,
        "type": "TRADE",
        "limit": limit,
        "offset": offset,
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
    }, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def fetch_new_trades_since(wallet: str, start_ts: int, last_seen_ts: int) -> Tuple[List[dict], bool]:
    """
    Returns (new_trades_sorted_old_to_new, clipped_flag).
    Pages from newest, stops when:
      - offset exceeds MAX_OFFSET
      - page returns empty
      - oldest_ts_on_page <= last_seen_ts
    """
    all_new: List[dict] = []
    clipped = False
    for page in range(MAX_PAGES_PER_POLL):
        offset = page * LIMIT
        if offset > MAX_OFFSET:
            clipped = True
            break
        try:
            page_data = fetch_trades_page(wallet, LIMIT, offset)
        except requests.HTTPError:
            clipped = True
            break
        if not page_data:
            break
        oldest_ts = int(page_data[-1].get("timestamp") or 0)
        newest_ts = int(page_data[0].get("timestamp") or 0)
        for t in page_data:
            raw_ts = t.get("timestamp")
            if raw_ts is None:
                continue
            ts_int = int(raw_ts)
            if ts_int < start_ts:
                continue
            if ts_int > last_seen_ts:
                all_new.append(t)
        if oldest_ts <= last_seen_ts:
            break
        if newest_ts <= last_seen_ts:
            break
    all_new.sort(key=lambda x: int(x.get("timestamp") or 0))
    return all_new, clipped


# -------------------- rollups --------------------
def write_minute_rollup(conn: sqlite3.Connection, path: str, since_epoch: int):
    cur = conn.execute("""
        SELECT timestamp_epoch, side, price, usdcSize, bestBid, bestAsk, spread, microprice,
               clob_token_id, book_suspect
        FROM trades
        WHERE timestamp_epoch >= ?
        ORDER BY timestamp_epoch ASC
    """, (since_epoch,))
    rows = cur.fetchall()
    if not rows:
        return

    # Build set of txHashes that have markout jobs
    tx_with_markouts: set = set()
    cur2 = conn.execute("""
        SELECT DISTINCT txHash FROM markouts
        WHERE t0_epoch >= ?
    """, (since_epoch,))
    for (txh,) in cur2.fetchall():
        tx_with_markouts.add(txh)

    # We also need txHash to check markout coverage — re-query with txHash
    cur3 = conn.execute("""
        SELECT timestamp_epoch, side, price, usdcSize, bestBid, bestAsk, spread, microprice,
               clob_token_id, book_suspect, txHash
        FROM trades
        WHERE timestamp_epoch >= ?
        ORDER BY timestamp_epoch ASC
    """, (since_epoch,))
    rows = cur3.fetchall()

    buckets: Dict[int, dict] = {}
    for ts, side, px, usdc, bb, ba, sp, micro, ctid, bsuspect, txh in rows:
        m = minute_bucket(int(ts))
        b = buckets.setdefault(m, {
            "trades": 0, "buys": 0, "sells": 0, "usdc_total": 0.0,
            "spreads": [], "micros": [], "cross": 0, "cross_den": 0,
            "with_clob_token": 0, "with_book": 0, "with_markouts": 0,
        })
        b["trades"] += 1
        s = (side or "").upper()
        if s == "BUY":
            b["buys"] += 1
        elif s == "SELL":
            b["sells"] += 1
        # Coverage columns
        if ctid not in (None, ""):
            b["with_clob_token"] += 1
        if bb not in (None, "") and bsuspect in (None, "", None):
            b["with_book"] += 1
        if txh in tx_with_markouts:
            b["with_markouts"] += 1
        try:
            b["usdc_total"] += float(usdc or 0)
        except Exception:
            pass
        try:
            if sp not in (None, ""):
                b["spreads"].append(float(sp))
        except Exception:
            pass
        try:
            if micro not in (None, ""):
                b["micros"].append(float(micro))
        except Exception:
            pass
        try:
            pxf = float(px)
            bbf = float(bb) if bb not in (None, "") else None
            baf = float(ba) if ba not in (None, "") else None
            if bbf is not None or baf is not None:
                b["cross_den"] += 1
                if s == "BUY" and baf is not None and pxf >= baf:
                    b["cross"] += 1
                elif s == "SELL" and bbf is not None and pxf <= bbf:
                    b["cross"] += 1
        except Exception:
            pass
    header = ["minute_epoch", "minute_iso", "trades", "buys", "sells",
              "usdc_total", "avg_spread", "avg_microprice", "aggressive_cross_pct",
              "trades_with_clob_token", "trades_with_book", "trades_with_markouts"]
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for m in sorted(buckets.keys()):
            b = buckets[m]
            avg_sp = (sum(b["spreads"]) / len(b["spreads"])) if b["spreads"] else ""
            avg_micro = (sum(b["micros"]) / len(b["micros"])) if b["micros"] else ""
            cross_pct = (b["cross"] / b["cross_den"] * 100.0) if b["cross_den"] else ""
            w.writerow([
                m, ts_to_iso(m),
                b["trades"], b["buys"], b["sells"],
                f"{b['usdc_total']:.4f}",
                (f"{avg_sp:.6f}" if avg_sp != "" else ""),
                (f"{avg_micro:.6f}" if avg_micro != "" else ""),
                (f"{cross_pct:.2f}" if cross_pct != "" else ""),
                b["with_clob_token"], b["with_book"], b["with_markouts"],
            ])
    os.replace(tmp, path)


def write_fingerprint(conn: sqlite3.Connection, path: str, window_sec: int = 60):
    since = epoch_sec() - window_sec
    cur = conn.execute("""
        SELECT side, price, bestBid, bestAsk, spread, usdcSize
        FROM trades
        WHERE timestamp_epoch >= ?
        ORDER BY timestamp_epoch ASC
    """, (since,))
    rows = cur.fetchall()
    if not rows:
        return
    n = len(rows)
    buys = sum(1 for r in rows if (r[0] or "").upper() == "BUY")
    sells = n - buys
    spreads = []
    cross = 0
    cross_den = 0
    usdc_total = 0.0
    for side, px, bb, ba, sp, usdc in rows:
        try:
            usdc_total += float(usdc or 0)
        except Exception:
            pass
        try:
            if sp not in (None, ""):
                spreads.append(float(sp))
        except Exception:
            pass
        try:
            pxf = float(px)
            bbf = float(bb) if bb not in (None, "") else None
            baf = float(ba) if ba not in (None, "") else None
            if bbf is not None or baf is not None:
                cross_den += 1
                if (side or "").upper() == "BUY" and baf is not None and pxf >= baf:
                    cross += 1
                elif (side or "").upper() == "SELL" and bbf is not None and pxf <= bbf:
                    cross += 1
        except Exception:
            pass
    avg_spread = (sum(spreads) / len(spreads)) if spreads else None
    cross_pct = (cross / cross_den * 100.0) if cross_den else None
    header = ["ts_iso", "window_sec", "trades", "buys", "sells",
              "usdc_total", "avg_spread", "aggressive_cross_pct"]
    init_csv(path, header)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            ts_to_iso(epoch_sec()),
            window_sec, n, buys, sells,
            f"{usdc_total:.4f}",
            (f"{avg_spread:.6f}" if avg_spread is not None else ""),
            (f"{cross_pct:.2f}" if cross_pct is not None else ""),
        ])


# -------------------- Layer 1: Spread Percentile Tracking --------------------
_spread_window: Dict[str, deque] = {}   # token_id -> deque of (epoch_sec, spread_float)


def spread_tracker_record(token_id: str, ts_epoch: int, spread_val: float):
    """Record a spread observation and evict entries older than SPREAD_WINDOW_SEC."""
    if not token_id:
        return
    dq = _spread_window.get(token_id)
    if dq is None:
        dq = deque()
        _spread_window[token_id] = dq
    dq.append((ts_epoch, spread_val))
    cutoff = ts_epoch - SPREAD_WINDOW_SEC
    while dq and dq[0][0] < cutoff:
        dq.popleft()


def spread_tracker_stats(token_id: str, current_spread: float) -> Dict[str, str]:
    """Compute percentile/mean/std of current_spread within the rolling 60s window."""
    result = {"spread_percentile_60s": "", "spread_mean_60s": "", "spread_std_60s": ""}
    dq = _spread_window.get(token_id)
    if not dq or len(dq) < 2:
        return result
    spreads = [s for _, s in dq]
    n = len(spreads)
    mean = sum(spreads) / n
    result["spread_mean_60s"] = clean_number(mean)
    if n >= 2:
        std = statistics.stdev(spreads)
        result["spread_std_60s"] = clean_number(std)
    # Percentile rank: fraction of window spreads <= current_spread
    rank = sum(1 for s in spreads if s <= current_spread)
    pct = rank / n * 100.0
    result["spread_percentile_60s"] = clean_number(pct)
    return result


# -------------------- Layer 2: Inventory Tracking --------------------
_inventory: Dict[Tuple[str, str], dict] = {}   # (slug, outcome) -> {net_shares, avg_cost, realized_pnl, trade_count}
_last_trade_ts: Dict[str, int] = {}   # clob_token_id -> epoch_sec of last trade


def inventory_get_before(slug: str, outcome: str) -> Dict[str, str]:
    """Return pre-trade inventory snapshot for wallet_entry_model."""
    key = (slug, outcome)
    inv = _inventory.get(key)
    if inv is None:
        return {"inv_net_shares_before": "0", "inv_avg_cost_before": "0"}
    return {
        "inv_net_shares_before": clean_number(inv["net_shares"]),
        "inv_avg_cost_before": clean_number(inv["avg_cost"]),
    }


def get_time_since_last_trade(token_id: str, current_ts: int) -> str:
    """Return seconds since last trade on this token_id, or '' if first trade."""
    last = _last_trade_ts.get(token_id)
    if last is None:
        return ""
    return str(current_ts - last)


def record_trade_ts(token_id: str, ts: int):
    """Record the timestamp of a trade on a token."""
    if token_id:
        _last_trade_ts[token_id] = ts


def inventory_update(slug: str, outcome: str, side: str, size_f: float, price_f: float) -> Dict[str, str]:
    """Update inventory for (slug, outcome) and return post-trade snapshot fields."""
    key = (slug, outcome)
    inv = _inventory.get(key)
    if inv is None:
        inv = {"net_shares": 0.0, "avg_cost": 0.0, "realized_pnl": 0.0, "trade_count": 0}
        _inventory[key] = inv
    inv["trade_count"] += 1
    if side == "BUY":
        old_shares = inv["net_shares"]
        old_cost = inv["avg_cost"]
        new_shares = old_shares + size_f
        if new_shares > 0:
            inv["avg_cost"] = (old_cost * old_shares + price_f * size_f) / new_shares
        inv["net_shares"] = new_shares
    elif side == "SELL":
        if size_f > 0 and inv["net_shares"] > 0:
            inv["realized_pnl"] += size_f * (price_f - inv["avg_cost"])
        inv["net_shares"] -= size_f
        if inv["net_shares"] <= 0:
            inv["net_shares"] = 0.0
            inv["avg_cost"] = 0.0
    return {
        "net_shares_after": clean_number(inv["net_shares"]),
        "avg_cost_after": clean_number(inv["avg_cost"]),
        "realized_pnl_cumulative": clean_number(inv["realized_pnl"]),
    }


def write_inventory_snapshot(path: str):
    """Write current inventory state for all tracked (slug, outcome) pairs."""
    if not _inventory:
        return
    header = ["ts_iso", "slug", "outcome", "net_shares", "avg_cost",
              "realized_pnl", "trade_count"]
    tmp = path + ".tmp"
    now_iso = ts_to_iso(epoch_sec())
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for (slug, outcome), inv in sorted(_inventory.items()):
            w.writerow([
                now_iso, slug, outcome,
                clean_number(inv["net_shares"]),
                clean_number(inv["avg_cost"]),
                clean_number(inv["realized_pnl"]),
                inv["trade_count"],
            ])
    os.replace(tmp, path)


# -------------------- Layer 3: Markout Performance Analysis --------------------
def compute_markout_stats(conn: sqlite3.Connection, window: int = MARKOUT_STATS_WINDOW) -> Dict[int, dict]:
    """Compute rolling markout stats from the last `window` completed markouts per horizon."""
    result: Dict[int, dict] = {}
    for h in MARKOUT_HORIZONS_SEC:
        cur = conn.execute("""
            SELECT m.horizon_sec, m.mid_t0, m.mid_t1, m.spread_t0, t.side
            FROM markouts m
            LEFT JOIN trades t ON m.txHash = t.txHash
            WHERE m.done = 1 AND m.horizon_sec = ?
              AND m.mid_t0 IS NOT NULL AND m.mid_t0 != ''
              AND m.mid_t1 IS NOT NULL AND m.mid_t1 != ''
            ORDER BY m.t0_epoch DESC
            LIMIT ?
        """, (h, window))
        rows = cur.fetchall()
        if not rows:
            result[h] = {"count": 0}
            continue
        markouts_buy = []
        markouts_sell = []
        all_markouts = []
        for _, mid0_s, mid1_s, _, side in rows:
            m0 = ffloat(mid0_s)
            m1 = ffloat(mid1_s)
            if m0 is None or m1 is None:
                continue
            delta = m1 - m0
            s = (side or "").upper()
            # For BUY, positive markout = price went up (good)
            # For SELL, positive markout = price went down (good), so negate
            signed = delta if s == "BUY" else -delta
            all_markouts.append(signed)
            if s == "BUY":
                markouts_buy.append(signed)
            elif s == "SELL":
                markouts_sell.append(signed)
        if not all_markouts:
            result[h] = {"count": 0}
            continue
        n = len(all_markouts)
        avg = sum(all_markouts) / n
        med = statistics.median(all_markouts)
        pct_pos = sum(1 for m in all_markouts if m > 0) / n * 100.0
        avg_buy = (sum(markouts_buy) / len(markouts_buy)) if markouts_buy else None
        avg_sell = (sum(markouts_sell) / len(markouts_sell)) if markouts_sell else None
        result[h] = {
            "count": n,
            "avg_markout": avg,
            "median_markout": med,
            "pct_positive": pct_pos,
            "avg_markout_buy": avg_buy,
            "avg_markout_sell": avg_sell,
        }
    return result


def write_markout_stats(conn: sqlite3.Connection, path: str):
    """Write rolling markout performance stats CSV."""
    stats = compute_markout_stats(conn)
    if not stats:
        return
    header = ["ts_iso", "horizon_sec", "count", "avg_markout", "median_markout",
              "pct_positive", "avg_markout_buy", "avg_markout_sell", "edge_decay_ratio"]
    # Compute edge_decay_ratio = avg_markout_10s / avg_markout_2s
    avg_2 = stats.get(2, {}).get("avg_markout")
    avg_10 = stats.get(10, {}).get("avg_markout")
    edge_decay = ""
    if avg_2 is not None and avg_2 != 0 and avg_10 is not None:
        edge_decay = clean_number(avg_10 / avg_2)
    tmp = path + ".tmp"
    now_iso = ts_to_iso(epoch_sec())
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for h in MARKOUT_HORIZONS_SEC:
            s = stats.get(h, {})
            if s.get("count", 0) == 0:
                w.writerow([now_iso, h, 0, "", "", "", "", "", ""])
                continue
            w.writerow([
                now_iso, h, s["count"],
                clean_number(s["avg_markout"]),
                clean_number(s["median_markout"]),
                clean_number(s["pct_positive"]),
                clean_number(s["avg_markout_buy"]) if s["avg_markout_buy"] is not None else "",
                clean_number(s["avg_markout_sell"]) if s["avg_markout_sell"] is not None else "",
                edge_decay if h == 10 else "",
            ])
    os.replace(tmp, path)


# -------------------- wallet_entry_model.csv --------------------
ENTRY_MODEL_EVERY_SECONDS = 120

_ENTRY_MODEL_HEADER = [
    "txHash", "timestamp_iso", "slug", "outcome", "side", "size",
    "trade_price", "bestBid", "bestAsk", "spread", "mid", "microprice",
    "imbalance_topN", "bid_depth_topN", "ask_depth_topN",
    "spread_percentile_60s", "spread_mean_60s", "spread_std_60s",
    "inv_net_shares_before", "inv_avg_cost_before",
    "time_since_last_trade",
    "crossing_estimate", "fee_rate_bps",
    "taker_maker", "fee_usdc",
    "markout_pnl_2s", "markout_pnl_10s", "markout_pnl_30s", "markout_pnl_60s",
    # Exit modeling columns
    "time_to_next_sell_sec", "realized_pnl_roundtrip", "inventory_action",
]


def write_wallet_entry_model(conn: sqlite3.Connection, path: str):
    """Produce wallet_entry_model.csv: one row per trade with all features + markout PnL.

    Joins trades with completed markouts to produce the dataset needed for
    fitting a statistical entry rule.

    Exit modeling columns:
      - time_to_next_sell_sec: for BUY rows, seconds until next SELL on same (slug, outcome)
      - realized_pnl_roundtrip: for SELL rows, FIFO PnL for the shares sold
      - inventory_action: OPEN (buy), REDUCE (partial sell), CLOSE (full close), FLIP (sell with no inventory)
    """
    cur = conn.execute("""
        SELECT t.txHash, t.timestamp_iso, t.slug, t.outcome, t.side, t.size,
               t.price, t.bestBid, t.bestAsk, t.spread, t.mid, t.microprice,
               t.imbalance_topN, t.bid_depth_topN, t.ask_depth_topN,
               t.spread_percentile_60s, t.spread_mean_60s, t.spread_std_60s,
               t.inv_net_shares_before, t.inv_avg_cost_before,
               t.time_since_last_trade,
               t.crossing_estimate, t.fee_rate_bps,
               t.taker_maker, t.fee,
               t.timestamp_epoch
        FROM trades t
        ORDER BY t.timestamp_epoch ASC
    """)
    trades = cur.fetchall()
    if not trades:
        return

    # Pre-fetch all completed markout PnLs, indexed by (txHash, horizon)
    mk_cur = conn.execute("""
        SELECT txHash, horizon_sec, markout_pnl_per_share
        FROM markouts
        WHERE done = 1 AND markout_pnl_per_share IS NOT NULL AND markout_pnl_per_share != ''
    """)
    mk_map: Dict[Tuple[str, int], str] = {}
    for mk_tx, mk_h, mk_pnl in mk_cur.fetchall():
        mk_map[(mk_tx, int(mk_h))] = mk_pnl

    # ---- Exit modeling: group trades by (slug, outcome) ----
    position_trades: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for i, row in enumerate(trades):
        slug_r = row[2] or ""
        outcome_r = row[3] or ""
        side_r = (row[4] or "").upper()
        position_trades[(slug_r, outcome_r)].append({
            "idx": i,
            "ts": int(row[25] or 0),
            "side": side_r,
            "size": ffloat(row[5]) or 0.0,
            "price": ffloat(row[6]) or 0.0,
        })

    exit_cols: Dict[int, dict] = {}   # row index -> exit model fields
    for (_slug_k, _out_k), tlist in position_trades.items():
        fifo_queue: List[List[float]] = []   # [[remaining_size, buy_price], ...]
        for ti in tlist:
            idx = ti["idx"]
            if ti["side"] == "BUY":
                # Find time_to_next_sell on same (slug, outcome)
                time_to_sell = ""
                for tj in tlist:
                    if tj["ts"] > ti["ts"] and tj["side"] == "SELL":
                        time_to_sell = str(tj["ts"] - ti["ts"])
                        break
                fifo_queue.append([ti["size"], ti["price"]])
                exit_cols[idx] = {
                    "time_to_next_sell_sec": time_to_sell,
                    "realized_pnl_roundtrip": "",
                    "inventory_action": "OPEN",
                }
            elif ti["side"] == "SELL":
                total_before = sum(q[0] for q in fifo_queue)
                sell_size = ti["size"]
                if total_before <= 1e-9:
                    action = "FLIP"
                elif sell_size >= total_before - 1e-9:
                    action = "CLOSE"
                else:
                    action = "REDUCE"
                # FIFO PnL
                pnl = 0.0
                remaining = sell_size
                while remaining > 1e-9 and fifo_queue:
                    avail = fifo_queue[0][0]
                    buy_px = fifo_queue[0][1]
                    matched = min(remaining, avail)
                    pnl += matched * (ti["price"] - buy_px)
                    fifo_queue[0][0] -= matched
                    remaining -= matched
                    if fifo_queue[0][0] < 1e-9:
                        fifo_queue.pop(0)
                exit_cols[idx] = {
                    "time_to_next_sell_sec": "",
                    "realized_pnl_roundtrip": clean_number(pnl) if sell_size > 0 else "",
                    "inventory_action": action,
                }
            else:
                exit_cols[idx] = {
                    "time_to_next_sell_sec": "",
                    "realized_pnl_roundtrip": "",
                    "inventory_action": "",
                }

    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(_ENTRY_MODEL_HEADER)
        for i, row in enumerate(trades):
            tx = row[0]
            # Columns 0-24 are the base query fields (25 = timestamp_epoch, skip)
            base = list(row[:25])
            ec = exit_cols.get(i, {})
            w.writerow(base + [
                mk_map.get((tx, 2), ""),
                mk_map.get((tx, 10), ""),
                mk_map.get((tx, 30), ""),
                mk_map.get((tx, 60), ""),
                ec.get("time_to_next_sell_sec", ""),
                ec.get("realized_pnl_roundtrip", ""),
                ec.get("inventory_action", ""),
            ])
    os.replace(tmp, path)


# -------------------- Layer 4: Behavior Summary --------------------
def print_behavior_summary(conn: sqlite3.Connection):
    """Print a 60-second behavior summary to console + JSONL."""
    since = epoch_sec() - 60
    cur = conn.execute("""
        SELECT side, spread, bestBid, bestAsk, price,
               spread_percentile_60s, net_shares_after, slug, outcome
        FROM trades
        WHERE timestamp_epoch >= ?
        ORDER BY timestamp_epoch ASC
    """, (since,))
    rows = cur.fetchall()
    if not rows:
        return
    n = len(rows)
    buys = 0
    sells = 0
    pcts = []
    cross = 0
    cross_den = 0
    for side, sp, bb, ba, px, pct60, net_sh, slug, outcome in rows:
        s = (side or "").upper()
        if s == "BUY":
            buys += 1
        elif s == "SELL":
            sells += 1
        pf = ffloat(pct60)
        if pf is not None:
            pcts.append(pf)
        pxf = ffloat(px)
        bbf = ffloat(bb)
        baf = ffloat(ba)
        if pxf is not None and (bbf is not None or baf is not None):
            cross_den += 1
            if s == "BUY" and baf is not None and pxf >= baf:
                cross += 1
            elif s == "SELL" and bbf is not None and pxf <= bbf:
                cross += 1
    avg_pct = (sum(pcts) / len(pcts)) if pcts else None
    cross_pct = (cross / cross_den * 100.0) if cross_den else None

    # Avg markout 2s from completed markouts in last 60s
    cur2 = conn.execute("""
        SELECT m.mid_t0, m.mid_t1, t.side
        FROM markouts m
        LEFT JOIN trades t ON m.txHash = t.txHash
        WHERE m.done = 1 AND m.horizon_sec = 2
          AND m.t0_epoch >= ?
          AND m.mid_t0 IS NOT NULL AND m.mid_t0 != ''
          AND m.mid_t1 IS NOT NULL AND m.mid_t1 != ''
    """, (since,))
    mk_rows = cur2.fetchall()
    markout_2s_vals = []
    for mid0_s, mid1_s, side in mk_rows:
        m0 = ffloat(mid0_s)
        m1 = ffloat(mid1_s)
        if m0 is not None and m1 is not None:
            delta = m1 - m0
            s = (side or "").upper()
            markout_2s_vals.append(delta if s == "BUY" else -delta)
    avg_mk2 = (sum(markout_2s_vals) / len(markout_2s_vals)) if markout_2s_vals else None

    # Top 3 inventory exposures by abs(net_shares)
    top_inv = sorted(_inventory.items(), key=lambda x: abs(x[1]["net_shares"]), reverse=True)[:3]
    inv_strs = []
    for (sl, out), inv in top_inv:
        if inv["net_shares"] != 0:
            inv_strs.append(f"{sl[:20]}:{out}={inv['net_shares']:.1f}")

    summary = {
        "event_type": "BEHAVIOR_SUMMARY",
        "trades_last_60s": n,
        "buys": buys,
        "sells": sells,
        "avg_spread_percentile": clean_number(avg_pct) if avg_pct is not None else "",
        "aggressive_cross_pct": clean_number(cross_pct) if cross_pct is not None else "",
        "avg_markout_2s": clean_number(avg_mk2) if avg_mk2 is not None else "",
        "top_inventory": inv_strs,
    }
    write_jsonl(summary)

    # Console
    mk2_str = f"{avg_mk2:+.4f}" if avg_mk2 is not None else "N/A"
    pct_str = f"{avg_pct:.1f}%" if avg_pct is not None else "N/A"
    cross_str = f"{cross_pct:.1f}%" if cross_pct is not None else "N/A"
    inv_display = " | ".join(inv_strs) if inv_strs else "flat"
    print(
        f"  [BEHAVIOR 60s] trades={n} B={buys} S={sells} "
        f"spr_pct={pct_str} cross={cross_str} mk2={mk2_str} "
        f"inv=[{inv_display}]"
    )


# -------------------- DATA_COVERAGE periodic warning --------------------
_coverage_counters = {"trades": 0, "clob_resolved": 0, "book_ok": 0, "markouts_created": 0}


def coverage_increment(field: str, count: int = 1):
    """Increment a coverage counter."""
    _coverage_counters[field] = _coverage_counters.get(field, 0) + count


def print_data_coverage():
    """Print DATA_COVERAGE summary for last period, then reset counters."""
    c = _coverage_counters
    msg = (f"  DATA_COVERAGE last60s: trades={c['trades']} "
           f"clob_resolved={c['clob_resolved']} "
           f"book_ok={c['book_ok']} "
           f"markouts_created={c['markouts_created']}")
    print(msg)
    write_jsonl({
        "event_type": "DATA_COVERAGE",
        "trades": c["trades"],
        "clob_resolved": c["clob_resolved"],
        "book_ok": c["book_ok"],
        "markouts_created": c["markouts_created"],
    })
    # Reset
    for k in _coverage_counters:
        _coverage_counters[k] = 0


# ==================== MAX FORENSICS v2: NEW SUBSYSTEMS ====================

# -------------------- Book tape in-memory (velocity lookups) --------------------
# token_id -> deque of (t_sec, spread_f, imb_f, mid_f, micro_f)
_book_tape_mem: Dict[str, deque] = {}


def book_tape_mem_record(token_id: str, t_sec: int,
                         spread_f: Optional[float], imb_f: Optional[float],
                         mid_f: Optional[float], micro_f: Optional[float]):
    """Store a book tape observation in memory for velocity feature lookups."""
    if not token_id:
        return
    dq = _book_tape_mem.get(token_id)
    if dq is None:
        dq = deque()
        _book_tape_mem[token_id] = dq
    dq.append((t_sec, spread_f, imb_f, mid_f, micro_f))
    cutoff = t_sec - BOOK_TAPE_MEMORY_SEC
    while dq and dq[0][0] < cutoff:
        dq.popleft()


# -------------------- Binance tape in-memory --------------------
# symbol -> deque of (t_sec, price_f)
_binance_tape_mem: Dict[str, deque] = {}
_active_cryptos: Dict[str, float] = {}  # symbol -> last_seen_monotonic


def binance_tape_mem_record(symbol: str, t_sec: int, price_f: float):
    """Store a Binance spot observation in memory."""
    if not symbol:
        return
    dq = _binance_tape_mem.get(symbol)
    if dq is None:
        dq = deque()
        _binance_tape_mem[symbol] = dq
    dq.append((t_sec, price_f))
    cutoff = t_sec - BINANCE_TAPE_MEMORY_SEC
    while dq and dq[0][0] < cutoff:
        dq.popleft()


_BINANCE_TAPE_HEADER = [
    "timestamp_iso", "timestamp_epoch", "symbol", "price", "tape_source",
]

_binance_tape_last_sec: Dict[str, int] = {}  # symbol -> last recorded epoch_sec


def write_binance_tape_tick():
    """Record 1Hz Binance spot prices for active crypto symbols."""
    if not _active_cryptos:
        return
    now_epoch = epoch_sec()
    now_iso = ts_to_iso(now_epoch)
    for symbol in list(_active_cryptos.keys()):
        # Deduplicate: at most 1 row per symbol per second
        if _binance_tape_last_sec.get(symbol) == now_epoch:
            continue
        price_str = binance_spot_price(symbol)
        price_f = ffloat(price_str)
        if price_f is None:
            continue
        _binance_tape_last_sec[symbol] = now_epoch
        binance_tape_mem_record(symbol, now_epoch, price_f)
        append_row_csv(BINANCE_TAPE_CSV, _BINANCE_TAPE_HEADER, {
            "timestamp_iso": now_iso,
            "timestamp_epoch": str(now_epoch),
            "symbol": symbol,
            "price": price_str,
            "tape_source": "BINANCE_SPOT",
        })


def register_active_crypto(symbol: str):
    """Register a crypto symbol for Binance tape recording."""
    if symbol:
        _active_cryptos[symbol] = time.monotonic()
        # Evict stale symbols (not seen in 60 min)
        cutoff = time.monotonic() - 3600
        stale = [s for s, t in _active_cryptos.items() if t < cutoff]
        for s in stale:
            del _active_cryptos[s]


# -------------------- Binance momentum snapshot at trade time --------------------
def _binance_closest_price(dq: deque, target_sec: int, max_delta: int = 2) -> Optional[float]:
    """Find the Binance price closest to target_sec within ±max_delta seconds."""
    best_entry = None
    best_diff = max_delta + 1
    for entry in reversed(dq):
        diff = abs(entry[0] - target_sec)
        if diff < best_diff:
            best_diff = diff
            best_entry = entry
        # Once we're past max_delta behind the target, stop searching
        if entry[0] < target_sec - max_delta:
            break
    if best_entry is not None and best_diff <= max_delta:
        return best_entry[1]
    return None


def compute_binance_momentum_at_trade(crypto: str, trade_ts: int) -> dict:
    """Compute Binance return snapshots at the exact trade moment.

    Returns binance_ret_5s, _30s, _60s as decimal returns (e.g. 0.0015 = 0.15%)
    using the closest Binance tape entry within ±2s of trade_ts.
    """
    r = {"binance_ret_5s_at_trade": "", "binance_ret_30s_at_trade": "",
         "binance_ret_60s_at_trade": ""}
    if not crypto:
        return r
    bdq = _binance_tape_mem.get(crypto)
    if not bdq or len(bdq) < 2:
        return r

    # Current price: closest to trade_ts within ±2s
    cur_price = _binance_closest_price(bdq, trade_ts, max_delta=2)
    if cur_price is None or cur_price <= 0:
        return r

    for label, lookback in [("binance_ret_5s_at_trade", 5),
                             ("binance_ret_30s_at_trade", 30),
                             ("binance_ret_60s_at_trade", 60)]:
        past_price = _binance_closest_price(bdq, trade_ts - lookback, max_delta=2)
        if past_price is not None and past_price > 0:
            ret_decimal = (cur_price - past_price) / past_price
            r[label] = clean_number(ret_decimal)
    return r


# -------------------- Orderbook imbalance at trade time --------------------
def compute_orderbook_imbalance_at_trade(token_id: str, trade_ts: int,
                                          snap_bid_depth: Optional[float],
                                          snap_ask_depth: Optional[float]) -> str:
    """Compute raw orderbook imbalance = bid_size / (bid_size + ask_size).

    Uses the live snap bid/ask depth.  Result is guaranteed in [0, 1].
    Logs a warning if inputs are negative (should never happen).
    """
    bid = max(0.0, snap_bid_depth) if snap_bid_depth is not None else None
    ask = max(0.0, snap_ask_depth) if snap_ask_depth is not None else None

    if bid is not None and ask is not None:
        if snap_bid_depth < 0 or snap_ask_depth < 0:
            import logging
            logging.warning(
                "Negative depth in imbalance: bid_depth=%.4f ask_depth=%.4f (token=%s ts=%d)",
                snap_bid_depth, snap_ask_depth, token_id, trade_ts,
            )
        denom = max(1e-9, bid + ask)
        imb = bid / denom
        if imb < 0 or imb > 1:
            import logging
            logging.warning(
                "Imbalance out of [0,1]: %.6f  bid=%.4f ask=%.4f (token=%s ts=%d)",
                imb, bid, ask, token_id, trade_ts,
            )
            imb = max(0.0, min(1.0, imb))
        return clean_number(imb)
    return ""


# -------------------- A) Velocity Features --------------------
def _tape_val_at(dq: deque, t0: int, offset_sec: int, idx: int) -> Optional[float]:
    """Get tape value at index `idx` for the latest entry at or before (t0 - offset_sec)."""
    target = t0 - offset_sec
    best = None
    for entry in reversed(dq):
        if entry[0] <= target:
            best = entry
            break
    if best is None:
        return None
    return best[idx]


def _tape_avg_over(dq: deque, t0: int, from_sec: int, to_sec: int, idx: int) -> Optional[float]:
    """Average tape value at index `idx` over entries in [t0-from_sec, t0-to_sec]."""
    lo = t0 - from_sec
    hi = t0 - to_sec
    vals = [e[idx] for e in dq if lo <= e[0] <= hi and e[idx] is not None]
    return (sum(vals) / len(vals)) if vals else None


def compute_velocity_features(token_id: str, t0: int, current_spread: Optional[float],
                              current_imb: Optional[float], current_mid: Optional[float],
                              current_micro: Optional[float],
                              crypto: str) -> dict:
    """Compute all velocity features for a trade at time t0."""
    r: dict = {}

    # --- Book tape velocities ---
    dq = _book_tape_mem.get(token_id) if token_id else None
    if dq and len(dq) >= 2:
        # Spread
        sp1 = _tape_val_at(dq, t0, 1, 1)
        sp5avg = _tape_avg_over(dq, t0, 5, 1, 1)
        r["spread_prev_1s"] = clean_number(sp1) if sp1 is not None else ""
        r["spread_prev_5s_avg"] = clean_number(sp5avg) if sp5avg is not None else ""
        r["spread_change_1s"] = clean_number(current_spread - sp1) if (current_spread is not None and sp1 is not None) else ""
        r["spread_change_5s"] = clean_number(current_spread - sp5avg) if (current_spread is not None and sp5avg is not None) else ""

        # Imbalance
        im1 = _tape_val_at(dq, t0, 1, 2)
        im5avg = _tape_avg_over(dq, t0, 5, 1, 2)
        r["imb_prev_1s"] = clean_number(im1) if im1 is not None else ""
        r["imb_prev_5s_avg"] = clean_number(im5avg) if im5avg is not None else ""
        r["imb_change_1s"] = clean_number(current_imb - im1) if (current_imb is not None and im1 is not None) else ""
        r["imb_change_5s"] = clean_number(current_imb - im5avg) if (current_imb is not None and im5avg is not None) else ""

        # Mid
        mid1 = _tape_val_at(dq, t0, 1, 3)
        mid5avg = _tape_avg_over(dq, t0, 5, 1, 3)
        mid10 = _tape_val_at(dq, t0, 10, 3)
        r["mid_prev_1s"] = clean_number(mid1) if mid1 is not None else ""
        r["mid_prev_5s_avg"] = clean_number(mid5avg) if mid5avg is not None else ""
        r["mid_change_1s"] = clean_number(current_mid - mid1) if (current_mid is not None and mid1 is not None) else ""
        r["mid_change_5s"] = clean_number(current_mid - mid5avg) if (current_mid is not None and mid5avg is not None) else ""
        r["mid_change_10s"] = clean_number(current_mid - mid10) if (current_mid is not None and mid10 is not None) else ""

        # Microprice
        mic1 = _tape_val_at(dq, t0, 1, 4)
        r["micro_prev_1s"] = clean_number(mic1) if mic1 is not None else ""
        r["micro_change_1s"] = clean_number(current_micro - mic1) if (current_micro is not None and mic1 is not None) else ""

        # time_since_spread_p90: seconds since spread percentile >= 90%
        # Use spread_tracker_stats to get current percentile, scan backward for p90+
        ts_spread_p90 = ""
        sdq = _spread_window.get(token_id)
        if sdq and len(sdq) >= 5:
            spreads_sorted = sorted(s for _, s in sdq)
            p90_threshold = spreads_sorted[int(len(spreads_sorted) * 0.9)]
            # Scan backward through book tape for last time spread >= p90
            for entry in reversed(dq):
                if entry[1] is not None and entry[1] >= p90_threshold:
                    ts_spread_p90 = str(t0 - entry[0])
                    break
        r["time_since_spread_p90"] = ts_spread_p90
    else:
        for k in ["spread_prev_1s", "spread_prev_5s_avg", "spread_change_1s", "spread_change_5s",
                   "imb_prev_1s", "imb_prev_5s_avg", "imb_change_1s", "imb_change_5s",
                   "mid_prev_1s", "mid_prev_5s_avg", "mid_change_1s", "mid_change_5s", "mid_change_10s",
                   "micro_prev_1s", "micro_change_1s", "time_since_spread_p90"]:
            r[k] = ""

    # --- Binance tape velocities ---
    bdq = _binance_tape_mem.get(crypto) if crypto else None
    if bdq and len(bdq) >= 2:
        # Get current binance price (latest in tape)
        cur_bn = bdq[-1][1] if bdq else None
        bn1 = _tape_val_at(bdq, t0, 1, 1)
        bn5 = _tape_val_at(bdq, t0, 5, 1)
        bn10 = _tape_val_at(bdq, t0, 10, 1)
        r["binance_prev_1s"] = clean_number(bn1) if bn1 is not None else ""
        r["binance_change_1s"] = clean_number(cur_bn - bn1) if (cur_bn is not None and bn1 is not None) else ""
        r["binance_prev_5s"] = clean_number(bn5) if bn5 is not None else ""
        r["binance_change_5s"] = clean_number(cur_bn - bn5) if (cur_bn is not None and bn5 is not None) else ""
        r["binance_prev_10s"] = clean_number(bn10) if bn10 is not None else ""
        r["binance_change_10s"] = clean_number(cur_bn - bn10) if (cur_bn is not None and bn10 is not None) else ""
    else:
        for k in ["binance_prev_1s", "binance_change_1s", "binance_prev_5s",
                   "binance_change_5s", "binance_prev_10s", "binance_change_10s"]:
            r[k] = ""

    return r


# -------------------- B) Aggressiveness Features --------------------
def compute_aggressiveness(side: str, px: Optional[float],
                           bb: Optional[float], ba: Optional[float],
                           mid_f: Optional[float], micro_f: Optional[float]) -> dict:
    """Compute detailed execution style / aggressiveness features."""
    r = {"aggressiveness": "", "edge_to_micro": "", "edge_to_mid": "",
         "spread_paid_est": "", "queue_position_proxy": ""}
    if px is None:
        return r

    # Aggressiveness classification
    if side == "BUY":
        if ba is not None and px >= ba - 1e-9:
            r["aggressiveness"] = "CROSS_ASK"
        elif mid_f is not None and px > mid_f:
            r["aggressiveness"] = "LIFT_INSIDE"
        else:
            r["aggressiveness"] = "PASSIVE_OR_BELOW_MID"
    elif side == "SELL":
        if bb is not None and px <= bb + 1e-9:
            r["aggressiveness"] = "CROSS_BID"
        elif mid_f is not None and px < mid_f:
            r["aggressiveness"] = "HIT_INSIDE"
        else:
            r["aggressiveness"] = "PASSIVE_OR_ABOVE_MID"

    # Edge to microprice
    if micro_f is not None:
        if side == "BUY":
            r["edge_to_micro"] = clean_number(micro_f - px)
        elif side == "SELL":
            r["edge_to_micro"] = clean_number(px - micro_f)

    # Edge to mid
    if mid_f is not None:
        if side == "BUY":
            r["edge_to_mid"] = clean_number(mid_f - px)
        elif side == "SELL":
            r["edge_to_mid"] = clean_number(px - mid_f)

    # Spread paid estimate
    if side == "BUY" and bb is not None:
        r["spread_paid_est"] = clean_number(max(0.0, px - bb))
    elif side == "SELL" and ba is not None:
        r["spread_paid_est"] = clean_number(max(0.0, ba - px))

    # Queue position proxy
    agg = r["aggressiveness"]
    if "PASSIVE" in agg or "ABOVE" in agg or "BELOW" in agg:
        if side == "BUY" and bb is not None and abs(px - bb) < 1e-9:
            r["queue_position_proxy"] = "JOIN_TOP"
        elif side == "SELL" and ba is not None and abs(px - ba) < 1e-9:
            r["queue_position_proxy"] = "JOIN_TOP"
        else:
            r["queue_position_proxy"] = "DEEP"
    else:
        r["queue_position_proxy"] = ""

    return r


# -------------------- C) FIFO Roundtrip PnL --------------------
# (slug, outcome) -> list of [remaining_size, buy_price, buy_ts_epoch]
_fifo_lots: Dict[Tuple[str, str], list] = {}


def fifo_process_trade(slug: str, outcome: str, side: str,
                       size_f: float, price_f: float, ts_epoch: int) -> dict:
    """Process a trade through FIFO lot matching. Returns realized PnL fields for SELL rows."""
    r = {"realized_pnl_usdc": "", "holding_time_sec_wavg": "", "avg_entry_price_matched": ""}
    if not slug or not outcome or size_f is None or price_f is None:
        return r

    key = (slug, outcome)
    if key not in _fifo_lots:
        _fifo_lots[key] = []
    lots = _fifo_lots[key]

    if side == "BUY":
        lots.append([size_f, price_f, ts_epoch])
        return r  # No realized PnL on buys

    if side != "SELL":
        return r

    # SELL: consume lots FIFO
    remaining = size_f
    total_pnl = 0.0
    weighted_hold_time = 0.0
    total_matched = 0.0
    weighted_entry_px = 0.0

    while remaining > 1e-9 and lots:
        avail = lots[0][0]
        buy_px = lots[0][1]
        buy_ts = lots[0][2]
        matched = min(remaining, avail)

        total_pnl += matched * (price_f - buy_px)
        hold_sec = max(0, ts_epoch - buy_ts)
        weighted_hold_time += matched * hold_sec
        weighted_entry_px += matched * buy_px
        total_matched += matched

        lots[0][0] -= matched
        remaining -= matched
        if lots[0][0] < 1e-9:
            lots.pop(0)

    if total_matched > 0:
        r["realized_pnl_usdc"] = clean_number(total_pnl)
        r["holding_time_sec_wavg"] = clean_number(weighted_hold_time / total_matched)
        r["avg_entry_price_matched"] = clean_number(weighted_entry_px / total_matched)

    return r


# Running realized PnL totals for summary export
_realized_pnl_by_slug: Dict[str, float] = {}  # slug -> cumulative realized pnl
_realized_pnl_total: float = 0.0


def fifo_accumulate_pnl(slug: str, pnl_usdc: float):
    """Accumulate realized PnL for summary reporting."""
    global _realized_pnl_total
    _realized_pnl_total += pnl_usdc
    _realized_pnl_by_slug[slug] = _realized_pnl_by_slug.get(slug, 0.0) + pnl_usdc


# -------------------- D) Trade Clustering / Burstiness --------------------
# token_id -> deque of (t_sec, side_str, usdc_f)
_trade_cluster_history: Dict[str, deque] = {}


def cluster_record_trade(token_id: str, t_sec: int, side: str, usdc_f: float):
    """Record a trade in the clustering history."""
    if not token_id:
        return
    dq = _trade_cluster_history.get(token_id)
    if dq is None:
        dq = deque()
        _trade_cluster_history[token_id] = dq
    dq.append((t_sec, side, usdc_f))
    # Evict entries older than 120s
    cutoff = t_sec - 120
    while dq and dq[0][0] < cutoff:
        dq.popleft()


def compute_cluster_features(token_id: str, t0: int) -> dict:
    """Compute trade clustering features for a trade at time t0."""
    r = {"trades_last_10s": "", "trades_last_60s": "",
         "buys_last_60s": "", "sells_last_60s": "", "usdc_last_60s": ""}
    if not token_id:
        return r
    dq = _trade_cluster_history.get(token_id)
    if not dq:
        r["trades_last_10s"] = "0"
        r["trades_last_60s"] = "0"
        r["buys_last_60s"] = "0"
        r["sells_last_60s"] = "0"
        r["usdc_last_60s"] = "0"
        return r

    t10 = t0 - 10
    t60 = t0 - 60
    cnt10 = 0
    cnt60 = 0
    buys60 = 0
    sells60 = 0
    usdc60 = 0.0
    for ts, sd, usd in dq:
        if ts >= t60 and ts < t0:  # Exclude current trade
            cnt60 += 1
            if sd == "BUY":
                buys60 += 1
            elif sd == "SELL":
                sells60 += 1
            usdc60 += usd
            if ts >= t10:
                cnt10 += 1

    r["trades_last_10s"] = str(cnt10)
    r["trades_last_60s"] = str(cnt60)
    r["buys_last_60s"] = str(buys60)
    r["sells_last_60s"] = str(sells60)
    r["usdc_last_60s"] = clean_number(usdc60)
    return r


# -------------------- E) Market Selection / Ranking --------------------
_market_ranks: Dict[str, dict] = {}  # token_id -> {spread_rank, imbalance_rank}
_last_rank_compute: float = 0.0


def compute_market_ranks():
    """Compute cross-market spread and imbalance ranks for all active tokens."""
    global _last_rank_compute
    now = time.monotonic()
    if now - _last_rank_compute < MARKET_RANK_EVERY_SECONDS:
        return
    _last_rank_compute = now

    if not _active_tokens:
        return

    # Gather latest book data for each active token from book_tape_mem
    token_data: List[Tuple[str, Optional[float], Optional[float]]] = []
    for tid in _active_tokens:
        dq = _book_tape_mem.get(tid)
        if dq and len(dq) > 0:
            latest = dq[-1]
            token_data.append((tid, latest[1], latest[2]))  # spread, imb
        else:
            token_data.append((tid, None, None))

    # Sort by spread (widest first) — rank 1 = widest
    spread_sorted = sorted(
        [(tid, sp) for tid, sp, _ in token_data if sp is not None],
        key=lambda x: x[1], reverse=True)
    for rank, (tid, _) in enumerate(spread_sorted, 1):
        _market_ranks.setdefault(tid, {})["spread_rank"] = rank

    # Sort by imbalance (highest first) — rank 1 = most imbalanced
    imb_sorted = sorted(
        [(tid, im) for tid, _, im in token_data if im is not None],
        key=lambda x: abs(x[1] - 1.0) if x[1] is not None else 0, reverse=True)
    for rank, (tid, _) in enumerate(imb_sorted, 1):
        _market_ranks.setdefault(tid, {})["imbalance_rank"] = rank


def get_market_ranks(token_id: str) -> dict:
    """Get market ranks for a token at trade time."""
    ranks = _market_ranks.get(token_id, {})
    return {
        "spread_rank_at_trade": str(ranks.get("spread_rank", "")) if ranks.get("spread_rank") else "",
        "imbalance_rank_at_trade": str(ranks.get("imbalance_rank", "")) if ranks.get("imbalance_rank") else "",
    }


# -------------------- F) Latency Tracking --------------------
_latency_window: deque = deque(maxlen=600)  # (api_delay_ms, book_age_ms, binance_age_ms)


def latency_record(api_delay_ms: int, book_age_ms: int, binance_age_ms: int):
    """Record a latency observation."""
    _latency_window.append((api_delay_ms, book_age_ms, binance_age_ms))


def compute_latency_fields(ts_int: int, book_tape_ts_epoch: Optional[int],
                           binance_last_epoch: Optional[int]) -> dict:
    """Compute latency fields for a single trade."""
    local_ms = now_ms()
    trade_ms = ts_int * 1000 if ts_int < 1_000_000_000_000 else ts_int
    api_delay = local_ms - trade_ms

    book_age = ""
    if book_tape_ts_epoch is not None and book_tape_ts_epoch > 0:
        book_age = str(local_ms - book_tape_ts_epoch * 1000)

    binance_age = ""
    if binance_last_epoch is not None and binance_last_epoch > 0:
        binance_age = str(local_ms - binance_last_epoch * 1000)

    # Record for rollup
    latency_record(
        int(api_delay),
        int(book_age) if book_age else 0,
        int(binance_age) if binance_age else 0,
    )

    return {
        "local_received_ms": str(local_ms),
        "api_to_local_delay_ms": str(int(api_delay)),
        "book_age_ms_at_trade": str(book_age) if book_age else "",
        "binance_age_ms_at_trade": str(binance_age) if binance_age else "",
    }


def _percentile(vals: List[int], pct: float) -> int:
    """Simple percentile computation."""
    if not vals:
        return 0
    s = sorted(vals)
    idx = int(len(s) * pct / 100.0)
    idx = min(idx, len(s) - 1)
    return s[idx]


def write_latency_rollup():
    """Write LATENCY_ROLLUP event to JSONL (once per minute)."""
    if not _latency_window:
        return
    api_delays = [x[0] for x in _latency_window]
    book_ages = [x[1] for x in _latency_window if x[1] > 0]
    binance_ages = [x[2] for x in _latency_window if x[2] > 0]

    event = {
        "event_type": "LATENCY_ROLLUP",
        "sample_count": len(api_delays),
        "api_to_local_delay_ms_p50": _percentile(api_delays, 50),
        "api_to_local_delay_ms_p90": _percentile(api_delays, 90),
        "api_to_local_delay_ms_p99": _percentile(api_delays, 99),
        "book_age_ms_p50": _percentile(book_ages, 50) if book_ages else "",
        "book_age_ms_p90": _percentile(book_ages, 90) if book_ages else "",
        "book_age_ms_p99": _percentile(book_ages, 99) if book_ages else "",
        "binance_age_ms_p50": _percentile(binance_ages, 50) if binance_ages else "",
        "binance_age_ms_p90": _percentile(binance_ages, 90) if binance_ages else "",
        "binance_age_ms_p99": _percentile(binance_ages, 99) if binance_ages else "",
    }
    write_jsonl(event)
    print(f"  [LATENCY] api_delay p50={event['api_to_local_delay_ms_p50']}ms "
          f"p90={event['api_to_local_delay_ms_p90']}ms "
          f"p99={event['api_to_local_delay_ms_p99']}ms "
          f"(n={event['sample_count']})")


# -------------------- Realized PnL Summary Export --------------------
def write_realized_pnl_summary(path: str):
    """Export periodic realized PnL summary CSV."""
    header = ["ts_iso", "window_sec", "realized_pnl_total",
              "realized_pnl_by_symbol", "realized_pnl_by_slug_top5"]
    now_iso = ts_to_iso(epoch_sec())

    # Group by crypto symbol (from slug detection)
    pnl_by_symbol: Dict[str, float] = {}
    for slug_key, pnl_val in _realized_pnl_by_slug.items():
        sym = detect_crypto(slug_key)
        if sym:
            pnl_by_symbol[sym] = pnl_by_symbol.get(sym, 0.0) + pnl_val

    # Top 5 slugs by absolute PnL
    top5 = sorted(_realized_pnl_by_slug.items(), key=lambda x: abs(x[1]), reverse=True)[:5]
    top5_str = ";".join(f"{s}={clean_number(p)}" for s, p in top5)
    symbol_str = ";".join(f"{s}={clean_number(p)}" for s, p in sorted(pnl_by_symbol.items()))

    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerow([now_iso, "cumulative", clean_number(_realized_pnl_total),
                     symbol_str, top5_str])
    os.replace(tmp, path)


# -------------------- main --------------------
def main():
    conn = db_connect(DB_PATH)
    db_init(conn)
    preload_token_cache(conn)

    start_ts = epoch_sec()
    last_seen_ts = start_ts
    seen_tx: set = set()

    # Lifetime counters
    total_trades = 0
    total_usdc = 0.0

    raw_header = [
        "timestamp_iso", "timestamp_epoch", "slug", "title", "outcome", "side",
        "size", "price", "usdcSize", "txHash", "clob_token_id", "erc1155_token_id", "order_id", "match_id",
        "crypto", "binanceSpot", "hour_ms", "hourOpen", "hourClose",
        "bestBid", "bestAsk", "spread", "mid", "microprice",
        "imbalance_topN", "bid_depth_topN", "ask_depth_topN",
        "bid_depth_total", "ask_depth_total",
        "bidsJson", "asksJson", "bookTs", "bookHash",
        "crossing_estimate", "trade_vs_mid", "trade_vs_micro", "book_suspect",
        "spread_percentile_60s", "spread_mean_60s", "spread_std_60s",
        "net_shares_after", "avg_cost_after", "realized_pnl_cumulative",
        "trade_id", "market_id", "event_id", "outcome_id", "taker_maker", "fee", "fee_rate_bps", "trade_status",
        "inv_net_shares_before", "inv_avg_cost_before", "time_since_last_trade",
        # === MAX FORENSICS v2 columns (appended right) ===
        # A) Velocity
        "spread_prev_1s", "spread_prev_5s_avg", "spread_change_1s", "spread_change_5s",
        "imb_prev_1s", "imb_prev_5s_avg", "imb_change_1s", "imb_change_5s",
        "mid_prev_1s", "mid_prev_5s_avg", "mid_change_1s", "mid_change_5s", "mid_change_10s",
        "micro_prev_1s", "micro_change_1s",
        "time_since_spread_p90",
        "binance_prev_1s", "binance_change_1s", "binance_prev_5s", "binance_change_5s",
        "binance_prev_10s", "binance_change_10s",
        # B) Execution style
        "aggressiveness", "edge_to_micro", "edge_to_mid", "spread_paid_est", "queue_position_proxy",
        # C) Roundtrip PnL
        "realized_pnl_usdc", "holding_time_sec_wavg", "avg_entry_price_matched",
        # D) Trade clustering
        "trades_last_10s", "trades_last_60s", "buys_last_60s", "sells_last_60s", "usdc_last_60s",
        # E) Market selection
        "spread_rank_at_trade", "imbalance_rank_at_trade", "spread_percentile_60s_at_trade",
        # E2) Raw imbalance + Binance momentum
        "orderbook_imbalance_at_trade",
        "binance_ret_5s_at_trade", "binance_ret_30s_at_trade", "binance_ret_60s_at_trade",
        # F) Latency
        "local_received_ms", "api_to_local_delay_ms", "book_age_ms_at_trade", "binance_age_ms_at_trade",
    ]
    init_csv(RAW_CSV, raw_header)

    mark_header = [
        "markout_id", "txHash", "clob_token_id", "t0_epoch", "horizon_sec", "due_epoch",
        "mid_t0", "spread_t0", "binance_t0", "mid_t1", "spread_t1", "binance_t1",
        "trade_price", "side", "markout_pnl_per_share",
    ]
    init_csv(MARKOUTS_CSV, mark_header)
    init_csv(BOOK_TAPE_CSV, _BOOK_TAPE_HEADER)
    init_csv(BINANCE_TAPE_CSV, _BINANCE_TAPE_HEADER)

    write_jsonl({"event_type": "TRACKER_START", "wallet": WALLET,
                 "start_ts": start_ts, "start_iso": ts_to_iso(start_ts)})

    print(f"[F247 COPYWALLET TRACKER]")
    print(f"  Start: {ts_to_iso(start_ts)} (epoch={start_ts})")
    print(f"  Wallet: {WALLET}")
    print(f"  Log dir: {LOG_DIR}")
    print(f"  DB: {DB_PATH}")
    print(f"  RAW CSV: {RAW_CSV}")
    print(f"  JSONL: {JSONL_LOG}")
    print(f"  Book tape: {BOOK_TAPE_CSV}")
    print(f"  Binance tape: {BINANCE_TAPE_CSV}")
    print(f"  Realized PnL summary: {REALIZED_PNL_SUMMARY_CSV}")
    print()

    # Log execution enrichment + book tape features
    write_jsonl({"event_type": "FEATURE_ENABLED",
                 "feature": "execution_enrichment",
                 "description": "3-tier taker/maker+fee: activity->CLOB->book_inferred",
                 "taker_fee_bps": TAKER_FEE_BPS,
                 "maker_fee_bps": MAKER_FEE_BPS})
    write_jsonl({"event_type": "FEATURE_ENABLED",
                 "feature": "book_tape",
                 "description": "1Hz continuous orderbook recording for active tokens",
                 "max_tokens": BOOK_TAPE_MAX_TOKENS,
                 "csv_path": BOOK_TAPE_CSV})
    write_jsonl({"event_type": "FEATURE_ENABLED",
                 "feature": "exit_modeling",
                 "description": "time_to_next_sell, realized_pnl_roundtrip, inventory_action in entry_model.csv"})
    write_jsonl({"event_type": "FEATURE_ENABLED",
                 "feature": "max_forensics_v2",
                 "description": "velocity+aggressiveness+fifo_pnl+clustering+ranking+latency",
                 "new_columns": 42,
                 "new_files": ["binance_tape.csv", "realized_pnl_summary.csv"]})
    print("  EXECUTION_ENRICHMENT=True (activity->CLOB->book_inferred)")
    print("  BOOK_TAPE=True (1Hz, max_tokens=%d)" % BOOK_TAPE_MAX_TOKENS)
    print("  EXIT_MODELING=True (entry_model.csv: time_to_sell, roundtrip_pnl, inv_action)")
    print("  MAX_FORENSICS_v2=True (velocity+aggressiveness+fifo+clustering+ranking+latency)")

    # Log orderbook sorting fix
    write_jsonl({"event_type": "ORDERBOOK_SORTING_FIX_ENABLED", "value": True,
                 "description": "bids sorted DESC, asks sorted ASC before computing best/spread/depth"})
    print("  ORDERBOOK_SORTING_FIX_ENABLED=True")

    # Probe Binance connectivity
    btc_probe = binance_spot_price("BTC")
    print(f"  Binance probe: BTC/USDT={btc_probe or 'N/A'}")

    # Startup orderbook sanity check: probe up to 3 cached token_ids
    # Cross-reference against the most recent trade price for each.
    # Uses /price fallback when /book is suspect.
    # NOTE: last_px comes from the trades table — if the cached token belongs
    # to a previous hour's (now-resolved) market, last_px will be stale and
    # the orderbook may be empty.  We detect this via trade age and warn.
    _sanity_items = list(_token_cache.items())[:6]  # (slug,outcome)->tid
    _sanity_done = set()
    _now_epoch = epoch_sec()
    if _sanity_items:
        print("  Orderbook sanity check (book vs last trade price):")
        for (_s_slug, _s_out), _st_tid in _sanity_items:
            if _st_tid in _sanity_done or len(_sanity_done) >= 3:
                continue
            _sanity_done.add(_st_tid)
            _st_snap = fetch_orderbook(_st_tid)
            # Get last known trade price AND its timestamp for this token from DB
            _last_px_row = conn.execute(
                "SELECT price, timestamp_epoch FROM trades "
                "WHERE clob_token_id = ? ORDER BY timestamp_epoch DESC LIMIT 1",
                (_st_tid,)).fetchone()
            _last_px = _last_px_row[0] if _last_px_row else None
            _last_px_epoch = int(_last_px_row[1]) if (_last_px_row and _last_px_row[1]) else 0
            _trade_age_sec = int(_now_epoch - _last_px_epoch) if _last_px_epoch else -1
            _stale_tag = ""
            if _trade_age_sec > 3600:
                _stale_tag = f" [STALE trade_age={_trade_age_sec}s]"
            # Also fetch live midprice for comparison regardless of suspect flag
            _live_mid = None
            _live_pp = fetch_clob_midprice(_st_tid)
            if _live_pp:
                _live_bp, _live_sp = _live_pp
                _live_bpf, _live_spf = ffloat(_live_bp), ffloat(_live_sp)
                if _live_bpf is not None and _live_spf is not None:
                    _live_mid = (_live_bpf + _live_spf) / 2.0
            _fallback_used = False
            if _st_snap and _st_snap.suspect:
                # Try /price fallback
                if _live_pp:
                    _bpf, _spf = ffloat(_live_pp[0]), ffloat(_live_pp[1])
                    if _bpf is not None and _spf is not None and _bpf > 0.02 and _spf < 0.98:
                        _st_snap = BookSnap(
                            best_bid=clean_number(_spf), best_ask=clean_number(_bpf),
                            spread=clean_number(_bpf - _spf),
                            mid=clean_number((_bpf + _spf) / 2.0), suspect="")
                        _fallback_used = True
            if _st_snap:
                _bb_s = ffloat(_st_snap.best_bid)
                _ba_s = ffloat(_st_snap.best_ask)
                _px_s = ffloat(_last_px)
                _verdict = "ok"
                if _px_s is not None and _bb_s is not None and _ba_s is not None:
                    _dist_s = min(abs(_px_s - _bb_s), abs(_px_s - _ba_s))
                    if _dist_s > 0.05:
                        _verdict = f"MISMATCH(dist={_dist_s:.3f})"
                        if _trade_age_sec > 3600:
                            _verdict += " (likely stale token from previous hour)"
                _src_tag = " [/price fallback]" if _fallback_used else ""
                _mid_tag = f" live_mid={clean_number(_live_mid)}" if _live_mid else ""
                print(f"    {_s_slug}:{_s_out} tid={_st_tid[:16]}... "
                      f"bb={_st_snap.best_bid} ba={_st_snap.best_ask} spr={_st_snap.spread} "
                      f"last_px={_last_px or 'N/A'}{_mid_tag} "
                      f"trade_age={_trade_age_sec}s -> {_verdict}{_src_tag}{_stale_tag}")
                write_jsonl({"event_type": "ORDERBOOK_SANITY_CHECK",
                             "slug": _s_slug, "outcome": _s_out,
                             "token_id": _st_tid, "best_bid": _st_snap.best_bid,
                             "best_ask": _st_snap.best_ask, "spread": _st_snap.spread,
                             "last_trade_price": _last_px or "",
                             "trade_age_sec": _trade_age_sec,
                             "live_midprice": clean_number(_live_mid) if _live_mid else "",
                             "suspect": _st_snap.suspect, "verdict": _verdict,
                             "price_fallback": _fallback_used,
                             "stale_token": _trade_age_sec > 3600})
            else:
                print(f"    {_s_slug}:{_s_out} tid={_st_tid[:16]}... FAILED (no data)"
                      f"{_stale_tag}")
    else:
        print("  Orderbook sanity check: no cached tokens yet, will verify on first trade")
    print("  Ctrl+C to stop.\n")

    last_backfill = 0.0
    last_export = 0.0
    last_roll = 0.0
    last_fp = 0.0
    last_inv = 0.0
    last_mk_stats = 0.0
    last_behavior = 0.0
    last_coverage = 0.0
    last_entry_model = 0.0
    last_latency_rollup = 0.0
    last_realized_pnl_summary = 0.0
    _activity_fields_warned = False

    while not STOP:
        try:
            new_trades, clipped = fetch_new_trades_since(WALLET, start_ts, last_seen_ts)
            if clipped:
                print("[warn] pagination clipped — lower POLL_SECONDS or increase LIMIT")

            for t in new_trades:
                raw_ts = t.get("timestamp")
                if raw_ts is None:
                    continue
                ts_int = int(raw_ts)
                if ts_int < start_ts:
                    continue
                tx = safe_get(t, ["transactionHash", "txHash", "hash"])
                if not tx or tx in seen_tx:
                    continue

                iso = ts_to_iso(ts_int)
                slug = safe_get(t, ["slug", "eventSlug"]) or ""
                title = safe_get(t, ["title", "eventTitle"]) or ""
                outcome = safe_get(t, ["outcome"]) or ""
                side = (safe_get(t, ["side"]) or "").upper()
                size = safe_get(t, ["size"]) or 0
                price = safe_get(t, ["price"]) or 0
                usdc_size = safe_get(t, ["usdcSize", "usdc_size"])
                if usdc_size is None:
                    try:
                        usdc_size = float(size) * float(price)
                    except Exception:
                        usdc_size = 0
                # Activity API returns the CLOB token ID as the "asset" field.
                # This IS the same value as the clobTokenId from Gamma.
                # We extract it here for cross-validation against Gamma resolution.
                asset_token_id = safe_get(t, ["asset", "asset_id", "assetId",
                                              "token_id", "tokenId"]) or ""
                erc1155_token_id = str(asset_token_id)
                order_id = safe_get(t, ["order_id", "orderId"])
                match_id = safe_get(t, ["match_id", "matchId", "tradeId", "trade_id"])
                condition_id = safe_get(t, ["conditionId", "condition_id"]) or ""

                # Best-effort extra fields from the activity payload
                trade_id = safe_get(t, ["id", "trade_id", "tradeId"]) or ""
                market_id = safe_get(t, ["market", "marketId", "market_id", "conditionId"]) or ""
                event_id = safe_get(t, ["event", "eventId", "event_id"]) or ""
                outcome_id = safe_get(t, ["outcomeIndex", "outcome_id", "outcomeId"]) or ""
                taker_maker = safe_get(t, ["type", "maker_address", "taker", "maker"]) or ""
                fee_val = safe_get(t, ["fee", "feeAmount", "fees"]) or ""
                fee_rate_bps_val = safe_get(t, ["feeRateBps", "fee_rate_bps", "feeRate"]) or ""
                # Compute fee_rate_bps from fee/usdcSize if not provided directly
                if not fee_rate_bps_val:
                    _fee_f = ffloat(fee_val)
                    _usdc_f = ffloat(usdc_size)
                    if _fee_f is not None and _usdc_f is not None and _usdc_f > 0:
                        fee_rate_bps_val = clean_number(_fee_f / _usdc_f * 10000)
                trade_status = safe_get(t, ["status", "settlement", "settled"]) or ""

                # Resolve CLOB token_id for this trade.
                # Priority: activity "asset" field (ground truth from the trade) > Gamma.
                # The Activity API "asset" field IS the clobTokenId.
                gamma_token_id = None
                if slug and outcome:
                    gamma_token_id = resolve_token_id(conn, slug, outcome)

                token_id = None
                token_source = ""
                if asset_token_id and len(str(asset_token_id)) > 10:
                    # Activity API gave us the clobTokenId directly
                    token_id = str(asset_token_id)
                    token_source = "activity_asset"
                    # Cross-validate against Gamma
                    if gamma_token_id and gamma_token_id != token_id:
                        write_jsonl({
                            "event_type": "TOKEN_CROSS_VALIDATION_MISMATCH",
                            "slug": slug, "outcome": outcome,
                            "asset_token": token_id,
                            "gamma_token": gamma_token_id,
                            "using": "activity_asset",
                            "txHash": tx,
                        })
                        # Also seed Gamma cache with the activity-sourced token
                        # so future trades for this (slug, outcome) use it
                        _token_cache[(slug, outcome)] = token_id
                        try:
                            conn.execute(
                                "INSERT OR REPLACE INTO slug_tokens "
                                "(slug, outcome, token_id, updated_epoch) "
                                "VALUES (?, ?, ?, ?)",
                                (slug, outcome, token_id, epoch_sec()))
                            conn.commit()
                        except Exception:
                            pass
                elif gamma_token_id:
                    token_id = gamma_token_id
                    token_source = "gamma"

                if not token_id:
                    write_jsonl({"event_type": "TOKEN_RESOLUTION_FAILED",
                                 "slug": slug, "outcome": outcome,
                                 "title": title, "txHash": tx,
                                 "asset_token_id": str(asset_token_id),
                                 "condition_id": condition_id,
                                 "source_field_used": "slug+outcome",
                                 "reason": "unresolved_after_all_tiers"})

                crypto = detect_crypto(slug, title)
                spot = ""
                hour_open = ""
                hour_ms_val = ""
                if crypto:
                    hour_ms_int = ts_to_hour_ms(ts_int)
                    hour_ms_val = str(hour_ms_int)
                    spot = binance_spot_price(crypto)
                    hour_open = binance_hour_open(crypto, hour_ms_int)

                snap = fetch_orderbook(str(token_id)) if token_id else None
                if snap is None:
                    snap = BookSnap()
                    if token_id:
                        write_jsonl({"event_type": "BOOK_EMPTY",
                                     "token_id": str(token_id),
                                     "slug": slug, "outcome": outcome,
                                     "txHash": tx})

                # ---- /price fallback when /book is suspect (known Polymarket issue) ----
                # The /book endpoint sometimes returns stale snapshots with bb=0.01/ba=0.99
                # while /price returns the correct live price.
                if snap.suspect and token_id:
                    price_pair = fetch_clob_midprice(str(token_id))
                    if price_pair:
                        _buy_px, _sell_px = price_pair
                        _bp = ffloat(_buy_px)
                        _sp = ffloat(_sell_px)
                        if _bp is not None and _sp is not None and _bp > 0.02 and _sp < 0.98:
                            # /price gave us sane data — synthesize a BookSnap
                            _fbb = _sp  # sell price = best bid
                            _fba = _bp  # buy price = best ask
                            _fspread = clean_number(_fba - _fbb)
                            _fmid = clean_number((_fba + _fbb) / 2.0)
                            snap = BookSnap(
                                best_bid=clean_number(_fbb),
                                best_ask=clean_number(_fba),
                                spread=_fspread,
                                mid=_fmid,
                                microprice=_fmid,
                                suspect="",
                            )
                            write_jsonl({
                                "event_type": "BOOK_PRICE_FALLBACK_USED",
                                "token_id": str(token_id),
                                "slug": slug, "outcome": outcome,
                                "buy_price": _buy_px, "sell_price": _sell_px,
                                "synth_bb": snap.best_bid,
                                "synth_ba": snap.best_ask,
                                "txHash": tx,
                            })

                # ---- BOOK_ID_MISMATCH: verify the book is plausibly for this trade ----
                _px_check = ffloat(price)
                _bb_check = ffloat(snap.best_bid)
                _ba_check = ffloat(snap.best_ask)
                if (_px_check is not None and _bb_check is not None and _ba_check is not None
                        and not snap.suspect):
                    _dist = min(abs(_px_check - _bb_check), abs(_px_check - _ba_check))
                    if _dist > 0.05:
                        # Check if the complementary outcome (1-price) would be closer.
                        # In a binary market, if we accidentally have the "No" token book
                        # for a "Yes" trade, best_bid ≈ 1-trade_price.
                        _compl_dist = min(
                            abs(_px_check - (1.0 - _bb_check)),
                            abs(_px_check - (1.0 - _ba_check)))
                        _compl_flag = _compl_dist < 0.05
                        write_jsonl({
                            "event_type": "BOOK_ID_MISMATCH",
                            "slug": slug, "outcome": outcome,
                            "trade_price": clean_number(price),
                            "bestBid": snap.best_bid, "bestAsk": snap.best_ask,
                            "mid": snap.mid,
                            "token_id_used": str(token_id),
                            "token_source": token_source,
                            "asset_token_id": str(asset_token_id),
                            "gamma_token_id": str(gamma_token_id) if gamma_token_id else "",
                            "condition_id": condition_id,
                            "txHash": tx,
                            "distance": clean_number(_dist),
                            "complementary_match": _compl_flag,
                            "complementary_distance": clean_number(_compl_dist),
                        })
                        print(f"  !! BOOK_ID_MISMATCH slug={slug} out={outcome} "
                              f"px={clean_number(price)} bb={snap.best_bid} ba={snap.best_ask} "
                              f"dist={_dist:.3f} compl={'YES' if _compl_flag else 'no'} "
                              f"src={token_source} tid={str(token_id)[:16]}...")

                # Derived diagnostics
                crossing = ""
                trade_vs_mid = ""
                trade_vs_micro = ""
                px = ffloat(price)
                midf = ffloat(snap.mid)
                microf = ffloat(snap.microprice)
                bbf = ffloat(snap.best_bid)
                baf = ffloat(snap.best_ask)
                if px is not None and bbf is not None and baf is not None:
                    if side == "BUY":
                        crossing = "CROSS" if px >= baf else "PASSIVE"
                    elif side == "SELL":
                        crossing = "CROSS" if px <= bbf else "PASSIVE"
                if px is not None and midf is not None:
                    trade_vs_mid = clean_number(px - midf)
                if px is not None and microf is not None:
                    trade_vs_micro = clean_number(px - microf)

                # ---- Execution enrichment: taker/maker + fee fallback ----
                enrich_source = "activity" if taker_maker else ""
                if not taker_maker and condition_id:
                    clob_info = fetch_clob_trade_info(
                        condition_id, tx_hash=tx,
                        match_id=str(match_id or ""))
                    if clob_info and clob_info.get("taker_maker"):
                        taker_maker = clob_info["taker_maker"]
                        if not fee_val:
                            fee_val = clob_info.get("fee", "")
                        if not fee_rate_bps_val:
                            fee_rate_bps_val = clob_info.get(
                                "fee_rate_bps", "")
                        enrich_source = "clob_live_activity"
                if not taker_maker:
                    _usdc_enrich = ffloat(usdc_size)
                    if (px is not None and bbf is not None
                            and baf is not None):
                        inferred = infer_execution_fields(
                            side, px, bbf, baf,
                            _usdc_enrich or 0.0)
                        taker_maker = inferred["taker_maker"]
                        if not fee_val:
                            fee_val = inferred["fee"]
                        if not fee_rate_bps_val:
                            fee_rate_bps_val = inferred["fee_rate_bps"]
                        enrich_source = "book_inferred"
                write_jsonl({
                    "event_type": "EXECUTION_ENRICH_RESULT",
                    "txHash": tx,
                    "taker_maker": str(taker_maker),
                    "fee": str(fee_val),
                    "fee_rate_bps": str(fee_rate_bps_val),
                    "source": enrich_source,
                })

                # Layer 1: Spread percentile tracking (skip suspect books)
                spread_stats = {"spread_percentile_60s": "", "spread_mean_60s": "", "spread_std_60s": ""}
                spread_f = ffloat(snap.spread)
                if spread_f is not None and token_id and not snap.suspect:
                    spread_tracker_record(str(token_id), ts_int, spread_f)
                    spread_stats = spread_tracker_stats(str(token_id), spread_f)

                # Capture pre-trade state for wallet_entry_model
                inv_before = inventory_get_before(slug, outcome) if slug and outcome else {
                    "inv_net_shares_before": "", "inv_avg_cost_before": ""}
                time_since_last = get_time_since_last_trade(
                    str(token_id), ts_int) if token_id else ""

                # Layer 2: Inventory tracking (mutates state — must be after inv_before)
                inv_fields = {"net_shares_after": "", "avg_cost_after": "", "realized_pnl_cumulative": ""}
                size_f = ffloat(size)
                price_f = ffloat(price)
                if slug and outcome and size_f is not None and price_f is not None and side in ("BUY", "SELL"):
                    inv_fields = inventory_update(slug, outcome, side, size_f, price_f)

                # Record trade timestamp (must be after time_since_last capture)
                if token_id:
                    record_trade_ts(str(token_id), ts_int)
                    # Register for book tape continuous recording
                    register_active_token(str(token_id), slug, outcome, ts_int)

                # Register crypto for Binance tape
                if crypto:
                    register_active_crypto(crypto)

                # === MAX FORENSICS v2: compute all new features ===
                # A) Velocity features
                v2_velocity = compute_velocity_features(
                    str(token_id) if token_id else "",
                    ts_int,
                    ffloat(snap.spread), ffloat(snap.imbalance_topN),
                    midf, microf, crypto)

                # B) Aggressiveness features
                v2_aggr = compute_aggressiveness(side, px, bbf, baf, midf, microf)

                # C) FIFO roundtrip PnL
                v2_fifo = {"realized_pnl_usdc": "", "holding_time_sec_wavg": "", "avg_entry_price_matched": ""}
                if slug and outcome and size_f is not None and price_f is not None and side in ("BUY", "SELL"):
                    v2_fifo = fifo_process_trade(slug, outcome, side, size_f, price_f, ts_int)
                    # Accumulate for summary
                    _fifo_pnl_f = ffloat(v2_fifo.get("realized_pnl_usdc", ""))
                    if _fifo_pnl_f is not None and _fifo_pnl_f != 0:
                        fifo_accumulate_pnl(slug, _fifo_pnl_f)

                # D) Trade clustering (compute BEFORE recording current trade)
                _usdc_f_cluster = ffloat(usdc_size) or 0.0
                v2_cluster = compute_cluster_features(str(token_id) if token_id else "", ts_int)
                # Now record this trade for future clustering
                if token_id:
                    cluster_record_trade(str(token_id), ts_int, side, _usdc_f_cluster)

                # E) Market selection ranks
                v2_ranks = get_market_ranks(str(token_id) if token_id else "")
                v2_ranks["spread_percentile_60s_at_trade"] = spread_stats.get("spread_percentile_60s", "")

                # E2) Raw orderbook imbalance + Binance momentum at trade time
                _bid_depth_f = ffloat(snap.bid_depth_topN)
                _ask_depth_f = ffloat(snap.ask_depth_topN)
                v2_ranks["orderbook_imbalance_at_trade"] = compute_orderbook_imbalance_at_trade(
                    str(token_id) if token_id else "", ts_int, _bid_depth_f, _ask_depth_f)
                v2_binance_mom = compute_binance_momentum_at_trade(crypto, ts_int)
                v2_ranks.update(v2_binance_mom)

                # F) Latency
                # Book tape ts: latest tape entry for this token
                _bt_ts = None
                _bt_dq = _book_tape_mem.get(str(token_id)) if token_id else None
                if _bt_dq and len(_bt_dq) > 0:
                    _bt_ts = _bt_dq[-1][0]
                # Binance tape ts: latest for this crypto
                _bn_ts = None
                if crypto:
                    _bn_dq = _binance_tape_mem.get(crypto)
                    if _bn_dq and len(_bn_dq) > 0:
                        _bn_ts = _bn_dq[-1][0]
                v2_latency = compute_latency_fields(ts_int, _bt_ts, _bn_ts)

                row = {
                    "timestamp_iso": iso,
                    "timestamp_epoch": str(ts_int),
                    "slug": slug,
                    "title": title,
                    "outcome": outcome,
                    "side": side,
                    "size": clean_number(size),
                    "price": clean_number(price),
                    "usdcSize": clean_number(usdc_size),
                    "txHash": tx,
                    "clob_token_id": (str(token_id) if token_id is not None else ""),
                    "erc1155_token_id": str(erc1155_token_id),
                    "order_id": (str(order_id) if order_id is not None else ""),
                    "match_id": (str(match_id) if match_id is not None else ""),
                    "crypto": crypto,
                    "binanceSpot": spot,
                    "hour_ms": hour_ms_val,
                    "hourOpen": hour_open,
                    "hourClose": "",
                    "bestBid": snap.best_bid,
                    "bestAsk": snap.best_ask,
                    "spread": snap.spread,
                    "mid": snap.mid,
                    "microprice": snap.microprice,
                    "imbalance_topN": snap.imbalance_topN,
                    "bid_depth_topN": snap.bid_depth_topN,
                    "ask_depth_topN": snap.ask_depth_topN,
                    "bid_depth_total": snap.bid_depth_total,
                    "ask_depth_total": snap.ask_depth_total,
                    "bidsJson": snap.bids_json,
                    "asksJson": snap.asks_json,
                    "bookTs": snap.book_ts,
                    "bookHash": snap.book_hash,
                    "crossing_estimate": crossing,
                    "trade_vs_mid": trade_vs_mid,
                    "trade_vs_micro": trade_vs_micro,
                    "book_suspect": snap.suspect,
                    "spread_percentile_60s": spread_stats["spread_percentile_60s"],
                    "spread_mean_60s": spread_stats["spread_mean_60s"],
                    "spread_std_60s": spread_stats["spread_std_60s"],
                    "net_shares_after": inv_fields["net_shares_after"],
                    "avg_cost_after": inv_fields["avg_cost_after"],
                    "realized_pnl_cumulative": inv_fields["realized_pnl_cumulative"],
                    "trade_id": str(trade_id),
                    "market_id": str(market_id),
                    "event_id": str(event_id),
                    "outcome_id": str(outcome_id),
                    "taker_maker": str(taker_maker),
                    "fee": str(fee_val),
                    "fee_rate_bps": str(fee_rate_bps_val),
                    "trade_status": str(trade_status),
                    "inv_net_shares_before": inv_before["inv_net_shares_before"],
                    "inv_avg_cost_before": inv_before["inv_avg_cost_before"],
                    "time_since_last_trade": str(time_since_last),
                    # === v2 columns ===
                    **v2_velocity,
                    **v2_aggr,
                    **v2_fifo,
                    **v2_cluster,
                    **v2_ranks,
                    **v2_latency,
                }

                inserted = db_insert_trade(conn, row)
                if inserted:
                    append_row_csv(RAW_CSV, raw_header, row)
                    seen_tx.add(tx)
                    last_seen_ts = max(last_seen_ts, ts_int)
                    total_trades += 1
                    try:
                        total_usdc += float(usdc_size)
                    except Exception:
                        pass

                    # Bridge fill to bot logger via callback
                    if ON_FILL_CB is not None:
                        try:
                            ON_FILL_CB({
                                "tx_hash": tx,
                                "ts_epoch": ts_int,
                                "ts_iso": iso,
                                "slug": slug,
                                "asset": crypto,
                                "outcome": outcome,
                                "side": side,
                                "price": float(price),
                                "qty_shares": float(size),
                                "notional_usd": float(usdc_size),
                                "token_id": str(token_id) if token_id else "",
                            })
                        except Exception:
                            pass  # never crash tracker for callback errors

                    # DATA_COVERAGE counters
                    coverage_increment("trades")
                    if token_id:
                        coverage_increment("clob_resolved")
                    if snap.best_bid and not snap.suspect:
                        coverage_increment("book_ok")

                    # Schedule markouts for every trade with a resolved token_id
                    if token_id:
                        db_add_markout_jobs(
                            conn=conn, txHash=tx, token_id=str(token_id),
                            t0_epoch=ts_int, mid_t0=snap.mid,
                            spread_t0=snap.spread, binance_t0=spot,
                            trade_price=clean_number(price), side=side,
                        )
                        coverage_increment("markouts_created")
                        write_jsonl({"event_type": "MARKOUT_JOB_CREATED",
                                     "txHash": tx,
                                     "clob_token_id": str(token_id),
                                     "horizons": MARKOUT_HORIZONS_SEC,
                                     "mid_t0": snap.mid,
                                     "spread_t0": snap.spread,
                                     "book_suspect": snap.suspect})
                        for h in MARKOUT_HORIZONS_SEC:
                            due = ts_int + h
                            markout_id = sha1_short(f"{tx}:{h}")
                            append_row_csv(MARKOUTS_CSV, mark_header, {
                                "markout_id": markout_id, "txHash": tx,
                                "clob_token_id": str(token_id),
                                "t0_epoch": str(ts_int), "horizon_sec": str(h),
                                "due_epoch": str(due), "mid_t0": snap.mid,
                                "spread_t0": snap.spread, "binance_t0": spot,
                                "mid_t1": "", "spread_t1": "", "binance_t1": "",
                                "trade_price": clean_number(price), "side": side,
                                "markout_pnl_per_share": "",
                            })

                    # JSONL event (enriched + raw Activity fields for debugging)
                    write_jsonl({
                        "event_type": "F247_TRADE",
                        "slug": slug, "crypto": crypto, "outcome": outcome,
                        "side": side, "size": clean_number(size),
                        "price": clean_number(price),
                        "usdc": clean_number(usdc_size),
                        "txHash": tx,
                        "clob_token_id": (str(token_id) if token_id else ""),
                        "token_source": token_source,
                        "asset_token_id": str(asset_token_id),
                        "condition_id": condition_id,
                        "crossing": crossing,
                        "spread": snap.spread, "mid": snap.mid,
                        "book_suspect": snap.suspect,
                        "binance_spot": spot, "hour_open": hour_open,
                        "spread_pct_60s": spread_stats["spread_percentile_60s"],
                        "net_shares": inv_fields["net_shares_after"],
                        "avg_cost": inv_fields["avg_cost_after"],
                        "realized_pnl": inv_fields["realized_pnl_cumulative"],
                        "raw_activity": {
                            k: v for k, v in t.items()
                            if k in ("asset", "token_id", "tokenId", "asset_id", "assetId",
                                     "conditionId", "condition_id",
                                     "marketSlug", "slug", "outcome",
                                     "side", "size", "price", "usdcSize",
                                     "transactionHash", "timestamp",
                                     "id", "trade_id", "tradeId",
                                     "market", "marketId", "market_id",
                                     "event", "eventId", "event_id", "eventSlug",
                                     "outcomeIndex", "outcome_id", "outcomeId",
                                     "type", "maker_address", "taker", "maker",
                                     "fee", "feeAmount", "fees", "feeRateBps",
                                     "status", "settlement", "settled",
                                     "proxyWallet", "title")
                        },
                    })

                    # One-time: dump all activity keys + warn about missing fields
                    if not _activity_fields_warned:
                        _activity_fields_warned = True
                        # Always log available keys for forensics
                        _all_keys = sorted(t.keys())
                        write_jsonl({"event_type": "ACTIVITY_SCHEMA_DUMP",
                                     "available_keys": _all_keys,
                                     "sample_txHash": tx})
                        print(f"  [info] Activity API keys: {_all_keys}")
                        _want = {"id": "trade_id", "asset": "asset_token",
                                 "conditionId": "condition_id",
                                 "fee": "fee", "feeRateBps": "fee_rate_bps",
                                 "outcomeIndex": "outcome_id",
                                 "status": "trade_status"}
                        _missing = [v for k, v in _want.items() if k not in t]
                        if _missing:
                            write_jsonl({"event_type": "ACTIVITY_MISSING_FIELDS",
                                         "missing": _missing,
                                         "available_keys": _all_keys})
                            print(f"  [warn] Activity API missing fields: {_missing}")

                    # Console
                    ob_tag = ""
                    if row["bestBid"] or row["bestAsk"]:
                        ob_tag = (f" book(bb={row['bestBid']} ba={row['bestAsk']} "
                                  f"spr={row['spread']} micro={row['microprice']} "
                                  f"imb={row['imbalance_topN']})")
                    bn_tag = f" [{crypto} spot={spot} O={hour_open}]" if crypto else ""
                    inv_tag = f" inv={inv_fields['net_shares_after']}" if inv_fields["net_shares_after"] else ""
                    spr_pct_tag = f" sprP={spread_stats['spread_percentile_60s']}%" if spread_stats["spread_percentile_60s"] else ""
                    suspect_tag = " !!SUSPECT_BOOK!!" if snap.suspect else ""
                    print(
                        f"  F247{bn_tag}{ob_tag}: {iso} {slug} {outcome} {side} "
                        f"sz={row['size']} px={row['price']} usdc={row['usdcSize']} "
                        f"{crossing} dMid={trade_vs_mid} dMicro={trade_vs_micro}"
                        f"{spr_pct_tag}{inv_tag}{suspect_tag}"
                    )

            # ---- markout processing ----
            now_epoch = epoch_sec()
            due_jobs = db_fetch_due_markouts(conn, now_epoch, MARKOUT_MAX_PER_TICK)
            for (markout_id, txHash, tok_mk, t0_epoch, horizon_sec,
                 due_epoch, mid_t0, spread_t0, bin_t0,
                 mk_trade_price, mk_side) in due_jobs:
                # Abandon markouts that are too old (stuck retrying)
                age = now_epoch - int(due_epoch)
                if age > MARKOUT_MAX_AGE_SEC:
                    write_jsonl({"event_type": "MARKOUT_ABANDONED",
                                 "markout_id": markout_id, "txHash": txHash,
                                 "horizon_sec": horizon_sec, "age_sec": age})
                    db_set_markout_done(conn, markout_id, "", "", "")
                    continue
                snap1 = fetch_orderbook(str(tok_mk))
                if snap1 is None:
                    snap1 = BookSnap()
                # Only mark done if we have valid t1 data and t0 was valid
                mid_t0_f = ffloat(mid_t0)
                mid_t1_f = ffloat(snap1.mid)
                if mid_t1_f is None or snap1.suspect:
                    # Skip: bad book at t1, will retry next tick
                    continue
                if mid_t0_f is None:
                    # t0 was bad, mark done but don't pollute stats
                    db_set_markout_done(conn, markout_id, "", "", "")
                    write_jsonl({"event_type": "MARKOUT_DONE",
                                 "markout_id": markout_id, "horizon_sec": horizon_sec,
                                 "status": "t0_invalid"})
                    continue
                # Direction-aware markout PnL per share
                # BUY: profit if mid goes up -> (mid_t1 - trade_px)
                # SELL: profit if mid goes down -> (trade_px - mid_t1)
                mk_pnl = ""
                _mk_px_f = ffloat(mk_trade_price)
                _mk_side = (mk_side or "").upper()
                if _mk_px_f is not None and mid_t1_f is not None:
                    if _mk_side == "BUY":
                        mk_pnl = clean_number(mid_t1_f - _mk_px_f)
                    elif _mk_side == "SELL":
                        mk_pnl = clean_number(_mk_px_f - mid_t1_f)
                db_set_markout_done(conn, markout_id, snap1.mid, snap1.spread, "",
                                    markout_pnl_per_share=mk_pnl)
                write_jsonl({"event_type": "MARKOUT_DONE",
                             "markout_id": markout_id, "txHash": txHash,
                             "horizon_sec": horizon_sec,
                             "mid_t0": mid_t0, "mid_t1": snap1.mid,
                             "spread_t0": spread_t0, "spread_t1": snap1.spread,
                             "trade_price": mk_trade_price or "",
                             "side": _mk_side,
                             "markout_pnl_per_share": mk_pnl,
                             "status": "ok"})

            # ---- backfill hourClose ----
            now = time.time()
            if now - last_backfill >= BACKFILL_EVERY_SECONDS:
                last_backfill = now
                pending = db_pending_hours(conn)
                updated_rows = 0
                for crypto_sym, hour_ms_int in pending:
                    close = binance_hour_close_if_final(crypto_sym, hour_ms_int)
                    if close is not None:
                        updated_rows += db_set_hour_close(conn, crypto_sym, hour_ms_int, close)
                if updated_rows > 0:
                    print(f"  [backfill] hourClose updated for {updated_rows} row(s)")

            # ---- export snapshot ----
            if now - last_export >= EXPORT_EVERY_SECONDS:
                last_export = now
                db_export_csv(conn, OUT_CSV)
                print(f"  [export] wrote {OUT_CSV}  (total={total_trades} usdc=${total_usdc:.2f})")

            # ---- rollups ----
            if now - last_roll >= ROLLUP_EVERY_SECONDS:
                last_roll = now
                write_minute_rollup(conn, MINUTE_ROLLUP_CSV, epoch_sec() - 3600)

            if now - last_fp >= FINGERPRINT_EVERY_SECONDS:
                last_fp = now
                write_fingerprint(conn, FINGERPRINT_CSV, window_sec=60)

            # ---- inventory snapshot ----
            if now - last_inv >= INVENTORY_EVERY_SECONDS:
                last_inv = now
                write_inventory_snapshot(INVENTORY_CSV)

            # ---- markout stats ----
            if now - last_mk_stats >= MARKOUT_STATS_EVERY_SECONDS:
                last_mk_stats = now
                write_markout_stats(conn, MARKOUT_STATS_CSV)

            # ---- wallet entry model ----
            if now - last_entry_model >= ENTRY_MODEL_EVERY_SECONDS:
                last_entry_model = now
                write_wallet_entry_model(conn, ENTRY_MODEL_CSV)

            # ---- behavior summary ----
            if now - last_behavior >= BEHAVIOR_SUMMARY_EVERY_SECONDS:
                last_behavior = now
                print_behavior_summary(conn)

            # ---- data coverage warning ----
            if now - last_coverage >= 60:
                last_coverage = now
                print_data_coverage()

            # ---- book tape: continuous 1Hz recording ----
            write_book_tape_tick()

            # ---- binance tape: continuous 1Hz recording ----
            write_binance_tape_tick()

            # ---- market ranking: every 10s ----
            compute_market_ranks()

            # ---- latency rollup: every 60s ----
            if now - last_latency_rollup >= LATENCY_ROLLUP_EVERY_SECONDS:
                last_latency_rollup = now
                write_latency_rollup()

            # ---- realized PnL summary: every 120s ----
            if now - last_realized_pnl_summary >= REALIZED_PNL_SUMMARY_EVERY_SECONDS:
                last_realized_pnl_summary = now
                write_realized_pnl_summary(REALIZED_PNL_SUMMARY_CSV)

            time.sleep(POLL_SECONDS)

        except requests.HTTPError as e:
            print(f"  HTTP error: {e} — backing off {HTTP_BACKOFF_SEC}s")
            time.sleep(HTTP_BACKOFF_SEC)
        except Exception as e:
            print(f"  Error: {e} — retrying in 2s")
            time.sleep(2)

    # ---- shutdown ----
    print("[stop] final export...")
    write_jsonl({"event_type": "TRACKER_STOP", "total_trades": total_trades,
                 "total_usdc": round(total_usdc, 4)})
    try:
        db_export_csv(conn, OUT_CSV)
        write_wallet_entry_model(conn, ENTRY_MODEL_CSV)
        print(f"  [export] final wrote {OUT_CSV} + {ENTRY_MODEL_CSV}")
    finally:
        if _jsonl_fh:
            _jsonl_fh.close()
        conn.close()
    print(f"Stopped. {total_trades} trades tracked, ${total_usdc:.2f} USDC total.")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    main()
