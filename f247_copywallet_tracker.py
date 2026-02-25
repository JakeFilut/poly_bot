#!/usr/bin/env python3
"""
F247 Copy-Wallet Trade Tracker (Start-From-Now + MAX FORENSICS)
================================================================
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

Outputs (all inside LOG_DIR):
  f247_copywallet_trades.db       SQLite source of truth
  f247_copywallet_fills_raw.csv   append-only raw fills
  f247_copywallet_fills_enriched.csv  periodic full snapshot from DB
  f247_copywallet_markouts.csv    append-only markout registry
  f247_copywallet_minute_rollup.csv   periodic snapshot
  f247_copywallet_fingerprint.csv     append-only

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
import requests
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

# Backoff
HTTP_BACKOFF_SEC = 6

# =======================================================
POLYMARKET_ACTIVITY = "https://data-api.polymarket.com/activity"
CLOB_BOOK = "https://clob.polymarket.com/book"
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


# -------------------- signals --------------------
def _handle_stop(signum, frame):
    global STOP
    STOP = True
    print("\n[stop] Finishing current loop...")


signal.signal(signal.SIGINT, _handle_stop)
signal.signal(signal.SIGTERM, _handle_stop)


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
            r = SESSION.get(CLOB_BOOK, params={"asset_id": token_id}, timeout=5)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, dict):
            return None
        bids = data.get("bids") or []
        asks = data.get("asks") or []
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
        bid_depth_top = _levels_depth(bids, ORDERBOOK_DEPTH_LEVELS)
        ask_depth_top = _levels_depth(asks, ORDERBOOK_DEPTH_LEVELS)
        bid_depth_tot = _levels_depth(bids, ORDERBOOK_TOTAL_DEPTH_LEVELS)
        ask_depth_tot = _levels_depth(asks, ORDERBOOK_TOTAL_DEPTH_LEVELS)
        imb = ""
        if ask_depth_top > 0:
            imb = clean_number(bid_depth_top / ask_depth_top)
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
        )
        _book_cache[token_id] = (now, snap)
        return snap
    except Exception:
        return None


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
        token_id TEXT,
        order_id TEXT,
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
        trade_vs_micro TEXT
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS markouts (
        markout_id TEXT PRIMARY KEY,
        txHash TEXT,
        token_id TEXT,
        t0_epoch INTEGER,
        horizon_sec INTEGER,
        due_epoch INTEGER,
        done INTEGER DEFAULT 0,
        mid_t0 TEXT,
        spread_t0 TEXT,
        binance_t0 TEXT,
        mid_t1 TEXT,
        spread_t1 TEXT,
        binance_t1 TEXT
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(timestamp_epoch);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_markouts_due ON markouts(done, due_epoch);")
    conn.commit()


def db_insert_trade(conn: sqlite3.Connection, row: dict) -> bool:
    try:
        conn.execute("""
        INSERT INTO trades (
            txHash, timestamp_epoch, timestamp_iso, slug, title, outcome, side, size, price, usdcSize,
            token_id, order_id,
            crypto, binanceSpot, hour_ms, hourOpen, hourClose,
            bestBid, bestAsk, spread, mid, microprice, imbalance_topN, bid_depth_topN, ask_depth_topN,
            bid_depth_total, ask_depth_total, bidsJson, asksJson, bookTs, bookHash,
            crossing_estimate, trade_vs_mid, trade_vs_micro
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["txHash"], row["timestamp_epoch"], row["timestamp_iso"], row["slug"], row["title"],
            row["outcome"], row["side"], row["size"], row["price"], row["usdcSize"],
            row["token_id"], row["order_id"],
            row["crypto"], row["binanceSpot"], row["hour_ms"], row["hourOpen"], row["hourClose"],
            row["bestBid"], row["bestAsk"], row["spread"], row["mid"], row["microprice"], row["imbalance_topN"],
            row["bid_depth_topN"], row["ask_depth_topN"], row["bid_depth_total"], row["ask_depth_total"],
            row["bidsJson"], row["asksJson"], row["bookTs"], row["bookHash"],
            row["crossing_estimate"], row["trade_vs_mid"], row["trade_vs_micro"],
        ))
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
                        mid_t0: str, spread_t0: str, binance_t0: str):
    for h in MARKOUT_HORIZONS_SEC:
        due = t0_epoch + h
        markout_id = sha1_short(f"{txHash}:{h}")
        try:
            conn.execute("""
                INSERT INTO markouts (
                    markout_id, txHash, token_id, t0_epoch, horizon_sec, due_epoch,
                    mid_t0, spread_t0, binance_t0
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (markout_id, txHash, token_id, t0_epoch, h, due, mid_t0, spread_t0, binance_t0))
        except sqlite3.IntegrityError:
            pass
    conn.commit()


def db_fetch_due_markouts(conn: sqlite3.Connection, now_epoch: int, limit: int) -> List[tuple]:
    cur = conn.execute("""
        SELECT markout_id, txHash, token_id, t0_epoch, horizon_sec, due_epoch, mid_t0, spread_t0, binance_t0
        FROM markouts
        WHERE done = 0 AND due_epoch <= ?
        ORDER BY due_epoch ASC
        LIMIT ?
    """, (now_epoch, limit))
    return cur.fetchall()


def db_set_markout_done(conn: sqlite3.Connection, markout_id: str, mid_t1: str, spread_t1: str, binance_t1: str):
    conn.execute("""
        UPDATE markouts
        SET done = 1, mid_t1 = ?, spread_t1 = ?, binance_t1 = ?
        WHERE markout_id = ?
    """, (mid_t1, spread_t1, binance_t1, markout_id))
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
        SELECT timestamp_epoch, side, price, usdcSize, bestBid, bestAsk, spread, microprice
        FROM trades
        WHERE timestamp_epoch >= ?
        ORDER BY timestamp_epoch ASC
    """, (since_epoch,))
    rows = cur.fetchall()
    if not rows:
        return
    buckets: Dict[int, dict] = {}
    for ts, side, px, usdc, bb, ba, sp, micro in rows:
        m = minute_bucket(int(ts))
        b = buckets.setdefault(m, {
            "trades": 0, "buys": 0, "sells": 0, "usdc_total": 0.0,
            "spreads": [], "micros": [], "cross": 0, "cross_den": 0
        })
        b["trades"] += 1
        s = (side or "").upper()
        if s == "BUY":
            b["buys"] += 1
        elif s == "SELL":
            b["sells"] += 1
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
              "usdc_total", "avg_spread", "avg_microprice", "aggressive_cross_pct"]
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


# -------------------- main --------------------
def main():
    conn = db_connect(DB_PATH)
    db_init(conn)

    start_ts = epoch_sec()
    last_seen_ts = start_ts
    seen_tx: set = set()

    # Lifetime counters
    total_trades = 0
    total_usdc = 0.0

    raw_header = [
        "timestamp_iso", "timestamp_epoch", "slug", "title", "outcome", "side",
        "size", "price", "usdcSize", "txHash", "token_id", "order_id",
        "crypto", "binanceSpot", "hour_ms", "hourOpen", "hourClose",
        "bestBid", "bestAsk", "spread", "mid", "microprice",
        "imbalance_topN", "bid_depth_topN", "ask_depth_topN",
        "bid_depth_total", "ask_depth_total",
        "bidsJson", "asksJson", "bookTs", "bookHash",
        "crossing_estimate", "trade_vs_mid", "trade_vs_micro",
    ]
    init_csv(RAW_CSV, raw_header)

    mark_header = [
        "markout_id", "txHash", "token_id", "t0_epoch", "horizon_sec", "due_epoch",
        "mid_t0", "spread_t0", "binance_t0", "mid_t1", "spread_t1", "binance_t1",
    ]
    init_csv(MARKOUTS_CSV, mark_header)

    write_jsonl({"event_type": "TRACKER_START", "wallet": WALLET,
                 "start_ts": start_ts, "start_iso": ts_to_iso(start_ts)})

    print(f"[F247 COPYWALLET TRACKER]")
    print(f"  Start: {ts_to_iso(start_ts)} (epoch={start_ts})")
    print(f"  Wallet: {WALLET}")
    print(f"  Log dir: {LOG_DIR}")
    print(f"  DB: {DB_PATH}")
    print(f"  RAW CSV: {RAW_CSV}")
    print(f"  JSONL: {JSONL_LOG}")
    print()

    # Probe Binance connectivity
    btc_probe = binance_spot_price("BTC")
    print(f"  Binance probe: BTC/USDT={btc_probe or 'N/A'}")
    print("  Ctrl+C to stop.\n")

    last_backfill = 0.0
    last_export = 0.0
    last_roll = 0.0
    last_fp = 0.0

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
                token_id = safe_get(t, ["token_id", "tokenId", "asset_id", "assetId"])
                order_id = safe_get(t, ["order_id", "orderId"])

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
                    "token_id": (str(token_id) if token_id is not None else ""),
                    "order_id": (str(order_id) if order_id is not None else ""),
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

                    # Schedule markouts
                    if token_id:
                        db_add_markout_jobs(
                            conn=conn, txHash=tx, token_id=str(token_id),
                            t0_epoch=ts_int, mid_t0=snap.mid,
                            spread_t0=snap.spread, binance_t0=spot,
                        )
                        for h in MARKOUT_HORIZONS_SEC:
                            due = ts_int + h
                            markout_id = sha1_short(f"{tx}:{h}")
                            append_row_csv(MARKOUTS_CSV, mark_header, {
                                "markout_id": markout_id, "txHash": tx,
                                "token_id": str(token_id),
                                "t0_epoch": str(ts_int), "horizon_sec": str(h),
                                "due_epoch": str(due), "mid_t0": snap.mid,
                                "spread_t0": snap.spread, "binance_t0": spot,
                                "mid_t1": "", "spread_t1": "", "binance_t1": "",
                            })

                    # JSONL event
                    write_jsonl({
                        "event_type": "F247_TRADE",
                        "slug": slug, "crypto": crypto, "outcome": outcome,
                        "side": side, "size": clean_number(size),
                        "price": clean_number(price),
                        "usdc": clean_number(usdc_size),
                        "txHash": tx,
                        "crossing": crossing,
                        "spread": snap.spread, "mid": snap.mid,
                        "binance_spot": spot, "hour_open": hour_open,
                    })

                    # Console
                    ob_tag = ""
                    if row["bestBid"] or row["bestAsk"]:
                        ob_tag = (f" book(bb={row['bestBid']} ba={row['bestAsk']} "
                                  f"spr={row['spread']} micro={row['microprice']} "
                                  f"imb={row['imbalance_topN']})")
                    bn_tag = f" [{crypto} spot={spot} O={hour_open}]" if crypto else ""
                    print(
                        f"  F247{bn_tag}{ob_tag}: {iso} {slug} {outcome} {side} "
                        f"sz={row['size']} px={row['price']} usdc={row['usdcSize']} "
                        f"{crossing} dMid={trade_vs_mid} dMicro={trade_vs_micro}"
                    )

            # ---- markout processing ----
            due_jobs = db_fetch_due_markouts(conn, epoch_sec(), MARKOUT_MAX_PER_TICK)
            for (markout_id, txHash, token_id, t0_epoch, horizon_sec,
                 due_epoch, mid_t0, spread_t0, bin_t0) in due_jobs:
                snap1 = fetch_orderbook(str(token_id))
                if snap1 is None:
                    snap1 = BookSnap()
                db_set_markout_done(conn, markout_id, snap1.mid, snap1.spread, "")

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
        print(f"  [export] final wrote {OUT_CSV}")
    finally:
        if _jsonl_fh:
            _jsonl_fh.close()
        conn.close()
    print(f"Stopped. {total_trades} trades tracked, ${total_usdc:.2f} USDC total.")


if __name__ == "__main__":
    main()
