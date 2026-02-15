# 1-Hour Market Support — Full Research & Implementation Plan

## Part 1: Existing Architecture (How the Bot Works Today)

### 1.1 Market Discovery

**Kalshi 15-min markets** (`ws_fetcher.py:136-272`):
- Series tickers defined at `ws_fetcher.py:93-98`:
  ```
  KXBTC15M, KXETH15M, KXSOL15M, KXXRP15M
  ```
- `_fetch_kalshi_market(crypto)` calls `kalshi_client.get_markets(series_ticker, limit=20)`
  which hits `GET /trade-api/v2/markets?series_ticker=KXBTC15M&status=open&limit=20`
- Filters response by `close_time`: finds the market whose close_time falls AFTER
  the current 15-min window start (picks the earliest/soonest-closing one)
- Window boundaries calculated from UTC:
  ```python
  current_minute_bucket = (now.minute // 15) * 15
  window_end = now.replace(minute=current_minute_bucket) + timedelta(minutes=15)
  window_start = window_end - timedelta(minutes=15)
  ```
- Extracts strike from `floor_strike` or `strike_price` field (subtracts $0.01 for Kalshi rounding)
- Fallbacks: extract from ticker regex `r'B(\d+)$'`, then market detail API `yes_sub_title`
- Caches result: `_ticker_cache[crypto] = (ticker, close_time, strike)`

**Polymarket 15-min markets** (`ws_fetcher.py:412-506`):
- Slug construction (`ws_fetcher.py:131-134`):
  ```python
  def _get_window_timestamp(self) -> int:
      now = int(time.time())
      return (now // 900) * 900   # Unix timestamp rounded to 15-min boundary
  ```
  Slug: `f"{crypto.lower()}-updown-15m-{window_ts}"` → e.g. `btc-updown-15m-1707993600`
- Calls `GET https://gamma-api.polymarket.com/events/slug/{slug}`
- Parses `markets[0]` for `outcomes` and `clobTokenIds`
- Maps tokens: searches outcomes for "UP"/"YES" and "DOWN"/"NO"
- Strike comes from Chainlink feed (NOT from Gamma API)
- Caches: `_poly_token_cache[crypto] = (up_token, down_token, slug)`

### 1.2 Window Timing System

**15-min window identifier** (`ws_fetcher.py:125-129`):
```python
def _get_current_window(self) -> str:
    now = datetime.utcnow()
    minute_window = (now.minute // 15) * 15
    return f"{now.hour:02d}:{minute_window:02d}"   # e.g. "14:30"
```

**Window progress** (`utils.py:52-65`):
```python
def get_window_progress() -> float:
    now = datetime.now(timezone.utc)
    mins_in_window = now.minute % 15
    secs_in_window = mins_in_window * 60 + now.second
    return secs_in_window / 900   # 0.0 to 1.0
```

**Market window for logging** (`logging_utils.py:211-221`):
```python
def get_current_market_window() -> str:
    now = datetime.now()
    minute_window = (now.minute // 15) * 15
    window_time = now.replace(minute=minute_window, second=0, microsecond=0)
    return window_time.strftime("%Y-%m-%d_%H-%M")   # e.g. "2026-02-12_15-15"
```

### 1.3 Chainlink Price Feed & Strike Capture

**Class**: `ChainlinkPriceFeed` (`websocket_client.py:1160-1443`)

**Connection**: WebSocket to `wss://ws-live-data.polymarket.com`, topic `crypto_prices_chainlink`
Symbols: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `XRPUSDT`

**Instance variables** (`websocket_client.py:1200-1210`):
```python
self.window_strikes: Dict[str, float] = {}      # 15-min strike at t+1s
self._current_window: str = ""
self._window_start_ts: float = 0
self.window_strikes_5m: Dict[str, float] = {}   # 5-min strike at t+601s
self._five_min_window_ts: float = 0
```

**Window rotation** (`websocket_client.py:1327-1337`):
```python
current_window = self._get_current_window()
if current_window != self._current_window:
    self._current_window = current_window
    self._window_start_ts = self._get_window_start_timestamp()
    self._five_min_window_ts = self._window_start_ts + 600
    self.window_strikes.clear()          # ← clears 15-min strikes
    self.window_strikes_5m.clear()       # ← clears 5-min strikes
    self._five_min_captured = False
    self._logged_prices.clear()
```

**15-min strike capture (exact timestamp match)** (`websocket_client.py:1339-1349`):
```python
window_start_ms = int(self._window_start_ts * 1000)
if crypto not in self.window_strikes and window_start_ms > 0:
    if timestamp and int(timestamp) == window_start_ms:
        self.window_strikes[crypto] = value
```
The Chainlink feed delivers a price where `timestamp == windowStart * 1000` (in milliseconds).
This arrives ~1 second into each 15-min window. The bot captures this as the "price to beat."

**5-min strike capture** (`websocket_client.py:1351-1366`):
Same logic but at `timestamp == (window_start_ts + 600) * 1000`.
Fallback: first price received after t+600s.

**Window ID format** (`websocket_client.py:1212-1216`):
```python
def _get_current_window(self) -> str:
    now = datetime.now(timezone.utc)
    minute_bucket = (now.minute // 15) * 15
    return f"{now.strftime('%Y%m%d_%H')}{minute_bucket:02d}"  # "20260212_1515"
```

**Window start timestamp** (`websocket_client.py:1218-1226`):
```python
def _get_window_start_timestamp(self) -> float:
    now = datetime.now(timezone.utc)
    window_start = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    return window_start.timestamp()
```

### 1.4 WebSocket Subscriptions

**Kalshi WebSocket** (`websocket_client.py:1560-1590`):
- `subscribe_kalshi(ticker, crypto, strike)` subscribes to a specific market ticker
- Pre-fetches initial price via REST API (`_prefetch_kalshi_price`)
- Stores price updates in `KalshiWebSocket.prices[crypto]` as `PriceUpdate` objects

**Polymarket WebSocket** (`websocket_client.py:1604-1614`):
- `subscribe_polymarket_both(up_token, down_token, crypto, strike)` queues both tokens
- Must flush all at once: `flush_polymarket_subscriptions()` sends one batch message
- Polymarket WS only supports one subscription message per connection

**Subscribe flow in main.py** (lines ~1088-1152):
```
For each crypto → ws_fetcher.subscribe_crypto(crypto, skip_scraper=True)
After all queued → ws_fetcher.flush_subscriptions(cryptos)
```

### 1.5 Snapshot & Price Fetching

**Snapshot dataclass** (`models.py:10-27`):
```python
@dataclass
class Snapshot:
    crypto: str
    kalshi_ticker: str
    kalshi_close_time: str
    poly_slug: str
    poly_question: str
    k_up: float        # Kalshi YES ask (cost to BUY YES)
    k_down: float      # Kalshi NO ask (cost to BUY NO)
    p_up: float         # Poly UP ask
    p_down: float       # Poly DOWN ask
    poly_up_token: Optional[str] = None
    poly_down_token: Optional[str] = None
    kalshi_strike: Optional[float] = None
    poly_strike: Optional[float] = None
    poly_up_asks: Optional[list] = None
    poly_down_asks: Optional[list] = None
```

**WebSocket cache → Snapshot** (`ws_fetcher.py:788-895`):
`get_snapshot(crypto)` reads cached `PriceUpdate` objects from the WebSocket manager
and constructs a `Snapshot`. These are the STALE WebSocket prices.

**Fresh Kalshi prices** (`main.py:245-300`):
`fetch_fresh_kalshi_prices(kalshi_client, ticker)` calls the REST API
`GET /trade-api/v2/markets/{ticker}/orderbook` to get real-time prices.
Returns `(yes_ask, no_ask, yes_depth, no_depth, yes_depth_buffered, no_depth_buffered)`.

The main scan loop ALWAYS overrides WebSocket prices with fresh REST prices:
```python
snap.k_up = fresh_k_yes
snap.k_down = fresh_k_no
```
If fresh fetch fails → skip that crypto (never use stale prices).

### 1.6 Trade Execution Flow

**Main scan loop** (`main.py:1376-2150+`):

1. **Get snapshots**: `snapshots = ws_fetcher.fetch_all(cryptos)` (WebSocket cache)
2. **For each crypto**:
   a. Fresh fetch Kalshi prices via REST (override snapshot)
   b. Skip if fresh fetch fails
   c. `pick_best_direction(snap)` → returns best direction and edge %
   d. Safe bet detection (penny strike or cross-strike)
   e. Edge threshold check (≥6%, ≤27%)
   f. Quantity sizing (safe bet: 30% of thinner book, max 100, min depth 15)
   g. **Execute Kalshi first**: `place_order(ticker, side, qty, price_cents + 1_cent_buffer)`
   h. Wait for Kalshi fill (up to 500ms)
   i. **Execute Poly second**: Fetch fresh orderbook, place at best ask
   j. If Poly fails → **unwind Kalshi** immediately

**Direction selection** (`main.py:1716-1727`):
```python
if best_dir == "K_UP+P_DOWN":
    kalshi_side = "yes"       # Buy YES on Kalshi
    poly_direction = "down"   # Buy DOWN on Polymarket
elif best_dir == "K_DOWN+P_UP":
    kalshi_side = "no"        # Buy NO on Kalshi
    poly_direction = "up"     # Buy UP on Polymarket
```

### 1.7 Window Rotation in Main Loop

**Detection** (`main.py:971-1014`):
```python
current_ws_window = get_current_market_window()  # "YYYY-MM-DD_HH-MM"
if current_ws_window != last_ws_window:
    # Clear everything:
    ws_fetcher._ticker_cache.clear()
    ws_fetcher._poly_token_cache.clear()
    ws_fetcher._kalshi_strikes.clear()
    ws_fetcher._poly_strikes.clear()
    traded_cryptos.clear()
    crypto_edges.clear()
    stopped_cryptos.clear()
    five_min_data = None
    five_min_fetched_this_window = False
    five_min_chainlink_tried = False
```

**Re-subscription sequence** (Phase 1 at t+60s, Phase 2 immediately after):
1. Phase 1: Subscribe to new Kalshi markets, fetch Kalshi strikes
2. Step 1: Wait for Chainlink t+1s price (up to 5s)
3. Step 2: Redeem one position
4. Step 3: Balance notification
5. Phase 2: Use Chainlink strikes for Poly (no scraper fallback)
6. Strike threshold comparison → mark BAD cryptos
7. Begin scanning

### 1.8 Existing 5-Min Cross-Timeframe (Precedent)

The bot already supports one cross-timeframe arb: **Kalshi 15-min vs Polymarket 5-min**.

**5-min market discovery** (`ws_fetcher.py:508-583`):
```python
fifteen_min_ts = self._get_window_timestamp()
five_min_ts = fifteen_min_ts + 600
slug = f"{crypto.lower()}-updown-5m-{five_min_ts}"
```

**Timing** (`main.py:2537-2560`):
- At t+601s: Check Chainlink feed for 5-min strike (`get_strike_5m("BTC")`)
- Fetch 5-min market tokens from Gamma API
- If Chainlink missed → skip 5-min trade this window (no scraper fallback)

**Cross-strike arb logic** (`main.py:2582-2670`):
```python
if kalshi_strike_15m < five_min_strike:
    direction_5m = "K_UP+P5_DOWN"
    k_price_5m = btc_snap.k_up
    kalshi_side_5m = "yes"
elif kalshi_strike_15m > five_min_strike:
    direction_5m = "K_DOWN+P5_UP"
    k_price_5m = btc_snap.k_down
    kalshi_side_5m = "no"
```

This existing pattern is the template for implementing 1-hour cross-timeframe trades.

### 1.9 Configuration Constants

**From `config.py`:**
```
MIN_BET_DOLLARS = 3.00          MAX_TRADE_QTY = 7
MIN_EDGE_PCT = 5.0              MAX_NET_EDGE_TO_TRADE_PCT = 50.0
KALSHI_SETUP_SECONDS = 60       STRIKE_PRICE_OFFSET_SECONDS = 1
PRICE_BUFFER_START = 0.01       PRICE_BUFFER_MAX = 0.02
ORDER_CHECK_INTERVAL = 0.05     MAX_WAIT_FOR_FILL = 0.5
POLY_WAIT_FOR_FILL = 0.175
```

**Trading cutoff**: No trading in last 1.5 min of window (progress > 0.90)

---

## Part 2: 1-Hour Market Research

### 2.1 Kalshi 1-Hour Markets

**Series tickers** (different from 15-min):
| Asset | 15-min Series | 1-hour Series | 1-hour Event Type |
|-------|---------------|---------------|-------------------|
| BTC | `KXBTC15M` | `KXBTCD` | "Price Above/Below" |
| ETH | `KXETH15M` | `KXETHD` | "Price Above/Below" |
| SOL | `KXSOL15M` | `KXSOLD` | "SOL Directional" |
| XRP | `KXXRP15M` | `KXXRPD` | "Price Above/Below" |

**Ticker format**: `kx{asset}d-{YY}{mon}{DD}{HH}`
- `{YY}` = 2-digit year
- `{mon}` = 3-letter month (lowercase: `jan`, `feb`, `dec`)
- `{DD}` = 2-digit day
- `{HH}` = 2-digit **closing/settlement hour** in ET (24-hour format)

**Examples verified from user URLs:**
| Ticker | Decoded | Window Covered |
|--------|---------|----------------|
| `kxbtcd-26feb1214` | Feb 12, 2026, settles at 14:00 (2pm) ET | 1pm-2pm ET |
| `kxbtcd-25dec0110` | Dec 1, 2025, settles at 10:00 (10am) ET | 9am-10am ET |
| `kxethd-26feb1214` | Feb 12, 2026, settles at 14:00 (2pm) ET | 1pm-2pm ET |
| `kxsold-26feb1214` | Feb 12, 2026, settles at 14:00 (2pm) ET | 1pm-2pm ET |
| `kxxrpd-26feb1214` | Feb 12, 2026, settles at 14:00 (2pm) ET | 1pm-2pm ET |

**Market structure**: Each hourly event contains YES/NO contracts at a specific strike.
The API returns `floor_strike` (same field as 15-min). We subtract $0.01 for Kalshi rounding.

**API discovery**: Same call as 15-min:
```python
markets = kalshi_client.get_markets("KXBTCD", limit=20)
# Filter by close_time within current 1-hour window
```

**Settlement**: CF Benchmarks Real-Time Index (60-second average).

### 2.2 Polymarket 1-Hour Markets

**Slug format**: `{asset_name}-up-or-down-{month}-{day}-{hour}{ampm}-et`
- `{asset_name}`: `bitcoin`, `ethereum`, `solana`, `xrp` (full names, lowercase)
- `{month}`: full month name lowercase (`january`, `february`, ..., `december`)
- `{day}`: day of month, NO leading zero (`1`, `12`, `28`)
- `{hour}`: 12-hour format, NO leading zero (`1`, `11`, `12`)
- `{ampm}`: lowercase `am` or `pm`
- Always ends with `-et`
- The hour is the **opening/start hour** in Eastern Time

**Examples verified from user URLs:**
| Slug | Decoded | Window Covered |
|------|---------|----------------|
| `bitcoin-up-or-down-february-12-1pm-et` | Feb 12, opens 1pm ET | 1pm-2pm ET |
| `bitcoin-up-or-down-february-12-11am-et` | Feb 12, opens 11am ET | 11am-12pm ET |
| `ethereum-up-or-down-february-12-1pm-et` | Feb 12, opens 1pm ET | 1pm-2pm ET |
| `solana-up-or-down-february-12-1pm-et` | Feb 12, opens 1pm ET | 1pm-2pm ET |
| `xrp-up-or-down-february-12-1pm-et` | Feb 12, opens 1pm ET | 1pm-2pm ET |

**API**: `GET https://gamma-api.polymarket.com/events/slug/{slug}`
Response structure matches 15-min events: `markets[0]` with `outcomes` and `clobTokenIds`.

**Settlement**: Binance 1H candle (BTC/USDT, ETH/USDT, etc.). Resolves "Up" if close >= open.

**Key difference from 15-min**: Different oracle source (Binance vs Chainlink).
The strike (reference price) is the Binance candle open, NOT Chainlink.
However, at the hour boundary, Chainlink and Binance prices are very close (~0.01-0.05% diff),
so the Chainlink t+1s price is a good approximation of the 1h strike.

### 2.3 Ticker Encoding Comparison

| Attribute | Kalshi 15-min | Kalshi 1-hour | Poly 15-min | Poly 1-hour |
|-----------|--------------|---------------|-------------|-------------|
| **Series** | `KXBTC15M` | `KXBTCD` | N/A | N/A |
| **Identifier** | Ticker w/ HHMM in UTC | Ticker w/ HH in ET | Unix timestamp slug | Human-readable slug |
| **Time ref** | Close time (UTC) | Close time (ET, 24h) | Open time (UTC) | Open time (ET, 12h) |
| **Strike in ID** | Yes (`-B{price}`) | No | No | No |
| **TZ** | UTC | Eastern Time | UTC | Eastern Time |

---

## Part 3: Window Alignment & Timing

### 3.1 How 1-Hour and 15-Min Windows Align

```
Hour boundary (ET):   1:00 PM                                    2:00 PM
                        │                                            │
1-hour window:          │ ◄──────────── 1h window ────────────────► │
                        │                                            │
15-min windows:         │  :00-:15  │  :15-:30  │  :30-:45  │  :45-:00  │
                        │           │           │           │  FINAL    │
                        │           │           │           │           │
Strikes captured:    1h strike   15m #2      15m #3      15m #4      Settle
                     + 15m #1   strike      strike      strike
                     (same ts)
```

**Key observations:**
1. The 1-hour window start IS a 15-min window start (every hour boundary is a 15-min boundary)
2. The Chainlink t+1s price captured at 1:00 PM serves as BOTH the 1h strike and the first 15m strike
3. Each subsequent 15-min window captures its own distinct strike
4. Only the FINAL 15-min window (1:45-2:00) settles at the same time as the 1-hour window (2:00)

### 3.2 Settlement Co-Termination

For cross-timeframe arb to work, both legs must settle at the same time:

| Combination | Leg 1 Settles | Leg 2 Settles | Co-terminate? | When to Trade |
|-------------|---------------|---------------|---------------|---------------|
| K_15m (1:45-2:00) + P_15m (1:45-2:00) | 2:00 PM | 2:00 PM | Yes | Anytime in :45-:00 |
| K_1h (1-2pm) + P_1h (1-2pm) | 2:00 PM | 2:00 PM | Yes | Anytime in 1-2pm |
| K_1h (1-2pm) + P_15m (1:45-2:00) | 2:00 PM | 2:00 PM | Yes | Only in :45-:00 |
| K_15m (1:45-2:00) + P_1h (1-2pm) | 2:00 PM | 2:00 PM | Yes | Only in :45-:00 |
| K_1h (1-2pm) + P_15m (1:00-1:15) | 2:00 PM | 1:15 PM | **NO** | **Never** |
| K_1h (1-2pm) + P_15m (1:15-1:30) | 2:00 PM | 1:30 PM | **NO** | **Never** |

**Rule**: Cross-timeframe trades (1h ↔ 15m) ONLY during the final 15 minutes of the hour.

### 3.3 Timing Gate Logic

```python
now_utc = datetime.now(timezone.utc)
in_final_15m_of_hour = (now_utc.minute >= 45)

# Also need: not too close to settlement (same 1.5min cutoff)
# For cross-timeframe: window progress of the 15-min side must be < 0.90
# For same-timeframe 1h: hour progress must be reasonable (not last 1.5min of hour)
```

### 3.4 1-Hour Strike Persistence

**Problem**: The existing code clears ALL strikes every 15-min rotation. The 1-hour strike
(captured at the hour boundary) must survive through 4 x 15-min rotations.

**Solution**: Store 1-hour strikes in a SEPARATE dict that is only cleared on hour boundaries:
```python
# Cleared every 15 min (existing):
self.window_strikes.clear()
self.window_strikes_5m.clear()

# Cleared every 1 hour (NEW — must NOT be in the 15-min clear block):
# Only cleared when self._current_1h_window changes
self.window_strikes_1h.clear()
```

### 3.5 When Is the 1-Hour Strike Captured?

The Chainlink feed delivers prices with timestamps that match window boundaries.
At 1:00:00 PM UTC, the feed delivers a price with `timestamp == 1:00:00 PM * 1000`.

This is the exact same mechanism used for 15-min strikes. At the top of the hour:
- The 15-min code captures it as `window_strikes["BTC"] = price`
- The NEW 1-hour code ALSO captures it as `window_strikes_1h["BTC"] = price`
- Both happen from the same Chainlink message

At 1:15:00 PM UTC:
- The 15-min code clears old strikes and captures a NEW 15-min strike
- The 1-hour code does NOT clear (still in the same 1-hour window)
- `window_strikes_1h["BTC"]` still has the 1:00 PM price

---

## Part 4: Implementation Plan

### 4.1 ws_fetcher.py Changes

**New constants** (add near existing `SERIES_TICKERS`):
```python
SERIES_TICKERS_1H = {
    "BTC": "KXBTCD",
    "ETH": "KXETHD",
    "SOL": "KXSOLD",
    "XRP": "KXXRPD",
}
POLY_ASSET_SLUGS_1H = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "XRP": "xrp",
}
```

**New instance variables** (add to `__init__`):
```python
# 1-hour market caches
self._ticker_cache_1h: Dict[str, tuple] = {}        # crypto -> (ticker, close_time, strike)
self._poly_token_cache_1h: Dict[str, tuple] = {}    # crypto -> (up_token, down_token, slug)
self._kalshi_strikes_1h: Dict[str, float] = {}      # crypto -> 1h Kalshi strike
self._poly_strikes_1h: Dict[str, float] = {}        # crypto -> 1h Poly strike
```

**New methods:**

1. `_get_current_1h_window()` → returns `"HH:00"` (UTC, hour-aligned)
2. `_get_1h_window_timestamp()` → `(now // 3600) * 3600` (Unix timestamp for hour start)
3. `_fetch_kalshi_1h_market(crypto)` → same as `_fetch_kalshi_market` but:
   - Uses `SERIES_TICKERS_1H` series
   - Window matching: `hour_start` to `hour_end` (1-hour boundaries)
   - Caches to `_ticker_cache_1h`
4. `_fetch_poly_1h_market(crypto)` → constructs human-readable slug in ET, calls Gamma API
5. `_build_poly_1h_slug(crypto)` → ET timezone slug construction
6. `subscribe_crypto_1h(crypto)` → subscribes to 1h Kalshi ticker + 1h Poly tokens
7. `get_snapshot_1h(crypto)` → builds Snapshot from 1h WebSocket prices

### 4.2 websocket_client.py Changes

**ChainlinkPriceFeed additions:**
```python
# New instance vars:
self.window_strikes_1h: Dict[str, float] = {}   # persists across 15-min rotations
self._current_1h_window: str = ""
self._1h_window_start_ts: float = 0.0
```

**Modify `_handle_message`:**
1. BEFORE the existing 15-min window check, add 1-hour window check:
   ```python
   current_1h = self._get_current_1h_window()
   if current_1h != self._current_1h_window:
       self._current_1h_window = current_1h
       self._1h_window_start_ts = self._get_1h_window_start_timestamp()
       self.window_strikes_1h.clear()
   ```
2. After the existing 15-min strike capture, add 1h strike capture:
   ```python
   window_start_1h_ms = int(self._1h_window_start_ts * 1000)
   if crypto not in self.window_strikes_1h and window_start_1h_ms > 0:
       if timestamp and int(timestamp) == window_start_1h_ms:
           self.window_strikes_1h[crypto] = value
   ```

**New methods:**
- `_get_current_1h_window()` → `f"{now.strftime('%Y%m%d_%H')}00"` (e.g. `"20260212_1400"`)
- `_get_1h_window_start_timestamp()` → `now.replace(minute=0, second=0).timestamp()`
- `get_strike_1h(crypto)` → `return self.window_strikes_1h.get(crypto)`

**WebSocketManager additions:**
- Track 1h Kalshi/Poly subscriptions separately
- May need separate subscription slots or reuse existing

### 4.3 main.py Changes

**New tracking variables** (near existing `last_ws_window`, `traded_cryptos`, etc.):
```python
last_1h_window = None
traded_cryptos_1h = {}
stopped_cryptos_1h = set()
crypto_edges_1h = {}
```

**Hour boundary detection** (in window rotation section, BEFORE the 15-min check):
```python
current_1h_window = get_current_1h_market_window()
if current_1h_window != last_1h_window:
    print(f"[1H WINDOW] {last_1h_window} -> {current_1h_window}")
    last_1h_window = current_1h_window
    traded_cryptos_1h.clear()
    stopped_cryptos_1h.clear()
    crypto_edges_1h.clear()
    ws_fetcher._ticker_cache_1h.clear()
    ws_fetcher._poly_token_cache_1h.clear()
    ws_fetcher._kalshi_strikes_1h.clear()
    ws_fetcher._poly_strikes_1h.clear()
    # Subscribe to new 1h markets
    for crypto in cryptos:
        ws_fetcher.subscribe_crypto_1h(crypto)
    # Wait for Chainlink 1h strike (same t+1s mechanism)
```

**1h strike capture at hour boundary:**
The Chainlink feed auto-captures 1h strikes. In the Phase 2 equivalent for 1h:
```python
if ws_fetcher.ws_manager and ws_fetcher.ws_manager.chainlink_feed:
    feed = ws_fetcher.ws_manager.chainlink_feed
    for c in cryptos:
        strike_1h = feed.get_strike_1h(c)
        if strike_1h and strike_1h > 0:
            ws_fetcher._poly_strikes_1h[c] = strike_1h
```

**Scan loop additions** (AFTER existing K15m+P15m scan, BEFORE the 5-min block):

```python
# ═══════════════════════════════════════════════
# 1-HOUR MARKET SCANS
# ═══════════════════════════════════════════════
now_utc = datetime.now(timezone.utc)
in_final_15m = (now_utc.minute >= 45)

# --- Combination 2: K_1h + P_1h (anytime in hour) ---
for crypto in cryptos:
    if crypto in stopped_cryptos_1h:
        continue
    # Fresh fetch 1h Kalshi prices
    # Fresh fetch 1h Poly prices
    # Calculate edge (same as existing logic)
    # Execute if edge good enough

# --- Combinations 3 & 4: Cross-timeframe (final 15 min only) ---
if in_final_15m:
    for crypto in cryptos:
        # Combo 3: K_1h + P_15m
        # K_1h strike vs P_15m strike → cross-strike arb
        # Fresh fetch K_1h prices + use existing P_15m snapshot

        # Combo 4: K_15m + P_1h
        # K_15m strike vs P_1h strike → cross-strike arb
        # Use existing K_15m snapshot + fresh fetch P_1h prices
```

### 4.4 Polymarket 1h Slug Construction

```python
from zoneinfo import ZoneInfo

def _build_poly_1h_slug(self, crypto: str) -> str:
    """Build Polymarket 1-hour event slug using Eastern Time."""
    et = ZoneInfo("America/New_York")
    now_et = datetime.now(et)
    hour_start = now_et.replace(minute=0, second=0, microsecond=0)

    asset_name = self.POLY_ASSET_SLUGS_1H[crypto]        # "bitcoin"
    month_name = hour_start.strftime("%B").lower()         # "february"
    day = hour_start.day                                    # 12
    hour_12 = int(hour_start.strftime("%I"))                # 1 (no zero-pad)
    ampm = hour_start.strftime("%p").lower()                # "pm"

    return f"{asset_name}-up-or-down-{month_name}-{day}-{hour_12}{ampm}-et"
    # → "bitcoin-up-or-down-february-12-1pm-et"
```

**Edge cases:**
- 12pm → `"12pm"` (noon)
- 12am → `"12am"` (midnight)
- DST transition: `ZoneInfo` handles EST ↔ EDT automatically

### 4.5 Kalshi 1h Window Matching

```python
def _fetch_kalshi_1h_market(self, crypto: str) -> Optional[tuple]:
    series = self.SERIES_TICKERS_1H.get(crypto)
    markets = self._kalshi_client.get_markets(series, limit=20)

    now = datetime.now(timezone.utc)
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)

    # Find market closing at hour_end (close_time > hour_start, soonest)
    best = None
    best_dt = None
    for m in markets:
        close_dt = parse_iso_dt(m.get("close_time", ""))
        if close_dt and close_dt > hour_start:
            if best_dt is None or close_dt < best_dt:
                best = m
                best_dt = close_dt

    if not best:
        return None

    ticker = best.get("ticker", "")
    close_time = best.get("close_time", "")
    strike = best.get("floor_strike") or best.get("strike_price")
    if strike is not None:
        strike = round(float(strike) - 0.01, 2)

    self._ticker_cache_1h[crypto] = (ticker, close_time, strike)
    return (ticker, close_time, strike)
```

---

## Part 5: Key Invariants

1. **Kalshi always trades first** — same as existing behavior for ALL combinations
2. **1h strikes persist across 15-min rotations** — only clear on hour boundary
3. **15-min behavior is completely unchanged** — 1h is purely additive
4. **Cross-timeframe only in final 15 min** — `minute >= 45` gate check
5. **Fresh prices required for 1h** — same pattern: REST fetch overrides WebSocket cache
6. **Separate trade counts** — 1h and 15m tracked independently
7. **No trading last 1.5 min** — applies to ALL combinations (same cutoff)

## Part 6: File Change Summary

| File | Changes |
|------|---------|
| `ws_fetcher.py` | +2 dicts (series tickers, asset slugs), +4 instance vars, +6 methods |
| `websocket_client.py` | +3 instance vars in ChainlinkPriceFeed, +3 methods, modify `_handle_message` |
| `main.py` | +hour rotation detection, +1h strike capture, +3 scan blocks, +trade tracking vars |
| `config.py` | +1h edge thresholds (if different from 15m) |
| `models.py` | No changes (reuse existing Snapshot) |
