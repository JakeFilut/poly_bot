"""
WebSocket-based fetcher for real-time market data.

This module provides a WebSocket-based alternative to the REST API fetcher,
offering ~10-50x faster price updates for arbitrage trading.
"""

import time
import threading
import requests
from typing import Dict, List, Optional, Callable
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from models import Snapshot
from websocket_client import WebSocketManager, PriceUpdate, OrderResult
from kalshi_client import KalshiClient
from config import DEBUG


class WebSocketFetcher:
    """
    Fetcher that uses WebSocket connections for real-time price streaming.

    Provides:
    - Real-time price updates (~50-100ms latency)
    - Fast order placement via WebSocket (Kalshi) and SDK (Polymarket)
    - Automatic reconnection handling
    - Snapshot generation compatible with existing code
    """

    # Kalshi series tickers for crypto up/down markets
    SERIES_TICKERS = {
        "BTC": "KXBTC15M",
        "ETH": "KXETH15M",
        "SOL": "KXSOL15M",
        "XRP": "KXXRP15M",
    }

    # Kalshi 1-hour series tickers (hourly "Above/Below" markets)
    SERIES_TICKERS_1H = {
        "BTC": "KXBTCD",
        "ETH": "KXETHD",
        "SOL": "KXSOLD",
        "XRP": "KXXRPD",
    }

    # Polymarket 1-hour asset name mapping (for slug construction)
    POLY_ASSET_SLUGS_1H = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
        "XRP": "xrp",
    }

    def __init__(
        self,
        kalshi_key_id: str,
        kalshi_private_key_path: str,
        poly_private_key: str,
        skip_chainlink: bool = False,
    ):
        """
        Initialize WebSocket fetcher with exchange credentials.

        Args:
            kalshi_key_id: Kalshi API key ID
            kalshi_private_key_path: Path to Kalshi private key file
            poly_private_key: Polymarket wallet private key
            skip_chainlink: If True, don't start Chainlink price feed
        """
        # Initialize WebSocket manager (for real-time prices)
        self.ws_manager = WebSocketManager(
            kalshi_key_id=kalshi_key_id,
            kalshi_private_key_path=kalshi_private_key_path,
            poly_private_key=poly_private_key,
            skip_chainlink=skip_chainlink,
        )

        # Initialize Kalshi REST client (for order placement - WS doesn't support orders)
        self._kalshi_client = KalshiClient(kalshi_key_id, kalshi_private_key_path)

        # Cache for market data
        self._ticker_cache: Dict[str, tuple] = {}  # crypto -> (ticker, close_time, strike)
        self._poly_token_cache: Dict[str, tuple] = {}  # crypto -> (up_token, down_token, slug)
        self._cache_window: Dict[str, str] = {}  # crypto -> window when cached

        # Strike prices (from Chainlink feed at window start)
        self._kalshi_strikes: Dict[str, float] = {}
        self._poly_strikes: Dict[str, float] = {}

        # 1-hour market caches
        self._ticker_cache_1h: Dict[str, tuple] = {}        # crypto -> (ticker, close_time, strike)
        self._poly_token_cache_1h: Dict[str, tuple] = {}    # crypto -> (up_token, down_token, slug)
        self._kalshi_strikes_1h: Dict[str, float] = {}      # crypto -> 1h Kalshi strike
        self._poly_strikes_1h: Dict[str, float] = {}        # crypto -> 1h Poly strike (Chainlink at hour boundary)

        # Track if we've logged strikes for this window
        self._strike_logged_window: Dict[str, str] = {}

        # HTTP session for initial market lookups
        self._session = requests.Session()

        # Price update callbacks
        self._price_callbacks: List[Callable[[PriceUpdate], None]] = []

        # Track if started
        self._started = False

    def start(self):
        """Start WebSocket connections."""
        if self._started:
            return

        self.ws_manager.start()
        self._started = True

        # Register our own price update handler
        self.ws_manager.on_price_update(self._on_price_update)


        # Brief wait for connections to establish
        time.sleep(0.5)

        status = self.ws_manager.is_connected()

    def stop(self):
        """Stop WebSocket connections."""
        if not self._started:
            return

        self.ws_manager.stop()
        self._started = False

    def _on_price_update(self, update: PriceUpdate):
        """Handle incoming price updates."""
        for callback in self._price_callbacks:
            try:
                callback(update)
            except Exception as e:
                if DEBUG:
                    print(f"[ws-fetcher] Callback error: {e}")

    def on_price_update(self, callback: Callable[[PriceUpdate], None]):
        """Register a callback for price updates."""
        self._price_callbacks.append(callback)

    def _get_current_window(self) -> str:
        """Get current 15-minute window identifier (e.g., '14:30')."""
        now = datetime.utcnow()
        minute_window = (now.minute // 15) * 15
        return f"{now.hour:02d}:{minute_window:02d}"

    def _get_window_timestamp(self) -> int:
        """Get Unix timestamp for current 15-minute window start."""
        now = int(time.time())
        return (now // 900) * 900

    def _get_current_1h_window(self) -> str:
        """Get current 1-hour window identifier (e.g., '14:00')."""
        now = datetime.utcnow()
        return f"{now.hour:02d}:00"

    def _get_1h_window_timestamp(self) -> int:
        """Get Unix timestamp for current 1-hour window start."""
        now = int(time.time())
        return (now // 3600) * 3600

    def _build_poly_1h_slug(self, crypto: str) -> str:
        """
        Build Polymarket 1-hour event slug using Eastern Time.

        Slug format: {asset}-up-or-down-{month}-{day}-{hour}{ampm}-et
        Example: bitcoin-up-or-down-february-12-1pm-et
        """
        et = ZoneInfo("America/New_York")
        now_et = datetime.now(et)
        hour_start = now_et.replace(minute=0, second=0, microsecond=0)

        asset_name = self.POLY_ASSET_SLUGS_1H[crypto]        # "bitcoin"
        month_name = hour_start.strftime("%B").lower()         # "february"
        day = hour_start.day                                    # 12 (no zero pad)
        hour_12 = int(hour_start.strftime("%I"))                # 1 (no zero-pad)
        ampm = hour_start.strftime("%p").lower()                # "pm"

        return f"{asset_name}-up-or-down-{month_name}-{day}-{hour_12}{ampm}-et"

    def _fetch_kalshi_1h_market(self, crypto: str) -> Optional[tuple]:
        """
        Fetch Kalshi 1-hour market data for current hour window.

        Returns:
            Tuple of (ticker, close_time, strike) or None
        """
        current_1h_window = self._get_current_1h_window()

        # Check cache
        if crypto in self._ticker_cache_1h:
            cached = self._ticker_cache_1h[crypto]
            return cached

        series_ticker = self.SERIES_TICKERS_1H.get(crypto)
        if not series_ticker:
            return None

        try:
            markets = self._kalshi_client.get_markets(series_ticker, limit=20)
            if not markets:
                print(f"  [KALSHI-1H] {crypto}: No markets returned for {series_ticker}")
                return None

            from utils import parse_iso_dt
            now = datetime.now(timezone.utc)
            hour_start = now.replace(minute=0, second=0, microsecond=0)

            # Find the market closing soonest AFTER the current hour start
            best = None
            best_dt = None
            for m in markets:
                close_dt = parse_iso_dt(m.get("close_time", ""))
                if close_dt and close_dt > hour_start:
                    if best_dt is None or close_dt < best_dt:
                        best = m
                        best_dt = close_dt

            if not best:
                print(f"  [KALSHI-1H] {crypto}: No market found for current 1h window")
                return None

            ticker = best.get("ticker", "")
            close_time = best.get("close_time", "")
            strike = best.get("floor_strike") or best.get("strike_price")
            if strike is not None:
                strike = float(strike)

            # Fallback: market detail API
            if strike is None and ticker:
                try:
                    detail_r = self._kalshi_client._make_request("GET", f"/trade-api/v2/markets/{ticker}")
                    if detail_r and detail_r.status_code == 200:
                        detail = detail_r.json().get("market", {})
                        detail_strike = detail.get("floor_strike") or detail.get("strike_price")
                        if detail_strike is not None:
                            strike = float(detail_strike)
                except Exception:
                    pass

            self._ticker_cache_1h[crypto] = (ticker, close_time, strike)
            self._kalshi_strikes_1h[crypto] = strike
            print(f"  [KALSHI-1H] {crypto}: {ticker} (strike=${strike:,.2f})" if strike else f"  [KALSHI-1H] {crypto}: {ticker} (no strike)")
            return (ticker, close_time, strike)

        except Exception as e:
            print(f"  [KALSHI-1H] {crypto}: Fetch error: {e}")
            return None

    def _fetch_poly_1h_market(self, crypto: str) -> Optional[tuple]:
        """
        Fetch Polymarket 1-hour market data for current hour window.

        Returns:
            Tuple of (up_token, down_token, slug) or None
        """
        # Check cache
        if crypto in self._poly_token_cache_1h:
            return self._poly_token_cache_1h[crypto]

        slug = self._build_poly_1h_slug(crypto)

        try:
            import json as _json
            url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
            r = self._session.get(url, timeout=3)
            if r.status_code != 200:
                print(f"  [POLY-1H] {crypto}: Gamma API returned {r.status_code} for {slug}")
                return None

            event = r.json()
            markets = event.get("markets", [])
            if not markets:
                markets = [event]

            market = markets[0]
            if market.get("closed") or market.get("archived"):
                print(f"  [POLY-1H] {crypto}: Market closed/archived (slug={slug})")
                return None

            # Parse outcomes and tokens (same logic as 15-min)
            outcomes_raw = market.get("outcomes")
            tokens_raw = market.get("clobTokenIds")

            outcomes = _json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
            tokens = _json.loads(tokens_raw) if isinstance(tokens_raw, str) else (tokens_raw or [])

            if len(outcomes) < 2 or len(tokens) < 2:
                print(f"  [POLY-1H] {crypto}: Incomplete tokens (outcomes={len(outcomes)}, tokens={len(tokens)})")
                return None

            up_token, down_token = None, None
            for i, out in enumerate(outcomes):
                if i >= len(tokens):
                    continue
                t = str(out).upper()
                if "UP" in t or "YES" in t:
                    up_token = str(tokens[i])
                elif "DOWN" in t or "NO" in t:
                    down_token = str(tokens[i])

            if not up_token or not down_token:
                up_token = str(tokens[0])
                down_token = str(tokens[1])

            self._poly_token_cache_1h[crypto] = (up_token, down_token, slug)
            print(f"  [POLY-1H] {crypto}: {slug}")
            return (up_token, down_token, slug)

        except Exception as e:
            print(f"  [POLY-1H] {crypto}: Fetch error: {e}")
            return None

    def fetch_1h_poly_prices(self, up_token: str, down_token: str) -> Optional[dict]:
        """
        Fetch orderbook prices for 1-hour Poly market tokens via REST API.
        UP and DOWN fetched in parallel (like 15-min does).

        Returns dict with: up_ask, down_ask, up_depth, down_depth, up_asks, down_asks or None
        """
        import concurrent.futures

        def _fetch_book(token_id):
            try:
                r = requests.get(
                    "https://clob.polymarket.com/book",
                    params={"token_id": token_id},
                    timeout=3
                )
                if r.status_code == 200:
                    book = r.json()
                    if book.get("asks"):
                        asks = [(float(a["price"]), float(a.get("size", 0))) for a in book["asks"]]
                        asks.sort(key=lambda x: x[0])
                        return asks
            except Exception:
                pass
            return []

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                up_future = executor.submit(_fetch_book, up_token)
                down_future = executor.submit(_fetch_book, down_token)
                up_asks_list = up_future.result(timeout=4)
                down_asks_list = down_future.result(timeout=4)

            up_ask = up_asks_list[0][0] if up_asks_list else None
            down_ask = down_asks_list[0][0] if down_asks_list else None
            up_depth = int(sum(a[1] for a in up_asks_list[:3])) if up_asks_list else 0
            down_depth = int(sum(a[1] for a in down_asks_list[:3])) if down_asks_list else 0

            if up_ask is None and down_ask is None:
                return None

            return {
                "up_ask": up_ask,
                "down_ask": down_ask,
                "up_depth": up_depth,
                "down_depth": down_depth,
                "up_asks": up_asks_list,
                "down_asks": down_asks_list,
            }

        except Exception as e:
            print(f"  [1H] Error fetching 1h Poly prices: {e}")
            return None

    def subscribe_crypto_1h(self, crypto: str):
        """
        Subscribe to 1-hour market price updates for a cryptocurrency.
        Fetches both Kalshi 1h and Polymarket 1h market data.
        """
        # Fetch Kalshi 1h market
        kalshi_data = self._fetch_kalshi_1h_market(crypto)
        if kalshi_data:
            ticker, close_time, strike = kalshi_data
            if strike:
                self._kalshi_strikes_1h[crypto] = strike

        # Fetch Poly 1h market
        self._fetch_poly_1h_market(crypto)

    def _fetch_kalshi_market(self, crypto: str, require_strike: bool = False) -> Optional[tuple]:
        """
        Fetch Kalshi market data for current window.

        Args:
            crypto: Crypto symbol (BTC, ETH, SOL)
            require_strike: If True, skip cache if we don't have a strike price

        Returns:
            Tuple of (ticker, close_time, strike) or None
        """
        current_window = self._get_current_window()
        DEBUG and print(f"  [LOG] _fetch_kalshi_market({crypto}) - window={current_window}, require_strike={require_strike}")

        # Check cache - but skip if we need a strike and don't have one
        if crypto in self._ticker_cache:
            if self._cache_window.get(crypto) == current_window:
                cached = self._ticker_cache[crypto]
                cached_strike = cached[2] if len(cached) > 2 else None
                DEBUG and print(f"  [LOG] {crypto}: Cache hit - ticker={cached[0]}, strike={cached_strike}")
                if not require_strike or cached_strike is not None:
                    return cached
                DEBUG and print(f"  [LOG] {crypto}: Cache skipped (need strike but none cached)")

        series_ticker = self.SERIES_TICKERS.get(crypto)
        if not series_ticker:
            DEBUG and print(f"  [LOG] {crypto}: No series ticker found!")
            return None
        DEBUG and print(f"  [LOG] {crypto}: Fetching from API with series_ticker={series_ticker}")

        # Fetch from API (using authenticated KalshiClient)
        try:
            markets = self._kalshi_client.get_markets(series_ticker, limit=20)
            if not markets:
                print(f"  [KALSHI] {crypto}: No markets returned from API for {series_ticker}")
                return None

            # Log all market tickers for debugging
            all_tickers = [m.get("ticker", "") for m in markets]
            close_times = [m.get("close_time", "")[-8:] for m in markets[:5]]
            DEBUG and print(f"  [LOG] {crypto}: {len(markets)} markets: {all_tickers[:5]}, closes: {close_times}")

            # Get current time to find the right window
            from datetime import datetime, timezone, timedelta
            from utils import parse_iso_dt
            now = datetime.now(timezone.utc)

            # Find the market whose close_time is in the CURRENT 15-minute window
            # close_time is in UTC, so we compare directly
            # Current window ends at the next 15-minute boundary
            current_minute_bucket = (now.minute // 15) * 15
            window_end = now.replace(minute=current_minute_bucket, second=0, microsecond=0) + timedelta(minutes=15)
            window_start = window_end - timedelta(minutes=15)

            DEBUG and print(f"  [LOG] {crypto}: Looking for market closing between {window_start.strftime('%H:%M')} and {window_end.strftime('%H:%M')} UTC")

            # Find the market that closes in the current window (or next available)
            latest = None
            latest_dt = None

            for m in markets:
                ticker = m.get("ticker", "")
                close_time_str = m.get("close_time", "")
                close_dt = parse_iso_dt(close_time_str)

                if close_dt:
                    # Check if this market closes in the current or future window
                    DEBUG and print(f"  [LOG] {crypto}: Ticker {ticker} closes at {close_dt.strftime('%H:%M')} UTC")

                    # Market must close AFTER window start (not AT window start - that's the previous window)
                    # e.g., for window 17:15-17:30, we want market closing at 17:30, not 17:15
                    if close_dt > window_start:
                        # Prefer market closing soonest (current window)
                        if latest_dt is None or close_dt < latest_dt:
                            latest = m
                            latest_dt = close_dt
                            DEBUG and print(f"  [LOG] {crypto}: Selected market: {ticker} (closes {close_dt.strftime('%H:%M')} UTC)")
                    else:
                        DEBUG and print(f"  [LOG] {crypto}: Skipping {ticker} - closes at window start (previous window's market)")

            # If no future market found, the API hasn't created the new market yet
            if not latest:
                market_tickers = [m.get("ticker", "") for m in markets[:5]]
                print(f"  [KALSHI] {crypto}: No market found for current window (closes after {window_start.strftime('%H:%M')} UTC)")
                print(f"  [KALSHI] {crypto}: API only has previous window's market: {market_tickers}")
                print(f"  [KALSHI] {crypto}: Waiting for Kalshi to create new market - will retry on next scan")
                return None

            ticker = latest.get("ticker", "")
            close_time = latest.get("close_time", "")

            # Get strike from API response (preferred) or extract from ticker (fallback)
            strike = latest.get("floor_strike") or latest.get("strike_price")
            if strike is None:
                # Fallback 1: try to extract from ticker
                strike = self._extract_strike_from_ticker(ticker, crypto)
            else:
                strike = float(strike)

            # Fallback 2: try market detail API for yes_sub_title (e.g., "Above $87.75")
            if strike is None and ticker:
                try:
                    import re as _re
                    detail_r = self._kalshi_client._make_request("GET", f"/trade-api/v2/markets/{ticker}")
                    if detail_r and detail_r.status_code == 200:
                        detail = detail_r.json().get("market", {})
                        detail_strike = detail.get("floor_strike") or detail.get("strike_price")
                        if detail_strike is not None:
                            strike = float(detail_strike)
                            DEBUG and print(f"  [LOG] {crypto}: Strike from market detail: ${strike:,.2f}")
                        else:
                            # Try yes_sub_title (e.g., "Above $87.75")
                            yes_sub = detail.get("yes_sub_title") or ""
                            if yes_sub:
                                match = _re.search(r'\$([0-9,]+\.?\d*)', yes_sub)
                                if match:
                                    strike = float(match.group(1).replace(',', ''))
                                    DEBUG and print(f"  [LOG] {crypto}: Strike from yes_sub_title '{yes_sub}': ${strike:,.2f}")
                except Exception as e:
                    DEBUG and print(f"  [LOG] {crypto}: Market detail fallback error: {e}")

            # Always cache ticker data (needed for snapshot even without strike)
            DEBUG and print(f"  [LOG] {crypto}: Caching ticker={ticker}, strike={strike}")
            self._ticker_cache[crypto] = (ticker, close_time, strike)
            self._cache_window[crypto] = current_window

            if strike is None:
                # Log that we'll need to get strike later
                print(f"  [KALSHI] {crypto}: No strike in API - will retry via market details")

            DEBUG and print(f"  [LOG] {crypto}: _fetch_kalshi_market returning ticker={ticker}")
            return (ticker, close_time, strike)

        except Exception as e:
            print(f"[ws-fetcher] Kalshi market fetch error: {e}")
            return None

    def _extract_strike_from_ticker(self, ticker: str, crypto: str) -> Optional[float]:
        """Extract strike price from Kalshi ticker."""
        import re
        match = re.search(r'B(\d+)$', ticker)
        if match:
            try:
                strike = float(match.group(1))
                return strike
            except ValueError:
                pass
        return None

    def _fetch_strike_with_retry(self, crypto: str, max_retries: int = 3) -> tuple:
        """
        Fetch strike prices with retry logic using multiple sources.

        Tries in order:
        1. Kalshi API (floor_strike/strike_price)
        2. Kalshi market details API

        Args:
            crypto: Crypto symbol (BTC, ETH, SOL)
            max_retries: Max retry attempts

        Returns:
            Tuple of (kalshi_strike, poly_strike)
        """
        kalshi_strike = None
        poly_strike = None

        for attempt in range(max_retries):
            if attempt > 0:
                wait_time = 0.5 * (attempt + 1)  # 0.5s, 1s fast backoff
                time.sleep(wait_time)

            # Method 1: Try Kalshi API with fresh request
            if kalshi_strike is None:
                series_ticker = self.SERIES_TICKERS.get(crypto)
                if series_ticker:
                    try:
                        markets = self._kalshi_client.get_markets(series_ticker, limit=20)
                        if markets:
                            # Find latest market
                            from utils import parse_iso_dt
                            latest = None
                            latest_dt = None
                            for m in markets:
                                dt = parse_iso_dt(m.get("close_time", ""))
                                if dt and (latest_dt is None or dt > latest_dt):
                                    latest = m
                                    latest_dt = dt

                            if latest:
                                # Try floor_strike first
                                strike = latest.get("floor_strike") or latest.get("strike_price")
                                if strike is not None:
                                    kalshi_strike = float(strike)
                                    if DEBUG:
                                        print(f"  [STRIKE] {crypto} Kalshi from API: ${kalshi_strike:,.2f}")

                                # If not found, try getting full market details
                                if kalshi_strike is None:
                                    ticker = latest.get("ticker")
                                    if ticker:
                                        try:
                                            detail_r = self._kalshi_client._make_request("GET", f"/trade-api/v2/markets/{ticker}")
                                            if detail_r and detail_r.status_code == 200:
                                                detail = detail_r.json().get("market", {})
                                                strike = detail.get("floor_strike") or detail.get("strike_price") or detail.get("yes_sub_title")
                                                if isinstance(strike, str) and '$' in strike:
                                                    import re
                                                    match = re.search(r'\$([0-9,]+\.?\d*)', strike)
                                                    if match:
                                                        kalshi_strike = float(match.group(1).replace(',', ''))
                                                elif strike is not None:
                                                    try:
                                                        kalshi_strike = float(strike)
                                                    except (ValueError, TypeError):
                                                        pass
                                                if kalshi_strike and DEBUG:
                                                    print(f"  [STRIKE] {crypto} Kalshi from market details: ${kalshi_strike:,.2f}")
                                        except Exception as e:
                                            if DEBUG:
                                                print(f"  [STRIKE] Market detail fetch error: {e}")
                    except Exception as e:
                        if DEBUG:
                            print(f"  [STRIKE] Kalshi API error: {e}")

            # If we have both strikes, we're done
            if kalshi_strike is not None and poly_strike is not None:
                break

            # If we have Kalshi strike, that's all we can get from this method
            # Poly strike must come from Chainlink — never substitute Kalshi for Poly
            if kalshi_strike is not None:
                break

        return kalshi_strike, poly_strike

    def refresh_strikes(self, crypto: str) -> tuple:
        """
        Force refresh strike prices for a crypto (call when strikes are missing).

        Args:
            crypto: Crypto symbol

        Returns:
            Tuple of (kalshi_strike, poly_strike)
        """
        print(f"  [STRIKE] Force refresh for {crypto}...")

        # Clear any cached strikes
        if crypto in self._kalshi_strikes:
            del self._kalshi_strikes[crypto]
        if crypto in self._poly_strikes:
            del self._poly_strikes[crypto]

        # Fetch with retry
        k_strike, p_strike = self._fetch_strike_with_retry(crypto, max_retries=3)

        # Store results
        if k_strike:
            self._kalshi_strikes[crypto] = k_strike
        if p_strike:
            self._poly_strikes[crypto] = p_strike

        return k_strike, p_strike

    def _fetch_poly_market(self, crypto: str, skip_scraper: bool = False) -> Optional[tuple]:
        """
        Fetch Polymarket market data for current window.

        Args:
            crypto: Crypto symbol
            skip_scraper: Ignored (kept for call-site compat). Scraper removed.

        Returns:
            Tuple of (up_token, down_token, slug, strike) or None
        """
        current_window = self._get_current_window()

        # Use cache if available for this window
        if crypto in self._poly_token_cache:
            if self._cache_window.get(f"poly_{crypto}") == current_window:
                cached = self._poly_token_cache[crypto]
                return (*cached, self._poly_strikes.get(crypto))

        # Build slug for current window
        window_ts = self._get_window_timestamp()
        slug = f"{crypto.lower()}-updown-15m-{window_ts}"

        try:
            url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
            r = self._session.get(url, timeout=2)
            if r.status_code != 200:
                return None

            event = r.json()
            markets = event.get("markets", [])
            if not markets:
                markets = [event]

            market = markets[0]

            # Check if market is active
            if market.get("closed") or market.get("archived"):
                print(f"  [POLY]   {crypto}: Market closed/archived (slug={slug})")
                return None

            # Parse outcomes and tokens
            import json
            outcomes_raw = market.get("outcomes")
            tokens_raw = market.get("clobTokenIds")

            outcomes = []
            if isinstance(outcomes_raw, str):
                try:
                    outcomes = json.loads(outcomes_raw)
                except:
                    outcomes = []
            elif isinstance(outcomes_raw, list):
                outcomes = outcomes_raw

            tokens = []
            if isinstance(tokens_raw, str):
                try:
                    tokens = json.loads(tokens_raw)
                except:
                    tokens = []
            elif isinstance(tokens_raw, list):
                tokens = tokens_raw

            if len(outcomes) < 2 or len(tokens) < 2:
                print(f"  [POLY]   {crypto}: Incomplete tokens (outcomes={len(outcomes)}, tokens={len(tokens)})")
                return None

            # Find UP and DOWN tokens
            up_token, down_token = None, None
            for i, out in enumerate(outcomes):
                if i >= len(tokens):
                    continue
                t = str(out).upper()
                if "UP" in t or "YES" in t:
                    up_token = str(tokens[i])
                elif "DOWN" in t or "NO" in t:
                    down_token = str(tokens[i])

            if not up_token or not down_token:
                up_token = str(tokens[0])
                down_token = str(tokens[1])

            # Strike price comes from other sources (Chainlink, Kalshi fallback)
            # not from Gamma API — use cached value if available
            poly_strike = self._poly_strikes.get(crypto)

            # Cache result
            self._poly_token_cache[crypto] = (up_token, down_token, slug)
            self._cache_window[f"poly_{crypto}"] = current_window

            return (up_token, down_token, slug, poly_strike or self._poly_strikes.get(crypto))

        except Exception as e:
            print(f"[ws-fetcher] Poly market fetch error: {e}")
            return None

    def fetch_5m_market(self, crypto: str = "BTC", skip_scraper: bool = False) -> Optional[dict]:
        """
        Fetch Polymarket 5-minute market data for the last 5-min window
        of the current 15-minute window (settles at same time as 15-min).

        Uses Gamma API for token discovery. Strike must be provided by caller
        from Chainlink feed.

        Args:
            crypto: Crypto symbol (default BTC)
            skip_scraper: Ignored (kept for call-site compat). Scraper removed.

        Returns dict with: up_token, down_token, slug, strike, or None
        """
        import json

        # The last 5-min window starts 600s into the 15-min window
        fifteen_min_ts = self._get_window_timestamp()
        five_min_ts = fifteen_min_ts + 600
        slug = f"{crypto.lower()}-updown-5m-{five_min_ts}"

        try:
            url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
            r = self._session.get(url, timeout=3)
            if r.status_code != 200:
                print(f"  [5M] {crypto}: Gamma API returned {r.status_code} for {slug}")
                return None

            event = r.json()
            markets = event.get("markets", [])
            if not markets:
                markets = [event]

            market = markets[0]
            if market.get("closed") or market.get("archived"):
                return None

            # Parse outcomes and tokens
            outcomes_raw = market.get("outcomes")
            tokens_raw = market.get("clobTokenIds")

            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
            tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else (tokens_raw or [])

            if len(outcomes) < 2 or len(tokens) < 2:
                return None

            up_token, down_token = None, None
            for i, out in enumerate(outcomes):
                if i >= len(tokens):
                    continue
                t = str(out).upper()
                if "UP" in t or "YES" in t:
                    up_token = str(tokens[i])
                elif "DOWN" in t or "NO" in t:
                    down_token = str(tokens[i])

            if not up_token or not down_token:
                up_token = str(tokens[0])
                down_token = str(tokens[1])

            # Strike will be set by caller from Chainlink feed
            strike = None

            print(f"  [5M] {crypto}: Found 5-min market (slug={slug}, strike=${strike:,.2f})" if strike else f"  [5M] {crypto}: Found 5-min market (slug={slug}, strike=from-caller)")

            return {
                "up_token": up_token,
                "down_token": down_token,
                "slug": slug,
                "strike": strike,
            }

        except Exception as e:
            print(f"  [5M] {crypto}: Error fetching 5-min market: {e}")
            return None

    def fetch_5m_prices(self, up_token: str, down_token: str) -> Optional[dict]:
        """
        Fetch orderbook prices for 5-min market tokens via REST API.

        Returns dict with: up_ask, down_ask, up_depth, down_depth, or None
        """
        try:
            base_url = "https://clob.polymarket.com"

            up_resp = self._session.get(f"{base_url}/book?token_id={up_token}", timeout=3)
            down_resp = self._session.get(f"{base_url}/book?token_id={down_token}", timeout=3)

            up_ask, down_ask = None, None
            up_depth, down_depth = 0, 0

            if up_resp.status_code == 200:
                up_book = up_resp.json()
                if up_book.get("asks"):
                    asks = [(float(a["price"]), float(a.get("size", 0))) for a in up_book["asks"]]
                    asks.sort(key=lambda x: x[0])
                    up_ask = asks[0][0] if asks else None
                    up_depth = int(sum(a[1] for a in asks[:3]))

            if down_resp.status_code == 200:
                down_book = down_resp.json()
                if down_book.get("asks"):
                    asks = [(float(a["price"]), float(a.get("size", 0))) for a in down_book["asks"]]
                    asks.sort(key=lambda x: x[0])
                    down_ask = asks[0][0] if asks else None
                    down_depth = int(sum(a[1] for a in asks[:3]))

            if up_ask is None and down_ask is None:
                return None

            return {
                "up_ask": up_ask,
                "down_ask": down_ask,
                "up_depth": up_depth,
                "down_depth": down_depth,
            }

        except Exception as e:
            print(f"  [5M] Error fetching 5-min prices: {e}")
            return None

    def subscribe_crypto(self, crypto: str, kalshi_strike: float = None, poly_strike: float = None, skip_scraper: bool = False):
        """
        Subscribe to price updates for a cryptocurrency.

        Args:
            crypto: Crypto symbol (BTC, ETH, SOL)
            kalshi_strike: Optional Kalshi strike price
            poly_strike: Optional Polymarket strike price
            skip_scraper: Ignored (kept for call-site compat). Scraper removed.
        """
        DEBUG and print(f"  [LOG] subscribe_crypto({crypto}) called")

        # Store strike prices if provided
        if kalshi_strike:
            self._kalshi_strikes[crypto] = kalshi_strike
        if poly_strike:
            self._poly_strikes[crypto] = poly_strike

        # Fetch Kalshi market and subscribe
        DEBUG and print(f"  [LOG] {crypto}: Calling _fetch_kalshi_market(require_strike=False)")
        kalshi_data = self._fetch_kalshi_market(crypto, require_strike=False)
        DEBUG and print(f"  [LOG] {crypto}: _fetch_kalshi_market returned: {kalshi_data}")
        if kalshi_data:
            ticker, close_time, extracted_strike = kalshi_data
            strike = kalshi_strike or extracted_strike
            if strike:
                self._kalshi_strikes[crypto] = strike
            DEBUG and print(f"  [LOG] {crypto}: Subscribing to Kalshi ticker={ticker}, strike={strike}")
            self.ws_manager.subscribe_kalshi(ticker, crypto, strike=strike)
            print(f"  [KALSHI] {crypto}: {ticker}")
        else:
            DEBUG and print(f"  [LOG] {crypto}: kalshi_data is None - no market found!")
            print(f"  [KALSHI] {crypto}: No market found")

        # Fetch Poly market and queue subscription (don't send yet)
        poly_data = self._fetch_poly_market(crypto, skip_scraper=skip_scraper)
        if poly_data:
            up_token, down_token, slug, poly_extracted_strike = poly_data
            strike = poly_strike or poly_extracted_strike or self._poly_strikes.get(crypto)

            # If Poly strike is missing, log it — do NOT use Kalshi as substitute
            if strike is None:
                print(f"  [POLY] {crypto}: No Chainlink strike — will skip until next window")

            print(f"  [POLY]   {crypto}: {slug}")
            self.ws_manager.subscribe_polymarket_both(up_token, down_token, crypto, strike=strike)

        # Update ticker cache if we now have the strike (needed for trade execution)
        if kalshi_data and crypto in self._kalshi_strikes:
            ticker, close_time, _ = kalshi_data
            kalshi_strike_found = self._kalshi_strikes[crypto]
            # Update cache with the strike we found
            self._ticker_cache[crypto] = (ticker, close_time, kalshi_strike_found)
            self._cache_window[crypto] = self._get_current_window()

    def flush_subscriptions(self, cryptos: list = None):
        """
        Send all queued Polymarket subscriptions and pre-fetch initial prices.

        Must be called after subscribe_crypto() for all cryptos.
        This is required because Polymarket WebSocket only supports one
        subscription message per connection.

        Args:
            cryptos: List of crypto symbols to pre-fetch (default: all subscribed)
        """
        self.ws_manager.flush_polymarket_subscriptions()

        # Give WebSocket a moment to process subscriptions
        time.sleep(0.5)

        # Pre-fetch initial prices via REST API (don't wait for WebSocket updates)
        # This ensures we have prices immediately
        self.ws_manager.prefetch_all_prices(cryptos)

    def enable_price_logs(self):
        """Enable price logging (call after all subscriptions are ready)."""
        self.ws_manager.enable_price_logs()

    def get_snapshot(self, crypto: str) -> Optional[Snapshot]:
        """
        Get current market snapshot for a cryptocurrency.

        This uses the latest prices from WebSocket streams instead of making
        REST API calls, providing much faster access to market data.

        Args:
            crypto: Crypto symbol (BTC, ETH, SOL)

        Returns:
            Snapshot with current prices, or None if data unavailable
        """
        kalshi_price = self.ws_manager.get_kalshi_price(crypto)
        poly_price = self.ws_manager.get_polymarket_price(crypto)

        if not kalshi_price or not poly_price:
            # Only log once per crypto to avoid spam
            if not hasattr(self, '_missing_logged'):
                self._missing_logged = set()
            if crypto not in self._missing_logged:
                missing = []
                if not kalshi_price:
                    missing.append("Kalshi")
                if not poly_price:
                    missing.append("Polymarket")
                print(f"  [WS] {crypto}: Missing prices from {', '.join(missing)}")
                self._missing_logged.add(crypto)
            return None

        # Get cached market data
        kalshi_data = self._ticker_cache.get(crypto)
        poly_data = self._poly_token_cache.get(crypto)

        if not kalshi_data or not poly_data:
            return None

        ticker, close_time, _ = kalshi_data
        up_token, down_token, slug = poly_data

        # Calculate ASK prices (cost to BUY) for arbitrage
        # k_up = cost to buy YES on Kalshi (yes_ask)
        # k_down = cost to buy NO on Kalshi (no_ask)
        # IMPORTANT: Don't use 0.5 fallback - it creates fake "edges"

        # Kalshi: yes_ask = 100 - no_bid, no_ask = 100 - yes_bid
        k_up = kalshi_price.yes_ask
        k_down = kalshi_price.no_ask

        # If ask prices not set, try to compute from bid prices
        if k_up is None and kalshi_price.no_price is not None:
            k_up = 1.0 - kalshi_price.no_price
        if k_down is None and kalshi_price.yes_price is not None:
            k_down = 1.0 - kalshi_price.yes_price

        # For Polymarket, use the ask prices (cost to buy)
        p_up = poly_price.yes_ask
        p_down = poly_price.no_ask

        # Fall back to bid prices only if ask not available (less accurate)
        if p_up is None:
            p_up = poly_price.yes_price
        if p_down is None:
            p_down = poly_price.no_price

        # Check if we have enough prices for at least one strategy
        # K_UP + P_DOWN needs: k_up and p_down
        # K_DOWN + P_UP needs: k_down and p_up
        can_trade_k_up_p_down = (k_up is not None and p_down is not None)
        can_trade_k_down_p_up = (k_down is not None and p_up is not None)

        if not can_trade_k_up_p_down and not can_trade_k_down_p_up:
            missing = []
            if k_up is None:
                missing.append("k_up")
            if k_down is None:
                missing.append("k_down")
            if p_up is None:
                missing.append("p_up")
            if p_down is None:
                missing.append("p_down")
            print(f"  [WS] {crypto}: Missing prices for both strategies: {', '.join(missing)}")
            return None

        # Log strikes once per window
        current_window = self._get_current_window()
        kalshi_strike = self._kalshi_strikes.get(crypto)
        poly_strike = self._poly_strikes.get(crypto)

        # Update window tracking (strikes already shown in startup summary)
        if self._strike_logged_window.get(crypto) != current_window:
            self._strike_logged_window[crypto] = current_window

        return Snapshot(
            crypto=crypto,
            kalshi_ticker=ticker,
            kalshi_close_time=close_time,
            poly_slug=slug,
            poly_question=f"{crypto} Up/Down",
            k_up=k_up,
            k_down=k_down,
            p_up=p_up,
            p_down=p_down,
            poly_up_token=up_token,
            poly_down_token=down_token,
            kalshi_strike=kalshi_strike,
            poly_strike=poly_strike,
        )

    def fetch(self, crypto: str) -> Optional[Snapshot]:
        """
        Fetch market snapshot (compatibility method with REST fetcher).

        This is an alias for get_snapshot() to match the REST fetcher interface.
        """
        return self.get_snapshot(crypto)

    def fetch_all(self, cryptos: List[str]) -> Dict[str, Optional[Snapshot]]:
        """
        Fetch snapshots for all cryptos.

        Args:
            cryptos: List of crypto symbols

        Returns:
            Dict mapping crypto symbol to Snapshot
        """
        results = {}
        for crypto in cryptos:
            results[crypto] = self.get_snapshot(crypto)
        return results

    def place_order(
        self,
        crypto: str,
        exchange: str,
        side: str,
        quantity: int,
        price: float,
    ) -> OrderResult:
        """
        Place an order on an exchange.

        Args:
            crypto: Crypto symbol
            exchange: "kalshi" or "polymarket"
            side: For Kalshi: "yes"/"no", For Poly: "up"/"down"
            quantity: Number of contracts
            price: Price (cents for Kalshi, 0-1 for Poly)

        Returns:
            OrderResult with success status
        """
        if exchange.lower() == "kalshi":
            return self.ws_manager.place_kalshi_order(crypto, side, quantity, int(price))
        else:
            return self.ws_manager.place_poly_order(crypto, side, quantity, price)

    def place_arb_orders(
        self,
        crypto: str,
        kalshi_side: str,
        poly_direction: str,
        quantity: int,
        kalshi_price_cents: int,
        poly_price: float,
    ) -> tuple:
        """
        Place arbitrage orders on both exchanges simultaneously.

        Args:
            crypto: Crypto symbol
            kalshi_side: "yes" or "no" for Kalshi
            poly_direction: "up" or "down" for Polymarket
            quantity: Number of contracts
            kalshi_price_cents: Kalshi price in cents
            poly_price: Polymarket price (0-1)

        Returns:
            Tuple of (kalshi_result, poly_result)
        """
        import concurrent.futures

        # Get Kalshi ticker from cache
        kalshi_ticker = None
        if crypto in self._ticker_cache:
            kalshi_ticker = self._ticker_cache[crypto][0]

        if not kalshi_ticker:
            return (
                OrderResult(success=False, error=f"No Kalshi ticker for {crypto}"),
                OrderResult(success=False, error="Kalshi order failed")
            )

        def place_kalshi():
            # Use REST API for Kalshi orders (WebSocket doesn't support orders)
            result = self._kalshi_client.place_order(
                kalshi_ticker, kalshi_side, quantity, kalshi_price_cents
            )
            if result:
                return OrderResult(
                    success=True,
                    order_id=result.get("order", {}).get("order_id"),
                    filled_qty=result.get("order", {}).get("filled_count", 0),
                    raw_response=result
                )
            return OrderResult(success=False, error="Kalshi order failed")

        def place_poly():
            return self.ws_manager.place_poly_order(crypto, poly_direction, quantity, poly_price)

        # Execute both orders in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            kalshi_future = executor.submit(place_kalshi)
            poly_future = executor.submit(place_poly)

            kalshi_result = kalshi_future.result()
            poly_result = poly_future.result()

        return kalshi_result, poly_result

    def is_connected(self) -> Dict[str, bool]:
        """Check WebSocket connection status."""
        return self.ws_manager.is_connected()

    def clear_cache(self):
        """Clear market data cache (call when window changes)."""
        self._ticker_cache.clear()
        self._poly_token_cache.clear()
        self._cache_window.clear()
        # Also clear strike refresh timestamps so we can retry in new window
        if hasattr(self, '_last_strike_refresh'):
            self._last_strike_refresh.clear()
        # Clear strikes so they get re-fetched for new window
        self._kalshi_strikes.clear()
        self._poly_strikes.clear()


# Example usage
if __name__ == "__main__":
    import os

    # Test the WebSocket fetcher
    kalshi_key = os.environ.get("KALSHI_API_KEY")
    kalshi_key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    poly_key = os.environ.get("POLYMARKET_PRIVATE_KEY")

    if not all([kalshi_key, kalshi_key_path, poly_key]):
        print("Missing environment variables:")
        print("  KALSHI_API_KEY, KALSHI_PRIVATE_KEY_PATH, POLYMARKET_PRIVATE_KEY")
        exit(1)

    fetcher = WebSocketFetcher(kalshi_key, kalshi_key_path, poly_key)

    def on_update(update: PriceUpdate):
        print(f"[{update.exchange}] {update.crypto}: yes={update.yes_price}, no={update.no_price}")

    fetcher.on_price_update(on_update)
    fetcher.start()

    # Subscribe to BTC
    fetcher.subscribe_crypto("BTC")

    try:
        while True:
            time.sleep(5)
            snapshot = fetcher.get_snapshot("BTC")
            if snapshot:
                print(f"\nSnapshot: k_up={snapshot.k_up:.4f}, k_down={snapshot.k_down:.4f}")
                print(f"          p_up={snapshot.p_up:.4f}, p_down={snapshot.p_down:.4f}")
            status = fetcher.is_connected()
            print(f"Status: {status}")
    except KeyboardInterrupt:
        fetcher.stop()
