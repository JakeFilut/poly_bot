"""
Snapshot fetcher for gathering market data from exchanges.
"""

import json
import requests
import threading
from web3 import Web3
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

try:
    import websocket
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False

try:
    import websockets
    import asyncio
    HAS_WEBSOCKETS_ASYNC = True
except ImportError:
    HAS_WEBSOCKETS_ASYNC = False

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

import time as _time_mod

from kalshi_client import KalshiClient
from polymarket_client import PolymarketClient
from models import Snapshot
from utils import parse_iso_dt


def fetch_rtds_chainlink_price(crypto: str, timeout: float = 8.0, require_window_ts: bool = False) -> Optional[float]:
    """
    Standalone function to fetch Chainlink price from Polymarket's RTDS WebSocket.

    Uses the async websockets library (same as websocket_client.py) to connect
    to wss://ws-live-data.polymarket.com which avoids Cloudflare 403 blocks.

    Args:
        crypto: Cryptocurrency symbol (BTC, ETH, SOL, XRP)
        timeout: Max seconds to wait for price
        require_window_ts: If True, only accept price where
            payload.timestamp == windowStart * 1000 (exact t+1s price).
            If False, accept any price for the symbol.

    Returns:
        Price in USD, or None if unavailable
    """
    if not HAS_WEBSOCKETS_ASYNC:
        return None

    symbol_map = {
        "BTC": "btc/usd",
        "ETH": "eth/usd",
        "SOL": "sol/usd",
        "XRP": "xrp/usd",
    }
    symbol = symbol_map.get(crypto)
    if not symbol:
        return None

    # Calculate the exact windowStart timestamp in milliseconds
    now = int(_time_mod.time())
    window_start_ms = ((now // 900) * 900) * 1000

    def _check_item(item):
        """Check if item matches symbol (and optionally exact window timestamp)."""
        # Handle "payload" wrapper format
        if "payload" in item and isinstance(item["payload"], dict):
            item = item["payload"]

        if item.get("symbol", "").lower() != symbol.lower():
            return None

        if require_window_ts:
            payload_ts = item.get("timestamp")
            if payload_ts is None or int(payload_ts) != window_start_ms:
                return None

        val = float(item.get("value") or item.get("price", 0))
        return val if val > 0 else None

    async def _fetch():
        try:
            ws = await asyncio.wait_for(
                websockets.connect(
                    "wss://ws-live-data.polymarket.com",
                    ping_interval=10,
                    ping_timeout=30,
                ),
                timeout=timeout
            )
        except Exception as e:
            print(f"    [RTDS] {crypto}: connection failed: {e}")
            return None

        try:
            # Subscribe
            sub_msg = {
                "action": "subscribe",
                "subscriptions": [{
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": json.dumps({"symbol": symbol})
                }]
            }
            await ws.send(json.dumps(sub_msg))

            # Listen for messages until timeout
            deadline = _time_mod.time() + timeout
            msg_count = 0
            while _time_mod.time() < deadline:
                remaining = deadline - _time_mod.time()
                if remaining <= 0:
                    break
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break

                msg_count += 1
                try:
                    data = json.loads(raw)
                    if msg_count <= 2:
                        preview = str(raw)[:300]
                        print(f"    [RTDS-DBG] {crypto} msg#{msg_count}: {preview}")
                    items = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
                    for item in items:
                        val = _check_item(item)
                        if val is not None:
                            await ws.close()
                            return val
                except Exception:
                    pass

            if msg_count == 0:
                print(f"    [RTDS-DBG] {crypto}: timeout after {timeout}s with 0 messages")
            await ws.close()
        except Exception as e:
            print(f"    [RTDS] {crypto}: error: {e}")
            try:
                await ws.close()
            except Exception:
                pass

        return None

    # Run the async function in a new event loop on a thread
    result = {"price": None}

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result["price"] = loop.run_until_complete(_fetch())
        finally:
            loop.close()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=timeout + 2)  # Extra 2s for connection overhead
    return result["price"]

# Import web scraper for exact strike prices
try:
    from web_scraper import get_scraper, scrape_strike_prices
    HAS_SCRAPER = True
except ImportError:
    HAS_SCRAPER = False
    print("[fetcher] WARNING: web_scraper not available")

# Import RTDS logging duration config
try:
    from config import RTDS_LOG_DURATION_SECONDS
except ImportError:
    RTDS_LOG_DURATION_SECONDS = 30


class SnapshotFetcher:
    """Fetches market snapshots from Kalshi and Polymarket."""

    # Kalshi series tickers for crypto up/down markets
    SERIES_TICKERS = {
        "BTC": "KXBTC15M",
        "ETH": "KXETH15M",
        "SOL": "KXSOL15M",
        "XRP": "KXXRP15M",
    }

    # Chainlink Price Feed addresses on Polygon mainnet
    # These are the same feeds Polymarket uses for resolution
    CHAINLINK_FEEDS = {
        "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",  # BTC/USD
        "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",  # ETH/USD
        "SOL": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC",  # SOL/USD
    }

    # Chainlink Aggregator ABI (just the functions we need)
    CHAINLINK_ABI = [
        {
            "inputs": [],
            "name": "latestRoundData",
            "outputs": [
                {"name": "roundId", "type": "uint80"},
                {"name": "answer", "type": "int256"},
                {"name": "startedAt", "type": "uint256"},
                {"name": "updatedAt", "type": "uint256"},
                {"name": "answeredInRound", "type": "uint80"}
            ],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"name": "_roundId", "type": "uint80"}],
            "name": "getRoundData",
            "outputs": [
                {"name": "roundId", "type": "uint80"},
                {"name": "answer", "type": "int256"},
                {"name": "startedAt", "type": "uint256"},
                {"name": "updatedAt", "type": "uint256"},
                {"name": "answeredInRound", "type": "uint80"}
            ],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "stateMutability": "view",
            "type": "function"
        }
    ]

    def __init__(self, kalshi: KalshiClient, poly: PolymarketClient):
        self.kalshi = kalshi
        self.poly = poly
        # Cache market tickers - refresh when market window changes
        self._ticker_cache: Dict[str, tuple] = {}
        self._ticker_cache_window: Dict[str, str] = {}  # Track which 15-min window the cache is for
        self._strike_logged_window: Dict[str, str] = {}  # Track if we've logged strikes for this window
        # Cache for Poly strike prices (fetched from Chainlink at window start)
        self._poly_strike_cache: Dict[str, float] = {}  # crypto -> strike price
        self._poly_strike_window: Dict[str, str] = {}   # crypto -> window when fetched
        # Cache for scraped strike prices (from website)
        self._scraped_strike_cache: Dict[str, Dict[str, float]] = {}  # {crypto: {kalshi: X, poly: Y}}
        self._scraped_strike_window: Dict[str, str] = {}  # crypto -> window when scraped
        # RTDS (Polymarket WebSocket) price cache for window-start capture
        self._rtds_strike_cache: Dict[str, float] = {}   # crypto -> RTDS price at window start
        self._rtds_strike_window: Dict[str, str] = {}    # crypto -> window when captured
        self._rtds_log_active: Dict[str, bool] = {}      # crypto -> whether we're still logging
        self._rtds_log_start: Dict[str, float] = {}      # crypto -> when logging started (unix ts)
        self._rtds_log_prices: Dict[str, list] = {}      # crypto -> list of (ts, price) for delay analysis
        # Web3 connection for Chainlink price feeds
        self._w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
        if not self._w3.is_connected():
            self._w3 = Web3(Web3.HTTPProvider("https://rpc-mainnet.matic.quiknode.pro"))

    def _scrape_strike_prices(self, crypto: str) -> tuple:
        """
        Scrape exact strike prices from Kalshi and Polymarket websites.
        Returns (kalshi_strike, poly_strike) tuple.
        Uses caching to avoid repeated scrapes in the same window.
        """
        if not HAS_SCRAPER:
            return (None, None)

        current_window = self._get_current_window()

        # Check cache
        if crypto in self._scraped_strike_cache:
            cached_window = self._scraped_strike_window.get(crypto)
            if cached_window == current_window:
                cached = self._scraped_strike_cache[crypto]
                return (cached.get('kalshi'), cached.get('poly'))

        # Scrape fresh
        try:
            kalshi_strike, poly_strike = scrape_strike_prices(crypto)

            # Cache the results
            self._scraped_strike_cache[crypto] = {
                'kalshi': kalshi_strike,
                'poly': poly_strike
            }
            self._scraped_strike_window[crypto] = current_window

            return (kalshi_strike, poly_strike)
        except Exception as e:
            print(f"[fetcher] Scrape error for {crypto}: {e}")
            return (None, None)

    def _fetch_chainlink_price(self, crypto: str) -> Optional[float]:
        """Fetch current price from Chainlink price feed on Polygon with retry."""
        feed_address = self.CHAINLINK_FEEDS.get(crypto)
        if not feed_address:
            return None

        # Try multiple RPC endpoints
        rpc_endpoints = [
            "https://polygon-rpc.com",
            "https://rpc-mainnet.matic.quiknode.pro",
            "https://polygon-mainnet.g.alchemy.com/v2/demo",
            "https://rpc.ankr.com/polygon",
        ]

        for rpc_url in rpc_endpoints:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 10}))
                if not w3.is_connected():
                    continue

                contract = w3.eth.contract(
                    address=Web3.to_checksum_address(feed_address),
                    abi=self.CHAINLINK_ABI
                )

                # Get decimals
                decimals = contract.functions.decimals().call()

                # Get latest price
                round_data = contract.functions.latestRoundData().call()
                answer = round_data[1]  # answer is at index 1

                # Convert to USD (divide by 10^decimals)
                price = float(answer) / (10 ** decimals)

                return price
            except Exception as e:
                error_str = str(e)
                if "rate limit" in error_str.lower() or "too many requests" in error_str.lower():
                    continue  # Try next RPC
                # For other errors, also try next RPC
                continue

        print(f"[fetcher] All RPCs failed for {crypto} Chainlink price")
        return None

    def _fetch_chainlink_price_at_timestamp(self, crypto: str, target_timestamp: int) -> Optional[float]:
        """
        Fetch Chainlink price that was valid at a specific timestamp.

        Searches backwards through rounds to find the price update that was
        active at the target timestamp (last update BEFORE or AT target time).

        Args:
            crypto: Cryptocurrency symbol (BTC, ETH, SOL)
            target_timestamp: Unix timestamp to get price for

        Returns:
            Price in USD at that timestamp, or None if not found
        """
        feed_address = self.CHAINLINK_FEEDS.get(crypto)
        if not feed_address:
            return None

        rpc_endpoints = [
            "https://polygon-rpc.com",
            "https://rpc-mainnet.matic.quiknode.pro",
            "https://polygon-mainnet.g.alchemy.com/v2/demo",
            "https://rpc.ankr.com/polygon",
        ]

        for rpc_url in rpc_endpoints:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 15}))
                if not w3.is_connected():
                    continue

                contract = w3.eth.contract(
                    address=Web3.to_checksum_address(feed_address),
                    abi=self.CHAINLINK_ABI
                )

                decimals = contract.functions.decimals().call()

                # Get latest round as starting point
                latest = contract.functions.latestRoundData().call()
                latest_round_id = latest[0]
                latest_updated_at = latest[3]

                # If target is in the future or very recent, use latest
                if target_timestamp >= latest_updated_at:
                    price = float(latest[1]) / (10 ** decimals)
                    return price

                # Search backwards to find the FIRST update AT or AFTER target time
                # This is the "open price" - first price published after window opens
                from datetime import datetime
                prev_round_data = None  # Track the previous (newer) round

                print(f"[fetcher] Searching for first price AFTER ts={target_timestamp} ({datetime.utcfromtimestamp(target_timestamp)})")
                print(f"[fetcher] Latest round: {latest_round_id}, updated_at={latest_updated_at} ({datetime.utcfromtimestamp(latest_updated_at)})")

                for i in range(50):
                    try:
                        round_id = latest_round_id - i
                        data = contract.functions.getRoundData(round_id).call()
                        updated_at = data[3]
                        round_price = float(data[1]) / (10 ** decimals)

                        if i < 5:  # Log first 5 rounds for debugging
                            print(f"[fetcher]   Round {round_id}: ${round_price:,.2f} @ {datetime.utcfromtimestamp(updated_at)} (diff={updated_at - target_timestamp}s)")

                        # If this update is BEFORE window start, return the NEXT round
                        # (the first update AFTER window start)
                        if updated_at < target_timestamp:
                            if prev_round_data:
                                price = float(prev_round_data[1]) / (10 ** decimals)
                                print(f"[fetcher] Found first price after window: ${price:,.2f} @ {datetime.utcfromtimestamp(prev_round_data[3])}")
                                return price
                            else:
                                # No newer round found, use latest
                                print(f"[fetcher] Window just started, using latest: ${latest[1] / (10 ** decimals):,.2f}")
                                return float(latest[1]) / (10 ** decimals)

                        # Save this round as potential "first after window start"
                        prev_round_data = data

                    except Exception as e:
                        print(f"[fetcher]   Round {latest_round_id - i}: Error - {e}")
                        continue

                if prev_round_data:
                    price = float(prev_round_data[1]) / (10 ** decimals)
                    return price

                return None

            except Exception as e:
                error_str = str(e)
                if "rate limit" in error_str.lower() or "too many requests" in error_str.lower():
                    continue
                continue

        print(f"[fetcher] All RPCs failed for {crypto} historical Chainlink price")
        return None

    def _fetch_rtds_chainlink_price(self, crypto: str, timeout: float = 5.0, require_window_ts: bool = False) -> Optional[float]:
        """
        Fetch Chainlink price from Polymarket's RTDS WebSocket.
        Delegates to the standalone fetch_rtds_chainlink_price function.

        Args:
            crypto: Cryptocurrency symbol (BTC, ETH, SOL, XRP)
            timeout: Max seconds to wait for price
            require_window_ts: If True, only accept exact windowStart timestamp.

        Returns:
            Price in USD, or None if unavailable
        """
        return fetch_rtds_chainlink_price(crypto, timeout=timeout, require_window_ts=require_window_ts)

    def _fetch_rtds_all_cryptos(self, timeout: float = 8.0) -> Dict[str, float]:
        """
        Fetch Chainlink prices for ALL cryptos (BTC, ETH, SOL) in a single
        WebSocket connection to Polymarket's RTDS.

        More efficient than 3 separate connections. Returns a dict of prices.

        Args:
            timeout: Max seconds to wait for all prices

        Returns:
            Dict mapping crypto symbol to price (e.g., {"BTC": 98234.56, "ETH": 3456.78, "SOL": 198.34})
        """
        if not HAS_WEBSOCKET:
            return {}

        symbol_to_crypto = {
            "btc/usd": "BTC",
            "eth/usd": "ETH",
            "sol/usd": "SOL",
        }
        all_symbols = list(symbol_to_crypto.keys())

        prices = {}
        price_lock = threading.Lock()

        def on_message(ws, message):
            try:
                data = json.loads(message)
                items = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
                for item in items:
                    sym = item.get("symbol", "").lower()
                    if sym in symbol_to_crypto:
                        val = float(item.get("value") or item.get("price", 0))
                        if val > 0:
                            crypto = symbol_to_crypto[sym]
                            with price_lock:
                                prices[crypto] = val
                                # Close once we have all 3
                                if len(prices) >= 3:
                                    ws.close()
            except Exception:
                pass

        def on_open(ws):
            subscribe_msg = {
                "action": "subscribe",
                "subscriptions": [{
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                }]
            }
            ws.send(json.dumps(subscribe_msg))

        def on_error(ws, error):
            pass

        try:
            ws = websocket.WebSocketApp(
                "wss://ws-live-data.polymarket.com/ws",
                on_message=on_message,
                on_open=on_open,
                on_error=on_error,
            )
            ws_thread = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 20})
            ws_thread.daemon = True
            ws_thread.start()
            ws_thread.join(timeout=timeout)

            try:
                ws.close()
            except Exception:
                pass

        except Exception as e:
            print(f"[RTDS] WebSocket error: {e}")

        return prices

    def capture_rtds_window_strike(self, crypto: str) -> Optional[float]:
        """
        Capture the RTDS Chainlink price at window start and cache it as the
        Polymarket "price to beat" for this 15-minute window.

        If within the first 10s of the window, tries exact timestamp match first
        (require_window_ts=True). Falls back to any RTDS price if we're later
        in the window or if the exact match fails.

        Args:
            crypto: Cryptocurrency symbol (BTC, ETH, SOL, XRP)

        Returns:
            Captured strike price, or None if RTDS unavailable
        """
        import time as time_mod

        current_window = self._get_current_window()

        # Already captured for this window - return cached
        if crypto in self._rtds_strike_cache:
            if self._rtds_strike_window.get(crypto) == current_window:
                # Continue delay logging if still active
                self._rtds_delay_log(crypto)
                return self._rtds_strike_cache[crypto]

        now = time_mod.time()
        window_start = self._get_window_start_timestamp()
        elapsed = now - window_start

        price = None
        source = "any"

        # If within first 10s, try exact windowStart timestamp match first
        if elapsed < 10:
            price = self._fetch_rtds_chainlink_price(crypto, timeout=8.0, require_window_ts=True)
            if price and price > 0:
                source = "exact"

        # Fallback: accept any RTDS price (latest snapshot)
        if not price or price <= 0:
            price = self._fetch_rtds_chainlink_price(crypto, timeout=8.0, require_window_ts=False)
            if price and price > 0:
                source = "fallback"

        if not price or price <= 0:
            return None

        now = time_mod.time()
        elapsed = now - window_start

        # Cache as the strike for this window
        self._rtds_strike_cache[crypto] = price
        self._rtds_strike_window[crypto] = current_window

        # Start delay logging
        self._rtds_log_active[crypto] = True
        self._rtds_log_start[crypto] = now
        self._rtds_log_prices[crypto] = [(now, price)]

        print(f"  [RTDS] {crypto}: Captured strike ${price:,.2f} at t+{elapsed:.1f}s ({source})")

        return price

    def _rtds_delay_log(self, crypto: str):
        """
        Continue logging RTDS prices after initial capture to detect delays.

        Logs every call for RTDS_LOG_DURATION_SECONDS after the initial capture,
        showing how the RTDS price changes (if at all) over the first ~30s.
        This helps identify:
        - Whether the RTDS price is stable at window start
        - Any delay before the "price to beat" settles
        - How much the price drifts in the first seconds
        """
        import time as time_mod

        if not self._rtds_log_active.get(crypto, False):
            return

        now = time_mod.time()
        start = self._rtds_log_start.get(crypto, now)
        elapsed_since_start = now - start

        # Stop logging after the configured duration
        if elapsed_since_start > RTDS_LOG_DURATION_SECONDS:
            if self._rtds_log_active.get(crypto):
                self._rtds_log_active[crypto] = False
                # Print summary of all logged prices
                log_entries = self._rtds_log_prices.get(crypto, [])
                if len(log_entries) >= 2:
                    first_price = log_entries[0][1]
                    last_price = log_entries[-1][1]
                    max_price = max(p for _, p in log_entries)
                    min_price = min(p for _, p in log_entries)
                    drift = abs(last_price - first_price)
                    drift_pct = drift / first_price * 100 if first_price else 0
                    spread = max_price - min_price
                    spread_pct = spread / first_price * 100 if first_price else 0
                    print(f"  [RTDS-LOG] {crypto}: {len(log_entries)} samples over {RTDS_LOG_DURATION_SECONDS}s")
                    print(f"    First: ${first_price:,.2f} → Last: ${last_price:,.2f} (drift: ${drift:,.2f} / {drift_pct:.4f}%)")
                    print(f"    Range: ${min_price:,.2f} - ${max_price:,.2f} (spread: ${spread:,.2f} / {spread_pct:.4f}%)")
            return

        # Fetch current RTDS price for logging (don't update the cached strike)
        current_price = self._fetch_rtds_chainlink_price(crypto, timeout=3.0)
        if current_price and current_price > 0:
            self._rtds_log_prices.setdefault(crypto, []).append((now, current_price))

            # Log every ~5 seconds (avoid spam)
            log_entries = self._rtds_log_prices.get(crypto, [])
            if len(log_entries) <= 1 or (len(log_entries) % 3 == 0):
                initial = self._rtds_strike_cache.get(crypto, current_price)
                diff = current_price - initial
                diff_pct = diff / initial * 100 if initial else 0
                window_start = self._get_window_start_timestamp()
                t_plus = now - window_start
                print(f"  [RTDS-LOG] {crypto}: ${current_price:,.2f} at t+{t_plus:.1f}s (Δ ${diff:+,.2f} / {diff_pct:+.4f}% from strike)")

    def _scrape_poly_strike(self, crypto: str, timeout: int = 15000, window_timestamp: int = None) -> Optional[float]:
        """
        Scrape the EXACT "price to beat" from Polymarket website using Playwright.

        This is the definitive source - scrapes directly from the UI.
        Only called once per 15-minute window (result is cached).

        Args:
            crypto: Cryptocurrency symbol (BTC, ETH, SOL)
            timeout: Page load timeout in milliseconds
            window_timestamp: Optional specific window timestamp (for previous windows)

        Returns:
            Price in USD, or None if scraping fails
        """
        if not HAS_PLAYWRIGHT:
            print(f"[fetcher] Playwright not installed, skipping web scrape")
            return None

        import time as time_module
        import re

        # Build the market URL
        if window_timestamp:
            interval_start = window_timestamp
        else:
            ts = int(time_module.time())
            interval_start = (ts // 900) * 900
        slug = f"{crypto.lower()}-updown-15m-{interval_start}"
        url = f"https://polymarket.com/event/{slug}"

        # Silently scrape - only log on success/failure

        try:
            with sync_playwright() as p:
                # Launch headless browser
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                page = context.new_page()

                # Navigate to page
                page.goto(url, timeout=timeout, wait_until="domcontentloaded")

                # Wait a bit for dynamic content to load
                page.wait_for_timeout(3000)

                # Get page content
                content = page.content()

                # Try multiple patterns to find "price to beat"
                patterns = [
                    # "Price to beat: $76,353.44" or similar
                    r'[Pp]rice\s+to\s+beat[:\s]*\$?([\d,]+\.?\d*)',
                    # "price to beat" followed by price somewhere nearby
                    r'beat[^$]*\$?([\d,]+\.?\d*)',
                    # Look for large dollar amounts that could be BTC/ETH/SOL prices
                    r'>\s*\$?([\d,]+\.\d{2})\s*<',
                ]

                # Try to find it in specific elements (silently)
                try:
                    elements = page.locator("text=/price to beat/i").all()
                    for elem in elements[:3]:
                        try:
                            # Try grandparent first
                            grandparent = elem.locator("../..").first
                            gp_text = grandparent.inner_text()
                            lower_text = gp_text.lower()
                            if "price to beat" in lower_text:
                                after_label = lower_text.split("price to beat")[1]
                                if "final" in after_label:
                                    after_label = after_label.split("final")[0]
                                match = re.search(r'\$?([\d,]+\.\d{2})', after_label)
                                if match:
                                    price = float(match.group(1).replace(',', ''))
                                    if crypto == "BTC" and 10000 < price < 500000:
                                        browser.close()
                                        return price
                                    elif crypto == "ETH" and 100 < price < 20000:
                                        browser.close()
                                        return price
                                    elif crypto == "SOL" and 1 < price < 2000:
                                        browser.close()
                                        return price

                            # Try great-grandparent
                            great_gp = elem.locator("../../..").first
                            ggp_text = great_gp.inner_text()
                            lower_ggp = ggp_text.lower()
                            if "price to beat" in lower_ggp:
                                after_label = lower_ggp.split("price to beat")[1]
                                if "final" in after_label:
                                    after_label = after_label.split("final")[0]
                                match = re.search(r'\$?([\d,]+\.\d{2})', after_label)
                                if match:
                                    price = float(match.group(1).replace(',', ''))
                                    if crypto == "BTC" and 10000 < price < 500000:
                                        browser.close()
                                        return price
                                    elif crypto == "ETH" and 100 < price < 20000:
                                        browser.close()
                                        return price
                                    elif crypto == "SOL" and 1 < price < 2000:
                                        browser.close()
                                        return price
                        except Exception:
                            continue
                except Exception:
                    pass

                # Fallback: search page text for "price to beat" section
                try:
                    page_text = page.inner_text("body")
                    lower_page = page_text.lower()

                    if "price to beat" in lower_page:
                        after_ptb = lower_page.split("price to beat")[1]
                        if "final" in after_ptb:
                            after_ptb = after_ptb.split("final")[0]

                        match = re.search(r'\$?([\d,]+\.\d{2})', after_ptb)
                        if match:
                            price = float(match.group(1).replace(',', ''))
                            if crypto == "BTC" and 10000 < price < 500000:
                                browser.close()
                                return price
                            elif crypto == "ETH" and 100 < price < 20000:
                                browser.close()
                                return price
                            elif crypto == "SOL" and 1 < price < 2000:
                                browser.close()
                                return price
                except Exception:
                    pass

                browser.close()
                return None

        except Exception:
            return None

    def _get_poly_strike(self, crypto: str) -> Optional[float]:
        """
        Get Polymarket's "price to beat" for this 15-minute window.

        Priority order:
        1. RTDS WebSocket (Polymarket's own Chainlink relay - exact "price to beat")
        2. Use web_scraper module (Chainlink on-chain + scraping)
        3. Fall back to old built-in Playwright scraper
        4. Return None (will use ANY direction - safer than wrong price)

        RTDS (priority 1) connects to Polymarket's live WebSocket and gets
        the exact same Chainlink price they display as "price to beat".
        No API keys needed, no on-chain timing mismatch.

        Result is cached for the entire 15-minute window.
        After initial capture, continues logging for delay analysis.
        """
        current_window = self._get_current_window()

        # Check if we already have the strike for this window
        if crypto in self._poly_strike_cache:
            cached_window = self._poly_strike_window.get(crypto)
            if cached_window == current_window:
                # Continue delay logging if active
                self._rtds_delay_log(crypto)
                return self._poly_strike_cache[crypto]

        # PRIORITY 1: RTDS WebSocket (Polymarket's own Chainlink price)
        if HAS_WEBSOCKET:
            price = self.capture_rtds_window_strike(crypto)
            if price and self._is_valid_strike(crypto, price):
                self._poly_strike_cache[crypto] = price
                self._poly_strike_window[crypto] = current_window
                return price

        # PRIORITY 2: Use new centralized scraper (also gets Kalshi)
        if HAS_SCRAPER:
            _, scraped_poly = self._scrape_strike_prices(crypto)
            if scraped_poly and self._is_valid_strike(crypto, scraped_poly):
                print(f"[fetcher] Using scraped Poly strike ${scraped_poly:,.2f} for {crypto}")
                self._poly_strike_cache[crypto] = scraped_poly
                self._poly_strike_window[crypto] = current_window
                return scraped_poly

        # PRIORITY 3: Fall back to old built-in Playwright scraper
        price = self._scrape_poly_strike(crypto)
        if price:
            self._poly_strike_cache[crypto] = price
            self._poly_strike_window[crypto] = current_window
            return price

        # If all methods fail, return None (will use ANY direction)
        return None

    def _get_window_start_timestamp(self) -> int:
        """Get the Unix timestamp for the start of the current 15-minute window."""
        import time
        now = int(time.time())
        # Round down to nearest 15-minute boundary (900 seconds)
        return (now // 900) * 900

    def _get_current_window(self) -> str:
        """Get current 15-minute window identifier (e.g., '14:30')."""
        from datetime import datetime
        now = datetime.utcnow()
        minute_window = (now.minute // 15) * 15
        return f"{now.hour:02d}:{minute_window:02d}"

    def _pick_latest_close(self, markets: List[Dict]) -> Optional[Dict]:
        """Pick the market with the latest close time."""
        if not markets:
            return None

        def key(m: Dict) -> float:
            dt = parse_iso_dt(m.get("close_time", "") or "")
            return dt.timestamp() if dt else 0.0

        return sorted(markets, key=key, reverse=True)[0]

    def _is_valid_strike(self, crypto: str, strike: float) -> bool:
        """Check if strike price is valid/reasonable for the crypto."""
        if strike is None or strike <= 0:
            return False
        # Minimum reasonable prices for each crypto (reject obviously wrong data)
        min_prices = {
            "BTC": 10000,  # BTC should be > $10,000
            "ETH": 500,    # ETH should be > $500
            "SOL": 50,     # SOL should be > $50 (was $10, but $14.99 is clearly wrong)
            "XRP": 0.50,   # XRP should be > $0.50
        }
        min_price = min_prices.get(crypto, 100)
        return strike >= min_price

    def _extract_kalshi_strike(self, market: dict, crypto: str = None) -> Optional[float]:
        """Extract strike price from Kalshi market data and subtract $0.01."""
        import re
        strike = None

        # Try floor_strike field first
        floor_strike = market.get("floor_strike")
        if floor_strike is not None:
            try:
                strike = float(floor_strike)
            except (ValueError, TypeError):
                pass

        # Try to extract from ticker (e.g., KXBTC15M-26FEB02-T0230-B75148)
        if strike is None:
            ticker = market.get("ticker", "")
            # Pattern: B followed by digits at the end
            match = re.search(r'B(\d+)$', ticker)
            if match:
                try:
                    strike = float(match.group(1))
                except ValueError:
                    pass

        # Try to extract from subtitle (e.g., "above $75,148")
        if strike is None:
            subtitle = market.get("subtitle", "") or market.get("title", "")
            match = re.search(r'\$?([\d,]+\.?\d*)', subtitle)
            if match:
                try:
                    strike = float(match.group(1).replace(',', ''))
                except ValueError:
                    pass

        # Use raw Kalshi strike with no rounding or adjustment
        if strike is not None:
            strike = float(strike)

        # Validate the strike is reasonable
        if crypto and strike is not None:
            if not self._is_valid_strike(crypto, strike):
                print(f"[fetcher] Invalid Kalshi strike ${strike:.2f} for {crypto} - market not ready")
                return None

        return strike

    def _get_kalshi_ticker(self, crypto: str) -> Optional[tuple]:
        """Get Kalshi ticker, using cache when possible (refreshes each 15-min window)."""
        current_window = self._get_current_window()

        # Check if cache is valid for current window
        if crypto in self._ticker_cache:
            cached_window = self._ticker_cache_window.get(crypto)
            if cached_window == current_window:
                return self._ticker_cache[crypto]

        # Fetch fresh data (new window or not cached)
        series_ticker = self.SERIES_TICKERS.get(crypto)
        if not series_ticker:
            return None

        markets = self.kalshi.get_markets(series_ticker, limit=20)
        if not markets:
            return None

        m = self._pick_latest_close(markets)
        if not m:
            return None

        ticker = m.get("ticker", "")
        close_time = m.get("close_time", "")

        # PRIORITY 1: Try web scraping for exact strike price from website
        strike = None
        if HAS_SCRAPER:
            scraped_kalshi, _ = self._scrape_strike_prices(crypto)
            if scraped_kalshi and self._is_valid_strike(crypto, scraped_kalshi):
                print(f"[fetcher] Using scraped Kalshi strike ${scraped_kalshi:,.2f} for {crypto}")
                strike = scraped_kalshi

        # PRIORITY 2: Try API extraction
        if not strike or not self._is_valid_strike(crypto, strike):
            strike = self._extract_kalshi_strike(m, crypto)

        # PRIORITY 3: Fall back to Chainlink price
        if not strike or not self._is_valid_strike(crypto, strike):
            chainlink_price = self._fetch_chainlink_price(crypto)
            if chainlink_price and self._is_valid_strike(crypto, chainlink_price):
                print(f"[fetcher] Using Chainlink price ${chainlink_price:,.2f} for {crypto} (fallback)")
                strike = chainlink_price

        # Only cache if strike is valid - otherwise retry will fetch fresh
        if strike and self._is_valid_strike(crypto, strike):
            self._ticker_cache[crypto] = (ticker, close_time, strike)
            self._ticker_cache_window[crypto] = current_window
        else:
            # Don't cache invalid data - clear any stale cache for this crypto
            self._ticker_cache.pop(crypto, None)
            self._ticker_cache_window.pop(crypto, None)

        return (ticker, close_time, strike)

    def _extract_poly_strike(self, text: str) -> Optional[float]:
        """Extract strike price from Polymarket question or description text."""
        import re

        if not text:
            return None

        # Try multiple patterns from most specific to least specific
        patterns = [
            # "above $75,054.39" or "below $75,054.39" or various prepositions
            r'(?:above|below|at|over|under|exceeds?|greater than|less than|more than|higher than|lower than)\s*\$?([\d,]+\.?\d*)',
            # "is $75,054.39" or "was $75,054.39" - capture price after is/was
            r'(?:is|was|be)\s*\$?([\d,]+\.?\d*)',
            # "$75,054.39" standalone with dollar sign
            r'\$([\d,]+\.?\d*)',
            # Any large number that looks like a crypto price (4+ digits before decimal)
            r'\b(\d{2,6}(?:,\d{3})*(?:\.\d+)?)\b',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    value = float(match.group(1).replace(',', ''))
                    # Sanity check: crypto strikes are typically in reasonable ranges
                    # BTC: $20k-$200k, ETH: $500-$10k, SOL: $10-$500
                    if value >= 10:  # Minimum reasonable strike
                        return value
                except ValueError:
                    pass

        return None

    def fetch(self, crypto: str) -> Optional[Snapshot]:
        """
        Fetch a market snapshot for the given cryptocurrency.

        Args:
            crypto: Cryptocurrency symbol (BTC, ETH, or SOL)

        Returns:
            Snapshot with prices from both exchanges, or None if unavailable
        """
        # Get ticker (cached)
        ticker_data = self._get_kalshi_ticker(crypto)
        if not ticker_data:
            return None

        kalshi_ticker, kalshi_close_time, kalshi_strike = ticker_data

        # Fetch Kalshi orderbook and Poly prices in parallel
        ob = None
        poly_prices = None

        with ThreadPoolExecutor(max_workers=2) as executor:
            kalshi_future = executor.submit(self.kalshi.get_orderbook, kalshi_ticker)
            poly_future = executor.submit(self.poly.get_prices, crypto)

            ob = kalshi_future.result()
            poly_prices = poly_future.result()

        if not ob:
            return None

        book = ob.get("orderbook", {}) or {}
        yes_bids = book.get("yes", []) or []
        no_bids = book.get("no", []) or []
        if not yes_bids or not no_bids:
            return None

        yes_bid = float(yes_bids[-1][0]) / 100.0
        no_bid = float(no_bids[-1][0]) / 100.0

        k_up = 1.0 - no_bid
        k_down = 1.0 - yes_bid

        if not poly_prices:
            return None

        # Get Poly strike - fetch live price at start of each 15-min window
        # Polymarket uses the price at window start as "price to beat"
        poly_strike = self._get_poly_strike(crypto)

        # Log strike prices once per window when market opens
        current_window = self._get_current_window()
        if self._strike_logged_window.get(crypto) != current_window:
            self._strike_logged_window[crypto] = current_window
            print(f"\n  [MARKET OPEN] {crypto} Window {current_window}")
            if kalshi_strike and poly_strike:
                diff_pct = abs(kalshi_strike - poly_strike) / kalshi_strike * 100
                status = "✅ OK (≤3%)" if diff_pct <= 3.0 else "⚠️ MISMATCH (>3%)"
                # Calculate spread % using min strike
                spread_pct = abs(kalshi_strike - poly_strike) / min(kalshi_strike, poly_strike) * 100
                # Get threshold for this crypto (TESTING mode thresholds)
                spread_thresholds = {"BTC": 0.05, "ETH": 0.07, "SOL": 0.12, "XRP": 0.12}
                threshold = spread_thresholds.get(crypto, 0.05)
                spread_status = "✅ TRADEABLE" if spread_pct < threshold else f"❌ SKIP (need <{threshold}%)"
                print(f"    Kalshi Strike: ${kalshi_strike:,.2f}")
                print(f"    Poly Strike:   ${poly_strike:,.2f}")
                print(f"    Difference:    {diff_pct:.3f}% {status}")
                print(f"    Spread:        {spread_pct:.3f}% {spread_status}")
            elif kalshi_strike and not poly_strike:
                # Couldn't fetch live price for Poly strike
                print(f"    Kalshi Strike: ${kalshi_strike:,.2f}")
                print(f"    Poly Strike:   FAILED TO FETCH (using Kalshi as reference)")
            else:
                print(f"    Kalshi Strike: NOT FOUND" if not kalshi_strike else f"    Kalshi Strike: ${kalshi_strike:,.2f}")
                print(f"    Poly Strike:   NOT FOUND" if not poly_strike else f"    Poly Strike:   ${poly_strike:,.2f}")
                print(f"    ⚠️ Cannot verify strikes - WILL NOT TRADE")

        return Snapshot(
            crypto=crypto,
            kalshi_ticker=kalshi_ticker,
            kalshi_close_time=kalshi_close_time,
            poly_slug=poly_prices["slug"],
            poly_question=poly_prices["question"],
            k_up=k_up,
            k_down=k_down,
            p_up=poly_prices["up_buy"],
            p_down=poly_prices["down_buy"],
            poly_up_token=poly_prices["up_token_id"],
            poly_down_token=poly_prices["down_token_id"],
            kalshi_strike=kalshi_strike,
            poly_strike=poly_strike,
        )

    def fetch_all(self, cryptos: List[str]) -> Dict[str, Optional[Snapshot]]:
        """
        Fetch market snapshots for all cryptos in parallel.

        Args:
            cryptos: List of cryptocurrency symbols (BTC, ETH, SOL)

        Returns:
            Dict mapping crypto symbol to Snapshot (or None if unavailable)
        """
        results = {}

        with ThreadPoolExecutor(max_workers=len(cryptos)) as executor:
            future_to_crypto = {
                executor.submit(self.fetch, crypto): crypto
                for crypto in cryptos
            }

            for future in as_completed(future_to_crypto):
                crypto = future_to_crypto[future]
                try:
                    results[crypto] = future.result()
                except Exception:
                    results[crypto] = None

        return results

    def fetch_previous_window_kalshi(self, crypto: str) -> tuple:
        """
        Fetch Kalshi's strike price and settlement for the PREVIOUS 15-minute window.

        Returns:
            Tuple of (strike_price, settlement_price) or (None, None)
        """
        import time as time_module
        from datetime import datetime, timezone

        series_map = {"BTC": "KXBTC", "ETH": "KXETH", "SOL": "KXSOL"}
        series_ticker = series_map.get(crypto)
        if not series_ticker:
            return (None, None)

        # Get markets from Kalshi
        markets = self.kalshi.get_markets(series_ticker, limit=20)
        if not markets:
            print(f"[fetcher] No Kalshi markets returned for {crypto}")
            return (None, None)

        # Find the most recently CLOSED market (close_time < now)
        now = datetime.now(timezone.utc)
        print(f"[fetcher] Checking {len(markets)} Kalshi markets for {crypto}, now={now.isoformat()}")

        closed_markets = []
        for m in markets:
            close_time_str = m.get("close_time", "")
            dt = parse_iso_dt(close_time_str)
            if dt:
                status = "CLOSED" if dt < now else "OPEN"
                print(f"[fetcher]   Market close_time={close_time_str} -> {status}")
                if dt < now:
                    closed_markets.append((m, dt))

        if not closed_markets:
            print(f"[fetcher] No closed Kalshi markets found for {crypto}")
            return (None, None)

        # Sort by close time descending, pick most recent closed
        closed_markets.sort(key=lambda x: x[1], reverse=True)
        prev_market = closed_markets[0][0]

        # Extract strike from previous market
        strike = self._extract_kalshi_strike(prev_market, crypto)
        if strike:
            print(f"[fetcher] Previous Kalshi window {crypto} strike: ${strike:.2f}")
        else:
            print(f"[fetcher] Could not extract strike from previous Kalshi market")

        # Try to get settlement value (might be in result, settlement_value, etc.)
        settlement = None
        for field in ['settlement_value', 'result', 'settlement_price', 'settled_price']:
            val = prev_market.get(field)
            if val is not None:
                try:
                    settlement = float(val)
                    print(f"[fetcher] Previous Kalshi {crypto} settlement ({field}): ${settlement:.2f}")
                    break
                except (ValueError, TypeError):
                    pass

        # Debug: print all market fields to see what's available
        print(f"[fetcher] Previous Kalshi market fields: {list(prev_market.keys())}")

        return (strike, settlement)

    def _scrape_poly_both_prices(self, crypto: str, window_timestamp: int) -> tuple:
        """
        Scrape BOTH "price to beat" AND "final price" from Polymarket previous window page.

        Returns:
            Tuple of (price_to_beat, final_price) or (None, None)
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            print("[fetcher] Playwright not installed")
            return (None, None)

        import re

        slug = f"{crypto.lower()}-updown-15m-{window_timestamp}"
        url = f"https://polymarket.com/event/{slug}"
        print(f"[fetcher] Scraping Poly both prices from: {url}")

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                page.goto(url, timeout=15000, wait_until="domcontentloaded")
                page.wait_for_timeout(3000)

                # Get full page text
                page_text = page.inner_text("body")
                lower_text = page_text.lower()

                price_to_beat = None
                final_price = None

                # Extract "price to beat" - text between "price to beat" and "final"
                if "price to beat" in lower_text:
                    after_ptb = lower_text.split("price to beat")[1]
                    if "final" in after_ptb:
                        ptb_section = after_ptb.split("final")[0]
                    else:
                        ptb_section = after_ptb[:100]

                    match = re.search(r'\$?([\d,]+\.\d{2})', ptb_section)
                    if match:
                        price_to_beat = float(match.group(1).replace(',', ''))
                        print(f"[fetcher] Found price to beat: ${price_to_beat:,.2f}")

                # Extract "final price" - text after "final price"
                if "final price" in lower_text:
                    after_final = lower_text.split("final price")[1]
                    # Take just the next section (before any other label)
                    final_section = after_final[:100]

                    match = re.search(r'\$?([\d,]+\.\d{2})', final_section)
                    if match:
                        final_price = float(match.group(1).replace(',', ''))
                        print(f"[fetcher] Found final price: ${final_price:,.2f}")

                browser.close()
                return (price_to_beat, final_price)

        except Exception as e:
            print(f"[fetcher] Scraping error: {e}")
            return (None, None)

    def fetch_previous_window_poly(self, crypto: str) -> tuple:
        """
        Fetch Polymarket's price and final price for the PREVIOUS 15-minute window.

        Returns:
            Tuple of (poly_price, poly_final) or (None, None) if unavailable
        """
        import time as time_module

        # Get previous window timestamp
        ts = int(time_module.time())
        current_window_start = (ts // 900) * 900
        prev_window_start = current_window_start - 900  # 15 minutes earlier

        # Scrape both prices from previous window's page
        poly_price, poly_final = self._scrape_poly_both_prices(crypto, prev_window_start)

        return (poly_price, poly_final)

    def fetch_previous_window(self, crypto: str) -> dict:
        """
        Fetch both Kalshi and Polymarket data for the PREVIOUS 15-minute window.

        The key insight: CURRENT window's start price = PREVIOUS window's final price
        So we get finals by fetching the current window's strike prices.

        Returns:
            Dict with kalshi_price, poly_price, kalshi_final, poly_final
        """
        # Get PREVIOUS window's prices (what the strike/price-to-beat was)
        kalshi_price, _ = self.fetch_previous_window_kalshi(crypto)
        poly_price, _ = self.fetch_previous_window_poly(crypto)

        # Get CURRENT window's prices - these ARE the previous window's final prices
        # (current start price = previous end price)
        ticker_data = self._get_kalshi_ticker(crypto)
        kalshi_final = ticker_data[2] if ticker_data else None  # (ticker, close_time, strike)

        # For Poly, scrape current window's "price to beat"
        poly_final = self._scrape_poly_strike(crypto)

        print(f"[fetcher] Previous window {crypto}: K_price=${kalshi_price}, P_price=${poly_price}")
        print(f"[fetcher] Previous window {crypto} finals: K_final=${kalshi_final}, P_final=${poly_final}")

        return {
            'kalshi_price': kalshi_price,
            'poly_price': poly_price,
            'kalshi_final': kalshi_final,
            'poly_final': poly_final
        }

    def _fetch_coingecko_price(self, crypto: str) -> Optional[float]:
        """Fetch current price from CoinGecko."""
        import requests
        ids = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana"}
        coin_id = ids.get(crypto)
        if not coin_id:
            return None
        try:
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return resp.json().get(coin_id, {}).get("usd")
        except Exception:
            pass
        return None
