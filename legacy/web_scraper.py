"""
Web scraper for getting exact strike prices from Kalshi and Polymarket websites.
Uses Chainlink oracles (fastest), HTTP scraping, or Playwright as fallback.
"""

import os
import re
import time
import multiprocessing
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

from dotenv import load_dotenv

# Import strike price timing config
try:
    from config import STRIKE_PRICE_OFFSET_SECONDS
except ImportError:
    STRIKE_PRICE_OFFSET_SECONDS = 1  # Default: 1 second after window start

# Load .env from keys directory
_SCRIPT_DIR = Path(__file__).resolve().parent
_KEYS_DIR = os.getenv("ARB_KEYS_DIR", str(_SCRIPT_DIR.parent / "keys"))
load_dotenv(Path(_KEYS_DIR) / ".env")

try:
    from playwright.sync_api import sync_playwright, Browser, Page
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    print("[scraper] WARNING: Playwright not installed. Run: pip install playwright && playwright install chromium")

try:
    from web3 import Web3
    HAS_WEB3 = True
except ImportError:
    HAS_WEB3 = False
    print("[scraper] WARNING: web3 not installed. Run: pip install web3")

# Chainlink price feed addresses on Polygon
# Verified from: https://polygonscan.com/address/<address>
CHAINLINK_FEEDS = {
    "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
    "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",  # Fixed - was wrong address
    "SOL": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC",
}

# Chainlink AggregatorV3Interface minimal ABI
CHAINLINK_ABI = [
    {"name": "decimals", "outputs": [{"type": "uint8"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "latestRoundData", "outputs": [
        {"type": "uint80"}, {"type": "int256"}, {"type": "uint256"}, {"type": "uint256"}, {"type": "uint80"}
    ], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "getRoundData", "outputs": [
        {"type": "uint80"}, {"type": "int256"}, {"type": "uint256"}, {"type": "uint256"}, {"type": "uint80"}
    ], "inputs": [{"type": "uint80"}], "stateMutability": "view", "type": "function"},
]

POLYGON_RPC = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")


class StrikeScraper:
    """Scrapes exact strike prices from Kalshi and Polymarket websites."""

    def __init__(self):
        self._browser: Optional[Browser] = None
        self._playwright = None
        self._initialized = False
        # Cache scraped prices per window
        self._price_cache: Dict[str, Dict[str, float]] = {}  # {window_id: {crypto: price}}

    def _get_window_id(self) -> str:
        """Get current 15-minute window identifier."""
        now = datetime.now(timezone.utc)
        window_minute = (now.minute // 15) * 15
        return f"{now.year}-{now.month:02d}-{now.day:02d}_{now.hour:02d}:{window_minute:02d}"

    def _get_window_unix_timestamp(self) -> int:
        """Get Unix timestamp for current 15-minute window start."""
        now = datetime.now(timezone.utc)
        window_minute = (now.minute // 15) * 15
        window_start = now.replace(minute=window_minute, second=0, microsecond=0)
        return int(window_start.timestamp())

    def _get_kalshi_date_code(self) -> str:
        """Get Kalshi date code for current window (e.g., '26FEB032030')."""
        now = datetime.now(timezone.utc)
        window_minute = (now.minute // 15) * 15
        # Format: {year_2digit}{MONTH_UPPER}{day}{hour24}{minute}
        # e.g., ticker KXBTC15M-26FEB032030-30 means Feb 3, 2026 @ 20:30 UTC
        year_2digit = str(now.year)[-2:]
        month = now.strftime("%b").upper()
        day = now.day
        hour = now.hour
        minute = window_minute
        return f"{year_2digit}{month}{day:02d}{hour:02d}{minute:02d}"

    def _extract_price(self, text: str) -> Optional[float]:
        """Extract price from text like '$78,835.49'."""
        if not text:
            return None
        # Match dollar amounts with optional commas and decimals
        match = re.search(r'\$?([\d,]+\.?\d*)', text)
        if match:
            try:
                return float(match.group(1).replace(',', ''))
            except ValueError:
                pass
        return None

    def _get_chainlink_price(self, crypto: str, target_ts: int = None) -> Optional[float]:
        """
        Get the Chainlink price at the window start timestamp.
        This is the source of truth for Polymarket strike prices.
        """
        if not HAS_WEB3:
            return None

        feed_address = CHAINLINK_FEEDS.get(crypto)
        if not feed_address:
            return None

        if target_ts is None:
            target_ts = self._get_window_unix_timestamp()

        # Retry logic with exponential backoff
        max_retries = 3
        for attempt in range(max_retries):
            try:
                w3 = Web3(Web3.HTTPProvider(POLYGON_RPC, request_kwargs={"timeout": 15}))
                if not w3.is_connected():
                    if attempt < max_retries - 1:
                        time.sleep(0.5 * (2 ** attempt))
                        continue
                    return None

                contract = w3.eth.contract(
                    address=Web3.to_checksum_address(feed_address),
                    abi=CHAINLINK_ABI
                )

                # Small stagger to avoid rate limiting
                if crypto == "ETH":
                    time.sleep(0.2)
                elif crypto == "SOL":
                    time.sleep(0.4)
                elif crypto == "XRP":
                    time.sleep(0.6)

                decimals = contract.functions.decimals().call()
                round_id, answer, _, _, _ = contract.functions.latestRoundData().call()

                # Walk backwards to find the FIRST update AT or AFTER window start
                # This is the "open price" - first price published after window opens
                max_steps = 500
                prev_round_data = None  # Track the previous (newer) round

                for _ in range(max_steps):
                    rid, ans, _, u_at, _ = contract.functions.getRoundData(round_id).call()

                    # If this update is BEFORE window start, return the NEXT round
                    # (the first update AFTER window start)
                    if u_at < target_ts:
                        if prev_round_data:
                            price = float(prev_round_data[1]) / (10 ** decimals)
                            return price
                        else:
                            # Current round is before window, but no newer round found
                            # This means window just started, use current latest
                            price = float(answer) / (10 ** decimals)
                            return price

                    # Save this round as potential "first after window start"
                    prev_round_data = (rid, ans, u_at)
                    round_id -= 1

                return None

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (2 ** attempt))
                else:
                    print(f"[CHAINLINK] {crypto}: Failed after {max_retries} attempts - {e}")
                    return None
        return None

    def _scrape_polymarket_fast(self, crypto: str) -> Optional[float]:
        """
        Fast HTTP-based scraper for Polymarket strike price.
        Tries to get strike from HTML or embedded JSON without Playwright.
        """
        import requests

        timestamp = self._get_window_unix_timestamp()
        url = f"https://polymarket.com/event/{crypto.lower()}-updown-15m-{timestamp}"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200:
                return None

            content = r.text
            price = None

            # Method 1: Look for strike in the specific span with tabular-nums class
            # The strike appears in: <span class="...font-semibold tabular-nums text-text-secondary">$70,531.79</span>
            span_patterns = [
                r'<span[^>]*tabular-nums[^>]*text-text-secondary[^>]*>\$?([\d,]+\.?\d*)',
                r'<span[^>]*text-text-secondary[^>]*tabular-nums[^>]*>\$?([\d,]+\.?\d*)',
                r'<span[^>]*font-semibold[^>]*tabular-nums[^>]*>\$?([\d,]+\.?\d*)',
            ]
            for pattern in span_patterns:
                match = re.search(pattern, content)
                if match:
                    try:
                        price = float(match.group(1).replace(',', ''))
                        # Sanity check based on crypto
                        min_prices = {"BTC": 10000, "ETH": 500, "SOL": 10, "XRP": 0.50}
                        if price >= min_prices.get(crypto, 100):
                            print(f"[scraper-fast] Polymarket {crypto} strike from span: ${price:,.2f}")
                            return price
                    except (ValueError, TypeError):
                        pass

            # Method 2: Look for "PRICE TO BEAT" pattern in HTML
            patterns = [
                r'PRICE TO BEAT[^$]*\$([\d,]+\.?\d*)',
                r'Price to beat[^$]*\$([\d,]+\.?\d*)',
                r'price.{0,30}beat[^$]*\$([\d,]+\.?\d*)',
            ]
            for pattern in patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    try:
                        price = float(match.group(1).replace(',', ''))
                        if price > 100:  # Sanity check
                            print(f"[scraper-fast] Polymarket {crypto} strike from HTML: ${price:,.2f}")
                            return price
                    except (ValueError, TypeError):
                        pass

            # Method 3: Look for __NEXT_DATA__ JSON blob
            import json
            next_data_match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>', content, re.DOTALL)
            if next_data_match:
                try:
                    data = json.loads(next_data_match.group(1))
                    # Navigate the JSON structure to find strike price
                    # Structure varies, try common paths
                    props = data.get("props", {}).get("pageProps", {})
                    event = props.get("event", {}) or props.get("dehydratedState", {})

                    # Look for price in various locations
                    for key in ["strike", "floor_strike", "strikePrice", "priceToBeat"]:
                        if key in event:
                            price = float(event[key])
                            if price > 100:
                                print(f"[scraper-fast] Polymarket {crypto} strike from JSON: ${price:,.2f}")
                                return price
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    pass

            # Fast scraper couldn't find strike - will fall back to Playwright
            return None

        except Exception as e:
            print(f"[scraper-fast] Polymarket {crypto} error: {e}")
            return None

    def scrape_polymarket_strike(self, crypto: str) -> Optional[float]:
        """
        Get Polymarket strike price using Chainlink on-chain aggregator.

        Note: This is a fallback method. The primary method is RTDS WebSocket
        in fetcher.py which gets the exact price Polymarket displays.
        This on-chain method has ~20-60s timing mismatch due to heartbeat delays.
        """
        window_start = self._get_window_unix_timestamp()
        target_ts = window_start + STRIKE_PRICE_OFFSET_SECONDS

        price = self._get_chainlink_price(crypto, target_ts=target_ts)
        if price:
            return price

        print(f"[chainlink] {crypto}: Failed to get price from Chainlink oracle")
        return None

        # =====================================================================
        # FALLBACK METHODS (commented out - using Chainlink only)
        # =====================================================================
        # print(f"[scraper] Polymarket {crypto}: Chainlink failed, trying HTTP scraper...")
        #
        # # Method 2: Try fast HTTP-based scraper
        # price = self._scrape_polymarket_fast(crypto)
        # if price:
        #     return price
        #
        # print(f"[scraper] Polymarket {crypto}: HTTP scraper failed, trying Playwright...")
        #
        # if not HAS_PLAYWRIGHT:
        #     return None
        #
        # timestamp = self._get_window_unix_timestamp()
        # url = f"https://polymarket.com/event/{crypto.lower()}-updown-15m-{timestamp}"
        #
        # try:
        #     # Use fresh playwright instance each time (avoid threading issues)
        #     with sync_playwright() as p:
        #         browser = p.chromium.launch(headless=True)
        #         page = browser.new_page()
        #         page.set_default_timeout(60000)  # 60s for slow VPS
        #
        #         page.goto(url, wait_until="domcontentloaded", timeout=45000)
        #
        #         # Wait for content to render
        #         page.wait_for_timeout(4000)
        #
        #         price = None
        #
        #         # Get page content and search for price
        #         content = page.content()
        #
        #         # Look for "PRICE TO BEAT" followed by a price
        #         patterns = [
        #             r'PRICE TO BEAT[^$]*\$([\d,]+\.?\d*)',
        #             r'Price to beat[^$]*\$([\d,]+\.?\d*)',
        #             r'price to beat[^$]*\$([\d,]+\.?\d*)',
        #         ]
        #
        #         for pattern in patterns:
        #             match = re.search(pattern, content, re.IGNORECASE)
        #             if match:
        #                 price = float(match.group(1).replace(',', ''))
        #                 if price > 100:  # Sanity check
        #                     break
        #
        #         browser.close()
        #
        #         if price:
        #             print(f"[scraper] Polymarket {crypto} strike: ${price:,.2f}")
        #         return price
        #
        # except Exception as e:
        #     print(f"[scraper] Polymarket scrape error: {e}")
        #     return None

    def scrape_kalshi_strike(self, crypto: str) -> Optional[float]:
        """Scrape strike price from Kalshi website or API."""
        # Method 1: Try API first (faster and more reliable)
        strike = self._fetch_kalshi_strike_from_api(crypto)
        if strike:
            return strike

        # Method 2: Fall back to web scraping
        if not HAS_PLAYWRIGHT:
            return None

        date_code = self._get_kalshi_date_code()
        crypto_lower = crypto.lower()
        crypto_upper = crypto.upper()

        # Try multiple URL formats (Kalshi sometimes changes them)
        urls_to_try = [
            f"https://kalshi.com/markets/kx{crypto_lower}15m/{crypto_lower}-price-up-down/KX{crypto_upper}15M-{date_code}",
            f"https://kalshi.com/markets/KX{crypto_upper}15M/{crypto_lower}-price-up-down/KX{crypto_upper}15M-{date_code}",
            f"https://kalshi.com/markets/kx{crypto_lower}15m",
        ]

        for url in urls_to_try:
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    page.set_default_timeout(60000)  # 60s for slow VPS

                    page.goto(url, wait_until="domcontentloaded", timeout=45000)

                    # Wait for content to load
                    page.wait_for_timeout(4000)

                    price = None

                    # Get page content
                    content = page.content()

                    # Look for "Price to beat: $X" - find ALL matches and use the one that makes sense
                    pattern = r'Price to beat:?\s*\$([\d,]+\.?\d*)'
                    matches = re.findall(pattern, content, re.IGNORECASE)

                    if matches:
                        # Convert all matches to floats
                        prices = []
                        for m in matches:
                            try:
                                p_val = float(m.replace(',', ''))
                                prices.append(p_val)
                            except:
                                pass

                        # Use the most recent/reasonable price (usually the last one on the page)
                        # Filter to valid prices for this crypto
                        min_prices = {"BTC": 10000, "ETH": 500, "SOL": 10, "XRP": 0.50}
                        min_price = min_prices.get(crypto, 100)
                        valid_prices = [p_val for p_val in prices if p_val >= min_price]

                        if valid_prices:
                            # Take the first valid price (the current market price)
                            price = valid_prices[0]
                            # Round to nearest penny
                            price = round(price, 2)

                    browser.close()

                    if price:
                        print(f"[scraper] Kalshi {crypto} strike: ${price:,.2f}")
                        return price

            except Exception as e:
                print(f"[scraper] Kalshi scrape error for {url}: {e}")
                continue

        return None

    def _fetch_kalshi_strike_from_api(self, crypto: str) -> Optional[float]:
        """Fetch strike price directly from Kalshi API."""
        import requests

        series_ticker = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M"}.get(crypto)
        if not series_ticker:
            return None

        try:
            # Fetch open markets for this series
            url = f"https://api.elections.kalshi.com/trade-api/v2/markets"
            params = {"series_ticker": series_ticker, "status": "open", "limit": 20}
            r = requests.get(url, params=params, timeout=10)

            if r.status_code != 200:
                return None

            markets = r.json().get("markets", [])
            if not markets:
                return None

            # Find the latest closing market
            from datetime import datetime as dt
            latest = None
            latest_dt = None
            for m in markets:
                close_str = m.get("close_time", "")
                if close_str:
                    try:
                        close_dt = dt.fromisoformat(close_str.replace('Z', '+00:00'))
                        if latest_dt is None or close_dt > latest_dt:
                            latest = m
                            latest_dt = close_dt
                    except:
                        pass

            if not latest:
                return None

            # Try to get strike from multiple fields
            strike = latest.get("floor_strike") or latest.get("strike_price")
            if strike:
                return float(strike)

            # Try getting from market details endpoint
            ticker = latest.get("ticker")
            if ticker:
                detail_url = f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}"
                detail_r = requests.get(detail_url, timeout=10)
                if detail_r.status_code == 200:
                    detail = detail_r.json().get("market", {})
                    strike = detail.get("floor_strike") or detail.get("strike_price")
                    if strike:
                        return float(strike)

                    # Try extracting from yes_sub_title (e.g., "Above $97,123.45")
                    yes_sub = detail.get("yes_sub_title") or ""
                    if yes_sub:
                        match = re.search(r'\$([0-9,]+\.?\d*)', yes_sub)
                        if match:
                            return float(match.group(1).replace(',', ''))

        except Exception as e:
            print(f"[scraper] Kalshi API error: {e}")

        return None

    def scrape_strikes(self, crypto: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Scrape strike prices from both Kalshi and Polymarket SEQUENTIALLY.
        (Playwright sync API doesn't work with threading)

        Returns:
            Tuple of (kalshi_strike, poly_strike)
        """
        # Check cache first
        window_id = self._get_window_id()
        cache_key = f"{window_id}_{crypto}"

        # Clear old cache entries from previous windows
        old_keys = [k for k in self._price_cache.keys() if not k.startswith(window_id)]
        for k in old_keys:
            del self._price_cache[k]

        if cache_key in self._price_cache:
            cached = self._price_cache[cache_key]
            return cached.get('kalshi'), cached.get('poly')

        # Scrape sequentially (not in parallel - Playwright doesn't like threads)
        kalshi_strike = self.scrape_kalshi_strike(crypto)
        poly_strike = self.scrape_polymarket_strike(crypto)

        # Cache results
        if kalshi_strike or poly_strike:
            self._price_cache[cache_key] = {
                'kalshi': kalshi_strike,
                'poly': poly_strike
            }

        return kalshi_strike, poly_strike

    def scrape_all_strikes(self, cryptos: list) -> Dict[str, Tuple[Optional[float], Optional[float]]]:
        """
        Scrape strike prices for all cryptos.

        Returns:
            Dict mapping crypto to (kalshi_strike, poly_strike) tuple
        """
        results = {}
        for crypto in cryptos:
            kalshi, poly = self.scrape_strikes(crypto)
            results[crypto] = (kalshi, poly)
        return results


# Global scraper instance
_scraper: Optional[StrikeScraper] = None


def get_scraper() -> StrikeScraper:
    """Get or create the global scraper instance."""
    global _scraper
    if _scraper is None:
        _scraper = StrikeScraper()
    return _scraper


def scrape_strike_prices(crypto: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Convenience function to scrape strike prices for a crypto.

    Returns:
        Tuple of (kalshi_strike, poly_strike)
    """
    return get_scraper().scrape_strikes(crypto)


def scrape_polymarket_price_to_beat(crypto: str, window_ts: int = None) -> Optional[float]:
    """
    Scrape the EXACT "price to beat" from Polymarket's event page using Playwright.

    This is the most reliable method - it reads exactly what Polymarket's UI displays.
    Should be called ~2 minutes into the window when the price is locked in.

    Args:
        crypto: Cryptocurrency symbol (BTC, ETH, SOL)
        window_ts: Unix timestamp for the 15-min window start (auto-calculated if None)

    Returns:
        Price in USD, or None if scraping fails
    """
    if not HAS_PLAYWRIGHT:
        print(f"[SCRAPE] Playwright not installed. Run: pip install playwright && playwright install chromium")
        return None

    if window_ts is None:
        window_ts = int(time.time())
        window_ts = (window_ts // 900) * 900  # Round down to 15-min boundary

    slug = f"{crypto.lower()}-updown-15m-{window_ts}"
    url = f"https://polymarket.com/event/{slug}"

    # Sanity check ranges for each crypto
    price_ranges = {
        "BTC": (10000, 500000),
        "ETH": (500, 20000),
        "SOL": (5, 2000),
        "XRP": (0.50, 100),
    }
    min_price, max_price = price_ranges.get(crypto, (100, 500000))

    print(f"[SCRAPE] {crypto}: Fetching price to beat from {url}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 720},
            )
            page = context.new_page()

            # Navigate to the event page
            page.goto(url, timeout=30000, wait_until="domcontentloaded")

            # Wait for page to render (Polymarket is JS-heavy)
            page.wait_for_timeout(5000)

            price = None

            # METHOD 1: Look for "PRICE TO BEAT" text and get the nearby price
            try:
                # Find all text on the page
                page_text = page.inner_text("body")

                # Search for "price to beat" followed by a dollar amount
                ptb_patterns = [
                    r'[Pp][Rr][Ii][Cc][Ee]\s+[Tt][Oo]\s+[Bb][Ee][Aa][Tt][^$]*\$?([\d,]+\.\d{2})',
                    r'[Pp]rice\s+to\s+[Bb]eat\s*\$?([\d,]+\.\d{2})',
                ]
                for pattern in ptb_patterns:
                    match = re.search(pattern, page_text)
                    if match:
                        val = float(match.group(1).replace(',', ''))
                        if min_price < val < max_price:
                            price = val
                            print(f"[SCRAPE] {crypto}: Found 'price to beat' = ${price:,.2f}")
                            break
            except Exception:
                pass

            # METHOD 2: Look for specific DOM elements
            if price is None:
                try:
                    # Polymarket uses "PRICE TO BEAT" label near the price
                    locators = page.locator("text=/price to beat/i").all()
                    for loc in locators[:3]:
                        try:
                            # Check parent, grandparent, great-grandparent for the price value
                            for depth in ["/..", "/../..", "/../../.."]:
                                parent = loc.locator(depth).first
                                parent_text = parent.inner_text()
                                match = re.search(r'\$?([\d,]+\.\d{2})', parent_text)
                                if match:
                                    val = float(match.group(1).replace(',', ''))
                                    if min_price < val < max_price:
                                        price = val
                                        print(f"[SCRAPE] {crypto}: Found price in DOM = ${price:,.2f}")
                                        break
                            if price:
                                break
                        except Exception:
                            continue
                except Exception:
                    pass

            # METHOD 3: Look for tabular-nums span (known Polymarket CSS class for prices)
            if price is None:
                try:
                    html_content = page.content()
                    tab_patterns = [
                        r'<span[^>]*tabular-nums[^>]*>\s*\$?([\d,]+\.\d{2})',
                        r'<span[^>]*font-semibold[^>]*tabular-nums[^>]*>\s*\$?([\d,]+\.\d{2})',
                    ]
                    for pattern in tab_patterns:
                        matches = re.findall(pattern, html_content)
                        for m in matches:
                            val = float(m.replace(',', ''))
                            if min_price < val < max_price:
                                price = val
                                print(f"[SCRAPE] {crypto}: Found price in tabular-nums span = ${price:,.2f}")
                                break
                        if price:
                            break
                except Exception:
                    pass

            # METHOD 4: Search __NEXT_DATA__ for strike/price fields
            if price is None:
                try:
                    import json as json_mod
                    html_content = page.content()
                    nd_match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>', html_content, re.DOTALL)
                    if nd_match:
                        data = json_mod.loads(nd_match.group(1))
                        # Walk through JSON looking for price-like values
                        def find_prices(obj, path=""):
                            results = []
                            if isinstance(obj, dict):
                                for k, v in obj.items():
                                    if isinstance(v, (int, float)) and min_price < v < max_price:
                                        results.append((f"{path}.{k}", v))
                                    elif isinstance(v, str):
                                        try:
                                            fv = float(v.replace(',', '').replace('$', ''))
                                            if min_price < fv < max_price:
                                                results.append((f"{path}.{k}", fv))
                                        except ValueError:
                                            pass
                                    results.extend(find_prices(v, f"{path}.{k}"))
                            elif isinstance(obj, list):
                                for i, v in enumerate(obj):
                                    results.extend(find_prices(v, f"{path}[{i}]"))
                            return results

                        price_candidates = find_prices(data)
                        if price_candidates:
                            # Pick the first reasonable price
                            price = price_candidates[0][1]
                            print(f"[SCRAPE] {crypto}: Found price in __NEXT_DATA__ at {price_candidates[0][0]} = ${price:,.2f}")
                except Exception:
                    pass

            browser.close()

            if price:
                print(f"[SCRAPE] {crypto}: Price to beat = ${price:,.2f}")
            else:
                print(f"[SCRAPE] {crypto}: FAILED to find price to beat on page")

            return price

    except Exception as e:
        print(f"[SCRAPE] {crypto}: Playwright error - {e}")
        return None


def scrape_polymarket_5m_price_to_beat(crypto: str, five_min_ts: int) -> Optional[float]:
    """
    Scrape the EXACT "price to beat" from Polymarket's 5-minute event page using Playwright.

    Uses the same Playwright browser automation as the 15-minute scraper, but with
    the 5-minute market URL pattern: https://polymarket.com/event/{crypto}-updown-5m-{timestamp}

    Args:
        crypto: Cryptocurrency symbol (BTC, ETH, SOL)
        five_min_ts: Unix timestamp for the 5-minute market (e.g., 1770878400)

    Returns:
        Price in USD, or None if scraping fails
    """
    if not HAS_PLAYWRIGHT:
        print(f"[SCRAPE-5M] Playwright not installed. Run: pip install playwright && playwright install chromium")
        return None

    slug = f"{crypto.lower()}-updown-5m-{five_min_ts}"
    url = f"https://polymarket.com/event/{slug}"

    # Sanity check ranges for each crypto
    price_ranges = {
        "BTC": (10000, 500000),
        "ETH": (500, 20000),
        "SOL": (5, 2000),
        "XRP": (0.50, 100),
    }
    min_price, max_price = price_ranges.get(crypto, (100, 500000))

    print(f"[SCRAPE-5M] {crypto}: Fetching 5-min price to beat from {url}")

    # Use subprocess worker (same as 15-min scraper) with 45s timeout
    result_queue = multiprocessing.Queue()

    proc = multiprocessing.Process(
        target=_scrape_single_crypto_worker,
        args=(url, crypto, min_price, max_price, result_queue),
    )
    t_start = time.time()
    proc.start()
    proc.join(timeout=45)
    elapsed = time.time() - t_start

    if proc.is_alive():
        proc.kill()
        proc.join(timeout=5)
        print(f"[SCRAPE-5M] {crypto}: TIMEOUT after {elapsed:.1f}s")
        return None
    elif not result_queue.empty():
        price, error = result_queue.get_nowait()
        if error:
            print(f"[SCRAPE-5M] {crypto}: Error - {error}")
            return None
        elif price:
            print(f"[SCRAPE-5M] {crypto}: 5-min price to beat = ${price:,.2f} ({elapsed:.1f}s)")
            return price
        else:
            print(f"[SCRAPE-5M] {crypto}: FAILED to find 5-min price ({elapsed:.1f}s)")
            return None
    else:
        print(f"[SCRAPE-5M] {crypto}: Process exited with no result ({elapsed:.1f}s)")
        return None


def _scrape_single_crypto_worker(url: str, crypto: str, min_price: float, max_price: float, result_queue):
    """
    Subprocess worker: launch its own Playwright browser, scrape one crypto, put result in queue.
    Runs in a separate process so Playwright greenlets don't conflict with the main process.
    """
    import sys, os
    # Suppress Node.js EPIPE errors when process is killed
    sys.stderr = open(os.devnull, 'w')
    try:
        from playwright.sync_api import sync_playwright as _sp
        with _sp() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"],
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 720},
                color_scheme="light",
            )
            page = context.new_page()
            page.set_default_timeout(25000)

            # Only block images/fonts/media - NOT stylesheets (breaks React rendering)
            def block_resources(route):
                if route.request.resource_type in ("image", "font", "media"):
                    route.abort()
                else:
                    route.continue_()
            page.route("**/*", block_resources)

            price = _scrape_page_for_price(page, url, crypto, min_price, max_price)

            try:
                page.close()
                browser.close()
            except Exception:
                pass
            result_queue.put((price, None))
    except Exception as e:
        result_queue.put((None, str(e)))


def _scrape_all_cryptos_worker(crypto_urls: list, result_queue):
    """
    Single-browser subprocess worker that scrapes ALL cryptos sequentially.
    Uses one Chromium instance to minimize RAM (~300MB total instead of 300MB per crypto).
    """
    import sys, os
    sys.stderr = open(os.devnull, 'w')
    results = {}
    try:
        from playwright.sync_api import sync_playwright as _sp
        with _sp() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"],
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 720},
                color_scheme="light",
            )

            for crypto, url, min_price, max_price in crypto_urls:
                t0 = time.time()
                page = context.new_page()
                page.set_default_timeout(25000)

                def block_resources(route):
                    if route.request.resource_type in ("image", "font", "media"):
                        route.abort()
                    else:
                        route.continue_()
                page.route("**/*", block_resources)

                price = _scrape_page_for_price(page, url, crypto, min_price, max_price)

                try:
                    page.close()
                except Exception:
                    pass

                elapsed = time.time() - t0
                results[crypto] = (price, elapsed)

            try:
                browser.close()
            except Exception:
                pass
    except Exception as e:
        # If browser crashes mid-way, return whatever we got
        results["_error"] = (None, str(e))

    result_queue.put(results)


def _scrape_page_for_price(page, url: str, crypto: str, min_price: float, max_price: float) -> Optional[float]:
    """
    Navigate to a Polymarket page and extract the strike price.
    Shared logic used by both single-crypto and batch workers.
    """
    try:
        page.goto(url, timeout=25000, wait_until="domcontentloaded")
    except Exception:
        return None

    price = None

    # Wait for React to render - use networkidle for more complete loading
    try:
        page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        # Fallback: fixed wait if networkidle times out
        page.wait_for_timeout(5000)

    for attempt in range(6):
        # Method 1: Direct text search for "PRICE TO BEAT" followed by dollar amount
        try:
            text = page.inner_text("body")
            ptb_idx = text.upper().find("PRICE TO BEAT")
            if ptb_idx >= 0:
                nearby = text[ptb_idx:ptb_idx + 100]
                match = re.search(r'\$?([\d,]+\.\d{2})', nearby)
                if match:
                    val = float(match.group(1).replace(',', ''))
                    if min_price < val < max_price:
                        price = val
                        break
        except Exception:
            pass

        # Method 2: Fall back to DOM/HTML extraction
        if not price:
            price = _extract_price_from_html(page, crypto, min_price, max_price)
            if price:
                break

        page.wait_for_timeout(2000)

    return price


def scrape_all_prices_to_beat(cryptos: list, window_ts: int = None) -> Dict[str, float]:
    """
    Scrape "price to beat" for ALL cryptos using separate Playwright subprocesses.

    Each crypto runs in its own subprocess with its own browser (one at a time for low RAM).
    Any that fail or timeout are retried once.

    Args:
        cryptos: List of crypto symbols (e.g., ["BTC", "ETH", "SOL"])
        window_ts: Unix timestamp for the 15-min window start (auto-calculated if None)

    Returns:
        Dict mapping crypto symbol to price (e.g., {"BTC": 69462.49, "ETH": 2098.25})
    """
    if not HAS_PLAYWRIGHT:
        print(f"[SCRAPE] Playwright not installed. Run: pip install playwright && playwright install chromium")
        return {}

    if window_ts is None:
        window_ts = int(time.time())
        window_ts = (window_ts // 900) * 900

    price_ranges = {
        "BTC": (10000, 500000),
        "ETH": (500, 20000),
        "SOL": (5, 2000),
        "XRP": (0.50, 100),
    }

    results = {}

    print(f"[SCRAPE] Scraping price to beat for {', '.join(cryptos)} (one browser per crypto)...")

    # Pass 1: scrape each crypto in its own subprocess (sequential = low RAM)
    for crypto in cryptos:
        slug = f"{crypto.lower()}-updown-15m-{window_ts}"
        url = f"https://polymarket.com/event/{slug}"
        min_price, max_price = price_ranges.get(crypto, (100, 500000))

        rq = multiprocessing.Queue()
        proc = multiprocessing.Process(
            target=_scrape_single_crypto_worker,
            args=(url, crypto, min_price, max_price, rq),
        )
        t0 = time.time()
        proc.start()
        proc.join(timeout=45)
        elapsed = time.time() - t0

        if proc.is_alive():
            proc.kill()
            proc.join(timeout=5)
            print(f"[SCRAPE] {crypto}: TIMEOUT after {elapsed:.1f}s")
        elif not rq.empty():
            price, error = rq.get_nowait()
            if price:
                results[crypto] = price
                print(f"[SCRAPE] {crypto}: Price to beat = ${price:,.2f} ({elapsed:.1f}s)")
            elif error:
                print(f"[SCRAPE] {crypto}: Error - {error}")
            else:
                print(f"[SCRAPE] {crypto}: FAILED to find price ({elapsed:.1f}s)")
        else:
            print(f"[SCRAPE] {crypto}: No result ({elapsed:.1f}s)")

    # Pass 2: retry missed cryptos once
    missed = [c for c in cryptos if c not in results]
    if missed:
        print(f"[SCRAPE] Retrying missed: {', '.join(missed)}")
        time.sleep(2)
        for crypto in missed:
            slug = f"{crypto.lower()}-updown-15m-{window_ts}"
            url = f"https://polymarket.com/event/{slug}"
            min_price, max_price = price_ranges.get(crypto, (100, 500000))

            rq = multiprocessing.Queue()
            rproc = multiprocessing.Process(
                target=_scrape_single_crypto_worker,
                args=(url, crypto, min_price, max_price, rq),
            )
            rt = time.time()
            rproc.start()
            rproc.join(timeout=45)
            relapsed = time.time() - rt

            if rproc.is_alive():
                rproc.kill()
                rproc.join(timeout=5)
                print(f"[SCRAPE] {crypto} (retry): TIMEOUT after {relapsed:.1f}s")
            elif not rq.empty():
                price, error = rq.get_nowait()
                if price:
                    results[crypto] = price
                    print(f"[SCRAPE] {crypto} (retry): Price to beat = ${price:,.2f} ({relapsed:.1f}s)")
                elif error:
                    print(f"[SCRAPE] {crypto} (retry): Error - {error}")
                else:
                    print(f"[SCRAPE] {crypto} (retry): FAILED ({relapsed:.1f}s)")
            else:
                print(f"[SCRAPE] {crypto} (retry): No result ({relapsed:.1f}s)")

    return results


def _extract_price_from_html(page, crypto: str, min_price: float, max_price: float) -> Optional[float]:
    """
    Extract the "price to beat" from Polymarket page.

    Uses Playwright DOM selectors first (handles React-rendered content),
    then falls back to regex on raw HTML.
    """
    # METHOD 1 (primary): Use Playwright JS to find price in DOM
    # This handles nested spans, React fragments, etc. that break regex
    try:
        price = page.evaluate("""(args) => {
            const { minPrice, maxPrice } = args;

            // Strategy A: Find elements with both tabular-nums AND text-text-secondary
            const spans = document.querySelectorAll('span.tabular-nums, span[class*="tabular-nums"]');
            for (const span of spans) {
                const classes = span.className || '';
                if (classes.includes('text-text-secondary') || span.closest('[class*="text-text-secondary"]')) {
                    const text = span.textContent.replace(/[\\$,\\s]/g, '');
                    const val = parseFloat(text);
                    if (!isNaN(val) && val > minPrice && val < maxPrice) {
                        return val;
                    }
                }
            }

            // Strategy B: Find "price to beat" label and get nearby dollar amount
            const allElements = document.querySelectorAll('*');
            for (const el of allElements) {
                if (el.children.length === 0 && el.textContent.toLowerCase().includes('price to beat')) {
                    // Look at parent and siblings for a dollar amount
                    const parent = el.closest('div') || el.parentElement;
                    if (parent) {
                        const text = parent.textContent;
                        const match = text.match(/\\$([\\d,]+\\.\\d{2})/);
                        if (match) {
                            const val = parseFloat(match[1].replace(/,/g, ''));
                            if (!isNaN(val) && val > minPrice && val < maxPrice) {
                                return val;
                            }
                        }
                    }
                }
            }

            // Strategy C: Find any dollar amount in the expected range near "price to beat"
            const body = document.body.textContent || '';
            const ptbIdx = body.toLowerCase().indexOf('price to beat');
            if (ptbIdx >= 0) {
                // Search within 200 chars after the label
                const nearby = body.substring(ptbIdx, ptbIdx + 200);
                const match = nearby.match(/\\$?([\\d,]+\\.\\d{2})/);
                if (match) {
                    const val = parseFloat(match[1].replace(/,/g, ''));
                    if (!isNaN(val) && val > minPrice && val < maxPrice) {
                        return val;
                    }
                }
            }

            return null;
        }""", {"minPrice": min_price, "maxPrice": max_price})
        if price:
            return price
    except Exception:
        pass

    # METHOD 2 (fallback): Regex on raw HTML
    try:
        html = page.content()
    except Exception:
        return None

    # Regex: span with tabular-nums AND text-text-secondary (handles nested content with .*?)
    exact_patterns = [
        r'<span[^>]*tabular-nums[^>]*text-text-secondary[^>]*>.*?\$?([\d,]+\.\d{2}).*?</span>',
        r'<span[^>]*text-text-secondary[^>]*tabular-nums[^>]*>.*?\$?([\d,]+\.\d{2}).*?</span>',
    ]
    for pattern in exact_patterns:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                val = float(match.group(1).replace(',', ''))
                if min_price < val < max_price:
                    return val
            except (ValueError, TypeError):
                pass

    # Regex: "price to beat" label near a dollar amount
    ptb_patterns = [
        r'(?i)price\s+to\s+beat[^$]*?\$([\d,]+\.\d{2})',
        r'(?i)price\s+to\s+beat.*?>\$?([\d,]+\.\d{2})<',
    ]
    for pattern in ptb_patterns:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                val = float(match.group(1).replace(',', ''))
                if min_price < val < max_price:
                    return val
            except (ValueError, TypeError):
                pass

    return None


def _extract_price_from_page(page, crypto: str, min_price: float, max_price: float) -> Optional[float]:
    """Legacy wrapper - calls the fast HTML-based extraction."""
    return _extract_price_from_html(page, crypto, min_price, max_price)


if __name__ == "__main__":
    # Test the batch scraper (one browser, all cryptos)
    print("=== Testing Batch Price-to-Beat Scraper (one browser session) ===\n")
    results = scrape_all_prices_to_beat(["BTC", "ETH", "SOL"])
    print(f"\nResults:")
    for crypto, price in results.items():
        print(f"  {crypto}: ${price:,.2f}")
    missing = [c for c in ["BTC", "ETH", "SOL"] if c not in results]
    if missing:
        print(f"  Missing: {', '.join(missing)}")
