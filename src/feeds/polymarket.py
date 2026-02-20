"""
Polymarket CLOB adapter — extracted from pm_hourly_clone_bot.py (Pass 1).

Handles:
  - Market discovery via Gamma API
  - Binance spot / hour-open fetching
  - Orderbook top-of-book with depth
  - Order placement (paper in LOG mode, real in LIVE)
  - Order cancellation and status

No behaviour changes from the monolith — just moved into its own module.
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests

import src.config.settings as _settings
from src.config.settings import CRYPTOS, _KEYS_DIR
from src.util.time import utc_now
from src.bot.context import MarketRef, BookTop

# ---------------------------------------------------------------------------
# write_jsonl callback — wired at runtime by the Bot / orchestrator.
# Default is a silent no-op so the module can be imported standalone.
# ---------------------------------------------------------------------------
def _noop_jsonl(event: dict) -> None:  # noqa: ARG001
    """No-op placeholder; replaced by set_jsonl_writer()."""
    pass

_write_jsonl = _noop_jsonl


def set_jsonl_writer(fn) -> None:
    """
    Let the Bot / orchestrator inject the real write_jsonl implementation.

    Usage (in the Bot __init__ or startup):
        from src.feeds.polymarket import set_jsonl_writer
        set_jsonl_writer(my_write_jsonl_function)
    """
    global _write_jsonl
    _write_jsonl = fn


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
        elif _settings.MODE == "LIVE":
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
        except Exception as e:
            print(f"ERROR: Failed to import py_clob_client: {e}")
            print("Try: pip install --upgrade py-clob-client httpx click")
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
                _write_jsonl({"event_type":"MARKET_DISCOVERY_ERROR", "crypto": crypto,
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
            r = self.session.get(url, params={"token_id": token_id}, timeout=2)
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
                          size: float, post_only: bool = True) -> dict:
        """
        Place a limit order on the CLOB.
        Returns dict with fill info: {order_id, filled, fill_qty, fill_price, status}.
        In LOG mode returns a paper result.
        """
        if _settings.MODE == "LOG":
            pid = f"paper_{int(time.time()*1000)}_{random.randint(100,999)}"
            return {"order_id": pid, "filled": True, "fill_qty": int(float(size)),
                    "fill_price": price, "status": "matched"}

        if not self._clob:
            raise RuntimeError("CLOB client not initialised (missing POLYMARKET_PRIVATE_KEY)")

        from py_clob_client.clob_types import OrderArgs, OrderType

        import math as _math
        price = round(max(0.01, min(0.99, price)), 3)  # round to tick (0.001)
        qty   = int(float(size))
        # Polymarket CLOB requires: min 5 shares AND min $1 total order value
        min_qty_for_usd = _math.ceil(1.01 / price) if price > 0 else 5
        clob_min = max(5, min_qty_for_usd)
        if qty < clob_min:
            return {"order_id": "", "filled": False, "fill_qty": 0,
                    "fill_price": 0.0, "status": "rejected"}

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
                status = response.get("status", "").lower()
                size_matched = response.get("size_matched") or 0
                tx_hashes = (response.get("transactionsHashes", [])
                             or response.get("transactionHashes", []))

                filled = False
                fill_qty = 0
                if status == "matched" or tx_hashes:
                    filled = True
                    fill_qty = int(size_matched) if size_matched else qty
                elif status == "live" and oid:
                    fill_qty = self._poll_order_fill(oid, qty, timeout=5)
                    filled = fill_qty > 0

                _write_jsonl({"event_type":"ORDER_PLACED", "order_id": oid,
                             "token_id": token_id[-12:], "side": side,
                             "price": price, "qty": qty,
                             "status": status, "filled": filled,
                             "fill_qty": fill_qty})
                return {"order_id": oid, "filled": filled, "fill_qty": fill_qty,
                        "fill_price": price, "status": status}
        except Exception as e:
            _write_jsonl({"event_type":"ORDER_ERROR", "err": str(e)[:200],
                         "token_id": token_id[-12:], "side": side,
                         "price": price, "qty": qty})
        return {"order_id": "", "filled": False, "fill_qty": 0,
                "fill_price": 0.0, "status": "error"}

    def _poll_order_fill(self, order_id: str, expected_qty: int,
                         timeout: int = 5) -> int:
        """Poll CLOB briefly for GTC order fill. Returns filled qty (0 if not filled)."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                order = self._clob.get_order(order_id)
                if order and isinstance(order, dict):
                    status = order.get("status", "").lower()
                    if status in ("matched", "filled"):
                        return int(order.get("size_matched", expected_qty)
                                   or expected_qty)
                    if status in ("cancelled", "canceled", "expired"):
                        return 0
            except Exception:
                pass
            time.sleep(1)
        return 0

    def cancel_order(self, order_id: str) -> None:
        """Cancel a single order by id."""
        if _settings.MODE == "LOG":
            return
        if not self._clob:
            return
        try:
            self._clob.cancel(order_id)
        except Exception as e:
            _write_jsonl({"event_type":"CANCEL_ERROR", "order_id": order_id, "err": str(e)[:120]})

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

    def get_balances(self) -> List[dict]:
        """Return token balances from the CLOB (for position verification).

        Returns list of dicts: [{token_id, balance, ...}, ...]
        Returns [] in LOG mode or on error.
        """
        if not self._clob:
            return []
        try:
            result = self._clob.get_balances()
            if isinstance(result, list):
                return result
            return []
        except Exception:
            return []
