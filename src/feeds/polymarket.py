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
            if _settings.MODE in ("LIVE", "LIVE_SAFE"):
                self._setup_sell_approvals(private_key)
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

    # ------------------------------------------------------------------ #
    # ERC-1155 approvals for SELL orders
    # ------------------------------------------------------------------ #
    _SELL_OPERATORS = [
        ("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8ED0a90", "CTF Exchange (legacy)"),
        ("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E", "CTF Exchange (SDK)"),
        ("0xC5d563A36AE78145C45a50134d48A1215220f80a", "Neg Risk CTF Exchange"),
        ("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296", "Neg Risk Adapter"),
    ]
    _CT_ADDR = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

    def _setup_sell_approvals(self, private_key: str):
        """Ensure the wallet has approved all exchange operators on the
        Conditional Tokens contract.  Required for SELL orders — the CLOB
        rejects sells if the exchange can't transfer your tokens."""
        try:
            from web3 import Web3
            w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
            if not w3.is_connected():
                w3 = Web3(Web3.HTTPProvider("https://rpc-mainnet.matic.quiknode.pro"))
            abi = [
                {"inputs": [{"name": "operator", "type": "address"},
                             {"name": "approved", "type": "bool"}],
                 "name": "setApprovalForAll", "outputs": [],
                 "stateMutability": "nonpayable", "type": "function"},
                {"inputs": [{"name": "owner", "type": "address"},
                             {"name": "operator", "type": "address"}],
                 "name": "isApprovedForAll",
                 "outputs": [{"name": "", "type": "bool"}],
                 "stateMutability": "view", "type": "function"},
            ]
            ct = w3.eth.contract(
                address=Web3.to_checksum_address(self._CT_ADDR), abi=abi)
            wallet = Web3.to_checksum_address(self._wallet_address)
            for addr, name in self._SELL_OPERATORS:
                op = Web3.to_checksum_address(addr)
                if ct.functions.isApprovedForAll(wallet, op).call():
                    continue
                print(f"[pm] Approving {name}...")
                nonce = w3.eth.get_transaction_count(wallet)
                tx = ct.functions.setApprovalForAll(op, True).build_transaction({
                    'from': wallet, 'nonce': nonce, 'gas': 100000,
                    'gasPrice': w3.eth.gas_price, 'chainId': 137,
                })
                signed = w3.eth.account.sign_transaction(tx, private_key)
                tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                if receipt['status'] != 1:
                    print(f"[pm] WARN: {name} approval tx failed")
                else:
                    print(f"[pm] {name} approved")
        except Exception as e:
            print(f"[pm] WARN: sell approval setup error: {e}")

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
    # ── Fill qty extraction helper ──────────────────────────────────
    @staticmethod
    def _extract_fill_qty(resp: dict) -> float:
        """Extract fill quantity from a CLOB response/order dict.
        Checks multiple field names the API may use."""
        for key in ("size_matched", "sizeMatched", "filled_size",
                     "filledSize", "matchedSize", "executedQty"):
            val = resp.get(key)
            if val:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue
        return 0.0

    # ── Key-shape logging (once per unique shape) ─────────────────
    _logged_key_shapes: set = set()

    @classmethod
    def _log_response_shape_once(cls, resp: dict, context: str) -> None:
        """Log sorted(response.keys()) once per unique key structure."""
        shape = tuple(sorted(resp.keys()))
        if shape not in cls._logged_key_shapes:
            cls._logged_key_shapes.add(shape)
            # Also log nested dict keys (one level deep)
            nested = {}
            for k, v in resp.items():
                if isinstance(v, dict):
                    nested[k] = sorted(v.keys())
            _write_jsonl({"event_type": "RESPONSE_SHAPE_NEW",
                         "context": context,
                         "keys": list(shape),
                         "nested_keys": nested})

    def place_limit_order(self, token_id: str, side: str, price: float,
                          size: float, post_only: bool = True) -> dict:
        """
        Place a limit order on the CLOB.

        Returns dict:
            order_id  : str
            filled    : True | False | "UNKNOWN"
            fill_qty  : float  (0.0 if not confirmed yet)
            fill_price: float
            status    : str    (raw CLOB status)

        filled="UNKNOWN" means CLOB said "matched" but we could not confirm
        the fill quantity.  The caller MUST NOT treat this as unfilled — it
        must reserve capital and let the lifecycle watcher confirm.

        This method NEVER blocks more than ~1s.  All long-running confirmation
        is delegated to the lifecycle watcher.
        """
        if _settings.MODE == "LOG":
            pid = f"paper_{int(time.time()*1000)}_{random.randint(100,999)}"
            return {"order_id": pid, "filled": True, "fill_qty": float(size),
                    "fill_price": price, "status": "matched"}

        if not self._clob:
            raise RuntimeError("CLOB client not initialised (missing POLYMARKET_PRIVATE_KEY)")

        # API error backoff: if recent order failed, wait before retrying
        now = time.time()
        if now < getattr(self, '_api_error_backoff_until', 0.0):
            return {"order_id": "", "filled": False, "fill_qty": 0.0,
                    "fill_price": 0.0, "status": "error"}

        from py_clob_client.clob_types import OrderArgs, OrderType

        import math as _math
        price = round(max(0.01, min(0.99, price)), 3)  # round to tick (0.001)
        qty   = float(size)  # preserve fractional shares — never cast to int
        # Polymarket CLOB requires: min 5 shares AND min $1 total order value
        min_qty_for_usd = _math.ceil(1.01 / price) if price > 0 else 5
        clob_min = max(5, min_qty_for_usd)
        if qty < clob_min:
            return {"order_id": "", "filled": False, "fill_qty": 0.0,
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
                fill_qty = self._extract_fill_qty(response)
                tx_hashes = (response.get("transactionsHashes", [])
                             or response.get("transactionHashes", []))

                # Log response shape once per unique key structure
                self._log_response_shape_once(response, f"post_order_{status}")

                filled = False  # True | False | "UNKNOWN"

                if status == "matched" or tx_hashes:
                    if fill_qty > 0:
                        # Confirmed fill — we have the qty
                        filled = True
                    else:
                        # CLOB said "matched" but no fill qty in response.
                        # Do ONE quick check (non-blocking) then hand off to
                        # lifecycle watcher.  NEVER return filled=False here.
                        if oid:
                            fill_qty = self._quick_fill_check(oid)
                        if fill_qty > 0:
                            filled = True
                        else:
                            # UNKNOWN: matched but unconfirmed — lifecycle
                            # watcher will track until terminal state.
                            filled = "UNKNOWN"
                            _write_jsonl({"event_type": "FILL_UNKNOWN",
                                         "order_id": oid, "status": status,
                                         "requested_qty": qty,
                                         "raw_keys": sorted(response.keys()),
                                         "note": "CLOB matched but no size; "
                                                 "handed to lifecycle watcher"})

                elif status == "live" and oid:
                    # Order resting — lifecycle watcher will track it.
                    # Don't block here.  Don't cancel — let the watcher handle.
                    filled = False

                # Reset backoff on successful API call
                self._api_error_backoff_sec = 0.0
                _write_jsonl({"event_type": "ORDER_PLACED", "order_id": oid,
                             "token_id": token_id[-12:], "side": side,
                             "price": price, "qty": qty,
                             "status": status, "filled": filled,
                             "fill_qty": fill_qty})
                return {"order_id": oid, "filled": filled, "fill_qty": fill_qty,
                        "fill_price": price, "status": status}
        except Exception as e:
            # Exponential backoff: 5s → 10s → 20s → cap at 30s
            prev = getattr(self, '_api_error_backoff_sec', 0.0)
            backoff = min(max(prev * 2, 5.0), 30.0)
            self._api_error_backoff_sec = backoff
            self._api_error_backoff_until = time.time() + backoff
            _write_jsonl({"event_type": "ORDER_ERROR", "err": str(e)[:200],
                         "token_id": token_id[-12:], "side": side,
                         "price": price, "qty": qty,
                         "backoff_sec": backoff})
        return {"order_id": "", "filled": False, "fill_qty": 0.0,
                "fill_price": 0.0, "status": "error"}

    def _quick_fill_check(self, order_id: str) -> float:
        """Single non-blocking check of order status.  Returns fill qty or 0."""
        try:
            order = self._clob.get_order(order_id)
            if order and isinstance(order, dict):
                self._log_response_shape_once(order, "get_order")
                status = order.get("status", "").lower()
                if status in ("matched", "filled"):
                    return self._extract_fill_qty(order)
        except Exception:
            pass
        return 0.0

    def place_immediate_order(self, token_id: str, side: str, price: float,
                              size: float, use_fok: bool = False) -> dict:
        """Place an immediate-execution order (IOC or FOK).

        IOC (use_fok=False, default): Fill whatever is available, kill remainder.
            Allows partial fills — better on thin books.
        FOK (use_fok=True): Fill entire qty or kill the whole order.

        Returns same dict as place_limit_order:
            {order_id, filled, fill_qty, fill_price, status, partial}
        """
        order_label = "FOK" if use_fok else "IOC"
        if _settings.MODE == "LOG":
            pid = f"paper_{int(time.time()*1000)}_{random.randint(100,999)}"
            return {"order_id": pid, "filled": True, "fill_qty": float(size),
                    "fill_price": price, "status": "matched", "partial": False}

        if not self._clob:
            raise RuntimeError("CLOB client not initialised")

        now = time.time()
        if now < getattr(self, '_api_error_backoff_until', 0.0):
            return {"order_id": "", "filled": False, "fill_qty": 0.0,
                    "fill_price": 0.0, "status": "error", "partial": False}

        from py_clob_client.clob_types import OrderArgs, OrderType

        import math as _math
        price = round(max(0.01, min(0.99, price)), 3)
        qty = float(size)
        min_qty_for_usd = _math.ceil(1.01 / price) if price > 0 else 5
        clob_min = max(5, min_qty_for_usd)
        if qty < clob_min:
            return {"order_id": "", "filled": False, "fill_qty": 0.0,
                    "fill_price": 0.0, "status": "rejected", "partial": False}

        # Select order type: FOK or FAK (IOC).  FAK = Fill-And-Kill = IOC.
        if use_fok:
            otype = OrderType.FOK
        elif hasattr(OrderType, "FAK"):
            otype = OrderType.FAK
        else:
            # Fallback: if FAK not available in this py_clob_client version, use FOK
            otype = OrderType.FOK
            order_label = "FOK_FALLBACK"

        try:
            args = OrderArgs(
                price=price, size=qty,
                side=side.upper(), token_id=token_id,
            )
            signed = self._clob.create_order(args)
            response = self._clob.post_order(signed, otype)

            if response and isinstance(response, dict):
                oid = response.get("orderID", "")
                status = response.get("status", "").lower()
                fill_qty = self._extract_fill_qty(response)
                self._log_response_shape_once(response, f"{order_label.lower()}_{status}")

                # ── MATCHED + fill qty confirmed ──
                if status == "matched" and fill_qty > 0:
                    self._api_error_backoff_sec = 0.0
                    partial = (fill_qty < qty)
                    _write_jsonl({"event_type": f"{order_label}_FILLED",
                                 "order_id": oid, "side": side,
                                 "price": price, "requested_qty": qty,
                                 "fill_qty": fill_qty,
                                 "partial": partial})
                    return {"order_id": oid, "filled": True,
                            "fill_qty": fill_qty, "fill_price": price,
                            "status": "matched", "partial": partial}

                # ── MATCHED but no fill qty in response ──
                if status == "matched" and fill_qty == 0:
                    if oid:
                        fill_qty = self._quick_fill_check(oid)
                    if fill_qty > 0:
                        self._api_error_backoff_sec = 0.0
                        partial = (fill_qty < qty)
                        _write_jsonl({"event_type": f"{order_label}_FILLED_DELAYED",
                                     "order_id": oid, "fill_qty": fill_qty,
                                     "partial": partial})
                        return {"order_id": oid, "filled": True,
                                "fill_qty": fill_qty, "fill_price": price,
                                "status": "matched", "partial": partial}
                    # Matched but truly unknown — caller MUST:
                    #   - keep reservation locked
                    #   - hold in-flight guard for this token_id
                    #   - let lifecycle watcher confirm/cancel
                    self._api_error_backoff_sec = 0.0
                    self._immediate_unknown_count = getattr(
                        self, '_immediate_unknown_count', 0) + 1
                    _write_jsonl({"event_type": f"{order_label}_UNKNOWN",
                                 "order_id": oid, "status": status,
                                 "requested_qty": qty,
                                 "unknown_count": self._immediate_unknown_count})
                    return {"order_id": oid, "filled": "UNKNOWN",
                            "fill_qty": 0.0, "fill_price": price,
                            "status": "matched", "partial": False,
                            "hold_inflight": True}

                # ── LIVE/OPEN: IOC came back resting ──
                # Check for partial fill FIRST — IOC can briefly show "live"
                # even after a partial fill has occurred.
                if status in ("live", "open") and oid:
                    self._immediate_live_open_count = getattr(
                        self, '_immediate_live_open_count', 0) + 1
                    # Check if any qty was actually filled before we cancel
                    late_fill_qty = self._quick_fill_check(oid)
                    if late_fill_qty > 0:
                        # Partial fill happened — cancel remainder, return what filled
                        try:
                            self._clob.cancel(oid)
                        except Exception:
                            pass
                        partial = (late_fill_qty < qty)
                        self._api_error_backoff_sec = 0.0
                        _write_jsonl({"event_type": f"{order_label}_LIVE_PARTIAL_FILL",
                                     "order_id": oid, "side": side,
                                     "price": price, "requested_qty": qty,
                                     "fill_qty": late_fill_qty,
                                     "partial": partial,
                                     "live_open_count": self._immediate_live_open_count})
                        return {"order_id": oid, "filled": True,
                                "fill_qty": late_fill_qty, "fill_price": price,
                                "status": "live_partial", "partial": partial}
                    # No fill found — cancel the resting order
                    _write_jsonl({"event_type": f"{order_label}_UNEXPECTED_LIVE",
                                 "order_id": oid, "side": side,
                                 "price": price, "qty": qty,
                                 "live_open_count": self._immediate_live_open_count})
                    try:
                        self._clob.cancel(oid)
                        _write_jsonl({"event_type": f"{order_label}_LIVE_CANCELLED",
                                     "order_id": oid})
                    except Exception as ce:
                        _write_jsonl({"event_type": f"{order_label}_LIVE_CANCEL_FAIL",
                                     "order_id": oid, "err": str(ce)[:120]})
                    return {"order_id": oid, "filled": False,
                            "fill_qty": 0.0, "fill_price": price,
                            "status": "live_cancelled", "partial": False}

                # ── Terminal: killed / cancelled / rejected / other ──
                # Check for late fills: CLOB may report "killed" even after
                # a partial fill has already settled on-chain.
                if oid:
                    late_fill_qty = self._quick_fill_check(oid)
                    if late_fill_qty > 0:
                        self._api_error_backoff_sec = 0.0
                        partial = (late_fill_qty < qty)
                        _write_jsonl({"event_type": f"{order_label}_LATE_FILL",
                                     "order_id": oid, "status": status,
                                     "fill_qty": late_fill_qty,
                                     "partial": partial,
                                     "note": "fill found after terminal status"})
                        return {"order_id": oid, "filled": True,
                                "fill_qty": late_fill_qty, "fill_price": price,
                                "status": "late_fill", "partial": partial}

                self._api_error_backoff_sec = 0.0
                _write_jsonl({"event_type": f"{order_label}_KILLED",
                             "order_id": oid, "status": status,
                             "side": side, "price": price, "qty": qty})
                return {"order_id": oid, "filled": False,
                        "fill_qty": 0.0, "fill_price": price,
                        "status": status or "killed", "partial": False}

        except Exception as e:
            err_str = str(e).lower()

            # ── "Crosses the book" 400: price on wrong side of spread ──
            # Retry once at the correct best price + 1 tick for safety.
            if "crosses" in err_str and not getattr(self, '_cross_retry_active', False):
                self._cross_retry_active = True
                try:
                    book = self.get_top_of_book(token_id, levels=1)
                    if side.upper() == "BUY" and book.ask > 0:
                        retry_price = round(min(book.ask + 0.01, 0.99), 3)
                    elif side.upper() == "SELL" and book.bid > 0:
                        retry_price = round(max(book.bid - 0.01, 0.01), 3)
                    else:
                        retry_price = 0.0
                    _write_jsonl({"event_type": f"{order_label}_CROSSES_RETRY",
                                 "token_id": token_id[-12:], "side": side,
                                 "orig_price": price, "retry_price": retry_price,
                                 "best_bid": book.bid, "best_ask": book.ask})
                    if retry_price > 0:
                        result = self.place_immediate_order(
                            token_id, side, retry_price, size, use_fok=use_fok)
                        return result
                except Exception as retry_err:
                    _write_jsonl({"event_type": f"{order_label}_CROSSES_RETRY_FAIL",
                                 "err": str(retry_err)[:200],
                                 "token_id": token_id[-12:]})
                finally:
                    self._cross_retry_active = False

            prev = getattr(self, '_api_error_backoff_sec', 0.0)
            backoff = min(max(prev * 2, 5.0), 30.0)
            self._api_error_backoff_sec = backoff
            self._api_error_backoff_until = time.time() + backoff
            _write_jsonl({"event_type": f"{order_label}_ERROR",
                         "err": str(e)[:200],
                         "token_id": token_id[-12:], "side": side,
                         "price": price, "qty": qty,
                         "backoff_sec": backoff})
        return {"order_id": "", "filled": False, "fill_qty": 0.0,
                "fill_price": 0.0, "status": "error", "partial": False}

    def place_fok_order(self, token_id: str, side: str, price: float,
                        size: float) -> dict:
        """Backwards-compatible alias: delegates to place_immediate_order(use_fok=True)."""
        return self.place_immediate_order(token_id, side, price, size, use_fok=True)

    def get_order_status(self, order_id: str) -> Optional[dict]:
        """Get order status from CLOB by order_id. Returns dict or None."""
        if not self._clob or not order_id:
            return None
        try:
            result = self._clob.get_order(order_id)
            if isinstance(result, dict):
                self._log_response_shape_once(result, "get_order_status")
                return result
            return None
        except Exception:
            return None

    def get_order_fills(self, order_id: str) -> List[dict]:
        """Fetch fills for a specific order from the CLOB trades/fills endpoint.

        Returns list of fill dicts [{price, size, ...}, ...] or [].
        This is confirmation source C — used when get_order shows matched
        but no size_matched field.
        """
        if not self._clob or not order_id:
            return []
        try:
            # py_clob_client may have get_trades or similar
            if hasattr(self._clob, 'get_trades'):
                trades = self._clob.get_trades(order_id=order_id)
                if isinstance(trades, list):
                    return trades
            # Fallback: try the REST endpoint directly
            url = f"{self.clob_host}/trades"
            r = self.session.get(url,
                                 params={"order_id": order_id},
                                 timeout=5)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

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

    def cancel_all_orders(self) -> int:
        """Cancel all open orders via CLOB batch cancel or one-by-one fallback.

        Returns count of orders cancelled (best-effort).
        """
        if _settings.MODE == "LOG":
            return 0
        if not self._clob:
            return 0

        # Try CLOB batch cancel_all first
        try:
            if hasattr(self._clob, "cancel_all"):
                self._clob.cancel_all()
                _write_jsonl({"event_type": "CANCEL_ALL_BATCH_OK",
                              "ts_ms": int(time.time() * 1000)})
                return -1  # unknown count, but succeeded
        except Exception as e:
            _write_jsonl({"event_type": "CANCEL_ALL_BATCH_FAIL",
                          "err": str(e)[:200],
                          "ts_ms": int(time.time() * 1000)})

        # Fallback: fetch open orders and cancel each
        cancelled = 0
        try:
            open_orders = self.get_open_orders()
        except Exception as e:
            _write_jsonl({"event_type": "CANCEL_ALL_FETCH_FAIL",
                          "err": str(e)[:200],
                          "ts_ms": int(time.time() * 1000)})
            return 0

        for o in open_orders:
            oid = o.get("id") or o.get("orderID") or o.get("order_id", "")
            if not oid:
                continue
            try:
                self._clob.cancel(oid)
                cancelled += 1
            except Exception:
                pass

        _write_jsonl({"event_type": "CANCEL_ALL_FALLBACK_OK",
                      "cancelled": cancelled,
                      "total_open": len(open_orders),
                      "ts_ms": int(time.time() * 1000)})
        return cancelled

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

    # ================================================================== #
    #  6.  USDC WALLET BALANCE (on-chain)
    # ================================================================== #
    # USDC.e on Polygon (6 decimals)
    _USDC_E_ADDR = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

    def get_usdc_balance(self) -> Optional[float]:
        """Fetch on-chain USDC.e balance for the wallet (EOA + proxy).

        Returns balance in USD (float) or None if fetch fails.
        Uses the same Polygon RPC as sell-approval setup.
        """
        if not self._wallet_address:
            return None
        try:
            from web3 import Web3
            w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
            if not w3.is_connected():
                w3 = Web3(Web3.HTTPProvider("https://rpc-mainnet.matic.quiknode.pro"))

            usdc_abi = [
                {"constant": True,
                 "inputs": [{"name": "_owner", "type": "address"}],
                 "name": "balanceOf",
                 "outputs": [{"name": "balance", "type": "uint256"}],
                 "type": "function"},
            ]
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(self._USDC_E_ADDR),
                abi=usdc_abi,
            )
            wallet = Web3.to_checksum_address(self._wallet_address)
            eoa_bal = float(contract.functions.balanceOf(wallet).call()) / 1e6

            # Also check proxy wallet (Polymarket uses smart contract wallets)
            proxy_bal = 0.0
            if self._clob:
                try:
                    proxy = self._clob.get_proxy_wallet_address()
                    if proxy and proxy.lower() != wallet.lower():
                        proxy_bal = float(
                            contract.functions.balanceOf(
                                Web3.to_checksum_address(proxy)
                            ).call()
                        ) / 1e6
                except Exception:
                    pass

            total = eoa_bal + proxy_bal
            _write_jsonl({
                "event_type": "USDC_BALANCE_FETCHED",
                "eoa_bal": round(eoa_bal, 6),
                "proxy_bal": round(proxy_bal, 6),
                "total": round(total, 6),
            })
            return total
        except Exception as e:
            _write_jsonl({
                "event_type": "USDC_BALANCE_ERROR",
                "err": str(e)[:200],
            })
            return None
