"""
Polymarket CLOB API client.
Handles: authentication, order placement, orderbook queries, balance checks,
position selling, and settled-position redemption.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import requests
from web3 import Web3

from src.utils.config import DEBUG

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Polymarket SDK imports
# ---------------------------------------------------------------------------
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.constants import POLYGON
except ImportError:
    print("ERROR: Please install py-clob-client:")
    print("  pip install py-clob-client web3")
    raise SystemExit(1)


class PolymarketClient:
    """Client for interacting with the Polymarket CLOB API."""

    # Polygon mainnet contract addresses
    CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8ED0a90"
    NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
    CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
    USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"

    # --------------------------------------------------------------------- #
    # Initialization
    # --------------------------------------------------------------------- #

    def __init__(self, private_key: str):
        """Initialize Polymarket client with wallet-based authentication."""
        host = "https://clob.polymarket.com"
        chain_id = POLYGON

        print("\n[poly] Initializing Polymarket client...")

        self._private_key = private_key

        try:
            # Step 1: basic client
            self.client = ClobClient(
                host=host,
                key=private_key,
                chain_id=chain_id,
                signature_type=0,
            )

            self.gamma_url = "https://gamma-api.polymarket.com"
            self.wallet_address = self.client.get_address()
            print(f"[poly]   Wallet: {self.wallet_address}")

            # Step 2: derive API credentials
            print("[poly]   Deriving API credentials...")
            try:
                creds = self.client.create_or_derive_api_creds()
                if creds:
                    print(f"[poly]   API Key: {creds.api_key[:8]}...")
                    self.client = ClobClient(
                        host=host,
                        key=private_key,
                        chain_id=chain_id,
                        creds=creds,
                        signature_type=0,
                        funder=self.wallet_address,
                    )
                    print("[poly]   Client reinitialized with full authentication")
                else:
                    print("[poly]   WARN: Could not derive API credentials")
            except Exception as e:
                print(f"[poly]   WARN: API credential derivation failed: {e}")

            # Step 3: Web3 for on-chain transactions
            self.w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
            if not self.w3.is_connected():
                self.w3 = Web3(Web3.HTTPProvider("https://rpc-mainnet.matic.quiknode.pro"))

            # Step 4: set trading allowances
            print("[poly]   Setting up trading allowances...")
            self._setup_allowances()

        except Exception as e:
            print(f"[poly]   ERROR: Client initialization failed: {e}")
            import traceback
            traceback.print_exc()
            raise

        # HTTP session for connection pooling
        self._session = requests.Session()

        # Event/token cache (refreshes every 60s)
        self._event_cache: Dict[str, Dict] = {}
        self._event_cache_time: Dict[str, float] = {}
        self._cache_ttl = 60

    # --------------------------------------------------------------------- #
    # Allowances / approvals
    # --------------------------------------------------------------------- #

    def _setup_allowances(self):
        """Set up all necessary allowances for trading via Web3."""
        try:
            approval_abi = [
                {"inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
                 "name": "setApprovalForAll", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
                {"inputs": [{"name": "owner", "type": "address"}, {"name": "operator", "type": "address"}],
                 "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}],
                 "stateMutability": "view", "type": "function"},
            ]
            ct_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.CONDITIONAL_TOKENS), abi=approval_abi
            )
            operators = [
                (self.CTF_EXCHANGE, "CTF Exchange (legacy)"),
                ("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E", "CTF Exchange (SDK)"),
                (self.NEG_RISK_CTF_EXCHANGE, "Neg Risk CTF Exchange"),
                (self.NEG_RISK_ADAPTER, "Neg Risk Adapter"),
            ]
            for addr, name in operators:
                op = Web3.to_checksum_address(addr)
                approved = ct_contract.functions.isApprovedForAll(
                    Web3.to_checksum_address(self.wallet_address), op
                ).call()
                if approved:
                    print(f"[poly]   {name} already approved")
                else:
                    print(f"[poly]   Approving {name}...")
                    self._send_approval_tx(ct_contract, op)
                    print(f"[poly]   {name} approved")
        except Exception as e:
            print(f"[poly]   WARN: Allowance setup error: {e}")

    def _send_approval_tx(self, contract, operator: str):
        nonce = self.w3.eth.get_transaction_count(Web3.to_checksum_address(self.wallet_address))
        tx = contract.functions.setApprovalForAll(operator, True).build_transaction({
            'from': Web3.to_checksum_address(self.wallet_address),
            'nonce': nonce, 'gas': 100000, 'gasPrice': self.w3.eth.gas_price, 'chainId': 137,
        })
        signed = self.w3.eth.account.sign_transaction(tx, self._private_key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt['status'] != 1:
            raise RuntimeError("Approval tx failed")

    def _ensure_allowances(self) -> bool:
        try:
            self._setup_allowances()
            return True
        except Exception as e:
            print(f"[poly] WARN: Allowance setting failed: {e}")
            return False

    # --------------------------------------------------------------------- #
    # Receipt / retry helpers
    # --------------------------------------------------------------------- #

    def _wait_for_receipt_with_retry(self, tx_hash, timeout: int = 120, max_retries: int = 4):
        for attempt in range(max_retries):
            try:
                return self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
            except Exception as e:
                if "too many requests" in str(e).lower() or "rate limit" in str(e).lower():
                    time.sleep((2 ** attempt) * 2)
                    continue
                raise
        print(f"[poly] Could not confirm tx after {max_retries} retries")
        return {'status': 1, 'gasUsed': 150000}

    # --------------------------------------------------------------------- #
    # Market / event discovery
    # --------------------------------------------------------------------- #

    def get_current_timestamp_slug(self) -> str:
        now = int(time.time())
        return str((now // 900) * 900)

    def get_event_by_slug(self, slug: str) -> Optional[Dict]:
        url = f"{self.gamma_url}/events/slug/{slug}"
        try:
            r = self._session.get(url, timeout=5)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            if DEBUG:
                print(f"[poly] event_by_slug error: {e}")
        return None

    def search_markets(self, query: str, limit: int = 10) -> List[Dict]:
        """Search Gamma API for markets matching a query string."""
        url = f"{self.gamma_url}/markets"
        try:
            r = self._session.get(url, params={"_q": query, "_limit": limit}, timeout=10)
            if r.status_code == 200:
                return r.json() if isinstance(r.json(), list) else []
        except Exception as e:
            if DEBUG:
                print(f"[poly] search_markets error: {e}")
        return []

    def _get_cached_event_data(self, crypto: str) -> Optional[Dict]:
        ts = self.get_current_timestamp_slug()
        slug = f"{crypto.lower()}-updown-15m-{ts}"
        now = time.time()

        if slug in self._event_cache:
            if now - self._event_cache_time.get(slug, 0) < self._cache_ttl:
                return self._event_cache[slug]

        event = self.get_event_by_slug(slug)
        if not event:
            return None

        markets = event.get("markets", [])
        if not isinstance(markets, list) or not markets:
            markets = [event]

        market = markets[0]
        question = market.get("question") or ""
        description = market.get("description") or ""
        strike_price = None

        outcomes_raw = market.get("outcomes")
        clob_tokens_raw = market.get("clobTokenIds")

        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
        clob_tokens = json.loads(clob_tokens_raw) if isinstance(clob_tokens_raw, str) else (clob_tokens_raw or [])

        if not outcomes or not clob_tokens or len(outcomes) < 2 or len(clob_tokens) < 2:
            return None

        up_token, down_token = None, None
        for i, out in enumerate(outcomes):
            if i >= len(clob_tokens):
                continue
            t = str(out).upper()
            if "UP" in t or "YES" in t:
                up_token = str(clob_tokens[i])
            elif "DOWN" in t or "NO" in t:
                down_token = str(clob_tokens[i])

        if not up_token or not down_token:
            up_token, down_token = str(clob_tokens[0]), str(clob_tokens[1])

        cached = {
            "slug": slug, "question": question, "description": description,
            "strike_price": strike_price, "up_token": up_token, "down_token": down_token,
        }
        self._event_cache[slug] = cached
        self._event_cache_time[slug] = now
        return cached

    # --------------------------------------------------------------------- #
    # Price / orderbook
    # --------------------------------------------------------------------- #

    def _get_price(self, token_id: str, side: str = "BUY") -> Optional[float]:
        url = f"{self.client.host}/price"
        try:
            r = self._session.get(url, params={"token_id": token_id, "side": side}, timeout=5)
            if r.status_code == 200:
                return _safe_float(r.json().get("price"))
        except Exception:
            pass
        return None

    def get_orderbook(self, token_id: str) -> Optional[Dict]:
        url = f"{self.client.host}/book"
        try:
            r = self._session.get(url, params={"token_id": token_id}, timeout=5)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return None

    def get_best_bid(self, token_id: str) -> Optional[float]:
        book = self.get_orderbook(token_id)
        if book and "bids" in book and book["bids"]:
            return _safe_float(book["bids"][0].get("price"))
        return None

    def get_best_ask(self, token_id: str) -> Optional[float]:
        book = self.get_orderbook(token_id)
        if book and "asks" in book and book["asks"]:
            asks = sorted(book["asks"], key=lambda x: _safe_float(x.get("price")) or float('inf'))
            return _safe_float(asks[0].get("price"))
        return None

    def get_fillable_ask_price(self, token_id: str, qty: int) -> Tuple[Optional[float], float]:
        book = self.get_orderbook(token_id)
        if not book or "asks" not in book or not book["asks"]:
            return None, 0.0

        asks = sorted(book["asks"], key=lambda x: _safe_float(x.get("price")) or float('inf'))
        total_available = 0.0
        qty_remaining = float(qty)
        highest_price = 0.0

        for ask in asks:
            p = _safe_float(ask.get("price"))
            s = _safe_float(ask.get("size"))
            if p is None or s is None:
                continue
            total_available += s
            if qty_remaining > 0:
                qty_remaining -= min(qty_remaining, s)
                highest_price = p
                if qty_remaining <= 0:
                    break

        return (highest_price if qty_remaining <= 0 else None), total_available

    def get_prices(self, crypto: str) -> Optional[Dict[str, Any]]:
        """Get current prices for a crypto up/down market."""
        event_data = self._get_cached_event_data(crypto)
        if not event_data:
            return None
        with ThreadPoolExecutor(max_workers=2) as ex:
            up_f = ex.submit(self._get_price, event_data["up_token"], "BUY")
            dn_f = ex.submit(self._get_price, event_data["down_token"], "BUY")
            up_buy, down_buy = up_f.result(), dn_f.result()

        if up_buy is None or down_buy is None:
            return None
        return {
            "slug": event_data["slug"], "question": event_data["question"],
            "description": event_data.get("description", ""),
            "strike_price": event_data.get("strike_price"),
            "up_buy": up_buy, "down_buy": down_buy,
            "up_token_id": event_data["up_token"],
            "down_token_id": event_data["down_token"],
        }

    # --------------------------------------------------------------------- #
    # Balance
    # --------------------------------------------------------------------- #

    def get_conditional_token_balance(self, token_id: str) -> float:
        try:
            abi = [{"inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}],
                    "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
                    "stateMutability": "view", "type": "function"}]
            ct = self.w3.eth.contract(address=Web3.to_checksum_address(self.CONDITIONAL_TOKENS), abi=abi)
            raw = ct.functions.balanceOf(Web3.to_checksum_address(self.wallet_address), int(token_id)).call()
            return float(raw) / 1e6
        except Exception as e:
            print(f"[poly] Error checking token balance: {e}")
            return 0.0

    def get_proxy_wallet(self) -> Optional[str]:
        for url_tpl in [
            "https://data-api.polymarket.com/positions?user={addr}",
            "https://data-api.polymarket.com/profile/{addr}",
            "{gamma}/users/{addr}",
        ]:
            try:
                url = url_tpl.format(addr=self.wallet_address.lower(), gamma=self.gamma_url)
                r = requests.get(url, timeout=10)
                if r.status_code != 200:
                    continue
                data = r.json()
                if isinstance(data, list):
                    for p in data:
                        pw = p.get("proxyWallet")
                        if pw:
                            return Web3.to_checksum_address(pw)
                elif isinstance(data, dict):
                    pw = data.get("proxyWallet") or data.get("proxy_wallet")
                    if pw:
                        return Web3.to_checksum_address(pw)
            except Exception:
                continue
        return None

    def get_balance(self, max_retries: int = 5) -> Optional[float]:
        """Get USDC.e balance (EOA + proxy wallet)."""
        usdc_abi = [{"constant": True, "inputs": [{"name": "_owner", "type": "address"}],
                     "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}],
                     "type": "function"}]
        last_error = None
        for attempt in range(max_retries):
            try:
                contract = self.w3.eth.contract(address=self.USDC_E, abi=usdc_abi)
                eoa_bal = float(contract.functions.balanceOf(self.wallet_address).call()) / 1e6
                proxy_bal = 0.0
                try:
                    proxy = self.get_proxy_wallet()
                    if proxy and proxy.lower() != self.wallet_address.lower():
                        proxy_bal = float(contract.functions.balanceOf(proxy).call()) / 1e6
                except Exception:
                    pass
                total = eoa_bal + proxy_bal
                self._last_known_balance = total
                return total
            except Exception as e:
                last_error = e
                if any(err in str(e).lower() for err in ["too many requests", "rate limit", "timeout", "connection"]):
                    time.sleep((2 ** attempt) * 0.5)
                    continue
                if attempt == max_retries - 1:
                    print(f"[poly] Balance check failed: {e}")

        if hasattr(self, '_last_known_balance'):
            return self._last_known_balance
        return None

    # --------------------------------------------------------------------- #
    # Order placement
    # --------------------------------------------------------------------- #

    def place_order(
        self,
        token_id: str,
        side: str,
        quantity: float,
        price: float,
        immediate: bool = True,
        quiet: bool = True,
        order_type_str: str = None,
        max_retries: int = 2,
    ) -> Optional[Dict]:
        """
        Place an order on Polymarket with retry logic.

        Args:
            token_id: Token to trade
            side: "BUY" or "SELL"
            quantity: Number of contracts
            price: Price between 0 and 1
            immediate: Use FOK if True, GTC if False
            order_type_str: Override - "FOK", "FAK", "GTC", "GTD"
        """
        price = max(0.01, min(0.99, price))
        quantity = int(float(quantity))
        if quantity < 1:
            return None

        if order_type_str:
            order_type_map = {"FOK": OrderType.FOK, "GTC": OrderType.GTC}
            if hasattr(OrderType, "FAK"):
                order_type_map["FAK"] = OrderType.FAK
            if hasattr(OrderType, "GTD"):
                order_type_map["GTD"] = OrderType.GTD
            order_type = order_type_map.get(order_type_str.upper(), OrderType.FOK)
        else:
            order_type = OrderType.FOK if immediate else OrderType.GTC

        if not quiet:
            print(f"[poly] Placing order: {side} {quantity} @ ${price:.4f} ({order_type_str or ('FOK' if immediate else 'GTC')})")

        last_error = None
        for attempt in range(max_retries + 1):
            try:
                order_args = OrderArgs(price=price, size=int(quantity), side=side.upper(), token_id=token_id)
                signed_order = self.client.create_order(order_args)
                response = self.client.post_order(signed_order, order_type)

                if not quiet or DEBUG:
                    print(f"[poly] Order response: {response}")

                if response:
                    if "success" not in response:
                        response["success"] = response.get("status", "").upper() == "MATCHED"
                    if "size_matched" not in response or response.get("size_matched") is None:
                        tx_hashes = response.get("transactionsHashes", []) or response.get("transactionHashes", [])
                        if tx_hashes:
                            response["size_matched"] = quantity
                            response["success"] = True
                        elif response.get("status", "").upper() == "MATCHED":
                            response["size_matched"] = quantity
                            response["success"] = True
                        else:
                            response["size_matched"] = 0
                    return response

                if attempt < max_retries:
                    time.sleep(0.05 * (attempt + 1))
                    continue
                return None

            except Exception as e:
                last_error = str(e)
                if "couldn't be fully filled" in last_error.lower():
                    return {"success": False, "errorMsg": "No fill available", "size_matched": 0}
                is_network = any(err in last_error.lower() for err in [
                    "request exception", "connection", "timeout", "502", "503", "429",
                    "too many requests", "rate limit", "network"
                ])
                if is_network and attempt < max_retries:
                    time.sleep(0.075 * (attempt + 1))
                    continue
                if not quiet:
                    print(f"[poly] Order failed: {last_error[:80]}")
                return {"success": False, "errorMsg": last_error, "size_matched": 0}
        return None

    def get_order_status(self, order_id: str) -> Optional[Dict]:
        try:
            return self.client.get_order(order_id)
        except Exception:
            return None

    def cancel_order(self, order_id: str) -> bool:
        try:
            self.client.cancel(order_id)
            return True
        except Exception:
            return False

    def cancel_all_orders(self) -> bool:
        """Cancel all open orders."""
        try:
            self.client.cancel_all()
            return True
        except Exception as e:
            print(f"[poly] cancel_all failed: {e}")
            return False

    # --------------------------------------------------------------------- #
    # Fast fill helper
    # --------------------------------------------------------------------- #

    def fill_level_by_level(
        self,
        token_id: str,
        target_qty: int,
        max_price: float = 0.99,
        price_buffer: float = 0.01,
        prefetched_best_ask: float = None,
    ) -> Dict[str, Any]:
        """Place a single FOK order at best_ask + buffer to sweep the book."""
        result = {"success": False, "total_filled": 0, "avg_price": 0.0,
                  "total_cost": 0.0, "levels_filled": 0, "fills": []}

        if prefetched_best_ask and prefetched_best_ask > 0:
            best_ask = prefetched_best_ask
        else:
            book = self.get_orderbook(token_id)
            if not book or "asks" not in book or not book["asks"]:
                print("[poly] No orderbook!")
                return result
            asks = sorted(book["asks"], key=lambda x: _safe_float(x.get("price")) or float('inf'))
            best_ask = _safe_float(asks[0].get("price")) if asks else max_price

        order_price = min(max(0.01, best_ask + price_buffer), min(max_price, 0.99))

        for attempt in range(3):
            try:
                order = self.place_order(token_id, "BUY", target_qty, order_price,
                                         immediate=True, quiet=True, order_type_str="FOK")
                if order and order.get("success"):
                    filled = order.get("size_matched", 0)
                    if not filled:
                        tx_h = order.get("transactionsHashes", []) or order.get("transactionHashes", [])
                        if tx_h:
                            filled = target_qty
                    if filled and filled > 0:
                        result.update(success=True, total_filled=filled, avg_price=order_price,
                                      total_cost=order_price * filled, levels_filled=1,
                                      fills=[(order_price, filled)])
                        print(f"[poly] FILLED {filled}x @ ${order_price:.2f}")
                        return result
                    return result
                if attempt < 2:
                    time.sleep(0.05)
                    continue
                return result
            except Exception:
                if attempt < 2:
                    time.sleep(0.05)
                    continue
                return result
        return result

    # --------------------------------------------------------------------- #
    # Sell / close position
    # --------------------------------------------------------------------- #

    def wait_for_buy_confirmation(self, tx_hash: str, timeout: int = 30) -> bool:
        if not tx_hash:
            return False
        try:
            raw = tx_hash[2:] if tx_hash.startswith("0x") else tx_hash
            receipt = self._wait_for_receipt_with_retry(bytes.fromhex(raw), timeout=timeout)
            return bool(receipt and receipt.get('status') == 1)
        except Exception:
            time.sleep(3)
            return True

    def sell_position(
        self,
        token_id: str,
        quantity: float,
        buy_price: float = None,
        buy_tx_hash: str = None,
    ) -> Optional[Dict]:
        """Sell/close a position. Tries descending prices with GTC orders."""
        if buy_tx_hash:
            self.wait_for_buy_confirmation(buy_tx_hash)
            print("[poly] Waiting 5s for CLOB to sync...")
            time.sleep(5)
        else:
            print("[poly] Waiting 8s for position to settle...")
            time.sleep(8)

        on_chain = self.get_conditional_token_balance(token_id)
        print(f"[poly] On-chain balance: {on_chain:.4f} shares")

        if on_chain < 0.01:
            print("[poly] No tokens on-chain, waiting 10s more...")
            time.sleep(10)
            on_chain = self.get_conditional_token_balance(token_id)
            if on_chain < 0.01:
                print("[poly] ERROR: Still no tokens")
                return None

        actual_qty = min(int(quantity), int(on_chain))
        print(f"[poly] Selling {actual_qty} contracts...")

        best_bid = self.get_best_bid(token_id)
        sell_price = best_bid if (best_bid and best_bid > 0) else (self._get_price(token_id, "SELL") or 0.50)

        if buy_price:
            diff = sell_price - buy_price
            print(f"[poly] Bought @ ${buy_price:.4f}, selling @ ${sell_price:.4f} (P&L: ${diff * actual_qty:+.2f})")

        prices_to_try = [
            (sell_price, "GTC"),
            (max(0.01, sell_price - 0.02), "GTC"),
            (max(0.01, sell_price - 0.05), "GTC"),
            (0.01, "GTC"),
        ]

        allowance_checked = False
        for attempt, (price, ot_str) in enumerate(prices_to_try):
            try:
                ot = OrderType.GTC if ot_str == "GTC" else OrderType.FOK
                print(f"[poly] SELL attempt {attempt + 1}: {actual_qty} @ ${price:.4f} ({ot_str})")
                args = OrderArgs(price=price, size=actual_qty, side="SELL", token_id=token_id)
                signed = self.client.create_order(args)
                response = self.client.post_order(signed, ot)

                if response and isinstance(response, dict):
                    success = response.get("success", False)
                    status = response.get("status", "").lower()
                    tx_hashes = response.get("transactionsHashes", [])
                    order_id = response.get("orderID", "")

                    if success and status == "matched" and tx_hashes:
                        print(f"[poly] SOLD at ${price:.4f}")
                        return response
                    elif success and status == "live" and order_id:
                        print(f"[poly] GTC order live ({order_id[:12]}...), waiting...")
                        filled = self._wait_for_gtc_fill(order_id, timeout=30)
                        if filled:
                            print("[poly] GTC order filled!")
                            return {"success": True, "status": "matched", "orderID": order_id}
                        else:
                            self.cancel_order(order_id)
                            time.sleep(1)
                            continue
                    else:
                        continue

            except Exception as e:
                err = str(e)
                print(f"[poly] Sell error at ${price:.4f}: {err[:60]}")
                if ("not enough balance" in err.lower() or "allowance" in err.lower()) and not allowance_checked:
                    self._ensure_allowances()
                    allowance_checked = True
                    time.sleep(2)
                continue

        print(f"[poly] ERROR: Could not sell position (token: ...{token_id[-12:]})")
        return None

    def _wait_for_gtc_fill(self, order_id: str, timeout: int = 30) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            try:
                order = self.get_order_status(order_id)
                if order:
                    status = order.get("status", "").lower()
                    if status in ("matched", "filled"):
                        return True
                    if status in ("cancelled", "canceled", "expired"):
                        return False
            except Exception:
                pass
            time.sleep(2)
        return False

    # --------------------------------------------------------------------- #
    # Positions
    # --------------------------------------------------------------------- #

    def get_positions(self, min_value: float = 0.0) -> List[Dict]:
        """Get all open positions from the data API."""
        try:
            url = "https://data-api.polymarket.com/positions"
            all_positions: List[Dict] = []
            offset = 0
            limit = 100
            while True:
                r = requests.get(url, params={"user": self.wallet_address.lower(),
                                              "limit": limit, "offset": offset}, timeout=30)
                r.raise_for_status()
                page = r.json() if isinstance(r.json(), list) else []
                if not page:
                    break
                all_positions.extend(page)
                if len(page) < limit:
                    break
                offset += limit

            if min_value > 0:
                all_positions = [p for p in all_positions if float(p.get('currentValue', 0)) >= min_value]
            return all_positions
        except Exception as e:
            print(f"[poly] Error fetching positions: {e}")
            return []

    def get_redeemable_positions(self, min_value: float = 0.01) -> List[Dict]:
        """Get positions ready for redemption."""
        try:
            all_positions = self.get_positions()
            print(f"[poly] Total positions: {len(all_positions)}")

            redeemables = [p for p in all_positions if p.get("redeemable") is True]
            settled = [p for p in all_positions
                       if p.get("curPrice") == 1.0 and p.get("redeemable") is not True
                       and float(p.get('currentValue', 0)) >= min_value]
            redeemables.extend(settled)

            redeemables = [p for p in redeemables if float(p.get('currentValue', 0)) >= min_value]
            redeemables.sort(key=lambda p: float(p.get('currentValue', 0)), reverse=True)

            print(f"[poly] Redeemable: {len(redeemables)}")
            for i, p in enumerate(redeemables):
                print(f"[poly]   {i+1}. {p.get('title', '?')[:50]} - ${float(p.get('currentValue', 0)):.2f}")
            return redeemables
        except Exception as e:
            print(f"[poly] Error getting redeemable positions: {e}")
            return []

    # --------------------------------------------------------------------- #
    # Redemption
    # --------------------------------------------------------------------- #

    def redeem_positions(self, condition_id: str) -> Tuple[Optional[str], float]:
        """Redeem positions for a settled market."""
        try:
            print(f"[poly] Redeeming condition: {condition_id[:20]}...")
            redeem_abi = [{"inputs": [
                {"name": "collateralToken", "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId", "type": "bytes32"},
                {"name": "indexSets", "type": "uint256[]"}],
                "name": "redeemPositions", "outputs": [],
                "stateMutability": "nonpayable", "type": "function"}]

            ct = self.w3.eth.contract(address=Web3.to_checksum_address(self.CONDITIONAL_TOKENS), abi=redeem_abi)
            nonce = self.w3.eth.get_transaction_count(Web3.to_checksum_address(self.wallet_address))

            cond_hex = (condition_id[2:] if condition_id.startswith("0x") else condition_id).zfill(64)
            gas_price = self.w3.eth.gas_price

            tx = ct.functions.redeemPositions(
                Web3.to_checksum_address(self.USDC_E), bytes(32),
                bytes.fromhex(cond_hex), [1, 2]
            ).build_transaction({
                'from': Web3.to_checksum_address(self.wallet_address),
                'nonce': nonce, 'gas': 300000, 'gasPrice': gas_price, 'chainId': 137,
            })

            signed = self.w3.eth.account.sign_transaction(tx, self._private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            print(f"[poly] Redemption tx: {tx_hash.hex()[:16]}...")

            receipt = self._wait_for_receipt_with_retry(tx_hash, timeout=120)
            gas_fee = float(receipt['gasUsed'] * gas_price) / 1e18

            if receipt['status'] == 1:
                print(f"[poly] Redeemed! Gas: {gas_fee:.6f} MATIC")
                return tx_hash.hex(), gas_fee
            else:
                print("[poly] Redemption tx failed")
                return None, gas_fee

        except Exception as e:
            err = str(e)
            if "rate limit" in err.lower() or "-32090" in err:
                for retry in range(4):
                    time.sleep((retry + 1) * 10)
                    try:
                        return self.redeem_positions(condition_id)
                    except Exception:
                        continue
            print(f"[poly] Redemption error: {err[:100]}")
            return None, 0.0

    def redeem_all_positions(self) -> Tuple[int, float]:
        """Redeem all settled positions individually."""
        redeemables = self.get_redeemable_positions()
        if not redeemables:
            print("[poly] No redeemable positions")
            return 0, 0.0

        total_value = sum(float(p.get('currentValue', 0)) for p in redeemables)
        print(f"[poly] Redeeming {len(redeemables)} positions (${total_value:.2f} total)")

        redeemed = 0
        total_gas = 0.0
        for i, pos in enumerate(redeemables):
            cid = pos.get('conditionId')
            if not cid:
                continue
            tx_hash, gas = self.redeem_positions(cid)
            total_gas += gas
            if tx_hash:
                redeemed += 1
            if i < len(redeemables) - 1:
                time.sleep(10)

        print(f"[poly] Redeemed {redeemed}/{len(redeemables)}, gas: {total_gas:.6f} MATIC")
        return redeemed, total_gas
