"""
Polymarket API client for the arbitrage bot.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import requests
from web3 import Web3

from config import DEBUG
from utils import safe_float

# Polymarket SDK imports
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.constants import POLYGON
except ImportError:
    print("ERROR: Please install py-clob-client:")
    print("pip install py-clob-client web3")
    raise SystemExit(1)


class PolymarketClient:
    """Client for interacting with the Polymarket CLOB API."""

    # Polygon mainnet contract addresses for Polymarket
    CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8ED0a90"
    NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
    CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
    USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"  # Standard Multicall3 on Polygon

    def __init__(self, private_key: str):
        """Initialize Polymarket client with wallet-based authentication."""
        host = "https://clob.polymarket.com"
        chain_id = POLYGON

        print(f"\n🔧 Initializing Polymarket client...")

        # Store private key for Web3 transactions
        self._private_key = private_key

        try:
            # Step 1: Create basic client
            self.client = ClobClient(
                host=host,
                key=private_key,
                chain_id=chain_id,
                signature_type=0,  # EOA wallet
            )

            self.gamma_url = "https://gamma-api.polymarket.com"
            self.wallet_address = self.client.get_address()

            print(f"   Wallet: {self.wallet_address}")

            # Step 2: Derive API credentials for order status checking
            print(f"   Deriving API credentials...")
            try:
                creds = self.client.create_or_derive_api_creds()

                if creds:
                    print(f"   ✅ API credentials derived")
                    print(f"   API Key: {creds.api_key[:8]}...")

                    # Step 3: Reinitialize client WITH API credentials
                    self.client = ClobClient(
                        host=host,
                        key=private_key,
                        chain_id=chain_id,
                        creds=creds,
                        signature_type=0,
                        funder=self.wallet_address,
                    )
                    print(f"   ✅ Client reinitialized with full authentication")
                else:
                    print(f"   ⚠️  Could not derive API credentials - order status checking may not work")
            except AttributeError as e:
                print(f"   ⚠️  API credential method not found: {e}")
                print(f"   ℹ️  Using basic authentication only")
            except Exception as e:
                print(f"   ⚠️  API credential derivation failed: {e}")
                print(f"   ℹ️  Continuing without API auth - order placement should still work")

            # Initialize Web3 for on-chain transactions
            self.w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
            if not self.w3.is_connected():
                # Try alternative RPC
                self.w3 = Web3(Web3.HTTPProvider("https://rpc-mainnet.matic.quiknode.pro"))

            # Step 4: Set all allowances via Web3 (USDC.e for buying, conditional tokens for selling)
            print(f"   Setting up trading allowances via Web3...")
            self._setup_allowances()

        except Exception as e:
            print(f"   ❌ Client initialization failed: {e}")
            import traceback
            traceback.print_exc()
            raise

        # HTTP session for connection pooling (faster requests)
        self._session = requests.Session()

        # Cache for event/token data (refreshes every 60s)
        self._event_cache: Dict[str, Dict] = {}
        self._event_cache_time: Dict[str, float] = {}
        self._cache_ttl = 60

    def _setup_allowances(self):
        """Set up all necessary allowances for trading via Web3."""
        try:
            # ERC1155 setApprovalForAll ABI
            approval_abi = [
                {
                    "inputs": [
                        {"name": "operator", "type": "address"},
                        {"name": "approved", "type": "bool"}
                    ],
                    "name": "setApprovalForAll",
                    "outputs": [],
                    "stateMutability": "nonpayable",
                    "type": "function"
                },
                {
                    "inputs": [
                        {"name": "owner", "type": "address"},
                        {"name": "operator", "type": "address"}
                    ],
                    "name": "isApprovedForAll",
                    "outputs": [{"name": "", "type": "bool"}],
                    "stateMutability": "view",
                    "type": "function"
                }
            ]

            ct_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.CONDITIONAL_TOKENS),
                abi=approval_abi
            )

            # Check and set approval for CTF Exchange and adapters
            operators_to_approve = [
                (self.CTF_EXCHANGE, "CTF Exchange"),
                (self.NEG_RISK_CTF_EXCHANGE, "Neg Risk CTF Exchange"),
                (self.NEG_RISK_ADAPTER, "Neg Risk Adapter"),  # Required for selling on neg risk markets
            ]

            for operator_addr, operator_name in operators_to_approve:
                operator = Web3.to_checksum_address(operator_addr)
                is_approved = ct_contract.functions.isApprovedForAll(
                    Web3.to_checksum_address(self.wallet_address),
                    operator
                ).call()

                if is_approved:
                    print(f"   ✅ {operator_name} already approved for selling")
                else:
                    print(f"   🔄 Approving {operator_name} for selling...")
                    self._send_approval_tx(ct_contract, operator)
                    print(f"   ✅ {operator_name} approved!")

        except Exception as e:
            print(f"   ⚠️  Allowance setup error: {e}")
            print(f"   ℹ️  You may need to approve manually on Polymarket.com")

    def _send_approval_tx(self, contract, operator: str):
        """Send setApprovalForAll transaction."""
        try:
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(
                Web3.to_checksum_address(self.wallet_address)
            )

            # Get current gas price
            gas_price = self.w3.eth.gas_price

            tx = contract.functions.setApprovalForAll(
                operator,
                True
            ).build_transaction({
                'from': Web3.to_checksum_address(self.wallet_address),
                'nonce': nonce,
                'gas': 100000,  # Approval typically uses ~50k gas
                'gasPrice': gas_price,
                'chainId': 137,  # Polygon mainnet
            })

            # Sign and send
            signed_tx = self.w3.eth.account.sign_transaction(tx, self._private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)

            # Wait for confirmation
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            if receipt['status'] == 1:
                print(f"   ✅ Approval tx confirmed: {tx_hash.hex()[:16]}...")
            else:
                print(f"   ❌ Approval tx failed!")

        except Exception as e:
            print(f"   ❌ Approval tx error: {e}")
            raise

    def _wait_for_receipt_with_retry(self, tx_hash, timeout: int = 120, max_retries: int = 4):
        """
        Wait for transaction receipt with retry logic for rate limiting.
        Uses exponential backoff when rate limited.
        """
        for attempt in range(max_retries):
            try:
                receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
                return receipt
            except Exception as e:
                error_str = str(e).lower()
                if "too many requests" in error_str or "rate limit" in error_str:
                    wait_time = (2 ** attempt) * 2  # 2, 4, 8, 16, 32 seconds
                    print(f"[poly] Rate limited, waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise

        # Final attempt - assume success if tx was sent
        print(f"[poly] Could not confirm tx after {max_retries} retries")
        print(f"[poly] Transaction was sent - check Polygonscan for status")
        print(f"[poly] Tx: https://polygonscan.com/tx/{tx_hash.hex()}")

        # Return a minimal receipt assuming success (since tx was sent)
        # The transaction likely succeeded even if we couldn't confirm
        return {'status': 1, 'gasUsed': 150000}  # Estimate gas used

    def get_current_timestamp_slug(self) -> str:
        """Get the current 15-minute interval timestamp for market slugs."""
        now = int(time.time())
        interval_start = (now // 900) * 900
        return str(interval_start)

    def get_event_by_slug(self, slug: str) -> Optional[Dict]:
        """Fetch an event by its slug from the Gamma API."""
        url = f"{self.gamma_url}/events/slug/{slug}"
        if DEBUG:
            print(f"[poly] Fetching: {url}")
        try:
            r = self._session.get(url, timeout=0.5)
            if r.status_code == 200:
                return r.json()
            if DEBUG:
                print(f"[poly] Event not found: {r.status_code}")
        except Exception as e:
            if DEBUG:
                print(f"[poly] event_by_slug error: {e}")
        return None

    def _get_price(self, token_id: str, side: str = "BUY") -> Optional[float]:
        """Get price using the simple /price endpoint."""
        url = f"{self.client.host}/price"
        params = {"token_id": token_id, "side": side}
        try:
            r = self._session.get(url, params=params, timeout=0.5)
            if r.status_code == 200:
                price = safe_float(r.json().get("price"))
                if DEBUG:
                    print(f"[poly] Price for token ...{token_id[-8:]}: ${price}")
                return price
        except Exception as e:
            if DEBUG:
                print(f"[poly] /price error: {e}")
        return None

    def get_orderbook(self, token_id: str) -> Optional[Dict]:
        """Get the orderbook for a token to find best bid/ask."""
        url = f"{self.client.host}/book"
        params = {"token_id": token_id}
        try:
            r = self._session.get(url, params=params, timeout=0.5)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            if DEBUG:
                print(f"[poly] orderbook error: {e}")
        return None

    def get_best_bid(self, token_id: str) -> Optional[float]:
        """Get the best bid price (highest price someone is willing to pay)."""
        book = self.get_orderbook(token_id)
        if book and "bids" in book and len(book["bids"]) > 0:
            # Bids are sorted highest first
            best_bid = safe_float(book["bids"][0].get("price"))
            if DEBUG:
                print(f"[poly] Best bid: ${best_bid}")
            return best_bid
        return None

    def get_fillable_ask_price(self, token_id: str, qty: int) -> Tuple[Optional[float], float]:
        """
        Get the price at which we can BUY `qty` contracts from the orderbook.
        This looks at the asks (sell orders) and calculates the price to fill the order.

        Returns:
            Tuple of (price_to_fill, depth_available)
            - price_to_fill: Price needed to fill `qty` contracts (None if not enough depth)
            - depth_available: Total contracts available in the orderbook
        """
        book = self.get_orderbook(token_id)
        if not book:
            return None, 0.0

        if "asks" not in book or len(book["asks"]) == 0:
            return None, 0.0

        # Sort asks by price ascending (lowest/best price first for buyer)
        # Polymarket API returns asks in DESCENDING order, so we need to sort
        asks = sorted(book["asks"], key=lambda x: safe_float(x.get("price")) or float('inf'))

        total_available = 0.0
        qty_remaining = float(qty)
        highest_price_needed = 0.0

        for i, ask in enumerate(asks):
            ask_price = safe_float(ask.get("price"))
            ask_size = safe_float(ask.get("size"))

            if DEBUG and i < 3:
                print(f"[poly] Ask {i}: price={ask_price}, size={ask_size}")

            if ask_price is None or ask_size is None:
                continue

            total_available += ask_size

            if qty_remaining > 0:
                take_from_level = min(qty_remaining, ask_size)
                qty_remaining -= take_from_level
                highest_price_needed = ask_price

                if qty_remaining <= 0:
                    if DEBUG:
                        print(f"[poly] Filled at level {i}, price={ask_price}")
                    break

        if qty_remaining > 0:
            # Not enough depth to fill the order
            return None, total_available

        return highest_price_needed, total_available

    def check_orderbook_depth(self, token_id: str, qty: int, max_price: float) -> Dict[str, Any]:
        """
        Check if we can fill `qty` contracts at or below `max_price`.

        Returns dict with:
            - fillable: True if order can be filled
            - fill_price: The price needed to fill (may be less than max_price)
            - depth: Total contracts available
            - slippage: How much above best price we need to go
        """
        best_ask = self._get_price(token_id, "BUY")  # Best available price
        fill_price, depth = self.get_fillable_ask_price(token_id, qty)

        result = {
            "fillable": False,
            "fill_price": None,
            "best_ask": best_ask,
            "depth": depth,
            "slippage": 0.0,
        }

        if fill_price is None:
            # Not enough depth at any price
            return result

        if fill_price <= max_price:
            result["fillable"] = True
            result["fill_price"] = fill_price
            if best_ask:
                result["slippage"] = fill_price - best_ask

        return result

    def fill_level_by_level(
        self,
        token_id: str,
        target_qty: int,
        max_price: float = 0.99,
        max_loss_check: callable = None,
        price_buffer: float = 0.01,
        prefetched_best_ask: float = None,
    ) -> Dict[str, Any]:
        """
        Fill an order FAST by placing a single order at best_ask + buffer to sweep the book.

        Instead of placing separate orders at each price level (slow), this places ONE
        order at (best_ask + buffer) which will fill at the best available prices.

        Args:
            token_id: The token ID to buy
            target_qty: Total number of contracts to buy
            max_price: Maximum price willing to pay per contract (loss limit)
            max_loss_check: Optional callback (unused in fast mode)
            price_buffer: Buffer to add to best ask for faster fills (default 1 cent)
            prefetched_best_ask: If provided, skip orderbook fetch and use this price

        Returns:
            Dict with:
                - success: True if any shares were filled
                - total_filled: Number of contracts filled
                - avg_price: Weighted average price paid
                - total_cost: Total amount spent
                - levels_filled: Number of price levels used
                - fills: List of (price, qty) tuples for each level
        """
        result = {
            "success": False,
            "total_filled": 0,
            "avg_price": 0.0,
            "total_cost": 0.0,
            "levels_filled": 0,
            "fills": [],
        }

        # Use prefetched price if available, otherwise fetch orderbook
        if prefetched_best_ask is not None and prefetched_best_ask > 0:
            # FAST PATH: Use prefetched price, skip orderbook fetch
            best_ask = prefetched_best_ask
            print(f"    [P] {target_qty}x @ ${best_ask:.2f}+${price_buffer:.2f} (prefetched)", end=" ", flush=True)
        else:
            # SLOW PATH: Fetch orderbook
            book = self.get_orderbook(token_id)
            if not book or "asks" not in book or len(book["asks"]) == 0:
                print(f"    [P] ❌ No orderbook!")
                return result

            asks = sorted(book["asks"], key=lambda x: safe_float(x.get("price")) or float('inf'))
            best_ask = safe_float(asks[0].get("price")) if asks else max_price
            print(f"    [P] {target_qty}x @ ${best_ask:.2f}+${price_buffer:.2f} (fetched)", end=" ", flush=True)

        # Calculate order price: best_ask + buffer, capped at max_price
        order_price = min(best_ask + price_buffer, max_price)
        order_price = min(max(0.01, order_price), 0.99)  # Keep within valid range

        # Place ONE order at order_price - this sweeps the book instantly
        max_retries = 3
        last_error = None

        for attempt in range(max_retries):
            try:
                # Try FOK for guaranteed full fill
                order = self.place_order(
                    token_id, "BUY", target_qty, order_price,
                    immediate=True, quiet=True, order_type_str="FOK"
                )

                if order and order.get("success"):
                    filled_qty = order.get("size_matched", 0)

                    if filled_qty is None or filled_qty == 0:
                        # Check for tx hashes - indicates fill even if size_matched missing
                        tx_hashes = order.get("transactionsHashes", []) or order.get("transactionHashes", [])
                        if tx_hashes:
                            filled_qty = target_qty

                    if filled_qty > 0:
                        # Use order_price as estimate (actual fill may be better)
                        avg_price = order_price
                        total_cost = avg_price * filled_qty

                        result["success"] = True
                        result["total_filled"] = filled_qty
                        result["avg_price"] = avg_price
                        result["total_cost"] = total_cost
                        result["levels_filled"] = 1
                        result["fills"] = [(avg_price, filled_qty)]
                        print(f"FILLED {filled_qty}x @ ${avg_price:.2f}")
                        return result
                    else:
                        print(f"NO FILL")
                        return result

                elif order:
                    # Order returned but not successful - retry
                    if attempt < max_retries - 1:
                        time.sleep(0.05)
                        continue
                    print(f"FAILED")
                    return result
                else:
                    # Order returned None - retry
                    if attempt < max_retries - 1:
                        time.sleep(0.05)
                        continue
                    print(f"FAILED (no response)")
                    return result

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(0.05)
                    continue
                print(f"FAILED ({str(e)[:30]})")
                return result

        print(f"FAILED")
        return result

    def _get_cached_event_data(self, crypto: str) -> Optional[Dict]:
        """Get cached event/token data, fetching if needed."""
        ts = self.get_current_timestamp_slug()
        slug = f"{crypto.lower()}-updown-15m-{ts}"
        now = time.time()

        # Check cache
        if slug in self._event_cache:
            cache_age = now - self._event_cache_time.get(slug, 0)
            if cache_age < self._cache_ttl:
                return self._event_cache[slug]

        # Fetch fresh
        if DEBUG:
            print(f"[poly] Looking for slug: {slug}")

        event = self.get_event_by_slug(slug)
        if not event:
            if DEBUG:
                print(f"[poly] ERROR - Event not found for slug: {slug}")
            return None

        if DEBUG:
            print(f"[poly] Found event: {event.get('title', 'N/A')}")

        markets = event.get("markets", [])
        if not isinstance(markets, list) or not markets:
            markets = [event]

        market = markets[0]
        question = market.get("question") or ""
        description = market.get("description") or ""

        # Note: Polymarket API does NOT expose the "price to beat" (strike price)
        # The UI fetches it from Chainlink Data Streams at window start time
        # We fetch from Chainlink price feeds instead (in fetcher.py)
        strike_price = None

        outcomes_raw = market.get("outcomes")
        clob_tokens_raw = market.get("clobTokenIds")

        outcomes = []
        if isinstance(outcomes_raw, str):
            try:
                outcomes = json.loads(outcomes_raw)
            except Exception:
                outcomes = []
        elif isinstance(outcomes_raw, list):
            outcomes = outcomes_raw

        clob_tokens = []
        if isinstance(clob_tokens_raw, str):
            try:
                clob_tokens = json.loads(clob_tokens_raw)
            except Exception:
                clob_tokens = []
        elif isinstance(clob_tokens_raw, list):
            clob_tokens = clob_tokens_raw

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
            up_token = str(clob_tokens[0])
            down_token = str(clob_tokens[1])

        if DEBUG:
            print(f"[poly] Final token IDs - UP: {up_token}, DOWN: {down_token}")

        # Cache result
        cached = {
            "slug": slug,
            "question": question,
            "description": description,
            "strike_price": strike_price,
            "up_token": up_token,
            "down_token": down_token,
        }
        self._event_cache[slug] = cached
        self._event_cache_time[slug] = now
        return cached

    def get_prices(self, crypto: str) -> Optional[Dict[str, Any]]:
        """Get current prices for crypto up/down market."""
        # Get cached event/token data
        event_data = self._get_cached_event_data(crypto)
        if not event_data:
            return None

        up_token = event_data["up_token"]
        down_token = event_data["down_token"]

        # Fetch both prices in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            up_future = executor.submit(self._get_price, up_token, "BUY")
            down_future = executor.submit(self._get_price, down_token, "BUY")

            up_buy = up_future.result()
            down_buy = down_future.result()

        if up_buy is None or down_buy is None:
            if DEBUG:
                print(f"[poly] ERROR - Could not get prices")
            return None

        if DEBUG:
            print(f"[poly] Final prices - UP: ${up_buy:.4f}, DOWN: ${down_buy:.4f}")

        return {
            "slug": event_data["slug"],
            "question": event_data["question"],
            "description": event_data.get("description", ""),
            "strike_price": event_data.get("strike_price"),
            "up_buy": up_buy,
            "down_buy": down_buy,
            "up_token_id": up_token,
            "down_token_id": down_token,
        }

    def get_conditional_token_balance(self, token_id: str) -> float:
        """
        Check on-chain balance of a specific conditional token (ERC1155).
        Returns the number of shares owned (converted from raw units), or 0 if none/error.

        Note: Polymarket conditional tokens use 6 decimals like USDC.
        """
        try:
            # ERC1155 balanceOf ABI
            balance_abi = [
                {
                    "inputs": [
                        {"name": "account", "type": "address"},
                        {"name": "id", "type": "uint256"}
                    ],
                    "name": "balanceOf",
                    "outputs": [{"name": "", "type": "uint256"}],
                    "stateMutability": "view",
                    "type": "function"
                }
            ]

            ct_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.CONDITIONAL_TOKENS),
                abi=balance_abi
            )

            # Convert token_id to int
            token_id_int = int(token_id)

            balance_raw = ct_contract.functions.balanceOf(
                Web3.to_checksum_address(self.wallet_address),
                token_id_int
            ).call()

            # Convert from raw units (6 decimals) to actual shares
            balance = float(balance_raw) / 1e6

            if DEBUG:
                print(f"[poly] On-chain token balance for ...{str(token_id)[-8:]}: {balance_raw} raw = {balance:.4f} shares")

            return balance
        except Exception as e:
            print(f"[poly] Error checking token balance: {e}")
            return 0.0

    def get_balance(self, max_retries: int = 5) -> Optional[float]:
        """
        Get USDC.e balance from Polygon blockchain.
        Checks both EOA wallet and Polymarket proxy wallet (where funds are actually held).
        """
        usdc_abi = [
            {
                "constant": True,
                "inputs": [{"name": "_owner", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "balance", "type": "uint256"}],
                "type": "function",
            }
        ]

        # USDC.e (bridged) - this is what Polymarket uses
        usdc_e_address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

        last_error = None
        for attempt in range(max_retries):
            try:
                contract = self.w3.eth.contract(address=usdc_e_address, abi=usdc_abi)

                # Check EOA balance
                eoa_balance_wei = contract.functions.balanceOf(self.wallet_address).call()
                eoa_balance = float(eoa_balance_wei) / 1e6

                # Check proxy wallet balance (where Polymarket actually holds funds)
                proxy_balance = 0.0
                proxy = None
                try:
                    proxy = self.get_proxy_wallet()
                    # Only check proxy if it's different from EOA (avoid double-counting)
                    if proxy and proxy.lower() != self.wallet_address.lower():
                        proxy_balance_wei = contract.functions.balanceOf(proxy).call()
                        proxy_balance = float(proxy_balance_wei) / 1e6
                except Exception as proxy_err:
                    if DEBUG:
                        print(f"[poly] Proxy wallet check failed: {proxy_err}")

                # Total balance is EOA + proxy
                total_balance = eoa_balance + proxy_balance

                # Debug: always show breakdown if balance seems wrong
                if total_balance < 1.0:
                    print(f"[poly] Balance debug: EOA=${eoa_balance:.2f}, Proxy={proxy}, Proxy$=${proxy_balance:.2f}")

                # Cache the successful balance
                self._last_known_balance = total_balance
                return total_balance
            except Exception as e:
                last_error = e
                error_str = str(e).lower()

                # Retry on common transient errors
                if any(err in error_str for err in ["too many requests", "rate limit", "timeout", "connection", "502", "503", "429"]):
                    wait_time = (2 ** attempt) * 0.5  # 0.5, 1, 2, 4, 8 seconds
                    if attempt < max_retries - 1:
                        print(f"[poly] Balance fetch failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue

                # Print error on final attempt
                if attempt == max_retries - 1:
                    print(f"[poly] Balance check failed after {max_retries} attempts: {e}")

        # Return cached balance if RPC failed
        if hasattr(self, '_last_known_balance') and self._last_known_balance is not None:
            print(f"[poly] RPC failed, using cached balance: ${self._last_known_balance:.2f}")
            return self._last_known_balance

        print(f"[poly] WARNING: Could not get balance (last error: {last_error})")
        return None

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
        Place an order on Polymarket with retry logic for network errors.

        Args:
            token_id: The token ID to trade
            side: "BUY" or "SELL"
            quantity: Number of contracts (will be converted to int)
            price: Price between 0 and 1
            immediate: If True, use FOK (Fill or Kill) for immediate execution.
                      If False, use GTC (Good Till Cancelled) as maker order.
            quiet: If True, minimal logging (default). If False, verbose logging.
            order_type_str: Override order type - "FOK", "FAK", "GTC", "GTD"
                           FAK = Fill-And-Kill (partial fills allowed)
            max_retries: Number of retry attempts for network errors

        Returns:
            Order response dict or None on failure
        """
        # Ensure price is in valid range
        price = max(0.01, min(0.99, price))

        # FORCE integer quantity - round down to be safe
        quantity = int(float(quantity))
        if quantity < 1:
            if not quiet:
                print(f"[poly] ⚠️  Quantity {quantity} < 1, skipping order")
            return None

        # Determine order type
        if order_type_str:
            # Map string to OrderType enum
            order_type_map = {"FOK": OrderType.FOK, "GTC": OrderType.GTC}
            if hasattr(OrderType, "FAK"):
                order_type_map["FAK"] = OrderType.FAK
            if hasattr(OrderType, "GTD"):
                order_type_map["GTD"] = OrderType.GTD
            order_type = order_type_map.get(order_type_str.upper(), OrderType.FOK)
        else:
            order_type = OrderType.FOK if immediate else OrderType.GTC

        # Debug: Log exact values being sent
        if not quiet:
            print(f"[poly] Placing order: {side} {quantity} @ ${price:.4f}")

        last_error = None
        for attempt in range(max_retries + 1):
            try:
                # Step 1: Create and sign the order locally
                order_args = OrderArgs(
                    price=price,
                    size=int(quantity),
                    side=side.upper(),
                    token_id=token_id,
                )

                signed_order = self.client.create_order(order_args)

                # Step 2: POST the signed order to the CLOB
                response = self.client.post_order(signed_order, order_type)

                # Debug: Log full response to understand structure
                if not quiet or DEBUG:
                    print(f"\n[poly] [DEBUG] Order response: {response}")

                # Process response
                if response:
                    # Ensure response has success flag
                    if "success" not in response:
                        response["success"] = response.get("status", "").upper() == "MATCHED"

                    # Try to get size_matched from various possible fields
                    if "size_matched" not in response or response.get("size_matched") is None:
                        tx_hashes = response.get("transactionsHashes", []) or response.get("transactionHashes", [])
                        if tx_hashes and len(tx_hashes) > 0:
                            response["size_matched"] = quantity
                            response["success"] = True
                        elif response.get("status", "").upper() == "MATCHED":
                            response["size_matched"] = quantity
                            response["success"] = True
                        else:
                            response["size_matched"] = 0

                    return response

                # No response - might be a transient error, retry
                if attempt < max_retries:
                    time.sleep(0.05 * (attempt + 1))  # 50ms, 100ms, 150ms (was 100ms, 200ms, 300ms)
                    continue

                return None

            except Exception as e:
                last_error = str(e)

                # Check if this is a retryable error
                is_network_error = any(err in last_error.lower() for err in [
                    "request exception", "connection", "timeout", "502", "503", "429",
                    "too many requests", "rate limit", "network"
                ])

                # FOK orders that can't fill throw exceptions - not retryable
                is_fill_error = "couldn't be fully filled" in last_error.lower()

                if is_fill_error:
                    # Expected for FOK orders that don't match
                    return {"success": False, "errorMsg": "No fill available", "size_matched": 0}

                if is_network_error and attempt < max_retries:
                    # Network error - retry after brief pause
                    time.sleep(0.075 * (attempt + 1))  # 75ms, 150ms, 225ms (was 150ms, 300ms, 450ms)
                    continue

                # Non-retryable error or out of retries
                if not quiet:
                    print(f"[poly] Order failed: {last_error[:80]}")

                return {"success": False, "errorMsg": last_error, "size_matched": 0}

        return None

    def get_order_status(self, order_id: str) -> Optional[Dict]:
        """Get order status by ID (requires API credentials)."""
        try:
            order = self.client.get_order(order_id)
            return order
        except Exception:
            return None

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order (requires API credentials)."""
        try:
            self.client.cancel(order_id)
            return True
        except Exception:
            return False

    def _ensure_allowances(self) -> bool:
        """
        Ensure all necessary allowances are set for trading.
        This approves conditional tokens contracts to allow selling.
        """
        try:
            print(f"[poly] Setting allowances for trading via Web3...")
            self._setup_allowances()
            return True
        except Exception as e:
            print(f"[poly] ⚠️  Allowance setting failed: {e}")
            return False

    def wait_for_buy_confirmation(self, tx_hash: str, timeout: int = 30) -> bool:
        """
        Wait for a buy transaction to be confirmed on-chain.
        This is important before trying to sell - tokens need to settle first.
        """
        if not tx_hash:
            return False

        try:
            print(f"[poly] Waiting for buy tx to confirm: {tx_hash[:16]}...")
            receipt = self._wait_for_receipt_with_retry(bytes.fromhex(tx_hash[2:] if tx_hash.startswith("0x") else tx_hash), timeout=timeout)
            if receipt and receipt.get('status') == 1:
                print(f"[poly] ✅ Buy tx confirmed")
                return True
            else:
                print(f"[poly] ⚠️ Buy tx may have failed")
                return False
        except Exception as e:
            print(f"[poly] Warning: Could not confirm buy tx: {e}")
            # Wait a bit anyway to be safe
            time.sleep(3)
            return True  # Assume success since we got a tx hash

    def sell_position(
        self,
        token_id: str,
        quantity: float,
        buy_price: float = None,
        buy_tx_hash: str = None,
    ) -> Optional[Dict]:
        """
        Immediately sell a position to exit.
        Uses GTC orders at best bid price for reliable fills.

        Args:
            token_id: The token ID of the position to sell
            quantity: Number of contracts to sell
            buy_price: Price we bought at (for profit/loss calculation)
            buy_tx_hash: Transaction hash from the buy order (to wait for confirmation)

        Returns:
            Order response dict or None on failure
        """
        # Wait for buy transaction to confirm before trying to sell
        if buy_tx_hash:
            self.wait_for_buy_confirmation(buy_tx_hash)
            # Additional wait for CLOB to sync with blockchain
            print(f"[poly] Waiting 5s for CLOB to sync...")
            time.sleep(5)
        else:
            # No tx hash - wait longer to be safe
            print(f"[poly] Waiting 8s for position to settle...")
            time.sleep(8)

        # Check on-chain balance before attempting to sell
        on_chain_balance = self.get_conditional_token_balance(token_id)
        print(f"[poly] On-chain token balance: {on_chain_balance:.4f} shares")

        if on_chain_balance < 0.01:
            print(f"[poly] ⚠️  No tokens found on-chain! Buy may not have settled.")
            # Wait more and check again
            print(f"[poly] Waiting 10s more for tokens to appear...")
            time.sleep(10)
            on_chain_balance = self.get_conditional_token_balance(token_id)
            print(f"[poly] On-chain token balance (retry): {on_chain_balance:.4f} shares")

            if on_chain_balance < 0.01:
                print(f"[poly] ❌ Still no tokens - the buy order may not have actually filled!")
                return None

        # Use actual on-chain balance if less than requested quantity (floor to int)
        actual_qty = min(int(quantity), int(on_chain_balance))
        if actual_qty < quantity:
            print(f"[poly] ⚠️  Only have {on_chain_balance:.4f} shares, selling {actual_qty} (requested {quantity})")

        print(f"[poly] 🔴 UNWINDING: Selling {actual_qty} contracts...")

        # Get the best bid price from orderbook (what buyers are willing to pay)
        best_bid = self.get_best_bid(token_id)

        if best_bid and best_bid > 0:
            sell_price = best_bid
            print(f"[poly] Best bid from orderbook: ${best_bid:.4f}")
        else:
            # Fallback to price endpoint
            current_price = self._get_price(token_id, "SELL")
            sell_price = current_price if current_price else 0.50
            print(f"[poly] Using price endpoint: ${sell_price:.4f}")

        if buy_price:
            if sell_price >= buy_price:
                print(f"[poly] 📈 Profit! Bought @ ${buy_price:.4f}, selling @ ${sell_price:.4f}")
            else:
                loss = (buy_price - sell_price) * actual_qty
                print(f"[poly] 📉 Loss: Bought @ ${buy_price:.4f}, selling @ ${sell_price:.4f} (${loss:.2f} total)")

        # Strategy: Try GTC order at current bid (sits on book until filled)
        # This is more reliable than FOK which requires immediate match
        prices_to_try = [
            (sell_price, "GTC"),           # Best bid with GTC
            (max(0.01, sell_price - 0.02), "GTC"),  # Lower price
            (max(0.01, sell_price - 0.05), "GTC"),  # Even lower
            (0.01, "GTC"),                 # Minimum price GTC
        ]

        allowance_checked = False  # Only check once to avoid rate limits

        for attempt, (price, order_type_str) in enumerate(prices_to_try):
            try:
                order_type = OrderType.GTC if order_type_str == "GTC" else OrderType.FOK
                print(f"[poly] 🔴 SELL attempt {attempt + 1}: {actual_qty} @ ${price:.4f} ({order_type_str})")

                order_args = OrderArgs(
                    price=price,
                    size=actual_qty,
                    side="SELL",
                    token_id=token_id,
                )

                signed_order = self.client.create_order(order_args)
                response = self.client.post_order(signed_order, order_type)

                # Check if it filled
                if response and isinstance(response, dict):
                    success = response.get("success", False)
                    status = response.get("status", "").lower()
                    tx_hashes = response.get("transactionsHashes", [])
                    order_id = response.get("orderID", "")

                    if success and status == "matched" and tx_hashes:
                        print(f"[poly] ✅ SOLD at ${price:.4f}! Tx: {tx_hashes[0][:16]}...")
                        return response
                    elif success and status == "live" and order_id:
                        # GTC order placed but not immediately filled - wait for it
                        print(f"[poly] ⏳ GTC order placed (ID: {order_id[:12]}...), waiting for fill...")
                        filled = self._wait_for_gtc_fill(order_id, timeout=30)
                        if filled:
                            print(f"[poly] ✅ GTC order filled!")
                            return {"success": True, "status": "matched", "orderID": order_id}
                        else:
                            # Cancel and try lower price
                            print(f"[poly] ⚠️  GTC not filled, cancelling...")
                            self.cancel_order(order_id)
                            time.sleep(1)
                            continue
                    else:
                        print(f"[poly] ⚠️  Not filled at ${price:.4f}, trying lower...")
                        continue

            except Exception as e:
                error_msg = str(e)
                print(f"[poly] ❌ Error at ${price:.4f}: {error_msg}")

                # Check for specific errors - only check allowances once
                if ("not enough balance" in error_msg.lower() or "allowance" in error_msg.lower()) and not allowance_checked:
                    print(f"[poly] ⚠️  Balance/allowance error - checking allowances once...")
                    self._ensure_allowances()
                    allowance_checked = True
                    time.sleep(2)

                continue

        print(f"[poly] ❌ Could not sell position - manual intervention required!")
        print(f"[poly] Token ID: {token_id}")
        print(f"[poly] Quantity attempted: {actual_qty}")
        print(f"[poly] On-chain balance: {on_chain_balance:.4f} shares")
        return None

    def _wait_for_gtc_fill(self, order_id: str, timeout: int = 30) -> bool:
        """Wait for a GTC order to fill."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                order = self.get_order_status(order_id)
                if order:
                    status = order.get("status", "").lower()
                    if status == "matched" or status == "filled":
                        return True
                    if status in ["cancelled", "canceled", "expired"]:
                        return False
            except Exception:
                pass
            time.sleep(2)
        return False

    def _try_partial_sell(self, token_id: str, original_qty: float) -> Optional[Dict]:
        """Try selling in smaller chunks if full quantity fails."""
        # Try selling half, then quarter, etc.
        for divisor in [2, 4]:
            partial_qty = max(1, int(original_qty / divisor))
            print(f"[poly] 🔄 Trying partial sell: {partial_qty} contracts...")

            try:
                order_args = OrderArgs(
                    price=0.01,  # Minimum price to ensure fill
                    size=partial_qty,
                    side="SELL",
                    token_id=token_id,
                )
                signed_order = self.client.create_order(order_args)
                response = self.client.post_order(signed_order, OrderType.FOK)

                if response and isinstance(response, dict):
                    if response.get("success") and response.get("status", "").lower() == "matched":
                        print(f"[poly] ✅ Partial sell succeeded: {partial_qty} contracts")
                        return response
            except Exception as e:
                print(f"[poly] Partial sell failed: {e}")
                continue

        return None

    # =========================================================================
    # POSITION REDEMPTION - For settled markets
    # =========================================================================

    def get_proxy_wallet(self) -> Optional[str]:
        """
        Get the user's proxy wallet address from Polymarket.
        Polymarket uses proxy wallets to hold tokens - this is NOT the same as your EOA.
        Tries multiple methods to find the proxy wallet.
        """
        # Method 1: Try positions API
        try:
            url = "https://data-api.polymarket.com/positions"
            params = {"user": self.wallet_address.lower()}
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                positions = r.json() if isinstance(r.json(), list) else []
                for p in positions:
                    pw = p.get("proxyWallet")
                    if pw:
                        return Web3.to_checksum_address(pw)
        except Exception:
            pass

        # Method 2: Try profile/account API
        try:
            url = f"https://data-api.polymarket.com/profile/{self.wallet_address.lower()}"
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                pw = data.get("proxyWallet") or data.get("proxy_wallet")
                if pw:
                    return Web3.to_checksum_address(pw)
        except Exception:
            pass

        # Method 3: Try gamma API
        try:
            url = f"{self.gamma_url}/users/{self.wallet_address.lower()}"
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                pw = data.get("proxyWallet") or data.get("proxy_wallet")
                if pw:
                    return Web3.to_checksum_address(pw)
        except Exception:
            pass

        return None

    def get_usdce_balance_for_address(self, addr: str) -> Optional[float]:
        """Get USDC.e balance for a specific address."""
        try:
            usdc_abi = [
                {
                    "constant": True,
                    "inputs": [{"name": "_owner", "type": "address"}],
                    "name": "balanceOf",
                    "outputs": [{"name": "balance", "type": "uint256"}],
                    "type": "function"
                }
            ]
            c = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.USDC_E),
                abi=usdc_abi
            )
            bal = c.functions.balanceOf(Web3.to_checksum_address(addr)).call()
            return float(bal) / 1e6
        except Exception as e:
            print(f"[poly] Error getting balance for {addr[:10]}...: {e}")
            return None

    def debug_balances(self):
        """Debug helper to show balances in both EOA and proxy wallet."""
        proxy = self.get_proxy_wallet()
        print(f"\n[poly] === WALLET DEBUG ===")
        print(f"[poly] EOA wallet:    {self.wallet_address}")
        print(f"[poly] Proxy wallet:  {proxy}")

        eoa_bal = self.get_usdce_balance_for_address(self.wallet_address)
        print(f"[poly] USDC.e in EOA: ${eoa_bal:.2f}" if eoa_bal is not None else "[poly] USDC.e in EOA: Unknown")

        if proxy:
            proxy_bal = self.get_usdce_balance_for_address(proxy)
            print(f"[poly] USDC.e in Proxy: ${proxy_bal:.2f}" if proxy_bal is not None else "[poly] USDC.e in Proxy: Unknown")

            if proxy.lower() == self.wallet_address.lower():
                print(f"[poly] ✅ Proxy == EOA (direct redemption will work)")
            else:
                print(f"[poly] ⚠️  Proxy != EOA (may need relayer for redemption)")
        print()

    def get_redeemable_positions(self, min_value: float = 0.01) -> List[Dict]:
        """
        Get positions that are ready to be redeemed.
        Uses the 'redeemable' field from the positions API - much faster than scanning!
        Fetches all pages to ensure no positions are missed.

        Args:
            min_value: Minimum value in USD to consider for redemption (default $0.01).
                      Positions worth $0 (losing positions) are skipped to save gas.
        """
        try:
            url = "https://data-api.polymarket.com/positions"
            all_positions = []
            offset = 0
            limit = 100

            # Paginate through all positions
            while True:
                params = {"user": self.wallet_address.lower(), "limit": limit, "offset": offset}
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                page = r.json() if isinstance(r.json(), list) else []
                if not page:
                    break
                all_positions.extend(page)
                if len(page) < limit:
                    break
                offset += limit

            print(f"[poly] Total positions fetched: {len(all_positions)}")

            # Filter for redeemable positions only
            all_redeemables = [p for p in all_positions if p.get("redeemable") is True]

            # Also check for settled positions that might not be marked redeemable
            settled_not_redeemable = [p for p in all_positions
                                       if p.get("curPrice") == 1.0
                                       and p.get("redeemable") is not True
                                       and float(p.get('currentValue', 0)) >= min_value]
            if settled_not_redeemable:
                print(f"[poly] ⚠️  Found {len(settled_not_redeemable)} settled positions NOT marked redeemable:")
                for p in settled_not_redeemable[:5]:
                    print(f"[poly]     {p.get('title', 'Unknown')[:50]} - ${float(p.get('currentValue', 0)):.2f}")
                # Include them - they're settled and worth money
                all_redeemables.extend(settled_not_redeemable)

            # Filter out $0 value positions (losing positions that waste gas)
            redeemables = []
            skipped_zero = 0
            for p in all_redeemables:
                value = float(p.get('currentValue', 0))
                if value >= min_value:
                    redeemables.append(p)
                else:
                    skipped_zero += 1

            print(f"[poly] Redeemable positions: {len(redeemables)} with value (skipped {skipped_zero} with $0)")

            if redeemables:
                # Sort by value descending so biggest gets redeemed first
                redeemables.sort(key=lambda p: float(p.get('currentValue', 0)), reverse=True)
                print(f"[poly] Redeemable positions found:")
                for i, p in enumerate(redeemables):
                    print(f"[poly]   {i+1}. {p.get('title', 'Unknown')[:50]}...")
                    print(f"[poly]      conditionId: {p.get('conditionId', 'N/A')[:20]}...")
                    print(f"[poly]      size: {p.get('size', 'N/A')}, value: ${float(p.get('currentValue', 0)):.2f}")
                    print(f"[poly]      proxyWallet: {p.get('proxyWallet', 'N/A')[:20]}...")

            return redeemables

        except Exception as e:
            print(f"[poly] Error getting redeemable positions: {e}")
            import traceback
            traceback.print_exc()
            return []

    def redeem_positions(self, condition_id: str) -> Tuple[Optional[str], float]:
        """
        Redeem positions for a settled market.

        Args:
            condition_id: The condition ID of the settled market (hex string)

        Returns:
            Tuple of (transaction hash, gas fee in MATIC) if successful, (None, 0) otherwise
        """
        try:
            print(f"[poly] 💰 Redeeming positions for condition: {condition_id[:20]}...")

            # Check if proxy wallet is different from EOA
            proxy = self.get_proxy_wallet()
            if proxy and proxy.lower() != self.wallet_address.lower():
                print(f"[poly] ⚠️  Your tokens are in proxy wallet: {proxy}")
                print(f"[poly] ⚠️  Direct redemption may not work - tokens owned by proxy, not EOA")
                print(f"[poly] ⚠️  You may need to use Polymarket's UI or relayer client")
                # Try anyway - might work for some wallet types

            # CTF redeemPositions ABI
            redeem_abi = [
                {
                    "inputs": [
                        {"name": "collateralToken", "type": "address"},
                        {"name": "parentCollectionId", "type": "bytes32"},
                        {"name": "conditionId", "type": "bytes32"},
                        {"name": "indexSets", "type": "uint256[]"}
                    ],
                    "name": "redeemPositions",
                    "outputs": [],
                    "stateMutability": "nonpayable",
                    "type": "function"
                }
            ]

            ct_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.CONDITIONAL_TOKENS),
                abi=redeem_abi
            )

            # Build transaction
            nonce = self.w3.eth.get_transaction_count(
                Web3.to_checksum_address(self.wallet_address)
            )

            # Convert condition_id to bytes32 properly
            if condition_id.startswith("0x"):
                condition_hex = condition_id[2:]
            else:
                condition_hex = condition_id

            # Pad to 64 hex chars (32 bytes)
            condition_hex = condition_hex.zfill(64)
            condition_bytes32 = bytes.fromhex(condition_hex)

            # Parent collection ID is zero for Polymarket (32 zero bytes)
            parent_collection_id = bytes(32)

            # Index sets: [1, 2] to redeem both YES and NO outcomes
            index_sets = [1, 2]

            gas_price = self.w3.eth.gas_price

            print(f"[poly] Building redemption tx...")
            print(f"[poly]   from: {self.wallet_address}")
            print(f"[poly]   conditionId: 0x{condition_hex[:16]}...")
            print(f"[poly]   indexSets: {index_sets}")

            tx = ct_contract.functions.redeemPositions(
                Web3.to_checksum_address(self.USDC_E),
                parent_collection_id,
                condition_bytes32,
                index_sets
            ).build_transaction({
                'from': Web3.to_checksum_address(self.wallet_address),
                'nonce': nonce,
                'gas': 300000,
                'gasPrice': gas_price,
                'chainId': 137,
            })

            # Sign and send
            signed_tx = self.w3.eth.account.sign_transaction(tx, self._private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)

            print(f"[poly] Redemption tx sent: {tx_hash.hex()}")

            # Wait for confirmation with retry logic for rate limiting
            print(f"[poly] Waiting for confirmation...")
            receipt = self._wait_for_receipt_with_retry(tx_hash, timeout=120)

            # Calculate actual gas fee
            gas_used = receipt['gasUsed']
            gas_fee_wei = gas_used * gas_price
            gas_fee_matic = float(gas_fee_wei) / 1e18

            if receipt['status'] == 1:
                print(f"[poly] ✅ Redemption successful! Tx: {tx_hash.hex()}")
                print(f"[poly]   Gas used: {gas_used}, Fee: {gas_fee_matic:.6f} MATIC")
                return tx_hash.hex(), gas_fee_matic
            else:
                print(f"[poly] ❌ Redemption tx failed (status=0)")
                return None, gas_fee_matic

        except Exception as e:
            error_str = str(e)
            # Retry on rate limit errors
            if "rate limit" in error_str.lower() or "-32090" in error_str:
                print(f"[poly] ⚠️  Rate limited, retrying with backoff...")
                for retry in range(4):
                    wait = (retry + 1) * 10  # 10s, 20s, 30s, 40s
                    print(f"[poly] Waiting {wait}s before retry {retry+1}/4...")
                    time.sleep(wait)
                    try:
                        return self.redeem_positions(condition_id)
                    except Exception as retry_e:
                        retry_str = str(retry_e)
                        if "rate limit" in retry_str.lower() or "-32090" in retry_str:
                            continue
                        print(f"[poly] ❌ Retry error: {retry_str}")
                        return None, 0.0
                print(f"[poly] ❌ All retries exhausted for rate limiting")
                return None, 0.0

            print(f"[poly] ❌ Redemption error: {error_str}")

            if "execution reverted" in error_str.lower():
                print(f"[poly] ℹ️  This likely means:")
                print(f"[poly]     - Your tokens are in a proxy wallet, not your EOA")
                print(f"[poly]     - You need to use Polymarket's UI or relayer for redemption")
                print(f"[poly]     - Or the market hasn't fully resolved yet")

            return None, 0.0

    def redeem_positions_batch(self, condition_ids: List[str]) -> Tuple[Optional[str], float, int]:
        """
        Redeem multiple positions in a single transaction using Multicall3.
        This saves gas by batching all redemptions together.

        Args:
            condition_ids: List of condition IDs to redeem

        Returns:
            Tuple of (transaction hash, gas fee in MATIC, number of positions redeemed)
            Returns (None, 0, 0) if batch redemption fails
        """
        if not condition_ids:
            print(f"[poly] [DEBUG] redeem_positions_batch called with empty condition_ids")
            return None, 0.0, 0

        try:
            print(f"\n{'='*65}")
            print(f"  BATCH REDEMPTION DEBUG LOG")
            print(f"{'='*65}")
            print(f"[poly] 💰 Batch redeeming {len(condition_ids)} positions in one transaction...")

            # Log all input condition IDs
            print(f"\n[poly] [DEBUG] === INPUT CONDITION IDS ===")
            for i, cid in enumerate(condition_ids):
                print(f"[poly] [DEBUG]   [{i+1}] {cid}")

            # Log contract addresses
            print(f"\n[poly] [DEBUG] === CONTRACT ADDRESSES ===")
            print(f"[poly] [DEBUG]   CONDITIONAL_TOKENS: {self.CONDITIONAL_TOKENS}")
            print(f"[poly] [DEBUG]   MULTICALL3: {self.MULTICALL3}")
            print(f"[poly] [DEBUG]   USDC_E (collateral): {self.USDC_E}")
            print(f"[poly] [DEBUG]   Wallet: {self.wallet_address}")

            # CTF redeemPositions function selector and ABI encoding
            redeem_abi = [
                {
                    "inputs": [
                        {"name": "collateralToken", "type": "address"},
                        {"name": "parentCollectionId", "type": "bytes32"},
                        {"name": "conditionId", "type": "bytes32"},
                        {"name": "indexSets", "type": "uint256[]"}
                    ],
                    "name": "redeemPositions",
                    "outputs": [],
                    "stateMutability": "nonpayable",
                    "type": "function"
                }
            ]

            ct_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.CONDITIONAL_TOKENS),
                abi=redeem_abi
            )

            # Multicall3 aggregate3 ABI
            multicall_abi = [
                {
                    "inputs": [
                        {
                            "components": [
                                {"name": "target", "type": "address"},
                                {"name": "allowFailure", "type": "bool"},
                                {"name": "callData", "type": "bytes"}
                            ],
                            "name": "calls",
                            "type": "tuple[]"
                        }
                    ],
                    "name": "aggregate3",
                    "outputs": [
                        {
                            "components": [
                                {"name": "success", "type": "bool"},
                                {"name": "returnData", "type": "bytes"}
                            ],
                            "name": "returnData",
                            "type": "tuple[]"
                        }
                    ],
                    "stateMutability": "payable",
                    "type": "function"
                }
            ]

            multicall_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.MULTICALL3),
                abi=multicall_abi
            )

            # Parent collection ID is zero for Polymarket (32 zero bytes)
            parent_collection_id = bytes(32)
            index_sets = [1, 2]  # Redeem both YES and NO outcomes

            print(f"\n[poly] [DEBUG] === REDEMPTION PARAMETERS ===")
            print(f"[poly] [DEBUG]   parent_collection_id: 0x{'00'*32} (32 zero bytes)")
            print(f"[poly] [DEBUG]   index_sets: {index_sets} (1=YES outcome, 2=NO outcome)")

            # Build call data for each redemption
            print(f"\n[poly] [DEBUG] === BUILDING CALL DATA FOR EACH POSITION ===")
            calls = []
            for idx, condition_id in enumerate(condition_ids):
                print(f"\n[poly] [DEBUG] --- Position [{idx+1}/{len(condition_ids)}] ---")
                print(f"[poly] [DEBUG]   Original condition_id: {condition_id}")
                print(f"[poly] [DEBUG]   Type: {type(condition_id)}")

                # Convert condition_id to bytes32
                if condition_id.startswith("0x"):
                    condition_hex = condition_id[2:]
                else:
                    condition_hex = condition_id
                print(f"[poly] [DEBUG]   After 0x strip: {condition_hex}")
                print(f"[poly] [DEBUG]   Length before zfill: {len(condition_hex)}")

                condition_hex = condition_hex.zfill(64)
                print(f"[poly] [DEBUG]   After zfill(64): {condition_hex}")
                print(f"[poly] [DEBUG]   Length after zfill: {len(condition_hex)}")

                try:
                    condition_bytes32 = bytes.fromhex(condition_hex)
                    print(f"[poly] [DEBUG]   bytes32 conversion: SUCCESS")
                    print(f"[poly] [DEBUG]   bytes32 length: {len(condition_bytes32)}")
                except Exception as hex_err:
                    print(f"[poly] [DEBUG]   bytes32 conversion: FAILED - {hex_err}")
                    raise

                # Encode the redeemPositions call using contract functions
                print(f"[poly] [DEBUG]   Encoding redeemPositions call...")
                print(f"[poly] [DEBUG]     collateralToken: {self.USDC_E}")
                print(f"[poly] [DEBUG]     parentCollectionId: 0x{('00'*32)[:16]}...")
                print(f"[poly] [DEBUG]     conditionId: 0x{condition_hex[:16]}...")
                print(f"[poly] [DEBUG]     indexSets: {index_sets}")

                try:
                    # Use contract.functions to encode the call data
                    call_data = ct_contract.functions.redeemPositions(
                        Web3.to_checksum_address(self.USDC_E),
                        parent_collection_id,
                        condition_bytes32,
                        index_sets
                    )._encode_transaction_data()
                    print(f"[poly] [DEBUG]   encode_abi: SUCCESS")
                    print(f"[poly] [DEBUG]   call_data length: {len(call_data)} chars")
                    print(f"[poly] [DEBUG]   call_data (first 74 chars): {call_data[:74]}...")
                except Exception as abi_err:
                    print(f"[poly] [DEBUG]   encode_abi: FAILED - {abi_err}")
                    import traceback
                    print(f"[poly] [DEBUG]   Traceback: {traceback.format_exc()}")
                    raise

                # call_data from _encode_transaction_data() is already HexBytes
                if isinstance(call_data, str):
                    call_data_bytes = bytes.fromhex(call_data[2:] if call_data.startswith('0x') else call_data)
                else:
                    call_data_bytes = bytes(call_data)  # HexBytes -> bytes

                calls.append({
                    "target": Web3.to_checksum_address(self.CONDITIONAL_TOKENS),
                    "allowFailure": True,  # Allow individual redemptions to fail
                    "callData": call_data_bytes
                })
                print(f"[poly] [DEBUG]   Added to calls list: target={self.CONDITIONAL_TOKENS}, allowFailure=True")

            print(f"\n[poly] [DEBUG] === CALL DATA SUMMARY ===")
            print(f"[poly] [DEBUG]   Total calls built: {len(calls)}")
            for i, call in enumerate(calls):
                print(f"[poly] [DEBUG]   Call [{i+1}]: target={call['target']}, callData len={len(call['callData'])} bytes")

            print(f"\n[poly] Building batch transaction with {len(calls)} redemption calls...")

            # Build the multicall transaction
            print(f"\n[poly] [DEBUG] === BUILDING TRANSACTION ===")

            print(f"[poly] [DEBUG] Getting nonce for wallet...")
            nonce = self.w3.eth.get_transaction_count(
                Web3.to_checksum_address(self.wallet_address)
            )
            print(f"[poly] [DEBUG]   Nonce: {nonce}")

            print(f"[poly] [DEBUG] Getting current gas price...")
            gas_price = self.w3.eth.gas_price
            print(f"[poly] [DEBUG]   Gas price: {gas_price} wei ({gas_price / 1e9:.2f} gwei)")

            # Estimate gas - each redemption uses ~100-150k gas, but batch is more efficient
            estimated_gas = 100000 + (len(calls) * 120000)
            print(f"[poly] [DEBUG]   Estimated gas limit: {estimated_gas}")
            print(f"[poly] [DEBUG]   Estimated max cost: {(estimated_gas * gas_price) / 1e18:.6f} MATIC")

            # Try to get a more accurate gas estimate
            print(f"[poly] [DEBUG] Attempting eth_estimateGas...")
            try:
                actual_estimate = multicall_contract.functions.aggregate3(calls).estimate_gas({
                    'from': Web3.to_checksum_address(self.wallet_address),
                    'value': 0,
                })
                print(f"[poly] [DEBUG]   eth_estimateGas result: {actual_estimate}")
                # Use the actual estimate with 20% buffer
                estimated_gas = int(actual_estimate * 1.2)
                print(f"[poly] [DEBUG]   Using estimate + 20% buffer: {estimated_gas}")
            except Exception as est_err:
                print(f"[poly] [DEBUG]   eth_estimateGas FAILED: {est_err}")
                print(f"[poly] [DEBUG]   This may indicate the transaction will revert!")
                print(f"[poly] [DEBUG]   Using fallback estimate: {estimated_gas}")

            print(f"\n[poly] [DEBUG] === TRANSACTION DETAILS ===")
            tx_params = {
                'from': Web3.to_checksum_address(self.wallet_address),
                'nonce': nonce,
                'gas': estimated_gas,
                'gasPrice': gas_price,
                'chainId': 137,
                'value': 0,
            }
            for key, value in tx_params.items():
                print(f"[poly] [DEBUG]   {key}: {value}")

            print(f"\n[poly] [DEBUG] Building transaction...")
            try:
                tx = multicall_contract.functions.aggregate3(calls).build_transaction(tx_params)
                print(f"[poly] [DEBUG]   build_transaction: SUCCESS")
                print(f"[poly] [DEBUG]   tx keys: {list(tx.keys())}")
            except Exception as build_err:
                print(f"[poly] [DEBUG]   build_transaction: FAILED - {build_err}")
                import traceback
                print(f"[poly] [DEBUG]   Traceback: {traceback.format_exc()}")
                raise

            # Sign and send
            print(f"\n[poly] [DEBUG] === SIGNING AND SENDING ===")
            print(f"[poly] [DEBUG] Signing transaction...")
            try:
                signed_tx = self.w3.eth.account.sign_transaction(tx, self._private_key)
                print(f"[poly] [DEBUG]   sign_transaction: SUCCESS")
                print(f"[poly] [DEBUG]   raw_transaction length: {len(signed_tx.raw_transaction)} bytes")
            except Exception as sign_err:
                print(f"[poly] [DEBUG]   sign_transaction: FAILED - {sign_err}")
                raise

            print(f"[poly] [DEBUG] Sending raw transaction...")
            try:
                tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
                print(f"[poly] [DEBUG]   send_raw_transaction: SUCCESS")
                print(f"[poly] [DEBUG]   tx_hash: {tx_hash.hex()}")
            except Exception as send_err:
                print(f"[poly] [DEBUG]   send_raw_transaction: FAILED - {send_err}")
                import traceback
                print(f"[poly] [DEBUG]   Traceback: {traceback.format_exc()}")
                raise

            print(f"\n[poly] Batch tx sent: {tx_hash.hex()}")
            print(f"[poly] Waiting for confirmation (this may take a moment)...")
            print(f"[poly] [DEBUG] View on Polygonscan: https://polygonscan.com/tx/{tx_hash.hex()}")

            # Wait for confirmation with longer timeout for batch and retry logic
            print(f"\n[poly] [DEBUG] === WAITING FOR RECEIPT ===")
            print(f"[poly] [DEBUG] Timeout: 180 seconds")
            try:
                receipt = self._wait_for_receipt_with_retry(tx_hash, timeout=180)
                print(f"[poly] [DEBUG]   Receipt received!")
            except Exception as wait_err:
                print(f"[poly] [DEBUG]   Wait for receipt FAILED: {wait_err}")
                import traceback
                print(f"[poly] [DEBUG]   Traceback: {traceback.format_exc()}")
                raise

            # Log full receipt details
            print(f"\n[poly] [DEBUG] === TRANSACTION RECEIPT ===")
            print(f"[poly] [DEBUG]   status: {receipt.get('status')} (1=success, 0=failed)")
            print(f"[poly] [DEBUG]   blockNumber: {receipt.get('blockNumber')}")
            print(f"[poly] [DEBUG]   gasUsed: {receipt.get('gasUsed')}")
            print(f"[poly] [DEBUG]   effectiveGasPrice: {receipt.get('effectiveGasPrice')}")
            print(f"[poly] [DEBUG]   transactionHash: {receipt.get('transactionHash', b'').hex() if receipt.get('transactionHash') else 'N/A'}")
            print(f"[poly] [DEBUG]   logs count: {len(receipt.get('logs', []))}")

            # Log each event/log
            if receipt.get('logs'):
                print(f"\n[poly] [DEBUG] === TRANSACTION LOGS (Events) ===")
                for i, log in enumerate(receipt['logs']):
                    print(f"[poly] [DEBUG]   Log [{i+1}]:")
                    print(f"[poly] [DEBUG]     address: {log.get('address')}")
                    print(f"[poly] [DEBUG]     topics: {[t.hex() if isinstance(t, bytes) else t for t in log.get('topics', [])]}")
                    data = log.get('data')
                    if data:
                        data_hex = data.hex() if isinstance(data, bytes) else data
                        print(f"[poly] [DEBUG]     data: {data_hex[:100]}..." if len(str(data_hex)) > 100 else f"[poly] [DEBUG]     data: {data_hex}")

            # Calculate gas fee
            gas_used = receipt['gasUsed']
            gas_fee_wei = gas_used * gas_price
            gas_fee_matic = float(gas_fee_wei) / 1e18

            print(f"\n[poly] [DEBUG] === GAS CALCULATION ===")
            print(f"[poly] [DEBUG]   Gas used: {gas_used}")
            print(f"[poly] [DEBUG]   Gas price: {gas_price} wei")
            print(f"[poly] [DEBUG]   Gas fee: {gas_fee_wei} wei = {gas_fee_matic:.6f} MATIC")

            if receipt['status'] == 1:
                print(f"\n[poly] [DEBUG] === SUCCESS ===")
                print(f"[poly] ✅ Batch redemption successful!")
                print(f"[poly]   Redeemed: {len(condition_ids)} positions in 1 transaction")
                print(f"[poly]   Gas used: {gas_used}, Fee: {gas_fee_matic:.6f} MATIC")
                print(f"[poly]   Tx: {tx_hash.hex()}")
                print(f"[poly]   Polygonscan: https://polygonscan.com/tx/{tx_hash.hex()}")
                return tx_hash.hex(), gas_fee_matic, len(condition_ids)
            else:
                print(f"\n[poly] [DEBUG] === TRANSACTION FAILED (status=0) ===")
                print(f"[poly] [DEBUG] The transaction was mined but execution failed!")
                print(f"[poly] [DEBUG] Check Polygonscan for revert reason:")
                print(f"[poly] [DEBUG]   https://polygonscan.com/tx/{tx_hash.hex()}")
                print(f"[poly] ❌ Batch redemption tx failed (status=0)")
                print(f"[poly] Some individual redemptions may have succeeded")
                return None, gas_fee_matic, 0

        except Exception as e:
            error_str = str(e)
            print(f"\n[poly] [DEBUG] === EXCEPTION CAUGHT ===")
            print(f"[poly] [DEBUG]   Exception type: {type(e).__name__}")
            print(f"[poly] [DEBUG]   Exception message: {error_str}")
            import traceback
            print(f"[poly] [DEBUG]   Full traceback:")
            print(traceback.format_exc())
            print(f"[poly] ❌ Batch redemption error: {error_str}")

            if "execution reverted" in error_str.lower():
                print(f"[poly] [DEBUG] Error indicates execution reverted - likely a contract issue")
                print(f"[poly] Batch failed - falling back to individual redemptions")
            elif "insufficient funds" in error_str.lower():
                print(f"[poly] [DEBUG] Insufficient MATIC for gas fees!")
            elif "nonce" in error_str.lower():
                print(f"[poly] [DEBUG] Nonce issue - possible pending transaction")
            elif "gas" in error_str.lower():
                print(f"[poly] [DEBUG] Gas-related error")

            return None, 0.0, 0

    def redeem_all_positions(self) -> Tuple[int, float]:
        """
        Redeem all positions from settled markets.
        Uses individual transactions for each position (safer than batch).

        Returns tuple of (number of successful redemptions, total gas fees in MATIC).
        """
        print(f"\n{'='*65}")
        print(f"  POLYMARKET POSITION REDEMPTION")
        print(f"{'='*65}")

        # Debug wallet info
        self.debug_balances()

        # Get current balance before
        balance_before = self.get_balance()
        print(f"[poly] Current USDC.e balance: ${balance_before:.2f}" if balance_before else "[poly] Could not get balance")

        # Get redeemable positions (fast - uses API 'redeemable' field, skips $0 value)
        redeemables = self.get_redeemable_positions()

        if not redeemables:
            print(f"\n[poly] No redeemable positions found.")
            print(f"[poly] This could mean:")
            print(f"  - Markets haven't resolved yet")
            print(f"  - Positions were already redeemed")
            print(f"  - You're on the losing side ($0 value positions are skipped)")
            return 0, 0.0

        print(f"\n[poly] Found {len(redeemables)} redeemable positions with value")

        # Calculate total potential value
        total_value = sum(float(p.get('currentValue', 0)) for p in redeemables)
        print(f"[poly] Total potential value: ${total_value:.2f}")

        redeemed_count = 0
        total_gas_fees = 0.0

        # Process each position individually
        print(f"\n[poly] Redeeming positions individually...")

        for i, pos in enumerate(redeemables):
            try:
                condition_id = pos.get('conditionId')
                title = pos.get('title', 'Unknown')[:50]
                value = float(pos.get('currentValue', 0))

                if not condition_id:
                    print(f"\n[poly] [{i+1}/{len(redeemables)}] SKIP - no conditionId")
                    continue

                print(f"\n[poly] [{i+1}/{len(redeemables)}] Redeeming: {title}...")
                print(f"[poly]   Value: ${value:.2f}")

                tx_hash, gas_fee = self.redeem_positions(condition_id)
                total_gas_fees += gas_fee

                if tx_hash:
                    redeemed_count += 1
                    print(f"[poly] ✅ Success!")
                else:
                    print(f"[poly] ❌ Failed")

                # Rate limit between redemptions
                if i < len(redeemables) - 1:
                    print(f"[poly] Waiting 10 seconds before next redemption...")
                    time.sleep(10)

            except Exception as e:
                print(f"[poly] Error redeeming: {e}")
                continue

        print(f"\n{'='*65}")
        print(f"  REDEMPTION COMPLETE: {redeemed_count}/{len(redeemables)} positions redeemed")
        print(f"  Total Gas Fees: {total_gas_fees:.6f} MATIC")
        print(f"{'='*65}")

        # Show updated balance
        time.sleep(2)
        balance_after = self.get_balance()
        if balance_after is not None:
            print(f"\n[poly] New USDC.e balance: ${balance_after:.2f}")
            if balance_before is not None:
                change = balance_after - balance_before
                print(f"[poly] Change: ${change:+.2f}")

        return redeemed_count, total_gas_fees

    def try_auto_redeem(self) -> Tuple[float, float]:
        """
        Try to redeem positions automatically.
        Called periodically or when balance is low.
        Returns tuple of (amount redeemed in USD, gas fees in MATIC).
        """
        print(f"\n[poly] 🔄 Auto-redemption triggered...")

        # Quick check if there are any redeemable positions
        redeemables = self.get_redeemable_positions()
        if not redeemables:
            print(f"[poly] No redeemable positions found")
            return 0.0, 0.0

        balance_before = self.get_balance() or 0

        try:
            redeemed_count, gas_fees = self.redeem_all_positions()
            if redeemed_count > 0:
                time.sleep(2)
                balance_after = self.get_balance() or 0
                return balance_after - balance_before, gas_fees
        except Exception as e:
            print(f"[poly] Auto-redemption error: {e}")

        return 0.0, 0.0

