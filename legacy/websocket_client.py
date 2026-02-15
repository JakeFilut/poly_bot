"""
WebSocket clients for real-time price updates AND order placement from Kalshi and Polymarket.

Much faster than REST APIs:
- Prices stream in real-time (~50-100ms latency vs 3-5 seconds for scraping)
- Orders sent over persistent connection (~20ms vs ~200ms for REST)
"""

import asyncio
import base64
import json
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Any
from queue import Queue
import concurrent.futures

import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from config import DEBUG, KEYS_DIR


@dataclass
class PriceUpdate:
    """Represents a price update from an exchange."""
    crypto: str
    exchange: str  # "kalshi" or "polymarket"
    strike: Optional[float] = None
    yes_price: Optional[float] = None
    no_price: Optional[float] = None
    yes_ask: Optional[float] = None  # Best ask (to buy YES)
    no_ask: Optional[float] = None   # Best ask (to buy NO)
    ticker: Optional[str] = None     # Market ticker for orders
    token_id: Optional[str] = None   # Token ID (for Polymarket)
    timestamp: float = 0.0


@dataclass
class OrderResult:
    """Represents the result of an order placement."""
    success: bool
    order_id: Optional[str] = None
    filled_qty: int = 0
    fill_price: Optional[float] = None
    error: Optional[str] = None
    tx_hash: Optional[str] = None  # For Polymarket on-chain orders
    raw_response: Optional[Dict] = None


class KalshiWebSocket:
    """
    WebSocket client for Kalshi real-time market data AND order placement.

    Kalshi WebSocket API:
    - URL: wss://api.elections.kalshi.com/trade-api/ws/v2
    - Authentication via headers during handshake
    - Ping/Pong heartbeats every 10 seconds
    - Orders can be placed via WebSocket for faster execution
    """

    def __init__(self, key_id: str, private_key_path: str):
        import os
        self.key_id = key_id
        # Resolve the private key path:
        # 1. Expand ~ to home directory
        # 2. If relative path, resolve against KEYS_DIR
        expanded_path = os.path.expanduser(private_key_path)
        if not os.path.isabs(expanded_path):
            expanded_path = os.path.join(KEYS_DIR, expanded_path)
        with open(expanded_path, "rb") as f:
            self.private_key = serialization.load_pem_private_key(f.read(), password=None)

        self.ws_url = "wss://api.elections.kalshi.com/trade-api/ws/v2"
        self.ws = None
        self.connected = False
        self.subscribed_tickers: Dict[str, str] = {}  # ticker -> crypto
        self.ticker_to_crypto: Dict[str, str] = {}    # ticker -> crypto mapping
        self.crypto_to_ticker: Dict[str, str] = {}    # crypto -> current ticker mapping
        self.prices: Dict[str, PriceUpdate] = {}      # crypto -> latest price
        self.strikes: Dict[str, float] = {}           # crypto -> strike price
        self._callbacks: list = []
        self._running = False
        self._loop = None
        self._thread = None
        self._loop_ready = threading.Event()  # Signal when event loop is ready
        self._connected_event = threading.Event()  # Signal when connected

        # Order tracking
        self._pending_orders: Dict[str, concurrent.futures.Future] = {}  # client_order_id -> Future
        self._order_results: Dict[str, OrderResult] = {}  # order_id -> result

        # Suppress initial price logs until enable_price_logs() is called
        self._suppress_price_logs = True

    def _generate_auth_headers(self) -> Dict[str, str]:
        """Generate authentication headers for WebSocket connection."""
        ts = str(int(time.time() * 1000))
        # For WebSocket, sign the timestamp + GET + /trade-api/ws/v2
        msg = f"{ts}GET/trade-api/ws/v2"

        sig_bytes = self.private_key.sign(
            msg.encode("utf-8"),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256(),
        )
        sig_b64 = base64.b64encode(sig_bytes).decode("utf-8")

        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": sig_b64,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    async def _connect(self):
        """Establish WebSocket connection with authentication."""
        headers = self._generate_auth_headers()

        try:
            self.ws = await websockets.connect(
                self.ws_url,
                additional_headers=headers,
                ping_interval=10,
                ping_timeout=30,
            )
            self.connected = True
            print(f"  [CONN] Kalshi WebSocket connected")
            return True
        except Exception as e:
            print(f"[kalshi-ws] Connection failed: {e}")
            self.connected = False
            return False

    async def _subscribe_to_market(self, ticker: str, crypto: str):
        """Subscribe to orderbook updates for a specific market."""
        if not self.connected or not self.ws:
            return False

        # Subscribe to orderbook delta channel
        subscribe_msg = {
            "id": int(time.time() * 1000),
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": [ticker]
            }
        }

        try:
            await self.ws.send(json.dumps(subscribe_msg))
            self.subscribed_tickers[ticker] = crypto
            return True
        except Exception as e:
            print(f"[kalshi-ws] Subscribe failed for {ticker}: {e}")
            return False

    async def _handle_message(self, message: str):
        """Process incoming WebSocket message."""
        try:
            data = json.loads(message)
            msg_type = data.get("type", "")


            if msg_type == "orderbook_delta" or msg_type == "orderbook_snapshot":
                ticker = data.get("msg", {}).get("market_ticker", "")
                crypto = self.subscribed_tickers.get(ticker)

                if crypto:
                    # Extract best bid/ask prices
                    msg = data.get("msg", {})

                    # Kalshi sends orderbook as arrays: [[price_cents, quantity], ...]
                    # Best bid = highest price someone is willing to pay
                    yes_orders = msg.get("yes", [])
                    no_orders = msg.get("no", [])

                    # Find best (highest) bid from each side
                    yes_bid = None
                    no_bid = None

                    if yes_orders and isinstance(yes_orders, list):
                        # Get highest price from yes orders (best bid for YES)
                        prices = [order[0] for order in yes_orders if isinstance(order, list) and len(order) >= 2]
                        if prices:
                            yes_bid = max(prices)

                    if no_orders and isinstance(no_orders, list):
                        # Get highest price from no orders (best bid for NO)
                        prices = [order[0] for order in no_orders if isinstance(order, list) and len(order) >= 2]
                        if prices:
                            no_bid = max(prices)

                    if yes_bid is not None or no_bid is not None:
                        # Kalshi prices are in cents
                        # To BUY YES, we need to match NO sellers (yes_ask = 100 - no_bid)
                        # To BUY NO, we need to match YES sellers (no_ask = 100 - yes_bid)
                        update = PriceUpdate(
                            crypto=crypto,
                            exchange="kalshi",
                            yes_price=yes_bid / 100.0 if yes_bid else None,
                            no_price=no_bid / 100.0 if no_bid else None,
                            yes_ask=(100 - no_bid) / 100.0 if no_bid else None,  # Cost to buy YES
                            no_ask=(100 - yes_bid) / 100.0 if yes_bid else None,  # Cost to buy NO
                            ticker=ticker,
                            strike=self.strikes.get(crypto),  # Include strike price
                            timestamp=time.time()
                        )

                        # Log first price received for each crypto (unless suppressed)
                        if crypto not in self.prices and not self._suppress_price_logs:
                            yes_ask_c = 100 - no_bid if no_bid else None
                            no_ask_c = 100 - yes_bid if yes_bid else None
                            print(f"[kalshi-ws] {crypto}: YES(bid={yes_bid}¢, ask={yes_ask_c}¢), NO(bid={no_bid}¢, ask={no_ask_c}¢)")

                        self.prices[crypto] = update

                        # Notify callbacks
                        for callback in self._callbacks:
                            try:
                                callback(update)
                            except Exception as e:
                                if DEBUG:
                                    print(f"[kalshi-ws] Callback error: {e}")

            elif msg_type == "subscribed":
                pass  # Subscription confirmed silently

            elif msg_type == "order":
                # Handle order update (fill, cancel, etc.)
                await self._handle_order_update(data.get("msg", {}))

            elif msg_type == "fill":
                # Handle fill notification
                await self._handle_fill(data.get("msg", {}))

            elif msg_type == "error":
                error_msg = data.get("msg", "Unknown error")
                print(f"[kalshi-ws] Error: {error_msg}")
                # Check if this is an order error
                if "order" in str(data).lower():
                    await self._handle_order_error(data)

        except json.JSONDecodeError:
            if DEBUG:
                print(f"[kalshi-ws] Invalid JSON: {message[:100]}")
        except Exception as e:
            if DEBUG:
                print(f"[kalshi-ws] Message handling error: {e}")

    async def _handle_order_update(self, msg: dict):
        """Handle order status update."""
        order_id = msg.get("order_id")
        client_order_id = msg.get("client_order_id")
        status = msg.get("status", "").lower()
        filled_count = msg.get("filled_count", 0)

        if DEBUG:
            print(f"[kalshi-ws] Order update: {order_id} status={status} filled={filled_count}")

        # Resolve pending order future
        if client_order_id and client_order_id in self._pending_orders:
            future = self._pending_orders.pop(client_order_id)
            result = OrderResult(
                success=status in ["filled", "partial", "resting"],
                order_id=order_id,
                filled_qty=filled_count,
                raw_response=msg
            )
            if not future.done():
                future.set_result(result)

    async def _handle_fill(self, msg: dict):
        """Handle fill notification."""
        order_id = msg.get("order_id")
        count = msg.get("count", 0)
        price = msg.get("yes_price") or msg.get("no_price")

        if DEBUG:
            print(f"[kalshi-ws] Fill: order={order_id} count={count} price={price}")

    async def _handle_order_error(self, data: dict):
        """Handle order-related errors."""
        # Try to find client_order_id in error response
        msg = data.get("msg", {})
        if isinstance(msg, dict):
            client_order_id = msg.get("client_order_id")
            if client_order_id and client_order_id in self._pending_orders:
                future = self._pending_orders.pop(client_order_id)
                result = OrderResult(
                    success=False,
                    error=str(msg.get("error", "Unknown error")),
                    raw_response=data
                )
                if not future.done():
                    future.set_result(result)

    async def _place_order_async(
        self,
        ticker: str,
        side: str,
        quantity: int,
        limit_price_cents: int,
    ) -> OrderResult:
        """Place an order via WebSocket (async)."""
        if not self.connected or not self.ws:
            return OrderResult(success=False, error="Not connected")

        client_order_id = f"ws_{int(time.time()*1000)}_{random.randint(1000,9999)}"

        # Create order message
        order_msg = {
            "id": int(time.time() * 1000),
            "cmd": "create_order",
            "params": {
                "ticker": ticker,
                "client_order_id": client_order_id,
                "side": side.lower(),
                "action": "buy",
                "count": quantity,
                "type": "limit",
            }
        }

        # Add price based on side
        if side.lower() == "yes":
            order_msg["params"]["yes_price"] = limit_price_cents
        else:
            order_msg["params"]["no_price"] = limit_price_cents

        # Create future for response
        future = asyncio.get_event_loop().create_future()
        self._pending_orders[client_order_id] = future

        try:
            # Send order
            await self.ws.send(json.dumps(order_msg))
            if DEBUG:
                print(f"[kalshi-ws] Order sent: {side} {quantity}x @ {limit_price_cents}c")

            # Wait for response with timeout
            try:
                result = await asyncio.wait_for(future, timeout=5.0)
                return result
            except asyncio.TimeoutError:
                self._pending_orders.pop(client_order_id, None)
                return OrderResult(success=False, error="Order timeout")

        except Exception as e:
            self._pending_orders.pop(client_order_id, None)
            return OrderResult(success=False, error=str(e))

    async def _listen(self):
        """Main loop to listen for WebSocket messages."""
        while self._running and self.ws:
            try:
                message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                await self._handle_message(message)
            except asyncio.TimeoutError:
                # No message received, send ping
                try:
                    await self.ws.ping()
                except:
                    break
            except websockets.ConnectionClosed:
                print("[kalshi-ws] Connection closed, reconnecting...")
                self.connected = False
                if self._running:
                    await self._reconnect()
            except Exception as e:
                if DEBUG:
                    print(f"[kalshi-ws] Listen error: {e}")
                break

    async def _reconnect(self):
        """Attempt to reconnect to WebSocket."""
        for attempt in range(5):
            await asyncio.sleep(0.5 * (attempt + 1))  # Fast backoff: 0.5s, 1s, 1.5s, 2s, 2.5s

            if await self._connect():
                # Resubscribe to all markets
                for ticker, crypto in list(self.subscribed_tickers.items()):
                    await self._subscribe_to_market(ticker, crypto)
                return True

        print("[kalshi-ws] Failed to reconnect after 5 attempts")
        return False

    def _run_async_loop(self):
        """Run the async event loop in a separate thread."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()  # Signal that event loop is ready
        self._loop.run_until_complete(self._async_main())

    async def _async_main(self):
        """Main async function."""
        if await self._connect():
            self._connected_event.set()  # Signal that connection is established
            await self._listen()
        else:
            # Signal anyway so start() doesn't block forever
            self._connected_event.set()

    def start(self, timeout: float = 5.0):
        """Start the WebSocket connection in a background thread.

        Args:
            timeout: Max seconds to wait for connection (default 5.0)
        """
        if self._running:
            return

        self._running = True
        self._loop_ready.clear()
        self._connected_event.clear()
        self._thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self._thread.start()

        # Wait for event loop to be ready
        if not self._loop_ready.wait(timeout=2.0):
            print("[kalshi-ws] Warning: Event loop not ready after 2s")

        # Wait for connection to be established
        if not self._connected_event.wait(timeout=timeout):
            print("[kalshi-ws] Warning: Connection not established after timeout")

    def stop(self):
        """Stop the WebSocket connection."""
        self._running = False
        if self.ws and self._loop:
            asyncio.run_coroutine_threadsafe(self.ws.close(), self._loop)
        self.connected = False

    def subscribe(self, ticker: str, crypto: str, strike: float = None):
        """Subscribe to a market ticker with optional strike price."""
        # Store strike price if provided
        if strike is not None:
            self.strikes[crypto] = strike

        # Store ticker mapping
        self.ticker_to_crypto[ticker] = crypto
        self.crypto_to_ticker[crypto] = ticker

        # Wait for connection to be established (up to 3s)
        self._connected_event.wait(timeout=3.0)

        if self._loop and self.connected:
            future = asyncio.run_coroutine_threadsafe(
                self._subscribe_to_market(ticker, crypto),
                self._loop
            )
            try:
                future.result(timeout=2.0)
            except Exception as e:
                if DEBUG:
                    print(f"  [DEBUG] {crypto}: Subscription failed: {e}")

    def set_strike(self, crypto: str, strike: float):
        """Set the strike price for a crypto."""
        self.strikes[crypto] = strike
        # Update existing price update with strike
        if crypto in self.prices:
            self.prices[crypto].strike = strike

    def clear_subscriptions(self):
        """Clear all subscriptions and cached prices (call on window change)."""
        self.subscribed_tickers.clear()
        self.ticker_to_crypto.clear()
        self.crypto_to_ticker.clear()
        self.prices.clear()
        self.strikes.clear()
        print(f"  [KALSHI-WS] Cleared all subscriptions and prices")

    def on_price_update(self, callback: Callable[[PriceUpdate], None]):
        """Register a callback for price updates."""
        self._callbacks.append(callback)

    def get_price(self, crypto: str) -> Optional[PriceUpdate]:
        """Get the latest price for a crypto."""
        return self.prices.get(crypto)

    def enable_price_logs(self):
        """Enable price logging (call after all subscriptions are ready)."""
        self._suppress_price_logs = False

    def place_order(
        self,
        ticker: str,
        side: str,
        quantity: int,
        limit_price_cents: int,
    ) -> OrderResult:
        """
        Place an order via WebSocket (synchronous wrapper).

        Args:
            ticker: Market ticker
            side: "yes" or "no"
            quantity: Number of contracts
            limit_price_cents: Price in cents (e.g., 40 for $0.40)

        Returns:
            OrderResult with success status and details
        """
        if not self._loop or not self.connected:
            return OrderResult(success=False, error="Not connected")

        future = asyncio.run_coroutine_threadsafe(
            self._place_order_async(ticker, side, quantity, limit_price_cents),
            self._loop
        )

        try:
            # Wait up to 10 seconds for the order
            return future.result(timeout=10.0)
        except concurrent.futures.TimeoutError:
            return OrderResult(success=False, error="Order timed out")
        except Exception as e:
            return OrderResult(success=False, error=str(e))

    def place_order_for_crypto(
        self,
        crypto: str,
        side: str,
        quantity: int,
        limit_price_cents: int,
    ) -> OrderResult:
        """
        Place an order using crypto symbol (looks up ticker automatically).

        Args:
            crypto: Crypto symbol (BTC, ETH, SOL)
            side: "yes" or "no"
            quantity: Number of contracts
            limit_price_cents: Price in cents

        Returns:
            OrderResult with success status and details
        """
        ticker = self.crypto_to_ticker.get(crypto)
        if not ticker:
            return OrderResult(success=False, error=f"No ticker for {crypto}")

        return self.place_order(ticker, side, quantity, limit_price_cents)


class PolymarketWebSocket:
    """
    WebSocket client for Polymarket real-time market data AND order placement.

    Polymarket WebSocket API:
    - URL: wss://ws-subscriptions-clob.polymarket.com/ws/market for prices
    - Orders placed via CLOB SDK (REST API - fastest available method)

    Note: Polymarket doesn't support WebSocket order placement, so we use their
    REST API through the SDK which is still very fast (~100-200ms).
    """

    def __init__(self, private_key: str = None):
        self.ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        self.ws = None
        self.connected = False
        self.subscribed_assets: Dict[str, str] = {}  # asset_id -> crypto
        self.asset_to_crypto: Dict[str, str] = {}    # asset_id -> crypto
        self.crypto_to_up_token: Dict[str, str] = {}  # crypto -> UP token ID
        self.crypto_to_down_token: Dict[str, str] = {}  # crypto -> DOWN token ID
        self.prices: Dict[str, PriceUpdate] = {}      # crypto -> latest price
        self.strikes: Dict[str, float] = {}           # crypto -> strike price
        self._callbacks: list = []
        self._running = False
        self._loop = None
        self._thread = None
        self._loop_ready = threading.Event()  # Signal when event loop is ready
        self._connected_event = threading.Event()  # Signal when connected

        # Pending subscriptions - batched and sent together
        # Polymarket WS only supports ONE subscription message per connection
        self._pending_assets: List[str] = []  # List of decimal asset IDs to subscribe

        # CLOB client for order placement (optional - only if private_key provided)
        self._clob_client = None
        self._private_key = private_key
        if private_key:
            self._init_clob_client()

    def _init_clob_client(self):
        """Initialize the CLOB client for order placement."""
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.constants import POLYGON

            self._clob_client = ClobClient(
                host="https://clob.polymarket.com",
                key=self._private_key,
                chain_id=POLYGON,
                signature_type=0,
            )

            # Derive API credentials for faster operations
            try:
                creds = self._clob_client.create_or_derive_api_creds()
                if creds:
                    self._clob_client = ClobClient(
                        host="https://clob.polymarket.com",
                        key=self._private_key,
                        chain_id=POLYGON,
                        creds=creds,
                        signature_type=0,
                        funder=self._clob_client.get_address(),
                    )
            except Exception as e:
                print(f"[poly-ws] Could not derive API creds: {e}")

        except ImportError:
            print("[poly-ws] py-clob-client not installed - order placement unavailable")
            self._clob_client = None
        except Exception as e:
            print(f"[poly-ws] CLOB client init error: {e}")
            self._clob_client = None

    async def _connect(self):
        """Establish WebSocket connection."""
        try:
            self.ws = await websockets.connect(
                self.ws_url,
                ping_interval=5,
                ping_timeout=20,
            )
            self.connected = True
            print(f"  [CONN] Polymarket WebSocket connected")
            return True
        except Exception as e:
            print(f"[poly-ws] Connection failed: {e}")
            self.connected = False
            return False

    async def _subscribe_to_all_pending(self):
        """
        Subscribe to ALL pending assets in a single message.

        Polymarket WebSocket only supports ONE subscription message per connection.
        Sending multiple subscriptions replaces previous ones, so we must batch
        all assets into a single subscription.
        """
        if not self.connected or not self.ws:
            print(f"[poly-ws] Cannot subscribe - not connected")
            return False

        if not self._pending_assets:
            return True  # Nothing to subscribe

        # Polymarket WebSocket subscription format (per official docs)
        # Requires both "assets_ids" array AND "type": "market"
        # ALL assets must be in ONE message
        subscribe_msg = {
            "assets_ids": self._pending_assets,
            "type": "market"
        }

        try:
            await self.ws.send(json.dumps(subscribe_msg))
            if DEBUG:
                print(f"[poly-ws] Subscribed to {len(self._pending_assets)} assets")
            return True
        except Exception as e:
            print(f"[poly-ws] Subscribe failed: {e}")
            return False

    def _queue_subscription(self, asset_id: str, crypto: str):
        """Queue an asset for subscription (will be sent in batch later)."""
        # Polymarket WebSocket uses DECIMAL string format for asset IDs
        decimal_asset_id = asset_id
        try:
            if asset_id.startswith("0x"):
                decimal_asset_id = str(int(asset_id, 16))
        except (ValueError, TypeError):
            pass

        # Add to pending list if not already there
        if decimal_asset_id not in self._pending_assets:
            self._pending_assets.append(decimal_asset_id)

        # Store mappings for message processing
        self.subscribed_assets[asset_id] = crypto
        self.subscribed_assets[decimal_asset_id] = crypto
        try:
            hex_id = hex(int(decimal_asset_id))
            self.subscribed_assets[hex_id] = crypto
        except (ValueError, TypeError):
            pass

        if DEBUG:
            print(f"[poly-ws] Queued {crypto}: {decimal_asset_id[:40]}...")

    async def _handle_message(self, message: str):
        """Process incoming WebSocket message."""
        try:
            data = json.loads(message)

            # Log first few messages for debugging (only in DEBUG mode)
            if DEBUG:
                if not hasattr(self, '_msg_count'):
                    self._msg_count = 0
                self._msg_count += 1
                if self._msg_count <= 3:
                    print(f"[poly-ws] RAW MSG #{self._msg_count}: {str(data)[:200]}...")

            # Handle different message formats from Polymarket
            # Format 1: Array of events
            if isinstance(data, list):
                for item in data:
                    await self._process_price_update(item)
            # Format 2: Single event with event_type
            elif isinstance(data, dict):
                event_type = data.get("event_type")

                if event_type == "price_change":
                    # price_change contains price_changes array
                    changes = data.get("price_changes", [])
                    for change in changes:
                        await self._process_price_update(change)
                elif event_type == "last_trade_price":
                    # Direct price update
                    await self._process_price_update(data)
                elif event_type in ("book", "best_bid_ask"):
                    # Order book or bid/ask update
                    await self._process_price_update(data)
                elif event_type:
                    # Unknown event type - still try to process
                    await self._process_price_update(data)
                else:
                    # No event_type - try processing directly
                    await self._process_price_update(data)

        except json.JSONDecodeError:
            # Polymarket sometimes sends non-JSON like "INVALID OPERATION" - ignore
            if DEBUG:
                print(f"[poly-ws] Invalid JSON: {message[:100]}")
        except Exception as e:
            if DEBUG:
                print(f"[poly-ws] Message handling error: {e}")

    async def _process_price_update(self, data: dict):
        """Process a single price update."""
        if not isinstance(data, dict):
            return

        asset_id = data.get("asset_id") or data.get("market")
        crypto = self.subscribed_assets.get(asset_id) if asset_id else None

        # If not found directly, try converting between hex and decimal formats
        if asset_id and not crypto:
            converted_id = None
            try:
                if str(asset_id).startswith("0x"):
                    # Convert hex to decimal string
                    converted_id = str(int(asset_id, 16))
                else:
                    # Convert decimal to hex string
                    converted_id = hex(int(asset_id))
                crypto = self.subscribed_assets.get(converted_id)
            except (ValueError, TypeError):
                pass

        if crypto:
            # Extract prices from message - check multiple possible fields
            # Polymarket WS events: price (trade price), best_bid (bid), best_ask (ask)
            # For BUYING, we need best_ask (the cost to buy)
            # For reference, best_bid is what someone will pay us
            price = data.get("price")
            best_bid = data.get("best_bid")
            best_ask = data.get("best_ask")

            # Prefer best_bid for the "price" (what we'd receive if selling)
            if price is None and best_bid is not None:
                price = best_bid

            if price is not None or best_ask is not None:
                # Determine if this is UP or DOWN token based on our mapping
                up_token_decimal = self.crypto_to_up_token.get(crypto)
                up_token_hex = getattr(self, '_hex_up_tokens', {}).get(crypto)
                is_up_token = (asset_id == up_token_decimal or asset_id == up_token_hex)

                # Get existing price data or create new
                existing = self.prices.get(crypto)

                # IMPORTANT: yes_ask/no_ask = cost to BUY (what we pay)
                # Only use best_ask for ask price - don't fall back to price
                # price is the bid or trade price, not what it costs to buy!
                if is_up_token:
                    # This is UP token - affects yes_price and yes_ask
                    new_yes_ask = float(best_ask) if best_ask else (existing.yes_ask if existing else None)
                    update = PriceUpdate(
                        crypto=crypto,
                        exchange="polymarket",
                        yes_price=float(best_bid) if best_bid else (float(price) if price else (existing.yes_price if existing else None)),
                        no_price=existing.no_price if existing else None,
                        yes_ask=new_yes_ask,
                        no_ask=existing.no_ask if existing else None,
                        token_id=asset_id,
                        strike=self.strikes.get(crypto),
                        timestamp=time.time()
                    )
                else:
                    # DOWN token - affects no_price and no_ask
                    new_no_ask = float(best_ask) if best_ask else (existing.no_ask if existing else None)
                    update = PriceUpdate(
                        crypto=crypto,
                        exchange="polymarket",
                        yes_price=existing.yes_price if existing else None,
                        no_price=float(best_bid) if best_bid else (float(price) if price else (existing.no_price if existing else None)),
                        yes_ask=existing.yes_ask if existing else None,
                        no_ask=new_no_ask,
                        token_id=asset_id,
                        strike=self.strikes.get(crypto),
                        timestamp=time.time()
                    )

                # Log first complete price (when we have both UP and DOWN with asks)
                old_price = self.prices.get(crypto)
                has_complete_asks = update.yes_ask is not None and update.no_ask is not None
                is_first_complete = (
                    (not old_price or old_price.yes_ask is None or old_price.no_ask is None) and
                    has_complete_asks
                )
                if is_first_complete:
                    # Show both bid and ask prices for clarity
                    up_bid = f"{update.yes_price:.2f}" if update.yes_price else "N/A"
                    up_ask = f"{update.yes_ask:.2f}" if update.yes_ask else "N/A"
                    down_bid = f"{update.no_price:.2f}" if update.no_price else "N/A"
                    down_ask = f"{update.no_ask:.2f}" if update.no_ask else "N/A"
                    print(f"[poly-ws] {crypto}: UP(bid={up_bid}, ask={up_ask}), DOWN(bid={down_bid}, ask={down_ask})")

                self.prices[crypto] = update

                # Notify callbacks
                for callback in self._callbacks:
                    try:
                        callback(update)
                    except Exception as e:
                        if DEBUG:
                            print(f"[poly-ws] Callback error: {e}")

    async def _listen(self):
        """Main loop to listen for WebSocket messages."""
        while self._running and self.ws:
            try:
                message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                await self._handle_message(message)
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    ping_msg = {"type": "ping"}
                    await self.ws.send(json.dumps(ping_msg))
                except:
                    break
            except websockets.ConnectionClosed:
                print("[poly-ws] Connection closed, reconnecting...")
                self.connected = False
                if self._running:
                    await self._reconnect()
            except Exception as e:
                if DEBUG:
                    print(f"[poly-ws] Listen error: {e}")
                break

    async def _reconnect(self):
        """Attempt to reconnect to WebSocket."""
        for attempt in range(5):
            await asyncio.sleep(0.5 * (attempt + 1))  # Fast backoff: 0.5s, 1s, 1.5s, 2s, 2.5s

            if await self._connect():
                # Resubscribe to all markets in single batch
                await self._subscribe_to_all_pending()
                return True
        return False

    def _run_async_loop(self):
        """Run the async event loop in a separate thread."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()  # Signal that event loop is ready
        self._loop.run_until_complete(self._async_main())

    async def _async_main(self):
        """Main async function."""
        if await self._connect():
            self._connected_event.set()  # Signal that connection is established
            await self._listen()
        else:
            # Signal anyway so start() doesn't block forever
            self._connected_event.set()

    def start(self, timeout: float = 5.0):
        """Start the WebSocket connection in a background thread.

        Args:
            timeout: Max seconds to wait for connection (default 5.0)
        """
        if self._running:
            return

        self._running = True
        self._loop_ready.clear()
        self._connected_event.clear()
        self._thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self._thread.start()

        # Wait for event loop to be ready
        if not self._loop_ready.wait(timeout=2.0):
            print("[poly-ws] Warning: Event loop not ready after 2s")

        # Wait for connection to be established
        if not self._connected_event.wait(timeout=timeout):
            print("[poly-ws] Warning: Connection not established after timeout")

    def stop(self):
        """Stop the WebSocket connection."""
        self._running = False
        if self.ws and self._loop:
            asyncio.run_coroutine_threadsafe(self.ws.close(), self._loop)
        self.connected = False

    def subscribe(self, asset_id: str, crypto: str, is_up: bool = True, strike: float = None):
        """
        Queue an asset for subscription with optional strike price.

        Note: Subscriptions are QUEUED and sent in a single batch when
        flush_subscriptions() is called. This is required because Polymarket
        WebSocket only supports one subscription message per connection.

        Args:
            asset_id: The token ID to subscribe to
            crypto: Crypto symbol (BTC, ETH, SOL)
            is_up: True if this is the UP token, False for DOWN
            strike: Optional strike price for this crypto
        """
        # Store strike price if provided
        if strike is not None:
            self.strikes[crypto] = strike

        # Ensure we have decimal format for storage (Polymarket WS uses decimal)
        decimal_asset_id = asset_id
        hex_asset_id = None
        try:
            if asset_id.startswith("0x"):
                decimal_asset_id = str(int(asset_id, 16))
                hex_asset_id = asset_id
            else:
                hex_asset_id = hex(int(asset_id))
        except (ValueError, TypeError):
            pass

        # Store token mapping (both decimal and hex for matching)
        self.asset_to_crypto[decimal_asset_id] = crypto
        if hex_asset_id:
            self.asset_to_crypto[hex_asset_id] = crypto

        if is_up:
            self.crypto_to_up_token[crypto] = decimal_asset_id
            if hex_asset_id:
                self._hex_up_tokens = getattr(self, '_hex_up_tokens', {})
                self._hex_up_tokens[crypto] = hex_asset_id
        else:
            self.crypto_to_down_token[crypto] = decimal_asset_id
            if hex_asset_id:
                self._hex_down_tokens = getattr(self, '_hex_down_tokens', {})
                self._hex_down_tokens[crypto] = hex_asset_id

        # Queue the subscription (don't send yet)
        self._queue_subscription(decimal_asset_id, crypto)

    def subscribe_both(self, up_token: str, down_token: str, crypto: str, strike: float = None):
        """Queue both UP and DOWN tokens for subscription."""
        self.subscribe(up_token, crypto, is_up=True, strike=strike)
        self.subscribe(down_token, crypto, is_up=False)

    def flush_subscriptions(self):
        """
        Send all queued subscriptions to the WebSocket in a single message.

        Call this after queueing all assets with subscribe() or subscribe_both().
        """
        # Wait for connection to be established (up to 3s)
        if not self._connected_event.wait(timeout=3.0):
            print("[poly-ws] Warning: Connection not ready after 3s - flush may fail")

        if self._loop and self.connected and self._pending_assets:
            future = asyncio.run_coroutine_threadsafe(
                self._subscribe_to_all_pending(),
                self._loop
            )
            # Wait briefly for subscription to complete
            try:
                future.result(timeout=3.0)
            except Exception as e:
                if DEBUG:
                    print(f"  [DEBUG] Polymarket: Flush failed: {e}")
        elif not self._loop:
            print("[poly-ws] Warning: Event loop not ready for flush")
        elif not self.connected:
            print("[poly-ws] Warning: Not connected, cannot flush subscriptions")
        elif not self._pending_assets:
            print("[poly-ws] No pending assets to flush")

    def set_strike(self, crypto: str, strike: float):
        """Set the strike price for a crypto."""
        self.strikes[crypto] = strike
        # Update existing price update with strike
        if crypto in self.prices:
            self.prices[crypto].strike = strike

    def on_price_update(self, callback: Callable[[PriceUpdate], None]):
        """Register a callback for price updates."""
        self._callbacks.append(callback)

    def get_price(self, crypto: str) -> Optional[PriceUpdate]:
        """Get the latest price for a crypto."""
        return self.prices.get(crypto)

    def place_order(
        self,
        token_id: str,
        side: str,
        quantity: int,
        price: float,
        immediate: bool = True,
        max_retries: int = 2,
    ) -> OrderResult:
        """
        Place an order via CLOB SDK with retry logic for network errors.

        Args:
            token_id: The token ID to trade
            side: "BUY" or "SELL"
            quantity: Number of contracts
            price: Price between 0 and 1
            immediate: If True, use FOK (Fill or Kill)
            max_retries: Number of retry attempts for network errors

        Returns:
            OrderResult with success status and details
        """
        if not self._clob_client:
            return OrderResult(success=False, error="CLOB client not initialized")

        from py_clob_client.clob_types import OrderArgs, OrderType
        import time as time_module

        # Ensure valid price range
        price = max(0.01, min(0.99, price))
        quantity = int(quantity)

        if quantity < 1:
            return OrderResult(success=False, error="Quantity must be >= 1")

        order_type = OrderType.FOK if immediate else OrderType.GTC

        last_error = None
        for attempt in range(max_retries + 1):
            try:
                order_args = OrderArgs(
                    price=price,
                    size=quantity,
                    side=side.upper(),
                    token_id=token_id,
                )

                signed_order = self._clob_client.create_order(order_args)
                response = self._clob_client.post_order(signed_order, order_type)

                if response:
                    success = response.get("success", False)
                    status = response.get("status", "").lower()
                    tx_hashes = response.get("transactionsHashes", [])

                    # Check for successful fill
                    if success or status == "matched" or tx_hashes:
                        return OrderResult(
                            success=True,
                            order_id=response.get("orderID"),
                            filled_qty=quantity if tx_hashes else 0,
                            fill_price=price,
                            tx_hash=tx_hashes[0] if tx_hashes else None,
                            raw_response=response
                        )

                return OrderResult(
                    success=False,
                    error="Order not filled",
                    raw_response=response
                )

            except Exception as e:
                last_error = str(e)

                # FOK orders that can't fill - expected, not an error
                if "couldn't be fully filled" in last_error:
                    return OrderResult(success=False, error="Not enough liquidity")

                # CRITICAL: Do NOT retry on PolyApiException or any other exception!
                # The order may have been submitted even if we got an exception.
                # Retrying could cause duplicate orders.
                # Only safe to retry if we're certain the request never reached the server.

                # Check if this is a pre-request error (safe to retry)
                is_pre_request_error = any(err in last_error.lower() for err in [
                    "connection refused", "dns", "name resolution",
                    "no route to host", "network unreachable"
                ])

                # Only retry on errors that happen BEFORE request is sent
                if is_pre_request_error and attempt < max_retries:
                    time_module.sleep(0.05 * (attempt + 1))  # 50ms, 100ms
                    continue

                # For any other error (including PolyApiException), don't retry
                # The order may have gone through even if we got an exception
                return OrderResult(success=False, error=last_error)

        return OrderResult(success=False, error=last_error or "Unknown error")

    def place_order_for_crypto(
        self,
        crypto: str,
        direction: str,
        quantity: int,
        price: float,
        immediate: bool = True,
    ) -> OrderResult:
        """
        Place an order using crypto symbol (looks up token automatically).

        Args:
            crypto: Crypto symbol (BTC, ETH, SOL)
            direction: "up" or "down"
            quantity: Number of contracts
            price: Price between 0 and 1
            immediate: If True, use FOK

        Returns:
            OrderResult with success status and details
        """
        # Get the appropriate token
        if direction.lower() == "up":
            token_id = self.crypto_to_up_token.get(crypto)
        else:
            token_id = self.crypto_to_down_token.get(crypto)

        if not token_id:
            return OrderResult(success=False, error=f"No {direction} token for {crypto}")

        return self.place_order(token_id, "BUY", quantity, price, immediate)


class ChainlinkPriceFeed:
    """
    WebSocket client for Polymarket's Chainlink price feed.

    Used to get real-time crypto prices from the same source Polymarket uses
    for strike price determination. Much faster than web scraping.

    Subscribe to: crypto_prices_chainlink topic
    Symbols: BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT
    """

    # Map crypto symbols to Polymarket RTDS Chainlink format
    # The RTDS feed may use either "btc/usd" or "BTCUSDT" format
    SYMBOL_MAP = {
        "BTC": "BTCUSDT",
        "ETH": "ETHUSDT",
        "SOL": "SOLUSDT",
        "XRP": "XRPUSDT",
    }
    # Alternative symbol format (btc/usd style)
    SYMBOL_MAP_ALT = {
        "BTC": "btc/usd",
        "ETH": "eth/usd",
        "SOL": "sol/usd",
        "XRP": "xrp/usd",
    }

    def __init__(self):
        # Correct URL for real-time data streams (RTDS) including Chainlink prices
        self.ws_url = "wss://ws-live-data.polymarket.com"
        self.ws = None
        self.connected = False
        self._running = False
        self._loop = None
        self._thread = None

        # Current prices from Chainlink
        self.current_prices: Dict[str, float] = {}  # crypto -> price
        self.price_timestamps: Dict[str, float] = {}  # crypto -> timestamp

        # Strike prices captured at 15-min window open (t+0s, exact match only)
        self.window_strikes: Dict[str, float] = {}  # crypto -> strike at window open
        self._current_window: str = ""  # Track current window
        self._window_start_ts: float = 0  # Unix timestamp when window started
        self._missed_15m_boundary: bool = False  # True if we missed the 15-min boundary
        self._logged_prices: Dict[str, set] = {}  # crypto -> set of logged second marks

        # (Best-candidate removed — only exact boundary matches accepted)

        # Pre-captured strikes: Chainlink may deliver boundary prices BEFORE the
        # system clock crosses the window boundary. These are stored here and
        # moved to window_strikes when the window actually changes.
        self._pending_strikes: Dict[str, float] = {}  # crypto -> price at next boundary
        self._pre_boundary_logged: bool = False  # Track if we logged the health check

        # 5-min market strikes: captured at t+600s (10 min into the 15-min window)
        # The 5-min market opens at fifteen_min_ts + 600
        self.window_strikes_5m: Dict[str, float] = {}  # crypto -> 5-min strike
        self._five_min_window_ts: float = 0  # Unix timestamp for 5-min window start
        self._five_min_captured: bool = False  # Track if we've captured the 5-min strike
        self._missed_5m_boundary: bool = False  # True if program wasn't running at 5-min boundary
        self._logged_prices_5m: Dict[str, set] = {}  # crypto -> set of logged second marks for 5-min boundary

        # 1-hour market strikes: persists across 15-min rotations, only cleared on hour boundary
        self.window_strikes_1h: Dict[str, float] = {}  # crypto -> 1h strike at hour open
        self._current_1h_window: str = ""  # Track current 1-hour window
        self._1h_window_start_ts: float = 0.0  # Unix timestamp when 1h window started
        self._missed_1h_boundary: bool = False  # True if program wasn't running at hour start

        # Pre-captured strikes: Chainlink may deliver boundary prices BEFORE the
        # system clock crosses the window boundary. These are stored here and
        # moved to window_strikes when the window actually changes.
        self._pending_strikes: Dict[str, float] = {}  # crypto -> price at next boundary
        self._pending_strikes_1h: Dict[str, float] = {}  # crypto -> price at next hour boundary
        self._pre_boundary_logged: bool = False  # Track if we logged the health check

    def _get_current_window(self) -> str:
        """Get current 15-minute window identifier."""
        now = datetime.now(timezone.utc)
        minute_bucket = (now.minute // 15) * 15
        return f"{now.strftime('%Y%m%d_%H')}{minute_bucket:02d}"

    def _get_window_start_timestamp(self) -> float:
        """Get Unix timestamp for start of current 15-minute window."""
        now = datetime.now(timezone.utc)
        window_start = now.replace(
            minute=(now.minute // 15) * 15,
            second=0,
            microsecond=0
        )
        return window_start.timestamp()

    async def _connect(self):
        """Establish WebSocket connection."""
        try:
            self.ws = await websockets.connect(
                self.ws_url,
                ping_interval=10,
                ping_timeout=30,
            )
            self.connected = True
            print(f"[chainlink] Connected to Polymarket Chainlink feed")
            return True
        except Exception as e:
            print(f"[chainlink] Connection error: {e}")
            return False

    async def _subscribe(self):
        """Subscribe to Chainlink price feed for all cryptos."""
        if not self.ws or not self.connected:
            return False

        try:
            # Subscribe to all crypto prices from Chainlink
            sub_msg = {
                "action": "subscribe",
                "subscriptions": [
                    {
                        "topic": "crypto_prices_chainlink",
                        "type": "*",
                        "filters": ""  # Get all symbols
                    }
                ]
            }
            await self.ws.send(json.dumps(sub_msg))
            print(f"[chainlink] Subscribed to crypto_prices_chainlink")
            return True
        except Exception as e:
            print(f"[chainlink] Subscribe error: {e}")
            return False

    async def _handle_message(self, msg: str):
        """Handle incoming Chainlink price message."""
        try:
            data = json.loads(msg)

            # Log first message for debugging
            if not hasattr(self, '_first_msg_logged'):
                self._first_msg_logged = True
                preview = msg[:400]
                print(f"[chainlink-dbg] First message: {preview}")

            # Handle multiple message formats:
            # Format 1: {"topic": "crypto_prices_chainlink", "payload": {...}}
            # Format 2: [{"symbol": "btc/usd", "value": 12345.67, "timestamp": ...}, ...]
            # Format 3: {"symbol": "btc/usd", "value": 12345.67, "timestamp": ...}

            items = []
            if data.get("topic") == "crypto_prices_chainlink":
                payload = data.get("payload", {})
                if payload:
                    items = [payload]
            elif isinstance(data, list):
                items = data
            elif isinstance(data, dict) and "symbol" in data:
                items = [data]

            for item in items:
                symbol_raw = item.get("symbol", "")
                symbol_upper = symbol_raw.upper()
                symbol_lower = symbol_raw.lower()
                value = item.get("value") or item.get("price")
                timestamp = item.get("timestamp", 0)

                if not symbol_raw or value is None:
                    continue

                value = float(value)

                # Map symbol back to crypto (try both formats)
                crypto = None
                for c, s in self.SYMBOL_MAP.items():
                    if s == symbol_upper:
                        crypto = c
                        break
                if not crypto:
                    for c, s in self.SYMBOL_MAP_ALT.items():
                        if s == symbol_lower:
                            crypto = c
                            break

                if not crypto:
                    continue

                # Update current price
                self.current_prices[crypto] = value
                self.price_timestamps[crypto] = timestamp

                if DEBUG:
                    print(f"[chainlink] {crypto} price update: ${value:,.2f} (symbol={symbol_raw})")

                # === Log price at t+0s after 15-min boundary ===
                if self._window_start_ts and timestamp:
                    window_start_ms = int(self._window_start_ts * 1000)
                    chainlink_sec = (int(timestamp) - window_start_ms) / 1000.0
                    if 0.0 <= chainlink_sec <= 0.5:
                        if crypto not in self._logged_prices:
                            self._logged_prices[crypto] = set()
                        if 0 not in self._logged_prices[crypto]:
                            self._logged_prices[crypto].add(0)
                            pfmt = f"${value:,.5f}" if value < 10 else f"${value:,.4f}" if value < 100 else f"${value:,.2f}"
                            print(f"[chainlink-t0] {crypto} t+{chainlink_sec:.1f}s: {pfmt}")

                # === Pre-boundary health check ===
                # ~10s before the next 15-min boundary, log that we're receiving prices
                now_utc = datetime.now(timezone.utc)
                secs_into_15m = (now_utc.minute % 15) * 60 + now_utc.second
                secs_until_boundary = 900 - secs_into_15m
                if 5 <= secs_until_boundary <= 12 and not self._pre_boundary_logged:
                    self._pre_boundary_logged = True
                    active_cryptos = list(self.current_prices.keys())
                    prices_str = ", ".join(f"{c}=${self.current_prices[c]:,.2f}" for c in sorted(active_cryptos))
                    print(f"[chainlink] Feed alive {secs_until_boundary}s before boundary: {prices_str}")

                # Check if 1-hour window changed (BEFORE 15-min check)
                # 1h strikes persist across 15-min rotations; only cleared on hour boundary
                current_1h = self._get_current_1h_window()
                if current_1h != self._current_1h_window:
                    self._current_1h_window = current_1h
                    self._1h_window_start_ts = self._get_1h_window_start_timestamp()
                    self.window_strikes_1h.clear()
                    # Check if we missed the hour boundary (program started mid-hour)
                    now_detect = datetime.now(timezone.utc).timestamp()
                    secs_since_hour = now_detect - self._1h_window_start_ts
                    if secs_since_hour > 30:
                        self._missed_1h_boundary = True
                        print(f"[chainlink-1h] Missed hour boundary (detected {secs_since_hour:.0f}s late) — no 1h strike this hour")
                    else:
                        self._missed_1h_boundary = False
                    # Use pre-captured 1h boundary prices as strikes (t+0s is the correct strike)
                    if self._pending_strikes_1h:
                        for c, v in self._pending_strikes_1h.items():
                            self.window_strikes_1h[c] = v
                            pfmt = f"${v:,.5f}" if v < 10 else f"${v:,.4f}" if v < 100 else f"${v:,.2f}"
                            print(f"[chainlink-1h] {c} 1h strike: {pfmt} (pre-captured t+0s)")
                        self._pending_strikes_1h.clear()
                    print(f"[chainlink] 1h window changed to {current_1h}")

                # Check if 15-min window changed - capture new strike
                self.rotate_if_needed()
                current_window = self._current_window

                # === 15-min strike: capture price at t+0s (exact boundary only) ===
                # The "price to beat" is the Chainlink price AT the boundary.
                # Chainlink timestamp must == windowStart * 1000 (exact match).
                # If we miss the exact price, the strike stays unset and the system
                # uses the Kalshi strike for Poly instead.
                window_start_ms = int(self._window_start_ts * 1000) if self._window_start_ts else 0
                target_15m_ms = window_start_ms if window_start_ms > 0 else 0
                if crypto not in self.window_strikes and target_15m_ms > 0 and not self._missed_15m_boundary:
                    if timestamp and int(timestamp) == target_15m_ms:
                        self.window_strikes[crypto] = value
                        chainlink_elapsed = (int(timestamp) - window_start_ms) / 1000.0
                        pfmt = f"${value:,.5f}" if value < 10 else f"${value:,.4f}" if value < 100 else f"${value:,.2f}"
                        print(f"[chainlink] {crypto} strike for window {current_window}: {pfmt} at t+{chainlink_elapsed:.1f}s (exact match)")

                # === Log prices at t+600s, t+601s, t+602s, t+603s (5-min boundary) ===
                if self._five_min_window_ts > 0 and timestamp and not self._missed_5m_boundary:
                    five_min_start_ms_log = int(self._five_min_window_ts * 1000)
                    chainlink_sec_5m = (int(timestamp) - five_min_start_ms_log) / 1000.0
                    if 0.0 <= chainlink_sec_5m <= 3.5:
                        sec_mark_5m = int(chainlink_sec_5m)
                        if crypto not in self._logged_prices_5m:
                            self._logged_prices_5m[crypto] = set()
                        if sec_mark_5m not in self._logged_prices_5m[crypto]:
                            self._logged_prices_5m[crypto].add(sec_mark_5m)
                            elapsed_from_window = (int(timestamp) - int(self._window_start_ts * 1000)) / 1000.0
                            pfmt = f"${value:,.5f}" if value < 10 else f"${value:,.4f}" if value < 100 else f"${value:,.2f}"
                            print(f"[chainlink-5m-t{sec_mark_5m}] {crypto} t+{elapsed_from_window:.0f}s: {pfmt}")

                # === 5-min strike: capture price at exact 5-min boundary (t+600s) ===
                # The 5-min "price to beat" is the Chainlink price AT the 5-min boundary.
                # Chainlink timestamp must == five_min_window * 1000 (exact match only).
                # If we miss it, the 5-min market is skipped this window.
                if self._five_min_window_ts > 0 and crypto not in self.window_strikes_5m and not self._missed_5m_boundary:
                    five_min_start_ms = int(self._five_min_window_ts * 1000)
                    target_5m_ms = five_min_start_ms

                    if timestamp and int(timestamp) == target_5m_ms:
                        self.window_strikes_5m[crypto] = value
                        chainlink_elapsed_5m = (int(timestamp) - int(self._window_start_ts * 1000)) / 1000.0
                        pfmt = f"${value:,.5f}" if value < 10 else f"${value:,.4f}" if value < 100 else f"${value:,.2f}"
                        print(f"[chainlink-5m] {crypto} 5-min strike: {pfmt} at t+{chainlink_elapsed_5m:.1f}s (exact match)")

                # === 1-hour strike: capture price at t+0s (exact hour boundary only) ===
                # The 1h "price to beat" is the Chainlink price AT the hour boundary.
                # Chainlink timestamp must == hourStart * 1000 (exact match only).
                # If we miss the exact price, the 1h strike stays unset.
                window_start_1h_ms = int(self._1h_window_start_ts * 1000) if self._1h_window_start_ts else 0
                target_1h_ms = window_start_1h_ms if window_start_1h_ms > 0 else 0
                if crypto not in self.window_strikes_1h and target_1h_ms > 0 and not self._missed_1h_boundary:
                    if timestamp and int(timestamp) == target_1h_ms:
                        self.window_strikes_1h[crypto] = value
                        now_ts = datetime.now(timezone.utc).timestamp()
                        elapsed = now_ts - self._1h_window_start_ts if self._1h_window_start_ts else 0
                        pfmt = f"${value:,.5f}" if value < 10 else f"${value:,.4f}" if value < 100 else f"${value:,.2f}"
                        print(f"[chainlink-1h] {crypto} 1h strike: {pfmt} at t+{elapsed:.1f}s (exact match)")

                # === Pre-capture: catch boundary prices that arrive BEFORE the clock crosses ===
                # The Chainlink feed may deliver the next window's boundary price slightly early
                # (e.g., timestamp == 19:15:00*1000 arrives at 19:14:59.8). If we don't catch
                # it here, we'll miss it entirely because the window change code only runs
                # AFTER the clock crosses.
                if self._window_start_ts > 0 and timestamp:
                    ts_int = int(timestamp)
                    # Check against NEXT 15-min boundary
                    next_15m_boundary_ms = int((self._window_start_ts + 900) * 1000)
                    if ts_int == next_15m_boundary_ms and crypto not in self._pending_strikes:
                        self._pending_strikes[crypto] = value
                        pfmt = f"${value:,.5f}" if value < 10 else f"${value:,.4f}" if value < 100 else f"${value:,.2f}"
                        print(f"[chainlink] {crypto} pre-captured next boundary strike: {pfmt}")

                    # Check against NEXT 1h boundary
                    if self._1h_window_start_ts > 0:
                        next_1h_boundary_ms = int((self._1h_window_start_ts + 3600) * 1000)
                        if ts_int == next_1h_boundary_ms and crypto not in self._pending_strikes_1h:
                            self._pending_strikes_1h[crypto] = value
                            pfmt = f"${value:,.5f}" if value < 10 else f"${value:,.4f}" if value < 100 else f"${value:,.2f}"
                            print(f"[chainlink-1h] {crypto} pre-captured next 1h boundary strike: {pfmt}")

                # Only log strike captures (no per-second rtds spam)

        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"[chainlink] Message error: {e}")

    async def _listen(self):
        """Listen for incoming messages."""
        while self._running and self.ws:
            try:
                msg = await asyncio.wait_for(self.ws.recv(), timeout=30)
                await self._handle_message(msg)
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    await self.ws.ping()
                except:
                    break
            except websockets.exceptions.ConnectionClosed:
                print("[chainlink] Connection closed")
                self.connected = False
                break
            except Exception as e:
                if DEBUG:
                    print(f"[chainlink] Listen error: {e}")

    async def _run(self):
        """Main async loop."""
        self._running = True

        while self._running:
            if not self.connected:
                if await self._connect():
                    await self._subscribe()
                else:
                    await asyncio.sleep(5)
                    continue

            await self._listen()

            if self._running:
                await asyncio.sleep(1)

    def start(self):
        """Start the Chainlink feed in a background thread."""
        def run_loop():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(self._run())
            finally:
                self._loop.close()

        self._thread = threading.Thread(target=run_loop, daemon=True)
        self._thread.start()
        print("[chainlink] Chainlink price feed starting...")

    def stop(self):
        """Stop the Chainlink feed."""
        self._running = False
        if self.ws:
            asyncio.run_coroutine_threadsafe(self.ws.close(), self._loop)

    def rotate_if_needed(self) -> bool:
        """Check system clock and rotate 15-min window if needed.

        Called from the main thread to ensure the feed's _current_window
        is up-to-date even if no WebSocket messages have arrived yet.
        Returns True if the feed is now on the current window.
        """
        current_window = self._get_current_window()
        if current_window == self._current_window:
            return True  # Already on correct window

        # Perform rotation (same logic as message handler)
        self._current_window = current_window
        self._window_start_ts = self._get_window_start_timestamp()
        self._five_min_window_ts = self._window_start_ts + 600

        # Check if we missed the 15-min boundary
        now_ts = datetime.now(timezone.utc).timestamp()
        secs_since_15m = now_ts - self._window_start_ts
        if secs_since_15m > 10:
            self._missed_15m_boundary = True
            print(f"[chainlink] Missed 15-min boundary (detected {secs_since_15m:.0f}s late) — Chainlink strike not available, using Kalshi")
        else:
            self._missed_15m_boundary = False

        # Check if we missed the 5-min boundary
        secs_since_5m = now_ts - self._five_min_window_ts
        if secs_since_5m > 10:
            self._missed_5m_boundary = True
            print(f"[chainlink-5m] Missed 5-min boundary (detected {secs_since_5m:.0f}s late) — no 5-min strike this window")
        else:
            self._missed_5m_boundary = False

        # Clear old 15-min and 5-min strikes (NOT 1h strikes!)
        self.window_strikes.clear()
        self.window_strikes_5m.clear()
        self._five_min_captured = False
        self._logged_prices.clear()
        self._logged_prices_5m.clear()
        self._pre_boundary_logged = False

        # Use pre-captured boundary prices as strikes
        if self._pending_strikes:
            for c, v in self._pending_strikes.items():
                self.window_strikes[c] = v
                pfmt = f"${v:,.5f}" if v < 10 else f"${v:,.4f}" if v < 100 else f"${v:,.2f}"
                print(f"[chainlink] {c} strike for window {current_window}: {pfmt} (pre-captured t+0s)")
            self._pending_strikes.clear()

        return True

    def get_strike(self, crypto: str) -> Optional[float]:
        """Get the 15-min strike price for a crypto (price at window open, t+0s)."""
        return self.window_strikes.get(crypto)

    def get_strike_5m(self, crypto: str) -> Optional[float]:
        """Get the 5-min strike price for a crypto (price at t+600s into 15-min window)."""
        return self.window_strikes_5m.get(crypto)

    def get_strike_1h(self, crypto: str) -> Optional[float]:
        """Get the 1-hour strike price for a crypto (price at hour boundary, t+0s)."""
        return self.window_strikes_1h.get(crypto)

    def _get_current_1h_window(self) -> str:
        """Get current 1-hour window identifier."""
        now = datetime.now(timezone.utc)
        return f"{now.strftime('%Y%m%d_%H')}00"

    def _get_1h_window_start_timestamp(self) -> float:
        """Get Unix timestamp for start of current 1-hour window."""
        now = datetime.now(timezone.utc)
        window_start = now.replace(minute=0, second=0, microsecond=0)
        return window_start.timestamp()

    def get_current_price(self, crypto: str) -> Optional[float]:
        """Get the current Chainlink price for a crypto."""
        return self.current_prices.get(crypto)


class WebSocketManager:
    """
    Manages both Kalshi and Polymarket WebSocket connections.

    Provides a unified interface for:
    - Real-time price streaming from both exchanges
    - Fast order placement via WebSocket (Kalshi) and SDK (Polymarket)
    """

    def __init__(
        self,
        kalshi_key_id: str = None,
        kalshi_private_key_path: str = None,
        poly_private_key: str = None,
        skip_chainlink: bool = False,
    ):
        self.kalshi_ws = None
        self.poly_ws = None
        self.chainlink_feed = None

        # Initialize Kalshi WebSocket if credentials provided
        if kalshi_key_id and kalshi_private_key_path:
            self.kalshi_ws = KalshiWebSocket(kalshi_key_id, kalshi_private_key_path)

        # Initialize Polymarket WebSocket with optional private key for orders
        self.poly_ws = PolymarketWebSocket(private_key=poly_private_key)

        # Chainlink feed for getting strike prices from Polymarket's RTDS
        # Uses crypto_prices_chainlink topic with BTCUSDT/ETHUSDT/SOLUSDT symbols
        if not skip_chainlink:
            self.chainlink_feed = ChainlinkPriceFeed()

        self._price_callbacks: list = []

        # Track latest prices from both exchanges
        self.kalshi_prices: Dict[str, PriceUpdate] = {}
        self.poly_prices: Dict[str, PriceUpdate] = {}

        # Store Polymarket token IDs for REST API fallback
        self._poly_tokens: Dict[str, tuple] = {}  # crypto -> (up_token, down_token)
        self._poly_strikes: Dict[str, float] = {}  # crypto -> strike price

        # HTTP session for REST API fallback
        import requests
        self._http_session = requests.Session()

    def start(self, timeout: float = 5.0):
        """Start all WebSocket connections.

        Args:
            timeout: Max seconds to wait for each connection (default 5.0)
        """
        if self.kalshi_ws:
            self.kalshi_ws.start(timeout=timeout)
            self.kalshi_ws.on_price_update(self._on_price_update)

        if self.poly_ws:
            self.poly_ws.start(timeout=timeout)
            self.poly_ws.on_price_update(self._on_price_update)

        # Start Chainlink feed for strike prices
        if self.chainlink_feed:
            self.chainlink_feed.start()


    def stop(self):
        """Stop all WebSocket connections."""
        if self.kalshi_ws:
            self.kalshi_ws.stop()
        if self.poly_ws:
            self.poly_ws.stop()
        if self.chainlink_feed:
            self.chainlink_feed.stop()
        print("  [CONN] WebSocket connections closed")

    def get_chainlink_strike(self, crypto: str) -> Optional[float]:
        """Get strike price from Chainlink feed (price at window open)."""
        if self.chainlink_feed:
            return self.chainlink_feed.get_strike(crypto)
        return None

    def get_chainlink_price(self, crypto: str) -> Optional[float]:
        """Get current Chainlink price."""
        if self.chainlink_feed:
            return self.chainlink_feed.get_current_price(crypto)
        return None

    def clear_all_subscriptions(self):
        """Clear all subscriptions and prices from both exchanges (call on window change)."""
        if self.kalshi_ws:
            self.kalshi_ws.clear_subscriptions()
        self.kalshi_prices.clear()
        self.poly_prices.clear()
        self._poly_tokens.clear()
        self._poly_strikes.clear()

    def _on_price_update(self, update: PriceUpdate):
        """Handle price updates from either exchange."""
        # Track prices locally
        if update.exchange == "kalshi":
            self.kalshi_prices[update.crypto] = update
        else:
            self.poly_prices[update.crypto] = update

        for callback in self._price_callbacks:
            try:
                callback(update)
            except Exception as e:
                if DEBUG:
                    print(f"[ws-manager] Callback error: {e}")

    def on_price_update(self, callback: Callable[[PriceUpdate], None]):
        """Register a callback for price updates from any exchange."""
        self._price_callbacks.append(callback)

    def subscribe_kalshi(self, ticker: str, crypto: str, strike: float = None):
        """Subscribe to a Kalshi market with optional strike price and pre-fetch initial prices."""
        if self.kalshi_ws:
            self.kalshi_ws.subscribe(ticker, crypto, strike=strike)
            self._prefetch_kalshi_price(ticker, crypto, strike)

    def _prefetch_kalshi_price(self, ticker: str, crypto: str, strike: float = None):
        """Pre-fetch Kalshi orderbook via REST API to seed initial prices."""
        try:
            url = f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}/orderbook"
            r = self._http_session.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                orderbook = data.get("orderbook", {}) or {}
                yes_bids = orderbook.get("yes", []) or []
                no_bids = orderbook.get("no", []) or []

                yes_bid = max(b[0] for b in yes_bids) if yes_bids else None
                no_bid = max(b[0] for b in no_bids) if no_bids else None

                if yes_bid is not None or no_bid is not None:
                    update = PriceUpdate(
                        crypto=crypto,
                        exchange="kalshi",
                        yes_price=yes_bid / 100.0 if yes_bid else None,
                        no_price=no_bid / 100.0 if no_bid else None,
                        yes_ask=(100 - no_bid) / 100.0 if no_bid else None,
                        no_ask=(100 - yes_bid) / 100.0 if yes_bid else None,
                        ticker=ticker,
                        strike=strike,
                        timestamp=time.time()
                    )
                    self.kalshi_prices[crypto] = update
                    if self.kalshi_ws:
                        self.kalshi_ws.prices[crypto] = update
        except Exception as e:
            if DEBUG:
                print(f"  [DEBUG] {crypto}: Prefetch error: {e}")

    def subscribe_polymarket(self, asset_id: str, crypto: str, is_up: bool = True, strike: float = None):
        """Subscribe to a Polymarket asset."""
        if self.poly_ws:
            self.poly_ws.subscribe(asset_id, crypto, is_up=is_up, strike=strike)

    def subscribe_polymarket_both(self, up_token: str, down_token: str, crypto: str, strike: float = None):
        """Queue both UP and DOWN tokens for subscription on Polymarket and pre-fetch initial prices."""
        # Store tokens for REST API fallback
        self._poly_tokens[crypto] = (up_token, down_token)
        if strike:
            self._poly_strikes[crypto] = strike
        if self.poly_ws:
            self.poly_ws.subscribe_both(up_token, down_token, crypto, strike=strike)
            # Pre-fetch initial price via REST API (don't wait for WebSocket update)
            self._prefetch_poly_prices(up_token, down_token, crypto, strike)

    def _prefetch_poly_prices(self, up_token: str, down_token: str, crypto: str, strike: float = None):
        """Pre-fetch Polymarket orderbooks via REST API to seed initial prices."""
        import concurrent.futures

        up_bid, up_ask, down_bid, down_ask = None, None, None, None

        def fetch_orderbook(token_id):
            try:
                url = f"https://clob.polymarket.com/book?token_id={token_id}"
                r = self._http_session.get(url, timeout=10)
                if r.status_code == 200:
                    book = r.json()
                    bid = None
                    ask = None
                    if book.get("bids"):
                        bids = [float(b["price"]) for b in book["bids"]]
                        bid = max(bids) if bids else None
                    if book.get("asks"):
                        asks = [float(a["price"]) for a in book["asks"]]
                        ask = min(asks) if asks else None
                    return bid, ask
            except Exception as e:
                if DEBUG:
                    print(f"[poly-rest] Orderbook fetch error: {e}")
            return None, None

        # Fetch both orderbooks in parallel
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                up_future = executor.submit(fetch_orderbook, up_token)
                down_future = executor.submit(fetch_orderbook, down_token)
                up_bid, up_ask = up_future.result(timeout=5)
                down_bid, down_ask = down_future.result(timeout=5)
        except Exception as e:
            if DEBUG:
                print(f"[poly-rest] Parallel fetch error: {e}")

        if up_ask is not None or down_ask is not None:
            update = PriceUpdate(
                crypto=crypto,
                exchange="polymarket",
                yes_price=up_bid,
                no_price=down_bid,
                yes_ask=up_ask,
                no_ask=down_ask,
                strike=strike,
                timestamp=time.time()
            )
            self.poly_prices[crypto] = update
            if self.poly_ws:
                self.poly_ws.prices[crypto] = update
            if DEBUG:
                print(f"[poly-rest] {crypto} pre-fetched: UP ask={up_ask}, DOWN ask={down_ask}")

    def prefetch_all_prices(self, cryptos: List[str] = None):
        """
        Pre-fetch prices for all cryptos in parallel via REST API.

        Call this after all subscriptions are set up to ensure we have
        initial prices immediately (don't wait for WebSocket updates).
        """
        import concurrent.futures

        if cryptos is None:
            cryptos = list(self._poly_tokens.keys())

        def fetch_crypto(crypto):
            # Fetch Kalshi
            if self.kalshi_ws and crypto in self.kalshi_ws.crypto_to_ticker:
                ticker = self.kalshi_ws.crypto_to_ticker[crypto]
                strike = self.kalshi_ws.strikes.get(crypto)
                self._prefetch_kalshi_price(ticker, crypto, strike)

            # Fetch Polymarket
            if crypto in self._poly_tokens:
                up_token, down_token = self._poly_tokens[crypto]
                strike = self._poly_strikes.get(crypto)
                self._prefetch_poly_prices(up_token, down_token, crypto, strike)

        # Fetch all cryptos in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(cryptos)) as executor:
            futures = [executor.submit(fetch_crypto, c) for c in cryptos]
            concurrent.futures.wait(futures, timeout=10)

        # Log what we got
        print(f"  [PREFETCH] Initial prices fetched for {len(cryptos)} cryptos")

    def flush_polymarket_subscriptions(self):
        """
        Send all queued Polymarket subscriptions in a single message.

        Must be called after queueing all subscriptions with subscribe_polymarket*()
        methods. This is required because Polymarket WebSocket only supports one
        subscription message per connection.
        """
        if self.poly_ws:
            self.poly_ws.flush_subscriptions()

    def enable_price_logs(self):
        """Enable price logging (call after all subscriptions are ready)."""
        if self.kalshi_ws:
            self.kalshi_ws.enable_price_logs()

    def set_strike(self, crypto: str, kalshi_strike: float = None, poly_strike: float = None):
        """Set strike prices for a crypto on both exchanges."""
        if kalshi_strike is not None and self.kalshi_ws:
            self.kalshi_ws.set_strike(crypto, kalshi_strike)
        if poly_strike is not None and self.poly_ws:
            self.poly_ws.set_strike(crypto, poly_strike)

    def get_kalshi_price(self, crypto: str) -> Optional[PriceUpdate]:
        """Get latest Kalshi price for a crypto."""
        if self.kalshi_ws:
            return self.kalshi_ws.get_price(crypto)
        return None

    def get_polymarket_price(self, crypto: str) -> Optional[PriceUpdate]:
        """Get latest Polymarket price for a crypto. Uses REST API as fallback."""
        # Try WebSocket first
        if self.poly_ws:
            ws_price = self.poly_ws.get_price(crypto)
            if ws_price and (ws_price.yes_price is not None or ws_price.no_price is not None):
                return ws_price

        # Fallback to REST API
        return self._fetch_poly_price_rest(crypto)

    def _fetch_poly_price_rest(self, crypto: str) -> Optional[PriceUpdate]:
        """Fetch Polymarket price via REST API fallback."""
        if crypto not in self._poly_tokens:
            return None

        up_token, down_token = self._poly_tokens[crypto]

        try:
            # Polymarket CLOB API for orderbook
            base_url = "https://clob.polymarket.com"

            # Fetch UP token orderbook
            up_resp = self._http_session.get(f"{base_url}/book?token_id={up_token}", timeout=3)
            # Fetch DOWN token orderbook
            down_resp = self._http_session.get(f"{base_url}/book?token_id={down_token}", timeout=3)

            up_bid, up_ask = None, None
            down_bid, down_ask = None, None

            if up_resp.status_code == 200:
                up_book = up_resp.json()
                # Best bid = highest price someone will pay (max of bids)
                # Best ask = lowest price someone will sell at (min of asks)
                if up_book.get("bids"):
                    bids = [float(b["price"]) for b in up_book["bids"]]
                    up_bid = max(bids) if bids else None
                if up_book.get("asks"):
                    asks = [float(a["price"]) for a in up_book["asks"]]
                    up_ask = min(asks) if asks else None

            if down_resp.status_code == 200:
                down_book = down_resp.json()
                if down_book.get("bids"):
                    bids = [float(b["price"]) for b in down_book["bids"]]
                    down_bid = max(bids) if bids else None
                if down_book.get("asks"):
                    asks = [float(a["price"]) for a in down_book["asks"]]
                    down_ask = min(asks) if asks else None

            # Create price update
            strike = self._poly_strikes.get(crypto)
            return PriceUpdate(
                exchange="polymarket",
                crypto=crypto,
                ticker=f"{crypto.lower()}-updown",
                yes_price=up_bid,
                no_price=down_bid,
                yes_ask=up_ask,
                no_ask=down_ask,
                strike=strike,
                timestamp=time.time(),
            )

        except Exception as e:
            if DEBUG:
                print(f"[poly-rest] Error fetching {crypto}: {e}")
            return None

    def is_connected(self) -> Dict[str, bool]:
        """Check connection status of both WebSockets."""
        return {
            "kalshi": self.kalshi_ws.connected if self.kalshi_ws else False,
            "polymarket": self.poly_ws.connected if self.poly_ws else False,
        }

    # ========== ORDER PLACEMENT METHODS ==========

    def place_kalshi_order(
        self,
        crypto: str,
        side: str,
        quantity: int,
        limit_price_cents: int,
    ) -> OrderResult:
        """
        Place an order on Kalshi via WebSocket.

        Args:
            crypto: Crypto symbol (BTC, ETH, SOL)
            side: "yes" or "no"
            quantity: Number of contracts
            limit_price_cents: Price in cents

        Returns:
            OrderResult with success status
        """
        if not self.kalshi_ws:
            return OrderResult(success=False, error="Kalshi WebSocket not initialized")
        if not self.kalshi_ws.connected:
            return OrderResult(success=False, error="Kalshi WebSocket not connected")

        return self.kalshi_ws.place_order_for_crypto(crypto, side, quantity, limit_price_cents)

    def place_poly_order(
        self,
        crypto: str,
        direction: str,
        quantity: int,
        price: float,
    ) -> OrderResult:
        """
        Place an order on Polymarket.

        Args:
            crypto: Crypto symbol (BTC, ETH, SOL)
            direction: "up" or "down"
            quantity: Number of contracts
            price: Price between 0 and 1

        Returns:
            OrderResult with success status
        """
        if not self.poly_ws:
            return OrderResult(success=False, error="Polymarket WebSocket not initialized")

        return self.poly_ws.place_order_for_crypto(crypto, direction, quantity, price)

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

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            kalshi_future = executor.submit(
                self.place_kalshi_order,
                crypto, kalshi_side, quantity, kalshi_price_cents
            )
            poly_future = executor.submit(
                self.place_poly_order,
                crypto, poly_direction, quantity, poly_price
            )

            kalshi_result = kalshi_future.result()
            poly_result = poly_future.result()

        return kalshi_result, poly_result


# Example usage
if __name__ == "__main__":
    import os

    # Test with environment variables
    kalshi_key = os.environ.get("KALSHI_API_KEY")
    kalshi_key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")

    def on_update(update: PriceUpdate):
        print(f"[{update.exchange}] {update.crypto}: yes={update.yes_price}, no={update.no_price}")

    manager = WebSocketManager(kalshi_key, kalshi_key_path)
    manager.on_price_update(on_update)
    manager.start()

    # Keep running
    try:
        while True:
            time.sleep(1)
            status = manager.is_connected()
            print(f"Status: Kalshi={status['kalshi']}, Poly={status['polymarket']}")
    except KeyboardInterrupt:
        manager.stop()
