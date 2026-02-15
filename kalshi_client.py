"""
Kalshi API client for the arbitrage bot.
"""

import base64
import random
import time
from typing import Dict, List, Optional

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from config import DEBUG, KEYS_DIR
from utils import safe_float


class KalshiClient:
    """Client for interacting with the Kalshi API."""

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
        self.base_url = "https://api.elections.kalshi.com"

    def _make_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
    ):
        """Make an authenticated request to the Kalshi API."""
        ts = str(int(time.time() * 1000))
        sign_path = path.split("?")[0]
        msg = f"{ts}{method}{sign_path}"

        sig_bytes = self.private_key.sign(
            msg.encode("utf-8"),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256(),
        )
        sig_b64 = base64.b64encode(sig_bytes).decode("utf-8")

        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": sig_b64,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }

        url = f"{self.base_url}{path}"
        try:
            return requests.request(method, url, headers=headers, params=params, json=json_data, timeout=10)
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            # Suppress timeout logs for VPS - fast failure is expected
            pass
            return None

    def get_markets(self, series_ticker: str, limit: int = 20) -> List[Dict]:
        """Get open markets for a series."""
        r = self._make_request(
            "GET",
            "/trade-api/v2/markets",
            {"series_ticker": series_ticker, "status": "open", "limit": limit},
        )
        if r and r.status_code == 200:
            return r.json().get("markets", [])
        if DEBUG and r:
            print(f"[kalshi] markets error {r.status_code}: {r.text[:200]}")
        return []

    def get_orderbook(self, ticker: str) -> Optional[Dict]:
        """Get the orderbook for a specific market."""
        r = self._make_request("GET", f"/trade-api/v2/markets/{ticker}/orderbook")
        if r and r.status_code == 200:
            return r.json()
        if DEBUG and r:
            print(f"[kalshi] orderbook error {r.status_code}: {r.text[:200]}")
        return None

    def get_depth_at_price(self, ticker: str, side: str, price_cents: int) -> int:
        """
        Check orderbook depth available at or better than a given price.

        For buying YES: checks asks (sellers) at price_cents or lower
        For buying NO: checks asks (sellers) at price_cents or lower

        Returns the total quantity available at that price or better.
        """
        ob = self.get_orderbook(ticker)
        if not ob:
            return 0

        orderbook = ob.get("orderbook", {}) or {}

        # Get the relevant side's orders
        # When buying YES, we need YES asks (people selling YES)
        # When buying NO, we need NO asks (people selling NO)
        # Kalshi orderbook shows bids - asks are implied at 100-bid

        total_depth = 0

        if side.lower() == "yes":
            # YES bids are people wanting to buy YES
            # We want to buy YES, so we need sellers (asks)
            # YES ask = 100 - NO bid
            no_bids = orderbook.get("no", []) or []
            for bid_price, qty in no_bids:
                # YES ask price = 100 - NO bid price
                yes_ask = 100 - bid_price
                if yes_ask <= price_cents:
                    total_depth += qty
        else:
            # NO bids are people wanting to buy NO
            # We want to buy NO, so we need sellers (asks)
            # NO ask = 100 - YES bid
            yes_bids = orderbook.get("yes", []) or []
            for bid_price, qty in yes_bids:
                # NO ask price = 100 - YES bid price
                no_ask = 100 - bid_price
                if no_ask <= price_cents:
                    total_depth += qty

        return total_depth

    def get_balance(self) -> Optional[float]:
        """Get account balance in dollars."""
        r = self._make_request("GET", "/trade-api/v2/portfolio/balance")
        if r and r.status_code == 200:
            data = r.json()
            return safe_float(data.get("balance", 0)) / 100.0  # Kalshi returns cents
        return None

    def place_order(
        self,
        ticker: str,
        side: str,
        quantity: int,
        limit_price_cents: int,
    ) -> Optional[Dict]:
        """
        Place a Fill-or-Kill order on Kalshi.
        FOK orders either fill completely at the limit price or better, or don't fill at all.

        Args:
            ticker: Market ticker
            side: "yes" or "no"
            quantity: Number of contracts
            limit_price_cents: Price in cents (e.g., 40 for $0.40)

        Returns:
            Order response dict or None on failure
        """
        order_data = {
            "ticker": ticker,
            "client_order_id": f"arb_{int(time.time()*1000)}_{random.randint(1000,9999)}",
            "side": side.lower(),
            "action": "buy",
            "count": quantity,
            "type": "limit",
            "yes_price": limit_price_cents if side.lower() == "yes" else None,
            "no_price": limit_price_cents if side.lower() == "no" else None,
        }

        order_data = {k: v for k, v in order_data.items() if v is not None}

        r = self._make_request("POST", "/trade-api/v2/portfolio/orders", json_data=order_data)
        if r and r.status_code in [200, 201]:
            return r.json()
        # Always print order errors for debugging
        if r:
            print(f"[kalshi] order error {r.status_code}: {r.text[:300]}")
        return None

    def get_order_status(self, order_id: str) -> Optional[Dict]:
        """Check order status by ID."""
        r = self._make_request("GET", f"/trade-api/v2/portfolio/orders/{order_id}")
        if r and r.status_code == 200:
            return r.json()
        return None

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order by ID."""
        r = self._make_request("DELETE", f"/trade-api/v2/portfolio/orders/{order_id}")
        return r and r.status_code in [200, 204]

    def sell_position(
        self,
        ticker: str,
        side: str,
        quantity: int,
        limit_price_cents: int,
        gtc: bool = True,
    ) -> Optional[Dict]:
        """
        Sell a position on Kalshi.

        Args:
            ticker: The market ticker
            side: "yes" or "no"
            quantity: Number of contracts to sell
            limit_price_cents: Minimum price in cents to accept
            gtc: If True, order is Good Till Cancelled (stays on book)

        Returns:
            Order response dict or None on failure
        """
        order_data = {
            "ticker": ticker,
            "client_order_id": f"arb_sell_{int(time.time()*1000)}_{random.randint(1000,9999)}",
            "side": side.lower(),
            "action": "sell",  # SELL instead of buy
            "count": quantity,
            "type": "limit",
            "yes_price": limit_price_cents if side.lower() == "yes" else None,
            "no_price": limit_price_cents if side.lower() == "no" else None,
        }

        order_data = {k: v for k, v in order_data.items() if v is not None}

        r = self._make_request("POST", "/trade-api/v2/portfolio/orders", json_data=order_data)
        if r and r.status_code in [200, 201]:
            return r.json()
        return None

    def get_positions(self) -> List[Dict]:
        """Get all current positions."""
        r = self._make_request("GET", "/trade-api/v2/portfolio/positions")
        if r and r.status_code == 200:
            data = r.json()
            return data.get("market_positions", [])
        return []

    def get_position_for_ticker(self, ticker: str) -> Optional[Dict]:
        """
        Get position for a specific ticker.
        Used to verify if we actually have a position (handles false-negatives).

        Returns:
            Position dict with 'yes_position' and 'no_position' counts, or None
        """
        positions = self.get_positions()
        for pos in positions:
            if pos.get("ticker") == ticker:
                return pos
        return None

    def verify_and_close_position(self, ticker: str, side: str, expected_qty: int) -> bool:
        """
        Verify if we have a position and close it if found.
        Handles false-negative scenarios where order appears failed but position exists.

        Args:
            ticker: Market ticker
            side: "yes" or "no"
            expected_qty: Expected quantity we might have

        Returns:
            True if position was found and closed, False if no position
        """
        pos = self.get_position_for_ticker(ticker)
        if not pos:
            return False

        # Check if we have the position on this side
        if side.lower() == "yes":
            actual_qty = pos.get("position", 0)
        else:
            actual_qty = pos.get("position", 0)
            # For NO positions, the value might be negative
            if actual_qty < 0:
                actual_qty = abs(actual_qty)

        if actual_qty > 0:
            print(f"[kalshi] ⚠️ Found unexpected position: {actual_qty} {side.upper()} on {ticker}")
            print(f"[kalshi] 🔄 Closing position...")
            # Sell at 1 cent to ensure fill
            result = self.sell_position(ticker, side, actual_qty, 1)
            return result is not None

        return False

