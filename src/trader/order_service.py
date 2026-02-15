"""
Order service for Polymarket trading.
Wraps the PolymarketClient with higher-level order operations:
  - place_buy / place_sell
  - cancel_order / cancel_all
  - check order status and fills
"""

import logging
from typing import Dict, Optional

from src.clients.polymarket_client import PolymarketClient
from src.utils.logging import log_trade

logger = logging.getLogger("polymarket_trader")


class OrderService:
    """High-level order management on top of PolymarketClient."""

    def __init__(self, client: PolymarketClient):
        self.client = client

    # ------------------------------------------------------------------ #
    # Buy
    # ------------------------------------------------------------------ #

    def place_buy(
        self,
        token_id: str,
        quantity: int,
        price: float,
        order_type: str = "FOK",
    ) -> Optional[Dict]:
        """
        Place a BUY order.

        Args:
            token_id: CLOB token ID
            quantity: Number of contracts
            price: Limit price (0.01 - 0.99)
            order_type: "FOK" (fill-or-kill), "GTC" (good-till-cancel), "FAK"

        Returns:
            Order response dict with 'success', 'size_matched', etc.
        """
        logger.info(f"BUY {quantity}x @ ${price:.4f} ({order_type}) token=...{token_id[-8:]}")
        resp = self.client.place_order(
            token_id=token_id,
            side="BUY",
            quantity=quantity,
            price=price,
            immediate=(order_type == "FOK"),
            quiet=False,
            order_type_str=order_type,
        )

        status = "FILLED" if (resp and resp.get("success")) else "FAILED"
        filled = resp.get("size_matched", 0) if resp else 0
        order_id = resp.get("orderID", "") if resp else ""
        tx = ""
        if resp:
            hashes = resp.get("transactionsHashes", []) or resp.get("transactionHashes", [])
            tx = hashes[0] if hashes else ""

        log_trade(action="BUY", token_id=token_id, side="BUY", quantity=filled,
                  price=price, order_type=order_type, status=status,
                  order_id=order_id, tx_hash=tx)

        if resp and resp.get("success"):
            logger.info(f"  -> Filled {filled} contracts")
        else:
            err = resp.get("errorMsg", "unknown") if resp else "no response"
            logger.warning(f"  -> Buy failed: {err}")

        return resp

    # ------------------------------------------------------------------ #
    # Sell
    # ------------------------------------------------------------------ #

    def place_sell(
        self,
        token_id: str,
        quantity: int,
        price: float,
        order_type: str = "GTC",
    ) -> Optional[Dict]:
        """
        Place a SELL order.

        Args:
            token_id: CLOB token ID
            quantity: Number of contracts
            price: Limit price (0.01 - 0.99)
            order_type: "GTC" recommended for sells, "FOK" for immediate
        """
        logger.info(f"SELL {quantity}x @ ${price:.4f} ({order_type}) token=...{token_id[-8:]}")
        resp = self.client.place_order(
            token_id=token_id,
            side="SELL",
            quantity=quantity,
            price=price,
            immediate=(order_type == "FOK"),
            quiet=False,
            order_type_str=order_type,
        )

        status = "FILLED" if (resp and resp.get("success")) else "FAILED"
        filled = resp.get("size_matched", 0) if resp else 0
        order_id = resp.get("orderID", "") if resp else ""

        log_trade(action="SELL", token_id=token_id, side="SELL", quantity=filled,
                  price=price, order_type=order_type, status=status, order_id=order_id)

        return resp

    def sell_position(
        self,
        token_id: str,
        quantity: float,
        buy_price: float = None,
        buy_tx_hash: str = None,
    ) -> Optional[Dict]:
        """
        Close/sell an existing position using the client's smart sell logic.
        Waits for on-chain settlement, then tries descending GTC prices.
        """
        logger.info(f"Closing position: {quantity}x token=...{token_id[-8:]}")
        resp = self.client.sell_position(
            token_id=token_id,
            quantity=quantity,
            buy_price=buy_price,
            buy_tx_hash=buy_tx_hash,
        )

        status = "FILLED" if (resp and resp.get("success")) else "FAILED"
        log_trade(action="CLOSE", token_id=token_id, side="SELL",
                  quantity=quantity, status=status,
                  notes=f"buy_price={buy_price}" if buy_price else "")

        return resp

    # ------------------------------------------------------------------ #
    # Cancel
    # ------------------------------------------------------------------ #

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a specific order by ID."""
        logger.info(f"Cancelling order: {order_id[:16]}...")
        ok = self.client.cancel_order(order_id)
        log_trade(action="CANCEL", order_id=order_id,
                  status="CANCELLED" if ok else "FAILED")
        return ok

    def cancel_all(self) -> bool:
        """Cancel all open orders."""
        logger.info("Cancelling all open orders")
        ok = self.client.cancel_all_orders()
        log_trade(action="CANCEL_ALL", status="OK" if ok else "FAILED")
        return ok

    # ------------------------------------------------------------------ #
    # Status
    # ------------------------------------------------------------------ #

    def get_order_status(self, order_id: str) -> Optional[Dict]:
        """Check order status by ID."""
        return self.client.get_order_status(order_id)

    def get_orderbook(self, token_id: str) -> Optional[Dict]:
        """Get current orderbook for a token."""
        return self.client.get_orderbook(token_id)
