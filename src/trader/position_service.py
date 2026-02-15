"""
Position service for Polymarket trading.
Handles: fetching positions, closing positions, redeeming settled markets.
"""

import logging
from typing import Dict, List, Optional, Tuple

from src.clients.polymarket_client import PolymarketClient

logger = logging.getLogger("polymarket_trader")


class PositionService:
    """High-level position management on top of PolymarketClient."""

    def __init__(self, client: PolymarketClient):
        self.client = client

    # ------------------------------------------------------------------ #
    # Query
    # ------------------------------------------------------------------ #

    def get_balance(self) -> Optional[float]:
        """Get USDC.e balance (EOA + proxy wallet)."""
        bal = self.client.get_balance()
        if bal is not None:
            logger.info(f"USDC.e balance: ${bal:.2f}")
        return bal

    def get_positions(self, min_value: float = 0.0) -> List[Dict]:
        """Get all open positions."""
        positions = self.client.get_positions(min_value=min_value)
        logger.info(f"Open positions: {len(positions)}")
        for p in positions:
            title = p.get('title', '?')[:50]
            value = float(p.get('currentValue', 0))
            size = p.get('size', '?')
            logger.info(f"  {title} | size={size} | value=${value:.2f}")
        return positions

    def get_token_balance(self, token_id: str) -> float:
        """Get on-chain balance for a specific conditional token."""
        return self.client.get_conditional_token_balance(token_id)

    # ------------------------------------------------------------------ #
    # Close
    # ------------------------------------------------------------------ #

    def close_position(
        self,
        token_id: str,
        quantity: float,
        buy_price: float = None,
        buy_tx_hash: str = None,
    ) -> Optional[Dict]:
        """
        Close a position by selling on the CLOB.

        Args:
            token_id: The conditional token ID to sell
            quantity: Number of contracts to sell
            buy_price: Original buy price (for P&L logging)
            buy_tx_hash: Original buy tx hash (to wait for settlement)
        """
        logger.info(f"Closing position: token=...{token_id[-12:]}, qty={quantity}")
        return self.client.sell_position(
            token_id=token_id,
            quantity=quantity,
            buy_price=buy_price,
            buy_tx_hash=buy_tx_hash,
        )

    # ------------------------------------------------------------------ #
    # Redemption (settled markets)
    # ------------------------------------------------------------------ #

    def get_redeemable(self) -> List[Dict]:
        """Get positions that can be redeemed from settled markets."""
        return self.client.get_redeemable_positions()

    def redeem_all(self) -> Tuple[int, float]:
        """
        Redeem all settled positions.
        Returns (count_redeemed, total_gas_matic).
        """
        logger.info("Starting redemption of settled positions...")
        count, gas = self.client.redeem_all_positions()
        logger.info(f"Redeemed {count} positions, gas: {gas:.6f} MATIC")
        return count, gas

    def redeem_one(self, condition_id: str) -> Tuple[Optional[str], float]:
        """Redeem a single settled position by condition ID."""
        return self.client.redeem_positions(condition_id)
