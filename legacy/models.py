"""
Data models for the arbitrage bot.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Snapshot:
    """Market snapshot containing prices from both exchanges."""
    crypto: str
    kalshi_ticker: str
    kalshi_close_time: str
    poly_slug: str
    poly_question: str
    k_up: float
    k_down: float
    p_up: float
    p_down: float
    poly_up_token: Optional[str] = None
    poly_down_token: Optional[str] = None
    kalshi_strike: Optional[float] = None  # Strike price from Kalshi
    poly_strike: Optional[float] = None    # Strike price from Polymarket
    # Cached orderbook data (avoid re-fetching during trade)
    poly_up_asks: Optional[list] = None    # List of (price, size) tuples
    poly_down_asks: Optional[list] = None  # List of (price, size) tuples

    def strikes_match(self, max_diff_pct: float = 3.0) -> bool:
        """Check if strike prices are within acceptable tolerance (3% default).

        Note: Polymarket API doesn't expose strike price. Both platforms use
        similar price sources (Chainlink vs CF Benchmarks), typically within ~0.1%.
        If Poly strike unavailable, we assume it's close enough.
        """
        # If Poly strike not available, assume it's close (API limitation)
        # Both use similar price sources, typically within 0.1%
        if self.poly_strike is None:
            return self.kalshi_strike is not None  # OK if we have Kalshi strike
        if self.kalshi_strike is None:
            return False  # Need at least Kalshi strike
        if self.kalshi_strike == 0 or self.poly_strike == 0:
            return False  # Invalid strikes
        diff_pct = abs(self.kalshi_strike - self.poly_strike) / self.kalshi_strike * 100
        return diff_pct <= max_diff_pct

    def allowed_direction(self) -> str:
        """
        Determine which direction is allowed based on strike price comparison.

        Rule: ALWAYS trade UP on the lower strike, DOWN on the higher strike.
        - Lower strike exchange bets UP (wins if price > lower strike)
        - Higher strike exchange bets DOWN (wins if price < higher strike)

        This ensures the gap between strikes works FOR us:
        - If price lands between strikes, BOTH bets win
        - If price is above both strikes, the UP bet on lower strike wins (win half)
        - If price is below both strikes, the DOWN bet on higher strike wins (win half)
        - Only lose both if price feeds diverge to opposite losing sides

        If Poly strike is unavailable (API limitation), allow any direction
        since both platforms use similar price sources (~0.1% difference).

        Returns:
            "K_UP+P_DOWN" - if Kalshi strike <= Poly strike (Kalshi UP, Poly DOWN)
            "K_DOWN+P_UP" - if Kalshi strike > Poly strike (Kalshi DOWN, Poly UP)
            "ANY" - if Poly strike unavailable (allow any direction)
            "NONE" - if Kalshi strike unavailable
        """
        if self.kalshi_strike is None or self.kalshi_strike == 0:
            return "NONE"

        # If Poly strike not available, allow any direction
        # Both platforms use similar price sources, typically within 0.1%
        if self.poly_strike is None or self.poly_strike == 0:
            return "ANY"

        # Lower strike exchange bets UP, higher strike exchange bets DOWN
        if self.kalshi_strike <= self.poly_strike:
            # Kalshi has lower/equal strike → Kalshi UP, Poly DOWN
            return "K_UP+P_DOWN"
        else:
            # Kalshi has higher strike → Kalshi DOWN, Poly UP
            return "K_DOWN+P_UP"


@dataclass
class Stats:
    """Trading statistics tracker."""
    scans: int = 0
    trade_attempts: int = 0
    executed: int = 0
    wins: int = 0
    losses: int = 0
    pnl: float = 0.0
