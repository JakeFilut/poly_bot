"""
Binance price feed placeholder (Pass 1).

The Binance spot + hour-open fetch logic currently lives inside
PolymarketClient.get_binance_spot_and_hour_open() in
src/feeds/polymarket.py.

In a future pass this may be extracted into its own BinanceFeed class
so that price fetching is decoupled from the Polymarket CLOB adapter.
"""


class BinanceFeed:
    """Placeholder — Binance fetch is currently inside PolymarketClient."""

    def __init__(self):
        raise NotImplementedError(
            "BinanceFeed is a placeholder. "
            "Use PolymarketClient.get_binance_spot_and_hour_open() instead."
        )
