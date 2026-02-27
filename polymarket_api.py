"""
polymarket_api.py – Polymarket CLOB interface with retries and idempotency.

All prices are in the range 0.00–1.00 (i.e. cents expressed as a fraction).
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

from config import Config
from logger import Logger


@dataclass
class BookLevel:
    price: float
    size: float


@dataclass
class BookSnapshot:
    bids: List[BookLevel]
    asks: List[BookLevel]
    best_bid: float
    best_ask: float
    bid_size: float
    ask_size: float
    mid: float
    spread: float
    ts: float  # epoch


# ---------------------------------------------------------------------------
# Retry decorator
# ---------------------------------------------------------------------------
def _retry(max_retries: int, backoff_base: float, logger: Logger):
    """Decorator factory for API calls with exponential backoff."""
    def decorator(fn):
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if attempt < max_retries:
                        wait = backoff_base * (2 ** attempt)
                        logger.api_error(
                            fn=fn.__name__, attempt=attempt + 1,
                            error=str(e), retry_in_sec=wait,
                        )
                        time.sleep(wait)
                    else:
                        logger.api_error(
                            fn=fn.__name__, attempt=attempt + 1,
                            error=str(e), exhausted=True,
                        )
            raise last_exc
        wrapper.__name__ = fn.__name__
        return wrapper
    return decorator


class PolymarketAPI:
    """Thin wrapper around py-clob-client with retries and structured logging."""

    def __init__(self, cfg: Config, logger: Logger):
        self.cfg = cfg
        self.log = logger
        self._clob: Optional[ClobClient] = None

        if cfg.MODE == "LIVE":
            self._init_clob_client()

    def _init_clob_client(self) -> None:
        """Initialize the CLOB client for live trading."""
        self._clob = ClobClient(
            self.cfg.CLOB_API_URL,
            key=self.cfg.POLYMARKET_PRIVATE_KEY,
            chain_id=137,  # Polygon mainnet
            funder=self.cfg.POLYMARKET_FUNDER or None,
            creds={
                "apiKey": self.cfg.POLYMARKET_API_KEY,
                "secret": self.cfg.POLYMARKET_API_SECRET,
                "passphrase": self.cfg.POLYMARKET_API_PASSPHRASE,
            },
        )
        self.log.info("CLOB client initialized", mode="LIVE")

    # ------------------------------------------------------------------
    # Market data (works in both modes)
    # ------------------------------------------------------------------
    def get_markets(self, tag_id: int = 21, active_only: bool = True) -> List[dict]:
        """Fetch markets from Gamma API by tag_id (default 21 = Crypto)."""
        retry = _retry(self.cfg.RETRY_MAX, self.cfg.RETRY_BACKOFF_BASE, self.log)

        @retry
        def _fetch():
            params = {
                "tag_id": tag_id,
                "closed": "false",
                "limit": 200,
                "order": "volume24hr",
                "ascending": "false",
            }
            if active_only:
                params["active"] = "true"
            resp = requests.get(
                f"{self.cfg.GAMMA_API_URL}/markets",
                params=params,
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()

        return _fetch()

    def get_orderbook(self, token_id: str) -> BookSnapshot | None:
        """Fetch orderbook for a token."""
        retry = _retry(self.cfg.RETRY_MAX, self.cfg.RETRY_BACKOFF_BASE, self.log)

        @retry
        def _fetch():
            resp = requests.get(
                f"{self.cfg.CLOB_API_URL}/book",
                params={"token_id": token_id},
                timeout=5,
            )
            resp.raise_for_status()
            return resp.json()

        try:
            data = _fetch()
        except Exception:
            return None

        bids = [BookLevel(float(b["price"]), float(b["size"]))
                for b in data.get("bids", [])]
        asks = [BookLevel(float(a["price"]), float(a["size"]))
                for a in data.get("asks", [])]
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)

        best_bid = bids[0].price if bids else 0.0
        best_ask = asks[0].price if asks else 1.0
        bid_size = bids[0].size if bids else 0.0
        ask_size = asks[0].size if asks else 0.0
        mid = (best_bid + best_ask) / 2 if bids and asks else 0.5
        spread = best_ask - best_bid if bids and asks else 1.0

        return BookSnapshot(
            bids=bids, asks=asks,
            best_bid=best_bid, best_ask=best_ask,
            bid_size=bid_size, ask_size=ask_size,
            mid=mid, spread=spread,
            ts=time.time(),
        )

    def get_midpoint(self, token_id: str) -> Optional[float]:
        """Quick midpoint fetch."""
        try:
            resp = requests.get(
                f"{self.cfg.CLOB_API_URL}/midpoint",
                params={"token_id": token_id},
                timeout=5,
            )
            resp.raise_for_status()
            data = resp.json()
            return float(data.get("mid", 0))
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Order placement (LIVE mode only; DRY_RUN returns mock)
    # ------------------------------------------------------------------
    def new_client_order_id(self) -> str:
        return str(uuid.uuid4())

    def place_limit_buy(self, token_id: str, price: float, size_shares: float,
                        client_order_id: str | None = None) -> dict:
        """Place a limit BUY order. Returns {order_id, client_order_id, ...}."""
        cid = client_order_id or self.new_client_order_id()

        if self.cfg.MODE == "DRY_RUN":
            mock_id = f"dry_{cid[:8]}"
            self.log.dry_run_order(
                action="BUY", token_id=token_id, price=price,
                size=size_shares, client_order_id=cid,
            )
            return {"order_id": mock_id, "client_order_id": cid, "status": "DRY_RUN"}

        return self._place_order(token_id, "BUY", price, size_shares, cid)

    def place_limit_sell(self, token_id: str, price: float, size_shares: float,
                         client_order_id: str | None = None) -> dict:
        """Place a limit SELL order."""
        cid = client_order_id or self.new_client_order_id()

        if self.cfg.MODE == "DRY_RUN":
            mock_id = f"dry_{cid[:8]}"
            self.log.dry_run_order(
                action="SELL", token_id=token_id, price=price,
                size=size_shares, client_order_id=cid,
            )
            return {"order_id": mock_id, "client_order_id": cid, "status": "DRY_RUN"}

        return self._place_order(token_id, "SELL", price, size_shares, cid)

    def _place_order(self, token_id: str, side: str, price: float,
                     size: float, client_order_id: str) -> dict:
        """Internal: place via CLOB client with retry."""
        retry = _retry(self.cfg.RETRY_MAX, self.cfg.RETRY_BACKOFF_BASE, self.log)

        @retry
        def _submit():
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side,
            )
            signed = self._clob.create_and_post_order(order_args)
            return signed

        result = _submit()
        order_id = ""
        if isinstance(result, dict):
            order_id = result.get("orderID", result.get("id", ""))
        elif hasattr(result, "orderID"):
            order_id = result.orderID
        elif hasattr(result, "id"):
            order_id = result.id

        return {
            "order_id": str(order_id),
            "client_order_id": client_order_id,
            "status": "SUBMITTED",
            "raw": result,
        }

    # ------------------------------------------------------------------
    # Order cancellation
    # ------------------------------------------------------------------
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order.  Returns True on success."""
        if self.cfg.MODE == "DRY_RUN":
            self.log.dry_run_order(action="CANCEL", order_id=order_id)
            return True

        retry = _retry(self.cfg.RETRY_MAX, self.cfg.RETRY_BACKOFF_BASE, self.log)

        @retry
        def _cancel():
            self._clob.cancel(order_id)
            return True

        try:
            return _cancel()
        except Exception as e:
            self.log.warn(f"cancel_order failed: {e}", order_id=order_id)
            return False

    def cancel_all(self) -> int:
        """Cancel all open orders.  Returns count cancelled."""
        if self.cfg.MODE == "DRY_RUN":
            self.log.dry_run_order(action="CANCEL_ALL")
            return 0

        try:
            self._clob.cancel_all()
            self.log.info("cancel_all executed")
            return -1  # unknown count
        except Exception as e:
            self.log.warn(f"cancel_all failed: {e}")
            return 0

    # ------------------------------------------------------------------
    # Open orders / positions / fills (LIVE only)
    # ------------------------------------------------------------------
    def get_open_orders(self) -> List[dict]:
        """Fetch open orders from CLOB."""
        if self.cfg.MODE == "DRY_RUN":
            return []

        retry = _retry(self.cfg.RETRY_MAX, self.cfg.RETRY_BACKOFF_BASE, self.log)

        @retry
        def _fetch():
            return self._clob.get_orders()

        try:
            result = _fetch()
            if isinstance(result, list):
                return result
            return []
        except Exception:
            return []

    def get_fills(self, since_ts: float | None = None) -> List[dict]:
        """Fetch recent fills/trades."""
        if self.cfg.MODE == "DRY_RUN":
            return []

        retry = _retry(self.cfg.RETRY_MAX, self.cfg.RETRY_BACKOFF_BASE, self.log)

        @retry
        def _fetch():
            params = {}
            if since_ts:
                params["after"] = int(since_ts * 1000)
            return self._clob.get_trades(params=params)

        try:
            result = _fetch()
            if isinstance(result, list):
                return result
            return []
        except Exception:
            return []

    def get_balances(self) -> Dict[str, float]:
        """Get token balances.  Returns {token_id: shares}."""
        if self.cfg.MODE == "DRY_RUN":
            return {}

        try:
            resp = requests.get(
                f"{self.cfg.CLOB_API_URL}/balances",
                headers=self._auth_headers(),
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            return {item["asset_id"]: float(item["balance"])
                    for item in data if float(item.get("balance", 0)) > 0}
        except Exception as e:
            self.log.api_error(fn="get_balances", error=str(e))
            return {}

    def _auth_headers(self) -> dict:
        """Build auth headers for direct API calls."""
        if self._clob and hasattr(self._clob, "headers"):
            return dict(self._clob.headers)
        return {}
