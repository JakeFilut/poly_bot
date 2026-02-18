"""
Data structures for the poly_bot refactoring (Pass 1 -- exact field-for-field
copy of the dataclasses formerly defined in pm_hourly_clone_bot.py).
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class MarketRef:
    crypto: str
    slug: str
    market_id: str              # polymarket market identifier (token / condition id)
    outcome_up_id: str          # token id for UP
    outcome_down_id: str        # token id for DOWN
    hour_open: float            # open reference price for the hour
    hour_start_utc: datetime    # hour start timestamp


@dataclass
class BookTop:
    bid: float
    ask: float
    bid_sz: float
    ask_sz: float
    spread: float
    imb: float                 # bid_depth/ask_depth over N levels (approx)
    mid: float
    # Depth at price increments (cumulative size within Xc of best)
    depth_1c_bid: float = 0.0
    depth_1c_ask: float = 0.0
    depth_2c_bid: float = 0.0
    depth_2c_ask: float = 0.0
    depth_5c_bid: float = 0.0
    depth_5c_ask: float = 0.0


@dataclass
class Position:
    qty: float = 0.0
    cost_usdc: float = 0.0      # total cost spent (for paper)
    vwap: float = 0.0
    tp1_done: bool = False
    tp2_done: bool = False
    tp3_done: bool = False
    opened_at: Optional[str] = None
    last_trade_ts: Optional[str] = None
    scalp_mode: bool = False
    scalp_open_ts: Optional[str] = None
    position_id: Optional[str] = None       # UUID lifecycle: first entry -> fully flat
    trade_id: Optional[str] = None          # persistent across entry -> TP1 -> TP2 -> cleanup
    entry_decision_id: Optional[str] = None # decision_id of original entry (parent)
    parent_order_id: Optional[str] = None   # client_order_id of entry order (for exit legs)
    entry_mid: float = 0.0                  # mid price at entry time
    max_favorable_mid: float = 0.0          # best mid seen while holding
    max_adverse_mid: float = 1.0            # worst mid seen while holding
    last_derisk_ts: Optional[str] = None    # ISO timestamp of last DERISK sell
    last_derisk_mid: float = 0.0            # mid price at last DERISK action
    fast_tp_done: bool = False              # FAST_TP fires only once per position


@dataclass
class MarketState:
    slug: str
    crypto: str
    hour_open: float
    hour_start_utc: str
    last_entry_ts: Optional[str] = None
    last_reentry_ts: Optional[str] = None
    peak_abs_delta_bps: float = 0.0
    hour_index: int = 0                          # monotonic counter per crypto
    delta_hist: List[Tuple[str, float]] = None   # (iso, delta_bps)
    price_hist: List[Tuple[str, float]] = None   # (iso, binance_spot)
    positions: Dict[str, Position] = None        # "Up" / "Down"

    def __post_init__(self):
        if self.delta_hist is None:
            self.delta_hist = []
        if self.price_hist is None:
            self.price_hist = []
        if self.positions is None:
            self.positions = {"Up": Position(), "Down": Position()}
