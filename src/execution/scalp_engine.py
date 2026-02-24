"""
ScalpEngine — copy-wallet-style micro-scalp engine for HYBRID_COPYWALLET.

Runs on a fast loop (200-500ms), looks for spread + imbalance signals,
enters/exits tiny positions with tight TP/SL.

Design:
- One scalp position per slug at a time (no stacking)
- Direction decided by micro order-book imbalance
- IOC entries, limit exits (TP), time-stop + loss-stop fallbacks
- Completely isolated from directional engine logic
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable, List, TYPE_CHECKING

from src.config import settings
from src.bot.context import BookTop, MarketRef

if TYPE_CHECKING:
    from src.execution.exposure_tracker import ExposureTracker


@dataclass
class ScalpState:
    """Tracks a single open scalp position."""
    slug: str
    outcome: str          # "Up" or "Down"
    entry_price: float
    entry_ts: float       # epoch
    qty: float
    exit_order_id: str = ""
    exit_posted: bool = False


class ScalpEngine:
    """Micro-scalp engine running alongside the directional engine.

    Lifecycle per slug:
      FLAT → signal → IOC entry → hold (with TP limit) → exit (TP/SL/time)

    The engine does NOT place orders itself — it returns ScalpAction objects
    that the main bot loop executes. This keeps order placement centralized.
    """

    def __init__(self, exposure_tracker: 'ExposureTracker',
                 write_jsonl: Optional[Callable] = None):
        self._tracker = exposure_tracker
        self._write_jsonl = write_jsonl or (lambda d: None)

        # Active scalp positions: slug -> ScalpState
        self._positions: Dict[str, ScalpState] = {}

        # Open scalp exit orders: slug -> order_id
        self._exit_orders: Dict[str, str] = {}

        # Per-slug consecutive stop counter for pause logic
        self._consec_stops: Dict[str, int] = {}
        self._slug_paused_until: Dict[str, float] = {}

        # Diagnostics
        self.diag_signals: int = 0
        self.diag_entries: int = 0
        self.diag_exits_tp: int = 0
        self.diag_exits_stop: int = 0
        self.diag_exits_time: int = 0
        self.diag_pnl_cents: List[float] = []

    # ──────────────────────────────────────────────────────────────────
    # Main tick — called from bot main loop
    # ──────────────────────────────────────────────────────────────────

    def tick(self, slug: str, m: MarketRef,
             up_book: Optional[BookTop], dn_book: Optional[BookTop],
             safe_mode: bool, desync_hard_stop: bool,
             t_min: float) -> List[dict]:
        """Run one scalp tick for a slug.

        Returns a list of action dicts for the bot to execute:
          {"action": "BUY", "slug": slug, "outcome": ..., "qty": ..., "price": ..., "engine": "SCALP"}
          {"action": "SELL", "slug": slug, "outcome": ..., "qty": ..., "price": ..., "engine": "SCALP", "reason": ...}
        """
        if not settings.SCALP_ENABLED:
            return []

        now = time.time()
        actions = []

        # ── Never trade during safe mode or desync ──
        if safe_mode or desync_hard_stop:
            return []

        # ── Don't enter during flatten phase ──
        if t_min >= settings.FLATTEN_START_MIN:
            # Only manage exits
            return self._manage_exits(slug, up_book, dn_book, now)

        # ── Manage existing position ──
        if slug in self._positions:
            return self._manage_exits(slug, up_book, dn_book, now)

        # ── Check if slug is paused (consecutive stops) ──
        if now < self._slug_paused_until.get(slug, 0.0):
            return []

        # ── Entry scan ──
        return self._scan_entry(slug, m, up_book, dn_book, now, t_min)

    # ──────────────────────────────────────────────────────────────────
    # Entry scanning
    # ──────────────────────────────────────────────────────────────────

    def _scan_entry(self, slug: str, m: MarketRef,
                    up_book: Optional[BookTop], dn_book: Optional[BookTop],
                    now: float, t_min: float) -> List[dict]:
        """Check if entry conditions are met for a scalp."""
        if not up_book or not dn_book:
            return []
        if up_book.bid <= 0 or dn_book.bid <= 0:
            return []

        # Check both sides for signal
        for book, outcome, opp_book in [
            (up_book, "Up", dn_book),
            (dn_book, "Down", up_book),
        ]:
            spread_cents = book.spread * 100
            if spread_cents < settings.SCALP_MIN_SPREAD_CENTS:
                continue

            # Micro imbalance: bid_sz / (bid_sz + ask_sz)
            total_sz = book.bid_sz + book.ask_sz
            if total_sz <= 0:
                continue
            micro_imb = book.bid_sz / total_sz

            # Determine direction from imbalance
            # Up: high bid imbalance (>= 0.62) → buy Up (price rising)
            if outcome == "Up" and micro_imb < settings.SCALP_IMB_LONG_THRESHOLD:
                continue
            # Down: low bid imbalance (<= 0.38) → buy Down (ask pressure, price falling)
            if outcome == "Down" and micro_imb > settings.SCALP_IMB_SHORT_THRESHOLD:
                continue

            # Edge check: spread must cover fees
            taker_fee_cents = book.ask * settings.TAKER_FEE_BPS / 100 * 100  # fee in cents
            net_edge_cents = spread_cents / 2 - taker_fee_cents
            if net_edge_cents < settings.SCALP_MIN_EDGE_CENTS:
                continue

            # Exposure check
            entry_usd = settings.SCALP_SIZE_USD
            if not self._tracker.can_open("SCALP", entry_usd):
                continue

            # Already have a position for this slug?
            if self._tracker.has_scalp_position(slug):
                continue

            # Compute qty
            entry_price = book.ask
            qty = entry_usd / max(0.01, entry_price)
            if qty < 5:  # CLOB minimum
                continue

            self.diag_signals += 1

            self._write_jsonl({
                "event_type": "SCALP_SIGNAL",
                "slug": slug, "outcome": outcome,
                "spread_cents": round(spread_cents, 2),
                "micro_imb": round(micro_imb, 3),
                "net_edge_cents": round(net_edge_cents, 2),
                "entry_price": round(entry_price, 4),
                "qty": round(qty, 1),
                "ts_ms": int(now * 1000),
            })

            return [{
                "action": "BUY",
                "slug": slug,
                "outcome": outcome,
                "qty": qty,
                "price": entry_price,
                "engine": "SCALP",
                "reason": "SCALP_ENTRY",
            }]

        return []

    def on_entry_fill(self, slug: str, outcome: str,
                      fill_qty: float, fill_price: float) -> None:
        """Called by bot after a scalp entry fill is confirmed."""
        now = time.time()
        self._positions[slug] = ScalpState(
            slug=slug,
            outcome=outcome,
            entry_price=fill_price,
            entry_ts=now,
            qty=fill_qty,
        )
        self.diag_entries += 1
        self._tracker.scalp_trades += 1

        self._write_jsonl({
            "event_type": "SCALP_ENTRY",
            "slug": slug, "outcome": outcome,
            "qty": round(fill_qty, 2),
            "entry_price": round(fill_price, 4),
            "ts_ms": int(now * 1000),
        })

    # ──────────────────────────────────────────────────────────────────
    # Exit management
    # ──────────────────────────────────────────────────────────────────

    def _manage_exits(self, slug: str,
                      up_book: Optional[BookTop], dn_book: Optional[BookTop],
                      now: float) -> List[dict]:
        """Check TP, SL, and time-stop for open scalp position."""
        state = self._positions.get(slug)
        if not state:
            return []

        book = up_book if state.outcome == "Up" else dn_book
        if not book or book.bid <= 0:
            return []

        mid = book.mid
        hold_sec = now - state.entry_ts
        pnl_cents = (mid - state.entry_price) * 100

        # ── Take Profit ──
        if pnl_cents >= settings.SCALP_TAKE_PROFIT_CENTS:
            sell_price = book.bid
            return [self._build_exit(state, sell_price, "SCALP_EXIT_TP", hold_sec, pnl_cents)]

        # ── Stop Loss ──
        if pnl_cents <= -settings.SCALP_STOP_LOSS_CENTS:
            # Cross at bid - 1 tick
            sell_price = max(0.01, book.bid - 0.01)
            return [self._build_exit(state, sell_price, "SCALP_EXIT_STOP", hold_sec, pnl_cents)]

        # ── Time Stop ──
        if hold_sec >= settings.SCALP_MAX_HOLD_SEC:
            sell_price = max(0.01, book.bid - 0.01)
            return [self._build_exit(state, sell_price, "SCALP_EXIT_TIME", hold_sec, pnl_cents)]

        return []

    def _build_exit(self, state: ScalpState, sell_price: float,
                    reason: str, hold_sec: float, pnl_cents: float) -> dict:
        """Build an exit action dict and update internal state."""
        now = time.time()

        # Estimate fees
        fee_est_cents = state.entry_price * settings.TAKER_FEE_BPS / 100 * 100 * 2  # round trip
        net_pnl_cents = pnl_cents - fee_est_cents

        self._write_jsonl({
            "event_type": reason,
            "slug": state.slug, "outcome": state.outcome,
            "entry_price": round(state.entry_price, 4),
            "exit_price": round(sell_price, 4),
            "hold_sec": round(hold_sec, 2),
            "pnl_cents": round(pnl_cents, 2),
            "net_pnl_cents": round(net_pnl_cents, 2),
            "fees_est_cents": round(fee_est_cents, 2),
            "ts_ms": int(now * 1000),
        })

        # Update diagnostics
        self.diag_pnl_cents.append(net_pnl_cents)
        self._tracker.scalp_hold_times.append(hold_sec)

        if reason == "SCALP_EXIT_TP":
            self.diag_exits_tp += 1
            self._consec_stops[state.slug] = 0
            self._tracker.scalp_wins += 1
        elif reason == "SCALP_EXIT_STOP":
            self.diag_exits_stop += 1
            stops = self._consec_stops.get(state.slug, 0) + 1
            self._consec_stops[state.slug] = stops
            if settings.SCALP_CONSEC_STOP_PAUSE_ENABLED and stops >= settings.SCALP_CONSEC_STOP_THRESHOLD:
                self._slug_paused_until[state.slug] = now + settings.SCALP_CONSEC_STOP_PAUSE_SEC
                self._write_jsonl({
                    "event_type": "SCALP_CONSEC_STOP_PAUSE",
                    "slug": state.slug,
                    "consec_stops": stops,
                    "pause_sec": settings.SCALP_CONSEC_STOP_PAUSE_SEC,
                    "ts_ms": int(now * 1000),
                })
        elif reason == "SCALP_EXIT_TIME":
            self.diag_exits_time += 1
            self._consec_stops[state.slug] = 0

        # Remove position from scalp state
        self._positions.pop(state.slug, None)

        return {
            "action": "SELL",
            "slug": state.slug,
            "outcome": state.outcome,
            "qty": state.qty,
            "price": sell_price,
            "engine": "SCALP",
            "reason": reason,
        }

    # ──────────────────────────────────────────────────────────────────
    # Cleanup
    # ──────────────────────────────────────────────────────────────────

    def on_exit_fill(self, slug: str) -> None:
        """Called by bot after a scalp exit fill is confirmed."""
        self._positions.pop(slug, None)
        self._exit_orders.pop(slug, None)

    def force_close_all(self) -> List[dict]:
        """Generate sell actions for all open scalp positions (for flatten)."""
        actions = []
        for slug, state in list(self._positions.items()):
            # Use a 0 price placeholder — caller must look up current book
            actions.append({
                "action": "SELL",
                "slug": state.slug,
                "outcome": state.outcome,
                "qty": state.qty,
                "price": 0.0,  # caller fills in real price
                "engine": "SCALP",
                "reason": "SCALP_FLATTEN",
            })
        return actions

    def has_position(self, slug: str) -> bool:
        return slug in self._positions

    def get_state(self, slug: str) -> Optional[ScalpState]:
        return self._positions.get(slug)

    def active_count(self) -> int:
        return len(self._positions)

    def summary(self) -> dict:
        """Return summary for end-of-hour report."""
        total_pnl = sum(self.diag_pnl_cents) if self.diag_pnl_cents else 0.0
        avg_hold = (sum(self._tracker.scalp_hold_times) /
                    max(1, len(self._tracker.scalp_hold_times)))
        return {
            "scalp_signals": self.diag_signals,
            "scalp_entries": self.diag_entries,
            "scalp_exits_tp": self.diag_exits_tp,
            "scalp_exits_stop": self.diag_exits_stop,
            "scalp_exits_time": self.diag_exits_time,
            "scalp_total_pnl_cents": round(total_pnl, 2),
            "scalp_avg_hold_sec": round(avg_hold, 2),
            "scalp_trades": self._tracker.scalp_trades,
            "scalp_wins": self._tracker.scalp_wins,
            "scalp_winrate": round(
                self._tracker.scalp_wins / max(1, self._tracker.scalp_trades), 3),
        }
