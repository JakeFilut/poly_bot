"""
strategy.py – Convergence dip-buy / rip-sell strategy for Polymarket binary options.

Core logic (from independent_backtest sweep on 44hrs / 287K rows):
  - BUY_UP: when Up-token mid > 0.55 AND price dipped in last 5s AND imbalance surging
  - SELL_UP (buy Down): when Up-token mid < 0.35 AND price rose in last 5s AND imbalance dropping
  - Hold for 120s, then exit full position at bestBid (passive)
  - Cooldown: 5s per market
  - Passive entry only (buy at bid)

Best backtest result: $54.51/hr gross, $43.58/hr net (0.5% fee), 70.3% win rate.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from config import Config
from features import SlugFeatures, TokenFeatures
from logger import Logger
from state import InventoryEntry, StateManager

_ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Action types returned by strategy
# ---------------------------------------------------------------------------
@dataclass
class TradeAction:
    """A desired trade action for execution to handle."""
    action: str         # "BUY", "SELL", "CANCEL", "SKIP"
    slug: str
    outcome: str        # "Up" or "Down"
    token_id: str
    price: float        # desired limit price
    size_shares: float  # shares
    size_usd: float     # notional USD
    reason: str         # human-readable reason for logging
    urgency: float = 0.5  # 0=low, 1=high (for execution priority)
    ladder_id: str = ""           # unique id linking all levels of one ladder
    ladder_level_idx: int = -1    # 0-based index within the ladder (-1 = not a ladder)
    ladder_levels_total: int = 0  # total levels in this ladder (0 = not a ladder)
    # -- Logging fields --
    intent_timestamp: float = 0.0  # epoch when signal was detected
    spread_cents: float = 0.0
    spread_percentile: float = 0.0
    return_5s: float = 0.0
    return_30s: float = 0.0
    orderbook_imbalance: float = 0.0
    entry_style: str = ""          # "passive" or "cross"


# ═══════════════════════════════════════════════════════════════════════════
# Cadence helpers (kept for sell_weight compatibility with execution engine)
# ═══════════════════════════════════════════════════════════════════════════
def _seconds_from_quarter(now_utc: datetime) -> int:
    """Seconds elapsed since the last 15-minute boundary in America/New_York."""
    now_et = now_utc.astimezone(_ET)
    total_sec = now_et.minute * 60 + now_et.second
    return total_sec % 900  # 900 = 15 * 60


def cadence_weight(cfg: Config, now_utc: datetime) -> tuple[float, float]:
    """Return (buy_weight, sell_weight). Convergence strategy doesn't use cadence
    for entries (signal-driven), but we keep the interface for execution engine."""
    return 1.0, 1.0


# ═══════════════════════════════════════════════════════════════════════════
# Main Strategy Runner — Convergence dip-buy / rip-sell
# ═══════════════════════════════════════════════════════════════════════════
class Strategy:
    """Convergence strategy: buy dips when mid is high, sell rips when mid is low.
    Time-based exit after hold_sec."""

    def __init__(self, cfg: Config, state: StateManager, logger: Logger):
        self.cfg = cfg
        self.state = state
        self.log = logger

        # Track entry timestamps for time-based exit: (slug, outcome) -> entry_epoch
        self._entry_times: Dict[Tuple[str, str], float] = {}

        # Cooldown tracking: (slug, outcome) -> last_trade_epoch
        self._last_trade_ts: Dict[str, float] = {}

        # Hourly budget tracking: $100/hr cap, sells refund the budget
        self._hourly_budget_spent: float = 0.0  # USD spent this hour
        self._hourly_budget_hour: int = -1       # current hour (ET)

        # Entry quality tracking
        self._entry_quality: Dict[str, list] = {
            "buy_prices": [],
            "edges_at_entry": [],
            "skips_by_gate": {},
            "profit_per_sell": [],
        }

    def get_and_reset_entry_quality(self) -> dict:
        """Return accumulated entry quality stats and reset."""
        stats = dict(self._entry_quality)
        buy_prices = stats.pop("buy_prices")
        edges = stats.pop("edges_at_entry")
        sells = stats.pop("profit_per_sell")
        skips = stats.pop("skips_by_gate")
        result = {
            "avg_buy_price": round(sum(buy_prices) / len(buy_prices), 4) if buy_prices else 0.0,
            "avg_edge_at_entry": round(sum(edges) / len(edges), 4) if edges else 0.0,
            "total_buys": len(buy_prices),
            "total_sells": len(sells),
            "profit_per_sell": round(sum(sells) / len(sells), 4) if sells else 0.0,
            "skips_by_gate": dict(skips),
            "total_skipped": sum(skips.values()),
        }
        self._entry_quality = {
            "buy_prices": [],
            "edges_at_entry": [],
            "skips_by_gate": {},
            "profit_per_sell": [],
        }
        return result

    def record_sell_pnl(self, pnl: float) -> None:
        """Record a sell fill PnL for entry quality tracking."""
        self._entry_quality["profit_per_sell"].append(pnl)

    def _get_hourly_budget_remaining(self, now_utc: datetime) -> float:
        """Return how much USD we can still spend this hour. Resets each ET hour."""
        hour_et = now_utc.astimezone(_ET).hour
        if hour_et != self._hourly_budget_hour:
            # New hour — reset budget
            self._hourly_budget_spent = 0.0
            self._hourly_budget_hour = hour_et
            self.log.log("INFO", msg="hourly_budget_reset", hour_et=hour_et,
                         budget_usd=self.cfg.CONV_HOURLY_BUDGET_USD)
        return self.cfg.CONV_HOURLY_BUDGET_USD - self._hourly_budget_spent

    def _spend_budget(self, usd: float) -> None:
        """Record USD spent against the hourly budget."""
        self._hourly_budget_spent += usd

    def _refund_budget(self, usd: float) -> None:
        """Refund USD to the hourly budget (on sell). Budget can't go below 0 spent."""
        before = self._hourly_budget_spent
        self._hourly_budget_spent = max(0.0, self._hourly_budget_spent - usd)
        self.log.log("INFO", msg="budget_refund",
                     refund_usd=round(usd, 2),
                     spent_before=round(before, 2),
                     spent_after=round(self._hourly_budget_spent, 2))

    def budget_snapshot(self) -> dict:
        """Return current hourly budget status."""
        return {
            "hourly_budget_usd": self.cfg.CONV_HOURLY_BUDGET_USD,
            "spent_usd": round(self._hourly_budget_spent, 2),
            "remaining_usd": round(self.cfg.CONV_HOURLY_BUDGET_USD - self._hourly_budget_spent, 2),
            "budget_hour_et": self._hourly_budget_hour,
        }

    def _cooldown_ok(self, key: str, now: float) -> bool:
        """Check if cooldown has elapsed for a market."""
        last = self._last_trade_ts.get(key, 0)
        return (now - last) >= self.cfg.PER_TOKEN_COOLDOWN_SEC

    def _record_trade(self, key: str, now: float) -> None:
        """Record a trade timestamp for cooldown tracking."""
        self._last_trade_ts[key] = now

    def generate_actions(self, all_features: dict,
                         risk_allows_buy: callable,
                         risk_allows_sell: callable) -> List[TradeAction]:
        """Generate trade actions using the convergence dip-buy / rip-sell strategy.

        For each slug:
          1. EXIT PASS: If we hold a position for >= hold_sec, sell it all.
          2. ENTRY PASS: Check convergence signals on the Up token.
             - mid > buy_thresh + price dipped + imbalance surging → BUY Up token
             - mid < sell_thresh + price rose + imbalance dropping → BUY Down token
        """
        now = time.time()
        now_utc = datetime.now(timezone.utc)
        intent_ts = now
        actions: List[TradeAction] = []

        cfg = self.cfg
        lookback_col = f"mid_chg_{cfg.CONV_LOOKBACK_SEC}s"

        for slug, sf in all_features.items():
            # ==============================================================
            # EXIT PASS: time-based exit for all held positions
            # ==============================================================
            for outcome in ("Up", "Down"):
                inv = self.state.get_inventory(slug, outcome)
                if inv is None or inv.shares <= 0:
                    # Clean up stale entry time if position gone
                    self._entry_times.pop((slug, outcome), None)
                    continue

                tf = sf.up if outcome == "Up" else sf.down
                if tf is None or not tf.has_book:
                    continue

                # Get entry time (if not tracked, use last_update_ts as fallback)
                entry_key = (slug, outcome)
                entry_ts = self._entry_times.get(entry_key)
                if entry_ts is None:
                    # Position exists but we don't have entry time (e.g. after restart)
                    # Start the clock now; will exit after CONV_HOLD_SEC from this point
                    entry_ts = now
                    self._entry_times[entry_key] = now

                held_sec = now - entry_ts
                sell_price = tf.best_bid
                if sell_price <= 0:
                    continue

                # --- Stop loss / take profit check ---
                edge_cents = (sell_price - inv.avg_cost) * 100
                stop_triggered = (cfg.CONV_STOP_LOSS_CENTS > 0
                                  and edge_cents <= -cfg.CONV_STOP_LOSS_CENTS)
                tp_triggered = (cfg.CONV_TAKE_PROFIT_CENTS > 0
                                and edge_cents >= cfg.CONV_TAKE_PROFIT_CENTS)

                if stop_triggered:
                    reason = f"STOP_LOSS(down={-edge_cents:.1f}c>=limit={cfg.CONV_STOP_LOSS_CENTS:.0f}c)"
                elif tp_triggered:
                    reason = f"TAKE_PROFIT(up={edge_cents:.1f}c>=limit={cfg.CONV_TAKE_PROFIT_CENTS:.0f}c)"
                elif held_sec >= cfg.CONV_HOLD_SEC:
                    reason = f"TIME_EXIT(held={held_sec:.0f}s>={cfg.CONV_HOLD_SEC}s)"
                else:
                    continue  # no exit condition triggered

                sell_shares = inv.shares
                sell_usd = sell_shares * sell_price

                sell_ok, sell_reason = risk_allows_sell(slug, outcome)
                if not sell_ok:
                    continue

                self.log.decision(
                    action="SELL", reason=reason,
                    slug=slug, outcome=outcome, asset=sf.asset,
                    shares=sell_shares, price=sell_price,
                    held_sec=round(held_sec, 1),
                    edge_vs_cost=round(sell_price - inv.avg_cost, 4),
                    spread_cents=round(tf.spread * 100, 1),
                )

                actions.append(TradeAction(
                    action="SELL", slug=slug, outcome=outcome,
                    token_id=tf.token_id, price=sell_price,
                    size_shares=sell_shares, size_usd=sell_usd,
                    reason=reason, urgency=1.0 if (stop_triggered or tp_triggered) else 0.9,
                    intent_timestamp=intent_ts,
                    spread_cents=round(tf.spread * 100, 1),
                    entry_style="cross",
                ))

                # Budget refund now happens on actual sell fill
                # (via execution._on_sell_fill_budget callback)

                # Clear entry time after exit
                self._entry_times.pop(entry_key, None)

            # ==============================================================
            # ENTRY PASS: convergence signals on the Up token
            # ==============================================================
            # We need the Up token to check mid price thresholds
            up_tf = sf.up
            if up_tf is None or not up_tf.has_book:
                continue

            up_mid = up_tf.mid
            mid_chg = getattr(up_tf, lookback_col, up_tf.mid_chg_5s)
            imb_chg = up_tf.imb_chg_5s

            # Dip/rip thresholds (convert cents to price units)
            dip_thresh = -cfg.CONV_MIN_DIP_CENTS / 100  # negative for dips
            rip_thresh = cfg.CONV_MIN_DIP_CENTS / 100   # positive for rips

            # --- Check BUY_UP signal ---
            # mid > buy_thresh: market trending toward $1 (Up likely wins)
            # price dipped: mid dropped in last N seconds (buy the dip)
            # imbalance surging: buyers stepping in
            buy_up_signal = (
                up_mid > cfg.CONV_MID_BUY_THRESH
                and mid_chg < dip_thresh
                and imb_chg > cfg.CONV_MIN_IMB_CHANGE
            )

            # --- Check SELL_UP signal (= BUY Down) ---
            # mid < sell_thresh: market trending toward $0 (Down likely wins)
            # price rose: mid rose in last N seconds (sell the rip)
            # imbalance dropping: sellers stepping in
            sell_up_signal = (
                up_mid < cfg.CONV_MID_SELL_THRESH
                and mid_chg > rip_thresh
                and imb_chg < -cfg.CONV_MIN_IMB_CHANGE
            )

            if not buy_up_signal and not sell_up_signal:
                self.log.decision(
                    action="SKIP", reason="no_convergence_signal",
                    slug=slug, asset=sf.asset,
                    up_mid=round(up_mid, 4),
                    mid_chg=round(mid_chg, 6),
                    imb_chg=round(imb_chg, 4),
                )
                continue

            # Determine direction and target token
            if buy_up_signal:
                direction = "Up"
                side_label = "BUY_UP"
            else:
                direction = "Down"
                side_label = "SELL_UP"

            tf = sf.up if direction == "Up" else sf.down
            if tf is None or not tf.has_book:
                continue

            # --- Cooldown check ---
            cooldown_key = f"{slug}|{direction}"
            if not self._cooldown_ok(cooldown_key, now):
                skips = self._entry_quality["skips_by_gate"]
                skips["cooldown"] = skips.get("cooldown", 0) + 1
                continue

            # --- Don't enter if we already hold this position ---
            inv = self.state.get_inventory(slug, direction)
            if inv is not None and inv.shares > 0:
                skips = self._entry_quality["skips_by_gate"]
                skips["already_holding"] = skips.get("already_holding", 0) + 1
                continue

            # --- Hourly budget check ---
            budget_remaining = self._get_hourly_budget_remaining(now_utc)
            position_size = cfg.CONV_POSITION_SIZE
            price = tf.best_ask  # cross entry (buy at ask for immediate fill)
            if price <= 0:
                continue
            desired_usd = position_size * price

            if desired_usd > budget_remaining:
                skips = self._entry_quality["skips_by_gate"]
                skips["hourly_budget"] = skips.get("hourly_budget", 0) + 1
                self.log.decision(
                    action="SKIP", reason=f"hourly_budget(need=${desired_usd:.2f},remaining=${budget_remaining:.2f})",
                    slug=slug, outcome=direction, asset=sf.asset,
                )
                continue

            # --- Risk gate ---
            buy_ok, buy_reason = risk_allows_buy(slug, direction, desired_usd)
            if not buy_ok:
                skips = self._entry_quality["skips_by_gate"]
                skips["risk"] = skips.get("risk", 0) + 1
                self.log.decision(
                    action="SKIP", reason=f"risk_gate({buy_reason})",
                    slug=slug, outcome=direction, asset=sf.asset,
                )
                continue

            # --- Generate BUY action ---
            shares = int(position_size)
            actual_usd = shares * price

            ob_imb = up_tf.imbalance_topN
            reason = (f"{side_label}(mid={up_mid:.3f},chg={mid_chg*100:.1f}c,"
                      f"imb_chg={imb_chg:.3f})")

            self.log.decision(
                action="BUY", reason=reason,
                slug=slug, outcome=direction, asset=sf.asset,
                price=price, shares=shares, usd=actual_usd,
                up_mid=round(up_mid, 4),
                mid_chg=round(mid_chg, 6),
                imb_chg=round(imb_chg, 4),
                ob_imbalance=round(ob_imb, 4),
                spread_cents=round(tf.spread * 100, 1),
                entry_style="cross",
                intent_timestamp=intent_ts,
            )

            # Spend against hourly budget (reservation for gating within tick)
            self._spend_budget(actual_usd)

            # Track entry quality
            self._entry_quality["buy_prices"].append(price)

            # Track entry time for time-based exit
            self._entry_times[(slug, direction)] = now
            self._record_trade(cooldown_key, now)

            actions.append(TradeAction(
                action="BUY", slug=slug, outcome=direction,
                token_id=tf.token_id, price=price,
                size_shares=shares, size_usd=actual_usd,
                reason=reason, urgency=0.8,
                intent_timestamp=intent_ts,
                spread_cents=round(tf.spread * 100, 1),
                spread_percentile=round(tf.spread_pctl_60s, 4),
                return_5s=round(tf.ret_5s or 0, 8),
                return_30s=round(tf.ret_30s or 0, 8),
                orderbook_imbalance=round(ob_imb, 4),
                entry_style="cross",
            ))

        return actions
