"""
strategy.py – F247-style cadence scalper strategy.

Core logic:
  - 15-minute cadence scheduler (BUY early, SELL at ~minute 5)
  - Direction selection via Binance 30s momentum
  - Entry gating: spread percentile, risk checks
  - Entry pricing: aggressive at 1¢ spread, passive at 2¢+
  - Entry sizing: discrete USD ladder
  - Exit logic: cost-basis TP/SL shaving
  - Both-sides inventory (no forced neutralization)
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional
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


# ═══════════════════════════════════════════════════════════════════════════
# 9.1  Cadence Scheduler
# ═══════════════════════════════════════════════════════════════════════════
def _seconds_from_quarter(now_utc: datetime) -> int:
    """Seconds elapsed since the last 15-minute boundary in America/New_York.

    The copied wallet's 15-minute cadence is aligned to ET clock boundaries.
    Using UTC would cause drift and miss the real burst points.
    Handles DST transitions automatically via zoneinfo.
    """
    now_et = now_utc.astimezone(_ET)
    total_sec = now_et.minute * 60 + now_et.second
    return total_sec % 900  # 900 = 15 * 60


def cadence_weight(cfg: Config, now_utc: datetime) -> tuple[float, float]:
    """Return (buy_weight, sell_weight) based on 15-min cadence in ET.

    buy_weight:  0.0 to 1.0 (how aggressively to buy)
    sell_weight: 0.0 to 1.0 (how aggressively to sell/shave)
    """
    sec = _seconds_from_quarter(now_utc)

    # BUY weight
    if sec < cfg.BUY_HEAVY_SEC:
        buy_w = 1.0
    elif sec < cfg.BUY_MED_SEC:
        buy_w = 0.6
    else:
        buy_w = 0.15  # background level: can still buy but rarely

    # SELL weight
    sell_w = 0.1  # background
    if cfg.SELL_START_SEC <= sec < cfg.SELL_END_SEC:
        sell_w = 1.0  # heavy sell at minute 5-6
    elif cfg.SELL_BURST2_START <= sec < cfg.SELL_BURST2_END:
        sell_w = 0.5  # medium at minute 10
    elif cfg.SELL_BURST3_START <= sec < cfg.SELL_BURST3_END:
        sell_w = 0.4  # lighter at minute 13

    return buy_w, sell_w


# ═══════════════════════════════════════════════════════════════════════════
# 9.2  Direction Selection
# ═══════════════════════════════════════════════════════════════════════════
def _pick_buy_direction(sf: SlugFeatures, cfg: Config) -> str | None:
    """Pick 'Up' or 'Down' for a BUY based on Binance momentum.

    Returns None if no valid direction (both sides unusable).
    """
    ret = sf.ret_30s
    if ret is None:
        # No Binance data: alternate or skip
        return None

    # Volatility gate: skip slow-drift periods to reduce chop bleed
    if abs(ret) < cfg.BIN_RET30_THRESHOLD:
        return None

    if ret > 0:
        preferred = "Up"
        fallback = "Down"
    else:
        preferred = "Down"
        fallback = "Up"

    # Check if preferred side has a usable book
    pref_feat = sf.up if preferred == "Up" else sf.down
    fall_feat = sf.up if fallback == "Up" else sf.down

    if pref_feat and pref_feat.has_book:
        return preferred
    if fall_feat and fall_feat.has_book:
        return fallback
    return None


# ═══════════════════════════════════════════════════════════════════════════
# 9.3  Entry Gating
# ═══════════════════════════════════════════════════════════════════════════
def _entry_gated(tf: TokenFeatures, cfg: Config,
                 exposure_ok: bool, cash_ok: bool) -> tuple[bool, str]:
    """Check if entry is allowed.  Returns (allowed, reason_if_blocked)."""
    if not tf.has_book:
        return False, "no_book"
    # Volatility filter: skip entry when Binance isn't moving
    bin_ret_30s = abs(tf.ret_30s or 0)
    if bin_ret_30s < cfg.VOL_MIN_RET_30S:
        return False, f"low_vol(bin_ret_30s={bin_ret_30s:.6f}<{cfg.VOL_MIN_RET_30S})"
    if tf.spread_pctl_60s < cfg.SPREAD_PCTL_MIN:
        return False, f"spread_pctl({tf.spread_pctl_60s:.2f}<{cfg.SPREAD_PCTL_MIN})"
    if tf.spread > cfg.SPREAD_MAX_SANE:
        return False, f"spread_too_wide({tf.spread:.4f}>{cfg.SPREAD_MAX_SANE})"
    if tf.spread <= 0:
        return False, "zero_spread"
    if not exposure_ok:
        return False, "exposure_cap"
    if not cash_ok:
        return False, "cash_reserve"
    return True, ""


# ═══════════════════════════════════════════════════════════════════════════
# 9.4  Entry Pricing (Aggression)
# ═══════════════════════════════════════════════════════════════════════════
def _entry_price(tf: TokenFeatures, cfg: Config) -> float:
    """Determine limit buy price based on spread."""
    spread_cents = round(tf.spread * 100)  # integer cents

    if spread_cents <= 1:
        # Weak momentum → force passive (bid-side) to reduce adverse selection
        if abs(tf.ret_30s or 0) < cfg.VOL_MIN_RET_30S:
            return tf.best_bid  # passive: avoid crossing in low-vol
        # Tight spread: sometimes cross
        if random.random() < cfg.CROSS_PROB_1C:
            return tf.best_ask  # cross (taker)
        else:
            return tf.best_bid  # join bid (maker)
    else:
        # Wider spread: passive, bid-side price improvement
        # Target: mid - 0.01, but at least best_bid
        target = round(tf.mid - 0.01, 2)
        return max(tf.best_bid, min(target, tf.best_ask - 0.01))


# ═══════════════════════════════════════════════════════════════════════════
# 9.5  Entry Sizing (Discrete Ladder)
# ═══════════════════════════════════════════════════════════════════════════
_ASSET_SIZE_MULT = {"BTC": 1.2, "ETH": 1.1, "SOL": 0.9, "XRP": 0.9}


def _entry_size_usd(cfg: Config, buy_weight: float,
                    spread: float, asset: str) -> float:
    """Compute USD clip size, snapped to the discrete ladder."""
    # Base score from cadence
    score = buy_weight

    # Adjust for spread tightness (tighter → slightly larger)
    if spread <= 0.01:
        score *= 1.2
    elif spread <= 0.02:
        score *= 1.0
    else:
        score *= 0.8

    # Asset multiplier
    score *= _ASSET_SIZE_MULT.get(asset, 1.0)

    # Cap score so multipliers don't always push to max ladder rung
    score = min(1.0, score)

    # Map score to ladder index
    ladder = cfg.CLIP_LADDER_MULTS
    idx = int(score * (len(ladder) - 1))
    idx = max(0, min(idx, len(ladder) - 1))
    mult = ladder[idx]

    return cfg.CLIP_UNIT_USD * mult


def _usd_to_shares(usd: float, price: float) -> float:
    """Convert USD clip to shares, rounded down."""
    if price <= 0:
        return 0.0
    return int(usd / price)  # integer shares


# ═══════════════════════════════════════════════════════════════════════════
# 9.6  Exit Logic (SELL shaving)
# ═══════════════════════════════════════════════════════════════════════════
def _exit_actions(cfg: Config, inv: InventoryEntry, tf: TokenFeatures,
                  sell_weight: float) -> TradeAction | None:
    """Determine if we should sell, and how much.

    Returns a TradeAction for SELL, or None if no sell.
    """
    if inv.shares <= 0 or not tf.has_book:
        return None

    edge_vs_cost = tf.best_bid - inv.avg_cost

    # -- Take profit --
    if edge_vs_cost >= cfg.TP_CENTS_MIN:
        # Scale sell fraction by how deep into TP band we are
        tp_progress = min(1.0, (edge_vs_cost - cfg.TP_CENTS_MIN) /
                          max(0.001, cfg.TP_CENTS_MAX - cfg.TP_CENTS_MIN))
        base_frac = cfg.SELL_FRAC_MED + tp_progress * (cfg.SELL_FRAC_MAX - cfg.SELL_FRAC_MED)

        # Cadence factor: respect cadence unless edge is very large
        if edge_vs_cost >= 0.12:
            cadence_factor = 1.0  # large edge — sell regardless of cadence
        else:
            cadence_factor = sell_weight  # no floor — cadence controls TP sells

        frac = base_frac * cadence_factor
        # Ensure at least SELL_MIN_SHARES via the clamp below (not via frac floor)
        shares = max(cfg.SELL_MIN_SHARES, round(inv.shares * frac))
        shares = min(shares, inv.shares)
        sell_price = tf.best_bid
        usd = shares * sell_price

        return TradeAction(
            action="SELL", slug=inv.slug, outcome=inv.outcome,
            token_id=tf.token_id, price=sell_price,
            size_shares=shares, size_usd=usd,
            reason=f"TP(edge={edge_vs_cost:.4f},frac={frac:.3f},cad={cadence_factor:.2f})",
            urgency=0.5 + tp_progress * 0.3,
        )

    # -- Stop loss --
    if edge_vs_cost <= -cfg.SL_CENTS:
        frac = min(cfg.SELL_FRAC_MAX, cfg.SELL_FRAC_MED * 2.0)
        shares = max(cfg.SELL_MIN_SHARES, round(inv.shares * frac))
        shares = min(shares, inv.shares)
        sell_price = max(0.01, tf.best_bid)
        usd = shares * sell_price

        return TradeAction(
            action="SELL", slug=inv.slug, outcome=inv.outcome,
            token_id=tf.token_id, price=sell_price,
            size_shares=shares, size_usd=usd,
            reason=f"SL(edge={edge_vs_cost:.4f})",
            urgency=0.9,
        )

    return None


# ═══════════════════════════════════════════════════════════════════════════
# Main Strategy Runner
# ═══════════════════════════════════════════════════════════════════════════
class Strategy:
    """Runs the F247-style strategy each loop iteration."""

    def __init__(self, cfg: Config, state: StateManager, logger: Logger):
        self.cfg = cfg
        self.state = state
        self.log = logger

    def generate_actions(self, all_features: dict,
                         risk_allows_buy: callable,
                         risk_allows_sell: callable) -> List[TradeAction]:
        """Generate desired trade actions for this tick.

        Args:
            all_features: slug -> SlugFeatures
            risk_allows_buy: fn(slug, outcome, usd) -> (bool, str)
            risk_allows_sell: fn(slug, outcome) -> (bool, str)

        Returns list of TradeActions for execution.
        """
        now_utc = datetime.now(timezone.utc)
        buy_weight, sell_weight = cadence_weight(self.cfg, now_utc)
        sec_from_q = _seconds_from_quarter(now_utc)

        actions: List[TradeAction] = []

        for slug, sf in all_features.items():
            # ----------------------------------------------------------
            # SELL pass: check inventory for both outcomes
            # ----------------------------------------------------------
            for outcome in ("Up", "Down"):
                inv = self.state.get_inventory(slug, outcome)
                if inv is None or inv.shares <= 0:
                    continue

                tf = sf.up if outcome == "Up" else sf.down
                if tf is None:
                    continue

                sell_ok, sell_reason = risk_allows_sell(slug, outcome)
                if not sell_ok:
                    continue

                sell_action = _exit_actions(self.cfg, inv, tf, sell_weight)
                if sell_action:
                    self.log.decision(
                        action="SELL", reason=sell_action.reason,
                        slug=slug, outcome=outcome,
                        edge_vs_cost=round(tf.best_bid - inv.avg_cost, 4),
                        shares=sell_action.size_shares,
                        price=sell_action.price,
                        sell_weight=sell_weight,
                        sec_from_q=sec_from_q,
                    )
                    actions.append(sell_action)

            # ----------------------------------------------------------
            # BUY pass: pick direction, check gates, size & price
            # ----------------------------------------------------------
            # Only consider buying with meaningful buy weight
            if buy_weight < 0.1:
                continue

            direction = _pick_buy_direction(sf, self.cfg)
            if direction is None:
                self.log.decision(
                    action="SKIP", reason="no_direction",
                    slug=slug, sec_from_q=sec_from_q,
                )
                continue

            tf = sf.up if direction == "Up" else sf.down
            if tf is None:
                continue

            # Compute desired USD size
            desired_usd = _entry_size_usd(
                self.cfg, buy_weight, tf.spread, sf.asset,
            )

            # Risk gate
            buy_ok, buy_reason = risk_allows_buy(slug, direction, desired_usd)
            cash_ok = buy_ok  # risk check includes cash
            exposure_ok = buy_ok

            # Entry gate
            allowed, gate_reason = _entry_gated(
                tf, self.cfg, exposure_ok, cash_ok,
            )
            if not allowed:
                self.log.decision(
                    action="SKIP", reason=gate_reason or buy_reason,
                    slug=slug, outcome=direction,
                    spread=tf.spread, spread_pctl=tf.spread_pctl_60s,
                    buy_weight=buy_weight, sec_from_q=sec_from_q,
                )
                continue

            # Price
            price = _entry_price(tf, self.cfg)
            shares = _usd_to_shares(desired_usd, price)
            if shares < 1:
                continue

            actual_usd = shares * price
            reason = (f"BUY(w={buy_weight:.2f},spd={tf.spread:.4f},"
                      f"pctl={tf.spread_pctl_60s:.2f},ret30={tf.ret_30s or 0:.6f})")

            self.log.decision(
                action="BUY", reason=reason,
                slug=slug, outcome=direction,
                price=price, shares=shares, usd=actual_usd,
                spread=tf.spread, spread_pctl=tf.spread_pctl_60s,
                ret_30s=tf.ret_30s, buy_weight=buy_weight,
                sec_from_q=sec_from_q,
            )

            actions.append(TradeAction(
                action="BUY", slug=slug, outcome=direction,
                token_id=tf.token_id, price=price,
                size_shares=shares, size_usd=actual_usd,
                reason=reason, urgency=buy_weight,
            ))

        return actions
