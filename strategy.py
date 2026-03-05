"""
strategy.py – F247-style cadence scalper strategy.

Core logic:
  - 15-minute cadence scheduler (BUY early, SELL at ~minute 5)
  - Direction selection via Binance 30s momentum
  - Entry gating: spread percentile, risk checks, entry quality gates
  - Entry pricing: bid-only by default, cross only on strong momentum
  - Entry sizing: discrete USD ladder
  - Exit logic: cost-basis TP/SL shaving
  - Both-sides inventory (no forced neutralization)
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from config import Config
from features import SlugFeatures, TokenFeatures
from logger import Logger
from state import InventoryEntry, StateManager

# ---------------------------------------------------------------------------
# Ladder level descriptor (internal)
# ---------------------------------------------------------------------------
@dataclass
class LadderLevel:
    """One level of a BUY inventory ladder."""
    price: float
    usd: float
    shares: int

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
# 9.1a  Inventory Ladder Builder
# ═══════════════════════════════════════════════════════════════════════════
def _clamp_to_tick(price: float) -> float:
    """Round price to nearest cent (Polymarket tick size)."""
    return round(price, 2)


def build_buy_ladder(
    cfg: Config,
    tf: TokenFeatures,
    desired_usd: float,
    entry_price: float,
) -> List[LadderLevel] | None:
    """Build a 2-3 level passive buy ladder around the bid.

    Returns None if laddering is disabled or spread gates fail,
    meaning the caller should fall back to a single order.
    """
    if cfg.LADDER_LEVELS <= 0:
        return None

    spread_cents = tf.spread * 100
    if spread_cents < cfg.LADDER_ONLY_IF_SPREAD_CENTS_GTE:
        return None
    if tf.spread_pctl_60s < cfg.LADDER_ONLY_IF_SPREAD_PCTL_GTE:
        return None

    # Base price: ensure passive (at or below best_bid)
    base_price = min(entry_price, tf.best_bid)

    # Floor: don't bid more than LADDER_LEVEL_CAP_BPS_FROM_MID below mid
    floor_price = tf.mid - cfg.LADDER_LEVEL_CAP_BPS_FROM_MID * tf.mid / 10_000
    floor_price = max(cfg.PRICE_MIN, floor_price)

    step = cfg.LADDER_STEP_CENTS / 100.0  # convert cents to dollars

    levels: List[LadderLevel] = []
    for i in range(cfg.LADDER_LEVELS):
        if i >= len(cfg.LADDER_SPLIT):
            break

        raw_price = base_price - i * step
        price = _clamp_to_tick(raw_price)

        # Clamp: not below floor, not below PRICE_MIN, not above best_bid
        price = max(price, cfg.PRICE_MIN)
        price = max(price, _clamp_to_tick(floor_price))
        price = min(price, tf.best_bid)

        level_usd = desired_usd * cfg.LADDER_SPLIT[i]
        shares = int(level_usd / price) if price > 0 else 0

        if shares < 1:
            continue

        levels.append(LadderLevel(price=price, usd=round(level_usd, 4), shares=shares))

    if not levels:
        return None

    return levels


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
                 exposure_ok: bool, cash_ok: bool,
                 direction: str = "",
                 sec_from_q: int = 0,
                 inv: InventoryEntry | None = None,
                 entry_price: float = 0.0) -> tuple[bool, str, dict]:
    """Check if entry is allowed.  Returns (allowed, reason_if_blocked, diagnostics)."""
    diag: dict = {}

    if not tf.has_book:
        return False, "no_book", diag

    bin_ret_30s_raw = tf.ret_30s or 0
    bin_ret_30s = abs(bin_ret_30s_raw)

    # Volatility filter: skip entry when Binance isn't moving
    if bin_ret_30s < cfg.VOL_MIN_RET_30S:
        return False, f"low_vol(bin_ret_30s={bin_ret_30s:.6f}<{cfg.VOL_MIN_RET_30S})", diag
    if tf.spread_pctl_60s < cfg.SPREAD_PCTL_MIN:
        return False, f"spread_pctl({tf.spread_pctl_60s:.2f}<{cfg.SPREAD_PCTL_MIN})", diag
    if tf.spread > cfg.SPREAD_MAX_SANE:
        return False, f"spread_too_wide({tf.spread:.4f}>{cfg.SPREAD_MAX_SANE})", diag
    if tf.spread <= 0:
        return False, "zero_spread", diag
    if not exposure_ok:
        return False, "exposure_cap", diag
    if not cash_ok:
        return False, "cash_reserve", diag

    # -- Gate 3: Minimum spread (cents) --
    spread_cents = tf.spread * 100
    if spread_cents < cfg.ENTRY_MIN_SPREAD_CENTS:
        return False, f"spread_gate(spread_cents={spread_cents:.1f}<{cfg.ENTRY_MIN_SPREAD_CENTS})", diag

    # -- Gate 4: Momentum direction must match trade direction --
    if direction and bin_ret_30s >= cfg.ENTRY_MIN_RET_30S:
        if direction == "Up" and bin_ret_30s_raw < 0:
            return False, f"momentum_gate(dir=Up,ret30={bin_ret_30s_raw:.6f})", diag
        if direction == "Down" and bin_ret_30s_raw > 0:
            return False, f"momentum_gate(dir=Down,ret30={bin_ret_30s_raw:.6f})", diag

    # -- Gate 5: Prevent buying late in 15-min window --
    seconds_to_resolution = 900 - sec_from_q
    if seconds_to_resolution < (900 - cfg.ENTRY_LATE_CUTOFF_SEC):
        return False, f"late_entry_gate(sec_to_res={seconds_to_resolution})", diag

    # -- Gate 1: Minimum edge before buying --
    if entry_price > 0:
        # Expected exit at best_bid
        edge_vs_cost = tf.best_bid - entry_price
        offset_from_bid = entry_price - tf.best_bid
        diag = {
            "entry_edge": round(edge_vs_cost, 4),
            "entry_spread": round(spread_cents, 1),
            "entry_offset_from_bid": round(offset_from_bid, 4),
            "entry_momentum": round(bin_ret_30s_raw, 6),
            "entry_seconds_to_resolution": seconds_to_resolution,
        }
        if edge_vs_cost < cfg.ENTRY_MIN_EDGE_CENTS:
            return False, f"edge_gate(edge={edge_vs_cost:.4f}<{cfg.ENTRY_MIN_EDGE_CENTS})", diag

        # -- Gate 2: Only buy near the bid --
        if offset_from_bid > cfg.ENTRY_MAX_OFFSET_FROM_BID:
            return False, f"bad_price(offset={offset_from_bid:.4f}>{cfg.ENTRY_MAX_OFFSET_FROM_BID})", diag

        # -- Gate 6: Do not average up --
        if inv is not None and inv.shares > 0 and inv.avg_cost > 0:
            if entry_price > inv.avg_cost + cfg.ENTRY_AVG_UP_TOLERANCE:
                return False, f"bad_average(price={entry_price:.4f}>avg_cost={inv.avg_cost:.4f}+{cfg.ENTRY_AVG_UP_TOLERANCE})", diag

    return True, "", diag


# ═══════════════════════════════════════════════════════════════════════════
# 9.4  Entry Pricing (Aggression)
# ═══════════════════════════════════════════════════════════════════════════
def _entry_price(tf: TokenFeatures, cfg: Config) -> float:
    """Determine limit buy price based on spread.

    Default: buy near the bid.  Only cross when strong momentum.
    BUY price = min(best_bid + 0.005, mid - 0.005)
    """
    bin_ret_30s = abs(tf.ret_30s or 0)
    spread_cents = round(tf.spread * 100)  # integer cents

    if spread_cents <= 1:
        # Only cross if strong momentum AND random check passes
        if bin_ret_30s > cfg.CROSS_MIN_RET_30S and random.random() < cfg.CROSS_PROB_1C:
            return tf.best_ask  # cross (taker)
        # Otherwise passive at bid
        return tf.best_bid
    else:
        # Wider spread: buy near the bid
        # Target: min(best_bid + 0.005, mid - 0.005) to stay near bid
        target = min(tf.best_bid + 0.005, tf.mid - 0.005)
        target = round(target, 2)
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
        # Entry quality tracking (reset hourly)
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
                        slug=slug, outcome=outcome, asset=sf.asset,
                        edge_vs_cost=round(tf.best_bid - inv.avg_cost, 4),
                        shares=sell_action.size_shares,
                        price=sell_action.price,
                        spread_pctl_60s=round(tf.spread_pctl_60s, 4),
                        sell_weight=sell_weight,
                        sec_from_q=sec_from_q,
                        inventory_shares_before=round(inv.shares, 2),
                        avg_cost_before=round(inv.avg_cost, 4),
                        total_exposure_usd=round(self.state.total_exposure_usd(), 2),
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
                    slug=slug, asset=sf.asset, sec_from_q=sec_from_q,
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

            # Compute price first so entry gates can evaluate edge
            price = _entry_price(tf, self.cfg)

            # Get existing inventory for avg-up check
            inv = self.state.get_inventory(slug, direction)

            # Entry gate (with all quality gates)
            allowed, gate_reason, diag = _entry_gated(
                tf, self.cfg, exposure_ok, cash_ok,
                direction=direction,
                sec_from_q=sec_from_q,
                inv=inv,
                entry_price=price,
            )
            if not allowed:
                reason_code = gate_reason.split("(")[0] if gate_reason else buy_reason
                skips = self._entry_quality["skips_by_gate"]
                skips[reason_code] = skips.get(reason_code, 0) + 1
                self.log.decision(
                    action="SKIP", reason=gate_reason or buy_reason,
                    slug=slug, outcome=direction, asset=sf.asset,
                    spread=tf.spread, spread_pctl_60s=round(tf.spread_pctl_60s, 4),
                    buy_weight=buy_weight, sec_from_q=sec_from_q,
                    **diag,
                )
                continue

            # -- Microstructure fields for ORDER_INTENT --
            ret_120s_val = tf.ret_120s
            ret_30s_val = tf.ret_30s or 0
            ret_accel = None
            book_imbalance = None
            if ret_120s_val is not None:
                ret_accel = round(ret_30s_val - ret_120s_val, 8)
            if tf.bid_size > 0 and tf.ask_size > 0:
                book_imbalance = round(
                    tf.bid_size / (tf.bid_size + tf.ask_size), 4
                )

            micro = {}
            if ret_120s_val is not None:
                micro["ret_120s"] = round(ret_120s_val, 8)
            if ret_accel is not None:
                micro["ret_accel"] = ret_accel
            if book_imbalance is not None:
                micro["book_imbalance"] = book_imbalance

            # -- Try inventory laddering --
            ladder = build_buy_ladder(self.cfg, tf, desired_usd, price)

            if ladder is not None:
                # Emit LADDER_INTENT
                ladder_desc = [
                    {"price": lv.price, "usd": lv.usd, "shares": lv.shares}
                    for lv in ladder
                ]
                reason = (f"LADDER_BUY(w={buy_weight:.2f},spd={tf.spread:.4f},"
                          f"pctl={tf.spread_pctl_60s:.2f},ret30={ret_30s_val:.6f})")

                self.log.log(
                    "LADDER_INTENT",
                    slug=slug, outcome=direction, asset=sf.asset,
                    levels=ladder_desc,
                    reason=reason,
                    spread_cents=round(tf.spread * 100, 1),
                    spread_pctl=round(tf.spread_pctl_60s, 3),
                    desired_usd=round(desired_usd, 2),
                    **micro,
                )

                # Emit ORDER_INTENT for each level
                for lvl in ladder:
                    self.log.log(
                        "ORDER_INTENT",
                        slug=slug, outcome=direction, asset=sf.asset,
                        side="BUY", price=lvl.price,
                        shares=lvl.shares, usd=lvl.usd,
                        buy_weight=buy_weight, sec_from_q=sec_from_q,
                        spread=tf.spread,
                        spread_pctl=tf.spread_pctl_60s,
                        ret_30s=ret_30s_val,
                        **micro,
                        **diag,
                    )

                # Track entry quality (use level0 price)
                self._entry_quality["buy_prices"].append(ladder[0].price)
                if diag.get("entry_edge") is not None:
                    self._entry_quality["edges_at_entry"].append(diag["entry_edge"])

                # Produce one TradeAction per ladder level
                for lvl in ladder:
                    actions.append(TradeAction(
                        action="BUY", slug=slug, outcome=direction,
                        token_id=tf.token_id, price=lvl.price,
                        size_shares=lvl.shares, size_usd=lvl.usd,
                        reason=reason, urgency=buy_weight,
                    ))
            else:
                # -- Single order fallback (no ladder) --
                shares = _usd_to_shares(desired_usd, price)
                if shares < 1:
                    continue

                actual_usd = shares * price

                # Track entry quality
                self._entry_quality["buy_prices"].append(price)
                if diag.get("entry_edge") is not None:
                    self._entry_quality["edges_at_entry"].append(diag["entry_edge"])

                reason = (f"BUY(w={buy_weight:.2f},spd={tf.spread:.4f},"
                          f"pctl={tf.spread_pctl_60s:.2f},ret30={ret_30s_val:.6f})")

                # Emit ORDER_INTENT with microstructure
                self.log.log(
                    "ORDER_INTENT",
                    slug=slug, outcome=direction, asset=sf.asset,
                    side="BUY", price=price,
                    shares=shares, usd=actual_usd,
                    buy_weight=buy_weight, sec_from_q=sec_from_q,
                    spread=tf.spread,
                    spread_pctl=tf.spread_pctl_60s,
                    ret_30s=ret_30s_val,
                    **micro,
                    **diag,
                )

                inv_before = self.state.get_inventory(slug, direction)
                self.log.decision(
                    action="BUY", reason=reason,
                    slug=slug, outcome=direction, asset=sf.asset,
                    price=price, shares=shares, usd=actual_usd,
                    spread=tf.spread, spread_pctl_60s=round(tf.spread_pctl_60s, 4),
                    ret_30s=tf.ret_30s, buy_weight=buy_weight,
                    sec_from_q=sec_from_q,
                    **diag,
                )

                actions.append(TradeAction(
                    action="BUY", slug=slug, outcome=direction,
                    token_id=tf.token_id, price=price,
                    size_shares=shares, size_usd=actual_usd,
                    reason=reason, urgency=buy_weight,
                ))

        return actions
