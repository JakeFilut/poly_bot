"""
strategy.py – Wallet-copy microstructure scalper strategy.

Core logic (tightened entry filters):
  - Entry when: spread_cents>=1.5, spread_pctl>=0.85, imbalance>=0.50
  - Momentum gate: |ret_30s| >= 0.0002 (2 bps minimum)
  - Directional imbalance: enabled (imbalance must agree with trade direction)
  - Book depth gate: bid+ask >= 50 shares, bid >= 20 shares
  - Direction: cascade ret_30s → ret_5s → ret_120s → orderbook imbalance
  - Passive bids only (no crossing except extreme momentum)
  - 2-level ladder: 62.5% @ best_bid, 37.5% @ best_bid - 0.01
  - Crossing only when |ret_30s|>=0.003 AND spread_pctl>=0.95
  - Late entry cutoff: 10 min into window (was 12)
  - Edge gate: 4¢ minimum (was 3¢)
  - Exit logic: cost-basis TP/SL shaving (unchanged)
"""
from __future__ import annotations

import time
import uuid
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
    ladder_id: str = ""           # unique id linking all levels of one ladder
    ladder_level_idx: int = -1    # 0-based index within the ladder (-1 = not a ladder)
    ladder_levels_total: int = 0  # total levels in this ladder (0 = not a ladder)
    # -- Wallet-copy logging fields --
    intent_timestamp: float = 0.0  # epoch when signal was detected
    spread_cents: float = 0.0
    spread_percentile: float = 0.0
    return_5s: float = 0.0
    return_30s: float = 0.0
    orderbook_imbalance: float = 0.0
    entry_style: str = ""          # "passive" or "cross"


# ═══════════════════════════════════════════════════════════════════════════
# 9.1a  Inventory Ladder Builder (2-level wallet style)
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
    """Build a 2-level passive buy ladder around the bid.

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
    """Seconds elapsed since the last 15-minute boundary in America/New_York."""
    now_et = now_utc.astimezone(_ET)
    total_sec = now_et.minute * 60 + now_et.second
    return total_sec % 900  # 900 = 15 * 60


def cadence_weight(cfg: Config, now_utc: datetime) -> tuple[float, float]:
    """Return (buy_weight, sell_weight) based on 15-min cadence in ET.

    buy_weight:  0.0 to 1.0 (how aggressively to buy)
    sell_weight: 0.0 to 1.0 (how aggressively to sell/shave)

    NOTE: For the wallet-copy strategy, buy_weight is always 1.0
    (entries are signal-driven, not cadence-driven). Cadence only
    controls sell timing.
    """
    sec = _seconds_from_quarter(now_utc)

    # BUY weight: always ready (wallet enters on signal, not cadence)
    buy_w = 1.0

    # SELL weight (keep original cadence for exits)
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

    Falls back to shorter/longer timeframes or orderbook imbalance
    when ret_30s is zero (wallet enters without momentum).
    Returns None if no valid direction (both sides unusable).
    """
    ret = sf.ret_30s
    if ret is None:
        return None

    # Use ENTRY_MIN_ABS_RET_30S as the direction-picking threshold
    threshold = cfg.ENTRY_MIN_ABS_RET_30S
    if threshold > 0 and abs(ret) < threshold:
        ret = 0  # treat sub-threshold as zero, try fallbacks below

    # Cascade: ret_30s → ret_5s → ret_120s → orderbook imbalance
    if ret == 0:
        # Try shorter timeframe
        if sf.ret_5s is not None and sf.ret_5s != 0:
            ret = sf.ret_5s
        # Try longer timeframe
        elif sf.ret_120s is not None and sf.ret_120s != 0:
            ret = sf.ret_120s
        # Fallback: use orderbook imbalance to pick direction
        else:
            up_tf = sf.up
            down_tf = sf.down
            if up_tf and up_tf.has_book and down_tf and down_tf.has_book:
                up_imb = up_tf.bid_size / max(1, up_tf.bid_size + up_tf.ask_size)
                down_imb = down_tf.bid_size / max(1, down_tf.bid_size + down_tf.ask_size)
                if up_imb > down_imb:
                    ret = 0.0001  # synthetic positive → Up
                elif down_imb > up_imb:
                    ret = -0.0001  # synthetic negative → Down
                else:
                    return None  # perfectly balanced, no signal
            elif up_tf and up_tf.has_book:
                ret = 0.0001  # only Up side available
            elif down_tf and down_tf.has_book:
                ret = -0.0001  # only Down side available
            else:
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
# 9.3  Entry Gating (wallet-copy microstructure conditions)
# ═══════════════════════════════════════════════════════════════════════════
def _compute_ob_imbalance(tf: TokenFeatures) -> float | None:
    """Compute orderbook imbalance: bid_size / (bid_size + ask_size).
    Returns None if sizes are unavailable."""
    if tf.bid_size <= 0 and tf.ask_size <= 0:
        return None
    total = tf.bid_size + tf.ask_size
    if total <= 0:
        return None
    return tf.bid_size / total


def _compute_timing_features(now_utc: datetime, sec_from_q: int) -> dict:
    """Compute timing-inside-window features for entry logging."""
    now_et = now_utc.astimezone(_ET)
    sec_from_quarter_et = sec_from_q
    sec_from_hour_et = now_et.minute * 60 + now_et.second
    seconds_to_resolution = 900 - sec_from_q
    return {
        "sec_from_quarter_et": sec_from_quarter_et,
        "sec_from_hour_et": sec_from_hour_et,
        "seconds_to_resolution": seconds_to_resolution,
    }


def _entry_gated(tf: TokenFeatures, cfg: Config,
                 exposure_ok: bool, cash_ok: bool,
                 direction: str = "",
                 sec_from_q: int = 0,
                 inv: InventoryEntry | None = None,
                 entry_price: float = 0.0) -> tuple[bool, str, dict]:
    """Check if entry is allowed.  Returns (allowed, reason_if_blocked, diagnostics).

    Entry conditions (all must be true):
      1. spread_cents >= ENTRY_MIN_SPREAD_CENTS (default 1.5)
      2. spread_percentile >= ENTRY_MIN_SPREAD_PCTL (default 0.85)
      3. abs(return_30s) >= ENTRY_MIN_ABS_RET_30S (default 0.0002)
      4. directional imbalance gate (enabled by default)
      5. book depth >= ENTRY_MIN_BOOK_DEPTH (default 50 shares)
      6. bid size >= ENTRY_MIN_BID_SIZE (default 20 shares)
      7. momentum acceleration gate (optional)
    """
    diag: dict = {}
    accepted_reasons: list = []

    if not tf.has_book:
        return False, "no_book", diag

    bin_ret_30s_raw = tf.ret_30s or 0
    bin_ret_30s = abs(bin_ret_30s_raw)
    spread_cents = tf.spread * 100

    # -- Condition 1: spread must be wide enough --
    if spread_cents < cfg.ENTRY_MIN_SPREAD_CENTS:
        return False, f"spread_gate(spread_cents={spread_cents:.1f}<{cfg.ENTRY_MIN_SPREAD_CENTS})", diag
    accepted_reasons.append("spread_cents_ok")

    # -- Condition 2: spread percentile must be high --
    entry_spread_pctl = cfg.ENTRY_MIN_SPREAD_PCTL
    if tf.spread_pctl_60s < entry_spread_pctl:
        return False, f"spread_pctl({tf.spread_pctl_60s:.2f}<{entry_spread_pctl})", diag
    accepted_reasons.append("spread_pctl_ok")

    # -- Soft bonus: preferred spread percentile --
    if tf.spread_pctl_60s >= cfg.ENTRY_PREFER_SPREAD_PCTL:
        accepted_reasons.append("spread_pctl_preferred")

    # -- Sanity: spread not insanely wide --
    if tf.spread > cfg.SPREAD_MAX_SANE:
        return False, f"spread_too_wide({tf.spread:.4f}>{cfg.SPREAD_MAX_SANE})", diag
    if tf.spread <= 0:
        return False, "zero_spread", diag

    # -- Condition 3: momentum gate --
    if cfg.ENTRY_MIN_ABS_RET_30S > 0 and bin_ret_30s < cfg.ENTRY_MIN_ABS_RET_30S:
        return False, f"low_momentum(bin_ret_30s={bin_ret_30s:.6f}<{cfg.ENTRY_MIN_ABS_RET_30S})", diag
    accepted_reasons.append("momentum_ok")

    # -- Condition 4: directional imbalance gate --
    ob_imbalance = _compute_ob_imbalance(tf)
    if cfg.ENTRY_IMBALANCE_ENABLED and ob_imbalance is not None:
        if cfg.ENTRY_IMBALANCE_DIRECTIONAL and direction:
            # Directional: buying Up requires imbalance >= threshold (bids dominant)
            #              buying Down requires imbalance <= (1 - threshold) (asks dominant)
            if direction == "Up" and ob_imbalance < cfg.ENTRY_MIN_IMBALANCE:
                return False, f"imbalance_dir(Up,imb={ob_imbalance:.3f}<{cfg.ENTRY_MIN_IMBALANCE})", diag
            if direction == "Down" and ob_imbalance > (1.0 - cfg.ENTRY_MIN_IMBALANCE):
                return False, f"imbalance_dir(Down,imb={ob_imbalance:.3f}>{1.0 - cfg.ENTRY_MIN_IMBALANCE:.3f})", diag
        else:
            # Non-directional: simple minimum
            if ob_imbalance < cfg.ENTRY_MIN_IMBALANCE:
                return False, f"ob_imbalance({ob_imbalance:.3f}<{cfg.ENTRY_MIN_IMBALANCE})", diag
        accepted_reasons.append("imbalance_ok")

    # -- Condition 5: book depth gate (reject thin books) --
    total_depth = tf.bid_size + tf.ask_size
    if total_depth < cfg.ENTRY_MIN_BOOK_DEPTH:
        return False, f"thin_book(depth={total_depth:.0f}<{cfg.ENTRY_MIN_BOOK_DEPTH})", diag
    if tf.bid_size < cfg.ENTRY_MIN_BID_SIZE:
        return False, f"thin_bids(bid_size={tf.bid_size:.0f}<{cfg.ENTRY_MIN_BID_SIZE})", diag
    accepted_reasons.append("book_depth_ok")

    # -- Condition 6: momentum acceleration gate (optional) --
    if cfg.ENTRY_MIN_RET_ACCEL > 0:
        ret_120s = tf.ret_120s or 0
        ret_accel = bin_ret_30s_raw - ret_120s
        # For Up direction, require positive acceleration; for Down, negative
        if direction == "Up" and ret_accel < cfg.ENTRY_MIN_RET_ACCEL:
            return False, f"low_accel(Up,accel={ret_accel:.6f}<{cfg.ENTRY_MIN_RET_ACCEL})", diag
        if direction == "Down" and (-ret_accel) < cfg.ENTRY_MIN_RET_ACCEL:
            return False, f"low_accel(Down,accel={-ret_accel:.6f}<{cfg.ENTRY_MIN_RET_ACCEL})", diag
        accepted_reasons.append("accel_ok")

    # -- Risk gates --
    if not exposure_ok:
        return False, "exposure_cap", diag
    if not cash_ok:
        return False, "cash_reserve", diag

    # -- Prevent buying late in 15-min window --
    seconds_to_resolution = 900 - sec_from_q
    if seconds_to_resolution < (900 - cfg.ENTRY_LATE_CUTOFF_SEC):
        return False, f"late_entry_gate(sec_to_res={seconds_to_resolution})", diag
    accepted_reasons.append("time_window_ok")

    # -- Edge and price gates (when entry_price is known) --
    if entry_price > 0:
        edge_vs_cost = tf.best_bid - entry_price
        offset_from_bid = entry_price - tf.best_bid
        diag = {
            "entry_edge": round(edge_vs_cost, 4),
            "entry_spread": round(spread_cents, 1),
            "entry_offset_from_bid": round(offset_from_bid, 4),
            "entry_momentum": round(bin_ret_30s_raw, 6),
            "entry_seconds_to_resolution": seconds_to_resolution,
            "ob_imbalance": round(ob_imbalance, 4) if ob_imbalance is not None else None,
        }
        if edge_vs_cost < cfg.ENTRY_MIN_EDGE_CENTS:
            return False, f"edge_gate(edge={edge_vs_cost:.4f}<{cfg.ENTRY_MIN_EDGE_CENTS})", diag

        # Only buy near the bid
        if offset_from_bid > cfg.ENTRY_MAX_OFFSET_FROM_BID:
            return False, f"bad_price(offset={offset_from_bid:.4f}>{cfg.ENTRY_MAX_OFFSET_FROM_BID})", diag

        # Do not average up
        if inv is not None and inv.shares > 0 and inv.avg_cost > 0:
            if entry_price > inv.avg_cost + cfg.ENTRY_AVG_UP_TOLERANCE:
                return False, f"bad_average(price={entry_price:.4f}>avg_cost={inv.avg_cost:.4f}+{cfg.ENTRY_AVG_UP_TOLERANCE})", diag

    diag["accepted_reason_codes"] = accepted_reasons
    return True, "", diag


# ═══════════════════════════════════════════════════════════════════════════
# 9.4  Entry Pricing (Wallet-copy: always passive, cross only extreme)
# ═══════════════════════════════════════════════════════════════════════════
def _entry_price(tf: TokenFeatures, cfg: Config) -> tuple[float, str]:
    """Determine limit buy price.

    Wallet-copy rules:
      - Default: passive at best_bid
      - Cross ONLY when |ret_30s| >= 0.003 AND spread_pctl >= 0.95
    Returns (price, entry_style).
    """
    bin_ret_30s = abs(tf.ret_30s or 0)

    # Cross only on extreme momentum + wide spread
    if (bin_ret_30s >= cfg.CROSS_MIN_RET_30S
            and tf.spread_pctl_60s >= cfg.CROSS_MIN_SPREAD_PCTL):
        return tf.best_ask, "cross"

    # Default: passive at best_bid
    return tf.best_bid, "passive"


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
    """Runs the wallet-copy strategy each loop iteration."""

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
        intent_ts = time.time()  # capture signal detection time
        timing_features = _compute_timing_features(now_utc, sec_from_q)

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
                    # Add wallet-copy logging fields to sell actions
                    sell_action.spread_cents = round(tf.spread * 100, 1)
                    sell_action.spread_percentile = round(tf.spread_pctl_60s, 4)
                    sell_action.return_5s = round(tf.ret_5s or 0, 8)
                    sell_action.return_30s = round(tf.ret_30s or 0, 8)
                    ob_imb = _compute_ob_imbalance(tf)
                    sell_action.orderbook_imbalance = round(ob_imb, 4) if ob_imb is not None else 0.0
                    sell_action.entry_style = "passive"
                    sell_action.intent_timestamp = intent_ts

                    self.log.decision(
                        action="SELL", reason=sell_action.reason,
                        slug=slug, outcome=outcome, asset=sf.asset,
                        edge_vs_cost=round(tf.best_bid - inv.avg_cost, 4),
                        shares=sell_action.size_shares,
                        price=sell_action.price,
                        spread_cents=sell_action.spread_cents,
                        spread_percentile=sell_action.spread_percentile,
                        return_5s=sell_action.return_5s,
                        return_30s=sell_action.return_30s,
                        orderbook_imbalance=sell_action.orderbook_imbalance,
                        entry_style=sell_action.entry_style,
                        intent_timestamp=intent_ts,
                        sell_weight=sell_weight,
                        sec_from_q=sec_from_q,
                        inventory_shares_before=round(inv.shares, 2),
                        avg_cost_before=round(inv.avg_cost, 4),
                        total_exposure_usd=round(self.state.total_exposure_usd(), 2),
                    )
                    actions.append(sell_action)

            # ----------------------------------------------------------
            # BUY pass: pick direction, check wallet-copy gates
            # ----------------------------------------------------------
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
            cash_ok = buy_ok
            exposure_ok = buy_ok

            # Compute price and entry style
            price, entry_style = _entry_price(tf, self.cfg)

            # Get existing inventory for avg-up check
            inv = self.state.get_inventory(slug, direction)

            # Entry gate (with all wallet-copy quality gates)
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

            # -- Microstructure fields for logging --
            ret_5s_val = tf.ret_5s or 0
            ret_30s_val = tf.ret_30s or 0
            ret_120s_val = tf.ret_120s
            ret_accel = None
            ob_imbalance = _compute_ob_imbalance(tf)

            if ret_120s_val is not None:
                ret_accel = round(ret_30s_val - ret_120s_val, 8)

            micro = {}
            if ret_120s_val is not None:
                micro["ret_120s"] = round(ret_120s_val, 8)
            if ret_accel is not None:
                micro["ret_accel"] = ret_accel
            if ob_imbalance is not None:
                micro["book_imbalance01"] = round(ob_imbalance, 4)
                micro["book_imbalance"] = round(
                    (tf.bid_size - tf.ask_size) / (tf.bid_size + tf.ask_size), 4
                ) if (tf.bid_size + tf.ask_size) > 0 else 0.0

            # Wallet-copy log fields
            spread_cents_val = round(tf.spread * 100, 1)
            spread_pctl_val = round(tf.spread_pctl_60s, 4)
            ob_imb_val = round(ob_imbalance, 4) if ob_imbalance is not None else 0.0

            # -- Try inventory laddering (wallet uses 2-level ladder) --
            ladder = build_buy_ladder(self.cfg, tf, desired_usd, price)

            if ladder is not None:
                # Generate unique ladder_id to link all levels
                lid = uuid.uuid4().hex[:12]
                n_levels = len(ladder)

                # Emit LADDER_INTENT with wallet-copy fields
                ladder_desc = [
                    {"price": lv.price, "usd": lv.usd, "shares": lv.shares}
                    for lv in ladder
                ]
                reason = (f"LADDER_BUY(spd={tf.spread:.4f},"
                          f"pctl={tf.spread_pctl_60s:.2f},ret30={ret_30s_val:.6f},"
                          f"imb={ob_imb_val:.3f},style={entry_style})")

                self.log.log(
                    "LADDER_INTENT",
                    slug=slug, outcome=direction, asset=sf.asset,
                    ladder_id=lid,
                    ladder_levels_total=n_levels,
                    levels=ladder_desc,
                    reason=reason,
                    spread_cents=spread_cents_val,
                    spread_percentile=spread_pctl_val,
                    return_5s=round(ret_5s_val, 8),
                    return_30s=round(ret_30s_val, 8),
                    orderbook_imbalance=ob_imb_val,
                    entry_style=entry_style,
                    intent_timestamp=intent_ts,
                    desired_usd=round(desired_usd, 2),
                    accepted_reason_codes=diag.get("accepted_reason_codes", []),
                    **timing_features,
                    **micro,
                )

                # Emit ORDER_INTENT for each level
                for i, lvl in enumerate(ladder):
                    self.log.log(
                        "ORDER_INTENT",
                        slug=slug, outcome=direction, asset=sf.asset,
                        side="BUY", price=lvl.price,
                        shares=lvl.shares, usd=lvl.usd,
                        ladder_id=lid,
                        ladder_level_idx=i,
                        ladder_levels_total=n_levels,
                        buy_weight=buy_weight, sec_from_q=sec_from_q,
                        spread_cents=spread_cents_val,
                        spread_percentile=spread_pctl_val,
                        return_5s=round(ret_5s_val, 8),
                        return_30s=round(ret_30s_val, 8),
                        orderbook_imbalance=ob_imb_val,
                        entry_style=entry_style,
                        ladder_level=i,
                        intent_timestamp=intent_ts,
                        accepted_reason_codes=diag.get("accepted_reason_codes", []),
                        **timing_features,
                        **micro,
                    )

                # Track entry quality (use level0 price)
                self._entry_quality["buy_prices"].append(ladder[0].price)
                if diag.get("entry_edge") is not None:
                    self._entry_quality["edges_at_entry"].append(diag["entry_edge"])

                # Produce one TradeAction per ladder level
                for i, lvl in enumerate(ladder):
                    actions.append(TradeAction(
                        action="BUY", slug=slug, outcome=direction,
                        token_id=tf.token_id, price=lvl.price,
                        size_shares=lvl.shares, size_usd=lvl.usd,
                        reason=reason, urgency=buy_weight,
                        ladder_id=lid,
                        ladder_level_idx=i,
                        ladder_levels_total=n_levels,
                        intent_timestamp=intent_ts,
                        spread_cents=spread_cents_val,
                        spread_percentile=spread_pctl_val,
                        return_5s=round(ret_5s_val, 8),
                        return_30s=round(ret_30s_val, 8),
                        orderbook_imbalance=ob_imb_val,
                        entry_style=entry_style,
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

                reason = (f"BUY(spd={tf.spread:.4f},"
                          f"pctl={tf.spread_pctl_60s:.2f},ret30={ret_30s_val:.6f},"
                          f"imb={ob_imb_val:.3f},style={entry_style})")

                # Emit ORDER_INTENT with wallet-copy fields
                self.log.log(
                    "ORDER_INTENT",
                    slug=slug, outcome=direction, asset=sf.asset,
                    side="BUY", price=price,
                    shares=shares, usd=actual_usd,
                    buy_weight=buy_weight, sec_from_q=sec_from_q,
                    spread_cents=spread_cents_val,
                    spread_percentile=spread_pctl_val,
                    return_5s=round(ret_5s_val, 8),
                    return_30s=round(ret_30s_val, 8),
                    orderbook_imbalance=ob_imb_val,
                    entry_style=entry_style,
                    intent_timestamp=intent_ts,
                    accepted_reason_codes=diag.get("accepted_reason_codes", []),
                    **timing_features,
                    **micro,
                )

                inv_before = self.state.get_inventory(slug, direction)
                self.log.decision(
                    action="BUY", reason=reason,
                    slug=slug, outcome=direction, asset=sf.asset,
                    price=price, shares=shares, usd=actual_usd,
                    spread_cents=spread_cents_val,
                    spread_percentile=spread_pctl_val,
                    return_5s=round(ret_5s_val, 8),
                    return_30s=round(ret_30s_val, 8),
                    orderbook_imbalance=ob_imb_val,
                    entry_style=entry_style,
                    intent_timestamp=intent_ts,
                    buy_weight=buy_weight,
                    sec_from_q=sec_from_q,
                    accepted_reason_codes=diag.get("accepted_reason_codes", []),
                    **timing_features,
                )

                actions.append(TradeAction(
                    action="BUY", slug=slug, outcome=direction,
                    token_id=tf.token_id, price=price,
                    size_shares=shares, size_usd=actual_usd,
                    reason=reason, urgency=buy_weight,
                    intent_timestamp=intent_ts,
                    spread_cents=spread_cents_val,
                    spread_percentile=spread_pctl_val,
                    return_5s=round(ret_5s_val, 8),
                    return_30s=round(ret_30s_val, 8),
                    orderbook_imbalance=ob_imb_val,
                    entry_style=entry_style,
                ))

        return actions
