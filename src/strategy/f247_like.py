"""
F247_LIKE strategy rules extracted from pm_hourly_clone_bot.py (Pass 1).

Entry thresholds, price caps, spread limits, taker gating, whipsaw filter,
persistence check, parity edge/fee/liquidity helpers.

No behavior changes -- exact same logic as the monolith.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, List, Optional, Tuple

from src.config.settings import (
    _THR_TABLE,
    BURST_SPREAD_HARD_LIMIT,
    CAP_0_5,
    CAP_5_15,
    CAP_15_30,
    CAP_30_45,
    CAP_45_60,
    CAP_BOOST_EDGE_FULL,
    CAP_BOOST_EDGE_THRESHOLD,
    CAP_BOOST_MAX,
    BLOCK_IF_VEL_OPPOSES,
    ENTRY_MIN_STABLE_SIGN_MS,
    MAKER_FEE_BPS,
    MAKER_MAX_SPREAD,
    MAX_SPREAD_FOR_PARITY_CENTS,
    MIN_TOP_LIQ_USD,
    PARITY_TAKER_ALLOWED_SPREAD_CENTS,
    PERSISTENCE_SEC,
    SPREAD_RELAXED_MAX,
    TAKER_FEE_BPS,
    TAKER_MAX_SPREAD_CENTS,
    TAKER_MIN_EDGE_EXTRA_BPS,
    TRADE_START_MIN,
    VEL_OPPOSE_THRESHOLD,
)
from src.util.time import utc_now

if TYPE_CHECKING:
    from src.bot.context import BookTop


# =============================================================================
# Entry threshold (coin-specific, time-varying)
# =============================================================================

def entry_threshold_bps(coin: str, t_min: float) -> float:
    """Return entry threshold in bps for *coin* at minute *t_min* of the hour."""
    tbl = _THR_TABLE.get(coin, {"early": 8, "mid": 10, "late": 6})
    if TRADE_START_MIN <= t_min < 15:
        thr = tbl["early"]
    elif 15 <= t_min < 45:
        thr = tbl["mid"]
    elif 45 <= t_min <= 57:
        thr = tbl["late"]
    else:
        return 10_000
    # Special rule: XRP min 30-40 reduce by 2 bps (F247 aggressive)
    if coin == "XRP" and 30 <= t_min < 40:
        thr = max(1, thr - 2)
    return thr


# =============================================================================
# Price cap (max price to BUY), piecewise by time bucket
# =============================================================================

def price_cap(t_min: float) -> float:
    if 0 <= t_min < 5:   return CAP_0_5
    if 5 <= t_min < 15:  return CAP_5_15
    if 15 <= t_min < 30: return CAP_15_30
    if 30 <= t_min < 45: return CAP_30_45
    if 45 <= t_min < 60: return CAP_45_60
    return 0.0


def dynamic_cap(t_min: float, abs_edge_bps: float) -> float:
    """Price cap with dynamic boost based on edge strength."""
    base = price_cap(t_min)
    if abs_edge_bps <= CAP_BOOST_EDGE_THRESHOLD:
        return base
    frac = min(1.0, (abs_edge_bps - CAP_BOOST_EDGE_THRESHOLD) /
               max(1.0, CAP_BOOST_EDGE_FULL - CAP_BOOST_EDGE_THRESHOLD))
    boost = frac * CAP_BOOST_MAX
    return min(0.99, base + boost)


# =============================================================================
# Spread limit
# =============================================================================

def spread_limit(t_min: float, abs_edge_bps: float, coin: str,
                 in_burst: bool = False) -> float:
    """Return max allowed spread for entry gating.

    Crossing only when spread <= 2c (IMB_MAX_SPREAD).
    Maker posting allowed up to MAKER_MAX_SPREAD (6c).
    During burst: controlled by burst engine's own maker/taker logic.
    """
    if in_burst:
        return BURST_SPREAD_HARD_LIMIT  # burst engine manages its own spread logic
    # Allow entry up to MAKER_MAX_SPREAD -- burst engine will decide maker vs taker
    thr = entry_threshold_bps(coin, t_min)
    if 45 <= t_min <= 57 or abs_edge_bps >= thr + 10:
        return SPREAD_RELAXED_MAX
    return MAKER_MAX_SPREAD


# =============================================================================
# Taker gate
# =============================================================================

def taker_gate_allows(spread_cents: float, abs_edge_bps: float,
                      thr_bps: float) -> bool:
    """Return True only if taker (crossing) is permitted.

    BOTH conditions must be true: spread <= 1c AND edge >= thr + 12.
    """
    return (spread_cents <= TAKER_MAX_SPREAD_CENTS and
            abs_edge_bps >= thr_bps + TAKER_MIN_EDGE_EXTRA_BPS)


# =============================================================================
# Whipsaw / anti-chop filter
# =============================================================================

def whipsaw_ok(delta_bps: float, vel: float,
               edge_sign_since: Optional[float]) -> Tuple[bool, str]:
    """Return (allowed, block_reason).  Blocks entry in chop conditions."""
    # 1. Sign stability: delta sign must be unchanged for >= ENTRY_MIN_STABLE_SIGN_MS
    if edge_sign_since is None:
        return False, "sign_no_history"
    elapsed_ms = (time.time() - edge_sign_since) * 1000
    if elapsed_ms < ENTRY_MIN_STABLE_SIGN_MS:
        return False, f"sign_unstable({elapsed_ms:.0f}ms<{ENTRY_MIN_STABLE_SIGN_MS}ms)"
    # 2. Velocity alignment: vel must not oppose delta_bps
    if BLOCK_IF_VEL_OPPOSES and abs(vel) >= VEL_OPPOSE_THRESHOLD:
        delta_sign = 1 if delta_bps > 0 else -1
        vel_sign = 1 if vel > 0 else -1
        if delta_sign != vel_sign:
            return False, f"vel_opposes(delta={delta_bps:+.1f},vel={vel:+.1f})"
    return True, ""


# =============================================================================
# Persistence check
# =============================================================================

def persistence_ok(signal_series: List[Tuple[str, bool]]) -> bool:
    """True if signal has been continuously True for >= PERSISTENCE_SEC.

    *signal_series* holds ``(ts_iso, signal_bool)`` for recent entries.
    """
    if not signal_series:
        return False
    now = utc_now()
    cutoff = now - timedelta(seconds=PERSISTENCE_SEC)
    # Must find that all points since cutoff are True
    for ts_iso, s in reversed(signal_series):
        t = datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if t < cutoff:
            break
        if not s:
            return False
    # Also ensure we have coverage back to cutoff
    oldest_ts = signal_series[0][0]
    try:
        oldest_t = datetime.strptime(oldest_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return False
    return oldest_t <= cutoff or len(signal_series) > 10


# =============================================================================
# Parity net edge (after fees & slippage)
# =============================================================================

def parity_net_edge_cents(raw_edge_cents: float, up_book: "BookTop",
                          dn_book: "BookTop",
                          is_buy: bool) -> Tuple[float, float, float]:
    """Compute net parity edge after estimated fees and slippage.

    Returns ``(net_edge_cents, total_fee_cents, total_slippage_cents)``.
    For BUY straddle: we cross both asks (taker) or post bids (maker).
    For SELL straddle: we cross both bids (taker) or post asks (maker).
    """
    total_fee_cents = 0.0
    total_slippage_cents = 0.0
    for book in (up_book, dn_book):
        spread_cents = book.spread * 100
        use_taker = spread_cents <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
        if use_taker:
            # Taker: fee + half-spread slippage
            fee = TAKER_FEE_BPS / 100.0  # bps -> cents (approx: 1 bps on $0.50 ~ 0.005c)
            slippage = spread_cents / 2.0
        else:
            # Maker: fee only, no slippage (we're at best price)
            fee = MAKER_FEE_BPS / 100.0
            slippage = 0.0
        total_fee_cents += fee
        total_slippage_cents += slippage
    net = raw_edge_cents - total_fee_cents - total_slippage_cents
    return net, total_fee_cents, total_slippage_cents


# =============================================================================
# Fee computation
# =============================================================================

def compute_fee_usdc(notional_usdc: float, maker_taker: str) -> float:
    """Compute fee in USDC for a given notional and maker/taker type."""
    if maker_taker == "maker":
        return notional_usdc * MAKER_FEE_BPS / 10000.0
    elif maker_taker == "taker":
        return notional_usdc * TAKER_FEE_BPS / 10000.0
    # If unknown, assume taker (conservative)
    return notional_usdc * TAKER_FEE_BPS / 10000.0


# =============================================================================
# Parity liquidity guard
# =============================================================================

def parity_liquidity_ok(up_book: "BookTop",
                        dn_book: "BookTop") -> Tuple[bool, str]:
    """Check liquidity and spread guards for parity entry.

    Returns ``(ok, block_reason)``.
    """
    for label, book in [("up", up_book), ("dn", dn_book)]:
        spread_cents = book.spread * 100
        if spread_cents > MAX_SPREAD_FOR_PARITY_CENTS:
            return False, f"{label}_spread({spread_cents:.1f}c>{MAX_SPREAD_FOR_PARITY_CENTS}c)"
        # Check top-of-book liquidity in USD
        bid_usd = book.bid_sz * book.bid if book.bid > 0 else 0.0
        ask_usd = book.ask_sz * book.ask if book.ask > 0 else 0.0
        if bid_usd < MIN_TOP_LIQ_USD:
            return False, f"{label}_bid_liq(${bid_usd:.1f}<${MIN_TOP_LIQ_USD})"
        if ask_usd < MIN_TOP_LIQ_USD:
            return False, f"{label}_ask_liq(${ask_usd:.1f}<${MIN_TOP_LIQ_USD})"
    return True, ""
