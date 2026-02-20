"""Position reconciliation engine — verifies wallet positions against internal state.

Fetches actual token balances from the CLOB API, compares against the bot's
internal position tracking, and corrects any mismatches.  Runs:
  - At startup
  - Every POSITION_RECONCILE_INTERVAL_SEC (default 60s)

Key design decisions:
  - Quantities are ALWAYS stored as float with full precision (no int/floor/ceil).
  - Internal state is NEVER trusted over wallet data — EXCEPT during the fill
    cooldown window (CLOB balances lag after a fill).
  - Realized PnL is preserved separately and never overwritten by reconciliation.
  - A CRITICAL desync (bot thinks flat but wallet has shares) blocks new entries
    until the next successful reconciliation resolves it.
  - The reconciler NEVER reduces a position that was traded within the last
    FILL_COOLDOWN_SEC seconds (to avoid wiping fills before balances settle).
"""
from __future__ import annotations

import time
from typing import Callable, Dict, List, Optional, Set, Tuple

from src.config.settings import MIN_QTY

# How long after a fill before we trust wallet balances for that position.
# CLOB get_balances() can lag 15-30s after a fill settles on-chain.
FILL_COOLDOWN_SEC = 30.0


class PositionReconciler:
    """Full position reconciliation against CLOB wallet balances.

    Wire into bot:
      - reconcile_positions(market_states, token_map, ...) → runs the full cycle
      - is_desynced() → True if a critical desync was detected (flat vs shares)
      - clear_desync() → called after state is corrected
      - last_reconcile_ts → epoch timestamp of last successful reconciliation
    """

    def __init__(
        self,
        get_balances_fn: Callable[[], List[dict]],
        get_open_orders_fn: Callable[[], List[dict]],
        write_jsonl_fn: Callable[[dict], None],
    ):
        self._get_balances = get_balances_fn
        self._get_open_orders = get_open_orders_fn
        self._write_jsonl = write_jsonl_fn

        # State
        self.last_reconcile_ts: float = 0.0
        self._desynced: bool = False
        self._desync_details: List[dict] = []

        # Counters
        self.reconcile_count: int = 0
        self.corrections_total: int = 0
        self.desync_events_total: int = 0
        self.cooldown_skips_total: int = 0

    # ── Public API ─────────────────────────────────────────────────────

    def is_desynced(self) -> bool:
        """True if a critical state desync was detected (bot flat, wallet has shares)."""
        return self._desynced

    def desync_details(self) -> List[dict]:
        """Return details of the last critical desync."""
        return list(self._desync_details)

    def clear_desync(self) -> None:
        """Mark desync as resolved after successful reconciliation."""
        if self._desynced:
            self._desynced = False
            self._desync_details = []
            self._write_jsonl({
                "event_type": "STATE_DESYNC_RESOLVED",
                "ts_ms": int(time.time() * 1000),
            })
            print("  [RECONCILE] STATE_DESYNC resolved — new trades allowed")

    def reconcile_positions(
        self,
        market_states: dict,
        token_to_slug_outcome: Dict[str, Tuple[str, str]],
        dscalp_positions: Optional[dict] = None,
        mid_price_lookup: Optional[Dict[Tuple[str, str], float]] = None,
        last_fill_ts: Optional[Dict[str, float]] = None,
    ) -> Dict[str, List[dict]]:
        """Run full position reconciliation cycle.

        Args:
            market_states: {slug: MarketState} — the bot's internal state
            token_to_slug_outcome: {token_id: (slug, outcome)} mapping
            dscalp_positions: optional {slug: {...}} directional scalp tracker
            mid_price_lookup: optional {(slug, outcome): mid_price} for vwap
                estimation on fresh-start positions with no prior vwap
            last_fill_ts: optional {slug: epoch_ts} — last fill time per slug.
                Positions traded within FILL_COOLDOWN_SEC are protected from
                downward corrections (CLOB balances lag after fills).

        Returns:
            dict with keys:
              - "corrections": list of {slug, outcome, old_qty, new_qty, diff}
              - "wallet_positions": {(slug, outcome): qty} actual balances
              - "open_orders": list of open order dicts
              - "desync_detected": bool
        """
        now = time.time()
        now_ms = int(now * 1000)

        self._write_jsonl({
            "event_type": "RECONCILE_START",
            "reconcile_count": self.reconcile_count,
            "ts_ms": now_ms,
        })

        result = {
            "corrections": [],
            "wallet_positions": {},
            "open_orders": [],
            "desync_detected": False,
        }

        # ── 1. Fetch actual wallet positions ──
        wallet_positions = self._fetch_wallet_positions(token_to_slug_outcome)
        if wallet_positions is None:
            # Fetch failed — skip reconciliation, don't clear desync
            self._write_jsonl({
                "event_type": "RECONCILE_SKIP_FETCH_FAILED",
                "ts_ms": now_ms,
            })
            return result
        result["wallet_positions"] = wallet_positions

        # ── 2. Fetch open orders ──
        try:
            open_orders = self._get_open_orders()
            result["open_orders"] = open_orders if isinstance(open_orders, list) else []
        except Exception as e:
            self._write_jsonl({
                "event_type": "RECONCILE_OPEN_ORDERS_ERROR",
                "err": str(e)[:200],
                "ts_ms": now_ms,
            })
            result["open_orders"] = []

        # ── Build fill-cooldown set ──
        # Slugs with recent fills — don't trust wallet for downward corrections
        if last_fill_ts is None:
            last_fill_ts = {}
        recently_traded: Set[str] = set()
        for slug, ts in last_fill_ts.items():
            if now - ts < FILL_COOLDOWN_SEC:
                recently_traded.add(slug)

        # ── TOTAL WIPEOUT GUARD ──
        # If we have significant internal positions but the wallet shows NOTHING
        # for any of them, this is almost certainly an API glitch — skip reconciliation.
        internal_positions_with_qty = []
        for slug, st in market_states.items():
            for outcome in ("Up", "Down"):
                pos = st.positions.get(outcome)
                if pos and pos.qty >= MIN_QTY:
                    internal_positions_with_qty.append((slug, outcome, pos.qty))

        if internal_positions_with_qty:
            # Check if wallet has ZERO for ALL our internal positions
            all_wallet_zero = all(
                wallet_positions.get((slug, outcome), 0.0) < MIN_QTY
                for slug, outcome, _ in internal_positions_with_qty
            )
            if all_wallet_zero:
                total_internal_qty = sum(q for _, _, q in internal_positions_with_qty)
                self._write_jsonl({
                    "event_type": "RECONCILE_TOTAL_WIPEOUT_BLOCKED",
                    "internal_positions": len(internal_positions_with_qty),
                    "total_internal_qty": total_internal_qty,
                    "wallet_has_zero": True,
                    "ts_ms": now_ms,
                })
                print(f"  [RECONCILE] *** TOTAL WIPEOUT BLOCKED: wallet shows 0 for all "
                      f"{len(internal_positions_with_qty)} positions "
                      f"(total {total_internal_qty:.2f} shares) — skipping reconciliation ***")
                print(f"  [RECONCILE]     This is likely an API glitch. Will retry next cycle.")
                return result

        # ── 3. Compare wallet vs internal state ──
        corrections = []
        critical_desyncs = []
        cooldown_skips = []

        # Collect all (slug, outcome) pairs from both wallet and internal state
        all_keys: Set[Tuple[str, str]] = set(wallet_positions.keys())
        for slug, st in market_states.items():
            for outcome in ("Up", "Down"):
                pos = st.positions.get(outcome)
                if pos and pos.qty >= MIN_QTY:
                    all_keys.add((slug, outcome))

        for slug, outcome in all_keys:
            # Internal qty
            internal_qty = 0.0
            st = market_states.get(slug)
            pos = None
            if st:
                pos = st.positions.get(outcome)
                if pos:
                    internal_qty = pos.qty

            # Wallet qty (exact float — no rounding)
            wallet_qty = wallet_positions.get((slug, outcome), 0.0)

            diff = wallet_qty - internal_qty

            # Skip negligible differences (< MIN_QTY tolerance)
            if abs(diff) < MIN_QTY:
                continue

            # ── FILL COOLDOWN GUARD ──
            # If we recently traded this slug AND the wallet wants to REDUCE
            # our position, skip it. CLOB balances lag after fills — trusting
            # a stale wallet=0 would wipe out a real fill.
            if slug in recently_traded and diff < 0:
                cooldown_skips.append({
                    "slug": slug,
                    "outcome": outcome,
                    "internal_qty": internal_qty,
                    "wallet_qty": wallet_qty,
                    "diff": diff,
                    "cooldown_remaining": FILL_COOLDOWN_SEC - (now - last_fill_ts.get(slug, 0)),
                })
                continue

            # ── Mismatch detected ──
            correction = {
                "slug": slug,
                "outcome": outcome,
                "old_qty": internal_qty,
                "new_qty": wallet_qty,
                "diff": diff,
            }
            corrections.append(correction)

            # Check for critical desync: bot thinks flat, wallet has shares
            bot_thinks_flat = internal_qty < MIN_QTY
            wallet_has_shares = wallet_qty >= MIN_QTY

            if bot_thinks_flat and wallet_has_shares:
                critical_desyncs.append(correction)

            # ── 4. Overwrite internal state with wallet data ──
            # Look up mid price for vwap estimation if available
            est_price = None
            if mid_price_lookup:
                est_price = mid_price_lookup.get((slug, outcome))

            if st and pos:
                self._apply_correction(pos, wallet_qty, est_price)
            elif st and wallet_qty >= MIN_QTY:
                new_pos = st.positions.get(outcome)
                if new_pos:
                    self._apply_correction(new_pos, wallet_qty, est_price)

        # ── 5. Handle positions that exist in wallet but not in market_states ──
        for (slug, outcome), qty in wallet_positions.items():
            if slug not in market_states and qty >= MIN_QTY:
                self._write_jsonl({
                    "event_type": "RECONCILE_UNKNOWN_WALLET_POSITION",
                    "slug": slug,
                    "outcome": outcome,
                    "wallet_qty": qty,
                    "ts_ms": now_ms,
                })

        # ── 6. Log cooldown skips ──
        if cooldown_skips:
            self.cooldown_skips_total += len(cooldown_skips)
            self._write_jsonl({
                "event_type": "RECONCILE_COOLDOWN_SKIPS",
                "count": len(cooldown_skips),
                "skips": [
                    {
                        "slug": s["slug"],
                        "outcome": s["outcome"],
                        "internal_qty": s["internal_qty"],
                        "wallet_qty": s["wallet_qty"],
                        "cooldown_remaining_sec": round(s["cooldown_remaining"], 1),
                    }
                    for s in cooldown_skips
                ],
                "ts_ms": now_ms,
            })
            for s in cooldown_skips:
                print(
                    f"  [RECONCILE] COOLDOWN SKIP: {s['slug']} {s['outcome']}: "
                    f"internal={s['internal_qty']:.2f} wallet={s['wallet_qty']:.2f} "
                    f"(recently traded, {s['cooldown_remaining']:.0f}s remaining)"
                )

        # ── 7. Log corrections ──
        if corrections:
            self.corrections_total += len(corrections)
            self._write_jsonl({
                "event_type": "RECONCILE_CORRECTIONS",
                "count": len(corrections),
                "corrections": [
                    {
                        "slug": c["slug"],
                        "outcome": c["outcome"],
                        "old_qty": c["old_qty"],
                        "new_qty": c["new_qty"],
                        "diff": c["diff"],
                    }
                    for c in corrections
                ],
                "ts_ms": now_ms,
            })
            for c in corrections:
                print(
                    f"  [RECONCILE] WARNING: {c['slug']} {c['outcome']}: "
                    f"internal={c['old_qty']:.6f} → wallet={c['new_qty']:.6f} "
                    f"(diff={c['diff']:+.6f})"
                )

        # Log wallet summary on first reconciliation (useful at startup)
        active_wallet = {
            f"{s}/{o}": q for (s, o), q in wallet_positions.items() if q >= MIN_QTY
        }
        if active_wallet and self.reconcile_count == 0:
            self._write_jsonl({
                "event_type": "RECONCILE_WALLET_SUMMARY",
                "positions": active_wallet,
                "total_positions": len(active_wallet),
                "ts_ms": now_ms,
            })
            print(f"  [RECONCILE] Wallet has {len(active_wallet)} active position(s):")
            for key, qty in sorted(active_wallet.items()):
                print(f"    {key}: {qty:.6f} shares")

        # ── 8. Handle critical desyncs ──
        if critical_desyncs:
            self._desynced = True
            self._desync_details = critical_desyncs
            self.desync_events_total += 1
            result["desync_detected"] = True

            self._write_jsonl({
                "event_type": "STATE_DESYNC_DETECTED",
                "level": "CRITICAL",
                "message": "Bot believed flat but wallet shows shares",
                "desyncs": [
                    {
                        "slug": d["slug"],
                        "outcome": d["outcome"],
                        "internal_qty": d["old_qty"],
                        "wallet_qty": d["new_qty"],
                    }
                    for d in critical_desyncs
                ],
                "ts_ms": now_ms,
            })
            for d in critical_desyncs:
                print(
                    f"  [RECONCILE] CRITICAL STATE_DESYNC_DETECTED: "
                    f"{d['slug']} {d['outcome']}: "
                    f"bot_qty={d['old_qty']:.6f} wallet_qty={d['new_qty']:.6f}"
                )
        elif self._desynced:
            # Previous desync resolved — clear it
            self.clear_desync()

        # ── 9. Sync dscalp tracker if provided ──
        if dscalp_positions is not None and corrections:
            self._sync_dscalp_tracker(
                dscalp_positions, market_states, corrections
            )

        result["corrections"] = corrections

        # ── 10. Done ──
        self.reconcile_count += 1
        self.last_reconcile_ts = now

        self._write_jsonl({
            "event_type": "RECONCILE_DONE",
            "reconcile_count": self.reconcile_count,
            "corrections_count": len(corrections),
            "cooldown_skips": len(cooldown_skips),
            "critical_desyncs": len(critical_desyncs),
            "wallet_positions_count": len(
                [(s, o) for (s, o), q in wallet_positions.items() if q >= MIN_QTY]
            ),
            "recently_traded_slugs": sorted(recently_traded),
            "ts_ms": now_ms,
        })

        return result

    # ── Internal helpers ───────────────────────────────────────────────

    def _fetch_wallet_positions(
        self,
        token_to_slug_outcome: Dict[str, Tuple[str, str]],
    ) -> Optional[Dict[Tuple[str, str], float]]:
        """Fetch token balances from CLOB and normalize to {(slug, outcome): qty}.

        Returns None if the fetch fails entirely OR returns suspicious empty data.
        Quantities are stored as exact floats — no rounding, no int cast.
        """
        try:
            balances = self._get_balances()
        except Exception as e:
            self._write_jsonl({
                "event_type": "RECONCILE_BALANCE_FETCH_ERROR",
                "err": str(e)[:200],
                "ts_ms": int(time.time() * 1000),
            })
            return None

        if not balances:
            # API returned empty/None — treat as fetch failure, NOT "wallet is empty".
            # Returning {} would tell the reconciler every position is 0, which is
            # catastrophically wrong if we actually hold positions.
            self._write_jsonl({
                "event_type": "RECONCILE_EMPTY_BALANCES",
                "ts_ms": int(time.time() * 1000),
            })
            print("  [RECONCILE] WARN: get_balances() returned empty — skipping reconciliation")
            return None

        result: Dict[Tuple[str, str], float] = {}
        for bal in balances:
            token_id = bal.get("token_id") or bal.get("asset_id", "")
            raw_balance = bal.get("balance", 0)
            if raw_balance is None:
                raw_balance = 0
            qty = float(raw_balance)

            if qty <= 0:
                continue

            if token_id in token_to_slug_outcome:
                slug, outcome = token_to_slug_outcome[token_id]
                result[(slug, outcome)] = result.get((slug, outcome), 0.0) + qty

        return result

    def _apply_correction(
        self, pos, wallet_qty: float, est_mid_price: Optional[float] = None
    ) -> None:
        """Overwrite position qty to match wallet.  Preserves vwap for cost basis
        estimation.  Does NOT touch realized PnL.

        If pos.vwap is 0 (fresh start, no prior trades), uses est_mid_price
        from the current orderbook as a reasonable cost-basis estimate.
        """
        if wallet_qty < MIN_QTY:
            pos.qty = 0.0
            pos.cost_usdc = 0.0
            pos.opened_at = None
            pos.last_trade_ts = None
            pos.position_id = None
            pos.trade_id = None
            pos.entry_decision_id = None
            pos.parent_order_id = None
            pos.tp1_done = False
            pos.tp2_done = False
            pos.tp3_done = False
            pos.fast_tp_done = False
            pos.scalp_mode = False
            pos.scalp_open_ts = None
        else:
            pos.qty = wallet_qty
            if pos.vwap > 0:
                pos.cost_usdc = pos.vwap * wallet_qty
            elif est_mid_price and est_mid_price > 0:
                pos.vwap = est_mid_price
                pos.cost_usdc = est_mid_price * wallet_qty
                self._write_jsonl({
                    "event_type": "RECONCILE_VWAP_ESTIMATED",
                    "vwap_source": "mid_price",
                    "estimated_vwap": est_mid_price,
                    "qty": wallet_qty,
                    "ts_ms": int(time.time() * 1000),
                })

    def _sync_dscalp_tracker(
        self,
        dscalp_positions: dict,
        market_states: dict,
        corrections: List[dict],
    ) -> None:
        """Sync the directional scalp tracker with corrected positions."""
        for c in corrections:
            slug = c["slug"]
            new_qty = c["new_qty"]

            if slug not in dscalp_positions:
                continue

            dpos = dscalp_positions[slug]
            if dpos.get("outcome") != c["outcome"]:
                continue

            if new_qty < MIN_QTY:
                dscalp_positions.pop(slug, None)
                self._write_jsonl({
                    "event_type": "RECONCILE_DSCALP_REMOVED",
                    "slug": slug,
                    "outcome": c["outcome"],
                    "old_qty": c["old_qty"],
                    "ts_ms": int(time.time() * 1000),
                })
            else:
                dpos["qty"] = new_qty

    def reset_minute_counters(self) -> None:
        """Reset per-minute diagnostic counters (called by DIAG report)."""
        pass
