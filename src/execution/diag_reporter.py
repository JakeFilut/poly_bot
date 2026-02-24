"""
DiagReporter — structured runtime diagnostic dump for DIAG_MODE.

Emits flat key=value diagnostics every DIAG_INTERVAL_SEC to console + file.
Covers: clock/window, safety, exposure, orders, positions, signals, invariants.

Output formats (all gated by DIAG_MODE=1):
  [DIAG]   — 10-second snapshot (flat key=value, per-slug sub-blocks)
  [GATE]   — entry rejection trace (multi-line, FIRST blocking gate only)
  [ORDER]  — order/fill events (multi-line)
  [SAFETY] — safe mode / desync transitions (multi-line)
  [BUG]    — exposure invariant violations

Usage from bot:
    reporter = DiagReporter(bot)
    reporter.tick()              # call from main loop — rate-limited internally
    DiagReporter.gate_trace(...)     # call at each entry rejection point
    DiagReporter.order_trace(...)    # call at order/fill events
    DiagReporter.safety_trace(...)   # call at safety state transitions
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional, Any

from src.config import settings

if TYPE_CHECKING:
    pass  # Bot type would cause circular import; we duck-type


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts_ms() -> int:
    return int(time.time() * 1000)


class DiagReporter:
    """Periodic structured diagnostic dump."""

    def __init__(self, bot: Any, write_jsonl_fn=None):
        self._bot = bot
        self._write_jsonl = write_jsonl_fn or (lambda d: None)
        self._last_dump_ts: float = 0.0
        self._diag_path = settings.DIAG_FILE
        # Ensure parent dir exists
        os.makedirs(os.path.dirname(self._diag_path) or ".", exist_ok=True)
        # Gate trace dedup: only keep last N
        self._gate_traces: List[dict] = []
        self._max_gate_traces = 50

    # ──────────────────────────────────────────────────────────────────
    # Main tick — called from bot loop
    # ──────────────────────────────────────────────────────────────────

    def tick(self) -> None:
        """Rate-limited dump: emit at most once per DIAG_INTERVAL_SEC."""
        if not settings.DIAG_MODE:
            return
        now = time.time()
        if now - self._last_dump_ts < settings.DIAG_INTERVAL_SEC:
            return
        self._last_dump_ts = now
        try:
            snapshot = self._build_snapshot()
            self._emit(snapshot)
            self._check_budget_invariant(snapshot)
        except Exception as e:
            print(f"  [DIAG] Error building snapshot: {e}")

    # ──────────────────────────────────────────────────────────────────
    # A) CLOCK / WINDOW
    # ──────────────────────────────────────────────────────────────────

    def _build_clock(self) -> dict:
        bot = self._bot
        now = datetime.now(timezone.utc)
        # Find the first market with valid hour_start_utc
        t_min = 0.0
        hour_start_utc = ""
        hour_end_utc = ""
        seconds_to_close = 9999.0
        active_slugs = []

        for slug, st in bot.market_states.items():
            if st.hour_start_utc:
                try:
                    from src.util.time import minutes_into_hour
                    hs = datetime.strptime(st.hour_start_utc, "%Y-%m-%dT%H:%M:%SZ").replace(
                        tzinfo=timezone.utc)
                    t_min = (now - hs).total_seconds() / 60.0
                    hour_start_utc = st.hour_start_utc
                    he = hs.replace(minute=0, second=0) + __import__("datetime").timedelta(hours=1)
                    hour_end_utc = _iso(he)
                    seconds_to_close = (he - now).total_seconds()
                except Exception:
                    pass

            is_active = True
            if hasattr(bot, '_truth') and hasattr(bot._truth, 'is_active_window_token'):
                token_id = None
                for mk in bot._cached_markets:
                    if mk.slug == slug:
                        token_id = mk.outcome_up_id
                        break
                if token_id:
                    is_active = bot._truth.is_active_window_token(token_id)

            active_slugs.append({
                "slug": slug,
                "crypto": st.crypto,
                "window_active": is_active,
            })

        # Window state
        state = "UNKNOWN"
        close_imminent = False
        if hasattr(bot, '_truth'):
            state = str(bot._truth.window_state.value) if hasattr(bot._truth.window_state, 'value') else str(bot._truth.window_state)
            if hasattr(bot._live_safety, 'is_close_imminent') and hour_start_utc:
                close_imminent = bot._live_safety.is_close_imminent(hour_start_utc)

        # Flatten/hard flatten phase
        flatten_phase = bot._is_in_flatten_phase() if hasattr(bot, '_is_in_flatten_phase') else False

        return {
            "now_utc": _iso(now),
            "hour_start_utc": hour_start_utc,
            "hour_end_utc": hour_end_utc,
            "t_min": round(t_min, 2),
            "seconds_to_close": round(seconds_to_close, 1),
            "state": state,
            "close_imminent": close_imminent,
            "flatten_phase": flatten_phase,
            "active_slugs": active_slugs,
        }

    # ──────────────────────────────────────────────────────────────────
    # B) SAFETY FLAGS / MODES
    # ──────────────────────────────────────────────────────────────────

    def _build_safety(self) -> dict:
        bot = self._bot
        # Truth safe mode
        truth_safe = False
        truth_safe_reason = ""
        if hasattr(bot, '_truth'):
            truth_safe = bot._truth.safe_mode
            truth_safe_reason = getattr(bot._truth, '_safe_mode_reason', "")

        # Ledger safe mode
        ledger_safe = False
        if bot._fills_ledger is not None:
            ledger_safe = bot._fills_ledger.safe_mode

        # Desync
        desync_hard_stop = bot._desync_hard_stop
        reconciler_desynced = False
        if hasattr(bot, '_exec_reconciler'):
            reconciler_desynced = bot._exec_reconciler.is_desynced()

        # Exec safety
        exec_kill = False
        exec_kill_reason = ""
        exec_drift_paused = False
        if hasattr(bot, '_exec_safety'):
            exec_kill = bot._exec_safety.is_kill_active()
            exec_kill_reason = getattr(bot._exec_safety, '_kill_reason', "")
            exec_drift_paused = getattr(bot._exec_safety, '_drift_paused', False)

        # Circuit breaker
        hedge_breaker = False
        if hasattr(bot, '_live_safety'):
            hedge_breaker = bot._live_safety.is_hedge_breaker_active()

        # Entry gates summary
        buys_common = bot._buys_allowed_common()
        buys_dir = bot._buys_allowed_dir()
        buys_scalp = bot._buys_allowed_scalp() if hasattr(bot, '_buys_allowed_scalp') else None

        # Top blocker
        top_blocker = "none"
        if not buys_dir:
            if reconciler_desynced:
                top_blocker = "RECONCILER_DESYNCED"
            elif ledger_safe:
                top_blocker = "LEDGER_SAFE_MODE"
            elif truth_safe:
                top_blocker = "TRUTH_SAFE_MODE"
            elif desync_hard_stop:
                top_blocker = "DESYNC_HARD_STOP"
            elif hasattr(bot, '_total_exposure_usd') and bot._total_exposure_usd() >= settings.MAX_TOTAL_EXPOSURE_USD:
                top_blocker = "EXPOSURE_CAP"
            elif hasattr(bot, '_is_in_flatten_phase') and bot._is_in_flatten_phase():
                top_blocker = "FLATTEN_PHASE"
            elif settings.ENTRY_ONLY_EARLY_WINDOW and hasattr(bot, '_is_past_early_window') and bot._is_past_early_window():
                top_blocker = "EARLY_WINDOW_ONLY"
            else:
                top_blocker = "UNKNOWN"

        return {
            "truth_safe_mode": truth_safe,
            "truth_safe_reason": truth_safe_reason,
            "ledger_safe_mode": ledger_safe,
            "desync_hard_stop": desync_hard_stop,
            "reconciler_desynced": reconciler_desynced,
            "exec_kill_switch": exec_kill,
            "exec_kill_reason": exec_kill_reason,
            "exec_drift_paused": exec_drift_paused,
            "hedge_circuit_breaker": hedge_breaker,
            "buys_allowed_common": buys_common,
            "buys_allowed_dir": buys_dir,
            "buys_allowed_scalp": buys_scalp,
            "top_blocker": top_blocker,
        }

    # ──────────────────────────────────────────────────────────────────
    # C) EXPOSURE + BUDGET
    # ──────────────────────────────────────────────────────────────────

    def _build_exposure(self) -> dict:
        bot = self._bot
        # Position exposure
        exposure_positions = 0.0
        per_slug = {}
        for slug, st in bot.market_states.items():
            slug_cost = 0.0
            slug_data = {}
            for outcome in ("Up", "Down"):
                pos = st.positions[outcome]
                if pos.qty >= 0.001:
                    cost = abs(pos.qty) * pos.vwap
                    exposure_positions += cost
                    slug_cost += cost
                    slug_data[outcome] = {
                        "qty": round(pos.qty, 2),
                        "vwap": round(pos.vwap, 4),
                        "cost_usd": round(cost, 2),
                    }
            if slug_data:
                per_slug[slug] = {
                    "position_usd": round(slug_cost, 2),
                    "open_orders_usd": 0.0,  # populated below
                    **slug_data,
                }

        # Open orders exposure
        exposure_orders = bot._total_reserved_usd() if hasattr(bot, '_total_reserved_usd') else 0.0

        # Per-slug open order breakdown from _reserved_usd
        # (we don't have per-slug order info easily, but we can count)

        exposure_total = exposure_positions + exposure_orders
        cap = settings.MAX_TOTAL_EXPOSURE_USD

        # Engine breakdown (HYBRID)
        engine_dir = 0.0
        engine_scalp = 0.0
        if hasattr(bot, '_exposure_tracker') and bot._exposure_tracker is not None:
            engine_dir = bot._exposure_tracker.engine_exposure_usd("DIR")
            engine_scalp = bot._exposure_tracker.engine_exposure_usd("SCALP")

        return {
            "cash": round(bot.cash_usdc, 2),
            "equity": round(bot._equity(), 2),
            "exposure_positions_usd": round(exposure_positions, 2),
            "exposure_open_orders_usd": round(exposure_orders, 2),
            "exposure_total_usd": round(exposure_total, 2),
            "MAX_TOTAL_EXPOSURE_USD": cap,
            "remaining_exposure_capacity": round(max(0, cap - exposure_total), 2),
            "hour_spend_cumulative_usd": round(getattr(bot, '_hour_budget_spent', 0.0), 2),
            "engine_dir_usd": round(engine_dir, 2),
            "engine_scalp_usd": round(engine_scalp, 2),
            "per_slug": per_slug,
        }

    # ──────────────────────────────────────────────────────────────────
    # D) ORDERS / WATCHERS
    # ──────────────────────────────────────────────────────────────────

    def _build_orders(self) -> dict:
        bot = self._bot
        now = time.time()
        watchers = []
        watchers_count = 0
        open_orders_usd = 0.0
        per_slug_orders: Dict[str, float] = {}

        # From order tracker
        if hasattr(bot, '_exec_tracker'):
            tracked = bot._exec_tracker.get_tracked_orders()
            watchers_count = len(tracked)
            for oid, order in tracked.items():
                age_sec = now - order.submit_ts
                notional = order.price * order.requested_qty
                open_orders_usd += notional
                slug_key = order.slug or "unknown"
                per_slug_orders[slug_key] = per_slug_orders.get(slug_key, 0.0) + notional
                watchers.append({
                    "order_id": oid[:16],
                    "slug": order.slug,
                    "outcome": order.outcome,
                    "side": order.side,
                    "qty": round(order.requested_qty, 1),
                    "filled_qty": round(order.filled_qty, 1),
                    "price": round(order.price, 4),
                    "age_sec": round(age_sec, 1),
                    "pending_confirmation": order.pending_confirmation,
                    "reason": order.reason,
                })

        # Reserved USD
        reserved_total = bot._total_reserved_usd() if hasattr(bot, '_total_reserved_usd') else 0.0

        return {
            "open_orders_count": watchers_count,
            "open_orders_total_usd": round(open_orders_usd, 2),
            "reserved_usd": round(reserved_total, 2),
            "per_slug_orders_usd": {k: round(v, 2) for k, v in per_slug_orders.items()},
            "watchers_count": watchers_count,
            "watchers": watchers[:20],  # cap at 20 for readability
        }

    # ──────────────────────────────────────────────────────────────────
    # E) POSITIONS — BOT vs TRUTH COMPARISON
    # ──────────────────────────────────────────────────────────────────

    def _build_positions(self) -> dict:
        bot = self._bot
        comparisons = []

        # Get bot positions
        bot_positions = bot._get_internal_positions() if hasattr(bot, '_get_internal_positions') else {}

        # Get truth positions
        truth_positions = {}
        if hasattr(bot, '_truth') and hasattr(bot._truth, 'get_positions'):
            try:
                truth_positions = bot._truth.get_positions()
            except Exception:
                pass
        elif hasattr(bot, '_truth') and hasattr(bot._truth, '_ledger_positions'):
            truth_positions = bot._truth._ledger_positions or {}

        # Merge all slugs
        all_slugs = set(list(bot_positions.keys()) + list(truth_positions.keys()))
        drift_count = 0
        for slug in sorted(all_slugs):
            bot_pos = bot_positions.get(slug, {"Up": 0.0, "Down": 0.0})
            truth_pos = truth_positions.get(slug, {"Up": 0.0, "Down": 0.0})
            for outcome in ("Up", "Down"):
                bq = bot_pos.get(outcome, 0.0)
                tq = truth_pos.get(outcome, 0.0)
                delta = round(bq - tq, 2)
                if abs(delta) > 0.01 or bq > 0 or tq > 0:
                    comparisons.append({
                        "slug": slug,
                        "outcome": outcome,
                        "bot_qty": round(bq, 2),
                        "truth_qty": round(tq, 2),
                        "delta_qty": delta,
                    })
                    if abs(delta) > 0.5:
                        drift_count += 1

        # Reconcile info
        last_reconcile = ""
        reconcile_ok = True
        if hasattr(bot, '_last_reconcile_ts'):
            last_reconcile = f"{time.time() - bot._last_reconcile_ts:.0f}s ago"
        if hasattr(bot, '_last_ledger_reconcile_ts') and bot._last_ledger_reconcile_ts:
            age = time.time() - bot._last_ledger_reconcile_ts
            last_reconcile = f"{age:.0f}s ago"

        # Applied fills dedup set
        dedup_size = len(bot._applied_fills) if hasattr(bot, '_applied_fills') else 0
        # Last 5 entries (these are tuples, show as strings)
        last_5 = []
        if hasattr(bot, '_applied_fills'):
            items = list(bot._applied_fills)
            for item in items[-5:]:
                last_5.append(str(item))

        return {
            "comparisons": comparisons,
            "drift_count": drift_count,
            "last_reconcile": last_reconcile,
            "applied_fills_size": dedup_size,
            "applied_fills_last_5": last_5,
        }

    # ──────────────────────────────────────────────────────────────────
    # F) SIGNAL SNAPSHOT
    # ──────────────────────────────────────────────────────────────────

    def _build_signals(self) -> dict:
        bot = self._bot
        from src.strategy.f247_like import entry_threshold_bps, price_cap
        signals = []

        for slug, st in bot.market_states.items():
            try:
                hour_start = datetime.strptime(st.hour_start_utc, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                t_min = (now - hour_start).total_seconds() / 60.0
            except Exception:
                t_min = 30.0

            up_book = bot.last_book.get(slug, {}).get("Up")
            dn_book = bot.last_book.get(slug, {}).get("Down")
            if not up_book or not dn_book:
                continue

            # Delta
            delta_bps = 0.0
            if st.hour_open > 0:
                # Use cached data if available
                cached = bot._data_cache.get(slug)
                spot = cached.get("spot", 0.0) if cached else 0.0
                if spot > 0 and st.hour_open > 0:
                    delta_bps = (spot / st.hour_open - 1.0) * 10000

            # Velocity
            from src.strategy.sizing import delta_velocity_bps_per_min, zscore
            vel = delta_velocity_bps_per_min(st.delta_hist, lookback_sec=30.0)
            z = zscore(st.delta_hist, window_sec=300.0)

            # IMB
            imb = up_book.imb if up_book else 0.0

            # Thresholds
            thr = entry_threshold_bps(st.crypto, t_min)
            cap = price_cap(t_min)

            # Signal evaluation
            abs_delta = abs(delta_bps)
            drift_dir = "Up" if delta_bps > 0 else "Down"
            book = up_book if drift_dir == "Up" else dn_book
            spread_cents = book.spread * 100 if book else 99.0

            # Check signal quality
            signal_ok = True
            blocker = ""
            if abs_delta < getattr(settings, 'DSCALP_DELTA_MIN_BPS', 15):
                signal_ok = False
                blocker = f"delta_too_low({abs_delta:.1f}<{getattr(settings, 'DSCALP_DELTA_MIN_BPS', 15)})"
            elif spread_cents > getattr(settings, 'DSCALP_MAX_SPREAD_CENTS', 2.0):
                signal_ok = False
                blocker = f"spread_wide({spread_cents:.1f}c>{getattr(settings, 'DSCALP_MAX_SPREAD_CENTS', 2.0)}c)"
            elif not bot._buys_allowed_dir():
                signal_ok = False
                blocker = "buys_not_allowed"

            signals.append({
                "slug": slug,
                "crypto": st.crypto,
                "best_bid_up": round(up_book.bid, 4),
                "best_ask_up": round(up_book.ask, 4),
                "spread_up_cents": round(up_book.spread * 100, 2),
                "best_bid_dn": round(dn_book.bid, 4),
                "best_ask_dn": round(dn_book.ask, 4),
                "spread_dn_cents": round(dn_book.spread * 100, 2),
                "depth_bid": round(up_book.depth_1c_bid + dn_book.depth_1c_bid, 1),
                "depth_ask": round(up_book.depth_1c_ask + dn_book.depth_1c_ask, 1),
                "delta_bps": round(delta_bps, 2),
                "z": round(z, 3),
                "velocity_bps_per_min": round(vel, 3),
                "imbalance": round(imb, 3),
                "chosen_side": drift_dir if abs_delta > 0 else None,
                "entry_threshold_bps": thr,
                "price_cap": round(cap, 3),
                "signal_ok": signal_ok,
                "blocker": blocker,
            })

        return {"signals": signals}

    # ──────────────────────────────────────────────────────────────────
    # Build full snapshot
    # ──────────────────────────────────────────────────────────────────

    def _build_snapshot(self) -> dict:
        return {
            "event_type": "DIAG_SNAPSHOT",
            "ts_ms": _ts_ms(),
            "clock": self._build_clock(),
            "safety": self._build_safety(),
            "exposure": self._build_exposure(),
            "orders": self._build_orders(),
            "positions": self._build_positions(),
            "signals": self._build_signals(),
        }

    # ──────────────────────────────────────────────────────────────────
    # Emit
    # ──────────────────────────────────────────────────────────────────

    def _emit(self, snapshot: dict) -> None:
        """Print [DIAG] flat key=value block and append JSON to diag_state.jsonl."""
        clk = snapshot["clock"]
        saf = snapshot["safety"]
        exp = snapshot["exposure"]
        ords = snapshot["orders"]
        pos = snapshot["positions"]
        sigs = snapshot.get("signals", {}).get("signals", [])

        # Build flat key=value output matching spec section 2
        lines = ["[DIAG]"]
        lines.append(f"now_utc={clk['now_utc']}")
        lines.append(f"hour_start_utc={clk['hour_start_utc']}")
        lines.append(f"hour_end_utc={clk['hour_end_utc']}")
        lines.append(f"t_min={clk['t_min']}")
        # Map state to spec values: IDLE/ACTIVE/SETTLEMENT/FLATTEN
        state = clk['state']
        if clk.get('flatten_phase'):
            state = "FLATTEN"
        lines.append(f"state={state}")
        lines.append(f"close_imminent={str(clk['close_imminent']).lower()}")
        lines.append(f"safe_mode={str(saf['truth_safe_mode']).lower()}")
        lines.append(f"desync_hard_stop={str(saf['desync_hard_stop']).lower()}")
        lines.append(f"cash={exp['cash']}")
        lines.append(f"equity={exp['equity']}")
        lines.append(f"exposure_positions_usd={exp['exposure_positions_usd']}")
        lines.append(f"exposure_open_orders_usd={exp['exposure_open_orders_usd']}")
        lines.append(f"exposure_total_usd={exp['exposure_total_usd']}")
        lines.append(f"MAX_TOTAL_EXPOSURE_USD={exp['MAX_TOTAL_EXPOSURE_USD']}")
        lines.append(f"hour_spend_cumulative_usd={exp['hour_spend_cumulative_usd']}")
        lines.append(f"remaining_exposure_capacity={exp['remaining_exposure_capacity']}")
        lines.append(f"open_orders_total_usd={ords['open_orders_total_usd']}")
        lines.append(f"open_orders_count={ords['open_orders_count']}")

        # Per-slug sub-blocks: merge position comparisons + signal data
        sig_by_slug = {s["slug"]: s for s in sigs}
        pos_by_slug: Dict[str, dict] = {}
        for c in pos.get("comparisons", []):
            slug = c["slug"]
            if slug not in pos_by_slug:
                pos_by_slug[slug] = {}
            pos_by_slug[slug][c["outcome"]] = c

        all_slugs = set()
        for s in clk.get("active_slugs", []):
            all_slugs.add(s["slug"])
        for c in pos.get("comparisons", []):
            all_slugs.add(c["slug"])

        for slug in sorted(all_slugs):
            pc = pos_by_slug.get(slug, {})
            sig = sig_by_slug.get(slug, {})
            bot_up = pc.get("Up", {}).get("bot_qty", 0.0)
            bot_dn = pc.get("Down", {}).get("bot_qty", 0.0)
            truth_up = pc.get("Up", {}).get("truth_qty", 0.0)
            truth_dn = pc.get("Down", {}).get("truth_qty", 0.0)
            drift = abs(bot_up - truth_up) > 0.5 or abs(bot_dn - truth_dn) > 0.5
            spread = sig.get("spread_up_cents", 0.0)
            edge = sig.get("delta_bps", 0.0)
            vel = sig.get("velocity_bps_per_min", 0.0)
            lines.append(f"  slug={slug}")
            lines.append(f"  bot_qty_up={bot_up}")
            lines.append(f"  bot_qty_down={bot_dn}")
            lines.append(f"  truth_qty_up={truth_up}")
            lines.append(f"  truth_qty_down={truth_dn}")
            lines.append(f"  drift_detected={str(drift).lower()}")
            lines.append(f"  spread_cents={spread}")
            lines.append(f"  computed_edge_bps={edge}")
            lines.append(f"  velocity_bps_per_min={vel}")

        print("\n".join(lines))

        # File: append one JSON line
        try:
            with open(self._diag_path, "a") as f:
                f.write(json.dumps(snapshot, default=str) + "\n")
        except Exception:
            pass

        # Also emit via write_jsonl for main log
        self._write_jsonl(snapshot)

    # ──────────────────────────────────────────────────────────────────
    # BUDGET INVARIANT CHECK
    # ──────────────────────────────────────────────────────────────────

    def _check_budget_invariant(self, snapshot: dict) -> None:
        """Assert exposure_total <= MAX_TOTAL_EXPOSURE_USD + epsilon."""
        exp = snapshot.get("exposure", {})
        total = exp.get("exposure_total_usd", 0.0)
        cap = exp.get("MAX_TOTAL_EXPOSURE_USD", 100.0)
        if total > cap + 0.01:
            msg = {
                "event_type": "BUG_EXPOSURE_OVER_CAP",
                "exposure_total_usd": total,
                "MAX_TOTAL_EXPOSURE_USD": cap,
                "overshoot_usd": round(total - cap, 4),
                "exposure_positions_usd": exp.get("exposure_positions_usd", 0),
                "exposure_open_orders_usd": exp.get("exposure_open_orders_usd", 0),
                "per_slug": exp.get("per_slug", {}),
                "ts_ms": _ts_ms(),
            }
            print(f"\n  [BUG] EXPOSURE_OVER_CAP: ${total:.2f} > ${cap:.2f}"
                  f"  overshoot=${total - cap:.4f}")
            print(f"  [BUG]   positions=${exp.get('exposure_positions_usd', 0)}"
                  f"  orders=${exp.get('exposure_open_orders_usd', 0)}")
            self._write_jsonl(msg)
            try:
                with open(self._diag_path, "a") as f:
                    f.write(json.dumps(msg, default=str) + "\n")
            except Exception:
                pass

    # ──────────────────────────────────────────────────────────────────
    # 2) GATE TRACE — call from entry rejection points
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def gate_trace(slug: str, side: str, stage: str, reason: str,
                   t_min: float = 0.0, spread: float = 0.0,
                   delta_bps: float = 0.0, exposure_total: float = 0.0,
                   bot: Any = None, velocity_bps_per_min: float = 0.0,
                   cooldown_remaining_ms: int = 0, edge_bps: float = 0.0,
                   write_fn=None) -> None:
        """Emit multi-line [GATE] trace. Called at FIRST blocking gate only."""
        if not settings.DIAG_MODE:
            return

        # Normalize side to UP/DOWN
        side_norm = side.upper() if side else ""

        # Enrich from bot if available
        safe_mode = False
        desync_hard_stop = False
        positions_cost = 0.0
        reserved_open_orders = 0.0
        max_exp = settings.MAX_TOTAL_EXPOSURE_USD
        open_orders_count = 0

        if bot is not None:
            if hasattr(bot, '_truth'):
                safe_mode = bot._truth.safe_mode
            desync_hard_stop = getattr(bot, '_desync_hard_stop', False)
            for _slug, _st in bot.market_states.items():
                for _outcome in ("Up", "Down"):
                    _pos = _st.positions[_outcome]
                    if _pos.qty >= 0.001:
                        positions_cost += abs(_pos.qty) * _pos.vwap
            reserved_open_orders = bot._total_reserved_usd() if hasattr(bot, '_total_reserved_usd') else 0.0
            if hasattr(bot, '_exec_tracker'):
                open_orders_count = len(bot._exec_tracker.get_tracked_orders())

        lines = [
            "[GATE]",
            f"slug={slug}",
            f"side={side_norm}",
            f"block_stage={stage}",
            f"block_reason={reason}",
            f"t_min={t_min:.2f}",
            f"safe_mode={str(safe_mode).lower()}",
            f"desync_hard_stop={str(desync_hard_stop).lower()}",
            f"exposure_total={exposure_total:.2f}",
            f"positions_cost={positions_cost:.2f}",
            f"reserved_open_orders={reserved_open_orders:.2f}",
            f"MAX_TOTAL_EXPOSURE_USD={max_exp:.2f}",
            f"spread_cents={spread:.2f}",
            f"edge_bps={edge_bps:.2f}",
            f"velocity_bps_per_min={velocity_bps_per_min:.2f}",
            f"cooldown_remaining_ms={int(cooldown_remaining_ms)}",
            f"open_orders_count={open_orders_count}",
        ]
        print("\n".join(lines))

        evt = {
            "event_type": "GATE_TRACE",
            "slug": slug, "side": side_norm,
            "block_stage": stage, "block_reason": reason,
            "t_min": round(t_min, 2),
            "safe_mode": safe_mode,
            "desync_hard_stop": desync_hard_stop,
            "exposure_total": round(exposure_total, 2),
            "positions_cost": round(positions_cost, 2),
            "reserved_open_orders": round(reserved_open_orders, 2),
            "MAX_TOTAL_EXPOSURE_USD": max_exp,
            "spread_cents": round(spread, 2),
            "edge_bps": round(edge_bps, 2),
            "velocity_bps_per_min": round(velocity_bps_per_min, 2),
            "cooldown_remaining_ms": int(cooldown_remaining_ms),
            "open_orders_count": open_orders_count,
            "ts_ms": _ts_ms(),
        }
        if write_fn:
            write_fn(evt)

    # ──────────────────────────────────────────────────────────────────
    # 3) ORDER TRACE — call at order/fill events
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def order_trace(event_type: str, slug: str, outcome: str, side: str,
                    order_id: str, price: float, qty: float,
                    pre_exposure: float = 0.0, post_exposure: float = 0.0,
                    bot_qty_after: float = 0.0, truth_qty_after: float = 0.0,
                    engine: str = "", write_fn=None) -> None:
        """Emit multi-line [ORDER] trace at order/fill events."""
        if not settings.DIAG_MODE:
            return
        notional = round(price * qty, 2)
        lines = [
            "[ORDER]",
            f"event_type={event_type}",
            f"engine={engine}",
            f"slug={slug}",
            f"outcome={outcome}",
            f"side={side}",
            f"qty={qty:.2f}",
            f"price={price:.4f}",
            f"notional_usd={notional:.2f}",
            f"pre_exposure_total={pre_exposure:.2f}",
            f"post_exposure_total={post_exposure:.2f}",
            f"bot_qty_after={bot_qty_after:.2f}",
            f"truth_qty_after={truth_qty_after:.2f}",
        ]
        print("\n".join(lines))
        evt = {
            "event_type": "ORDER_TRACE",
            "action": event_type,
            "engine": engine,
            "slug": slug, "outcome": outcome, "side": side,
            "qty": round(qty, 2), "price": round(price, 4),
            "notional_usd": notional,
            "pre_exposure_total": round(pre_exposure, 2),
            "post_exposure_total": round(post_exposure, 2),
            "bot_qty_after": round(bot_qty_after, 2),
            "truth_qty_after": round(truth_qty_after, 2),
            "ts_ms": _ts_ms(),
        }
        if write_fn:
            write_fn(evt)

    # ──────────────────────────────────────────────────────────────────
    # 4) SAFETY TRACE — call at safe mode / desync transitions
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def safety_trace(event: str, reason: str, drift_counter: int = 0,
                     write_fn=None) -> None:
        """Emit multi-line [SAFETY] trace at safety state transitions."""
        if not settings.DIAG_MODE:
            return
        lines = [
            "[SAFETY]",
            f"event={event}",
            f"reason={reason}",
            f"drift_counter={drift_counter}",
        ]
        print("\n".join(lines))
        evt = {
            "event_type": "SAFETY_TRACE",
            "event": event, "reason": reason,
            "drift_counter": drift_counter,
            "ts_ms": _ts_ms(),
        }
        if write_fn:
            write_fn(evt)
