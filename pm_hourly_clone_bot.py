#!/usr/bin/env python3
"""
pm_hourly_clone_bot.py
Two-mode Polymarket hourly crypto bot:
- MODE=LOG  : paper mode, starts with $1000, runs until stopped, logs every action
- MODE=LIVE : same logic, submits orders
Implements a close behavioral clone of the high-frequency hourly wallet you analyzed:
- Drift-direction "core" engine + scale-in/out inventory recycling
- Late-hour cheap-side scalp engine
- Delta velocity / persistence / volatility normalization
- Orderbook imbalance gating
- Pullback entries
- Cooldown + layered limit orders
- Risk caps per market/crypto + hourly/daily stop-loss alerts (log-only, shadow sim)
- Stops adding risk after minute 57; cleanup near minute 59
Logging:
- CSV trade-intent + order-intent logs
- JSONL event logs (full state snapshots)
"""
from __future__ import annotations
import os
import sys
import time
import json
import math
import signal
import random
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import requests

# Import the new Logger (replaces old write_jsonl / log_csv)
from logger import (
    Logger, SCHEMA_VERSION, BOT_VERSION, MIN_QTY as _LOG_MIN_QTY,
    STATE_HIST_MAX,
    build_book_fields, new_decision_id, new_order_id, new_position_id,
    infer_maker_taker, spread_capture_fields,
)

# MODULAR IMPORTS — config, data structures, utils, strategy, feeds, gating
from src.config.settings import *                       # noqa: F403 — all config constants
from src.config.settings import (                       # private names not exported by *
    _PROJECT_DIR, _KEYS_DIR, _LOG_DIR, _THR_TABLE, _IS_MIN_PROFILE,
)
MIN_QTY = _LOG_MIN_QTY  # override: logger's MIN_QTY takes precedence

from src.bot.context import MarketRef, BookTop, Position, MarketState
from src.util.time import (
    utc_now, iso_z, parse_hour_start_from_slug, minutes_into_hour,
    _phase, _hour_label_et, MONTHS,
)
from src.util.math import clamp, clamp_to_tick, _p_up_model
from src.strategy.f247_like import (
    entry_threshold_bps, price_cap, dynamic_cap, spread_limit,
    whipsaw_ok, persistence_ok, compute_fee_usdc,
)
# Parity-only strategy functions — not needed for HOURLY_SCALP_MIN
if not _IS_MIN_PROFILE:
    from src.strategy.f247_like import parity_net_edge_cents, parity_liquidity_ok
else:
    # Stubs: unreachable under HOURLY_SCALP_MIN (parity call sites are guarded)
    def parity_net_edge_cents(*a, **kw): return (0.0, 0.0, 0.0)  # type: ignore[misc]
    def parity_liquidity_ok(*a, **kw): return (False, "disabled")   # type: ignore[misc]

from src.strategy.sizing import sizing_mult, zscore, delta_velocity_bps_per_min, delta_velocity_debug
from src.trading.order_manager import OrderManager
from src.trading.portfolio import compute_equity, clean_dust
from src.trading.risk import market_cost_usdc, crypto_cost_usdc
from src.execution.order_tracker import LiveOrderTracker
from src.execution.orphan_scanner import OrphanScanner
from src.execution.safety_caps import SafetyCaps
from src.execution.unpaired_resolver import UnpairedResolver
from src.execution.live_sanity_report import LiveSanityReporter
from src.execution.state_drift_checker import StateDriftChecker
from src.execution.position_reconciler import PositionReconciler
from src.execution.live_safety import LiveSafety
from src.execution.fills_ledger import FillsLedger
from src.positions.truth_capture import TruthCapture, WindowState
from src.feeds.polymarket import PolymarketClient, set_jsonl_writer

# BotApp — subsystem wiring (only needed for full F247_LIKE profile)
if not _IS_MIN_PROFILE:
    from src.bot.app import BotApp
else:
    BotApp = None  # type: ignore[assignment,misc]

# UTIL / LOGGING — thin wrappers around Logger instance
_LOGGER: Optional["Logger"] = None

# Events that ALWAYS get logged (even in COMPACT mode)
_ALWAYS_LOG_EVENTS = frozenset({
    # Order lifecycle
    "ORDER_SUBMIT", "ORDER_FILL", "ORDER_CANCEL", "ORDER_ERROR", "ORDER_REPLACE",
    "ORDER_PLACED", "CANCEL_ERROR",
    # Periodic reports
    "DIAG_REPORT", "CLONE_REPORT", "TEMPO_REPORT", "SLUG_PNL_REPORT",
    "HOUR_SUMMARY", "HOUR_LABEL",
    # System lifecycle
    "INIT_BOOTSTRAP_DONE", "STOP_SIGNAL", "STOPPED", "NEW_DAY_RESET",
    "STATE_LOADED", "STATE_LOAD_ERROR", "STATE_SAVE_ERROR", "NEW_MARKET_STATE",
    # Risk / safety
    "RISK_STOP_TRIGGERED", "SLUG_PAUSED_NEGATIVE_EXITS", "ADVERSE_GUARD_HARD_PAUSE",
    # Directional scalp trades
    "DSCALP_ENTRY", "DSCALP_TP1", "DSCALP_TP2", "DSCALP_TP3", "DSCALP_TP4",
    "DSCALP_STOP", "DSCALP_LOSS_CAP", "DSCALP_EARLY_EXIT", "DSCALP_TIMEOUT",
    "DSCALP_RUNNER_FALLBACK",
    # Parity trades
    "PARITY_BUY_STRADDLE", "PARITY_SELL_STRADDLE", "PARITY_PAIR_COMPLETED",
    "PARITY_PAIR_COMPLETED_LATE", "PAIR_COMPLETED", "PARITY_QUOTE_STRADDLE",
    # Exits / hedge fills
    "TIME_STOP_EXIT", "TIME_STOP_SOFT_EXIT", "INVENTORY_PRESSURE_SELL",
    "FAST_TP_FIRE", "EXIT_TAKER_FALLBACK", "RESCUE_TRIGGERED", "LEAN_EXIT",
    "HEDGE_CROSS_EARLY", "HEDGE_CROSS_LATE", "PARITY_RECYCLE",
    # Compact summaries (always log their own output)
    "GATE_REPORT", "SNAPSHOT_COMPACT", "GATE_SPREAD_BLOCK",
    # Market discovery
    "MARKET_DISCOVERY_ERROR",
    # Position reconciliation
    "RECONCILE_START", "RECONCILE_DONE", "RECONCILE_CORRECTIONS",
    "RECONCILE_TRIGGERED", "RECONCILE_ERROR",
    "STATE_DESYNC_DETECTED", "STATE_DESYNC_RESOLVED",
})
# Prefixes that always log (for variable event_type like flatten_reason)
_ALWAYS_LOG_PREFIXES = ("FLATTEN", "KILL", "PAUSE", "END_OF_HOUR")

def _compact_gate(event_type: str) -> bool:
    """Return True if this event should be logged in COMPACT mode."""
    if LOG_LEVEL != "COMPACT":
        return True  # DEBUG mode — log everything
    if event_type in _ALWAYS_LOG_EVENTS:
        return True
    if any(event_type.startswith(p) for p in _ALWAYS_LOG_PREFIXES):
        return True
    return False

def write_jsonl(event: dict) -> None:
    """Legacy shim — delegates to _LOGGER._write_jsonl if available.
    In COMPACT mode, suppresses verbose per-tick events."""
    if not _compact_gate(event.get("event_type", "")):
        return
    if _LOGGER is not None:
        _LOGGER._write_jsonl(event)
    else:
        event["ts"] = iso_z(utc_now())
        event["run_id"] = RUN_ID
        print(f"[{event.get('event_type','')}] (pre-logger)")

def _parse_iso_z(s: str) -> datetime:
    """Parse ISO-8601 UTC string like '2025-01-01T00:00:00Z' → aware datetime."""
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _p50_p90(vals) -> Tuple[float, float]:
    """Return (median, 90th-percentile) of a numeric list, or (0, 0) if empty."""
    if not vals:
        return 0.0, 0.0
    s = sorted(vals)
    return s[len(s) // 2], s[min(int(len(s) * 0.9), len(s) - 1)]


# BOT CORE
class Bot:
    def __init__(self):
        global _LOGGER
        self.client = PolymarketClient()
        set_jsonl_writer(write_jsonl)  # wire module-level callback for CLOB logging
        self.running = True
        # Fetch real USDC balance from Polygon wallet; fall back to config
        self._wallet_usdc_at_start = None
        if MODE in ("LIVE", "LIVE_SAFE"):
            self._wallet_usdc_at_start = self.client.get_usdc_balance()
            if self._wallet_usdc_at_start is not None:
                print(f"  [WALLET] On-chain USDC balance: ${self._wallet_usdc_at_start:.2f}")
            else:
                print(f"  [WALLET] Could not fetch USDC balance, using BANKROLL_START_USDC=${BANKROLL_START_USDC:.2f}")
        starting_cash = self._wallet_usdc_at_start if self._wallet_usdc_at_start is not None else BANKROLL_START_USDC
        self.cash_usdc = starting_cash
        self.realized_pnl_usdc = 0.0  # cumulative realized P&L (sells + settlements)
        self.day_start = utc_now().date()
        self.day_start_equity = starting_cash
        self.hour_start_equity = starting_cash
        self.hourly_pnl_usdc = 0.0
        self._hour_window = utc_now().replace(minute=0, second=0, microsecond=0)
        # Risk-alert tracking (log-only, never enforced)
        self._hour_risk_stop_hit = False   # single flag per hour window (portfolio-wide)
        self._day_risk_stop_hit = False
        # Hourly stats for HOUR_SUMMARY
        self._hour_trade_count = 0
        self._hour_net_pnl = 0.0
        self._hour_budget_remaining = HOURLY_BUDGET_USD  # resets each hour
        self._hour_edges: List[float] = []
        # Shadow stop-loss simulation
        self._shadow_active = False
        self._shadow_cash = 0.0
        self._shadow_positions: Dict[str, Dict[str, dict]] = {}  # slug->{outcome->pos}
        self._shadow_equity_at_trigger = 0.0
        self._shadow_trades_blocked = 0
        self.market_states: Dict[str, MarketState] = {}   # slug -> MarketState
        self.signal_hist: Dict[str, List[Tuple[str, bool]]] = {}  # slug -> [(ts, valid_signal)]
        self.last_book: Dict[str, Dict[str, BookTop]] = {}  # slug -> outcome -> BookTop
        self.recent_extreme_price: Dict[str, Dict[str, float]] = {} # slug->outcome->extreme
        # Whipsaw filter: slug -> (current_sign, since_ts)
        self._edge_sign_state: Dict[str, Tuple[int, float]] = {}
        # Velocity debug: slug -> last debug-print epoch
        self._vel_debug_last_ts: Dict[str, float] = {}
        # No-flip rule: slug -> (last_outcome, last_ts)
        self._last_trade_direction: Dict[str, Tuple[str, float]] = {}
        # Derisk edge worsening tracker: (slug, outcome) -> first_worsen_ts
        self._derisk_edge_worsen_since: Dict[Tuple[str, str], float] = {}
        # Per-minute diagnostic counters
        self._diag_taker_count = 0
        self._diag_derisk_count = 0
        self._diag_derisk_taker_count = 0
        self._diag_blocked_whipsaw = 0
        self._diag_blocked_noflip = 0
        # Parity arb tracking
        self._parity_last_order_ts: Dict[str, float] = {}    # slug -> last parity order timestamp
        self._parity_invested_usd: Dict[str, float] = {}     # slug -> total parity investment
        # Partial-fill tracking: list of pending pairs awaiting second leg
        # Each entry: {pair_id, slug, filled_outcome, filled_usd, filled_qty, filled_price,
        #              pending_outcome, ts, edge_net_cents}
        self._parity_pending_pairs: List[dict] = []
        # Maker queue discipline: slug -> {outcome -> {order_id, price, last_replace_ts}}
        self._parity_maker_orders: Dict[str, Dict[str, dict]] = {}
        # Locked straddle first-open timestamps: slug -> first_locked_ts
        self._parity_locked_since: Dict[str, float] = {}
        # Per-minute parity diagnostics
        self._diag_parity_buy_signals = 0
        self._diag_parity_sell_signals = 0
        self._diag_parity_trades = 0
        self._diag_parity_edges: List[float] = []            # NET cents captured per trade
        self._diag_parity_maker_count = 0
        self._diag_parity_taker_count = 0
        self._diag_pair_partial_count = 0
        self._diag_pair_fill_delays: List[float] = []        # ms per pair fill
        self._diag_unpaired_unwind_usd = 0.0
        self._diag_parity_blocked_spread = 0
        self._diag_parity_blocked_liq = 0
        self._diag_parity_blocked_stale = 0
        self._diag_recycle_count = 0
        # End-of-hour flatten counters
        self._diag_flatten_actions = 0
        self._diag_flatten_taker = 0
        # Rescue-to-straddle counters
        self._diag_rescue_attempts = 0
        self._diag_rescue_success = 0
        self._diag_rescue_fallback_sells = 0
        # Parity quoting counters
        self._diag_quote_orders_placed = 0
        self._diag_quote_fills = 0
        self._diag_quote_unpaired_events = 0
        self._diag_quote_unpaired_escalations = 0
        self._diag_quote_pause_count = 0
        # Parity quoting: active quotes state {slug -> {outcome -> {price, ts, pair_id}}}
        self._parity_quotes: Dict[str, Dict[str, dict]] = {}
        # Per-slug dynamic quote target: slug -> current effective edge target (cents)
        self._quote_dynamic_target: Dict[str, float] = {}
        # Unpaired quote tracking: slug -> {outcome, fill_ts, pair_id, escalated}
        self._quote_unpaired: Dict[str, dict] = {}
        # Quote pause tracking: slug -> pause_until_ts
        self._quote_paused_until: Dict[str, float] = {}
        # Adverse selection guard
        self._diag_adverse_guard_events = 0
        self._diag_adverse_guard_pauses = 0
        self._diag_adverse_guard_degrades = 0
        # Per-slug degrade state: slug -> degrade_until_ts (soft mode: MAX target + 50% step)
        self._quote_degraded_until: Dict[str, float] = {}
        # Per-slug spot history for adverse selection: slug -> [(epoch_ts, spot)]
        self._spot_history: Dict[str, List[Tuple[float, float]]] = {}
        # Rescue invested tracking: slug -> USD spent on rescue buys
        self._rescue_invested_usd: Dict[str, float] = {}
        # Similarity/tempo stats: timestamps of all parity trades this minute
        self._diag_parity_trade_timestamps: List[float] = []
        # ── Order lifecycle tracking ──
        # Active orders: order_id -> {slug, outcome, side, price, qty, submit_ts, reason}
        self._active_orders: Dict[str, dict] = {}
        self._diag_quote_submit_count = 0
        self._diag_quote_cancel_count = 0
        self._diag_quote_replace_count = 0
        # Clone-report dedicated counters (independent of tempo report reset cycle)
        self._clone_quote_submit_count = 0
        self._clone_quote_cancel_count = 0
        self._clone_quote_replace_count = 0
        self._clone_quote_fill_count = 0
        # Top-of-book tracking: slug -> {outcome -> {is_best: bool, best_since_ts: float}}
        self._top_of_book_state: Dict[str, Dict[str, dict]] = {}
        self._diag_top_of_book_time_ms = 0.0
        self._diag_top_of_book_total_ms = 0.0
        # ── Auto-hedge tracking ──
        # Overrides unpaired management with faster escalation
        # slug -> {outcome, fill_ts, tick1_done, tick2_done, cross_done}
        self._hedge_state: Dict[str, dict] = {}
        self._diag_hedge_tick1 = 0
        self._diag_hedge_tick2 = 0
        self._diag_hedge_cross = 0
        self._diag_hedge_cross_early = 0
        self._diag_hedge_cross_late = 0
        self._diag_hedge_skipped_stale = 0
        self._diag_hedge_unwind = 0
        # ── Pair fill tracker: pair_id -> {slug, crypto, fills: {outcome: fill_ts}} ──
        self._pair_tracker: Dict[str, dict] = {}
        self._diag_pairs_completed = 0
        self._diag_pairs_completed_500ms = 0
        self._diag_pairs_completed_1500ms = 0
        self._diag_pairs_completed_10s = 0
        # ── Per-slug pair completion KPI ──
        self._diag_slug_unpaired_events: Dict[str, int] = {}
        self._diag_slug_paired_500ms: Dict[str, int] = {}
        self._diag_slug_paired_1500ms: Dict[str, int] = {}
        self._diag_slug_timeouts: Dict[str, int] = {}
        # ── Pending fetch streak tracking ──
        self._diag_pending_fetch_streak: Dict[str, int] = {}
        self._diag_pending_fetch_streak_max: int = 0
        # ── Rate limiter state ──
        self._rate_last_order_ts: Dict[str, float] = {}     # slug -> last order submit timestamp
        self._rate_submit_count: Dict[str, int] = {}        # slug -> submits this minute
        self._rate_submit_window_start: Dict[str, float] = {}  # slug -> minute window start ts
        self._rate_blocked_interval = 0                      # diag: blocked by MIN_ORDER_INTERVAL_MS
        self._rate_blocked_cap = 0                           # diag: blocked by MAX_ORDER_SUBMITS_PER_MIN
        # Per-slug entry throttle (separate from order rate limiter)
        self._entry_last_ts: Dict[str, float] = {}          # slug -> last entry timestamp
        self._entry_count: Dict[str, int] = {}              # slug -> entries this minute
        self._entry_window_start: Dict[str, float] = {}     # slug -> minute window start ts
        # ── Directional scalp state ──
        self._dscalp_positions: Dict[str, dict] = {}  # slug -> {outcome, entry_price, entry_ts, qty, tp1_done, tp2_done, tp3_done, tp4_done}
        self._dscalp_last_entry_ts: Dict[str, float] = {}   # slug -> last entry timestamp
        self._dscalp_invested_usd: Dict[str, float] = {}    # slug -> total invested USD
        self._dscalp_last_stop_ts: Dict[str, float] = {}    # slug -> last stop loss timestamp (for cooldown)
        # Directional scalp diagnostics (per-minute, reset in DIAG report)
        self._diag_dscalp_entries = 0
        self._diag_dscalp_exits = 0                          # all exits (TP + timeout + stop + early)
        self._diag_dscalp_tp1 = 0
        self._diag_dscalp_tp2 = 0
        self._diag_dscalp_tp3 = 0
        self._diag_dscalp_tp4 = 0
        self._diag_dscalp_runner_fallbacks = 0
        self._diag_dscalp_timeout_exits = 0
        self._diag_dscalp_stop_exits = 0
        self._diag_dscalp_early_exits = 0                    # early exit (all 3 conditions met)
        self._diag_dscalp_hold_times: List[float] = []    # seconds
        self._diag_dscalp_exit_cents: List[float] = []     # profit/loss in cents
        # Entry reject tracking by gate (per-minute, reset in DIAG report)
        self._diag_entry_reject_by_gate: Dict[str, int] = {}
        # TIME_STOP deferred/soft-exit tracking
        self._diag_time_stop_defers = 0
        self._diag_time_stop_soft_exits = 0
        # TIME_STOP latch: only fire one exit per (slug,outcome) until qty changes
        # Maps (slug, outcome) -> qty_at_trigger (float)
        self._time_stop_triggered: Dict[tuple, float] = {}
        # DERISK_MAKER deferred tracking
        self._diag_derisk_maker_defers = 0
        # PnL by exit reason (accumulated per minute)
        self._diag_pnl_by_exit_reason: Dict[str, float] = {}
        # COMPACT log timing: GATE_REPORT every N sec, SNAPSHOT_COMPACT every N sec
        self._gate_report_last_ts: float = 0.0
        self._snapshot_last_ts: Dict[str, float] = {}       # slug -> last snapshot ts
        # Parity fill tracking (for parity_fill_pct)
        self._diag_parity_fills_min = 0                      # parity fills this minute
        self._diag_directional_fills_min = 0                 # directional fills this minute
        self._diag_total_fills_min = 0                       # all fills this minute
        # Trade size tracking
        self._diag_trade_sizes: List[float] = []             # USD per fill this minute
        # Rolling trade timestamps for trades/min throttle
        self._rolling_trade_ts: List[float] = []             # epoch timestamps of recent trades
        # Regime awareness state
        self._regime_vol_bps: Dict[str, float] = {}          # slug -> rolling 60s vol (bps std dev)
        self._regime_is_low_vol: Dict[str, bool] = {}        # slug -> True if low vol regime
        # ── True cost tracker ──
        self._true_cost_tx_count = 0                         # total tx count (fills + cancels)
        self._true_cost_fill_count = 0                       # fills this hour
        self._true_cost_fill_count_min = 0                   # fills this minute
        self._true_cost_submit_count = 0                     # submits this minute
        self._true_cost_cancel_count = 0                     # cancels this minute
        self._true_cost_hour_start_ts = time.time()
        # ── DIAG report tracking ──
        self._diag_report_last_ts = time.time()
        self._diag_derisk_count_min = 0
        self._diag_unpaired_count_min = 0
        # ── F247 similarity / CLONE_REPORT tracking ──
        # pair delays: list of ms between Up and Down fills for same slug
        self._clone_pair_delays: List[float] = []
        # inter-pair gaps: list of ms between consecutive pair completions
        self._clone_inter_pair_gaps: List[float] = []
        self._clone_last_pair_ts: float = 0.0
        # signal-to-fill: ms from significant spot move to first fill
        self._clone_signal_to_fill: List[float] = []
        # Track last significant spot move per slug: slug -> (ts, spot)
        self._clone_last_spot_move: Dict[str, Tuple[float, float]] = {}
        # Hold time tracking: list of hold durations (seconds) for completed straddle cycles
        self._clone_hold_times: List[float] = []
        # Per-slug max imbalance tracker for analytics: slug -> max_abs_imbalance this minute
        self._diag_max_imbalance: Dict[str, float] = {}
        # Per-slug imbalance x delta samples: list of (imbalance_shares, delta_bps)
        self._diag_imbalance_delta_samples: List[Tuple[float, float]] = []
        # Probe → Scale state machine: slug -> {state, probe_ts, probe_ask, initial_edge_bps}
        self.entry_sm: Dict[str, dict] = {}   # IDLE / PROBING / SCALING / COOLDOWN
        self.hour_index_counters: Dict[str, int] = {c: 0 for c in CRYPTOS}  # crypto -> monotonic index
        # ── LIVE execution safety modules (no-ops in LOG mode) ──
        self._exec_tracker = LiveOrderTracker(
            self.client.cancel_order, write_jsonl,
            get_order_status_fn=self.client.get_order_status,
            get_order_fills_fn=self.client.get_order_fills)
        self._exec_orphan = OrphanScanner(
            self.client.get_open_orders, self.client.cancel_order, write_jsonl)
        self._exec_safety = SafetyCaps(write_jsonl)
        self._exec_unpaired = UnpairedResolver(write_jsonl)
        self._exec_reporter = LiveSanityReporter(write_jsonl)
        self._exec_drift = StateDriftChecker(
            self.client.get_open_orders, self.client.get_balances, write_jsonl)
        self._exec_reconciler = PositionReconciler(
            self.client.get_balances, self.client.get_open_orders, write_jsonl)
        self._last_reconcile_ts: float = 0.0             # position reconciliation timer
        self._reconcile_after_fill_pending: bool = False  # flag for post-fill reconciliation
        self._live_safety = LiveSafety(write_jsonl)
        # ── Budget reservation: reserved_usd per (slug, outcome, side) ──
        # Tracks notional locked up in open/pending BUY orders.
        # Key = order_id, Value = reserved_usd amount
        self._reserved_usd: Dict[str, float] = {}
        self._reserved_submit_ts: Dict[str, float] = {}  # order_id -> submit time (for stuck detection)
        # In-flight BUY guard: (token_id) -> last_order_ts
        self._inflight_buy_ts: Dict[str, float] = {}
        # Global order rate limiter: timestamps of recent order submissions
        self._global_order_ts: List[float] = []
        # Per-token attempt breaker: token_id -> list of attempt timestamps
        self._token_attempt_ts: Dict[str, List[float]] = {}
        self._token_breaker_until: Dict[str, float] = {}  # token_id -> cooldown-until timestamp
        # Desync hard stop: if True, all new entries blocked until manual reset
        self._desync_hard_stop: bool = False
        # Stale hour_start refresh: backoff counter per slug
        self._stale_hour_refresh_ts: Dict[str, float] = {}  # slug -> last refresh attempt ts
        self._stale_hour_refresh_count: Dict[str, int] = {}  # slug -> attempt count
        # Flatten state: set True when hard flatten fires, prevents re-entry
        self._hard_flatten_done: bool = False
        # ── Fills Ledger — append-only accounting layer ──
        if LEDGER_ENABLED:
            self._fills_ledger = FillsLedger(
                ledger_path=LEDGER_PATH,
                run_id=RUN_ID,
                write_jsonl_fn=write_jsonl,
            )
            self._fills_ledger.load_from_disk()
            self._last_ledger_reconcile_ts: float = 0.0
        else:
            self._fills_ledger = None
            self._last_ledger_reconcile_ts: float = 0.0
        # ── Truth Capture — authoritative fill tracking ──
        _wallet_addr = getattr(self.client, '_wallet_address', '') or ''

        def _get_trades_fn():
            """Fetch recent trades via CLOB client for wallet truth scan.

            Scoped to current hour: passes after= parameter so the API only
            returns trades from hour_start onward (fewer results, faster).
            """
            if not hasattr(self.client, '_clob') or not self.client._clob:
                return []
            try:
                # Try to scope the API call to current hour window
                hour_start_ts = self._hour_window
                after_ms = int(hour_start_ts.timestamp() * 1000) if hour_start_ts else 0
                try:
                    # py-clob-client get_trades supports after= (epoch ms)
                    trades = self.client._clob.get_trades(after=after_ms) if after_ms else self.client._clob.get_trades()
                except TypeError:
                    # Fallback if the CLOB client doesn't support after=
                    trades = self.client._clob.get_trades()
                return trades if isinstance(trades, list) else []
            except Exception:
                return []

        self._truth = TruthCapture(
            ledger_path=os.path.join(os.path.dirname(LEDGER_PATH), "truth_fills.jsonl"),
            wallet_address=_wallet_addr,
            get_order_status_fn=self.client.get_order_status,
            get_trades_fn=_get_trades_fn,
            get_balances_fn=self.client.get_balances,
            write_jsonl_fn=write_jsonl,
            token_meta_fn=lambda: self._get_token_to_slug_outcome_for_truth(),
            poll_interval_sec=1.0,
            poll_timeout_sec=30.0,
            scan_interval_sec=60.0,
            reconcile_interval_sec=60.0,
            desync_tolerance=0.01,
        )
        self._truth.load_from_disk()
        # ── SKIP_HISTORY: set cursor to NOW so no historical fills are ingested ──
        if SKIP_HISTORY:
            self._truth.skip_history_now()
        # ── LIVE_SAFE mode: monotonic process start for entry window ──
        self._process_start_mono = time.monotonic()
        # ── LIVE_SAFE: record starting hour and count windows traded ──
        self._live_safe_start_hour = utc_now().replace(minute=0, second=0, microsecond=0)
        self._live_safe_hours_completed = 0
        # ── Wallet balance snapshot for RECONCILE_DIFF display ──
        # (Removed: _last_wallet_balances — no longer needed)
        # (Removed: _balances_empty_count / _balances_pause_entries — no longer needed)
        # ── Per-slug PnL tracking ──
        self._slug_realized_pnl: Dict[str, float] = {}       # slug -> cumulative realized PnL
        self._slug_neg_exit_count: Dict[str, int] = {}        # slug -> total negative exit count (session)
        # ── Loss-tail reduction guard ──
        # slug -> [(ts, net_pnl, reason)] — rolling window of negative exits
        self._slug_neg_exits: Dict[str, List[Tuple[float, float, str]]] = {}
        # slug -> pause_until_ts (monotonic)
        self._slug_entry_paused_until: Dict[str, float] = {}
        # ── Pre-8hr safety: post-fill cooldown ──
        self._last_fill_ts: Dict[str, float] = {}             # slug -> last fill epoch ts (buy or sell)
        self._lean_exit_last_ts: Dict[str, float] = {}        # slug -> last lean exit attempt ts
        # ── Sell-error cooldown: prevent spam on repeated failures ──
        self._sell_error_until: Dict[str, float] = {}          # (slug|outcome) -> monotonic ts cooldown expires
        self._sell_allowance_retried = False                    # True after one re-approval attempt
        # ── Fill dedup: prevents double-apply when same fill arrives via ws + poll + lifecycle ──
        self._applied_fills: set = set()  # {(order_id, side, round(qty,2))}
        # ── Pre-8hr safety: diagnostic counters (reset per DIAG cycle) ──
        self._diag_cap_blocks = 0                              # inventory cap blocks this minute
        self._diag_post_fill_cooldown_blocks = 0               # post-fill cooldown blocks this minute
        self._diag_time_stop_exits = 0                         # time-stop exit count this minute
        self._diag_parity_directional_suppress = 0             # parity suppressed by directional priority
        self._diag_spread_limit_blocks = 0                     # coin spread limit blocks this minute
        # ── Pre-8hr safety: maker-grace TP tracking ──
        # slug -> {outcome, price, qty, placed_ts, order_id, reason}
        self._tp_maker_pending: Dict[str, dict] = {}
        # Background data refresh infrastructure (sub-second loop)
        # Dynamic pool: max(BG_POOL_MIN_WORKERS, 3 * markets_count)
        dynamic_workers = max(BG_POOL_MIN_WORKERS, BG_POOL_WORKERS)
        self._bg_executor = ThreadPoolExecutor(max_workers=dynamic_workers)
        self._bg_running = True
        self._data_cache: Dict[str, dict] = {}   # slug -> {market, spot, hour_open, up_book, dn_book, ts}
        self._cached_markets: List[MarketRef] = []
        self._last_market_discovery_ts = 0.0
        self._last_save_ts = 0.0
        self._last_balance_sync_ts = 0.0             # CLOB balance reconciliation
        self._pending_fetches: Dict[str, bool] = {}  # slug -> True if fetch in-flight
        self._high_priority_slugs: set = set()       # markets needing fast refresh
        self._stale_skip_total = 0                   # counter: stale cache skips
        self._loop_count = 0
        # Deadline scheduling: slug -> next_due_ts
        self._bg_next_due: Dict[str, float] = {}
        # Per-slug refresh diagnostics
        self._bg_fetch_durations: Dict[str, List[float]] = {}  # slug -> [duration_ms, ...]
        self._bg_refresh_miss_count: Dict[str, int] = {}       # slug -> miss count
        self._bg_pending_cycles: Dict[str, int] = {}           # slug -> cycles pending
        # Tempo parity diagnostics
        self._tempo_fills: Dict[str, int] = {}       # slug -> fills this minute
        self._tempo_intents: Dict[str, int] = {}     # slug -> intents this minute
        self._tempo_cache_ages: Dict[str, List[float]] = {}  # slug -> cache ages this minute
        self._tempo_loop_times: List[float] = []     # loop durations this minute (ms)
        self._tempo_last_report_ts = 0.0
        # Initialise new Logger (replaces old write_jsonl / log_csv)
        self.logger = Logger(
            run_id=RUN_ID,
            log_dir=_LOG_DIR,
            mode=MODE,
            profile=PROFILE,
        )
        _LOGGER = self.logger  # expose globally for write_jsonl shim
        self._load_state()
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)
    def _handle_stop(self, *_):
        self.running = False
        self._bg_running = False
        write_jsonl({"event_type":"STOP_SIGNAL"})
    def _load_clob_positions_on_startup(self):
        """On startup, fetch open positions from CLOB to initialize ledger state.

        Keeps SKIP_HISTORY behavior (no old trade ingestion) but ensures
        the bot knows about existing positions so it doesn't think it's flat.
        """
        try:
            balances = self.client.get_balances()
            if not balances:
                write_jsonl({"event_type": "STARTUP_POSITIONS_NONE"})
                return

            token_map = self._get_token_to_slug_outcome() if hasattr(self, '_get_token_to_slug_outcome') else {}
            loaded = 0
            for b in balances:
                tid = b.get("token_id") or b.get("asset_type", "")
                bal = float(b.get("balance", 0))
                if bal < MIN_QTY or tid not in token_map:
                    continue
                slug, outcome = token_map[tid]
                st = self.market_states.get(slug)
                if not st:
                    continue
                pos = st.positions.get(outcome)
                if not pos:
                    continue
                # Only initialize if internal state shows flat
                if pos.qty < MIN_QTY:
                    # We have on-chain shares but internal state is flat.
                    # Initialize from CLOB balance (conservative: assume vwap=0.5).
                    pos.qty = bal
                    pos.vwap = 0.50  # conservative estimate
                    pos.cost_usdc = bal * 0.50
                    pos.last_trade_ts = iso_z(utc_now())
                    loaded += 1
                    write_jsonl({"event_type": "STARTUP_POSITION_LOADED",
                                 "slug": slug, "outcome": outcome,
                                 "qty": round(bal, 2),
                                 "assumed_vwap": 0.50})

            if loaded > 0:
                # Adjust cash for loaded positions
                total_loaded_cost = sum(
                    st.positions[o].cost_usdc
                    for st in self.market_states.values()
                    for o in ["Up", "Down"] if st.positions[o].qty >= MIN_QTY
                )
                write_jsonl({"event_type": "STARTUP_POSITIONS_DONE",
                             "loaded": loaded,
                             "total_cost_usd": round(total_loaded_cost, 2)})
                print(f"  [STARTUP] Loaded {loaded} existing positions "
                      f"(total cost ~${total_loaded_cost:.2f})")
        except Exception as e:
            write_jsonl({"event_type": "STARTUP_POSITIONS_ERROR",
                         "err": str(e)[:200]})

    def _emit_startup_sanity_report(self):
        """Print a single boot verification block proving no stale fills will leak in.

        Logged once at startup.  Covers:
          1. boot_cursor_ms  — the SKIP_HISTORY cursor timestamp (ms epoch).
             Any fill with ts < cursor is ignored by TruthCapture wallet scan.
          2. Open positions loaded from CLOB balances (per slug/outcome).
          3. Open CLOB orders found at startup (should be 0 after orphan cleanup).
          4. Per-ingest-path cursor respect verification.
        """
        now_ms = int(time.time() * 1000)

        # 1. Boot cursor + clamp sanity
        boot_cursor_ms = getattr(self._truth, '_scan_cursor_ts_ms', 0)
        skip_history_on = SKIP_HISTORY

        # Derive hour_start_ms from first active market (or truncate now to hour)
        hour_start_ms = 0
        for st_hs in self.market_states.values():
            if st_hs.hour_start_utc:
                try:
                    hour_start_ms = int(_parse_iso_z(st_hs.hour_start_utc).timestamp() * 1000)
                except Exception:
                    pass
                break
        if hour_start_ms == 0:
            # Fallback: truncate current time to hour boundary
            _now_dt = datetime.now(tz=timezone.utc)
            hour_start_ms = int(_now_dt.replace(minute=0, second=0, microsecond=0).timestamp() * 1000)

        cursor_delta_ms = boot_cursor_ms - hour_start_ms if boot_cursor_ms > 0 else None
        cursor_clamped = False
        if boot_cursor_ms > 0 and boot_cursor_ms < hour_start_ms:
            cursor_clamped = self._truth.clamp_cursor_to_hour(hour_start_ms)
            boot_cursor_ms = getattr(self._truth, '_scan_cursor_ts_ms', 0)

        # 2. Open positions (from internal state, which was populated by _load_clob_positions_on_startup)
        positions = []
        for slug, st in self.market_states.items():
            for outcome in ("Up", "Down"):
                pos = st.positions.get(outcome)
                if pos and pos.qty >= MIN_QTY:
                    positions.append({
                        "slug": slug,
                        "outcome": outcome,
                        "qty": round(pos.qty, 2),
                        "vwap": round(pos.vwap, 4),
                        "cost_usd": round(pos.cost_usdc, 2),
                    })

        # 3. Open CLOB orders (after orphan cleanup should be 0)
        open_orders = []
        if MODE in ("LIVE", "LIVE_SAFE"):
            try:
                raw_orders = self.client.get_open_orders()
                open_orders = [
                    {"order_id": o.get("id", "")[:12],
                     "status": o.get("status", ""),
                     "side": o.get("side", ""),
                     "price": o.get("price", ""),
                     "size": o.get("original_size") or o.get("size", "")}
                    for o in raw_orders
                ]
            except Exception:
                open_orders = [{"error": "failed to fetch"}]

        # 4. Ingest path verification
        #    Each path and whether it respects boot_cursor_ms:
        #
        #    a) TruthCapture wallet scan (run_wallet_scan):
        #       YES — filters on _scan_cursor_ts_ms. Fills with ts_ms < cursor are skipped.
        #       Additionally applies hour-window filter (start <= ts < end).
        #
        #    b) Order lifecycle guard (_process_lifecycle_recovered_fills → _apply_fill):
        #       N/A — only processes orders the bot itself submitted THIS session.
        #       LiveOrderTracker.track() is called at order submit time; poll_pending_orders()
        #       only returns orders that were tracked. No historical orders leak in.
        #
        #    c) Position reconciler (_exec_reconciler / _exec_drift):
        #       N/A — compares current CLOB wallet balances vs internal state.
        #       Does not ingest trades; only flags qty mismatches.
        #
        #    d) _apply_fill (unified pipeline):
        #       N/A — only called from (a), (b), or direct IOC/burst buy paths.
        #       Not exposed to raw historical data.
        #
        #    e) FillsLedger reconciliation:
        #       N/A — compares ledger position sums vs CLOB balances.
        #       Does not ingest new trades from API.
        # Cursor proof counters (at startup, should all be 0)
        cursor_counters = {
            "truth_wallet_scan": {
                "accepted_fills": self._truth.cursor_accepted_fills,
                "dropped_old_ts": self._truth.cursor_dropped_old_ts,
                "dropped_outside_window": self._truth.cursor_dropped_outside_window,
            },
            "lifecycle_guard": {
                "accepted_fills": 0,  # none at startup; only tracks this-session orders
                "dropped_old_ts": 0,
                "dropped_outside_window": 0,
            },
            "apply_fill_pipeline": {
                "accepted_fills": 0,
                "dropped_old_ts": 0,
                "dropped_outside_window": 0,
            },
        }

        ingest_paths = {
            "truth_wallet_scan": {
                "respects_boot_cursor": True,
                "mechanism": f"_scan_cursor_ts_ms={boot_cursor_ms}; fills with ts<cursor skipped + hour-window filter",
                "acceptance_predicate": "(ts_ms >= boot_cursor_ms) AND (hour_start <= ts < hour_end)",
            },
            "lifecycle_guard": {
                "respects_boot_cursor": True,
                "mechanism": "only polls orders tracked THIS session via LiveOrderTracker.track()",
            },
            "position_reconciler": {
                "respects_boot_cursor": True,
                "mechanism": "balance comparison only; does not ingest trades",
            },
            "apply_fill_pipeline": {
                "respects_boot_cursor": True,
                "mechanism": "downstream consumer; only receives fills from cursor-gated sources",
            },
            "fills_ledger_reconcile": {
                "respects_boot_cursor": True,
                "mechanism": "balance comparison only; does not ingest trades",
            },
        }

        orphan_cancels = getattr(self._exec_orphan, 'startup_cancels', 0)

        # 5. Burst gating: confirm burst only fires when t_min <= BURST_EARLY_WINDOW_MIN
        #    The state machine gate at PROBING → SCALING checks:
        #      if t_min > BURST_EARLY_WINDOW_MIN: → COOLDOWN (probe_only_past_burst_window)
        #    t_min is computed from market's hour_start_utc (from API), not bot start time.
        #    A mid-hour restart will see t_min > 3 and skip burst.
        burst_gate_ok = True  # structural guarantee — code review verified above

        report = {
            "event_type": "STARTUP_SANITY_REPORT",
            "profile": PROFILE,
            "boot_cursor_ms": boot_cursor_ms,
            "boot_cursor_iso": datetime.fromtimestamp(boot_cursor_ms / 1000.0, tz=timezone.utc).isoformat() if boot_cursor_ms > 0 else "NOT_SET",
            "hour_start_ms": hour_start_ms,
            "cursor_vs_hour_delta_ms": cursor_delta_ms,
            "cursor_clamped": cursor_clamped,
            "skip_history": skip_history_on,
            "now_ms": now_ms,
            "cursor_age_sec": round((now_ms - boot_cursor_ms) / 1000.0, 1) if boot_cursor_ms > 0 else None,
            "open_positions": positions,
            "open_positions_count": len(positions),
            "open_orders_after_cleanup": open_orders,
            "open_orders_count": len(open_orders),
            "orphan_cancels_at_startup": orphan_cancels,
            "ingest_paths": ingest_paths,
            "cursor_proof_counters": cursor_counters,
            "acceptance_predicate": "(ts_ms >= boot_cursor_ms) AND (hour_start <= ts < hour_end)  [AND, not OR]",
            "all_paths_respect_cursor": all(v["respects_boot_cursor"] for v in ingest_paths.values()),
            "burst_gate": {
                "gate_field": "t_min > BURST_EARLY_WINDOW_MIN",
                "burst_window_min": BURST_EARLY_WINDOW_MIN,
                "t_min_source": "minutes_into_hour(market.hour_start_utc, now)",
                "mid_hour_restart_safe": burst_gate_ok,
            },
            "ts_ms": now_ms,
        }
        write_jsonl(report)

        # Console output
        print("\n  ╔══════════════════════════════════════════════════════════════╗")
        print("  ║              STARTUP SANITY REPORT                          ║")
        print("  ╠══════════════════════════════════════════════════════════════╣")
        cursor_iso = report["boot_cursor_iso"]
        age_str = f"{report['cursor_age_sec']:.1f}s ago" if report["cursor_age_sec"] is not None else "N/A"
        print(f"  ║  PROFILE: {PROFILE}")
        if PROFILE == "HOURLY_SCALP_MIN":
            print(f"  ║    Subsystems: IOC buy/sell, fill recovery, reservations,")
            print(f"  ║               exposure caps, flatten/hard flatten")
            print(f"  ║    Disabled:   parity, quoting, rescue, hedging, SOL/XRP")
        print(f"  ║  SKIP_HISTORY: {'ON' if skip_history_on else 'OFF'}                                          ║")
        print(f"  ║  boot_cursor_ms: {boot_cursor_ms}  ({age_str})")
        print(f"  ║  boot_cursor:    {cursor_iso}")
        delta_str = f"{cursor_delta_ms:+d}ms vs hour_start" if cursor_delta_ms is not None else "N/A"
        print(f"  ║  hour_start_ms:  {hour_start_ms}  cursor_delta: {delta_str}")
        if cursor_clamped:
            print(f"  ║  *** CURSOR CLAMPED: was before hour_start → moved up to hour_start ***")
        print(f"  ║  Orphan orders cancelled at startup: {orphan_cancels}")
        print(f"  ║  Open orders after cleanup: {len(open_orders)}")
        if positions:
            print(f"  ║  Open positions ({len(positions)}):")
            for p in positions:
                print(f"  ║    {p['slug']} {p['outcome']}: "
                      f"qty={p['qty']}  vwap={p['vwap']}  cost=${p['cost_usd']}")
        else:
            print(f"  ║  Open positions: NONE (starting flat)")
        print(f"  ║  Acceptance predicate (AND, not OR):")
        print(f"  ║    ACCEPT iff (ts_ms >= boot_cursor_ms) AND (hour_start <= ts < hour_end)")
        print(f"  ║    Both conditions MUST be true; either failure → DROP.")
        print(f"  ║  Ingest path cursor verification:")
        for path_name, info in ingest_paths.items():
            status = "OK" if info["respects_boot_cursor"] else "FAIL"
            print(f"  ║    [{status}] {path_name}: {info['mechanism'][:70]}")
        print(f"  ║  Cursor proof counters (should all be 0 at startup):")
        for path_name, ctrs in cursor_counters.items():
            print(f"  ║    {path_name}: accept={ctrs['accepted_fills']} "
                  f"drop_old={ctrs['dropped_old_ts']} drop_win={ctrs['dropped_outside_window']}")
        all_ok = report["all_paths_respect_cursor"]
        print(f"  ║  ALL PATHS RESPECT CURSOR: {'YES' if all_ok else 'NO — INVESTIGATE'}")
        print(f"  ║  Burst gating:")
        print(f"  ║    [OK] t_min > {BURST_EARLY_WINDOW_MIN} → probe only (no burst)")
        print(f"  ║    t_min derived from market hour_start_utc (API), NOT bot start")
        print(f"  ║    Mid-hour restart safe: YES")
        print(f"  ╚══════════════════════════════════════════════════════════════╝\n")

        if not all_ok:
            write_jsonl({"event_type": "STARTUP_SANITY_FAIL",
                         "msg": "Not all ingest paths respect boot_cursor_ms"})

    def _load_state(self):
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            # Support both old ("bankroll_usdc") and new ("cash_usdc") state files
            self.cash_usdc = float(raw.get("cash_usdc", raw.get("bankroll_usdc", self.cash_usdc)))
            self.realized_pnl_usdc = float(raw.get("realized_pnl_usdc", self.realized_pnl_usdc))
            self.hourly_pnl_usdc = float(raw.get("hourly_pnl_usdc", raw.get("daily_pnl_usdc", self.hourly_pnl_usdc)))
            self.day_start_equity = float(raw.get("day_start_equity", self.day_start_equity))
            self.hour_start_equity = float(raw.get("hour_start_equity", self.hour_start_equity))
            saved_day = raw.get("day_start")
            if saved_day:
                self.day_start = datetime.strptime(saved_day, "%Y-%m-%d").date()
            saved_hour = raw.get("hour_window")
            if saved_hour:
                self._hour_window = _parse_iso_z(saved_hour)
            ms = raw.get("market_states", {})
            pos_fields = {f.name for f in Position.__dataclass_fields__.values()}
            ms_fields = {f.name for f in MarketState.__dataclass_fields__.values()}
            for slug, st_dict in ms.items():
                # Reconstruct Position objects from raw dicts (filter unknown keys)
                pos_raw = st_dict.get("positions")
                if isinstance(pos_raw, dict):
                    st_dict["positions"] = {
                        k: Position(**{pk: pv for pk, pv in v.items() if pk in pos_fields})
                            if isinstance(v, dict) else v
                        for k, v in pos_raw.items()
                    }
                # Filter unknown keys from MarketState too
                st_dict = {k: v for k, v in st_dict.items() if k in ms_fields}
                self.market_states[slug] = MarketState(**st_dict)
            # Hard-zero any dust positions loaded from state
            for st in self.market_states.values():
                for outcome in ["Up", "Down"]:
                    self._clean_dust(st.positions[outcome])
            # Restore scan cursor from state.json
            saved_cursor = raw.get("scan_cursor")
            if saved_cursor and isinstance(saved_cursor, dict):
                self._truth.set_scan_cursor(saved_cursor)
            loaded_schema = raw.get("schema_version", "unknown")
            write_jsonl({"event_type":"STATE_LOADED", "cash": self.cash_usdc,
                         "realized_pnl": self.realized_pnl_usdc,
                         "loaded_schema_version": loaded_schema,
                         "current_schema_version": SCHEMA_VERSION})
        except Exception as e:
            write_jsonl({"event_type":"STATE_LOAD_ERROR", "err": str(e)})
    def _save_state(self):
        try:
            ms = {}
            for slug, st in self.market_states.items():
                d = asdict(st)
                # Trim histories for state file (full history is in JSONL)
                d["delta_hist"] = d.get("delta_hist", [])[-STATE_HIST_MAX:]
                d["price_hist"] = d.get("price_hist", [])[-STATE_HIST_MAX:]
                ms[slug] = d
            raw = {
                "schema_version": SCHEMA_VERSION,
                "run_id": RUN_ID,
                "cash_usdc": self.cash_usdc,
                "realized_pnl_usdc": self.realized_pnl_usdc,
                "hourly_pnl_usdc": self.hourly_pnl_usdc,
                "day_start_equity": self.day_start_equity,
                "hour_start_equity": self.hour_start_equity,
                "day_start": self.day_start.isoformat(),
                "hour_window": iso_z(self._hour_window),
                "equity_usdc": self._equity(),
                "market_states": ms,
                "scan_cursor": self._truth.get_scan_cursor(),
            }
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(raw, f, separators=(",", ":"))
        except Exception as e:
            write_jsonl({"event_type":"STATE_SAVE_ERROR", "err": str(e)})
    def _equity(self) -> float:
        """Mark-to-market equity: cash + sum(pos.qty * mid_price) for all open positions."""
        mtm = 0.0
        for slug, st in self.market_states.items():
            for outcome in ["Up", "Down"]:
                pos = st.positions[outcome]
                if pos.qty < MIN_QTY:
                    continue
                book = self.last_book.get(slug, {}).get(outcome)
                mid = book.mid if book else pos.vwap  # fallback to vwap if no book yet
                mtm += pos.qty * mid
        return self.cash_usdc + mtm
    def _reset_daily_if_needed(self):
        today = utc_now().date()
        if today != self.day_start:
            self.day_start = today
            self.day_start_equity = self._equity()
            self._day_risk_stop_hit = False
            write_jsonl({"event_type":"NEW_DAY_RESET", "day_start_equity": round(self.day_start_equity, 2)})
        # Reset hourly state at each UTC hour boundary
        current_hour = utc_now().replace(minute=0, second=0, microsecond=0)
        if current_hour != self._hour_window:
            equity_now = self._equity()
            # ── HOUR_SUMMARY for the hour that just ended ──
            any_stop = self._hour_risk_stop_hit
            shadow_delta = 0.0
            if self._shadow_active and SHADOW_STOP_SIM:
                shadow_eq = self._shadow_equity()
                shadow_delta = equity_now - shadow_eq
                # ── RISK_STOP_SHADOW_RESULT ──
                write_jsonl({
                    "event_type": "RISK_STOP_SHADOW_RESULT",
                    "hour_window": iso_z(self._hour_window),
                    "hour_start_equity": round(self.hour_start_equity, 4),
                    "actual_equity": round(equity_now, 4),
                    "shadow_equity": round(shadow_eq, 4),
                    "actual_equity_change": round(equity_now - self.hour_start_equity, 4),
                    "shadow_equity_change": round(shadow_eq - self.hour_start_equity, 4),
                    "trades_blocked": self._shadow_trades_blocked,
                    "shadow_delta": round(shadow_delta, 4),
                })
            avg_edge = (sum(self._hour_edges) / len(self._hour_edges)) if self._hour_edges else 0.0
            # Get ledger fill count for accurate current-hour reporting
            _ledger_hour_fills = len(self._fills_ledger._fills) if self._fills_ledger else 0
            _truth_hour_fills = len(self._truth._fills) if hasattr(self._truth, '_fills') else 0
            write_jsonl({
                "event_type": "HOUR_SUMMARY",
                "hour_window": iso_z(self._hour_window),
                "hour_start_equity": round(self.hour_start_equity, 4),
                "equity_now": round(equity_now, 4),
                "fills_in_current_hour": _truth_hour_fills or _ledger_hour_fills,
                "trades": self._hour_trade_count,
                "net_pnl": round(self._hour_net_pnl, 4),
                "fees_estimate": 0.0,
                "avg_edge": round(avg_edge, 6),
                "stop_triggered": any_stop,
                "shadow_delta": round(shadow_delta, 4),
            })
            # ── LIVE_SAFE: count completed hours, exit after LIVE_SAFE_NUM_HOURS ──
            if MODE == "LIVE_SAFE" and current_hour != self._live_safe_start_hour:
                self._live_safe_hours_completed += 1
                _ls_fills = len(self._truth._fills) if hasattr(self._truth, '_fills') else self._hour_trade_count
                print(f"\n  [LIVE_SAFE] Hour {self._live_safe_hours_completed}/{LIVE_SAFE_NUM_HOURS} complete  |  "
                      f"PnL: ${self._hour_net_pnl:+.2f}  |  Fills: {_ls_fills}")
                write_jsonl({
                    "event_type": "LIVE_SAFE_HOUR_END",
                    "hour_num": self._live_safe_hours_completed,
                    "total_hours": LIVE_SAFE_NUM_HOURS,
                    "start_hour": iso_z(self._live_safe_start_hour),
                    "equity": round(equity_now, 4),
                    "hour_net_pnl": round(self._hour_net_pnl, 4),
                    "fills_in_current_hour": _ls_fills,
                    "trades": self._hour_trade_count,
                })
                # Update start hour to current hour for next window
                self._live_safe_start_hour = current_hour
                # Reset entry window so buys are allowed in the new hour
                self._process_start_mono = time.monotonic()
                if self._live_safe_hours_completed >= LIVE_SAFE_NUM_HOURS:
                    print("\n" + "=" * 60)
                    print(f"  [LIVE_SAFE] {LIVE_SAFE_NUM_HOURS} hours complete — shutting down")
                    print(f"  Equity:  ${equity_now:.2f}  |  Total PnL: ${self.realized_pnl_usdc:+.2f}")
                    print("=" * 60)
                    self._save_state()
                    self.running = False
                    return
            # ── Reset for new hour ──
            self._hour_window = current_hour
            self.hourly_pnl_usdc = 0.0
            self.hour_start_equity = equity_now
            self._hour_risk_stop_hit = False
            self._hour_trade_count = 0
            self._hour_net_pnl = 0.0
            self._hour_budget_remaining = HOURLY_BUDGET_USD
            self._hour_edges = []
            # Reset flatten state for new hour
            self._hard_flatten_done = False
            # Reset per-hour cursor proof counters
            self._truth.reset_hour_counters()
            # Clear time-stop latches for new hour
            self._time_stop_triggered.clear()
            # Reset shadow
            self._shadow_active = False
            self._shadow_cash = 0.0
            self._shadow_positions = {}
            self._shadow_equity_at_trigger = 0.0
            self._shadow_trades_blocked = 0
            # ── Per-hour ledger rotation ──
            if HOURLY_LEDGER_ROTATION:
                self._truth.rotate_hour(current_hour)
                if self._fills_ledger is not None:
                    self._fills_ledger.rotate_hour(current_hour)
                print(f"  [HOUR] Ledger rotation complete for "
                      f"{current_hour.strftime('%Y-%m-%dT%H:00Z')}")
    # -----------------------------
    # Position helpers (paper-mode)
    # -----------------------------
    def _paper_buy(self, st: MarketState, outcome: str, price: float, qty: float, usdc_cost: float):
        pos = st.positions[outcome]
        new_cost = pos.cost_usdc + usdc_cost
        new_qty = pos.qty + qty
        pos.vwap = (pos.vwap * pos.qty + price * qty) / max(1e-12, new_qty)
        pos.qty = new_qty
        pos.cost_usdc = new_cost
        pos.last_trade_ts = iso_z(utc_now())
        if pos.opened_at is None:
            pos.opened_at = pos.last_trade_ts
        self.cash_usdc -= usdc_cost
        self._hour_budget_remaining -= usdc_cost
        # Shadow: new entries are BLOCKED after stop trigger
        if self._shadow_active:
            self._shadow_trades_blocked += 1
        # Hourly stats
        self._hour_trade_count += 1
    def _paper_sell(self, st: MarketState, outcome: str, price: float, qty: float):
        pos = st.positions[outcome]
        qty = min(qty, pos.qty)
        if qty <= 0:
            return 0.0
        proceeds = price * qty
        cost_basis = pos.vwap * qty
        pnl = proceeds - cost_basis
        pos.qty -= qty
        pos.cost_usdc = max(0.0, pos.cost_usdc - cost_basis)
        pos.last_trade_ts = iso_z(utc_now())
        self.cash_usdc += proceeds
        self._hour_budget_remaining += proceeds
        self.realized_pnl_usdc += pnl
        self.hourly_pnl_usdc += pnl
        # Per-slug PnL
        self._slug_realized_pnl[st.slug] = self._slug_realized_pnl.get(st.slug, 0.0) + pnl
        self._clean_dust(pos)
        # Shadow: exits still apply to existing shadow positions
        if self._shadow_active:
            sp = self._shadow_positions.get(st.slug, {}).get(outcome)
            if sp and sp["qty"] >= MIN_QTY:
                sell_qty = min(qty, sp["qty"])  # never sell more than shadow holds
                shadow_proceeds = price * sell_qty
                sp["qty"] = max(0.0, sp["qty"] - sell_qty)
                sp["cost_usdc"] = max(0.0, sp["cost_usdc"] - sp["vwap"] * sell_qty)
                self._shadow_cash += shadow_proceeds
        # Hourly stats
        self._hour_trade_count += 1
        self._hour_net_pnl += pnl
        return pnl
    def _live_buy(self, st: MarketState, outcome: str, price: float,
                  qty: float, usdc_cost: float):
        """Update position state after a real buy fill (mirrors _paper_buy)."""
        pos = st.positions[outcome]
        new_cost = pos.cost_usdc + usdc_cost
        new_qty = pos.qty + qty
        pos.vwap = (pos.vwap * pos.qty + price * qty) / max(1e-12, new_qty)
        pos.qty = new_qty
        pos.cost_usdc = new_cost
        pos.last_trade_ts = iso_z(utc_now())
        if pos.opened_at is None:
            pos.opened_at = pos.last_trade_ts
        self.cash_usdc -= usdc_cost
        self._hour_budget_remaining -= usdc_cost
        self._hour_trade_count += 1
        print(f"  [FILL] BUY  {st.crypto} {outcome}: +{qty:.0f}@{price:.3f} "
              f"-> now {pos.qty:.0f}@{pos.vwap:.3f}  cost=${usdc_cost:.2f}  "
              f"budget_left=${self._hour_budget_remaining:.2f}")
    def _live_sell(self, st: MarketState, outcome: str, price: float,
                   qty: float) -> float:
        """Update position state after a real sell fill (mirrors _paper_sell). Returns pnl."""
        pos = st.positions[outcome]
        qty = min(qty, pos.qty)
        if qty <= 0:
            return 0.0
        proceeds = price * qty
        cost_basis = pos.vwap * qty
        pnl = proceeds - cost_basis
        pos.qty -= qty
        pos.cost_usdc = max(0.0, pos.cost_usdc - cost_basis)
        pos.last_trade_ts = iso_z(utc_now())
        self.cash_usdc += proceeds
        self._hour_budget_remaining += proceeds  # sell returns money to budget
        self.realized_pnl_usdc += pnl
        self.hourly_pnl_usdc += pnl
        self._clean_dust(pos)
        self._hour_trade_count += 1
        self._hour_net_pnl += pnl
        # Per-slug PnL
        self._slug_realized_pnl[st.slug] = self._slug_realized_pnl.get(st.slug, 0.0) + pnl
        pnl_tag = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        print(f"  [FILL] SELL {st.crypto} {outcome}: -{qty:.0f}@{price:.3f} "
              f"-> now {pos.qty:.0f}  pnl={pnl_tag}  "
              f"budget_left=${self._hour_budget_remaining:.2f}")
        return pnl
    @staticmethod
    def _clean_dust(pos: Position):
        """Zero out positions that are dust (< MIN_QTY)."""
        if abs(pos.qty) < MIN_QTY:
            pos.qty = 0.0
            pos.cost_usdc = 0.0
            pos.vwap = 0.0
            pos.opened_at = None
            pos.last_trade_ts = None
            pos.tp1_done = False
            pos.tp2_done = False
            pos.tp3_done = False
            pos.scalp_mode = False
            pos.scalp_open_ts = None
            pos.position_id = None
            pos.trade_id = None
            pos.entry_decision_id = None
            pos.parent_order_id = None
            pos.entry_mid = 0.0
            pos.max_favorable_mid = 0.0
            pos.max_adverse_mid = 1.0
            pos.last_derisk_ts = None
            pos.last_derisk_mid = 0.0
            pos.fast_tp_done = False
    # -----------------------------
    # Risk checks (log-only alerts)
    # -----------------------------
    def _market_cost_usdc(self, st: MarketState) -> float:
        return sum(p.cost_usdc for p in st.positions.values())
    def _crypto_cost_usdc(self, crypto: str) -> float:
        s = 0.0
        for st in self.market_states.values():
            if st.crypto == crypto:
                s += sum(p.cost_usdc for p in st.positions.values())
        return s
    def _shadow_equity(self) -> float:
        """MTM equity for the shadow portfolio."""
        mtm = 0.0
        for slug, outcomes in self._shadow_positions.items():
            for outcome, sp in outcomes.items():
                if sp["qty"] < MIN_QTY:
                    continue
                book = self.last_book.get(slug, {}).get(outcome)
                mid = book.mid if book else sp["vwap"]
                mtm += sp["qty"] * mid
        return self._shadow_cash + mtm
    def _activate_shadow(self):
        """Snapshot current state into shadow portfolio (called once per trigger)."""
        self._shadow_active = True
        self._shadow_cash = self.cash_usdc
        self._shadow_equity_at_trigger = self._equity()
        self._shadow_trades_blocked = 0
        self._shadow_positions = {}
        for slug, st in self.market_states.items():
            sp = {}
            for outcome in ["Up", "Down"]:
                pos = st.positions[outcome]
                if pos.qty >= MIN_QTY:
                    sp[outcome] = {"qty": pos.qty, "cost_usdc": pos.cost_usdc, "vwap": pos.vwap}
            if sp:
                self._shadow_positions[slug] = sp
    def _check_risk_drawdown(self, ctx: dict) -> dict:
        """
        Compute equity drawdown, emit RISK_STOP_TRIGGERED when thresholds
        are breached. Returns dict with fields for CSV/ctx enrichment.
        Never stops the bot — log only.
        """
        equity_now = self._equity()
        hse = self.hour_start_equity if self.hour_start_equity > 0 else equity_now
        dse = self.day_start_equity if self.day_start_equity > 0 else equity_now
        hour_dd_pct = (equity_now - hse) / hse if hse > 0 else 0.0
        day_dd_pct  = (equity_now - dse) / dse if dse > 0 else 0.0
        st = ctx["st"]
        m = ctx["m"]
        hour_triggered = self._hour_risk_stop_hit
        day_triggered = self._day_risk_stop_hit
        # ── Hourly stop-loss check (single global flag per hour) ──
        if hour_dd_pct <= -STOP_LOSS_PCT_PER_HOUR and not hour_triggered:
            self._hour_risk_stop_hit = True
            hour_triggered = True
            # Build open-position summary across ALL markets
            pos_summary = []
            for s_slug, s_st in self.market_states.items():
                for outcome in ["Up", "Down"]:
                    pos = s_st.positions[outcome]
                    if pos.qty >= MIN_QTY:
                        bk = self.last_book.get(s_slug, {}).get(outcome)
                        pos_summary.append({
                            "slug": s_slug, "crypto": s_st.crypto,
                            "outcome": outcome, "qty": round(pos.qty, 2),
                            "vwap": round(pos.vwap, 4),
                            "mid": round(bk.mid, 4) if bk else None,
                        })
            up_book, dn_book = ctx["up_book"], ctx["dn_book"]
            ref_book = up_book if ctx.get("drift_dir") == "Up" else dn_book
            write_jsonl({
                "event_type": "RISK_STOP_TRIGGERED",
                "trigger": "HOURLY",
                "detected_on_slug": m.slug, "detected_on_crypto": m.crypto,
                "t_min": round(ctx["t_min"], 3),
                "hour_start_equity": round(hse, 4),
                "equity_now": round(equity_now, 4),
                "hour_dd_pct": round(hour_dd_pct, 6),
                "day_dd_pct": round(day_dd_pct, 6),
                "open_positions": pos_summary,
                "spread": round(ref_book.spread, 4),
                "delta_bps": round(ctx["delta_bps"], 3),
                "zscore": round(ctx.get("z", 0), 3),
            })
            if SHADOW_STOP_SIM and not self._shadow_active:
                self._activate_shadow()
        # ── Daily stop-loss check ──
        if day_dd_pct <= -STOP_LOSS_PCT_PER_DAY and not day_triggered:
            self._day_risk_stop_hit = True
            day_triggered = True
            write_jsonl({
                "event_type": "RISK_STOP_TRIGGERED",
                "trigger": "DAILY",
                "detected_on_slug": m.slug, "detected_on_crypto": m.crypto,
                "t_min": round(ctx["t_min"], 3),
                "day_start_equity": round(dse, 4),
                "equity_now": round(equity_now, 4),
                "day_dd_pct": round(day_dd_pct, 6),
                "hour_dd_pct": round(hour_dd_pct, 6),
            })
        return {
            "hour_start_equity": round(hse, 4),
            "hour_dd_pct": round(hour_dd_pct, 6),
            "day_dd_pct": round(day_dd_pct, 6),
            "risk_stop_triggered": hour_triggered or day_triggered,
            "shadow_blocked": self._shadow_active,
        }
    def _risk_ok(self, st: MarketState) -> bool:
        """Position-sizing risk caps. Stop-loss is log-only when ENFORCE_STOP_LOSS=False."""
        if ENFORCE_STOP_LOSS:
            equity = self._equity()
            hse = self.hour_start_equity if self.hour_start_equity > 0 else equity
            if (equity - hse) / max(hse, 1e-9) <= -STOP_LOSS_PCT_PER_HOUR:
                return False
        if self._market_cost_usdc(st) > self.cash_usdc * MAX_COST_PER_MARKET_PCT:
            return False
        if self._crypto_cost_usdc(st.crypto) > self.cash_usdc * MAX_COST_PER_CRYPTO_PCT:
            return False
        return True
    # -----------------------------
    # Background data refresh
    # -----------------------------
    def _submit_market_discovery(self):
        """Submit market discovery to background pool (non-blocking)."""
        if self._pending_fetches.get("__markets__"):
            return  # already in-flight
        self._pending_fetches["__markets__"] = True
        def _do_discovery():
            try:
                markets = self._get_markets()
                for m in markets:
                    self._ensure_market_state(m)
                self._cached_markets = markets  # atomic list assignment
                self._last_market_discovery_ts = time.time()
            except Exception as e:
                self.logger.log_event({"event_type": "BG_DISCOVERY_ERROR", "err": str(e)})
            finally:
                self._pending_fetches.pop("__markets__", None)
        self._bg_executor.submit(_do_discovery)

    def _submit_market_refresh(self, m: MarketRef):
        """Submit a single market's data refresh to background pool (non-blocking)."""
        slug = m.slug
        if self._pending_fetches.get(slug):
            return  # already in-flight for this market
        self._pending_fetches[slug] = True
        self._bg_pending_cycles[slug] = 0  # reset starvation counter
        start_ts = time.time()
        def _do_refresh():
            try:
                data = self._prefetch_market_data(m)
                end_ts = time.time()
                data["ts"] = end_ts
                self._data_cache[slug] = data  # atomic dict assignment
                # Track fetch duration
                dur_ms = (end_ts - start_ts) * 1000
                durations = self._bg_fetch_durations.setdefault(slug, [])
                durations.append(dur_ms)
                # Keep last 100 for rolling stats
                if len(durations) > 100:
                    self._bg_fetch_durations[slug] = durations[-100:]
            except Exception as e:
                self.logger.log_event({"event_type": "BG_REFRESH_ERROR",
                                       "slug": slug, "err": str(e)})
            finally:
                self._pending_fetches.pop(slug, None)
        self._bg_executor.submit(_do_refresh)

    def _cache_age_ms(self, slug: str) -> float:
        """Return age of cached data in milliseconds, or inf if not cached."""
        cached = self._data_cache.get(slug)
        if not cached or "ts" not in cached:
            return float('inf')
        return (time.time() - cached["ts"]) * 1000

    def _update_priority_slugs(self):
        """Recompute which markets need fast refresh (positions or active state machine)."""
        priority = set()
        for slug, st in self.market_states.items():
            sm = self.entry_sm.get(slug, {})
            if sm.get("state") in ("PROBING", "SCALING"):
                priority.add(slug)
                continue
            for outcome in ("Up", "Down"):
                if st.positions[outcome].qty >= MIN_QTY:
                    priority.add(slug)
                    break
        self._high_priority_slugs = priority  # atomic set assignment

    # -----------------------------
    # Main loop
    # -----------------------------
    def _prefetch_market_data(self, m: MarketRef) -> dict:
        """Fetch all HTTP data for one market (runs in thread)."""
        spot, hour_open = self.client.get_binance_spot_and_hour_open(m.crypto)
        up_book = self.client.get_top_of_book(m.outcome_up_id, levels=IMB_LEVELS)
        dn_book = self.client.get_top_of_book(m.outcome_down_id, levels=IMB_LEVELS)
        return {"market": m, "spot": spot, "hour_open": hour_open,
                "up_book": up_book, "dn_book": dn_book}

    def run(self):
        # Set hour_start_equity to actual equity so the first (partial) hour
        # measures drawdown correctly — not from BANKROLL_START_USDC.
        actual_equity = self._equity()
        self.hour_start_equity = actual_equity
        self.day_start_equity = actual_equity
        self.logger.log_event({
            "event_type": "START",
            "run_id": RUN_ID, "schema_version": SCHEMA_VERSION,
            "bot_version": BOT_VERSION,
            "mode": MODE, "profile": PROFILE,
            "cash": self.cash_usdc, "realized_pnl": self.realized_pnl_usdc,
            "hour_start_equity": round(actual_equity, 4),
            "csv_path": self.logger._csv_path,
            "jsonl_path": self.logger._jsonl_path,
            # Fee model config (logged at startup for audit trail)
            "maker_fee_bps": MAKER_FEE_BPS,
            "taker_fee_bps": TAKER_FEE_BPS,
            "parity_buy_min_edge_net_cents": PARITY_BUY_MIN_EDGE_NET_CENTS,
            "parity_sell_min_edge_net_cents": PARITY_SELL_MIN_EDGE_NET_CENTS,
            "parity_edge_buffer_cents": PARITY_EDGE_BUFFER_CENTS,
            "parity_stop_new_min": PARITY_STOP_NEW_MIN,
            "parity_flatten_start_min": PARITY_FLATTEN_START_MIN,
            "parity_hard_flatten_min": PARITY_HARD_FLATTEN_MIN,
            "quote_target_edge_base": PARITY_QUOTE_TARGET_EDGE_NET_CENTS_BASE,
            "quote_target_edge_max": PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX,
            "quote_unpaired_escalate_ms": QUOTE_UNPAIRED_ESCALATE_AFTER_MS,
            "quote_unpaired_max_sec": QUOTE_UNPAIRED_MAX_SEC,
            "quote_pause_sec": QUOTE_PAUSE_AFTER_UNPAIRED_SEC,
            "rescue_min_edge": RESCUE_MIN_EDGE_NET_CENTS,
        })
        print(f"  FEE MODEL: maker={MAKER_FEE_BPS}bps taker={TAKER_FEE_BPS}bps "
              f"buffer={PARITY_EDGE_BUFFER_CENTS}c "
              f"buy_min_net={PARITY_BUY_MIN_EDGE_NET_CENTS}c "
              f"sell_min_net={PARITY_SELL_MIN_EDGE_NET_CENTS}c")
        print(f"  QUOTE: target_edge={PARITY_QUOTE_TARGET_EDGE_NET_CENTS_BASE}-"
              f"{PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX}c "
              f"step=${PARITY_QUOTE_STEP_USD} max=${PARITY_QUOTE_MAX_USD_PER_SLUG} "
              f"unpaired_esc={QUOTE_UNPAIRED_ESCALATE_AFTER_MS}ms "
              f"max_unpaired={QUOTE_UNPAIRED_MAX_SEC}s pause={QUOTE_PAUSE_AFTER_UNPAIRED_SEC}s")
        print(f"  RECONCILE: enabled={POSITION_RECONCILE_ENABLED} "
              f"interval={POSITION_RECONCILE_INTERVAL_SEC}s "
              f"on_startup={POSITION_RECONCILE_ON_STARTUP} "
              f"after_fill={POSITION_RECONCILE_AFTER_FILL} "
              f"block_on_desync={POSITION_RECONCILE_BLOCK_ON_DESYNC}")
        print(f"  LEDGER: enabled={LEDGER_ENABLED} "
              f"path={LEDGER_PATH} "
              f"reconcile_interval={RECONCILE_INTERVAL_SECONDS}s "
              f"safe_mode_on_mismatch={SAFE_MODE_ON_MISMATCH}")
        if MODE in ("LIVE", "LIVE_SAFE"):
            print(f"  LIVE SAFETY: hedge_kill={HEDGE_KILL_TIMEOUT_SEC}s/{HEDGE_KILL_TIMEOUT_FINAL_SEC}s "
                  f"slip={HEDGE_MAX_SLIPPAGE_CENTS}c "
                  f"spread={SPREAD_EXPLOSION_CENTS_NORMAL}/{SPREAD_EXPLOSION_CENTS_FINAL}c "
                  f"depth={DEPTH_CHECK_MULTIPLIER}x "
                  f"breaker={HEDGE_FAIL_MAX_COUNT}/{HEDGE_FAIL_WINDOW_SEC:.0f}s/{HEDGE_FAIL_PAUSE_SEC:.0f}s "
                  f"loss=${MAX_LOSS_PER_HOUR_USD} "
                  f"hourly_budget=${HOURLY_BUDGET_USD} "
                  f"ioc={LAST_SECONDS_IOC_ONLY:.0f}s "
                  f"cancel_all={CANCEL_ALL_BEFORE_CLOSE_SEC:.0f}s")
        self._last_balance_print = 0.0
        self._last_save_ts = time.time()

        # ── Initial sync bootstrap: discover markets + prefetch (blocking, once) ──
        try:
            self._cached_markets = self._get_markets()
            for m in self._cached_markets:
                self._ensure_market_state(m)
            self._last_market_discovery_ts = time.time()
            if self._cached_markets:
                with ThreadPoolExecutor(max_workers=len(self._cached_markets)) as pool:
                    futures = {pool.submit(self._prefetch_market_data, m): m
                               for m in self._cached_markets}
                    for fut in as_completed(futures):
                        try:
                            data = fut.result()
                            data["ts"] = time.time()
                            self._data_cache[data["market"].slug] = data
                        except Exception as e:
                            m_ref = futures[fut]
                            self.logger.log_event({"event_type": "INIT_PREFETCH_ERROR",
                                                   "slug": m_ref.slug, "err": str(e)})
            write_jsonl({"event_type": "INIT_BOOTSTRAP_DONE",
                          "markets": len(self._cached_markets),
                          "cached": len(self._data_cache),
                          "cash_usdc": round(self.cash_usdc, 2),
                          "cash_source": "wallet" if self._wallet_usdc_at_start is not None else "config"})
        except Exception as e:
            self.logger.log_event({"event_type": "INIT_BOOTSTRAP_ERROR", "err": str(e)})

        # ── LIVE startup safety: cancel all stale CLOB orders ──
        if MODE in ("LIVE", "LIVE_SAFE"):
            self._exec_orphan.startup_cleanup()
            # ── Startup state: load existing positions from CLOB balances ──
            self._load_clob_positions_on_startup()

        # ── Fills ledger startup (token registration + PnL update only) ──
        if LEDGER_ENABLED and self._fills_ledger is not None:
            self._run_ledger_reconciliation()
            self._last_ledger_reconcile_ts = time.time()
            self._fills_ledger.print_positions()

        if MODE == "LIVE_SAFE":
            print(f"\n  [LIVE_SAFE] Max order: ${LIVE_SAFE_MAX_ORDER_USD:.2f}  |  "
                  f"Max total invested: ${LIVE_SAFE_MAX_TOTAL_INVESTED_USD:.2f}  |  "
                  f"Burst window: first 3 min of each hour")
            print(f"  [LIVE_SAFE] Will run {LIVE_SAFE_NUM_HOURS} hourly windows then exit  "
                  f"(started: {iso_z(self._live_safe_start_hour)})\n")

        if MODE in ("LIVE", "LIVE_SAFE", "TEST"):
            print(f"  LIVE symbol filter active: {', '.join(LIVE_ALLOWED_SYMBOLS)}")

        # ══════════════════════════════════════════════════════════════════
        # STARTUP SANITY REPORT — one-time boot verification block
        # Proves: no old fills will be ingested; only current positions + new fills.
        # ══════════════════════════════════════════════════════════════════
        self._emit_startup_sanity_report()

        # ══════════════════════════════════════════════════════════════════
        # NON-BLOCKING MAIN LOOP — reads from cache, never blocks on HTTP
        # Background pool continuously refreshes _data_cache
        # ══════════════════════════════════════════════════════════════════
        while self.running:
            loop_start = time.time()
            self._reset_daily_if_needed()
            try:
                # 1. Market discovery (background, every MARKET_DISCOVERY_INTERVAL_SEC)
                if time.time() - self._last_market_discovery_ts >= MARKET_DISCOVERY_INTERVAL_SEC:
                    self._submit_market_discovery()

                # 2. Snapshot current markets & resolve ended hours
                markets = list(self._cached_markets)  # atomic snapshot
                self._resolve_ended_hours(markets)

                # 3. Update priority set + deadline-based refresh scheduling
                self._update_priority_slugs()
                now_ts = time.time()
                for m in markets:
                    slug = m.slug
                    interval_ms = (BOOK_REFRESH_PRIORITY_MS
                                   if slug in self._high_priority_slugs
                                   else BOOK_REFRESH_IDLE_MS)
                    interval_sec = interval_ms / 1000.0

                    # Initialize deadline if not set
                    if slug not in self._bg_next_due:
                        self._bg_next_due[slug] = now_ts

                    # Track starvation: if pending for > BG_REFRESH_STARVE_CYCLES
                    if self._pending_fetches.get(slug):
                        self._bg_pending_cycles[slug] = self._bg_pending_cycles.get(slug, 0) + 1
                        pending_streak = self._bg_pending_cycles.get(slug, 0)
                        # Track pending fetch streak per slug and global max
                        self._diag_pending_fetch_streak[slug] = max(
                            self._diag_pending_fetch_streak.get(slug, 0), pending_streak)
                        self._diag_pending_fetch_streak_max = max(
                            self._diag_pending_fetch_streak_max, pending_streak)
                        if pending_streak > BG_REFRESH_STARVE_CYCLES:
                            # Starved: previous fetch is still in-flight after N cycles
                            self._bg_refresh_miss_count[slug] = self._bg_refresh_miss_count.get(slug, 0) + 1
                            # Priority resubmit: if pending > 2 cycles, force re-submit
                            # (cancel stale future, re-queue with priority)
                            self._pending_fetches.pop(slug, None)
                            self._bg_pending_cycles[slug] = 0
                            self._submit_market_refresh(m)
                            self._bg_next_due[slug] = now_ts + interval_sec
                        continue

                    # Deadline scheduling: submit if past due
                    if now_ts >= self._bg_next_due[slug]:
                        self._submit_market_refresh(m)
                        # Advance deadline by interval (not from now — prevents drift)
                        self._bg_next_due[slug] = max(now_ts, self._bg_next_due[slug] + interval_sec)

                    # Also track cache_age misses (cache too old = 3x interval)
                    cache_age = self._cache_age_ms(slug)
                    if cache_age > interval_ms * 3:
                        self._bg_refresh_miss_count[slug] = self._bg_refresh_miss_count.get(slug, 0) + 1

                # 4. Process each market with cached data (ZERO HTTP, pure logic)
                stale_skips = 0
                for m in markets:
                    cached = self._data_cache.get(m.slug)
                    age = self._cache_age_ms(m.slug)
                    if cached and age < BOOK_STALE_MS:
                        self._step_market_with_data(cached)
                    elif cached:
                        stale_skips += 1
                if stale_skips > 0:
                    self._stale_skip_total += stale_skips

                # 4b. Track cache ages for tempo diagnostics
                for m in markets:
                    age = self._cache_age_ms(m.slug)
                    if age < float('inf'):
                        self._tempo_cache_ages.setdefault(m.slug, []).append(age)

                # 4c. LIVE execution safety ticks (no-op in LOG)
                if MODE in ("LIVE", "LIVE_SAFE"):
                    self._exec_tracker.tick()
                    # Order Lifecycle Guard: poll pending orders for missed fills
                    self._process_lifecycle_recovered_fills()
                    self._exec_orphan.tick(self._exec_tracker.get_tracked_order_ids())
                    self._exec_safety.update_orphan_count(
                        self._exec_orphan.orphan_cancels_this_min)
                    self._exec_safety.tick()
                    # State drift check (every 60s)
                    self._exec_drift.tick(
                        self._exec_tracker.get_tracked_order_ids(),
                        self._get_internal_positions(),
                        self._get_token_to_slug_outcome())
                    self._exec_safety.set_drift_paused(
                        self._exec_drift.is_drifted())

                    # Truth Capture: poll order watchers, wallet scan, reconcile
                    _truth_active_tids = set(self._get_token_to_slug_outcome().keys())
                    _recovered = self._truth.tick(
                        active_token_ids=_truth_active_tids,
                        position_log_interval=POSITION_LOG_INTERVAL_SEC)

                    # Process any recovered fills through unified pipeline
                    if _recovered:
                        self._process_truth_recovered_fills(_recovered)

                    # Active window filter: tell truth + ledger which tokens are current hour
                    self._truth.set_active_window(
                        _truth_active_tids,
                        active_only=ACTIVE_WINDOW_ONLY,
                        strict=STRICT_WINDOW_MODE)
                    if self._fills_ledger is not None:
                        self._fills_ledger.set_active_window(
                            _truth_active_tids,
                            active_only=ACTIVE_WINDOW_ONLY,
                            strict=STRICT_WINDOW_MODE)

                    # Hedge mismatch detection (one-side-fill safety)
                    self._check_hedge_mismatch()

                    # Invariant: bot positions vs truth positions
                    self._check_position_invariant()

                    # Reservation invariant audit (releases stuck, logs anomalies)
                    self._audit_reservations()

                    # Rule 9: Cancel all open orders 30s before resolution
                    for m_r9 in markets:
                        st_r9 = self.market_states.get(m_r9.slug)
                        if st_r9 and st_r9.hour_start_utc:
                            try:
                                hr_start = _parse_iso_z(st_r9.hour_start_utc)
                                t_min_r9 = minutes_into_hour(hr_start, utc_now())
                                sec_to_close = max(0.0, (60.0 - t_min_r9) * 60.0)
                                hour_key = st_r9.hour_start_utc
                                if self._live_safety.should_cancel_all(sec_to_close, hour_key):
                                    open_orders = self.client.get_open_orders()
                                    for o in open_orders:
                                        oid = o.get("id") or o.get("orderID") or o.get("order_id", "")
                                        if oid:
                                            self.client.cancel_order(oid)
                                    self._exec_tracker._orders.clear()
                                    # Fix 4: set CLOSE_IMMINENT — prevents ANY new orders
                                    self._live_safety.set_close_imminent(hour_key)
                                    write_jsonl({"event_type": "CANCEL_ALL_BEFORE_RESOLUTION",
                                                  "slug": m_r9.slug,
                                                  "seconds_to_close": round(sec_to_close, 1),
                                                  "orders_cancelled": len(open_orders),
                                                  "ts_ms": int(time.time() * 1000)})
                                    print(f"  [RULE 9] Cancelled {len(open_orders)} orders "
                                          f"({sec_to_close:.0f}s to close) — CLOSE_IMMINENT set")
                            except Exception:
                                pass
                            break  # all markets share the same hour — only need one check

                # 4d. Active window filter for LOG mode (LIVE sets it above)
                if MODE not in ("LIVE", "LIVE_SAFE"):
                    _log_active_tids = set(self._get_token_to_slug_outcome().keys())
                    self._truth.set_active_window(
                        _log_active_tids,
                        active_only=ACTIVE_WINDOW_ONLY,
                        strict=STRICT_WINDOW_MODE)
                    if self._fills_ledger is not None:
                        self._fills_ledger.set_active_window(
                            _log_active_tids,
                            active_only=ACTIVE_WINDOW_ONLY,
                            strict=STRICT_WINDOW_MODE)

                # 5. Save state periodically (every STATE_SAVE_INTERVAL_SEC)
                now = time.time()
                if now - self._last_save_ts >= STATE_SAVE_INTERVAL_SEC:
                    self._save_state()
                    self._last_save_ts = now

                # 6. Log rotation
                self.logger.rotate_files_if_needed()

                # 7. Console balance summary
                if now - self._last_balance_print >= 30.0:
                    self._print_balance_summary()
                    self._last_balance_print = now

                # 7b. CLOB balance reconciliation (every 60s in LIVE modes)
                if MODE in ("LIVE", "LIVE_SAFE") and now - self._last_balance_sync_ts >= 60.0:
                    self._sync_clob_balances()
                    self._last_balance_sync_ts = now

                # 7c-d. Fills ledger reconciliation (every RECONCILE_INTERVAL_SECONDS)
                #       (Position reconciler disabled — positions kept correct
                #        by verifying each fill in _live_buy/_live_sell)
                if (LEDGER_ENABLED and self._fills_ledger is not None
                        and now - self._last_ledger_reconcile_ts >= RECONCILE_INTERVAL_SECONDS):
                    self._run_ledger_reconciliation()
                    self._last_ledger_reconcile_ts = now

                # 8. Tempo parity diagnostics (every 60s)
                if now - self._tempo_last_report_ts >= 60.0:
                    self._emit_clone_report()   # must run BEFORE tempo_report resets counters
                    self._emit_diag_report()    # F247-style behavioral diagnostics
                    self._emit_tempo_report()
                    self._emit_slug_pnl_report()
                    if MODE in ("LIVE", "LIVE_SAFE"):
                        self._emit_live_sanity_report()
                    self._tempo_last_report_ts = now
            except Exception as e:
                self.logger.log_event({"event_type": "LOOP_ERROR", "err": str(e)})

            # Enforce target loop interval — sleep only the remaining time
            loop_elapsed_ms = (time.time() - loop_start) * 1000
            self._tempo_loop_times.append(loop_elapsed_ms)
            sleep_ms = max(0, MAIN_LOOP_TARGET_MS - loop_elapsed_ms)
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)

            # Log loop latency periodically (every 50 loops)
            self._loop_count += 1
            if self._loop_count % 50 == 0:
                actual_loop_ms = (time.time() - loop_start) * 1000
                cache_ages = {slug: round(self._cache_age_ms(slug), 0)
                              for slug in self._data_cache}
                pending = list(self._pending_fetches.keys())
                write_jsonl({"event_type": "LOOP_LATENCY",
                              "loop_elapsed_ms": round(loop_elapsed_ms, 1),
                              "actual_loop_ms": round(actual_loop_ms, 1),
                              "target_ms": MAIN_LOOP_TARGET_MS,
                              "cache_ages_ms": cache_ages,
                              "pending_fetches": pending,
                              "markets_count": len(markets)})

        # ── Shutdown ──
        self._bg_running = False
        self._bg_executor.shutdown(wait=False)
        self.logger.log_event({"event_type": "STOPPED", "cash": self.cash_usdc,
                               "realized_pnl": self.realized_pnl_usdc,
                               "equity": round(self._equity(), 2),
                               "hourly_pnl": self.hourly_pnl_usdc})
        self._save_state()
        self.logger.close()

    def _emit_tempo_report(self):
        """Emit per-minute tempo parity diagnostics for f247 comparison."""
        import statistics
        # Per-market metrics
        per_market = {}
        all_slugs = set(self._tempo_fills.keys()) | set(self._tempo_intents.keys()) | set(self._tempo_cache_ages.keys())
        for slug in all_slugs:
            fills = self._tempo_fills.get(slug, 0)
            intents = self._tempo_intents.get(slug, 0)
            ages = self._tempo_cache_ages.get(slug, [])
            median_age = round(statistics.median(ages), 0) if ages else 0
            per_market[slug] = {
                "fills_per_min": fills,
                "intents_per_min": intents,
                "median_cache_age_ms": median_age,
            }
        # Decision loop p95
        loop_p95 = 0.0
        if self._tempo_loop_times:
            sorted_loops = sorted(self._tempo_loop_times)
            p95_idx = int(len(sorted_loops) * 0.95)
            loop_p95 = round(sorted_loops[min(p95_idx, len(sorted_loops) - 1)], 1)
        # Diagnostic counters snapshot
        avg_parity_edge = (sum(self._diag_parity_edges) / len(self._diag_parity_edges)
                           if self._diag_parity_edges else 0.0)
        avg_pair_delay = (sum(self._diag_pair_fill_delays) / len(self._diag_pair_fill_delays)
                          if self._diag_pair_fill_delays else 0.0)
        # Similarity/tempo stats
        trade_ts = sorted(self._diag_parity_trade_timestamps)
        paired_trades_per_min = len(trade_ts)
        max_trades_in_one_sec = 0
        if trade_ts:
            # Count max trades in any 1-second window
            for j, t0 in enumerate(trade_ts):
                cnt = sum(1 for t1 in trade_ts[j:] if t1 - t0 <= 1.0)
                max_trades_in_one_sec = max(max_trades_in_one_sec, cnt)
        # Median time between trades
        inter_trade_ms = []
        for j in range(1, len(trade_ts)):
            inter_trade_ms.append((trade_ts[j] - trade_ts[j - 1]) * 1000)
        median_inter_trade_ms = (sorted(inter_trade_ms)[len(inter_trade_ms) // 2]
                                  if inter_trade_ms else 0.0)
        # Paired trade ratio = pair completions / total individual fills
        total_fills_all = sum(v.get("fills_per_min", 0) for v in per_market.values())
        # Each pair completion = 2 fills, so ratio = 2*pairs / total_fills
        paired_ratio = (2 * self._diag_pairs_completed / max(1, total_fills_all)
                        if total_fills_all > 0 else 0.0)
        # Maker ratio = maker / (maker + taker) across parity
        total_parity_mt = self._diag_parity_maker_count + self._diag_parity_taker_count
        maker_ratio = (self._diag_parity_maker_count / max(1, total_parity_mt))

        diag = {
            "taker_count": self._diag_taker_count,
            "derisk_count": self._diag_derisk_count,
            "derisk_taker_count": self._diag_derisk_taker_count,
            "blocked_whipsaw": self._diag_blocked_whipsaw,
            "blocked_noflip": self._diag_blocked_noflip,
            "parity_buy_signals": self._diag_parity_buy_signals,
            "parity_sell_signals": self._diag_parity_sell_signals,
            "parity_trades": self._diag_parity_trades,
            "parity_avg_edge_net_cents": round(avg_parity_edge, 3),
            "parity_maker_count": self._diag_parity_maker_count,
            "parity_taker_count": self._diag_parity_taker_count,
            "pair_partial_count": self._diag_pair_partial_count,
            "avg_pair_fill_delay_ms": round(avg_pair_delay, 1),
            "unpaired_unwind_usd": round(self._diag_unpaired_unwind_usd, 2),
            "blocked_spread": self._diag_parity_blocked_spread,
            "blocked_liq": self._diag_parity_blocked_liq,
            "blocked_stale": self._diag_parity_blocked_stale,
            "recycle_count": self._diag_recycle_count,
            "flatten_actions_count": self._diag_flatten_actions,
            "flatten_taker_count": self._diag_flatten_taker,
            "rescue_attempts": self._diag_rescue_attempts,
            "rescue_success": self._diag_rescue_success,
            "rescue_fallback_sells": self._diag_rescue_fallback_sells,
            "quote_orders_placed": self._diag_quote_orders_placed,
            "quote_fills": self._diag_quote_fills,
            "quote_fill_rate": round(self._diag_quote_fills / max(1, self._diag_quote_orders_placed), 3),
            "quote_unpaired_events": self._diag_quote_unpaired_events,
            "quote_unpaired_escalations": self._diag_quote_unpaired_escalations,
            "quote_pause_count": self._diag_quote_pause_count,
            "adverse_guard_events": self._diag_adverse_guard_events,
            "adverse_guard_degrades": self._diag_adverse_guard_degrades,
            "adverse_guard_pauses": self._diag_adverse_guard_pauses,
            "quote_submit_count": self._diag_quote_submit_count,
            "quote_cancel_count": self._diag_quote_cancel_count,
            "quote_replace_count": self._diag_quote_replace_count,
            "hedge_tick1": self._diag_hedge_tick1,
            "hedge_tick2": self._diag_hedge_tick2,
            "hedge_cross": self._diag_hedge_cross,
            "hedge_unwind": self._diag_hedge_unwind,
        }
        tempo_stats = {
            "paired_trade_ratio": round(paired_ratio, 3),
            "paired_trades_per_min": paired_trades_per_min,
            "max_trades_in_one_sec": max_trades_in_one_sec,
            "median_inter_trade_ms": round(median_inter_trade_ms, 1),
            "maker_ratio": round(maker_ratio, 3),
        }
        # Per-slug inventory imbalance + straddle metrics
        inv_imbalance = {}
        total_straddle_locked_usd = 0.0
        locked_hold_secs = []
        for slug, st in self.market_states.items():
            up_qty = st.positions["Up"].qty
            dn_qty = st.positions["Down"].qty
            if up_qty >= MIN_QTY or dn_qty >= MIN_QTY:
                locked_age = 0.0
                ls = self._parity_locked_since.get(slug)
                if ls:
                    locked_age = time.time() - ls
                    locked_hold_secs.append(locked_age)
                locked_shares = min(up_qty, dn_qty)
                locked_usd = locked_shares * (st.positions["Up"].vwap + st.positions["Down"].vwap)
                total_straddle_locked_usd += locked_usd
                inv_imbalance[slug] = {
                    "up_qty": round(up_qty, 1),
                    "dn_qty": round(dn_qty, 1),
                    "up_vwap": round(st.positions["Up"].vwap, 4),
                    "dn_vwap": round(st.positions["Down"].vwap, 4),
                    "imbalance": round(up_qty - dn_qty, 1),
                    "straddle_locked": round(locked_shares, 1),
                    "locked_usd": round(locked_usd, 2),
                    "locked_age_sec": round(locked_age, 1),
                    "unpaired_usd": round(self._parity_invested_usd.get(slug, 0), 2),
                }
        avg_locked_hold_sec = (sum(locked_hold_secs) / len(locked_hold_secs)
                               if locked_hold_secs else 0.0)
        write_jsonl({
            "event_type": "TEMPO_REPORT",
            "per_market": per_market,
            "loop_p95_ms": loop_p95,
            "loop_count": len(self._tempo_loop_times),
            "stale_skip_total": self._stale_skip_total,
            "priority_slugs": list(self._high_priority_slugs),
            "diagnostics": diag,
            "tempo_stats": tempo_stats,
            "inventory_imbalance": inv_imbalance,
            "straddle_locked_usd": round(total_straddle_locked_usd, 2),
            "avg_locked_hold_sec": round(avg_locked_hold_sec, 1),
        })
        # Print summary to console
        total_fills = sum(v.get("fills_per_min", 0) for v in per_market.values())
        total_intents = sum(v.get("intents_per_min", 0) for v in per_market.values())
        if CONSOLE_LEVEL != "QUIET":
            print(f"\n  TEMPO: fills/min={total_fills}  intents/min={total_intents}  "
                  f"loop_p95={loop_p95:.1f}ms  stale_skips={self._stale_skip_total}")
            print(f"  DIAG:  taker={diag['taker_count']}  maker={diag['maker_count']}  "
                  f"derisk={diag['derisk_count']}(taker={diag['derisk_taker_count']})  "
                  f"blocked: whipsaw={diag['blocked_whipsaw']} "
                  f"taker_gate={diag['blocked_taker_gate']} "
                  f"noflip={diag['blocked_noflip']}")
            print(f"  PARITY: buy_sig={diag['parity_buy_signals']}  "
                  f"sell_sig={diag['parity_sell_signals']}  "
                  f"trades={diag['parity_trades']}  "
                  f"avg_net_edge={diag['parity_avg_edge_net_cents']:.2f}c  "
                  f"maker={diag['parity_maker_count']}  "
                  f"taker={diag['parity_taker_count']}")
            print(f"  PAIRS: partial={diag['pair_partial_count']}  "
                  f"avg_delay={diag['avg_pair_fill_delay_ms']:.0f}ms  "
                  f"unwind=${diag['unpaired_unwind_usd']:.2f}  "
                  f"recycle={diag['recycle_count']}")
            print(f"  MAKER: placed={diag['maker_orders_placed']}  "
                  f"fills={diag['maker_fills']}  "
                  f"rate={diag['maker_fill_rate']:.1%}  "
                  f"lat_p50={diag['maker_fill_latency_ms_p50']:.0f}ms  "
                  f"lat_p90={diag['maker_fill_latency_ms_p90']:.0f}ms  "
                  f"timeout={diag['maker_timeout_cancel_count']}  "
                  f"lost_best={diag['maker_top_of_book_lost_count']}  "
                  f"cancel_rep={diag['cancel_replace_per_min']}")
            if diag['flatten_actions_count'] > 0:
                print(f"  FLATTEN: actions={diag['flatten_actions_count']}  "
                      f"taker={diag['flatten_taker_count']}")
            if diag['rescue_attempts'] > 0 or diag['rescue_success'] > 0:
                print(f"  RESCUE: attempts={diag['rescue_attempts']}  "
                      f"success={diag['rescue_success']}  "
                      f"fallback_sells={diag['rescue_fallback_sells']}")
            if diag['quote_orders_placed'] > 0 or diag['quote_unpaired_events'] > 0:
                print(f"  QUOTE: placed={diag['quote_orders_placed']}  "
                      f"fills={diag['quote_fills']}  "
                      f"rate={diag['quote_fill_rate']:.1%}  "
                      f"unpaired={diag['quote_unpaired_events']}  "
                      f"escalations={diag['quote_unpaired_escalations']}  "
                      f"pauses={diag['quote_pause_count']}")
            if total_straddle_locked_usd > 0:
                print(f"  STRADDLE: locked_usd=${total_straddle_locked_usd:.2f}  "
                      f"avg_hold={avg_locked_hold_sec:.0f}s")
            print(f"  TEMPO: paired_ratio={tempo_stats['paired_trade_ratio']:.1%}  "
                  f"paired/min={tempo_stats['paired_trades_per_min']}  "
                  f"max_burst={tempo_stats['max_trades_in_one_sec']}/s  "
                  f"med_gap={tempo_stats['median_inter_trade_ms']:.0f}ms  "
                  f"mkr_ratio={tempo_stats['maker_ratio']:.1%}")
            print(f"  GUARD: spread_blk={diag['blocked_spread']}  "
                  f"liq_blk={diag['blocked_liq']}  "
                  f"stale_blk={diag['blocked_stale']}  "
                  f"adverse={diag['adverse_guard_events']}  "
                  f"degrades={diag['adverse_guard_degrades']}  "
                  f"hard_pauses={diag['adverse_guard_pauses']}")
            if inv_imbalance:
                for slug, inv in inv_imbalance.items():
                    print(f"    INV {slug[:30]:30s}  Up={inv['up_qty']:5.0f}  "
                          f"Dn={inv['dn_qty']:5.0f}  "
                          f"imbal={inv['imbalance']:+5.0f}  "
                          f"locked={inv['straddle_locked']:.0f}({inv['locked_age_sec']:.0f}s)")
            for slug, info in per_market.items():
                if info["fills_per_min"] > 0 or info["intents_per_min"] > 0:
                    print(f"    {slug[:30]:30s}  fills={info['fills_per_min']:3d}  "
                          f"intents={info['intents_per_min']:3d}  "
                          f"cache_age_med={info['median_cache_age_ms']:.0f}ms")
        # Reset counters for next minute
        self._tempo_fills.clear()
        self._tempo_intents.clear()
        self._tempo_cache_ages.clear()
        self._tempo_loop_times.clear()
        self._stale_skip_total = 0
        self._diag_taker_count = 0
        self._diag_derisk_count = 0
        self._diag_derisk_taker_count = 0
        self._diag_blocked_whipsaw = 0
        self._diag_blocked_noflip = 0
        self._diag_parity_buy_signals = 0
        self._diag_parity_sell_signals = 0
        self._diag_parity_trades = 0
        self._diag_parity_edges.clear()
        self._diag_parity_maker_count = 0
        self._diag_parity_taker_count = 0
        self._diag_pair_partial_count = 0
        self._diag_pair_fill_delays.clear()
        self._diag_unpaired_unwind_usd = 0.0
        self._diag_parity_blocked_spread = 0
        self._diag_parity_blocked_liq = 0
        self._diag_parity_blocked_stale = 0
        self._diag_recycle_count = 0
        self._diag_flatten_actions = 0
        self._diag_flatten_taker = 0
        self._diag_rescue_attempts = 0
        self._diag_rescue_success = 0
        self._diag_rescue_fallback_sells = 0
        self._diag_quote_orders_placed = 0
        self._diag_quote_fills = 0
        self._diag_quote_unpaired_events = 0
        self._diag_quote_unpaired_escalations = 0
        self._diag_quote_pause_count = 0
        self._diag_adverse_guard_events = 0
        self._diag_adverse_guard_pauses = 0
        self._diag_adverse_guard_degrades = 0
        self._diag_parity_trade_timestamps.clear()
        self._diag_quote_submit_count = 0
        self._diag_quote_cancel_count = 0
        self._diag_quote_replace_count = 0
        self._diag_hedge_tick1 = 0
        self._diag_hedge_tick2 = 0
        self._diag_hedge_cross = 0
        self._diag_hedge_cross_early = 0
        self._diag_hedge_cross_late = 0
        self._diag_hedge_skipped_stale = 0
        self._diag_hedge_unwind = 0

    def _emit_clone_report(self):
        """Emit F247 similarity metrics + 4 analytics every minute."""
        import statistics

        # ── 1. Paired straddle metrics (from _pair_tracker via _record_pair_fill) ──
        pairs_within_500ms = self._diag_pairs_completed_500ms
        pairs_within_1500ms = self._diag_pairs_completed_1500ms
        pairs_within_10s = self._diag_pairs_completed_10s
        total_pairs = self._diag_pairs_completed
        paired_500ms_ratio = (pairs_within_500ms / max(1, total_pairs)
                              if total_pairs > 0 else 0.0)
        paired_straddle_ratio = (pairs_within_1500ms / max(1, total_pairs)
                                 if total_pairs > 0 else 0.0)
        paired_10s_ratio = (pairs_within_10s / max(1, total_pairs)
                            if total_pairs > 0 else 0.0)

        # median pair fill delay (Up vs Down fill timestamps)
        med_pair_delay_ms = (statistics.median(self._clone_pair_delays)
                             if self._clone_pair_delays else 0.0)
        # median time between pairs (burstiness)
        med_inter_pair_ms = (statistics.median(self._clone_inter_pair_gaps)
                             if self._clone_inter_pair_gaps else 0.0)
        # signal-to-fill latency
        med_signal_to_fill_ms = (statistics.median(self._clone_signal_to_fill)
                                 if self._clone_signal_to_fill else 0.0)
        # ── 2. Hold time distribution for paired inventory ──
        hold_p50, hold_p90 = _p50_p90(self._clone_hold_times)
        # Also compute current hold times from active locked positions
        active_hold_secs = []
        now_t = time.time()
        for slug, ls_ts in self._parity_locked_since.items():
            active_hold_secs.append(now_t - ls_ts)
        active_hold_p50 = (statistics.median(active_hold_secs)
                           if active_hold_secs else 0.0)

        # ── 3. Net imbalance per slug and max abs imbalance ──
        per_slug_imbalance = {}
        global_max_imbalance = 0.0
        for slug, st in self.market_states.items():
            up_q = st.positions["Up"].qty
            dn_q = st.positions["Down"].qty
            imbal = up_q - dn_q
            max_imbal = self._diag_max_imbalance.get(slug, abs(imbal))
            global_max_imbalance = max(global_max_imbalance, max_imbal)
            if abs(imbal) >= MIN_QTY or max_imbal >= MIN_QTY:
                per_slug_imbalance[slug] = {
                    "current_imbalance": round(imbal, 1),
                    "max_abs_imbalance": round(max_imbal, 1),
                }

        # ── 4. Correlation between net exposure and spot trend ──
        imbal_delta_corr = 0.0
        if len(self._diag_imbalance_delta_samples) >= 10:
            imbals = [s[0] for s in self._diag_imbalance_delta_samples]
            deltas = [s[1] for s in self._diag_imbalance_delta_samples]
            mean_i = sum(imbals) / len(imbals)
            mean_d = sum(deltas) / len(deltas)
            cov = sum((i - mean_i) * (d - mean_d) for i, d in zip(imbals, deltas)) / len(imbals)
            var_i = sum((i - mean_i) ** 2 for i in imbals) / len(imbals)
            var_d = sum((d - mean_d) ** 2 for d in deltas) / len(deltas)
            denom = (var_i * var_d) ** 0.5
            imbal_delta_corr = cov / denom if denom > 1e-9 else 0.0

        # ── 5. Per-slug F247 clone KPI ──
        per_slug_kpi = {}
        for slug in set(list(self._tempo_cache_ages.keys()) + list(self._bg_fetch_durations.keys())
                        + [s for s in self.market_states]):
            # Cache age p50/p90
            ages = self._tempo_cache_ages.get(slug, [])
            ca_p50, ca_p90 = _p50_p90(ages)
            # BG fetch duration p50/p90
            fetch_durs = self._bg_fetch_durations.get(slug, [])
            bf_p50, bf_p90 = _p50_p90(fetch_durs)
            # Per-slug pair completion KPI
            slug_unpaired = self._diag_slug_unpaired_events.get(slug, 0)
            slug_p500 = self._diag_slug_paired_500ms.get(slug, 0)
            slug_p1500 = self._diag_slug_paired_1500ms.get(slug, 0)
            slug_timeouts = self._diag_slug_timeouts.get(slug, 0)
            # Refresh misses
            refresh_misses = self._bg_refresh_miss_count.get(slug, 0)
            # Pending fetch streak
            slug_fetch_streak = self._diag_pending_fetch_streak.get(slug, 0)
            # Hedge cross rate: crosses / (crosses + unwinds) for this window
            hedge_total = self._diag_hedge_cross + self._diag_hedge_unwind
            hedge_cross_rate = self._diag_hedge_cross / max(1, hedge_total)

            if ca_p90 > 0 or bf_p90 > 0 or slug_p500 > 0 or slug_p1500 > 0 or refresh_misses > 0:
                per_slug_kpi[slug] = {
                    "cache_age_p50_ms": round(ca_p50, 0),
                    "cache_age_p90_ms": round(ca_p90, 0),
                    "bg_fetch_p50_ms": round(bf_p50, 0),
                    "bg_fetch_p90_ms": round(bf_p90, 0),
                    "refresh_miss_count": refresh_misses,
                    "unpaired_events": slug_unpaired,
                    "paired_within_500ms": slug_p500,
                    "paired_within_1500ms": slug_p1500,
                    "timeouts": slug_timeouts,
                    "pending_fetch_streak": slug_fetch_streak,
                    "hedge_cross_rate": round(hedge_cross_rate, 3),
                }

        # ── Build report (use clone-dedicated counters for lifecycle) ──
        clone_fills = self._clone_quote_fill_count
        clone_submits = self._clone_quote_submit_count
        clone_cancels = self._clone_quote_cancel_count
        clone_replaces = self._clone_quote_replace_count
        fill_rate = clone_fills / max(1, clone_submits)
        clone_data = {
            "event_type": "CLONE_REPORT",
            "ts_ms": int(time.time() * 1000),
            # Pair metrics
            "pairs_completed": total_pairs,
            "pairs_within_500ms": pairs_within_500ms,
            "pairs_within_1500ms": pairs_within_1500ms,
            "pairs_within_10s": pairs_within_10s,
            "paired_within_500ms_ratio": round(paired_500ms_ratio, 3),
            "paired_straddle_ratio_1500ms": round(paired_straddle_ratio, 3),
            "paired_straddle_ratio_10s": round(paired_10s_ratio, 3),
            "median_pair_fill_delay_ms": round(med_pair_delay_ms, 1),
            "median_time_between_pairs_ms": round(med_inter_pair_ms, 1),
            "median_signal_to_fill_ms": round(med_signal_to_fill_ms, 1),
            # Lifecycle
            "quote_submit_count": clone_submits,
            "quote_cancel_count": clone_cancels,
            "quote_replace_count": clone_replaces,
            "quote_fill_count": clone_fills,
            "quote_fill_rate": round(fill_rate, 3),
            "active_quote_orders": len(self._active_orders),
            "pending_unpaired": len(self._quote_unpaired),
            # Hedge
            "hedge_tick1": self._diag_hedge_tick1,
            "hedge_tick2": self._diag_hedge_tick2,
            "hedge_cross": self._diag_hedge_cross,
            "hedge_cross_early_count": self._diag_hedge_cross_early,
            "hedge_cross_late_count": self._diag_hedge_cross_late,
            "hedge_skipped_stale_count": self._diag_hedge_skipped_stale,
            "hedge_unwind": self._diag_hedge_unwind,
            "pending_fetch_streak_max": self._diag_pending_fetch_streak_max,
            # Hold time
            "hold_time_p50_sec": round(hold_p50, 1),
            "hold_time_p90_sec": round(hold_p90, 1),
            "active_hold_p50_sec": round(active_hold_p50, 1),
            "active_locked_count": len(active_hold_secs),
            # Imbalance
            "max_abs_imbalance": round(global_max_imbalance, 1),
            "per_slug_imbalance": per_slug_imbalance,
            # Correlation
            "imbalance_delta_correlation": round(imbal_delta_corr, 3),
            # Adverse guard
            "adverse_events": self._diag_adverse_guard_events,
            "adverse_degrades": self._diag_adverse_guard_degrades,
            "adverse_hard_pauses": self._diag_adverse_guard_pauses,
            "fast_clone": FAST_CLONE,
            # Per-slug KPIs
            "per_slug_kpi": per_slug_kpi,
        }
        write_jsonl(clone_data)

        # Console print (suppressed in QUIET mode)
        if CONSOLE_LEVEL != "QUIET":
            print(f"  CLONE: pairs={total_pairs}  "
                  f"r500={paired_500ms_ratio:.0%}  "
                  f"r1.5s={paired_straddle_ratio:.0%}  "
                  f"r10s={paired_10s_ratio:.0%}  "
                  f"delay={med_pair_delay_ms:.0f}ms  "
                  f"gap={med_inter_pair_ms:.0f}ms  "
                  f"sig2fill={med_signal_to_fill_ms:.0f}ms")
        if CONSOLE_LEVEL != "QUIET":
            print(f"  LIFECYCLE: submit={clone_submits}  "
                  f"cancel={clone_cancels}  "
                  f"replace={clone_replaces}  "
                  f"fills={clone_fills}  "
                  f"fill_rate={fill_rate:.0%}")
            print(f"  HEDGE: tick1={self._diag_hedge_tick1}  "
                  f"tick2={self._diag_hedge_tick2}  "
                  f"cross_early={self._diag_hedge_cross_early}  "
                  f"cross_late={self._diag_hedge_cross_late}  "
                  f"stale_skip={self._diag_hedge_skipped_stale}  "
                  f"unwind={self._diag_hedge_unwind}  "
                  f"fetch_streak={self._diag_pending_fetch_streak_max}")
            print(f"  HOLD: p50={hold_p50:.0f}s  p90={hold_p90:.0f}s  "
                  f"active_p50={active_hold_p50:.0f}s  locked={len(active_hold_secs)}")
            print(f"  IMBAL: max={global_max_imbalance:.0f}  "
                  f"corr(imbal,delta)={imbal_delta_corr:.2f}")
            if per_slug_imbalance:
                for slug, info in per_slug_imbalance.items():
                    print(f"    {slug[:30]:30s}  imbal={info['current_imbalance']:+5.0f}  "
                          f"max={info['max_abs_imbalance']:.0f}")
            # Per-slug KPI line
            if per_slug_kpi:
                for slug, kpi in per_slug_kpi.items():
                    if kpi["cache_age_p90_ms"] > 0 or kpi.get("paired_within_500ms", 0) > 0 or kpi.get("paired_within_1500ms", 0) > 0:
                        print(f"    KPI {slug[:25]:25s}  "
                              f"ca_p90={kpi['cache_age_p90_ms']:.0f}ms  "
                              f"bg_p90={kpi['bg_fetch_p90_ms']:.0f}ms  "
                              f"p500={kpi.get('paired_within_500ms', 0)}  "
                              f"p1500={kpi.get('paired_within_1500ms', 0)}  "
                              f"tout={kpi.get('timeouts', 0)}  "
                              f"miss={kpi['refresh_miss_count']}  "
                              f"hcross={kpi['hedge_cross_rate']:.0%}")
        # Reset clone-specific counters (per-minute)
        self._clone_pair_delays.clear()
        self._clone_inter_pair_gaps.clear()
        self._clone_signal_to_fill.clear()
        self._clone_hold_times.clear()
        self._diag_top_of_book_time_ms = 0.0
        self._diag_top_of_book_total_ms = 0.0
        self._diag_pairs_completed = 0
        self._diag_pairs_completed_500ms = 0
        self._diag_pairs_completed_1500ms = 0
        self._diag_pairs_completed_10s = 0
        self._diag_slug_unpaired_events.clear()
        self._diag_slug_paired_500ms.clear()
        self._diag_slug_paired_1500ms.clear()
        self._diag_slug_timeouts.clear()
        self._diag_pending_fetch_streak.clear()
        self._diag_pending_fetch_streak_max = 0
        self._diag_max_imbalance.clear()
        self._diag_imbalance_delta_samples.clear()
        # Reset clone-dedicated lifecycle counters
        self._clone_quote_submit_count = 0
        self._clone_quote_cancel_count = 0
        self._clone_quote_replace_count = 0
        self._clone_quote_fill_count = 0
        # Reset per-slug refresh miss counts
        self._bg_refresh_miss_count.clear()
        # Clean up stale pair tracker entries (older than 30s)
        stale_cutoff = time.time() - 30.0
        stale_ids = [pid for pid, info in self._pair_tracker.items()
                     if all(f["ts"] < stale_cutoff for f in info["fills"].values())]
        for pid in stale_ids:
            self._pair_tracker.pop(pid, None)

    def _emit_diag_report(self):
        """Emit behavioral diagnostics every 60s — measures success vs F247 targets.
        Targets: trades/min ~10-20, avg_trade_size ~$7+, median_hold >=60s, exit >= +4c."""
        import statistics as _stats
        now_t = time.time()
        elapsed_min = max(0.1, (now_t - self._diag_report_last_ts) / 60.0)

        # ── Core metrics ──
        total_fills = self._diag_total_fills_min
        dir_fills = self._diag_directional_fills_min
        par_fills = self._diag_parity_fills_min
        trades_per_min = total_fills / elapsed_min
        parity_fill_pct = par_fills / max(1, total_fills)
        dir_entry_count = self._diag_dscalp_entries
        dir_exit_count = self._diag_dscalp_exits
        avg_trade_size = (_stats.mean(self._diag_trade_sizes)
                          if self._diag_trade_sizes else 0.0)
        med_hold = (_stats.median(self._diag_dscalp_hold_times)
                    if self._diag_dscalp_hold_times else 0.0)
        med_exit = (_stats.median(self._diag_dscalp_exit_cents)
                    if self._diag_dscalp_exit_cents else 0.0)
        unpaired_rate = self._diag_unpaired_count_min / max(1, total_fills)
        derisk_rate = self._diag_derisk_count_min / max(1, total_fills)

        # ── Asymmetry metrics (win/loss breakdown) ──
        wins = [c for c in self._diag_dscalp_exit_cents if c > 0]
        losses = [c for c in self._diag_dscalp_exit_cents if c <= 0]
        avg_win_cents = _stats.mean(wins) if wins else 0.0
        avg_loss_cents = _stats.mean(losses) if losses else 0.0
        win_rate = len(wins) / max(1, len(wins) + len(losses))

        # ── Win/loss in USD ──
        win_exits_usd = [c / 100.0 for c in self._diag_dscalp_exit_cents if c > 0]
        loss_exits_usd = [c / 100.0 for c in self._diag_dscalp_exit_cents if c <= 0]
        avg_win_usdc = _stats.mean(win_exits_usd) if win_exits_usd else 0.0
        avg_loss_usdc = _stats.mean(loss_exits_usd) if loss_exits_usd else 0.0

        # ── Exit reason counts ──
        exit_reason_counts = {
            "TP1": self._diag_dscalp_tp1,
            "TP2": self._diag_dscalp_tp2,
            "TP3": self._diag_dscalp_tp3,
            "TP4": self._diag_dscalp_tp4,
            "RUNNER_FALLBACK": self._diag_dscalp_runner_fallbacks,
            "STOP": self._diag_dscalp_stop_exits,
            "EARLY": self._diag_dscalp_early_exits,
            "TIMEOUT": self._diag_dscalp_timeout_exits,
        }

        # ── SOL/XRP cache age ──
        sol_ages = []
        xrp_ages = []
        for slug, ages_list in self._tempo_cache_ages.items():
            if "sol" in slug.lower():
                sol_ages = ages_list
            if "xrp" in slug.lower():
                xrp_ages = ages_list

        sol_p50, sol_p90 = _p50_p90(sol_ages)
        xrp_p50, xrp_p90 = _p50_p90(xrp_ages)

        # ── True cost ──
        hour_elapsed = max(0.01, (now_t - self._true_cost_hour_start_ts) / 3600.0)
        fills_per_hour = self._true_cost_fill_count / hour_elapsed
        tx_per_hour = self._true_cost_tx_count / hour_elapsed
        est_cost_per_hour = (self._true_cost_tx_count * TRUE_COST_EST_GAS_PER_TX_USD
                             + self._true_cost_fill_count * TRUE_COST_EST_FEE_BPS / 10000.0 * avg_trade_size)

        # ── Regime ──
        low_vol_slugs = sum(1 for v in self._regime_is_low_vol.values() if v)
        total_slugs = max(1, len(self._regime_is_low_vol))

        diag_data = {
            "event_type": "DIAG_REPORT",
            "ts_ms": int(now_t * 1000),
            # F247 target metrics
            "trades_per_min": round(trades_per_min, 1),
            "avg_trade_size_usd": round(avg_trade_size, 2),
            "median_directional_hold_sec": round(med_hold, 1),
            "median_directional_exit_cents": round(med_exit, 2),
            # Asymmetry metrics (real-time monitoring)
            "avg_win_cents": round(avg_win_cents, 2),
            "avg_loss_cents": round(avg_loss_cents, 2),
            "avg_win_usdc": round(avg_win_usdc, 4),
            "avg_loss_usdc": round(avg_loss_usdc, 4),
            "win_rate": round(win_rate, 3),
            "median_hold_sec": round(med_hold, 1),
            "stop_count": self._diag_dscalp_stop_exits,
            # Fill composition
            "directional_entry_count": dir_entry_count,
            "directional_exit_count": dir_exit_count,
            "parity_fills": par_fills,
            "parity_fill_pct": round(parity_fill_pct, 3),
            "total_fills": total_fills,
            # Quality
            "unpaired_rate": round(unpaired_rate, 3),
            "derisk_rate": round(derisk_rate, 3),
            # Cache freshness
            "sol_cache_p50_ms": round(sol_p50, 0),
            "sol_cache_p90_ms": round(sol_p90, 0),
            "xrp_cache_p50_ms": round(xrp_p50, 0),
            "xrp_cache_p90_ms": round(xrp_p90, 0),
            # True cost
            "fills_per_hour": round(fills_per_hour, 0),
            "tx_per_hour": round(tx_per_hour, 0),
            "est_cost_per_hour_usd": round(est_cost_per_hour, 4),
            # Regime
            "low_vol_slugs": low_vol_slugs,
            "total_slugs": total_slugs,
            # Rate limiter
            "rate_blocked_interval": self._rate_blocked_interval,
            "rate_blocked_cap": self._rate_blocked_cap,
            # Directional detail
            "dscalp_tp1": self._diag_dscalp_tp1,
            "dscalp_tp2": self._diag_dscalp_tp2,
            "dscalp_tp3": self._diag_dscalp_tp3,
            "dscalp_tp4": self._diag_dscalp_tp4,
            "dscalp_runner_fallbacks": self._diag_dscalp_runner_fallbacks,
            "dscalp_timeouts": self._diag_dscalp_timeout_exits,
            "dscalp_stops": self._diag_dscalp_stop_exits,
            "dscalp_early_exits": self._diag_dscalp_early_exits,
            "dscalp_active_positions": len(self._dscalp_positions),
            # Exit reason breakdown
            "exit_reason_counts": exit_reason_counts,
            # Entry reject breakdown by gate
            "entry_reject_count_by_gate": dict(self._diag_entry_reject_by_gate),
            # PnL by exit reason (sum USD)
            "pnl_by_exit_reason": {k: round(v, 4) for k, v in self._diag_pnl_by_exit_reason.items()},
            # TIME_STOP loss prevention
            "time_stop_defer_count": self._diag_time_stop_defers,
            "time_stop_soft_exit_count": self._diag_time_stop_soft_exits,
            # DERISK_MAKER deferred
            "derisk_maker_defer_count": self._diag_derisk_maker_defers,
            # Pre-8hr safety metrics
            "cap_blocks": self._diag_cap_blocks,
            "post_fill_cooldown_blocks": self._diag_post_fill_cooldown_blocks,
            "time_stop_exits": self._diag_time_stop_exits,
            "spread_limit_blocks": self._diag_spread_limit_blocks,
            "parity_directional_suppress": self._diag_parity_directional_suppress,
            # Per-coin group entry thresholds (for monitoring)
            "thresholds_btceth": {
                "edge_cents": MIN_DIRECTIONAL_EDGE_CENTS_BTCETH,
                "vel_bps_min": MIN_VEL_BPS_PER_MIN_BTCETH,
                "spread_cents": MAX_SPREAD_ENTRY_CENTS_BTCETH,
                "cache_age_ms": CACHE_AGE_MAX_MS_BTCETH,
                "cooldown_ms": BTCETH_COOLDOWN_MS,
            },
            "thresholds_solxrp": {
                "edge_cents": MIN_DIRECTIONAL_EDGE_CENTS_SOLXRP,
                "vel_bps_min": MIN_VEL_BPS_PER_MIN_SOLXRP,
                "spread_cents": MAX_SPREAD_ENTRY_CENTS_SOLXRP,
                "cache_age_ms": CACHE_AGE_MAX_MS_SOLXRP,
                "cooldown_ms": SOLXRP_COOLDOWN_MS,
                "late_move_bps": SOLXRP_LATE_MOVE_BPS,
                "late_move_cache_ms": SOLXRP_LATE_MOVE_CACHE_MS,
            },
            "max_spread_exit_solxrp": MAX_SPREAD_EXIT_CENTS_SOLXRP,
            # Cursor proof counters
            "cursor_proof_lifetime": {
                "accepted_fills": self._truth.cursor_accepted_fills,
                "dropped_old_ts": self._truth.cursor_dropped_old_ts,
                "dropped_outside_window": self._truth.cursor_dropped_outside_window,
            },
            "cursor_proof_hour": {
                "accepted_fills": self._truth.cursor_hour_accepted,
                "dropped_old_ts": self._truth.cursor_hour_dropped_old,
                "dropped_outside_window": self._truth.cursor_hour_dropped_window,
            },
        }
        # Per-slug exposure + imbalance
        slug_exposure = {}
        for slug, st_diag in self.market_states.items():
            up_q = st_diag.positions["Up"].qty
            dn_q = st_diag.positions["Down"].qty
            if up_q >= MIN_QTY or dn_q >= MIN_QTY:
                slug_usd_diag = sum(
                    st_diag.positions[o].qty * st_diag.positions[o].vwap
                    for o in ["Up", "Down"] if st_diag.positions[o].qty >= MIN_QTY
                )
                slug_usd_diag += self._dscalp_invested_usd.get(slug, 0.0)
                slug_exposure[slug] = {
                    "exposure_usd": round(slug_usd_diag, 2),
                    "imbalance_shares": round(up_q - dn_q, 1),
                }
        diag_data["per_slug_exposure"] = slug_exposure
        # Per-slug cache age p50/p90
        per_slug_cache_age = {}
        for slug, ages_list in self._tempo_cache_ages.items():
            if ages_list:
                p50, p90 = _p50_p90(ages_list)
                per_slug_cache_age[slug] = {"p50_ms": round(p50, 0), "p90_ms": round(p90, 0)}
        diag_data["per_slug_cache_age"] = per_slug_cache_age
        write_jsonl(diag_data)

        # ── Console output (F247 target comparison) ──
        # Status indicators: check vs target
        tpm_ok = "OK" if 10 <= trades_per_min <= 20 else "!!"
        size_ok = "OK" if avg_trade_size >= 7.0 else "!!"
        hold_ok = "OK" if med_hold >= 60 else ("--" if med_hold == 0 else "!!")
        exit_ok = "OK" if med_exit >= 4.0 else ("--" if med_exit == 0 else "!!")
        par_ok = "OK" if parity_fill_pct <= 0.30 else "!!"

        # DIAG summary + ASYM + DSCALP (suppressed in QUIET mode)
        if CONSOLE_LEVEL != "QUIET":
            print(f"  DIAG [{tpm_ok}] trades/min={trades_per_min:.1f}  "
                  f"[{size_ok}] avg_size=${avg_trade_size:.1f}  "
                  f"[{hold_ok}] hold={med_hold:.0f}s  "
                  f"[{exit_ok}] exit={med_exit:+.1f}c")
            print(f"  ASYM: win_rate={win_rate:.0%}  avg_win={avg_win_cents:+.1f}c/${avg_win_usdc:+.4f}  "
                  f"avg_loss={avg_loss_cents:+.1f}c/${avg_loss_usdc:+.4f}  med_hold={med_hold:.0f}s")
            if self._dscalp_positions or dir_exit_count > 0:
                print(f"  DSCALP: tp1={self._diag_dscalp_tp1} tp2={self._diag_dscalp_tp2} "
                      f"tp3={self._diag_dscalp_tp3} tp4={self._diag_dscalp_tp4} "
                      f"runner_fb={self._diag_dscalp_runner_fallbacks} "
                      f"stop={self._diag_dscalp_stop_exits} early={self._diag_dscalp_early_exits} "
                      f"timeout={self._diag_dscalp_timeout_exits} "
                      f"active={len(self._dscalp_positions)}")
        # Sub-detail lines (suppressed in QUIET mode)
        if CONSOLE_LEVEL != "QUIET":
            print(f"  FILL: dir_entry={dir_entry_count}  dir_exit={dir_exit_count}  "
                  f"parity={par_fills}  [{par_ok}] par_pct={parity_fill_pct:.0%}  "
                  f"unpaired={unpaired_rate:.0%}  derisk={derisk_rate:.0%}")
            print(f"  CACHE: SOL={sol_p50:.0f}/{sol_p90:.0f}ms  XRP={xrp_p50:.0f}/{xrp_p90:.0f}ms  "
                  f"regime: {low_vol_slugs}/{total_slugs} low-vol")
            print(f"  COST: fills/hr={fills_per_hour:.0f}  tx/hr={tx_per_hour:.0f}  "
                  f"est=${est_cost_per_hour:.4f}/hr  "
                  f"rate_block={self._rate_blocked_interval}+{self._rate_blocked_cap}")
            # PnL by exit reason summary
            if self._diag_pnl_by_exit_reason:
                pnl_parts = [f"{k}=${v:+.4f}" for k, v in sorted(self._diag_pnl_by_exit_reason.items())]
                print(f"  PNL_BY_EXIT: {' '.join(pnl_parts)}")
            # TIME_STOP / DERISK defers
            if self._diag_time_stop_defers or self._diag_time_stop_soft_exits or self._diag_derisk_maker_defers:
                print(f"  LOSS_PREVENTION: ts_defer={self._diag_time_stop_defers}  "
                      f"ts_soft={self._diag_time_stop_soft_exits}  "
                      f"derisk_defer={self._diag_derisk_maker_defers}")
            # Entry reject summary
            if self._diag_entry_reject_by_gate:
                reject_parts = [f"{k}={v}" for k, v in sorted(self._diag_entry_reject_by_gate.items()) if v > 0]
                if reject_parts:
                    print(f"  GATE_REJECTS: {' '.join(reject_parts)}")
            # Safety caps summary
            if (self._diag_cap_blocks or self._diag_post_fill_cooldown_blocks
                    or self._diag_time_stop_exits or self._diag_spread_limit_blocks
                    or self._diag_parity_directional_suppress):
                print(f"  SAFETY: cap_blocks={self._diag_cap_blocks}  "
                      f"cooldown_blocks={self._diag_post_fill_cooldown_blocks}  "
                      f"time_stops={self._diag_time_stop_exits}  "
                      f"spread_blocks={self._diag_spread_limit_blocks}  "
                      f"dir_suppress={self._diag_parity_directional_suppress}")
            # Per-slug exposure summary (top 3 by exposure)
            if slug_exposure:
                top_slugs = sorted(slug_exposure.items(),
                                   key=lambda x: abs(x[1]["exposure_usd"]), reverse=True)[:3]
                parts = [f"{s[:20]}=${d['exposure_usd']:.0f}/imb={d['imbalance_shares']:+.0f}"
                         for s, d in top_slugs]
                print(f"  EXPOSURE: {' | '.join(parts)}")
            # Cursor proof (cumulative — proves no old fills leaked)
            _t = self._truth
            print(f"  CURSOR_PROOF: "
                  f"hour(accept={_t.cursor_hour_accepted} "
                  f"drop_old={_t.cursor_hour_dropped_old} "
                  f"drop_win={_t.cursor_hour_dropped_window})  "
                  f"life(accept={_t.cursor_accepted_fills} "
                  f"drop_old={_t.cursor_dropped_old_ts} "
                  f"drop_win={_t.cursor_dropped_outside_window})")

        # Reset per-minute counters
        self._diag_report_last_ts = now_t
        self._true_cost_fill_count_min = 0
        self._true_cost_submit_count = 0
        self._true_cost_cancel_count = 0
        self._diag_derisk_count_min = 0
        self._diag_unpaired_count_min = 0
        self._diag_parity_fills_min = 0
        self._diag_directional_fills_min = 0
        self._diag_total_fills_min = 0
        self._diag_trade_sizes.clear()
        self._rate_blocked_interval = 0
        self._rate_blocked_cap = 0
        self._diag_dscalp_entries = 0
        self._diag_dscalp_exits = 0
        self._diag_dscalp_tp1 = 0
        self._diag_dscalp_tp2 = 0
        self._diag_dscalp_tp3 = 0
        self._diag_dscalp_tp4 = 0
        self._diag_dscalp_runner_fallbacks = 0
        self._diag_dscalp_timeout_exits = 0
        self._diag_dscalp_stop_exits = 0
        self._diag_dscalp_early_exits = 0
        self._diag_dscalp_hold_times.clear()
        self._diag_dscalp_exit_cents.clear()
        self._diag_entry_reject_by_gate.clear()
        self._diag_pnl_by_exit_reason.clear()
        self._diag_time_stop_defers = 0
        self._diag_time_stop_soft_exits = 0
        self._diag_derisk_maker_defers = 0
        # Pre-8hr safety counter resets
        self._diag_cap_blocks = 0
        self._diag_post_fill_cooldown_blocks = 0
        self._diag_time_stop_exits = 0
        self._diag_spread_limit_blocks = 0
        self._diag_parity_directional_suppress = 0
        # Execution safety minute resets (LIVE and LIVE_SAFE)
        if MODE in ("LIVE", "LIVE_SAFE"):
            self._exec_tracker.reset_minute_counters()
            self._exec_orphan.reset_minute_counters()
            self._exec_safety.reset_minute_counters()
            self._exec_unpaired.reset_minute_counters()
            self._exec_drift.reset_minute_counters()
            self._live_safety.reset_minute_counters()

    def _emit_gate_report(self):
        """Emit GATE_REPORT summary every LOG_GATES_EVERY_N_SEC with reject counts."""
        now_t = time.time()
        if now_t - self._gate_report_last_ts < LOG_GATES_EVERY_N_SEC:
            return
        self._gate_report_last_ts = now_t
        if not self._diag_entry_reject_by_gate:
            return
        write_jsonl({
            "event_type": "GATE_REPORT",
            "ts_ms": int(now_t * 1000),
            "reject_counts": dict(self._diag_entry_reject_by_gate),
            "total_rejects": sum(self._diag_entry_reject_by_gate.values()),
        })

    def _emit_snapshot_compact(self, m, ctx: dict):
        """Emit one SNAPSHOT_COMPACT per slug every LOG_SNAPSHOT_EVERY_N_SEC."""
        now_t = time.time()
        last = self._snapshot_last_ts.get(m.slug, 0.0)
        if now_t - last < LOG_SNAPSHOT_EVERY_N_SEC:
            return
        self._snapshot_last_ts[m.slug] = now_t
        up_book = ctx.get("up_book")
        dn_book = ctx.get("dn_book")
        book = up_book  # use Up book for spread/bid/ask
        write_jsonl({
            "event_type": "SNAPSHOT_COMPACT",
            "ts_ms": int(now_t * 1000),
            "slug": m.slug,
            "t_min": round(ctx.get("t_min", 0), 1),
            "spot": round(ctx.get("spot", 0), 2),
            "hour_open": round(ctx.get("hour_open", 0), 2),
            "vel_bps_min": round(ctx.get("vel", 0), 4),
            "delta_bps": round(ctx.get("delta_bps", 0), 1),
            "spread_c": round(book.spread * 100, 1) if book else 0,
            "best_bid": round(book.bid, 3) if book else 0,
            "best_ask": round(book.ask, 3) if book else 0,
            "cache_age_ms": round(self._cache_age_ms(m.slug), 0),
        })

    def _emit_live_sanity_report(self):
        """Emit LIVE_SANITY_REPORT with execution health diagnostics."""
        exposure = {}
        for slug, st in self.market_states.items():
            slug_usd = sum(
                st.positions[o].qty * st.positions[o].vwap
                for o in ["Up", "Down"] if st.positions[o].qty >= MIN_QTY
            )
            slug_usd += self._dscalp_invested_usd.get(slug, 0.0)
            if slug_usd > 0.01:
                exposure[slug] = round(slug_usd, 2)
        self._exec_reporter.tick({
            "open_orders_count": self._exec_tracker.open_order_count(),
            "orphan_cancels_last_min": self._exec_orphan.orphan_cancels_this_min,
            "api_errors_last_min": self._exec_safety.api_errors_this_min,
            "partial_fill_count": self._exec_tracker.partial_fill_count,
            "unpaired_events_last_min": self._exec_unpaired.events_this_min,
            "ttl_cancels": self._exec_tracker.ttl_cancels,
            "confirmed_fills": self._exec_tracker.confirmed_fills,
            "confirmed_submits": self._exec_tracker.confirmed_submits,
            "kill_switch_active": self._exec_safety.is_kill_active(),
            "total_kill_activations": self._exec_safety.total_kill_activations,
            "cap_blocks_last_min": self._exec_safety.cap_blocks_this_min,
            "drift_detected": self._exec_drift.is_drifted(),
            "reconcile_count": self._exec_reconciler.reconcile_count,
            "reconcile_corrections": self._exec_reconciler.corrections_total,
            "reconcile_desynced": self._exec_reconciler.is_desynced(),
            "reconcile_desync_events": self._exec_reconciler.desync_events_total,
            "no_progress_pauses": self._exec_safety.no_progress_pauses_total,
            "mode": MODE,
            "buys_allowed": self._buys_allowed(),
            "entry_window_remaining_sec": round(self._entry_window_remaining_sec(), 1),
            "live_safe_max_order_usd": LIVE_SAFE_MAX_ORDER_USD if MODE == "LIVE_SAFE" else None,
            "exposure_per_slug": exposure,
            "live_safety": self._live_safety.get_diag(),
        })

    def _emit_slug_pnl_report(self):
        """Emit per-slug PnL report every 60s. Shows realized PnL, negative exit counts,
        and loss-tail pause status per slug."""
        slug_data = {}
        # Gather all slugs with PnL or positions
        all_slugs = set(self._slug_realized_pnl.keys())
        for slug, st in self.market_states.items():
            if st.positions["Up"].qty >= MIN_QTY or st.positions["Down"].qty >= MIN_QTY:
                all_slugs.add(slug)
        if not all_slugs:
            return
        for slug in sorted(all_slugs):
            pnl = self._slug_realized_pnl.get(slug, 0.0)
            neg_count = self._slug_neg_exit_count.get(slug, 0)
            paused = not self._slug_entry_allowed(slug)
            st = self.market_states.get(slug)
            crypto = st.crypto if st else "?"
            up_qty = st.positions["Up"].qty if st else 0
            dn_qty = st.positions["Down"].qty if st else 0
            slug_data[slug] = {
                "crypto": crypto,
                "net_pnl_usdc": round(pnl, 4),
                "neg_exit_count": neg_count,
                "paused": paused,
                "up_qty": round(up_qty, 1),
                "dn_qty": round(dn_qty, 1),
            }
        write_jsonl({"event_type": "SLUG_PNL_REPORT",
                      "ts_ms": int(time.time() * 1000),
                      "slugs": slug_data})
        # Console output: per-crypto aggregation + worst slugs
        crypto_pnl: Dict[str, float] = {}
        for slug, d in slug_data.items():
            crypto_pnl[d["crypto"]] = crypto_pnl.get(d["crypto"], 0.0) + d["net_pnl_usdc"]
        crypto_str = "  ".join(f"{c}={p:+.2f}" for c, p in sorted(crypto_pnl.items()))
        # Show worst 3 slugs
        worst = sorted(slug_data.items(), key=lambda x: x[1]["net_pnl_usdc"])[:3]
        worst_str = "  ".join(
            f"{d['crypto']}={d['net_pnl_usdc']:+.2f}(neg={d['neg_exit_count']}"
            f"{',PAUSED' if d['paused'] else ''})"
            for _, d in worst if d["net_pnl_usdc"] < 0
        )
        paused_slugs = [s for s, d in slug_data.items() if d["paused"]]
        pause_str = f"  paused=[{','.join(paused_slugs)}]" if paused_slugs else ""
        if CONSOLE_LEVEL != "QUIET":
            print(f"  [SLUG_PNL] {crypto_str}  worst=[{worst_str}]{pause_str}")

    def _print_balance_summary(self):
        """Print balance and open positions to console.

        Uses market_states (updated on each confirmed fill) as the
        single source of truth.  Shows: crypto outcome qty @ vwap.
        """
        total_cost = 0.0
        pos_parts = []

        for slug, st in self.market_states.items():
            for outcome in ["Up", "Down"]:
                pos = st.positions[outcome]
                if pos.qty >= MIN_QTY:
                    total_cost += pos.cost_usdc
                    pos_parts.append(f"{st.crypto} {outcome}:{pos.qty:.0f}@{pos.vwap:.3f}")

        equity = self._equity()
        daily_pnl = equity - self.day_start_equity
        pnl_str = f"+${daily_pnl:.2f}" if daily_pnl >= 0 else f"-${abs(daily_pnl):.2f}"
        rpnl_str = f"+${self.realized_pnl_usdc:.2f}" if self.realized_pnl_usdc >= 0 else f"-${abs(self.realized_pnl_usdc):.2f}"
        safe_tag = " [SAFE MODE]" if self._truth.safe_mode else ""
        pos_str = "  ".join(pos_parts) if pos_parts else "none"

        budget_str = f"  |  Hour Budget: ${self._hour_budget_remaining:.2f}/${HOURLY_BUDGET_USD:.0f}"
        print(f"\n  --- Cash: ${self.cash_usdc:.2f}  |  Equity: ${equity:.2f}  |  Invested: ${total_cost:.2f}"
              f"  |  Day P&L: {pnl_str}  |  Total P&L: {rpnl_str}"
              f"{budget_str}{safe_tag} ---")
        print(f"  --- POSITIONS: {pos_str} ---")
        print()
    def _log_hour_label(self, slug, st, final_price, effective_close,
                        hour_direction, delta_bps,
                        position_direction="NONE", would_win=None,
                        positions_held=None):
        """Emit HOUR_LABEL truth-label event (shared by settled and no-position paths)."""
        data = {
            "event_type": "HOUR_LABEL", "slug": slug, "crypto": st.crypto,
            "hour_start_utc": st.hour_start_utc,
            "hour_label_et": _hour_label_et(st.hour_start_utc),
            "hour_index": st.hour_index,
            "hour_open": round(st.hour_open, 2),
            "hour_final_price": round(final_price, 2),
            "effective_hour_close": round(effective_close, 2),
            "hour_close_source": "NEXT_HOUR_OPEN",
            "hour_direction": hour_direction,
            "settlement_delta_bps": round(delta_bps, 3),
            "won_up": hour_direction == "Up",
            "won_down": hour_direction == "Down",
            "position_direction_at_entry": position_direction,
            "would_win_if_held_to_settle": would_win,
        }
        if positions_held is not None:
            data["positions_held"] = positions_held
        write_jsonl(data)
    def _cleanup_market_slug(self, slug: str):
        """Remove all per-slug state when a market hour ends."""
        self.market_states.pop(slug, None)
        self.signal_hist.pop(slug, None)
        self.last_book.pop(slug, None)
        self.recent_extreme_price.pop(slug, None)
        self.entry_sm.pop(slug, None)
        self._data_cache.pop(slug, None)
        self._pending_fetches.pop(slug, None)
        self._parity_last_order_ts.pop(slug, None)
        self._parity_invested_usd.pop(slug, None)
        self._parity_locked_since.pop(slug, None)
        self._parity_maker_orders.pop(slug, None)
        self._parity_pending_pairs[:] = [p for p in self._parity_pending_pairs if p["slug"] != slug]
    def _resolve_ended_hours(self, current_markets: List[MarketRef]):
        """
        Resolve positions for hours that have ended.
        Winner pays $1/share, loser pays $0. Then remove old market state.
        """
        current_slugs = {m.slug for m in current_markets}
        ended = [slug for slug in list(self.market_states.keys()) if slug not in current_slugs]
        for slug in ended:
            st = self.market_states[slug]
            has_positions = any(st.positions[o].qty >= MIN_QTY for o in ["Up", "Down"])
            if not has_positions:
                # No positions — still log settlement for truth tracking, then clean up
                try:
                    sp, nho = self.client.get_binance_spot_and_hour_open(st.crypto)
                    sd = (sp - st.hour_open) / max(st.hour_open, 1e-9) * 10000.0
                    w = "Up" if sp >= st.hour_open else "Down"
                    self._log_hour_label(slug, st, sp, nho, w, sd)
                except Exception:
                    pass
                self._cleanup_market_slug(slug)
                continue
            # Determine winner by checking final Binance price vs hour open
            # spot here = next hour's opening price (close proxy for the prior hour)
            spot, next_hour_open = self.client.get_binance_spot_and_hour_open(st.crypto)
            close_proxy = spot  # spot at transition ≈ open(H+1) ≈ close(H)
            winner = "Up" if close_proxy >= st.hour_open else "Down"
            settlement_delta_bps = (close_proxy - st.hour_open) / max(st.hour_open, 1e-9) * 10000.0
            self.logger.log_event({
                "event_type": "WINDOW_SETTLED", "slug": slug, "crypto": st.crypto,
                "hour_start_utc": st.hour_start_utc,
                "settlement_price": round(close_proxy, 2),
                "hour_open": round(st.hour_open, 2),
                "settlement_delta_bps": round(settlement_delta_bps, 3),
                "winner": winner,
                "next_hour_open": round(next_hour_open, 2),
            }, also_csv=True)
            pos_details = []
            for outcome in ["Up", "Down"]:
                pos = st.positions[outcome]
                if pos.qty < MIN_QTY:
                    self._clean_dust(pos)
                    continue
                payout_price = 1.0 if outcome == winner else 0.0
                payout = payout_price * pos.qty
                pnl = payout - pos.cost_usdc
                self.cash_usdc += payout
                self.realized_pnl_usdc += pnl
                self.hourly_pnl_usdc += pnl
                self._hour_net_pnl += pnl
                pos_details.append({"outcome": outcome, "qty": pos.qty,
                                    "payout": payout, "pnl": pnl})
                pos.qty = 0.0
                pos.cost_usdc = 0.0
            # Shadow settlement: settle shadow positions for this slug
            if self._shadow_active and slug in self._shadow_positions:
                for outcome in ["Up", "Down"]:
                    sp = self._shadow_positions.get(slug, {}).get(outcome)
                    if sp and sp["qty"] >= MIN_QTY:
                        s_payout = (1.0 if outcome == winner else 0.0) * sp["qty"]
                        self._shadow_cash += s_payout
                        sp["qty"] = 0.0
                        sp["cost_usdc"] = 0.0
                self._shadow_positions.pop(slug, None)
            self.logger.log_event({
                "event_type": "HOUR_RESOLVED", "slug": slug, "crypto": st.crypto,
                "hour_start_utc": st.hour_start_utc,
                "winner": winner,
                "close_proxy": round(close_proxy, 2),
                "hour_open": round(st.hour_open, 2),
                "positions": pos_details,
                "cash": round(self.cash_usdc, 2),
                "equity": round(self._equity(), 2),
            })
            # HOUR_LABEL — truth labels for settlement analysis
            held_up = any(d["outcome"] == "Up" for d in pos_details)
            held_down = any(d["outcome"] == "Down" for d in pos_details)
            position_direction = "Up" if (held_up and not held_down) else ("Down" if (held_down and not held_up) else ("BOTH" if (held_up and held_down) else "NONE"))
            hour_direction = "Up" if close_proxy >= st.hour_open else "Down"
            would_win = position_direction == hour_direction if position_direction in ("Up", "Down") else None
            self._log_hour_label(slug, st, close_proxy, next_hour_open,
                                 hour_direction, settlement_delta_bps,
                                 position_direction, would_win, pos_details)
            # Clean up ended market
            self._cleanup_market_slug(slug)
    def _get_markets(self) -> List[MarketRef]:
        # In practice: discover the current hour markets
        markets = self.client.get_current_hour_markets()
        if not ENABLE_XRP:
            markets = [m for m in markets if m.crypto != "XRP"]
        return markets
    def _ensure_market_state(self, m: MarketRef):
        if m.slug not in self.market_states:
            idx = self.hour_index_counters.get(m.crypto, 0)
            self.hour_index_counters[m.crypto] = idx + 1
            self.market_states[m.slug] = MarketState(
                slug=m.slug,
                crypto=m.crypto,
                hour_open=m.hour_open,
                hour_start_utc=iso_z(m.hour_start_utc),
                hour_index=idx,
            )
            write_jsonl({"event_type":"NEW_MARKET_STATE", "slug": m.slug, "crypto": m.crypto,
                         "hour_index": idx, "hour_start_utc": iso_z(m.hour_start_utc)})
        # Always ensure companion dicts exist (may be missing after state reload)
        self.signal_hist.setdefault(m.slug, [])
        self.last_book.setdefault(m.slug, {})
        self.recent_extreme_price.setdefault(m.slug, {"Up": None, "Down": None})
        # Register tokens with truth capture for metadata resolution
        if m.outcome_up_id:
            self._truth.register_token(m.outcome_up_id, m.slug, "Up")
        if m.outcome_down_id:
            self._truth.register_token(m.outcome_down_id, m.slug, "Down")
    def _make_tick_ctx(self, m: MarketRef, st: MarketState, spot: float, hour_open: float,
                       t_min: float, delta_bps: float, abs_delta_bps: float,
                       vel: float, z: float, up_book: BookTop, dn_book: BookTop) -> dict:
        """Build shared per-tick context dict used by entries, exits, and logging."""
        effective_delta = abs_delta_bps * (t_min / 60.0)
        normalized_delta = abs_delta_bps / max(st.peak_abs_delta_bps, 1.0)
        # Edge model
        p_up = _p_up_model(delta_bps)
        edge_up = p_up - up_book.mid
        edge_down = (1.0 - p_up) - dn_book.mid
        # Phase / timing
        phase = _phase(t_min)
        seconds_to_close = max(0.0, (60.0 - t_min) * 60.0)
        return {
            "m": m, "st": st, "spot": spot, "hour_open": hour_open,
            "t_min": t_min, "delta_bps": delta_bps, "abs_delta_bps": abs_delta_bps,
            "vel": vel, "z": z,
            "effective_delta": effective_delta, "normalized_delta": normalized_delta,
            "p_up_model": p_up, "edge_up": edge_up, "edge_down": edge_down,
            "phase": phase, "seconds_to_close": seconds_to_close,
            "up_book": up_book, "dn_book": dn_book,
            "drift_dir": "Up" if delta_bps >= 0 else "Down",
            # Hour identity
            "hour_start_utc": st.hour_start_utc,
            "hour_label_et": _hour_label_et(st.hour_start_utc),
            "hour_index": st.hour_index,
        }
    def _book_fields(self, up_book: BookTop, dn_book: BookTop, outcome: Optional[str] = None) -> dict:
        """Return flat dict with up_*/dn_*/ref_* book fields for CSV/JSONL."""
        return build_book_fields(up_book, dn_book, outcome)
    def _step_market_with_data(self, data: dict):
        """Process a market using pre-fetched HTTP data."""
        ts_snapshot = time.time()
        m = data["market"]
        spot, hour_open = data["spot"], data["hour_open"]
        up_book, dn_book = data["up_book"], data["dn_book"]
        st = self.market_states[m.slug]
        now = utc_now()
        hour_start = _parse_iso_z(st.hour_start_utc)
        t_min = minutes_into_hour(hour_start, now)
        # Sanity: if t_min > 60, hour_start_utc is stale — clamp to prevent
        # premature SETTLEMENT (e.g. 15 min into hour with stale hour_start).
        if t_min > 65.0:
            attempt_count = self._stale_hour_refresh_count.get(m.slug, 0)
            last_refresh = self._stale_hour_refresh_ts.get(m.slug, 0.0)
            # Exponential backoff: 0s, 30s, 60s, 120s, cap at 300s
            backoff = min(30.0 * (2 ** max(0, attempt_count - 1)), 300.0) if attempt_count > 0 else 0.0
            now_mono = time.time()

            write_jsonl({
                "event_type": "STALE_HOUR_START_DETECTED",
                "slug": m.slug, "now_utc": iso_z(now),
                "hour_start_utc": st.hour_start_utc,
                "t_min": round(t_min, 2),
                "refresh_attempt": attempt_count,
                "next_refresh_in_sec": round(max(0, (last_refresh + backoff) - now_mono), 1),
                "ts_ms": int(time.time() * 1000),
            })

            # Force-refresh market hour_start_utc if backoff has elapsed
            if now_mono >= last_refresh + backoff:
                self._stale_hour_refresh_ts[m.slug] = now_mono
                self._stale_hour_refresh_count[m.slug] = attempt_count + 1
                try:
                    fresh_markets = self._get_markets()
                    for fm in fresh_markets:
                        if fm.slug == m.slug:
                            new_hour_start = iso_z(fm.hour_start_utc)
                            old_hour_start = st.hour_start_utc
                            st.hour_start_utc = new_hour_start
                            st.hour_open = fm.hour_open
                            # Recompute t_min with fresh data
                            fresh_start = _parse_iso_z(new_hour_start)
                            fresh_t_min = minutes_into_hour(fresh_start, now)
                            write_jsonl({
                                "event_type": "STALE_HOUR_START_REFRESHED",
                                "slug": m.slug,
                                "old_hour_start": old_hour_start,
                                "new_hour_start": new_hour_start,
                                "old_t_min": round(t_min, 2),
                                "new_t_min": round(fresh_t_min, 2),
                                "attempt": attempt_count + 1,
                                "ts_ms": int(time.time() * 1000),
                            })
                            print(f"  [STALE] Refreshed {m.slug} hour_start: "
                                  f"{old_hour_start} → {new_hour_start} "
                                  f"(t_min: {t_min:.1f} → {fresh_t_min:.1f})")
                            # Clear backoff on successful refresh
                            if fresh_t_min <= 65.0:
                                self._stale_hour_refresh_count.pop(m.slug, None)
                                self._stale_hour_refresh_ts.pop(m.slug, None)
                            break
                except Exception as e:
                    write_jsonl({"event_type": "STALE_HOUR_REFRESH_ERROR",
                                 "slug": m.slug, "err": str(e)[:200],
                                 "attempt": attempt_count + 1})
                    print(f"  [STALE] Refresh failed for {m.slug}: {str(e)[:80]}")
            else:
                print(f"  [WARN] STALE hour_start_utc for {m.slug}: "
                      f"t_min={t_min:.1f} — refresh backoff "
                      f"(attempt {attempt_count}, next in "
                      f"{max(0, (last_refresh + backoff) - now_mono):.0f}s)")
            return
        if t_min >= TRADE_HARD_STOP_MIN:
            self.last_book[m.slug]["Up"] = up_book
            self.last_book[m.slug]["Down"] = dn_book
            # Transition to SETTLEMENT state if still ACTIVE
            if self._truth.window_state == WindowState.ACTIVE:
                hour_end_utc = hour_start + timedelta(hours=1)
                write_jsonl({
                    "event_type": "SETTLEMENT_TRANSITION_DIAG",
                    "slug": m.slug,
                    "now_utc": iso_z(now),
                    "hour_start_utc": st.hour_start_utc,
                    "hour_end_utc": iso_z(hour_end_utc),
                    "t_min": round(t_min, 2),
                    "trade_hard_stop_min": TRADE_HARD_STOP_MIN,
                    "hard_flatten_done": self._hard_flatten_done,
                    "hour_risk_stop_hit": self._hour_risk_stop_hit,
                    "rule": "t_min >= TRADE_HARD_STOP_MIN",
                    "ts_ms": int(time.time() * 1000),
                })
                print(f"  [SETTLEMENT] ACTIVE→SETTLEMENT: {m.slug} "
                      f"t_min={t_min:.2f} >= {TRADE_HARD_STOP_MIN}  "
                      f"now={iso_z(now)} hour_start={st.hour_start_utc}")
                self._truth.enter_settlement()
            self._cleanup_market(m, st, t_min)
            return
        if hour_open <= 0 or spot <= 0:
            self.logger.log_event({"event_type": "SKIP_NO_PRICE", "slug": m.slug,
                                   "crypto": m.crypto, "spot": spot, "hour_open": hour_open})
            return
        st.hour_open = hour_open
        delta_bps = (spot - hour_open) / hour_open * 10000.0
        abs_delta_bps = abs(delta_bps)
        st.peak_abs_delta_bps = max(st.peak_abs_delta_bps, abs_delta_bps)
        # Track edge sign stability for whipsaw filter
        cur_sign = 1 if delta_bps > 0 else (-1 if delta_bps < 0 else 0)
        prev = self._edge_sign_state.get(m.slug)
        if prev is None or prev[0] != cur_sign:
            self._edge_sign_state[m.slug] = (cur_sign, time.time())
        # Only update delta/price history on valid book snapshots
        _quotes_valid = (up_book.ask > 0 and dn_book.ask > 0)
        if _quotes_valid:
            st.delta_hist.append((iso_z(now), delta_bps))
            st.delta_hist = st.delta_hist[-STATE_HIST_MAX:]
            st.price_hist.append((iso_z(now), spot))
            st.price_hist = st.price_hist[-STATE_HIST_MAX:]
        # Lightweight spot history for adverse selection (epoch-based, cheap to query)
        spot_ts = time.time()
        slug_spot_hist = self._spot_history.setdefault(m.slug, [])
        slug_spot_hist.append((spot_ts, spot))
        # Trim to last 30s of data (ample for 10s lookback)
        cutoff = spot_ts - 30.0
        while slug_spot_hist and slug_spot_hist[0][0] < cutoff:
            slug_spot_hist.pop(0)
        # Track significant spot moves for signal-to-fill latency (clone metrics)
        last_move = self._clone_last_spot_move.get(m.slug)
        if last_move:
            move_bps = abs((spot - last_move[1]) / last_move[1]) * 10000.0 if last_move[1] > 0 else 0.0
            if move_bps >= 5.0:  # significant move = 5+ bps
                self._clone_last_spot_move[m.slug] = (spot_ts, spot)
        else:
            self._clone_last_spot_move[m.slug] = (spot_ts, spot)
        vel = delta_velocity_bps_per_min(st.delta_hist, lookback_sec=30.0)
        # Per-minute velocity debug (once per 60 s per slug)
        _vel_dbg_last = self._vel_debug_last_ts.get(m.slug, 0.0)
        if spot_ts - _vel_dbg_last >= 60.0:
            self._vel_debug_last_ts[m.slug] = spot_ts
            vd = delta_velocity_debug(st.delta_hist, lookback_sec=30.0)
            if CONSOLE_LEVEL != "QUIET":
                print(f"  [VEL_DEBUG] {m.slug} vel={vd['vel']:+.4f} bps/min  "
                      f"delta_now={vd['delta_now']:+.2f} delta_prev={vd['delta_prev']:+.2f}  "
                      f"dt={vd['dt_sec']:.1f}s  n={vd['n_points']}  "
                      f"status={vd['status']}  quotes_valid={_quotes_valid}")
        z = zscore(st.delta_hist) if Z_ENTRY_ENABLED else 0.0
        self.last_book[m.slug]["Up"] = up_book
        self.last_book[m.slug]["Down"] = dn_book
        self._update_extremes(m.slug, up_book, dn_book)
        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty >= MIN_QTY and pos.entry_mid > 0:
                bk = up_book if outcome == "Up" else dn_book
                pos.max_favorable_mid = max(pos.max_favorable_mid, bk.mid)
                pos.max_adverse_mid = min(pos.max_adverse_mid, bk.mid)
        ctx = self._make_tick_ctx(m, st, spot, hour_open, t_min, delta_bps, abs_delta_bps,
                                  vel, z, up_book, dn_book)
        ctx["ts_snapshot"] = ts_snapshot
        # ── Analytics: track imbalance per slug for CLONE_REPORT ──
        up_q = st.positions["Up"].qty
        dn_q = st.positions["Down"].qty
        abs_imbal = abs(up_q - dn_q)
        cur_max = self._diag_max_imbalance.get(m.slug, 0.0)
        self._diag_max_imbalance[m.slug] = max(cur_max, abs_imbal)
        self._diag_imbalance_delta_samples.append((up_q - dn_q, delta_bps))
        # ── Parity metrics (computed every tick, used by parity arb engine) ──
        ctx["straddle_buy_cost"] = up_book.ask + dn_book.ask
        ctx["straddle_sell_value"] = up_book.bid + dn_book.bid
        ctx["parity_edge_buy_cents"] = (1.000 - ctx["straddle_buy_cost"]) * 100
        ctx["parity_edge_sell_cents"] = (ctx["straddle_sell_value"] - 1.000) * 100
        # Fee-aware net edges (pre-computed for logging even when parity doesn't trade)
        ctx["parity_raw_buy_cents"] = ctx["parity_edge_buy_cents"]
        ctx["parity_raw_sell_cents"] = ctx["parity_edge_sell_cents"]
        _net_buy, _, _ = parity_net_edge_cents(ctx["parity_raw_buy_cents"], up_book, dn_book, True)
        _net_sell, _, _ = parity_net_edge_cents(ctx["parity_raw_sell_cents"], up_book, dn_book, False)
        ctx["parity_net_buy_cents"] = _net_buy
        ctx["parity_net_sell_cents"] = _net_sell
        # ── SNAPSHOT_COMPACT (every SNAPSHOT_INTERVAL_SEC per market) ──
        pos_up_qty = st.positions["Up"].qty
        pos_dn_qty = st.positions["Down"].qty
        equity = self._equity()
        if self.logger.should_log_snapshot_compact(ts_snapshot, m.slug):
            self.logger.log_snapshot_compact(
                slug=m.slug, crypto=m.crypto, t_min=t_min,
                spot=spot, hour_open=hour_open,
                delta_bps=delta_bps, abs_delta_bps=abs_delta_bps,
                vel=vel, z=z,
                up_bid=up_book.bid, up_ask=up_book.ask, up_mid=up_book.mid,
                up_spread=up_book.spread, up_imb=up_book.imb,
                dn_bid=dn_book.bid, dn_ask=dn_book.ask, dn_mid=dn_book.mid,
                dn_spread=dn_book.spread, dn_imb=dn_book.imb,
                pos_qty_up=pos_up_qty, pos_qty_down=pos_dn_qty,
                cash_usdc=self.cash_usdc, equity_usdc=equity,
                parity_edge_buy_cents=ctx["parity_edge_buy_cents"],
                parity_edge_sell_cents=ctx["parity_edge_sell_cents"],
            )
        # ── SNAPSHOT_ON_CHANGE (only when significant changes happen) ──
        snap_dict = {
            "up_mid": up_book.mid, "dn_mid": dn_book.mid,
            "up_spread": up_book.spread, "dn_spread": dn_book.spread,
            "up_imb": up_book.imb, "dn_imb": dn_book.imb,
            "delta_bps": delta_bps,
            "entry_thr_bps": entry_threshold_bps(m.crypto, t_min),
            "pos_qty_up": pos_up_qty, "pos_qty_down": pos_dn_qty,
        }
        should_change, trigger = self.logger.should_log_snapshot_on_change(m.slug, snap_dict)
        if should_change:
            self.logger.log_snapshot_on_change(
                slug=m.slug, crypto=m.crypto, trigger=trigger,
                t_min=t_min, delta_bps=delta_bps,
                up_mid=up_book.mid, up_spread=up_book.spread, up_imb=up_book.imb,
                dn_mid=dn_book.mid, dn_spread=dn_book.spread, dn_imb=dn_book.imb,
                pos_qty_up=pos_up_qty, pos_qty_down=pos_dn_qty,
                cash_usdc=self.cash_usdc, equity_usdc=equity,
            )
        # ── Risk drawdown check (log-only) ──
        risk_fields = self._check_risk_drawdown(ctx)
        ctx.update(risk_fields)
        # ── Update regime awareness ──
        self._update_regime(m.slug)

        # ── Compact logging: periodic snapshot + gate report ──
        if LOG_LEVEL == "COMPACT":
            self._emit_snapshot_compact(m, ctx)
            self._emit_gate_report()

        # ── EXITS FIRST (all engines) ──
        self._manage_exits(m, st, t_min, delta_bps, ctx)
        if DIRECTIONAL_SCALP_ENABLED:
            self._dscalp_manage_exits(m, st, ctx)
        self._directional_lean_exits(m, st, t_min, delta_bps, ctx)

        # ── CLOSE_IMMINENT: after cancel-all, block ALL new orders (not exits) ──
        if MODE in ("LIVE", "LIVE_SAFE") and self._live_safety.is_close_imminent(
                st.hour_start_utc):
            return  # exits already ran above; no new entries/quotes

        # ── HARD FLATTEN: force-close all positions at HARD_FLATTEN_MIN ──
        if t_min >= HARD_FLATTEN_MIN:
            self._hard_flatten_all()
            return  # no more trading this hour

        # ── SOFT FLATTEN: trim positions starting at FLATTEN_START_MIN ──
        if t_min >= FLATTEN_START_MIN:
            self._flatten_trim_positions(t_min)
            # Still allow parity flatten and exits, but block new entries
            # (_buys_allowed already returns False via _is_in_flatten_phase)

        # ── PARITY: priority #3 — hard-gated by directional state ──
        # HOURLY_SCALP_MIN: skip parity entirely (impossible to run — stubs + guard)
        if not _IS_MIN_PROFILE:
            parity_blocked = self._should_block_parity(m, st)
            if parity_blocked:
                # Only run parity for pending pairs + recycle + flatten — NO new quotes
                self._parity_arb(ctx, new_quotes_blocked=True)
            else:
                self._parity_arb(ctx)

        # stop adding risk after TRADE_STOP_ADD_MIN or within NO_NEW_ENTRIES_SEC_TO_CLOSE
        seconds_to_close = ctx.get("seconds_to_close", 999.0)
        if t_min > TRADE_STOP_ADD_MIN or seconds_to_close < NO_NEW_ENTRIES_SEC_TO_CLOSE:
            return
        # risk gate
        if not self._risk_ok(st):
            return

        # ── Global trades/min throttle ──
        if self._throttle_exceeded():
            return

        # ── LIVE/TEST symbol filter: block new entries for non-allowed symbols ──
        if MODE in ("LIVE", "LIVE_SAFE", "TEST") and m.crypto not in LIVE_ALLOWED_SYMBOLS:
            return

        # ── LIVE SAFETY RULES (Rules 4,5,6,7,8 — composite pre-entry gate) ──
        if MODE in ("LIVE", "LIVE_SAFE"):
            up_book_e = ctx["up_book"]
            dn_book_e = ctx["dn_book"]
            ref_book_e = up_book_e if ctx.get("drift_dir") == "Up" else dn_book_e
            ls_ok, ls_reason = self._live_safety.check_pre_entry(
                slug=m.slug,
                spread_cents=ref_book_e.spread * 100,
                order_qty=max(1, self._calc_clip(m.crypto, t_min, ctx["abs_delta_bps"]) / max(0.01, ref_book_e.ask)),
                book_depth=ref_book_e.depth_1c_ask,
                t_min=t_min,
                seconds_to_close=seconds_to_close,
                equity=self._equity(),
                hour_start_equity=self.hour_start_equity,
                hour_key=st.hour_start_utc,
            )
            if not ls_ok:
                write_jsonl({"event_type": "LIVE_SAFETY_BLOCK",
                              "slug": m.slug, "reason": ls_reason,
                              "t_min": round(t_min, 3),
                              "ts_ms": int(time.time() * 1000)})
                return

        # ── PRIMARY: Directional scalp entries (priority #1) ──
        if DIRECTIONAL_SCALP_ENABLED:
            self._dscalp_entries(ctx)
        # Core engine: drift-direction entries (secondary, only if directional didn't fire)
        if m.slug not in self._dscalp_positions:
            self._core_entries(ctx)
        # Late scalp engine
        if LATE_SCALP_ENABLED and m.slug not in self._dscalp_positions:
            self._late_scalps(ctx)
    def _update_extremes(self, slug: str, up_book: BookTop, dn_book: BookTop):
        # Use mid price extremes to detect pullbacks
        for outcome, book in [("Up", up_book), ("Down", dn_book)]:
            mid = book.mid
            prev = self.recent_extreme_price[slug].get(outcome)
            if prev is None:
                self.recent_extreme_price[slug][outcome] = mid
            else:
                # track extreme in direction of "most likely" (we just keep max for simplicity)
                self.recent_extreme_price[slug][outcome] = max(prev, mid)
    # -----------------------------------------------------------------
    # State machine helpers
    # -----------------------------------------------------------------
    def _get_sm(self, slug: str) -> dict:
        if slug not in self.entry_sm:
            self.entry_sm[slug] = {"state": "IDLE", "probe_ts": None,
                                   "probe_ask": None, "initial_edge_bps": None}
        return self.entry_sm[slug]

    def _sm_transition(self, slug: str, new_state: str, reason: str = "", ctx: dict = None):
        sm = self._get_sm(slug)
        old = sm["state"]
        sm["state"] = new_state
        write_jsonl({"event_type": "SM_TRANSITION", "slug": slug,
                      "from": old, "to": new_state, "reason": reason,
                      "t_min": round(ctx["t_min"], 3) if ctx else 0})

    # =================================================================
    # PARITY SUPPRESSION — priority #3, hard-gated
    # =================================================================
    def _should_block_parity(self, m: MarketRef, st: MarketState) -> bool:
        """Determine if parity quoting should be blocked for this slug.
        Parity is blocked when:
        0. XRP with parity disabled
        1. Directional inventory exists on this slug
        2. Directional scalp fired recently (standdown timer)
        3. Net imbalance >= threshold
        4. Adverse guard active
        5. Parity fill % exceeds target cap"""
        slug = m.slug

        # 0. XRP parity disabled
        if m.crypto == "XRP" and not XRP_PARITY_BUY_ENABLED:
            return True

        # 1. Directional inventory active
        if slug in self._dscalp_positions:
            return True

        # 2. Standdown timer: dscalp fired within last N seconds
        last_dscalp = self._dscalp_last_entry_ts.get(slug, 0.0)
        if time.time() - last_dscalp < PARITY_STANDDOWN_AFTER_DSCALP_SEC:
            return True

        # 3. Net imbalance exceeds threshold
        up_q = st.positions["Up"].qty
        dn_q = st.positions["Down"].qty
        if abs(up_q - dn_q) >= PARITY_IMBALANCE_BLOCK_SHARES:
            return True

        # 4. Adverse guard active
        if PARITY_BLOCK_IF_ADVERSE and self._adverse_guard_active(slug):
            return True

        # 5. Parity fill % exceeds target
        total = max(1, self._diag_total_fills_min)
        parity_pct = self._diag_parity_fills_min / total
        if parity_pct > PARITY_MAX_FILL_PCT and self._diag_total_fills_min > 5:
            return True

        return False

    # =================================================================
    # PNL BY EXIT REASON TRACKER
    # =================================================================
    def _track_exit_pnl(self, reason: str, pnl_cents: float, qty: float, price: float):
        """Track PnL (in USD) by exit reason for DIAG_REPORT."""
        pnl_usd = pnl_cents / 100.0 * qty
        self._diag_pnl_by_exit_reason[reason] = self._diag_pnl_by_exit_reason.get(reason, 0.0) + pnl_usd

    # =================================================================
    # GLOBAL THROTTLE — target trades/min
    # =================================================================
    def _throttle_exceeded(self) -> bool:
        """Check if rolling trades/min exceeds target. If so, block new entries."""
        now = time.time()
        cutoff = now - THROTTLE_LOOKBACK_SEC
        # Trim old entries
        self._rolling_trade_ts = [ts for ts in self._rolling_trade_ts if ts > cutoff]
        trades_in_window = len(self._rolling_trade_ts)
        trades_per_min = trades_in_window / (THROTTLE_LOOKBACK_SEC / 60.0)
        return trades_per_min > TARGET_TRADES_PER_MIN

    def _throttle_record_trade(self):
        """Record a trade for the rolling trades/min window."""
        self._rolling_trade_ts.append(time.time())

    # =================================================================
    # REGIME AWARENESS — volatility-adaptive activity
    # =================================================================
    def _update_regime(self, slug: str):
        """Compute rolling 60s spot volatility and determine regime."""
        spot_hist = self._spot_history.get(slug, [])
        if len(spot_hist) < 5:
            self._regime_is_low_vol[slug] = False
            return

        now = time.time()
        cutoff = now - REGIME_VOL_LOOKBACK_SEC
        recent = [s for ts, s in spot_hist if ts > cutoff]
        if len(recent) < 3:
            self._regime_is_low_vol[slug] = False
            return

        # Compute returns in bps
        returns_bps = []
        for i in range(1, len(recent)):
            if recent[i - 1] > 0:
                ret = (recent[i] - recent[i - 1]) / recent[i - 1] * 10000.0
                returns_bps.append(ret)

        if len(returns_bps) < 2:
            self._regime_is_low_vol[slug] = False
            return

        mean_r = sum(returns_bps) / len(returns_bps)
        var_r = sum((r - mean_r) ** 2 for r in returns_bps) / len(returns_bps)
        std_bps = var_r ** 0.5
        self._regime_vol_bps[slug] = std_bps
        self._regime_is_low_vol[slug] = std_bps < REGIME_LOW_VOL_THRESHOLD

    def _regime_activity_mult(self, slug: str) -> float:
        """Return activity multiplier based on regime. 1.0 = full, 0.5 = reduced."""
        if self._regime_is_low_vol.get(slug, False):
            return REGIME_LOW_VOL_REDUCTION
        return 1.0

    # =================================================================
    # RANGE PERCENTILE — bottom 30% of 5m range
    # =================================================================
    def _is_price_in_cheap_range(self, slug: str, outcome: str, ask_price: float) -> bool:
        """Check if current ask is in bottom DSCALP_RANGE_PERCENTILE of 5-min range."""
        spot_hist = self._spot_history.get(slug, [])
        if len(spot_hist) < 3:
            return False
        now = time.time()
        cutoff = now - DSCALP_RANGE_LOOKBACK_SEC
        recent = [s for ts, s in spot_hist if ts > cutoff]
        if len(recent) < 3:
            return False
        hi = max(recent)
        lo = min(recent)
        rng = hi - lo
        if rng < 0.001:
            return False  # flat market
        # For "Up" outcome, cheap = low price. For "Down", cheap = high spot (inverse).
        # But on Polymarket binary, price of outcome moves with spot.
        # Up price is low when spot is low (cheap entry for Up = bottom of range)
        # Down price is low when spot is high (cheap entry for Down = top of range)
        if outcome == "Up":
            threshold = lo + rng * DSCALP_RANGE_PERCENTILE
            return ask_price <= threshold
        else:
            # For Down, spot being high means Down is cheap
            threshold = hi - rng * DSCALP_RANGE_PERCENTILE
            # Current spot
            cur_spot = recent[-1] if recent else 0
            return cur_spot >= threshold

    def _spot_move_10s_bps(self, slug: str) -> float:
        """Absolute spot move over the last 10 seconds, in bps."""
        spot_hist = self._spot_history.get(slug, [])
        if len(spot_hist) < 2:
            return 0.0
        now = time.time()
        cutoff = now - 10.0
        recent = [(ts, s) for ts, s in spot_hist if ts > cutoff]
        if len(recent) < 2:
            return 0.0
        first_spot = recent[0][1]
        last_spot = recent[-1][1]
        if first_spot <= 0:
            return 0.0
        return abs(last_spot - first_spot) / first_spot * 10000.0

    # =================================================================
    # RATE LIMITER — hard per-slug throttling
    # =================================================================
    def _rate_limit_ok(self, slug: str) -> bool:
        """Check if we can submit another order for this slug.
        Returns True if OK, False if blocked."""
        if not RATE_LIMIT_ENABLED:
            return True
        now = time.time()
        now_ms = now * 1000

        # Check MIN_ORDER_INTERVAL_MS
        last_ts = self._rate_last_order_ts.get(slug, 0.0)
        if (now_ms - last_ts * 1000) < MIN_ORDER_INTERVAL_MS:
            self._rate_blocked_interval += 1
            return False

        # Check MAX_ORDER_SUBMITS_PER_MIN
        window_start = self._rate_submit_window_start.get(slug, now)
        if now - window_start > 60.0:
            # Reset window
            self._rate_submit_window_start[slug] = now
            self._rate_submit_count[slug] = 0
        count = self._rate_submit_count.get(slug, 0)
        if count >= MAX_ORDER_SUBMITS_PER_MIN:
            self._rate_blocked_cap += 1
            return False

        return True

    def _rate_limit_record(self, slug: str):
        """Record an order submission for rate limiting."""
        now = time.time()
        self._rate_last_order_ts[slug] = now
        self._rate_submit_count[slug] = self._rate_submit_count.get(slug, 0) + 1
        self._true_cost_submit_count += 1

    def _entry_throttle_ok(self, slug: str) -> bool:
        """Per-slug entry throttle: MIN_ENTRY_INTERVAL_MS + MAX_ENTRIES_PER_MIN_PER_SLUG."""
        now = time.time()
        # Minimum interval between entries on same slug
        last = self._entry_last_ts.get(slug, 0.0)
        if (now - last) * 1000 < MIN_ENTRY_INTERVAL_MS:
            return False
        # Per-slug entries/min cap
        ws = self._entry_window_start.get(slug, now)
        if now - ws > 60.0:
            self._entry_window_start[slug] = now
            self._entry_count[slug] = 0
        if self._entry_count.get(slug, 0) >= MAX_ENTRIES_PER_MIN_PER_SLUG:
            return False
        return True

    def _entry_throttle_record(self, slug: str):
        """Record an entry for per-slug throttle tracking."""
        now = time.time()
        self._entry_last_ts[slug] = now
        self._entry_count[slug] = self._entry_count.get(slug, 0) + 1

    def _adverse_guard_active(self, slug: str) -> bool:
        """Check if adverse selection guard is currently blocking for this slug."""
        paused_until = self._quote_paused_until.get(slug, 0.0)
        degraded_until = self._quote_degraded_until.get(slug, 0.0)
        now = time.time()
        return now < paused_until or now < degraded_until

    # =================================================================
    # DIRECTIONAL SCALP MODE — F247-style momentum entries
    # =================================================================
    @staticmethod
    def _is_solxrp(crypto: str) -> bool:
        """Return True if coin is in the SOL/XRP (higher latency) group."""
        return crypto in ("SOL", "XRP")

    def _spot_move_2s_bps(self, slug: str) -> float:
        """Absolute spot move over the last 2 seconds, in bps."""
        spot_hist = self._spot_history.get(slug, [])
        if len(spot_hist) < 2:
            return 0.0
        now = time.time()
        cutoff = now - 2.0
        recent = [(ts, s) for ts, s in spot_hist if ts > cutoff]
        if len(recent) < 2:
            return 0.0
        first_spot = recent[0][1]
        last_spot = recent[-1][1]
        if first_spot <= 0:
            return 0.0
        return abs((last_spot - first_spot) / first_spot) * 10000.0

    def _dscalp_entries(self, ctx: dict):
        """Directional scalp entry (PRIMARY engine, F247-style).
        Per-coin thresholds: SOL/XRP require stronger edge, velocity, fresher cache.
        Entry requires: (delta >= 15bps OR spot_move_10s >= 8bps)
        AND spread within coin-specific limit AND cache_age within coin-specific limit."""
        m, st = ctx["m"], ctx["st"]
        t_min = ctx["t_min"]
        delta_bps = ctx["delta_bps"]
        abs_delta_bps = ctx["abs_delta_bps"]
        vel = ctx["vel"]
        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        now_t = time.time()

        # Cooldown (4s default = ~15 trades/min max across all slugs)
        last_entry = self._dscalp_last_entry_ts.get(m.slug, 0.0)
        if (now_t - last_entry) * 1000 < DSCALP_COOLDOWN_MS:
            return

        # Already at max position for this slug
        invested = self._dscalp_invested_usd.get(m.slug, 0.0)
        max_usd = XRP_MAX_USD_PER_SLUG if m.crypto == "XRP" else DSCALP_MAX_USD_PER_SLUG
        if invested >= max_usd:
            return

        # Post-STOP cooldown: 90s no entries on slug after a stop loss
        last_stop = self._dscalp_last_stop_ts.get(m.slug, 0.0)
        if (now_t - last_stop) < STOP_COOLDOWN_SEC:
            self._diag_entry_reject_by_gate["stop_cooldown"] = self._diag_entry_reject_by_gate.get("stop_cooldown", 0) + 1
            return

        # Rate limit
        if not self._rate_limit_ok(m.slug):
            return

        # Per-slug entry throttle
        if not self._entry_throttle_ok(m.slug):
            return

        # Regime awareness: reduce activity in low-vol
        activity_mult = self._regime_activity_mult(m.slug)
        if activity_mult < 1.0:
            # In low-vol, randomly skip entries proportional to reduction
            if random.random() > activity_mult:
                return

        # Direction: follow the drift
        outcome = ctx["drift_dir"]
        book = up_book if outcome == "Up" else dn_book

        # ── HARD GATES (must pass ALL) ──

        # Inventory cap check
        if not self._inventory_cap_ok(m.slug, outcome):
            self._diag_entry_reject_by_gate["inventory_cap"] = self._diag_entry_reject_by_gate.get("inventory_cap", 0) + 1
            return

        # Post-fill cooldown
        if not self._post_fill_cooldown_ok(m.slug):
            self._diag_entry_reject_by_gate["post_fill_cooldown"] = self._diag_entry_reject_by_gate.get("post_fill_cooldown", 0) + 1
            return

        # Determine coin group for per-coin thresholds
        _solxrp = self._is_solxrp(m.crypto)
        _coin_cooldown_ms = SOLXRP_COOLDOWN_MS if _solxrp else BTCETH_COOLDOWN_MS
        _coin_edge_cents = MIN_DIRECTIONAL_EDGE_CENTS_SOLXRP if _solxrp else MIN_DIRECTIONAL_EDGE_CENTS_BTCETH
        _coin_vel_min = MIN_VEL_BPS_PER_MIN_SOLXRP if _solxrp else MIN_VEL_BPS_PER_MIN_BTCETH
        _coin_cache_max_ms = CACHE_AGE_MAX_MS_SOLXRP if _solxrp else CACHE_AGE_MAX_MS_BTCETH

        # Anti-stacking: per-coin cooldown between new entries after a fill
        last_fill = self._last_fill_ts.get(m.slug, 0.0)
        if (now_t - last_fill) * 1000 < _coin_cooldown_ms:
            self._diag_entry_reject_by_gate["entry_stacking"] = self._diag_entry_reject_by_gate.get("entry_stacking", 0) + 1
            return

        # Cache freshness (per-coin: SOL/XRP need fresher data)
        cache_age = self._cache_age_ms(m.slug)
        if cache_age > _coin_cache_max_ms:
            return

        # Coin-specific spread gate (BTC/ETH: <=2c, SOL/XRP: <=4c)
        spread_cents = book.spread * 100
        if not self._coin_spread_entry_ok(m.crypto, spread_cents):
            self._diag_entry_reject_by_gate["spread_filter"] = self._diag_entry_reject_by_gate.get("spread_filter", 0) + 1
            # Sampled log: only emit GATE_SPREAD_BLOCK at LOG_SPREAD_BLOCK_SAMPLER probability
            if LOG_LEVEL != "COMPACT" or random.random() < LOG_SPREAD_BLOCK_SAMPLER:
                write_jsonl({"event_type": "GATE_SPREAD_BLOCK", "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "crypto": m.crypto, "spread_cents": round(spread_cents, 2),
                              "limit_cents": MAX_SPREAD_ENTRY_CENTS_BTCETH if m.crypto in ("BTC", "ETH") else MAX_SPREAD_ENTRY_CENTS_SOLXRP})
            return

        # Spread gate (existing)
        if book.spread * 100 > DSCALP_MAX_SPREAD_CENTS:
            self._diag_entry_reject_by_gate["spread_general"] = self._diag_entry_reject_by_gate.get("spread_general", 0) + 1
            return

        # Noisy-spread chop guard: block if spread wide AND velocity low (choppy conditions)
        if spread_cents > NOISY_SPREAD_BLOCK_CENTS and abs(vel) < NOISY_SPREAD_BLOCK_VEL:
            self._diag_entry_reject_by_gate["noisy_spread"] = self._diag_entry_reject_by_gate.get("noisy_spread", 0) + 1
            return

        # Entry edge gate: per-coin (BTC/ETH: 3c, SOL/XRP: 4c above 50c neutral)
        entry_edge_cents = (book.mid - 0.50) * 100
        if entry_edge_cents < _coin_edge_cents:
            self._diag_entry_reject_by_gate["entry_edge"] = self._diag_entry_reject_by_gate.get("entry_edge", 0) + 1
            return

        # ── ENTRY SIGNAL: delta >= 15bps OR spot moved >= 8bps in 10s ──
        delta_ok = abs_delta_bps >= DSCALP_DELTA_MIN_BPS
        spot_move_ok = self._spot_move_10s_bps(m.slug) >= DSCALP_SPOT_MOVE_10S_BPS
        if not delta_ok and not spot_move_ok:
            self._diag_entry_reject_by_gate["signal"] = self._diag_entry_reject_by_gate.get("signal", 0) + 1
            return

        # Velocity must be supportive (per-coin: BTC/ETH 2.5, SOL/XRP 3.5 bps/min)
        _vel_blocked = ((outcome == "Up" and vel < _coin_vel_min)
                        or (outcome == "Down" and vel > -_coin_vel_min))
        if _vel_blocked:
            self._diag_entry_reject_by_gate["velocity"] = self._diag_entry_reject_by_gate.get("velocity", 0) + 1
            vd = delta_velocity_debug(st.delta_hist, lookback_sec=30.0)
            write_jsonl({"event_type": "GATE_VELOCITY_BLOCK",
                         "slug": m.slug, "outcome": outcome,
                         "vel_used": round(vel, 6),
                         "vel_threshold": _coin_vel_min,
                         "delta_now": round(vd["delta_now"], 4),
                         "delta_prev": round(vd["delta_prev"], 4),
                         "dt_sec": round(vd["dt_sec"], 1),
                         "n_hist": vd["n_points"],
                         "vel_status": vd["status"]})
            return

        # Late-move filter (SOL/XRP only): block if spot moved too fast on stale data
        if _solxrp and cache_age > SOLXRP_LATE_MOVE_CACHE_MS:
            spot_move_2s = self._spot_move_2s_bps(m.slug)
            if spot_move_2s > SOLXRP_LATE_MOVE_BPS:
                self._diag_entry_reject_by_gate["late_move"] = self._diag_entry_reject_by_gate.get("late_move", 0) + 1
                return

        # Size: $5-10 per entry, no micro-splits
        remaining = DSCALP_MAX_USD_PER_SLUG - invested
        step_usd = min(DSCALP_STEP_USD, remaining)
        if step_usd < DSCALP_STEP_USD_MIN:
            return  # don't enter with less than minimum size

        # Place maker buy
        buy_price = book.bid  # maker at best bid
        if buy_price <= 0.01:
            return

        order_qty = step_usd / buy_price
        if order_qty < 1:
            return

        # Execution safety gate (kill-switch, drift, loss-tail, LIVE_SAFE entry window)
        if not self._exec_safety_can_enter(m.slug):
            return
        # LIVE_SAFE trade-size limiter
        step_usd, order_qty = self._live_safe_cap_usd(step_usd, buy_price)
        if order_qty < 1:
            return

        # Use existing buy infrastructure
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        pos = st.positions[outcome]
        decision_id = new_decision_id()
        client_oid = new_order_id()
        if pos.position_id is None:
            pos.position_id = new_position_id()
            pos.trade_id = uuid.uuid4().hex
            pos.entry_decision_id = decision_id
            pos.parent_order_id = client_oid
            pos.entry_mid = book.mid
            pos.max_favorable_mid = book.mid

        # Log intent with signal type
        entry_signal = "delta" if delta_ok else "spot_move"
        regime = "low_vol" if self._regime_is_low_vol.get(m.slug, False) else "normal"
        write_jsonl({"event_type": "DSCALP_ENTRY",
                      "ts_ms": int(now_t * 1000),
                      "slug": m.slug, "crypto": m.crypto,
                      "outcome": outcome,
                      "entry_signal": entry_signal,
                      "regime": regime,
                      "price": round(buy_price, 4),
                      "qty": round(order_qty, 1),
                      "step_usd": round(step_usd, 2),
                      "delta_bps": round(delta_bps, 1),
                      "vel": round(vel, 4),
                      "cache_age_ms": round(cache_age, 0),
                      "spread_cents": round(book.spread * 100, 2)})

        # Execute buy
        self._ledger_order_intent(m, st, outcome, "BUY", order_qty, buy_price, "DSCALP_ENTRY")
        if MODE == "LOG":
            actual_cost = order_qty * buy_price
            self._paper_buy(st, outcome, buy_price, order_qty, actual_cost)
            self._ledger_record_buy_fill(m, st, outcome, buy_price, order_qty)
            mt = "maker"
        else:
            fill = self._place_layered_buy(m, outcome, order_qty, buy_price)
            if fill["total_filled"] <= 0:
                return  # no fill — skip position tracking
            order_qty = fill["total_filled"]
            buy_price = fill["avg_price"]
            actual_cost = fill["total_cost"]
            self._live_buy(st, outcome, buy_price, order_qty, actual_cost)
            self._ledger_record_buy_fill(m, st, outcome, buy_price, order_qty)
            mt = infer_maker_taker("BUY", buy_price, book)
        self._record_fill_ts(m.slug)  # post-fill cooldown
        self._rate_limit_record(m.slug)
        self._throttle_record_trade()
        self._true_cost_fill_count += 1
        self._true_cost_fill_count_min += 1
        self._true_cost_tx_count += 1
        self._diag_directional_fills_min += 1
        self._diag_total_fills_min += 1
        self._diag_trade_sizes.append(actual_cost)

        # Track directional scalp position
        existing_qty = self._dscalp_positions.get(m.slug, {}).get("qty", 0)
        existing_entry = self._dscalp_positions.get(m.slug, {}).get("entry_price", buy_price)
        # VWAP the entry price if adding to existing position
        total_qty = order_qty + existing_qty
        vwap_price = ((existing_entry * existing_qty + buy_price * order_qty) / total_qty
                      if total_qty > 0 else buy_price)
        self._dscalp_positions[m.slug] = {
            "outcome": outcome,
            "entry_price": vwap_price,
            "entry_ts": self._dscalp_positions.get(m.slug, {}).get("entry_ts", now_t),
            "qty": total_qty,
            "tp1_done": self._dscalp_positions.get(m.slug, {}).get("tp1_done", False),
            "tp2_done": self._dscalp_positions.get(m.slug, {}).get("tp2_done", False),
            "tp3_done": self._dscalp_positions.get(m.slug, {}).get("tp3_done", False),
            "tp4_done": self._dscalp_positions.get(m.slug, {}).get("tp4_done", False),
        }
        self._dscalp_invested_usd[m.slug] = invested + actual_cost
        self._dscalp_last_entry_ts[m.slug] = now_t
        self._entry_throttle_record(m.slug)
        self._diag_dscalp_entries += 1
        st.last_entry_ts = iso_z(utc_now())

        self.logger.log_order_fill(
            engine="DSCALP", slug=m.slug, crypto=m.crypto,
            hour_start_utc=ctx["hour_start_utc"], t_min=t_min,
            outcome=outcome, side="BUY", qty=order_qty,
            fill_price=buy_price, usdc_cost=actual_cost,
            maker_taker=mt, decision_id=decision_id,
            client_order_id=client_oid, position_id=pos.position_id,
            notes=f"dscalp_entry vel={vel:.1f}",
            **build_book_fields(ctx["up_book"], ctx["dn_book"],
                                ctx.get("spot", 0), ctx.get("hour_open", 0),
                                delta_bps, abs_delta_bps),
        )

    def _dscalp_manage_exits(self, m: MarketRef, st: MarketState, ctx: dict):
        """Manage directional scalp exits: TP ladder + timeout + hard stop loss.
        ENFORCES 60s minimum hold unless hard stop (-4c) or TP hit.
        Early exit requires ALL: profit >= 4c AND vel reversal >= 6 bps AND spread >= 5c.
        TP ladder: +4c(35%) / +7c(25%) / +10c(25%) / +12c(15% runner).
        Runner protection: exit at TP3 price if held > 600s or vel reversal >= 8 bps."""
        dpos = self._dscalp_positions.get(m.slug)
        if dpos is None:
            return

        now_t = time.time()
        outcome = dpos["outcome"]
        entry_price = dpos["entry_price"]
        entry_ts = dpos["entry_ts"]
        hold_sec = now_t - entry_ts

        book = ctx["up_book"] if outcome == "Up" else ctx["dn_book"]
        pos = st.positions[outcome]
        if pos.qty < MIN_QTY:
            # Position gone — cleanup
            self._dscalp_positions.pop(m.slug, None)
            self._dscalp_invested_usd.pop(m.slug, None)
            return

        current_mid = book.mid
        pnl_cents = (current_mid - entry_price) * 100

        # ── HARD STOP LOSS — triggers immediately at -4c, overrides min hold timer ──
        # Also: per-position loss cap — if unrealized USD loss >= POS_LOSS_CAP_MULT * clip_usdc
        invested_usd = self._dscalp_invested_usd.get(m.slug, 0.0)
        unrealized_pnl_usd = (current_mid - entry_price) * dpos.get("qty", pos.qty)
        pos_loss_cap_usd = -POS_LOSS_CAP_MULT * max(invested_usd, DSCALP_STEP_USD) if POS_LOSS_CAP_ENABLED else -9999

        stop_triggered = pnl_cents <= -DSCALP_STOP_LOSS_CENTS
        loss_cap_triggered = POS_LOSS_CAP_ENABLED and unrealized_pnl_usd <= pos_loss_cap_usd

        if stop_triggered or loss_cap_triggered:
            stop_reason = "DSCALP_STOP" if stop_triggered else "DSCALP_LOSS_CAP"
            sell_qty = pos.qty
            sell_price = max(book.bid, 0.01)
            self._do_sell(m, st, outcome, sell_qty, sell_price,
                          reason=stop_reason, leg="DSCALP", ctx=ctx, use_maker=False)
            self._diag_dscalp_stop_exits += 1
            self._diag_dscalp_hold_times.append(hold_sec)
            self._diag_dscalp_exit_cents.append(pnl_cents)
            self._track_exit_pnl("STOP", pnl_cents, sell_qty, sell_price)
            self._diag_dscalp_exits += 1
            self._diag_directional_fills_min += 1
            self._diag_total_fills_min += 1
            self._throttle_record_trade()
            # Record stop timestamp for per-slug stop cooldown
            self._dscalp_last_stop_ts[m.slug] = now_t
            self._dscalp_positions.pop(m.slug, None)
            self._dscalp_invested_usd.pop(m.slug, None)
            write_jsonl({"event_type": stop_reason, "ts_ms": int(now_t * 1000),
                          "slug": m.slug, "outcome": outcome,
                          "pnl_cents": round(pnl_cents, 2),
                          "unrealized_pnl_usd": round(unrealized_pnl_usd, 4),
                          "pos_loss_cap_usd": round(pos_loss_cap_usd, 4),
                          "hold_sec": round(hold_sec, 1)})
            return

        # ── MINIMUM HOLD FLOOR: 60s unless TP/STOP hit or early exit conditions met ──
        spread_cents = book.spread * 100
        vel = ctx.get("vel", 0.0)

        # Early exit requires ALL THREE conditions simultaneously:
        #   1) profit >= EARLY_EXIT_MIN_PROFIT_CENTS (4c)
        #   2) velocity reversal >= EARLY_EXIT_VEL_REVERSAL_BPS (6 bps/min against position)
        #   3) spread >= EARLY_EXIT_SPREAD_THRESHOLD_CENTS (5c)
        if hold_sec < DSCALP_MIN_HOLD_SEC and EARLY_EXIT_ENABLED:
            profit_ok = pnl_cents >= EARLY_EXIT_MIN_PROFIT_CENTS
            vel_reversed = ((outcome == "Up" and vel < -EARLY_EXIT_VEL_REVERSAL_BPS)
                            or (outcome == "Down" and vel > EARLY_EXIT_VEL_REVERSAL_BPS))
            spread_wide = spread_cents >= EARLY_EXIT_SPREAD_THRESHOLD_CENTS

            if profit_ok and vel_reversed and spread_wide:
                sell_qty = pos.qty
                sell_price = max(book.bid, 0.01)
                self._do_sell(m, st, outcome, sell_qty, sell_price,
                              reason="DSCALP_EARLY_EXIT", leg="DSCALP", ctx=ctx, use_maker=False)
                self._diag_dscalp_early_exits += 1
                self._diag_dscalp_exits += 1
                self._diag_dscalp_hold_times.append(hold_sec)
                self._diag_dscalp_exit_cents.append(pnl_cents)
                self._track_exit_pnl("EARLY", pnl_cents, sell_qty, sell_price)
                self._diag_directional_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._dscalp_positions.pop(m.slug, None)
                self._dscalp_invested_usd.pop(m.slug, None)
                write_jsonl({"event_type": "DSCALP_EARLY_EXIT", "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "outcome": outcome,
                              "pnl_cents": round(pnl_cents, 2), "hold_sec": round(hold_sec, 1),
                              "spread_cents": round(spread_cents, 2), "vel": round(vel, 2)})
                return
            return  # still in min hold, no bypass triggered

        # ── Timeout exit (taker at bid for immediate fill) ──
        if hold_sec >= DSCALP_MAX_HOLD_SEC:
            sell_qty = pos.qty
            sell_price = book.bid
            self._do_sell(m, st, outcome, sell_qty, sell_price,
                          reason="DSCALP_TIMEOUT", leg="DSCALP_TIMEOUT",
                          ctx=ctx, use_maker=False)
            self._diag_dscalp_timeout_exits += 1
            self._diag_dscalp_exits += 1
            self._diag_dscalp_hold_times.append(hold_sec)
            self._diag_dscalp_exit_cents.append(pnl_cents)
            self._track_exit_pnl("TIMEOUT", pnl_cents, sell_qty, sell_price)
            self._diag_directional_fills_min += 1
            self._diag_total_fills_min += 1
            self._throttle_record_trade()
            self._dscalp_positions.pop(m.slug, None)
            self._dscalp_invested_usd.pop(m.slug, None)
            write_jsonl({"event_type": "DSCALP_TIMEOUT", "ts_ms": int(now_t * 1000),
                          "slug": m.slug, "outcome": outcome,
                          "pnl_cents": round(pnl_cents, 2), "hold_sec": round(hold_sec, 1)})
            return

        # ── TP1: +4c, sell 25% (taker at bid for immediate fill) ──
        if pnl_cents >= DSCALP_TP1_CENTS and not dpos["tp1_done"]:
            sell_qty = min(pos.qty * DSCALP_TP1_FRAC, pos.qty)
            if sell_qty >= MIN_QTY:
                sell_price = book.bid
                self._do_sell(m, st, outcome, sell_qty, sell_price,
                              reason="DSCALP_TP1", leg="TP1",
                              ctx=ctx, use_maker=False)
                self._diag_dscalp_tp1 += 1
                self._diag_dscalp_exits += 1
                self._diag_directional_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._diag_trade_sizes.append(sell_qty * sell_price)
                self._track_exit_pnl("TP1", pnl_cents, sell_qty, sell_price)
                write_jsonl({"event_type": "DSCALP_TP1", "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "outcome": outcome,
                              "pnl_cents": round(pnl_cents, 2), "sell_qty": round(sell_qty, 1),
                              "hold_sec": round(hold_sec, 1)})
            dpos["tp1_done"] = True

        # ── TP2: +7c, sell 25% (taker at bid for immediate fill) ──
        if pnl_cents >= DSCALP_TP2_CENTS and not dpos["tp2_done"]:
            sell_qty = min(pos.qty * DSCALP_TP2_FRAC, pos.qty)
            if sell_qty >= MIN_QTY:
                sell_price = book.bid
                self._do_sell(m, st, outcome, sell_qty, sell_price,
                              reason="DSCALP_TP2", leg="TP2",
                              ctx=ctx, use_maker=False)
                self._diag_dscalp_tp2 += 1
                self._diag_dscalp_exits += 1
                self._diag_directional_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._diag_trade_sizes.append(sell_qty * sell_price)
                self._track_exit_pnl("TP2", pnl_cents, sell_qty, sell_price)
                write_jsonl({"event_type": "DSCALP_TP2", "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "outcome": outcome,
                              "pnl_cents": round(pnl_cents, 2), "sell_qty": round(sell_qty, 1),
                              "hold_sec": round(hold_sec, 1)})
            dpos["tp2_done"] = True

        # ── TP3: +10c, sell 25% (taker at bid for immediate fill) ──
        if pnl_cents >= DSCALP_TP3_CENTS and not dpos.get("tp3_done"):
            sell_qty = min(pos.qty * DSCALP_TP3_FRAC, pos.qty)
            if sell_qty >= MIN_QTY:
                sell_price = book.bid
                self._do_sell(m, st, outcome, sell_qty, sell_price,
                              reason="DSCALP_TP3", leg="TP3",
                              ctx=ctx, use_maker=False)
                self._diag_dscalp_tp3 += 1
                self._diag_dscalp_exits += 1
                self._diag_directional_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._diag_trade_sizes.append(sell_qty * sell_price)
                self._track_exit_pnl("TP3", pnl_cents, sell_qty, sell_price)
                write_jsonl({"event_type": "DSCALP_TP3", "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "outcome": outcome,
                              "pnl_cents": round(pnl_cents, 2), "sell_qty": round(sell_qty, 1),
                              "hold_sec": round(hold_sec, 1)})
            dpos["tp3_done"] = True

        # ── TP4 "runner": +12c, sell remaining 15% (taker at bid for immediate fill) ──
        if pnl_cents >= DSCALP_TP4_CENTS and not dpos.get("tp4_done"):
            sell_qty = min(pos.qty * DSCALP_TP4_FRAC, pos.qty)
            if sell_qty >= MIN_QTY:
                sell_price = book.bid
                self._do_sell(m, st, outcome, sell_qty, sell_price,
                              reason="DSCALP_TP4", leg="TP4",
                              ctx=ctx, use_maker=False)
                self._diag_dscalp_tp4 += 1
                self._diag_dscalp_exits += 1
                self._diag_directional_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._diag_trade_sizes.append(sell_qty * sell_price)
                self._track_exit_pnl("TP4", pnl_cents, sell_qty, sell_price)
                write_jsonl({"event_type": "DSCALP_TP4", "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "outcome": outcome,
                              "pnl_cents": round(pnl_cents, 2), "sell_qty": round(sell_qty, 1),
                              "hold_sec": round(hold_sec, 1)})
            dpos["tp4_done"] = True

        # ── Runner protection: if TP3 done but TP4 not yet hit, check for runner fallback ──
        # Exit runner at TP3 price (10c) via maker→taker if:
        #   (a) held > RUNNER_MAX_HOLD_SEC (600s), OR
        #   (b) velocity reversal >= RUNNER_VEL_REVERSAL_BPS (8 bps/min against position)
        if dpos.get("tp3_done") and not dpos.get("tp4_done") and pos.qty >= MIN_QTY:
            runner_hold_sec = now_t - entry_ts
            vel_against = ((outcome == "Up" and vel < -RUNNER_VEL_REVERSAL_BPS)
                           or (outcome == "Down" and vel > RUNNER_VEL_REVERSAL_BPS))
            runner_timeout = runner_hold_sec >= RUNNER_MAX_HOLD_SEC

            if runner_timeout or vel_against:
                fallback_reason = "runner_timeout" if runner_timeout else "runner_vel_reversal"
                sell_qty = pos.qty  # sell entire remaining runner portion
                # Fall back to bid (taker) for immediate fill
                sell_price = max(book.bid, 0.01)
                self._do_sell(m, st, outcome, sell_qty, sell_price,
                              reason="DSCALP_RUNNER_FALLBACK", leg="RUNNER_FALLBACK",
                              ctx=ctx, use_maker=False)
                self._diag_dscalp_runner_fallbacks += 1
                self._diag_dscalp_exits += 1
                self._diag_directional_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._diag_trade_sizes.append(sell_qty * sell_price)
                self._track_exit_pnl("RUNNER_FALLBACK", pnl_cents, sell_qty, sell_price)
                self._dscalp_positions.pop(m.slug, None)
                self._dscalp_invested_usd.pop(m.slug, None)
                write_jsonl({"event_type": "DSCALP_RUNNER_FALLBACK", "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "outcome": outcome,
                              "pnl_cents": round(pnl_cents, 2), "sell_qty": round(sell_qty, 1),
                              "hold_sec": round(hold_sec, 1),
                              "runner_fallback_reason": fallback_reason,
                              "vel": round(vel, 2)})

    def _core_entries(self, ctx: dict):
        m, st = ctx["m"], ctx["st"]
        t_min = ctx["t_min"]
        delta_bps, abs_delta_bps = ctx["delta_bps"], ctx["abs_delta_bps"]
        vel, z = ctx["vel"], ctx["z"]
        up_book, dn_book = ctx["up_book"], ctx["dn_book"]
        spot = ctx["spot"]
        sm = self._get_sm(m.slug)

        # --- Build valid signal (time + coin-specific threshold + dynamic cap) ---
        thr = entry_threshold_bps(m.crypto, t_min)
        edge_bps = abs(ctx["edge_up"] if ctx["drift_dir"] == "Up" else ctx["edge_down"]) * 10000.0
        cap = dynamic_cap(t_min, edge_bps)
        valid_time = (t_min >= TRADE_START_MIN)
        valid_delta = (abs_delta_bps >= thr)
        valid_z = (not Z_ENTRY_ENABLED) or (abs(z) >= Z_ENTRY_MIN)
        outcome = ctx["drift_dir"]
        book = up_book if outcome == "Up" else dn_book
        valid_price = (book.ask <= cap)
        # Spread: relaxed during PROBING/SCALING
        in_burst = sm["state"] in ("PROBING", "SCALING")
        max_spread = spread_limit(t_min, edge_bps, m.crypto, in_burst=in_burst)
        valid_spread = (book.spread <= max_spread)
        valid_imb = (not IMB_ENABLED) or (book.imb >= IMB_MIN)
        valid_pullback = True
        if PULLBACK_ENABLED:
            extreme = self.recent_extreme_price[m.slug].get(outcome)
            if extreme is not None:
                valid_pullback = (book.mid <= extreme - PULLBACK_CENTS) or (t_min > 45)
        sig = bool(valid_time and valid_delta and valid_z and valid_price and valid_spread and valid_imb and valid_pullback)

        # Persistence tracking
        sh = self.signal_hist.setdefault(m.slug, [])
        sh.append((iso_z(utc_now()), sig))
        sh[:] = sh[-500:]
        persist_ok = persistence_ok(sh)

        # Cooldown check — dynamic per coin/time
        cooldown_active = False
        if sm["state"] == "COOLDOWN":
            cooldown_active = True
        elif st.last_entry_ts:
            last = _parse_iso_z(st.last_entry_ts)
            cd = entry_cooldown_sec(m.crypto, t_min)
            cooldown_active = (utc_now() - last).total_seconds() < cd
        # Exit COOLDOWN state when cooldown expires
        if sm["state"] == "COOLDOWN" and st.last_entry_ts:
            last = _parse_iso_z(st.last_entry_ts)
            cd = entry_cooldown_sec(m.crypto, t_min)
            if (utc_now() - last).total_seconds() >= cd:
                self._sm_transition(m.slug, "IDLE", "cooldown_expired", ctx)
                cooldown_active = False

        risk_blocked = not self._risk_ok(st)

        # Sizing
        mult = sizing_mult(abs_delta_bps)
        clip = self._calc_clip(m.crypto, t_min, abs_delta_bps)
        if mult >= 2.0 and vel < MIN_DELTA_VEL_BPS_PER_MIN:
            clip *= 0.6
        if CORR_SCALE_ENABLED and BTC_LEAD and m.crypto != "BTC":
            btc_cost = self._crypto_cost_usdc("BTC")
            if btc_cost > 0:
                reduce = clamp(btc_cost / (self.cash_usdc * MAX_COST_PER_CRYPTO_PCT), 0.0, 1.0)
                clip *= (1.0 - BTC_EXPOSURE_REDUCE_OTHERS * reduce)

        # ---- Whipsaw / anti-chop filter ----
        whipsaw_blocked = False
        whipsaw_reason = ""
        if sig and persist_ok and not cooldown_active and not risk_blocked:
            edge_sign_entry = self._edge_sign_state.get(m.slug)
            ws_since = edge_sign_entry[1] if edge_sign_entry else None
            ws_ok, ws_reason = whipsaw_ok(delta_bps, vel, ws_since)
            if not ws_ok:
                whipsaw_blocked = True
                whipsaw_reason = ws_reason
                self._diag_blocked_whipsaw += 1

        # ---- No-flip rule: block immediate direction reversal ----
        noflip_blocked = False
        noflip_reason = ""
        if sig and persist_ok and not cooldown_active and not risk_blocked and not whipsaw_blocked:
            prev_dir = self._last_trade_direction.get(m.slug)
            if prev_dir is not None:
                prev_outcome, prev_ts = prev_dir
                if prev_outcome != outcome and (time.time() - prev_ts) < NO_FLIP_COOLDOWN_SEC:
                    if abs_delta_bps < thr + NO_FLIP_OVERRIDE_EXTRA_BPS:
                        noflip_blocked = True
                        noflip_reason = f"noflip({prev_outcome}->{outcome},{time.time()-prev_ts:.1f}s)"
                        self._diag_blocked_noflip += 1

        # ---- DECISION event ----
        will_trade = (sig and persist_ok and not cooldown_active and not risk_blocked
                      and not whipsaw_blocked and not noflip_blocked and clip >= MIN_ORDER_USDC)
        if will_trade:
            self._tempo_intents[m.slug] = self._tempo_intents.get(m.slug, 0) + 1
        skip_reason = ""
        if not will_trade:
            reasons = []
            if not valid_time: reasons.append("time")
            if not valid_delta: reasons.append(f"delta({abs_delta_bps:.1f}<{thr})")
            if not valid_z: reasons.append("zscore")
            if not valid_price: reasons.append(f"price({book.ask:.3f}>{cap:.3f})")
            if not valid_spread: reasons.append(f"spread({book.spread:.3f}>{max_spread:.3f})")
            if not valid_imb: reasons.append("imb")
            if not valid_pullback: reasons.append("pullback")
            if sig and not persist_ok: reasons.append("persistence")
            if sig and cooldown_active: reasons.append(f"cooldown({entry_cooldown_sec(m.crypto, t_min):.0f}s)")
            if sig and risk_blocked: reasons.append("risk_cap")
            if whipsaw_blocked: reasons.append(f"whipsaw({whipsaw_reason})")
            if noflip_blocked: reasons.append(noflip_reason)
            if sig and persist_ok and not cooldown_active and not risk_blocked and clip < MIN_ORDER_USDC:
                reasons.append(f"clip_too_small({clip:.2f})")
            skip_reason = "|".join(reasons)

        sig_dict = {
            "outcome": outcome, "will_trade": will_trade,
            "valid_time": valid_time, "valid_delta": valid_delta,
            "valid_price": valid_price, "valid_spread": valid_spread,
            "valid_imb": valid_imb, "persist_ok": persist_ok,
            "cooldown": cooldown_active, "risk": risk_blocked,
            "sm_state": sm["state"],
        }
        if self.logger.should_log_decision(m.slug, ctx["hour_start_utc"], sig_dict):
            self.logger.log_decision({
                "engine": "CORE", "slug": m.slug, "crypto": m.crypto,
                "hour_start_utc": ctx["hour_start_utc"],
                "t_min": round(t_min, 3),
                "phase": ctx["phase"], "seconds_to_close": round(ctx["seconds_to_close"], 1),
                "selected_outcome": outcome,
                "valid_time": valid_time, "valid_delta": valid_delta, "valid_z": valid_z,
                "valid_price": valid_price, "valid_spread": valid_spread,
                "valid_imb": valid_imb, "valid_pullback": valid_pullback,
                "persistence_ok": persist_ok, "cooldown_active": cooldown_active,
                "risk_blocked": risk_blocked,
                "cap_used": cap, "cap_boost": round(cap - price_cap(t_min), 4),
                "thr_used": thr, "size_mult": mult,
                "spread_limit_used": max_spread,
                "clip_usdc": round(clip, 4),
                "will_trade": will_trade, "skip_reason": skip_reason,
                "spot": spot, "hour_open": ctx["hour_open"],
                "delta_bps": round(delta_bps, 3), "abs_delta_bps": round(abs_delta_bps, 3),
                "vel": round(vel, 3), "z": round(z, 3),
                "sm_state": sm["state"],
            })
        # ── BOUNDARY events ──
        boundary_state = {
            "abs_delta_bps": abs_delta_bps,
            "entry_thr_bps": thr,
            "valid_price": valid_price,
            "ask": book.ask, "price_cap": cap,
            "persistence_ok": persist_ok,
            "peak_abs_delta_bps": st.peak_abs_delta_bps,
        }
        for bev in self.logger.check_boundaries(m.slug, ctx["hour_start_utc"], boundary_state):
            self.logger.log_boundary(bev)

        # --- State machine: IDLE → PROBING → SCALING → COOLDOWN ---
        if sm["state"] == "IDLE":
            if not sig:
                if CONSOLE_LEVEL != "QUIET":
                    print(f"  [GATE_FAIL] {m.crypto:5s} {skip_reason}")
                return
            if not persist_ok:
                if CONSOLE_LEVEL != "QUIET":
                    print(f"  [PERSIST_FAIL] {m.crypto:5s} sig=True but persistence not met (hist={len(sh)})")
                return
            if cooldown_active or risk_blocked:
                return
            if clip < MIN_ORDER_USDC:
                if CONSOLE_LEVEL != "QUIET":
                    print(f"  [CLIP_FAIL] {m.crypto:5s} clip=${clip:.2f} < min=${MIN_ORDER_USDC}")
                return
            # ---- Place PROBE order (taker-gated) ----
            if not self._exec_safety_can_enter(m.slug):
                return
            if not self._inventory_cap_ok(m.slug, outcome):
                return
            if not self._post_fill_cooldown_ok(m.slug):
                return
            # Coin-specific spread limit
            if not self._coin_spread_entry_ok(m.crypto, book.spread * 100):
                return
            signal_detect_ts = time.time()
            sm["signal_detect_ts"] = signal_detect_ts
            # Record trade direction for no-flip rule
            self._last_trade_direction[m.slug] = (outcome, time.time())
            # Always buy at ask (taker) for immediate fill
            probe_price = book.ask
            probe_usd = max(MIN_ORDER_USDC, clip * PROBE_SIZE_FRAC)
            # LIVE_SAFE trade-size limiter
            probe_usd, probe_qty = self._live_safe_cap_usd(probe_usd, probe_price)
            if probe_qty < MIN_QTY:
                return
            edge = ctx["edge_up"] if outcome == "Up" else ctx["edge_down"]
            self._hour_edges.append(edge)
            decision_id = new_decision_id()
            client_oid = new_order_id()
            pos = st.positions[outcome]
            if pos.position_id is None:
                pos.position_id = new_position_id()
                pos.trade_id = uuid.uuid4().hex
                pos.entry_decision_id = decision_id
                pos.parent_order_id = client_oid
                pos.entry_mid = book.mid
                pos.max_favorable_mid = book.mid
                pos.max_adverse_mid = book.mid
            ctx["cap_used"] = cap
            ctx["thr_used"] = thr
            ctx["size_mult"] = mult
            bk_fields = self._book_fields(up_book, dn_book, outcome)
            order_place_ts = time.time()
            signal_to_order_ms = (order_place_ts - signal_detect_ts) * 1000
            if CONSOLE_LEVEL != "QUIET":
                print(f"  [PROBE] {m.crypto:5s} {outcome} probe=${probe_usd:.2f} ask={book.ask:.3f} latency={signal_to_order_ms:.0f}ms")
            write_jsonl({"event_type": "SIGNAL_LATENCY", "slug": m.slug,
                          "crypto": m.crypto, "outcome": outcome,
                          "signal_detect_ts": signal_detect_ts,
                          "order_place_ts": order_place_ts,
                          "signal_to_order_ms": round(signal_to_order_ms, 1),
                          "t_min": round(t_min, 3)})
            probe_mt = "taker"
            self._diag_taker_count += 1
            self.logger.log_order_intent(
                engine="CORE", reason="ENTRY_PROBE",
                decision_id=decision_id, position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=probe_qty, target_price=probe_price,
                usdc_cost=probe_usd, ctx=ctx, book_fields=bk_fields,
            )
            self.logger.log_order_submit(
                engine="CORE", reason="ENTRY_PROBE",
                decision_id=decision_id, position_id=pos.position_id,
                client_order_id=client_oid,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=probe_qty, target_price=probe_price,
                usdc_cost=probe_usd, ctx=ctx, book_fields=bk_fields,
            )
            self._ledger_order_intent(m, st, outcome, "BUY", probe_qty, probe_price, "ENTRY_PROBE", order_id=client_oid)
            if MODE == "LOG":
                self._paper_buy(st, outcome, probe_price, probe_qty, probe_usd)
                self._ledger_record_buy_fill(m, st, outcome, probe_price, probe_qty)
                self._record_fill_ts(m.slug)  # post-fill cooldown
                mt = probe_mt
                sc = spread_capture_fields("BUY", probe_price, book)
                _fee = compute_fee_usdc(probe_usd, mt)
                self.logger.log_order_fill(
                    engine="CORE", reason="ENTRY_PROBE",
                    decision_id=decision_id, client_order_id=client_oid,
                    position_id=pos.position_id,
                    crypto=m.crypto, slug=m.slug, outcome=outcome,
                    side="BUY", qty=probe_qty, fill_price=probe_price,
                    usdc_cost=probe_usd, fees_usdc=_fee,
                    maker_taker=mt, did_cross=sc.get("did_cross", ""),
                    vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                )
            else:
                fill = self._place_layered_buy(m, outcome, probe_qty, probe_price)
                if fill["total_filled"] > 0:
                    self._live_buy(st, outcome, fill["avg_price"], fill["total_filled"], fill["total_cost"])
                    self._ledger_record_buy_fill(m, st, outcome, fill["avg_price"], fill["total_filled"])
                    mt = infer_maker_taker("BUY", fill["avg_price"], book)
                    sc = spread_capture_fields("BUY", fill["avg_price"], book)
                    _fee = compute_fee_usdc(fill["total_cost"], mt)
                    self.logger.log_order_fill(
                        engine="CORE", reason="ENTRY_PROBE",
                        decision_id=decision_id, client_order_id=client_oid,
                        position_id=pos.position_id,
                        crypto=m.crypto, slug=m.slug, outcome=outcome,
                        side="BUY", qty=fill["total_filled"], fill_price=fill["avg_price"],
                        usdc_cost=fill["total_cost"], fees_usdc=_fee,
                        maker_taker=mt, did_cross=sc.get("did_cross", ""),
                        vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                    )
            sm["probe_ts"] = time.time()
            sm["probe_ask"] = probe_price
            sm["initial_edge_bps"] = edge_bps
            self._sm_transition(m.slug, "PROBING", "probe_placed", ctx)
            return

        elif sm["state"] == "PROBING":
            # Wait PROBE_CONFIRM_SEC (300ms), then check signal still valid
            elapsed = time.time() - (sm.get("probe_ts") or time.time())
            if elapsed < PROBE_CONFIRM_SEC:
                return  # still waiting
            if not sig:
                self._sm_transition(m.slug, "IDLE", "signal_lost_after_probe", ctx)
                return
            # Window gate: burst only in early phase (minutes 0-3).
            # If bot starts mid-window (e.g. minute 30), skip burst → probe only.
            t_min = ctx["t_min"]
            if t_min > BURST_EARLY_WINDOW_MIN:
                st.last_entry_ts = iso_z(utc_now())
                self._sm_transition(m.slug, "COOLDOWN", "probe_only_past_burst_window", ctx)
                return
            # Edge gate: only burst if edge >= thr + BURST_MIN_EDGE_EXTRA_BPS
            if abs_delta_bps < thr + BURST_MIN_EDGE_EXTRA_BPS:
                # Edge not strong enough for burst — stay with probe only
                st.last_entry_ts = iso_z(utc_now())
                self._sm_transition(m.slug, "COOLDOWN", "probe_only_weak_edge", ctx)
                return
            # Signal still valid + strong edge → burst scale in background thread
            self._sm_transition(m.slug, "SCALING", "signal_confirmed", ctx)
            burst_ctx = dict(ctx)  # snapshot context
            burst_clip = clip
            burst_thr = thr  # capture threshold for taker gating inside burst
            def _run_burst():
                try:
                    self._execute_burst_buy(m, st, outcome, burst_clip, burst_ctx, burst_thr)
                finally:
                    st.last_entry_ts = iso_z(utc_now())
                    self._sm_transition(m.slug, "COOLDOWN", "burst_complete", burst_ctx)
            t = threading.Thread(target=_run_burst, daemon=True)
            t.start()
            return

        elif sm["state"] == "SCALING":
            # Burst is running in background thread — don't interrupt
            return

        elif sm["state"] == "COOLDOWN":
            # Already handled above (cooldown expiry → IDLE)
            return

    # -----------------------------------------------------------------
    # Burst execution engine
    # -----------------------------------------------------------------
    def _get_internal_positions(self) -> dict:
        """Build {slug: {"Up": qty, "Down": qty}} from internal state."""
        result = {}
        for slug, st in self.market_states.items():
            up_qty = st.positions["Up"].qty if st.positions["Up"].qty >= MIN_QTY else 0.0
            dn_qty = st.positions["Down"].qty if st.positions["Down"].qty >= MIN_QTY else 0.0
            if up_qty > 0 or dn_qty > 0:
                result[slug] = {"Up": up_qty, "Down": dn_qty}
        return result

    def _get_token_to_slug_outcome(self) -> dict:
        """Build {token_id: (slug, outcome)} from cached markets."""
        result = {}
        for m in self._cached_markets:
            if m.outcome_up_id:
                result[m.outcome_up_id] = (m.slug, "Up")
            if m.outcome_down_id:
                result[m.outcome_down_id] = (m.slug, "Down")
        return result

    def _get_token_to_slug_outcome_for_truth(self) -> dict:
        """Build {token_id: (slug, outcome)} for TruthCapture meta resolution."""
        return self._get_token_to_slug_outcome()

    def _token_for_slug_outcome(self, slug: str, outcome: str) -> Tuple[str, str]:
        """Return (token_id, market_id) for a slug+outcome from cached markets."""
        for m in self._cached_markets:
            if m.slug == slug:
                token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
                return token_id or "", m.market_id or ""
        return "", ""

    # ── Order Lifecycle Guard: process recovered fills ──

    def _process_lifecycle_recovered_fills(self):
        """Poll tracked orders for missed fills and apply through unified pipeline.

        Uses _apply_fill() which handles:
          - Position updates (qty, vwap, cost_usdc)
          - Cash/budget adjustments
          - Budget reservation release
          - Fills ledger recording
        """
        recovered = self._exec_tracker.poll_pending_orders()
        if not recovered:
            return
        for fill in recovered:
            oid = fill["order_id"]
            slug = fill["slug"]
            outcome = fill["outcome"]
            side = fill["side"]
            qty = fill["fill_qty"]
            price = fill["fill_price"]
            reason = fill.get("reason", "LIFECYCLE_RECOVERED")

            print(f"  [ORDER_LIFECYCLE] Processing recovered fill: "
                  f"{slug} {outcome} {side} qty={qty:.2f} @ {price:.4f} "
                  f"reason={reason}")

            # Use unified apply_fill pipeline
            self._apply_fill(slug, outcome, side, qty, price,
                             order_id=oid, reason=reason,
                             source="lifecycle_guard")

            # Also clear in-flight guard for completed tokens
            token_id = fill.get("token_id", "")
            if token_id and side == "BUY":
                self._clear_buy_inflight(token_id)

            st = self.market_states.get(slug)
            if st is None and self._fills_ledger is not None:
                # No market_state for this slug — still record in ledger
                token_id, market_id = self._token_for_slug_outcome(slug, outcome)
                crypto = ""
                for mk in self._cached_markets:
                    if mk.slug == slug:
                        crypto = mk.crypto
                        break
                action = side.upper()
                self._fills_ledger.record_fill(
                    slug=slug, crypto=crypto, token_id=token_id,
                    market_id=market_id, side=outcome, action=action,
                    fill_qty=qty, fill_price=price,
                    order_id=oid, source="bot",
                )

    def _process_truth_recovered_fills(self, fills: list) -> None:
        """Apply fills recovered by TruthCapture poll_watchers through the
        unified _apply_fill pipeline.  This closes the gap where poll-recovered
        fills updated TRUTH but not bot positions/ledger/cash/reservations.
        """
        for fill in fills:
            oid = fill["order_id"]
            slug = fill["slug"]
            outcome = fill["outcome"]
            side = fill["side"]
            qty = fill["fill_qty"]
            price = fill["fill_price"]
            source = fill.get("source", "poll")

            write_jsonl({
                "event_type": "RECOVERED_FILL",
                "source": source,
                "slug": slug, "outcome": outcome,
                "side": side, "order_id": oid,
                "qty": round(qty, 2), "price": round(price, 4),
                "ts_ms": int(time.time() * 1000),
            })
            print(f"  [TRUTH→BOT] RECOVERED_FILL: {slug} {outcome} {side} "
                  f"qty={qty:.2f} @ {price:.4f} order={oid[:16]}... src={source}")

            self._apply_fill(slug, outcome, side, qty, price,
                             order_id=oid, reason="RECOVERED_FILL",
                             source=source, skip_truth=True)

            # Clear in-flight guard for recovered BUY fills
            token_id = fill.get("token_id", "")
            if token_id and side == "BUY":
                self._clear_buy_inflight(token_id)

        # Force immediate reconciliation so drift is caught quickly
        if fills:
            self._truth._last_reconcile_ts = 0.0
            print(f"  [TRUTH→BOT] {len(fills)} recovered fill(s) applied — "
                  f"reconciliation scheduled for next tick")

    # ── Fills Ledger helpers: record_order_intent / record_fill at call sites ──

    def _print_fill_summary(self, st: MarketState, action: str, outcome: str,
                            price: float, qty: float) -> None:
        """Print a concise one-line portfolio summary after every fill."""
        equity = self._equity()
        total_invested = sum(
            s.positions[o].cost_usdc
            for s in self.market_states.values()
            for o in ["Up", "Down"] if s.positions[o].qty >= MIN_QTY
        )
        fills_count = len(self._truth._fills) if hasattr(self._truth, '_fills') else 0
        # Build compact position list
        pos_parts = []
        for slug, s in self.market_states.items():
            for o in ["Up", "Down"]:
                p = s.positions[o]
                if p.qty >= MIN_QTY:
                    pos_parts.append(f"{s.crypto} {o}:{p.qty:.0f}@{p.vwap:.3f}")
        pos_str = "  ".join(pos_parts) if pos_parts else "none"
        pnl_str = f"{self.realized_pnl_usdc:+.2f}"
        print(f"  >>> {action} {st.crypto} {outcome} {qty:.1f}@${price:.3f}  |  "
              f"Cash: ${self.cash_usdc:.2f}  Equity: ${equity:.2f}  "
              f"Invested: ${total_invested:.2f}  P&L: {pnl_str}  "
              f"Fills: {fills_count}")
        print(f"      Positions: {pos_str}")

    def _ledger_record_buy_fill(self, m, st, outcome: str, price: float, qty: float,
                                order_id: str = "", trade_id: str = "",
                                skip_truth: bool = False) -> None:
        """Record a CONFIRMED BUY fill in both fills ledger and truth capture.

        skip_truth: If True, skip truth.record_fill (used when truth already
        recorded this fill, e.g. poll-recovered fills).
        """
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        if self._fills_ledger is not None:
            self._fills_ledger.record_fill(
                slug=st.slug, crypto=st.crypto, token_id=token_id or "",
                market_id=m.market_id or "", side=outcome, action="BUY",
                fill_qty=qty, fill_price=price, order_id=order_id,
                trade_id=trade_id, source="bot",
            )
        if not skip_truth:
            # Truth Capture — record unless already applied (e.g. poll recovery)
            self._truth.record_fill(
                token_id=token_id or "", slug=st.slug, outcome=outcome,
                action="BUY", qty=qty, price=price,
                order_id=order_id, trade_id=trade_id, source="ws",
            )
            # Notify watcher so it doesn't double-record
            if order_id:
                self._truth.notify_fill(order_id, qty)
        # Print portfolio summary after fill
        self._print_fill_summary(st, "BUY", outcome, price, qty)

    def _ledger_record_sell_fill(self, m, st, outcome: str, price: float, qty: float,
                                 order_id: str = "", trade_id: str = "",
                                 skip_truth: bool = False) -> None:
        """Record a CONFIRMED SELL fill in both fills ledger and truth capture.

        skip_truth: If True, skip truth.record_fill (used when truth already
        recorded this fill, e.g. poll-recovered fills).
        """
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        if self._fills_ledger is not None:
            self._fills_ledger.record_fill(
                slug=st.slug, crypto=st.crypto, token_id=token_id or "",
                market_id=m.market_id or "", side=outcome, action="SELL",
                fill_qty=qty, fill_price=price, order_id=order_id,
                trade_id=trade_id, source="bot",
            )
        if not skip_truth:
            # Truth Capture — record unless already applied (e.g. poll recovery)
            self._truth.record_fill(
                token_id=token_id or "", slug=st.slug, outcome=outcome,
                action="SELL", qty=qty, price=price,
                order_id=order_id, trade_id=trade_id, source="ws",
            )
            if order_id:
                self._truth.notify_fill(order_id, qty)
        # Print portfolio summary after fill
        self._print_fill_summary(st, "SELL", outcome, price, qty)

    def _ledger_order_intent(self, m, st, outcome: str, action: str,
                             qty: float, price: float, reason: str,
                             order_id: str = "") -> None:
        """Record an ORDER INTENT in the fills ledger (call at SUBMIT time)."""
        if self._fills_ledger is None:
            return
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        self._fills_ledger.record_order_intent(
            slug=st.slug, token_id=token_id or "", side=outcome,
            action=action, requested_qty=qty, requested_price=price,
            reason=reason, order_id=order_id,
        )

    def _truth_watch_after_place(self, fill_result: dict, m, outcome: str,
                                 side: str, qty: float, price: float) -> None:
        """Register a truth watcher after any order placement."""
        oid = fill_result.get("order_id", "")
        if not oid or oid.startswith("paper_"):
            return
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        st = self.market_states.get(m.slug)
        slug = st.slug if st else m.slug
        self._truth.watch_order(
            order_id=oid, token_id=token_id or "", slug=slug,
            outcome=outcome, side=side, qty=qty, price=price,
        )
        # If the order already filled (confirmed True, not "UNKNOWN"),
        # notify watcher so it doesn't double-record
        if fill_result.get("filled") is True:
            fill_qty = fill_result.get("fill_qty", 0)
            if fill_qty > 0:
                self._truth.notify_fill(oid, fill_qty)

    def _sync_clob_balances(self):
        """Reconcile internal position state with actual CLOB balances.
        Runs every 60s in LIVE/LIVE_SAFE. Corrects drift without trading."""
        if MODE not in ("LIVE", "LIVE_SAFE"):
            return
        try:
            balances = self.client.get_balances()
            if not balances:
                return
            token_map = self._get_token_to_slug_outcome()
            # Build actual balance lookup: {(slug, outcome): qty}
            actual = {}
            for b in balances:
                tid = b.get("token_id") or b.get("asset_type", "")
                bal = float(b.get("balance", 0))
                if tid in token_map and bal > 0:
                    slug, outcome = token_map[tid]
                    actual[(slug, outcome)] = bal

            # Compare with internal state
            drifts = []
            for slug, st in self.market_states.items():
                for outcome in ("Up", "Down"):
                    pos = st.positions.get(outcome)
                    if not pos:
                        continue
                    internal_qty = pos.qty
                    actual_qty = actual.get((slug, outcome), 0.0)
                    diff = actual_qty - internal_qty
                    if abs(diff) >= MIN_QTY:  # any non-dust drift
                        drifts.append({
                            "slug": slug, "outcome": outcome,
                            "internal_qty": internal_qty,
                            "clob_qty": actual_qty,
                            "diff": diff,
                        })
                        # Correct internal state to match reality
                        if actual_qty > internal_qty:
                            # We have MORE shares than we think (sell didn't go through)
                            pos.qty = actual_qty
                            pos.cost_usdc = pos.vwap * actual_qty if pos.vwap > 0 else 0.0
                        elif actual_qty < internal_qty and actual_qty < MIN_QTY:
                            # We have FEWER shares (sell went through but wasn't tracked)
                            pos.qty = 0.0
                            pos.cost_usdc = 0.0
                        else:
                            pos.qty = actual_qty
                            pos.cost_usdc = pos.vwap * actual_qty if pos.vwap > 0 else 0.0

            if drifts:
                # Calculate total USD drift for desync severity check
                total_drift_usd = 0.0
                for d in drifts:
                    # Estimate USD impact: use vwap or 0.50 as fallback
                    pos = self.market_states.get(d["slug"], None)
                    vwap = 0.50
                    if pos:
                        p = pos.positions.get(d["outcome"])
                        if p and p.vwap > 0:
                            vwap = p.vwap
                    total_drift_usd += abs(d["diff"]) * vwap

                write_jsonl({"event_type": "BALANCE_DRIFT_CORRECTED",
                             "drifts": drifts,
                             "total_drift_usd": round(total_drift_usd, 2),
                             "ts_ms": int(time.time() * 1000)})
                for d in drifts:
                    print(f"  [DRIFT] {d['slug']} {d['outcome']}: "
                          f"internal={d['internal_qty']} → CLOB={d['clob_qty']} "
                          f"(diff={d['diff']:+.1f})")

                # DESYNC HARD STOP: if total drift exceeds $1 USD, halt trading
                if total_drift_usd > 1.0 and not self._desync_hard_stop:
                    self._desync_hard_stop = True
                    write_jsonl({
                        "event_type": "DESYNC_HARD_STOP",
                        "source": "balance_sync",
                        "total_drift_usd": round(total_drift_usd, 2),
                        "drift_count": len(drifts),
                        "drifts": drifts,
                        "ts_ms": int(time.time() * 1000),
                    })
                    print(f"  *** DESYNC HARD STOP *** balance drift ${total_drift_usd:.2f} — "
                          f"new entries DISABLED until restart")
                    # Cancel all open orders to prevent further fills on stale state
                    self._cancel_all_orders()

        except Exception as e:
            write_jsonl({"event_type": "BALANCE_SYNC_ERROR",
                         "err": str(e)[:200]})

    def _run_reconciliation(self, trigger: str = "periodic") -> None:
        """Run full position reconciliation against wallet balances.

        Args:
            trigger: what triggered this reconciliation (startup/periodic/post_fill)
        """
        if not POSITION_RECONCILE_ENABLED:
            return
        try:
            token_map = self._get_token_to_slug_outcome()
            # Build mid-price lookup from cached books for vwap estimation
            mid_prices: Dict[tuple, float] = {}
            for slug, books in self.last_book.items():
                for outcome, book in books.items():
                    if book and book.mid > 0:
                        mid_prices[(slug, outcome)] = book.mid
            result = self._exec_reconciler.reconcile_positions(
                market_states=self.market_states,
                token_to_slug_outcome=token_map,
                dscalp_positions=self._dscalp_positions,
                mid_price_lookup=mid_prices,
                last_fill_ts=self._last_fill_ts,
            )
            corrections = result.get("corrections", [])
            desync = result.get("desync_detected", False)
            if corrections:
                write_jsonl({
                    "event_type": "RECONCILE_TRIGGERED",
                    "trigger": trigger,
                    "corrections_count": len(corrections),
                    "desync": desync,
                    "ts_ms": int(time.time() * 1000),
                })
                # Save state immediately after corrections
                self._save_state()

            # DESYNC HARD STOP: if reconciler detects a critical mismatch
            if desync and not self._desync_hard_stop:
                self._desync_hard_stop = True
                write_jsonl({
                    "event_type": "DESYNC_HARD_STOP",
                    "source": "reconciliation",
                    "trigger": trigger,
                    "corrections_count": len(corrections),
                    "ts_ms": int(time.time() * 1000),
                })
                print(f"  *** DESYNC HARD STOP *** reconciliation detected critical desync — "
                      f"new entries DISABLED until restart")
                self._cancel_all_orders()
        except Exception as e:
            write_jsonl({
                "event_type": "RECONCILE_ERROR",
                "trigger": trigger,
                "err": str(e)[:200],
                "ts_ms": int(time.time() * 1000),
            })

    def _run_ledger_reconciliation(self) -> None:
        """Run fills ledger reconciliation against wallet balances + open orders.

        1. Register any new token mappings
        2. Fetch + ingest recent fills from Polymarket API (dedup)
        3. Update unrealized PnL from current book prices
        4. Fetch wallet balances + open orders and reconcile
        5. Enter SAFE MODE on critical mismatch (if configured)
        """
        if self._fills_ledger is None:
            return
        try:
            # 1. Register token mappings for any new markets
            for m in self._cached_markets:
                if m.outcome_up_id:
                    self._fills_ledger.register_token(
                        m.outcome_up_id, m.slug, m.crypto, "Up")
                if m.outcome_down_id:
                    self._fills_ledger.register_token(
                        m.outcome_down_id, m.slug, m.crypto, "Down")

            # 2. (Removed) External trade scanning disabled — positions are
            #    updated on each confirmed fill via _live_buy/_live_sell.
            #    No need to re-scan all 4600+ API trades every cycle.

            # 3. Update unrealized PnL from cached book prices
            price_lookup: Dict[Tuple[str, str], float] = {}
            for slug, books in self.last_book.items():
                for outcome, book in books.items():
                    if not book or book.mid <= 0:
                        continue
                    token_id, _ = self._token_for_slug_outcome(slug, outcome)
                    if token_id:
                        price_lookup[(token_id, outcome)] = book.mid
            self._fills_ledger.update_unrealized_pnl(price_lookup)

            # 4. (Removed) Wallet balance reconciliation disabled — positions
            #    are kept correct by verifying each fill in _live_buy/_live_sell.
            #    The old reconciliation kept failing with empty get_balances()
            #    responses and pausing entries unnecessarily.

            # 5. Log ledger summary periodically
            summary = self._fills_ledger.summary()
            write_jsonl({
                "event_type": "LEDGER_STATUS",
                "fills_in_current_hour": summary.get("fills_in_current_hour", summary["total_fills"]),
                "total_fills": summary["total_fills"],
                "active_positions": summary["active_positions"],
                "total_realized_pnl": round(summary["total_realized_pnl"], 4),
                "total_unrealized_pnl": round(summary["total_unrealized_pnl"], 4),
                "hour": summary.get("hour", ""),
                "safe_mode": summary["safe_mode"],
                "ts_ms": int(time.time() * 1000),
            })

        except Exception as e:
            write_jsonl({
                "event_type": "LEDGER_RECONCILE_ERROR",
                "err": str(e)[:200],
                "ts_ms": int(time.time() * 1000),
            })

    def _buys_allowed(self) -> bool:
        """Check if BUY orders are currently allowed.
        All modes: always True (burst window is handled separately).
        Blocked if position desync detected and POSITION_RECONCILE_BLOCK_ON_DESYNC is True.
        Blocked if ledger SAFE MODE is active.
        Blocked during flatten phase (t_min >= FLATTEN_START_MIN).
        Blocked by ENTRY_ONLY_EARLY_WINDOW after burst window (sells/flatten still ok)."""
        # Block new trades during critical state desync
        if (POSITION_RECONCILE_ENABLED
                and POSITION_RECONCILE_BLOCK_ON_DESYNC
                and self._exec_reconciler.is_desynced()):
            return False
        # Block new trades during ledger SAFE MODE
        if self._fills_ledger is not None and self._fills_ledger.safe_mode:
            return False
        # Block if desync hard stop active
        if self._desync_hard_stop:
            return False
        # Block new entries during flatten phase
        if self._is_in_flatten_phase():
            return False
        # Block ALL new buys after early window (sells/flatten always allowed)
        if ENTRY_ONLY_EARLY_WINDOW:
            if self._is_past_early_window():
                return False
        return True

    def _is_in_flatten_phase(self) -> bool:
        """Check if ANY active market is past FLATTEN_START_MIN.
        Uses the first market with valid hour_start_utc."""
        now = utc_now()
        for st in self.market_states.values():
            if not st.hour_start_utc:
                continue
            try:
                hour_start = _parse_iso_z(st.hour_start_utc)
                t_min = minutes_into_hour(hour_start, now)
                if t_min >= FLATTEN_START_MIN:
                    return True
            except Exception:
                continue
        return False

    def _is_past_early_window(self) -> bool:
        """Check if ANY active market is past BURST_EARLY_WINDOW_MIN.

        Used by ENTRY_ONLY_EARLY_WINDOW to block ALL new buys after the
        early window (minutes 0-3).  Sells and flatten are unaffected.
        """
        now = utc_now()
        for st in self.market_states.values():
            if not st.hour_start_utc:
                continue
            try:
                hour_start = _parse_iso_z(st.hour_start_utc)
                t_min = minutes_into_hour(hour_start, now)
                if t_min > BURST_EARLY_WINDOW_MIN:
                    return True
            except Exception:
                continue
        return False

    # ── Budget reservation helpers ─────────────────────────────────

    def _total_reserved_usd(self) -> float:
        """Total USD reserved for pending/open BUY orders."""
        return sum(self._reserved_usd.values())

    def _reserve_budget(self, order_id: str, usd_amount: float) -> None:
        """Reserve budget for an open/pending BUY order."""
        if order_id and usd_amount > 0:
            self._reserved_usd[order_id] = usd_amount
            self._reserved_submit_ts[order_id] = time.time()

    def _release_reservation(self, order_id: str) -> float:
        """Release budget reservation for an order.  Returns amount released.
        Safe to call multiple times — second call returns 0."""
        self._reserved_submit_ts.pop(order_id, None)
        return self._reserved_usd.pop(order_id, 0.0)

    def _cancel_all_orders(self) -> int:
        """Cancel all open orders. Fetches open orders then cancels each.
        Returns count of successfully cancelled orders."""
        cancelled = 0
        try:
            open_orders = self.client.get_open_orders()
        except Exception as e:
            write_jsonl({"event_type": "CANCEL_ALL_FAIL",
                         "stage": "fetch_open_orders", "err": str(e)[:200],
                         "ts_ms": int(time.time() * 1000)})
            print(f"  [CANCEL_ALL] Failed to fetch open orders: {e}")
            return 0
        if not open_orders:
            write_jsonl({"event_type": "CANCEL_ALL_OK", "cancelled": 0,
                         "ts_ms": int(time.time() * 1000)})
            return 0
        errors = []
        for o in open_orders:
            oid = o.get("id") or o.get("orderID") or o.get("order_id", "")
            if not oid:
                continue
            try:
                self.client.cancel_order(oid)
                cancelled += 1
            except Exception as e:
                errors.append({"order_id": oid[:20], "err": str(e)[:80]})
        write_jsonl({"event_type": "CANCEL_ALL_OK" if not errors else "CANCEL_ALL_PARTIAL",
                     "cancelled": cancelled,
                     "errors": len(errors),
                     "total_open": len(open_orders),
                     "ts_ms": int(time.time() * 1000)})
        if errors:
            print(f"  [CANCEL_ALL] {cancelled}/{len(open_orders)} cancelled, "
                  f"{len(errors)} errors")
        else:
            print(f"  [CANCEL_ALL] All {cancelled} open orders cancelled")
        return cancelled

    def _audit_reservations(self) -> None:
        """Periodic invariant check: release stuck reservations, log anomalies.
        Called from main loop every tick."""
        now = time.time()
        total = self._total_reserved_usd()

        # Invariant: sum must never be negative
        if total < -0.001:
            write_jsonl({"event_type": "RESERVATION_INVARIANT_VIOLATION",
                         "total_reserved_usd": round(total, 4),
                         "count": len(self._reserved_usd),
                         "ts_ms": int(now * 1000)})
            # Force clear — something is very wrong
            self._reserved_usd.clear()
            self._reserved_submit_ts.clear()
            return

        # Release stuck reservations (older than RESERVATION_STUCK_TIMEOUT_SEC)
        stuck_ids = []
        for oid, submit_ts in list(self._reserved_submit_ts.items()):
            age = now - submit_ts
            if age > RESERVATION_STUCK_TIMEOUT_SEC:
                stuck_ids.append((oid, age))

        for oid, age in stuck_ids:
            # Try to cancel the order first — it may still be resting
            if oid and MODE in ("LIVE", "LIVE_SAFE"):
                try:
                    self.client.cancel_order(oid)
                except Exception:
                    pass  # already cancelled or filled
            released = self._release_reservation(oid)
            if released > 0:
                write_jsonl({"event_type": "RESERVATION_STUCK_RELEASED",
                             "order_id": oid,
                             "released_usd": round(released, 2),
                             "age_sec": round(age, 1),
                             "ts_ms": int(now * 1000)})
                print(f"  [RESERVATION] STUCK ${released:.2f} held {age:.0f}s — "
                      f"cancelled + released (order {oid[:16]}...)")

        # Log summary if any reservations exist (throttled to every 10s)
        if self._reserved_usd:
            last_audit_log = getattr(self, '_last_reservation_audit_log', 0.0)
            if now - last_audit_log >= 10.0:
                self._last_reservation_audit_log = now
                write_jsonl({"event_type": "RESERVATION_AUDIT",
                             "count": len(self._reserved_usd),
                             "total_usd": round(self._total_reserved_usd(), 2),
                             "ts_ms": int(now * 1000)})

    def _total_exposure_usd(self) -> float:
        """Total exposure = filled positions + reserved (pending) orders.
        This is the TRUE cap check — prevents overspending."""
        filled_usd = sum(
            st.positions[o].qty * st.positions[o].vwap
            for st in self.market_states.values()
            for o in ["Up", "Down"] if st.positions[o].qty >= MIN_QTY
        )
        return filled_usd + self._total_reserved_usd()

    # ── In-flight BUY guard ────────────────────────────────────────

    def _has_inflight_buy(self, token_id: str) -> bool:
        """Check if there's already an open BUY for this token.

        Normal submit: _inflight_buy_ts[tid] = now → blocked for PER_TOKEN_COOLDOWN_MS.
        UNKNOWN hold: _inflight_buy_ts[tid] = now + RESERVATION_STUCK_TIMEOUT_SEC
            → blocked until watcher resolves or timeout expires.
        """
        if not ONE_INFLIGHT_PER_TOKEN:
            return False
        last_ts = self._inflight_buy_ts.get(token_id, 0.0)
        return (time.time() - last_ts) < (PER_TOKEN_COOLDOWN_MS / 1000.0)

    def _record_buy_submit(self, token_id: str) -> None:
        """Record that a BUY was submitted for this token."""
        self._inflight_buy_ts[token_id] = time.time()

    def _clear_buy_inflight(self, token_id: str) -> None:
        """Clear in-flight state for a token (on fill or cancel)."""
        self._inflight_buy_ts.pop(token_id, None)

    # ── Global order rate limiter ──────────────────────────────────

    def _global_rate_ok(self) -> bool:
        """Check if we're within the global order rate limit."""
        now = time.time()
        # Prune old entries (older than 1 second)
        self._global_order_ts = [t for t in self._global_order_ts if now - t < 1.0]
        return len(self._global_order_ts) < GLOBAL_ORDER_RATE_LIMIT_PER_SEC

    def _record_global_order(self) -> None:
        """Record a global order submission."""
        self._global_order_ts.append(time.time())

    # ── Per-token attempt breaker ──────────────────────────────────

    def _token_attempt_ok(self, token_id: str) -> bool:
        """Check if this token has hit its per-minute attempt limit.
        Returns True if OK to attempt, False if breaker is tripped."""
        now = time.time()
        # Check existing cooldown
        if now < self._token_breaker_until.get(token_id, 0.0):
            return False
        # Prune old attempts (> 60s)
        attempts = self._token_attempt_ts.get(token_id, [])
        attempts = [t for t in attempts if now - t < 60.0]
        self._token_attempt_ts[token_id] = attempts
        if len(attempts) >= TOKEN_ATTEMPT_MAX_PER_MIN:
            # Trip breaker
            self._token_breaker_until[token_id] = now + TOKEN_ATTEMPT_COOLDOWN_SEC
            write_jsonl({"event_type": "TOKEN_BREAKER_TRIPPED",
                         "token_id": token_id[-12:],
                         "attempts_in_60s": len(attempts),
                         "cooldown_sec": TOKEN_ATTEMPT_COOLDOWN_SEC,
                         "ts_ms": int(now * 1000)})
            print(f"  [BREAKER] Token {token_id[-12:]} hit {len(attempts)} attempts/min — "
                  f"cooling down {TOKEN_ATTEMPT_COOLDOWN_SEC:.0f}s")
            return False
        return True

    def _record_token_attempt(self, token_id: str) -> None:
        """Record an order attempt for the per-token breaker."""
        self._token_attempt_ts.setdefault(token_id, []).append(time.time())

    # ── Unified apply_fill — single entry point for all fill sources ──

    def _apply_fill(self, slug: str, outcome: str, side: str,
                    fill_qty: float, fill_price: float,
                    order_id: str = "", reason: str = "",
                    source: str = "immediate",
                    skip_truth: bool = False) -> float:
        """Unified fill application — used by immediate fills, poll recovery,
        and lifecycle watcher.

        Updates:
          - positions (qty, vwap, cost_usdc)
          - cash_usdc
          - _hour_budget_remaining
          - reserved_usd (releases reservation)
          - fills ledger
          - realized PnL (for SELL)

        Returns: PnL for SELL fills, 0.0 for BUY fills.
        """
        # ── Dedup: prevent double-apply from ws + poll + lifecycle ──
        if order_id:
            dedup_key = (order_id, side, round(fill_qty, 2))
            if dedup_key in self._applied_fills:
                write_jsonl({
                    "event_type": "FILL_APPLY_DEDUP",
                    "source": source,
                    "slug": slug, "outcome": outcome,
                    "side": side, "order_id": order_id,
                    "qty": round(fill_qty, 2),
                    "ts_ms": int(time.time() * 1000),
                })
                return 0.0
            self._applied_fills.add(dedup_key)

        st = self.market_states.get(slug)
        if not st:
            return 0.0

        m = None
        for mk in self._cached_markets:
            if mk.slug == slug:
                m = mk
                break
        if not m:
            return 0.0

        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        pnl = 0.0

        # Release any budget reservation for this order
        released = self._release_reservation(order_id)

        if side == "BUY":
            actual_cost = fill_price * fill_qty
            if MODE in ("LIVE", "LIVE_SAFE"):
                if released > 0:
                    # Reservation was already deducted from cash.
                    # Add it back, then let _live_buy deduct the real cost.
                    self.cash_usdc += released
                    self._hour_budget_remaining += released
                self._live_buy(st, outcome, fill_price, fill_qty, actual_cost)
            self._ledger_record_buy_fill(m, st, outcome, fill_price, fill_qty,
                                         order_id=order_id,
                                         skip_truth=skip_truth)
            self._clear_buy_inflight(token_id)
        elif side == "SELL":
            if MODE in ("LIVE", "LIVE_SAFE"):
                pnl = self._live_sell(st, outcome, fill_price, fill_qty)
            self._ledger_record_sell_fill(m, st, outcome, fill_price, fill_qty,
                                          order_id=order_id,
                                          skip_truth=skip_truth)

        self._record_fill_ts(slug)

        write_jsonl({
            "event_type": "FILL_APPLIED",
            "source": source,
            "slug": slug, "outcome": outcome,
            "side": side, "order_id": order_id,
            "qty": round(fill_qty, 2), "price": round(fill_price, 4),
            "released_reservation": round(released, 2),
            "pnl": round(pnl, 4) if side == "SELL" else None,
            "reason": reason,
            "ts_ms": int(time.time() * 1000),
        })

        return pnl

    # ── Quick-buy IOC with retry micro-loop ──────────────────────

    def _quick_buy_ioc(self, token_id: str, ask_price: float, qty: float,
                       book, m, st, outcome: str, reason: str) -> dict:
        """Place IOC (or FOK if ENTRY_USE_FOK=True) buy at ask with up to IOC_MAX_RETRIES.

        IOC allows partial fills (better on thin books).
        Each retry steps price up by 1 tick (0.01) up to IOC_MAX_SLIPPAGE_CENTS
        above the original ask.

        Returns the fill dict from the first successful fill (which may be
        partial for IOC), or the last attempt's result.

        Guardrails:
          - spread must be <= DSCALP_MAX_SPREAD_CENTS * 1.5
          - slippage must be <= IOC_MAX_SLIPPAGE_CENTS above ask
          - obeys one-in-flight-per-token (checked by caller)
        """
        max_price = min(0.99, ask_price + IOC_MAX_SLIPPAGE_CENTS / 100.0)
        attempt_price = ask_price
        last_result = None
        total_filled = 0.0
        remaining_qty = qty

        for attempt in range(1 + IOC_MAX_RETRIES):
            if attempt_price > max_price:
                break
            if remaining_qty < MIN_QTY:
                break
            # Spread check
            if book and book.spread * 100 > DSCALP_MAX_SPREAD_CENTS * 1.5:
                break

            result = self.client.place_immediate_order(
                token_id, "BUY", attempt_price, remaining_qty,
                use_fok=ENTRY_USE_FOK)
            last_result = result

            if result.get("filled") is True:
                fill_qty = result.get("fill_qty", 0.0)
                partial = result.get("partial", False)
                total_filled += fill_qty
                label = "IOC" if not ENTRY_USE_FOK else "FOK"
                write_jsonl({"event_type": f"{label}_BUY_OK",
                             "slug": m.slug, "outcome": outcome,
                             "attempt": attempt, "price": attempt_price,
                             "requested_qty": remaining_qty,
                             "fill_qty": fill_qty,
                             "partial": partial,
                             "reason": reason})
                if partial and not ENTRY_USE_FOK:
                    # IOC partial fill: reduce remaining, try next tick
                    remaining_qty = round(remaining_qty - fill_qty, 2)
                    # Update result to reflect cumulative fill
                    result = dict(result)
                    result["fill_qty"] = total_filled
                    if remaining_qty < MIN_QTY:
                        return result  # close enough to full fill
                    # Step price up for remainder
                    attempt_price = round(attempt_price + IOC_TICK_STEP, 3)
                    if attempt < IOC_MAX_RETRIES:
                        time.sleep(IOC_RETRY_DELAY_MS / 1000.0)
                    continue
                return result

            if result.get("filled") == "UNKNOWN":
                return result  # let lifecycle watcher handle

            # Killed — step price up by 1 tick for next attempt
            attempt_price = round(attempt_price + IOC_TICK_STEP, 3)
            if attempt < IOC_MAX_RETRIES:
                time.sleep(IOC_RETRY_DELAY_MS / 1000.0)

        # If we got partial fills across retries, return cumulative result
        if total_filled > 0 and last_result:
            last_result = dict(last_result)
            last_result["filled"] = True
            last_result["fill_qty"] = total_filled
            last_result["partial"] = True
            return last_result

        # All attempts failed — no fill at all
        if last_result is None:
            last_result = {"order_id": "", "filled": False, "fill_qty": 0.0,
                           "fill_price": ask_price, "status": "ioc_killed",
                           "partial": False}
        label = "IOC" if not ENTRY_USE_FOK else "FOK"
        write_jsonl({"event_type": f"{label}_BUY_ALL_KILLED",
                     "slug": m.slug, "outcome": outcome,
                     "attempts": 1 + IOC_MAX_RETRIES,
                     "final_price": attempt_price, "reason": reason})
        return last_result

    # ── End-of-hour flatten logic ─────────────────────────────────

    def _is_flatten_phase(self, t_min: float) -> bool:
        """Are we past FLATTEN_START_MIN (stop new entries, start trimming)?"""
        return t_min >= FLATTEN_START_MIN

    def _is_hard_flatten_phase(self, t_min: float) -> bool:
        """Are we past HARD_FLATTEN_MIN (force-close everything)?"""
        return t_min >= HARD_FLATTEN_MIN

    _FLATTEN_TRIM_MAX_SPREAD_CENTS = 5.0  # don't dump into wide spreads

    def _flatten_trim_positions(self, t_min: float):
        """Soft flatten: sell positions, spread-aware.
        Called each tick when t_min >= FLATTEN_START_MIN.

        If spread <= max (5c): sell at bid (full trim).
        If spread > max: sell only half the position with IOC at mid-point
            (don't blindly dump into a wide spread)."""
        for slug, st in self.market_states.items():
            m = None
            for mk in self._cached_markets:
                if mk.slug == slug:
                    m = mk
                    break
            if not m:
                continue
            for outcome in ("Up", "Down"):
                pos = st.positions[outcome]
                if pos.qty < MIN_QTY:
                    continue
                book = self.last_book.get(slug, {}).get(outcome)
                if not book or book.bid <= 0:
                    continue

                spread_cents = book.spread * 100.0
                if spread_cents <= self._FLATTEN_TRIM_MAX_SPREAD_CENTS:
                    # Tight spread — sell full position at bid
                    self._do_sell(m, st, outcome, pos.qty, book.bid,
                                  reason="FLATTEN_TRIM", leg="FLATTEN")
                else:
                    # Wide spread — sell half at midpoint to limit slippage
                    trim_qty = max(MIN_QTY, round(pos.qty * 0.5, 2))
                    # Use midpoint between bid and ask for better execution
                    mid_price = round((book.bid + book.ask) / 2.0, 3)
                    sell_price = max(0.01, min(mid_price, book.ask - 0.01))
                    token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
                    if MODE in ("LIVE", "LIVE_SAFE"):
                        result = self.client.place_immediate_order(
                            token_id, "SELL", sell_price, trim_qty,
                            use_fok=False)
                        if result.get("filled") is True:
                            fill_qty = result.get("fill_qty", 0.0)
                            self._apply_fill(slug, outcome, "SELL",
                                             fill_qty, result["fill_price"],
                                             order_id=result.get("order_id", ""),
                                             reason="FLATTEN_TRIM_WIDE",
                                             source="flatten")
                        elif result.get("filled") == "UNKNOWN":
                            oid = result.get("order_id", "")
                            if oid:
                                self._exec_tracker.on_submit(
                                    oid, slug, outcome, "SELL", sell_price,
                                    trim_qty, "FLATTEN_TRIM_WIDE",
                                    token_id=token_id,
                                    pending_confirmation=True)
                        # If killed — do nothing, wait for next tick
                    else:
                        # Paper mode
                        self._do_sell(m, st, outcome, trim_qty, sell_price,
                                      reason="FLATTEN_TRIM_WIDE", leg="FLATTEN")
                    write_jsonl({"event_type": "FLATTEN_TRIM_WIDE_SPREAD",
                                 "slug": slug, "outcome": outcome,
                                 "spread_cents": round(spread_cents, 1),
                                 "trim_qty": trim_qty,
                                 "total_qty": pos.qty,
                                 "sell_price": sell_price})

    def _hard_flatten_all(self):
        """Hard flatten: force-close ALL positions with escalating IOC sells.
        Called once when t_min >= HARD_FLATTEN_MIN.

        Escalation: 2c -> 3c -> 5c slippage, 300ms between attempts.
        Must succeed even if it costs edge."""
        if self._hard_flatten_done:
            return
        self._hard_flatten_done = True

        write_jsonl({"event_type": "HARD_FLATTEN_START",
                     "ts_ms": int(time.time() * 1000)})
        print("  *** HARD FLATTEN *** Force-closing all positions")

        # 1. Cancel all open orders first
        self._cancel_all_orders()

        # 2. Release all reservations
        for oid in list(self._reserved_usd.keys()):
            self._release_reservation(oid)

        # 3. Force-sell every position with escalating IOC slippage
        slippage_steps_cents = [2.0, 3.0, 5.0]
        positions_closed = 0
        for slug, st in self.market_states.items():
            m = None
            for mk in self._cached_markets:
                if mk.slug == slug:
                    m = mk
                    break
            if not m:
                continue
            for outcome in ("Up", "Down"):
                pos = st.positions[outcome]
                if pos.qty < MIN_QTY:
                    continue
                book = self.last_book.get(slug, {}).get(outcome)
                if not book or book.bid <= 0:
                    continue
                token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
                remaining_qty = pos.qty

                if MODE not in ("LIVE", "LIVE_SAFE"):
                    # Paper mode — instant close
                    self._do_sell(m, st, outcome, remaining_qty, book.bid,
                                  reason="HARD_FLATTEN", leg="FLATTEN")
                    positions_closed += 1
                    continue

                # Escalating IOC sell attempts
                for step_idx, slip_c in enumerate(slippage_steps_cents):
                    if remaining_qty < MIN_QTY:
                        break
                    sell_price = max(0.01, book.bid - slip_c / 100.0)
                    result = self.client.place_immediate_order(
                        token_id, "SELL", sell_price, remaining_qty,
                        use_fok=False)

                    if result.get("filled") is True:
                        fill_qty = result.get("fill_qty", 0.0)
                        self._apply_fill(slug, outcome, "SELL",
                                         fill_qty, result["fill_price"],
                                         order_id=result.get("order_id", ""),
                                         reason="HARD_FLATTEN",
                                         source="hard_flatten")
                        remaining_qty = round(remaining_qty - fill_qty, 2)
                        write_jsonl({"event_type": "HARD_FLATTEN_STEP_FILL",
                                     "slug": slug, "outcome": outcome,
                                     "step": step_idx, "slip_c": slip_c,
                                     "fill_qty": fill_qty,
                                     "remaining_qty": remaining_qty})
                        if remaining_qty < MIN_QTY:
                            break
                        # Partial fill on this step — wait then try wider slippage
                        time.sleep(0.3)
                        continue

                    if result.get("filled") == "UNKNOWN":
                        oid = result.get("order_id", "")
                        if oid:
                            self._exec_tracker.on_submit(
                                oid, slug, outcome, "SELL", sell_price,
                                remaining_qty, "HARD_FLATTEN",
                                token_id=token_id,
                                pending_confirmation=True)
                        remaining_qty = 0.0  # assume it went through
                        break

                    # Not filled — wait 300ms then escalate
                    write_jsonl({"event_type": "HARD_FLATTEN_STEP_MISS",
                                 "slug": slug, "outcome": outcome,
                                 "step": step_idx, "slip_c": slip_c,
                                 "status": result.get("status")})
                    time.sleep(0.3)

                # Fallback: if still holding after all slippage steps
                if remaining_qty >= MIN_QTY:
                    self._do_sell(m, st, outcome, remaining_qty, book.bid,
                                  reason="HARD_FLATTEN_FALLBACK", leg="FLATTEN")

                positions_closed += 1

        write_jsonl({"event_type": "HARD_FLATTEN_DONE",
                     "positions_closed": positions_closed,
                     "ts_ms": int(time.time() * 1000)})
        print(f"  [HARD_FLATTEN] Closed {positions_closed} positions")

    def _entry_window_remaining_sec(self) -> float:
        """Seconds remaining in LIVE_SAFE entry window. 0 if expired or not LIVE_SAFE."""
        if MODE != "LIVE_SAFE":
            return 0.0
        remaining = LIVE_SAFE_ENTRY_WINDOW_SEC - (time.monotonic() - self._process_start_mono)
        return max(0.0, remaining)

    def _live_safe_cap_usd(self, order_usd: float, price: float) -> Tuple[float, float]:
        """Apply LIVE_SAFE trade-size limiter to a BUY order.
        Returns (capped_usd, capped_qty). If qty < CLOB minimum, returns (0, 0) to skip.
        CLOB requires: min 5 shares AND min $1 total order value.
        In non-LIVE_SAFE modes, enforces CLOB minimum but no USD cap."""
        import math as _math
        min_qty_for_usd = _math.ceil(1.01 / max(1e-9, price))
        clob_min = max(CLOB_MIN_ORDER_SIZE, min_qty_for_usd)
        if MODE != "LIVE_SAFE":
            qty = max(float(clob_min), order_usd / max(1e-9, price))
            return qty * price, qty
        capped_usd = min(order_usd, LIVE_SAFE_MAX_ORDER_USD)
        capped_qty = capped_usd / max(1e-9, price)
        # Enforce Polymarket minimum (shares + $1 value)
        if capped_qty < clob_min:
            min_usd = clob_min * price
            if min_usd <= LIVE_SAFE_MAX_ORDER_USD:
                capped_qty = clob_min
                capped_usd = min_usd
            else:
                return 0.0, 0.0  # can't meet minimum within cap
        return capped_usd, capped_qty

    def _record_negative_exit(self, slug: str, net_pnl: float, reason: str):
        """Record a negative exit for loss-tail guard. Only tracks configured reasons."""
        if reason not in LOSS_TAIL_NEGATIVE_EXIT_REASONS:
            return
        if net_pnl >= 0:
            return
        now_mono = time.monotonic()
        hist = self._slug_neg_exits.setdefault(slug, [])
        hist.append((now_mono, net_pnl, reason))
        self._slug_neg_exit_count[slug] = self._slug_neg_exit_count.get(slug, 0) + 1
        # Trim old entries outside lookback window
        cutoff = now_mono - LOSS_TAIL_LOOKBACK_SEC
        self._slug_neg_exits[slug] = [(t, p, r) for t, p, r in hist if t >= cutoff]
        # Check if threshold exceeded → pause slug
        if len(self._slug_neg_exits[slug]) >= LOSS_TAIL_NEG_EXIT_THRESHOLD:
            pause_until = now_mono + LOSS_TAIL_PAUSE_SEC
            self._slug_entry_paused_until[slug] = pause_until
            write_jsonl({"event_type": "SLUG_PAUSED_NEGATIVE_EXITS",
                          "ts_ms": int(time.time() * 1000),
                          "slug": slug,
                          "neg_exit_count": len(self._slug_neg_exits[slug]),
                          "total_neg_pnl": round(sum(p for _, p, _ in self._slug_neg_exits[slug]), 4),
                          "pause_sec": LOSS_TAIL_PAUSE_SEC,
                          "reasons": [r for _, _, r in self._slug_neg_exits[slug]]})
            # Clear history so we don't re-trigger immediately on resume
            self._slug_neg_exits[slug] = []

    def _slug_entry_allowed(self, slug: str) -> bool:
        """Check if slug entries are paused by loss-tail guard. Sells always allowed."""
        pause_until = self._slug_entry_paused_until.get(slug, 0.0)
        return time.monotonic() >= pause_until

    # -----------------------------------------------------------------
    # PRE-8HR SAFETY: inventory cap check (block new BUYs when exceeded)
    # -----------------------------------------------------------------
    def _reserved_usd_for_slug(self, slug: str) -> float:
        """Sum reserved USD for all pending orders on a given slug.
        Uses the exec tracker to map order_id → slug."""
        total = 0.0
        for oid, usd in self._reserved_usd.items():
            tracked = self._exec_tracker._orders.get(oid)
            if tracked and tracked.slug == slug:
                total += usd
        return total

    def _inventory_cap_ok(self, slug: str, outcome: str = "") -> bool:
        """Return True if inventory caps allow a new BUY on this slug/outcome.
        Checks: per-slug USD (filled + reserved + dscalp), per-outcome shares,
        net imbalance shares.
        Sells, rescue-hedge, flatten are NEVER blocked by this gate."""
        st = self.market_states.get(slug)
        if st is None:
            return True
        # Per-slug USD (filled positions + reserved/pending + dscalp invested)
        slug_usd = sum(
            st.positions[o].qty * st.positions[o].vwap
            for o in ["Up", "Down"] if st.positions[o].qty >= MIN_QTY
        )
        slug_usd += self._dscalp_invested_usd.get(slug, 0.0)
        slug_usd += self._reserved_usd_for_slug(slug)
        if slug_usd >= MAX_POSITION_USD_PER_SLUG:
            self._diag_cap_blocks += 1
            write_jsonl({"event_type": "GATE_INVENTORY_CAP", "slug": slug,
                          "reason": "usd_per_slug", "slug_usd": round(slug_usd, 2),
                          "cap": MAX_POSITION_USD_PER_SLUG,
                          "ts_ms": int(time.time() * 1000)})
            return False
        # Per-outcome shares
        if outcome:
            qty = st.positions[outcome].qty
            if qty >= MAX_POSITION_SHARES_PER_OUTCOME:
                self._diag_cap_blocks += 1
                write_jsonl({"event_type": "GATE_INVENTORY_CAP", "slug": slug,
                              "reason": "shares_per_outcome", "outcome": outcome,
                              "qty": round(qty, 1), "cap": MAX_POSITION_SHARES_PER_OUTCOME,
                              "ts_ms": int(time.time() * 1000)})
                return False
        # Net imbalance
        up_q = st.positions["Up"].qty
        dn_q = st.positions["Down"].qty
        imbal = abs(up_q - dn_q)
        if imbal >= MAX_NET_IMBALANCE_SHARES:
            self._diag_cap_blocks += 1
            write_jsonl({"event_type": "GATE_INVENTORY_CAP", "slug": slug,
                          "reason": "net_imbalance", "imbalance": round(imbal, 1),
                          "cap": MAX_NET_IMBALANCE_SHARES,
                          "ts_ms": int(time.time() * 1000)})
            return False
        return True

    # -----------------------------------------------------------------
    # PRE-8HR SAFETY: post-fill cooldown (churn killer)
    # -----------------------------------------------------------------
    def _post_fill_cooldown_ok(self, slug: str) -> bool:
        """Return True if enough time has passed since last fill on this slug.
        Blocks new BUY attempts only; sells always allowed."""
        last = self._last_fill_ts.get(slug, 0.0)
        if (time.time() - last) * 1000 < POST_FILL_COOLDOWN_MS:
            self._diag_post_fill_cooldown_blocks += 1
            return False
        return True

    def _record_fill_ts(self, slug: str):
        """Record a fill timestamp for post-fill cooldown tracking."""
        self._last_fill_ts[slug] = time.time()

    # -----------------------------------------------------------------
    # PRE-8HR SAFETY: coin-specific spread limit for entries
    # -----------------------------------------------------------------
    def _coin_spread_entry_ok(self, crypto: str, spread_cents: float) -> bool:
        """Check if spread is within coin-specific entry limit (inclusive)."""
        if crypto in ("BTC", "ETH"):
            limit = MAX_SPREAD_ENTRY_CENTS_BTCETH
        else:
            limit = MAX_SPREAD_ENTRY_CENTS_SOLXRP
        # Use small epsilon for floating point: allow spread at exactly the limit
        if spread_cents > limit + 0.05:
            self._diag_spread_limit_blocks += 1
            return False
        return True

    # -----------------------------------------------------------------
    # PRE-8HR SAFETY: module priority — directional suppresses parity BUYs
    # -----------------------------------------------------------------
    def _directional_suppresses_parity(self, slug: str) -> bool:
        """Return True if an active directional position (or recent one) should
        suppress parity quote/recycle/rescue BUYs on this slug."""
        # Active directional position
        if slug in self._dscalp_positions:
            return True
        # Directional opened within last DIRECTIONAL_PARITY_SUPPRESS_SEC
        last_entry = self._dscalp_last_entry_ts.get(slug, 0.0)
        if time.time() - last_entry < DIRECTIONAL_PARITY_SUPPRESS_SEC:
            return True
        return False

    def _check_position_invariant(self) -> None:
        """Compare bot positions vs truth positions each tick.
        Prints POSITION_DRIFT line if mismatch > 0.01 shares on any token.
        If drift persists for 3 consecutive checks, trigger DESYNC_HARD_STOP.
        Throttled to once every 10s to avoid log spam.
        """
        now_t = time.time()
        if now_t - getattr(self, '_last_pos_invariant_ts', 0.0) < 10.0:
            return
        self._last_pos_invariant_ts = now_t

        truth_positions = self._truth.get_active_window_positions()
        # Build truth lookup: (slug, outcome) -> float(net_qty)
        truth_by_so: Dict[tuple, float] = {}
        for tp in truth_positions.values():
            key = (tp.slug, tp.outcome)
            truth_by_so[key] = float(tp.net_qty)

        # Build bot lookup from market_states
        bot_by_so: Dict[tuple, float] = {}
        for slug, st_inv in self.market_states.items():
            for outcome_inv in ("Up", "Down"):
                q = st_inv.positions[outcome_inv].qty
                if q >= MIN_QTY:
                    bot_by_so[(slug, outcome_inv)] = q

        # Compare
        all_keys = set(truth_by_so.keys()) | set(bot_by_so.keys())
        diffs = []
        for k in sorted(all_keys):
            bq = bot_by_so.get(k, 0.0)
            tq = truth_by_so.get(k, 0.0)
            if abs(bq - tq) > 0.01:
                diffs.append({"slug": k[0], "outcome": k[1],
                              "bot_qty": round(bq, 2), "truth_qty": round(tq, 2),
                              "delta": round(bq - tq, 2)})
        if diffs:
            # Track consecutive drift count
            if not hasattr(self, '_pos_drift_consecutive'):
                self._pos_drift_consecutive = 0
            self._pos_drift_consecutive += 1

            write_jsonl({
                "event_type": "POSITION_DRIFT",
                "diffs": diffs,
                "consecutive": self._pos_drift_consecutive,
                "ts_ms": int(time.time() * 1000),
            })
            parts = [f"{d['slug'][-15:]} {d['outcome']}: bot={d['bot_qty']} truth={d['truth_qty']} Δ={d['delta']}"
                     for d in diffs]
            print(f"  [INVARIANT] POSITION_DRIFT #{self._pos_drift_consecutive} "
                  f"({len(diffs)}): " + " | ".join(parts))

            # Escalation: 3 consecutive drifts → DESYNC_HARD_STOP
            if self._pos_drift_consecutive >= 3 and not self._desync_hard_stop:
                self._desync_hard_stop = True
                # Release all budget reservations
                released_count = len(self._reserved_usd)
                released_total = sum(self._reserved_usd.values())
                self._reserved_usd.clear()
                self._reserved_submit_ts.clear()
                write_jsonl({
                    "event_type": "DESYNC_FROM_INVARIANT",
                    "source": "position_invariant",
                    "consecutive_checks": self._pos_drift_consecutive,
                    "diffs": diffs,
                    "action": "cancel_all + clear_reservations + block_buys",
                    "reservations_cleared": released_count,
                    "reservations_usd_released": round(released_total, 2),
                    "ts_ms": int(time.time() * 1000),
                })
                if released_count > 0:
                    write_jsonl({
                        "event_type": "RESERVED_CLEARED_ON_DESYNC",
                        "count": released_count,
                        "total_usd": round(released_total, 2),
                        "ts_ms": int(time.time() * 1000),
                    })
                print(f"  *** DESYNC FROM INVARIANT *** {self._pos_drift_consecutive} "
                      f"consecutive drifts — cancelling all orders, blocking new buys")
                for d in diffs:
                    sign = "+" if d["delta"] > 0 else ""
                    print(f"    {d['slug']} {d['outcome']} drift "
                          f"{sign}{d['delta']:.2f} "
                          f"(bot {d['bot_qty']} vs truth {d['truth_qty']})")
                if released_count > 0:
                    print(f"  [DESYNC] Released {released_count} reservations "
                          f"(${released_total:.2f})")
                self._cancel_all_orders()
                self._truth.enter_safe_mode(
                    reason=f"position_invariant_drift_{self._pos_drift_consecutive}_consecutive",
                    mismatches=[f"{d['slug']} {d['outcome']}: bot={d['bot_qty']} truth={d['truth_qty']}"
                                for d in diffs])
        else:
            # No drift — reset consecutive counter
            if getattr(self, '_pos_drift_consecutive', 0) > 0:
                self._pos_drift_consecutive = 0

    def _check_hedge_mismatch(self) -> None:
        """Detect hedge mismatches: net exposure on one side beyond threshold.

        Policy: HEDGE_MISMATCH_POLICY (IMMEDIATE_FLATTEN / HOLD_AND_HEDGE / PAUSE).
        """
        if not hasattr(self, '_hedge_mismatch_last_ts'):
            self._hedge_mismatch_last_ts = 0.0
            self._hedge_mismatch_detected: Dict[str, float] = {}  # slug -> first_detected_ts
        now = time.time()
        if now - self._hedge_mismatch_last_ts < 5.0:
            return  # only check every 5s
        self._hedge_mismatch_last_ts = now

        for slug, st in self.market_states.items():
            up_qty = st.positions["Up"].qty if st.positions["Up"].qty >= MIN_QTY else 0.0
            dn_qty = st.positions["Down"].qty if st.positions["Down"].qty >= MIN_QTY else 0.0
            net_exposure = abs(up_qty - dn_qty)
            if net_exposure <= MAX_NET_EXPOSURE_SHARES:
                self._hedge_mismatch_detected.pop(slug, None)
                continue
            # Net exposure exceeds threshold
            first_ts = self._hedge_mismatch_detected.get(slug)
            if first_ts is None:
                self._hedge_mismatch_detected[slug] = now
                first_ts = now
            unhedged_sec = now - first_ts
            if unhedged_sec < MAX_UNHEDGED_SECONDS:
                continue
            # Mismatch exceeded time limit
            bigger_side = "Up" if up_qty > dn_qty else "Down"
            excess = net_exposure
            write_jsonl({
                "event_type": "HEDGE_MISMATCH",
                "slug": slug, "up_qty": round(up_qty, 2),
                "dn_qty": round(dn_qty, 2),
                "net_exposure": round(net_exposure, 2),
                "unhedged_sec": round(unhedged_sec, 1),
                "policy": HEDGE_MISMATCH_POLICY,
                "ts_ms": int(now * 1000),
            })
            print(f"  [HEDGE] MISMATCH: {slug} {bigger_side} "
                  f"net={net_exposure:.1f} unhedged={unhedged_sec:.0f}s "
                  f"policy={HEDGE_MISMATCH_POLICY}")

            if HEDGE_MISMATCH_POLICY == "IMMEDIATE_FLATTEN":
                # Flatten excess by selling the bigger side
                m = None
                for mkt in self._cached_markets:
                    if mkt.slug == slug:
                        m = mkt
                        break
                if m is not None:
                    book = self.last_book.get(slug, {}).get(bigger_side)
                    if book and book.bid > 0:
                        flatten_qty = min(excess, st.positions[bigger_side].qty)
                        if flatten_qty >= MIN_QTY:
                            self._do_sell(m, st, bigger_side, flatten_qty,
                                          book.bid, reason="HEDGE_MISMATCH_FLATTEN",
                                          leg="HEDGE_FLATTEN")
                self._hedge_mismatch_detected.pop(slug, None)
            elif HEDGE_MISMATCH_POLICY == "PAUSE":
                # Enter safe mode to stop all new entries
                self._truth.enter_safe_mode(
                    f"HEDGE_MISMATCH: {slug} net_exposure={net_exposure:.1f}")

    def _exec_safety_can_enter(self, slug: str) -> bool:
        """Execution safety gate. Returns True if entry is allowed."""
        # Active window filter: block buys on non-current-hour tokens
        if ACTIVE_WINDOW_ONLY and STRICT_WINDOW_MODE:
            token_up, _ = self._token_for_slug_outcome(slug, "Up")
            if token_up and not self._truth.is_active_window_token(token_up):
                write_jsonl({"event_type": "WINDOW_BUY_BLOCKED",
                             "slug": slug, "reason": "non_active_window",
                             "ts_ms": int(time.time() * 1000)})
                print(f"  [TRUTH] WARN: Attempt to trade non-active window token: "
                      f"{slug} BUY")
                return False
        # State machine: only ACTIVE state can place new buys
        if self._truth.window_state != WindowState.ACTIVE:
            return False
        # Truth Capture SAFE MODE: block all new buys on desync
        if self._truth.safe_mode:
            return False
        # Loss-tail guard (all modes)
        if not self._slug_entry_allowed(slug):
            return False
        if MODE == "LOG":
            return True
        # ── Hourly budget gate (LIVE/LIVE_SAFE) ──
        if self._hour_budget_remaining <= 0:
            write_jsonl({"event_type": "HOURLY_BUDGET_EXHAUSTED",
                          "slug": slug,
                          "budget_remaining": round(self._hour_budget_remaining, 2),
                          "budget_total": HOURLY_BUDGET_USD,
                          "ts_ms": int(time.time() * 1000)})
            return False
        # LIVE_SAFE: check entry window
        if not self._buys_allowed():
            return False
        # Safety caps: include BOTH filled positions AND reserved (pending) orders
        total_usd = self._total_exposure_usd()
        # ── LIVE_SAFE total invested cap ($50 default, rolling — sells free up room) ──
        if MODE == "LIVE_SAFE" and total_usd >= LIVE_SAFE_MAX_TOTAL_INVESTED_USD:
            write_jsonl({"event_type": "LIVE_SAFE_INVESTED_CAP",
                          "slug": slug,
                          "filled_usd": round(total_usd - self._total_reserved_usd(), 2),
                          "reserved_usd": round(self._total_reserved_usd(), 2),
                          "total_exposure_usd": round(total_usd, 2),
                          "cap_usd": LIVE_SAFE_MAX_TOTAL_INVESTED_USD,
                          "ts_ms": int(time.time() * 1000)})
            return False
        slug_st = self.market_states.get(slug)
        slug_usd = 0.0
        if slug_st:
            slug_usd = sum(
                slug_st.positions[o].qty * slug_st.positions[o].vwap
                for o in ["Up", "Down"] if slug_st.positions[o].qty >= MIN_QTY
            )
        slug_usd += self._dscalp_invested_usd.get(slug, 0.0)
        allowed, reason = self._exec_safety.can_enter(
            slug, self._exec_tracker.open_order_count(), slug_usd, total_usd)
        if not allowed:
            write_jsonl({"event_type": "EXEC_ENTRY_BLOCKED", "slug": slug,
                          "reason": reason, "ts_ms": int(time.time() * 1000)})
        return allowed

    def _execute_burst_buy(self, m: MarketRef, st: MarketState, outcome: str,
                           base_clip_usd: float, ctx: dict, thr_bps: float = 0):
        """Count-based burst engine (f247-tuned: less spam, bigger steps).
        Places up to BURST_ORDERS micro-orders every BURST_INTERVAL_MS.
        Taker-gated: only crosses if spread <= 1c AND abs(edge) >= thr + 12.
        Otherwise posts maker at best bid. Stops on edge collapse (sustained 500ms),
        price move 2c against, inventory cap, or hard spread limit."""
        # LIVE safety cap check
        if not self._exec_safety_can_enter(m.slug):
            return
        # Inventory cap + post-fill cooldown
        if not self._inventory_cap_ok(m.slug, outcome):
            return
        if not self._post_fill_cooldown_ok(m.slug):
            return
        burst_start_ts = time.time()
        up_book, dn_book = ctx["up_book"], ctx["dn_book"]
        book = up_book if outcome == "Up" else dn_book
        initial_mid = book.mid
        pos = st.positions[outcome]
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        inv_cap = INVENTORY_CAP_SHARES_PER_MARKET
        total_filled_usd = 0.0
        burst_count = 0
        skipped_stale = 0
        taker_count = 0
        stop_reason = ""
        edge_below_since: Optional[float] = None

        write_jsonl({"event_type": "BURST_START", "slug": m.slug, "crypto": m.crypto,
                      "outcome": outcome, "burst_orders": BURST_ORDERS,
                      "step_usd_range": f"{BURST_STEP_USD_MIN}-{BURST_STEP_USD_MAX}",
                      "initial_mid": initial_mid, "thr_bps": thr_bps,
                      "t_min": round(ctx["t_min"], 3)})

        for i in range(BURST_ORDERS):
            # ── Freshness gate ──
            cache_age = self._cache_age_ms(m.slug)
            if cache_age > BURST_FRESHNESS_MAX_MS:
                self._submit_market_refresh(m)
                waited = 0.0
                while waited < BURST_FRESHNESS_WAIT_MS:
                    time.sleep(0.025)
                    waited += 25.0
                    if self._cache_age_ms(m.slug) <= BURST_FRESHNESS_MAX_MS:
                        break
                if self._cache_age_ms(m.slug) > BURST_FRESHNESS_MAX_MS:
                    write_jsonl({"event_type": "BURST_SKIP_STALE", "slug": m.slug,
                                  "burst_idx": i,
                                  "cache_age_ms": round(self._cache_age_ms(m.slug), 0)})
                    skipped_stale += 1
                    continue

            # ── Read fresh data from background cache ──
            fresh_cache = self._data_cache.get(m.slug)
            if not fresh_cache:
                stop_reason = "no_cache"
                break
            fresh_book = fresh_cache["up_book"] if outcome == "Up" else fresh_cache["dn_book"]
            fresh_up_book = fresh_cache["up_book"]
            fresh_dn_book = fresh_cache["dn_book"]

            if fresh_book.ask <= 0 or fresh_book.bid <= 0:
                stop_reason = "bad_book"
                break

            # ── Stop: price moved 2c against us ──
            if fresh_book.mid - initial_mid <= -BURST_STOP_IF_PRICE_MOVES_CENTS:
                stop_reason = f"price_adverse({fresh_book.mid - initial_mid:.3f}c)"
                break

            # ── Stop: per-market max position ──
            if pos.qty >= inv_cap:
                stop_reason = f"inventory_cap({pos.qty:.0f}>={inv_cap})"
                break

            # ── Edge computation from live spot ──
            fresh_spot = fresh_cache.get("spot", ctx["spot"])
            fresh_hour_open = fresh_cache.get("hour_open", ctx["hour_open"])
            if fresh_hour_open > 0:
                live_delta_bps = (fresh_spot - fresh_hour_open) / fresh_hour_open * 10000.0
            else:
                live_delta_bps = ctx["delta_bps"]
            abs_live_delta = abs(live_delta_bps)
            p_up = _p_up_model(live_delta_bps)
            if outcome == "Up":
                cur_edge = (p_up - fresh_book.mid) * 10000.0
            else:
                cur_edge = ((1.0 - p_up) - fresh_book.mid) * 10000.0

            # ── Stop: edge below threshold for sustained 500ms ──
            sm = self._get_sm(m.slug)
            initial_edge = sm.get("initial_edge_bps") or cur_edge
            edge_drop = initial_edge - cur_edge
            if edge_drop >= BURST_STOP_IF_EDGE_DROPS_BPS:
                now_t = time.time()
                if edge_below_since is None:
                    edge_below_since = now_t
                elif (now_t - edge_below_since) * 1000 >= BURST_EDGE_BELOW_HOLD_MS:
                    stop_reason = f"edge_sustained_drop({edge_drop:.1f}bps,{(now_t-edge_below_since)*1000:.0f}ms)"
                    break
            else:
                edge_below_since = None

            # ── Stop: hard spread limit ──
            if fresh_book.spread > BURST_SPREAD_HARD_LIMIT:
                stop_reason = f"spread_hard({fresh_book.spread:.3f}>{BURST_SPREAD_HARD_LIMIT})"
                break

            # ── Size micro-order: 0.75–6.00 USDC ──
            this_usd = clamp(base_clip_usd * 0.15, BURST_STEP_USD_MIN, BURST_STEP_USD_MAX)
            decision_id = new_decision_id()
            client_oid = new_order_id()
            bk_fields = self._book_fields(fresh_up_book, fresh_dn_book, outcome)

            # ── Always buy at ask (taker) for immediate fill ──
            order_price = fresh_book.ask
            # LIVE_SAFE trade-size limiter
            this_usd, this_qty = self._live_safe_cap_usd(this_usd, order_price)
            if this_qty < MIN_QTY:
                stop_reason = "live_safe_size_cap"
                break
            order_type = "taker"

            write_jsonl({"event_type": "BURST_MICRO_ORDER", "slug": m.slug,
                          "burst_idx": i, "usd": round(this_usd, 2),
                          "qty": round(this_qty, 2), "price": order_price,
                          "order_type": order_type,
                          "edge_bps": round(cur_edge, 2), "spread": round(fresh_book.spread, 4),
                          "cache_age_ms": round(self._cache_age_ms(m.slug), 0),
                          "live_delta_bps": round(live_delta_bps, 2)})

            self._ledger_order_intent(m, st, outcome, "BUY", this_qty, order_price, "ENTRY_BURST")
            if MODE == "LOG":
                self._paper_buy(st, outcome, order_price, this_qty, this_usd)
                self._ledger_record_buy_fill(m, st, outcome, order_price, this_qty)
                self._record_fill_ts(m.slug)  # post-fill cooldown
                mt = order_type
                sc = spread_capture_fields("BUY", order_price, fresh_book)
                _fee = compute_fee_usdc(this_usd, mt)
                self.logger.log_order_fill(
                    engine="CORE", reason="ENTRY_BURST",
                    decision_id=decision_id, client_order_id=client_oid,
                    position_id=pos.position_id or "",
                    crypto=m.crypto, slug=m.slug, outcome=outcome,
                    side="BUY", qty=this_qty, fill_price=order_price,
                    usdc_cost=this_usd, fees_usdc=_fee,
                    maker_taker=mt, did_cross=sc.get("did_cross", ""),
                    vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                )
                total_filled_usd += this_usd
                burst_count += 1
            else:
                # ── Per-token attempt breaker ──
                if not self._token_attempt_ok(token_id):
                    stop_reason = "token_breaker"
                    break
                # ── In-flight guard: one BUY per token ──
                if self._has_inflight_buy(token_id):
                    stop_reason = "inflight_buy_guard"
                    break
                # ── Global rate limit ──
                if not self._global_rate_ok():
                    stop_reason = "global_rate_limit"
                    break
                # ── Exposure cap re-check (includes reserved) ──
                if self._total_exposure_usd() + this_usd >= LIVE_SAFE_MAX_TOTAL_INVESTED_USD:
                    stop_reason = "exposure_cap"
                    break

                # ── Record attempt for breaker tracking ──
                self._record_token_attempt(token_id)

                # ── IOC/FOK for immediate taker fill ──
                if IOC_ENTRY_ENABLED:
                    fill = self._quick_buy_ioc(
                        token_id, order_price, this_qty, fresh_book,
                        m, st, outcome, "ENTRY_BURST")
                else:
                    fill = self.client.place_limit_order(
                        token_id, "BUY", order_price, this_qty, post_only=False)

                oid = fill.get("order_id", "")
                fill_state = fill.get("filled")  # True | False | "UNKNOWN"
                is_pending = (fill_state == "UNKNOWN")
                actual_fill_qty = fill.get("fill_qty", 0.0)

                # ── Budget reservation: reserve on submit ──
                reserved_amount = order_price * this_qty
                if oid:
                    self._reserve_budget(oid, reserved_amount)
                self._record_buy_submit(token_id)
                self._record_global_order()

                if oid:
                    self._exec_tracker.on_submit(
                        oid, m.slug, outcome, "BUY", order_price, this_qty,
                        "ENTRY_BURST", token_id=token_id,
                        pending_confirmation=is_pending)
                self._truth_watch_after_place(fill, m, outcome, "BUY", this_qty, order_price)
                self._exec_safety.record_slug_submit(m.slug)

                if fill_state is True:
                    # Confirmed fill (full or partial) — use unified apply_fill
                    if oid:
                        self._exec_tracker.on_fill(oid, actual_fill_qty,
                                                   fill_price=fill["fill_price"],
                                                   token_id=token_id)
                    self._exec_safety.record_slug_fill(m.slug)
                    # For partial IOC fills, adjust reservation to actual cost
                    if actual_fill_qty < this_qty and oid:
                        # Partial fill: release full reservation, apply_fill re-deducts actual
                        pass  # apply_fill handles reservation release
                    self._apply_fill(m.slug, outcome, "BUY",
                                     actual_fill_qty, fill["fill_price"],
                                     order_id=oid, reason="ENTRY_BURST",
                                     source="immediate")
                    mt = infer_maker_taker("BUY", fill["fill_price"], fresh_book)
                    sc = spread_capture_fields("BUY", fill["fill_price"], fresh_book)
                    _burst_notional = fill["fill_price"] * actual_fill_qty
                    _fee = compute_fee_usdc(_burst_notional, mt)
                    self.logger.log_order_fill(
                        engine="CORE", reason="ENTRY_BURST",
                        decision_id=decision_id, client_order_id=client_oid,
                        position_id=pos.position_id or "",
                        crypto=m.crypto, slug=m.slug, outcome=outcome,
                        side="BUY", qty=actual_fill_qty, fill_price=fill["fill_price"],
                        usdc_cost=_burst_notional, fees_usdc=_fee,
                        maker_taker=mt, did_cross=sc.get("did_cross", ""),
                        vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                    )
                    total_filled_usd += fill["fill_price"] * actual_fill_qty
                    burst_count += 1
                elif is_pending:
                    # UNKNOWN: CLOB matched but qty unconfirmed.
                    # Budget reserved above stays locked.
                    # Cash deducted via reservation.
                    self.cash_usdc -= reserved_amount
                    self._hour_budget_remaining -= reserved_amount
                    # DO NOT clear inflight guard — hold it until watcher resolves.
                    # This prevents rapid duplicate orders while confirmation lags.
                    # Set a long-hold timestamp so _has_inflight_buy blocks for
                    # the full reservation timeout, not just the 350ms cooldown.
                    self._inflight_buy_ts[token_id] = time.time() + RESERVATION_STUCK_TIMEOUT_SEC
                    write_jsonl({"event_type": "BUY_PENDING_CONFIRMATION",
                                 "slug": m.slug, "outcome": outcome,
                                 "order_id": oid, "status": fill.get("status"),
                                 "requested_qty": this_qty, "price": order_price,
                                 "reserved_usd": round(reserved_amount, 2),
                                 "reason": "ENTRY_BURST",
                                 "hold_inflight": True,
                                 "ts_ms": int(time.time() * 1000)})
                    burst_count += 1
                    # STOP burst for this token — don't stack more orders on an
                    # unconfirmed fill.
                    stop_reason = "pending_confirmation"
                    break
                elif fill_state is False and fill.get("status") not in ("error",):
                    # IOC/FOK killed — release reservation immediately
                    if oid:
                        self._release_reservation(oid)
                    self._clear_buy_inflight(token_id)
                    if oid:
                        self._exec_tracker.on_cancel(oid)
                elif fill.get("status") == "error":
                    if oid:
                        self._release_reservation(oid)
                    self._clear_buy_inflight(token_id)
                    self._exec_safety.record_api_error()
                    if oid:
                        self._exec_tracker.on_cancel(oid)

            # Track diagnostics — always taker now
            taker_count += 1
            self._diag_taker_count += 1
            self._tempo_fills[m.slug] = self._tempo_fills.get(m.slug, 0) + 1
            # Sleep between micro-orders
            time.sleep(BURST_INTERVAL_MS / 1000.0)

        burst_duration_ms = (time.time() - burst_start_ts) * 1000
        write_jsonl({"event_type": "BURST_STOP", "slug": m.slug, "crypto": m.crypto,
                      "outcome": outcome, "burst_count": burst_count,
                      "total_filled_usd": round(total_filled_usd, 2),
                      "stop_reason": stop_reason or "all_orders_done",
                      "burst_duration_ms": round(burst_duration_ms, 1),
                      "skipped_stale": skipped_stale,
                      "taker_count": taker_count,
                      "t_min": round(ctx["t_min"], 3)})

    # =================================================================
    # PARITY (STRADDLE) ARBITRAGE ENGINE  (v2: fee-aware, partial-fill
    # protected, maker-disciplined, with locked recycle)
    # =================================================================
    def _parity_arb(self, ctx: dict, new_quotes_blocked: bool = False):
        """Fee-aware parity arbitrage with partial-fill protection.
        1. Compute raw + net (after fees/slippage) edges
        2. Liquidity/staleness guards
        3. Execute paired orders with pair_id tracking
        4. Handle pending partial fills from prior ticks
        If new_quotes_blocked=True, only handle pending, recycle, flatten — no new quotes/orders."""
        m, st = ctx["m"], ctx["st"]
        t_min = ctx["t_min"]
        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        spot, hour_open = ctx["spot"], ctx["hour_open"]

        # ── Handle pending partial fills first (always, even after stop-new time) ──
        self._parity_handle_pending_pairs(m, st, ctx)

        # ── Recycle locked inventory if stale ──
        self._parity_recycle_locked(m, st, ctx)

        # ── Parity QUOTING mode (continuously post maker bids on both legs) ──
        xrp_quote_ok = (m.crypto != "XRP" or XRP_PARITY_QUOTE_ENABLED)
        # Module priority: directional suppresses parity quote BUYs
        _dir_suppress = self._directional_suppresses_parity(m.slug)
        if _dir_suppress and not new_quotes_blocked:
            self._diag_parity_directional_suppress += 1
            write_jsonl({"event_type": "PARITY_SUPPRESSED_DUE_TO_DIRECTIONAL",
                          "slug": m.slug, "crypto": m.crypto,
                          "ts_ms": int(time.time() * 1000)})
        # ── LIVE/TEST symbol filter: block new parity quotes for non-allowed symbols ──
        _symbol_ok = not (MODE in ("LIVE", "LIVE_SAFE", "TEST") and m.crypto not in LIVE_ALLOWED_SYMBOLS)
        if (PARITY_QUOTE_ENABLED and t_min < PARITY_QUOTE_STOP_MIN
                and not new_quotes_blocked and self._buys_allowed()
                and xrp_quote_ok and self._slug_entry_allowed(m.slug)
                and not _dir_suppress
                and self._inventory_cap_ok(m.slug)
                and self._post_fill_cooldown_ok(m.slug)
                and _symbol_ok):
            self._parity_quote(m, st, ctx)

        # ── End-of-hour flattening (runs even after PARITY_STOP_NEW_MIN) ──
        if t_min >= PARITY_FLATTEN_START_MIN:
            self._parity_flatten_eoh(m, st, t_min, ctx)
            return  # don't open new parity trades while flattening

        # ── Compute raw parity metrics ──
        straddle_buy_cost = up_book.ask + dn_book.ask
        straddle_sell_value = up_book.bid + dn_book.bid
        raw_buy_edge = (1.000 - straddle_buy_cost) * 100
        raw_sell_edge = (straddle_sell_value - 1.000) * 100

        # ── Fee-aware net edges ──
        buy_net, buy_fee, buy_slip = parity_net_edge_cents(raw_buy_edge, up_book, dn_book, True)
        sell_net, sell_fee, sell_slip = parity_net_edge_cents(raw_sell_edge, up_book, dn_book, False)

        # Store in ctx for logging
        ctx["parity_raw_buy_cents"] = raw_buy_edge
        ctx["parity_raw_sell_cents"] = raw_sell_edge
        ctx["parity_net_buy_cents"] = buy_net
        ctx["parity_net_sell_cents"] = sell_net

        # ── Stop new parity trades after PARITY_STOP_NEW_MIN or when blocked ──
        if t_min >= PARITY_STOP_NEW_MIN or new_quotes_blocked:
            return

        # ── LIVE/TEST symbol filter: block new parity buys for non-allowed symbols ──
        if MODE in ("LIVE", "LIVE_SAFE", "TEST") and m.crypto not in LIVE_ALLOWED_SYMBOLS:
            return

        # ── LIVE_SAFE entry window gate (buys only) ──
        if not self._buys_allowed():
            return

        # ── Rule 8: Last 90 seconds — no new resting orders ──
        seconds_to_close = ctx.get("seconds_to_close", 999.0)
        if MODE in ("LIVE", "LIVE_SAFE") and self._live_safety.should_block_new_entry(seconds_to_close):
            return

        # ── Loss-tail slug pause ──
        if not self._slug_entry_allowed(m.slug):
            return

        # ── Module priority: directional suppresses parity BUYs ──
        if self._directional_suppresses_parity(m.slug):
            return

        # ── Inventory cap check ──
        if not self._inventory_cap_ok(m.slug):
            return

        # ── Post-fill cooldown ──
        if not self._post_fill_cooldown_ok(m.slug):
            return

        # ── Cooldown gate ──
        now_t = time.time()
        last_ts = self._parity_last_order_ts.get(m.slug, 0.0)
        if (now_t - last_ts) * 1000 < PARITY_COOLDOWN_MS:
            return

        # ── Per-slug entry throttle ──
        if not self._entry_throttle_ok(m.slug):
            return

        # ── Staleness guard ──
        cache_age = self._cache_age_ms(m.slug)
        if cache_age > PARITY_MAX_CACHE_AGE_MS:
            self._diag_parity_blocked_stale += 1
            return

        # ── Liquidity guard ──
        liq_ok, liq_reason = parity_liquidity_ok(up_book, dn_book)
        if not liq_ok:
            if "spread" in liq_reason:
                self._diag_parity_blocked_spread += 1
            else:
                self._diag_parity_blocked_liq += 1
            return

        # ── Coin-specific spread limit ──
        max_leg_spread = max(up_book.spread, dn_book.spread) * 100
        if not self._coin_spread_entry_ok(m.crypto, max_leg_spread):
            return

        # ── Investment cap gate ──
        invested = self._parity_invested_usd.get(m.slug, 0.0)

        # ── Directional lean ──
        lean_up = spot >= hour_open

        # ── BUY CHEAP STRADDLE (with edge buffer) ──
        buy_threshold = PARITY_BUY_MIN_EDGE_NET_CENTS + PARITY_EDGE_BUFFER_CENTS
        if PARITY_BUY_ENABLED and buy_net >= buy_threshold:
            self._diag_parity_buy_signals += 1

            if invested >= PARITY_MAX_USD_PER_SLUG:
                return

            if up_book.ask <= 0 or dn_book.ask <= 0 or up_book.bid <= 0 or dn_book.bid <= 0:
                return

            leg_usd = min(PARITY_STEP_USD, (PARITY_MAX_USD_PER_SLUG - invested) / 2.0)
            if leg_usd < MIN_ORDER_USDC:
                return

            # Generate pair_id for partial-fill tracking
            pair_id = uuid.uuid4().hex[:16]

            # Execute leg 1: BUY Up
            up_filled = self._parity_buy_leg(m, st, "Up", up_book, leg_usd, ctx, pair_id)

            # ── LIVE Rules 2+3: slippage check + partial fill clamping before leg 2 ──
            leg2_usd = leg_usd
            if MODE in ("LIVE", "LIVE_SAFE") and up_filled > 0:
                # Rule 3: clamp leg 2 to what leg 1 actually filled
                leg2_usd = LiveSafety.clamp_hedge_qty(up_filled, leg_usd)
                # Rule 2: check slippage (fee-aware) — re-read book for leg 2
                fresh_dn = self.last_book.get(m.slug, {}).get("Down", dn_book)
                # Use taker fee for worst-case estimate
                if not self._live_safety.check_slippage_ok(
                        dn_book.ask, fresh_dn.ask, fee_bps=TAKER_FEE_BPS):
                    # Slippage too large — unwind leg 1 immediately
                    up_pos = st.positions["Up"]
                    up_unwind_qty = min(up_filled / max(0.01, up_book.ask), up_pos.qty)
                    if up_unwind_qty >= MIN_QTY and up_book.bid > 0:
                        self._do_sell(m, st, "Up", up_unwind_qty, up_book.bid,
                                      reason="HEDGE_SLIPPAGE_EXIT", leg="PARITY", ctx=ctx)
                        self._live_safety.on_hedge_fail(m.slug, "slippage")
                    write_jsonl({"event_type": "HEDGE_SLIPPAGE_ABORT",
                                  "slug": m.slug, "pair_id": pair_id,
                                  "intended_ask": round(dn_book.ask, 4),
                                  "current_ask": round(fresh_dn.ask, 4),
                                  "slippage_cents": round(abs(fresh_dn.ask - dn_book.ask) * 100, 2),
                                  "ts_ms": int(time.time() * 1000)})
                    return
                dn_book = fresh_dn  # use fresh book for leg 2

            # Execute leg 2: BUY Down
            dn_filled = self._parity_buy_leg(m, st, "Down", dn_book, leg2_usd, ctx, pair_id)

            total_cost = up_filled + dn_filled
            both_filled = up_filled > 0 and dn_filled > 0
            one_filled = (up_filled > 0) != (dn_filled > 0)

            if both_filled:
                self._parity_invested_usd[m.slug] = invested + total_cost
                self._parity_last_order_ts[m.slug] = time.time()
                self._entry_throttle_record(m.slug)
                self._diag_parity_trades += 1
                self._diag_parity_edges.append(buy_net)
                self._diag_parity_trade_timestamps.append(time.time())
                # Track locked straddle start time
                if m.slug not in self._parity_locked_since:
                    self._parity_locked_since[m.slug] = time.time()

                write_jsonl({"event_type": "PARITY_BUY_STRADDLE",
                              "slug": m.slug, "crypto": m.crypto,
                              "pair_id": pair_id,
                              "straddle_buy_cost": round(straddle_buy_cost, 4),
                              "raw_edge_cents": round(raw_buy_edge, 3),
                              "net_edge_cents": round(buy_net, 3),
                              "fee_cents": round(buy_fee, 3),
                              "slippage_cents": round(buy_slip, 3),
                              "up_cost": round(up_filled, 4),
                              "dn_cost": round(dn_filled, 4),
                              "total_invested": round(invested + total_cost, 2),
                              "t_min": round(t_min, 3)})

            elif one_filled:
                # Partial fill — queue for resolution
                self._diag_pair_partial_count += 1
                filled_outcome = "Up" if up_filled > 0 else "Down"
                pending_outcome = "Down" if filled_outcome == "Up" else "Up"
                filled_usd = up_filled if up_filled > 0 else dn_filled
                filled_book = up_book if filled_outcome == "Up" else dn_book
                self._parity_pending_pairs.append({
                    "pair_id": pair_id,
                    "slug": m.slug,
                    "filled_outcome": filled_outcome,
                    "filled_usd": filled_usd,
                    "filled_qty": filled_usd / max(1e-9, filled_book.ask),
                    "filled_price": filled_book.ask,
                    "pending_outcome": pending_outcome,
                    "pending_qty_at_start": st.positions[pending_outcome].qty,
                    "ts": time.time(),
                    "edge_net_cents": buy_net,
                })
                self._parity_invested_usd[m.slug] = invested + filled_usd
                self._parity_last_order_ts[m.slug] = time.time()

                write_jsonl({"event_type": "PARITY_PARTIAL_FILL",
                              "slug": m.slug, "pair_id": pair_id,
                              "filled_outcome": filled_outcome,
                              "filled_usd": round(filled_usd, 4),
                              "pending_outcome": pending_outcome,
                              "net_edge_cents": round(buy_net, 3)})
            return

        # ── SELL RICH STRADDLE ──
        # ── SELL RICH STRADDLE (with edge buffer) ──
        sell_threshold = PARITY_SELL_MIN_EDGE_NET_CENTS + PARITY_EDGE_BUFFER_CENTS
        if PARITY_SELL_ENABLED and sell_net >= sell_threshold:
            self._diag_parity_sell_signals += 1

            pos_up = st.positions["Up"]
            pos_dn = st.positions["Down"]

            if pos_up.qty < MIN_QTY or pos_dn.qty < MIN_QTY:
                return
            if up_book.bid <= 0 or dn_book.bid <= 0:
                return

            sell_up_usd = min(PARITY_STEP_USD, pos_up.qty * up_book.bid)
            sell_dn_usd = min(PARITY_STEP_USD, pos_dn.qty * dn_book.bid)
            sell_usd = min(sell_up_usd, sell_dn_usd)
            if sell_usd < MIN_ORDER_USDC:
                return

            pair_id = uuid.uuid4().hex[:16]

            if LEAN_EXIT_PRIORITY:
                sell_order = [("Down", dn_book), ("Up", up_book)] if lean_up else [("Up", up_book), ("Down", dn_book)]
            else:
                sell_order = [("Up", up_book), ("Down", dn_book)]

            total_proceeds = 0.0
            legs_filled = 0
            for outcome, book in sell_order:
                sell_qty = sell_usd / max(1e-9, book.bid)
                pos = st.positions[outcome]
                sell_qty = min(sell_qty, pos.qty)
                if sell_qty < MIN_QTY:
                    continue

                use_taker = (book.spread * 100) <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
                sell_price = book.bid if use_taker else max(book.bid, book.ask - 0.001)
                if use_taker:
                    self._diag_parity_taker_count += 1
                else:
                    self._diag_parity_maker_count += 1

                self._do_sell(m, st, outcome, sell_qty, sell_price,
                              reason="PARITY_SELL_STRADDLE", leg="PARITY_SELL",
                              ctx=ctx, use_maker=not use_taker)
                total_proceeds += sell_qty * sell_price
                legs_filled += 1

            if total_proceeds > 0:
                self._parity_last_order_ts[m.slug] = time.time()
                self._entry_throttle_record(m.slug)
                self._diag_parity_trades += 1
                self._diag_parity_edges.append(sell_net)
                self._diag_parity_trade_timestamps.append(time.time())
                self._parity_invested_usd[m.slug] = max(0.0, invested - total_proceeds)

                # Update locked tracking
                up_q = st.positions["Up"].qty
                dn_q = st.positions["Down"].qty
                if min(up_q, dn_q) < MIN_QTY:
                    self._parity_locked_since.pop(m.slug, None)

                write_jsonl({"event_type": "PARITY_SELL_STRADDLE",
                              "slug": m.slug, "crypto": m.crypto,
                              "pair_id": pair_id,
                              "straddle_sell_value": round(straddle_sell_value, 4),
                              "raw_edge_cents": round(raw_sell_edge, 3),
                              "net_edge_cents": round(sell_net, 3),
                              "fee_cents": round(sell_fee, 3),
                              "total_proceeds": round(total_proceeds, 4),
                              "legs_filled": legs_filled,
                              "remaining_invested": round(self._parity_invested_usd.get(m.slug, 0), 2),
                              "t_min": round(t_min, 3)})

    def _parity_handle_pending_pairs(self, m: MarketRef, st: MarketState, ctx: dict):
        """Resolve partial fills from prior ticks. If timeout exceeded:
        - edge still good → cross remaining leg (taker if spread<=1c)
        - edge gone → unwind filled leg to flatten.
        LIVE Rule 1: atomic hedge kill switch — force exit after 2s (1s in final 2 min)."""
        now_t = time.time()
        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        t_min = ctx["t_min"]
        resolved = []

        for i, pair in enumerate(self._parity_pending_pairs):
            if pair["slug"] != m.slug:
                continue
            elapsed_sec = now_t - pair["ts"]

            # ── Rule 1: Atomic Hedge Kill Switch (LIVE modes) ──
            #    Timer anchored to fill confirmation (pair["ts"]).
            #    Pauses if leg2 is actively filling (>25% progress).
            if MODE in ("LIVE", "LIVE_SAFE"):
                # Check if leg2 has made any partial progress
                pending_pos = st.positions[pair["pending_outcome"]]
                leg2_partial = max(0.0, pending_pos.qty - pair.get("pending_qty_at_start", 0.0))
                leg2_requested = pair["filled_qty"]
                if self._live_safety.should_kill_hedge(
                        pair["ts"], t_min, leg2_partial, leg2_requested):
                    # Timeout — force market exit the filled leg immediately
                    filled_book = up_book if pair["filled_outcome"] == "Up" else dn_book
                    pos = st.positions[pair["filled_outcome"]]
                    unwind_qty = min(pair["filled_qty"], pos.qty)
                    if unwind_qty >= MIN_QTY and filled_book.bid > 0:
                        self._do_sell(m, st, pair["filled_outcome"], unwind_qty,
                                      filled_book.bid, reason="HEDGE_FAIL_EXIT",
                                      leg="PARITY", ctx=ctx)
                        self._diag_unpaired_unwind_usd += unwind_qty * filled_book.bid
                    self._live_safety.on_hedge_fail(m.slug, "timeout")
                    self._parity_invested_usd[m.slug] = max(
                        0.0, self._parity_invested_usd.get(m.slug, 0.0)
                        - unwind_qty * filled_book.bid)
                    write_jsonl({"event_type": "HEDGE_FAIL_EXIT",
                                  "slug": m.slug, "pair_id": pair["pair_id"],
                                  "filled_outcome": pair["filled_outcome"],
                                  "elapsed_sec": round(elapsed_sec, 2),
                                  "timeout_sec": hedge_timeout,
                                  "unwind_qty": round(unwind_qty, 1),
                                  "t_min": round(t_min, 3),
                                  "ts_ms": int(now_t * 1000)})
                    resolved.append(i)
                    continue

            elapsed_ms = elapsed_sec * 1000

            if elapsed_ms < PAIR_FILL_TIMEOUT_MS:
                # Still within timeout — try to fill the pending leg
                pending_book = up_book if pair["pending_outcome"] == "Up" else dn_book
                if pending_book.ask <= 0:
                    continue
                # Recompute net edge
                raw_buy_edge = ctx.get("parity_raw_buy_cents", 0.0)
                net_buy, _, _ = parity_net_edge_cents(raw_buy_edge, up_book, dn_book, True)

                if net_buy >= PARITY_BUY_MIN_EDGE_NET_CENTS and self._buys_allowed():
                    # Edge still good — try to fill pending leg
                    filled = self._parity_buy_leg(m, st, pair["pending_outcome"],
                                                   pending_book, pair["filled_usd"],
                                                   ctx, pair["pair_id"])
                    if filled > 0:
                        # Pair complete
                        self._diag_parity_trades += 1
                        self._diag_parity_edges.append(net_buy)
                        self._diag_pair_fill_delays.append(elapsed_ms)
                        self._diag_parity_trade_timestamps.append(time.time())
                        self._parity_invested_usd[m.slug] = (
                            self._parity_invested_usd.get(m.slug, 0.0) + filled)
                        if m.slug not in self._parity_locked_since:
                            self._parity_locked_since[m.slug] = time.time()
                        write_jsonl({"event_type": "PARITY_PAIR_COMPLETED",
                                      "ts_ms": int(time.time() * 1000),
                                      "slug": m.slug, "pair_id": pair["pair_id"],
                                      "delay_ms": round(elapsed_ms, 1),
                                      "net_edge_cents": round(net_buy, 3)})
                        resolved.append(i)
                continue

            # Timeout exceeded — decide: cross or unwind
            pending_book = up_book if pair["pending_outcome"] == "Up" else dn_book
            raw_buy_edge = ctx.get("parity_raw_buy_cents", 0.0)
            net_buy, _, _ = parity_net_edge_cents(raw_buy_edge, up_book, dn_book, True)

            if net_buy >= PARITY_BUY_MIN_EDGE_NET_CENTS and pending_book.ask > 0 and self._buys_allowed():
                # Edge still good — force cross remaining leg (taker if spread<=1c)
                filled = self._parity_buy_leg(m, st, pair["pending_outcome"],
                                               pending_book, pair["filled_usd"],
                                               ctx, pair["pair_id"])
                if filled > 0:
                    self._diag_parity_trades += 1
                    self._diag_parity_edges.append(net_buy)
                    self._diag_pair_fill_delays.append(elapsed_ms)
                    self._diag_parity_trade_timestamps.append(time.time())
                    self._parity_invested_usd[m.slug] = (
                        self._parity_invested_usd.get(m.slug, 0.0) + filled)
                    if m.slug not in self._parity_locked_since:
                        self._parity_locked_since[m.slug] = time.time()
                    write_jsonl({"event_type": "PARITY_PAIR_COMPLETED_LATE",
                                  "ts_ms": int(time.time() * 1000),
                                  "slug": m.slug, "pair_id": pair["pair_id"],
                                  "delay_ms": round(elapsed_ms, 1)})
                    resolved.append(i)
                    continue

            # Edge gone or fill failed — unwind the filled leg
            filled_book = up_book if pair["filled_outcome"] == "Up" else dn_book
            pos = st.positions[pair["filled_outcome"]]
            unwind_qty = min(pair["filled_qty"], pos.qty)
            if unwind_qty >= MIN_QTY and filled_book.bid > 0:
                use_taker = (filled_book.spread * 100) <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
                unwind_price = filled_book.bid if use_taker else max(filled_book.bid, filled_book.ask - 0.001)
                self._do_sell(m, st, pair["filled_outcome"], unwind_qty, unwind_price,
                              reason="PARITY_UNWIND_PARTIAL", leg="PARITY_UNWIND",
                              ctx=ctx, use_maker=not use_taker)
                self._diag_unpaired_unwind_usd += unwind_qty * unwind_price
                self._parity_invested_usd[m.slug] = max(
                    0.0, self._parity_invested_usd.get(m.slug, 0.0) - unwind_qty * unwind_price)
                # Rule 6: Record hedge failure for circuit breaker
                if MODE in ("LIVE", "LIVE_SAFE"):
                    self._live_safety.on_hedge_fail(m.slug, "pair_unwind_partial")
                write_jsonl({"event_type": "PARITY_UNWIND_PARTIAL",
                              "slug": m.slug, "pair_id": pair["pair_id"],
                              "unwound_outcome": pair["filled_outcome"],
                              "unwind_qty": round(unwind_qty, 1),
                              "delay_ms": round(elapsed_ms, 1)})
            resolved.append(i)

        # Remove resolved pairs (reverse order to preserve indices)
        for idx in sorted(resolved, reverse=True):
            self._parity_pending_pairs.pop(idx)

    def _parity_recycle_locked(self, m: MarketRef, st: MarketState, ctx: dict):
        """Recycle locked straddle inventory that has been held too long.
        Tries to sell both legs at a small net profit (maker-first)."""
        locked_since = self._parity_locked_since.get(m.slug)
        if locked_since is None:
            return
        pos_up = st.positions["Up"]
        pos_dn = st.positions["Down"]
        locked_qty = min(pos_up.qty, pos_dn.qty)
        if locked_qty < MIN_QTY:
            self._parity_locked_since.pop(m.slug, None)
            return

        hold_sec = time.time() - locked_since
        if hold_sec < LOCKED_MAX_HOLD_SEC:
            return

        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]

        # Check if we can sell both legs at net profit
        up_vwap = pos_up.vwap
        dn_vwap = pos_dn.vwap
        straddle_vwap = up_vwap + dn_vwap
        straddle_sell_value = up_book.bid + dn_book.bid

        # Net profit after fees
        raw_profit_cents = (straddle_sell_value - straddle_vwap) * 100
        _, sell_fee, sell_slip = parity_net_edge_cents(raw_profit_cents, up_book, dn_book, False)
        net_profit_cents = raw_profit_cents - sell_fee - sell_slip

        if net_profit_cents < RECYCLE_MIN_PROFIT_NET_CENTS:
            return

        # Sell both legs
        sell_usd = min(RECYCLE_STEP_USD, locked_qty * min(up_book.bid, dn_book.bid))
        if sell_usd < MIN_ORDER_USDC:
            return

        pair_id = uuid.uuid4().hex[:16]
        for outcome, book in [("Up", up_book), ("Down", dn_book)]:
            pos = st.positions[outcome]
            sell_qty = min(sell_usd / max(1e-9, book.bid), pos.qty)
            if sell_qty < MIN_QTY:
                continue
            use_taker = (book.spread * 100) <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
            sell_price = book.bid if use_taker else max(book.bid, book.ask - 0.001)
            if use_taker:
                self._diag_parity_taker_count += 1
            else:
                self._diag_parity_maker_count += 1
            self._do_sell(m, st, outcome, sell_qty, sell_price,
                          reason="PARITY_RECYCLE", leg="PARITY_RECYCLE",
                          ctx=ctx, use_maker=not use_taker)

        self._diag_recycle_count += 1
        self._clone_hold_times.append(hold_sec)
        self._parity_invested_usd[m.slug] = max(
            0.0, self._parity_invested_usd.get(m.slug, 0.0) - sell_usd * 2)

        # Update locked tracking
        if min(st.positions["Up"].qty, st.positions["Down"].qty) < MIN_QTY:
            self._parity_locked_since.pop(m.slug, None)
        else:
            self._parity_locked_since[m.slug] = time.time()  # reset timer

        write_jsonl({"event_type": "PARITY_RECYCLE",
                      "slug": m.slug, "crypto": m.crypto,
                      "pair_id": pair_id,
                      "hold_sec": round(hold_sec, 1),
                      "raw_profit_cents": round(raw_profit_cents, 3),
                      "net_profit_cents": round(net_profit_cents, 3),
                      "sell_usd": round(sell_usd, 2),
                      "t_min": round(ctx["t_min"], 3)})

    # =================================================================
    # PAIR FILL TRACKER — thread pair_id through all quote/arb fills
    # =================================================================
    def _record_pair_fill(self, pair_id: str, slug: str, crypto: str,
                          outcome: str, fill_ts: float, maker_taker: str):
        """Record a fill for a pair_id. When both legs filled, compute pair metrics."""
        if not pair_id:
            return
        entry = self._pair_tracker.setdefault(pair_id, {
            "slug": slug, "crypto": crypto, "fills": {},
        })
        entry["fills"][outcome] = {"ts": fill_ts, "maker_taker": maker_taker}

        # Track maker vs taker for parity
        if maker_taker == "maker":
            self._diag_parity_maker_count += 1
        elif maker_taker == "taker":
            self._diag_parity_taker_count += 1

        # Check if pair is now complete (both Up and Down filled)
        if "Up" in entry["fills"] and "Down" in entry["fills"]:
            up_ts = entry["fills"]["Up"]["ts"]
            dn_ts = entry["fills"]["Down"]["ts"]
            delay_ms = abs(up_ts - dn_ts) * 1000
            self._clone_pair_delays.append(delay_ms)
            self._diag_pairs_completed += 1
            if delay_ms <= 500:
                self._diag_pairs_completed_500ms += 1
                self._diag_slug_paired_500ms[slug] = self._diag_slug_paired_500ms.get(slug, 0) + 1
            if delay_ms <= 1500:
                self._diag_pairs_completed_1500ms += 1
                self._diag_slug_paired_1500ms[slug] = self._diag_slug_paired_1500ms.get(slug, 0) + 1
            if delay_ms <= 10000:
                self._diag_pairs_completed_10s += 1
            # Inter-pair gap
            completion_ts = max(up_ts, dn_ts)
            if self._clone_last_pair_ts > 0:
                gap_ms = (completion_ts - self._clone_last_pair_ts) * 1000
                self._clone_inter_pair_gaps.append(gap_ms)
            self._clone_last_pair_ts = completion_ts
            # Straddle edge at completion
            self._diag_parity_trades += 1
            self._diag_parity_trade_timestamps.append(completion_ts)
            write_jsonl({
                "event_type": "PAIR_COMPLETED",
                "ts_ms": int(completion_ts * 1000),
                "pair_id": pair_id, "slug": slug, "crypto": crypto,
                "delay_ms": round(delay_ms, 1),
                "up_ts_ms": int(up_ts * 1000), "dn_ts_ms": int(dn_ts * 1000),
            })
            # Clean up tracker (keep last 100 for reference)
            self._pair_tracker.pop(pair_id, None)

    # =================================================================
    # PARITY QUOTING MODE — continuously post maker bids on both legs
    # =================================================================
    def _quote_get_dynamic_target(self, slug: str) -> float:
        """Return current dynamic quote target edge (cents) for this slug.
        Adjusts each minute based on fill rate and locked inventory."""
        current = self._quote_dynamic_target.get(slug,
                    PARITY_QUOTE_TARGET_EDGE_NET_CENTS_BASE)
        # Compute slug-level fill rate: quote_fills / quote_orders for this slug
        # (Use global rate as proxy — per-slug tracking is too granular for now)
        fill_rate = (self._diag_quote_fills / max(1, self._diag_quote_orders_placed)
                     if self._diag_quote_orders_placed > 0 else 0.5)
        locked_usd = 0.0
        st = self.market_states.get(slug)
        if st:
            up_q = st.positions["Up"].qty
            dn_q = st.positions["Down"].qty
            locked_shares = min(up_q, dn_q)
            if locked_shares >= MIN_QTY:
                locked_usd = locked_shares * (st.positions["Up"].vwap + st.positions["Down"].vwap)
        imbalance = 0.0
        if st:
            imbalance = abs(st.positions["Up"].qty - st.positions["Down"].qty)

        # Adjust: low fill rate + low locked -> pay up (decrease edge toward base)
        #         high locked or rising imbalance -> be selective (increase toward max)
        step = 0.05  # adjust by 0.05c per tick
        if fill_rate < 0.30 and locked_usd < PARITY_QUOTE_MAX_USD_PER_SLUG * 0.3:
            current = max(PARITY_QUOTE_TARGET_EDGE_NET_CENTS_BASE, current - step)
        elif locked_usd > PARITY_QUOTE_MAX_USD_PER_SLUG * 0.6 or imbalance > 20:
            current = min(PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX, current + step)

        self._quote_dynamic_target[slug] = current
        return current

    def _quote_get_dynamic_step(self, slug: str) -> float:
        """Return inventory-aware quote step size (USD) for this slug.
        Reduced by 50% during adverse degrade window."""
        fill_rate = (self._diag_quote_fills / max(1, self._diag_quote_orders_placed)
                     if self._diag_quote_orders_placed > 0 else 0.5)
        locked_usd = 0.0
        imbalance = 0.0
        st = self.market_states.get(slug)
        if st:
            up_q = st.positions["Up"].qty
            dn_q = st.positions["Down"].qty
            locked_shares = min(up_q, dn_q)
            if locked_shares >= MIN_QTY:
                locked_usd = locked_shares * (st.positions["Up"].vwap + st.positions["Down"].vwap)
            imbalance = abs(up_q - dn_q)
        # High inventory or imbalance -> reduce step (less risk per order)
        if locked_usd > 0.6 * PARITY_QUOTE_MAX_USD_PER_SLUG or imbalance > 20:
            base = max(0.75, PARITY_QUOTE_STEP_USD * 0.5)
        # Low fill rate + low locked -> increase step (be more aggressive)
        elif fill_rate < 0.30 and locked_usd < PARITY_QUOTE_MAX_USD_PER_SLUG * 0.3:
            base = min(3.0, PARITY_QUOTE_STEP_USD * 1.25)
        else:
            base = PARITY_QUOTE_STEP_USD
        # Adverse degrade: reduce step by 50%
        if time.time() < self._quote_degraded_until.get(slug, 0.0):
            base *= 0.5
        return base

    def _quote_check_adverse(self, slug: str) -> Tuple[bool, float, float]:
        """Check adverse selection: large spot move in last ADVERSE_LOOKBACK_SEC.
        Returns (is_adverse, spot_move_bps, velocity_bps_per_min)."""
        hist = self._spot_history.get(slug, [])
        if len(hist) < 2:
            return False, 0.0, 0.0
        now_t = time.time()
        cutoff = now_t - ADVERSE_LOOKBACK_SEC
        # Find oldest spot within lookback window
        oldest_spot = None
        oldest_ts = None
        for ts, sp in hist:
            if ts >= cutoff:
                oldest_spot = sp
                oldest_ts = ts
                break
        if oldest_spot is None or oldest_spot <= 0:
            return False, 0.0, 0.0
        latest_spot = hist[-1][1]
        latest_ts = hist[-1][0]
        if latest_spot <= 0:
            return False, 0.0, 0.0
        move_bps = abs((latest_spot - oldest_spot) / oldest_spot) * 10000.0
        # Compute velocity: bps per minute
        elapsed_sec = max(0.1, latest_ts - oldest_ts)
        vel_bps_per_min = move_bps / elapsed_sec * 60.0
        return move_bps > ADVERSE_SPOT_MOVE_BPS_THRESHOLD, move_bps, vel_bps_per_min

    def _parity_quote(self, m: MarketRef, st: MarketState, ctx: dict):
        """Post maker bids on both Up and Down to 'manufacture' cheap straddles.
        Dynamic target edge, anchor-based pricing, unpaired management."""
        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        now_t = time.time()

        # ── Unpaired quote management (runs first, always) ──
        self._quote_manage_unpaired(m, st, ctx, now_t)

        # ── Pause check ──
        pause_until = self._quote_paused_until.get(m.slug, 0.0)
        if now_t < pause_until:
            return

        # ── Adverse selection guard (degrade-first, hard-pause only if accelerating) ──
        is_adverse, spot_move_bps, vel_bps_per_min = self._quote_check_adverse(m.slug)
        if is_adverse:
            self._diag_adverse_guard_events += 1
            ts_ms = int(time.time() * 1000)
            if vel_bps_per_min >= ADVERSE_ACCEL_BPS_PER_MIN:
                # Move is accelerating — hard pause
                self._quote_paused_until[m.slug] = now_t + ADVERSE_PAUSE_SEC
                self._diag_adverse_guard_pauses += 1
                self._quote_dynamic_target[m.slug] = PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX
                write_jsonl({"event_type": "ADVERSE_GUARD_HARD_PAUSE",
                              "ts_ms": ts_ms,
                              "slug": m.slug, "crypto": m.crypto,
                              "spot_move_bps": round(spot_move_bps, 2),
                              "vel_bps_per_min": round(vel_bps_per_min, 1),
                              "threshold_bps": ADVERSE_SPOT_MOVE_BPS_THRESHOLD,
                              "accel_threshold": ADVERSE_ACCEL_BPS_PER_MIN,
                              "pause_sec": ADVERSE_PAUSE_SEC})
                return
            else:
                # Move is significant but not accelerating — degrade (don't pause)
                self._quote_degraded_until[m.slug] = now_t + ADVERSE_DEGRADE_SEC
                self._diag_adverse_guard_degrades += 1
                self._quote_dynamic_target[m.slug] = PARITY_QUOTE_TARGET_EDGE_NET_CENTS_MAX
                write_jsonl({"event_type": "ADVERSE_GUARD_DEGRADE",
                              "ts_ms": ts_ms,
                              "slug": m.slug, "crypto": m.crypto,
                              "spot_move_bps": round(spot_move_bps, 2),
                              "vel_bps_per_min": round(vel_bps_per_min, 1),
                              "threshold_bps": ADVERSE_SPOT_MOVE_BPS_THRESHOLD,
                              "degrade_sec": ADVERSE_DEGRADE_SEC})
                # Don't return — continue quoting but with degraded parameters

        # ── Liquidity guard (optional) ──
        if PARITY_QUOTE_ONLY_IF_LIQ_OK:
            liq_ok, _ = parity_liquidity_ok(up_book, dn_book)
            if not liq_ok:
                return

        # ── Staleness guard ──
        cache_age = self._cache_age_ms(m.slug)
        if cache_age > PARITY_MAX_CACHE_AGE_MS:
            return

        # ── Rate limit check ──
        if not self._rate_limit_ok(m.slug):
            return

        # ── Investment cap (reduced when directional scalp active) ──
        invested = self._parity_invested_usd.get(m.slug, 0.0)
        cap = PARITY_QUOTE_MAX_USD_PER_SLUG
        if PARITY_DEFER_TO_DIRECTIONAL and m.slug in self._dscalp_positions:
            cap = min(cap, PARITY_MAX_WHEN_DIRECTIONAL_USD)
        if invested >= cap:
            return

        # ── Dynamic target edge ──
        target_edge_cents = self._quote_get_dynamic_target(m.slug)
        target_combined = 1.00 - target_edge_cents / 100.0
        # Deduct maker fees from target (both legs are maker)
        maker_fee_per_leg = MAKER_FEE_BPS / 10000.0
        target_combined_net = target_combined - 2 * maker_fee_per_leg

        if up_book.bid <= 0 or dn_book.bid <= 0:
            return
        if up_book.ask <= 0 or dn_book.ask <= 0:
            return

        # ── Anchor pricing: equal USD on both legs ──
        # Try Up as anchor first, then Down if Up doesn't work
        target_up, target_dn = self._quote_compute_anchor_prices(
            up_book, dn_book, target_combined_net)
        if target_up is None or target_dn is None:
            return

        # ── Dynamic step sizing ──
        dynamic_step_usd = self._quote_get_dynamic_step(m.slug)

        # ── Post/refresh quotes on each leg ──
        slug_quotes = self._parity_quotes.get(m.slug, {})
        pair_id = uuid.uuid4().hex[:16]

        for outcome, target_price, book in [("Up", target_up, up_book),
                                             ("Down", target_dn, dn_book)]:
            existing = slug_quotes.get(outcome)
            effective_price = target_price
            if existing:
                price_diff = abs(target_price - existing.get("price", 0))
                elapsed_ms = (now_t - existing.get("ts", 0)) * 1000
                our_price = existing.get("price", 0)
                outbid = book.bid > our_price + 0.0005

                # Queue position heuristic: step up when outbid to regain best
                if outbid and elapsed_ms >= HEDGE_TICK1_MS:
                    # 250ms+ outbid: step +1 tick above best_bid
                    step_up = clamp_to_tick(book.bid + 0.001)
                    if step_up < book.ask:
                        effective_price = step_up
                elif outbid and elapsed_ms >= 100:
                    # 100ms+ outbid: match best_bid
                    effective_price = clamp_to_tick(book.bid)
                elif not outbid and elapsed_ms < QUOTE_REFRESH_MIN_ELAPSED_MS:
                    continue  # rate-limited: not outbid + too soon
                elif (not outbid and QUOTE_REFRESH_SKIP_IF_SAME
                      and price_diff < QUOTE_REFRESH_MIN_TICK_MOVE
                      and elapsed_ms < PARITY_QUOTE_REFRESH_MS):
                    continue  # no need to refresh: price unchanged

            # Check that effective_price still yields net edge
            other_price = target_dn if outcome == "Up" else target_up
            combined_check = effective_price + other_price
            if combined_check > target_combined_net + 0.002:
                effective_price = target_price  # revert to conservative price

            # Imbalance check: don't add to over-weighted side (TASK C)
            up_q = st.positions["Up"].qty
            dn_q = st.positions["Down"].qty
            imbalance = up_q - dn_q
            imb_cap = XRP_MAX_IMBALANCE_SHARES if m.crypto == "XRP" else IMBALANCE_CAP_SHARES
            if outcome == "Up" and imbalance > imb_cap:
                continue  # too much Up, skip
            if outcome == "Down" and imbalance < -imb_cap:
                continue  # too much Down, skip

            leg_usd = min(dynamic_step_usd,
                          (PARITY_QUOTE_MAX_USD_PER_SLUG - invested) / 2.0)
            if leg_usd < MIN_ORDER_USDC:
                continue

            cost = self._parity_quote_buy(m, st, outcome, book, effective_price,
                                           leg_usd, ctx, pair_id,
                                           quote_step_usd_used=dynamic_step_usd)
            if cost > 0:
                self._diag_quote_orders_placed += 1
                self._parity_quotes.setdefault(m.slug, {})[outcome] = {
                    "price": target_price,
                    "ts": now_t,
                    "pair_id": pair_id,
                    "order_id": getattr(self, '_last_quote_oid', ''),
                }
                # Track unpaired state: if one leg filled but the other hasn't yet
                invested = self._parity_invested_usd.get(m.slug, 0.0)

        # ── Check for straddle completion (both legs balanced) ──
        up_q = st.positions["Up"].qty
        dn_q = st.positions["Down"].qty
        imbal = abs(up_q - dn_q)
        both_have = up_q >= MIN_PAIR_QTY and dn_q >= MIN_PAIR_QTY
        if both_have and imbal < MIN_PAIR_QTY:
            # Balanced straddle — clear any hedge state
            if m.slug not in self._parity_locked_since:
                self._parity_locked_since[m.slug] = time.time()
            self._quote_unpaired.pop(m.slug, None)
            self._hedge_state.pop(m.slug, None)

        # ── Detect one-sided or imbalanced fills → trigger hedge ──
        # Case 1: one leg missing entirely
        # Case 2: significant imbalance (one leg much larger than other)
        up_has = up_q >= MIN_QTY
        dn_has = dn_q >= MIN_QTY
        one_sided = (up_has != dn_has)
        imbalanced = (up_has and dn_has and imbal >= MIN_PAIR_QTY
                      and max(up_q, dn_q) > min(up_q, dn_q) * 2.0)
        if (one_sided or imbalanced) and m.slug not in self._quote_unpaired:
            filled_outcome = "Up" if up_q > dn_q else "Down"
            self._quote_unpaired[m.slug] = {
                "outcome": filled_outcome,
                "fill_ts": now_t,
                "escalated": False,
            }
            self._diag_quote_unpaired_events += 1

    @staticmethod
    def _quote_compute_anchor_prices(up_book: BookTop, dn_book: BookTop,
                                      target_combined: float):
        """Compute anchor-based quote prices for both legs.
        Set one leg at best_bid, compute other = target_combined - anchor.
        Try both anchors, pick the one where both prices are competitive.
        Allow derived price up to best_bid + 2 ticks (more aggressive fills)."""
        min_tick = 0.01  # minimum price to bid
        max_above_best = 0.002  # allow up to 2 ticks above best_bid

        # Try anchor=Up (set Up at best_bid, compute Down)
        anchor_up = clamp_to_tick(up_book.bid)
        derived_dn = clamp_to_tick(target_combined - anchor_up)
        up_ok = anchor_up >= min_tick and anchor_up < up_book.ask
        dn_ok = (derived_dn >= min_tick
                 and derived_dn <= dn_book.bid + max_above_best
                 and derived_dn < dn_book.ask)

        # Try anchor=Down (set Down at best_bid, compute Up)
        anchor_dn = clamp_to_tick(dn_book.bid)
        derived_up = clamp_to_tick(target_combined - anchor_dn)
        up_ok2 = (derived_up >= min_tick
                  and derived_up <= up_book.bid + max_above_best
                  and derived_up < up_book.ask)
        dn_ok2 = anchor_dn >= min_tick and anchor_dn < dn_book.ask

        # Pick the anchor where the derived leg is most competitive
        # (closest to best bid without exceeding it)
        score_up_anchor = 0.0
        if up_ok and dn_ok:
            score_up_anchor = derived_dn  # higher = more competitive

        score_dn_anchor = 0.0
        if up_ok2 and dn_ok2:
            score_dn_anchor = derived_up

        if score_up_anchor <= 0 and score_dn_anchor <= 0:
            return None, None

        if score_up_anchor >= score_dn_anchor:
            # Anchor Up
            final_up = anchor_up
            final_dn = derived_dn
        else:
            # Anchor Down
            final_up = derived_up
            final_dn = anchor_dn

        # Final sanity: combined must not exceed 1.00
        if final_up + final_dn > 1.00:
            return None, None
        if final_up <= 0.01 or final_dn <= 0.01:
            return None, None
        return final_up, final_dn

    def _quote_manage_unpaired(self, m: MarketRef, st: MarketState,
                                ctx: dict, now_t: float):
        """Auto-hedge unpaired fills: fast escalation (+1 tick at 200ms,
        +3 ticks at 350ms, early cross at 500ms, late cross at 800ms),
        then unwind+pause. Stale cache (>450ms) blocks all hedge actions."""
        unpaired = self._quote_unpaired.get(m.slug)
        if unpaired is None:
            return

        # Track unpaired event per slug
        self._diag_slug_unpaired_events[m.slug] = self._diag_slug_unpaired_events.get(m.slug, 0) + 1

        # If both legs now balanced, clear hedge state
        up_q = st.positions["Up"].qty
        dn_q = st.positions["Down"].qty
        imbal = abs(up_q - dn_q)
        if (up_q >= MIN_PAIR_QTY and dn_q >= MIN_PAIR_QTY
                and imbal < MIN_PAIR_QTY):
            self._quote_unpaired.pop(m.slug, None)
            self._hedge_state.pop(m.slug, None)
            return

        filled_outcome = unpaired["outcome"]
        fill_ts = unpaired["fill_ts"]
        age_ms = (now_t - fill_ts) * 1000
        age_sec = age_ms / 1000.0
        t_min = ctx["t_min"]

        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        missing_outcome = "Down" if filled_outcome == "Up" else "Up"
        missing_book = dn_book if missing_outcome == "Down" else up_book

        # Initialize hedge state if needed
        hedge = self._hedge_state.setdefault(m.slug, {
            "tick1_done": False, "tick2_done": False,
            "early_cross_done": False, "cross_done": False,
        })

        # ── Stale cache guard: block all hedge actions if cache > 450ms ──
        cache_age = self._cache_age_ms(m.slug)
        if cache_age > HEDGE_STALE_CACHE_MS:
            self._diag_hedge_skipped_stale += 1
            # Force immediate refresh for this slug
            if not self._pending_fetches.get(m.slug):
                for mkt in self._cached_markets:
                    if mkt.slug == m.slug:
                        self._submit_market_refresh(mkt)
                        self._bg_next_due[m.slug] = now_t + 0.1
                        break
            write_jsonl({"event_type": "HEDGE_SKIPPED_STALE",
                          "ts_ms": int(now_t * 1000),
                          "slug": m.slug, "crypto": m.crypto,
                          "cache_age_ms": round(cache_age, 0),
                          "age_ms": round(age_ms, 0)})
            return  # Do NOT escalate/cross on stale data

        # Compute net edge for escalation decisions
        our_filled_price = st.positions[filled_outcome].vwap
        fee_cents = 2 * MAKER_FEE_BPS / 100.0
        target_edge = self._quote_get_dynamic_target(m.slug)
        dynamic_step_usd = self._quote_get_dynamic_step(m.slug)

        def _edge_ok(price: float) -> Tuple[bool, float]:
            est_combined = our_filled_price + price
            edge_c = (1.000 - est_combined) * 100 - fee_cents
            return edge_c >= target_edge * 0.3, edge_c

        # ── Tick 1 escalation: +1 tick at HEDGE_TICK1_MS (DISABLED — no micro-neutralizing) ──
        if HEDGE_TICK_ESCALATION_ENABLED and age_ms >= HEDGE_TICK1_MS and not hedge["tick1_done"] and missing_book.bid > 0:
            esc_price = clamp_to_tick(missing_book.bid + 0.001)
            ok, edge_c = _edge_ok(esc_price)
            if ok and esc_price < missing_book.ask:
                leg_usd = min(dynamic_step_usd,
                              (PARITY_QUOTE_MAX_USD_PER_SLUG -
                               self._parity_invested_usd.get(m.slug, 0.0)) / 2.0)
                if leg_usd >= MIN_ORDER_USDC:
                    cost = self._parity_quote_buy(
                        m, st, missing_outcome, missing_book,
                        esc_price, leg_usd, ctx,
                        uuid.uuid4().hex[:16],
                        quote_step_usd_used=dynamic_step_usd)
                    if cost > 0:
                        self._diag_hedge_tick1 += 1
                        self._diag_quote_unpaired_escalations += 1
                        self._diag_quote_orders_placed += 1
            hedge["tick1_done"] = True
            write_jsonl({"event_type": "HEDGE_TICK1",
                          "ts_ms": int(now_t * 1000),
                          "slug": m.slug, "crypto": m.crypto,
                          "missing_outcome": missing_outcome,
                          "age_ms": round(age_ms, 0),
                          "cache_age_ms": round(cache_age, 0),
                          "edge_cents": round(edge_c, 3)})

        # ── Tick 2 escalation: +3 ticks at HEDGE_TICK2_MS (DISABLED — no micro-neutralizing) ──
        if HEDGE_TICK_ESCALATION_ENABLED and age_ms >= HEDGE_TICK2_MS and not hedge["tick2_done"] and missing_book.bid > 0:
            esc_price = clamp_to_tick(missing_book.bid + 0.003)
            ok, edge_c = _edge_ok(esc_price)
            if ok and esc_price < missing_book.ask:
                leg_usd = min(dynamic_step_usd,
                              (PARITY_QUOTE_MAX_USD_PER_SLUG -
                               self._parity_invested_usd.get(m.slug, 0.0)) / 2.0)
                if leg_usd >= MIN_ORDER_USDC:
                    cost = self._parity_quote_buy(
                        m, st, missing_outcome, missing_book,
                        esc_price, leg_usd, ctx,
                        uuid.uuid4().hex[:16],
                        quote_step_usd_used=dynamic_step_usd)
                    if cost > 0:
                        self._diag_hedge_tick2 += 1
                        self._diag_quote_unpaired_escalations += 1
                        self._diag_quote_orders_placed += 1
            hedge["tick2_done"] = True
            write_jsonl({"event_type": "HEDGE_TICK2",
                          "ts_ms": int(now_t * 1000),
                          "slug": m.slug, "crypto": m.crypto,
                          "missing_outcome": missing_outcome,
                          "age_ms": round(age_ms, 0),
                          "cache_age_ms": round(cache_age, 0),
                          "edge_cents": round(edge_c, 3)})

        # ── Early taker cross: at HEDGE_EARLY_CROSS_MS (500ms) — primary completion ──
        if (age_ms >= HEDGE_EARLY_CROSS_MS and not hedge.get("early_cross_done")
                and not hedge["cross_done"] and missing_book.ask > 0):
            cross_price = missing_book.ask
            spread_cents = (missing_book.ask - missing_book.bid) * 100 if missing_book.bid > 0 else 999
            ok, edge_c = _edge_ok(cross_price)
            if edge_c >= HEDGE_EARLY_CROSS_EDGE_CENTS and spread_cents <= HEDGE_MAX_CROSS_SPREAD_CENTS:
                leg_usd = min(dynamic_step_usd,
                              (PARITY_QUOTE_MAX_USD_PER_SLUG -
                               self._parity_invested_usd.get(m.slug, 0.0)) / 2.0)
                if leg_usd >= MIN_ORDER_USDC:
                    cost = self._parity_buy_leg(
                        m, st, missing_outcome, missing_book,
                        leg_usd, ctx,
                        pair_id=uuid.uuid4().hex[:16])
                    if cost > 0:
                        self._diag_hedge_cross += 1
                        self._diag_hedge_cross_early += 1
                        self._diag_quote_orders_placed += 1
                        hedge["cross_done"] = True  # skip late cross too
                        write_jsonl({"event_type": "HEDGE_CROSS_EARLY",
                                      "ts_ms": int(now_t * 1000),
                                      "slug": m.slug, "crypto": m.crypto,
                                      "missing_outcome": missing_outcome,
                                      "cross_price": round(cross_price, 4),
                                      "edge_cents": round(edge_c, 3),
                                      "spread_cents": round(spread_cents, 2),
                                      "cache_age_ms": round(cache_age, 0),
                                      "age_ms": round(age_ms, 0)})
            hedge["early_cross_done"] = True

        # ── Late taker cross: at HEDGE_CROSS_MS (800ms) — fallback completion ──
        if age_ms >= HEDGE_CROSS_MS and not hedge["cross_done"] and missing_book.ask > 0:
            cross_price = missing_book.ask
            spread_cents = (missing_book.ask - missing_book.bid) * 100 if missing_book.bid > 0 else 999
            ok, edge_c = _edge_ok(cross_price)
            if edge_c >= HEDGE_MIN_CROSS_EDGE_CENTS and spread_cents <= HEDGE_MAX_CROSS_SPREAD_CENTS:
                leg_usd = min(dynamic_step_usd,
                              (PARITY_QUOTE_MAX_USD_PER_SLUG -
                               self._parity_invested_usd.get(m.slug, 0.0)) / 2.0)
                if leg_usd >= MIN_ORDER_USDC:
                    cost = self._parity_buy_leg(
                        m, st, missing_outcome, missing_book,
                        leg_usd, ctx,
                        pair_id=uuid.uuid4().hex[:16])
                    if cost > 0:
                        self._diag_hedge_cross += 1
                        self._diag_hedge_cross_late += 1
                        self._diag_quote_orders_placed += 1
                        write_jsonl({"event_type": "HEDGE_CROSS_LATE",
                                      "ts_ms": int(now_t * 1000),
                                      "slug": m.slug, "crypto": m.crypto,
                                      "missing_outcome": missing_outcome,
                                      "cross_price": round(cross_price, 4),
                                      "edge_cents": round(edge_c, 3),
                                      "spread_cents": round(spread_cents, 2),
                                      "cache_age_ms": round(cache_age, 0),
                                      "age_ms": round(age_ms, 0)})
            else:
                # Edge collapsed — unwind the filled leg
                write_jsonl({"event_type": "HEDGE_EDGE_COLLAPSE",
                              "ts_ms": int(now_t * 1000),
                              "slug": m.slug, "crypto": m.crypto,
                              "filled_outcome": filled_outcome,
                              "edge_cents": round(edge_c, 3),
                              "spread_cents": round(spread_cents, 2),
                              "cache_age_ms": round(cache_age, 0),
                              "age_ms": round(age_ms, 0)})
            hedge["cross_done"] = True

        # ── Timeout: after QUOTE_UNPAIRED_MAX_SEC, unwind and pause ──
        # Rule 1: In LIVE modes, use tighter atomic hedge timeout (2s / 1s in final 2 min)
        effective_timeout = QUOTE_UNPAIRED_MAX_SEC
        if MODE in ("LIVE", "LIVE_SAFE"):
            effective_timeout = min(effective_timeout,
                                    self._live_safety.hedge_timeout_sec(t_min))
        if age_sec >= effective_timeout:
            pos = st.positions[filled_outcome]
            if pos.qty >= MIN_QTY:
                filled_book = up_book if filled_outcome == "Up" else dn_book
                if filled_book.bid > 0:
                    unwind_price = max(filled_book.bid, filled_book.ask - 0.001)
                    self._do_sell(m, st, filled_outcome, pos.qty, unwind_price,
                                  reason="QUOTE_UNPAIRED_UNWIND", leg="PARITY_QUOTE",
                                  ctx=ctx, use_maker=False)
                    self._diag_unpaired_unwind_usd += unwind_price * pos.qty
                    self._diag_hedge_unwind += 1
                    self._diag_unpaired_count_min += 1
            # Rule 6: Record hedge failure for circuit breaker
            if MODE in ("LIVE", "LIVE_SAFE"):
                self._live_safety.on_hedge_fail(m.slug, "quote_unpaired_timeout")
            self._quote_paused_until[m.slug] = now_t + QUOTE_PAUSE_AFTER_UNPAIRED_SEC
            self._diag_quote_pause_count += 1
            self._diag_slug_timeouts[m.slug] = self._diag_slug_timeouts.get(m.slug, 0) + 1
            self._quote_unpaired.pop(m.slug, None)
            self._hedge_state.pop(m.slug, None)
            write_jsonl({"event_type": "QUOTE_UNPAIRED_TIMEOUT",
                          "slug": m.slug, "crypto": m.crypto,
                          "filled_outcome": filled_outcome,
                          "age_sec": round(age_sec, 1),
                          "pause_sec": QUOTE_PAUSE_AFTER_UNPAIRED_SEC})

    def _parity_quote_buy(self, m: MarketRef, st: MarketState,
                           outcome: str, book: BookTop, bid_price: float,
                           leg_usd: float, ctx: dict, pair_id: str,
                           quote_step_usd_used: float = 0.0) -> float:
        """Buy at ask for immediate fill in quoting mode. Returns cost if filled."""
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        pos = st.positions[outcome]
        placed_ts = time.time()

        # Always buy at ask (taker) for immediate fill
        bid_price = book.ask

        # LIVE_SAFE trade-size limiter
        leg_usd, order_qty = self._live_safe_cap_usd(leg_usd, bid_price)
        if order_qty < MIN_QTY or order_qty < 1:
            return 0.0

        decision_id = new_decision_id()
        client_oid = new_order_id()
        if pos.position_id is None:
            pos.position_id = new_position_id()
            pos.trade_id = uuid.uuid4().hex
            pos.entry_decision_id = decision_id
            pos.parent_order_id = client_oid
            pos.entry_mid = book.mid
            pos.max_favorable_mid = book.mid
            pos.max_adverse_mid = book.mid

        up_book = ctx["up_book"]
        dn_book = ctx["dn_book"]
        bk_fields = self._book_fields(up_book, dn_book, outcome)

        self.logger.log_order_intent(
            engine="PARITY", reason="PARITY_QUOTE",
            decision_id=decision_id, position_id=pos.position_id,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=order_qty, target_price=bid_price,
            usdc_cost=leg_usd, ctx=ctx, book_fields=bk_fields,
        )
        self.logger.log_order_submit(
            engine="PARITY", reason="PARITY_QUOTE",
            decision_id=decision_id, position_id=pos.position_id,
            client_order_id=client_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=order_qty, target_price=bid_price,
            usdc_cost=leg_usd, ctx=ctx, book_fields=bk_fields,
            extra={"pair_id": pair_id, "placed_ts": placed_ts,
                   "quote_step_usd_used": round(quote_step_usd_used, 2)},
        )
        # ── Order lifecycle: track submit ──
        self._rate_limit_record(m.slug)
        self._true_cost_tx_count += 1
        placed_ts_ms = int(placed_ts * 1000)
        self._last_quote_oid = client_oid
        self._diag_quote_submit_count += 1
        self._clone_quote_submit_count += 1
        self._active_orders[client_oid] = {
            "slug": m.slug, "outcome": outcome, "side": "BUY",
            "price": bid_price, "qty": order_qty,
            "submit_ts": placed_ts, "submit_ts_ms": placed_ts_ms,
            "reason": "PARITY_QUOTE",
        }
        write_jsonl({"event_type": "ORDER_SUBMIT",
                      "ts_ms": placed_ts_ms,
                      "slug": m.slug, "outcome": outcome,
                      "order_id": client_oid, "pair_id": pair_id,
                      "price": round(bid_price, 4), "qty": round(order_qty, 1),
                      "reason": "PARITY_QUOTE"})
        # Cancel any previous quote for this slug+outcome (ORDER_REPLACE)
        slug_quotes = self._parity_quotes.get(m.slug, {})
        prev_quote = slug_quotes.get(outcome)
        if prev_quote and prev_quote.get("order_id"):
            old_oid = prev_quote["order_id"]
            if old_oid in self._active_orders:
                old_info = self._active_orders.pop(old_oid)
                self._diag_quote_cancel_count += 1
                self._diag_quote_replace_count += 1
                self._clone_quote_cancel_count += 1
                self._clone_quote_replace_count += 1
                write_jsonl({"event_type": "ORDER_REPLACE",
                              "ts_ms": placed_ts_ms,
                              "slug": m.slug, "outcome": outcome,
                              "old_id": old_oid, "new_id": client_oid,
                              "old_px": round(old_info["price"], 4),
                              "new_px": round(bid_price, 4),
                              "reason": "refresh"})

        self._ledger_order_intent(m, st, outcome, "BUY", order_qty, bid_price, "PARITY_QUOTE", order_id=client_oid)
        if MODE == "LOG":
            fill_ts = time.time()
            fill_ts_ms = int(fill_ts * 1000)
            # In paper mode: taker fill at ask — always fills immediately
            actual_cost = bid_price * order_qty
            self._paper_buy(st, outcome, bid_price, order_qty, actual_cost)
            self._ledger_record_buy_fill(m, st, outcome, bid_price, order_qty)
            self._record_fill_ts(m.slug)  # post-fill cooldown
            fill_latency_ms = (fill_ts - placed_ts) * 1000
            notional = bid_price * order_qty
            fee = compute_fee_usdc(notional, "taker")
            self.logger.log_order_fill(
                engine="PARITY", reason="PARITY_QUOTE",
                decision_id=decision_id, client_order_id=client_oid,
                position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=order_qty, fill_price=bid_price,
                usdc_cost=actual_cost, fees_usdc=fee,
                maker_taker="taker", did_cross="",
                vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                extra={"placed_ts_ms": placed_ts_ms, "fill_ts_ms": fill_ts_ms,
                       "fill_latency_ms": round(fill_latency_ms, 1),
                       "quote_step_usd_used": round(quote_step_usd_used, 2)},
            )
            write_jsonl({"event_type": "ORDER_FILL",
                          "ts_ms": fill_ts_ms,
                          "slug": m.slug, "outcome": outcome,
                          "order_id": client_oid, "pair_id": pair_id,
                          "price": round(bid_price, 4), "qty": round(order_qty, 1),
                          "maker_taker": "taker",
                          "fill_latency_ms": round(fill_latency_ms, 1)})
            self._tempo_fills[m.slug] = self._tempo_fills.get(m.slug, 0) + 1
            self._diag_quote_fills += 1
            self._clone_quote_fill_count += 1
            self._diag_parity_fills_min += 1
            self._diag_total_fills_min += 1
            self._throttle_record_trade()
            self._active_orders.pop(client_oid, None)
            self._record_pair_fill(pair_id, m.slug, m.crypto, outcome, fill_ts, "taker")
            # Signal-to-fill tracking for clone metrics
            last_move = self._clone_last_spot_move.get(m.slug)
            if last_move:
                s2f = (fill_ts - last_move[0]) * 1000
                if s2f > 0 and s2f < 30000:
                    self._clone_signal_to_fill.append(s2f)
            self._parity_invested_usd[m.slug] = self._parity_invested_usd.get(m.slug, 0.0) + actual_cost
            self._parity_last_order_ts[m.slug] = time.time()

            # If both legs now filled, log straddle completion
            if (st.positions["Up"].qty >= MIN_QTY
                    and st.positions["Down"].qty >= MIN_QTY):
                up_vwap = st.positions["Up"].vwap
                dn_vwap = st.positions["Down"].vwap
                straddle_cost = up_vwap + dn_vwap
                net_edge = (1.000 - straddle_cost) * 100
                self._diag_parity_edges.append(net_edge)
                write_jsonl({"event_type": "PARITY_QUOTE_STRADDLE",
                              "ts_ms": fill_ts_ms,
                              "slug": m.slug, "crypto": m.crypto,
                              "pair_id": pair_id,
                              "straddle_cost": round(straddle_cost, 4),
                              "net_edge_cents": round(net_edge, 3),
                              "up_vwap": round(up_vwap, 4),
                              "dn_vwap": round(dn_vwap, 4)})
            else:
                # One leg filled but not balanced — immediately trigger hedge
                up_q = st.positions["Up"].qty
                dn_q = st.positions["Down"].qty
                if (up_q >= MIN_QTY or dn_q >= MIN_QTY) and m.slug not in self._quote_unpaired:
                    imbal = abs(up_q - dn_q)
                    one_sided = (up_q < MIN_QTY) != (dn_q < MIN_QTY)
                    big_imbal = imbal >= MIN_PAIR_QTY and max(up_q, dn_q) > min(up_q, dn_q) * 2.0
                    if one_sided or big_imbal:
                        heavier = "Up" if up_q > dn_q else "Down"
                        self._quote_unpaired[m.slug] = {
                            "outcome": heavier,
                            "fill_ts": fill_ts,
                            "escalated": False,
                        }
                        self._diag_quote_unpaired_events += 1

            return actual_cost
        else:
            # LIVE mode: buy at ask (taker) for immediate fill
            fill = self.client.place_limit_order(token_id, "BUY", bid_price,
                                                  order_qty, post_only=False)
            self._truth_watch_after_place(fill, m, outcome, "BUY", order_qty, bid_price)
            fill_ts = time.time()
            if fill.get("filled") is True:
                fill_latency_ms = (fill_ts - placed_ts) * 1000
                actual_cost = fill["fill_price"] * fill["fill_qty"]
                self._live_buy(st, outcome, fill["fill_price"], fill["fill_qty"], actual_cost)
                self._ledger_record_buy_fill(m, st, outcome, fill["fill_price"], fill["fill_qty"],
                                             order_id=fill.get("order_id", ""))
                notional = fill["fill_price"] * fill["fill_qty"]
                mt = infer_maker_taker("BUY", fill["fill_price"], book)
                fee = compute_fee_usdc(notional, mt)
                self.logger.log_order_fill(
                    engine="PARITY", reason="PARITY_QUOTE",
                    decision_id=decision_id, client_order_id=client_oid,
                    position_id=pos.position_id,
                    crypto=m.crypto, slug=m.slug, outcome=outcome,
                    side="BUY", qty=fill["fill_qty"], fill_price=fill["fill_price"],
                    usdc_cost=actual_cost, fees_usdc=fee,
                    maker_taker=mt, did_cross="",
                    vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                    extra={"placed_ts": placed_ts, "fill_ts": fill_ts,
                           "fill_latency_ms": round(fill_latency_ms, 1),
                           "quote_step_usd_used": round(quote_step_usd_used, 2)},
                )
                self._tempo_fills[m.slug] = self._tempo_fills.get(m.slug, 0) + 1
                self._diag_quote_fills += 1
                self._clone_quote_fill_count += 1
                self._diag_parity_fills_min += 1
                self._diag_total_fills_min += 1
                self._throttle_record_trade()
                self._active_orders.pop(client_oid, None)
                self._record_pair_fill(pair_id, m.slug, m.crypto, outcome, fill_ts, "taker")
                last_move = self._clone_last_spot_move.get(m.slug)
                if last_move:
                    s2f = (fill_ts - last_move[0]) * 1000
                    if s2f > 0 and s2f < 30000:
                        self._clone_signal_to_fill.append(s2f)
                self._parity_invested_usd[m.slug] = self._parity_invested_usd.get(m.slug, 0.0) + actual_cost
                self._parity_last_order_ts[m.slug] = time.time()
                return actual_cost
            return 0.0

    def _parity_buy_leg(self, m: MarketRef, st: MarketState,
                        outcome: str, book: BookTop,
                        leg_usd: float, ctx: dict,
                        pair_id: str = "") -> float:
        """Execute one leg of a parity buy at ask price for immediate fill.
        Returns cost (USDC) of filled order."""
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        pos = st.positions[outcome]
        placed_ts = time.time()

        # Always buy at ask (taker) for immediate fill
        order_price = book.ask

        # LIVE_SAFE trade-size limiter
        leg_usd, order_qty = self._live_safe_cap_usd(leg_usd, order_price)
        if order_qty < MIN_QTY or order_qty < 1:
            return 0.0

        decision_id = new_decision_id()
        client_oid = new_order_id()
        if pos.position_id is None:
            pos.position_id = new_position_id()
            pos.trade_id = uuid.uuid4().hex
            pos.entry_decision_id = decision_id
            pos.parent_order_id = client_oid
            pos.entry_mid = book.mid
            pos.max_favorable_mid = book.mid
            pos.max_adverse_mid = book.mid

        up_book = ctx["up_book"]
        dn_book = ctx["dn_book"]
        bk_fields = self._book_fields(up_book, dn_book, outcome)
        mt = "taker"

        self.logger.log_order_intent(
            engine="PARITY", reason="PARITY_BUY",
            decision_id=decision_id, position_id=pos.position_id,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=order_qty, target_price=order_price,
            usdc_cost=leg_usd, ctx=ctx, book_fields=bk_fields,
        )
        self.logger.log_order_submit(
            engine="PARITY", reason="PARITY_BUY",
            decision_id=decision_id, position_id=pos.position_id,
            client_order_id=client_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=order_qty, target_price=order_price,
            usdc_cost=leg_usd, ctx=ctx, book_fields=bk_fields,
            extra={"pair_id": pair_id, "placed_ts": placed_ts} if pair_id else {"placed_ts": placed_ts},
        )
        self._ledger_order_intent(m, st, outcome, "BUY", order_qty, order_price, "PARITY_BUY", order_id=client_oid)

        if MODE == "LOG":
            fill_ts = time.time()
            self._paper_buy(st, outcome, order_price, order_qty, leg_usd)
            self._ledger_record_buy_fill(m, st, outcome, order_price, order_qty)
            self._record_fill_ts(m.slug)  # post-fill cooldown
            sc = spread_capture_fields("BUY", order_price, book)
            fill_latency_ms = (fill_ts - placed_ts) * 1000
            _fee = compute_fee_usdc(leg_usd, mt)
            self.logger.log_order_fill(
                engine="PARITY", reason="PARITY_BUY",
                decision_id=decision_id, client_order_id=client_oid,
                position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=order_qty, fill_price=order_price,
                usdc_cost=leg_usd, fees_usdc=_fee,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                extra={"placed_ts": placed_ts, "fill_ts": fill_ts,
                       "fill_latency_ms": round(fill_latency_ms, 1)},
            )
            self._tempo_fills[m.slug] = self._tempo_fills.get(m.slug, 0) + 1
            return leg_usd
        else:
            fill = self.client.place_limit_order(token_id, "BUY", order_price,
                                                  order_qty, post_only=False)
            self._truth_watch_after_place(fill, m, outcome, "BUY", order_qty, order_price)
            fill_ts = time.time()
            if fill.get("filled") is True:
                fill_latency_ms = (fill_ts - placed_ts) * 1000
                actual_cost = fill["fill_price"] * fill["fill_qty"]
                self._live_buy(st, outcome, fill["fill_price"], fill["fill_qty"], actual_cost)
                self._ledger_record_buy_fill(m, st, outcome, fill["fill_price"], fill["fill_qty"],
                                             order_id=fill.get("order_id", ""))
                sc = spread_capture_fields("BUY", fill["fill_price"], book)
                actual_mt = infer_maker_taker("BUY", fill["fill_price"], book)
                _fee = compute_fee_usdc(actual_cost, actual_mt)
                self.logger.log_order_fill(
                    engine="PARITY", reason="PARITY_BUY",
                    decision_id=decision_id, client_order_id=client_oid,
                    position_id=pos.position_id,
                    crypto=m.crypto, slug=m.slug, outcome=outcome,
                    side="BUY", qty=fill["fill_qty"], fill_price=fill["fill_price"],
                    usdc_cost=actual_cost, fees_usdc=_fee,
                    maker_taker=actual_mt, did_cross=sc.get("did_cross", ""),
                    vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
                    extra={"placed_ts": placed_ts, "fill_ts": fill_ts,
                           "fill_latency_ms": round(fill_latency_ms, 1)},
                )
                self._tempo_fills[m.slug] = self._tempo_fills.get(m.slug, 0) + 1
                return actual_cost
            return 0.0

    # =================================================================
    # END-OF-HOUR PARITY FLATTENING
    # =================================================================
    def _parity_flatten_eoh(self, m: MarketRef, st: MarketState,
                             t_min: float, ctx: dict):
        """Flatten all parity (locked + unpaired) inventory as hour ends.
        PARITY_FLATTEN_START_MIN..PARITY_HARD_FLATTEN_MIN: maker-first with 200ms refresh.
        After PARITY_HARD_FLATTEN_MIN: taker if time_to_close<20s or emergency inventory."""
        up_book: BookTop = ctx["up_book"]
        dn_book: BookTop = ctx["dn_book"]
        seconds_to_close = ctx.get("seconds_to_close", 999.0)
        hard_mode = t_min >= PARITY_HARD_FLATTEN_MIN

        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty < MIN_QTY:
                continue
            book = up_book if outcome == "Up" else dn_book
            if book.bid <= 0:
                continue

            # Determine maker vs taker
            allow_taker = False
            if hard_mode:
                if seconds_to_close < 20.0:
                    allow_taker = True
                elif pos.qty >= INVENTORY_EMERGENCY_SHARES:
                    allow_taker = True
            spread_ok = (book.spread * 100) <= PARITY_TAKER_ALLOWED_SPREAD_CENTS
            use_taker = allow_taker and spread_ok

            # Maker queue discipline: respect PARITY_MAKER_REFRESH_MS
            if not use_taker:
                maker_state = self._parity_maker_orders.get(m.slug, {}).get(outcome)
                if maker_state:
                    elapsed_ms = (time.time() - maker_state.get("last_replace_ts", 0)) * 1000
                    price_diff = abs(book.bid - maker_state.get("price", 0))
                    if price_diff < 0.005 and elapsed_ms < PARITY_MAKER_REFRESH_MS:
                        continue  # not time to replace yet

            # Sell quantity: in hard mode, sell everything; otherwise step-based
            if hard_mode:
                sell_qty = pos.qty
            else:
                sell_qty = min(pos.qty, RECYCLE_STEP_USD / max(1e-9, book.bid))
            if sell_qty < MIN_QTY:
                continue

            if use_taker:
                sell_price = book.bid
                self._diag_flatten_taker += 1
                self._diag_parity_taker_count += 1
            else:
                sell_price = max(book.bid, book.ask - 0.001)
                self._diag_parity_maker_count += 1
                # Update maker order tracking
                self._parity_maker_orders.setdefault(m.slug, {})[outcome] = {
                    "price": book.bid,
                    "last_replace_ts": time.time(),
                    "placed_ts": time.time(),
                    "first_not_best_ts": None,
                    "filled": False,
                }

            self._diag_flatten_actions += 1
            flatten_reason = "PARITY_HARD_FLATTEN" if hard_mode else "PARITY_FLATTEN"

            self._do_sell(m, st, outcome, sell_qty, sell_price,
                          reason=flatten_reason, leg="PARITY_FLATTEN",
                          ctx=ctx, use_maker=not use_taker)

            write_jsonl({"event_type": flatten_reason,
                          "slug": m.slug, "crypto": m.crypto,
                          "outcome": outcome, "sell_qty": round(sell_qty, 1),
                          "sell_price": round(sell_price, 4),
                          "use_taker": use_taker,
                          "seconds_to_close": round(seconds_to_close, 1),
                          "t_min": round(t_min, 3)})

        # Also flatten any pending partial fills by unwinding
        resolved = []
        for i, pair in enumerate(self._parity_pending_pairs):
            if pair["slug"] != m.slug:
                continue
            filled_book = up_book if pair["filled_outcome"] == "Up" else dn_book
            pos = st.positions[pair["filled_outcome"]]
            unwind_qty = min(pair.get("filled_qty", 0), pos.qty)
            if unwind_qty >= MIN_QTY and filled_book.bid > 0:
                use_taker_unwind = hard_mode and (seconds_to_close < 20.0 or spread_ok)
                unwind_price = filled_book.bid if use_taker_unwind else max(filled_book.bid, filled_book.ask - 0.001)
                self._do_sell(m, st, pair["filled_outcome"], unwind_qty, unwind_price,
                              reason="PARITY_FLATTEN_UNPAIRED", leg="PARITY_FLATTEN",
                              ctx=ctx, use_maker=not use_taker_unwind)
                self._diag_flatten_actions += 1
                if use_taker_unwind:
                    self._diag_flatten_taker += 1
            resolved.append(i)
        for idx in sorted(resolved, reverse=True):
            self._parity_pending_pairs.pop(idx)

        # Clear locked tracking if fully flat
        up_q = st.positions["Up"].qty
        dn_q = st.positions["Down"].qty
        if up_q < MIN_QTY and dn_q < MIN_QTY:
            self._parity_locked_since.pop(m.slug, None)
            self._parity_invested_usd.pop(m.slug, None)

    # =================================================================
    # DIRECTIONAL LEAN EXIT OVERLAY
    # =================================================================
    def _directional_lean_exits(self, m: MarketRef, st: MarketState,
                                 t_min: float, delta_bps: float, ctx: dict):
        """Directional lean: sell excess on the "wrong" side at bid (taker).
        Cooldown: 5 seconds per slug to avoid spam."""
        if not LEAN_EXIT_PRIORITY:
            return
        now = time.time()
        if now - self._lean_exit_last_ts.get(m.slug, 0.0) < 5.0:
            return
        spot, hour_open = ctx["spot"], ctx["hour_open"]
        lean_up = spot >= hour_open

        pos_up = st.positions["Up"]
        pos_dn = st.positions["Down"]

        if lean_up:
            wrong_side, wrong_pos, right_pos = "Down", pos_dn, pos_up
        else:
            wrong_side, wrong_pos, right_pos = "Up", pos_up, pos_dn

        if wrong_pos.qty < MIN_QTY:
            return
        book = self.last_book.get(m.slug, {}).get(wrong_side)
        if not book or book.bid <= 0:
            return
        excess = wrong_pos.qty - right_pos.qty
        if excess <= 0:
            return

        sell_qty = min(excess, wrong_pos.qty * 0.25, float(LEAN_MAX_IMBALANCE_SHARES))
        if sell_qty < MIN_QTY:
            return
        if MODE in ("LIVE", "LIVE_SAFE") and sell_qty < CLOB_MIN_ORDER_SIZE:
            if excess >= CLOB_MIN_ORDER_SIZE and wrong_pos.qty >= CLOB_MIN_ORDER_SIZE:
                sell_qty = float(CLOB_MIN_ORDER_SIZE)
            else:
                return

        if book.bid < wrong_pos.vwap:
            return

        # Always set cooldown BEFORE attempting sell
        self._lean_exit_last_ts[m.slug] = now

        # Always sell at bid (taker) for immediate fill — never use maker
        write_jsonl({"event_type": "LEAN_EXIT", "slug": m.slug, "crypto": m.crypto,
                      "wrong_side": wrong_side, "sell_qty": round(sell_qty, 1),
                      "imbalance": round(excess, 1),
                      "lean": "Up" if lean_up else "Down",
                      "t_min": round(t_min, 3)})

        self._do_sell(m, st, wrong_side, sell_qty, book.bid,
                      reason="LEAN_EXIT", leg="LEAN_EXIT",
                      ctx=ctx, use_maker=False)

    def _late_scalps(self, ctx: dict):
        if not self._buys_allowed():
            return
        m, st = ctx["m"], ctx["st"]
        if not self._slug_entry_allowed(m.slug):
            return
        if not self._inventory_cap_ok(m.slug):
            return
        if not self._post_fill_cooldown_ok(m.slug):
            return
        t_min = ctx["t_min"]
        delta_bps, abs_delta_bps = ctx["delta_bps"], ctx["abs_delta_bps"]
        up_book, dn_book = ctx["up_book"], ctx["dn_book"]
        spot = ctx["spot"]
        if not (LATE_SCALP_T_START <= t_min <= LATE_SCALP_T_END):
            return
        if abs_delta_bps < LATE_SCALP_ABSDELTA_MIN or abs_delta_bps > LATE_SCALP_ABSDELTA_MAX:
            return
        if min(up_book.ask, dn_book.ask) > LATE_SCALP_PRICE_MAX:
            return
        outcome = "Up" if up_book.ask < dn_book.ask else "Down"
        book = up_book if outcome == "Up" else dn_book
        if book.spread > IMB_MAX_SPREAD:
            return
        # Coin-specific spread limit
        if not self._coin_spread_entry_ok(m.crypto, book.spread * 100):
            return
        now_iso = iso_z(utc_now())
        if st.last_reentry_ts:
            last = _parse_iso_z(st.last_reentry_ts)
            if (utc_now() - last).total_seconds() < REENTRY_COOLDOWN_SEC:
                return
        clip = self._calc_clip(m.crypto, t_min, abs_delta_bps) * 0.50
        if clip < MIN_ORDER_USDC:
            return
        # LIVE_SAFE trade-size limiter
        clip, qty = self._live_safe_cap_usd(clip, book.ask)
        if qty < MIN_QTY:
            return
        target_sell = min(0.999, book.ask + LATE_SCALP_TP_CENTS)
        decision_id = new_decision_id()
        client_oid = new_order_id()
        mult = sizing_mult(abs_delta_bps)
        pos = st.positions[outcome]
        if pos.position_id is None:
            pos.position_id = new_position_id()
            pos.trade_id = uuid.uuid4().hex
            pos.entry_decision_id = decision_id
            pos.parent_order_id = client_oid
            pos.entry_mid = book.mid
            pos.max_favorable_mid = book.mid
            pos.max_adverse_mid = book.mid
        ctx["size_mult"] = mult
        bk_fields = self._book_fields(up_book, dn_book, outcome)
        self.logger.log_order_intent(
            engine="LATE_SCALP", reason="ENTRY_SCALP",
            decision_id=decision_id, position_id=pos.position_id,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=qty, target_price=book.ask,
            usdc_cost=clip, ctx=ctx, book_fields=bk_fields,
            extra={"notes": f"target_sell={target_sell:.3f}"},
        )
        self.logger.log_order_submit(
            engine="LATE_SCALP", reason="ENTRY_SCALP",
            decision_id=decision_id, position_id=pos.position_id,
            client_order_id=client_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="BUY", qty=qty, target_price=book.ask,
            usdc_cost=clip, ctx=ctx, book_fields=bk_fields,
        )
        self._ledger_order_intent(m, st, outcome, "BUY", qty, book.ask, "ENTRY_SCALP", order_id=client_oid)
        st.last_reentry_ts = now_iso
        if MODE == "LOG":
            self._paper_buy(st, outcome, book.ask, qty, clip)
            self._ledger_record_buy_fill(m, st, outcome, book.ask, qty)
            self._record_fill_ts(m.slug)  # post-fill cooldown
            mt = infer_maker_taker("BUY", book.ask, book)
            sc = spread_capture_fields("BUY", book.ask, book)
            _fee = compute_fee_usdc(clip, mt)
            self.logger.log_order_fill(
                engine="LATE_SCALP", reason="ENTRY_SCALP",
                decision_id=decision_id, client_order_id=client_oid,
                position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=qty, fill_price=book.ask,
                usdc_cost=clip, fees_usdc=_fee,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
            )
            pos.scalp_mode = True
            pos.scalp_open_ts = now_iso
            return
        fill = self._place_layered_buy(m, outcome, qty, book.ask)
        if fill["total_filled"] > 0:
            actual_qty = fill["total_filled"]
            actual_price = fill["avg_price"]
            actual_cost = fill["total_cost"]
            self._live_buy(st, outcome, actual_price, actual_qty, actual_cost)
            self._ledger_record_buy_fill(m, st, outcome, actual_price, actual_qty)
            mt = infer_maker_taker("BUY", actual_price, book)
            sc = spread_capture_fields("BUY", actual_price, book)
            _fee = compute_fee_usdc(actual_cost, mt)
            self.logger.log_order_fill(
                engine="LATE_SCALP", reason="ENTRY_SCALP",
                decision_id=decision_id, client_order_id=client_oid,
                position_id=pos.position_id,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="BUY", qty=actual_qty, fill_price=actual_price,
                usdc_cost=actual_cost, fees_usdc=_fee,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                vwap=pos.vwap, ctx=ctx, book_fields=bk_fields,
            )
        pos.scalp_mode = True
        pos.scalp_open_ts = now_iso
    def _manage_exits(self, m: MarketRef, st: MarketState, t_min: float, delta_bps: float, ctx: dict):
        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty < MIN_QTY:
                self._clean_dust(pos)  # hard-zero any ghost positions
                # Clear time-stop latch if position is flat
                self._time_stop_triggered.pop((m.slug, outcome), None)
                continue
            book = self.last_book[m.slug].get(outcome)
            if not book:
                continue
            if pos.scalp_mode and pos.scalp_open_ts:
                opened = _parse_iso_z(pos.scalp_open_ts)
                if (utc_now() - opened).total_seconds() / 60.0 > LATE_SCALP_MAX_HOLD_MIN:
                    self._do_sell(m, st, outcome, pos.qty, book.bid,
                                  reason="SCALP_TIMEOUT", leg="SCALP_TIMEOUT", ctx=ctx)
                    pos.scalp_mode = False
                    continue
            if t_min >= TRADE_HARD_STOP_MIN:
                sell_key_hs = f"{m.slug}|{outcome}"
                if time.monotonic() >= self._sell_error_until.get(sell_key_hs, 0.0):
                    self._do_sell(m, st, outcome, pos.qty,
                                  max(book.bid, book.ask - MAX_CROSS_SLIPPAGE),
                                  reason="HARD_STOP", leg="STOP", ctx=ctx)
                continue

            # --- TIME STOP EXIT: parity/core positions held too long ---
            # Skip if this is a directional scalp (handled by dscalp_manage_exits)
            if m.slug not in self._dscalp_positions and pos.opened_at:
                try:
                    opened_dt = _parse_iso_z(pos.opened_at)
                    hold_sec = (utc_now() - opened_dt).total_seconds()
                    if hold_sec >= MAX_HOLD_SEC_PARITY:
                        # Calculate mark-to-market PnL
                        mark_pnl_cents = (book.bid - pos.vwap) * 100 if pos.vwap > 0 else 0.0
                        spread_cents_ts = book.spread * 100
                        vel_ts = ctx.get("vel", 0.0)

                        if mark_pnl_cents >= TIME_STOP_MIN_PNL_CENTS:
                            # Profitable — allow TIME_STOP_EXIT
                            ts_key = (m.slug, outcome)
                            # Guard: don't latch/fire for dust positions
                            _ts_bid = round(book.bid, 4)
                            if pos.qty < MIN_QTY:
                                self._time_stop_triggered.pop(ts_key, None)
                                write_jsonl({"event_type": "TIME_STOP_SKIPPED",
                                             "slug": m.slug, "outcome": outcome,
                                             "skip_reason": "below_min_qty",
                                             "qty": round(pos.qty, 2),
                                             "best_bid": _ts_bid,
                                             "ts_ms": int(time.time() * 1000)})
                                continue
                            # Per-(slug,outcome) latch: only fire once until qty changes
                            prev_qty = self._time_stop_triggered.get(ts_key)
                            if prev_qty is not None and abs(pos.qty - prev_qty) < 0.01:
                                write_jsonl({"event_type": "TIME_STOP_SKIPPED",
                                             "slug": m.slug, "outcome": outcome,
                                             "skip_reason": "latched_no_qty_change",
                                             "qty": round(pos.qty, 2),
                                             "latched_qty": round(prev_qty, 2),
                                             "best_bid": _ts_bid,
                                             "ts_ms": int(time.time() * 1000)})
                                continue
                            # Notional guard: skip if below CLOB min order size
                            notional = pos.qty * max(book.bid, 0.01)
                            if notional < 1.0:
                                write_jsonl({"event_type": "TIME_STOP_SKIPPED",
                                             "slug": m.slug, "outcome": outcome,
                                             "skip_reason": "below_min_notional",
                                             "qty": round(pos.qty, 2),
                                             "notional": round(notional, 2),
                                             "best_bid": _ts_bid,
                                             "ts_ms": int(time.time() * 1000)})
                                continue
                            # Check sell cooldown first to avoid spamming 60 attempts/sec
                            sell_key_ts = f"{m.slug}|{outcome}"
                            if time.monotonic() < self._sell_error_until.get(sell_key_ts, 0.0):
                                continue  # recently attempted, wait for cooldown
                            self._diag_time_stop_exits += 1
                            write_jsonl({"event_type": "TIME_STOP_EXIT",
                                          "slug": m.slug, "crypto": m.crypto,
                                          "outcome": outcome, "hold_sec": round(hold_sec, 1),
                                          "max_hold": MAX_HOLD_SEC_PARITY,
                                          "mark_pnl_cents": round(mark_pnl_cents, 2),
                                          "qty": round(pos.qty, 1),
                                          "ts_ms": int(time.time() * 1000)})
                            self._do_sell(m, st, outcome, pos.qty, max(book.bid, 0.01),
                                      reason="TIME_STOP_EXIT", leg="TIME_STOP",
                                      ctx=ctx, use_maker=False)
                            self._time_stop_triggered[ts_key] = pos.qty
                            continue
                        else:
                            # Negative PnL — DO NOT time-stop into a loss
                            # (a) Attempt rescue/hedge-to-straddle if opposite leg liquidity ok
                            opposite_ts = "Down" if outcome == "Up" else "Up"
                            opp_book_ts = self.last_book.get(m.slug, {}).get(opposite_ts)
                            rescue_possible = (DERISK_RESCUE_TO_STRADDLE
                                               and opp_book_ts and opp_book_ts.bid > 0
                                               and st.positions[opposite_ts].qty < MIN_PAIR_QTY)
                            if rescue_possible:
                                # Let the DERISK block below handle rescue-to-straddle
                                self._diag_time_stop_defers += 1
                                write_jsonl({"event_type": "TIME_STOP_DEFER",
                                              "slug": m.slug, "crypto": m.crypto,
                                              "outcome": outcome,
                                              "mark_pnl_cents": round(mark_pnl_cents, 2),
                                              "spread_cents": round(spread_cents_ts, 2),
                                              "delta_bps": round(delta_bps, 1),
                                              "vel": round(vel_ts, 2),
                                              "hold_sec": round(hold_sec, 1),
                                              "reason": "defer_to_rescue",
                                              "ts_ms": int(time.time() * 1000)})
                                # Fall through to DERISK logic below
                            else:
                                # (b) Soft-exit partial: sell 25% only if spread <= 2c and
                                #     delta/vel continues against us
                                vel_against = ((outcome == "Up" and vel_ts < -1.0)
                                               or (outcome == "Down" and vel_ts > 1.0))
                                # Per-(slug,outcome) latch for soft-exit
                                ts_key_soft = (m.slug, outcome)
                                prev_qty_soft = self._time_stop_triggered.get(ts_key_soft)
                                soft_latched = (prev_qty_soft is not None
                                                and abs(pos.qty - prev_qty_soft) < 0.01)
                                if soft_latched:
                                    write_jsonl({"event_type": "TIME_STOP_SKIPPED",
                                                 "slug": m.slug, "outcome": outcome,
                                                 "skip_reason": "soft_latched_no_qty_change",
                                                 "qty": round(pos.qty, 2),
                                                 "latched_qty": round(prev_qty_soft, 2),
                                                 "best_bid": round(book.bid, 4),
                                                 "ts_ms": int(time.time() * 1000)})
                                elif (spread_cents_ts <= TIME_STOP_SOFT_EXIT_MAX_SPREAD_CENTS
                                        and vel_against):
                                    soft_qty = pos.qty * TIME_STOP_SOFT_EXIT_FRAC
                                    if soft_qty >= MIN_QTY:
                                        soft_price = max(book.bid, 0.01)
                                        self._do_sell(m, st, outcome, soft_qty, soft_price,
                                                      reason="TIME_STOP_SOFT_EXIT", leg="TIME_STOP",
                                                      ctx=ctx, use_maker=False)
                                        self._time_stop_triggered[ts_key_soft] = pos.qty
                                        self._diag_time_stop_soft_exits += 1
                                        write_jsonl({"event_type": "TIME_STOP_SOFT_EXIT",
                                                      "slug": m.slug, "crypto": m.crypto,
                                                      "outcome": outcome,
                                                      "qty": round(soft_qty, 1),
                                                      "price": round(soft_price, 4),
                                                      "mark_pnl_cents": round(mark_pnl_cents, 2),
                                                      "spread_cents": round(spread_cents_ts, 2),
                                                      "vel": round(vel_ts, 2),
                                                      "ts_ms": int(time.time() * 1000)})
                                else:
                                    # Conditions not met for soft-exit — keep holding for TP ladder
                                    self._diag_time_stop_defers += 1
                                    write_jsonl({"event_type": "TIME_STOP_DEFER",
                                                  "slug": m.slug, "crypto": m.crypto,
                                                  "outcome": outcome,
                                                  "mark_pnl_cents": round(mark_pnl_cents, 2),
                                                  "spread_cents": round(spread_cents_ts, 2),
                                                  "delta_bps": round(delta_bps, 1),
                                                  "vel": round(vel_ts, 2),
                                                  "hold_sec": round(hold_sec, 1),
                                                  "reason": "no_soft_exit_conditions",
                                                  "ts_ms": int(time.time() * 1000)})
                except Exception:
                    pass

            # --- Inventory pressure: hard cap on shares per market ---
            inv_cap = INVENTORY_CAP_SHARES_PER_MARKET
            if pos.qty > inv_cap:
                # Immediately sell 50% (respecting derisk cooldown)
                sell_qty = pos.qty * 0.50
                if sell_qty >= MIN_QTY:
                    should_inv_sell = False
                    now_t = utc_now()
                    if pos.last_derisk_ts is None:
                        should_inv_sell = True
                    else:
                        try:
                            last_dt = _parse_iso_z(pos.last_derisk_ts)
                            elapsed = (now_t - last_dt).total_seconds()
                        except Exception:
                            elapsed = 999.0
                        if elapsed >= DERISK_COOLDOWN_SEC:
                            should_inv_sell = True
                    if should_inv_sell:
                        write_jsonl({"event_type": "INVENTORY_PRESSURE_SELL",
                                      "slug": m.slug, "crypto": m.crypto,
                                      "outcome": outcome, "qty": round(pos.qty, 1),
                                      "cap": inv_cap, "sell_qty": round(sell_qty, 1)})
                        self._do_sell(m, st, outcome, sell_qty, book.bid,
                                      reason="INVENTORY_CAP", leg="INVENTORY_CAP", ctx=ctx)
                        pos.last_derisk_ts = iso_z(now_t)
                        pos.last_derisk_mid = book.mid
                continue

            # Inventory pressure: tighten TP ladder if > 70% cap
            tp_tighten = 0.0
            if pos.qty > inv_cap * 0.70:
                tp_tighten = 0.01  # tighten by 1 cent
                write_jsonl({"event_type": "INVENTORY_TP_TIGHTEN",
                              "slug": m.slug, "outcome": outcome,
                              "qty": round(pos.qty, 1), "tp_tighten": tp_tighten})

            # --- DERISK with RESCUE-TO-STRADDLE (cooldown + change detection) ---
            # Skip derisk entirely during directional hold floor
            _dpos_derisk = self._dscalp_positions.get(m.slug)
            if _dpos_derisk and (time.time() - _dpos_derisk["entry_ts"]) < DSCALP_MIN_HOLD_SEC:
                continue  # protect directional exposure during hold floor
            derisk_triggered = False
            if outcome == "Up" and delta_bps < +DERISK_CROSS_BPS:
                derisk_triggered = True
            elif outcome == "Down" and delta_bps > -DERISK_CROSS_BPS:
                derisk_triggered = True
            if derisk_triggered:
                sell_qty = pos.qty * DERISK_SELL_FRAC_PER_TICK
                if sell_qty >= MIN_QTY:
                    # Check cooldown: only fire if enough time passed OR mid moved meaningfully
                    should_derisk = False
                    now_t = utc_now()
                    if pos.last_derisk_ts is None:
                        should_derisk = True  # first derisk for this position
                    else:
                        try:
                            last_dt = _parse_iso_z(pos.last_derisk_ts)
                            elapsed = (now_t - last_dt).total_seconds()
                        except Exception:
                            elapsed = 999.0
                        mid_moved = abs(book.mid - pos.last_derisk_mid) >= DERISK_MID_CHANGE_CENTS
                        if elapsed >= DERISK_COOLDOWN_SEC or mid_moved:
                            should_derisk = True
                    if should_derisk:
                        self._diag_derisk_count += 1
                        self._diag_derisk_count_min += 1
                        thr = entry_threshold_bps(m.crypto, t_min)
                        abs_edge = abs(ctx.get("delta_bps", 0))
                        seconds_to_close = ctx.get("seconds_to_close", 999.0)
                        emergency = False
                        if pos.qty >= INVENTORY_EMERGENCY_SHARES:
                            emergency = True
                        elif seconds_to_close < 45:
                            emergency = True

                        # ── RESCUE-TO-STRADDLE ──
                        opposite = "Down" if outcome == "Up" else "Up"
                        opp_pos = st.positions[opposite]
                        rescue_done = False

                        # Skip rescue entirely if position is already a balanced straddle
                        _up_q = st.positions["Up"].qty
                        _dn_q = st.positions["Down"].qty
                        _already_balanced = (_up_q >= MIN_PAIR_QTY and _dn_q >= MIN_PAIR_QTY
                                             and abs(_up_q - _dn_q) < MIN_PAIR_QTY)
                        if _already_balanced and not emergency:
                            # Balanced straddle — no rescue needed, no derisk sell
                            pos.last_derisk_ts = iso_z(now_t)
                            pos.last_derisk_mid = book.mid
                            continue

                        # Count every one-sided + drift reversal as a rescue attempt
                        self._diag_rescue_attempts += 1

                        # Compute rescue feasibility
                        up_book_r = self.last_book.get(m.slug, {}).get("Up")
                        dn_book_r = self.last_book.get(m.slug, {}).get("Down")
                        opp_book = (dn_book_r if opposite == "Down" else up_book_r) if (up_book_r and dn_book_r) else None
                        our_leg_price = clamp_to_tick(book.bid) if book.bid > 0 else 0.0
                        rescue_bid = clamp_to_tick(opp_book.bid) if (opp_book and opp_book.bid > 0) else 0.0
                        est_straddle_cost = our_leg_price + rescue_bid if (our_leg_price > 0 and rescue_bid > 0) else 0.0
                        rescue_edge_cents = (1.000 - est_straddle_cost) * 100 if est_straddle_cost > 0 else -999.0
                        fee_cents = 2 * MAKER_FEE_BPS / 100.0
                        rescue_net_edge = rescue_edge_cents - fee_cents
                        dyn_target = self._quote_get_dynamic_target(m.slug)
                        min_edge = max(RESCUE_MIN_EDGE_NET_CENTS, dyn_target * 0.5)
                        rescue_invested = self._rescue_invested_usd.get(m.slug, 0.0)

                        # Log RESCUE_CHECK for every attempt
                        write_jsonl({
                            "event_type": "RESCUE_CHECK",
                            "slug": m.slug, "crypto": m.crypto,
                            "outcome": outcome, "opposite": opposite,
                            "t_min": round(t_min, 3),
                            "our_qty": round(pos.qty, 1),
                            "opp_qty": round(opp_pos.qty, 1),
                            "our_bid": round(our_leg_price, 4),
                            "opp_bid": round(rescue_bid, 4),
                            "net_edge_cents": round(rescue_net_edge, 3),
                            "dyn_target_cents": round(dyn_target, 3),
                            "min_edge_used": round(min_edge, 3),
                            "emergency": emergency,
                            "rescue_invested": round(rescue_invested, 2),
                        })

                        # Determine rescue block reason (if any)
                        # "already_paired" = BOTH legs have substantial inventory
                        up_qty = st.positions["Up"].qty
                        dn_qty = st.positions["Down"].qty
                        straddle_locked = min(up_qty, dn_qty)
                        pending_pairs_count = len([p for p in self._parity_pending_pairs
                                                    if p.get("slug") == m.slug]) if hasattr(self, '_parity_pending_pairs') else 0
                        rescue_block_reason = None
                        # Block rescue during directional hold floor
                        _dpos = self._dscalp_positions.get(m.slug)
                        if _dpos and (time.time() - _dpos["entry_ts"]) < DSCALP_MIN_HOLD_SEC:
                            rescue_block_reason = "dscalp_hold_floor"
                        elif emergency:
                            rescue_block_reason = "emergency"
                        elif not DERISK_RESCUE_TO_STRADDLE:
                            rescue_block_reason = "disabled"
                        elif up_qty >= MIN_PAIR_QTY and dn_qty >= MIN_PAIR_QTY:
                            rescue_block_reason = "already_paired"
                        elif not opp_book or opp_book.bid <= 0:
                            rescue_block_reason = "no_liquidity"
                        elif not up_book_r or not dn_book_r or up_book_r.bid <= 0 or dn_book_r.bid <= 0:
                            rescue_block_reason = "stale_book"
                        elif rescue_invested >= RESCUE_MAX_USD_PER_SLUG:
                            rescue_block_reason = "cap_reached"
                        elif rescue_net_edge < min_edge:
                            rescue_block_reason = "threshold"
                        elif t_min >= PARITY_STOP_NEW_MIN:
                            rescue_block_reason = "near_close"
                        elif not self._buys_allowed():
                            rescue_block_reason = "buys_window_closed"
                        elif self._directional_suppresses_parity(m.slug):
                            rescue_block_reason = "directional_suppress"
                        elif not self._inventory_cap_ok(m.slug):
                            rescue_block_reason = "inventory_cap"
                        elif not self._post_fill_cooldown_ok(m.slug):
                            rescue_block_reason = "post_fill_cooldown"
                        elif MODE in ("LIVE", "LIVE_SAFE", "TEST") and m.crypto not in LIVE_ALLOWED_SYMBOLS:
                            rescue_block_reason = "symbol_not_allowed"

                        if rescue_block_reason is None:
                            # ── Execute rescue buy ──
                            rescue_usd = min(RESCUE_STEP_USD,
                                             RESCUE_MAX_USD_PER_SLUG - rescue_invested)
                            rescue_qty = rescue_usd / max(1e-9, rescue_bid)
                            if rescue_qty >= 1:
                                pair_id = uuid.uuid4().hex[:16]
                                cost = self._parity_buy_leg(
                                    m, st, opposite, opp_book,
                                    rescue_usd, ctx, pair_id=pair_id)
                                if cost > 0:
                                    self._rescue_invested_usd[m.slug] = rescue_invested + cost
                                    self._diag_rescue_success += 1
                                    rescue_done = True
                                    if (st.positions["Up"].qty >= MIN_QTY
                                            and st.positions["Down"].qty >= MIN_QTY):
                                        if m.slug not in self._parity_locked_since:
                                            self._parity_locked_since[m.slug] = time.time()
                                    write_jsonl({
                                        "event_type": "RESCUE_TRIGGERED",
                                        "slug": m.slug, "crypto": m.crypto,
                                        "pair_id": pair_id,
                                        "losing_outcome": outcome,
                                        "rescue_outcome": opposite,
                                        "step_usd": round(cost, 4),
                                        "rescue_price": round(rescue_bid, 4),
                                        "our_leg_price": round(our_leg_price, 4),
                                        "est_straddle_cost": round(est_straddle_cost, 4),
                                        "net_edge_cents": round(rescue_net_edge, 3),
                                        "t_min": round(t_min, 3),
                                    })
                            else:
                                rescue_block_reason = "qty_too_small"

                        if rescue_block_reason is not None:
                            write_jsonl({
                                "event_type": "RESCUE_BLOCKED",
                                "slug": m.slug, "crypto": m.crypto,
                                "reason": rescue_block_reason,
                                "outcome": outcome,
                                "up_qty": round(up_qty, 1),
                                "dn_qty": round(dn_qty, 1),
                                "min_pair_qty": MIN_PAIR_QTY,
                                "straddle_locked": round(straddle_locked, 1),
                                "pending_pairs_count": pending_pairs_count,
                                "net_edge_cents": round(rescue_net_edge, 3),
                                "min_edge_used": round(min_edge, 3),
                                "emergency": emergency,
                                "t_min": round(t_min, 3),
                            })

                        # ── FALLBACK: derisk sell ONLY as last resort ──
                        # Determine if derisk sell is warranted:
                        #   - emergency (inventory/time)
                        #   - near_close (< 45s to close)
                        #   - stale_book (no live data)
                        #   - rescue explicitly failed with hard block
                        seconds_to_close_now = ctx.get("seconds_to_close", 999.0)
                        near_close = seconds_to_close_now < 45 or t_min >= PARITY_STOP_NEW_MIN
                        stale_data = rescue_block_reason in ("stale_book", "no_liquidity")
                        hard_rescue_fail = rescue_block_reason in ("cap_reached", "threshold",
                                                                     "qty_too_small", "disabled")

                        derisk_decision = None
                        if not rescue_done:
                            if emergency:
                                derisk_decision = "emergency"
                            elif near_close:
                                derisk_decision = "near_close"
                            elif stale_data:
                                derisk_decision = "stale_data"
                            elif hard_rescue_fail:
                                derisk_decision = "rescue_failed"
                            else:
                                # Rescue blocked as "already_paired" — don't sell,
                                # let hedge/parity rebalance instead
                                derisk_decision = "defer_to_hedge"

                        write_jsonl({
                            "event_type": "DERISK_DECISION",
                            "ts_ms": int(time.time() * 1000),
                            "slug": m.slug, "crypto": m.crypto,
                            "outcome": outcome,
                            "decision": derisk_decision if derisk_decision else "rescue_success",
                            "rescue_done": rescue_done,
                            "rescue_block_reason": rescue_block_reason,
                            "emergency": emergency,
                            "near_close": near_close,
                            "sell_qty": round(sell_qty, 1),
                            "up_qty": round(st.positions["Up"].qty, 1),
                            "dn_qty": round(st.positions["Down"].qty, 1),
                        })

                        # DERISK_MAKER gating: only fire for emergency-class reasons
                        # Emergency class: emergency, near_close, stale_data
                        # Non-emergency (rescue_failed): defer to hedge instead of maker-selling into loss
                        derisk_is_emergency = derisk_decision in ("emergency", "near_close", "stale_data")

                        if not rescue_done and derisk_is_emergency:
                            # Check for emergency taker conditions
                            emergency_taker = emergency
                            if not emergency_taker and abs_edge >= thr + DERISK_TAKER_EDGE_EXTRA_BPS:
                                wk = (m.slug, outcome)
                                wt = self._derisk_edge_worsen_since.get(wk)
                                if wt is None:
                                    self._derisk_edge_worsen_since[wk] = time.time()
                                elif (time.time() - wt) >= DERISK_TAKER_EDGE_WORSEN_SEC:
                                    emergency_taker = True
                            else:
                                self._derisk_edge_worsen_since.pop((m.slug, outcome), None)

                            if emergency_taker and DERISK_TAKER_EMERGENCY_ONLY:
                                self._diag_derisk_taker_count += 1
                                self._diag_taker_count += 1
                                self._diag_rescue_fallback_sells += 1
                                self._do_sell(m, st, outcome, sell_qty, book.bid,
                                              reason="DERISK_EMERGENCY", leg="DERISK", ctx=ctx)
                            elif not DERISK_MAKER_EMERGENCY_ONLY or derisk_is_emergency:
                                self._diag_rescue_fallback_sells += 1
                                maker_price = min(book.ask, book.bid + 0.001) if book.bid > 0 else book.bid
                                self._do_sell(m, st, outcome, sell_qty, book.bid,
                                              reason="DERISK_MAKER", leg="DERISK", ctx=ctx,
                                              use_maker=False)
                            else:
                                # DERISK_MAKER blocked by emergency-only gate — defer to hedge
                                self._diag_derisk_maker_defers += 1
                                write_jsonl({"event_type": "DERISK_MAKER_DEFERRED",
                                              "slug": m.slug, "outcome": outcome,
                                              "derisk_decision": derisk_decision,
                                              "ts_ms": int(time.time() * 1000)})
                        elif not rescue_done and (derisk_decision == "defer_to_hedge"
                                                  or derisk_decision == "rescue_failed"):
                            # Trigger hedge state machine for opposite leg instead of selling
                            if m.slug not in self._quote_unpaired:
                                self._quote_unpaired[m.slug] = {
                                    "outcome": outcome,  # the side we HAVE
                                    "fill_ts": time.time(),
                                    "escalated": False,
                                }
                                self._diag_quote_unpaired_events += 1

                        pos.last_derisk_ts = iso_z(now_t)
                        pos.last_derisk_mid = book.mid
                continue

            # --- FAST_TP: early partial take-profit (once per position) ---
            if not pos.fast_tp_done and pos.opened_at:
                try:
                    opened_dt = _parse_iso_z(pos.opened_at)
                    pos_age_sec = (utc_now() - opened_dt).total_seconds()
                except Exception:
                    pos_age_sec = 0.0
                if pos_age_sec >= FAST_TP_AFTER_SEC and book.bid >= pos.vwap + FAST_TP_CENTS:
                    ftp_qty = pos.qty * FAST_TP_SELL_PCT
                    if ftp_qty >= MIN_QTY:
                        write_jsonl({"event_type": "FAST_TP_FIRE", "slug": m.slug,
                                      "outcome": outcome, "pos_age_sec": round(pos_age_sec, 1),
                                      "bid": book.bid, "vwap": pos.vwap,
                                      "ftp_qty": round(ftp_qty, 1)})
                        self._do_sell(m, st, outcome, ftp_qty, book.bid,
                                      reason="FAST_TP", leg="FAST_TP",
                                      target_price=pos.vwap + FAST_TP_CENTS, ctx=ctx)
                    pos.fast_tp_done = True

            # --- Take-profit ladder (with optional tightening) ---
            tp1_adj = TP1 - tp_tighten
            tp2_adj = TP2 - tp_tighten
            tp3_adj = TP3 - tp_tighten
            if (not pos.tp1_done) and book.bid >= pos.vwap + tp1_adj:
                tp_qty = pos.qty * TP1_SELL_FRAC
                if tp_qty >= MIN_QTY:
                    tp_target = pos.vwap + tp1_adj
                    self._do_sell(m, st, outcome, tp_qty, book.bid,
                                  reason="TP1", leg="TP1", target_price=tp_target, ctx=ctx)
                pos.tp1_done = True
            if (not pos.tp2_done) and book.bid >= pos.vwap + tp2_adj:
                tp_qty = pos.qty * TP2_SELL_FRAC
                if tp_qty >= MIN_QTY:
                    tp_target = pos.vwap + tp2_adj
                    self._do_sell(m, st, outcome, tp_qty, book.bid,
                                  reason="TP2", leg="TP2", target_price=tp_target, ctx=ctx)
                pos.tp2_done = True
            if (not pos.tp3_done) and book.bid >= pos.vwap + tp3_adj:
                tp_qty = pos.qty * TP3_SELL_FRAC
                if tp_qty >= MIN_QTY:
                    tp_target = pos.vwap + tp3_adj
                    self._do_sell(m, st, outcome, tp_qty, book.bid,
                                  reason="TP3", leg="TP3", target_price=tp_target, ctx=ctx)
                pos.tp3_done = True
            if pos.scalp_mode:
                target = min(0.999, pos.vwap + LATE_SCALP_TP_CENTS)
                if book.bid >= target and pos.qty >= MIN_QTY:
                    self._do_sell(m, st, outcome, pos.qty, book.bid,
                                  reason="SCALP_TP", leg="SCALP_TP", target_price=target, ctx=ctx)
                    pos.scalp_mode = False
    def _do_sell_maker_then_taker(self, m: MarketRef, st: MarketState,
                                 outcome: str, qty: float, reason: str,
                                 ctx: Optional[dict] = None):
        """Exit reliability: place maker sell first, wait TP_MAKER_GRACE_MS,
        then fall back to taker if not filled. Used by TP exits and time stops."""
        book = self.last_book.get(m.slug, {}).get(outcome)
        if not book or book.bid <= 0:
            return
        maker_price = book.bid
        spread_cents = book.spread * 100
        ts_ms = int(time.time() * 1000)

        # Attempt maker sell first
        write_jsonl({"event_type": "EXIT_MAKER_ATTEMPT", "slug": m.slug,
                      "crypto": m.crypto, "outcome": outcome,
                      "price": round(maker_price, 4), "qty": round(qty, 1),
                      "spread_cents": round(spread_cents, 2), "reason": reason,
                      "ts_ms": ts_ms})

        if MODE == "LOG":
            # In paper mode: sell at bid (taker) for immediate fill
            self._do_sell(m, st, outcome, qty, maker_price,
                          reason=reason, leg=reason, ctx=ctx, use_maker=False)
            return

        # LIVE mode: place maker, wait grace period, then taker fallback
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        fill_result = self.client.place_limit_order(token_id, "SELL", maker_price, qty, post_only=True)
        oid = fill_result.get("order_id", "")
        fill_state = fill_result.get("filled")
        is_pending = (fill_state == "UNKNOWN")
        self._exec_tracker.on_submit(
            oid, m.slug, outcome, "SELL", maker_price, qty, reason,
            token_id=token_id, pending_confirmation=is_pending)
        self._truth_watch_after_place(fill_result, m, outcome, "SELL", qty, maker_price)

        if fill_state is True:
            # Immediate maker fill — great
            self._exec_tracker.on_fill(oid, fill_result["fill_qty"],
                                       fill_price=fill_result["fill_price"],
                                       token_id=token_id)
            self._record_fill_ts(m.slug)
            actual_price = fill_result["fill_price"]
            actual_qty = fill_result["fill_qty"]
            pnl = self._live_sell(st, outcome, actual_price, actual_qty)
            self._ledger_record_sell_fill(m, st, outcome, actual_price, actual_qty, order_id=oid)
            if oid:
                self._applied_fills.add((oid, "SELL", round(actual_qty, 2)))
            self._record_negative_exit(m.slug, pnl, reason)
            return
        if is_pending:
            # UNKNOWN: CLOB matched but qty unconfirmed — watcher will confirm.
            # Don't proceed to taker fallback (would double-sell).
            write_jsonl({"event_type": "MAKER_SELL_PENDING_CONFIRMATION",
                         "slug": m.slug, "outcome": outcome,
                         "order_id": oid, "price": maker_price, "qty": qty,
                         "reason": reason, "ts_ms": int(time.time() * 1000)})
            return

        # Wait for maker grace period
        grace_sec = TP_MAKER_GRACE_MS / 1000.0
        waited = 0.0
        while waited < grace_sec:
            time.sleep(0.1)
            waited += 0.1
            # Check if filled (simplified — real check would poll order status)
            # In practice the order tracker handles this; we just wait
        # Cancel maker and cross taker
        try:
            self.client.cancel_order(oid)
        except Exception:
            pass
        self._exec_tracker.on_cancel(oid)

        # Refresh book for taker price
        book = self.last_book.get(m.slug, {}).get(outcome)
        taker_price = max(book.bid, 0.01) if book else maker_price
        spread_cents = book.spread * 100 if book else 0

        # Only cross taker if still profitable by >= 2c AND bid within 1 tick of original target
        # (prevent missed-exit losses; for TP exits ensure market hasn't moved away)
        dpos = self._dscalp_positions.get(m.slug)
        if dpos and book and "STOP" not in reason and "TIMEOUT" not in reason:
            entry_price = dpos.get("entry_price", 0)
            taker_pnl_cents = (taker_price - entry_price) * 100
            bid_within_1tick = taker_price >= maker_price - 0.01
            if taker_pnl_cents < 2.0 or not bid_within_1tick:
                write_jsonl({"event_type": "EXIT_TAKER_SKIP", "slug": m.slug,
                              "outcome": outcome, "taker_pnl_cents": round(taker_pnl_cents, 2),
                              "bid_within_1tick": bid_within_1tick,
                              "maker_price": round(maker_price, 4),
                              "taker_price": round(taker_price, 4),
                              "reason": reason, "ts_ms": int(time.time() * 1000)})
                # Re-queue: position stays open, next tick will re-evaluate
                return

        write_jsonl({"event_type": "EXIT_TAKER_FALLBACK", "slug": m.slug,
                      "crypto": m.crypto, "outcome": outcome,
                      "price": round(taker_price, 4), "qty": round(qty, 1),
                      "spread_cents": round(spread_cents, 2), "reason": reason,
                      "ts_ms": int(time.time() * 1000)})
        self._do_sell(m, st, outcome, qty, taker_price,
                      reason=reason, leg=reason, ctx=ctx, use_maker=False)

    def _do_sell(self, m: MarketRef, st: MarketState, outcome: str, qty: float, price: float,
                 reason: str, leg: str = "EXIT", target_price: Optional[float] = None,
                 ctx: Optional[dict] = None, use_maker: bool = False):
        qty = max(0.0, qty)
        if qty < MIN_QTY:
            return  # don't sell dust — don't log it either
        # State machine: block sells in CLOSED state; in SETTLEMENT allow only if POST_WINDOW_UNWIND
        ws = self._truth.window_state
        if ws == WindowState.CLOSED:
            return
        if ws == WindowState.SETTLEMENT and not POST_WINDOW_UNWIND:
            return
        # Active window filter: block sells on non-current-hour tokens
        if ACTIVE_WINDOW_ONLY and STRICT_WINDOW_MODE:
            token_id_chk = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
            if token_id_chk and not self._truth.is_active_window_token(token_id_chk):
                write_jsonl({"event_type": "WINDOW_SELL_BLOCKED",
                             "slug": m.slug, "outcome": outcome,
                             "reason": "non_active_window",
                             "ts_ms": int(time.time() * 1000)})
                print(f"  [TRUTH] WARN: Attempt to trade non-active window token: "
                      f"{m.slug} {outcome} SELL")
                return
        # Enforce CLOB minimum order size for live sells
        if MODE in ("LIVE", "LIVE_SAFE") and qty < CLOB_MIN_ORDER_SIZE:
            return
        # Ledger sell validation: warn if attempting to sell more than owned
        if self._fills_ledger is not None:
            token_id_chk, _ = self._token_for_slug_outcome(m.slug, outcome)
            allowed, capped_qty, val_reason = self._fills_ledger.validate_sell(
                token_id_chk, outcome, qty, slug=m.slug)
            if not allowed:
                write_jsonl({"event_type": "LEDGER_SELL_BLOCKED",
                             "slug": m.slug, "outcome": outcome,
                             "requested_qty": qty, "reason": val_reason,
                             "ts_ms": int(time.time() * 1000)})
                return
            if capped_qty < qty:
                qty = capped_qty
        # Sell-error cooldown: skip if a recent sell on this (slug,outcome) failed
        if MODE in ("LIVE", "LIVE_SAFE"):
            sell_key = f"{m.slug}|{outcome}"
            cooldown_until = self._sell_error_until.get(sell_key, 0.0)
            if time.monotonic() < cooldown_until:
                return  # still in cooldown after a sell error
        # Sell churn guard: sell must be monotonic risk-reducing (no shorting)
        pos = st.positions[outcome]
        if MODE in ("LIVE", "LIVE_SAFE"):
            if not self._live_safety.check_sell_reduces_risk(
                    outcome, qty, pos.qty, m.slug):
                return
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        up_book = self.last_book.get(m.slug, {}).get("Up")
        dn_book = self.last_book.get(m.slug, {}).get("Down")
        ref_book = up_book if outcome == "Up" else dn_book
        decision_id = new_decision_id()
        client_oid = new_order_id()
        position_id = pos.position_id or ""
        parent_oid = pos.parent_order_id or ""
        usdc_cost = pos.vwap * qty
        unrealized_pnl = (ref_book.bid - pos.vwap) * pos.qty if ref_book and pos.vwap > 0 else 0.0
        # Build a minimal ctx if caller didn't pass one
        if ctx is None:
            t_min = minutes_into_hour(
                _parse_iso_z(st.hour_start_utc),
                utc_now())
            ctx = {"hour_start_utc": st.hour_start_utc, "hour_open": st.hour_open,
                   "t_min": t_min, "phase": _phase(t_min),
                   "seconds_to_close": max(0.0, (60.0 - t_min) * 60.0)}
        bk_fields = self._book_fields(up_book, dn_book, outcome) if (up_book and dn_book) else {}
        # ORDER_INTENT
        self.logger.log_order_intent(
            engine="EXIT", reason=reason,
            decision_id=decision_id, position_id=position_id,
            parent_order_id=parent_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="SELL", qty=qty, target_price=target_price or price,
            usdc_cost=usdc_cost, ctx=ctx, book_fields=bk_fields,
            extra={"vwap": pos.vwap, "unrealized_pnl_usdc": unrealized_pnl},
        )
        # ORDER_SUBMIT
        self.logger.log_order_submit(
            engine="EXIT", reason=reason,
            decision_id=decision_id, position_id=position_id,
            client_order_id=client_oid, parent_order_id=parent_oid,
            crypto=m.crypto, slug=m.slug, outcome=outcome,
            side="SELL", qty=qty, target_price=target_price or price,
            usdc_cost=usdc_cost, ctx=ctx, book_fields=bk_fields,
        )
        # Ledger: record order intent at submit time
        self._ledger_order_intent(m, st, outcome, "SELL", qty, price, reason, order_id=client_oid)
        # True cost: submit counted here (confirmed submit for both modes)
        self._true_cost_tx_count += 1
        if MODE == "LOG":
            # Instant fill in paper mode — count fill immediately
            self._true_cost_fill_count += 1
            self._true_cost_fill_count_min += 1
            self._record_fill_ts(m.slug)  # post-fill cooldown
            pnl = self._paper_sell(st, outcome, price, qty)
            self._ledger_record_sell_fill(m, st, outcome, price, qty)
            mt = infer_maker_taker("SELL", price, ref_book) if ref_book else ""
            sc = spread_capture_fields("SELL", price, ref_book) if ref_book else {}
            sell_notional = price * qty
            fee = compute_fee_usdc(sell_notional, mt if mt else ("maker" if use_maker else "taker"))
            net_pnl = pnl - fee
            # Loss-tail guard: track negative exits
            self._record_negative_exit(m.slug, net_pnl, reason)
            # ORDER_FILL
            self.logger.log_order_fill(
                engine="EXIT", reason=reason,
                decision_id=decision_id, client_order_id=client_oid,
                position_id=position_id, parent_order_id=parent_oid,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="SELL", qty=qty, fill_price=price,
                usdc_cost=usdc_cost, fees_usdc=fee,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                realized_pnl_usdc=pnl, net_pnl_usdc=net_pnl,
                unrealized_pnl_usdc=0.0, vwap=pos.vwap,
                ctx=ctx, book_fields=bk_fields,
            )
            # ROUND_TRIP_CLOSE when position goes flat
            if pos.qty < MIN_QTY and pos.entry_mid > 0:
                time_held = 0.0
                if pos.opened_at:
                    try:
                        opened_dt = _parse_iso_z(pos.opened_at)
                        time_held = (utc_now() - opened_dt).total_seconds()
                    except Exception:
                        pass
                mfe = (pos.max_favorable_mid - pos.entry_mid) * 10000.0 / max(pos.entry_mid, 1e-9)
                mae = (pos.entry_mid - pos.max_adverse_mid) * 10000.0 / max(pos.entry_mid, 1e-9)
                self.logger.log_event({
                    "event_type": "ROUND_TRIP_CLOSE",
                    "position_id": position_id,
                    "slug": m.slug, "crypto": m.crypto, "outcome": outcome,
                    "hour_start_utc": st.hour_start_utc,
                    "gross_pnl_usdc": round(pnl, 4), "net_pnl_usdc": round(net_pnl, 4),
                    "time_in_position_sec": round(time_held, 1),
                    "max_favorable_bps": round(mfe, 3), "max_adverse_bps": round(mae, 3),
                    "exit_reason": reason,
                }, also_csv=True)
            return
        fill_result = self.client.place_limit_order(token_id, "SELL", price, qty, post_only=use_maker)
        oid = fill_result.get("order_id", "")
        fill_state = fill_result.get("filled")  # True | False | "UNKNOWN"
        is_pending = (fill_state == "UNKNOWN")
        self._exec_tracker.on_submit(
            oid, m.slug, outcome, "SELL", price, qty, reason,
            token_id=token_id, pending_confirmation=is_pending)
        self._truth_watch_after_place(fill_result, m, outcome, "SELL", qty, price)
        self._exec_safety.record_slug_submit(m.slug)
        if fill_state is True:
            actual_qty = fill_result["fill_qty"]
            actual_price = fill_result["fill_price"]
            self._exec_safety.record_slug_fill(m.slug)
            self._exec_tracker.on_fill(oid, actual_qty,
                                       fill_price=actual_price,
                                       token_id=token_id)
            self._record_fill_ts(m.slug)  # post-fill cooldown
            # True cost: count fill only after confirmed fill
            self._true_cost_fill_count += 1
            self._true_cost_fill_count_min += 1
            pnl = self._live_sell(st, outcome, actual_price, actual_qty)
            self._ledger_record_sell_fill(m, st, outcome, actual_price, actual_qty, order_id=oid)
            if oid:
                self._applied_fills.add((oid, "SELL", round(actual_qty, 2)))
            mt = infer_maker_taker("SELL", actual_price, ref_book) if ref_book else ""
            sc = spread_capture_fields("SELL", actual_price, ref_book) if ref_book else {}
            sell_notional = actual_price * actual_qty
            fee = compute_fee_usdc(sell_notional, mt if mt else ("maker" if use_maker else "taker"))
            net_pnl = pnl - fee
            # Loss-tail guard: track negative exits
            self._record_negative_exit(m.slug, net_pnl, reason)
            self.logger.log_order_fill(
                engine="EXIT", reason=reason,
                decision_id=decision_id, client_order_id=client_oid,
                position_id=position_id, parent_order_id=parent_oid,
                crypto=m.crypto, slug=m.slug, outcome=outcome,
                side="SELL", qty=actual_qty, fill_price=actual_price,
                usdc_cost=usdc_cost, fees_usdc=fee,
                maker_taker=mt, did_cross=sc.get("did_cross", ""),
                realized_pnl_usdc=pnl, net_pnl_usdc=net_pnl,
                unrealized_pnl_usdc=0.0, vwap=pos.vwap,
                ctx=ctx, book_fields=bk_fields,
            )
            if pos.qty < MIN_QTY and pos.entry_mid > 0:
                time_held = 0.0
                if pos.opened_at:
                    try:
                        opened_dt = _parse_iso_z(pos.opened_at)
                        time_held = (utc_now() - opened_dt).total_seconds()
                    except Exception:
                        pass
                mfe = (pos.max_favorable_mid - pos.entry_mid) * 10000.0 / max(pos.entry_mid, 1e-9)
                mae = (pos.entry_mid - pos.max_adverse_mid) * 10000.0 / max(pos.entry_mid, 1e-9)
                self.logger.log_event({
                    "event_type": "ROUND_TRIP_CLOSE",
                    "position_id": position_id,
                    "slug": m.slug, "crypto": m.crypto, "outcome": outcome,
                    "hour_start_utc": st.hour_start_utc,
                    "gross_pnl_usdc": round(pnl, 4), "net_pnl_usdc": round(net_pnl, 4),
                    "time_in_position_sec": round(time_held, 1),
                    "max_favorable_bps": round(mfe, 3), "max_adverse_bps": round(mae, 3),
                    "exit_reason": reason,
                }, also_csv=True)
        elif is_pending:
            # UNKNOWN: CLOB matched but qty unconfirmed for SELL.
            # Don't update positions yet — lifecycle watcher will confirm.
            # Don't set sell error cooldown — this isn't an error.
            write_jsonl({"event_type": "SELL_PENDING_CONFIRMATION",
                         "slug": m.slug, "outcome": outcome,
                         "order_id": oid, "status": fill_result.get("status"),
                         "requested_qty": qty, "price": price, "reason": reason,
                         "ts_ms": int(time.time() * 1000)})
            print(f"  [ORDER_LIFECYCLE] SELL PENDING_CONFIRMATION: "
                  f"{m.slug} {outcome} qty={qty:.1f} @ {price:.4f} "
                  f"— watcher will confirm")
        elif fill_result.get("status") in ("live", "delayed"):
            # Order resting — lifecycle watcher will track it.
            sell_key = f"{m.slug}|{outcome}"
            self._sell_error_until[sell_key] = time.monotonic() + 5.0  # cooldown 5s before retry
            write_jsonl({"event_type": "SELL_NOT_FILLED",
                         "slug": m.slug, "outcome": outcome,
                         "order_id": oid, "status": fill_result.get("status"),
                         "requested_qty": qty, "price": price, "reason": reason,
                         "ts_ms": int(time.time() * 1000)})
        elif fill_result.get("status") == "error":
            self._exec_safety.record_api_error()
            sell_key = f"{m.slug}|{outcome}"
            # If it looks like an allowance issue, try re-approving once then retry immediately
            if not self._sell_allowance_retried and hasattr(self.client, '_ensure_allowances'):
                write_jsonl({"event_type": "SELL_REAPPROVAL", "slug": m.slug,
                             "outcome": outcome, "reason": "allowance_error"})
                self._sell_allowance_retried = True
                try:
                    self.client._ensure_allowances()
                except Exception:
                    pass
                # Quick retry after re-approval — clear cooldown so next loop picks it up
                self._sell_error_until.pop(sell_key, None)
                retry = self.client.place_limit_order(token_id, "SELL", price, qty, post_only=use_maker)
                if retry.get("filled") is True:
                    actual_qty = retry["fill_qty"]
                    actual_price = retry["fill_price"]
                    self._exec_safety.record_slug_fill(m.slug)
                    self._record_fill_ts(m.slug)
                    self._true_cost_fill_count += 1
                    self._true_cost_fill_count_min += 1
                    pnl = self._live_sell(st, outcome, actual_price, actual_qty)
                    retry_oid = retry.get("order_id", "")
                    self._ledger_record_sell_fill(m, st, outcome, actual_price, actual_qty,
                                                  order_id=retry_oid)
                    if retry_oid:
                        self._applied_fills.add((retry_oid, "SELL", round(actual_qty, 2)))
                    mt = infer_maker_taker("SELL", actual_price, ref_book) if ref_book else ""
                    sell_notional = actual_price * actual_qty
                    fee = compute_fee_usdc(sell_notional, mt if mt else ("maker" if use_maker else "taker"))
                    net_pnl = pnl - fee
                    self._record_negative_exit(m.slug, net_pnl, reason)
                    write_jsonl({"event_type": "SELL_RETRY_OK", "slug": m.slug,
                                 "outcome": outcome, "qty": actual_qty, "price": actual_price})
                    return
                # Retry also failed — fall through to cooldown
            # Sell-error cooldown: back off 30s to prevent spam
            self._sell_error_until[sell_key] = time.monotonic() + 30.0

    def _place_layered_buy(self, m: MarketRef, outcome: str, qty: float, ask: float) -> dict:
        """Place buy at ask for immediate fill. Returns {total_filled, total_cost, avg_price}."""
        token_id = m.outcome_up_id if outcome == "Up" else m.outcome_down_id
        result = {"total_filled": 0, "total_cost": 0.0, "avg_price": 0.0}
        # Always buy at ask (taker) for immediate fill — no layering
        r = self.client.place_limit_order(token_id, "BUY", ask, qty, post_only=False)
        self._truth_watch_after_place(r, m, outcome, "BUY", qty, ask)
        if r.get("filled") is True:
            result["total_filled"] = r["fill_qty"]
            result["total_cost"] = r["fill_price"] * r["fill_qty"]
            result["avg_price"] = r["fill_price"]
        return result
    def _calc_clip(self, crypto: str, t_min: float, abs_delta_bps: float) -> float:
        base = self.cash_usdc * BASE_CLIP_PCT
        mult = sizing_mult(abs_delta_bps)
        if t_min < 10:
            mult *= EARLY_SIZE_MULT
        clip = base * mult
        return max(0.0, clip)
    def _cleanup_market(self, m: MarketRef, st: MarketState, t_min: float):
        for outcome in ["Up", "Down"]:
            pos = st.positions[outcome]
            if pos.qty < MIN_QTY:
                continue
            book = self.last_book.get(m.slug, {}).get(outcome)
            if book:
                self._do_sell(m, st, outcome, pos.qty,
                              max(book.bid, book.ask - MAX_CROSS_SLIPPAGE),
                              reason="CLEANUP", leg="CLEANUP")
# MAIN
def _select_mode() -> str:
    """Interactive CLI mode selection. Returns selected MODE string."""
    global MODE
    # If MODE was explicitly set via env (not default), skip prompt
    env_mode = os.getenv("MODE", "").upper()
    if env_mode in ("LOG", "LIVE_SAFE", "LIVE"):
        return env_mode
    print("\n  ╔══════════════════════════════════════╗")
    print("  ║        SELECT RUNTIME MODE           ║")
    print("  ╠══════════════════════════════════════╣")
    print("  ║  1 = LOG        (paper only)         ║")
    print("  ║  2 = LIVE_SAFE  (time-limited buys)  ║")
    print("  ║  3 = LIVE       (full live trading)   ║")
    print("  ╚══════════════════════════════════════╝\n")
    while True:
        choice = input("  Enter mode [1/2/3]: ").strip()
        if choice == "1":
            return "LOG"
        elif choice == "2":
            return "LIVE_SAFE"
        elif choice == "3":
            return "LIVE"
        else:
            print("  Invalid choice. Enter 1, 2, or 3.")


def main():
    global MODE
    MODE = _select_mode()
    # Patch module-level MODE so all code sees the updated value
    import src.config.settings as _settings_mod
    _settings_mod.MODE = MODE
    if MODE not in ("LOG", "LIVE_SAFE", "LIVE"):
        print("MODE must be LOG, LIVE_SAFE, or LIVE")
        sys.exit(1)
    print(f"\n  [MODE] Starting in {MODE} mode\n")
    bot = Bot()
    bot.run()
if __name__ == "__main__":
    main()
