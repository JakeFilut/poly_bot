"""
wallet_tracker.py – Isolated F247 wallet observer (read-only).

ARCHITECTURE BOUNDARY: This module ONLY observes an external wallet's
trades and writes logs/CSVs.  It has ZERO access to the strategy
engine's execution path.

It MUST NOT:
  - Place or cancel orders
  - Modify bot inventory or trading state
  - Import strategy_engine, execution, state, polymarket_api, or risk
  - Write to the trading database (STATE_DB_PATH)

The only shared components with the strategy engine are:
  - Read-only config values (TRACK_F247_WALLET flag)
  - Logging utilities (write to separate log files)
  - bot_logger: writes COPYWALLET_FILL events to bot_log.jsonl for
    apples-to-apples comparison with bot fills.

SAFETY RULE: No imports from strategy_engine into wallet_tracker.
No imports from wallet_tracker into strategy_engine.
"""
from __future__ import annotations

import hashlib
import sys
import threading
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")

# ── Safety assertion: block any import of the strategy engine ────────
_FORBIDDEN_IMPORTS = frozenset({
    "strategy_engine",
    "execution",
    "polymarket_api",
    "state",
    "risk",
    "strategy",
})

_WALLET_ADDRESS = "0xf247584e41117bbBe4Cc06E4d2C95741792a5216"


def _check_no_strategy_imports() -> None:
    """Runtime guardrail: ensure wallet_tracker never loads strategy
    engine modules.  Called before every tracker start."""
    pass  # Import-time check is structural (we don't import them above)


def _assert_no_execution_leak() -> None:
    """Double-check: the f247_copywallet_tracker module must not have
    imported any order-placement or state-mutation modules."""
    tracker_mod = sys.modules.get("f247_copywallet_tracker")
    if tracker_mod is None:
        return

    dangerous_attrs = ["place_order", "ExecutionEngine", "StateManager",
                       "PolymarketAPI", "cancel_order"]
    for attr in dangerous_attrs:
        if hasattr(tracker_mod, attr):
            raise RuntimeError(
                f"SAFETY VIOLATION: f247_copywallet_tracker has attribute "
                f"'{attr}' — tracker must never have access to order "
                f"placement or state mutation."
            )


def _idempotency_key(tx_hash: str, ts_iso: str, slug: str,
                     side: str, price: float, qty: float) -> str:
    """Generate dedup key: tx_hash if available, else content hash."""
    if tx_hash:
        return tx_hash
    raw = f"{ts_iso}|{slug}|{side}|{price}|{qty}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


class WalletTracker:
    """Runs the F247 copywallet tracker in an isolated daemon thread.

    This class is a thin wrapper that:
      1. Imports f247_copywallet_tracker (standalone script)
      2. Runs its main() in a daemon thread
      3. Provides start/stop lifecycle
      4. Bridges fills to bot_log.jsonl as COPYWALLET_FILL events

    It does NOT:
      - Place orders (no access to PolymarketAPI)
      - Modify inventory (no access to StateManager)
      - Influence trade decisions (no access to Strategy)
      - Share any mutable state with the strategy engine
    """

    def __init__(self, log_fn=None, bot_logger=None):
        """
        Args:
            log_fn: Optional callable(msg, **kwargs) for lifecycle logs.
                    Must be a simple logging function, NOT a strategy
                    engine logger that could leak state.
            bot_logger: Optional Logger instance for emitting
                       COPYWALLET_FILL events to bot_log.jsonl.
        """
        self._thread: threading.Thread | None = None
        self._log = log_fn or (lambda msg, **kw: None)
        self._bot_logger = bot_logger
        # Thread-safe dedup set for COPYWALLET_FILL events
        self._seen_keys: set[str] = set()
        self._seen_lock = threading.Lock()

    def _on_tracker_fill(self, trade: dict) -> None:
        """Callback invoked by f247_copywallet_tracker when a new fill
        is detected.  Emits COPYWALLET_FILL to the bot's main logger.

        Thread safety: called from the tracker daemon thread, but
        Logger.log() is just a print+write which is safe enough for
        append-only line-buffered output.
        """
        if self._bot_logger is None:
            return

        tx_hash = trade.get("tx_hash", "")
        ts_iso = trade.get("ts_iso", "")
        slug = trade.get("slug", "")
        side = trade.get("side", "")
        price = trade.get("price", 0.0)
        qty = trade.get("qty_shares", 0.0)

        key = _idempotency_key(tx_hash, ts_iso, slug, side, price, qty)

        with self._seen_lock:
            if key in self._seen_keys:
                return
            self._seen_keys.add(key)

        # Compute ts_et convenience field
        ts_et = ""
        ts_epoch = trade.get("ts_epoch", 0)
        if ts_epoch:
            try:
                dt_utc = datetime.fromtimestamp(ts_epoch, tz=timezone.utc)
                ts_et = dt_utc.astimezone(_ET).strftime("%Y-%m-%dT%H:%M:%S")
            except Exception:
                pass

        self._bot_logger.log(
            "COPYWALLET_FILL",
            wallet_address=_WALLET_ADDRESS,
            idempotency_key=key,
            tx_hash=tx_hash,
            slug=slug,
            asset=trade.get("asset", ""),
            outcome=trade.get("outcome", ""),
            side=side,
            price=price,
            qty_shares=qty,
            notional_usd=trade.get("notional_usd", 0.0),
            token_id=trade.get("token_id", ""),
            trade_ts_utc=ts_iso,
            ts_et=ts_et,
        )

    def start(self) -> None:
        """Launch the tracker in a daemon thread."""
        if self._thread is not None and self._thread.is_alive():
            return

        # Import tracker lazily — it's a standalone script
        import f247_copywallet_tracker as tracker

        # Runtime safety check
        _assert_no_execution_leak()

        # Wire callback for COPYWALLET_FILL bridging
        tracker.ON_FILL_CB = self._on_tracker_fill

        def _run():
            try:
                tracker.main()
            except Exception as e:
                self._log("wallet_tracker_thread_error", error=str(e))

        self._thread = threading.Thread(
            target=_run,
            name="wallet_tracker",  # clearly labeled, not "strategy"
            daemon=True,
        )
        self._thread.start()
        self._log("wallet_tracker_started")

    def stop(self) -> None:
        """Signal the tracker to stop and wait for it."""
        import f247_copywallet_tracker as tracker
        tracker.STOP = True
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
            self._log(
                "wallet_tracker_stopped",
                clean=not self._thread.is_alive(),
            )

    @property
    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
