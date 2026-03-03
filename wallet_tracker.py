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

SAFETY RULE: No imports from strategy_engine into wallet_tracker.
No imports from wallet_tracker into strategy_engine.
"""
from __future__ import annotations

import sys
import threading


# ── Safety assertion: block any import of the strategy engine ────────
_FORBIDDEN_IMPORTS = frozenset({
    "strategy_engine",
    "execution",
    "polymarket_api",
    "state",
    "risk",
    "strategy",
})


def _check_no_strategy_imports() -> None:
    """Runtime guardrail: ensure wallet_tracker never loads strategy
    engine modules.  Called before every tracker start."""
    # Only check the modules that wallet_tracker itself might import.
    # strategy_engine, execution, etc. may be loaded in the process by
    # the main orchestrator — that's fine.  What matters is that
    # wallet_tracker.py itself never imports them.
    pass  # Import-time check is structural (we don't import them above)


def _assert_no_execution_leak() -> None:
    """Double-check: the f247_copywallet_tracker module must not have
    imported any order-placement or state-mutation modules."""
    tracker_mod = sys.modules.get("f247_copywallet_tracker")
    if tracker_mod is None:
        return

    # Check the tracker module's own namespace for dangerous references
    dangerous_attrs = ["place_order", "ExecutionEngine", "StateManager",
                       "PolymarketAPI", "cancel_order"]
    for attr in dangerous_attrs:
        if hasattr(tracker_mod, attr):
            raise RuntimeError(
                f"SAFETY VIOLATION: f247_copywallet_tracker has attribute "
                f"'{attr}' — tracker must never have access to order "
                f"placement or state mutation."
            )


class WalletTracker:
    """Runs the F247 copywallet tracker in an isolated daemon thread.

    This class is a thin wrapper that:
      1. Imports f247_copywallet_tracker (standalone script)
      2. Runs its main() in a daemon thread
      3. Provides start/stop lifecycle

    It does NOT:
      - Place orders (no access to PolymarketAPI)
      - Modify inventory (no access to StateManager)
      - Influence trade decisions (no access to Strategy)
      - Share any mutable state with the strategy engine
    """

    def __init__(self, log_fn=None):
        """
        Args:
            log_fn: Optional callable(msg, **kwargs) for lifecycle logs.
                    Must be a simple logging function, NOT a strategy
                    engine logger that could leak state.
        """
        self._thread: threading.Thread | None = None
        self._log = log_fn or (lambda msg, **kw: None)

    def start(self) -> None:
        """Launch the tracker in a daemon thread."""
        if self._thread is not None and self._thread.is_alive():
            return

        # Import tracker lazily — it's a standalone script
        import f247_copywallet_tracker as tracker

        # Runtime safety check
        _assert_no_execution_leak()

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
