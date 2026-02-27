"""
logger.py – Structured JSON logging for the F247-style scalper.

Every log line is valid JSON with:
  - ts: UTC ISO-8601 timestamp
  - event: event type tag
  - payload: event-specific data

Supports file output and periodic rollup summaries.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from typing import IO, Optional


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class Logger:
    """Structured JSON logger.  Thread-unsafe (single bot loop)."""

    def __init__(self, log_file: str = "", rollup_sec: int = 60):
        self._fh: Optional[IO] = None
        if log_file:
            self._fh = open(log_file, "a", buffering=1)  # line-buffered
        self._rollup_sec = rollup_sec
        self._last_rollup_ts = time.monotonic()

        # Rollup accumulators
        self._buy_count = 0
        self._sell_count = 0
        self._skip_count = 0
        self._cancel_count = 0
        self._fill_count = 0
        self._api_errors = 0
        self._dry_run_orders = 0

    # ------------------------------------------------------------------
    # Core emit
    # ------------------------------------------------------------------
    def log(self, event: str, **kwargs) -> None:
        """Emit one structured JSON log line."""
        record = {"ts": _utc_iso(), "event": event}
        if kwargs:
            record.update(kwargs)
        line = json.dumps(record, default=str)
        print(line, flush=True)
        if self._fh:
            self._fh.write(line + "\n")

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------
    def feature(self, **kw):
        self.log("FEATURE", **kw)

    def decision(self, action: str, reason: str, **kw):
        if action == "BUY":
            self._buy_count += 1
        elif action == "SELL":
            self._sell_count += 1
        elif action == "SKIP":
            self._skip_count += 1
        self.log("DECISION", action=action, reason=reason, **kw)

    def order_place(self, **kw):
        self.log("ORDER_PLACE", **kw)

    def order_cancel(self, **kw):
        self._cancel_count += 1
        self.log("ORDER_CANCEL", **kw)

    def fill(self, **kw):
        self._fill_count += 1
        self.log("FILL", **kw)

    def inventory(self, **kw):
        self.log("INVENTORY", **kw)

    def risk(self, **kw):
        self.log("RISK", **kw)

    def api_error(self, **kw):
        self._api_errors += 1
        self.log("API_ERROR", **kw)

    def dry_run_order(self, **kw):
        self._dry_run_orders += 1
        self.log("DRY_RUN_ORDER", **kw)

    def info(self, msg: str, **kw):
        self.log("INFO", msg=msg, **kw)

    def warn(self, msg: str, **kw):
        self.log("WARN", msg=msg, **kw)

    def error(self, msg: str, **kw):
        self.log("ERROR", msg=msg, **kw)

    # ------------------------------------------------------------------
    # Periodic rollup
    # ------------------------------------------------------------------
    def maybe_rollup(self, inventory_snapshot: dict | None = None,
                     unrealized_usd: float = 0.0,
                     realized_usd: float = 0.0) -> bool:
        """Emit a periodic rollup if interval has elapsed.  Returns True if emitted."""
        now = time.monotonic()
        if now - self._last_rollup_ts < self._rollup_sec:
            return False
        elapsed = now - self._last_rollup_ts
        self._last_rollup_ts = now
        payload = {
            "period_sec": round(elapsed, 1),
            "buys": self._buy_count,
            "sells": self._sell_count,
            "skips": self._skip_count,
            "cancels": self._cancel_count,
            "fills": self._fill_count,
            "api_errors": self._api_errors,
            "dry_run_orders": self._dry_run_orders,
            "unrealized_usd": round(unrealized_usd, 4),
            "realized_usd": round(realized_usd, 4),
        }
        if inventory_snapshot:
            # top 5 positions by USD value
            sorted_inv = sorted(
                inventory_snapshot.items(),
                key=lambda kv: kv[1].get("usd_value", 0),
                reverse=True,
            )[:5]
            payload["top_positions"] = {k: v for k, v in sorted_inv}
        self.log("ROLLUP", **payload)
        # Reset accumulators
        self._buy_count = 0
        self._sell_count = 0
        self._skip_count = 0
        self._cancel_count = 0
        self._fill_count = 0
        self._api_errors = 0
        self._dry_run_orders = 0
        return True

    # ------------------------------------------------------------------
    # Startup dump
    # ------------------------------------------------------------------
    def log_config(self, config_dict: dict) -> None:
        self.log("CONFIG", **config_dict)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    def close(self):
        if self._fh:
            self._fh.close()
            self._fh = None
