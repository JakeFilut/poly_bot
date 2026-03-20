"""
logger.py – Structured JSON logging for the F247-style scalper.

Every log line is valid JSON with:
  - ts: UTC ISO-8601 timestamp
  - event: event type tag
  - payload: event-specific data

Supports file output, periodic rollup summaries, decision throttling,
and rotating file output.
"""
from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import IO, Optional


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class Logger:
    """Structured JSON logger.  Thread-unsafe (single bot loop)."""

    # Events shown on console in quiet mode (human-readable format)
    _QUIET_CONSOLE_EVENTS = frozenset({
        "FILL", "DRY_FILL", "PORTFOLIO_STATUS",
        "ERROR", "WARN", "API_ERROR",
        "HOURLY_PNL",
    })

    def __init__(self, log_file: str = "", rollup_sec: int = 60,
                 run_id: str = "",
                 log_decisions: bool = False,
                 decision_sample_every_n: int = 200,
                 decision_min_interval_ms: int = 2000,
                 rotate_max_bytes: int = 50_000_000,
                 rotate_backup_count: int = 5,
                 quiet_console: bool = False):
        self._fh: Optional[IO] = None
        self._rotating_handler: Optional[RotatingFileHandler] = None
        if log_file:
            if rotate_max_bytes > 0:
                self._rotating_handler = RotatingFileHandler(
                    log_file,
                    maxBytes=rotate_max_bytes,
                    backupCount=rotate_backup_count,
                    encoding="utf-8",
                )
            else:
                self._fh = open(log_file, "a", buffering=1)  # line-buffered
        self._rollup_sec = rollup_sec
        self._last_rollup_ts = time.monotonic()
        self._run_id = run_id
        self._quiet_console = quiet_console

        # -- Decision throttling config --
        self._log_decisions = log_decisions
        self._decision_sample_n = decision_sample_every_n
        self._decision_min_interval_ms = decision_min_interval_ms

        # -- Decision throttling state --
        # Per-(slug, reason) tick counter for sampling
        self._decision_skip_counter: dict[str, int] = defaultdict(int)
        # Per-(slug, reason) last log time (monotonic ms) for time-based throttle
        self._decision_skip_last_log_ms: dict[str, float] = {}
        # Per-(slug, reason) last reason for change detection
        self._decision_last_reason: dict[str, str] = {}

        # -- Decision rollup accumulators (reset each rollup) --
        self._decision_action_counts: dict[str, int] = defaultdict(int)
        self._decision_skip_reasons: dict[str, int] = defaultdict(int)
        self._decision_asset_counts: dict[str, int] = defaultdict(int)

        # Rollup accumulators
        self._buy_count = 0
        self._sell_count = 0
        self._skip_count = 0
        self._cancel_count = 0
        self._fill_count = 0
        self._api_errors = 0
        self._dry_run_orders = 0

        # Hourly fill tracking
        self._hourly_buy_fills = 0
        self._hourly_sell_fills = 0
        self._hourly_gross_buy_usd = 0.0
        self._hourly_gross_sell_usd = 0.0

        # Rollup-period fill tracking (reset each rollup)
        self._rollup_buy_fills = 0
        self._rollup_sell_fills = 0

    # ------------------------------------------------------------------
    # Core emit
    # ------------------------------------------------------------------
    def log(self, event: str, **kwargs) -> None:
        """Emit one structured JSON log line."""
        ts = _utc_iso()
        record = {"ts": ts, "event": event}
        if self._run_id:
            record["run_id"] = self._run_id
        if kwargs:
            record.update(kwargs)
        line = json.dumps(record, default=str)

        # Console output
        if self._quiet_console:
            pretty = self._format_console(event, ts, kwargs)
            if pretty is not None:
                print(pretty, flush=True)
        else:
            print(line, flush=True)

        # Always write to file
        if self._rotating_handler is not None:
            self._rotating_handler.emit(
                _make_log_record(line)
            )
        elif self._fh:
            self._fh.write(line + "\n")

    @staticmethod
    def _format_console(event: str, ts: str, kw: dict) -> str | None:
        """Return a human-readable console line, or None to suppress."""
        t = ts[11:19]  # HH:MM:SS

        if event in ("FILL", "DRY_FILL"):
            side = kw.get("side", "?")
            slug = kw.get("slug", "?")
            outcome = kw.get("outcome", "")
            price = kw.get("fill_price", kw.get("price", 0))
            shares = kw.get("fill_shares", kw.get("qty", 0))
            usd = shares * price if shares and price else 0
            return f"{t}  {side:<4} {slug} {outcome}  {shares}@${price:.2f} (${usd:.2f})"

        if event == "PORTFOLIO_STATUS":
            pos = kw.get("position_count", 0)
            pnl = kw.get("total_pnl_usd", 0)
            real = kw.get("realized_pnl_usd", 0)
            unreal = kw.get("unrealized_pnl_usd", 0)
            spent = kw.get("budget_spent_usd", 0)
            remaining = kw.get("budget_remaining_usd", 0)
            parts = [
                f"{t}  PORTFOLIO  positions={pos}",
                f"pnl=${pnl:+.2f} (real=${real:+.2f} unreal=${unreal:+.2f})",
                f"budget=${spent:.0f}/{spent + remaining:.0f}",
            ]
            return "  ".join(parts)

        if event == "HOURLY_PNL":
            hour = kw.get("hour_et", "?")
            pnl = kw.get("realized_pnl_usd", kw.get("pnl_usd", 0))
            buys = kw.get("total_buy_fills", kw.get("buys", 0))
            sells = kw.get("total_sell_fills", kw.get("sells", 0))
            return f"{t}  HOURLY PNL  hour={hour}  pnl=${pnl:+.2f}  buys={buys} sells={sells}"

        if event in ("ERROR", "API_ERROR"):
            msg = kw.get("msg", kw.get("error", str(kw)))
            return f"{t}  ERROR  {msg}"

        if event == "WARN":
            msg = kw.get("msg", str(kw))
            return f"{t}  WARN  {msg}"

        return None

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------
    def feature(self, **kw):
        self.log("FEATURE", **kw)

    def decision(self, action: str, reason: str, **kw):
        # Always count for rollup (even when not logging the line)
        self._decision_action_counts[action] += 1
        if action == "SKIP":
            self._decision_skip_reasons[reason] += 1
        slug = kw.get("slug", "?")
        asset = kw.get("asset", "?")
        self._decision_asset_counts[f"{slug}:{asset}"] += 1

        if action == "BUY":
            self._buy_count += 1
        elif action == "SELL":
            self._sell_count += 1
        elif action == "SKIP":
            self._skip_count += 1

        # -- Decide whether to actually emit the log line --
        if action != "SKIP":
            # Non-SKIP decisions always logged
            self.log("DECISION", action=action, reason=reason, **kw)
            return

        # SKIP decisions: throttle unless LOG_DECISIONS is True
        if self._log_decisions:
            self.log("DECISION", action=action, reason=reason, **kw)
            return

        # Staleness SKIPs get special treatment (throttled to once per 2s per asset)
        is_stale = reason in ("stale_book", "stale_binance", "stale_universe")

        throttle_key = f"{slug}:{reason}"
        now_ms = time.monotonic() * 1000.0

        # Log if reason changed for this slug
        prev_reason = self._decision_last_reason.get(slug)
        self._decision_last_reason[slug] = reason
        if prev_reason is not None and prev_reason != reason:
            self.log("DECISION", action=action, reason=reason, **kw)
            self._decision_skip_last_log_ms[throttle_key] = now_ms
            self._decision_skip_counter[throttle_key] = 0
            return

        # Time-based throttle: log at most once per DECISION_LOG_MIN_INTERVAL_MS
        last_log_ms = self._decision_skip_last_log_ms.get(throttle_key, 0.0)
        interval = 2000 if is_stale else self._decision_min_interval_ms
        if now_ms - last_log_ms >= interval:
            suppressed = self._decision_skip_counter.get(throttle_key, 0)
            if suppressed > 0:
                kw["suppressed_since_last"] = suppressed
            self.log("DECISION", action=action, reason=reason, **kw)
            self._decision_skip_last_log_ms[throttle_key] = now_ms
            self._decision_skip_counter[throttle_key] = 0
            return

        # Sample-based: log every N ticks
        self._decision_skip_counter[throttle_key] += 1
        if self._decision_skip_counter[throttle_key] >= self._decision_sample_n:
            kw["sampled"] = True
            kw["suppressed_since_last"] = self._decision_skip_counter[throttle_key]
            self.log("DECISION", action=action, reason=reason, **kw)
            self._decision_skip_counter[throttle_key] = 0
            self._decision_skip_last_log_ms[throttle_key] = now_ms

    def order_place(self, **kw):
        self.log("ORDER_PLACE", **kw)

    def order_canceled(self, **kw):
        self._cancel_count += 1
        self.log("ORDER_CANCELED", **kw)

    def fill(self, **kw):
        self._fill_count += 1
        self._track_hourly_fill(kw)
        self.log("FILL", **kw)

    def dry_fill(self, **kw):
        self._fill_count += 1
        self._track_hourly_fill(kw)
        self.log("DRY_FILL", **kw)

    def _track_hourly_fill(self, kw: dict) -> None:
        """Accumulate per-hour and per-rollup fill counts and volumes."""
        side = kw.get("side", "")
        qty = kw.get("fill_shares", 0) or kw.get("qty_shares", 0) or kw.get("qty", 0)
        price = kw.get("fill_price", 0) or kw.get("price", 0)
        usd = kw.get("fill_usd", 0) or (qty * price if qty and price else kw.get("usd", 0))
        if side == "BUY":
            self._hourly_buy_fills += 1
            self._hourly_gross_buy_usd += usd
            self._rollup_buy_fills += 1
        elif side == "SELL":
            self._hourly_sell_fills += 1
            self._hourly_gross_sell_usd += usd
            self._rollup_sell_fills += 1

    def inventory(self, **kw):
        self.log("INVENTORY", **kw)

    def risk(self, **kw):
        self.log("RISK", **kw)

    def api_error(self, **kw):
        self._api_errors += 1
        self.log("API_ERROR", **kw)

    def dry_run_order(self, **kw):
        """Deprecated: only emits when DEBUG_LOG_ORDERS is set.
        Called from polymarket_api.py behind a config guard."""
        self._dry_run_orders += 1
        self.log("DRY_RUN_ORDER", **kw)

    def info(self, msg: str, **kw):
        self.log("INFO", msg=msg, **kw)

    def warn(self, msg: str, **kw):
        self.log("WARN", msg=msg, **kw)

    def error(self, msg: str, **kw):
        self.log("ERROR", msg=msg, **kw)

    # ------------------------------------------------------------------
    # Decision rollup (emitted alongside the periodic ROLLUP)
    # ------------------------------------------------------------------
    def maybe_decision_rollup(self, tick_metrics: dict | None = None) -> bool:
        """Emit DECISION_ROLLUP if there are accumulated decision counts.
        Called from the same place as maybe_rollup. Returns True if emitted."""
        total = sum(self._decision_action_counts.values())
        if total == 0:
            return False

        # Top skip reasons sorted by count descending
        top_skip = sorted(
            self._decision_skip_reasons.items(),
            key=lambda kv: kv[1], reverse=True,
        )[:10]

        # Per-asset counts sorted by count descending
        top_assets = sorted(
            self._decision_asset_counts.items(),
            key=lambda kv: kv[1], reverse=True,
        )[:10]

        payload: dict = {
            "action_counts": dict(self._decision_action_counts),
            "top_skip_reasons": {k: v for k, v in top_skip},
            "top_asset_counts": {k: v for k, v in top_assets},
        }
        if tick_metrics:
            payload.update(tick_metrics)

        self.log("DECISION_ROLLUP", **payload)

        # Reset
        self._decision_action_counts.clear()
        self._decision_skip_reasons.clear()
        self._decision_asset_counts.clear()
        return True

    # ------------------------------------------------------------------
    # Periodic rollup
    # ------------------------------------------------------------------
    def maybe_rollup(self, inventory_snapshot: dict | None = None,
                     unrealized_usd: float = 0.0,
                     realized_usd: float = 0.0,
                     mark_details: list[dict] | None = None,
                     tick_metrics: dict | None = None) -> bool:
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
            "buy_fills": self._rollup_buy_fills,
            "sell_fills": self._rollup_sell_fills,
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
        if mark_details:
            payload["mark_details"] = mark_details
        self.log("ROLLUP", **payload)

        # Emit DECISION_ROLLUP alongside
        self.maybe_decision_rollup(tick_metrics=tick_metrics)

        # Reset accumulators
        self._buy_count = 0
        self._sell_count = 0
        self._skip_count = 0
        self._cancel_count = 0
        self._fill_count = 0
        self._api_errors = 0
        self._dry_run_orders = 0
        self._rollup_buy_fills = 0
        self._rollup_sell_fills = 0
        return True

    # ------------------------------------------------------------------
    # Hourly PnL
    # ------------------------------------------------------------------
    def hourly_pnl(self, **kw) -> None:
        """Emit an HOURLY_PNL event at each hour boundary."""
        self.log("HOURLY_PNL", **kw)

    def entry_quality_report(self, **kw) -> None:
        """Emit an ENTRY_QUALITY_REPORT event at each hour boundary."""
        self.log("ENTRY_QUALITY_REPORT", **kw)

    def get_and_reset_hourly_fills(self) -> dict:
        """Return accumulated hourly fill stats and reset counters."""
        stats = {
            "total_buy_fills": self._hourly_buy_fills,
            "total_sell_fills": self._hourly_sell_fills,
            "gross_buy_usd": round(self._hourly_gross_buy_usd, 4),
            "gross_sell_usd": round(self._hourly_gross_sell_usd, 4),
        }
        self._hourly_buy_fills = 0
        self._hourly_sell_fills = 0
        self._hourly_gross_buy_usd = 0.0
        self._hourly_gross_sell_usd = 0.0
        return stats

    # ------------------------------------------------------------------
    # Startup dump
    # ------------------------------------------------------------------
    def log_config(self, config_dict: dict) -> None:
        self.log("CONFIG", **config_dict)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    def close(self):
        if self._rotating_handler is not None:
            self._rotating_handler.close()
            self._rotating_handler = None
        if self._fh:
            self._fh.close()
            self._fh = None


def _make_log_record(line: str):
    """Create a minimal logging.LogRecord for the RotatingFileHandler."""
    import logging
    record = logging.LogRecord(
        name="bot", level=logging.INFO, pathname="", lineno=0,
        msg=line, args=(), exc_info=None,
    )
    # Override getMessage to return the raw JSON line (no formatting)
    record.getMessage = lambda: line  # type: ignore[assignment]
    return record
