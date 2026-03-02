"""
diagnostics.py – Auto-generated paper-trade diagnostics files.

Every ET hour boundary and at shutdown, writes:
  reports/paper_hourly_pnl.csv
  reports/paper_hourly_pnl_chart.txt
  reports/paper_diagnostics_report.txt
  reports/snapshots/paper_report_YYYY-mm-dd_HH00_ET.txt
  reports/snapshots/paper_hourly_YYYY-mm-dd_HH00_ET.csv

All in-process — no external log parsing.  Persists hourly buckets to
SQLite so data survives restarts.

No external dependencies beyond stdlib.
"""
from __future__ import annotations

import math
import os
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from state import StateManager

_ET = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _et_hour_key(dt_utc: datetime | None = None) -> str:
    """Current ET hour as 'YYYY-MM-DD HH:00'."""
    if dt_utc is None:
        dt_utc = datetime.now(timezone.utc)
    return dt_utc.astimezone(_ET).strftime("%Y-%m-%d %H:00")


def _et_snapshot_tag(dt_utc: datetime | None = None) -> str:
    """Tag for snapshot filenames like '2026-03-02_1500_ET'."""
    if dt_utc is None:
        dt_utc = datetime.now(timezone.utc)
    return dt_utc.astimezone(_ET).strftime("%Y-%m-%d_%H00_ET")


def _mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _std(xs: List[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def _median(xs: List[float]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    n = len(s)
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2


# ---------------------------------------------------------------------------
# Diagnostics class
# ---------------------------------------------------------------------------
class Diagnostics:
    """In-process diagnostics tracker.  Hooks called from execution.py / main.py."""

    def __init__(self, state: StateManager, reports_dir: str = "reports"):
        self._state = state
        self._reports_dir = reports_dir
        self._snapshots_dir = os.path.join(reports_dir, "snapshots")

        # Ensure dirs exist
        os.makedirs(self._reports_dir, exist_ok=True)
        os.makedirs(self._snapshots_dir, exist_ok=True)

        # --- Counters (in-memory, persisted periodically) ---
        self._start_ts: float = time.time()
        self._orders_placed: int = 0
        self._orders_canceled: int = 0
        self._buy_fills: int = 0
        self._sell_fills: int = 0
        self._passive_intents: int = 0
        self._cross_intents: int = 0
        self._passive_fills: int = 0
        self._cross_fills: int = 0
        self._decisions: int = 0

        # Per-sell PnL tracking
        self._sell_pnls: List[float] = []
        self._passive_sell_pnls: List[float] = []
        self._cross_sell_pnls: List[float] = []

        # --- Hourly PnL buckets: hour_et -> realized_pnl ---
        self._hourly_pnl: OrderedDict[str, float] = OrderedDict()
        self._current_hour_et: str = _et_hour_key()

        # --- Restore from SQLite ---
        self._restore()

    # ------------------------------------------------------------------
    # Restore / persist
    # ------------------------------------------------------------------
    def _restore(self) -> None:
        """Load persisted hourly buckets + counters from SQLite."""
        saved = self._state.diag_load_hourly_pnl()
        for k, v in saved.items():
            self._hourly_pnl[k] = v

        # Restore counters
        import json
        raw = self._state.diag_get_counter("diag_counters")
        if raw:
            try:
                d = json.loads(raw)
                self._start_ts = d.get("start_ts", self._start_ts)
                self._orders_placed = d.get("orders_placed", 0)
                self._orders_canceled = d.get("orders_canceled", 0)
                self._buy_fills = d.get("buy_fills", 0)
                self._sell_fills = d.get("sell_fills", 0)
                self._passive_intents = d.get("passive_intents", 0)
                self._cross_intents = d.get("cross_intents", 0)
                self._passive_fills = d.get("passive_fills", 0)
                self._cross_fills = d.get("cross_fills", 0)
                self._decisions = d.get("decisions", 0)
                self._sell_pnls = d.get("sell_pnls", [])
                self._passive_sell_pnls = d.get("passive_sell_pnls", [])
                self._cross_sell_pnls = d.get("cross_sell_pnls", [])
            except (json.JSONDecodeError, TypeError):
                pass

    def _persist(self) -> None:
        """Persist counters + hourly buckets to SQLite."""
        import json
        # Persist hourly buckets
        for hour_et, pnl in self._hourly_pnl.items():
            self._state.diag_set_hourly_pnl(hour_et, pnl)
        # Persist counters
        d = {
            "start_ts": self._start_ts,
            "orders_placed": self._orders_placed,
            "orders_canceled": self._orders_canceled,
            "buy_fills": self._buy_fills,
            "sell_fills": self._sell_fills,
            "passive_intents": self._passive_intents,
            "cross_intents": self._cross_intents,
            "passive_fills": self._passive_fills,
            "cross_fills": self._cross_fills,
            "decisions": self._decisions,
            "sell_pnls": self._sell_pnls[-500:],  # cap to avoid unbounded growth
            "passive_sell_pnls": self._passive_sell_pnls[-500:],
            "cross_sell_pnls": self._cross_sell_pnls[-500:],
        }
        self._state.diag_set_counter("diag_counters", json.dumps(d))

    # ------------------------------------------------------------------
    # Event hooks (called from execution.py)
    # ------------------------------------------------------------------
    def on_order_intent(self, *, side: str, entry_style: str, **kw) -> None:
        self._decisions += 1
        if entry_style == "PASSIVE":
            self._passive_intents += 1
        elif entry_style == "CROSS":
            self._cross_intents += 1

    def on_order_placed(self, **kw) -> None:
        self._orders_placed += 1

    def on_order_canceled(self, **kw) -> None:
        self._orders_canceled += 1

    def on_fill(self, *, side: str, fill_price: float, fill_shares: float,
                entry_style: str = "UNKNOWN",
                realized_pnl: float = 0.0, **kw) -> None:
        """Record a fill (DRY_FILL or LIVE FILL)."""
        if side == "BUY":
            self._buy_fills += 1
            if entry_style == "PASSIVE":
                self._passive_fills += 1
            elif entry_style == "CROSS":
                self._cross_fills += 1
        elif side == "SELL":
            self._sell_fills += 1
            if entry_style == "PASSIVE":
                self._passive_fills += 1
            elif entry_style == "CROSS":
                self._cross_fills += 1
            # Track PnL for this sell
            self._sell_pnls.append(realized_pnl)
            if entry_style == "PASSIVE":
                self._passive_sell_pnls.append(realized_pnl)
            elif entry_style == "CROSS":
                self._cross_sell_pnls.append(realized_pnl)

            # Add to hourly bucket
            hour = _et_hour_key()
            if hour not in self._hourly_pnl:
                self._hourly_pnl[hour] = 0.0
            self._hourly_pnl[hour] += realized_pnl

    # ------------------------------------------------------------------
    # Hourly flush check (called from main loop)
    # ------------------------------------------------------------------
    def maybe_flush_reports(self, now_utc: datetime | None = None) -> bool:
        """Check if ET hour boundary crossed; if so, write all reports.

        Returns True if reports were written.
        """
        current = _et_hour_key(now_utc)
        if current != self._current_hour_et:
            # Hour boundary crossed — finalize previous hour
            prev_hour = self._current_hour_et
            self._current_hour_et = current
            # Ensure current hour bucket exists
            if current not in self._hourly_pnl:
                self._hourly_pnl[current] = 0.0
            self._persist()
            self._write_all_reports(snapshot_tag=_et_snapshot_tag(now_utc))
            return True
        return False

    def flush_reports(self, final: bool = False) -> None:
        """Force-write all reports now (called on shutdown)."""
        self._persist()
        tag = "final" if final else _et_snapshot_tag()
        self._write_all_reports(snapshot_tag=tag)

    # ------------------------------------------------------------------
    # Report writers
    # ------------------------------------------------------------------
    def _write_all_reports(self, snapshot_tag: str = "") -> None:
        csv_content = self._build_csv()
        chart_content = self._build_chart()
        report_content = self._build_report()

        # Main files
        self._safe_write(os.path.join(self._reports_dir, "paper_hourly_pnl.csv"),
                         csv_content)
        self._safe_write(os.path.join(self._reports_dir, "paper_hourly_pnl_chart.txt"),
                         chart_content)
        self._safe_write(os.path.join(self._reports_dir, "paper_diagnostics_report.txt"),
                         report_content)

        # Timestamped snapshots
        if snapshot_tag:
            self._safe_write(
                os.path.join(self._snapshots_dir,
                             f"paper_report_{snapshot_tag}.txt"),
                report_content)
            self._safe_write(
                os.path.join(self._snapshots_dir,
                             f"paper_hourly_{snapshot_tag}.csv"),
                csv_content)

    def _safe_write(self, path: str, content: str) -> None:
        try:
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                f.write(content)
            os.replace(tmp, path)
        except OSError:
            pass

    # ------------------------------------------------------------------
    # CSV builder
    # ------------------------------------------------------------------
    def _build_csv(self) -> str:
        lines = ["hour_et,realized_pnl"]
        for hour, pnl in self._hourly_pnl.items():
            lines.append(f"{hour},{pnl:.4f}")
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------
    # ASCII chart builder
    # ------------------------------------------------------------------
    def _build_chart(self) -> str:
        if not self._hourly_pnl:
            return "No hourly data yet.\n"

        max_abs = max(abs(v) for v in self._hourly_pnl.values()) or 1.0
        max_bars = 40
        lines = ["HOURLY REALIZED PnL (ET)", ""]

        for hour, pnl in self._hourly_pnl.items():
            # Extract just HH:MM from the key
            short_hour = hour.split(" ")[-1] if " " in hour else hour
            bar_len = int(abs(pnl) / max_abs * max_bars)
            bar_len = max(bar_len, 1) if abs(pnl) > 0.0001 else 0
            if pnl >= 0:
                bar = "\u2588" * bar_len
                sign = "+"
            else:
                bar = "\u2591" * bar_len
                sign = ""
            lines.append(f"  {short_hour}  {sign}{pnl:>8.4f}  {bar}")

        # Totals
        total = sum(self._hourly_pnl.values())
        lines.append("")
        lines.append(f"  TOTAL   {'+' if total >= 0 else ''}{total:.4f}")
        lines.append("")
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------
    # Full diagnostics report builder
    # ------------------------------------------------------------------
    def _build_report(self) -> str:
        now_utc = datetime.now(timezone.utc)
        now_et = now_utc.astimezone(_ET)
        start_et = datetime.fromtimestamp(self._start_ts, tz=timezone.utc).astimezone(_ET)
        elapsed_sec = time.time() - self._start_ts
        elapsed_hr = elapsed_sec / 3600

        lines: List[str] = []
        w = 70  # width

        lines.append("=" * w)
        lines.append("  PAPER DIAGNOSTICS REPORT")
        lines.append("=" * w)
        lines.append("")
        lines.append(f"  Generated: {now_et.strftime('%Y-%m-%d %H:%M:%S ET')}")
        lines.append(f"  Run start: {start_et.strftime('%Y-%m-%d %H:%M:%S ET')}")
        lines.append(f"  Elapsed:   {elapsed_hr:.1f} hours ({elapsed_sec:.0f}s)")

        # --- Counts ---
        lines.append("")
        lines.append("-" * w)
        lines.append("  1. EVENT COUNTS")
        lines.append("-" * w)
        lines.append(f"  Decisions:       {self._decisions}")
        lines.append(f"  Orders placed:   {self._orders_placed}")
        lines.append(f"  Orders canceled: {self._orders_canceled}")
        lines.append(f"  Buy fills:       {self._buy_fills}")
        lines.append(f"  Sell fills:      {self._sell_fills}")

        # --- Passive vs Cross ---
        lines.append("")
        lines.append("-" * w)
        lines.append("  2. PASSIVE vs CROSS FILL RATES")
        lines.append("-" * w)
        lines.append(f"  Intents:  {self._passive_intents} passive, {self._cross_intents} cross")
        total_fills = self._passive_fills + self._cross_fills
        lines.append(f"  Fills:    {self._passive_fills} passive, {self._cross_fills} cross")
        if self._passive_intents > 0:
            lines.append(f"  Passive fill rate: {self._passive_fills/self._passive_intents*100:.1f}%")
        if self._cross_intents > 0:
            lines.append(f"  Cross fill rate:   {self._cross_fills/self._cross_intents*100:.1f}%")

        # --- Profit per sell ---
        lines.append("")
        lines.append("-" * w)
        lines.append("  3. PROFIT PER SELL")
        lines.append("-" * w)
        if self._sell_pnls:
            wins = [p for p in self._sell_pnls if p > 0]
            losses = [p for p in self._sell_pnls if p <= 0]
            win_rate = len(wins) / len(self._sell_pnls) * 100 if self._sell_pnls else 0
            lines.append(f"  Total sells:   {len(self._sell_pnls)}")
            lines.append(f"  Total PnL:     ${sum(self._sell_pnls):.4f}")
            lines.append(f"  Avg per sell:  ${_mean(self._sell_pnls):.4f}")
            lines.append(f"  Win rate:      {win_rate:.1f}%")
            lines.append(f"  Avg win:       ${_mean(wins):.4f}" if wins else "  Avg win:       --")
            lines.append(f"  Avg loss:      ${_mean(losses):.4f}" if losses else "  Avg loss:      --")
        else:
            lines.append("  No sell fills yet.")

        if self._passive_sell_pnls:
            lines.append(f"  Passive sells: n={len(self._passive_sell_pnls)}  "
                         f"total=${sum(self._passive_sell_pnls):.4f}  "
                         f"avg=${_mean(self._passive_sell_pnls):.4f}")
        if self._cross_sell_pnls:
            lines.append(f"  Cross sells:   n={len(self._cross_sell_pnls)}  "
                         f"total=${sum(self._cross_sell_pnls):.4f}  "
                         f"avg=${_mean(self._cross_sell_pnls):.4f}")

        # --- Hourly realized PnL ---
        lines.append("")
        lines.append("-" * w)
        lines.append("  4. HOURLY REALIZED PnL")
        lines.append("-" * w)
        if self._hourly_pnl:
            lines.append(f"  {'Hour ET':<20} {'Realized':>10}")
            lines.append(f"  {'-'*20} {'-'*10}")
            for hour, pnl in self._hourly_pnl.items():
                lines.append(f"  {hour:<20} ${pnl:>9.4f}")
        else:
            lines.append("  No hourly data yet.")

        # --- Drawdown from cumulative realized curve ---
        lines.append("")
        lines.append("-" * w)
        lines.append("  5. DRAWDOWN (from cumulative realized)")
        lines.append("-" * w)
        if self._sell_pnls:
            cum = 0.0
            peak = 0.0
            max_dd = 0.0
            for pnl in self._sell_pnls:
                cum += pnl
                if cum > peak:
                    peak = cum
                dd = peak - cum
                if dd > max_dd:
                    max_dd = dd
            lines.append(f"  Cumulative realized: ${cum:.4f}")
            lines.append(f"  Peak realized:       ${peak:.4f}")
            lines.append(f"  Max drawdown:        ${max_dd:.4f}")
        else:
            lines.append("  No data.")

        # --- Hourly mean/std/sharpe ---
        lines.append("")
        lines.append("-" * w)
        lines.append("  6. HOURLY STATISTICS")
        lines.append("-" * w)
        hourly_vals = list(self._hourly_pnl.values())
        if len(hourly_vals) >= 2:
            m = _mean(hourly_vals)
            s = _std(hourly_vals)
            sharpe = m / s if s > 0 else 0.0
            lines.append(f"  Hours:            {len(hourly_vals)}")
            lines.append(f"  Mean/hr:          ${m:.4f}")
            lines.append(f"  Std/hr:           ${s:.4f}")
            lines.append(f"  Sharpe-like (hr): {sharpe:.3f}")
            lines.append(f"  Best hour:        ${max(hourly_vals):.4f}")
            lines.append(f"  Worst hour:       ${min(hourly_vals):.4f}")
        elif len(hourly_vals) == 1:
            lines.append(f"  Only 1 hour. Realized: ${hourly_vals[0]:.4f}")
        else:
            lines.append("  No hourly data yet.")

        # --- Summary ---
        total_pnl = sum(self._sell_pnls) if self._sell_pnls else 0.0
        lines.append("")
        lines.append("=" * w)
        lines.append(f"  SUMMARY: {self._buy_fills} buys, {self._sell_fills} sells, "
                     f"realized = ${total_pnl:.4f}")
        if elapsed_hr > 0:
            lines.append(f"  PnL/hr: ${total_pnl / elapsed_hr:.4f}")
        lines.append("=" * w)
        lines.append("")

        return "\n".join(lines)
