"""LIVE_SANITY_REPORT — periodic diagnostic output for LIVE mode.

Every 60s print:
  - open_orders_count
  - orphan_cancels_last_min
  - api_errors_last_min
  - partial_fill_count
  - unpaired_events_last_min
  - cap_blocks_last_min
  - drift_detected
  - no_progress_pauses
  - exposure_per_slug

Diagnostics only — no strategy impact.
"""
from __future__ import annotations

import time
from typing import Callable, Dict

from src.config.settings import OM_SANITY_REPORT_INTERVAL_SEC


class LiveSanityReporter:
    """Periodic diagnostic reporter for LIVE execution health.

    Wire into bot:
      - tick(stats_dict) → call every main loop iteration
    """

    def __init__(self, write_jsonl_fn: Callable[[dict], None]):
        self._write_jsonl = write_jsonl_fn
        self._last_report_ts = time.time()

    def tick(self, stats: dict):
        """Emit sanity report if interval has elapsed.

        Args:
            stats: dict with keys:
                open_orders_count, orphan_cancels_last_min, api_errors_last_min,
                partial_fill_count, unpaired_events_last_min, exposure_per_slug,
                kill_switch_active, total_kill_activations, ttl_cancels,
                confirmed_fills, confirmed_submits, cap_blocks_last_min,
                drift_detected, no_progress_pauses, mode,
                buys_allowed, entry_window_remaining_sec, live_safe_max_order_usd
        """
        now = time.time()
        if now - self._last_report_ts < OM_SANITY_REPORT_INTERVAL_SEC:
            return
        self._last_report_ts = now

        report = {
            "event_type": "LIVE_SANITY_REPORT",
            "ts_ms": int(now * 1000),
        }
        report.update(stats)

        self._write_jsonl(report)

        # Also print to stdout for operator visibility
        exposure = stats.get("exposure_per_slug", {})
        total_usd = sum(exposure.values()) if exposure else 0.0
        top_slugs = sorted(exposure.items(), key=lambda x: -x[1])[:5]
        top_str = " ".join(f"{s}=${v:.0f}" for s, v in top_slugs) if top_slugs else "none"

        # LIVE_SAFE fields
        mode = stats.get("mode", "LIVE")
        buys_str = ""
        if mode == "LIVE_SAFE":
            buys_ok = "YES" if stats.get("buys_allowed") else "NO"
            remaining = stats.get("entry_window_remaining_sec", 0)
            max_usd = stats.get("live_safe_max_order_usd", 0)
            buys_str = f"buys={buys_ok}({remaining:.0f}s)  max_order=${max_usd:.2f}  "

        # Win/loss stats
        wins = stats.get('wins_last_min', 0)
        losses = stats.get('losses_last_min', 0)
        wr = stats.get('win_rate', 0)
        avg_w = stats.get('avg_win_cents', 0)
        avg_l = stats.get('avg_loss_cents', 0)

        # Guard flags
        flags = []
        if stats.get('velocity_stale'):
            flags.append("VEL_STALE")
        if stats.get('recon_lag_paused'):
            flags.append("RECON_LAG")
        if stats.get('balance_paused'):
            flags.append("BAL_LOW")
        flag_str = f"  flags=[{','.join(flags)}]" if flags else ""

        print(
            f"  [LIVE_SANITY] "
            f"{buys_str}"
            f"open_orders={stats.get('open_orders_count', 0)}  "
            f"fills_1m={stats.get('fills_last_min', 0)}  "
            f"W/L={wins}/{losses}({wr:.0f}%)  "
            f"avg_w={avg_w:.1f}c  avg_l={avg_l:.1f}c  "
            f"orphan_cx={stats.get('orphan_cancels_last_min', 0)}  "
            f"api_err={stats.get('api_errors_last_min', 0)}  "
            f"dedup={stats.get('dedup_blocks_last_min', 0)}  "
            f"drift={'YES' if stats.get('drift_detected') else 'no'}  "
            f"kill={'ON' if stats.get('kill_switch_active') else 'off'}  "
            f"exposure=${total_usd:.0f}  top=[{top_str}]"
            f"{flag_str}"
        )
