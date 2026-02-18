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
                drift_detected, no_progress_pauses
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

        print(
            f"  [LIVE_SANITY] "
            f"open_orders={stats.get('open_orders_count', 0)}  "
            f"orphan_cx={stats.get('orphan_cancels_last_min', 0)}  "
            f"api_err={stats.get('api_errors_last_min', 0)}  "
            f"partial={stats.get('partial_fill_count', 0)}  "
            f"unpaired={stats.get('unpaired_events_last_min', 0)}  "
            f"ttl_cx={stats.get('ttl_cancels', 0)}  "
            f"cap_blk={stats.get('cap_blocks_last_min', 0)}  "
            f"drift={'YES' if stats.get('drift_detected') else 'no'}  "
            f"kill={'ON' if stats.get('kill_switch_active') else 'off'}  "
            f"exposure=${total_usd:.0f}  top=[{top_str}]"
        )
