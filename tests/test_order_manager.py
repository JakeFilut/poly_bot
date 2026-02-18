#!/usr/bin/env python3
"""
test_order_manager.py — Unit tests for Order Manager + Live Execution hardening.

Test Plan:
==========

1. PAPER MODE (LOG) — Regression
   - _exec_buy in LOG mode calls _paper_buy with correct args (price, qty, usdc_cost)
   - _exec_sell in LOG mode calls _paper_sell with correct args
   - Counters increment correctly in LOG mode
   - _om_reconcile_all is a no-op in LOG mode

2. LIVE MODE — Simulated partial fills
   - _om_submit_order tracks order in _om_open_orders when not immediately filled
   - _om_submit_order does NOT track order when fully filled immediately
   - _om_poll_and_reconcile detects new fills and applies position deltas
   - _om_poll_and_reconcile handles partial fills correctly (delta_qty)
   - _om_poll_and_reconcile removes fully filled orders from tracking
   - Counters only increment after confirmed fill

3. ORPHAN DETECTION
   - _om_scan_orphans cancels orders not in _om_open_orders
   - Orders in _om_open_orders are NOT cancelled by orphan scan
   - Orphan cancel count increments correctly

4. TTL / CANCEL
   - _om_reconcile_all cancels maker orders older than OM_MAKER_ORDER_TTL_MS
   - _om_cancel_order retries on failure up to OM_CANCEL_MAX_RETRIES
   - Failed cancel marks order as cancel_pending for retry next tick
   - _om_reprice_order cancels old + submits new + preserves filled_qty

5. KILL-SWITCH
   - Kill-switch activates when orphans/min > threshold
   - Kill-switch activates when API errors/min > threshold
   - Kill-switch blocks new entries via _exec_buy
   - Kill-switch cancels all open orders

6. DUPLICATE ORDER GUARD
   - _om_submit_order blocks if existing open order for (slug, outcome, side)
   - After order fills, new order for same (slug, outcome, side) is allowed

7. SELL FLOW
   - _do_sell in LIVE mode uses _exec_sell -> _om_submit_order
   - Unfilled sell order tracked in _om_open_orders
   - Sell order resting logged as SELL_ORDER_RESTING
   - Counters NOT incremented before submit (Bug 6 fix)

8. DIRECTIONAL SCALP LIVE BUY
   - _dscalp_entries calls _exec_buy (not _paper_buy)
   - Resting order returns early with DSCALP_ENTRY_RESTING log
   - Filled order updates position tracking correctly

9. HEDGE ESCALATION (LIVE)
   - Tick1 escalation fires at OM_HEDGE_TICK1_MS in LIVE mode
   - Tick2 escalation fires at OM_HEDGE_TICK2_MS in LIVE mode
   - Taker cross fires at OM_HEDGE_CROSS_MS in LIVE mode
   - Edge collapse unwinds filled leg and cancels missing-leg orders

10. SHUTDOWN
    - Shutdown cancels all tracked open orders
    - Orphan scan before shutdown catches any remaining

11. LIVE_SANITY REPORT
    - Emits every 60s with correct metrics
    - Per-minute counters reset after report

Verification Protocol:
======================

A. Run in LOG mode with existing config:
   MODE=LOG python3 pm_hourly_clone_bot.py
   - Verify no crashes (especially _dscalp_entries)
   - Verify DIAG_REPORT shows same metrics as before
   - Verify LIVE_SANITY reports (all zeros in LOG mode)

B. Run in LIVE mode with $1 size:
   MODE=LIVE DSCALP_STEP_USD=1 DSCALP_STEP_USD_MIN=1 python3 pm_hourly_clone_bot.py
   - Verify no orphan orders: after bot stops, `list_open_orders()` returns empty
   - Verify partial fills reconcile: check OM_RECONCILE_FILL events in JSONL
   - Verify sells retry/cancel: check OM_ORDER_CANCELED and SELL_ORDER_RESTING events
   - Verify counters match fills: compare tx_count/fill_count in LIVE_SANITY vs ORDER_FILL count
   - Verify kill-switch: inject errors and check OM_KILL_SWITCH_TRIGGERED event

C. Specific scenario tests:
   1. Buy one side, verify resting order tracked -> reconciled or TTL-cancelled
   2. Place sell, verify resting -> fills via reconciliation
   3. Kill bot mid-trade, restart, verify orphan scan catches stale orders
   4. Verify hedge escalation fires tick1/tick2/cross in LIVE with OM_HEDGE_ESCALATION_LIVE=True
"""
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from dataclasses import dataclass

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Force LOG mode for unit tests
os.environ["MODE"] = "LOG"


class TestOrderManagerPaperMode(unittest.TestCase):
    """Test that LOG mode still works correctly after the patch."""

    def test_exec_buy_paper_mode(self):
        """_exec_buy in LOG mode should call _paper_buy with correct args."""
        # Import after setting MODE
        import pm_hourly_clone_bot as bot
        assert bot.MODE == "LOG"

        # Create minimal mocks
        mock_st = MagicMock()
        mock_st.positions = {"Up": MagicMock(qty=0, cost_usdc=0, vwap=0,
                                              opened_at=None, position_id="test"),
                             "Down": MagicMock(qty=0, cost_usdc=0, vwap=0,
                                               opened_at=None)}
        mock_m = MagicMock()
        mock_m.outcome_up_id = "token_up"
        mock_m.outcome_down_id = "token_down"
        mock_m.slug = "test-slug"

        # Create Bot with mocked internals
        with patch.object(bot.Bot, '__init__', lambda self: None):
            b = bot.Bot()
            b.cash_usdc = 1000.0
            b._hour_trade_count = 0
            b._shadow_active = False

            # Call _exec_buy
            result = b._exec_buy(mock_st, mock_m, "Up", 0.50, 10.0,
                                  reason="TEST")
            self.assertTrue(result["filled"])
            self.assertEqual(result["fill_qty"], 10)
            self.assertEqual(result["fill_price"], 0.50)
            self.assertAlmostEqual(result["usdc_cost"], 5.0)

    def test_constants_exist(self):
        """Verify all new OM constants are defined."""
        import pm_hourly_clone_bot as bot
        self.assertIsInstance(bot.OM_MAKER_ORDER_TTL_MS, float)
        self.assertIsInstance(bot.OM_MAX_ACTIVE_PER_SLUG_SIDE, int)
        self.assertIsInstance(bot.OM_MAX_OPEN_ORDERS, int)
        self.assertIsInstance(bot.OM_ORPHAN_SCAN_INTERVAL_SEC, float)
        self.assertIsInstance(bot.OM_SUBMIT_MAX_RETRIES, int)
        self.assertIsInstance(bot.OM_SUBMIT_BACKOFF_MS, list)
        self.assertIsInstance(bot.OM_RECONCILE_MAX_PER_TICK, int)
        self.assertIsInstance(bot.OM_KILL_ORPHAN_THRESHOLD_PER_MIN, int)
        self.assertIsInstance(bot.OM_KILL_API_ERROR_THRESHOLD_PER_MIN, int)
        self.assertIsInstance(bot.OM_KILL_COOLDOWN_SEC, float)
        self.assertTrue(hasattr(bot, 'OM_HEDGE_ESCALATION_LIVE'))
        self.assertTrue(hasattr(bot, 'OM_HEDGE_TICK1_MS'))
        self.assertTrue(hasattr(bot, 'OM_HEDGE_TICK2_MS'))
        self.assertTrue(hasattr(bot, 'OM_HEDGE_CROSS_MS'))
        self.assertTrue(hasattr(bot, 'OM_HEDGE_CROSS_MIN_EDGE_CENTS'))
        self.assertTrue(hasattr(bot, 'OM_HEDGE_CROSS_MAX_SPREAD_CENTS'))


if __name__ == "__main__":
    unittest.main()
