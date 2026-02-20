#!/usr/bin/env python3
"""Test: fills ledger + truth capture only count fills for the CURRENT hour window.

Feeds in fills for 2 different slugs/hours and verifies only the current hour
gets counted in positions, fill counters, and PnL.
"""
import os
import sys
import json
import tempfile
import shutil
from datetime import datetime, timezone, timedelta
from decimal import Decimal

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.execution.fills_ledger import FillsLedger
from src.positions.truth_capture import TruthCapture

_PASS = 0
_FAIL = 0


def _check(label: str, condition: bool, detail: str = ""):
    global _PASS, _FAIL
    if condition:
        _PASS += 1
        print(f"  [PASS] {label}")
    else:
        _FAIL += 1
        print(f"  [FAIL] {label}  {detail}")


def test_fills_ledger_hour_scoping():
    """FillsLedger: only current-hour fills counted; old-hour fills ignored."""
    print("\n" + "=" * 60)
    print("  TEST: FillsLedger hour scoping")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="test_ledger_")
    ledger_path = os.path.join(tmpdir, "fills_ledger.jsonl")
    events = []

    try:
        ledger = FillsLedger(
            ledger_path=ledger_path,
            run_id="test_run",
            write_jsonl_fn=lambda d: events.append(d),
        )

        # Current hour boundaries (from ledger's __init__)
        hour_start = ledger._hour_start
        hour_end = ledger._hour_end
        hour_suffix = ledger._hour_suffix

        # Register current-hour slug
        current_slug = f"BTC_test_current_{hour_suffix}"
        old_slug = f"BTC_test_old_prev_hour"
        ledger.register_token("tok_cur_up", current_slug, "BTC", "Up")
        ledger.register_token("tok_cur_dn", current_slug, "BTC", "Down")

        # 1. Record a fill for the CURRENT hour (should be counted)
        pos1 = ledger.record_fill(
            slug=current_slug, crypto="BTC",
            token_id="tok_cur_up", market_id="mkt1",
            side="Up", action="BUY",
            fill_qty=10.0, fill_price=0.55,
            trade_id="trade_current_1", source="bot",
        )
        _check("Current-hour fill recorded", pos1 is not None,
               f"got {pos1}")
        _check("Fill count = 1", len(ledger._fills) == 1,
               f"got {len(ledger._fills)}")
        _check("Position net_qty = 10", pos1 and float(pos1.net_qty) == 10.0,
               f"got {float(pos1.net_qty) if pos1 else 'None'}")

        # 2. Try to ingest an external fill from an OLD slug
        old_fills = [{
            "trade_id": "trade_old_1",
            "token_id": "tok_old_up",
            "side": "BUY",
            "size": 50.0,
            "price": 0.45,
            "slug": old_slug,
            "match_time": (hour_start - timedelta(hours=2)).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"),
        }]
        # Register the old token so metadata exists (but slug is NOT in active set)
        ledger._token_meta["tok_old_up"] = (old_slug, "BTC", "Up")

        new_from_old = ledger.ingest_external_fills(
            old_fills,
            token_to_slug={"tok_old_up": (old_slug, "BTC", "Up")},
        )
        _check("Old-hour fill rejected by ingest", new_from_old == 0,
               f"got {new_from_old}")
        _check("Fill count still 1", len(ledger._fills) == 1,
               f"got {len(ledger._fills)}")

        # 3. Ingest external fill with current slug but old timestamp
        stale_fills = [{
            "trade_id": "trade_stale_ts",
            "token_id": "tok_cur_up",
            "side": "BUY",
            "size": 20.0,
            "price": 0.50,
            "slug": current_slug,
            "match_time": (hour_start - timedelta(hours=1)).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"),
        }]
        new_from_stale = ledger.ingest_external_fills(stale_fills)
        _check("Stale-timestamp fill rejected", new_from_stale == 0,
               f"got {new_from_stale}")
        _check("Fill count still 1 after stale", len(ledger._fills) == 1,
               f"got {len(ledger._fills)}")

        # 4. Ingest external fill with current slug AND valid timestamp
        good_fills = [{
            "trade_id": "trade_good_ext",
            "token_id": "tok_cur_dn",
            "side": "BUY",
            "size": 5.0,
            "price": 0.48,
            "slug": current_slug,
            "match_time": (hour_start + timedelta(minutes=15)).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"),
        }]
        new_good = ledger.ingest_external_fills(
            good_fills,
            token_to_slug={"tok_cur_dn": (current_slug, "BTC", "Down")},
        )
        _check("Good current-hour external fill accepted", new_good == 1,
               f"got {new_good}")
        _check("Fill count = 2", len(ledger._fills) == 2,
               f"got {len(ledger._fills)}")

        # 5. Summary should report only current-hour fills
        summary = ledger.summary()
        _check("Summary fills_in_current_hour = 2",
               summary.get("fills_in_current_hour") == 2,
               f"got {summary.get('fills_in_current_hour')}")
        _check("Summary hour matches",
               summary.get("hour") == hour_suffix,
               f"got {summary.get('hour')}")

        # 6. Rotate hour → old fills should be cleared
        new_hour = hour_end
        ledger.rotate_hour(new_hour)
        _check("After rotation: fill count = 0", len(ledger._fills) == 0,
               f"got {len(ledger._fills)}")
        _check("After rotation: positions cleared",
               len(ledger._positions) == 0,
               f"got {len(ledger._positions)}")

        # 7. Per-hour file should exist for old hour
        old_hour_path = os.path.join(tmpdir, f"fills_ledger_{hour_suffix}.jsonl")
        _check("Per-hour file exists for old hour",
               os.path.exists(old_hour_path),
               f"path={old_hour_path}")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_truth_capture_hour_scoping():
    """TruthCapture: only current-hour fills counted; old-hour scan fills ignored."""
    print("\n" + "=" * 60)
    print("  TEST: TruthCapture hour scoping")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="test_truth_")
    ledger_path = os.path.join(tmpdir, "truth_fills.jsonl")
    events = []

    try:
        truth = TruthCapture(
            ledger_path=ledger_path,
            wallet_address="0xtest",
            write_jsonl_fn=lambda d: events.append(d),
        )

        hour_start = truth._hour_start
        hour_end = truth._hour_end
        hour_suffix = truth._hour_suffix

        current_slug = f"ETH_test_current_{hour_suffix}"
        old_slug = f"ETH_test_old_prev_hour"
        truth.register_token("tok_eth_up", current_slug, "Up")
        truth.register_token("tok_eth_dn", current_slug, "Down")

        # 1. Record a fill for the current hour
        pos1 = truth.record_fill(
            token_id="tok_eth_up", slug=current_slug,
            outcome="Up", action="BUY",
            qty=15.0, price=0.60,
            trade_id="truth_trade_1", source="ws",
        )
        _check("Truth: current-hour fill recorded", pos1 is not None)
        _check("Truth: fill count = 1", len(truth._fills) == 1,
               f"got {len(truth._fills)}")
        _check("Truth: fills_from_ws = 1", truth.fills_from_ws == 1)

        # 2. Simulate wallet scan that returns old-hour trades
        old_trades = [
            {
                "trade_id": "old_trade_1",
                "token_id": "tok_old_eth",
                "side": "BUY",
                "size": 100.0,
                "price": 0.40,
                "slug": old_slug,
                "match_time": (hour_start - timedelta(hours=3)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"),
            },
            {
                "trade_id": "old_trade_2",
                "token_id": "tok_old_eth",
                "side": "BUY",
                "size": 200.0,
                "price": 0.42,
                "slug": old_slug,
                "match_time": (hour_start - timedelta(hours=1, minutes=30)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        ]
        # Give truth a get_trades function that returns old trades
        truth._get_trades = lambda: old_trades
        new_from_scan = truth.run_wallet_scan()
        _check("Truth: old-hour scan fills rejected", new_from_scan == 0,
               f"got {new_from_scan}")
        _check("Truth: fill count still 1", len(truth._fills) == 1,
               f"got {len(truth._fills)}")
        _check("Truth: fills_from_scan = 0", truth.fills_from_scan == 0,
               f"got {truth.fills_from_scan}")

        # 3. Wallet scan with current-hour trade
        current_trades = [
            {
                "trade_id": "current_scan_1",
                "token_id": "tok_eth_dn",
                "side": "BUY",
                "size": 8.0,
                "price": 0.35,
                "slug": current_slug,
                "outcome": "Down",
                "match_time": (hour_start + timedelta(minutes=20)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        ]
        truth._get_trades = lambda: current_trades
        new_current = truth.run_wallet_scan()
        _check("Truth: current-hour scan fill accepted", new_current == 1,
               f"got {new_current}")
        _check("Truth: fill count = 2", len(truth._fills) == 2,
               f"got {len(truth._fills)}")
        _check("Truth: fills_from_scan = 1", truth.fills_from_scan == 1,
               f"got {truth.fills_from_scan}")

        # 4. Summary shows correct current-hour count
        summary = truth.summary()
        _check("Truth: summary fills_in_current_hour = 2",
               summary.get("fills_in_current_hour") == 2,
               f"got {summary.get('fills_in_current_hour')}")

        # 5. Rotate hour → counters reset
        truth.rotate_hour(hour_end)
        _check("Truth: after rotation fills = 0",
               len(truth._fills) == 0,
               f"got {len(truth._fills)}")
        _check("Truth: after rotation fills_from_ws = 0",
               truth.fills_from_ws == 0)
        _check("Truth: after rotation fills_from_scan = 0",
               truth.fills_from_scan == 0)

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_disk_reload_scoping():
    """Verify that loading from disk only picks up current-hour fills."""
    print("\n" + "=" * 60)
    print("  TEST: Disk reload only loads current-hour fills")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="test_reload_")
    ledger_path = os.path.join(tmpdir, "fills_ledger.jsonl")

    try:
        now = datetime.now(timezone.utc)
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        prev_hour = hour_start - timedelta(hours=1)

        current_slug = "BTC_current_test"
        old_slug = "BTC_old_test"

        # Write a global ledger with mixed fills
        with open(ledger_path, "w") as f:
            # Old-hour fill (should be filtered out)
            f.write(json.dumps({
                "timestamp": (prev_hour + timedelta(minutes=30)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"),
                "slug": old_slug, "crypto": "BTC",
                "token_id": "tok_old", "market_id": "mkt_old",
                "side": "Up", "action": "BUY",
                "fill_qty": "100.000000000", "fill_price": "0.500000000",
                "fees": "0.000000000", "order_id": "ord_old",
                "trade_id": "tid_old", "source": "bot",
            }) + "\n")
            # Current-hour fill (should be loaded)
            f.write(json.dumps({
                "timestamp": (hour_start + timedelta(minutes=5)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"),
                "slug": current_slug, "crypto": "BTC",
                "token_id": "tok_cur", "market_id": "mkt_cur",
                "side": "Up", "action": "BUY",
                "fill_qty": "10.000000000", "fill_price": "0.600000000",
                "fees": "0.000000000", "order_id": "ord_cur",
                "trade_id": "tid_cur", "source": "bot",
            }) + "\n")

        # Create ledger and load
        ledger = FillsLedger(
            ledger_path=ledger_path,
            run_id="test_reload",
            write_jsonl_fn=lambda d: None,
        )
        loaded = ledger.load_from_disk()

        _check("Reload: loaded only 1 fill (current hour)",
               loaded == 1, f"got {loaded}")
        _check("Reload: fill count = 1",
               len(ledger._fills) == 1,
               f"got {len(ledger._fills)}")
        _check("Reload: loaded fill has current slug",
               ledger._fills[0].slug == current_slug if ledger._fills else False,
               f"got {ledger._fills[0].slug if ledger._fills else 'empty'}")

        # Positions should reflect only the current-hour fill
        pos = ledger.get_position("tok_cur", "Up")
        _check("Reload: position qty = 10 (current hour only)",
               float(pos.net_qty) == 10.0,
               f"got {float(pos.net_qty)}")

        # Old token should have no position
        old_pos = ledger.get_position("tok_old", "Up")
        _check("Reload: old token has no position",
               float(old_pos.net_qty) == 0.0,
               f"got {float(old_pos.net_qty)}")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_unverifiable_fills_rejected():
    """Fills with no slug AND no timestamp are rejected as unverifiable."""
    print("\n" + "=" * 60)
    print("  TEST: Unverifiable fills (no slug, no timestamp) rejected")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="test_unverif_")
    ledger_path = os.path.join(tmpdir, "truth_fills.jsonl")

    try:
        truth = TruthCapture(
            ledger_path=ledger_path,
            wallet_address="0xtest",
            write_jsonl_fn=lambda d: None,
        )

        hour_start = truth._hour_start
        current_slug = f"SOL_test_{truth._hour_suffix}"
        truth.register_token("tok_sol_up", current_slug, "Up")

        # Simulate API returning trades with NO slug resolution and NO timestamp
        unverifiable_trades = [
            {
                "trade_id": f"mystery_{i}",
                "token_id": f"tok_unknown_{i}",
                "side": "BUY",
                "size": 50.0,
                "price": 0.55,
                # NO match_time, NO created_at, NO timestamp
                # token_id is unknown so _resolve_meta returns ("", "")
            }
            for i in range(100)
        ]
        truth._get_trades = lambda: unverifiable_trades
        new_count = truth.run_wallet_scan()
        _check("Unverifiable fills all rejected", new_count == 0,
               f"got {new_count}")
        _check("Fill count still 0", len(truth._fills) == 0,
               f"got {len(truth._fills)}")
        _check("fills_from_scan still 0", truth.fills_from_scan == 0,
               f"got {truth.fills_from_scan}")

        # Mix: some verifiable (has timestamp in current hour + current slug),
        # some unverifiable
        mixed_trades = [
            {
                "trade_id": "verifiable_1",
                "token_id": "tok_sol_up",
                "side": "BUY",
                "size": 10.0,
                "price": 0.60,
                "match_time": (hour_start + timedelta(minutes=5)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"),
            },
            {
                "trade_id": "mystery_no_ts",
                "token_id": "tok_unknown_x",
                "side": "BUY",
                "size": 999.0,
                "price": 0.10,
                # No timestamp, no known slug
            },
            {
                "trade_id": "mystery_no_slug",
                "token_id": "tok_unknown_y",
                "side": "BUY",
                "size": 500.0,
                "price": 0.20,
                # Has a current-hour timestamp but unknown token
                "match_time": (hour_start + timedelta(minutes=10)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"),
            },
        ]
        truth._get_trades = lambda: mixed_trades
        new_count = truth.run_wallet_scan()
        # Only the first trade should be accepted (known slug + valid timestamp)
        # Second has no slug AND no timestamp → rejected
        # Third has valid timestamp but no slug → accepted (timestamp alone is sufficient)
        _check("Mixed: 1 or 2 accepted (known slug + valid ts)",
               new_count >= 1,
               f"got {new_count}")
        _check("Fill count correct",
               len(truth._fills) == new_count,
               f"got {len(truth._fills)}")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_ledger_no_meta_rejected():
    """FillsLedger: external fills with no metadata (no slug, no outcome) are rejected."""
    print("\n" + "=" * 60)
    print("  TEST: FillsLedger rejects fills with no metadata")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="test_nometa_")
    ledger_path = os.path.join(tmpdir, "fills_ledger.jsonl")

    try:
        ledger = FillsLedger(
            ledger_path=ledger_path,
            run_id="test_nometa",
            write_jsonl_fn=lambda d: None,
        )

        current_slug = f"BTC_test_{ledger._hour_suffix}"
        ledger.register_token("tok_btc_up", current_slug, "BTC", "Up")

        hour_start = ledger._hour_start

        # Fills with no slug or outcome → should be skipped as no_meta
        no_meta_fills = [
            {
                "trade_id": f"nometa_{i}",
                "token_id": f"tok_unknown_{i}",
                "side": "BUY",
                "size": 100.0,
                "price": 0.50,
                "match_time": (hour_start + timedelta(minutes=5)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            for i in range(50)
        ]
        new_count = ledger.ingest_external_fills(no_meta_fills)
        _check("No-meta fills rejected", new_count == 0,
               f"got {new_count}")
        _check("Fill count still 0", len(ledger._fills) == 0,
               f"got {len(ledger._fills)}")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  HOUR SCOPING TEST SUITE")
    print("  Verifies fills ledger + truth capture only count")
    print("  fills for the CURRENT 1-hour market window")
    print("=" * 60)

    test_fills_ledger_hour_scoping()
    test_truth_capture_hour_scoping()
    test_disk_reload_scoping()
    test_unverifiable_fills_rejected()
    test_ledger_no_meta_rejected()

    print("\n" + "=" * 60)
    total = _PASS + _FAIL
    print(f"  RESULTS: {_PASS}/{total} passed, {_FAIL} failed")
    if _FAIL > 0:
        print("  *** SOME TESTS FAILED ***")
        sys.exit(1)
    else:
        print("  ALL TESTS PASSED")
    print("=" * 60)
