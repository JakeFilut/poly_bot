#!/usr/bin/env python3
"""Standalone validator: compare fills ledger vs wallet positions vs recent trades.

Usage:
    python scripts/audit_wallet_vs_ledger.py [--ledger ./logs/fills_ledger.jsonl]

Exits non-zero if mismatches exist.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from decimal import Decimal, InvalidOperation

# Allow imports from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_ZERO = Decimal("0")
_DUST = Decimal("0.01")


def _to_dec(v) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v is None:
        return _ZERO
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return _ZERO


def load_ledger(path: str) -> dict:
    """Load ledger and compute positions.
    Returns {token_id: {"slug", "outcome", "net_qty", "avg_price", "buys", "sells"}}
    """
    positions = {}
    fill_count = 0

    if not os.path.exists(path):
        print(f"ERROR: ledger not found at {path}")
        return positions

    with open(path, "r") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                print(f"  WARN: malformed line {line_num}")
                continue

            action = (raw.get("action") or "").upper()
            if action not in ("BUY", "SELL"):
                continue

            token_id = str(raw.get("token_id", ""))
            qty = _to_dec(raw.get("qty") or raw.get("fill_qty") or 0)
            price = _to_dec(raw.get("price") or raw.get("fill_price") or 0)
            slug = raw.get("slug", "")
            outcome = raw.get("outcome") or raw.get("side", "")

            if qty <= _ZERO or not token_id:
                continue

            if token_id not in positions:
                positions[token_id] = {
                    "slug": slug, "outcome": outcome,
                    "net_qty": _ZERO, "avg_price": _ZERO,
                    "total_cost": _ZERO,
                    "buys": 0, "sells": 0,
                }
            pos = positions[token_id]
            pos["slug"] = slug or pos["slug"]
            pos["outcome"] = outcome or pos["outcome"]

            if action == "BUY":
                old_cost = pos["avg_price"] * pos["net_qty"]
                new_cost = qty * price
                new_qty = pos["net_qty"] + qty
                if new_qty > _ZERO:
                    pos["avg_price"] = (old_cost + new_cost) / new_qty
                pos["net_qty"] = new_qty
                pos["total_cost"] = pos["avg_price"] * pos["net_qty"]
                pos["buys"] += 1
            elif action == "SELL":
                sell_qty = min(qty, pos["net_qty"])
                pos["net_qty"] -= sell_qty
                pos["total_cost"] = pos["avg_price"] * pos["net_qty"]
                pos["sells"] += 1

            if pos["net_qty"] < Decimal("0.001"):
                pos["net_qty"] = _ZERO
                pos["avg_price"] = _ZERO
                pos["total_cost"] = _ZERO

            fill_count += 1

    print(f"Loaded {fill_count} fills from ledger")
    return positions


def fetch_wallet_balances() -> dict:
    """Fetch wallet balances from Polymarket CLOB. Returns {token_id: qty}."""
    try:
        from src.feeds.polymarket import PolymarketClient
        client = PolymarketClient()
        balances = client.get_balances()
        result = {}
        for b in balances:
            tid = str(b.get("token_id") or b.get("asset_id") or "")
            bal = float(b.get("balance") or b.get("size") or 0)
            if tid and bal > 0.001:
                result[tid] = bal
        return result
    except Exception as e:
        print(f"ERROR fetching wallet balances: {e}")
        return {}


def fetch_recent_trades() -> list:
    """Fetch recent trades from Polymarket."""
    try:
        from src.feeds.polymarket import PolymarketClient
        client = PolymarketClient()
        if hasattr(client, '_clob') and client._clob:
            trades = client._clob.get_trades()
            if isinstance(trades, list):
                return trades
        return []
    except Exception as e:
        print(f"ERROR fetching recent trades: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Audit wallet vs ledger")
    parser.add_argument("--ledger", default="./logs/fills_ledger.jsonl",
                        help="Path to fills ledger JSONL")
    parser.add_argument("--tolerance", type=float, default=0.01,
                        help="Qty mismatch tolerance (shares)")
    parser.add_argument("--skip-wallet", action="store_true",
                        help="Skip wallet fetch (ledger-only audit)")
    args = parser.parse_args()

    print("=" * 70)
    print("  WALLET vs LEDGER AUDIT")
    print("=" * 70)

    # 1. Load ledger positions
    ledger_pos = load_ledger(args.ledger)
    active_ledger = {k: v for k, v in ledger_pos.items()
                     if v["net_qty"] > _ZERO}

    print(f"\nLedger active positions: {len(active_ledger)}")
    for tid, p in sorted(active_ledger.items(), key=lambda x: x[1]["slug"]):
        print(f"  {p['slug']:<40s} {p['outcome']:<5s} "
              f"qty={float(p['net_qty']):8.2f}  avg={float(p['avg_price']):.4f}  "
              f"cost=${float(p['total_cost']):7.2f}  "
              f"({p['buys']}B/{p['sells']}S)")

    if args.skip_wallet:
        print("\n--skip-wallet: skipping wallet comparison")
        sys.exit(0)

    # 2. Fetch wallet balances
    print("\nFetching wallet balances...")
    wallet = fetch_wallet_balances()
    print(f"Wallet positions: {len(wallet)}")
    for tid, qty in sorted(wallet.items()):
        slug = active_ledger.get(tid, {}).get("slug", tid[-16:])
        outcome = active_ledger.get(tid, {}).get("outcome", "?")
        print(f"  {slug:<40s} {outcome:<5s} qty={qty:8.2f}")

    # 3. Compare
    print("\n" + "=" * 70)
    print("  COMPARISON")
    print("=" * 70)

    all_tids = set(wallet.keys()) | set(active_ledger.keys())
    mismatches = []
    tol = args.tolerance

    for tid in sorted(all_tids):
        ledger_qty = float(active_ledger.get(tid, {}).get("net_qty", 0))
        wallet_qty = wallet.get(tid, 0.0)
        diff = wallet_qty - ledger_qty
        abs_diff = abs(diff)

        slug = active_ledger.get(tid, {}).get("slug", tid[-16:])
        outcome = active_ledger.get(tid, {}).get("outcome", "?")

        if abs_diff < tol:
            status = "OK"
        else:
            status = "MISMATCH"
            mismatches.append({
                "token_id": tid, "slug": slug, "outcome": outcome,
                "ledger": ledger_qty, "wallet": wallet_qty, "diff": diff,
            })

        print(f"  [{status:>8s}] {slug:<35s} {outcome:<5s} "
              f"ledger={ledger_qty:8.2f}  wallet={wallet_qty:8.2f}  "
              f"diff={diff:+8.4f}")

    # 4. Fetch recent trades for context
    print("\nFetching recent trades for context...")
    trades = fetch_recent_trades()
    if trades:
        print(f"Recent trades: {len(trades)}")
        for t in trades[-10:]:
            tid = t.get("token_id", t.get("asset_id", "?"))
            action = t.get("side", t.get("action", "?"))
            qty = t.get("size", t.get("amount", "?"))
            price = t.get("price", "?")
            trade_id = t.get("trade_id", t.get("id", "?"))
            print(f"    {action:<5s} qty={qty}  price={price}  "
                  f"tid=...{str(tid)[-12:]}  trade_id={str(trade_id)[:16]}")

    # 5. Summary
    print("\n" + "=" * 70)
    if mismatches:
        print(f"  RESULT: {len(mismatches)} MISMATCHES FOUND")
        for m in mismatches:
            print(f"    {m['slug']} {m['outcome']}: "
                  f"ledger={m['ledger']:.4f} wallet={m['wallet']:.4f} "
                  f"diff={m['diff']:+.4f}")
        print("=" * 70)
        sys.exit(1)
    else:
        print("  RESULT: ALL POSITIONS MATCH")
        print("=" * 70)
        sys.exit(0)


if __name__ == "__main__":
    main()
