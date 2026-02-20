#!/usr/bin/env python3
"""Emergency sell-all: dumps every position at the best available bid.

Usage:  python sell_all.py
        python sell_all.py --dry-run     (show positions without selling)

Reads on-chain conditional token balances for all current-hour markets,
loads cost basis from state.json (VWAP), sells each position at the best
bid (or bid - 1c for faster fill), logs every trade, and updates state.json
with proceeds and realised P&L.
"""
import json, math, os, sys, time
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3

# Load .env
_PROJECT_DIR = Path(__file__).resolve().parent
_KEYS_DIR = os.getenv("KEYS_DIR", str(_PROJECT_DIR.parent / "keys"))
for env_path in [os.path.join(_KEYS_DIR, ".env"), str(_PROJECT_DIR / ".env")]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break

os.environ["MODE"] = "LIVE"

from src.feeds.polymarket import PolymarketClient
from src.utils.logging import log_trade
from src.config.settings import LEDGER_ENABLED, LEDGER_PATH
from src.execution.fills_ledger import FillsLedger
from py_clob_client.clob_types import OrderArgs, OrderType

DRY_RUN = "--dry-run" in sys.argv or "-n" in sys.argv

PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
if not PRIVATE_KEY:
    print("ERROR: POLYMARKET_PRIVATE_KEY not found")
    sys.exit(1)

# ── State file path (same as main bot uses) ──
_LOG_DIR = os.path.join(os.path.dirname(str(_PROJECT_DIR)), "logs", "poly_bot")
STATE_FILE = os.getenv("STATE_FILE", os.path.join(_LOG_DIR, "state.json"))

print("=" * 55)
print("  SELL ALL POSITIONS" + ("  [DRY RUN]" if DRY_RUN else ""))
print("=" * 55)

# ── Init ──
feed = PolymarketClient()
clob = feed._clob
wallet = Web3.to_checksum_address(feed._wallet_address)
print(f"  Wallet: {wallet}")

# ── Init fills ledger ──
_ledger = None
if LEDGER_ENABLED:
    _ledger = FillsLedger(ledger_path=LEDGER_PATH, run_id="sell_all")
    _ledger.load_from_disk()
    print(f"  Ledger loaded: {LEDGER_PATH}")

# ── Load state.json for cost basis ──
state_data = {}
if os.path.exists(STATE_FILE):
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state_data = json.load(f)
        print(f"  State loaded: {STATE_FILE}")
    except Exception as e:
        print(f"  WARN: Could not load state.json: {e}")
else:
    print(f"  WARN: No state.json found at {STATE_FILE}")
    print(f"         P&L will not be calculated (no cost basis).")

def _get_state_position(slug: str, outcome: str) -> dict:
    """Look up position data from state.json for a given slug+outcome."""
    ms = state_data.get("market_states", {})
    st = ms.get(slug, {})
    positions = st.get("positions", {})
    return positions.get(outcome, {})

# Web3 for on-chain balance checks
w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
if not w3.is_connected():
    w3 = Web3(Web3.HTTPProvider("https://rpc-mainnet.matic.quiknode.pro"))

CT_ADDR = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
ct_abi = [{"inputs": [{"name": "account", "type": "address"},
                       {"name": "id", "type": "uint256"}],
           "name": "balanceOf",
           "outputs": [{"name": "", "type": "uint256"}],
           "stateMutability": "view", "type": "function"}]
ct = w3.eth.contract(address=Web3.to_checksum_address(CT_ADDR), abi=ct_abi)

# ── Ensure sell approvals ──
OPERATORS = [
    ("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8ED0a90", "CTF Exchange (legacy)"),
    ("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E", "CTF Exchange (SDK)"),
    ("0xC5d563A36AE78145C45a50134d48A1215220f80a", "Neg Risk CTF Exchange"),
    ("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296", "Neg Risk Adapter"),
]
approval_abi = [
    {"inputs": [{"name": "operator", "type": "address"},
                {"name": "approved", "type": "bool"}],
     "name": "setApprovalForAll", "outputs": [],
     "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "owner", "type": "address"},
                {"name": "operator", "type": "address"}],
     "name": "isApprovedForAll",
     "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "view", "type": "function"},
]
ct_approval = w3.eth.contract(address=Web3.to_checksum_address(CT_ADDR),
                               abi=approval_abi)

print("\n[1] Checking sell approvals...")
for op_addr, op_name in OPERATORS:
    op = Web3.to_checksum_address(op_addr)
    approved = ct_approval.functions.isApprovedForAll(wallet, op).call()
    if approved:
        print(f"    {op_name}: OK")
    else:
        print(f"    {op_name}: NOT approved — sending tx...")
        nonce = w3.eth.get_transaction_count(wallet)
        tx = ct_approval.functions.setApprovalForAll(op, True).build_transaction({
            'from': wallet, 'nonce': nonce, 'gas': 100000,
            'gasPrice': w3.eth.gas_price, 'chainId': 137,
        })
        signed_tx = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt['status'] == 1:
            print(f"    {op_name}: approved OK")
        else:
            print(f"    {op_name}: FAILED — sell may not work")

# ── Discover markets ──
print("\n[2] Discovering current-hour markets...")
markets = feed.get_current_hour_markets()
if not markets:
    print("  No markets found for current hour.")
    sys.exit(0)

for m in markets:
    print(f"  {m.crypto}: {m.slug}")

# ── Check balances for each token ──
print("\n[3] Checking on-chain balances...")
positions = []  # (market, outcome, token_id, qty, bid, vwap, cost_usdc)

for m in markets:
    for outcome, token_id in [("Up", m.outcome_up_id), ("Down", m.outcome_down_id)]:
        if not token_id:
            continue
        try:
            raw = ct.functions.balanceOf(wallet, int(token_id)).call()
            # Polymarket CT tokens use 6 decimal places (like USDC)
            qty = float(raw) / 1e6
        except Exception as e:
            print(f"  ERROR reading {m.crypto} {outcome}: {e}")
            continue

        if qty < 0.01:
            continue

        # Get orderbook for price
        try:
            book = feed.get_top_of_book(token_id)
            bid = book.bid if book else 0.0
        except Exception:
            bid = 0.0

        # Get cost basis from state.json
        state_pos = _get_state_position(m.slug, outcome)
        vwap = state_pos.get("vwap", 0.0)
        cost_usdc = state_pos.get("cost_usdc", 0.0)

        positions.append((m, outcome, token_id, qty, bid, vwap, cost_usdc))
        value = qty * bid if bid > 0 else 0
        cost_display = f"  vwap=${vwap:.3f}" if vwap > 0 else "  vwap=N/A"
        print(f"  {m.crypto:4s} {outcome:5s}: {qty:10.2f} shares  "
              f"bid=${bid:.3f}{cost_display}  value=${value:.2f}")

if not positions:
    print("\n  No positions found — wallet is flat.")
    sys.exit(0)

total_value = sum(qty * bid for _, _, _, qty, bid, _, _ in positions)
total_cost = sum(cost for _, _, _, _, _, _, cost in positions)
print(f"\n  Total positions: {len(positions)}  |  "
      f"Estimated value: ${total_value:.2f}  |  "
      f"Total cost: ${total_cost:.2f}")
if total_cost > 0:
    print(f"  Estimated P&L: ${total_value - total_cost:.2f}")

if DRY_RUN:
    print("\n  [DRY RUN] — not selling. Run without --dry-run to sell.")
    sys.exit(0)

# ── Confirm ──
print(f"\n  About to sell ALL {len(positions)} positions.")
confirm = input("  Type 'yes' to confirm: ").strip().lower()
if confirm != "yes":
    print("  Aborted.")
    sys.exit(0)

# ── Sell each position ──
print("\n[4] Selling positions...")
results = []  # (crypto, outcome, sell_qty, fill_price, status, vwap, pnl)

for m, outcome, token_id, qty, bid, vwap, cost_usdc in positions:
    sell_qty = qty  # preserve full precision — never cast to int
    if sell_qty < 0.01:
        print(f"  SKIP {m.crypto} {outcome}: qty={qty:.6f} < 0.01 share")
        continue

    # Sell at bid - 1c for faster fill (aggressive)
    sell_price = max(0.01, round(bid - 0.01, 3)) if bid > 0.02 else 0.01

    print(f"\n  SELL {m.crypto} {outcome}: {sell_qty} shares @ ${sell_price:.3f}")

    filled = False
    fill_price = 0.0
    fill_order_id = ""
    for attempt in range(3):
        try:
            price = max(0.01, round(sell_price - (attempt * 0.02), 3))
            args = OrderArgs(
                price=price,
                size=sell_qty,
                side="SELL",
                token_id=token_id,
            )
            signed = clob.create_order(args)
            response = clob.post_order(signed, OrderType.GTC)

            if response and isinstance(response, dict):
                status = response.get("status", "").lower()
                oid = response.get("orderID", "")
                tx_hashes = response.get("transactionsHashes", [])
                size_matched = response.get("size_matched", 0)

                if status == "matched" and tx_hashes:
                    print(f"    SOLD @ ${price:.3f} (matched)")
                    filled = True
                    fill_price = price
                    fill_order_id = oid
                    break
                elif status == "live" and oid:
                    # Poll briefly for fill
                    print(f"    Order resting (attempt {attempt+1})... polling")
                    for _ in range(5):
                        time.sleep(1)
                        try:
                            order = clob.get_order(oid)
                            if order and order.get("status", "").lower() in ("matched", "filled"):
                                print(f"    SOLD @ ${price:.3f} (filled after polling)")
                                filled = True
                                fill_price = price
                                fill_order_id = oid
                                break
                        except Exception:
                            pass
                    if filled:
                        break
                    # Cancel resting order, try lower price
                    try:
                        clob.cancel(order_id=oid)
                    except Exception:
                        pass
                else:
                    print(f"    Attempt {attempt+1}: status={status}")
        except Exception as e:
            err_msg = str(e)[:100]
            print(f"    Attempt {attempt+1} ERROR: {err_msg}")
            # If allowance error, wait and retry
            if "allowance" in err_msg.lower() or "not enough" in err_msg.lower():
                time.sleep(3)

    # Calculate P&L
    if filled:
        proceeds = fill_price * sell_qty
        cost_basis = vwap * sell_qty if vwap > 0 else 0.0
        pnl = proceeds - cost_basis if vwap > 0 else 0.0
        results.append((m.crypto, outcome, sell_qty, fill_price, "FILLED",
                        vwap, pnl, m.slug, token_id))
        # Log the trade
        log_trade(
            action="SELL",
            token_id=token_id,
            side="SELL",
            quantity=sell_qty,
            price=fill_price,
            order_type="GTC",
            status="FILLED",
            order_id=fill_order_id,
            notes=f"sell_all {m.crypto} {outcome} pnl={pnl:.2f}",
        )
        # Record in fills ledger
        if _ledger is not None:
            _ledger.record_fill(
                slug=m.slug, crypto=m.crypto, token_id=token_id,
                market_id=m.market_id, side=outcome, action="SELL",
                fill_qty=sell_qty, fill_price=fill_price,
                order_id=fill_order_id, source="bot",
                extra={"reason": "sell_all"},
            )
        print(f"    Proceeds: ${proceeds:.2f}  "
              f"Cost basis: ${cost_basis:.2f}  "
              f"P&L: ${pnl:+.2f}")
    else:
        print(f"    FAILED to sell {m.crypto} {outcome}")
        results.append((m.crypto, outcome, sell_qty, 0, "FAILED",
                        vwap, 0.0, m.slug, token_id))
        log_trade(
            action="SELL",
            token_id=token_id,
            side="SELL",
            quantity=sell_qty,
            price=sell_price,
            order_type="GTC",
            status="FAILED",
            notes=f"sell_all {m.crypto} {outcome} FAILED after 3 attempts",
        )

# ── Update state.json ──
if state_data and any(r[4] == "FILLED" for r in results):
    print("\n[5] Updating state.json...")
    ms = state_data.get("market_states", {})
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for crypto, outcome, sell_qty, fill_price, status, vwap, pnl, slug, token_id in results:
        if status != "FILLED":
            continue
        st = ms.get(slug, {})
        pos = st.get("positions", {}).get(outcome, {})
        if not pos:
            continue

        proceeds = fill_price * sell_qty
        cost_basis = vwap * sell_qty if vwap > 0 else 0.0

        # Update position: subtract sold qty
        old_qty = pos.get("qty", 0.0)
        new_qty = max(0.0, old_qty - sell_qty)
        pos["qty"] = 0.0 if new_qty < 0.01 else new_qty
        pos["cost_usdc"] = max(0.0, pos.get("cost_usdc", 0.0) - cost_basis)
        pos["last_trade_ts"] = now_iso

        # Zero out dust
        if pos["qty"] < 0.01:
            pos["qty"] = 0.0
            pos["cost_usdc"] = 0.0

        # Update cash and realized P&L at top level
        state_data["cash_usdc"] = state_data.get("cash_usdc", 0.0) + proceeds
        state_data["realized_pnl_usdc"] = state_data.get("realized_pnl_usdc", 0.0) + pnl
        state_data["hourly_pnl_usdc"] = state_data.get("hourly_pnl_usdc", 0.0) + pnl

    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state_data, f, separators=(",", ":"))
        print(f"  State saved: {STATE_FILE}")
    except Exception as e:
        print(f"  ERROR saving state: {e}")

# ── Summary ──
print("\n" + "=" * 55)
print("  SELL SUMMARY")
print("=" * 55)
total_proceeds = 0.0
total_pnl = 0.0
for crypto, outcome, qty, price, status, vwap, pnl, slug, token_id in results:
    proceeds = qty * price if status == "FILLED" else 0
    total_proceeds += proceeds
    total_pnl += pnl
    marker = "OK" if status == "FILLED" else "FAILED"
    cost_str = f"vwap=${vwap:.3f}" if vwap > 0 else "vwap=N/A"
    pnl_str = f"pnl=${pnl:+.2f}" if status == "FILLED" and vwap > 0 else ""
    print(f"  {marker:6s}  {crypto:4s} {outcome:5s}  {qty:10.2f} shares @ ${price:.3f}  "
          f"= ${proceeds:.2f}  {cost_str}  {pnl_str}")

print(f"\n  Total proceeds: ${total_proceeds:.2f}")
if total_pnl != 0:
    print(f"  Total P&L:      ${total_pnl:+.2f}")
print(f"  Positions attempted: {len(results)}")
print(f"  Filled: {sum(1 for r in results if r[4] == 'FILLED')}")
print(f"  Failed: {sum(1 for r in results if r[4] == 'FAILED')}")
