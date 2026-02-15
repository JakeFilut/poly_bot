#!/usr/bin/env python3
"""
polymarket_allowance_check.py

Polymarket USDC.e readiness checker for Polygon (Web3.py v6/v7/v8 friendly).

Fixes included:
- Works with Web3 versions where SignedTransaction uses raw_transaction (new) or rawTransaction (old)
- Version-safe POA middleware injection to avoid ExtraDataLengthError on some Polygon RPCs
- RPC rate-limit retry/backoff
- Fallback RPC rotation
- Checks:
  - POL/MATIC balance (for gas)
  - USDC.e balance (used by Polymarket)
  - USDC.e allowances to Polymarket spenders
- Optional:
  - Approve USDC.e (MaxUint256) to spenders and wait for receipts

Install:
  pip install web3 python-dotenv

.env:
  POLY_PRIVATE_KEY=0x....  (or without 0x; script adds it)
  (Only needed for --approve mode)

Run:
  # Read-only check for any wallet address:
  python polymarket_allowance_check.py --wallet 0xDAF73067678248A425F4527f6f7E07A894630D6d --fallback

  # Check wallet derived from private key:
  python polymarket_allowance_check.py --fallback

  # Approve USDC.e to Polymarket spenders (requires private key):
  python polymarket_allowance_check.py --approve --wait --rpc https://rpc.ankr.com/polygon
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from typing import Callable, List, Optional, Tuple

from dotenv import load_dotenv
from web3 import Web3


# -----------------------------
# Version-safe POA middleware
# -----------------------------
def inject_poa_middleware(w3: Web3) -> None:
    """
    Web3.py changed POA middleware APIs across versions.
    This injects the correct middleware when available.
    """
    # Web3.py <= 6 (common)
    try:
        from web3.middleware import geth_poa_middleware  # type: ignore
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        return
    except Exception:
        pass

    # Web3.py >= 7
    try:
        from web3.middleware.proof_of_authority import ExtraDataToPOAMiddleware  # type: ignore
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        return
    except Exception:
        pass

    # If not available, proceed without it (some RPCs don't require it)
    return


# -----------------------------
# Contracts (Polygon)
# -----------------------------
USDC_E = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")  # USDC.e (bridged) - used by Polymarket

# Polymarket spenders (USDC.e approvals typically needed)
SPENDERS = [
    Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),
    Web3.to_checksum_address("0xC5d563A36AE78145C45a50134d48A1215220f80a"),
    Web3.to_checksum_address("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"),
]

ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}, {"name": "_spender", "type": "address"}],
        "name": "allowance",
        "outputs": [{"name": "remaining", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": False,
        "inputs": [{"name": "_spender", "type": "address"}, {"name": "_value", "type": "uint256"}],
        "name": "approve",
        "outputs": [{"name": "success", "type": "bool"}],
        "type": "function",
    },
]


# -----------------------------
# Models
# -----------------------------
@dataclass
class AllowanceRow:
    spender: str
    allowance_usdce: float
    ok: bool


@dataclass
class Summary:
    rpc_used: str
    wallet: str
    pol_matic: float
    usdc_e: float
    min_allowance_usdce: float
    allowances: List[AllowanceRow]
    all_allowances_ok: bool
    ready_to_trade: bool
    approval_txs: List[str]
    notes: List[str]


# -----------------------------
# Helpers
# -----------------------------
def to_float_usdc(raw: int) -> float:
    return float(raw) / 1e6


def to_float_pol(raw_wei: int) -> float:
    return float(raw_wei) / 1e18


def looks_like_rate_limit(e: Exception) -> bool:
    s = str(e).lower()
    return "too many requests" in s or "rate limit" in s or "retry in" in s or "429" in s


def call_with_retry(fn: Callable[[], int], label: str, tries: int = 7, base_sleep: float = 1.2) -> int:
    last: Optional[Exception] = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if looks_like_rate_limit(e):
                sleep_s = base_sleep * (2 ** i)
                print(f"[{label}] rate limited, sleeping {sleep_s:.1f}s then retrying...")
                time.sleep(sleep_s)
                continue
            raise
    raise RuntimeError(f"[{label}] RPC error after {tries} retries: {last}")


def load_private_key(env_path: Optional[str] = None) -> str:
    if env_path:
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()

    pk = os.getenv("POLY_PRIVATE_KEY") or os.getenv("PRIVATE_KEY") or os.getenv("POLYGON_PRIVATE_KEY")
    if not pk:
        raise RuntimeError("Missing private key. Set POLY_PRIVATE_KEY (recommended) or PRIVATE_KEY / POLYGON_PRIVATE_KEY.")

    pk = pk.strip()
    if not pk.startswith("0x"):
        pk = "0x" + pk

    if len(pk) != 66:
        raise RuntimeError(f"Private key length looks wrong (got {len(pk)} chars). Expected 66: '0x' + 64 hex characters.")
    return pk


def connect_web3(primary_rpc: str, use_fallback: bool) -> Tuple[Web3, str]:
    rpcs = [primary_rpc]
    if use_fallback:
        rpcs += [
            "https://rpc.ankr.com/polygon",
            "https://polygon.llamarpc.com",
            "https://1rpc.io/matic",
            "https://polygon-bor.publicnode.com",
        ]

    last_err: Optional[str] = None
    for rpc in rpcs:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc))
            inject_poa_middleware(w3)

            if not w3.is_connected():
                last_err = f"not connected to {rpc}"
                continue

            cid = call_with_retry(lambda: w3.eth.chain_id, label="chain_id")
            if cid != 137:
                last_err = f"{rpc} returned chain_id={cid} (expected 137)"
                continue

            return w3, rpc
        except Exception as e:
            last_err = f"{rpc}: {e}"
            continue

    raise RuntimeError(f"Could not connect to a usable Polygon RPC. Last error: {last_err}")


def erc20(w3: Web3, token: str):
    return w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)


def allowance_ok(val: float, min_allow: float) -> bool:
    return val > 0.0 if min_allow <= 0 else val >= min_allow


def _signed_raw_tx_bytes(signed_tx) -> bytes:
    """
    Web3.py changed SignedTransaction field name across versions:
      - rawTransaction (older)
      - raw_transaction (newer)
    """
    if hasattr(signed_tx, "rawTransaction"):
        return signed_tx.rawTransaction  # type: ignore[attr-defined]
    if hasattr(signed_tx, "raw_transaction"):
        return signed_tx.raw_transaction  # type: ignore[attr-defined]
    raise AttributeError("SignedTransaction has neither rawTransaction nor raw_transaction")


def sign_and_send_approve(w3: Web3, token_contract, owner: str, pk: str, spender: str, nonce: int) -> str:
    max_uint = (1 << 256) - 1

    gas_price = call_with_retry(lambda: w3.eth.gas_price, label="gas_price")

    tx = token_contract.functions.approve(spender, max_uint).build_transaction(
        {
            "from": owner,
            "nonce": nonce,
            "gasPrice": gas_price,
        }
    )

    try:
        tx["gas"] = call_with_retry(lambda: w3.eth.estimate_gas(tx), label="estimate_gas")
    except Exception:
        tx["gas"] = 150_000

    signed = w3.eth.account.sign_transaction(tx, private_key=pk)
    raw_bytes = _signed_raw_tx_bytes(signed)
    tx_hash = call_with_retry(lambda: w3.eth.send_raw_transaction(raw_bytes), label="send_raw_tx")
    # Ensure tx hash has 0x prefix for consistency
    tx_hash_hex = tx_hash.hex()
    if not tx_hash_hex.startswith("0x"):
        tx_hash_hex = "0x" + tx_hash_hex
    return tx_hash_hex


def check_allowances(w3: Web3, usdc_e_c, wallet: str, min_allow: float) -> List[AllowanceRow]:
    """Check USDC.e allowances for all spenders."""
    allowances: List[AllowanceRow] = []
    for sp in SPENDERS:
        raw = call_with_retry(lambda sp=sp: usdc_e_c.functions.allowance(wallet, sp).call(), label="allowance")
        val = to_float_usdc(raw)
        allowances.append(AllowanceRow(spender=sp, allowance_usdce=val, ok=allowance_ok(val, min_allow)))
    return allowances


def build_notes(pol_matic: float, usdc_e: float, all_allowances_ok: bool) -> List[str]:
    """Build notes based on current state. Called after all operations complete."""
    notes: List[str] = []
    if pol_matic <= 0.0:
        notes.append("You have 0 POL/MATIC for gas. You need POL/MATIC to approve and trade.")
    if usdc_e <= 0.0:
        notes.append("You have 0 USDC.e. Polymarket trading typically spends USDC.e.")
    if not all_allowances_ok:
        notes.append("One or more USDC.e allowances are below the threshold.")
    return notes


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc", default="https://rpc.ankr.com/polygon", help="Primary Polygon RPC URL (recommended)")
    parser.add_argument("--fallback", action="store_true", help="Try fallback RPCs if primary fails/throttles")
    parser.add_argument("--min", type=float, default=0.0, help="Minimum allowance threshold in USDC.e (0 = any nonzero)")
    parser.add_argument("--approve", action="store_true", help="Approve USDC.e to Polymarket spenders if low")
    parser.add_argument("--wait", action="store_true", help="Wait for receipts after approval txs")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    parser.add_argument("--env", default=None, help=r"Explicit path to .env (optional), e.g. C:\path\to\.env")
    parser.add_argument("--wallet", default=None, help="Wallet address to check (read-only mode, no private key needed)")
    args = parser.parse_args()

    # Load private key only if needed (for approve mode or if no wallet specified)
    pk: Optional[str] = None
    if args.approve:
        try:
            pk = load_private_key(args.env)
        except Exception as e:
            print(f"ERROR: {e}")
            sys.exit(1)

    try:
        w3, rpc_used = connect_web3(args.rpc, args.fallback)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # Determine wallet address
    if args.wallet:
        # Use provided wallet address (read-only mode)
        wallet = Web3.to_checksum_address(args.wallet)
    elif pk:
        # Derive wallet from private key
        acct = w3.eth.account.from_key(pk)
        wallet = Web3.to_checksum_address(acct.address)
    else:
        # Try to load private key to get wallet
        try:
            pk = load_private_key(args.env)
            acct = w3.eth.account.from_key(pk)
            wallet = Web3.to_checksum_address(acct.address)
        except Exception as e:
            print(f"ERROR: Must specify --wallet or have a private key in .env: {e}")
            sys.exit(1)

    approval_txs: List[str] = []

    usdc_e_c = erc20(w3, USDC_E)

    # Fetch balances (USDC.e is what Polymarket uses)
    pol_matic = to_float_pol(call_with_retry(lambda: w3.eth.get_balance(wallet), label="pol_balance"))
    usdc_e = to_float_usdc(call_with_retry(lambda: usdc_e_c.functions.balanceOf(wallet).call(), label="usdce_balance"))

    # Check allowances
    allowances = check_allowances(w3, usdc_e_c, wallet, args.min)
    all_allowances_ok = all(a.ok for a in allowances)

    # Handle approvals if requested
    if args.approve:
        if not pk:
            print("ERROR: --approve requires a private key. Set POLY_PRIVATE_KEY in .env")
            sys.exit(1)
        nonce = call_with_retry(lambda: w3.eth.get_transaction_count(wallet), label="nonce")
        sent = 0
        failed_txs: List[str] = []

        for row in allowances:
            if row.ok:
                continue
            txh = sign_and_send_approve(w3, usdc_e_c, wallet, pk, row.spender, nonce + sent)
            approval_txs.append(txh)
            sent += 1

            if args.wait:
                try:
                    receipt = call_with_retry(lambda txh=txh: w3.eth.wait_for_transaction_receipt(txh, timeout=180), label="wait_receipt")
                    if receipt and receipt.get("status") != 1:
                        failed_txs.append(f"Approve tx failed (status!=1): {txh}")
                except Exception as e:
                    failed_txs.append(f"Timed out waiting for receipt for {txh}: {e}")

        # Re-check allowances after approvals
        if approval_txs:
            allowances = check_allowances(w3, usdc_e_c, wallet, args.min)
            all_allowances_ok = all(a.ok for a in allowances)

    # Determine final state
    ready_to_trade = (pol_matic > 0.0) and (usdc_e > 0.0) and all_allowances_ok

    # Build notes AFTER all operations complete (fixes the stale notes bug)
    notes = build_notes(pol_matic, usdc_e, all_allowances_ok)

    # Add any failed tx notes
    if args.approve and 'failed_txs' in dir() and failed_txs:
        notes.extend(failed_txs)

    summary = Summary(
        rpc_used=rpc_used,
        wallet=wallet,
        pol_matic=pol_matic,
        usdc_e=usdc_e,
        min_allowance_usdce=args.min,
        allowances=allowances,
        all_allowances_ok=all_allowances_ok,
        ready_to_trade=ready_to_trade,
        approval_txs=approval_txs,
        notes=notes,
    )

    if args.json:
        out = asdict(summary)
        out["allowances"] = [asdict(a) for a in summary.allowances]
        print(json.dumps(out, indent=2))
        return

    print("\n==============================")
    print("Polymarket USDC.e Trade Check")
    print("==============================")
    print(f"RPC used:     {summary.rpc_used}")
    print(f"Wallet:       {summary.wallet}")
    print(f"POL/MATIC:    {summary.pol_matic:.6f}")
    print(f"USDC.e:       {summary.usdc_e:.6f}   ({USDC_E})")
    print(
        f"Min allowance: {summary.min_allowance_usdce:.6f} USDC.e "
        f"({'any nonzero' if summary.min_allowance_usdce <= 0 else 'threshold'})"
    )

    print("\nAllowances (USDC.e):")
    for row in summary.allowances:
        print(f"  {'OK' if row.ok else 'LOW'}  {row.spender}  allowance={row.allowance_usdce:.6f}")

    print(f"\nAll allowances OK: {summary.all_allowances_ok}")
    print(f"Ready to trade:    {summary.ready_to_trade}")

    if summary.approval_txs:
        print("\nApproval txs sent:")
        for txh in summary.approval_txs:
            print(f"  {txh}")

    if summary.notes:
        print("\nNotes:")
        for n in summary.notes:
            print(f" - {n}")

    print("\nDone.\n")


if __name__ == "__main__":
    main()
