#!/usr/bin/env python3
"""Pre-flight check: verify ERC1155 approvals + token balances for selling.
Run this BEFORE launching the bot to confirm sells will work.

Usage:  python check_sell_ready.py
"""
import os, sys
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3

# Load .env the same way the bot does (../keys/.env or ./.env)
_PROJECT_DIR = Path(__file__).resolve().parent
_KEYS_DIR = os.getenv("KEYS_DIR", str(_PROJECT_DIR.parent / "keys"))
for env_path in [os.path.join(_KEYS_DIR, ".env"), str(_PROJECT_DIR / ".env")]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"Loaded: {env_path}")
        break

PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
if not PRIVATE_KEY:
    print("ERROR: POLYMARKET_PRIVATE_KEY not found")
    print(f"  Searched: {_KEYS_DIR}/.env and {_PROJECT_DIR}/.env")
    sys.exit(1)

# Polygon RPC
w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))
if not w3.is_connected():
    w3 = Web3(Web3.HTTPProvider("https://rpc-mainnet.matic.quiknode.pro"))
if not w3.is_connected():
    print("ERROR: Cannot connect to Polygon RPC")
    sys.exit(1)

acct = w3.eth.account.from_key(PRIVATE_KEY)
wallet = acct.address
print(f"Wallet: {wallet}")
print(f"MATIC:  {w3.from_wei(w3.eth.get_balance(wallet), 'ether'):.4f}")

# Contracts
CONDITIONAL_TOKENS    = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_EXCHANGE          = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8ED0a90"
NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
NEG_RISK_ADAPTER      = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

approval_abi = [
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "operator", "type": "address"}],
     "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "view", "type": "function"},
]
balance_abi = [
    {"inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}],
     "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
]

ct = w3.eth.contract(address=Web3.to_checksum_address(CONDITIONAL_TOKENS),
                     abi=approval_abi + balance_abi)

# Check approvals
print("\n--- ERC1155 Approvals (required for selling) ---")
CTF_EXCHANGE_SDK = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
operators = [
    (CTF_EXCHANGE, "CTF Exchange (legacy)"),
    (CTF_EXCHANGE_SDK, "CTF Exchange (SDK)"),
    (NEG_RISK_CTF_EXCHANGE, "Neg Risk CTF Exchange"),
    (NEG_RISK_ADAPTER, "Neg Risk Adapter"),
]
all_approved = True
for addr, name in operators:
    approved = ct.functions.isApprovedForAll(
        Web3.to_checksum_address(wallet),
        Web3.to_checksum_address(addr),
    ).call()
    status = "APPROVED" if approved else "NOT APPROVED"
    print(f"  {name:25s} {status}")
    if not approved:
        all_approved = False

# Check token balances for known positions (from state file)
print("\n--- Conditional Token Balances ---")
try:
    import json
    state_files = [f for f in os.listdir(".") if f.startswith("bot_state") and f.endswith(".json")]
    if not state_files:
        state_files = [f for f in os.listdir("logs") if f.startswith("bot_state") and f.endswith(".json")]
    if state_files:
        with open(state_files[0]) as f:
            state = json.load(f)
        for slug, mdata in state.get("markets", {}).items():
            for outcome in ["Up", "Down"]:
                pos = mdata.get("positions", {}).get(outcome, {})
                qty = pos.get("qty", 0)
                if qty > 0:
                    token_id = mdata.get(f"outcome_{'up' if outcome == 'Up' else 'down'}_id", "")
                    if token_id:
                        on_chain = ct.functions.balanceOf(
                            Web3.to_checksum_address(wallet),
                            int(token_id),
                        ).call()
                        match = "OK" if on_chain >= int(qty) else f"MISMATCH (on-chain={on_chain})"
                        crypto = mdata.get("crypto", "?")
                        print(f"  {crypto:4s} {outcome:5s}  bot={int(qty):4d}  chain={on_chain:4d}  {match}")
    else:
        print("  (no state file found — skipping balance check)")
except Exception as e:
    print(f"  (balance check error: {e})")

# USDC balance
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
usdc_abi = [{"inputs": [{"name": "account", "type": "address"}], "name": "balanceOf",
             "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}]
usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_E), abi=usdc_abi)
usdc_bal = usdc.functions.balanceOf(Web3.to_checksum_address(wallet)).call()
print(f"\nUSDC.e: ${usdc_bal / 1e6:.2f}")

# Verdict
print("\n" + "=" * 50)
if all_approved:
    print("SELL READY: All exchange contracts are approved.")
else:
    print("SELL BLOCKED: Missing approvals! The bot will attempt")
    print("to approve on startup, but check MATIC balance for gas.")
print("=" * 50)
