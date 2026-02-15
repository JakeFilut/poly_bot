#!/usr/bin/env python3
"""
Polymarket Trader - single non-interactive entrypoint.

Usage:
    python -m src.run_trader          # from repo root
    python src/run_trader.py          # also works

Runs until killed (Ctrl+C / SIGTERM). No input() prompts.
Suitable for systemd / tmux / screen.
"""

import os
import signal
import sys
import time

# Ensure repo root is on sys.path so `src.*` imports work
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.utils.config import load_config, SCAN_INTERVAL_SECONDS, LOG_DIR
from src.utils.logging import setup_logging
from src.clients.polymarket_client import PolymarketClient
from src.trader.order_service import OrderService
from src.trader.position_service import PositionService


# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    _shutdown = True
    print("\n[trader] Shutdown signal received, cleaning up...")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    global _shutdown

    # --- Config -----------------------------------------------------------
    cfg = load_config()
    logger = setup_logging(debug=cfg["debug"])

    logger.info("=" * 60)
    logger.info("  POLYMARKET TRADER  -  starting up")
    logger.info("=" * 60)

    # Log non-secret config
    logger.info(f"  Log dir    : {cfg['log_dir']}")
    logger.info(f"  Debug      : {cfg['debug']}")
    logger.info(f"  Scan interval: {SCAN_INTERVAL_SECONDS}s")

    os.makedirs(LOG_DIR, exist_ok=True)

    # --- Signal handlers --------------------------------------------------
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # --- Initialize client ------------------------------------------------
    logger.info("Connecting to Polymarket...")
    client = PolymarketClient(cfg["polymarket_private_key"])

    orders = OrderService(client)
    positions = PositionService(client)

    # --- Startup checks ---------------------------------------------------
    balance = positions.get_balance()
    if balance is not None:
        logger.info(f"  USDC.e balance: ${balance:.2f}")
    else:
        logger.warning("  Could not fetch balance - continuing anyway")

    # Show open positions
    pos_list = positions.get_positions()
    logger.info(f"  Open positions: {len(pos_list)}")

    # Redeem any settled positions at startup
    redeemable = positions.get_redeemable()
    if redeemable:
        logger.info(f"  Found {len(redeemable)} redeemable positions, redeeming...")
        positions.redeem_all()

    logger.info("")
    logger.info("Entering main loop (Ctrl+C to stop)")
    logger.info("-" * 60)

    # --- Main loop --------------------------------------------------------
    cryptos = ["BTC", "ETH", "SOL", "XRP"]

    while not _shutdown:
        try:
            for crypto in cryptos:
                if _shutdown:
                    break

                prices = client.get_prices(crypto)
                if prices is None:
                    continue

                up_price = prices["up_buy"]
                dn_price = prices["down_buy"]
                total = up_price + dn_price

                logger.info(
                    f"  {crypto:4s}  UP=${up_price:.3f}  DN=${dn_price:.3f}  "
                    f"sum=${total:.3f}  spread={abs(1.0 - total):.3f}"
                )

            # Sleep between scans
            for _ in range(int(SCAN_INTERVAL_SECONDS * 10)):
                if _shutdown:
                    break
                time.sleep(0.1)

        except KeyboardInterrupt:
            _shutdown = True
        except Exception as e:
            logger.error(f"Loop error: {e}")
            if not _shutdown:
                time.sleep(5)

    # --- Shutdown ---------------------------------------------------------
    logger.info("")
    logger.info("=" * 60)
    logger.info("  POLYMARKET TRADER  -  shutdown complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
