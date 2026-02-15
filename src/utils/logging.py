"""
Logging utilities for the Polymarket trader bot.
Provides structured logging with CSV trade log output.
"""

import csv
import logging
import os
import sys
from datetime import datetime, timezone

from src.utils.config import LOG_DIR


def setup_logging(debug: bool = False) -> logging.Logger:
    """
    Set up the application logger.
    Logs to both console and file.
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger("polymarket_trader")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    # Avoid duplicate handlers on re-init
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if debug else logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler
    log_file = os.path.join(LOG_DIR, "trader.log")
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# ---------------------------------------------------------------------------
# Trade CSV logger
# ---------------------------------------------------------------------------

TRADE_LOG_HEADER = [
    "timestamp",
    "action",       # BUY, SELL, CANCEL
    "token_id",
    "side",         # BUY or SELL
    "quantity",
    "price",
    "order_type",   # FOK, GTC, FAK
    "status",       # FILLED, PARTIAL, FAILED, CANCELLED
    "order_id",
    "tx_hash",
    "notes",
]

_trade_log_path = os.path.join(LOG_DIR, "trades.csv")


def _ensure_trade_csv():
    os.makedirs(LOG_DIR, exist_ok=True)
    if not os.path.exists(_trade_log_path):
        with open(_trade_log_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(TRADE_LOG_HEADER)


def log_trade(
    action: str,
    token_id: str = "",
    side: str = "",
    quantity: float = 0,
    price: float = 0,
    order_type: str = "",
    status: str = "",
    order_id: str = "",
    tx_hash: str = "",
    notes: str = "",
):
    """Append a row to the trades CSV log."""
    _ensure_trade_csv()
    row = [
        datetime.now(timezone.utc).isoformat(timespec="seconds"),
        action,
        token_id[-12:] if token_id else "",
        side,
        quantity,
        f"{price:.4f}" if price else "",
        order_type,
        status,
        order_id[:16] if order_id else "",
        tx_hash[:16] if tx_hash else "",
        notes,
    ]
    with open(_trade_log_path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)
