"""
config.py – All configuration for the F247-style Polymarket scalper.

Every knob is an env-var with a safe default.  Validated once at startup.
Secrets are redacted when printed.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field, fields
from typing import List


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_float(key: str, default: float) -> float:
    return float(_env(key, str(default)))


def _env_int(key: str, default: int) -> int:
    return int(_env(key, str(default)))


def _env_bool(key: str, default: bool) -> bool:
    return _env(key, str(default)).lower() in ("1", "true", "yes")


def _env_list_float(key: str, default: List[float]) -> List[float]:
    raw = _env(key, "")
    if not raw:
        return list(default)
    return [float(x.strip()) for x in raw.split(",")]


def _env_list_int(key: str, default: List[int]) -> List[int]:
    raw = _env(key, "")
    if not raw:
        return list(default)
    return [int(x.strip()) for x in raw.split(",")]


# ---------------------------------------------------------------------------
# Dataclass config – every field is populated from env
# ---------------------------------------------------------------------------
@dataclass
class Config:
    # -- Mode --
    MODE: str = ""  # DRY_RUN | LIVE
    DRY_RUN_FILL_MODE: str = "none"  # none | probabilistic | instant
    DRY_RUN_SELFTEST: bool = False   # Force-fill next N orders to prove pipeline
    DRY_RUN_SELFTEST_N: int = 10     # Number of orders to force-fill in selftest

    # -- Polymarket credentials --
    POLYMARKET_API_KEY: str = ""
    POLYMARKET_API_SECRET: str = ""
    POLYMARKET_API_PASSPHRASE: str = ""
    POLYMARKET_PRIVATE_KEY: str = ""  # Wallet private key for signing
    POLYMARKET_FUNDER: str = ""  # Funder/proxy wallet address (optional)
    WALLET_ADDRESS: str = ""  # Optional: your wallet address

    # -- CLOB endpoint --
    CLOB_API_URL: str = "https://clob.polymarket.com"
    GAMMA_API_URL: str = "https://gamma-api.polymarket.com"

    # -- Universe --
    MAX_ACTIVE_SLUGS: int = 60
    ASSETS: List[str] = field(default_factory=lambda: ["BTC", "ETH", "SOL", "XRP"])

    # -- Loop timing --
    TARGET_LOOP_MS: int = 500

    # -- Order management --
    MAX_OPEN_ORDERS_PER_MARKET: int = 2
    ORDER_TTL_MS: int = 2500
    MAX_ORDER_OPS_PER_LOOP: int = 20

    # -- Position limits --
    MAX_POSITION_USD_PER_OUTCOME: float = 150.0
    MAX_TOTAL_EXPOSURE_USD: float = 1500.0
    MIN_CASH_USD: float = 50.0

    # -- Spread / gating --
    SPREAD_PCTL_MIN: float = 0.75
    SPREAD_MAX_SANE: float = 0.05  # 5 cents
    BIN_RET30_THRESHOLD: float = 0.0

    # -- Take-profit / stop-loss (in cents i.e. 0.06 = 6¢) --
    TP_CENTS_MIN: float = 0.06
    TP_CENTS_TARGET: float = 0.10
    TP_CENTS_MAX: float = 0.16
    SL_CENTS: float = 0.05

    # -- Sell sizing --
    SELL_FRAC_MED: float = 0.036
    SELL_FRAC_MAX: float = 0.15
    SELL_MIN_SHARES: float = 5.0  # Minimum sell clip to avoid dust

    # -- Buy sizing --
    CLIP_UNIT_USD: float = 1.10
    CLIP_LADDER_MULTS: List[int] = field(default_factory=lambda: [1, 3, 8, 10, 12])

    # -- Order precision (Polymarket rules) --
    PRICE_MIN: float = 0.01
    PRICE_MAX: float = 0.99
    MIN_ORDER_SHARES: float = 1.0  # minimum shares per order

    # -- Aggression --
    CROSS_PROB_1C: float = 0.40  # probability of crossing at 1¢ spread

    # -- Cadence (15-min) --
    BUY_HEAVY_SEC: int = 60  # first N seconds: heavy buy
    BUY_MED_SEC: int = 120  # 60-120s: medium buy
    SELL_START_SEC: int = 300  # minute 5
    SELL_END_SEC: int = 360  # minute 6
    SELL_BURST2_START: int = 600  # minute 10
    SELL_BURST2_END: int = 660
    SELL_BURST3_START: int = 780  # minute 13
    SELL_BURST3_END: int = 840

    # -- Universe refresh --
    UNIVERSE_REFRESH_SEC: int = 120

    # -- State persistence --
    STATE_DB_PATH: str = "/home/ubuntu/github/logs/poly_bot/state.db"
    STATE_FLUSH_SEC: int = 10

    # -- Per-token cooldown (after a fill, wait before placing another order) --
    PER_TOKEN_COOLDOWN_SEC: float = 3.0  # seconds after fill before new order
    MIN_CANCEL_REPLACE_INTERVAL_SEC: float = 0.5  # min time between cancel/replace ops
    MAX_CANCEL_REPLACE_PER_SEC: int = 4  # global cap on cancel/replace ops per second
    MIN_PRICE_CHANGE_FOR_REPLACE: float = 0.01  # 1¢ minimum price change to justify replace

    # -- Risk cooldown --
    ERROR_COOLDOWN_BASE_SEC: float = 2.0
    ERROR_COOLDOWN_MAX_SEC: float = 60.0

    # -- Retry --
    RETRY_MAX: int = 3
    RETRY_BACKOFF_BASE: float = 0.5

    # -- Logging --
    LOG_FILE: str = "/home/ubuntu/github/logs/poly_bot/bot_log.jsonl"
    LOG_ROLLUP_SEC: int = 60

    # -- Secrets to redact --
    _SECRETS: List[str] = field(
        default_factory=lambda: [
            "POLYMARKET_API_KEY",
            "POLYMARKET_API_SECRET",
            "POLYMARKET_API_PASSPHRASE",
            "POLYMARKET_PRIVATE_KEY",
        ],
        repr=False,
    )

    def redacted_dict(self) -> dict:
        """Return config as dict with secrets redacted."""
        d = {}
        for f in fields(self):
            if f.name.startswith("_"):
                continue
            val = getattr(self, f.name)
            if f.name in self._SECRETS and val:
                d[f.name] = val[:4] + "****"
            else:
                d[f.name] = val
        return d


def load_config() -> Config:
    """Load config from env vars, validate, return frozen Config."""
    cfg = Config(
        MODE=_env("MODE", "DRY_RUN").upper(),
        DRY_RUN_FILL_MODE=_env("DRY_RUN_FILL_MODE", "none").lower(),
        DRY_RUN_SELFTEST=_env_bool("DRY_RUN_SELFTEST", False),
        DRY_RUN_SELFTEST_N=_env_int("DRY_RUN_SELFTEST_N", 10),
        POLYMARKET_API_KEY=_env("POLYMARKET_API_KEY", ""),
        POLYMARKET_API_SECRET=_env("POLYMARKET_API_SECRET", ""),
        POLYMARKET_API_PASSPHRASE=_env("POLYMARKET_API_PASSPHRASE", ""),
        POLYMARKET_PRIVATE_KEY=_env("POLYMARKET_PRIVATE_KEY", ""),
        POLYMARKET_FUNDER=_env("POLYMARKET_FUNDER", ""),
        WALLET_ADDRESS=_env("WALLET_ADDRESS", ""),
        CLOB_API_URL=_env("CLOB_API_URL", "https://clob.polymarket.com"),
        GAMMA_API_URL=_env("GAMMA_API_URL", "https://gamma-api.polymarket.com"),
        MAX_ACTIVE_SLUGS=_env_int("MAX_ACTIVE_SLUGS", 60),
        ASSETS=_env("ASSETS", "BTC,ETH,SOL,XRP").split(","),
        TARGET_LOOP_MS=_env_int("TARGET_LOOP_MS", 500),
        MAX_OPEN_ORDERS_PER_MARKET=_env_int("MAX_OPEN_ORDERS_PER_MARKET", 2),
        ORDER_TTL_MS=_env_int("ORDER_TTL_MS", 2500),
        MAX_ORDER_OPS_PER_LOOP=_env_int("MAX_ORDER_OPS_PER_LOOP", 20),
        MAX_POSITION_USD_PER_OUTCOME=_env_float("MAX_POSITION_USD_PER_OUTCOME", 150.0),
        MAX_TOTAL_EXPOSURE_USD=_env_float("MAX_TOTAL_EXPOSURE_USD", 1500.0),
        MIN_CASH_USD=_env_float("MIN_CASH_USD", 50.0),
        SPREAD_PCTL_MIN=_env_float("SPREAD_PCTL_MIN", 0.75),
        SPREAD_MAX_SANE=_env_float("SPREAD_MAX_SANE", 0.05),
        BIN_RET30_THRESHOLD=_env_float("BIN_RET30_THRESHOLD", 0.0),
        TP_CENTS_MIN=_env_float("TP_CENTS_MIN", 0.06),
        TP_CENTS_TARGET=_env_float("TP_CENTS_TARGET", 0.10),
        TP_CENTS_MAX=_env_float("TP_CENTS_MAX", 0.16),
        SL_CENTS=_env_float("SL_CENTS", 0.05),
        SELL_FRAC_MED=_env_float("SELL_FRAC_MED", 0.036),
        SELL_FRAC_MAX=_env_float("SELL_FRAC_MAX", 0.15),
        SELL_MIN_SHARES=_env_float("SELL_MIN_SHARES", 5.0),
        CLIP_UNIT_USD=_env_float("CLIP_UNIT_USD", 1.10),
        CLIP_LADDER_MULTS=_env_list_int("CLIP_LADDER_MULTS", [1, 3, 8, 10, 12]),
        CROSS_PROB_1C=_env_float("CROSS_PROB_1C", 0.40),
        BUY_HEAVY_SEC=_env_int("BUY_HEAVY_SEC", 60),
        BUY_MED_SEC=_env_int("BUY_MED_SEC", 120),
        SELL_START_SEC=_env_int("SELL_START_SEC", 300),
        SELL_END_SEC=_env_int("SELL_END_SEC", 360),
        SELL_BURST2_START=_env_int("SELL_BURST2_START", 600),
        SELL_BURST2_END=_env_int("SELL_BURST2_END", 660),
        SELL_BURST3_START=_env_int("SELL_BURST3_START", 780),
        SELL_BURST3_END=_env_int("SELL_BURST3_END", 840),
        UNIVERSE_REFRESH_SEC=_env_int("UNIVERSE_REFRESH_SEC", 120),
        STATE_DB_PATH=_env("STATE_DB_PATH", "/home/ubuntu/github/logs/poly_bot/state.db"),
        STATE_FLUSH_SEC=_env_int("STATE_FLUSH_SEC", 10),
        PER_TOKEN_COOLDOWN_SEC=_env_float("PER_TOKEN_COOLDOWN_SEC", 3.0),
        MIN_CANCEL_REPLACE_INTERVAL_SEC=_env_float("MIN_CANCEL_REPLACE_INTERVAL_SEC", 0.5),
        MAX_CANCEL_REPLACE_PER_SEC=_env_int("MAX_CANCEL_REPLACE_PER_SEC", 4),
        MIN_PRICE_CHANGE_FOR_REPLACE=_env_float("MIN_PRICE_CHANGE_FOR_REPLACE", 0.01),
        ERROR_COOLDOWN_BASE_SEC=_env_float("ERROR_COOLDOWN_BASE_SEC", 2.0),
        ERROR_COOLDOWN_MAX_SEC=_env_float("ERROR_COOLDOWN_MAX_SEC", 60.0),
        RETRY_MAX=_env_int("RETRY_MAX", 3),
        RETRY_BACKOFF_BASE=_env_float("RETRY_BACKOFF_BASE", 0.5),
        LOG_FILE=_env("LOG_FILE", "/home/ubuntu/github/logs/poly_bot/bot_log.jsonl"),
        LOG_ROLLUP_SEC=_env_int("LOG_ROLLUP_SEC", 60),
    )
    _validate(cfg)
    # Ensure log/state directories exist
    for path in (cfg.LOG_FILE, cfg.STATE_DB_PATH):
        if path:
            os.makedirs(os.path.dirname(path), exist_ok=True)
    return cfg


def _validate(cfg: Config) -> None:
    """Validate config; abort on fatal errors."""
    errors: List[str] = []

    if cfg.MODE not in ("DRY_RUN", "LIVE"):
        errors.append(f"MODE must be DRY_RUN or LIVE, got '{cfg.MODE}'")
    if cfg.DRY_RUN_FILL_MODE not in ("none", "probabilistic", "instant"):
        errors.append(f"DRY_RUN_FILL_MODE must be none|probabilistic|instant, got '{cfg.DRY_RUN_FILL_MODE}'")

    if cfg.MODE == "LIVE":
        if not cfg.POLYMARKET_API_KEY:
            errors.append("LIVE mode requires POLYMARKET_API_KEY")
        if not cfg.POLYMARKET_API_SECRET:
            errors.append("LIVE mode requires POLYMARKET_API_SECRET")
        if not cfg.POLYMARKET_PRIVATE_KEY:
            errors.append("LIVE mode requires POLYMARKET_PRIVATE_KEY")

    if cfg.MAX_TOTAL_EXPOSURE_USD <= 0:
        errors.append("MAX_TOTAL_EXPOSURE_USD must be > 0")
    if cfg.MAX_POSITION_USD_PER_OUTCOME <= 0:
        errors.append("MAX_POSITION_USD_PER_OUTCOME must be > 0")
    if cfg.TARGET_LOOP_MS < 100:
        errors.append("TARGET_LOOP_MS must be >= 100")
    if not (0.0 <= cfg.SPREAD_PCTL_MIN <= 1.0):
        errors.append("SPREAD_PCTL_MIN must be in [0, 1]")
    if cfg.TP_CENTS_MIN > cfg.TP_CENTS_TARGET:
        errors.append("TP_CENTS_MIN must be <= TP_CENTS_TARGET")
    if cfg.TP_CENTS_TARGET > cfg.TP_CENTS_MAX:
        errors.append("TP_CENTS_TARGET must be <= TP_CENTS_MAX")

    if errors:
        for e in errors:
            print(f"[CONFIG_ERROR] {e}", file=sys.stderr)
        sys.exit(1)
