"""
market_universe.py – Discover and maintain the active set of Polymarket slugs.

STRICT filtering:
  - Only BTC/ETH/SOL/XRP "Up or Down" 60-minute (hourly) markets
  - Slug must start with exact prefix (e.g. "bitcoin-up-or-down-")
  - Market must be in the CURRENT active hour (start <= now < end)
  - Exactly 1 market per symbol at a time
  - No 15-minute, no future, no expired, no resolved markets
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from config import Config
from logger import Logger
from polymarket_api import PolymarketAPI

# ──────────────────────────────────────────────────────────────────────
# Strict slug prefix → asset mapping
# Only these exact prefixes are accepted.  No regex, no partial match.
# ──────────────────────────────────────────────────────────────────────
_SLUG_PREFIX_TO_ASSET: Dict[str, str] = {
    "bitcoin-up-or-down-":  "BTC",
    "ethereum-up-or-down-": "ETH",
    "solana-up-or-down-":   "SOL",
    "xrp-up-or-down-":     "XRP",
}

# Sub-hourly slug patterns to reject explicitly (safety net)
_REJECT_SLUG_SUBSTRINGS = ("15m", "5m", "30m", "15-min", "5-min", "30-min")


@dataclass
class TokenPair:
    """Represents both outcomes (Up/Down) of a single slug."""
    slug: str
    asset: str                  # BTC, ETH, SOL, XRP
    condition_id: str
    up_token_id: str
    down_token_id: str
    description: str = ""
    last_refresh_ts: float = 0.0
    volume_24h: float = 0.0     # for ranking
    liquidity_score: float = 0.0
    end_date_utc: datetime | None = None   # parsed from Gamma endDate
    start_date_utc: datetime | None = None # computed: end - 60 min
    duration_minutes: int = 0              # computed from end - start


class MarketUniverse:
    """Discovers and ranks slugs for trading with STRICT filtering."""

    CRYPTO_TAG_ID = 21  # Polymarket tag_id for Crypto
    REQUIRED_DURATION_MINUTES = 60

    def __init__(self, cfg: Config, api: PolymarketAPI, logger: Logger):
        self.cfg = cfg
        self.api = api
        self.log = logger

        # Active slugs: slug -> TokenPair
        self.pairs: Dict[str, TokenPair] = {}

        # Token ID → (slug, outcome) reverse map for fast lookup
        self.token_lookup: Dict[str, Tuple[str, str]] = {}

        self._last_refresh = 0.0

    def needs_refresh(self) -> bool:
        return time.time() - self._last_refresh > self.cfg.UNIVERSE_REFRESH_SEC

    def refresh(self) -> int:
        """Refresh universe from Gamma API.  Returns count of active pairs.

        Pipeline:
          1. Fetch all active crypto markets (tag_id=21)
          2. Strict filter each market (slug prefix, duration, time window)
          3. Per-symbol dedup: keep only the one current-hour market per asset
          4. Log every candidate with pass/reject reason
        """
        now_utc = datetime.now(timezone.utc)

        try:
            raw_markets = self.api.get_markets(tag_id=self.CRYPTO_TAG_ID)
        except Exception as e:
            self.log.api_error(fn="universe_refresh", error=str(e))
            self.log.warn("universe_refresh_failed; keeping current universe")
            self._last_refresh = time.time()
            return len(self.pairs)

        debug = getattr(self.cfg, "UNIVERSE_DEBUG", False)

        if debug:
            self.log.info(
                "universe_debug_fetch",
                now_utc=now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                raw_count=len(raw_markets),
            )

        # ── Phase 1: strict filter every market ──────────────────────
        candidates: List[TokenPair] = []
        rejections: List[dict] = []

        for m in raw_markets:
            slug = m.get("slug", "") or m.get("question_id", "") or ""
            question = m.get("question", "") or m.get("title", "") or ""
            result = self._strict_filter(m, slug, question, now_utc)

            if result["pass"]:
                candidates.append(result["pair"])
            else:
                rejections.append({
                    "slug": slug[:80],
                    "reason": result["reason"],
                })

        # Log rejections (compact)
        if debug and rejections:
            # Group by reason
            reason_counts: Dict[str, int] = {}
            reason_samples: Dict[str, List[str]] = {}
            for r in rejections:
                reason_counts[r["reason"]] = reason_counts.get(r["reason"], 0) + 1
                if r["reason"] not in reason_samples:
                    reason_samples[r["reason"]] = []
                if len(reason_samples[r["reason"]]) < 3:
                    reason_samples[r["reason"]].append(r["slug"])
            self.log.info(
                "universe_rejections_summary",
                total_rejected=len(rejections),
                by_reason={k: {"count": v, "samples": reason_samples.get(k, [])}
                           for k, v in reason_counts.items()},
            )

        # ── Phase 2: per-symbol dedup ────────────────────────────────
        # For each asset, keep ONLY the market whose time window contains now.
        # If multiple qualify (shouldn't happen), pick the one ending soonest.
        best_per_asset: Dict[str, TokenPair] = {}
        dupes_rejected: List[dict] = []

        for pair in candidates:
            asset = pair.asset
            if asset not in best_per_asset:
                best_per_asset[asset] = pair
            else:
                existing = best_per_asset[asset]
                # Prefer the one ending soonest (tightest current window)
                if (pair.end_date_utc and existing.end_date_utc
                        and pair.end_date_utc < existing.end_date_utc):
                    dupes_rejected.append({
                        "kept": pair.slug,
                        "rejected": existing.slug,
                        "asset": asset,
                        "reason": "duplicate_asset_replaced",
                    })
                    best_per_asset[asset] = pair
                else:
                    dupes_rejected.append({
                        "kept": existing.slug,
                        "rejected": pair.slug,
                        "asset": asset,
                        "reason": "duplicate_asset_skipped",
                    })

        if dupes_rejected:
            self.log.info(
                "universe_dedup_rejected",
                count=len(dupes_rejected),
                details=dupes_rejected,
            )

        # ── Phase 3: build final universe ────────────────────────────
        new_pairs: Dict[str, TokenPair] = {}
        new_lookup: Dict[str, Tuple[str, str]] = {}
        for pair in best_per_asset.values():
            new_pairs[pair.slug] = pair
            new_lookup[pair.up_token_id] = (pair.slug, "Up")
            new_lookup[pair.down_token_id] = (pair.slug, "Down")

        added = set(new_pairs) - set(self.pairs)
        removed = set(self.pairs) - set(new_pairs)

        # Track removed token IDs for cache purging
        removed_token_ids: List[str] = []
        for slug in removed:
            old_pair = self.pairs.get(slug)
            if old_pair:
                removed_token_ids.append(old_pair.up_token_id)
                removed_token_ids.append(old_pair.down_token_id)

        self.pairs = new_pairs
        self.token_lookup = new_lookup
        self._last_refresh = time.time()

        # ── Logging ──────────────────────────────────────────────────
        active_summary = {}
        for pair in self.pairs.values():
            start_s = pair.start_date_utc.strftime("%H:%M") if pair.start_date_utc else "?"
            end_s = pair.end_date_utc.strftime("%H:%M") if pair.end_date_utc else "?"
            active_summary[pair.asset] = {
                "slug": pair.slug,
                "window": f"{start_s}-{end_s} UTC",
                "duration_min": pair.duration_minutes,
            }

        self.log.info(
            "universe_refreshed",
            now_utc=now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            raw_count=len(raw_markets),
            candidates_passed=len(candidates),
            total_rejected=len(rejections),
            active=len(self.pairs),
            added=len(added),
            removed=len(removed),
            active_markets=active_summary,
        )

        if debug:
            # Print every active market explicitly
            for asset in ("BTC", "ETH", "SOL", "XRP"):
                info = active_summary.get(asset)
                if info:
                    self.log.info(
                        "universe_active_market",
                        asset=asset,
                        slug=info["slug"],
                        window=info["window"],
                        duration_min=info["duration_min"],
                        status="TRADING",
                    )
                else:
                    self.log.info(
                        "universe_active_market",
                        asset=asset,
                        slug="NONE",
                        status="NO_QUALIFYING_MARKET",
                    )

        # Return removed token IDs for caller to purge caches
        self._last_removed_token_ids = removed_token_ids
        return len(self.pairs)

    # ------------------------------------------------------------------
    # Strict filter: one market at a time
    # ------------------------------------------------------------------
    def _strict_filter(
        self, m: dict, slug: str, question: str, now_utc: datetime,
    ) -> dict:
        """Apply ALL filter gates. Returns {"pass": bool, "reason": str, "pair": TokenPair|None}."""

        # ── Gate 1: slug prefix must match exactly one of the 4 cryptos ──
        asset = None
        for prefix, asset_name in _SLUG_PREFIX_TO_ASSET.items():
            if slug.startswith(prefix):
                asset = asset_name
                break

        if asset is None:
            return {"pass": False, "reason": "slug_prefix_mismatch", "pair": None}

        # Verify asset is in our configured list
        if asset not in self.cfg.ASSETS:
            return {"pass": False, "reason": f"asset_not_configured:{asset}", "pair": None}

        # ── Gate 2: reject sub-hourly slug patterns ──
        slug_lower = slug.lower()
        for sub in _REJECT_SLUG_SUBSTRINGS:
            if sub in slug_lower:
                return {"pass": False, "reason": f"sub_hourly_slug:{sub}", "pair": None}

        # ── Gate 3: market must not be closed/resolved ──
        closed = m.get("closed", False)
        if isinstance(closed, str):
            closed = closed.lower() in ("true", "1", "yes")
        if closed:
            return {"pass": False, "reason": "market_closed", "pair": None}

        active = m.get("active", True)
        if isinstance(active, str):
            active = active.lower() in ("true", "1", "yes")
        if not active:
            return {"pass": False, "reason": "market_not_active", "pair": None}

        # ── Gate 4: must be accepting orders with order book enabled ──
        accepting = m.get("acceptingOrders", True)
        if isinstance(accepting, str):
            accepting = accepting.lower() in ("true", "1", "yes")
        enable_ob = m.get("enableOrderBook", True)
        if isinstance(enable_ob, str):
            enable_ob = enable_ob.lower() in ("true", "1", "yes")
        if not accepting or not enable_ob:
            return {"pass": False, "reason": "not_accepting_orders", "pair": None}

        # ── Gate 5: parse endDate, compute duration & time window ──
        end_date_raw = m.get("endDate") or m.get("end_date_iso") or ""
        if not end_date_raw:
            return {"pass": False, "reason": "no_end_date", "pair": None}

        end_date_utc = _parse_iso_utc(end_date_raw)
        if end_date_utc is None:
            return {"pass": False, "reason": f"unparseable_end_date:{end_date_raw[:30]}", "pair": None}

        # For hourly markets: start = end - 60 minutes
        start_date_utc = end_date_utc - timedelta(minutes=self.REQUIRED_DURATION_MINUTES)
        duration_minutes = self.REQUIRED_DURATION_MINUTES

        # If game_start_time is available, use it for precise duration check
        game_start_raw = m.get("game_start_time") or m.get("startDate") or ""
        if game_start_raw:
            parsed_start = _parse_iso_utc(game_start_raw)
            if parsed_start:
                actual_duration = (end_date_utc - parsed_start).total_seconds() / 60
                duration_minutes = round(actual_duration)
                start_date_utc = parsed_start

        # ── Gate 6: duration must be exactly 60 minutes ──
        # Allow small tolerance (55-65 min) for API rounding
        if not (55 <= duration_minutes <= 65):
            return {"pass": False, "reason": f"wrong_duration:{duration_minutes}m", "pair": None}

        # ── Gate 7: current time must be within the market window ──
        if now_utc < start_date_utc:
            return {"pass": False, "reason": "future_market", "pair": None}
        if now_utc >= end_date_utc:
            return {"pass": False, "reason": "expired_market", "pair": None}

        # ── Gate 8: extract both Up and Down token IDs ──
        tokens = m.get("tokens", [])
        if not tokens:
            clob_ids = _parse_stringified(m.get("clobTokenIds", []))
            outcomes = _parse_stringified(m.get("outcomes", []))
            if len(clob_ids) >= 2 and len(outcomes) >= 2:
                tokens = [
                    {"token_id": clob_ids[i], "outcome": outcomes[i]}
                    for i in range(min(len(clob_ids), len(outcomes)))
                ]

        up_id, down_id = _extract_outcome_tokens(tokens)
        if not up_id or not down_id:
            return {"pass": False, "reason": "missing_token_ids", "pair": None}

        # ── All gates passed ──
        condition_id = m.get("condition_id", "") or m.get("conditionId", "") or ""
        volume = float(m.get("volume", 0) or m.get("volume24hr", 0) or 0)

        pair = TokenPair(
            slug=slug,
            asset=asset,
            condition_id=condition_id,
            up_token_id=up_id,
            down_token_id=down_id,
            description=question,
            volume_24h=volume,
            end_date_utc=end_date_utc,
            start_date_utc=start_date_utc,
            duration_minutes=duration_minutes,
        )

        return {"pass": True, "reason": "ok", "pair": pair}

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------
    def get_pair(self, slug: str) -> TokenPair | None:
        return self.pairs.get(slug)

    def lookup_token(self, token_id: str) -> Tuple[str, str] | None:
        """Returns (slug, outcome) for a token_id, or None."""
        return self.token_lookup.get(token_id)

    def all_token_ids(self) -> List[str]:
        """All token IDs we're tracking."""
        ids = []
        for pair in self.pairs.values():
            ids.append(pair.up_token_id)
            ids.append(pair.down_token_id)
        return ids

    def active_slugs(self) -> List[str]:
        return list(self.pairs.keys())

    def asset_for_slug(self, slug: str) -> str | None:
        pair = self.pairs.get(slug)
        return pair.asset if pair else None

    def get_removed_token_ids(self) -> List[str]:
        """Return token IDs removed in the last refresh (for cache purging)."""
        return getattr(self, "_last_removed_token_ids", [])


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _parse_iso_utc(s: str) -> datetime | None:
    """Parse ISO 8601 datetime string to UTC datetime."""
    if not s:
        return None
    try:
        # Handle various ISO formats
        s = s.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def _extract_outcome_tokens(tokens: list) -> Tuple[str, str]:
    """Extract (up_token_id, down_token_id) from token list."""
    up_id = ""
    down_id = ""
    for t in tokens:
        outcome = (t.get("outcome", "") or "").lower()
        token_id = (t.get("token_id", "") or t.get("tokenId", "") or "")
        if not token_id:
            continue
        if outcome in ("up", "yes"):
            up_id = token_id
        elif outcome in ("down", "no"):
            down_id = token_id
    return up_id, down_id


def _parse_stringified(value) -> list:
    """Parse a value that may be a stringified JSON list or already a list."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
    return []
