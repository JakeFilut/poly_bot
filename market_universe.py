"""
market_universe.py – Discover and maintain the active set of Polymarket slugs.

STRICT filtering:
  - Only BTC/ETH/SOL/XRP "Up or Down" 60-minute (hourly) markets
  - Slug must start with exact prefix (e.g. "bitcoin-up-or-down-")
  - Market must be in the CURRENT active hour (start <= now < end)
  - Exactly 1 market per symbol at a time
  - No 15-minute, no future, no expired, no resolved markets

Provides both sync (legacy refresh) and async_refresh for background use.
"""
from __future__ import annotations

import asyncio
import json
import re
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

    def earliest_end_utc(self) -> datetime | None:
        """Return the soonest end_date_utc across active pairs, or None."""
        ends = [p.end_date_utc for p in self.pairs.values() if p.end_date_utc]
        return min(ends) if ends else None

    def needs_rollover(self, now_utc: datetime | None = None) -> bool:
        """True when active markets have expired (now >= earliest_end - 1s).

        This allows the main loop to force an immediate refresh at the hour
        boundary instead of waiting for the periodic UNIVERSE_REFRESH_SEC timer.
        """
        if not self.pairs:
            return False
        if now_utc is None:
            now_utc = datetime.now(timezone.utc)
        earliest = self.earliest_end_utc()
        if earliest is None:
            return False
        return now_utc >= earliest - timedelta(seconds=1)

    def refresh(self) -> int:
        """Refresh universe from Gamma API.  Returns count of active pairs.

        Pipeline (deterministic per-asset search):
          1. For each slug prefix, query Gamma /markets with time-window
             narrowing (end_date_min=now, end_date_max=now+65m) + tag_id=21.
             Falls back to broad tag_id=21 pagination if time-window yields 0.
          2. Client-side filter by slug prefix.
          3. Strict filter each market (duration, time window, active, etc.)
          4. Per-symbol dedup: keep only the one current-hour market per asset
          5. Log endpoint, params, and first 50 slugs per asset for verification
        """
        now_utc = datetime.now(timezone.utc)

        debug = getattr(self.cfg, "UNIVERSE_DEBUG", False)

        # ── Per-asset deterministic fetch ────────────────────────────
        # Instead of a single bulk fetch that may miss low-volume hourly
        # markets, query once per slug prefix so we are guaranteed to see
        # our target markets regardless of their volume ranking.
        raw_markets: List[dict] = []
        per_asset_raw: Dict[str, List[dict]] = {}

        for prefix, asset_name in _SLUG_PREFIX_TO_ASSET.items():
            if asset_name not in self.cfg.ASSETS:
                continue
            try:
                batch, diag = self.api.search_markets_by_slug_prefix(
                    prefix, now_utc=now_utc,
                )
            except Exception as e:
                self.log.api_error(
                    fn="universe_refresh_search",
                    slug_prefix=prefix, asset=asset_name, error=str(e),
                )
                batch = []
                diag = {"error": str(e)}

            per_asset_raw[asset_name] = batch
            raw_markets.extend(batch)

            # Always log discovery diagnostics for every asset search
            self.log.info(
                "universe_debug_fetch",
                asset=asset_name,
                slug_prefix=prefix,
                strategy=diag.get("strategy", "?"),
                endpoint=diag.get("endpoint", "?"),
                params=diag.get("params", {}),
                now_utc=now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                pages_fetched=diag.get("pages_fetched", 0),
                total_fetched=diag.get("total_fetched", 0),
                prefix_matched=diag.get("prefix_matched", 0),
                slugs_first_50=diag.get("all_slugs", [])[:50],
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
        # If multiple qualify, WARNING + keep ending-soonest only.
        best_per_asset: Dict[str, TokenPair] = {}
        # Track ALL candidates per asset to detect duplicates
        all_per_asset: Dict[str, List[TokenPair]] = {}

        for pair in candidates:
            asset = pair.asset
            if asset not in all_per_asset:
                all_per_asset[asset] = []
            all_per_asset[asset].append(pair)

        for asset, asset_pairs in all_per_asset.items():
            if len(asset_pairs) > 1:
                # GUARDRAIL: multiple markets for same symbol
                # Sort by end_date ascending → pick ending-soonest
                asset_pairs.sort(
                    key=lambda p: p.end_date_utc or datetime.max.replace(tzinfo=timezone.utc),
                )
                kept = asset_pairs[0]
                rejected_slugs = [p.slug for p in asset_pairs[1:]]
                self.log.warn(
                    f"GUARDRAIL: {asset} has {len(asset_pairs)} qualifying markets, "
                    f"keeping ending-soonest only",
                    asset=asset,
                    kept_slug=kept.slug,
                    kept_end=kept.end_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ") if kept.end_date_utc else "?",
                    rejected_slugs=rejected_slugs,
                )
                best_per_asset[asset] = kept
            else:
                best_per_asset[asset] = asset_pairs[0]

        # ── Phase 3: hard guardrail — max 4 active markets ─────────
        if len(best_per_asset) > 4:
            all_slugs = [p.slug for p in best_per_asset.values()]
            self.log.error(
                f"GUARDRAIL: {len(best_per_asset)} active markets (max 4). "
                f"Refusing to trade. Slugs: {all_slugs}",
                active_count=len(best_per_asset),
                slugs=all_slugs,
            )
            # Return empty universe — no trading this cycle
            self.pairs = {}
            self.token_lookup = {}
            self._last_refresh = time.time()
            self._last_removed_token_ids = []
            return 0

        # ── Phase 4: build final universe ────────────────────────────
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

        # ── Rollover detection: emit clear log when slugs change ─────
        if removed or added:
            self.log.info(
                "UNIVERSE_ROLLOVER",
                now_utc=now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                from_slugs=sorted(removed),
                to_slugs=sorted(added),
                removed_count=len(removed),
                added_count=len(added),
                removed_token_ids=[t[:16] + "..." for t in removed_token_ids],
            )

        self.pairs = new_pairs
        self.token_lookup = new_lookup
        self._last_refresh = time.time()

        # ── FINAL TRADABLE UNIVERSE — single JSON line per refresh ──
        final_universe = []
        for pair in self.pairs.values():
            start_iso = pair.start_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ") if pair.start_date_utc else None
            end_iso = pair.end_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ") if pair.end_date_utc else None
            final_universe.append({
                "asset": pair.asset,
                "slug": pair.slug,
                "start_utc": start_iso,
                "end_utc": end_iso,
                "duration_min": pair.duration_minutes,
            })
        # Sort for deterministic output
        final_universe.sort(key=lambda x: x["asset"])

        self.log.info(
            "TRADABLE_UNIVERSE",
            now_utc=now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            count=len(self.pairs),
            markets=final_universe,
        )

        # ── Additional logging ───────────────────────────────────────
        self.log.info(
            "universe_refreshed",
            raw_count=len(raw_markets),
            per_asset_raw_counts={a: len(b) for a, b in per_asset_raw.items()},
            candidates_passed=len(candidates),
            total_rejected=len(rejections),
            active=len(self.pairs),
            added=len(added),
            removed=len(removed),
        )

        if debug:
            # Print every active market explicitly
            for asset in ("BTC", "ETH", "SOL", "XRP"):
                pair = best_per_asset.get(asset)
                if pair:
                    self.log.info(
                        "universe_active_market",
                        asset=asset,
                        slug=pair.slug,
                        start_utc=pair.start_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ") if pair.start_date_utc else "?",
                        end_utc=pair.end_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ") if pair.end_date_utc else "?",
                        duration_min=pair.duration_minutes,
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

        # Default for hourly markets: start = end - 60 minutes.
        # This is the correct interpretation for Polymarket hourly up/down
        # markets whose endDate marks the hour boundary.
        start_date_utc = end_date_utc - timedelta(minutes=self.REQUIRED_DURATION_MINUTES)
        duration_minutes = self.REQUIRED_DURATION_MINUTES

        # Try game_start_time / startDate for a more precise window, but
        # ONLY adopt it if the resulting duration is sane (55-65 min).
        # Gamma's startDate is often the *event series creation date*
        # (e.g. March 1 for a series that runs through March 3), which
        # produces wildly wrong durations like 2936m.  Ignore those.
        game_start_raw = m.get("game_start_time") or ""
        start_date_raw = m.get("startDate") or ""
        _start_source = "default(end-60m)"

        for candidate_raw, label in [
            (game_start_raw, "game_start_time"),
            (start_date_raw, "startDate"),
        ]:
            if not candidate_raw:
                continue
            parsed_start = _parse_iso_utc(candidate_raw)
            if parsed_start is None:
                continue  # skip unparseable, don't reject the market
            candidate_dur = round(
                (end_date_utc - parsed_start).total_seconds() / 60
            )
            if 55 <= candidate_dur <= 65:
                start_date_utc = parsed_start
                duration_minutes = candidate_dur
                _start_source = label
                break  # use first sane source

        # Debug log: raw Gamma time fields for this slug
        self.log.info(
            "universe_gate5_debug",
            slug=slug,
            endDate_raw=end_date_raw[:40],
            game_start_time_raw=game_start_raw[:40] if game_start_raw else "",
            startDate_raw=start_date_raw[:40] if start_date_raw else "",
            computed_start=start_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            computed_end=end_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            duration_min=duration_minutes,
            start_source=_start_source,
        )

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

    # ==================================================================
    # Async refresh — uses async API methods, same filter logic
    # ==================================================================
    async def async_refresh(self) -> int:
        """Async version of refresh() using the shared httpx client.

        Fetches markets concurrently per-asset, then applies the same
        strict filter + dedup pipeline as the sync version.
        """
        now_utc = datetime.now(timezone.utc)
        debug = getattr(self.cfg, "UNIVERSE_DEBUG", False)

        # Concurrent per-asset fetch
        async def _fetch_asset(prefix: str, asset_name: str):
            try:
                batch, diag = await self.api.async_search_markets_by_slug_prefix(
                    prefix, now_utc=now_utc,
                )
            except Exception as e:
                self.log.api_error(
                    fn="async_universe_refresh_search",
                    slug_prefix=prefix, asset=asset_name, error=str(e),
                )
                batch = []
                diag = {"error": str(e)}
            self.log.info(
                "universe_debug_fetch",
                asset=asset_name,
                slug_prefix=prefix,
                strategy=diag.get("strategy", "?"),
                endpoint=diag.get("endpoint", "?"),
                params=diag.get("params", {}),
                now_utc=now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                pages_fetched=diag.get("pages_fetched", 0),
                total_fetched=diag.get("total_fetched", 0),
                prefix_matched=diag.get("prefix_matched", 0),
                slugs_first_50=diag.get("all_slugs", [])[:50],
            )
            return asset_name, batch

        tasks = []
        for prefix, asset_name in _SLUG_PREFIX_TO_ASSET.items():
            if asset_name not in self.cfg.ASSETS:
                continue
            tasks.append(_fetch_asset(prefix, asset_name))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        raw_markets: List[dict] = []
        per_asset_raw: Dict[str, List[dict]] = {}
        for r in results:
            if isinstance(r, Exception):
                continue
            asset_name, batch = r
            per_asset_raw[asset_name] = batch
            raw_markets.extend(batch)

        # Phase 1: strict filter (reuse sync method)
        candidates: List[TokenPair] = []
        rejections: List[dict] = []
        for m in raw_markets:
            slug = m.get("slug", "") or m.get("question_id", "") or ""
            question = m.get("question", "") or m.get("title", "") or ""
            result = self._strict_filter(m, slug, question, now_utc)
            if result["pass"]:
                candidates.append(result["pair"])
            else:
                rejections.append({"slug": slug[:80], "reason": result["reason"]})

        if debug and rejections:
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

        # Phase 2: per-symbol dedup (same logic as sync)
        best_per_asset: Dict[str, TokenPair] = {}
        all_per_asset: Dict[str, List[TokenPair]] = {}
        for pair in candidates:
            all_per_asset.setdefault(pair.asset, []).append(pair)

        for asset, asset_pairs in all_per_asset.items():
            if len(asset_pairs) > 1:
                asset_pairs.sort(
                    key=lambda p: p.end_date_utc or datetime.max.replace(tzinfo=timezone.utc),
                )
                kept = asset_pairs[0]
                self.log.warn(
                    f"GUARDRAIL: {asset} has {len(asset_pairs)} qualifying markets, "
                    f"keeping ending-soonest only",
                    asset=asset, kept_slug=kept.slug,
                    kept_end=kept.end_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ") if kept.end_date_utc else "?",
                    rejected_slugs=[p.slug for p in asset_pairs[1:]],
                )
                best_per_asset[asset] = kept
            else:
                best_per_asset[asset] = asset_pairs[0]

        # Phase 3: hard guardrail
        if len(best_per_asset) > 4:
            self.log.error(
                f"GUARDRAIL: {len(best_per_asset)} active markets (max 4). Refusing to trade.",
                active_count=len(best_per_asset),
            )
            self.pairs = {}
            self.token_lookup = {}
            self._last_refresh = time.time()
            self._last_removed_token_ids = []
            return 0

        # Phase 4: build final universe
        new_pairs: Dict[str, TokenPair] = {}
        new_lookup: Dict[str, Tuple[str, str]] = {}
        for pair in best_per_asset.values():
            new_pairs[pair.slug] = pair
            new_lookup[pair.up_token_id] = (pair.slug, "Up")
            new_lookup[pair.down_token_id] = (pair.slug, "Down")

        added = set(new_pairs) - set(self.pairs)
        removed = set(self.pairs) - set(new_pairs)

        removed_token_ids: List[str] = []
        for slug in removed:
            old_pair = self.pairs.get(slug)
            if old_pair:
                removed_token_ids.append(old_pair.up_token_id)
                removed_token_ids.append(old_pair.down_token_id)

        if removed or added:
            self.log.info(
                "UNIVERSE_ROLLOVER",
                now_utc=now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                from_slugs=sorted(removed), to_slugs=sorted(added),
                removed_count=len(removed), added_count=len(added),
                removed_token_ids=[t[:16] + "..." for t in removed_token_ids],
            )

        self.pairs = new_pairs
        self.token_lookup = new_lookup
        self._last_refresh = time.time()

        final_universe = []
        for pair in self.pairs.values():
            start_iso = pair.start_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ") if pair.start_date_utc else None
            end_iso = pair.end_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ") if pair.end_date_utc else None
            final_universe.append({
                "asset": pair.asset, "slug": pair.slug,
                "start_utc": start_iso, "end_utc": end_iso,
                "duration_min": pair.duration_minutes,
            })
        final_universe.sort(key=lambda x: x["asset"])

        self.log.info(
            "TRADABLE_UNIVERSE",
            now_utc=now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            count=len(self.pairs), markets=final_universe,
        )
        self.log.info(
            "universe_refreshed",
            raw_count=len(raw_markets),
            per_asset_raw_counts={a: len(b) for a, b in per_asset_raw.items()},
            candidates_passed=len(candidates),
            total_rejected=len(rejections),
            active=len(self.pairs),
            added=len(added), removed=len(removed),
        )

        self._last_removed_token_ids = removed_token_ids
        return len(self.pairs)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _parse_iso_utc(s: str) -> datetime | None:
    """Parse ISO 8601 datetime string to UTC datetime.

    Handles variable-length fractional seconds (e.g. .57018Z) which
    datetime.fromisoformat on Python < 3.11 rejects (only 3 or 6 digits).
    """
    if not s:
        return None
    try:
        s = s.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # Normalise fractional seconds to exactly 6 digits so fromisoformat
        # works on all Python 3.x versions.
        s = _normalise_fractional_seconds(s)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


_FRAC_RE = re.compile(r"(\d{2}:\d{2}:\d{2})\.(\d+)")


def _normalise_fractional_seconds(s: str) -> str:
    """Pad or truncate fractional seconds to exactly 6 digits."""
    m = _FRAC_RE.search(s)
    if not m:
        return s
    frac = m.group(2)
    if len(frac) == 3 or len(frac) == 6:
        return s  # already fine
    # Pad to 6 or truncate to 6
    normalised = frac[:6].ljust(6, "0")
    return s[:m.start(2)] + normalised + s[m.end(2):]


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
