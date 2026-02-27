"""
market_universe.py – Discover and maintain the active set of Polymarket slugs.

Only BTC/ETH/SOL/XRP "Up or Down" markets.
Keeps up to MAX_ACTIVE_SLUGS tracked, preferring liquid + actively traded ones.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from config import Config
from logger import Logger
from polymarket_api import PolymarketAPI

# Asset ticker → lowercase names used in questions/slugs
_ASSET_NAMES: Dict[str, List[str]] = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth"],
    "SOL": ["solana", "sol"],
    "XRP": ["xrp", "ripple"],
}

# Pre-compiled pattern: matches "up or down", "up-or-down", "up/down", "updown"
_UP_DOWN_RE = re.compile(
    r"up[\s_/-]*(?:or[\s_/-]+)?down|updown",
    re.IGNORECASE,
)


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


class MarketUniverse:
    """Discovers and ranks slugs for trading."""

    CRYPTO_TAG_ID = 21  # Polymarket tag_id for Crypto

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

        Fetches all active crypto markets once (tag_id=21), then filters
        client-side per asset for "up or down" patterns.
        If the fetch fails, the current universe is preserved.
        """
        discovered: List[TokenPair] = []

        try:
            raw_markets = self.api.get_markets(tag_id=self.CRYPTO_TAG_ID)

            # --- DEBUG: log request details ---
            self.log.info(
                "universe_debug_fetch",
                url=f"{self.cfg.GAMMA_API_URL}/markets",
                params={
                    "tag_id": self.CRYPTO_TAG_ID,
                    "closed": "false",
                    "active": "true",
                    "limit": 200,
                    "order": "volume24hr",
                    "ascending": "false",
                },
                http_status=200,
                raw_count=len(raw_markets),
            )

            # --- DEBUG: sample titles/slugs and schema ---
            if raw_markets:
                self.log.info(
                    "universe_debug_samples",
                    sample_titles=[
                        (m.get("question", "") or m.get("title", ""))[:80]
                        for m in raw_markets[:10]
                    ],
                    sample_slugs=[
                        m.get("slug", "")[:60] for m in raw_markets[:10]
                    ],
                )
                first = raw_markets[0]
                self.log.info(
                    "universe_debug_schema",
                    first_obj_keys=sorted(first.keys())[:20],
                    has_tokens="tokens" in first,
                    has_clobTokenIds="clobTokenIds" in first,
                    has_outcomes="outcomes" in first,
                    clobTokenIds_type=type(first.get("clobTokenIds")).__name__,
                    outcomes_type=type(first.get("outcomes")).__name__,
                )
            else:
                self.log.warn(
                    "universe_debug_empty_response",
                )

            # --- Filter per asset ---
            for asset in self.cfg.ASSETS:
                pairs = self._parse_markets(asset, raw_markets)
                discovered.extend(pairs)

        except Exception as e:
            self.log.api_error(fn="universe_refresh", error=str(e))
            self.log.warn("universe_refresh_failed; keeping current universe")
            self._last_refresh = time.time()
            return len(self.pairs)

        # Rank by liquidity/volume, keep top N
        discovered.sort(key=lambda p: p.volume_24h, reverse=True)
        top = discovered[: self.cfg.MAX_ACTIVE_SLUGS]

        # Build new maps
        new_pairs: Dict[str, TokenPair] = {}
        new_lookup: Dict[str, Tuple[str, str]] = {}
        for pair in top:
            new_pairs[pair.slug] = pair
            new_lookup[pair.up_token_id] = (pair.slug, "Up")
            new_lookup[pair.down_token_id] = (pair.slug, "Down")

        added = set(new_pairs) - set(self.pairs)
        removed = set(self.pairs) - set(new_pairs)

        self.pairs = new_pairs
        self.token_lookup = new_lookup
        self._last_refresh = time.time()

        self.log.info(
            "universe_refreshed",
            total_discovered=len(discovered),
            active=len(self.pairs),
            added=len(added),
            removed=len(removed),
            assets={a: sum(1 for p in self.pairs.values() if p.asset == a)
                    for a in self.cfg.ASSETS},
        )
        return len(self.pairs)

    def _parse_markets(self, asset: str, markets: list) -> List[TokenPair]:
        """Parse Gamma API response into TokenPair objects for a given asset."""
        pairs = []
        n_total = len(markets)
        n_up_down = 0
        n_accepting = 0
        n_has_tokens = 0

        for m in markets:
            slug = m.get("slug", "") or m.get("question_id", "") or ""
            condition_id = (m.get("condition_id", "") or
                           m.get("conditionId", "") or "")

            # --- Filter 1: "Up or Down" match (check question AND slug) ---
            question = m.get("question", "") or m.get("title", "") or ""
            if not self._is_up_down_market(question, slug, asset):
                continue
            n_up_down += 1

            # --- Filter 2: tradability ---
            accepting = m.get("acceptingOrders", True)
            enable_ob = m.get("enableOrderBook", True)
            if not accepting or not enable_ob:
                continue
            n_accepting += 1

            # --- Extract token IDs ---
            tokens = m.get("tokens", [])

            # Gamma API returns clobTokenIds and outcomes as stringified JSON
            if not tokens:
                clob_ids = _parse_stringified(m.get("clobTokenIds", []))
                outcomes = _parse_stringified(m.get("outcomes", []))

                if len(clob_ids) >= 2 and len(outcomes) >= 2:
                    tokens = [
                        {"token_id": clob_ids[i], "outcome": outcomes[i]}
                        for i in range(min(len(clob_ids), len(outcomes)))
                    ]

            up_id, down_id = self._extract_outcome_tokens(tokens)
            if not up_id or not down_id:
                # DEBUG: log why token extraction failed
                if n_up_down <= 3:  # only log first few to avoid spam
                    self.log.info(
                        "universe_debug_token_fail",
                        asset=asset,
                        slug=slug[:60],
                        raw_clobTokenIds_type=type(
                            m.get("clobTokenIds")).__name__,
                        raw_outcomes_type=type(m.get("outcomes")).__name__,
                        tokens_len=len(tokens),
                        sample_token=tokens[0] if tokens else None,
                    )
                continue
            n_has_tokens += 1

            volume = float(m.get("volume", 0) or m.get("volume24hr", 0) or 0)

            pairs.append(TokenPair(
                slug=slug,
                asset=asset,
                condition_id=condition_id,
                up_token_id=up_id,
                down_token_id=down_id,
                description=question,
                volume_24h=volume,
            ))

        # --- DEBUG: per-asset filter funnel ---
        self.log.info(
            "universe_debug_filter",
            asset=asset,
            total_markets=n_total,
            after_up_down_match=n_up_down,
            after_accepting_orders=n_accepting,
            after_has_token_ids=n_has_tokens,
            final_pairs=len(pairs),
            sample_slugs=[p.slug[:60] for p in pairs[:5]],
        )
        return pairs

    def _is_up_down_market(self, question: str, slug: str, asset: str) -> bool:
        """Check if market question or slug matches 'X up or down' for asset."""
        names = _ASSET_NAMES.get(asset, [asset.lower()])
        text = f"{question} {slug}".lower()

        # Must contain an up-or-down pattern
        if not _UP_DOWN_RE.search(text):
            return False

        # Must reference the specific asset
        return any(name in text for name in names)

    def _extract_outcome_tokens(self, tokens: list) -> Tuple[str, str]:
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


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
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
