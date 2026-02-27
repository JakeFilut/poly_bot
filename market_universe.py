"""
market_universe.py – Discover and maintain the active set of Polymarket slugs.

Only BTC/ETH/SOL/XRP "Up or Down" markets.
Keeps up to MAX_ACTIVE_SLUGS tracked, preferring liquid + actively traded ones.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from config import Config
from logger import Logger
from polymarket_api import PolymarketAPI


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
        """Refresh universe from Gamma API.  Returns count of active pairs."""
        discovered: List[TokenPair] = []

        for asset in self.cfg.ASSETS:
            try:
                markets = self.api.get_markets(asset)
                pairs = self._parse_markets(asset, markets)
                discovered.extend(pairs)
            except Exception as e:
                self.log.api_error(fn="universe_refresh", asset=asset, error=str(e))

        # Rank by liquidity/volume, keep top N
        discovered.sort(key=lambda p: p.volume_24h, reverse=True)
        top = discovered[: self.cfg.MAX_ACTIVE_SLUGS]

        # Update internal maps
        new_pairs: Dict[str, TokenPair] = {}
        new_lookup: Dict[str, Tuple[str, str]] = {}
        for pair in top:
            new_pairs[pair.slug] = pair
            new_lookup[pair.up_token_id] = (pair.slug, "Up")
            new_lookup[pair.down_token_id] = (pair.slug, "Down")

        self.pairs = new_pairs
        self.token_lookup = new_lookup
        self._last_refresh = time.time()

        self.log.info(
            "universe_refreshed",
            total_discovered=len(discovered),
            active=len(self.pairs),
            assets={a: sum(1 for p in self.pairs.values() if p.asset == a)
                    for a in self.cfg.ASSETS},
        )
        return len(self.pairs)

    def _parse_markets(self, asset: str, markets: list) -> List[TokenPair]:
        """Parse Gamma API response into TokenPair objects."""
        pairs = []
        for m in markets:
            # Accept various response formats
            slug = m.get("slug", "") or m.get("question_id", "") or ""
            condition_id = m.get("condition_id", "") or m.get("conditionId", "") or ""

            # Look for "Up or Down" in title/question
            title = (m.get("question", "") or m.get("title", "")).lower()
            if not self._is_up_down_market(title, asset):
                continue

            # Extract token IDs for Up and Down outcomes
            tokens = m.get("tokens", [])
            if not tokens and m.get("clobTokenIds"):
                # Alternative format
                clob_ids = m.get("clobTokenIds", [])
                outcomes = m.get("outcomes", [])
                if len(clob_ids) >= 2 and len(outcomes) >= 2:
                    tokens = [
                        {"token_id": clob_ids[i], "outcome": outcomes[i]}
                        for i in range(len(clob_ids))
                    ]

            up_id, down_id = self._extract_outcome_tokens(tokens)
            if not up_id or not down_id:
                continue

            volume = float(m.get("volume", 0) or m.get("volume24hr", 0) or 0)

            pairs.append(TokenPair(
                slug=slug,
                asset=asset,
                condition_id=condition_id,
                up_token_id=up_id,
                down_token_id=down_id,
                description=m.get("question", ""),
                volume_24h=volume,
            ))
        return pairs

    def _is_up_down_market(self, title: str, asset: str) -> bool:
        """Check if market title matches 'X up or down' pattern."""
        asset_lower = asset.lower()
        # Match patterns like "Will Bitcoin go up or down", "BTC up or down", etc.
        patterns = [
            rf"{asset_lower}.*(?:up|down)",
            rf"(?:bitcoin|ethereum|solana|xrp|ripple).*(?:up|down)",
        ]
        asset_names = {
            "BTC": "bitcoin", "ETH": "ethereum",
            "SOL": "solana", "XRP": "xrp",
        }
        full_name = asset_names.get(asset, asset_lower)
        patterns.append(rf"{full_name}.*(?:up|down)")

        for pattern in patterns:
            if re.search(pattern, title):
                return True
        return False

    def _extract_outcome_tokens(self, tokens: list) -> Tuple[str, str]:
        """Extract (up_token_id, down_token_id) from token list."""
        up_id = ""
        down_id = ""
        for t in tokens:
            outcome = (t.get("outcome", "") or "").lower()
            token_id = t.get("token_id", "") or t.get("tokenId", "") or ""
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
