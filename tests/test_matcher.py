"""
Tests for backend/matcher.py — fuzzy prefilter and matching pipeline.
"""
import pytest
from unittest.mock import AsyncMock, patch

from backend.matcher import (
    fuzzy_prefilter,
    claude_batch_match,
    run_matching,
    MatchCandidate,
    FUZZY_THRESHOLD,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _m(platform: str, title: str, category: str = "", platform_id: str = "") -> dict:
    """Minimal market dict for matcher tests."""
    return {
        "platform": platform,
        "platform_id": platform_id or f"{platform}-{title[:20]}",
        "title": title,
        "category": category,
        "yes_price": 0.5,
        "no_price": 0.5,
        "yes_ask": 0.5,
        "no_ask": 0.5,
        "yes_bid": 0.5,
        "no_bid": 0.5,
        "volume": 1000,
        "volume_24h": 500,
        "liquidity": 10000,
        "status": "open",
    }


# ── fuzzy_prefilter ─────────────────────────────────────────────────────────

class TestFuzzyPrefilter:

    def test_identical_titles_match(self):
        """Identical market titles get a high fuzzy score."""
        kalshi = [_m("kalshi", "Will Bitcoin exceed $100k by end of 2026?")]
        poly = [_m("polymarket", "Will Bitcoin exceed $100k by end of 2026?")]

        candidates = fuzzy_prefilter(kalshi, poly)

        assert len(candidates) == 1
        assert candidates[0].fuzzy_score == 100.0

    def test_similar_titles_match(self):
        """Similar but not identical titles still match above threshold."""
        kalshi = [_m("kalshi", "Will Bitcoin exceed $100k by end of 2026?")]
        poly = [_m("polymarket", "Bitcoin to exceed $100k by end of 2026")]

        candidates = fuzzy_prefilter(kalshi, poly)

        assert len(candidates) >= 1
        assert candidates[0].fuzzy_score >= FUZZY_THRESHOLD

    def test_unrelated_titles_no_match(self):
        """Completely different markets don't match."""
        kalshi = [_m("kalshi", "Will it rain in NYC tomorrow?")]
        poly = [_m("polymarket", "Next president of France")]

        candidates = fuzzy_prefilter(kalshi, poly)

        assert len(candidates) == 0

    def test_max_candidates_per_market(self):
        """Each Kalshi market gets at most MAX_CANDIDATES_PER_MARKET matches."""
        kalshi = [_m("kalshi", "Will event X happen?")]
        # Create many similar polymarket titles
        poly = [
            _m("polymarket", f"Will event X happen? (variant {i})", platform_id=f"poly-{i}")
            for i in range(20)
        ]

        candidates = fuzzy_prefilter(kalshi, poly)

        # All from the same kalshi market, so capped at 5
        assert len(candidates) <= 5

    def test_empty_inputs(self):
        """Empty market lists return no candidates."""
        assert fuzzy_prefilter([], []) == []
        assert fuzzy_prefilter([_m("kalshi", "test")], []) == []
        assert fuzzy_prefilter([], [_m("polymarket", "test")]) == []

    def test_category_grouping(self):
        """Markets in the same category are more likely to be matched."""
        kalshi = [_m("kalshi", "Will Tesla stock rise 10%?", category="Finance")]
        poly_same = [_m("polymarket", "Tesla stock up 10%", category="Finance", platform_id="p1")]
        poly_diff = [_m("polymarket", "Tesla stock up 10%", category="Crypto", platform_id="p2")]

        # Both should match on title, but same-category searched first
        candidates = fuzzy_prefilter(kalshi, poly_same + poly_diff)
        assert len(candidates) >= 1

    def test_sorted_by_score_descending(self):
        """Candidates come back sorted by fuzzy score, highest first."""
        kalshi = [_m("kalshi", "Will it snow in December 2026?")]
        poly = [
            _m("polymarket", "Will it snow in December 2026?", platform_id="exact"),
            _m("polymarket", "Snow in Dec 2026", platform_id="partial"),
        ]

        candidates = fuzzy_prefilter(kalshi, poly)

        if len(candidates) >= 2:
            assert candidates[0].fuzzy_score >= candidates[1].fuzzy_score


# ── claude_batch_match (with mock) ───────────────────────────────────────────

class TestClaudeBatchMatch:

    @pytest.mark.asyncio
    async def test_fallback_without_api_key(self):
        """Without an API key, falls back to fuzzy scores as confidence."""
        candidates = [
            MatchCandidate(
                kalshi_market=_m("kalshi", "Test market"),
                poly_market=_m("polymarket", "Test market"),
                fuzzy_score=80.0,
            ),
            MatchCandidate(
                kalshi_market=_m("kalshi", "Another market"),
                poly_market=_m("polymarket", "Something else"),
                fuzzy_score=40.0,  # below threshold (0.6)
            ),
        ]

        with patch("backend.matcher.settings") as mock_settings:
            mock_settings.anthropic_api_key = ""
            confirmed = await claude_batch_match(candidates)

        # Only the high-score one should pass the 0.6 threshold
        assert len(confirmed) == 1
        assert confirmed[0].confidence == 0.8  # 80/100


# ── run_matching ─────────────────────────────────────────────────────────────

class TestRunMatching:

    @pytest.mark.asyncio
    async def test_empty_markets(self):
        """No markets → no matches."""
        result = await run_matching([], [])
        assert result == []

    @pytest.mark.asyncio
    async def test_deduplication(self):
        """Each Kalshi and Polymarket market appears in at most one match."""
        kalshi = [
            _m("kalshi", "Will X happen?", platform_id="k1"),
            _m("kalshi", "Will X happen soon?", platform_id="k2"),
        ]
        poly = [_m("polymarket", "Will X happen?", platform_id="p1")]

        with patch("backend.matcher.settings") as mock_settings:
            mock_settings.anthropic_api_key = ""
            result = await run_matching(kalshi, poly)

        # p1 can only match one kalshi market
        poly_ids = [c.poly_market["platform_id"] for c in result]
        assert len(poly_ids) == len(set(poly_ids))
