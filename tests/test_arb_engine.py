"""
Tests for backend/arb_engine.py — written FIRST (TDD).

Covers:
- Arb detection in both directions
- No arb when prices sum >= 1 after fees
- Fee deductions (Kalshi 2%, Polymarket 1%)
- Edge cases: zero prices, prices at boundary, missing data
- Annualized return calculation
- Composite score properties
"""
import pytest
from datetime import datetime, timedelta

from backend.arb_engine import (
    calculate_arb,
    calculate_annualized_return,
    calculate_composite_score,
    build_opportunity,
    ArbResult,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _market(
    platform: str,
    yes_ask: float,
    no_ask: float,
    yes_bid: float = 0.0,
    no_bid: float = 0.0,
    liquidity: float = 10000.0,
    close_time: datetime | None = None,
    title: str = "Test Market",
    platform_id: str = "test-123",
) -> dict:
    """Build a minimal market dict matching the DB shape."""
    return {
        "platform": platform,
        "platform_id": platform_id,
        "title": title,
        "yes_price": yes_ask,  # mid price, not used for arb calc
        "no_price": no_ask,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "volume": 0,
        "volume_24h": 0,
        "liquidity": liquidity,
        "close_time": close_time,
        "status": "open",
    }


# ── calculate_arb ───────────────────────────────────────────────────────────

class TestCalculateArb:
    """Test the core arb detection function."""

    def test_clear_arb_kalshi_yes_poly_no(self):
        """Kalshi YES cheap + Poly NO cheap = guaranteed profit."""
        # Kalshi YES ask = 0.30, Poly NO ask = 0.40 → cost 0.70, payout 1.00
        # Fees: Kalshi 2% on profit, Poly 1% on profit
        kalshi = _market("kalshi", yes_ask=0.30, no_ask=0.70)
        poly = _market("polymarket", yes_ask=0.60, no_ask=0.40)

        result = calculate_arb(kalshi, poly)

        assert result is not None
        assert result.direction == "kalshi_yes_poly_no"
        assert result.raw_spread > 0
        assert result.net_profit_pct > 0
        assert result.kalshi_price == 0.30
        assert result.poly_price == 0.40

    def test_clear_arb_kalshi_no_poly_yes(self):
        """Kalshi NO cheap + Poly YES cheap = guaranteed profit."""
        # Kalshi NO ask = 0.30, Poly YES ask = 0.40 → cost 0.70
        kalshi = _market("kalshi", yes_ask=0.70, no_ask=0.30)
        poly = _market("polymarket", yes_ask=0.40, no_ask=0.60)

        result = calculate_arb(kalshi, poly)

        assert result is not None
        assert result.direction == "kalshi_no_poly_yes"
        assert result.raw_spread > 0
        assert result.net_profit_pct > 0

    def test_no_arb_prices_sum_to_one(self):
        """No arb when ask prices sum to exactly 1.0."""
        kalshi = _market("kalshi", yes_ask=0.50, no_ask=0.50)
        poly = _market("polymarket", yes_ask=0.50, no_ask=0.50)

        result = calculate_arb(kalshi, poly)
        assert result is None

    def test_no_arb_prices_sum_above_one(self):
        """No arb when best combination of asks still sums above 1."""
        kalshi = _market("kalshi", yes_ask=0.55, no_ask=0.50)
        poly = _market("polymarket", yes_ask=0.55, no_ask=0.50)

        result = calculate_arb(kalshi, poly)
        assert result is None

    def test_fees_reduce_profit(self):
        """Fees reduce net profit below the raw spread."""
        kalshi = _market("kalshi", yes_ask=0.49, no_ask=0.51)
        poly = _market("polymarket", yes_ask=0.51, no_ask=0.49)

        result = calculate_arb(kalshi, poly)
        # There IS still an arb (3% fees on spread are tiny), but net < raw
        assert result is not None
        assert result.fee_amount > 0
        assert result.net_profit_pct < (result.raw_spread / result.total_cost * 100)

    def test_returns_best_direction(self):
        """When both directions have arb, returns the more profitable one."""
        # Direction 1: kalshi_yes(0.20) + poly_no(0.20) = 0.40 → huge arb
        # Direction 2: kalshi_no(0.80) + poly_yes(0.80) = 1.60 → no arb
        kalshi = _market("kalshi", yes_ask=0.20, no_ask=0.80)
        poly = _market("polymarket", yes_ask=0.80, no_ask=0.20)

        result = calculate_arb(kalshi, poly)

        assert result is not None
        assert result.direction == "kalshi_yes_poly_no"

    def test_zero_ask_price(self):
        """Handle zero ask prices gracefully (free contract)."""
        kalshi = _market("kalshi", yes_ask=0.0, no_ask=1.0)
        poly = _market("polymarket", yes_ask=1.0, no_ask=0.0)

        result = calculate_arb(kalshi, poly)
        # 0 + 0 = 0 cost → guaranteed profit of 1.0
        assert result is not None
        assert result.net_profit_pct > 0

    def test_ask_price_at_one(self):
        """Asking price of 1.0 on both sides means no arb."""
        kalshi = _market("kalshi", yes_ask=1.0, no_ask=1.0)
        poly = _market("polymarket", yes_ask=1.0, no_ask=1.0)

        result = calculate_arb(kalshi, poly)
        assert result is None

    def test_result_fields_present(self):
        """ArbResult has all required fields populated."""
        kalshi = _market("kalshi", yes_ask=0.25, no_ask=0.75)
        poly = _market("polymarket", yes_ask=0.80, no_ask=0.25)

        result = calculate_arb(kalshi, poly)

        assert result is not None
        assert isinstance(result.direction, str)
        assert isinstance(result.kalshi_price, float)
        assert isinstance(result.poly_price, float)
        assert isinstance(result.total_cost, float)
        assert isinstance(result.raw_spread, float)
        assert isinstance(result.fee_amount, float)
        assert isinstance(result.net_profit_pct, float)

    def test_net_profit_pct_is_percentage_of_cost(self):
        """net_profit_pct = (1 - cost - fees) / cost × 100."""
        kalshi = _market("kalshi", yes_ask=0.30, no_ask=0.70)
        poly = _market("polymarket", yes_ask=0.60, no_ask=0.40)

        result = calculate_arb(kalshi, poly)

        assert result is not None
        expected_pct = (result.raw_spread - result.fee_amount) / result.total_cost * 100
        assert abs(result.net_profit_pct - expected_pct) < 0.001


# ── calculate_annualized_return ──────────────────────────────────────────────

class TestAnnualizedReturn:
    """Test annualized return calculation: (1 + r)^(365/days) - 1."""

    def test_one_year_horizon(self):
        """With exactly 365 days, annualized == raw return."""
        result = calculate_annualized_return(net_profit_pct=10.0, days_to_resolution=365)
        assert abs(result - 10.0) < 0.01

    def test_short_horizon_magnifies(self):
        """Shorter time horizons magnify the annualized return."""
        result = calculate_annualized_return(net_profit_pct=5.0, days_to_resolution=30)
        # 5% in 30 days annualizes to much more than 5%
        assert result > 50.0

    def test_long_horizon_reduces(self):
        """Longer horizons reduce the annualized return."""
        result = calculate_annualized_return(net_profit_pct=10.0, days_to_resolution=730)
        # 10% over 2 years → less than 10% annualized
        assert result < 10.0

    def test_one_day_resolution(self):
        """1-day resolution → massive annualized return."""
        result = calculate_annualized_return(net_profit_pct=1.0, days_to_resolution=1)
        assert result > 100.0

    def test_zero_days_returns_zero(self):
        """Zero days to resolution → return 0 (safety, avoid division by zero)."""
        result = calculate_annualized_return(net_profit_pct=5.0, days_to_resolution=0)
        assert result == 0.0

    def test_negative_days_returns_zero(self):
        """Negative days (expired market) → return 0."""
        result = calculate_annualized_return(net_profit_pct=5.0, days_to_resolution=-10)
        assert result == 0.0

    def test_negative_profit(self):
        """Negative profit stays negative after annualization."""
        result = calculate_annualized_return(net_profit_pct=-5.0, days_to_resolution=30)
        assert result < 0.0

    def test_zero_profit(self):
        """Zero profit → zero annualized."""
        result = calculate_annualized_return(net_profit_pct=0.0, days_to_resolution=30)
        assert result == 0.0


# ── calculate_composite_score ────────────────────────────────────────────────

class TestCompositeScore:
    """Test composite scoring: 35% profit + 25% annualized + 25% confidence + 15% liquidity."""

    def test_score_in_range(self):
        """Score is always between 0 and 1."""
        score = calculate_composite_score(
            net_profit_pct=5.0,
            annualized_return=50.0,
            match_confidence=0.9,
            liquidity_score=0.8,
        )
        assert 0.0 <= score <= 1.0

    def test_perfect_inputs_high_score(self):
        """Excellent inputs → high score."""
        score = calculate_composite_score(
            net_profit_pct=20.0,
            annualized_return=200.0,
            match_confidence=1.0,
            liquidity_score=1.0,
        )
        assert score > 0.8

    def test_zero_inputs_low_score(self):
        """All-zero inputs → score is 0."""
        score = calculate_composite_score(
            net_profit_pct=0.0,
            annualized_return=0.0,
            match_confidence=0.0,
            liquidity_score=0.0,
        )
        assert score == 0.0

    def test_monotonic_in_profit(self):
        """Higher profit → higher score, all else equal."""
        low = calculate_composite_score(2.0, 50.0, 0.8, 0.5)
        high = calculate_composite_score(10.0, 50.0, 0.8, 0.5)
        assert high > low

    def test_monotonic_in_annualized(self):
        """Higher annualized → higher score, all else equal."""
        low = calculate_composite_score(5.0, 10.0, 0.8, 0.5)
        high = calculate_composite_score(5.0, 100.0, 0.8, 0.5)
        assert high > low

    def test_monotonic_in_confidence(self):
        """Higher confidence → higher score, all else equal."""
        low = calculate_composite_score(5.0, 50.0, 0.5, 0.5)
        high = calculate_composite_score(5.0, 50.0, 0.95, 0.5)
        assert high > low

    def test_monotonic_in_liquidity(self):
        """Higher liquidity → higher score, all else equal."""
        low = calculate_composite_score(5.0, 50.0, 0.8, 0.2)
        high = calculate_composite_score(5.0, 50.0, 0.8, 0.9)
        assert high > low


# ── build_opportunity ────────────────────────────────────────────────────────

class TestBuildOpportunity:
    """Test the full opportunity builder that ties everything together."""

    def test_builds_with_close_time(self):
        """When both markets have close times, annualized return is computed."""
        close = datetime.utcnow() + timedelta(days=30)
        kalshi = _market("kalshi", yes_ask=0.25, no_ask=0.75, close_time=close)
        poly = _market("polymarket", yes_ask=0.80, no_ask=0.25, close_time=close)

        opp = build_opportunity(kalshi, poly, match_confidence=0.9)

        assert opp is not None
        assert opp["net_profit_pct"] > 0
        assert opp["annualized_return"] > 0
        assert opp["composite_score"] > 0
        assert opp["match_confidence"] == 0.9

    def test_builds_without_close_time(self):
        """When no close time, annualized return defaults to 0."""
        kalshi = _market("kalshi", yes_ask=0.25, no_ask=0.75)
        poly = _market("polymarket", yes_ask=0.80, no_ask=0.25)

        opp = build_opportunity(kalshi, poly, match_confidence=0.85)

        assert opp is not None
        assert opp["annualized_return"] == 0.0
        assert opp["net_profit_pct"] > 0

    def test_returns_none_when_no_arb(self):
        """No opportunity when there's no arb."""
        kalshi = _market("kalshi", yes_ask=0.55, no_ask=0.55)
        poly = _market("polymarket", yes_ask=0.55, no_ask=0.55)

        opp = build_opportunity(kalshi, poly, match_confidence=0.9)
        assert opp is None

    def test_liquidity_score_from_markets(self):
        """Liquidity score is derived from market liquidity values."""
        close = datetime.utcnow() + timedelta(days=60)
        kalshi = _market("kalshi", yes_ask=0.20, no_ask=0.80, liquidity=50000, close_time=close)
        poly = _market("polymarket", yes_ask=0.85, no_ask=0.20, liquidity=100000, close_time=close)

        opp = build_opportunity(kalshi, poly, match_confidence=0.9)

        assert opp is not None
        assert 0.0 <= opp["liquidity_score"] <= 1.0
