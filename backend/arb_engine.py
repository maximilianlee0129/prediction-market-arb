"""
Arbitrage detection engine.

Core logic:
- Check both directions (kalshi_yes + poly_no, kalshi_no + poly_yes)
- Use ASK prices (what you actually pay to enter)
- Apply platform fees: Kalshi 2%, Polymarket 1%
- Calculate net profit, annualized return, composite score
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

# Platform fees as fractions of profit
KALSHI_FEE_RATE = 0.02
POLY_FEE_RATE = 0.01

# Minimum price to consider — filters out illiquid/missing prices
MIN_PRICE = 0.02

# Cap on annualized return to prevent blowup on near-expiry markets
MAX_ANNUALIZED_PCT = 10_000.0

# Max allowed probability divergence between platforms.
# If Kalshi says YES=0.40 and Polymarket says YES=0.85 (inferred from NO=0.15),
# that's a 0.45 divergence — almost certainly a bad match, not a real arb.
# Real arbs come from small discrepancies (~2–15%) on the same market.
MAX_PRICE_DIVERGENCE = 0.25

# Composite score weights
# Confidence is a gate (all accepted pairs already passed threshold), not a ranking signal.
# Liquidity is a display metric — don't let it distort opportunity ranking.
W_PROFIT = 0.65
W_ANNUALIZED = 0.35
W_CONFIDENCE = 0.0
W_LIQUIDITY = 0.0

# Normalization caps for composite score (values above these get score=1.0)
PROFIT_CAP = 20.0       # 20% net profit → max score
ANNUALIZED_CAP = 200.0  # 200% annualized → max score

# Liquidity normalization: log scale, this value → score=1.0
LIQUIDITY_HIGH = 100_000.0


@dataclass
class ArbResult:
    """Result of an arb calculation for one pair of markets."""
    direction: str          # "kalshi_yes_poly_no" or "kalshi_no_poly_yes"
    kalshi_price: float     # ask price used on the Kalshi side
    poly_price: float       # ask price used on the Polymarket side
    total_cost: float       # kalshi_price + poly_price
    raw_spread: float       # 1.0 - total_cost (before fees)
    fee_amount: float       # total fees deducted from spread
    net_profit_pct: float   # (raw_spread - fees) / cost × 100


def calculate_arb(kalshi: dict, poly: dict) -> Optional[ArbResult]:
    """
    Check both arb directions and return the best one, or None.

    Direction 1: Buy YES on Kalshi + Buy NO on Polymarket
    Direction 2: Buy NO on Kalshi + Buy YES on Polymarket

    Uses ASK prices (conservative — what you actually pay).
    """
    candidates: list[ArbResult] = []

    for direction, k_price, p_price in [
        ("kalshi_yes_poly_no", kalshi["yes_ask"], poly["no_ask"]),
        ("kalshi_no_poly_yes", kalshi["no_ask"], poly["yes_ask"]),
    ]:
        # Skip missing or illiquid prices — these produce nonsense arb figures
        if k_price < MIN_PRICE or p_price < MIN_PRICE:
            continue

        # Sanity check: both platforms should roughly agree on the probability.
        # For direction kalshi_yes_poly_no: we buy YES on Kalshi + NO on Poly.
        # Poly's implied YES price = 1 - p_no_ask. If |k_yes - implied_p_yes| > threshold,
        # the platforms disagree too much — this is a bad match, not a real arb.
        implied_other_yes = 1.0 - p_price  # e.g. if p_no=0.11 then implied p_yes=0.89
        if abs(k_price - implied_other_yes) > MAX_PRICE_DIVERGENCE:
            continue

        total_cost = k_price + p_price

        if total_cost <= 0:
            # Free money edge case — still valid
            raw_spread = 1.0
            fee_amount = KALSHI_FEE_RATE * raw_spread + POLY_FEE_RATE * raw_spread
            net_profit = raw_spread - fee_amount
            net_profit_pct = float("inf") if total_cost == 0 else (net_profit / total_cost) * 100
            if net_profit > 0:
                candidates.append(ArbResult(
                    direction=direction,
                    kalshi_price=k_price,
                    poly_price=p_price,
                    total_cost=total_cost,
                    raw_spread=raw_spread,
                    fee_amount=fee_amount,
                    net_profit_pct=net_profit_pct,
                ))
            continue

        if total_cost >= 1.0:
            continue

        raw_spread = 1.0 - total_cost

        # Fees are charged on profit (the spread), not on the total cost
        kalshi_fee = KALSHI_FEE_RATE * raw_spread
        poly_fee = POLY_FEE_RATE * raw_spread
        fee_amount = kalshi_fee + poly_fee

        net_profit = raw_spread - fee_amount
        if net_profit <= 0:
            continue

        net_profit_pct = (net_profit / total_cost) * 100

        candidates.append(ArbResult(
            direction=direction,
            kalshi_price=k_price,
            poly_price=p_price,
            total_cost=total_cost,
            raw_spread=raw_spread,
            fee_amount=fee_amount,
            net_profit_pct=net_profit_pct,
        ))

    if not candidates:
        return None

    # Return the most profitable direction
    return max(candidates, key=lambda c: c.net_profit_pct)


def calculate_annualized_return(net_profit_pct: float, days_to_resolution: int) -> float:
    """
    Annualize a return: (1 + r)^(365/days) - 1, expressed as a percentage.

    Returns 0.0 for zero or negative days (expired/invalid markets).
    """
    if days_to_resolution <= 0:
        return 0.0

    if net_profit_pct == 0.0:
        return 0.0

    r = net_profit_pct / 100.0
    exponent = 365.0 / days_to_resolution
    # Guard against exponent overflow before computing
    if exponent > 500:
        return MAX_ANNUALIZED_PCT
    try:
        annualized = ((1 + r) ** exponent) - 1
    except (OverflowError, ValueError, OSError):
        return MAX_ANNUALIZED_PCT
    return min(annualized * 100.0, MAX_ANNUALIZED_PCT)


def calculate_composite_score(
    net_profit_pct: float,
    annualized_return: float,
    match_confidence: float,
    liquidity_score: float,
) -> float:
    """
    Weighted composite score in [0, 1].

    Weights: 35% profit + 25% annualized + 25% confidence + 15% liquidity.
    Profit and annualized are normalized against caps before weighting.
    """
    profit_norm = min(max(net_profit_pct, 0.0) / PROFIT_CAP, 1.0)
    annual_norm = min(max(annualized_return, 0.0) / ANNUALIZED_CAP, 1.0)
    conf_norm = min(max(match_confidence, 0.0), 1.0)
    liq_norm = min(max(liquidity_score, 0.0), 1.0)

    score = (
        W_PROFIT * profit_norm
        + W_ANNUALIZED * annual_norm
        + W_CONFIDENCE * conf_norm
        + W_LIQUIDITY * liq_norm
    )
    return min(score, 1.0)


def _liquidity_score(kalshi_liquidity: float, poly_liquidity: float) -> float:
    """
    Normalize liquidity to [0, 1] using the minimum of the two platforms.

    Uses the *minimum* because the bottleneck platform limits executable size.
    """
    import math
    min_liq = min(kalshi_liquidity or 0, poly_liquidity or 0)
    if min_liq <= 0:
        return 0.0
    # Log scale: $1 → 0, $100k+ → 1.0
    return min(math.log10(min_liq + 1) / math.log10(LIQUIDITY_HIGH + 1), 1.0)


def build_opportunity(
    kalshi: dict,
    poly: dict,
    match_confidence: float,
) -> Optional[dict]:
    """
    Full pipeline: detect arb → compute annualized return → score → return dict.

    Returns None if no arb exists.
    """
    arb = calculate_arb(kalshi, poly)
    if arb is None or arb.net_profit_pct <= 0:
        return None

    # Days to resolution: use the earlier close time
    days = 0
    k_close = kalshi.get("close_time")
    p_close = poly.get("close_time")
    if k_close and p_close:
        earliest = min(k_close, p_close)
        delta = earliest - datetime.utcnow()
        days = max(int(delta.total_seconds() / 86400), 0)
    elif k_close:
        delta = k_close - datetime.utcnow()
        days = max(int(delta.total_seconds() / 86400), 0)
    elif p_close:
        delta = p_close - datetime.utcnow()
        days = max(int(delta.total_seconds() / 86400), 0)

    annualized = calculate_annualized_return(arb.net_profit_pct, days)
    liq_score = _liquidity_score(kalshi.get("liquidity", 0), poly.get("liquidity", 0))
    composite = calculate_composite_score(
        arb.net_profit_pct, annualized, match_confidence, liq_score
    )

    return {
        "direction": arb.direction,
        "kalshi_price": arb.kalshi_price,
        "poly_price": arb.poly_price,
        "total_cost": arb.total_cost,
        "raw_spread": arb.raw_spread,
        "fee_amount": arb.fee_amount,
        "net_profit_pct": arb.net_profit_pct,
        "annualized_return": annualized,
        "liquidity_score": liq_score,
        "match_confidence": match_confidence,
        "composite_score": composite,
    }
