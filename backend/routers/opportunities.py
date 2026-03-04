"""Opportunities API router — browse and query arb opportunities."""
from fastapi import APIRouter, Query
from sqlalchemy import select

from backend.database import AsyncSessionLocal
from backend.models.opportunity import ArbitrageOpportunity, OpportunityLog
from backend.models.matched_pair import MatchedPair
from backend.models.market import Market

router = APIRouter(prefix="/api/opportunities", tags=["opportunities"])


@router.get("")
async def list_opportunities(
    active_only: bool = True,
    min_profit: float = 0.0,
    sort_by: str = Query("composite_score", pattern="^(composite_score|net_profit_pct|annualized_return)$"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List arb opportunities with market titles, sorted by chosen metric."""
    KalshiMarket = Market.__table__.alias("km")
    PolyMarket = Market.__table__.alias("pm")

    async with AsyncSessionLocal() as session:
        stmt = (
            select(
                ArbitrageOpportunity,
                KalshiMarket.c.title.label("kalshi_title"),
                PolyMarket.c.title.label("poly_title"),
            )
            .join(MatchedPair, ArbitrageOpportunity.matched_pair_id == MatchedPair.id)
            .join(KalshiMarket, MatchedPair.kalshi_market_id == KalshiMarket.c.id)
            .join(PolyMarket, MatchedPair.poly_market_id == PolyMarket.c.id)
        )

        if active_only:
            stmt = stmt.where(ArbitrageOpportunity.is_active == True)
        if min_profit > 0:
            stmt = stmt.where(ArbitrageOpportunity.net_profit_pct >= min_profit)

        sort_col = getattr(ArbitrageOpportunity, sort_by)
        stmt = stmt.order_by(sort_col.desc()).offset(offset).limit(limit)

        rows = (await session.execute(stmt)).all()

        return [
            {
                "id": o.id,
                "matched_pair_id": o.matched_pair_id,
                "kalshi_title": kalshi_title or "",
                "poly_title": poly_title or "",
                "direction": o.direction,
                "kalshi_price": o.kalshi_price,
                "poly_price": o.poly_price,
                "raw_spread": o.raw_spread,
                "net_profit_pct": round(o.net_profit_pct, 3),
                "annualized_return": round(o.annualized_return, 1),
                "liquidity_score": round(o.liquidity_score, 3),
                "match_confidence": round(o.match_confidence, 3),
                "composite_score": round(o.composite_score, 4),
                "is_active": o.is_active,
                "detected_at": o.detected_at.isoformat() if o.detected_at else None,
                "expired_at": o.expired_at.isoformat() if o.expired_at else None,
            }
            for o, kalshi_title, poly_title in rows
        ]


@router.get("/{opportunity_id}")
async def get_opportunity(opportunity_id: int):
    """Get a single opportunity with full details including market titles."""
    async with AsyncSessionLocal() as session:
        opp = (
            await session.execute(
                select(ArbitrageOpportunity).where(ArbitrageOpportunity.id == opportunity_id)
            )
        ).scalar_one_or_none()

        if not opp:
            return {"error": "Not found"}, 404

        # Fetch matched pair and market titles
        pair = (
            await session.execute(
                select(MatchedPair).where(MatchedPair.id == opp.matched_pair_id)
            )
        ).scalar_one_or_none()

        kalshi_title = poly_title = ""
        if pair:
            km = (await session.execute(
                select(Market).where(Market.id == pair.kalshi_market_id)
            )).scalar_one_or_none()
            pm = (await session.execute(
                select(Market).where(Market.id == pair.poly_market_id)
            )).scalar_one_or_none()
            kalshi_title = km.title if km else ""
            poly_title = pm.title if pm else ""

        return {
            "id": opp.id,
            "direction": opp.direction,
            "kalshi_title": kalshi_title,
            "poly_title": poly_title,
            "kalshi_price": opp.kalshi_price,
            "poly_price": opp.poly_price,
            "raw_spread": opp.raw_spread,
            "net_profit_pct": round(opp.net_profit_pct, 3),
            "annualized_return": round(opp.annualized_return, 1),
            "liquidity_score": round(opp.liquidity_score, 3),
            "match_confidence": round(opp.match_confidence, 3),
            "composite_score": round(opp.composite_score, 4),
            "is_active": opp.is_active,
            "detected_at": opp.detected_at.isoformat() if opp.detected_at else None,
        }


@router.get("/history/log")
async def opportunity_history(
    limit: int = Query(100, ge=1, le=500),
):
    """Historical log of opportunity events."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(OpportunityLog)
            .order_by(OpportunityLog.recorded_at.desc())
            .limit(limit)
        )
        logs = result.scalars().all()

        return [
            {
                "id": l.id,
                "opportunity_id": l.opportunity_id,
                "event_type": l.event_type,
                "net_profit_pct": round(l.net_profit_pct, 3),
                "recorded_at": l.recorded_at.isoformat() if l.recorded_at else None,
            }
            for l in logs
        ]
