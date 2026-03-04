"""Matched pairs API router — browse cross-platform market matches."""
from fastapi import APIRouter, Query
from sqlalchemy import select

from backend.database import AsyncSessionLocal
from backend.models.matched_pair import MatchedPair
from backend.models.market import Market

router = APIRouter(prefix="/api/matched-pairs", tags=["matched-pairs"])


@router.get("")
async def list_matched_pairs(
    active_only: bool = True,
    min_confidence: float = 0.0,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List matched market pairs with market titles."""
    async with AsyncSessionLocal() as session:
        stmt = select(MatchedPair)

        if active_only:
            stmt = stmt.where(MatchedPair.is_active == True)
        if min_confidence > 0:
            stmt = stmt.where(MatchedPair.confidence_score >= min_confidence)

        stmt = stmt.order_by(MatchedPair.confidence_score.desc()).offset(offset).limit(limit)
        result = await session.execute(stmt)
        pairs = result.scalars().all()

        # Batch-fetch market titles
        market_ids = set()
        for p in pairs:
            market_ids.add(p.kalshi_market_id)
            market_ids.add(p.poly_market_id)

        titles = {}
        if market_ids:
            markets = (
                await session.execute(
                    select(Market).where(Market.id.in_(market_ids))
                )
            ).scalars().all()
            titles = {m.id: m.title for m in markets}

        return [
            {
                "id": p.id,
                "kalshi_market_id": p.kalshi_market_id,
                "poly_market_id": p.poly_market_id,
                "kalshi_title": titles.get(p.kalshi_market_id, ""),
                "poly_title": titles.get(p.poly_market_id, ""),
                "confidence_score": round(p.confidence_score, 3),
                "match_method": p.match_method,
                "match_reasoning": p.match_reasoning,
                "is_active": p.is_active,
                "created_at": p.created_at.isoformat() if p.created_at else None,
            }
            for p in pairs
        ]
