"""Markets API router — browse markets from both platforms."""
from fastapi import APIRouter, Query
from sqlalchemy import select, func

from backend.database import AsyncSessionLocal
from backend.models.market import Market

router = APIRouter(prefix="/api/markets", tags=["markets"])


@router.get("")
async def list_markets(
    platform: str = "",
    category: str = "",
    search: str = "",
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """List markets with optional filters."""
    async with AsyncSessionLocal() as session:
        stmt = select(Market).where(Market.status.in_(["open", "active"]))

        if platform:
            stmt = stmt.where(Market.platform == platform)
        if category:
            stmt = stmt.where(Market.category == category)
        if search:
            stmt = stmt.where(Market.title.ilike(f"%{search}%"))

        stmt = stmt.order_by(Market.volume_24h.desc()).offset(offset).limit(limit)
        result = await session.execute(stmt)
        markets = result.scalars().all()

        return [
            {
                "id": m.id,
                "platform": m.platform,
                "platform_id": m.platform_id,
                "title": m.title,
                "category": m.category,
                "yes_price": m.yes_price,
                "no_price": m.no_price,
                "yes_bid": m.yes_bid,
                "yes_ask": m.yes_ask,
                "no_bid": m.no_bid,
                "no_ask": m.no_ask,
                "volume_24h": m.volume_24h,
                "liquidity": m.liquidity,
                "close_time": m.close_time.isoformat() if m.close_time else None,
                "last_updated": m.last_updated.isoformat() if m.last_updated else None,
            }
            for m in markets
        ]


@router.get("/stats")
async def market_stats():
    """Aggregate market stats by platform."""
    async with AsyncSessionLocal() as session:
        rows = (
            await session.execute(
                select(
                    Market.platform,
                    func.count(Market.id).label("count"),
                )
                .where(Market.status.in_(["open", "active"]))
                .group_by(Market.platform)
            )
        ).all()

        stats = {row.platform: row.count for row in rows}
        stats["total"] = sum(stats.values())
        return stats
