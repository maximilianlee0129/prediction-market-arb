"""
Standalone re-matching script.

Clears existing matched pairs + opportunities, then re-runs Claude matching
on all markets already in the DB — no need to re-fetch from APIs.

Run: python -m backend.rematch
"""
import asyncio
from sqlalchemy import select, delete

from backend.database import init_db, AsyncSessionLocal
from backend.models.market import Market
from backend.models.matched_pair import MatchedPair
from backend.models.opportunity import ArbitrageOpportunity, OpportunityLog
from backend.matcher import run_matching
from backend.utils.logger import get_logger

logger = get_logger(__name__)


async def rematch():
    await init_db()

    async with AsyncSessionLocal() as session:
        # Count what we're about to clear
        old_pairs = (await session.execute(select(MatchedPair))).scalars().all()
        old_opps = (await session.execute(select(ArbitrageOpportunity))).scalars().all()
        print(f"Clearing {len(old_pairs)} existing matched pairs and {len(old_opps)} opportunities...")

        await session.execute(delete(OpportunityLog))
        await session.execute(delete(ArbitrageOpportunity))
        await session.execute(delete(MatchedPair))
        await session.commit()
        print("Cleared.")

        # Load all active markets from DB
        all_markets = (
            await session.execute(
                select(Market).where(Market.status.in_(["open", "active"]))
            )
        ).scalars().all()

    kalshi_markets = []
    poly_markets = []
    for m in all_markets:
        d = {
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
            "volume": m.volume,
            "volume_24h": m.volume_24h,
            "liquidity": m.liquidity,
            "close_time": m.close_time,
        }
        if m.platform == "kalshi":
            kalshi_markets.append(d)
        elif m.platform == "polymarket":
            poly_markets.append(d)

    print(f"Loaded {len(kalshi_markets)} Kalshi + {len(poly_markets)} Polymarket markets from DB")
    print("Running Claude matching...")

    matches = await run_matching(kalshi_markets, poly_markets)

    # Build DB ID lookup
    async with AsyncSessionLocal() as session:
        all_db = (await session.execute(select(Market))).scalars().all()
        id_lookup = {(m.platform, m.platform_id): m.id for m in all_db}

        saved = 0
        for match in matches:
            k_db_id = id_lookup.get(("kalshi", match.kalshi_market["platform_id"]))
            p_db_id = id_lookup.get(("polymarket", match.poly_market["platform_id"]))
            if not k_db_id or not p_db_id:
                continue

            method = "claude_api" if match.reasoning and "fuzzy" not in match.reasoning else "fuzzy"
            pair = MatchedPair(
                kalshi_market_id=k_db_id,
                poly_market_id=p_db_id,
                confidence_score=match.confidence,
                match_method=method,
                match_reasoning=match.reasoning,
            )
            session.add(pair)
            saved += 1

        await session.commit()

    print(f"\nDone. {saved} matched pairs saved.")
    print("Start the backend normally to run arb detection on the new pairs.")


if __name__ == "__main__":
    asyncio.run(rematch())
