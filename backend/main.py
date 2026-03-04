"""
FastAPI application — Phase 3: Full Pipeline.

Background tasks:
- Every 5 minutes: fetch all markets from both APIs → upsert to DB → run matching
- Every 30 seconds: refresh prices → run arb detection on matched pairs → broadcast

Run: cd backend && uvicorn main:app --reload
Or standalone poller: python -m backend.main
"""
import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, update, func

from backend.config import settings
from backend.database import init_db, AsyncSessionLocal
from backend.models.market import Market, PriceSnapshot
from backend.models.matched_pair import MatchedPair
from backend.models.opportunity import ArbitrageOpportunity, OpportunityLog
from backend.collectors.kalshi import KalshiCollector
from backend.collectors.polymarket import PolymarketCollector
from backend.arb_engine import build_opportunity
from backend.matcher import run_matching
from backend.websocket_manager import manager
from backend.utils.logger import get_logger

from backend.routers import markets as markets_router
from backend.routers import opportunities as opportunities_router
from backend.routers import matched_pairs as matched_pairs_router

logger = get_logger(__name__)

kalshi = KalshiCollector()
polymarket = PolymarketCollector()


# ── Market upsert (unchanged from Phase 1) ──────────────────────────────────

async def upsert_markets(markets: list[dict]) -> tuple[int, int]:
    """Insert new markets or update existing ones. Returns (inserted, updated).

    Optimized: loads existing keys into memory first, then batches writes
    with periodic commits to avoid holding a huge transaction.
    """
    if not markets:
        return 0, 0

    import time
    t0 = time.monotonic()

    # Deduplicate: keep last occurrence of each (platform, platform_id)
    seen: dict[tuple[str, str], int] = {}
    for idx, m in enumerate(markets):
        seen[(m["platform"], m["platform_id"])] = idx
    markets = [markets[i] for i in sorted(seen.values())]

    inserted = 0
    updated = 0
    BATCH_SIZE = 500
    now = datetime.utcnow()

    async with AsyncSessionLocal() as session:
        # Load all existing (platform, platform_id) → id into memory
        result = await session.execute(
            select(Market.id, Market.platform, Market.platform_id)
        )
        existing_map = {(r.platform, r.platform_id): r.id for r in result.all()}
        logger.info(f"Loaded {len(existing_map)} existing market keys in {time.monotonic()-t0:.1f}s")

        for i in range(0, len(markets), BATCH_SIZE):
            batch = markets[i:i + BATCH_SIZE]

            for m in batch:
                key = (m["platform"], m["platform_id"])
                existing_id = existing_map.get(key)

                if existing_id:
                    await session.execute(
                        update(Market)
                        .where(Market.id == existing_id)
                        .values(
                            yes_price=m["yes_price"],
                            no_price=m["no_price"],
                            yes_bid=m["yes_bid"],
                            yes_ask=m["yes_ask"],
                            no_bid=m["no_bid"],
                            no_ask=m["no_ask"],
                            volume=m["volume"],
                            volume_24h=m["volume_24h"],
                            liquidity=m["liquidity"],
                            open_interest=m.get("open_interest", 0),
                            status=m["status"],
                            last_updated=now,
                        )
                    )
                    updated += 1
                else:
                    new_market = Market(
                        platform=m["platform"],
                        platform_id=m["platform_id"],
                        event_id=m.get("event_id", ""),
                        title=m["title"],
                        category=m.get("category", ""),
                        yes_price=m["yes_price"],
                        no_price=m["no_price"],
                        yes_bid=m["yes_bid"],
                        yes_ask=m["yes_ask"],
                        no_bid=m["no_bid"],
                        no_ask=m["no_ask"],
                        volume=m["volume"],
                        volume_24h=m["volume_24h"],
                        liquidity=m["liquidity"],
                        open_interest=m.get("open_interest", 0),
                        close_time=m.get("close_time"),
                        status=m["status"],
                        outcome_count=m.get("outcome_count", 2),
                        clob_token_id_yes=m.get("clob_token_id_yes"),
                        clob_token_id_no=m.get("clob_token_id_no"),
                        raw_data=m.get("raw_data"),
                        last_updated=now,
                    )
                    session.add(new_market)
                    inserted += 1

            # Commit each batch to avoid huge transactions
            await session.commit()

            processed = i + len(batch)
            if processed % 5000 < BATCH_SIZE:
                elapsed = time.monotonic() - t0
                rate = processed / elapsed if elapsed > 0 else 0
                logger.info(f"Upsert progress: {processed}/{len(markets)} ({rate:.0f}/s)")

    elapsed = time.monotonic() - t0
    logger.info(f"Upsert complete: {inserted} new + {updated} updated in {elapsed:.1f}s")
    return inserted, updated


# ── Market matching (Phase 3) ────────────────────────────────────────────────

async def run_market_matching(
    kalshi_markets: list[dict],
    poly_markets: list[dict],
) -> int:
    """Run the matcher on fresh market data and persist new pairs to DB."""
    if not kalshi_markets or not poly_markets:
        return 0

    # Get already-matched platform_ids so we don't re-match
    async with AsyncSessionLocal() as session:
        existing_pairs = (
            await session.execute(
                select(MatchedPair.kalshi_market_id, MatchedPair.poly_market_id)
                .where(MatchedPair.is_active == True)
            )
        ).all()

        # Build lookup of already-matched market DB IDs
        matched_kalshi_ids = {p.kalshi_market_id for p in existing_pairs}
        matched_poly_ids = {p.poly_market_id for p in existing_pairs}

        # Get DB ID mapping: (platform, platform_id) → market.id
        all_markets = (
            await session.execute(select(Market))
        ).scalars().all()
        id_lookup = {(m.platform, m.platform_id): m.id for m in all_markets}

    # Filter out already-matched markets
    unmatched_kalshi = [
        m for m in kalshi_markets
        if id_lookup.get(("kalshi", m["platform_id"])) not in matched_kalshi_ids
    ]
    unmatched_poly = [
        m for m in poly_markets
        if id_lookup.get(("polymarket", m["platform_id"])) not in matched_poly_ids
    ]

    if not unmatched_kalshi or not unmatched_poly:
        logger.info("No new unmatched markets to process")
        return 0

    logger.info(f"Running matcher on {len(unmatched_kalshi)} Kalshi + "
                f"{len(unmatched_poly)} Polymarket unmatched markets")

    matches = await run_matching(unmatched_kalshi, unmatched_poly)

    # Persist new matched pairs
    new_pairs = 0
    async with AsyncSessionLocal() as session:
        for match in matches:
            k_db_id = id_lookup.get(("kalshi", match.kalshi_market["platform_id"]))
            p_db_id = id_lookup.get(("polymarket", match.poly_market["platform_id"]))

            if not k_db_id or not p_db_id:
                continue

            pair = MatchedPair(
                kalshi_market_id=k_db_id,
                poly_market_id=p_db_id,
                confidence_score=match.confidence,
                match_method="claude_api" if match.reasoning and "fuzzy" not in match.reasoning else "fuzzy",
                match_reasoning=match.reasoning,
            )
            session.add(pair)
            new_pairs += 1

        await session.commit()

    logger.info(f"Matching complete: {new_pairs} new pairs saved")
    return new_pairs


# ── Arb detection (Phase 3) ──────────────────────────────────────────────────

async def detect_arbs() -> list[dict]:
    """Scan all active matched pairs for arb opportunities. Returns new/updated opps."""
    async with AsyncSessionLocal() as session:
        pairs = (
            await session.execute(
                select(MatchedPair).where(MatchedPair.is_active == True)
            )
        ).scalars().all()

        if not pairs:
            return []

        # Load all markets we need
        market_ids = set()
        for p in pairs:
            market_ids.add(p.kalshi_market_id)
            market_ids.add(p.poly_market_id)

        markets_result = (
            await session.execute(
                select(Market).where(Market.id.in_(market_ids))
            )
        ).scalars().all()
        market_map = {m.id: m for m in markets_result}

        # Load existing active opportunities by matched_pair_id
        existing_opps = (
            await session.execute(
                select(ArbitrageOpportunity).where(ArbitrageOpportunity.is_active == True)
            )
        ).scalars().all()
        opp_by_pair = {o.matched_pair_id: o for o in existing_opps}

        new_opps = []

        for pair in pairs:
            km = market_map.get(pair.kalshi_market_id)
            pm = market_map.get(pair.poly_market_id)
            if not km or not pm:
                continue

            # Convert ORM objects to dicts for arb engine
            k_dict = {
                "yes_ask": km.yes_ask or 0,
                "no_ask": km.no_ask or 0,
                "liquidity": km.liquidity or 0,
                "close_time": km.close_time,
            }
            p_dict = {
                "yes_ask": pm.yes_ask or 0,
                "no_ask": pm.no_ask or 0,
                "liquidity": pm.liquidity or 0,
                "close_time": pm.close_time,
            }

            opp_data = build_opportunity(k_dict, p_dict, match_confidence=pair.confidence_score)

            existing_opp = opp_by_pair.get(pair.id)

            if opp_data:
                if existing_opp:
                    # Update existing opportunity
                    await session.execute(
                        update(ArbitrageOpportunity)
                        .where(ArbitrageOpportunity.id == existing_opp.id)
                        .values(
                            direction=opp_data["direction"],
                            kalshi_price=opp_data["kalshi_price"],
                            poly_price=opp_data["poly_price"],
                            raw_spread=opp_data["raw_spread"],
                            net_profit_pct=opp_data["net_profit_pct"],
                            annualized_return=opp_data["annualized_return"],
                            liquidity_score=opp_data["liquidity_score"],
                            composite_score=opp_data["composite_score"],
                        )
                    )
                    session.add(OpportunityLog(
                        opportunity_id=existing_opp.id,
                        event_type="updated",
                        net_profit_pct=opp_data["net_profit_pct"],
                    ))
                else:
                    # New opportunity
                    new_opp = ArbitrageOpportunity(
                        matched_pair_id=pair.id,
                        direction=opp_data["direction"],
                        kalshi_price=opp_data["kalshi_price"],
                        poly_price=opp_data["poly_price"],
                        raw_spread=opp_data["raw_spread"],
                        net_profit_pct=opp_data["net_profit_pct"],
                        annualized_return=opp_data["annualized_return"],
                        liquidity_score=opp_data["liquidity_score"],
                        match_confidence=opp_data["match_confidence"],
                        composite_score=opp_data["composite_score"],
                    )
                    session.add(new_opp)
                    await session.flush()  # get the ID

                    session.add(OpportunityLog(
                        opportunity_id=new_opp.id,
                        event_type="detected",
                        net_profit_pct=opp_data["net_profit_pct"],
                    ))

                opp_data["kalshi_title"] = km.title
                opp_data["poly_title"] = pm.title
                new_opps.append(opp_data)

            elif existing_opp:
                # Arb has closed — mark expired
                await session.execute(
                    update(ArbitrageOpportunity)
                    .where(ArbitrageOpportunity.id == existing_opp.id)
                    .values(is_active=False, expired_at=datetime.utcnow())
                )
                session.add(OpportunityLog(
                    opportunity_id=existing_opp.id,
                    event_type="closed",
                    net_profit_pct=0.0,
                ))

        await session.commit()

    return new_opps


# ── Polling loop (upgraded from Phase 1) ─────────────────────────────────────

# Keep last-fetched market data in memory for matching
_last_kalshi: list[dict] = []
_last_poly: list[dict] = []


async def fetch_markets() -> tuple[list[dict], list[dict]]:
    """Fetch from both APIs, handle errors, return (kalshi, poly)."""
    kalshi_markets, poly_markets = await asyncio.gather(
        kalshi.fetch_all_markets(),
        polymarket.fetch_all_markets(),
        return_exceptions=True,
    )
    if isinstance(kalshi_markets, Exception):
        logger.error(f"Kalshi fetch failed: {kalshi_markets}")
        kalshi_markets = []
    if isinstance(poly_markets, Exception):
        logger.error(f"Polymarket fetch failed: {poly_markets}")
        poly_markets = []
    return kalshi_markets, poly_markets


async def poll_loop() -> None:
    """
    Main polling loop:
    - Full market refresh + matching every 5 minutes (also on startup)
    - Price refresh + arb detection every 30 seconds
    """
    global _last_kalshi, _last_poly
    cycle = 0
    market_refresh_interval = settings.market_poll_seconds // settings.price_poll_seconds

    while True:
        try:
            if cycle % market_refresh_interval == 0:
                # Full refresh — fetch, upsert, match
                logger.info("Full market refresh + matching...")
                k_markets, p_markets = await fetch_markets()

                all_markets = k_markets + p_markets
                if all_markets:
                    inserted, updated = await upsert_markets(all_markets)
                    logger.info(
                        f"Market refresh: {len(k_markets)} Kalshi + "
                        f"{len(p_markets)} Polymarket "
                        f"({inserted} new, {updated} updated)"
                    )

                    _print_top_markets("KALSHI", k_markets)
                    _print_top_markets("POLYMARKET", p_markets)

                    # Run matching on new unmatched markets
                    _last_kalshi, _last_poly = k_markets, p_markets
                    new_pairs = await run_market_matching(k_markets, p_markets)
                    if new_pairs:
                        logger.info(f"New matched pairs: {new_pairs}")

                # Run arb detection on all matched pairs
                opps = await detect_arbs()
                if opps:
                    _print_opportunities(opps)
                    await manager.broadcast({
                        "type": "opportunities",
                        "data": opps,
                        "count": len(opps),
                    })

            else:
                # Price-only refresh + arb scan
                logger.info(f"Price refresh cycle {cycle}...")
                k_markets, p_markets = await fetch_markets()

                all_markets = k_markets + p_markets
                if all_markets:
                    inserted, updated = await upsert_markets(all_markets)
                    logger.info(f"Price refresh: {updated} updated, {inserted} new")

                # Re-scan for arbs with fresh prices
                opps = await detect_arbs()
                if opps:
                    await manager.broadcast({
                        "type": "opportunities",
                        "data": opps,
                        "count": len(opps),
                    })

        except Exception as e:
            logger.error(f"Poll loop error: {e}", exc_info=True)

        cycle += 1
        await asyncio.sleep(settings.price_poll_seconds)


def _print_top_markets(platform: str, markets: list[dict], n: int = 10) -> None:
    """Print top N markets by volume to console."""
    sorted_markets = sorted(markets, key=lambda m: m.get("volume_24h", 0), reverse=True)
    print(f"\n  Top {n} {platform} markets by 24h volume:")
    print(f"  {'Title':<50} {'YES':>6} {'NO':>6} {'Bid':>6} {'Ask':>6} {'Vol24h':>10}")
    print(f"  {'-'*86}")
    for m in sorted_markets[:n]:
        title = m["title"][:48]
        print(
            f"  {title:<50} "
            f"{m['yes_price']:>5.2f} {m['no_price']:>5.2f} "
            f"{m['yes_bid']:>5.2f} {m['yes_ask']:>5.2f} "
            f"{m['volume_24h']:>10.0f}"
        )
    if len(markets) > n:
        print(f"  ... and {len(markets) - n} more\n")


def _print_opportunities(opps: list[dict], n: int = 10) -> None:
    """Print top arb opportunities to console."""
    sorted_opps = sorted(opps, key=lambda o: o.get("composite_score", 0), reverse=True)
    print(f"\n  {'='*90}")
    print(f"  ARB OPPORTUNITIES: {len(opps)} active")
    print(f"  {'='*90}")
    print(f"  {'Kalshi Market':<30} {'Poly Market':<30} {'Spread':>7} {'Net%':>7} {'Score':>6}")
    print(f"  {'-'*86}")
    for o in sorted_opps[:n]:
        k_title = o.get("kalshi_title", "")[:28]
        p_title = o.get("poly_title", "")[:28]
        print(
            f"  {k_title:<30} {p_title:<30} "
            f"{o['raw_spread']:>6.3f} {o['net_profit_pct']:>6.2f}% "
            f"{o['composite_score']:>5.3f}"
        )
    if len(opps) > n:
        print(f"  ... and {len(opps) - n} more")
    print()


# ── FastAPI app ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    logger.info("Database initialized")

    poll_task = asyncio.create_task(poll_loop())
    logger.info(
        f"Polling started: prices every {settings.price_poll_seconds}s, "
        f"markets every {settings.market_poll_seconds}s"
    )
    yield

    poll_task.cancel()
    await kalshi.close()
    await polymarket.close()
    logger.info("Shutdown complete")


app = FastAPI(title="Arb Scanner API", version="0.2.0", lifespan=lifespan)
_allowed_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(markets_router.router)
app.include_router(opportunities_router.router)
app.include_router(matched_pairs_router.router)


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "ws_clients": manager.client_count,
    }


@app.get("/api/stats")
async def stats():
    async with AsyncSessionLocal() as session:
        kalshi_count = (await session.execute(
            select(func.count(Market.id)).where(Market.platform == "kalshi", Market.status.in_(["open", "active"]))
        )).scalar()
        poly_count = (await session.execute(
            select(func.count(Market.id)).where(Market.platform == "polymarket", Market.status.in_(["open", "active"]))
        )).scalar()
        pair_count = (await session.execute(
            select(func.count(MatchedPair.id)).where(MatchedPair.is_active == True)
        )).scalar()
        opp_count = (await session.execute(
            select(func.count(ArbitrageOpportunity.id)).where(ArbitrageOpportunity.is_active == True)
        )).scalar()
        return {
            "kalshi_markets": kalshi_count,
            "polymarket_markets": poly_count,
            "matched_pairs": pair_count,
            "active_opportunities": opp_count,
            "ws_clients": manager.client_count,
        }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            # Keep connection alive, client can send pings
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)


# ── Standalone runner ────────────────────────────────────────────────────────

if __name__ == "__main__":
    async def standalone():
        print("Arb Scanner — Phase 3: Full Pipeline")
        print("Initializing database...")
        await init_db()
        print("Starting poll loop (Ctrl+C to stop)\n")
        try:
            await poll_loop()
        except KeyboardInterrupt:
            pass
        finally:
            await kalshi.close()
            await polymarket.close()
            print("\nShutdown.")

    asyncio.run(standalone())
