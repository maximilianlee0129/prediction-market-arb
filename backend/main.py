"""
FastAPI application — Phase 3: Full Pipeline.

Background tasks:
- Every 5 minutes: fetch all markets from both APIs → upsert to DB → run matching
- Every 30 seconds: refresh prices → run arb detection on matched pairs → broadcast

Run: cd backend && uvicorn main:app --reload
Or standalone poller: python -m backend.main
"""
import asyncio
import gc
import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, update, func
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from backend.config import settings
from backend.database import init_db, AsyncSessionLocal
from backend.models.market import Market, PriceSnapshot
from backend.models.matched_pair import MatchedPair
from backend.models.rejected_pair import RejectedPair
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

# In-memory cache of last successful Kalshi slim market list for matching fallback
_kalshi_slim_cache: list[dict] = []


# ── Market upsert (unchanged from Phase 1) ──────────────────────────────────

async def upsert_markets(markets: list[dict]) -> int:
    """Bulk upsert markets using INSERT OR REPLACE. Returns total processed.

    Uses SQLite's INSERT ... ON CONFLICT DO UPDATE (one SQL statement per
    batch of 500) instead of individual INSERT/UPDATE per row — much faster
    when most rows already exist.
    """
    if not markets:
        return 0

    import time
    t0 = time.monotonic()

    # Deduplicate: keep last occurrence of each (platform, platform_id)
    seen: dict[tuple[str, str], int] = {}
    for idx, m in enumerate(markets):
        seen[(m["platform"], m["platform_id"])] = idx
    markets = [markets[i] for i in sorted(seen.values())]

    BATCH_SIZE = 500
    now = datetime.utcnow()

    async with AsyncSessionLocal() as session:
        for i in range(0, len(markets), BATCH_SIZE):
            batch = markets[i:i + BATCH_SIZE]

            values = [
                {
                    "platform": m["platform"],
                    "platform_id": m["platform_id"],
                    "event_id": m.get("event_id", ""),
                    "title": m["title"],
                    "category": m.get("category", ""),
                    "yes_price": m["yes_price"],
                    "no_price": m["no_price"],
                    "yes_bid": m["yes_bid"],
                    "yes_ask": m["yes_ask"],
                    "no_bid": m["no_bid"],
                    "no_ask": m["no_ask"],
                    "volume": m["volume"],
                    "volume_24h": m["volume_24h"],
                    "liquidity": m["liquidity"],
                    "open_interest": m.get("open_interest", 0),
                    "close_time": m.get("close_time"),
                    "status": m["status"],
                    "outcome_count": m.get("outcome_count", 2),
                    "clob_token_id_yes": m.get("clob_token_id_yes"),
                    "clob_token_id_no": m.get("clob_token_id_no"),
                    "last_updated": now,
                }
                for m in batch
            ]

            stmt = sqlite_insert(Market).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=["platform", "platform_id"],
                set_={
                    "yes_price": stmt.excluded.yes_price,
                    "no_price": stmt.excluded.no_price,
                    "yes_bid": stmt.excluded.yes_bid,
                    "yes_ask": stmt.excluded.yes_ask,
                    "no_bid": stmt.excluded.no_bid,
                    "no_ask": stmt.excluded.no_ask,
                    "volume": stmt.excluded.volume,
                    "volume_24h": stmt.excluded.volume_24h,
                    "liquidity": stmt.excluded.liquidity,
                    "open_interest": stmt.excluded.open_interest,
                    "status": stmt.excluded.status,
                    "last_updated": stmt.excluded.last_updated,
                },
            )
            await session.execute(stmt)
            await session.commit()

            processed = i + len(batch)
            if processed % 5000 < BATCH_SIZE:
                elapsed = time.monotonic() - t0
                rate = processed / elapsed if elapsed > 0 else 0
                logger.info(f"Upsert progress: {processed}/{len(markets)} ({rate:.0f}/s)")

    elapsed = time.monotonic() - t0
    logger.info(f"Upsert complete: {len(markets)} markets in {elapsed:.1f}s")
    return len(markets)


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

        # Get DB ID mapping: (platform, platform_id) → market.id (3 columns only, not full ORM)
        result = await session.execute(
            select(Market.id, Market.platform, Market.platform_id)
        )
        id_lookup = {(r.platform, r.platform_id): r.id for r in result.all()}

        # Load all previously rejected pairs so Claude doesn't re-evaluate them
        rejected_result = await session.execute(
            select(RejectedPair.kalshi_platform_id, RejectedPair.poly_platform_id)
        )
        rejected_pairs: set[tuple[str, str]] = {
            (r.kalshi_platform_id, r.poly_platform_id) for r in rejected_result.all()
        }

    logger.info(f"Loaded {len(rejected_pairs)} previously-rejected pairs from DB")

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

    matches, newly_rejected = await run_matching(unmatched_kalshi, unmatched_poly, rejected_pairs)

    # Persist new matched pairs and newly rejected pairs
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

        # Save newly rejected pairs — INSERT OR IGNORE so duplicates don't crash
        if newly_rejected:
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert
            rejected_values = [
                {
                    "kalshi_platform_id": c.kalshi_market["platform_id"],
                    "poly_platform_id": c.poly_market["platform_id"],
                    "fuzzy_score": c.fuzzy_score,
                }
                for c in newly_rejected
            ]
            stmt = sqlite_insert(RejectedPair).values(rejected_values)
            stmt = stmt.on_conflict_do_nothing(index_elements=["kalshi_platform_id", "poly_platform_id"])
            await session.execute(stmt)
            logger.info(f"Saved {len(newly_rejected)} newly rejected pairs to DB")

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

    new_opps.sort(key=lambda o: o.get("composite_score", 0), reverse=True)
    return new_opps


# ── Polling loop (upgraded from Phase 1) ─────────────────────────────────────

async def _fetch_and_upsert(collector, platform: str) -> list[dict]:
    """Fetch markets from one platform, upsert to DB, return the list.

    Only stores markets with volume >= MIN_VOLUME_FOR_MATCHING. Zero-volume
    markets (e.g. Kalshi multi-leg combo bets with no activity) are never
    candidates for matching and waste significant disk space.
    """
    from backend.matcher import MIN_VOLUME_FOR_MATCHING
    try:
        markets = await collector.fetch_all_markets()
    except Exception as e:
        logger.error(f"{platform} fetch failed: {e}")
        return []
    if markets:
        before = len(markets)
        markets = [m for m in markets if (m.get("volume") or 0) >= MIN_VOLUME_FOR_MATCHING]
        logger.info(f"{platform}: filtered {before} → {len(markets)} markets (volume >= {MIN_VOLUME_FOR_MATCHING})")
        total = await upsert_markets(markets)
        logger.info(f"{platform}: {total} markets upserted")
    return markets


async def poll_loop() -> None:
    """
    Main polling loop:
    - Full market refresh + matching every 5 minutes (also on startup)
    - Price refresh + arb detection every 30 seconds

    Fetches platforms sequentially (not in parallel) to keep peak RAM low.
    """
    global _kalshi_slim_cache
    cycle = 0
    market_refresh_interval = settings.market_poll_seconds // settings.price_poll_seconds

    while True:
        try:
            is_full_refresh = cycle % market_refresh_interval == 0

            if is_full_refresh:
                # Full refresh: fetch all markets, upsert, match new pairs
                logger.info("Full market refresh + matching...")
                k_markets = await _fetch_and_upsert(kalshi, "Kalshi")
                p_markets = await _fetch_and_upsert(polymarket, "Polymarket")

                MIN_FRESH_MARKETS = 500
                is_fresh_kalshi = len(k_markets) >= MIN_FRESH_MARKETS

                if not is_fresh_kalshi:
                    # Primary fallback: in-memory cache from last successful collection
                    if _kalshi_slim_cache:
                        logger.warning(
                            f"Kalshi fresh fetch returned only {len(k_markets)} markets, "
                            f"using memory cache ({len(_kalshi_slim_cache)} markets)"
                        )
                        k_markets = _kalshi_slim_cache
                    else:
                        # Secondary fallback: DB (sparse on startup until first success)
                        logger.warning(
                            f"Kalshi fresh fetch returned only {len(k_markets)} markets "
                            f"and cache empty, loading from DB"
                        )
                        async with AsyncSessionLocal() as session:
                            from backend.matcher import MIN_VOLUME_FOR_MATCHING
                            db_result = await session.execute(
                                select(Market.platform_id, Market.title, Market.category, Market.volume)
                                .where(
                                    Market.platform == "kalshi",
                                    Market.status.in_(["open", "active"]),
                                    Market.volume >= MIN_VOLUME_FOR_MATCHING,
                                )
                            )
                            k_markets = [
                                {"platform_id": r.platform_id, "title": r.title,
                                 "category": r.category or "", "volume": r.volume or 0}
                                for r in db_result.all()
                            ]
                        logger.info(f"Loaded {len(k_markets)} Kalshi markets from DB for matching")

                if k_markets and p_markets:
                    if is_fresh_kalshi:
                        _print_top_markets("KALSHI", k_markets)
                    _print_top_markets("POLYMARKET", p_markets)

                    k_slim = [{"platform_id": m["platform_id"], "title": m["title"],
                               "category": m.get("category", ""), "volume": m.get("volume", 0)}
                              for m in k_markets]
                    p_slim = [{"platform_id": m["platform_id"], "title": m["title"],
                               "category": m.get("category", ""), "volume": m.get("volume", 0)}
                              for m in p_markets]

                    # Update in-memory cache after a fresh successful collection
                    if is_fresh_kalshi:
                        _kalshi_slim_cache = k_slim
                        logger.info(f"Kalshi market cache updated: {len(_kalshi_slim_cache)} markets")

                    del k_markets, p_markets
                    gc.collect()

                    new_pairs = await run_market_matching(k_slim, p_slim)
                    del k_slim, p_slim
                    gc.collect()

                    if new_pairs:
                        logger.info(f"New matched pairs: {new_pairs}")
                else:
                    del k_markets, p_markets
                    gc.collect()
            else:
                # Price refresh: skip full fetch — just re-scan arbs on current DB prices
                logger.info(f"Arb scan cycle {cycle} (using stored prices)...")

            # Arb detection runs every cycle on whatever prices are in the DB
            opps = await detect_arbs()
            if opps:
                if is_full_refresh:
                    _print_opportunities(opps)
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
