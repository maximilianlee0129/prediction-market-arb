"""
Kalshi data collector.

Uses the public v2 REST API — no authentication needed for market data.
Prices come as dollar strings ("0.6500") already on 0-1 scale.
Cursor-based pagination, up to 1000 per page.
"""
import asyncio
import random
import time
from datetime import datetime
from typing import Optional

import httpx

from backend.config import settings
from backend.utils.logger import get_logger, log_api_call

logger = get_logger(__name__)

# Delay between pages to stay under rate limits
PAGE_DELAY = 0.3  # seconds — halves request rate vs 0.15, avoids sustained 429s


class KalshiCollector:
    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=settings.kalshi_base_url,
                timeout=30.0,
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _fetch_page(self, client: httpx.AsyncClient, path: str, params: dict) -> dict:
        """Fetch a single page with per-request retry on 429."""
        for attempt in range(5):
            resp = await client.get(path, params=params)
            if resp.status_code == 429:
                delay = min(1.0 * (2**attempt) + random.uniform(0, 1), 30.0)
                logger.warning(f"Kalshi 429 on {path}, retrying in {delay:.1f}s (attempt {attempt + 1})")
                await asyncio.sleep(delay)
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()  # raise on final failure — caller handles this
        return {}

    @log_api_call(logger)
    async def fetch_all_markets(self) -> list[dict]:
        """
        Fetch all active/open markets via paginated GET /markets.
        Returns list of normalized market dicts with prices on 0-1 scale.
        Handles rate limits per-page so pagination doesn't restart.
        """
        client = await self._get_client()
        markets: list[dict] = []
        cursor: Optional[str] = None
        start_time = time.monotonic()
        last_log = start_time

        while True:
            params: dict = {"status": "open", "limit": 1000}
            if cursor:
                params["cursor"] = cursor

            try:
                data = await self._fetch_page(client, "/markets", params)
            except Exception as e:
                logger.warning(f"Kalshi page fetch failed, returning {len(markets):,} partial markets: {e}")
                break

            for raw in data.get("markets", []):
                normalized = self._normalize(raw)
                if normalized:
                    markets.append(normalized)

            now = time.monotonic()
            if now - last_log >= 180:
                elapsed = now - start_time
                logger.info(f"Kalshi fetch progress: {len(markets):,} markets fetched ({elapsed/60:.1f} min elapsed)")
                last_log = now

            cursor = data.get("cursor")
            if not cursor:
                break

            await asyncio.sleep(PAGE_DELAY)

        logger.info(f"Kalshi: fetched {len(markets)} open markets")
        return markets

    @log_api_call(logger)
    async def fetch_events(self) -> list[dict]:
        """Fetch all open events for matching context."""
        client = await self._get_client()
        events: list[dict] = []
        cursor: Optional[str] = None

        while True:
            params: dict = {"status": "open", "limit": 1000}
            if cursor:
                params["cursor"] = cursor

            data = await self._fetch_page(client, "/events", params)
            events.extend(data.get("events", []))

            cursor = data.get("cursor")
            if not cursor:
                break

            await asyncio.sleep(PAGE_DELAY)

        logger.info(f"Kalshi: fetched {len(events)} open events")
        return events

    @staticmethod
    def _normalize(raw: dict) -> Optional[dict]:
        """
        Convert raw Kalshi API response to our internal format.

        Kalshi v2 returns prices as dollar strings: "0.6500"
        Already 0-1 scale — no division needed.
        """
        # Skip multi-leg combo/parlay markets (KXMVE prefix).
        # These are cross-category and multi-game parlays with no Polymarket equivalent.
        # They make up 70%+ of Kalshi's market count but can never produce arb opportunities.
        ticker = raw.get("ticker", "")
        if ticker.startswith("KXMVE"):
            return None

        # Parse dollar-string prices, defaulting to 0 if missing/empty
        def _price(field: str) -> float:
            val = raw.get(field, "")
            try:
                return float(val) if val else 0.0
            except (ValueError, TypeError):
                return 0.0

        yes_bid = _price("yes_bid_dollars")
        yes_ask = _price("yes_ask_dollars")
        no_bid = _price("no_bid_dollars")
        no_ask = _price("no_ask_dollars")
        last_price = _price("last_price_dollars")

        # Midpoint price for display
        yes_price = (yes_bid + yes_ask) / 2 if (yes_bid + yes_ask) > 0 else last_price
        no_price = 1.0 - yes_price

        # Parse close time
        close_time = None
        for field in ("close_time", "expected_expiration_time", "latest_expiration_time"):
            if raw.get(field):
                try:
                    close_time = datetime.fromisoformat(raw[field].replace("Z", "+00:00"))
                    break
                except ValueError:
                    continue

        return {
            "platform": "kalshi",
            "platform_id": raw.get("ticker", ""),
            "event_id": raw.get("event_ticker", ""),
            "title": raw.get("title") or raw.get("yes_sub_title", ""),
            "category": raw.get("category", ""),
            "yes_price": round(yes_price, 4),
            "no_price": round(no_price, 4),
            "yes_bid": round(yes_bid, 4),
            "yes_ask": round(yes_ask, 4),
            "no_bid": round(no_bid, 4),
            "no_ask": round(no_ask, 4),
            "volume": _price("volume_fp"),
            "volume_24h": _price("volume_24h_fp"),
            "liquidity": 0.0,  # deprecated in v2 API
            "open_interest": _price("open_interest_fp"),
            "close_time": close_time,
            "status": raw.get("status", "open"),
            "outcome_count": 2,  # Kalshi markets are binary
        }


# Standalone test: python -m backend.collectors.kalshi
if __name__ == "__main__":
    import asyncio

    async def main():
        collector = KalshiCollector()
        try:
            markets = await collector.fetch_all_markets()
            print(f"\n{'='*80}")
            print(f"KALSHI: {len(markets)} open markets")
            print(f"{'='*80}\n")
            for m in markets[:20]:
                print(
                    f"  {m['platform_id']:<40} "
                    f"YES={m['yes_price']:.2f}  NO={m['no_price']:.2f}  "
                    f"bid={m['yes_bid']:.2f} ask={m['yes_ask']:.2f}  "
                    f"vol24h={m['volume_24h']:.0f}"
                )
            if len(markets) > 20:
                print(f"  ... and {len(markets) - 20} more")
        finally:
            await collector.close()

    asyncio.run(main())
