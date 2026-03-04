"""
Polymarket data collector via Gamma API.

No authentication needed. Prices in outcomePrices are 0-1 scale strings
inside a JSON-encoded string array — must json.loads() to parse.
Offset-based pagination.
"""
import asyncio
import json
import random
import time
from datetime import datetime
from typing import Optional

import httpx

from backend.config import settings
from backend.utils.logger import get_logger, log_api_call

logger = get_logger(__name__)

PAGE_DELAY = 0.1  # seconds between pages


class PolymarketCollector:
    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=settings.gamma_base_url,
                timeout=30.0,
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _fetch_page(self, client: httpx.AsyncClient, path: str, params: dict) -> list | dict:
        """Fetch a single page with per-request retry on 429."""
        for attempt in range(5):
            resp = await client.get(path, params=params)
            if resp.status_code == 429:
                delay = min(2.0 * (2**attempt) + random.uniform(0, 1), 30.0)
                logger.warning(f"Polymarket 429 on {path}, retrying in {delay:.1f}s (attempt {attempt + 1})")
                await asyncio.sleep(delay)
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()
        return []

    @log_api_call(logger)
    async def fetch_all_markets(self) -> list[dict]:
        """
        Fetch all active binary markets via Gamma API with offset pagination.
        Skips non-binary markets (outcomes != 2) and archived markets.
        """
        client = await self._get_client()
        markets: list[dict] = []
        offset = 0
        limit = 100
        start_time = time.monotonic()
        last_log = start_time

        while True:
            params = {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "limit": limit,
                "offset": offset,
                "order": "volume24hr",
                "ascending": "false",
            }
            data = await self._fetch_page(client, "/markets", params)

            if not data:
                break

            for raw in data:
                normalized = self._normalize(raw)
                if normalized:
                    markets.append(normalized)

            now = time.monotonic()
            if now - last_log >= 180:
                elapsed = now - start_time
                logger.info(f"Polymarket fetch progress: {len(markets):,} markets fetched ({elapsed/60:.1f} min elapsed)")
                last_log = now

            if len(data) < limit:
                break
            offset += limit
            await asyncio.sleep(PAGE_DELAY)

        logger.info(f"Polymarket: fetched {len(markets)} open binary markets")
        return markets

    @log_api_call(logger)
    async def fetch_events(self) -> list[dict]:
        """Fetch all active events for matching context."""
        client = await self._get_client()
        events: list[dict] = []
        offset = 0
        limit = 100

        while True:
            params = {
                "active": "true",
                "closed": "false",
                "limit": limit,
                "offset": offset,
            }
            data = await self._fetch_page(client, "/events", params)

            if not data:
                break
            events.extend(data)
            if len(data) < limit:
                break
            offset += limit
            await asyncio.sleep(PAGE_DELAY)

        logger.info(f"Polymarket: fetched {len(events)} active events")
        return events

    @staticmethod
    def _parse_json_string(val: str) -> list:
        """Parse a JSON-encoded string field like outcomePrices."""
        if not val:
            return []
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return []

    @classmethod
    def _normalize(cls, raw: dict) -> Optional[dict]:
        """
        Convert Gamma API market to our internal format.

        outcomePrices, outcomes, and clobTokenIds are JSON-encoded strings
        that need json.loads() to parse. Prices are already 0-1 scale.
        Only returns binary (Yes/No) markets.
        """
        outcomes = cls._parse_json_string(raw.get("outcomes", ""))
        outcome_prices = cls._parse_json_string(raw.get("outcomePrices", ""))
        clob_token_ids = cls._parse_json_string(raw.get("clobTokenIds", ""))

        # Only handle binary markets
        if len(outcomes) != 2:
            return None

        # Parse prices
        try:
            yes_price = float(outcome_prices[0]) if outcome_prices else 0.0
            no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else 0.0
        except (ValueError, IndexError):
            yes_price, no_price = 0.0, 0.0

        # Parse bid/ask from direct fields
        def _float(field: str) -> float:
            val = raw.get(field)
            try:
                return float(val) if val is not None else 0.0
            except (ValueError, TypeError):
                return 0.0

        best_bid = _float("bestBid")
        best_ask = _float("bestAsk")

        # Token IDs for CLOB order book (Phase 2+)
        token_yes = clob_token_ids[0] if len(clob_token_ids) > 0 else None
        token_no = clob_token_ids[1] if len(clob_token_ids) > 1 else None

        # Parse end date
        close_time = None
        if raw.get("endDate"):
            try:
                close_time = datetime.fromisoformat(raw["endDate"].replace("Z", "+00:00"))
            except ValueError:
                pass

        return {
            "platform": "polymarket",
            "platform_id": raw.get("conditionId") or raw.get("id", ""),
            "event_id": "",
            "title": raw.get("question", ""),
            "category": raw.get("category", ""),
            "yes_price": round(yes_price, 4),
            "no_price": round(no_price, 4),
            "yes_bid": round(best_bid, 4),
            "yes_ask": round(best_ask, 4),
            "no_bid": round(1 - best_ask, 4) if best_ask else 0.0,
            "no_ask": round(1 - best_bid, 4) if best_bid else 0.0,
            "volume": _float("volumeNum"),
            "volume_24h": _float("volume24hr"),
            "liquidity": _float("liquidityNum"),
            "open_interest": 0.0,
            "close_time": close_time,
            "status": "open",
            "outcome_count": 2,
            "clob_token_id_yes": token_yes,
            "clob_token_id_no": token_no,
            "raw_data": json.dumps(raw),
        }


# Standalone test: python -m backend.collectors.polymarket
if __name__ == "__main__":
    import asyncio

    async def main():
        collector = PolymarketCollector()
        try:
            markets = await collector.fetch_all_markets()
            print(f"\n{'='*80}")
            print(f"POLYMARKET: {len(markets)} open binary markets")
            print(f"{'='*80}\n")
            for m in markets[:20]:
                title = m["title"][:50]
                print(
                    f"  {title:<52} "
                    f"YES={m['yes_price']:.2f}  NO={m['no_price']:.2f}  "
                    f"bid={m['yes_bid']:.2f} ask={m['yes_ask']:.2f}  "
                    f"liq=${m['liquidity']:.0f}"
                )
            if len(markets) > 20:
                print(f"  ... and {len(markets) - 20} more")
        finally:
            await collector.close()

    asyncio.run(main())
