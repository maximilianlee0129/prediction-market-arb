"""
Market matcher: find equivalent markets across Kalshi and Polymarket.

Pipeline:
1. fuzzy_prefilter — rapidfuzz token_sort_ratio to find candidates (fast, broad)
2. claude_batch_match — Claude Sonnet to confirm matches with reasoning (accurate, slow)
3. run_matching — full pipeline combining both steps
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Optional

import anthropic
from rapidfuzz import fuzz, process

from backend.config import settings
from backend.utils.logger import get_logger

logger = get_logger(__name__)

FUZZY_THRESHOLD = 68  # minimum token_sort_ratio to consider as candidate
MAX_CANDIDATES_PER_MARKET = 5
CLAUDE_BATCH_SIZE = 20  # pairs per API call
CLAUDE_CONCURRENCY = 1  # sequential — avoids rate limits entirely
MATCH_CONFIDENCE_THRESHOLD = 0.6  # minimum to accept as a match
CLAUDE_MODEL = "claude-haiku-4-5-20251001"  # faster + higher rate limits than Sonnet
MIN_VOLUME_FOR_MATCHING = 10_000  # skip low-volume markets — no arb worth taking


@dataclass
class MatchCandidate:
    """A potential cross-platform market match."""
    kalshi_market: dict
    poly_market: dict
    fuzzy_score: float
    confidence: float = 0.0
    reasoning: str = ""


def fuzzy_prefilter(
    kalshi_markets: list[dict],
    poly_markets: list[dict],
) -> list[MatchCandidate]:
    """
    Fast first pass: use rapidfuzz process.extract to find candidate pairs.

    Filters to markets with meaningful volume first, then uses the optimized
    C-level extract function instead of Python-level loops.

    Returns candidates sorted by fuzzy score descending.
    """
    import time
    t0 = time.monotonic()

    # Filter to markets with meaningful activity
    active_kalshi = [m for m in kalshi_markets if (m.get("volume", 0) or 0) >= MIN_VOLUME_FOR_MATCHING]
    active_poly = [m for m in poly_markets if (m.get("volume", 0) or 0) >= MIN_VOLUME_FOR_MATCHING]

    logger.info(f"Fuzzy prefilter: {len(active_kalshi)}/{len(kalshi_markets)} Kalshi + "
                f"{len(active_poly)}/{len(poly_markets)} Polymarket above volume threshold")

    if not active_kalshi or not active_poly:
        return []

    # Build title → market index for Polymarket
    poly_titles = [pm["title"] for pm in active_poly]

    candidates: list[MatchCandidate] = []

    for i, km in enumerate(active_kalshi):
        # process.extract uses C-optimized comparison against all choices
        results = process.extract(
            km["title"],
            poly_titles,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=FUZZY_THRESHOLD,
            limit=MAX_CANDIDATES_PER_MARKET,
        )

        for title, score, idx in results:
            candidates.append(MatchCandidate(
                kalshi_market=km,
                poly_market=active_poly[idx],
                fuzzy_score=score,
            ))

        if (i + 1) % 1000 == 0:
            elapsed = time.monotonic() - t0
            logger.info(f"Fuzzy progress: {i+1}/{len(active_kalshi)} ({(i+1)/elapsed:.0f}/s)")

    candidates.sort(key=lambda c: c.fuzzy_score, reverse=True)
    elapsed = time.monotonic() - t0
    logger.info(f"Fuzzy prefilter: {len(candidates)} candidates in {elapsed:.1f}s "
                f"({len(active_kalshi)} × {len(active_poly)})")
    return candidates


def _build_claude_prompt(pairs: list[MatchCandidate]) -> str:
    """Build a prompt for Claude to evaluate match candidates."""
    pair_descriptions = []
    for i, c in enumerate(pairs):
        pair_descriptions.append(
            f"Pair {i+1}:\n"
            f"  Kalshi: \"{c.kalshi_market['title']}\"\n"
            f"  Polymarket: \"{c.poly_market['title']}\"\n"
            f"  Kalshi category: {c.kalshi_market.get('category', 'N/A')}\n"
            f"  Polymarket category: {c.poly_market.get('category', 'N/A')}"
        )

    pairs_text = "\n\n".join(pair_descriptions)

    return f"""You are evaluating whether prediction market questions from two different platforms refer to the SAME real-world event/outcome.

Two markets "match" if they resolve identically — a YES on one platform means YES on the other, and vice versa. Be strict: similar topics are NOT matches unless they ask the exact same question with the same resolution criteria.

For each pair below, respond with a JSON object containing:
- "pair": the pair number (integer)
- "match": true or false
- "confidence": 0.0 to 1.0 (how confident you are)
- "reasoning": brief explanation (1 sentence)

Respond with a JSON array of objects, one per pair. No other text.

{pairs_text}"""


async def claude_batch_match(
    candidates: list[MatchCandidate],
) -> list[MatchCandidate]:
    """
    Use Claude Sonnet to confirm/reject match candidates.

    Sends candidates in batches of CLAUDE_BATCH_SIZE, parses JSON responses,
    and updates confidence + reasoning on each candidate.

    Returns only confirmed matches (confidence >= threshold).
    """
    if not settings.use_claude_matching or not settings.anthropic_api_key:
        reason = "USE_CLAUDE_MATCHING=false" if not settings.use_claude_matching else "no ANTHROPIC_API_KEY"
        logger.info(f"Claude matching disabled ({reason}) — using fuzzy scores as confidence")
        # Fallback: use fuzzy score as confidence (0-100 → 0-1)
        for c in candidates:
            c.confidence = c.fuzzy_score / 100.0
            c.reasoning = "fuzzy-only (no API key)"
        return [c for c in candidates if c.confidence >= MATCH_CONFIDENCE_THRESHOLD]

    client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
    confirmed: list[MatchCandidate] = []

    # Split into batches
    batches = [
        candidates[i:i + CLAUDE_BATCH_SIZE]
        for i in range(0, len(candidates), CLAUDE_BATCH_SIZE)
    ]

    async def process_one_batch(batch_idx: int, batch: list[MatchCandidate]) -> list[MatchCandidate]:
        """Send one batch to Claude and return confirmed matches."""
        prompt = _build_claude_prompt(batch)
        try:
            response = await client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
            )
            response_text = response.content[0].text.strip()
            if response_text.startswith("```"):
                response_text = response_text.split("\n", 1)[1]
                if response_text.endswith("```"):
                    response_text = response_text[:-3]

            results = json.loads(response_text)
            batch_confirmed = []
            for result in results:
                idx = result["pair"] - 1
                if 0 <= idx < len(batch):
                    candidate = batch[idx]
                    candidate.confidence = float(result.get("confidence", 0))
                    candidate.reasoning = result.get("reasoning", "")
                    if result.get("match") and candidate.confidence >= MATCH_CONFIDENCE_THRESHOLD:
                        batch_confirmed.append(candidate)

            logger.info(f"Claude batch {batch_idx + 1}: "
                        f"{len([r for r in results if r.get('match')])} matches "
                        f"from {len(batch)} candidates")
            return batch_confirmed

        except json.JSONDecodeError as e:
            logger.error(f"Claude batch {batch_idx + 1} parse error: {e}")
            fallback = []
            for c in batch:
                c.confidence = c.fuzzy_score / 100.0
                c.reasoning = "fuzzy-fallback (parse error)"
                if c.confidence >= MATCH_CONFIDENCE_THRESHOLD:
                    fallback.append(c)
            return fallback

        except anthropic.APIError as e:
            logger.error(f"Claude batch {batch_idx + 1} API error: {e}")
            fallback = []
            for c in batch:
                c.confidence = c.fuzzy_score / 100.0
                c.reasoning = "fuzzy-fallback (API error)"
                if c.confidence >= MATCH_CONFIDENCE_THRESHOLD:
                    fallback.append(c)
            return fallback

    # Process batches with controlled concurrency
    semaphore = asyncio.Semaphore(CLAUDE_CONCURRENCY)

    async def process_with_semaphore(batch_idx: int, batch: list[MatchCandidate]) -> list[MatchCandidate]:
        async with semaphore:
            return await process_one_batch(batch_idx, batch)

    results_list = await asyncio.gather(
        *[process_with_semaphore(i, batch) for i, batch in enumerate(batches)]
    )
    for batch_confirmed in results_list:
        confirmed.extend(batch_confirmed)

    return confirmed


async def run_matching(
    kalshi_markets: list[dict],
    poly_markets: list[dict],
) -> list[MatchCandidate]:
    """
    Full matching pipeline:
    1. Fuzzy prefilter → candidate pairs
    2. Claude batch verification → confirmed matches

    Returns confirmed MatchCandidate objects with confidence and reasoning.
    """
    if not kalshi_markets or not poly_markets:
        return []

    # Step 1: Fast fuzzy prefilter
    candidates = fuzzy_prefilter(kalshi_markets, poly_markets)
    if not candidates:
        logger.info("No fuzzy candidates found")
        return []

    logger.info(f"Sending {len(candidates)} candidates to Claude for verification")

    # Step 2: Claude verification
    confirmed = await claude_batch_match(candidates)

    # Deduplicate: keep highest confidence match per Kalshi market
    best_by_kalshi: dict[str, MatchCandidate] = {}
    for c in confirmed:
        k_id = c.kalshi_market["platform_id"]
        if k_id not in best_by_kalshi or c.confidence > best_by_kalshi[k_id].confidence:
            best_by_kalshi[k_id] = c

    # Also deduplicate per Polymarket market (one poly market → one match)
    best_by_poly: dict[str, MatchCandidate] = {}
    for c in best_by_kalshi.values():
        p_id = c.poly_market["platform_id"]
        if p_id not in best_by_poly or c.confidence > best_by_poly[p_id].confidence:
            best_by_poly[p_id] = c

    final = list(best_by_poly.values())
    final.sort(key=lambda c: c.confidence, reverse=True)

    logger.info(f"Matching complete: {len(final)} confirmed pairs "
                f"(from {len(candidates)} candidates)")
    return final
