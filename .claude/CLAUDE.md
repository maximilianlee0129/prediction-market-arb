# CLAUDE.md — Arbitrage Scanner

## Project
Prediction market arbitrage scanner comparing Kalshi and Polymarket.
Python FastAPI backend + React frontend + SQLite database.

## Tech
- Backend: Python 3.11+, FastAPI, asyncio, httpx
- Frontend: React, Tailwind CSS, Recharts
- Database: SQLite via SQLAlchemy
- APIs: kalshi-python SDK, py-clob-client (Polymarket)

## Key files
- backend/collectors/kalshi.py — Kalshi data fetcher
- backend/collectors/polymarket.py — Polymarket data fetcher
- backend/matcher.py — Market matching engine
- backend/arb_engine.py — Arbitrage calculator
- backend/main.py — FastAPI server + scheduling

## Rules
- Always handle API rate limits gracefully (exponential backoff)
- Never hardcode API keys — use environment variables via python-dotenv
- All prices normalized to 0-1 scale (Kalshi prices are already 0-1 dollar strings)
- Log every API call and its response time
- Write tests for the arb math — this MUST be correct
- Use type hints everywhere
- Handle edge cases: markets with different resolution dates,
  markets that resolve early, markets with >2 outcomes

## Running
- Backend: `cd backend && uvicorn main:app --reload`
- Frontend: `cd frontend && npm start`
- Full stack: use the run script (TBD)

## Current Phase
Phase 4 complete — Full stack operational.
Backend: collectors + arb engine + matcher + routers + WebSocket.
Frontend: React + Tailwind + Recharts. Dashboard, Markets, History pages.
Need: ANTHROPIC_API_KEY in .env for Claude-powered matching (fuzzy-only without it).