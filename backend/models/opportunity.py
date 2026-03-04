from datetime import datetime
from typing import Optional

from sqlalchemy import String, Float, Integer, Text, DateTime, Boolean
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class ArbitrageOpportunity(Base):
    __tablename__ = "arbitrage_opportunities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    matched_pair_id: Mapped[int] = mapped_column(Integer, nullable=False)
    direction: Mapped[str] = mapped_column(String(30), nullable=False)
    kalshi_price: Mapped[float] = mapped_column(Float, nullable=False)
    poly_price: Mapped[float] = mapped_column(Float, nullable=False)
    raw_spread: Mapped[float] = mapped_column(Float, nullable=False)
    net_profit_pct: Mapped[float] = mapped_column(Float, nullable=False)
    annualized_return: Mapped[float] = mapped_column(Float, default=0.0)
    liquidity_score: Mapped[float] = mapped_column(Float, default=0.0)
    match_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    composite_score: Mapped[float] = mapped_column(Float, default=0.0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    expired_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class OpportunityLog(Base):
    __tablename__ = "opportunity_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    opportunity_id: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(20), nullable=False)  # detected/updated/closed
    net_profit_pct: Mapped[float] = mapped_column(Float, nullable=False)
    snapshot_data: Mapped[Optional[str]] = mapped_column(Text)
    recorded_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
