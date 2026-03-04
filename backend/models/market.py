from datetime import datetime
from typing import Optional

from sqlalchemy import String, Float, Integer, Text, DateTime, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class Market(Base):
    __tablename__ = "markets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    platform_id: Mapped[str] = mapped_column(String(200), nullable=False)
    event_id: Mapped[Optional[str]] = mapped_column(String(200))
    title: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[Optional[str]] = mapped_column(String(100))
    yes_price: Mapped[Optional[float]] = mapped_column(Float)
    no_price: Mapped[Optional[float]] = mapped_column(Float)
    yes_bid: Mapped[Optional[float]] = mapped_column(Float)
    yes_ask: Mapped[Optional[float]] = mapped_column(Float)
    no_bid: Mapped[Optional[float]] = mapped_column(Float)
    no_ask: Mapped[Optional[float]] = mapped_column(Float)
    volume: Mapped[Optional[float]] = mapped_column(Float)
    volume_24h: Mapped[Optional[float]] = mapped_column(Float)
    liquidity: Mapped[Optional[float]] = mapped_column(Float)
    open_interest: Mapped[Optional[float]] = mapped_column(Float)
    close_time: Mapped[Optional[datetime]] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(String(20), default="open")
    outcome_count: Mapped[int] = mapped_column(Integer, default=2)
    # Polymarket-specific
    clob_token_id_yes: Mapped[Optional[str]] = mapped_column(String(200))
    clob_token_id_no: Mapped[Optional[str]] = mapped_column(String(200))
    raw_data: Mapped[Optional[str]] = mapped_column(Text)
    last_updated: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (UniqueConstraint("platform", "platform_id"),)


class PriceSnapshot(Base):
    __tablename__ = "price_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_id: Mapped[int] = mapped_column(Integer, nullable=False)
    yes_price: Mapped[float] = mapped_column(Float, nullable=False)
    no_price: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str] = mapped_column(String(20), default="rest")
    recorded_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
