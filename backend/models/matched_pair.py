from datetime import datetime

from sqlalchemy import String, Float, Integer, Text, DateTime, Boolean
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class MatchedPair(Base):
    __tablename__ = "matched_pairs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    kalshi_market_id: Mapped[int] = mapped_column(Integer, nullable=False)
    poly_market_id: Mapped[int] = mapped_column(Integer, nullable=False)
    confidence_score: Mapped[float] = mapped_column(Float, nullable=False)
    match_method: Mapped[str] = mapped_column(String(20), nullable=False)  # "claude_api" or "fuzzy"
    match_reasoning: Mapped[str] = mapped_column(Text, default="")
    resolution_delta_days: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
