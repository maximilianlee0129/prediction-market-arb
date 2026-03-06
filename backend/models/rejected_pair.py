from datetime import datetime

from sqlalchemy import String, Float, DateTime, UniqueConstraint, Integer
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class RejectedPair(Base):
    """Tracks (kalshi, poly) candidate pairs that Claude confirmed are NOT matches.

    Purpose: prevent re-evaluating the same pairs on every poll cycle.
    A pair stays here permanently — market titles don't change, so if it
    wasn't a match today it won't be one tomorrow.
    """
    __tablename__ = "rejected_pairs"
    __table_args__ = (
        UniqueConstraint("kalshi_platform_id", "poly_platform_id", name="uq_rejected_pair"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    kalshi_platform_id: Mapped[str] = mapped_column(String(200), nullable=False)
    poly_platform_id: Mapped[str] = mapped_column(String(200), nullable=False)
    fuzzy_score: Mapped[float] = mapped_column(Float, nullable=False)
    evaluated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
