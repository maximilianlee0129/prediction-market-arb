from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from backend.config import settings

engine = create_async_engine(
    settings.database_url,
    echo=False,
    connect_args={"timeout": 30},  # wait up to 30s for locks instead of failing immediately
)


@event.listens_for(engine.sync_engine, "connect")
def set_wal_mode(dbapi_connection, connection_record):
    """Enable WAL journal mode — allows concurrent reads during writes."""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")  # safe with WAL, faster than FULL
    cursor.close()
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


async def init_db() -> None:
    from backend.models import market, matched_pair, opportunity  # noqa: F401 — registers models with Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
