"""Database engine and session management for sync and async operations."""

from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Generator
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

from smartdialer.core.config import settings
from smartdialer.db.base import Base

# Sync engine & session maker
sync_engine = create_engine(
    settings.sync_database_url,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_pre_ping=True,
)

SyncSessionFactory = sessionmaker(
    bind=sync_engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)

# Async engine & session maker
async_engine: AsyncEngine = create_async_engine(
    settings.async_database_url,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_pre_ping=True,
)

AsyncSessionFactory = async_sessionmaker(
    bind=async_engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
    class_=AsyncSession,
)


@contextmanager
def get_sync_session() -> Generator[Session, None, None]:
    """Context manager yielding a synchronous database session with automatic commit/rollback."""
    session = SyncSessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@asynccontextmanager
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """Async context manager yielding an async database session with automatic commit/rollback."""
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def init_db(engine=None, drop_all: bool = False) -> None:
    """Initialize database tables synchronously."""
    target_engine = engine or sync_engine
    if drop_all:
        Base.metadata.drop_all(bind=target_engine)
    Base.metadata.create_all(bind=target_engine)


async def async_init_db(target_async_engine=None, drop_all: bool = False) -> None:
    """Initialize database tables asynchronously."""
    eng = target_async_engine or async_engine
    async with eng.begin() as conn:
        if drop_all:
            await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
