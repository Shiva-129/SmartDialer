"""Pytest configuration and database fixtures for PostgreSQL."""

import asyncio
from typing import AsyncGenerator, Generator
import pytest
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


@pytest.fixture(scope="session")
def sync_test_engine():
    """Create synchronous engine connected to PostgreSQL test database."""
    engine = create_engine(
        settings.get_test_sync_url(),
        pool_pre_ping=True,
    )
    # Ensure fresh schema
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def async_test_engine():
    """Create asynchronous engine connected to PostgreSQL test database."""
    engine = create_async_engine(
        settings.get_test_async_url(),
        pool_pre_ping=True,
    )
    yield engine


@pytest.fixture
def db_session(sync_test_engine) -> Generator[Session, None, None]:
    """Yield a synchronous database session with isolated transaction rollback."""
    connection = sync_test_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection, expire_on_commit=False)

    yield session

    session.close()
    if transaction.is_active:
        transaction.rollback()
    connection.close()


@pytest.fixture
async def async_db_session(async_test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Yield an asynchronous database session with isolated transaction rollback."""
    connection = await async_test_engine.connect()
    transaction = await connection.begin()
    session = AsyncSession(bind=connection, expire_on_commit=False)

    yield session

    await session.close()
    if transaction.is_active:
        await transaction.rollback()
    await connection.close()
