"""Database configuration, models and session management."""

from smartdialer.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from smartdialer.db.session import (
    AsyncSessionFactory,
    SyncSessionFactory,
    async_engine,
    async_init_db,
    get_async_session,
    get_sync_session,
    init_db,
    sync_engine,
)

__all__ = [
    "Base",
    "TimestampMixin",
    "UUIDPrimaryKeyMixin",
    "sync_engine",
    "async_engine",
    "SyncSessionFactory",
    "AsyncSessionFactory",
    "get_sync_session",
    "get_async_session",
    "init_db",
    "async_init_db",
]
