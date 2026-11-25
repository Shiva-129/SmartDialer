"""Database dependency injection for FastAPI endpoints."""

from typing import Generator
from sqlalchemy.orm import Session

from smartdialer.db.session import SyncSessionFactory


def get_db() -> Generator[Session, None, None]:
    """
    Yield a synchronous SQLAlchemy session for FastAPI route handlers.
    Allows clean overriding in tests via app.dependency_overrides[get_db].
    """
    session = SyncSessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
