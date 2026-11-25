"""Transactional Outbox model to ensure atomic persistence and guaranteed dispatch."""

import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from sqlalchemy import DateTime, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from smartdialer.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from smartdialer.models.enums import OutboxStatus


class OutboxEvent(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Transactional Outbox record guaranteeing at-least-once message delivery to providers."""

    __tablename__ = "outbox_events"

    aggregate_type: Mapped[str] = mapped_column(String(50), nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    payload: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20),
        default=OutboxStatus.PENDING.value,
        nullable=False,
        index=True,
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_error: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    locked_until: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    worker_id: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        index=True,
    )
    fencing_token: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
        index=True,
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        Index("ix_outbox_status_created", "status", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<OutboxEvent id={self.id} agg={self.aggregate_type}:{self.aggregate_id} status='{self.status}'>"
