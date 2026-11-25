"""Provider event store and deduplication log."""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TYPE_CHECKING
from sqlalchemy import Boolean, DateTime, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from smartdialer.db.base import Base, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from smartdialer.models.call import Call


class ProviderEvent(Base, UUIDPrimaryKeyMixin):
    """Raw telecom provider event log with strict unique constraint for deduplication."""

    __tablename__ = "provider_events"

    provider_name: Mapped[str] = mapped_column(String(50), nullable=False)
    event_id: Mapped[str] = mapped_column(String(100), nullable=False)
    call_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("calls.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    raw_payload: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    processed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    processed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    error_message: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Relationships
    call: Mapped[Optional["Call"]] = relationship(
        "Call",
        back_populates="events",
    )

    __table_args__ = (
        UniqueConstraint("provider_name", "event_id", name="uq_provider_name_event_id"),
        Index("ix_provider_event_call_type", "call_id", "event_type"),
    )

    def __repr__(self) -> str:
        return f"<ProviderEvent id={self.id} provider='{self.provider_name}' event_id='{self.event_id}' type='{self.event_type}'>"
