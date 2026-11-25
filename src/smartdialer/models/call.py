"""Call database model with state progression tracking and idempotency enforcement."""

import uuid
from datetime import datetime
from typing import Optional, List, TYPE_CHECKING
from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from smartdialer.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from smartdialer.models.enums import CallState, CALL_STATE_RANK, TerminationStatus

if TYPE_CHECKING:
    from smartdialer.models.campaign import Campaign
    from smartdialer.models.agent import Agent
    from smartdialer.models.borrower import Borrower
    from smartdialer.models.event import ProviderEvent


class Call(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Outbound call entity representing an active, completed, or failed telephonic interaction."""

    __tablename__ = "calls"

    campaign_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("campaigns.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    agent_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("agents.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    borrower_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("borrowers.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status: Mapped[str] = mapped_column(
        String(20),
        default=CallState.QUEUED.value,
        nullable=False,
        index=True,
    )
    state_rank: Mapped[int] = mapped_column(
        Integer,
        default=CALL_STATE_RANK[CallState.QUEUED],
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        nullable=False,
        index=True,
    )
    provider_name: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
    )
    provider_call_id: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        index=True,
    )
    initiated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    ringing_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    answered_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    connected_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    duration_seconds: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
    )
    termination_reason: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
    )
    disposition: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
    )
    call_notes: Mapped[Optional[str]] = mapped_column(
        String(2000),
        nullable=True,
    )
    termination_status: Mapped[str] = mapped_column(
        String(30),
        default=TerminationStatus.NOT_REQUIRED.value,
        nullable=False,
    )
    termination_attempts: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False,
    )
    next_termination_retry_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    termination_error: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
    )
    termination_confirmed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    dispatch_token: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
    )
    dialing_mode: Mapped[str] = mapped_column(
        String(20),
        default="PROGRESSIVE",
        nullable=False,
    )

    # Relationships
    campaign: Mapped["Campaign"] = relationship(
        "Campaign",
        back_populates="calls",
    )
    agent: Mapped[Optional["Agent"]] = relationship(
        "Agent",
        back_populates="calls",
        foreign_keys=[agent_id],
    )
    borrower: Mapped["Borrower"] = relationship(
        "Borrower",
        back_populates="calls",
    )
    events: Mapped[List["ProviderEvent"]] = relationship(
        "ProviderEvent",
        back_populates="call",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_calls_idempotency_key"),
        Index("ix_calls_campaign_status", "campaign_id", "status"),
        Index("ix_calls_provider_lookup", "provider_name", "provider_call_id"),
    )

    def __repr__(self) -> str:
        return f"<Call id={self.id} status='{self.status}' agent_id={self.agent_id} borrower_id={self.borrower_id}>"
