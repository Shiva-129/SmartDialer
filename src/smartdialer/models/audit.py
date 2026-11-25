"""Audit and safety decision logs."""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TYPE_CHECKING
from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from smartdialer.db.base import Base, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from smartdialer.models.campaign import Campaign


class SafetyDecisionLog(Base, UUIDPrimaryKeyMixin):
    """Immutable audit record of every Safety Controller evaluation and pacing verdict."""

    __tablename__ = "safety_decisions"

    campaign_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("campaigns.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    dialing_mode: Mapped[str] = mapped_column(String(20), nullable=False)
    requested_calls: Mapped[int] = mapped_column(Integer, nullable=False)
    approved_calls: Mapped[int] = mapped_column(Integer, nullable=False)
    decision: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    available_agents: Mapped[int] = mapped_column(Integer, nullable=False)
    reserved_agents: Mapped[int] = mapped_column(Integer, nullable=False)
    active_calls: Mapped[int] = mapped_column(Integer, nullable=False)
    ringing_calls: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_failure_rate: Mapped[float] = mapped_column(Float, nullable=False)
    provider_healthy: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reasoning: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )

    # Relationships
    campaign: Mapped["Campaign"] = relationship(
        "Campaign",
        back_populates="safety_decisions",
    )

    def __repr__(self) -> str:
        return (
            f"<SafetyDecisionLog id={self.id} decision='{self.decision}' "
            f"requested={self.requested_calls} approved={self.approved_calls}>"
        )


class AuditLog(Base, UUIDPrimaryKeyMixin):
    """System-wide state transition audit log."""

    __tablename__ = "audit_logs"

    entity_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    entity_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    action: Mapped[str] = mapped_column(String(50), nullable=False)
    old_state: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    new_state: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    actor: Mapped[str] = mapped_column(String(100), default="system", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )

    def __repr__(self) -> str:
        return f"<AuditLog id={self.id} entity={self.entity_type}:{self.entity_id} action='{self.action}'>"
