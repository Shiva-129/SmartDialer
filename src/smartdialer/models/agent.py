"""Agent database model with atomic concurrency guarantees."""

import uuid
from datetime import datetime
from typing import Optional, TYPE_CHECKING
from sqlalchemy import DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from smartdialer.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from smartdialer.models.enums import AgentState

if TYPE_CHECKING:
    from smartdialer.models.campaign import Campaign
    from smartdialer.models.call import Call


class Agent(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Collections agent representation with lifecycle tracking and concurrency versioning."""

    __tablename__ = "agents"

    campaign_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("campaigns.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20),
        default=AgentState.OFFLINE.value,
        nullable=False,
        index=True,
    )
    reservation_token: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
        index=True,
    )
    reserved_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    current_call_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
    )
    version: Mapped[int] = mapped_column(
        Integer,
        default=1,
        nullable=False,
    )
    last_call_ended_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    dispatch_token: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
    )

    # Relationships
    campaign: Mapped["Campaign"] = relationship(
        "Campaign",
        back_populates="agents",
    )
    calls: Mapped[list["Call"]] = relationship(
        "Call",
        back_populates="agent",
        foreign_keys="[Call.agent_id]",
    )

    __table_args__ = (
        Index("ix_agents_campaign_status", "campaign_id", "status"),
        Index("ix_agents_reserved_at", "status", "reserved_at"),
    )

    __mapper_args__ = {
        "version_id_col": version,
    }

    def __repr__(self) -> str:
        return f"<Agent id={self.id} name='{self.name}' status='{self.status}' version={self.version}>"
