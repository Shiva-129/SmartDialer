"""Borrower database model for dialer allocation and compliance."""

import uuid
from datetime import datetime
from typing import Optional, TYPE_CHECKING
from sqlalchemy import DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from smartdialer.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from smartdialer.models.enums import BorrowerState

if TYPE_CHECKING:
    from smartdialer.models.campaign import Campaign
    from smartdialer.models.call import Call


class Borrower(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Borrower record to be contacted during collections campaign."""

    __tablename__ = "borrowers"

    campaign_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("campaigns.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    phone_number: Mapped[str] = mapped_column(String(30), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20),
        default=BorrowerState.ELIGIBLE.value,
        nullable=False,
        index=True,
    )
    priority: Mapped[int] = mapped_column(Integer, default=0, nullable=False, index=True)
    reservation_token: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
        index=True,
    )
    reserved_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    call_attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_called_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    campaign: Mapped["Campaign"] = relationship(
        "Campaign",
        back_populates="borrowers",
    )
    calls: Mapped[list["Call"]] = relationship(
        "Call",
        back_populates="borrower",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_borrowers_campaign_status_priority", "campaign_id", "status", "priority"),
        Index("ix_borrowers_reserved_at", "status", "reserved_at"),
        Index("ix_borrowers_campaign_phone", "campaign_id", "phone_number"),
    )

    def __repr__(self) -> str:
        return f"<Borrower id={self.id} phone='{self.phone_number}' status='{self.status}' priority={self.priority}>"
