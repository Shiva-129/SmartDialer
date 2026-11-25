"""Campaign database model."""

from typing import List, TYPE_CHECKING
from sqlalchemy import Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from smartdialer.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from smartdialer.models.enums import CampaignMode

if TYPE_CHECKING:
    from smartdialer.models.agent import Agent
    from smartdialer.models.borrower import Borrower
    from smartdialer.models.call import Call
    from smartdialer.models.audit import SafetyDecisionLog


class Campaign(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Campaign configuration and state."""

    __tablename__ = "campaigns"

    name: Mapped[str] = mapped_column(String(100), nullable=False)
    mode: Mapped[str] = mapped_column(
        String(20),
        default=CampaignMode.PROGRESSIVE.value,
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(20),
        default="ACTIVE",
        nullable=False,
    )
    target_abandonment_rate: Mapped[float] = mapped_column(
        Float,
        default=0.03,
        nullable=False,
    )
    max_active_calls: Mapped[int] = mapped_column(
        Integer,
        default=100,
        nullable=False,
    )

    # Relationships
    agents: Mapped[List["Agent"]] = relationship(
        "Agent",
        back_populates="campaign",
        cascade="all, delete-orphan",
    )
    borrowers: Mapped[List["Borrower"]] = relationship(
        "Borrower",
        back_populates="campaign",
        cascade="all, delete-orphan",
    )
    calls: Mapped[List["Call"]] = relationship(
        "Call",
        back_populates="campaign",
        cascade="all, delete-orphan",
    )
    safety_decisions: Mapped[List["SafetyDecisionLog"]] = relationship(
        "SafetyDecisionLog",
        back_populates="campaign",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Campaign id={self.id} name='{self.name}' mode='{self.mode}' status='{self.status}'>"
