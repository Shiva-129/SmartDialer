"""Database model for telecom provider request idempotency ledger."""

import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import Boolean, DateTime, Float, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from smartdialer.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class ProviderRequestLedger(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """
    Persistent idempotency ledger for telecom carrier calls (FIND-CRIT-04 / FIX 5).
    
    Guarantees that duplicate initiate_call invocations across worker restarts or
    concurrent workers return the identical provider_call_id and never trigger
    duplicate outbound phone calls.
    """

    __tablename__ = "provider_request_ledger"

    provider_name: Mapped[str] = mapped_column(String(50), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(100), nullable=False)
    call_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    phone_number: Mapped[str] = mapped_column(String(30), nullable=False)
    provider_call_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    success: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="PENDING", nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    latency_ms: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    worker_id: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        default=1,
        nullable=False,
    )
    fencing_token: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint("provider_name", "idempotency_key", name="uq_provider_idempotency_key"),
        Index("ix_provider_request_key", "provider_name", "idempotency_key"),
    )

    def __repr__(self) -> str:
        return f"<ProviderRequestLedger provider={self.provider_name} key={self.idempotency_key} prov_call_id={self.provider_call_id}>"
