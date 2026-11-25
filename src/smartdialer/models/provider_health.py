"""Database model for distributed provider health tracking (FIND-HIGH-03)."""

from sqlalchemy import Boolean, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from smartdialer.db.base import Base, TimestampMixin


class ProviderHealthRecord(Base, TimestampMixin):
    """
    Persistent database-backed provider health status shared across all worker processes.
    
    Guarantees cross-worker visibility of provider outages and circuit breaker trips (FIND-HIGH-03).
    """

    __tablename__ = "provider_health_status"

    provider_name: Mapped[str] = mapped_column(String(50), primary_key=True)
    is_healthy: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    failure_rate: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    total_calls: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failed_calls: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    manually_degraded: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    def __repr__(self) -> str:
        return f"<ProviderHealthRecord {self.provider_name} healthy={self.is_healthy} fail_rate={self.failure_rate:.1%}>"
