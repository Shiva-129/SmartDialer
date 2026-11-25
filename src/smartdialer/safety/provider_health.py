"""Provider health monitor with database-backed distributed persistence (FIND-HIGH-03)."""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque, Dict, Optional, Tuple
from sqlalchemy.orm import Session

from smartdialer.models.provider_health import ProviderHealthRecord


@dataclass
class ProviderMetrics:
    """Rolling window metrics for a telecom provider."""
    window_size: int = 50
    events: Deque[Tuple[bool, float, datetime]] = field(default_factory=lambda: deque(maxlen=50))
    failure_threshold: float = 0.20  # Trip threshold at 20% failure rate
    min_samples: int = 5
    manually_degraded: bool = False

    def record_outcome(self, success: bool, latency_ms: float = 0.0) -> None:
        """Record an outcome (success/failure) with latency."""
        self.events.append((success, latency_ms, datetime.now(timezone.utc)))

    @property
    def total_samples(self) -> int:
        return len(self.events)

    @property
    def failure_rate(self) -> float:
        """Calculate recent failure rate in rolling window."""
        if not self.events:
            return 0.0
        failures = sum(1 for success, _, _ in self.events if not success)
        return failures / len(self.events)

    @property
    def average_latency_ms(self) -> float:
        """Calculate average latency of recent calls."""
        if not self.events:
            return 0.0
        return sum(lat for _, lat, _ in self.events) / len(self.events)

    @property
    def is_healthy(self) -> bool:
        """Check if provider is operating safely below failure threshold."""
        if self.manually_degraded:
            return False
        if len(self.events) < self.min_samples:
            return True
        return self.failure_rate < self.failure_threshold


class ProviderHealthTracker:
    """
    Distributed telecom provider health tracker.
    
    Combines in-memory rolling windows for fast per-worker metrics with
    database-backed persistent state in `provider_health_status` table
    to ensure cluster-wide visibility of carrier degradation (FIND-HIGH-03).
    """

    def __init__(self) -> None:
        self._providers: Dict[str, ProviderMetrics] = {
            "provider_a": ProviderMetrics(),
            "provider_b": ProviderMetrics(),
        }

    def record(
        self,
        provider_name: str,
        success: bool,
        latency_ms: float = 0.0,
        session: Optional[Session] = None,
    ) -> None:
        """Record provider outcome in local window and database."""
        if provider_name not in self._providers:
            self._providers[provider_name] = ProviderMetrics()
        metrics = self._providers[provider_name]
        metrics.record_outcome(success, latency_ms)

        if session is not None:
            try:
                rec = session.get(ProviderHealthRecord, provider_name)
                if not rec:
                    rec = ProviderHealthRecord(
                        provider_name=provider_name,
                        is_healthy=metrics.is_healthy,
                        failure_rate=metrics.failure_rate,
                        total_calls=1,
                        failed_calls=0 if success else 1,
                        manually_degraded=metrics.manually_degraded,
                    )
                    session.add(rec)
                else:
                    rec.total_calls += 1
                    if not success:
                        rec.failed_calls += 1
                    rec.failure_rate = metrics.failure_rate
                    rec.is_healthy = metrics.is_healthy and not rec.manually_degraded
                session.flush()
            except Exception as exc:
                try:
                    session.rollback()
                except Exception:
                    pass
                logger.warning("Failed to record provider health to database", provider=provider_name, error=str(exc))

    def is_healthy(self, provider_name: str, session: Optional[Session] = None) -> bool:
        """
        Query if provider is healthy.
        Checks both database status and local metrics.
        """
        # 1. Query database record if session is available
        if session is not None:
            try:
                rec = session.get(ProviderHealthRecord, provider_name)
                if rec:
                    if rec.manually_degraded or not rec.is_healthy:
                        return False
            except Exception:
                try:
                    session.rollback()
                except Exception:
                    pass

        # 2. Check in-memory local state
        if provider_name not in self._providers:
            return True
        return self._providers[provider_name].is_healthy

    def get_failure_rate(self, provider_name: str, session: Optional[Session] = None) -> float:
        """Retrieve failure rate from DB or local window."""
        if session is not None:
            try:
                rec = session.get(ProviderHealthRecord, provider_name)
                if rec:
                    return rec.failure_rate
            except Exception:
                try:
                    session.rollback()
                except Exception:
                    pass
        if provider_name not in self._providers:
            return 0.0
        return self._providers[provider_name].failure_rate

    def set_manual_status(
        self,
        provider_name: str,
        degraded: bool,
        session: Optional[Session] = None,
    ) -> None:
        """Manually trip or reset provider health status across all workers."""
        if not degraded:
            self._providers[provider_name] = ProviderMetrics()
        else:
            if provider_name not in self._providers:
                self._providers[provider_name] = ProviderMetrics()
            self._providers[provider_name].manually_degraded = True

        if session is not None:
            try:
                rec = session.get(ProviderHealthRecord, provider_name)
                if not rec:
                    rec = ProviderHealthRecord(
                        provider_name=provider_name,
                        is_healthy=not degraded,
                        failure_rate=1.0 if degraded else 0.0,
                        total_calls=0,
                        failed_calls=0,
                        manually_degraded=degraded,
                    )
                    session.add(rec)
                else:
                    rec.manually_degraded = degraded
                    rec.is_healthy = not degraded
                    if not degraded:
                        rec.failure_rate = 0.0
                session.flush()
            except Exception as exc:
                try:
                    session.rollback()
                except Exception:
                    pass
                logger.warning("Failed to set manual provider status in database", provider=provider_name, error=str(exc))

    def reset(self, provider_name: Optional[str] = None, session: Optional[Session] = None) -> None:
        """Reset metrics in local memory and database."""
        if provider_name and provider_name in self._providers:
            self._providers[provider_name] = ProviderMetrics()
        else:
            self._providers = {
                "provider_a": ProviderMetrics(),
                "provider_b": ProviderMetrics(),
            }

        if session is not None:
            try:
                if provider_name:
                    rec = session.get(ProviderHealthRecord, provider_name)
                    if rec:
                        rec.is_healthy = True
                        rec.failure_rate = 0.0
                        rec.manually_degraded = False
                else:
                    for p in ("provider_a", "provider_b"):
                        rec = session.get(ProviderHealthRecord, p)
                        if rec:
                            rec.is_healthy = True
                            rec.failure_rate = 0.0
                            rec.manually_degraded = False
                session.flush()
            except Exception:
                pass


# Global singleton instance
provider_health_tracker = ProviderHealthTracker()
