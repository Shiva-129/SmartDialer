"""Telecom provider interface abstraction and outcome data structures."""

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class ProviderInitiateResult:
    """Result returned by a telecom provider upon initiating an outbound call."""
    success: bool
    provider_call_id: Optional[str] = None
    latency_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class ProviderTerminateResult:
    """Result returned by a telecom provider upon attempting call termination."""
    success: bool
    status: str = "CONFIRMED"  # CONFIRMED, FAILED, UNKNOWN
    error: Optional[str] = None


@dataclass
class SimulatedWebhookEvent:
    """Telephony webhook callback emitted by provider to event processor."""
    event_id: str
    provider_name: str
    call_id: uuid.UUID
    event_type: str
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class TelecomProvider(ABC):
    """
    Abstract telecom carrier interface.
    
    The rest of the SmartDialer application depends ONLY on this interface.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique provider identifier (e.g. 'provider_a', 'provider_b')."""
        pass

    @abstractmethod
    def initiate_call(
        self,
        call_id: uuid.UUID,
        phone_number: str,
        idempotency_key: str,
        callback_url: Optional[str] = None,
        session: Optional[Any] = None,
    ) -> ProviderInitiateResult:
        """Place an outbound call request to the carrier network."""
        pass

    @abstractmethod
    def terminate_call(self, provider_call_id: str) -> bool:
        """Hang up or cancel an active in-flight call."""
        pass

    def query_call_by_key(self, idempotency_key: str) -> Optional[ProviderInitiateResult]:
        """Inspect carrier-side status for an idempotency key if supported by carrier."""
        return None

    @abstractmethod
    def generate_simulated_lifecycle_events(
        self,
        call_id: uuid.UUID,
        provider_call_id: str,
        outcome: str = "COMPLETED",  # COMPLETED or FAILED
        duration_seconds: float = 60.0,
    ) -> List[SimulatedWebhookEvent]:
        """
        Generate mock webhook event stream for simulation or integration testing.
        Allows providers to inject duplicate, out-of-order, or delayed events.
        """
        pass
