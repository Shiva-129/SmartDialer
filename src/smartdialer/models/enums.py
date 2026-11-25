"""Core domain enumerations for Agent, Call, Borrower, and System States."""

from enum import Enum


class AgentState(str, Enum):
    """Lifecycle states of a collections agent."""
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


class CallState(str, Enum):
    """Lifecycle states of an outbound call."""
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    ABANDONMENT_PENDING = "ABANDONMENT_PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# Monotonic progression rank for CallState to detect and reject out-of-order events
CALL_STATE_RANK = {
    CallState.QUEUED: 0,
    CallState.RESERVED: 1,
    CallState.INITIATED: 2,
    CallState.RINGING: 3,
    CallState.ANSWERED: 4,
    CallState.ABANDONMENT_PENDING: 5,
    CallState.CONNECTED: 5,
    CallState.COMPLETED: 6,
    CallState.FAILED: 6,
    CallState.CANCELLED: 6,
}

TERMINAL_CALL_STATES = {
    CallState.COMPLETED,
    CallState.FAILED,
    CallState.CANCELLED,
}


class TerminationStatus(str, Enum):
    """Durable status of carrier call termination requests."""
    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    CONFIRMED = "CONFIRMED"
    UNKNOWN = "UNKNOWN"
    FAILED = "FAILED"


class BorrowerState(str, Enum):
    """Lifecycle states of a borrower record within a campaign."""
    ELIGIBLE = "ELIGIBLE"
    RESERVED = "RESERVED"
    IN_CALL = "IN_CALL"
    COMPLETED = "COMPLETED"
    DO_NOT_CALL = "DO_NOT_CALL"
    FAILED = "FAILED"


class CampaignMode(str, Enum):
    """Supported dialing modes."""
    PROGRESSIVE = "PROGRESSIVE"
    PREDICTIVE = "PREDICTIVE"


class SafetyDecisionType(str, Enum):
    """Decisions rendered by the independent Safety Controller."""
    APPROVED = "APPROVED"
    REDUCED = "REDUCED"
    REJECTED = "REJECTED"
    FALLBACK_TO_PROGRESSIVE = "FALLBACK_TO_PROGRESSIVE"


class OutboxStatus(str, Enum):
    """Status of transactional outbox records."""
    PENDING = "PENDING"
    DISPATCHED = "DISPATCHED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
