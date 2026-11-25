"""Domain exceptions for SmartDialer state transitions and allocation."""

class SmartDialerError(Exception):
    """Base exception for all domain errors."""
    pass


class InvalidStateTransitionError(SmartDialerError):
    """Raised when an illegal state transition is attempted."""
    def __init__(self, entity_type: str, current_state: str, target_state: str, details: str = ""):
        self.entity_type = entity_type
        self.current_state = current_state
        self.target_state = target_state
        self.details = details
        message = (
            f"Invalid {entity_type} state transition: "
            f"cannot move from '{current_state}' to '{target_state}'."
        )
        if details:
            message += f" Details: {details}"
        super().__init__(message)


class TerminalStateError(SmartDialerError):
    """Raised when attempting to mutate an entity in an immutable terminal state."""
    pass


class AllocationError(SmartDialerError):
    """Base exception for resource allocation failures."""
    pass


class AgentNotAvailableError(AllocationError):
    """Raised when no suitable agent is available for reservation."""
    pass


class BorrowerNotEligibleError(AllocationError):
    """Raised when no eligible borrower is available for dialing."""
    pass


class ConcurrencyConflictError(AllocationError):
    """Raised when an optimistic locking version conflict occurs."""
    pass
