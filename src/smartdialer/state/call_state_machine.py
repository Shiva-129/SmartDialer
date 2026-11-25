"""Call lifecycle state machine with monotonic progression and out-of-order resilience."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Set
from smartdialer.core.logging import get_logger
from smartdialer.models.call import Call
from smartdialer.models.enums import (
    CALL_STATE_RANK,
    TERMINAL_CALL_STATES,
    CallState,
)
from smartdialer.state.exceptions import InvalidStateTransitionError, TerminalStateError

logger = get_logger("call_state_machine")


@dataclass
class TransitionResult:
    """Detailed outcome of a call state transition attempt."""
    success: bool
    current_state: CallState
    target_state: CallState
    is_terminal: bool = False
    is_out_of_order: bool = False
    is_duplicate: bool = False
    error: Optional[str] = None


class CallStateMachine:
    """Enforces monotonic progression and validates outbound call state transitions."""

    VALID_TRANSITIONS: Dict[CallState, Set[CallState]] = {
        CallState.QUEUED: {
            CallState.RESERVED,
            CallState.CANCELLED,
        },
        CallState.RESERVED: {
            CallState.INITIATED,
            CallState.CANCELLED,
            CallState.FAILED,
        },
        CallState.INITIATED: {
            CallState.RINGING,
            CallState.ANSWERED,   # Quick pickup before ringing webhook
            CallState.CONNECTED,  # Direct bridge callback without intermediate webhooks (FIND-HIGH-05)
            CallState.ABANDONMENT_PENDING,
            CallState.COMPLETED,  # Immediate hangup or dropped intermediate webhooks
            CallState.FAILED,
            CallState.CANCELLED,
        },
        CallState.RINGING: {
            CallState.ANSWERED,
            CallState.CONNECTED,  # Direct bridge callback skipping ANSWERED (FIND-HIGH-05)
            CallState.ABANDONMENT_PENDING,
            CallState.FAILED,     # Busy, no answer, rejected
            CallState.COMPLETED,  # Short call or immediate drop
            CallState.CANCELLED,
        },
        CallState.ANSWERED: {
            CallState.CONNECTED,  # Bridge to agent complete
            CallState.ABANDONMENT_PENDING, # Bridge timeout / no agent
            CallState.COMPLETED,  # Caller hung up before bridge
            CallState.FAILED,     # Abandoned / bridge error
        },
        CallState.CONNECTED: {
            CallState.ABANDONMENT_PENDING, # Agent lost during call teardown
            CallState.COMPLETED,  # Normal completion
            CallState.FAILED,     # Dropped mid-call
        },
        CallState.ABANDONMENT_PENDING: {
            CallState.FAILED,     # Teardown confirmed
            CallState.CANCELLED,
            CallState.COMPLETED,  # Provider late webhook
        },
        CallState.COMPLETED: set(),  # Terminal state
        CallState.FAILED: set(),     # Terminal state
        CallState.CANCELLED: set(),  # Terminal state
    }

    @classmethod
    def can_transition(cls, current_state: CallState | str, target_state: CallState | str) -> bool:
        """Check whether a direct transition is permissible."""
        curr = CallState(current_state) if isinstance(current_state, str) else current_state
        target = CallState(target_state) if isinstance(target_state, str) else target_state
        return target in cls.VALID_TRANSITIONS.get(curr, set())

    @classmethod
    def evaluate_transition(
        cls,
        current_state_val: str,
        target_state_val: str,
    ) -> TransitionResult:
        """Evaluate a requested state transition without applying side-effects."""
        current = CallState(current_state_val)
        target = CallState(target_state_val)

        # 1. Check if already in terminal state
        if current in TERMINAL_CALL_STATES:
            return TransitionResult(
                success=False,
                current_state=current,
                target_state=target,
                is_terminal=True,
                is_out_of_order=True,
                error=f"Call is in terminal state '{current.value}'; late '{target.value}' event rejected.",
            )

        # 2. Check for duplicate transition
        if current == target:
            return TransitionResult(
                success=False,
                current_state=current,
                target_state=target,
                is_duplicate=True,
                error=f"Duplicate event: call is already in state '{current.value}'.",
            )

        # 3. Check rank progression to catch out-of-order events (e.g. RINGING arriving after ANSWERED)
        curr_rank = CALL_STATE_RANK[current]
        target_rank = CALL_STATE_RANK[target]

        if target_rank < curr_rank:
            return TransitionResult(
                success=False,
                current_state=current,
                target_state=target,
                is_out_of_order=True,
                error=(
                    f"Out-of-order event rejected: target '{target.value}' (rank {target_rank}) "
                    f"cannot supersede '{current.value}' (rank {curr_rank})."
                ),
            )

        # 4. Check explicit permitted transition
        if not cls.can_transition(current, target):
            return TransitionResult(
                success=False,
                current_state=current,
                target_state=target,
                error=f"Invalid progression from '{current.value}' to '{target.value}'.",
            )

        return TransitionResult(
            success=True,
            current_state=current,
            target_state=target,
        )

    @classmethod
    def transition(
        cls,
        call: Call,
        target_state: CallState | str,
        termination_reason: Optional[str] = None,
        duration_seconds: Optional[float] = None,
    ) -> TransitionResult:
        """
        Validate and apply state transition to a Call entity.
        
        Handles out-of-order events gracefully without regressing state or raising exceptions.
        Raises InvalidStateTransitionError only for impossible forward jumps.
        """
        target = CallState(target_state) if isinstance(target_state, str) else target_state
        result = cls.evaluate_transition(call.status, target.value)

        if not result.success:
            if result.is_terminal or result.is_out_of_order or result.is_duplicate:
                logger.info(
                    "Ignored non-advancing call event",
                    call_id=str(call.id),
                    current_status=call.status,
                    event_status=target.value,
                    reason=result.error,
                )
                return result
            # Outright invalid transition
            logger.warning(
                "Rejected illegal call transition",
                call_id=str(call.id),
                current_status=call.status,
                target_status=target.value,
            )
            raise InvalidStateTransitionError(
                entity_type="Call",
                current_state=call.status,
                target_state=target.value,
                details=result.error or "",
            )

        now = datetime.now(timezone.utc)
        call.status = target.value
        call.state_rank = CALL_STATE_RANK[target]

        if target == CallState.INITIATED:
            call.initiated_at = call.initiated_at or now
        elif target == CallState.RINGING:
            call.ringing_at = call.ringing_at or now
        elif target == CallState.ANSWERED:
            call.answered_at = call.answered_at or now
        elif target == CallState.CONNECTED:
            call.answered_at = call.answered_at or now
            call.connected_at = call.connected_at or now
        elif target in TERMINAL_CALL_STATES:
            call.completed_at = now
            if termination_reason:
                call.termination_reason = termination_reason
            if duration_seconds is not None:
                call.duration_seconds = duration_seconds
            elif call.connected_at:
                call.duration_seconds = max(0.0, (now - call.connected_at).total_seconds())

        logger.info(
            "Call state updated",
            call_id=str(call.id),
            from_status=result.current_state.value,
            to_status=target.value,
            state_rank=call.state_rank,
        )
        return result
