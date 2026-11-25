"""Agent lifecycle state machine with explicit transition validation."""

import uuid
from datetime import datetime, timezone
from typing import Dict, Optional, Set
from smartdialer.core.logging import get_logger
from smartdialer.models.agent import Agent
from smartdialer.models.enums import AgentState
from smartdialer.state.exceptions import InvalidStateTransitionError

logger = get_logger("agent_state_machine")


class AgentStateMachine:
    """Enforces explicit and valid state transitions for collections agents."""

    VALID_TRANSITIONS: Dict[AgentState, Set[AgentState]] = {
        AgentState.OFFLINE: {
            AgentState.AVAILABLE,
        },
        AgentState.AVAILABLE: {
            AgentState.RESERVED,
            AgentState.CONNECTED,  # Predictive answer bridge
            AgentState.PAUSED,
            AgentState.OFFLINE,
        },
        AgentState.RESERVED: {
            AgentState.DIALING,
            AgentState.CONNECTED,  # Immediate bridge upon answer
            AgentState.AVAILABLE,  # Reservation canceled or expired
            AgentState.OFFLINE,    # Emergency logoff
            AgentState.PAUSED,
        },
        AgentState.DIALING: {
            AgentState.CONNECTED,  # Borrower answered
            AgentState.WRAP_UP,    # Unanswered / busy / failed call
            AgentState.AVAILABLE,  # Quick reset on failure
            AgentState.OFFLINE,
            AgentState.PAUSED,
        },
        AgentState.CONNECTED: {
            AgentState.WRAP_UP,    # Call ended normally
            AgentState.OFFLINE,    # Drop/disconnect
        },
        AgentState.WRAP_UP: {
            AgentState.AVAILABLE,  # Wrap-up work complete
            AgentState.PAUSED,     # Agent takes break after call
            AgentState.OFFLINE,    # Agent logs off after call
        },
        AgentState.PAUSED: {
            AgentState.AVAILABLE,  # Break finished
            AgentState.OFFLINE,    # Log off from break
        },
    }

    @classmethod
    def can_transition(cls, current_state: AgentState | str, target_state: AgentState | str) -> bool:
        """Check whether a transition from current_state to target_state is permissible."""
        curr = AgentState(current_state) if isinstance(current_state, str) else current_state
        target = AgentState(target_state) if isinstance(target_state, str) else target_state
        allowed = cls.VALID_TRANSITIONS.get(curr, set())
        return target in allowed

    @classmethod
    def transition(
        cls,
        agent: Agent,
        target_state: AgentState | str,
        current_call_id: Optional[uuid.UUID] = None,
        reservation_token: Optional[uuid.UUID] = None,
    ) -> Agent:
        """
        Validate and apply state transition to an Agent entity.
        
        Raises InvalidStateTransitionError if transition is not permitted.
        """
        current = AgentState(agent.status)
        target = AgentState(target_state) if isinstance(target_state, str) else target_state

        if not cls.can_transition(current, target):
            logger.warning(
                "Rejected agent state transition",
                agent_id=str(agent.id),
                from_state=current.value,
                to_state=target.value,
            )
            raise InvalidStateTransitionError(
                entity_type="Agent",
                current_state=current.value,
                target_state=target.value,
            )

        now = datetime.now(timezone.utc)
        agent.status = target.value
        agent.version += 1

        if target == AgentState.RESERVED:
            agent.reservation_token = reservation_token or uuid.uuid4()
            agent.reserved_at = now
        elif target == AgentState.DIALING:
            if current_call_id:
                agent.current_call_id = current_call_id
        elif target == AgentState.CONNECTED:
            if current_call_id:
                agent.current_call_id = current_call_id
        elif target in (AgentState.WRAP_UP, AgentState.AVAILABLE, AgentState.PAUSED, AgentState.OFFLINE):
            if current in (AgentState.CONNECTED, AgentState.DIALING):
                agent.last_call_ended_at = now
            if target == AgentState.WRAP_UP:
                agent.reservation_token = None
                agent.reserved_at = None
            elif target in (AgentState.AVAILABLE, AgentState.OFFLINE, AgentState.PAUSED):
                agent.reservation_token = None
                agent.reserved_at = None
                agent.current_call_id = None

        logger.info(
            "Agent transitioned state",
            agent_id=str(agent.id),
            from_state=current.value,
            to_state=target.value,
            version=agent.version,
        )
        return agent
