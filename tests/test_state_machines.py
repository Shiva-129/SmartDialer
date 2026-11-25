"""Tests for Agent and Call state machines, valid transitions, and out-of-order handling."""

import uuid
import pytest
from smartdialer.models.agent import Agent
from smartdialer.models.call import Call
from smartdialer.models.campaign import Campaign
from smartdialer.models.enums import (
    CALL_STATE_RANK,
    AgentState,
    CallState,
)
from smartdialer.state.agent_state_machine import AgentStateMachine
from smartdialer.state.call_state_machine import CallStateMachine
from smartdialer.state.exceptions import (
    InvalidStateTransitionError,
)


def test_agent_valid_lifecycle():
    """Verify happy-path agent state progression."""
    agent = Agent(
        campaign_id=uuid.uuid4(),
        name="Test Agent",
        status=AgentState.OFFLINE.value,
        version=1,
    )

    # OFFLINE -> AVAILABLE
    AgentStateMachine.transition(agent, AgentState.AVAILABLE)
    assert agent.status == AgentState.AVAILABLE.value
    assert agent.version == 2

    # AVAILABLE -> RESERVED
    token = uuid.uuid4()
    AgentStateMachine.transition(agent, AgentState.RESERVED, reservation_token=token)
    assert agent.status == AgentState.RESERVED.value
    assert agent.reservation_token == token
    assert agent.reserved_at is not None
    assert agent.version == 3

    # RESERVED -> DIALING
    call_id = uuid.uuid4()
    AgentStateMachine.transition(agent, AgentState.DIALING, current_call_id=call_id)
    assert agent.status == AgentState.DIALING.value
    assert agent.current_call_id == call_id
    assert agent.version == 4

    # DIALING -> CONNECTED
    AgentStateMachine.transition(agent, AgentState.CONNECTED)
    assert agent.status == AgentState.CONNECTED.value
    assert agent.version == 5

    # CONNECTED -> WRAP_UP
    AgentStateMachine.transition(agent, AgentState.WRAP_UP)
    assert agent.status == AgentState.WRAP_UP.value
    assert agent.last_call_ended_at is not None
    assert agent.version == 6

    # WRAP_UP -> AVAILABLE
    AgentStateMachine.transition(agent, AgentState.AVAILABLE)
    assert agent.status == AgentState.AVAILABLE.value
    assert agent.reservation_token is None
    assert agent.current_call_id is None
    assert agent.version == 7


def test_agent_invalid_transitions():
    """Verify that illegal agent state jumps raise InvalidStateTransitionError."""
    agent = Agent(
        campaign_id=uuid.uuid4(),
        name="Test Agent",
        status=AgentState.OFFLINE.value,
        version=1,
    )

    # Cannot jump directly from OFFLINE to CONNECTED
    with pytest.raises(InvalidStateTransitionError):
        AgentStateMachine.transition(agent, AgentState.CONNECTED)

    # Cannot jump from AVAILABLE to WRAP_UP
    AgentStateMachine.transition(agent, AgentState.AVAILABLE)
    with pytest.raises(InvalidStateTransitionError):
        AgentStateMachine.transition(agent, AgentState.WRAP_UP)


def test_call_valid_lifecycle():
    """Verify normal progression of an outbound call."""
    call = Call(
        campaign_id=uuid.uuid4(),
        borrower_id=uuid.uuid4(),
        idempotency_key=f"call-{uuid.uuid4()}",
        status=CallState.QUEUED.value,
        state_rank=CALL_STATE_RANK[CallState.QUEUED],
    )

    res = CallStateMachine.transition(call, CallState.RESERVED)
    assert res.success is True
    assert call.status == CallState.RESERVED.value

    res = CallStateMachine.transition(call, CallState.INITIATED)
    assert res.success is True
    assert call.initiated_at is not None

    res = CallStateMachine.transition(call, CallState.RINGING)
    assert res.success is True
    assert call.ringing_at is not None

    res = CallStateMachine.transition(call, CallState.ANSWERED)
    assert res.success is True
    assert call.answered_at is not None

    res = CallStateMachine.transition(call, CallState.CONNECTED)
    assert res.success is True
    assert call.connected_at is not None

    res = CallStateMachine.transition(call, CallState.COMPLETED)
    assert res.success is True
    assert call.completed_at is not None
    assert call.status == CallState.COMPLETED.value


def test_call_terminal_state_cannot_regress():
    """An ANSWERED or RINGING event must NEVER regress a call already marked COMPLETED."""
    call = Call(
        campaign_id=uuid.uuid4(),
        borrower_id=uuid.uuid4(),
        idempotency_key=f"call-{uuid.uuid4()}",
        status=CallState.COMPLETED.value,
        state_rank=CALL_STATE_RANK[CallState.COMPLETED],
    )

    # Late ANSWERED arriving after COMPLETED
    res = CallStateMachine.transition(call, CallState.ANSWERED)
    assert res.success is False
    assert res.is_terminal is True
    assert res.is_out_of_order is True
    assert call.status == CallState.COMPLETED.value  # State remains untouched!

    # Late RINGING arriving after COMPLETED
    res = CallStateMachine.transition(call, CallState.RINGING)
    assert res.success is False
    assert res.is_terminal is True
    assert call.status == CallState.COMPLETED.value


def test_call_out_of_order_non_terminal_regression():
    """A late RINGING event arriving after ANSWERED must not regress the call state."""
    call = Call(
        campaign_id=uuid.uuid4(),
        borrower_id=uuid.uuid4(),
        idempotency_key=f"call-{uuid.uuid4()}",
        status=CallState.ANSWERED.value,
        state_rank=CALL_STATE_RANK[CallState.ANSWERED],
    )

    # Late RINGING arriving after ANSWERED
    res = CallStateMachine.transition(call, CallState.RINGING)
    assert res.success is False
    assert res.is_out_of_order is True
    assert call.status == CallState.ANSWERED.value  # Did not regress!


def test_call_duplicate_events():
    """Identical duplicate state updates are recognized and skipped idempotently."""
    call = Call(
        campaign_id=uuid.uuid4(),
        borrower_id=uuid.uuid4(),
        idempotency_key=f"call-{uuid.uuid4()}",
        status=CallState.RINGING.value,
        state_rank=CALL_STATE_RANK[CallState.RINGING],
    )

    # Second RINGING event
    res = CallStateMachine.transition(call, CallState.RINGING)
    assert res.success is False
    assert res.is_duplicate is True
    assert call.status == CallState.RINGING.value
