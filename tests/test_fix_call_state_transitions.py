"""Regression test for FIND-HIGH-05: Missing direct state transition INITIATED -> CONNECTED."""

import uuid
from sqlalchemy.orm import sessionmaker

from smartdialer.allocator.allocator import CallAllocator
from smartdialer.events.processor import EventProcessor
from smartdialer.models import Agent, AgentState, Borrower, Campaign
from smartdialer.models.call import Call
from smartdialer.models.enums import CallState
from smartdialer.state.call_state_machine import CallStateMachine


def test_call_transitions_directly_from_initiated_to_connected():
    """
    REGRESSION TEST for FIND-HIGH-05:
    When telecom carrier drops intermediate ANSWERED callback,
    CallStateMachine must accept direct transition from INITIATED to CONNECTED.
    """
    call = Call(
        campaign_id=uuid.uuid4(),
        borrower_id=uuid.uuid4(),
        status=CallState.INITIATED.value,
        state_rank=2,
    )

    res = CallStateMachine.transition(call, CallState.CONNECTED)
    assert res.success is True
    assert call.status == CallState.CONNECTED.value
    assert call.connected_at is not None
    assert call.answered_at is not None


def test_call_transitions_directly_from_ringing_to_connected():
    """
    REGRESSION TEST for FIND-HIGH-05:
    Call in RINGING status transitions directly to CONNECTED when carrier skips ANSWERED.
    """
    call = Call(
        campaign_id=uuid.uuid4(),
        borrower_id=uuid.uuid4(),
        status=CallState.RINGING.value,
        state_rank=3,
    )

    res = CallStateMachine.transition(call, CallState.CONNECTED)
    assert res.success is True
    assert call.status == CallState.CONNECTED.value


def test_event_processor_handles_initiated_to_connected_jump(sync_test_engine):
    """
    REGRESSION TEST for FIND-HIGH-05 in end-to-end EventProcessor:
    A call in INITIATED status receives CONNECTED webhook; EventProcessor must succeed
    and transition both call and agent to CONNECTED.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Direct Connect Campaign")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Bridge Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15552223333", name="Bridge Borrower")
        session.add_all([agent, borrower])
        session.flush()

        alloc = CallAllocator.allocate_call(
            session=session,
            campaign_id=campaign.id,
            provider_name="provider_a",
            agent_id=agent.id,
            borrower_id=borrower.id,
        )
        session.commit()
        call_id = alloc.call.id
        agent_id = agent.id

    with SessionMaker() as session:
        # Move call to INITIATED
        EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            event_type="INITIATED",
        )
        # Directly receive CONNECTED (ANSWERED dropped by carrier)
        res = EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            event_type="CONNECTED",
        )
        session.commit()

    assert res.success is True
    assert res.new_call_state == "CONNECTED"
    assert res.new_agent_state == "CONNECTED"

    with SessionMaker() as session:
        call = session.get(Call, call_id)
        agent = session.get(Agent, agent_id)
        assert call.status == "CONNECTED"
        assert agent.status == AgentState.CONNECTED.value
