"""Regression tests for FIND-HIGH-01 & FIND-HIGH-02: Telemetry updates & predictive safety."""

import uuid
from sqlalchemy.orm import sessionmaker

from smartdialer.allocator.allocator import CallAllocator
from smartdialer.events.processor import EventProcessor
from smartdialer.models import (
    Agent,
    AgentState,
    Borrower,
    Campaign,
    CampaignMode,
)
from smartdialer.pacing.metrics import telemetry_registry
from smartdialer.safety.safety_controller import (
    DialingRequest,
    SafetyController,
    SafetyDecisionType,
)


def test_event_processor_updates_telemetry_on_call_terminal(sync_test_engine):
    """
    REGRESSION TEST for FIND-HIGH-02 (FIX 4):
    When calls complete or fail, EventProcessor MUST record the outcomes into
    CampaignTelemetry so the predictive pacing engine learns real answer rates and durations.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Telemetry Test")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Tel Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15559990001", name="Tel Borrower")
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
        cid_str = str(campaign.id)

    # Clear telemetry history for clean assertion
    telemetry = telemetry_registry.get(cid_str)
    telemetry.history.clear()
    assert len(telemetry.history) == 0

    # Simulate Call lifecycle: INITIATED -> ANSWERED -> COMPLETED
    with SessionMaker() as session:
        EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            event_type="INITIATED",
        )
        EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            event_type="ANSWERED",
        )
        EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            event_type="COMPLETED",
            raw_payload={"duration_seconds": 75.0},
        )
        session.commit()

    # Verify telemetry tracker was updated
    assert len(telemetry.history) == 1, (
        f"DEFECT: CampaignTelemetry was not updated upon call completion! History length is {len(telemetry.history)}"
    )
    answered, duration, _ = telemetry.history[0]
    assert answered is True
    assert duration == 75.0


def test_duplicate_events_do_not_double_count_telemetry(sync_test_engine):
    """
    REGRESSION TEST for FIND-HIGH-02:
    A duplicate carrier callback on a terminal call must NOT record into telemetry twice.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Telemetry Deduplication Test")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Tel Agent 2", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15559990002", name="Tel Borrower 2")
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
        cid_str = str(campaign.id)

    telemetry = telemetry_registry.get(cid_str)
    telemetry.history.clear()

    dup_event_id = f"evt-dup-{uuid.uuid4().hex[:8]}"

    with SessionMaker() as session:
        EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            event_type="INITIATED",
        )
        # First COMPLETED event
        EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=dup_event_id,
            call_id=call_id,
            event_type="COMPLETED",
            raw_payload={"duration_seconds": 45.0},
        )
        session.commit()

    assert len(telemetry.history) == 1

    # Second duplicate COMPLETED event with same event_id
    with SessionMaker() as session:
        res = EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=dup_event_id,
            call_id=call_id,
            event_type="COMPLETED",
            raw_payload={"duration_seconds": 45.0},
        )
        session.commit()

    assert res.is_duplicate is True
    # Telemetry history must still have exactly 1 record
    assert len(telemetry.history) == 1


def test_safety_controller_approves_predictive_with_connected_freeing_agents(sync_test_engine):
    """
    REGRESSION TEST for FIND-HIGH-01:
    If 0 agents are currently AVAILABLE, but 20 agents are CONNECTED in active calls,
    Safety Controller in PREDICTIVE mode must use Little's law departure projections
    to approve calls rather than rejecting with '0 available agents'.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Predictive Freeing Capacity Test", max_active_calls=100)
        session.add(campaign)
        session.flush()

        # Create 20 agents in CONNECTED status (0 in AVAILABLE)
        agents = [
            Agent(campaign_id=campaign.id, name=f"Connected-Agent-{i}", status=AgentState.CONNECTED.value)
            for i in range(20)
        ]
        session.add_all(agents)
        session.commit()
        campaign_id = campaign.id

    with SessionMaker() as session:
        req = DialingRequest(
            campaign_id=campaign_id,
            mode=CampaignMode.PREDICTIVE,
            desired_calls=2,
            estimated_answer_rate=0.20,
            provider_name="provider_a",
        )
        decision = SafetyController.evaluate(session=session, request=req)
        session.commit()

    assert decision.decision in (SafetyDecisionType.APPROVED, SafetyDecisionType.REDUCED)
    assert decision.approved_calls == 2, (
        f"FIND-HIGH-01 DEFECT: Safety Controller rejected predictive dialing with 20 connected agents! "
        f"Decision: {decision.decision.value}, Approved: {decision.approved_calls}, Reason: {decision.reasoning}"
    )
