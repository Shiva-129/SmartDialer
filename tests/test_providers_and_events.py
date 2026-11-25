"""Tests for Mock Telecom Providers, Event Processor deduplication, and out-of-order event resilience."""

import uuid
import pytest
from sqlalchemy import select
from smartdialer.allocator.allocator import CallAllocator
from smartdialer.events.processor import EventProcessor
from smartdialer.models import (
    Agent,
    AgentState,
    Borrower,
    BorrowerState,
    Call,
    Campaign,
    ProviderEvent,
)
from smartdialer.providers.provider_a import ProviderA
from smartdialer.providers.provider_b import ProviderB
from smartdialer.safety.provider_health import provider_health_tracker


@pytest.fixture(autouse=True)
def cleanup():
    provider_health_tracker.reset()
    yield
    provider_health_tracker.reset()


def test_provider_a_normal_initiate_and_lifecycle():
    """Verify Provider A is fast, reliable, and generates ordered lifecycle events."""
    prov = ProviderA(failure_rate=0.0)
    assert prov.name == "provider_a"

    call_id = uuid.uuid4()
    init_res = prov.initiate_call(
        call_id=call_id,
        phone_number="+15551234567",
        idempotency_key=f"idemp-{uuid.uuid4()}",
    )
    assert init_res.success is True
    assert init_res.provider_call_id is not None
    assert init_res.latency_ms > 0

    events = prov.generate_simulated_lifecycle_events(
        call_id=call_id,
        provider_call_id=init_res.provider_call_id,
        outcome="COMPLETED",
    )
    event_types = [e.event_type for e in events]
    assert event_types == ["INITIATED", "RINGING", "ANSWERED", "CONNECTED", "COMPLETED"]


def test_provider_b_chaos_and_reordering():
    """Verify Provider B can inject duplicates and out-of-order events."""
    prov = ProviderB(failure_rate=0.0, duplicate_probability=1.0, reorder_probability=1.0)
    assert prov.name == "provider_b"

    call_id = uuid.uuid4()
    init_res = prov.initiate_call(
        call_id=call_id,
        phone_number="+15559876543",
        idempotency_key=f"idemp-{uuid.uuid4()}",
    )
    assert init_res.success is True

    events = prov.generate_simulated_lifecycle_events(
        call_id=call_id,
        provider_call_id=init_res.provider_call_id,
        outcome="COMPLETED",
    )
    event_ids = [e.event_id for e in events]
    # With duplicate_probability = 1.0, there must be at least one duplicate event_id
    assert len(event_ids) > len(set(event_ids))


def test_event_processor_duplicate_event_handling(db_session):
    """
    Test requirement #5: Duplicate provider events.
    Send the exact same provider event multiple times.
    Only ONE state transition occurs; subsequent events are acknowledged idempotently.
    """
    campaign = Campaign(name="Duplicate Event Campaign")
    db_session.add(campaign)
    db_session.flush()

    agent = Agent(campaign_id=campaign.id, name="A1", status=AgentState.AVAILABLE.value)
    borrower = Borrower(campaign_id=campaign.id, phone_number="+15551112233", name="B1")
    db_session.add_all([agent, borrower])
    db_session.flush()

    alloc = CallAllocator.allocate_call(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_b",
    )
    call_id = alloc.call.id
    event_id = f"evt-dup-{uuid.uuid4().hex[:8]}"

    # Send INITIATED event first time
    res1 = EventProcessor.process_event(
        session=db_session,
        provider_name="provider_b",
        event_id=event_id,
        call_id=call_id,
        event_type="INITIATED",
        raw_payload={"status": "initiated"},
    )
    assert res1.success is True
    assert res1.is_duplicate is False
    assert res1.new_call_state == "INITIATED"

    # Send identical event second time
    res2 = EventProcessor.process_event(
        session=db_session,
        provider_name="provider_b",
        event_id=event_id,
        call_id=call_id,
        event_type="INITIATED",
        raw_payload={"status": "initiated", "retry": 1},
    )
    assert res2.success is True
    assert res2.is_duplicate is True  # Detected by DB constraint!

    # Send identical event third time
    res3 = EventProcessor.process_event(
        session=db_session,
        provider_name="provider_b",
        event_id=event_id,
        call_id=call_id,
        event_type="INITIATED",
    )
    assert res3.success is True
    assert res3.is_duplicate is True


def test_event_processor_out_of_order_event_handling(db_session):
    """
    Test requirement #6: Out-of-order events.
    Send COMPLETED, then late ANSWERED, then late RINGING.
    The system must remain consistent and must not regress a terminal call.
    """
    campaign = Campaign(name="Out-of-Order Campaign")
    db_session.add(campaign)
    db_session.flush()

    agent = Agent(campaign_id=campaign.id, name="A1", status=AgentState.AVAILABLE.value)
    borrower = Borrower(campaign_id=campaign.id, phone_number="+15551112233", name="B1")
    db_session.add_all([agent, borrower])
    db_session.flush()

    alloc = CallAllocator.allocate_call(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_b",
    )
    call_id = alloc.call.id

    # 1. Step: Call initiates normally
    EventProcessor.process_event(
        session=db_session,
        provider_name="provider_b",
        event_id=f"evt-{uuid.uuid4().hex[:8]}",
        call_id=call_id,
        event_type="INITIATED",
    )

    # 2. Out-of-order jump: COMPLETED arrives before ANSWERED
    res_completed = EventProcessor.process_event(
        session=db_session,
        provider_name="provider_b",
        event_id=f"evt-{uuid.uuid4().hex[:8]}",
        call_id=call_id,
        event_type="COMPLETED",
        raw_payload={"duration_seconds": 45.0},
    )
    assert res_completed.success is True
    assert res_completed.new_call_state == "COMPLETED"

    # 3. Late delayed ANSWERED arrives
    res_late_answered = EventProcessor.process_event(
        session=db_session,
        provider_name="provider_b",
        event_id=f"evt-{uuid.uuid4().hex[:8]}",
        call_id=call_id,
        event_type="ANSWERED",
    )
    assert res_late_answered.success is True
    assert res_late_answered.is_out_of_order is True

    # 4. Late delayed RINGING arrives
    res_late_ringing = EventProcessor.process_event(
        session=db_session,
        provider_name="provider_b",
        event_id=f"evt-{uuid.uuid4().hex[:8]}",
        call_id=call_id,
        event_type="RINGING",
    )
    assert res_late_ringing.success is True
    assert res_late_ringing.is_out_of_order is True

    # Critical Assertion: Call state in DB remains COMPLETED! Never regressed!
    call_in_db = db_session.get(Call, call_id)
    assert call_in_db.status == "COMPLETED"
    assert call_in_db.state_rank == 6


def test_full_call_and_agent_lifecycle_reconciliation(db_session):
    """
    Verify complete event stream synchronizes Call, Agent, and Borrower entities.
    """
    campaign = Campaign(name="Lifecycle Reconciliation Campaign")
    db_session.add(campaign)
    db_session.flush()

    agent = Agent(campaign_id=campaign.id, name="A1", status=AgentState.AVAILABLE.value)
    borrower = Borrower(campaign_id=campaign.id, phone_number="+15551112233", name="B1")
    db_session.add_all([agent, borrower])
    db_session.flush()

    alloc = CallAllocator.allocate_call(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
    )
    call_id = alloc.call.id
    agent_id = alloc.agent.id
    borrower_id = alloc.borrower.id

    assert alloc.agent.status == "RESERVED"

    # INITIATED -> Agent is DIALING
    EventProcessor.process_event(
        session=db_session,
        provider_name="provider_a",
        event_id=f"e-{uuid.uuid4().hex[:8]}",
        call_id=call_id,
        event_type="INITIATED",
    )
    db_session.refresh(alloc.agent)
    assert alloc.agent.status == "DIALING"

    # ANSWERED -> Agent is CONNECTED
    EventProcessor.process_event(
        session=db_session,
        provider_name="provider_a",
        event_id=f"e-{uuid.uuid4().hex[:8]}",
        call_id=call_id,
        event_type="ANSWERED",
    )
    db_session.refresh(alloc.agent)
    assert alloc.agent.status == "CONNECTED"

    # COMPLETED -> Agent is WRAP_UP, Borrower is COMPLETED
    EventProcessor.process_event(
        session=db_session,
        provider_name="provider_a",
        event_id=f"e-{uuid.uuid4().hex[:8]}",
        call_id=call_id,
        event_type="COMPLETED",
        raw_payload={"duration_seconds": 120.0},
    )
    db_session.refresh(alloc.agent)
    db_session.refresh(alloc.borrower)
    db_session.refresh(alloc.call)

    assert alloc.call.status == "COMPLETED"
    assert alloc.agent.status == "WRAP_UP"
    assert alloc.borrower.status == "COMPLETED"
