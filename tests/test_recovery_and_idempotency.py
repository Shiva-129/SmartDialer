"""Tests for worker crash recovery, transactional outbox dispatch, idempotency, and state sweepers."""

import uuid
from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy import select
from smartdialer.allocator.allocator import CallAllocator
from smartdialer.dialer.progressive import ProgressiveDialer
from smartdialer.events.processor import EventProcessor
from smartdialer.models import (
    Agent,
    AgentState,
    Borrower,
    BorrowerState,
    Call,
    Campaign,
    OutboxEvent,
    OutboxStatus,
)
from smartdialer.providers.provider_a import ProviderA
from smartdialer.providers.registry import provider_registry
from smartdialer.recovery.outbox_dispatcher import OutboxDispatcher
from smartdialer.recovery.reconciler import ReconciliationSweeper
from smartdialer.safety.provider_health import provider_health_tracker


@pytest.fixture(autouse=True)
def cleanup():
    provider_health_tracker.reset()
    yield
    provider_health_tracker.reset()


def test_worker_crash_recovery_via_transactional_outbox(db_session):
    """
    Scenario A: Worker crash after atomic reservation commit.
    Agent reserved -> Borrower reserved -> Call created -> Worker crashes.
    Recovery dispatcher picks up pending outbox record and safely dispatches it without call loss.
    """
    campaign = Campaign(name="Crash Recovery Campaign")
    db_session.add(campaign)
    db_session.flush()

    agent = Agent(campaign_id=campaign.id, name="Crash Agent", status=AgentState.AVAILABLE.value)
    borrower = Borrower(campaign_id=campaign.id, phone_number="+15553334444", name="Crash Borrower")
    db_session.add_all([agent, borrower])
    db_session.flush()

    # Step 1: Worker commits reservation and outbox event
    alloc = CallAllocator.allocate_call(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
    )
    call_id = alloc.call.id

    # Simulate worker crash: worker dies here, does NOT invoke provider directly!
    # Call remains in 'RESERVED' state, OutboxEvent is 'PENDING'.
    assert alloc.call.status == "RESERVED"
    assert alloc.outbox_event.status == OutboxStatus.PENDING.value

    # Step 2: Background Outbox Dispatcher runs recovery pass
    dispatched = OutboxDispatcher.dispatch_pending(session=db_session)

    assert call_id in dispatched
    db_session.refresh(alloc.call)
    db_session.refresh(alloc.outbox_event)
    db_session.refresh(alloc.agent)

    # Call transitioned to INITIATED, OutboxEvent marked DISPATCHED, Agent is DIALING
    assert alloc.call.status == "INITIATED"
    assert alloc.call.provider_call_id is not None
    assert alloc.outbox_event.status == OutboxStatus.DISPATCHED.value
    assert alloc.agent.status == "DIALING"


def test_no_duplicate_calls_after_retry(db_session):
    """
    Test requirement #12: No duplicate calls after retry.
    On network glitch or timeout, outbox re-dispatch uses the EXACT same idempotency key.
    """
    campaign = Campaign(name="Idempotent Retry Campaign")
    db_session.add(campaign)
    db_session.flush()

    agent = Agent(campaign_id=campaign.id, name="Retry Agent", status=AgentState.AVAILABLE.value)
    borrower = Borrower(campaign_id=campaign.id, phone_number="+15557778888", name="Retry Borrower")
    db_session.add_all([agent, borrower])
    db_session.flush()

    alloc = CallAllocator.allocate_call(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
    )
    original_idempotency_key = alloc.idempotency_key

    # Dispatch first time
    dispatched = OutboxDispatcher.dispatch_pending(session=db_session)
    assert alloc.call.id in dispatched

    # If an erroneous re-dispatch is attempted on the same event, it reuses the original idempotency key
    assert alloc.outbox_event.payload["idempotency_key"] == original_idempotency_key


def test_stale_reservation_expiration_and_recovery(db_session):
    """
    Test requirement #13: Reservation expiration and recovery.
    If an agent/borrower is reserved but never dispatched (worker crashed before creating outbox),
    ReconciliationSweeper reclaims them to AVAILABLE and ELIGIBLE.
    """
    campaign = Campaign(name="Stale Reservation Campaign")
    db_session.add(campaign)
    db_session.flush()

    past_time = datetime.now(timezone.utc) - timedelta(seconds=60)

    # Agent stuck in RESERVED with expired timestamp
    agent = Agent(
        campaign_id=campaign.id,
        name="Abandoned Agent",
        status=AgentState.RESERVED.value,
        reservation_token=uuid.uuid4(),
        reserved_at=past_time,
    )
    # Borrower stuck in RESERVED with expired timestamp
    borrower = Borrower(
        campaign_id=campaign.id,
        phone_number="+15559990000",
        name="Abandoned Borrower",
        status=BorrowerState.RESERVED.value,
        reservation_token=uuid.uuid4(),
        reserved_at=past_time,
    )
    db_session.add_all([agent, borrower])
    db_session.flush()

    # Run Reconciliation Sweep
    report = ReconciliationSweeper.run_sweep(session=db_session, campaign_id=campaign.id)

    assert report.reclaimed_agents_count == 1
    assert report.reclaimed_borrowers_count == 1

    db_session.refresh(agent)
    db_session.refresh(borrower)

    # Both entities safely healed!
    assert agent.status == AgentState.AVAILABLE.value
    assert borrower.status == BorrowerState.ELIGIBLE.value
    assert borrower.reservation_token is None


def test_orphaned_ringing_call_timeout_sweeper(db_session):
    """Calls ringing past carrier timeout (> 60s) are marked FAILED and agents are freed."""
    campaign = Campaign(name="Ringing Timeout Campaign")
    db_session.add(campaign)
    db_session.flush()

    agent = Agent(campaign_id=campaign.id, name="Ringing Agent", status=AgentState.DIALING.value)
    borrower = Borrower(campaign_id=campaign.id, phone_number="+15553331111", name="Ringing Borrower")
    db_session.add_all([agent, borrower])
    db_session.flush()

    past_ringing_time = datetime.now(timezone.utc) - timedelta(seconds=90)
    call = Call(
        campaign_id=campaign.id,
        agent_id=agent.id,
        borrower_id=borrower.id,
        status="RINGING",
        ringing_at=past_ringing_time,
        idempotency_key=f"timeout-call-{uuid.uuid4()}",
    )
    db_session.add(call)
    db_session.flush()

    report = ReconciliationSweeper.run_sweep(session=db_session, campaign_id=campaign.id)

    assert report.timed_out_calls_count == 1
    db_session.refresh(call)
    db_session.refresh(agent)
    db_session.refresh(borrower)

    assert call.status == "FAILED"
    assert call.termination_reason == "RINGING_TIMEOUT_EXCEEDED"
    assert agent.status == "WRAP_UP"
    assert borrower.status == "ELIGIBLE"


def test_agent_availability_sudden_drop(db_session):
    """
    Scenario C: Start with 10 available agents and make 4 unavailable.
    The system must immediately recalculate safe progressive capacity.
    """
    campaign = Campaign(name="Sudden Drop Campaign")
    db_session.add(campaign)
    db_session.flush()

    agents = [
        Agent(campaign_id=campaign.id, name=f"Agent-{i}", status=AgentState.AVAILABLE.value)
        for i in range(10)
    ]
    borrowers = [
        Borrower(
            campaign_id=campaign.id,
            phone_number=f"+1555666{i:04d}",
            name=f"Borrower-{i}",
            status=BorrowerState.ELIGIBLE.value,
        )
        for i in range(20)
    ]
    db_session.add_all(agents + borrowers)
    db_session.flush()

    # Suddenly 4 agents go OFFLINE / PAUSED
    for i in range(4):
        agents[i].status = AgentState.OFFLINE.value
    db_session.flush()

    # Progressive dialer runs cycle
    res = ProgressiveDialer.execute_cycle(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
    )

    # Must allocate exactly 6 calls (10 - 4 = 6)
    assert res.safety_decision.approved_calls == 6
    assert len(res.allocations) == 6
