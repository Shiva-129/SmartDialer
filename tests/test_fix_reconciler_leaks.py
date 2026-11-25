"""Regression tests for FIX 1 and FIND-CRIT-01 / FIND-MED-05: Agent leak during worker crash recovery."""

import uuid
from datetime import datetime, timedelta, timezone
import pytest
from smartdialer.allocator.allocator import CallAllocator
from smartdialer.models import (
    Agent,
    AgentState,
    Borrower,
    BorrowerState,
    Call,
    Campaign,
)
from smartdialer.recovery.reconciler import ReconciliationSweeper


def test_reconciler_reclaims_stale_reserved_call_and_agent(db_session):
    """
    CRITICAL REGRESSION TEST for FIND-CRIT-01:
    When a worker crashes immediately after allocate_call, the Call remains in RESERVED.
    The reconciler must time out the Call, reclaim the Agent to AVAILABLE, and the Borrower to ELIGIBLE.
    In the unpatched code, the agent is permanently leaked in RESERVED status!
    """
    campaign = Campaign(name="Crash Leak Test Campaign")
    db_session.add(campaign)
    db_session.flush()

    agent = Agent(campaign_id=campaign.id, name="Crash Agent", status=AgentState.AVAILABLE.value)
    borrower = Borrower(campaign_id=campaign.id, phone_number="+15551239999", name="Borrower Leak")
    db_session.add_all([agent, borrower])
    db_session.flush()

    # Step 1: Allocate call (Call=RESERVED, Agent=RESERVED, Borrower=RESERVED)
    alloc = CallAllocator.allocate_call(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
    )
    call_id = alloc.call.id
    agent_id = alloc.agent.id
    borrower_id = alloc.borrower.id

    # Step 2: Worker crashes! Simulate time passing beyond reservation timeout (40s ago)
    expired_time = datetime.now(timezone.utc) - timedelta(seconds=45)
    alloc.call.created_at = expired_time
    alloc.agent.reserved_at = expired_time
    alloc.borrower.reserved_at = expired_time
    db_session.flush()

    # Step 3: Run Reconciliation Sweep
    report = ReconciliationSweeper.run_sweep(session=db_session, campaign_id=campaign.id)

    # Step 4: Verify that the agent was reclaimed and NOT permanently leaked
    db_session.refresh(alloc.agent)
    db_session.refresh(alloc.call)
    db_session.refresh(alloc.borrower)

    assert alloc.call.status == "FAILED", f"Expected call to be FAILED, got {alloc.call.status}"
    assert alloc.call.termination_reason == "RESERVATION_TIMEOUT_EXPIRED"
    assert alloc.agent.status == AgentState.AVAILABLE.value, (
        f"CRITICAL DEFECT: Agent remained in {alloc.agent.status} (permanently leaked)!"
    )
    assert alloc.agent.reservation_token is None
    assert alloc.agent.current_call_id is None
    assert alloc.borrower.status == BorrowerState.ELIGIBLE.value
    assert alloc.borrower.reservation_token is None


def test_reconciler_reclaims_expired_wrap_up_agent(db_session):
    """
    Regression test for FIND-MED-05:
    Agents entering WRAP_UP must not be abandoned if the agent client never clicks 'ready'.
    Reconciliation sweeper must reclaim agents stuck in WRAP_UP beyond WRAP_UP_TIMEOUT_SEC.
    """
    campaign = Campaign(name="WrapUp Timeout Campaign")
    db_session.add(campaign)
    db_session.flush()

    past_wrapup_time = datetime.now(timezone.utc) - timedelta(seconds=150)
    agent = Agent(
        campaign_id=campaign.id,
        name="Stuck In WrapUp",
        status=AgentState.WRAP_UP.value,
        last_call_ended_at=past_wrapup_time,
    )
    db_session.add(agent)
    db_session.flush()

    report = ReconciliationSweeper.run_sweep(session=db_session, campaign_id=campaign.id)

    db_session.refresh(agent)
    assert agent.status == AgentState.AVAILABLE.value, (
        f"Agent in WRAP_UP for 150s was not reclaimed to AVAILABLE; current status: {agent.status}"
    )


def test_reconciler_does_not_release_legitimate_active_call(db_session):
    """Reconciler must NOT release an agent or call that is legitimately CONNECTED."""
    campaign = Campaign(name="Active Call Campaign")
    db_session.add(campaign)
    db_session.flush()

    agent = Agent(campaign_id=campaign.id, name="Active Agent", status=AgentState.CONNECTED.value)
    borrower = Borrower(campaign_id=campaign.id, phone_number="+15550001234", name="Active Borrower")
    db_session.add_all([agent, borrower])
    db_session.flush()

    call = Call(
        campaign_id=campaign.id,
        agent_id=agent.id,
        borrower_id=borrower.id,
        status="CONNECTED",
        connected_at=datetime.now(timezone.utc) - timedelta(seconds=120),
        idempotency_key=f"connected-{uuid.uuid4()}",
    )
    db_session.add(call)
    db_session.flush()
    agent.current_call_id = call.id
    db_session.flush()

    report = ReconciliationSweeper.run_sweep(session=db_session, campaign_id=campaign.id)

    db_session.refresh(agent)
    db_session.refresh(call)
    assert agent.status == AgentState.CONNECTED.value
    assert call.status == "CONNECTED"
