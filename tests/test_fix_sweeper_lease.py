"""
REGRESSION TEST SUITE: Sweeper and Outbox Lease Coordination
Verifies Priority 5 from audit/POST_REPAIR_AUDIT.md:
- Reconciler sweeper coordinates with active outbox leases (locked_until > now).
- Prevents sweeper from cancelling in-flight dispatches and stealing agents.
- Sweeps stale calls only when outbox lease has expired.
"""

from datetime import datetime, timedelta, timezone
import uuid
import pytest
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from smartdialer.allocator.allocator import CallAllocator
from smartdialer.models.agent import Agent
from smartdialer.models.borrower import Borrower
from smartdialer.models.call import Call
from smartdialer.models.campaign import Campaign
from smartdialer.models.enums import AgentState, BorrowerState, CallState, OutboxStatus
from smartdialer.models.outbox import OutboxEvent
from smartdialer.recovery.reconciler import ReconciliationSweeper


def test_sweeper_skips_call_with_active_outbox_lease(sync_test_engine):
    """
    CRITICAL REGRESSION TEST for FIND-CRIT-03 / Priority 5:
    When a call in RESERVED is older than 30s (created_at < now - 30s),
    BUT an outbox worker actively holds a lease on the outbox event (locked_until > now):
    The sweeper MUST NOT mark the call FAILED or release the agent!
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)
    stale_time = now - timedelta(seconds=45)  # Created 45s ago!

    with SessionMaker() as session:
        campaign = Campaign(name="Active Lease Protection Campaign")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Lease Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15556667777", name="Borrower")
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
        event_id = alloc.outbox_event.id

    # Manually backdate call.created_at to 45s ago, but set outbox_event.locked_until to now + 20s
    with SessionMaker() as session:
        call = session.get(Call, call_id)
        call.created_at = stale_time
        event = session.get(OutboxEvent, event_id)
        event.locked_until = now + timedelta(seconds=20)  # Active lease held!
        session.commit()

    # Run sweeper pass
    with SessionMaker() as session:
        report = ReconciliationSweeper.run_sweep(session=session)
        session.commit()

    # Call and Agent MUST NOT have been reclaimed!
    with SessionMaker() as session:
        call = session.get(Call, call_id)
        agent = session.get(Agent, agent_id)
        assert call.status == CallState.RESERVED.value  # NOT FAILED!
        assert agent.status == AgentState.RESERVED.value  # NOT AVAILABLE!
        assert agent.current_call_id == call.id


def test_sweeper_reclaims_call_when_outbox_lease_expired(sync_test_engine):
    """
    When a call in RESERVED is older than 30s AND the outbox lease has expired
    (locked_until < now, simulating crashed worker that abandoned lease):
    The sweeper MUST mark the call FAILED and reclaim the agent to AVAILABLE.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)
    stale_time = now - timedelta(seconds=45)

    with SessionMaker() as session:
        campaign = Campaign(name="Expired Lease Reclaim Campaign")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Stale Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15556668888", name="Borrower")
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
        event_id = alloc.outbox_event.id

    # Backdate both call.created_at and event.locked_until (expired lease)
    with SessionMaker() as session:
        call = session.get(Call, call_id)
        call.created_at = stale_time
        event = session.get(OutboxEvent, event_id)
        event.locked_until = now - timedelta(seconds=5)  # Lease expired!
        session.commit()

    # Run sweeper pass
    with SessionMaker() as session:
        report = ReconciliationSweeper.run_sweep(session=session)
        session.commit()

    # Call MUST be FAILED and Agent reclaimed to AVAILABLE
    with SessionMaker() as session:
        call = session.get(Call, call_id)
        agent = session.get(Agent, agent_id)
        assert call.status == CallState.FAILED.value
        assert call.termination_reason == "RESERVATION_TIMEOUT_EXPIRED"
        assert agent.status == AgentState.AVAILABLE.value
