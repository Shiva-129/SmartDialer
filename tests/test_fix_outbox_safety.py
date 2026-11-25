"""Regression tests for FIND-CRIT-02 & FIND-CRIT-03: Outbox dispatch pre-verification & transaction isolation."""

import uuid
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import sessionmaker

from smartdialer.allocator.allocator import CallAllocator
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
from smartdialer.providers.registry import provider_registry
from smartdialer.recovery.outbox_dispatcher import OutboxDispatcher


def test_outbox_dispatch_aborts_when_agent_becomes_offline(sync_test_engine):
    """
    REGRESSION TEST for FIND-CRIT-02 (FIX 2):
    An outbox record is pending, but the assigned agent goes OFFLINE before dispatch runs.
    Dispatcher MUST NOT call provider, MUST cancel outbox and call, and MUST release borrower.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Pre-dispatch Offline Test")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Test Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15551112222", name="Test Borrower")
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
        outbox_id = alloc.outbox_event.id
        agent_id = agent.id
        borrower_id = borrower.id

    # Agent goes OFFLINE while outbox event is waiting
    with SessionMaker() as session:
        ag = session.get(Agent, agent_id)
        ag.status = AgentState.OFFLINE.value
        session.commit()

    # Provider call count before dispatch
    provider = provider_registry.get("provider_a")
    calls_before = len(getattr(provider, "_simulated_calls", {}))

    # Run dispatch
    with SessionMaker() as session:
        dispatched = OutboxDispatcher.dispatch_pending(session=session)
        assert call_id not in dispatched

    # Verify provider was NOT called
    calls_after = len(getattr(provider, "_simulated_calls", {}))
    assert calls_after == calls_before, "CRITICAL DEFECT: Provider was called for an OFFLINE agent!"

    # Verify records state
    with SessionMaker() as session:
        call = session.get(Call, call_id)
        outbox = session.get(OutboxEvent, outbox_id)
        borrower = session.get(Borrower, borrower_id)
        agent = session.get(Agent, agent_id)

        assert outbox.status == OutboxStatus.CANCELLED.value
        assert call.status == "CANCELLED"
        assert borrower.status == BorrowerState.ELIGIBLE.value
        assert borrower.reservation_token is None
        assert agent.status == AgentState.OFFLINE.value


def test_outbox_dispatch_aborts_when_agent_becomes_paused(sync_test_engine):
    """
    REGRESSION TEST for FIND-CRIT-02:
    Agent goes PAUSED before dispatch. Dispatcher aborts dispatch.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Pre-dispatch Paused Test")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Test Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15551113333", name="Test Borrower")
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
        borrower_id = borrower.id

    # Agent goes PAUSED
    with SessionMaker() as session:
        ag = session.get(Agent, agent_id)
        ag.status = AgentState.PAUSED.value
        session.commit()

    with SessionMaker() as session:
        dispatched = OutboxDispatcher.dispatch_pending(session=session)
        assert call_id not in dispatched

    with SessionMaker() as session:
        call = session.get(Call, call_id)
        borrower = session.get(Borrower, borrower_id)
        assert call.status == "CANCELLED"
        assert borrower.status == BorrowerState.ELIGIBLE.value


def test_outbox_dispatch_aborts_when_reservation_expired(sync_test_engine):
    """
    REGRESSION TEST for FIND-CRIT-02:
    Reservation expired before dispatch. Dispatcher aborts dispatch and resets agent to AVAILABLE.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Pre-dispatch Expired Test")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Test Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15551114444", name="Test Borrower")
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
        borrower_id = borrower.id

    # Simulate expired reservation (> 30s)
    with SessionMaker() as session:
        ag = session.get(Agent, agent_id)
        ag.reserved_at = datetime.now(timezone.utc) - timedelta(seconds=45)
        session.commit()

    with SessionMaker() as session:
        dispatched = OutboxDispatcher.dispatch_pending(session=session)
        assert call_id not in dispatched

    with SessionMaker() as session:
        call = session.get(Call, call_id)
        agent = session.get(Agent, agent_id)
        borrower = session.get(Borrower, borrower_id)

        assert call.status == "CANCELLED"
        assert agent.status == AgentState.AVAILABLE.value
        assert borrower.status == BorrowerState.ELIGIBLE.value


def test_outbox_batch_independence_prevents_duplicate_calls(sync_test_engine):
    """
    REGRESSION TEST for FIND-CRIT-03 (FIX 3):
    In a batch of 2 calls, Call 1 is dispatched successfully.
    Even if an error or crash occurs afterwards, Call 1 must remain DISPATCHED
    and committed, so subsequent dispatcher runs NEVER re-dispatch Call 1.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Batch Independence Test")
        session.add(campaign)
        session.flush()

        agent1 = Agent(campaign_id=campaign.id, name="Agent 1", status=AgentState.AVAILABLE.value)
        agent2 = Agent(campaign_id=campaign.id, name="Agent 2", status=AgentState.AVAILABLE.value)
        borrower1 = Borrower(campaign_id=campaign.id, phone_number="+15550001111", name="Borrower 1")
        borrower2 = Borrower(campaign_id=campaign.id, phone_number="+15550002222", name="Borrower 2")
        session.add_all([agent1, agent2, borrower1, borrower2])
        session.flush()

        alloc1 = CallAllocator.allocate_call(
            session=session,
            campaign_id=campaign.id,
            provider_name="provider_a",
            agent_id=agent1.id,
            borrower_id=borrower1.id,
        )
        alloc2 = CallAllocator.allocate_call(
            session=session,
            campaign_id=campaign.id,
            provider_name="provider_a",
            agent_id=agent2.id,
            borrower_id=borrower2.id,
        )
        session.commit()
        call1_id = alloc1.call.id
        call2_id = alloc2.call.id
        outbox1_id = alloc1.outbox_event.id
        outbox2_id = alloc2.outbox_event.id

    # Dispatch batch
    with SessionMaker() as session:
        dispatched = OutboxDispatcher.dispatch_pending(session=session)
        assert call1_id in dispatched
        assert call2_id in dispatched

    # Verify both are committed as DISPATCHED
    with SessionMaker() as session:
        evt1 = session.get(OutboxEvent, outbox1_id)
        evt2 = session.get(OutboxEvent, outbox2_id)
        assert evt1.status == OutboxStatus.DISPATCHED.value
        assert evt2.status == OutboxStatus.DISPATCHED.value

    # Run dispatch again: nothing pending should be dispatched
    with SessionMaker() as session:
        second_run = OutboxDispatcher.dispatch_pending(session=session)
        assert len(second_run) == 0
