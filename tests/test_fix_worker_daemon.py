"""Regression test for FIND-MED-02: Background worker daemon execution and graceful shutdown."""

import asyncio
import pytest
from sqlalchemy.orm import sessionmaker

from smartdialer.allocator.allocator import CallAllocator
from smartdialer.models import Agent, AgentState, Borrower, Campaign, OutboxEvent, OutboxStatus
from smartdialer.services.worker import SmartDialerWorker


def test_worker_run_once_dispatches_pending_outbox_items(sync_test_engine):
    """
    REGRESSION TEST for FIND-MED-02:
    SmartDialerWorker.run_once() must poll outbox and dispatch pending calls,
    as well as run the reconciliation sweeper.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Worker Daemon Campaign")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Worker Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15557778888", name="Worker Borrower")
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

    worker = SmartDialerWorker(session_factory=SessionMaker)
    result = worker.run_once()

    assert result["dispatched_count"] >= 1
    assert call_id in result["dispatched_ids"]
    assert hasattr(result["sweeper_stats"], "reclaimed_agents_count")

    with SessionMaker() as session:
        outbox = session.get(OutboxEvent, outbox_id)
        assert outbox.status == OutboxStatus.DISPATCHED.value


@pytest.mark.asyncio
async def test_worker_daemon_start_and_graceful_stop(sync_test_engine):
    """
    REGRESSION TEST for FIND-MED-02:
    Verify that SmartDialerWorker starts background tasks and stops cleanly without dangling tasks.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    worker = SmartDialerWorker(
        session_factory=SessionMaker,
        outbox_poll_interval_sec=0.05,
        reconcile_poll_interval_sec=0.1,
    )

    assert not worker.is_running
    await worker.start()
    assert worker.is_running
    assert len(worker._tasks) == 2

    # Let the loops cycle briefly
    await asyncio.sleep(0.15)

    await worker.stop()
    assert not worker.is_running
    assert len(worker._tasks) == 0
