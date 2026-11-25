"""Concurrency and atomic allocation tests against PostgreSQL."""

import concurrent.futures
import uuid
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from smartdialer.allocator import CallAllocator
from smartdialer.core.config import settings
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
from smartdialer.state.exceptions import (
    AgentNotAvailableError,
    BorrowerNotEligibleError,
)


@pytest.fixture
def dedicated_sessionmaker(sync_test_engine):
    """Session factory for concurrent multi-threaded database testing."""
    return sessionmaker(bind=sync_test_engine, expire_on_commit=False)


def test_concurrent_agent_reservation_race_condition(sync_test_engine, dedicated_sessionmaker):
    """
    Core concurrency invariant test:
    Two or more concurrent workers must NEVER be able to reserve the same agent.
    """
    # Setup test campaign and exactly 1 AVAILABLE agent
    with dedicated_sessionmaker() as session:
        campaign = Campaign(name="Agent Concurrency Test")
        session.add(campaign)
        session.flush()

        agent = Agent(
            campaign_id=campaign.id,
            name="Sole Survivor",
            status=AgentState.AVAILABLE.value,
        )
        session.add(agent)
        session.commit()
        campaign_id = campaign.id
        agent_id = agent.id

    num_workers = 8
    successes = []
    failures = []

    def worker_reserve(worker_id: int):
        token = uuid.uuid4()
        with dedicated_sessionmaker() as session:
            try:
                res_agent = CallAllocator.reserve_agent_atomic(
                    session=session,
                    campaign_id=campaign_id,
                    reservation_token=token,
                )
                session.commit()
                return ("SUCCESS", worker_id, str(res_agent.id), token)
            except AgentNotAvailableError as e:
                session.rollback()
                return ("FAILURE", worker_id, str(e))
            except Exception as e:
                session.rollback()
                return ("ERROR", worker_id, str(e))

    # Launch all workers simultaneously
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker_reserve, i) for i in range(num_workers)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    for outcome in results:
        if outcome[0] == "SUCCESS":
            successes.append(outcome)
        else:
            failures.append(outcome)

    # Invariant assertion: Exactly 1 worker must succeed, 7 must fail
    assert len(successes) == 1, f"Expected exactly 1 success, got {len(successes)}: {successes}"
    assert len(failures) == num_workers - 1

    # Verify database state
    with dedicated_sessionmaker() as session:
        agent_in_db = session.get(Agent, agent_id)
        assert agent_in_db.status == AgentState.RESERVED.value
        assert agent_in_db.reservation_token == successes[0][3]


def test_concurrent_borrower_reservation_race_condition(sync_test_engine, dedicated_sessionmaker):
    """Two or more concurrent workers must NEVER allocate the same borrower."""
    with dedicated_sessionmaker() as session:
        campaign = Campaign(name="Borrower Concurrency Test")
        session.add(campaign)
        session.flush()

        borrower = Borrower(
            campaign_id=campaign.id,
            phone_number="+15550001122",
            name="Single Borrower",
            status=BorrowerState.ELIGIBLE.value,
        )
        session.add(borrower)
        session.commit()
        campaign_id = campaign.id
        borrower_id = borrower.id

    num_workers = 6
    successes = []
    failures = []

    def worker_reserve_borrower(worker_id: int):
        token = uuid.uuid4()
        with dedicated_sessionmaker() as session:
            try:
                res_b = CallAllocator.reserve_borrower_atomic(
                    session=session,
                    campaign_id=campaign_id,
                    reservation_token=token,
                )
                session.commit()
                return ("SUCCESS", worker_id, str(res_b.id))
            except BorrowerNotEligibleError:
                session.rollback()
                return ("FAILURE", worker_id)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker_reserve_borrower, i) for i in range(num_workers)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    for outcome in results:
        if outcome[0] == "SUCCESS":
            successes.append(outcome)
        else:
            failures.append(outcome)

    assert len(successes) == 1
    assert len(failures) == num_workers - 1


def test_atomic_call_allocation_full_transaction(db_session):
    """Test full atomic allocation creates Call and OutboxEvent and links agent/borrower."""
    campaign = Campaign(name="Full Allocation Campaign")
    db_session.add(campaign)
    db_session.flush()

    agent = Agent(
        campaign_id=campaign.id,
        name="Agent 007",
        status=AgentState.AVAILABLE.value,
    )
    borrower = Borrower(
        campaign_id=campaign.id,
        phone_number="+15554443322",
        name="Bob Jones",
        status=BorrowerState.ELIGIBLE.value,
    )
    db_session.add_all([agent, borrower])
    db_session.flush()

    # Perform atomic allocation
    res = CallAllocator.allocate_call(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
    )

    assert res.call is not None
    assert res.call.agent_id == agent.id
    assert res.call.borrower_id == borrower.id
    assert res.call.status == "RESERVED"

    assert res.agent.status == AgentState.RESERVED.value
    assert res.agent.current_call_id == res.call.id

    assert res.borrower.status == BorrowerState.RESERVED.value

    assert res.outbox_event is not None
    assert res.outbox_event.aggregate_id == res.call.id
    assert res.outbox_event.status == OutboxStatus.PENDING.value
    assert res.outbox_event.payload["phone_number"] == borrower.phone_number


def test_allocation_reverts_agent_when_no_borrowers_available(db_session):
    """If no borrower is eligible, the reserved agent must be immediately reverted to AVAILABLE."""
    campaign = Campaign(name="Exhausted Borrowers Campaign")
    db_session.add(campaign)
    db_session.flush()

    agent = Agent(
        campaign_id=campaign.id,
        name="Idle Agent",
        status=AgentState.AVAILABLE.value,
    )
    db_session.add(agent)
    db_session.flush()

    # No borrowers added!
    with pytest.raises(BorrowerNotEligibleError):
        CallAllocator.allocate_call(
            session=db_session,
            campaign_id=campaign.id,
            provider_name="provider_a",
        )

    # Verify agent was reverted to AVAILABLE
    db_session.refresh(agent)
    assert agent.status == AgentState.AVAILABLE.value
    assert agent.reservation_token is None
