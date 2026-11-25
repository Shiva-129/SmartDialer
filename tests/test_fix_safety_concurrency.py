"""Regression test for FIND-HIGH-04: Safety Controller TOCTOU race condition under concurrent workers."""

import concurrent.futures
import uuid
import pytest
from sqlalchemy.orm import sessionmaker

from smartdialer.models import Agent, AgentState, Campaign, CampaignMode
from smartdialer.safety.safety_controller import DialingRequest, SafetyController


def test_safety_controller_concurrent_evaluations_prevent_over_approval(sync_test_engine):
    """
    REGRESSION TEST for FIND-HIGH-04:
    If 10 agents are available, and 4 concurrent workers simultaneously ask for 10 calls,
    the total approved calls across all workers must NOT exceed 10.
    In the unpatched code with SELECT COUNT(*), each worker sees 10 and approves 10 (40 total approved)!
    """
    dedicated_sessionmaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)

    with dedicated_sessionmaker() as session:
        campaign = Campaign(name="Safety TOCTOU Test")
        session.add(campaign)
        session.flush()

        agents = [
            Agent(campaign_id=campaign.id, name=f"Agent-{i}", status=AgentState.AVAILABLE.value)
            for i in range(10)
        ]
        db_session = session
        db_session.add_all(agents)
        db_session.commit()
        campaign_id = campaign.id

    num_workers = 4
    approved_counts = []

    def evaluate_worker(worker_id: int):
        with dedicated_sessionmaker() as session:
            req = DialingRequest(
                campaign_id=campaign_id,
                mode=CampaignMode.PROGRESSIVE,
                desired_calls=10,
                estimated_answer_rate=1.0,
                provider_name="provider_a",
            )
            decision = SafetyController.evaluate(session=session, request=req)
            session.commit()
            return decision.approved_calls

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(evaluate_worker, i) for i in range(num_workers)]
        approved_counts = [f.result() for f in concurrent.futures.as_completed(futures)]

    total_approved = sum(approved_counts)
    assert total_approved <= 10, (
        f"TOCTOU DEFECT DETECTED: Total approved calls ({total_approved}) exceeded "
        f"actual available capacity (10)! Worker approvals: {approved_counts}"
    )
