"""Regression test for FIND-HIGH-03: Distributed database-backed provider health tracking."""

import uuid
from sqlalchemy.orm import sessionmaker

from smartdialer.models import Agent, AgentState, Campaign, CampaignMode
from smartdialer.safety.provider_health import ProviderHealthTracker, provider_health_tracker
from smartdialer.safety.safety_controller import (
    DialingRequest,
    SafetyController,
    SafetyDecisionType,
)


def test_provider_health_degradation_persists_across_independent_trackers(sync_test_engine):
    """
    REGRESSION TEST for FIND-HIGH-03:
    When Worker 1 marks provider_a as degraded, Worker 2 (a distinct tracker instance
    with empty in-memory state, representing a separate worker process) MUST recognize
    the degraded status via PostgreSQL and refuse calls.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)

    # Worker 1 trips Provider A in the database
    with SessionMaker() as session1:
        provider_health_tracker.set_manual_status("provider_a", degraded=True, session=session1)
        session1.commit()

    # Worker 2: brand-new tracker instance (simulating separate OS worker process)
    worker2_tracker = ProviderHealthTracker()
    assert worker2_tracker._providers["provider_a"].manually_degraded is False  # Local memory is unaware

    with SessionMaker() as session2:
        is_healthy = worker2_tracker.is_healthy("provider_a", session=session2)
        assert is_healthy is False, (
            "DEFECT: Worker 2 did not observe provider degradation from database! In-memory isolation leak."
        )

    # Worker 2 runs Safety Controller evaluation: must reject due to provider degradation
    with SessionMaker() as session2:
        campaign = Campaign(name="Distributed Health Campaign")
        session2.add(campaign)
        session2.flush()
        agent = Agent(campaign_id=campaign.id, name="Test Agent", status=AgentState.AVAILABLE.value)
        session2.add(agent)
        session2.commit()

        req = DialingRequest(
            campaign_id=campaign.id,
            mode=CampaignMode.PROGRESSIVE,
            desired_calls=1,
            estimated_answer_rate=0.5,
            provider_name="provider_a",
        )
        decision = SafetyController.evaluate(session=session2, request=req)
        session2.commit()

        assert decision.decision == SafetyDecisionType.REJECTED
        assert "exceeds safety threshold" in decision.reasoning or "degraded" in decision.reasoning.lower()

    # Reset health after test
    with SessionMaker() as session:
        provider_health_tracker.reset(session=session)
        session.commit()
