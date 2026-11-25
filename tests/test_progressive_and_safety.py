"""Tests for Progressive Dialer, Safety Controller boundary, and provider health gates."""

import uuid
import pytest
from sqlalchemy import select
from smartdialer.dialer.progressive import ProgressiveDialer
from smartdialer.models import (
    Agent,
    AgentState,
    Borrower,
    BorrowerState,
    Call,
    Campaign,
    CampaignMode,
    SafetyDecisionLog,
    SafetyDecisionType,
)
from smartdialer.safety.provider_health import provider_health_tracker
from smartdialer.safety.safety_controller import (
    DialingRequest,
    SafetyController,
)


@pytest.fixture(autouse=True)
def reset_health():
    """Ensure provider health tracker is clean before each test."""
    provider_health_tracker.reset()
    yield
    provider_health_tracker.reset()


def test_progressive_dialing_with_10_agents(db_session):
    """
    Test requirement #1: Progressive dialing with 10 agents.
    - 10 AVAILABLE agents, 15 eligible borrowers.
    - Progressive dialer must allocate exactly 10 calls (1 call per available agent).
    """
    campaign = Campaign(name="Progressive 10 Agents Test")
    db_session.add(campaign)
    db_session.flush()

    # Create 10 agents
    agents = [
        Agent(
            campaign_id=campaign.id,
            name=f"Agent-{i:02d}",
            status=AgentState.AVAILABLE.value,
        )
        for i in range(10)
    ]
    db_session.add_all(agents)

    # Create 15 borrowers
    borrowers = [
        Borrower(
            campaign_id=campaign.id,
            phone_number=f"+15550000{i:02d}",
            name=f"Borrower-{i:02d}",
            status=BorrowerState.ELIGIBLE.value,
            priority=i,
        )
        for i in range(15)
    ]
    db_session.add_all(borrowers)
    db_session.flush()

    # Execute Progressive Dialing Cycle
    res = ProgressiveDialer.execute_cycle(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
    )

    assert res.safety_decision.decision == SafetyDecisionType.APPROVED
    assert res.safety_decision.approved_calls == 10
    assert len(res.allocations) == 10
    assert res.failed_allocations == 0

    # Verify all 10 agents are now RESERVED
    reserved_agents = db_session.execute(
        select(Agent).where(
            Agent.campaign_id == campaign.id,
            Agent.status == AgentState.RESERVED.value,
        )
    ).scalars().all()
    assert len(reserved_agents) == 10

    # A second progressive cycle immediately after must allocate 0 calls (0 available agents)
    second_res = ProgressiveDialer.execute_cycle(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
    )
    assert second_res.safety_decision.decision == SafetyDecisionType.REJECTED
    assert len(second_res.allocations) == 0


def test_agent_disappearing_during_setup(db_session):
    """
    Test requirement: Handle an agent disappearing during setup.
    If an agent becomes unavailable right before row reservation, dialer proceeds safely.
    """
    campaign = Campaign(name="Agent Disappears Test")
    db_session.add(campaign)
    db_session.flush()

    # Add 2 agents and 5 borrowers
    agent1 = Agent(campaign_id=campaign.id, name="A1", status=AgentState.AVAILABLE.value)
    agent2 = Agent(campaign_id=campaign.id, name="A2", status=AgentState.AVAILABLE.value)
    db_session.add_all([agent1, agent2])

    borrowers = [
        Borrower(
            campaign_id=campaign.id,
            phone_number=f"+155511100{i}",
            name=f"Borrower-{i}",
            status=BorrowerState.ELIGIBLE.value,
        )
        for i in range(5)
    ]
    db_session.add_all(borrowers)
    db_session.flush()

    # Artificially move agent2 to OFFLINE before second allocation
    # By running an allocation that requests 2 calls, but we turn agent2 OFFLINE
    agent2.status = AgentState.OFFLINE.value
    db_session.flush()

    # Safety controller requested with desired_calls = 2 (simulating stale count)
    req = DialingRequest(
        campaign_id=campaign.id,
        mode=CampaignMode.PROGRESSIVE,
        desired_calls=2,
        estimated_answer_rate=1.0,
        provider_name="provider_a",
    )
    # The safety controller checks DB and sees only 1 available agent -> REDUCED to 1
    decision = SafetyController.evaluate(db_session, req)
    assert decision.decision == SafetyDecisionType.REDUCED
    assert decision.approved_calls == 1


def test_safety_controller_rejects_unsafe_zero_agent_request(db_session):
    """Safety Controller must reject requests when zero agents are available."""
    campaign = Campaign(name="Zero Agents Campaign")
    db_session.add(campaign)
    db_session.flush()

    req = DialingRequest(
        campaign_id=campaign.id,
        mode=CampaignMode.PREDICTIVE,
        desired_calls=15,
        estimated_answer_rate=0.5,
        provider_name="provider_a",
    )
    decision = SafetyController.evaluate(db_session, req)

    assert decision.decision == SafetyDecisionType.REJECTED
    assert decision.approved_calls == 0
    assert "Zero available agents" in decision.reasoning


def test_safety_controller_rejects_when_ringing_calls_exhaust_headroom(db_session):
    """
    If ringing calls backlog matches or exceeds available agents,
    predictive requests must be rejected or reduced to prevent overdialing abandonment.
    """
    campaign = Campaign(name="Ringing Backlog Campaign")
    db_session.add(campaign)
    db_session.flush()

    # 3 available agents
    agents = [
        Agent(campaign_id=campaign.id, name=f"A-{i}", status=AgentState.AVAILABLE.value)
        for i in range(3)
    ]
    db_session.add_all(agents)

    # 3 currently RINGING calls
    borrower = Borrower(
        campaign_id=campaign.id,
        phone_number="+15559999999",
        name="Dummy",
    )
    db_session.add(borrower)
    db_session.flush()

    ringing_calls = [
        Call(
            campaign_id=campaign.id,
            borrower_id=borrower.id,
            status="RINGING",
            idempotency_key=f"call-ring-{uuid.uuid4()}",
        )
        for _ in range(3)
    ]
    db_session.add_all(ringing_calls)
    db_session.flush()

    # Predictive engine asks for 5 calls
    req = DialingRequest(
        campaign_id=campaign.id,
        mode=CampaignMode.PREDICTIVE,
        desired_calls=5,
        estimated_answer_rate=0.4,
        provider_name="provider_a",
    )
    decision = SafetyController.evaluate(db_session, req)

    assert decision.decision == SafetyDecisionType.REJECTED
    assert decision.approved_calls == 0
    assert "backlog" in decision.reasoning


def test_provider_outage_trips_safety_controller(db_session):
    """
    When telecom provider experiences failure rate spike,
    Safety Controller must suspend dialing or fallback.
    """
    campaign = Campaign(name="Provider Outage Campaign")
    db_session.add(campaign)
    db_session.flush()

    agent = Agent(campaign_id=campaign.id, name="A1", status=AgentState.AVAILABLE.value)
    db_session.add(agent)
    db_session.flush()

    # Simulate provider failure rate spike to 40%
    for _ in range(6):
        provider_health_tracker.record("provider_a", success=False)
    for _ in range(4):
        provider_health_tracker.record("provider_a", success=True)

    assert provider_health_tracker.get_failure_rate("provider_a") == 0.60
    assert not provider_health_tracker.is_healthy("provider_a")

    # In Progressive mode: request rejected due to provider failure
    req = DialingRequest(
        campaign_id=campaign.id,
        mode=CampaignMode.PROGRESSIVE,
        desired_calls=1,
        estimated_answer_rate=1.0,
        provider_name="provider_a",
    )
    decision = SafetyController.evaluate(db_session, req)
    assert decision.decision == SafetyDecisionType.REJECTED
    assert "failure rate" in decision.reasoning


def test_safety_decisions_are_audited_in_database(db_session):
    """Verify that every single Safety Controller decision creates an immutable DB audit log."""
    campaign = Campaign(name="Audit Test Campaign")
    db_session.add(campaign)
    db_session.flush()

    req = DialingRequest(
        campaign_id=campaign.id,
        mode=CampaignMode.PROGRESSIVE,
        desired_calls=0,
        estimated_answer_rate=1.0,
        provider_name="provider_a",
    )
    SafetyController.evaluate(db_session, req)

    logs = db_session.execute(
        select(SafetyDecisionLog).where(SafetyDecisionLog.campaign_id == campaign.id)
    ).scalars().all()

    assert len(logs) == 1
    assert logs[0].decision == SafetyDecisionType.REJECTED.value
