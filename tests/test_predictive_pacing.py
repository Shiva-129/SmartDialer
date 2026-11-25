"""Tests for Predictive Pacing Engine, statistical answer rate estimation, and explainability."""

import uuid
import pytest
from smartdialer.dialer.predictive import PredictiveDialer
from smartdialer.models import (
    Agent,
    AgentState,
    Borrower,
    BorrowerState,
    Call,
    Campaign,
    SafetyDecisionType,
)
from smartdialer.pacing.engine import PredictivePacingEngine
from smartdialer.pacing.metrics import telemetry_registry
from smartdialer.safety.provider_health import provider_health_tracker


@pytest.fixture(autouse=True)
def cleanup():
    telemetry_registry.reset()
    provider_health_tracker.reset()
    yield
    telemetry_registry.reset()
    provider_health_tracker.reset()


def test_predictive_pacing_different_answer_rates(db_session):
    """
    Test pacing multiplier scaling with answer rates:
    - 20% answer rate -> higher pacing demand
    - 50% answer rate -> moderate pacing demand
    - 70% answer rate -> conservative pacing demand
    """
    campaign = Campaign(name="Answer Rate Scaling Campaign")
    db_session.add(campaign)
    db_session.flush()

    # 10 available agents
    agents = [
        Agent(campaign_id=campaign.id, name=f"A-{i}", status=AgentState.AVAILABLE.value)
        for i in range(10)
    ]
    db_session.add_all(agents)
    db_session.flush()

    # 1. Test 20% answer rate
    prop_20 = PredictivePacingEngine.calculate_pacing(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
        simulated_answer_rate=0.20,
        simulated_talk_duration_sec=120.0,
    )
    # 2. Test 50% answer rate
    prop_50 = PredictivePacingEngine.calculate_pacing(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
        simulated_answer_rate=0.50,
        simulated_talk_duration_sec=90.0,
    )
    # 3. Test 70% answer rate
    prop_70 = PredictivePacingEngine.calculate_pacing(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
        simulated_answer_rate=0.70,
        simulated_talk_duration_sec=180.0,
    )

    # Low answer rate should request more attempts than high answer rate to achieve the same target connections
    assert prop_20.desired_calls >= prop_50.desired_calls >= prop_70.desired_calls
    assert prop_20.pacing_multiplier >= prop_50.pacing_multiplier >= prop_70.pacing_multiplier

    # Check explainability
    for prop in [prop_20, prop_50, prop_70]:
        assert prop.explanation != ""
        assert "Pacing target" in prop.explanation
        assert prop.estimated_answer_probability > 0.0
        assert prop.utilization_target == 0.90


def test_predictive_pacing_ringing_backlog_dampening(db_session):
    """If many calls are already ringing, desired new calls should decrease to avoid over-dialing."""
    campaign = Campaign(name="Ringing Dampening Campaign")
    db_session.add(campaign)
    db_session.flush()

    # 10 available agents
    agents = [
        Agent(campaign_id=campaign.id, name=f"A-{i}", status=AgentState.AVAILABLE.value)
        for i in range(10)
    ]
    db_session.add_all(agents)

    borrower = Borrower(campaign_id=campaign.id, phone_number="+15558881111", name="B1")
    db_session.add(borrower)
    db_session.flush()

    # Proposal with 0 ringing calls
    prop_clean = PredictivePacingEngine.calculate_pacing(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
        simulated_answer_rate=0.50,
    )

    # Now add 15 ringing calls
    ringing_calls = [
        Call(
            campaign_id=campaign.id,
            borrower_id=borrower.id,
            status="RINGING",
            idempotency_key=f"ring-{uuid.uuid4()}",
        )
        for _ in range(15)
    ]
    db_session.add_all(ringing_calls)
    db_session.flush()

    # Proposal with 15 ringing calls (15 * 0.50 = 7.5 expected answers already in-flight!)
    prop_backlog = PredictivePacingEngine.calculate_pacing(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
        simulated_answer_rate=0.50,
    )

    assert prop_backlog.desired_calls < prop_clean.desired_calls


def test_predictive_dialer_routes_through_safety_controller(db_session):
    """
    PredictiveDialer must route through SafetyController and respect its approved bounds.
    If Safety Controller reduces or rejects, the dialer cannot allocate more than approved.
    """
    campaign = Campaign(name="Predictive Safety Test")
    db_session.add(campaign)
    db_session.flush()

    # 5 available agents and 20 borrowers
    agents = [
        Agent(campaign_id=campaign.id, name=f"Agent-{i}", status=AgentState.AVAILABLE.value)
        for i in range(5)
    ]
    borrowers = [
        Borrower(
            campaign_id=campaign.id,
            phone_number=f"+15552220{i}",
            name=f"Borrower-{i}",
            status=BorrowerState.ELIGIBLE.value,
        )
        for i in range(20)
    ]
    db_session.add_all(agents + borrowers)
    db_session.flush()

    # Execute Predictive Cycle with 20% answer rate (engine might propose 15 calls)
    res = PredictiveDialer.execute_cycle(
        session=db_session,
        campaign_id=campaign.id,
        provider_name="provider_a",
        simulated_answer_rate=0.20,
    )

    # In strict safety mode, Safety Controller will cap approved calls to available agents (5)
    assert res.safety_decision.approved_calls <= 5
    assert len(res.allocations) == res.safety_decision.approved_calls
    assert len(res.allocations) <= 5  # NEVER creates more calls than agent capacity!
