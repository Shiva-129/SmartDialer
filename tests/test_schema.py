"""Tests for database models, constraints, and schema invariants."""

import uuid
import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from smartdialer.models import (
    Agent,
    AgentState,
    AuditLog,
    Borrower,
    BorrowerState,
    Call,
    CallState,
    Campaign,
    CampaignMode,
    OutboxEvent,
    OutboxStatus,
    ProviderEvent,
    SafetyDecisionLog,
    SafetyDecisionType,
)


def test_schema_tables_and_model_persistence(db_session):
    """Verify that models can be persisted and queried with all relational links."""
    # 1. Create Campaign
    campaign = Campaign(
        name="Q4 Collections Alpha",
        mode=CampaignMode.PROGRESSIVE.value,
        status="ACTIVE",
    )
    db_session.add(campaign)
    db_session.flush()
    assert campaign.id is not None

    # 2. Create Agent
    agent = Agent(
        campaign_id=campaign.id,
        name="Agent Smith",
        status=AgentState.AVAILABLE.value,
    )
    db_session.add(agent)
    db_session.flush()
    assert agent.id is not None
    assert agent.version == 1

    # 3. Create Borrower
    borrower = Borrower(
        campaign_id=campaign.id,
        phone_number="+15551234567",
        name="John Doe",
        status=BorrowerState.ELIGIBLE.value,
        priority=10,
    )
    db_session.add(borrower)
    db_session.flush()
    assert borrower.id is not None

    # 4. Create Call
    call = Call(
        campaign_id=campaign.id,
        agent_id=agent.id,
        borrower_id=borrower.id,
        status=CallState.INITIATED.value,
        idempotency_key=f"call-init-{uuid.uuid4()}",
        provider_name="provider_a",
        provider_call_id="prov-call-101",
    )
    db_session.add(call)
    db_session.flush()
    assert call.id is not None

    # 5. Create OutboxEvent
    outbox = OutboxEvent(
        aggregate_type="call",
        aggregate_id=call.id,
        event_type="DISPATCH_CALL",
        payload={"call_id": str(call.id), "phone": borrower.phone_number},
        status=OutboxStatus.PENDING.value,
    )
    db_session.add(outbox)
    db_session.flush()
    assert outbox.id is not None

    # 6. Create ProviderEvent
    event = ProviderEvent(
        provider_name="provider_a",
        event_id="evt-prov-001",
        call_id=call.id,
        event_type="RINGING",
        raw_payload={"status": "ringing", "timestamp": "2026-11-24T12:00:00Z"},
    )
    db_session.add(event)
    db_session.flush()
    assert event.id is not None

    # 7. Create SafetyDecisionLog
    decision = SafetyDecisionLog(
        campaign_id=campaign.id,
        dialing_mode=CampaignMode.PROGRESSIVE.value,
        requested_calls=1,
        approved_calls=1,
        decision=SafetyDecisionType.APPROVED.value,
        available_agents=1,
        reserved_agents=0,
        active_calls=0,
        ringing_calls=0,
        provider_failure_rate=0.01,
        provider_healthy=True,
        reasoning="1 available agent meets 1:1 progressive constraint.",
    )
    db_session.add(decision)
    db_session.flush()
    assert decision.id is not None


def test_provider_event_deduplication_constraint(db_session):
    """Enforce that duplicate provider events are rejected by database unique constraint."""
    event1 = ProviderEvent(
        provider_name="provider_b",
        event_id="unique-event-xyz-999",
        event_type="ANSWERED",
        raw_payload={"status": "answered"},
    )
    db_session.add(event1)
    db_session.flush()

    # Attempt to insert identical event_id for the same provider
    event2 = ProviderEvent(
        provider_name="provider_b",
        event_id="unique-event-xyz-999",
        event_type="ANSWERED",
        raw_payload={"status": "answered", "retry": True},
    )
    db_session.add(event2)

    with pytest.raises(IntegrityError):
        db_session.flush()


def test_call_idempotency_key_constraint(db_session):
    """Enforce that calls cannot be created with duplicate idempotency keys."""
    campaign = Campaign(name="Idempotency Test Campaign")
    db_session.add(campaign)
    db_session.flush()

    borrower = Borrower(
        campaign_id=campaign.id,
        phone_number="+15559876543",
        name="Alice Smith",
    )
    db_session.add(borrower)
    db_session.flush()

    idemp_key = f"idemp-{uuid.uuid4()}"
    call1 = Call(
        campaign_id=campaign.id,
        borrower_id=borrower.id,
        idempotency_key=idemp_key,
    )
    db_session.add(call1)
    db_session.flush()

    call2 = Call(
        campaign_id=campaign.id,
        borrower_id=borrower.id,
        idempotency_key=idemp_key,
    )
    db_session.add(call2)

    with pytest.raises(IntegrityError):
        db_session.flush()
