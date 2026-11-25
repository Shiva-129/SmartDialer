"""Regression tests for FIND-MED-03 & FIND-MED-04: Callback lookup by provider_call_id & deterministic key."""

import uuid
from sqlalchemy.orm import sessionmaker

from smartdialer.allocator.allocator import CallAllocator
from smartdialer.events.processor import EventProcessor
from smartdialer.models import Agent, AgentState, Borrower, Campaign, CallState
from smartdialer.models.call import Call
from smartdialer.recovery.outbox_dispatcher import OutboxDispatcher


def test_event_processor_resolves_call_by_provider_call_id(sync_test_engine):
    """
    REGRESSION TEST for FIND-MED-03:
    When telecom carrier sends webhooks with only `provider_call_id` (and call_id is None),
    EventProcessor MUST look up the call row using (provider_name, provider_call_id).
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Provider Callback Lookup Campaign")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Lookup Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15554443333", name="Lookup Borrower")
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

    # Dispatch call to generate real provider_call_id
    with SessionMaker() as session:
        OutboxDispatcher.dispatch_pending(session=session)
        call = session.get(Call, call_id)
        prov_call_id = call.provider_call_id
        assert prov_call_id is not None
        assert call.status == CallState.INITIATED.value

    # Telecom webhook arrives with call_id=None, passing only provider_call_id
    with SessionMaker() as session:
        res = EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-webhook-{uuid.uuid4().hex[:8]}",
            call_id=None,
            provider_call_id=prov_call_id,
            event_type="ANSWERED",
        )
        session.commit()

    assert res.success is True
    assert res.new_call_state == "ANSWERED"

    with SessionMaker() as session:
        call = session.get(Call, call_id)
        assert call.status == "ANSWERED"


def test_allocator_deterministic_idempotency_key(sync_test_engine):
    """
    REGRESSION TEST for FIND-MED-04:
    CallAllocator must formulate deterministic idempotency keys based on
    campaign, borrower id, and attempt number so retries match keys.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Deterministic Key Campaign")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Det Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15554442222", name="Det Borrower")
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

        # Idempotency key must be deterministic and end with attempt number (e.g. -att1)
        expected_suffix = f"-att{borrower.call_attempts}"
        assert alloc.call.idempotency_key.endswith(expected_suffix), (
            f"Idempotency key '{alloc.call.idempotency_key}' does not end with deterministic suffix '{expected_suffix}'"
        )
