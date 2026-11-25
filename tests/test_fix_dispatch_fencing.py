"""
REGRESSION TEST SUITE: Dispatch Reservation Consistency and Fencing Tokens
Verifies Priority 3 from audit/POST_REPAIR_AUDIT.md:
- Detects agent disconnect (OFFLINE / PAUSED) during external telecom I/O.
- Immediately invokes provider.terminate_call() rather than leaving orphaned ringing calls.
- Transitions call cleanly to CANCELLED and releases borrower back to pool.
- Rejects stale worker completions using fencing tokens.
"""

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
from smartdialer.providers.base import ProviderInitiateResult, TelecomProvider
from smartdialer.providers.registry import provider_registry
from smartdialer.recovery.outbox_dispatcher import OutboxDispatcher


class InterceptingProvider(TelecomProvider):
    """Mock provider that allows executing a mutation hook during the simulated carrier call."""

    def __init__(self, in_flight_hook=None):
        self._name = "intercepting_provider"
        self.in_flight_hook = in_flight_hook
        self.terminated_calls = []

    @property
    def name(self) -> str:
        return self._name

    def initiate_call(self, call_id, phone_number, idempotency_key, callback_url=None, session=None):
        # Trigger mutation hook while carrier call is supposedly in flight
        if self.in_flight_hook:
            self.in_flight_hook()
        prov_id = f"prov-intercept-{uuid.uuid4().hex[:8]}"
        return ProviderInitiateResult(success=True, provider_call_id=prov_id, latency_ms=10.0)

    def terminate_call(self, provider_call_id: str) -> bool:
        self.terminated_calls.append(provider_call_id)
        return True

    def generate_simulated_lifecycle_events(self, call_id, provider_call_id, outcome="COMPLETED", duration_seconds=60.0):
        return []


def test_agent_transitions_to_offline_during_dispatch_terminates_call(sync_test_engine):
    """
    REGRESSION TEST for Priority 3:
    If an agent disconnects (OFFLINE) while provider network I/O is in flight:
    OutboxDispatcher MUST detect this upon return, immediately invoke provider.terminate_call(),
    mark the call CANCELLED, and release the borrower. It MUST NOT leave an orphaned ringing call.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Agent Disconnect Test Campaign")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Disconnecting Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15554441111", name="Borrower")
        session.add_all([agent, borrower])
        session.flush()

        alloc = CallAllocator.allocate_call(
            session=session,
            campaign_id=campaign.id,
            provider_name="intercepting_provider",
            agent_id=agent.id,
            borrower_id=borrower.id,
        )
        session.commit()
        call_id = alloc.call.id
        agent_id = agent.id
        borrower_id = borrower.id

    # Hook that transitions the agent to OFFLINE while carrier call is in-flight
    def disconnect_agent():
        with SessionMaker() as s:
            ag = s.get(Agent, agent_id)
            ag.status = AgentState.OFFLINE.value
            s.commit()

    test_provider = InterceptingProvider(in_flight_hook=disconnect_agent)
    provider_registry.register(test_provider)

    try:
        with SessionMaker() as session:
            dispatched = OutboxDispatcher.dispatch_pending(session=session)
            # Dispatch must NOT count as a successful call
            assert call_id not in dispatched

        # Verify carrier call was terminated!
        assert len(test_provider.terminated_calls) == 1

        # Verify call is CANCELLED (never INITIATED / orphaned ringing)
        with SessionMaker() as session:
            call = session.get(Call, call_id)
            borrower = session.get(Borrower, borrower_id)
            agent = session.get(Agent, agent_id)

            assert call.status == CallState.CANCELLED.value
            assert "AGENT_OFFLINE_DURING_DISPATCH" in (call.termination_reason or "")
            # Borrower was released back to ELIGIBLE
            assert borrower.status == BorrowerState.ELIGIBLE.value
            # Agent remains OFFLINE
            assert agent.status == AgentState.OFFLINE.value
    finally:
        provider_registry._providers.pop("intercepting_provider", None)


def test_agent_transitions_to_paused_during_dispatch_terminates_call(sync_test_engine):
    """
    REGRESSION TEST for Priority 3:
    If an agent takes a break (PAUSED) while provider dispatch is in flight,
    the dispatcher must cancel the call and terminate the carrier connection.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Agent Pause Test Campaign")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Pausing Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15554442222", name="Borrower")
        session.add_all([agent, borrower])
        session.flush()

        alloc = CallAllocator.allocate_call(
            session=session,
            campaign_id=campaign.id,
            provider_name="intercepting_provider",
            agent_id=agent.id,
            borrower_id=borrower.id,
        )
        session.commit()
        call_id = alloc.call.id
        agent_id = agent.id

    def pause_agent():
        with SessionMaker() as s:
            ag = s.get(Agent, agent_id)
            ag.status = AgentState.PAUSED.value
            s.commit()

    test_provider = InterceptingProvider(in_flight_hook=pause_agent)
    provider_registry.register(test_provider)

    try:
        with SessionMaker() as session:
            dispatched = OutboxDispatcher.dispatch_pending(session=session)
            assert call_id not in dispatched

        assert len(test_provider.terminated_calls) == 1

        with SessionMaker() as session:
            call = session.get(Call, call_id)
            assert call.status == CallState.CANCELLED.value
            assert "AGENT_PAUSED_DURING_DISPATCH" in (call.termination_reason or "")
    finally:
        provider_registry._providers.pop("intercepting_provider", None)


def test_fencing_token_rejects_stale_worker_completion(sync_test_engine):
    """
    REGRESSION TEST for Priority 3:
    If a worker experiences an extreme pause and another process invalidates or
    re-allocates the reservation (mutating dispatch_token), the stale worker
    MUST be rejected and its carrier call terminated immediately.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Fencing Token Test Campaign")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Fencing Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15554443333", name="Borrower")
        session.add_all([agent, borrower])
        session.flush()

        alloc = CallAllocator.allocate_call(
            session=session,
            campaign_id=campaign.id,
            provider_name="intercepting_provider",
            agent_id=agent.id,
            borrower_id=borrower.id,
        )
        session.commit()
        call_id = alloc.call.id

    # Hook that simulates another worker/sweeper stealing or altering the call
    def simulate_token_invalidation():
        with SessionMaker() as s:
            c = s.get(Call, call_id)
            c.dispatch_token = uuid.uuid4()  # Changed by another process!
            s.commit()

    test_provider = InterceptingProvider(in_flight_hook=simulate_token_invalidation)
    provider_registry.register(test_provider)

    try:
        with SessionMaker() as session:
            dispatched = OutboxDispatcher.dispatch_pending(session=session)
            assert call_id not in dispatched

        # Carrier call was hung up!
        assert len(test_provider.terminated_calls) == 1
    finally:
        provider_registry._providers.pop("intercepting_provider", None)
