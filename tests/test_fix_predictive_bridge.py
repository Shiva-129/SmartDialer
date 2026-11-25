"""
REGRESSION TEST SUITE: Predictive Dialing Deadlock Fix and Bridge Allocation
Verifies Priority 1 from audit/POST_REPAIR_AUDIT.md:
- Predictive dialing allocates agent-less calls when agents are busy.
- Atomic Answer-to-Agent Bridge Allocator matches answering borrowers to agents.
- Mutex via SELECT ... FOR UPDATE SKIP LOCKED prevents double-assignment.
- Configurable answer timeout enforces compliant call abandonment.
"""

import threading
import time
import uuid
import pytest
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from smartdialer.allocator.bridge import BridgeAllocator
from smartdialer.dialer.predictive import PredictiveDialer
from smartdialer.events.processor import EventProcessor
from smartdialer.models.agent import Agent
from smartdialer.models.borrower import Borrower
from smartdialer.models.call import Call
from smartdialer.models.campaign import Campaign
from smartdialer.models.enums import AgentState, BorrowerState, CallState, CampaignMode
from smartdialer.pacing.metrics import telemetry_registry
from smartdialer.providers.registry import provider_registry
from smartdialer.recovery.outbox_dispatcher import OutboxDispatcher


def test_predictive_cycle_allocates_calls_when_all_agents_connected(sync_test_engine):
    """
    CRITICAL REGRESSION TEST for FIND-HIGH-01 / Priority 1:
    When N_avail = 0 and N_conn = 20, PredictiveDialer.execute_cycle()
    MUST NOT throw AgentNotAvailableError or abort with 0 calls.
    It MUST allocate agent-less predictive calls with dialing_mode='PREDICTIVE'.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    provider_registry.get("provider_a").failure_rate = 0.0
    with SessionMaker() as session:
        campaign = Campaign(name="Busy Predictive Center Campaign", mode=CampaignMode.PREDICTIVE.value, max_active_calls=50)
        session.add(campaign)
        session.flush()

        # Seed telemetry with high answer rate & 120s duration
        telem = telemetry_registry.get(str(campaign.id))
        for _ in range(50):
            telem.record_call_result(answered=True, duration_sec=120.0)

        # 20 connected agents (0 available)
        agents = [
            Agent(campaign_id=campaign.id, name=f"Busy Agent {i}", status=AgentState.CONNECTED.value)
            for i in range(20)
        ]
        session.add_all(agents)

        # 10 eligible borrowers
        borrowers = [
            Borrower(campaign_id=campaign.id, phone_number=f"+1555000{i:04d}", name=f"Borrower {i}")
            for i in range(10)
        ]
        session.add_all(borrowers)
        session.commit()
        cid = campaign.id

    # Execute predictive cycle
    with SessionMaker() as session:
        res = PredictiveDialer.execute_cycle(
            session=session,
            campaign_id=cid,
            provider_name="provider_a",
            simulated_answer_rate=0.20,
            simulated_talk_duration_sec=120.0,
        )
        session.commit()

        # MUST have allocated calls! (Previously 0 due to deadlock)
        assert len(res.allocations) > 0, f"Predictive cycle deadlocked with 0 calls! {res.message}"
        for alloc in res.allocations:
            assert alloc.call.dialing_mode == "PREDICTIVE"
            assert alloc.call.agent_id is None  # Agent-less until answered!
            assert alloc.call.status == CallState.RESERVED.value

    # Verify OutboxDispatcher successfully dispatches these agent-less predictive calls
    with SessionMaker() as session:
        dispatched_ids = OutboxDispatcher.dispatch_pending(session=session)
        assert len(dispatched_ids) > 0

    # Verify calls are now INITIATED with real carrier IDs but NO agent yet
    with SessionMaker() as session:
        initiated_calls = session.execute(
            select(Call).where(Call.campaign_id == cid, Call.status == CallState.INITIATED.value)
        ).scalars().all()
        assert len(initiated_calls) == len(res.allocations)
        for c in initiated_calls:
            assert c.provider_call_id is not None
            assert c.agent_id is None


def test_bridge_allocator_matches_answering_call_to_available_agent(sync_test_engine):
    """
    When an unassigned predictive call receives an ANSWERED webhook,
    BridgeAllocator MUST match it to an available agent and transition agent to CONNECTED.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Bridge Match Campaign", mode=CampaignMode.PREDICTIVE.value)
        session.add(campaign)
        session.flush()

        # 1 Available Agent
        agent = Agent(campaign_id=campaign.id, name="Free Agent", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15553334444", name="Borrower")
        session.add_all([agent, borrower])
        session.flush()

        call = Call(
            campaign_id=campaign.id,
            agent_id=None,
            borrower_id=borrower.id,
            status=CallState.INITIATED.value,
            idempotency_key="bridge-test-1",
            provider_name="provider_a",
            provider_call_id="prov-bridge-123",
            dialing_mode="PREDICTIVE",
        )
        session.add(call)
        session.commit()
        call_id = call.id
        agent_id = agent.id

    # Telecom webhook arrives: ANSWERED
    with SessionMaker() as session:
        res = EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id="evt-ans-1",
            call_id=call_id,
            event_type="ANSWERED",
            raw_payload={"answer_timeout_sec": 0.5},
        )
        session.commit()
        assert res.success is True

    # Verify call is now CONNECTED and linked to the agent
    with SessionMaker() as session:
        c = session.get(Call, call_id)
        ag = session.get(Agent, agent_id)
        assert c.status == CallState.CONNECTED.value
        assert c.agent_id == ag.id
        assert ag.status == AgentState.CONNECTED.value
        assert ag.current_call_id == c.id


def test_bridge_allocator_abandons_call_when_no_agent_available(sync_test_engine):
    """
    When an answering call has NO agent available within the configured timeout,
    the call MUST be compliantly terminated with status FAILED and reason 'ABANDONED'.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Abandonment Test Campaign", mode=CampaignMode.PREDICTIVE.value)
        session.add(campaign)
        session.flush()

        # All agents are BUSY
        busy_agent = Agent(campaign_id=campaign.id, name="Busy Agent", status=AgentState.CONNECTED.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15559998888", name="Abandon Borrower")
        session.add_all([busy_agent, borrower])
        session.flush()

        call = Call(
            campaign_id=campaign.id,
            agent_id=None,
            borrower_id=borrower.id,
            status=CallState.INITIATED.value,
            idempotency_key="abandon-test-1",
            provider_name="provider_a",
            provider_call_id="prov-abandon-999",
            dialing_mode="PREDICTIVE",
        )
        session.add(call)
        session.commit()
        call_id = call.id

    # Webhook arrives: ANSWERED with tight 0.05s timeout
    with SessionMaker() as session:
        res = EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id="evt-ans-abandon-1",
            call_id=call_id,
            event_type="ANSWERED",
            raw_payload={"answer_timeout_sec": 0.05},
        )
        session.commit()
        assert res.success is True

    # Verify compliant abandonment
    with SessionMaker() as session:
        c = session.get(Call, call_id)
        assert c.status == CallState.FAILED.value
        assert c.termination_reason == "ABANDONED"
        assert c.agent_id is None


def test_concurrent_answering_calls_cannot_claim_same_agent(sync_test_engine):
    """
    When 2 calls answer simultaneously and only 1 agent is available,
    SELECT ... FOR UPDATE SKIP LOCKED ensures exactly 1 call claims the agent.
    The other call cannot double-claim it.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Concurrent Bridge Campaign", mode=CampaignMode.PREDICTIVE.value)
        session.add(campaign)
        session.flush()

        # ONLY 1 available agent
        agent = Agent(campaign_id=campaign.id, name="Sole Free Agent", status=AgentState.AVAILABLE.value)
        b1 = Borrower(campaign_id=campaign.id, phone_number="+15550011111", name="B1")
        b2 = Borrower(campaign_id=campaign.id, phone_number="+15550022222", name="B2")
        session.add_all([agent, b1, b2])
        session.flush()

        c1 = Call(
            campaign_id=campaign.id,
            agent_id=None,
            borrower_id=b1.id,
            status=CallState.INITIATED.value,
            idempotency_key="conc-bridge-1",
            provider_name="provider_a",
            provider_call_id="prov-conc-1",
            dialing_mode="PREDICTIVE",
        )
        c2 = Call(
            campaign_id=campaign.id,
            agent_id=None,
            borrower_id=b2.id,
            status=CallState.INITIATED.value,
            idempotency_key="conc-bridge-2",
            provider_name="provider_a",
            provider_call_id="prov-conc-2",
            dialing_mode="PREDICTIVE",
        )
        session.add_all([c1, c2])
        session.commit()
        c1_id = c1.id
        c2_id = c2.id
        agent_id = agent.id

    results = {}

    def answer_call(c_id, evt_id):
        with SessionMaker() as s:
            r = EventProcessor.process_event(
                session=s,
                provider_name="provider_a",
                event_id=evt_id,
                call_id=c_id,
                event_type="ANSWERED",
                raw_payload={"answer_timeout_sec": 0.05},
            )
            s.commit()
            results[c_id] = r

    t1 = threading.Thread(target=answer_call, args=(c1_id, "evt-conc-1"))
    t2 = threading.Thread(target=answer_call, args=(c2_id, "evt-conc-2"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Check database: exactly ONE call has the agent; the other is ABANDONED
    with SessionMaker() as session:
        call_1 = session.get(Call, c1_id)
        call_2 = session.get(Call, c2_id)
        ag = session.get(Agent, agent_id)

        statuses = [call_1.status, call_2.status]
        assert CallState.CONNECTED.value in statuses
        assert CallState.FAILED.value in statuses

        connected_call = call_1 if call_1.status == CallState.CONNECTED.value else call_2
        abandoned_call = call_2 if connected_call == call_1 else call_1

        assert connected_call.agent_id == ag.id
        assert abandoned_call.agent_id is None
        assert abandoned_call.termination_reason == "ABANDONED"
        assert ag.status == AgentState.CONNECTED.value
        assert ag.current_call_id == connected_call.id


def test_agent_becomes_available_immediately_before_answer_timeout(sync_test_engine):
    """
    When an answering predictive call arrives and 0 agents are free,
    but an agent finishes WRAP_UP and transitions to AVAILABLE at t=100ms
    (before the 300ms timeout expires), BridgeAllocator MUST acquire the agent
    and connect the call rather than abandoning it.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Late Agent Freeing Campaign", mode=CampaignMode.PREDICTIVE.value)
        session.add(campaign)
        session.flush()

        # Agent is in WRAP_UP
        agent = Agent(campaign_id=campaign.id, name="Finishing Agent", status=AgentState.WRAP_UP.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15558887777", name="Late Borrower")
        session.add_all([agent, borrower])
        session.flush()

        call = Call(
            campaign_id=campaign.id,
            agent_id=None,
            borrower_id=borrower.id,
            status=CallState.INITIATED.value,
            idempotency_key="late-free-1",
            provider_name="provider_a",
            provider_call_id="prov-late-free",
            dialing_mode="PREDICTIVE",
        )
        session.add(call)
        session.commit()
        call_id = call.id
        agent_id = agent.id

    def free_agent_delayed():
        time.sleep(0.08)  # Free agent at 80ms
        with SessionMaker() as s:
            ag = s.get(Agent, agent_id)
            ag.status = AgentState.AVAILABLE.value
            s.commit()

    freer_thread = threading.Thread(target=free_agent_delayed)
    freer_thread.start()

    # Answer event arrives with 0.35s timeout
    with SessionMaker() as session:
        res = EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id="evt-late-free-ans",
            call_id=call_id,
            event_type="ANSWERED",
            raw_payload={"answer_timeout_sec": 0.35},
        )
        session.commit()

    freer_thread.join()

    # Call MUST have successfully bridged!
    with SessionMaker() as session:
        c = session.get(Call, call_id)
        ag = session.get(Agent, agent_id)
        assert c.status == CallState.CONNECTED.value
        assert c.agent_id == ag.id
        assert ag.status == AgentState.CONNECTED.value

