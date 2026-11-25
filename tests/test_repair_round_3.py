"""
SMARTDIALER — REPAIR ROUND 3 COMPREHENSIVE INTEGRATION TEST SUITE
Independent Verification of Distributed Failure Mode Fixes (Stage 1-7):
- Test A: Predictive call answers -> no agent -> termination requested -> carrier confirms -> borrower released.
- Test B: Predictive call answers -> no agent -> termination times out -> borrower retained in non-eligible state -> retry -> confirmed -> released.
- Test C: Agent goes OFFLINE during ringing -> bridge rejected -> fallback agent claimed (or terminated if no agent).
- Test D: Provider call takes longer than lease duration -> heartbeat renews lease -> sweeper does not steal call/agent.
- Test E: Worker crashes after provider accepts call -> retry with same idempotency key returns existing provider_call_id, no duplicate carrier call.
- Test F: Worker crashes with ledger PENDING -> recovery reconciles request without CONCURRENT_DISPATCH_TIMEOUT.
- Test G: Provider termination raises exception -> worker lives -> retry scheduled -> final state reconciled.
- Test H: Two workers attempt termination/recovery concurrently -> atomic transition, no double release.
- Test I: Fencing token rejects late worker after lease lost -> rogue call terminated.
- Test J: Borrower cannot be called while previous call teardown is unconfirmed.
"""

import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional
import pytest
from sqlalchemy import select, update
from sqlalchemy.orm import sessionmaker

from smartdialer.allocator.allocator import CallAllocator
from smartdialer.allocator.bridge import BridgeAllocator
from smartdialer.events.processor import EventProcessor
from smartdialer.models.agent import Agent
from smartdialer.models.borrower import Borrower
from smartdialer.models.call import Call
from smartdialer.models.campaign import Campaign
from smartdialer.models.enums import AgentState, BorrowerState, CallState, CampaignMode, OutboxStatus, TerminationStatus
from smartdialer.models.outbox import OutboxEvent
from smartdialer.models.provider_request import ProviderRequestLedger
from smartdialer.providers.provider_a import ProviderA
from smartdialer.providers.provider_b import ProviderB
from smartdialer.providers.registry import provider_registry
from smartdialer.recovery.outbox_dispatcher import LeaseHeartbeat, OutboxDispatcher
from smartdialer.recovery.reconciler import ReconciliationSweeper
from smartdialer.services.termination_service import TerminationService


@pytest.fixture(autouse=True)
def reset_providers():
    prov_a = provider_registry.get("provider_a")
    prov_b = provider_registry.get("provider_b")
    prov_a.failure_rate = 0.0
    prov_b.failure_rate = 0.0
    yield
    prov_a.failure_rate = 0.0
    prov_b.failure_rate = 0.0


def test_a_predictive_answer_no_agent_termination_confirmed_borrower_released(sync_test_engine):
    """
    TEST A:
    Predictive call answers -> no agent -> termination requested -> carrier confirms -> borrower released.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Test A Campaign", mode=CampaignMode.PREDICTIVE.value)
        session.add(campaign)
        session.flush()

        borrower = Borrower(
            campaign_id=campaign.id,
            phone_number="+15551000001",
            name="Borrower A",
            status=BorrowerState.RESERVED.value,
            reservation_token=uuid.uuid4(),
            reserved_at=datetime.now(timezone.utc),
        )
        session.add(borrower)
        session.flush()

        call = Call(
            campaign_id=campaign.id,
            borrower_id=borrower.id,
            agent_id=None,
            status=CallState.INITIATED.value,
            idempotency_key="idemp-test-a",
            provider_name="provider_a",
            provider_call_id="provA-test-a-12345",
            dialing_mode="PREDICTIVE",
            termination_status=TerminationStatus.NOT_REQUIRED.value,
        )
        session.add(call)
        session.commit()
        call_id = call.id
        borrower_id = borrower.id

    # Simulated carrier call in Provider A
    prov = provider_registry.get("provider_a")
    prov._carrier_calls["idemp-test-a"] = {
        "success": True,
        "provider_call_id": "provA-test-a-12345",
        "terminated": False,
    }

    # Borrower answers webhook arrives, but no agent exists in the campaign
    with SessionMaker() as session:
        res = EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-ans-{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            event_type="ANSWERED",
            raw_payload={"answer_timeout_sec": 0.01},
        )
        session.commit()
        assert res.success is True

    # Assert Call is FAILED with reason ABANDONED, termination CONFIRMED
    with SessionMaker() as session:
        c = session.get(Call, call_id)
        b = session.get(Borrower, borrower_id)
        assert c.status == CallState.FAILED.value
        assert c.termination_reason == "ABANDONED"
        assert c.termination_status == TerminationStatus.CONFIRMED.value
        assert c.termination_confirmed_at is not None

        # Borrower MUST be released to ELIGIBLE because termination was confirmed
        assert b.status == BorrowerState.ELIGIBLE.value
        assert b.reservation_token is None

    # Telecom provider record must show carrier teardown
    assert prov._carrier_calls["idemp-test-a"]["terminated"] is True


def test_b_predictive_answer_no_agent_termination_timeout_retains_borrower_until_retry(sync_test_engine, monkeypatch):
    """
    TEST B:
    Predictive call answers -> no agent -> termination times out -> borrower retained in non-eligible state -> retry -> confirmed -> released.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Test B Campaign", mode=CampaignMode.PREDICTIVE.value)
        session.add(campaign)
        session.flush()

        res_token_b = uuid.uuid4()
        borrower = Borrower(
            campaign_id=campaign.id,
            phone_number="+15551000002",
            name="Borrower B",
            status=BorrowerState.RESERVED.value,
            reservation_token=res_token_b,
            reserved_at=datetime.now(timezone.utc),
        )
        session.add(borrower)
        session.flush()

        call = Call(
            campaign_id=campaign.id,
            borrower_id=borrower.id,
            agent_id=None,
            status=CallState.INITIATED.value,
            idempotency_key="idemp-test-b",
            provider_name="provider_a",
            provider_call_id="provA-test-b-99999",
            dialing_mode="PREDICTIVE",
            termination_status=TerminationStatus.NOT_REQUIRED.value,
        )
        session.add(call)
        session.commit()
        call_id = call.id
        borrower_id = borrower.id

    prov = provider_registry.get("provider_a")
    prov._carrier_calls["idemp-test-b"] = {
        "success": True,
        "provider_call_id": "provA-test-b-99999",
        "terminated": False,
    }

    # Simulate carrier termination timeout on initial attempt
    def mock_terminate_timeout(provider_call_id: str):
        raise TimeoutError("Carrier gateway 504 Gateway Timeout during termination")

    monkeypatch.setattr(prov, "terminate_call", mock_terminate_timeout)

    # Webhook arrives: ANSWERED with no agents available
    with SessionMaker() as session:
        EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-ans-{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            event_type="ANSWERED",
            raw_payload={"answer_timeout_sec": 0.01},
        )
        session.commit()

    # CRITICAL CHECK: Call is in ABANDONMENT_PENDING / FAILED with UNKNOWN termination status
    # Borrower MUST NOT be released to ELIGIBLE!
    with SessionMaker() as session:
        c = session.get(Call, call_id)
        b = session.get(Borrower, borrower_id)
        assert c.termination_status in (TerminationStatus.UNKNOWN.value, TerminationStatus.FAILED.value)
        assert c.termination_attempts >= 1
        assert c.next_termination_retry_at is not None

        # Safety invariant: Borrower is retained in non-eligible state while teardown unconfirmed
        assert b.status != BorrowerState.ELIGIBLE.value
        assert b.reservation_token == res_token_b

    # Now restore carrier connectivity and trigger termination retry
    monkeypatch.undo()
    prov._carrier_calls["idemp-test-b"]["terminated"] = False

    with SessionMaker() as session:
        # Accelerate retry time
        session.execute(
            update(Call)
            .where(Call.id == call_id)
            .values(next_termination_retry_at=datetime.now(timezone.utc) - timedelta(seconds=5))
        )
        session.commit()

        # Run termination retry worker
        retried_count = TerminationService.retry_pending_terminations(session=session)
        session.commit()
        assert retried_count >= 1

    # Now teardown is confirmed and borrower is released
    with SessionMaker() as session:
        c = session.get(Call, call_id)
        b = session.get(Borrower, borrower_id)
        assert c.termination_status == TerminationStatus.CONFIRMED.value
        assert c.status == CallState.FAILED.value
        assert b.status == BorrowerState.ELIGIBLE.value
        assert b.reservation_token is None


def test_c_agent_offline_during_ringing_rejected_fallback_agent_claimed(sync_test_engine):
    """
    TEST C:
    Agent goes OFFLINE during ringing -> bridge rejected -> fallback agent claimed (or terminated if no agent).
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Test C Campaign", mode=CampaignMode.PROGRESSIVE.value)
        session.add(campaign)
        session.flush()

        agent_1 = Agent(campaign_id=campaign.id, name="Agent 1 (Flaky)", status=AgentState.AVAILABLE.value)
        agent_2 = Agent(campaign_id=campaign.id, name="Agent 2 (Available Fallback)", status=AgentState.AVAILABLE.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15551000003", name="Borrower C")
        session.add_all([agent_1, agent_2, borrower])
        session.flush()

        alloc = CallAllocator.allocate_call(
            session=session,
            campaign_id=campaign.id,
            provider_name="provider_a",
            agent_id=agent_1.id,
            borrower_id=borrower.id,
        )
        session.commit()
        call_id = alloc.call.id
        agent_1_id = agent_1.id
        agent_2_id = agent_2.id

    # Call starts ringing: INITIATED -> RINGING
    with SessionMaker() as session:
        EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-init-{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            event_type="INITIATED",
        )
        EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-ring-{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            event_type="RINGING",
        )
        session.commit()

    # Agent 1 disconnects / goes OFFLINE during ringing!
    with SessionMaker() as session:
        ag1 = session.get(Agent, agent_1_id)
        ag1.status = AgentState.OFFLINE.value
        session.commit()

    # Webhook arrives: ANSWERED
    with SessionMaker() as session:
        res = EventProcessor.process_event(
            session=session,
            provider_name="provider_a",
            event_id=f"evt-ans-{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            event_type="ANSWERED",
            raw_payload={"answer_timeout_sec": 0.5},
        )
        session.commit()
        assert res.success is True

    # Assert Call was NOT bridged to OFFLINE Agent 1, but successfully re-bridged to Agent 2!
    with SessionMaker() as session:
        c = session.get(Call, call_id)
        ag1 = session.get(Agent, agent_1_id)
        ag2 = session.get(Agent, agent_2_id)

        assert c.agent_id == ag2.id
        assert c.agent_id != ag1.id
        assert ag1.status == AgentState.OFFLINE.value
        assert ag2.status == AgentState.CONNECTED.value
        assert ag2.current_call_id == c.id


def test_d_heartbeat_renews_lease_preventing_sweeper_preemption(sync_test_engine):
    """
    TEST D:
    Provider call takes longer than original lease duration -> heartbeat renews lease -> sweeper does not steal call/agent.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    worker_id = f"worker-test-d-{uuid.uuid4().hex[:6]}"
    fencing_token = uuid.uuid4()
    now = datetime.now(timezone.utc)

    with SessionMaker() as session:
        campaign = Campaign(name="Test D Campaign")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Agent D", status=AgentState.RESERVED.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15551000004", name="Borrower D")
        session.add_all([agent, borrower])
        session.flush()

        call = Call(
            campaign_id=campaign.id,
            agent_id=agent.id,
            borrower_id=borrower.id,
            status=CallState.RESERVED.value,
            idempotency_key="idemp-test-d",
            provider_name="provider_a",
            dispatch_token=fencing_token,
            created_at=now - timedelta(seconds=35),
        )
        session.add(call)
        session.flush()

        # Outbox event with very short 1.0s initial lease
        event = OutboxEvent(
            aggregate_type="CALL",
            aggregate_id=call.id,
            event_type="CALL_DISPATCH_REQUESTED",
            payload={"call_id": str(call.id)},
            locked_until=now + timedelta(seconds=1.0),
            worker_id=worker_id,
            fencing_token=fencing_token,
        )
        session.add(event)
        session.commit()
        event_id = event.id
        call_id = call.id

    # Start LeaseHeartbeat with 0.3s renewal interval and 2.0s extension
    heartbeat = LeaseHeartbeat(
        session_factory=SessionMaker,
        event_id=event_id,
        worker_id=worker_id,
        fencing_token=fencing_token,
        lease_duration_sec=2,
        interval_sec=0.3,
    )
    heartbeat.start()

    try:
        # Sleep 1.2s: longer than initial 1.0s lease!
        time.sleep(1.2)

        # Run Reconciler sweeper - should NOT steal the call/event because heartbeat renewed it
        with SessionMaker() as session:
            report = ReconciliationSweeper.run_sweep(session=session)
            session.commit()
            assert report.timed_out_calls_count == 0

            # Verify locked_until is still in the future
            ev = session.get(OutboxEvent, event_id)
            c = session.get(Call, call_id)
            assert ev.worker_id == worker_id
            assert ev.locked_until > datetime.now(timezone.utc)
            assert c.status == CallState.RESERVED.value
    finally:
        heartbeat.stop()


def test_e_worker_crashes_after_provider_accepts_call_retry_deduplicates(sync_test_engine):
    """
    TEST E:
    Worker crashes after provider accepts call -> retry with same idempotency key returns existing provider_call_id, no duplicate carrier call.
    """
    prov = provider_registry.get("provider_a")
    prov.failure_rate = 0.0
    idempotency_key = f"idemp-test-e-{uuid.uuid4().hex[:8]}"
    call_id = uuid.uuid4()
    phone = "+15551000005"

    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)

    # Worker 1 initiates call to provider
    with SessionMaker() as session:
        res1 = prov.initiate_call(
            call_id=call_id,
            phone_number=phone,
            idempotency_key=idempotency_key,
            session=session,
        )
        assert res1.success is True
        prov_call_id_1 = res1.provider_call_id

    # Simulate Worker 1 crashing before updating application call table!
    # Worker 2 restarts or retries with the SAME idempotency key
    with SessionMaker() as session:
        res2 = prov.initiate_call(
            call_id=call_id,
            phone_number=phone,
            idempotency_key=idempotency_key,
            session=session,
        )
        assert res2.success is True
        prov_call_id_2 = res2.provider_call_id

    # CRITICAL INVARIANT: Exact same provider call ID returned, NO second carrier call created!
    assert prov_call_id_1 == prov_call_id_2
    carrier_records = [
        k for k, v in prov._carrier_calls.items()
        if v.get("provider_call_id") == prov_call_id_1
    ]
    assert len(carrier_records) == 1


def test_f_worker_crashes_with_ledger_pending_recovery_reconciles_safely(sync_test_engine):
    """
    TEST F:
    Worker crashes with ledger PENDING -> recovery reconciles request without CONCURRENT_DISPATCH_TIMEOUT.
    """
    prov = provider_registry.get("provider_a")
    prov.failure_rate = 0.0
    idempotency_key = f"idemp-test-f-{uuid.uuid4().hex[:8]}"
    call_id = uuid.uuid4()
    phone = "+15551000006"
    now = datetime.now(timezone.utc)

    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)

    # Insert a stale crashed PENDING ledger entry (e.g. lease expired 30 seconds ago)
    with SessionMaker() as session:
        stale_entry = ProviderRequestLedger(
            provider_name="provider_a",
            idempotency_key=idempotency_key,
            call_id=call_id,
            phone_number=phone,
            status="PENDING",
            worker_id="dead-worker-999",
            fencing_token=uuid.uuid4(),
            lease_expires_at=now - timedelta(seconds=30),
            attempt_count=1,
            success=False,
        )
        session.add(stale_entry)
        session.commit()

    # Recovery worker dispatches with the same key
    with SessionMaker() as session:
        res = prov.initiate_call(
            call_id=call_id,
            phone_number=phone,
            idempotency_key=idempotency_key,
            session=session,
        )

    # MUST succeed without CONCURRENT_DISPATCH_TIMEOUT!
    assert res.success is True
    assert res.provider_call_id is not None
    assert "provA-" in res.provider_call_id

    with SessionMaker() as session:
        entry = session.execute(
            select(ProviderRequestLedger).where(
                ProviderRequestLedger.provider_name == "provider_a",
                ProviderRequestLedger.idempotency_key == idempotency_key,
            )
        ).scalar_one()
        assert entry.status == "DISPATCHED"
        assert entry.success is True
        assert entry.provider_call_id == res.provider_call_id


def test_g_provider_termination_exception_worker_survives_retry_reconciles(sync_test_engine, monkeypatch):
    """
    TEST G:
    Provider termination raises exception -> worker lives -> retry scheduled -> final state reconciled.
    """
    prov = provider_registry.get("provider_a")
    prov_call_id = f"provA-test-g-{uuid.uuid4().hex[:8]}"
    idemp_key = f"idemp-test-g-{uuid.uuid4().hex[:8]}"
    prov._carrier_calls[idemp_key] = {"success": True, "provider_call_id": prov_call_id, "terminated": False}

    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Test G Campaign")
        session.add(campaign)
        session.flush()

        borrower = Borrower(campaign_id=campaign.id, phone_number="+15551000007", name="Borrower G", status=BorrowerState.RESERVED.value)
        session.add(borrower)
        session.flush()

        call = Call(
            campaign_id=campaign.id,
            borrower_id=borrower.id,
            status=CallState.ABANDONMENT_PENDING.value,
            idempotency_key=idemp_key,
            provider_name="provider_a",
            provider_call_id=prov_call_id,
            termination_status=TerminationStatus.PENDING.value,
        )
        session.add(call)
        session.commit()
        call_id = call.id
        borrower_id = borrower.id

    # Carrier explodes with fatal network exception
    def mock_explode(provider_call_id: str):
        raise ConnectionResetError("Carrier SIP interface connection reset by peer")

    monkeypatch.setattr(prov, "terminate_call", mock_explode)

    # Worker executes termination: MUST NOT crash or raise!
    with SessionMaker() as session:
        result = TerminationService.execute_termination(
            session=session,
            call_id=call_id,
            provider_name="provider_a",
            target_terminal_state=CallState.FAILED.value,
            termination_reason="ABANDONED",
        )
        assert result.success is False
        assert result.status == TerminationStatus.UNKNOWN.value

    with SessionMaker() as session:
        c = session.get(Call, call_id)
        b = session.get(Borrower, borrower_id)
        assert c.termination_status in (TerminationStatus.UNKNOWN.value, TerminationStatus.FAILED.value)
        assert "connection reset" in c.termination_error.lower()
        # Borrower retained safely!
        assert b.status != BorrowerState.ELIGIBLE.value

    # Restore carrier and retry
    monkeypatch.undo()
    with SessionMaker() as session:
        session.execute(
            update(Call)
            .where(Call.id == call_id)
            .values(next_termination_retry_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

        retried = TerminationService.retry_pending_terminations(session=session)
        session.commit()
        assert retried >= 1

    with SessionMaker() as session:
        c = session.get(Call, call_id)
        b = session.get(Borrower, borrower_id)
        assert c.termination_status == TerminationStatus.CONFIRMED.value
        assert c.status == CallState.FAILED.value
        assert b.status == BorrowerState.ELIGIBLE.value


def test_h_concurrent_termination_workers_atomic_no_double_release(sync_test_engine):
    """
    TEST H:
    Two workers attempt termination/recovery concurrently -> atomic transition, no double release.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    prov = provider_registry.get("provider_a")
    prov_call_id = f"provA-test-h-{uuid.uuid4().hex[:8]}"
    idemp_key = f"idemp-test-h-{uuid.uuid4().hex[:8]}"
    prov._carrier_calls[idemp_key] = {"success": True, "provider_call_id": prov_call_id, "terminated": False}

    with SessionMaker() as session:
        campaign = Campaign(name="Test H Campaign")
        session.add(campaign)
        session.flush()

        borrower = Borrower(
            campaign_id=campaign.id,
            phone_number="+15551000008",
            name="Borrower H",
            status=BorrowerState.RESERVED.value,
            reservation_token=uuid.uuid4(),
            reserved_at=datetime.now(timezone.utc),
        )
        session.add(borrower)
        session.flush()

        call = Call(
            campaign_id=campaign.id,
            borrower_id=borrower.id,
            status=CallState.ABANDONMENT_PENDING.value,
            idempotency_key=idemp_key,
            provider_name="provider_a",
            provider_call_id=prov_call_id,
            termination_status=TerminationStatus.PENDING.value,
            next_termination_retry_at=datetime.now(timezone.utc) - timedelta(seconds=10),
        )
        session.add(call)
        session.commit()
        call_id = call.id
        borrower_id = borrower.id

    results = []
    errors = []

    def worker_run():
        try:
            with SessionMaker() as s:
                cnt = TerminationService.retry_pending_terminations(session=s)
                s.commit()
                results.append(cnt)
        except Exception as ex:
            errors.append(ex)

    t1 = threading.Thread(target=worker_run)
    t2 = threading.Thread(target=worker_run)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(errors) == 0
    # Exactly one worker processed the retry, the other skipped or found 0
    assert sum(results) == 1

    with SessionMaker() as session:
        c = session.get(Call, call_id)
        b = session.get(Borrower, borrower_id)
        assert c.termination_status == TerminationStatus.CONFIRMED.value
        assert b.status == BorrowerState.ELIGIBLE.value
        assert b.reservation_token is None


def test_i_fencing_token_rejects_late_worker_and_tears_down_rogue_call(sync_test_engine):
    """
    TEST I:
    Fencing token rejects late worker after lease lost -> rogue call terminated.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    token_a = uuid.uuid4()
    token_b = uuid.uuid4()
    now = datetime.now(timezone.utc)

    with SessionMaker() as session:
        campaign = Campaign(name="Test I Campaign")
        session.add(campaign)
        session.flush()

        agent = Agent(campaign_id=campaign.id, name="Agent I", status=AgentState.RESERVED.value)
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15551000009", name="Borrower I")
        session.add_all([agent, borrower])
        session.flush()

        # Call has been stolen by Worker B (token_b)
        call = Call(
            campaign_id=campaign.id,
            agent_id=agent.id,
            borrower_id=borrower.id,
            status=CallState.RESERVED.value,
            idempotency_key="idemp-test-i",
            provider_name="provider_a",
            dispatch_token=token_b,
        )
        session.add(call)
        session.flush()

        # Outbox event is owned by Worker B
        event = OutboxEvent(
            aggregate_type="CALL",
            aggregate_id=call.id,
            event_type="CALL_DISPATCH_REQUESTED",
            payload={"call_id": str(call.id)},
            locked_until=now + timedelta(seconds=30),
            worker_id="worker-b",
            fencing_token=token_b,
        )
        session.add(event)
        session.commit()
        call_id = call.id
        event_id = event.id

    prov = provider_registry.get("provider_a")
    rogue_prov_call_id = "provA-rogue-call-999"
    prov._carrier_calls["idemp-test-i"] = {
        "success": True,
        "provider_call_id": rogue_prov_call_id,
        "terminated": False,
    }

    # Worker A returns late holding stale token_a, having placed rogue_prov_call_id
    with SessionMaker() as session:
        # Worker A attempts conditional write with stale fencing token
        update_stmt = (
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id, OutboxEvent.fencing_token == token_a)
            .values(status=OutboxStatus.DISPATCHED.value)
        )
        res = session.execute(update_stmt)
        session.commit()

        # Fencing check rejected stale write!
        assert res.rowcount == 0

        # Worker A detects fence breach and must tear down rogue call
        teardown_res = TerminationService.execute_termination(
            session=session,
            call_id=call_id,
            provider_name="provider_a",
            provider_call_id=rogue_prov_call_id,
            target_terminal_state=CallState.FAILED.value,
            termination_reason="ROGUE_DISPATCH_FENCE_BREACH",
        )
        assert teardown_res.success is True
        assert teardown_res.status == TerminationStatus.CONFIRMED.value

    # Carrier call was hung up!
    assert prov._carrier_calls["idemp-test-i"]["terminated"] is True


def test_j_borrower_cannot_be_called_while_teardown_unconfirmed(sync_test_engine):
    """
    TEST J:
    Borrower cannot be called while previous call teardown is unconfirmed.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Test J Campaign", mode=CampaignMode.PROGRESSIVE.value)
        session.add(campaign)
        session.flush()

        agent_1 = Agent(campaign_id=campaign.id, name="Agent J1", status=AgentState.AVAILABLE.value)
        agent_2 = Agent(campaign_id=campaign.id, name="Agent J2", status=AgentState.AVAILABLE.value)
        # Borrower is in RESERVED state with unconfirmed termination on previous call
        borrower = Borrower(
            campaign_id=campaign.id,
            phone_number="+15551000010",
            name="Borrower J",
            status=BorrowerState.RESERVED.value,
            reservation_token=uuid.uuid4(),
            reserved_at=datetime.now(timezone.utc),
        )
        session.add_all([agent_1, agent_2, borrower])
        session.flush()

        # Prior call has UNKNOWN termination status
        call_1 = Call(
            campaign_id=campaign.id,
            borrower_id=borrower.id,
            agent_id=None,
            status=CallState.ABANDONMENT_PENDING.value,
            idempotency_key="idemp-test-j-1",
            provider_name="provider_a",
            provider_call_id="provA-test-j-old",
            termination_status=TerminationStatus.UNKNOWN.value,
        )
        session.add(call_1)
        session.commit()
        call_1_id = call_1.id
        borrower_id = borrower.id
        campaign_id = campaign.id
        agent_2_id = agent_2.id

    # Attempt to allocate another call to Borrower J
    with SessionMaker() as session:
        with pytest.raises(Exception):  # BorrowerNotEligibleError
            CallAllocator.allocate_call(
                session=session,
                campaign_id=campaign_id,
                provider_name="provider_a",
                agent_id=agent_2_id,
                borrower_id=borrower_id,
            )

    # Carrier teardown is now confirmed!
    prov = provider_registry.get("provider_a")
    prov._carrier_calls["idemp-test-j-1"] = {"success": True, "provider_call_id": "provA-test-j-old", "terminated": False}

    with SessionMaker() as session:
        TerminationService.execute_termination(
            session=session,
            call_id=call_1_id,
            provider_name="provider_a",
            target_terminal_state=CallState.FAILED.value,
            termination_reason="ABANDONED",
        )
        session.commit()

    # Now borrower is ELIGIBLE and can be allocated!
    with SessionMaker() as session:
        b = session.get(Borrower, borrower_id)
        assert b.status == BorrowerState.ELIGIBLE.value
        assert b.reservation_token is None

        alloc = CallAllocator.allocate_call(
            session=session,
            campaign_id=campaign_id,
            provider_name="provider_a",
            agent_id=agent_2_id,
            borrower_id=borrower_id,
        )
        session.commit()
        assert alloc.call is not None
        assert alloc.call.borrower_id == borrower_id
