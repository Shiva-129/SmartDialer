"""
REGRESSION TEST SUITE: Durable Autonomous Provider Idempotency
Verifies Priority 2 from audit/POST_REPAIR_AUDIT.md:
- Autonomous transactions guarantee ProviderRequestLedger survives worker crashes.
- Session rollbacks cannot erase committed provider idempotency records.
- Concurrent workers cannot independently create duplicate carrier calls.
- Uncertain / timeout outcomes are represented explicitly and prevent blind retries.
"""

import threading
import uuid
import pytest
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from smartdialer.models.call import Call
from smartdialer.models.campaign import Campaign
from smartdialer.models.provider_request import ProviderRequestLedger
from smartdialer.providers.provider_a import ProviderA
from smartdialer.providers.provider_b import ProviderB
from smartdialer.providers.registry import provider_registry


def test_provider_acceptance_survives_worker_crash_and_session_rollback(sync_test_engine):
    """
    CRITICAL REGRESSION TEST for FIND-CRIT-04 / Priority 2:
    When a worker calls provider.initiate_call(), provider commits the ledger row autonomously.
    If the worker crashes or session.rollback() is invoked before main commit,
    the ledger row in PostgreSQL MUST persist.
    A subsequent dispatch attempt MUST retrieve the cached provider_call_id
    and NOT place a duplicate phone call!
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    provider = ProviderA(failure_rate=0.0)
    call_id = uuid.uuid4()
    idempotency_key = f"crash-test-{uuid.uuid4().hex[:12]}"
    phone = "+15551234567"

    # Step 1: Worker 1 calls initiate_call, but its session rolls back (simulating process crash / DB drop)
    with SessionMaker() as session1:
        res1 = provider.initiate_call(
            call_id=call_id,
            phone_number=phone,
            idempotency_key=idempotency_key,
            session=session1,
        )
        assert res1.success is True
        prov_call_id_1 = res1.provider_call_id
        assert prov_call_id_1 is not None

        # CRASH! Session rolls back!
        session1.rollback()

    # Step 2: Clear provider's in-memory cache to prove database durability!
    provider._in_memory_ledger.clear()

    # Step 3: Verify the ledger row exists in PostgreSQL despite session1.rollback()!
    with SessionMaker() as session:
        stmt = select(ProviderRequestLedger).where(
            ProviderRequestLedger.provider_name == provider.name,
            ProviderRequestLedger.idempotency_key == idempotency_key,
        )
        ledger_row = session.execute(stmt).scalar_one_or_none()
        assert ledger_row is not None, "Ledger row was rolled back! Autonomous transaction failed!"
        assert ledger_row.provider_call_id == prov_call_id_1
        assert ledger_row.status == "DISPATCHED"

    # Step 4: Worker 2 restarts and retries the same outbox event with a fresh provider instance
    fresh_provider = ProviderA(failure_rate=0.0)
    with SessionMaker() as session2:
        res2 = fresh_provider.initiate_call(
            call_id=call_id,
            phone_number=phone,
            idempotency_key=idempotency_key,
            session=session2,
        )
        session2.commit()

        # MUST return the identical provider_call_id (NO duplicate call!)
        assert res2.success is True
        assert res2.provider_call_id == prov_call_id_1


def test_concurrent_workers_cannot_create_duplicate_carrier_requests(sync_test_engine):
    """
    When two workers dispatch the exact same logical call concurrently,
    database constraint on (provider_name, idempotency_key) guarantees exactly ONE
    carrier call is dispatched. Both workers receive the same provider_call_id.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    provider = ProviderA(failure_rate=0.0)
    call_id = uuid.uuid4()
    idempotency_key = f"conc-prov-test-{uuid.uuid4().hex[:12]}"
    phone = "+15557778888"

    results = []

    def dispatch_worker():
        with SessionMaker() as s:
            p = ProviderA(failure_rate=0.0)
            res = p.initiate_call(
                call_id=call_id,
                phone_number=phone,
                idempotency_key=idempotency_key,
                session=s,
            )
            s.commit()
            results.append(res)

    t1 = threading.Thread(target=dispatch_worker)
    t2 = threading.Thread(target=dispatch_worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Both workers received results
    assert len(results) == 2
    successful_results = [r for r in results if r.success]
    assert len(successful_results) >= 1
    # If both succeeded, their provider_call_ids MUST be identical!
    if len(successful_results) == 2:
        assert successful_results[0].provider_call_id == successful_results[1].provider_call_id


def test_uncertain_timeout_outcome_prevents_blind_retry(sync_test_engine):
    """
    When Provider B returns an uncertain timeout (504 Gateway Error),
    the ledger status is set to 'UNCERTAIN'.
    A subsequent retry MUST NOT blindly place a duplicate outbound call.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    # Force 100% failure to simulate gateway timeout
    provider = ProviderB(failure_rate=1.0)
    call_id = uuid.uuid4()
    idempotency_key = f"uncertain-test-{uuid.uuid4().hex[:12]}"
    phone = "+15559990000"

    with SessionMaker() as session:
        res = provider.initiate_call(
            call_id=call_id,
            phone_number=phone,
            idempotency_key=idempotency_key,
            session=session,
        )
        assert res.success is False

    # Verify ledger row is UNCERTAIN in PostgreSQL
    with SessionMaker() as session:
        stmt = select(ProviderRequestLedger).where(
            ProviderRequestLedger.provider_name == provider.name,
            ProviderRequestLedger.idempotency_key == idempotency_key,
        )
        ledger_row = session.execute(stmt).scalar_one_or_none()
        assert ledger_row is not None
        assert ledger_row.status == "UNCERTAIN"

    # Now attempt a retry: system MUST refuse blind retry of unconfirmed outcome
    with SessionMaker() as session:
        fresh_provider = ProviderB(failure_rate=0.0)
        retry_res = fresh_provider.initiate_call(
            call_id=call_id,
            phone_number=phone,
            idempotency_key=idempotency_key,
            session=session,
        )
        assert retry_res.success is False
        assert "PREVIOUS_DISPATCH_UNCERTAIN" in retry_res.error
