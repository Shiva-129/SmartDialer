"""Regression tests for FIND-CRIT-04 (FIX 5): Provider persistent database-backed idempotency ledger."""

import concurrent.futures
import uuid
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from smartdialer.models.provider_request import ProviderRequestLedger
from smartdialer.providers.provider_a import ProviderA
from smartdialer.providers.provider_b import ProviderB


def test_provider_a_idempotency_returns_identical_call_id(sync_test_engine):
    """
    REGRESSION TEST for FIND-CRIT-04:
    Repeated calls with the same idempotency key must return the exact same provider_call_id.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    provider = ProviderA(failure_rate=0.0)
    idemp_key = f"idemp-test-{uuid.uuid4().hex}"
    call_id = uuid.uuid4()
    phone = "+15554445555"

    with SessionMaker() as session:
        res1 = provider.initiate_call(call_id=call_id, phone_number=phone, idempotency_key=idemp_key, session=session)
        session.commit()

    assert res1.success is True
    prov_call_id_1 = res1.provider_call_id
    assert prov_call_id_1 is not None

    with SessionMaker() as session:
        res2 = provider.initiate_call(call_id=call_id, phone_number=phone, idempotency_key=idemp_key, session=session)
        session.commit()

    assert res2.success is True
    assert res2.provider_call_id == prov_call_id_1, (
        f"DEFECT: Repeated call returned different provider_call_id! ({res2.provider_call_id} != {prov_call_id_1})"
    )


def test_provider_idempotency_survives_process_restart(sync_test_engine):
    """
    REGRESSION TEST for FIND-CRIT-04:
    A new provider instance (simulating a restarted worker process) must query the database
    idempotency ledger and return the same provider_call_id instead of dialing again.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    idemp_key = f"idemp-restart-{uuid.uuid4().hex}"
    call_id = uuid.uuid4()
    phone = "+15556667777"

    # Worker 1 runs
    worker1_provider = ProviderA(failure_rate=0.0)
    with SessionMaker() as session:
        res1 = worker1_provider.initiate_call(call_id=call_id, phone_number=phone, idempotency_key=idemp_key, session=session)
        session.commit()

    prov_call_id_1 = res1.provider_call_id

    # Worker 1 dies, Worker 2 starts fresh with empty in-memory state
    worker2_provider = ProviderA(failure_rate=0.0)
    assert len(worker2_provider._in_memory_ledger) == 0

    with SessionMaker() as session:
        res2 = worker2_provider.initiate_call(call_id=call_id, phone_number=phone, idempotency_key=idemp_key, session=session)
        session.commit()

    assert res2.provider_call_id == prov_call_id_1, (
        f"CRITICAL DEFECT: New worker instance did not recognize existing DB ledger entry! "
        f"({res2.provider_call_id} != {prov_call_id_1})"
    )

    # Verify exactly 1 ledger record exists in PostgreSQL
    with SessionMaker() as session:
        count = session.execute(
            select(func.count(ProviderRequestLedger.id)).where(
                ProviderRequestLedger.provider_name == "provider_a",
                ProviderRequestLedger.idempotency_key == idemp_key,
            )
        ).scalar()
        assert count == 1


def test_provider_b_concurrent_calls_same_key_deduplicate(sync_test_engine):
    """
    REGRESSION TEST for FIND-CRIT-04:
    Multiple concurrent worker threads dispatching the same idempotency key to Provider B
    must all resolve to the same provider_call_id without database uniqueness violation crash.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    idemp_key = f"idemp-concurrent-{uuid.uuid4().hex}"
    call_id = uuid.uuid4()
    phone = "+15558889999"

    def worker_dispatch():
        provider = ProviderB(failure_rate=0.0)
        with SessionMaker() as session:
            res = provider.initiate_call(call_id=call_id, phone_number=phone, idempotency_key=idemp_key, session=session)
            session.commit()
            return res.provider_call_id

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(worker_dispatch) for _ in range(4)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert len(results) == 4
    # All 4 workers must receive the exact same provider_call_id
    assert len(set(results)) == 1, f"Concurrent dispatches produced different call IDs: {results}"
