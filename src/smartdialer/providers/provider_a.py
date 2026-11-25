"""Provider A: Fast, highly reliable telecom carrier with deterministic event ordering."""

from datetime import datetime, timedelta, timezone
import random
import time
import uuid
from typing import Any, Dict, List, Optional
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from smartdialer.core.logging import get_logger
from smartdialer.db.session import sync_engine
from smartdialer.models.provider_request import ProviderRequestLedger
from smartdialer.providers.base import (
    ProviderInitiateResult,
    ProviderTerminateResult,
    SimulatedWebhookEvent,
    TelecomProvider,
)
from smartdialer.safety.provider_health import provider_health_tracker

logger = get_logger("provider_a")


class ProviderA(TelecomProvider):
    """
    Tier-1 mock telecom provider:
    - Ultra-fast latency (10-30ms)
    - 99% network reliability
    - Strictly ordered webhook delivery
    - Persistent database-backed idempotency ledger (FIND-CRIT-04 / FIX 5)
    """

    def __init__(self, failure_rate: float = 0.01) -> None:
        self._name = "provider_a"
        self.failure_rate = failure_rate
        self._in_memory_ledger: Dict[str, Dict[str, Any]] = {}
        self._carrier_calls: Dict[str, Dict[str, Any]] = {}

    @property
    def name(self) -> str:
        return self._name

    def query_call_by_key(self, idempotency_key: str) -> Optional[ProviderInitiateResult]:
        """Query carrier-side dispatch record by idempotency key."""
        if idempotency_key in self._carrier_calls:
            rec = self._carrier_calls[idempotency_key]
            return ProviderInitiateResult(
                success=rec["success"],
                provider_call_id=rec["provider_call_id"],
                latency_ms=0.0,
                error=rec.get("error"),
            )
        return None

    def initiate_call(
        self,
        call_id: uuid.UUID,
        phone_number: str,
        idempotency_key: str,
        callback_url: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> ProviderInitiateResult:
        """
        Initiate outbound call with durable autonomous database-backed idempotency.
        (FIND-CRIT-04 / Priority 2 Fix / FIX 4 Recovery)
        
        Guarantees:
        1. Dispatch intent ('PENDING') is committed in an autonomous transaction BEFORE carrier I/O.
        2. Final carrier acceptance ('DISPATCHED') is committed autonomously before returning.
        3. Worker crash or client transaction rollback CANNOT erase the ledger row.
        4. Re-dispatch finds the committed row and never places a duplicate phone call.
        5. Crashed workers in PENDING state are safely reconciled using provider-side query.
        """
        bind = session.get_bind() if session is not None else sync_engine
        AutonomousSession = sessionmaker(bind=bind, expire_on_commit=False)
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        fencing_token = uuid.uuid4()
        now = datetime.now(timezone.utc)
        proceed_to_dispatch = False

        wait_for_inflight = False

        # 1. Check database-backed idempotency ledger autonomously (NO lock held across polling)
        with AutonomousSession() as auto_session:
            stmt = select(ProviderRequestLedger).where(
                ProviderRequestLedger.provider_name == self._name,
                ProviderRequestLedger.idempotency_key == idempotency_key,
            )
            existing = auto_session.execute(stmt).scalar_one_or_none()
            if existing:
                if existing.status in ("DISPATCHED", "SUCCEEDED"):
                    logger.info(
                        "Provider A idempotency hit (database DISPATCHED)",
                        idempotency_key=idempotency_key,
                        provider_call_id=existing.provider_call_id,
                    )
                    return ProviderInitiateResult(
                        success=existing.success,
                        provider_call_id=existing.provider_call_id,
                        latency_ms=0.0,
                        error=existing.error_message,
                    )
                elif existing.status == "UNCERTAIN":
                    carrier_rec = self.query_call_by_key(idempotency_key)
                    if carrier_rec and carrier_rec.success:
                        existing.status = "DISPATCHED"
                        existing.provider_call_id = carrier_rec.provider_call_id
                        existing.success = True
                        auto_session.commit()
                        return carrier_rec
                    logger.warning(
                        "Provider A idempotency hit (database UNCERTAIN - outcome unconfirmed)",
                        idempotency_key=idempotency_key,
                    )
                    return ProviderInitiateResult(
                        success=False,
                        error="PREVIOUS_DISPATCH_UNCERTAIN: carrier outcome unconfirmed; refusing blind retry.",
                    )
                elif existing.status in ("PENDING", "IN_PROGRESS"):
                    is_stale = (
                        (existing.lease_expires_at is not None and existing.lease_expires_at <= now)
                        or (existing.created_at is not None and (now - existing.created_at).total_seconds() > 20.0)
                    )
                    if not is_stale:
                        wait_for_inflight = True
                    else:
                        # Lock row exclusively only to claim stale recovery
                        locked = auto_session.execute(stmt.with_for_update(skip_locked=True)).scalar_one_or_none()
                        if locked and locked.status in ("PENDING", "IN_PROGRESS"):
                            logger.warning(
                                "Found stale PENDING/IN_PROGRESS ledger entry from crashed worker; claiming recovery",
                                idempotency_key=idempotency_key,
                            )
                            carrier_rec = self.query_call_by_key(idempotency_key)
                            if carrier_rec and carrier_rec.success:
                                locked.status = "DISPATCHED"
                                locked.provider_call_id = carrier_rec.provider_call_id
                                locked.success = True
                                auto_session.commit()
                                return carrier_rec

                            locked.status = "IN_PROGRESS"
                            locked.worker_id = worker_id
                            locked.fencing_token = fencing_token
                            locked.lease_expires_at = now + timedelta(seconds=20)
                            locked.attempt_count += 1
                            auto_session.commit()
                            proceed_to_dispatch = True
                        else:
                            wait_for_inflight = True

        if wait_for_inflight:
            return self._wait_for_inflight_ledger(AutonomousSession, idempotency_key)

        if not proceed_to_dispatch:
            # 2. Check in-memory fallback cache
            if idempotency_key in self._in_memory_ledger:
                cached = self._in_memory_ledger[idempotency_key]
                logger.info("Provider A idempotency hit (in-memory)", idempotency_key=idempotency_key)
                return ProviderInitiateResult(
                    success=cached["success"],
                    provider_call_id=cached["provider_call_id"],
                    latency_ms=0.0,
                    error=cached.get("error"),
                )

            # 3. Autonomous intent registration: Persist PENDING state BEFORE carrier I/O
            wait_after_conflict = False
            with AutonomousSession() as auto_session:
                try:
                    ledger_entry = ProviderRequestLedger(
                        provider_name=self._name,
                        idempotency_key=idempotency_key,
                        call_id=call_id,
                        phone_number=phone_number,
                        status="PENDING",
                        worker_id=worker_id,
                        fencing_token=fencing_token,
                        lease_expires_at=now + timedelta(seconds=20),
                        attempt_count=1,
                        success=False,
                    )
                    auto_session.add(ledger_entry)
                    auto_session.commit()
                except IntegrityError:
                    auto_session.rollback()
                    stmt = select(ProviderRequestLedger).where(
                        ProviderRequestLedger.provider_name == self._name,
                        ProviderRequestLedger.idempotency_key == idempotency_key,
                    )
                    existing = auto_session.execute(stmt).scalar_one_or_none()
                    if existing and existing.status in ("DISPATCHED", "SUCCEEDED"):
                        return ProviderInitiateResult(
                            success=existing.success,
                            provider_call_id=existing.provider_call_id,
                            latency_ms=0.0,
                            error=existing.error_message,
                        )
                    elif existing and existing.status == "UNCERTAIN":
                        return ProviderInitiateResult(
                            success=False,
                            error="PREVIOUS_DISPATCH_UNCERTAIN: carrier outcome unconfirmed; refusing blind retry.",
                        )
                    elif existing and existing.status in ("PENDING", "IN_PROGRESS"):
                        wait_after_conflict = True

            if wait_after_conflict:
                return self._wait_for_inflight_ledger(AutonomousSession, idempotency_key)

        # 4. Invoke carrier (simulated network I/O)
        start_time = time.perf_counter()
        time.sleep(random.uniform(0.005, 0.015))
        elapsed_ms = (time.perf_counter() - start_time) * 1000

        # Simulate network drop
        if random.random() < self.failure_rate or not provider_health_tracker.is_healthy(self._name):
            provider_health_tracker.record(self._name, success=False, latency_ms=elapsed_ms)
            logger.warning("Provider A simulated network failure", call_id=str(call_id))
            with AutonomousSession() as auto_session:
                stmt = select(ProviderRequestLedger).where(
                    ProviderRequestLedger.provider_name == self._name,
                    ProviderRequestLedger.idempotency_key == idempotency_key,
                )
                entry = auto_session.execute(stmt).scalar_one_or_none()
                if entry:
                    entry.status = "FAILED"
                    entry.error_message = "Provider A network timeout or carrier congestion"
                    entry.latency_ms = elapsed_ms
                    auto_session.commit()
            return ProviderInitiateResult(
                success=False,
                error="Provider A network timeout or carrier congestion",
                latency_ms=elapsed_ms,
            )

        provider_call_id = f"provA-{uuid.uuid4().hex[:16]}"
        provider_health_tracker.record(self._name, success=True, latency_ms=elapsed_ms)
        self._carrier_calls[idempotency_key] = {
            "success": True,
            "provider_call_id": provider_call_id,
        }

        # 5. Durably commit DISPATCHED status autonomously
        with AutonomousSession() as auto_session:
            stmt = select(ProviderRequestLedger).where(
                ProviderRequestLedger.provider_name == self._name,
                ProviderRequestLedger.idempotency_key == idempotency_key,
            )
            entry = auto_session.execute(stmt).scalar_one_or_none()
            if entry:
                entry.provider_call_id = provider_call_id
                entry.status = "DISPATCHED"
                entry.success = True
                entry.latency_ms = elapsed_ms
                auto_session.commit()

        self._in_memory_ledger[idempotency_key] = {
            "success": True,
            "provider_call_id": provider_call_id,
            "error": None,
        }

        logger.info(
            "Provider A accepted call dispatch",
            call_id=str(call_id),
            provider_call_id=provider_call_id,
            latency_ms=round(elapsed_ms, 2),
        )

        return ProviderInitiateResult(
            success=True,
            provider_call_id=provider_call_id,
            latency_ms=elapsed_ms,
        )

    def _wait_for_inflight_ledger(self, AutonomousSession, idempotency_key: str) -> ProviderInitiateResult:
        """Poll ledger cleanly without holding any database row locks."""
        start_wait = time.monotonic()
        while time.monotonic() - start_wait < 2.0:
            time.sleep(0.01)
            with AutonomousSession() as check_session:
                entry = check_session.execute(
                    select(ProviderRequestLedger).where(
                        ProviderRequestLedger.provider_name == self._name,
                        ProviderRequestLedger.idempotency_key == idempotency_key,
                    )
                ).scalar_one_or_none()
                if entry and entry.status in ("DISPATCHED", "SUCCEEDED"):
                    return ProviderInitiateResult(
                        success=entry.success,
                        provider_call_id=entry.provider_call_id,
                        latency_ms=0.0,
                        error=entry.error_message,
                    )
                elif entry and entry.status == "UNCERTAIN":
                    return ProviderInitiateResult(
                        success=False,
                        error="PREVIOUS_DISPATCH_UNCERTAIN: carrier outcome unconfirmed; refusing blind retry.",
                    )
                elif entry and entry.status == "FAILED":
                    return ProviderInitiateResult(
                        success=False,
                        error=entry.error_message or "In-flight carrier dispatch failed.",
                    )
        return ProviderInitiateResult(
            success=False,
            error="CONCURRENT_DISPATCH_TIMEOUT: in-flight carrier dispatch did not complete within timeout.",
        )

    def terminate_call(self, provider_call_id: str) -> bool:
        """Hang up active call."""
        logger.info("Provider A call hung up", provider_call_id=provider_call_id)
        for rec in self._carrier_calls.values():
            if rec.get("provider_call_id") == provider_call_id:
                rec["terminated"] = True
        return True

    def generate_simulated_lifecycle_events(
        self,
        call_id: uuid.UUID,
        provider_call_id: str,
        outcome: str = "COMPLETED",
        duration_seconds: float = 60.0,
    ) -> List[SimulatedWebhookEvent]:
        """Generate strictly ordered event sequence."""
        events = [
            SimulatedWebhookEvent(
                event_id=f"evt-{uuid.uuid4().hex[:12]}",
                provider_name=self._name,
                call_id=call_id,
                event_type="INITIATED",
                payload={"provider_call_id": provider_call_id, "status": "initiated"},
            ),
            SimulatedWebhookEvent(
                event_id=f"evt-{uuid.uuid4().hex[:12]}",
                provider_name=self._name,
                call_id=call_id,
                event_type="RINGING",
                payload={"provider_call_id": provider_call_id, "status": "ringing"},
            ),
        ]

        if outcome == "COMPLETED":
            events.extend([
                SimulatedWebhookEvent(
                    event_id=f"evt-{uuid.uuid4().hex[:12]}",
                    provider_name=self._name,
                    call_id=call_id,
                    event_type="ANSWERED",
                    payload={"provider_call_id": provider_call_id, "status": "answered"},
                ),
                SimulatedWebhookEvent(
                    event_id=f"evt-{uuid.uuid4().hex[:12]}",
                    provider_name=self._name,
                    call_id=call_id,
                    event_type="CONNECTED",
                    payload={"provider_call_id": provider_call_id, "status": "connected"},
                ),
                SimulatedWebhookEvent(
                    event_id=f"evt-{uuid.uuid4().hex[:12]}",
                    provider_name=self._name,
                    call_id=call_id,
                    event_type="COMPLETED",
                    payload={
                        "provider_call_id": provider_call_id,
                        "status": "completed",
                        "duration_seconds": duration_seconds,
                    },
                ),
            ])
        else:
            events.append(
                SimulatedWebhookEvent(
                    event_id=f"evt-{uuid.uuid4().hex[:12]}",
                    provider_name=self._name,
                    call_id=call_id,
                    event_type="FAILED",
                    payload={
                        "provider_call_id": provider_call_id,
                        "status": "failed",
                        "reason": "NO_ANSWER_OR_BUSY",
                    },
                )
            )

        return events
