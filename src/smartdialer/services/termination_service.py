"""Durable carrier call termination service with retry scheduling, exception safety, and borrower retention."""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

from smartdialer.core.logging import get_logger
from smartdialer.models.borrower import Borrower
from smartdialer.models.call import Call
from smartdialer.models.enums import BorrowerState, CallState, TerminationStatus
from smartdialer.providers.base import ProviderTerminateResult
from smartdialer.providers.registry import provider_registry
from smartdialer.state.call_state_machine import CallStateMachine

logger = get_logger("termination_service")


class TerminationService:
    """
    Manages durable telecommunication carrier call termination.
    
    Guarantees:
    1. Invokes provider.terminate_call() outside database transaction to avoid holding locks.
    2. Inspects provider response; distinguishes confirmed success from uncertain timeouts.
    3. Traps network exceptions without crashing worker loops.
    4. NEVER releases a borrower to ELIGIBLE until carrier teardown is confirmed.
    5. Retries pending, failed, or uncertain terminations with exponential backoff.
    6. Ensures duplicate termination attempts are strictly idempotent.
    """

    MAX_BACKOFF_SECONDS: float = 60.0
    BASE_BACKOFF_SECONDS: float = 2.0

    @classmethod
    def execute_termination(
        cls,
        session: Session,
        call_id: uuid.UUID,
        provider_name: Optional[str] = None,
        provider_call_id: Optional[str] = None,
        target_terminal_state: str = CallState.FAILED.value,
        termination_reason: Optional[str] = None,
    ) -> ProviderTerminateResult:
        """
        Atomically initiate and resolve carrier call termination.
        
        If provider confirms teardown, transitions call to target_terminal_state
        and safely releases the borrower. If uncertain or failed, retains borrower reservation.
        """
        now = datetime.now(timezone.utc)

        # 1. Inspect call in database
        call = session.get(Call, call_id, with_for_update=True)
        if not call:
            logger.warning("Termination requested for non-existent call", call_id=str(call_id))
            return ProviderTerminateResult(success=False, status=TerminationStatus.FAILED.value, error="Call not found")

        # Idempotency & concurrency checks: Already confirmed or actively in-progress by another worker!
        if call.termination_status == TerminationStatus.CONFIRMED.value:
            logger.info("Call termination already confirmed", call_id=str(call_id))
            return ProviderTerminateResult(success=True, status=TerminationStatus.CONFIRMED.value)

        if call.termination_status == TerminationStatus.IN_PROGRESS.value:
            is_stale_in_progress = (
                call.updated_at is not None
                and (now - call.updated_at).total_seconds() > 30.0
            )
            if not is_stale_in_progress:
                logger.info("Call termination already in progress by another worker", call_id=str(call_id))
                return ProviderTerminateResult(
                    success=False,
                    status=TerminationStatus.IN_PROGRESS.value,
                    error="Concurrent termination already in progress.",
                )

        p_name = provider_name or call.provider_name
        p_call_id = provider_call_id or call.provider_call_id

        if not p_call_id or not p_name:
            # No carrier call was actually placed
            call.termination_status = TerminationStatus.CONFIRMED.value
            call.termination_confirmed_at = now
            if call.status == CallState.ABANDONMENT_PENDING.value:
                CallStateMachine.transition(call, target_terminal_state, termination_reason=termination_reason or call.termination_reason)
            cls._release_borrower_if_confirmed(session, call)
            session.flush()
            return ProviderTerminateResult(success=True, status=TerminationStatus.CONFIRMED.value)

        provider = provider_registry.get(p_name)
        if not provider:
            call.termination_status = TerminationStatus.UNKNOWN.value
            call.termination_error = f"Provider '{p_name}' not found in registry"
            session.flush()
            return ProviderTerminateResult(success=False, status=TerminationStatus.UNKNOWN.value, error="Provider not found")

        # 2. Mark termination in-progress
        call.termination_status = TerminationStatus.IN_PROGRESS.value
        call.termination_attempts += 1
        session.commit()

        # 3. Invoke carrier teardown outside database transaction
        term_res: ProviderTerminateResult
        try:
            raw_res = provider.terminate_call(p_call_id)
            if isinstance(raw_res, ProviderTerminateResult):
                term_res = raw_res
            elif raw_res is True:
                term_res = ProviderTerminateResult(success=True, status=TerminationStatus.CONFIRMED.value)
            else:
                term_res = ProviderTerminateResult(
                    success=False,
                    status=TerminationStatus.FAILED.value,
                    error="Carrier returned false during call termination",
                )
        except (TimeoutError, OSError) as exc:
            logger.warning(
                "Carrier call termination timed out or socket error",
                call_id=str(call_id),
                prov_id=p_call_id,
                error=str(exc),
            )
            term_res = ProviderTerminateResult(
                success=False,
                status=TerminationStatus.UNKNOWN.value,
                error=f"Carrier termination timeout / socket error: {exc}",
            )
        except Exception as exc:
            logger.error(
                "Unexpected exception during carrier call termination",
                call_id=str(call_id),
                prov_id=p_call_id,
                error=str(exc),
            )
            term_res = ProviderTerminateResult(
                success=False,
                status=TerminationStatus.UNKNOWN.value,
                error=f"Carrier termination exception: {exc}",
            )

        # 4. Record outcome in fresh transaction with lock
        session.expire_all()
        call = session.get(Call, call_id, with_for_update=True)
        if not call:
            return term_res

        now_after = datetime.now(timezone.utc)
        if term_res.success and term_res.status == TerminationStatus.CONFIRMED.value:
            call.termination_status = TerminationStatus.CONFIRMED.value
            call.termination_confirmed_at = now_after
            call.termination_error = None
            call.next_termination_retry_at = None

            # Advance state machine if needed
            if call.status in (CallState.ABANDONMENT_PENDING.value, CallState.ANSWERED.value, CallState.RINGING.value, CallState.INITIATED.value):
                reason = termination_reason or call.termination_reason or "ABANDONED"
                CallStateMachine.transition(call, target_terminal_state, termination_reason=reason)

            # Release borrower ONLY on confirmed teardown!
            cls._release_borrower_if_confirmed(session, call)
            logger.info("Carrier call termination confirmed and borrower released", call_id=str(call_id), prov_id=p_call_id)
        else:
            # Uncertain outcome or failure: schedule retry and DO NOT release borrower!
            call.termination_status = term_res.status
            call.termination_error = term_res.error
            backoff_sec = min(cls.MAX_BACKOFF_SECONDS, cls.BASE_BACKOFF_SECONDS ** min(call.termination_attempts, 6))
            call.next_termination_retry_at = now_after + timedelta(seconds=backoff_sec)
            logger.warning(
                "Carrier call termination unresolved; borrower retained in non-eligible state",
                call_id=str(call_id),
                status=term_res.status,
                attempts=call.termination_attempts,
                next_retry_sec=backoff_sec,
                error=term_res.error,
            )

        session.commit()
        return term_res

    @classmethod
    def retry_pending_terminations(cls, session: Session, limit: int = 20) -> int:
        """
        Sweep and retry pending, unknown, or failed carrier terminations whose retry backoff has elapsed.
        Uses FOR UPDATE SKIP LOCKED + conditional claim to prevent concurrent workers from double-hanging up.
        Returns count of newly confirmed terminations executed by this worker.
        """
        now = datetime.now(timezone.utc)
        stale_in_progress_cutoff = now - timedelta(seconds=30)
        stmt = (
            select(Call.id)
            .where(
                (
                    Call.termination_status.in_([
                        TerminationStatus.PENDING.value,
                        TerminationStatus.UNKNOWN.value,
                        TerminationStatus.FAILED.value,
                    ])
                )
                | (
                    (Call.termination_status == TerminationStatus.IN_PROGRESS.value)
                    & (Call.updated_at < stale_in_progress_cutoff)
                ),
                (Call.next_termination_retry_at.is_(None)) | (Call.next_termination_retry_at <= now),
            )
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        call_ids = session.execute(stmt).scalars().all()
        confirmed_count = 0

        for cid in call_ids:
            try:
                pre_call = session.get(Call, cid)
                if not pre_call or pre_call.termination_status == TerminationStatus.CONFIRMED.value:
                    continue
                res = cls.execute_termination(session=session, call_id=cid)
                if res.success and res.status == TerminationStatus.CONFIRMED.value:
                    confirmed_count += 1
            except Exception as exc:
                logger.error("Failed to retry termination for call", call_id=str(cid), error=str(exc))

        return confirmed_count

    @staticmethod
    def _release_borrower_if_confirmed(session: Session, call: Call) -> None:
        """Safely transition borrower state only after carrier teardown confirmation."""
        if not call.borrower_id:
            return
        borrower = session.get(Borrower, call.borrower_id, with_for_update=True)
        if borrower:
            if borrower.call_attempts < 3:
                borrower.status = BorrowerState.ELIGIBLE.value
            else:
                borrower.status = BorrowerState.FAILED.value
            borrower.reservation_token = None
            borrower.reserved_at = None
