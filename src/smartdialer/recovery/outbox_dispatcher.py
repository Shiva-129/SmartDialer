"""Transactional Outbox dispatcher with idempotent dispatch, lease locking, and retry handling."""

import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from sqlalchemy import select, update
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.exc import StaleDataError

from smartdialer.core.logging import get_logger
from smartdialer.models.agent import Agent
from smartdialer.models.borrower import Borrower
from smartdialer.models.call import Call
from smartdialer.models.campaign import Campaign
from smartdialer.models.enums import AgentState, BorrowerState, CallState, OutboxStatus
from smartdialer.models.outbox import OutboxEvent
from smartdialer.providers.base import ProviderInitiateResult
from smartdialer.providers.registry import provider_registry
from smartdialer.safety.provider_health import provider_health_tracker
from smartdialer.services.termination_service import TerminationService
from smartdialer.state.agent_state_machine import AgentStateMachine
from smartdialer.state.call_state_machine import CallStateMachine

logger = get_logger("outbox_dispatcher")


class LeaseHeartbeat:
    """Background daemon thread maintaining periodic outbox lease renewal conditional on worker identity and fencing token."""

    def __init__(
        self,
        session_factory,
        event_id: uuid.UUID,
        worker_id: str,
        fencing_token: uuid.UUID,
        lease_duration_sec: int = 20,
        interval_sec: float = 5.0,
    ) -> None:
        self.session_factory = session_factory
        self.event_id = event_id
        self.worker_id = worker_id
        self.fencing_token = fencing_token
        self.lease_duration_sec = lease_duration_sec
        self.interval_sec = interval_sec
        self.stop_event = threading.Event()
        self.ownership_lost = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def _run(self) -> None:
        while not self.stop_event.wait(self.interval_sec):
            try:
                with self.session_factory() as s:
                    now = datetime.now(timezone.utc)
                    new_locked_until = now + timedelta(seconds=self.lease_duration_sec)
                    stmt = (
                        update(OutboxEvent)
                        .where(
                            OutboxEvent.id == self.event_id,
                            OutboxEvent.worker_id == self.worker_id,
                            OutboxEvent.fencing_token == self.fencing_token,
                            OutboxEvent.status == OutboxStatus.PENDING.value,
                        )
                        .values(locked_until=new_locked_until)
                    )
                    res = s.execute(stmt)
                    s.commit()
                    if res.rowcount == 0:
                        logger.warning(
                            "Lease heartbeat detected lost ownership or preemption",
                            event_id=str(self.event_id),
                            worker_id=self.worker_id,
                        )
                        self.ownership_lost = True
                        break
            except Exception as exc:
                logger.warning("Error renewing outbox lease heartbeat", error=str(exc))


class OutboxDispatcher:
    """
    Processes pending outbox events.
    
    Guarantees:
    1. Pre-dispatch verification: Never dispatches a call without an active, non-expired,
       valid reservation for the assigned agent (FIND-CRIT-02 / FIX 2).
    2. Per-record independent transactions: Never holds locks across external provider HTTP
       calls; commits each record independently so crashes midway cannot cause duplicate
       calls upon retry (FIND-CRIT-03 / FIX 3).
    3. Uses SELECT ... FOR UPDATE SKIP LOCKED with lease locking to prevent multiple workers
       from processing the same outbox event concurrently.
    4. Retries reuse the EXACT same idempotency key so the telecom provider never creates duplicates.
    """

    MAX_RETRIES: int = 3
    LEASE_DURATION_SECONDS: int = 20
    HEARTBEAT_INTERVAL_SECONDS: float = 5.0
    RESERVATION_TIMEOUT_SECONDS: int = 30

    @classmethod
    def dispatch_pending(
        cls,
        session: Session,
        limit: int = 20,
        call_ids: Optional[List[uuid.UUID]] = None,
    ) -> List[uuid.UUID]:
        """
        Poll, lock, and dispatch pending outbox events.
        
        Returns list of successfully dispatched call IDs.
        """
        now = datetime.now(timezone.utc)
        dispatched_ids: List[uuid.UUID] = []

        # 1. Fetch pending events with row-level lock and lease check
        filters = [
            OutboxEvent.status == OutboxStatus.PENDING.value,
            (OutboxEvent.locked_until.is_(None)) | (OutboxEvent.locked_until < now),
        ]
        if call_ids is not None:
            if not call_ids:
                return dispatched_ids
            filters.append(OutboxEvent.aggregate_id.in_(call_ids))

        stmt = (
            select(OutboxEvent)
            .where(*filters)
            .order_by(OutboxEvent.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        events = session.execute(stmt).scalars().all()
        if not events:
            return dispatched_ids

        event_ids = [e.id for e in events]
        session.commit()

        # 2. Process each event in its own isolated transaction boundary
        for event_id in event_ids:
            try:
                dispatched_call_id = cls._dispatch_single_record(session, event_id)
                if dispatched_call_id:
                    dispatched_ids.append(dispatched_call_id)
            except Exception as exc:
                logger.error(
                    "Unexpected error during outbox record dispatch",
                    event_id=str(event_id),
                    error=str(exc),
                )
                try:
                    session.rollback()
                except Exception:
                    pass

        return dispatched_ids

    @classmethod
    def _dispatch_single_record(cls, session: Session, event_id: uuid.UUID) -> Optional[uuid.UUID]:
        """Process one outbox record with strict pre-dispatch validation and independent commit."""
        now = datetime.now(timezone.utc)

        # Step 1: Lock outbox record with FOR UPDATE
        stmt = select(OutboxEvent).where(OutboxEvent.id == event_id).with_for_update(skip_locked=True)
        event = session.execute(stmt).scalar_one_or_none()
        if not event or event.status != OutboxStatus.PENDING.value:
            session.rollback()
            return None

        call_id = event.aggregate_id
        payload = event.payload or {}
        provider_name = payload.get("provider_name", "provider_a")
        idempotency_key = payload.get("idempotency_key", str(uuid.uuid4()))
        phone_number = payload.get("phone_number", "")

        # Step 2: Lock and inspect associated Call
        call = session.get(Call, call_id, with_for_update=True)
        if not call:
            event.status = OutboxStatus.FAILED.value
            event.last_error = f"Call record {call_id} not found."
            event.locked_until = None
            session.commit()
            return None

        if call.status != CallState.RESERVED.value:
            # Call was already cancelled, completed, or failed
            event.status = OutboxStatus.CANCELLED.value
            event.last_error = f"Call is in status '{call.status}', not eligible for dispatch."
            event.locked_until = None
            session.commit()
            return None

        # Step 3: Lock and inspect associated Agent
        agent = None
        if not call.agent_id:
            if call.dialing_mode != "PREDICTIVE":
                event.status = OutboxStatus.CANCELLED.value
                event.last_error = "No agent assigned to call reservation."
                event.locked_until = None
                CallStateMachine.transition(call, CallState.CANCELLED, termination_reason="NO_ASSIGNED_AGENT")
                cls._release_borrower(session, call.borrower_id)
                session.commit()
                return None
            # In PREDICTIVE mode, agent-less call dials ahead; agent bridged upon ANSWERED
        else:
            agent = session.get(Agent, call.agent_id, with_for_update=True)
            if not agent:
                event.status = OutboxStatus.CANCELLED.value
                event.last_error = f"Agent {call.agent_id} not found in database."
                event.locked_until = None
                CallStateMachine.transition(call, CallState.CANCELLED, termination_reason="AGENT_NOT_FOUND")
                cls._release_borrower(session, call.borrower_id)
                session.commit()
                return None

            # Critical Check 3a: Agent MUST be in RESERVED state
            if agent.status != AgentState.RESERVED.value:
                logger.warning(
                    "Pre-dispatch validation failed: Agent is not in RESERVED state",
                    call_id=str(call_id),
                    agent_id=str(agent.id),
                    agent_status=agent.status,
                )
                event.status = OutboxStatus.CANCELLED.value
                event.last_error = f"Agent status is '{agent.status}', expected 'RESERVED'."
                event.locked_until = None
                CallStateMachine.transition(call, CallState.CANCELLED, termination_reason=f"AGENT_STATE_{agent.status}")
                cls._release_borrower(session, call.borrower_id)
                if agent.current_call_id == call.id:
                    agent.current_call_id = None
                    agent.reservation_token = None
                session.commit()
                return None

            # Critical Check 3b: Agent reservation timeout check
            if agent.reserved_at and (now - agent.reserved_at).total_seconds() > cls.RESERVATION_TIMEOUT_SECONDS:
                logger.warning(
                    "Pre-dispatch validation failed: Agent reservation expired",
                    call_id=str(call_id),
                    agent_id=str(agent.id),
                    reserved_at=str(agent.reserved_at),
                )
                event.status = OutboxStatus.CANCELLED.value
                event.last_error = "Agent reservation expired before provider dispatch."
                event.locked_until = None
                CallStateMachine.transition(call, CallState.CANCELLED, termination_reason="RESERVATION_EXPIRED")
                AgentStateMachine.transition(agent, AgentState.AVAILABLE)
                cls._release_borrower(session, call.borrower_id)
                session.commit()
                return None

        # Critical Check 3c: Campaign active check
        campaign = session.get(Campaign, call.campaign_id)
        if campaign and campaign.status != "ACTIVE":
            logger.warning(
                "Pre-dispatch validation failed: Campaign is inactive",
                call_id=str(call_id),
                campaign_id=str(campaign.id),
                campaign_status=campaign.status,
            )
            event.status = OutboxStatus.CANCELLED.value
            event.last_error = f"Campaign is '{campaign.status}', not ACTIVE."
            event.locked_until = None
            CallStateMachine.transition(call, CallState.CANCELLED, termination_reason="CAMPAIGN_INACTIVE")
            if agent:
                AgentStateMachine.transition(agent, AgentState.AVAILABLE)
            cls._release_borrower(session, call.borrower_id)
            session.commit()
            return None

        # Critical Check 3d: Provider health check
        provider = provider_registry.get(provider_name)
        if not provider:
            event.status = OutboxStatus.FAILED.value
            event.last_error = f"Unknown provider '{provider_name}'."
            event.locked_until = None
            CallStateMachine.transition(call, CallState.FAILED, termination_reason="UNKNOWN_PROVIDER")
            if agent:
                AgentStateMachine.transition(agent, AgentState.AVAILABLE)
            cls._release_borrower(session, call.borrower_id)
            session.commit()
            return None

        if not provider_health_tracker.is_healthy(provider_name, session=session):
            logger.warning(
                "Pre-dispatch validation failed: Provider unhealthy",
                call_id=str(call_id),
                provider_name=provider_name,
            )
            event.status = OutboxStatus.FAILED.value
            event.last_error = f"Provider '{provider_name}' is currently degraded/unhealthy."
            event.locked_until = None
            CallStateMachine.transition(call, CallState.FAILED, termination_reason="PROVIDER_UNHEALTHY")
            if agent:
                AgentStateMachine.transition(agent, AgentState.AVAILABLE)
            cls._release_borrower(session, call.borrower_id)
            session.commit()
            return None

        # Step 4: Generate dispatch fencing token, worker ID and claim lease prior to network call
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        dispatch_token = uuid.uuid4()
        call.dispatch_token = dispatch_token
        if agent:
            agent.dispatch_token = dispatch_token

        event.worker_id = worker_id
        event.fencing_token = dispatch_token
        event.locked_until = now + timedelta(seconds=cls.LEASE_DURATION_SECONDS)
        session.commit()

        # Step 5: Start heartbeat and invoke telecom provider OUTSIDE database lock transaction
        bind = session.get_bind()
        session_factory = sessionmaker(bind=bind, expire_on_commit=False)
        heartbeat = LeaseHeartbeat(
            session_factory=session_factory,
            event_id=event_id,
            worker_id=worker_id,
            fencing_token=dispatch_token,
            lease_duration_sec=cls.LEASE_DURATION_SECONDS,
            interval_sec=cls.HEARTBEAT_INTERVAL_SECONDS,
        )
        heartbeat.start()

        try:
            res: ProviderInitiateResult = provider.initiate_call(
                call_id=call_id,
                phone_number=phone_number,
                idempotency_key=idempotency_key,
                session=session,
            )
        except Exception as exc:
            res = ProviderInitiateResult(success=False, error=str(exc))
        finally:
            heartbeat.stop()

        # Check if heartbeat detected loss of lease ownership during network I/O
        if heartbeat.ownership_lost:
            logger.warning(
                "Lease heartbeat lost ownership during provider dispatch",
                call_id=str(call_id),
                worker_id=worker_id,
            )
            if res.success and res.provider_call_id:
                try:
                    TerminationService.execute_termination(
                        session=session,
                        call_id=call_id,
                        provider_name=provider_name,
                        provider_call_id=res.provider_call_id,
                        target_terminal_state=CallState.CANCELLED.value,
                        termination_reason="LEASE_LOST_DURING_DISPATCH",
                    )
                except Exception as exc:
                    logger.error("Error executing rogue call termination", error=str(exc))
            return None

        # Step 6: Record dispatch outcome in fresh transaction with fencing verification
        session.expire_all()
        try:
            event = session.get(OutboxEvent, event_id, with_for_update=True)
            call = session.get(Call, call_id, with_for_update=True)
            agent = session.get(Agent, call.agent_id, with_for_update=True) if (call and call.agent_id) else None

            # Fencing Check A: Has this call or outbox lease been stolen/reallocated?
            is_stale_worker = (
                call is None
                or call.dispatch_token != dispatch_token
                or (event and event.status != OutboxStatus.PENDING.value)
                or (event and event.worker_id != worker_id)
                or (call and call.status != CallState.RESERVED.value)
            )
            if is_stale_worker:
                logger.warning(
                    "Stale worker dispatch completion rejected by fencing token",
                    call_id=str(call_id),
                    expected_token=str(dispatch_token),
                    actual_token=str(call.dispatch_token if call else None),
                )
                if res.success and res.provider_call_id:
                    # Terminate rogue carrier call safely via TerminationService
                    try:
                        TerminationService.execute_termination(
                            session=session,
                            call_id=call_id,
                            provider_name=provider_name,
                            provider_call_id=res.provider_call_id,
                            target_terminal_state=CallState.CANCELLED.value,
                            termination_reason="STALE_WORKER_FENCED_OUT",
                        )
                    except Exception as term_exc:
                        logger.error("Failed to terminate rogue carrier call after stale detection", error=str(term_exc))
                session.rollback()
                return None

            # Fencing Check B: If agent was assigned, verify agent is still RESERVED with matching token
            if agent is not None:
                agent_lost = (
                    agent.status != AgentState.RESERVED.value
                    or agent.dispatch_token != dispatch_token
                )
                if agent_lost:
                    logger.warning(
                        "Agent state mutated or disconnected during external carrier I/O",
                        call_id=str(call_id),
                        agent_id=str(agent.id),
                        agent_status=agent.status,
                    )
                    if res.success and res.provider_call_id:
                        try:
                            TerminationService.execute_termination(
                                session=session,
                                call_id=call_id,
                                provider_name=provider_name,
                                provider_call_id=res.provider_call_id,
                                target_terminal_state=CallState.CANCELLED.value,
                                termination_reason=f"AGENT_{agent.status}_DURING_DISPATCH",
                            )
                        except Exception as term_exc:
                            logger.error("Failed to terminate call after agent disconnect", error=str(term_exc))

                    if call:
                        CallStateMachine.transition(
                            call,
                            CallState.CANCELLED,
                            termination_reason=f"AGENT_{agent.status}_DURING_DISPATCH",
                        )
                        cls._release_borrower(session, call.borrower_id)

                    if event:
                        event.status = OutboxStatus.CANCELLED.value
                        event.last_error = f"Agent transitioned to {agent.status} during provider I/O; call safely terminated at carrier."
                        event.locked_until = None

                    session.commit()
                    return None

            if res.success:
                if event:
                    event.status = OutboxStatus.DISPATCHED.value
                    event.processed_at = datetime.now(timezone.utc)
                    event.locked_until = None

                if call:
                    call.provider_name = provider_name
                    call.provider_call_id = res.provider_call_id
                    CallStateMachine.transition(call, CallState.INITIATED)

                if agent and agent.status == AgentState.RESERVED.value:
                    AgentStateMachine.transition(agent, AgentState.DIALING, current_call_id=call.id if call else None)

                session.commit()
                logger.info(
                    "Outbox event successfully dispatched",
                    event_id=str(event_id),
                    call_id=str(call_id),
                    provider_call_id=res.provider_call_id,
                )
                return call_id
            else:
                if event:
                    event.retry_count += 1
                    event.last_error = res.error or "Provider initiation failed."

                    if event.retry_count >= cls.MAX_RETRIES:
                        event.status = OutboxStatus.FAILED.value
                        event.locked_until = None

                        if call:
                            CallStateMachine.transition(call, CallState.FAILED, termination_reason="DISPATCH_RETRIES_EXCEEDED")
                        if agent and agent.status in (AgentState.RESERVED.value, AgentState.DIALING.value):
                            AgentStateMachine.transition(agent, AgentState.AVAILABLE)
                        if call and call.borrower_id:
                            cls._release_borrower(session, call.borrower_id)

                        logger.error(
                            "Outbox dispatch exhausted retries; resources reclaimed",
                            event_id=str(event_id),
                            call_id=str(call_id),
                            error=res.error,
                        )
                    else:
                        backoff_sec = 2 ** event.retry_count
                        event.locked_until = now + timedelta(seconds=backoff_sec)
                        logger.warning(
                            "Outbox dispatch failed; scheduled retry",
                            event_id=str(event_id),
                            call_id=str(call_id),
                            retry_count=event.retry_count,
                            retry_in_sec=backoff_sec,
                        )

                session.commit()
                return None
        except StaleDataError as stale_exc:
            logger.warning(
                "Optimistic lock version conflict during provider dispatch completion",
                call_id=str(call_id),
                error=str(stale_exc),
            )
            if res.success and res.provider_call_id:
                try:
                    TerminationService.execute_termination(
                        session=session,
                        call_id=call_id,
                        provider_name=provider_name,
                        provider_call_id=res.provider_call_id,
                        target_terminal_state=CallState.CANCELLED.value,
                        termination_reason="STALE_DATA_VERSION_CONFLICT",
                    )
                except Exception as term_exc:
                    logger.error("Failed to terminate call after StaleDataError", error=str(term_exc))
            session.rollback()
            return None

    @staticmethod
    def _release_borrower(session: Session, borrower_id: Optional[uuid.UUID]) -> None:
        if not borrower_id:
            return
        borrower = session.get(Borrower, borrower_id, with_for_update=True)
        if borrower:
            borrower.status = BorrowerState.ELIGIBLE.value
            borrower.reservation_token = None
