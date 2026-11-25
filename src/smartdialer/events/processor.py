"""Event processor handling telecom webhooks, deduplication, and state reconciliation."""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from smartdialer.allocator.bridge import BridgeAllocator
from smartdialer.core.logging import get_logger
from smartdialer.models.agent import Agent
from smartdialer.models.borrower import Borrower
from smartdialer.models.call import Call
from smartdialer.models.enums import AgentState, BorrowerState, CallState, TerminationStatus
from smartdialer.models.event import ProviderEvent
from smartdialer.pacing.metrics import telemetry_registry
from smartdialer.services.termination_service import TerminationService
from smartdialer.state.agent_state_machine import AgentStateMachine
from smartdialer.state.call_state_machine import CallStateMachine, TransitionResult

logger = get_logger("event_processor")


@dataclass
class EventProcessResult:
    """Outcome of processing an incoming provider event."""
    success: bool
    is_duplicate: bool = False
    is_out_of_order: bool = False
    new_call_state: Optional[str] = None
    new_agent_state: Optional[str] = None
    message: str = ""


class EventProcessor:
    """
    Ingests raw telephony callbacks, guarantees deduplication at the DB layer,
    and reconciles Call and Agent state machines.
    """

    @classmethod
    def process_event(
        cls,
        session: Session,
        provider_name: str,
        event_id: str,
        call_id: Optional[uuid.UUID] = None,
        event_type: str = "",
        raw_payload: Optional[Dict[str, Any]] = None,
        provider_call_id: Optional[str] = None,
    ) -> EventProcessResult:
        """
        Process a telecom callback.
        
        Guarantees:
        1. Duplicate events are caught by UNIQUE(provider_name, event_id) and acknowledged safely.
        2. Webhooks can look up call by internal call_id OR telecom provider_call_id (FIND-MED-03).
        3. Out-of-order events never regress advanced or terminal call states.
        4. Agent lifecycle is automatically synchronized with call lifecycle.
        """
        payload = raw_payload or {}
        now = datetime.now(timezone.utc)

        # 1. Resolve call_id if only provider_call_id is supplied (FIND-MED-03)
        if call_id is None:
            target_prov_id = provider_call_id or payload.get("provider_call_id") or payload.get("call_sid")
            if target_prov_id:
                call_match = session.execute(
                    select(Call).where(
                        Call.provider_name == provider_name,
                        Call.provider_call_id == target_prov_id,
                    ).with_for_update()
                ).scalar_one_or_none()
                if call_match:
                    call_id = call_match.id
                else:
                    logger.warning("Event arrived for unknown provider_call_id", provider=provider_name, prov_id=target_prov_id)
                    return EventProcessResult(
                        success=False,
                        message=f"Call with provider_call_id '{target_prov_id}' not found.",
                    )
            else:
                return EventProcessResult(
                    success=False,
                    message="Neither call_id nor provider_call_id supplied in event payload.",
                )

        # 2. Enforce DB-Level Deduplication via Savepoint
        provider_event = ProviderEvent(
            provider_name=provider_name,
            event_id=event_id,
            call_id=call_id,
            event_type=event_type,
            raw_payload=payload,
            received_at=now,
        )

        try:
            # Use nested transaction (savepoint) so unique violation does not invalidate outer session
            with session.begin_nested():
                session.add(provider_event)
                session.flush()
        except IntegrityError:
            # Duplicate event caught by database constraint!
            logger.info(
                "Duplicate provider event detected and skipped",
                provider=provider_name,
                event_id=event_id,
                call_id=str(call_id),
            )
            return EventProcessResult(
                success=True,
                is_duplicate=True,
                message=f"Duplicate event '{event_id}' acknowledged idempotently.",
            )

        # 2. Lock and retrieve Call row
        call = session.execute(
            select(Call).where(Call.id == call_id).with_for_update()
        ).scalar_one_or_none()

        if not call:
            provider_event.processed = True
            provider_event.error_message = f"Call {call_id} not found."
            session.flush()
            logger.warning("Event arrived for unknown call", call_id=str(call_id))
            return EventProcessResult(
                success=False,
                message=f"Call {call_id} not found.",
            )

        # Extract optional duration and termination reason
        duration = payload.get("duration_seconds")
        reason = payload.get("reason")

        # 3. Apply state transition through monotonic Call State Machine
        trans_res: TransitionResult = CallStateMachine.transition(
            call=call,
            target_state=event_type,
            termination_reason=reason,
            duration_seconds=duration,
        )

        # 4. Handle out-of-order or late callbacks on terminal states
        if not trans_res.success:
            provider_event.processed = True
            provider_event.processed_at = now
            session.flush()

            logger.info(
                "Event did not advance call state",
                call_id=str(call.id),
                event_type=event_type,
                reason=trans_res.error,
            )
            return EventProcessResult(
                success=True,
                is_out_of_order=trans_res.is_out_of_order,
                new_call_state=call.status,
                message=trans_res.error or "Event did not mutate call state.",
            )

        # 5. Synchronize linked Agent or bridge answering call
        new_agent_state: Optional[str] = None
        bridge_timeout = payload.get("answer_timeout_sec")

        if call.status in (CallState.ANSWERED.value, CallState.CONNECTED.value):
            if call.agent_id is None:
                # Unassigned predictive call: must bridge to an available agent
                agent = BridgeAllocator.bridge_call_to_agent(
                    session=session,
                    call=call,
                    timeout_seconds=bridge_timeout,
                )
                if agent:
                    new_agent_state = agent.status
                    if call.status != CallState.CONNECTED.value:
                        CallStateMachine.transition(call, CallState.CONNECTED)
                else:
                    # No eligible agent available -> initiate durable carrier termination & abandonment!
                    logger.warning(
                        "Predictive call abandoned: No eligible agent available within answer timeout",
                        call_id=str(call.id),
                        campaign_id=str(call.campaign_id),
                    )
                    CallStateMachine.transition(call, CallState.ABANDONMENT_PENDING, termination_reason="ABANDONED")
                    call.termination_status = TerminationStatus.PENDING.value
                    session.flush()

                    # Execute carrier termination outside DB lock; retains borrower if teardown uncertain
                    TerminationService.execute_termination(
                        session=session,
                        call_id=call.id,
                        provider_name=provider_name,
                        target_terminal_state=CallState.FAILED.value,
                        termination_reason="ABANDONED",
                    )
            else:
                # Progressive call: validate assigned agent
                assigned_agent = session.execute(
                    select(Agent).where(Agent.id == call.agent_id).with_for_update()
                ).scalar_one_or_none()

                # Check if assigned agent is stale, offline, or paused (FIX 2)
                agent_stale = (
                    assigned_agent is None
                    or assigned_agent.status in (AgentState.OFFLINE.value, AgentState.PAUSED.value)
                    or (assigned_agent.current_call_id is not None and assigned_agent.current_call_id != call.id)
                )
                if agent_stale:
                    logger.warning(
                        "Assigned agent is no longer eligible (offline/paused/claimed); attempting re-bridge",
                        call_id=str(call.id),
                        stale_agent_id=str(call.agent_id),
                    )
                    # Detach stale agent
                    call.agent_id = None
                    alt_agent = BridgeAllocator.bridge_call_to_agent(
                        session=session,
                        call=call,
                        timeout_seconds=bridge_timeout,
                    )
                    if alt_agent:
                        new_agent_state = alt_agent.status
                        if call.status == CallState.CONNECTED.value:
                            AgentStateMachine.transition(alt_agent, AgentState.CONNECTED, current_call_id=call.id)
                    else:
                        logger.warning(
                            "Progressive call abandoned: Assigned agent stale and no fallback agent available",
                            call_id=str(call.id),
                        )
                        CallStateMachine.transition(call, CallState.ABANDONMENT_PENDING, termination_reason="ABANDONED")
                        call.termination_status = TerminationStatus.PENDING.value
                        session.flush()

                        TerminationService.execute_termination(
                            session=session,
                            call_id=call.id,
                            provider_name=provider_name,
                            target_terminal_state=CallState.FAILED.value,
                            termination_reason="ABANDONED",
                        )
                else:
                    # Assigned agent is healthy: transition agent to CONNECTED upon borrower answer/connect
                    if assigned_agent.status != AgentState.CONNECTED.value:
                        AgentStateMachine.transition(assigned_agent, AgentState.CONNECTED, current_call_id=call.id)
                    new_agent_state = assigned_agent.status
        elif call.agent_id:
            agent = session.execute(
                select(Agent).where(Agent.id == call.agent_id).with_for_update()
            ).scalar_one_or_none()

            if agent:
                if call.status in (CallState.INITIATED.value, CallState.RINGING.value):
                    if agent.status == AgentState.RESERVED.value:
                        AgentStateMachine.transition(agent, AgentState.DIALING, current_call_id=call.id)
                        new_agent_state = agent.status
                elif call.status in (CallState.COMPLETED.value, CallState.FAILED.value, CallState.CANCELLED.value):
                    if agent.status in (AgentState.CONNECTED.value, AgentState.DIALING.value, AgentState.RESERVED.value):
                        AgentStateMachine.transition(agent, AgentState.WRAP_UP)
                        new_agent_state = agent.status

        # Synchronize borrower
        borrower = session.get(Borrower, call.borrower_id)
        if borrower:
            if call.status == CallState.CONNECTED.value:
                borrower.status = BorrowerState.IN_CALL.value
            elif call.status == CallState.COMPLETED.value:
                borrower.status = BorrowerState.COMPLETED.value
            elif call.status in (CallState.FAILED.value, CallState.CANCELLED.value):
                # Only release borrower to ELIGIBLE if carrier call termination is confirmed or not required!
                if call.termination_status in (TerminationStatus.NOT_REQUIRED.value, TerminationStatus.CONFIRMED.value):
                    borrower.status = BorrowerState.ELIGIBLE.value if borrower.call_attempts < 3 else BorrowerState.FAILED.value
                    borrower.reservation_token = None
                    borrower.reserved_at = None
                else:
                    logger.warning(
                        "Borrower retained in non-eligible state pending confirmed carrier teardown",
                        borrower_id=str(borrower.id),
                        call_id=str(call.id),
                        termination_status=call.termination_status,
                    )

        # Update live campaign telemetry on terminal call outcomes (FIND-HIGH-02 / FIX 4)
        if call.status in (CallState.COMPLETED.value, CallState.FAILED.value, CallState.CANCELLED.value):
            was_answered = (
                call.answered_at is not None
                or call.connected_at is not None
                or (duration is not None and duration > 0)
            )
            dur_sec = float(call.duration_seconds or duration or 0.0)
            telemetry_registry.get(str(call.campaign_id)).record_call_result(
                answered=was_answered,
                duration_sec=dur_sec,
            )

        provider_event.processed = True
        provider_event.processed_at = now
        session.flush()

        logger.info(
            "Provider event processed successfully",
            provider=provider_name,
            event_id=event_id,
            call_id=str(call.id),
            call_state=call.status,
            agent_state=new_agent_state,
        )

        return EventProcessResult(
            success=True,
            new_call_state=call.status,
            new_agent_state=new_agent_state,
            message=f"Call advanced to {call.status}.",
        )
