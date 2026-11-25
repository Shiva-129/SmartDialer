"""State reconciliation sweeper for self-healing of stale reservations, timeouts, and crashed workers."""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

from smartdialer.core.logging import get_logger
from smartdialer.models.agent import Agent
from smartdialer.models.borrower import Borrower
from smartdialer.models.call import Call
from smartdialer.models.enums import AgentState, BorrowerState, CallState, OutboxStatus
from smartdialer.models.outbox import OutboxEvent
from smartdialer.models.provider_request import ProviderRequestLedger
from smartdialer.services.termination_service import TerminationService
from smartdialer.state.agent_state_machine import AgentStateMachine
from smartdialer.state.call_state_machine import CallStateMachine

logger = get_logger("reconciler")


@dataclass
class ReconciliationReport:
    """Summary of self-healing actions executed during a reconciliation sweep."""
    reclaimed_agents_count: int = 0
    reclaimed_borrowers_count: int = 0
    timed_out_calls_count: int = 0
    stale_outbox_unlocked_count: int = 0
    confirmed_terminations_count: int = 0


class ReconciliationSweeper:
    """
    Background recovery engine that sweeps PostgreSQL to detect and resolve:
    1. Calls stuck in RESERVED state due to crashed workers before dispatch.
    2. Agents stuck in RESERVED state due to crashed workers.
    3. Agents stuck in WRAP_UP state beyond the wrap-up timeout.
    4. Borrowers stuck in RESERVED state.
    5. Calls stuck in RINGING state past carrier timeout.
    """

    CALL_RESERVATION_TIMEOUT_SEC: int = 30
    AGENT_RESERVATION_TIMEOUT_SEC: int = 30
    BORROWER_RESERVATION_TIMEOUT_SEC: int = 45
    RINGING_TIMEOUT_SEC: int = 60
    WRAP_UP_TIMEOUT_SEC: int = 60

    @classmethod
    def run_sweep(
        cls,
        session: Session,
        campaign_id: Optional[uuid.UUID] = None,
    ) -> ReconciliationReport:
        """
        Execute full reconciliation pass.
        
        Guarantees self-healing across worker crashes and dropped carrier callbacks.
        """
        now = datetime.now(timezone.utc)
        report = ReconciliationReport()

        # -------------------------------------------------------------
        # 1. Reconcile Stale Calls in RESERVED status (Worker Crash Pre-Dispatch)
        # -------------------------------------------------------------
        call_cutoff = now - timedelta(seconds=cls.CALL_RESERVATION_TIMEOUT_SEC)
        stale_call_stmt = select(Call).where(
            Call.status == CallState.RESERVED.value,
            Call.created_at < call_cutoff,
        ).with_for_update(skip_locked=True)

        if campaign_id:
            stale_call_stmt = stale_call_stmt.where(Call.campaign_id == campaign_id)

        stale_reserved_calls = session.execute(stale_call_stmt).scalars().all()
        for call in stale_reserved_calls:
            # Check if an outbox worker actively holds a lease on this call
            outbox_event = session.execute(
                select(OutboxEvent).where(
                    OutboxEvent.aggregate_id == call.id,
                    OutboxEvent.status == OutboxStatus.PENDING.value,
                ).with_for_update(skip_locked=True)
            ).scalar_one_or_none()

            # If an outbox worker actively holds a lease (locked_until > now),
            # DO NOT cancel the call or steal its agent!
            if outbox_event and outbox_event.locked_until and outbox_event.locked_until > now:
                logger.info(
                    "Skipping stale RESERVED call; dispatch lease actively held by worker",
                    call_id=str(call.id),
                    locked_until=str(outbox_event.locked_until),
                )
                continue

            # Check if ProviderRequestLedger holds an active in-flight lease for this call
            ledger_entry = session.execute(
                select(ProviderRequestLedger).where(
                    ProviderRequestLedger.call_id == call.id,
                )
            ).scalar_one_or_none()
            if ledger_entry and ledger_entry.status in ("PENDING", "IN_PROGRESS"):
                if ledger_entry.lease_expires_at and ledger_entry.lease_expires_at > now:
                    logger.info(
                        "Skipping stale RESERVED call; provider ledger lease actively held by worker",
                        call_id=str(call.id),
                        lease_expires_at=str(ledger_entry.lease_expires_at),
                    )
                    continue

            # Mark call as FAILED
            CallStateMachine.transition(
                call=call,
                target_state=CallState.FAILED,
                termination_reason="RESERVATION_TIMEOUT_EXPIRED",
            )
            report.timed_out_calls_count += 1

            # Release associated Agent
            if call.agent_id:
                agent = session.execute(
                    select(Agent).where(Agent.id == call.agent_id).with_for_update()
                ).scalar_one_or_none()
                if agent and agent.status == AgentState.RESERVED.value:
                    AgentStateMachine.transition(agent, AgentState.AVAILABLE)
                    report.reclaimed_agents_count += 1
                    logger.warning(
                        "Reclaimed agent from stale RESERVED call",
                        agent_id=str(agent.id),
                        call_id=str(call.id),
                    )

            # Release associated Borrower
            if call.borrower_id:
                borrower = session.execute(
                    select(Borrower).where(Borrower.id == call.borrower_id).with_for_update()
                ).scalar_one_or_none()
                if borrower and borrower.status == BorrowerState.RESERVED.value:
                    borrower.status = BorrowerState.ELIGIBLE.value
                    borrower.reservation_token = None
                    borrower.reserved_at = None
                    report.reclaimed_borrowers_count += 1
                    logger.warning(
                        "Reclaimed borrower from stale RESERVED call",
                        borrower_id=str(borrower.id),
                        call_id=str(call.id),
                    )

            # Mark associated pending OutboxEvent as FAILED so it won't be dispatched
            if outbox_event:
                outbox_event.status = OutboxStatus.FAILED.value
                outbox_event.last_error = "RESERVATION_TIMEOUT_EXPIRED"
                outbox_event.locked_until = None

        session.flush()

        # -------------------------------------------------------------
        # 2. Reclaim Orphaned Stale Agents in RESERVED Status
        # -------------------------------------------------------------
        agent_cutoff = now - timedelta(seconds=cls.AGENT_RESERVATION_TIMEOUT_SEC)
        agent_stmt = select(Agent).where(
            Agent.status == AgentState.RESERVED.value,
            Agent.reserved_at < agent_cutoff,
        ).with_for_update(skip_locked=True)

        if campaign_id:
            agent_stmt = agent_stmt.where(Agent.campaign_id == campaign_id)

        stale_agents = session.execute(agent_stmt).scalars().all()
        for agent in stale_agents:
            if agent.status != AgentState.RESERVED.value:
                continue
            has_active_call = False
            if agent.current_call_id:
                call = session.get(Call, agent.current_call_id)
                if call and call.status not in ("COMPLETED", "FAILED", "CANCELLED"):
                    has_active_call = True

            if not has_active_call:
                AgentStateMachine.transition(agent, AgentState.AVAILABLE)
                report.reclaimed_agents_count += 1
                logger.warning(
                    "Reclaimed stale agent reservation",
                    agent_id=str(agent.id),
                    reserved_at=str(agent.reserved_at),
                )

        # -------------------------------------------------------------
        # 3. Reclaim Agents Stuck in WRAP_UP Beyond Timeout (FIND-MED-05)
        # -------------------------------------------------------------
        wrapup_cutoff = now - timedelta(seconds=cls.WRAP_UP_TIMEOUT_SEC)
        wrapup_stmt = select(Agent).where(
            Agent.status == AgentState.WRAP_UP.value,
            Agent.last_call_ended_at < wrapup_cutoff,
        ).with_for_update(skip_locked=True)

        if campaign_id:
            wrapup_stmt = wrapup_stmt.where(Agent.campaign_id == campaign_id)

        stuck_wrapup_agents = session.execute(wrapup_stmt).scalars().all()
        for agent in stuck_wrapup_agents:
            AgentStateMachine.transition(agent, AgentState.AVAILABLE)
            report.reclaimed_agents_count += 1
            logger.warning(
                "Reclaimed agent stuck in WRAP_UP past timeout",
                agent_id=str(agent.id),
                last_call_ended_at=str(agent.last_call_ended_at),
            )

        # -------------------------------------------------------------
        # 4. Reclaim Orphaned Borrowers in RESERVED Status
        # -------------------------------------------------------------
        borrower_cutoff = now - timedelta(seconds=cls.BORROWER_RESERVATION_TIMEOUT_SEC)
        borrower_stmt = select(Borrower).where(
            Borrower.status == BorrowerState.RESERVED.value,
            Borrower.reserved_at < borrower_cutoff,
        ).with_for_update(skip_locked=True)

        if campaign_id:
            borrower_stmt = borrower_stmt.where(Borrower.campaign_id == campaign_id)

        stale_borrowers = session.execute(borrower_stmt).scalars().all()
        for borrower in stale_borrowers:
            borrower.status = BorrowerState.ELIGIBLE.value
            borrower.reservation_token = None
            borrower.reserved_at = None
            report.reclaimed_borrowers_count += 1
            logger.warning(
                "Reclaimed stale borrower reservation",
                borrower_id=str(borrower.id),
                reserved_at=str(borrower.reserved_at),
            )

        # -------------------------------------------------------------
        # 5. Clean Up Calls Stuck in RINGING Past Carrier Timeout
        # -------------------------------------------------------------
        ringing_cutoff = now - timedelta(seconds=cls.RINGING_TIMEOUT_SEC)
        call_stmt = select(Call).where(
            Call.status == CallState.RINGING.value,
            Call.ringing_at < ringing_cutoff,
        ).with_for_update(skip_locked=True)

        if campaign_id:
            call_stmt = call_stmt.where(Call.campaign_id == campaign_id)

        stale_calls = session.execute(call_stmt).scalars().all()
        for call in stale_calls:
            CallStateMachine.transition(
                call=call,
                target_state=CallState.FAILED,
                termination_reason="RINGING_TIMEOUT_EXCEEDED",
            )
            report.timed_out_calls_count += 1

            # Free linked agent
            if call.agent_id:
                agent = session.get(Agent, call.agent_id)
                if agent and agent.status in (AgentState.DIALING.value, AgentState.RESERVED.value):
                    AgentStateMachine.transition(agent, AgentState.WRAP_UP)

            # Reset borrower for retry
            if call.borrower_id:
                borrower = session.get(Borrower, call.borrower_id)
                if borrower:
                    borrower.status = BorrowerState.ELIGIBLE.value if borrower.call_attempts < 3 else BorrowerState.FAILED.value
                    borrower.reservation_token = None

            logger.warning(
                "Timed out orphaned ringing call",
                call_id=str(call.id),
                ringing_at=str(call.ringing_at),
            )

        # -------------------------------------------------------------
        # 6. Retry Pending / Uncertain Carrier Teardowns (FIX 1 / FIX 5)
        # -------------------------------------------------------------
        confirmed_terms = TerminationService.retry_pending_terminations(session)
        report.confirmed_terminations_count = confirmed_terms

        session.flush()
        return report
