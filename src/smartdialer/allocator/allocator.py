"""Atomic call, agent, and borrower allocator with PostgreSQL row-level locking."""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from smartdialer.core.logging import get_logger
from smartdialer.models.agent import Agent
from smartdialer.models.borrower import Borrower
from smartdialer.models.call import Call
from smartdialer.models.enums import (
    CALL_STATE_RANK,
    AgentState,
    BorrowerState,
    CallState,
    OutboxStatus,
)
from smartdialer.models.outbox import OutboxEvent
from smartdialer.state.agent_state_machine import AgentStateMachine
from smartdialer.state.exceptions import (
    AgentNotAvailableError,
    BorrowerNotEligibleError,
)

logger = get_logger("call_allocator")


@dataclass
class AllocationResult:
    """Encapsulates all entities created or modified during an atomic call allocation."""
    call: Call
    agent: Optional[Agent]
    borrower: Borrower
    outbox_event: OutboxEvent
    idempotency_key: str
    reservation_token: uuid.UUID


class CallAllocator:
    """
    Handles atomic reservations and call record creation using PostgreSQL row-level locking.
    
    Guarantees that two concurrent workers can never reserve the same agent or borrower.
    """

    @classmethod
    def reserve_agent_atomic(
        cls,
        session: Session,
        campaign_id: uuid.UUID,
        reservation_token: uuid.UUID,
        agent_id: Optional[uuid.UUID] = None,
    ) -> Agent:
        """
        Atomically reserve an available agent using SELECT ... FOR UPDATE SKIP LOCKED.
        
        Raises AgentNotAvailableError if no available agent is found.
        """
        stmt = (
            select(Agent)
            .where(
                Agent.campaign_id == campaign_id,
                Agent.status == AgentState.AVAILABLE.value,
            )
            .order_by(Agent.last_call_ended_at.asc().nullsfirst(), Agent.id.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if agent_id:
            stmt = (
                select(Agent)
                .where(
                    Agent.id == agent_id,
                    Agent.campaign_id == campaign_id,
                    Agent.status.in_([AgentState.AVAILABLE.value, AgentState.RESERVED.value]),
                )
                .with_for_update(skip_locked=True)
            )
            agent = session.execute(stmt).scalar_one_or_none()
            if not agent:
                raise AgentNotAvailableError(
                    f"Agent {agent_id} is not available for reservation."
                )
            if agent.status == AgentState.AVAILABLE.value:
                AgentStateMachine.transition(
                    agent=agent,
                    target_state=AgentState.RESERVED,
                    reservation_token=reservation_token,
                )
            elif agent.status == AgentState.RESERVED.value and reservation_token:
                agent.reservation_token = reservation_token
            session.flush()
            return agent

        agent = session.execute(stmt).scalar_one_or_none()
        if not agent:
            raise AgentNotAvailableError(
                f"No available agents for campaign {campaign_id} to reserve."
            )

        AgentStateMachine.transition(
            agent=agent,
            target_state=AgentState.RESERVED,
            reservation_token=reservation_token,
        )
        session.flush()
        return agent

    @classmethod
    def reserve_borrower_atomic(
        cls,
        session: Session,
        campaign_id: uuid.UUID,
        reservation_token: uuid.UUID,
        borrower_id: Optional[uuid.UUID] = None,
    ) -> Borrower:
        """
        Atomically reserve an eligible borrower using SELECT ... FOR UPDATE SKIP LOCKED.
        
        Raises BorrowerNotEligibleError if no eligible borrower is found.
        """
        stmt = (
            select(Borrower)
            .where(
                Borrower.campaign_id == campaign_id,
                Borrower.status == BorrowerState.ELIGIBLE.value,
            )
            .order_by(Borrower.priority.desc(), Borrower.call_attempts.asc(), Borrower.id.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if borrower_id:
            stmt = (
                select(Borrower)
                .where(
                    Borrower.id == borrower_id,
                    Borrower.campaign_id == campaign_id,
                    Borrower.status == BorrowerState.ELIGIBLE.value,
                )
                .with_for_update(skip_locked=True)
            )

        borrower = session.execute(stmt).scalar_one_or_none()
        if not borrower:
            raise BorrowerNotEligibleError(
                f"No eligible borrowers for campaign {campaign_id} to reserve."
            )

        now = datetime.now(timezone.utc)
        borrower.status = BorrowerState.RESERVED.value
        borrower.reservation_token = reservation_token
        borrower.reserved_at = now
        borrower.call_attempts += 1
        borrower.last_called_at = now
        session.flush()
        return borrower

    @classmethod
    def allocate_call(
        cls,
        session: Session,
        campaign_id: uuid.UUID,
        provider_name: str,
        agent_id: Optional[uuid.UUID] = None,
        borrower_id: Optional[uuid.UUID] = None,
        idempotency_key: Optional[str] = None,
        dialing_mode: str = "PROGRESSIVE",
    ) -> AllocationResult:
        """
        Atomically allocates an agent, borrower, call record, and outbox event in a single transaction.
        
        In PROGRESSIVE mode (or PREDICTIVE with an available agent): an agent is atomically reserved.
        In PREDICTIVE mode with agent_id=None: allocates an agent-less predictive call to dial out;
        the answering borrower will be matched to an agent via BridgeAllocator upon answer.
        """
        reservation_token = uuid.uuid4()

        # 1. Atomically lock and reserve agent if progressive or specific agent requested
        agent = None
        if dialing_mode != "PREDICTIVE" or agent_id is not None:
            agent = cls.reserve_agent_atomic(
                session=session,
                campaign_id=campaign_id,
                reservation_token=reservation_token,
                agent_id=agent_id,
            )

        # 2. Atomically lock and reserve borrower
        try:
            borrower = cls.reserve_borrower_atomic(
                session=session,
                campaign_id=campaign_id,
                reservation_token=reservation_token,
                borrower_id=borrower_id,
            )
        except BorrowerNotEligibleError:
            # Revert agent reservation before re-raising
            if agent:
                AgentStateMachine.transition(agent, AgentState.AVAILABLE)
                agent.reservation_token = None
                agent.reserved_at = None
                agent.current_call_id = None
            session.flush()
            raise

        # 3. Formulate deterministic idempotency key (FIND-MED-04)
        key = idempotency_key or f"call-{campaign_id}-{borrower.id}-att{borrower.call_attempts}"

        # 4. Create Call entity in RESERVED state
        call = Call(
            campaign_id=campaign_id,
            agent_id=agent.id if agent else None,
            borrower_id=borrower.id,
            provider_name=provider_name,
            status=CallState.RESERVED.value,
            state_rank=CALL_STATE_RANK[CallState.RESERVED],
            idempotency_key=key,
            dialing_mode=dialing_mode,
        )
        session.add(call)
        session.flush()

        # Link call to agent if assigned
        if agent:
            agent.current_call_id = call.id

        # 5. Persist Transactional Outbox Event
        outbox_event = OutboxEvent(
            aggregate_type="call",
            aggregate_id=call.id,
            event_type="DISPATCH_CALL",
            payload={
                "call_id": str(call.id),
                "campaign_id": str(campaign_id),
                "agent_id": str(agent.id) if agent else None,
                "borrower_id": str(borrower.id),
                "phone_number": borrower.phone_number,
                "provider_name": provider_name,
                "idempotency_key": key,
                "dialing_mode": dialing_mode,
            },
            status=OutboxStatus.PENDING.value,
        )
        session.add(outbox_event)
        session.flush()

        logger.info(
            "Atomic call allocation completed",
            call_id=str(call.id),
            agent_id=str(agent.id) if agent else None,
            borrower_id=str(borrower.id),
            idempotency_key=key,
            dialing_mode=dialing_mode,
        )

        return AllocationResult(
            call=call,
            agent=agent,
            borrower=borrower,
            outbox_event=outbox_event,
            idempotency_key=key,
            reservation_token=reservation_token,
        )

    @classmethod
    def batch_allocate(
        cls,
        session: Session,
        campaign_id: uuid.UUID,
        count: int,
        provider_name: str,
    ) -> List[AllocationResult]:
        """
        Allocate up to 'count' agent-borrower pairs atomically.
        
        Stops when either agent pool or borrower pool is exhausted.
        """
        results: List[AllocationResult] = []
        for _ in range(count):
            try:
                res = cls.allocate_call(
                    session=session,
                    campaign_id=campaign_id,
                    provider_name=provider_name,
                )
                results.append(res)
            except (AgentNotAvailableError, BorrowerNotEligibleError):
                break
        return results

    @classmethod
    def release_reservation(
        cls,
        session: Session,
        reservation_token: uuid.UUID,
        reason: str = "Reservation released or cancelled",
    ) -> None:
        """Safely release agent and borrower associated with a reservation token."""
        # Find agent with reservation token
        agent = session.execute(
            select(Agent).where(Agent.reservation_token == reservation_token)
        ).scalar_one_or_none()
        if agent and agent.status == AgentState.RESERVED.value:
            AgentStateMachine.transition(agent, AgentState.AVAILABLE)

        # Find borrower with reservation token
        borrower = session.execute(
            select(Borrower).where(Borrower.reservation_token == reservation_token)
        ).scalar_one_or_none()
        if borrower and borrower.status == BorrowerState.RESERVED.value:
            borrower.status = BorrowerState.ELIGIBLE.value
            borrower.reservation_token = None
            borrower.reserved_at = None

        session.flush()
        logger.info("Reservation released", token=str(reservation_token), reason=reason)
