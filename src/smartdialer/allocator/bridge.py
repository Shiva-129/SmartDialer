"""Answer-to-Agent Bridge Allocator for predictive dialing."""

from datetime import datetime, timezone
import time
import uuid
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

from smartdialer.core.logging import get_logger
from smartdialer.models.agent import Agent
from smartdialer.models.call import Call
from smartdialer.models.enums import AgentState
from smartdialer.state.agent_state_machine import AgentStateMachine

logger = get_logger("bridge_allocator")


class BridgeAllocator:
    """
    Matches answering calls to available collections agents.
    
    Guarantees:
    1. Pre-assigned agents (progressive) are validated with row-level locks.
       If an agent went OFFLINE, PAUSED, or expired, bridging is rejected and
       fallback to an available agent is attempted.
    2. Atomic claim via SELECT ... FOR UPDATE SKIP LOCKED prevents concurrent answered
       calls from claiming the same agent.
    3. Configurable answer timeout ensures FCC/Ofcom compliance: if no agent becomes
       available before timeout, returns None to trigger compliant abandonment.
    """

    DEFAULT_ANSWER_TIMEOUT_SEC: float = 2.0
    POLL_INTERVAL_SEC: float = 0.05
    AGENT_RESERVATION_TIMEOUT_SEC: float = 30.0

    @classmethod
    def bridge_call_to_agent(
        cls,
        session: Session,
        call: Call,
        timeout_seconds: Optional[float] = None,
        poll_interval: Optional[float] = None,
    ) -> Optional[Agent]:
        """
        Atomically bridge an answered call to an available, eligible agent.
        
        Returns the claimed Agent transitioned to CONNECTED, or None if timeout expired.
        NEVER connects to an OFFLINE or PAUSED agent.
        """
        timeout = cls.DEFAULT_ANSWER_TIMEOUT_SEC if timeout_seconds is None else timeout_seconds
        interval = cls.POLL_INTERVAL_SEC if poll_interval is None else poll_interval

        # 1. If call already has an assigned agent (e.g. progressive), verify eligibility atomically
        if call.agent_id is not None:
            stmt = select(Agent).where(Agent.id == call.agent_id).with_for_update()
            agent = session.execute(stmt).scalar_one_or_none()
            if agent and agent.status in (AgentState.RESERVED.value, AgentState.DIALING.value):
                # Verify reservation belongs to this call and has not expired
                is_valid = True
                if agent.current_call_id and agent.current_call_id != call.id:
                    is_valid = False
                if agent.reserved_at:
                    now = datetime.now(timezone.utc)
                    if (now - agent.reserved_at).total_seconds() > cls.AGENT_RESERVATION_TIMEOUT_SEC:
                        is_valid = False

                if is_valid:
                    AgentStateMachine.transition(agent, AgentState.CONNECTED, current_call_id=call.id)
                    session.flush()
                    logger.info(
                        "Pre-assigned agent successfully bridged to call",
                        call_id=str(call.id),
                        agent_id=str(agent.id),
                    )
                    return agent

            # Pre-assigned agent lost or ineligible (OFFLINE/PAUSED/expired): detach
            logger.warning(
                "Pre-assigned agent is no longer eligible for bridging; attempting fallback bridge",
                call_id=str(call.id),
                agent_id=str(call.agent_id),
                status=agent.status if agent else "NOT_FOUND",
            )
            call.agent_id = None
            session.flush()

        # 2. Predictive or fallback unassigned call: claim an available agent with SKIP LOCKED
        start_time = time.monotonic()
        while True:
            stmt = (
                select(Agent)
                .where(
                    Agent.campaign_id == call.campaign_id,
                    Agent.status == AgentState.AVAILABLE.value,
                )
                .order_by(Agent.last_call_ended_at.asc().nullsfirst(), Agent.id.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            agent = session.execute(stmt).scalar_one_or_none()

            if agent is not None:
                call.agent_id = agent.id
                agent.current_call_id = call.id
                AgentStateMachine.transition(agent, AgentState.CONNECTED, current_call_id=call.id)
                session.flush()
                logger.info(
                    "Call bridged to agent successfully",
                    call_id=str(call.id),
                    agent_id=str(agent.id),
                    agent_name=agent.name,
                )
                return agent

            elapsed = time.monotonic() - start_time
            if elapsed >= timeout:
                break

            time.sleep(min(interval, max(0.001, timeout - elapsed)))

        logger.warning(
            "Bridge timeout: No available agent within answer timeout",
            call_id=str(call.id),
            campaign_id=str(call.campaign_id),
            timeout_sec=timeout,
        )
        return None
