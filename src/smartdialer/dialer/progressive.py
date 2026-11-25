"""Progressive Dialer: Conservative 1:1 outbound dialing engine."""

import uuid
from dataclasses import dataclass, field
from typing import List, Optional
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from smartdialer.allocator.allocator import AllocationResult, CallAllocator
from smartdialer.core.logging import get_logger
from smartdialer.models.agent import Agent
from smartdialer.models.enums import AgentState, CampaignMode, SafetyDecisionType
from smartdialer.safety.safety_controller import (
    DialingRequest,
    SafetyController,
    SafetyDecision,
)
from smartdialer.state.agent_state_machine import AgentStateMachine
from smartdialer.state.exceptions import (
    AgentNotAvailableError,
    BorrowerNotEligibleError,
)

logger = get_logger("progressive_dialer")


@dataclass
class ProgressiveDialResult:
    """Outcome of a progressive dialing cycle."""
    campaign_id: uuid.UUID
    safety_decision: SafetyDecision
    allocations: List[AllocationResult] = field(default_factory=list)
    failed_allocations: int = 0
    message: str = ""


class ProgressiveDialer:
    """
    Progressive dialing implementation.
    
    Guarantees:
    1. Exactly one outbound call per safely available agent (1:1 ratio).
    2. Mandatory submission to Safety Controller before any call creation.
    3. Agent and Borrower are atomically reserved before dispatch.
    4. Automatically handles an agent disappearing during setup.
    """

    @classmethod
    def execute_cycle(
        cls,
        session: Session,
        campaign_id: uuid.UUID,
        provider_name: str,
    ) -> ProgressiveDialResult:
        """
        Execute one progressive dialing pass.
        
        Steps:
        1. Query database for count of AVAILABLE agents.
        2. Formulate DialingRequest and submit to Safety Controller.
        3. If approved or reduced, allocate up to approved calls atomically.
        4. Catch concurrent resource exhaustion (e.g. agent logs off mid-setup).
        """
        # Step 1: Detect available agent capacity
        available_agents = session.execute(
            select(func.count(Agent.id)).where(
                Agent.campaign_id == campaign_id,
                Agent.status == AgentState.AVAILABLE.value,
            )
        ).scalar() or 0

        # Step 2: Submit request through mandatory Safety Controller boundary
        request = DialingRequest(
            campaign_id=campaign_id,
            mode=CampaignMode.PROGRESSIVE,
            desired_calls=available_agents,
            estimated_answer_rate=1.0,  # Progressive assumes deterministic 1:1 match
            provider_name=provider_name,
            pacing_reasoning=f"Progressive cycle requesting 1 call per {available_agents} available agent(s).",
        )
        decision = SafetyController.evaluate(session, request)

        if decision.approved_calls <= 0:
            logger.info(
                "Progressive cycle completed with zero approvals",
                campaign_id=str(campaign_id),
                decision=decision.decision.value,
                reason=decision.reasoning,
            )
            return ProgressiveDialResult(
                campaign_id=campaign_id,
                safety_decision=decision,
                allocations=[],
                message=decision.reasoning,
            )

        # Step 3: Perform atomic allocations for approved calls
        allocations: List[AllocationResult] = []
        failed_count = 0

        target_agent_ids = list(decision.claimed_agent_ids) if decision.claimed_agent_ids else [None] * decision.approved_calls

        try:
            for agent_id in target_agent_ids:
                try:
                    alloc_res = CallAllocator.allocate_call(
                        session=session,
                        campaign_id=campaign_id,
                        provider_name=provider_name,
                        agent_id=agent_id,
                    )
                    allocations.append(alloc_res)
                except AgentNotAvailableError:
                    # An agent disappeared (e.g. logged off or grabbed by concurrent worker)
                    logger.warning(
                        "Agent became unavailable during progressive allocation setup",
                        campaign_id=str(campaign_id),
                    )
                    failed_count += 1
                    break
                except BorrowerNotEligibleError:
                    # No more eligible borrowers in campaign pool
                    logger.info(
                        "No further eligible borrowers available for progressive cycle",
                        campaign_id=str(campaign_id),
                    )
                    failed_count += 1
                    break
        finally:
            # Reclaim unallocated claimed agents immediately (FIND-CRIT-01 / Defect 4)
            allocated_agent_ids = {a.agent.id for a in allocations if a.agent is not None}
            unallocated_agent_ids = set(decision.claimed_agent_ids) - allocated_agent_ids
            if unallocated_agent_ids:
                for unallocated_id in unallocated_agent_ids:
                    ag = session.get(Agent, unallocated_id)
                    if ag and ag.status == AgentState.RESERVED.value:
                        AgentStateMachine.transition(ag, AgentState.AVAILABLE)
                        ag.reservation_token = None
                        ag.reserved_at = None
                        ag.current_call_id = None
                session.flush()
                logger.info(
                    "Cleaned up unclaimed agent reservations",
                    campaign_id=str(campaign_id),
                    count=len(unallocated_agent_ids),
                )

        session.flush()
        logger.info(
            "Progressive cycle completed",
            campaign_id=str(campaign_id),
            approved=decision.approved_calls,
            allocated=len(allocations),
            failed=failed_count,
        )

        return ProgressiveDialResult(
            campaign_id=campaign_id,
            safety_decision=decision,
            allocations=allocations,
            failed_allocations=failed_count,
            message=f"Allocated {len(allocations)} calls safely.",
        )
