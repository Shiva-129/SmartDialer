"""Predictive Dialer coordinating Pacing Engine, Safety Controller, and Call Allocator."""

import uuid
from dataclasses import dataclass, field
from typing import List, Optional
from sqlalchemy.orm import Session

from smartdialer.allocator.allocator import AllocationResult, CallAllocator
from smartdialer.core.logging import get_logger
from smartdialer.models.agent import Agent
from smartdialer.models.enums import AgentState, CampaignMode
from smartdialer.pacing.engine import PacingProposal, PredictivePacingEngine
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

logger = get_logger("predictive_dialer")


@dataclass
class PredictiveDialResult:
    """Outcome of a predictive dialing cycle."""
    campaign_id: uuid.UUID
    proposal: PacingProposal
    safety_decision: SafetyDecision
    allocations: List[AllocationResult] = field(default_factory=list)
    failed_allocations: int = 0
    message: str = ""


class PredictiveDialer:
    """
    Executes a predictive dialing cycle.
    
    CRITICAL INVARIANT:
    The pacing engine ONLY submits proposals to the Safety Controller.
    Neither strategy invokes a telecom provider directly or bypasses the Safety Controller.
    """

    @classmethod
    def execute_cycle(
        cls,
        session: Session,
        campaign_id: uuid.UUID,
        provider_name: str,
        simulated_answer_rate: Optional[float] = None,
        simulated_talk_duration_sec: Optional[float] = None,
    ) -> PredictiveDialResult:
        """
        Execute one predictive cycle through the mandatory safety boundary.
        
        Flow:
        1. Calculate statistical PacingProposal via PredictivePacingEngine.
        2. Submit proposal as DialingRequest to SafetyController.
        3. Allocate calls according to Safety Controller's approved count.
        """
        # Step 1: Calculate statistical pacing recommendation
        proposal = PredictivePacingEngine.calculate_pacing(
            session=session,
            campaign_id=campaign_id,
            provider_name=provider_name,
            simulated_answer_rate=simulated_answer_rate,
            simulated_talk_duration_sec=simulated_talk_duration_sec,
        )

        # Step 2: Mandatory submission to Safety Controller
        request = DialingRequest(
            campaign_id=campaign_id,
            mode=CampaignMode.PREDICTIVE,
            desired_calls=proposal.desired_calls,
            estimated_answer_rate=proposal.estimated_answer_probability,
            provider_name=provider_name,
            pacing_reasoning=proposal.explanation,
        )
        decision = SafetyController.evaluate(session, request)

        if decision.approved_calls <= 0:
            logger.info(
                "Predictive cycle completed with zero approvals",
                campaign_id=str(campaign_id),
                decision=decision.decision.value,
                reason=decision.reasoning,
            )
            return PredictiveDialResult(
                campaign_id=campaign_id,
                proposal=proposal,
                safety_decision=decision,
                allocations=[],
                message=decision.reasoning,
            )

        # Step 3: Perform atomic reservations and outbox preparation for approved calls
        allocations: List[AllocationResult] = []
        failed_count = 0

        target_agent_ids = list(decision.claimed_agent_ids) if decision.claimed_agent_ids else []
        if len(target_agent_ids) < decision.approved_calls:
            target_agent_ids.extend([None] * (decision.approved_calls - len(target_agent_ids)))

        try:
            for agent_id in target_agent_ids:
                try:
                    alloc_res = CallAllocator.allocate_call(
                        session=session,
                        campaign_id=campaign_id,
                        provider_name=provider_name,
                        agent_id=agent_id,
                        dialing_mode="PREDICTIVE",
                    )
                    allocations.append(alloc_res)
                except AgentNotAvailableError:
                    logger.warning(
                        "Agent pool exhausted during predictive cycle allocation",
                        campaign_id=str(campaign_id),
                    )
                    failed_count += 1
                    break
                except BorrowerNotEligibleError:
                    logger.info(
                        "Borrower pool exhausted during predictive cycle allocation",
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
                    "Cleaned up unclaimed agent reservations in predictive cycle",
                    campaign_id=str(campaign_id),
                    count=len(unallocated_agent_ids),
                )

        session.flush()
        logger.info(
            "Predictive cycle finished",
            campaign_id=str(campaign_id),
            proposed=proposal.desired_calls,
            approved=decision.approved_calls,
            allocated=len(allocations),
            decision=decision.decision.value,
        )

        return PredictiveDialResult(
            campaign_id=campaign_id,
            proposal=proposal,
            safety_decision=decision,
            allocations=allocations,
            failed_allocations=failed_count,
            message=f"Pacing proposed {proposal.desired_calls}, Safety Controller approved {decision.approved_calls}, allocated {len(allocations)}.",
        )
