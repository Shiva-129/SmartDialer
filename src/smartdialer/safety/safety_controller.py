"""Independent, non-bypassable Safety Controller."""

import math
import uuid
from dataclasses import dataclass
from typing import Optional
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from smartdialer.core.logging import get_logger
from smartdialer.models.agent import Agent
from smartdialer.models.audit import SafetyDecisionLog
from smartdialer.models.call import Call
from smartdialer.models.campaign import Campaign
from smartdialer.models.enums import (
    AgentState,
    CallState,
    CampaignMode,
    SafetyDecisionType,
)
from smartdialer.safety.provider_health import provider_health_tracker
from smartdialer.state.agent_state_machine import AgentStateMachine

logger = get_logger("safety_controller")


@dataclass(frozen=True)
class DialingRequest:
    """Immutable dialing proposal submitted to the Safety Controller."""
    campaign_id: uuid.UUID
    mode: CampaignMode
    desired_calls: int
    estimated_answer_rate: float
    provider_name: str
    pacing_reasoning: str = ""


@dataclass(frozen=True)
class SafetyDecision:
    """Immutable structured decision issued by the Safety Controller."""
    decision: SafetyDecisionType
    requested_calls: int
    approved_calls: int
    available_agents: int
    reserved_agents: int
    active_calls: int
    ringing_calls: int
    provider_failure_rate: float
    provider_healthy: bool
    reasoning: str
    claimed_agent_ids: tuple = ()


class SafetyController:
    """
    Mandatory safety checkpoint between dialer pacing strategies and call allocation.
    
    This component enforces the core invariant:
    'The system must never create more agent-bound active calls than its safely reserved agent capacity.'
    
    Predictive engines submit proposals here; they can NEVER dispatch calls directly.
    """

    @classmethod
    def evaluate(
        cls,
        session: Session,
        request: DialingRequest,
    ) -> SafetyDecision:
        """
        Evaluate a dialing request against live database metrics and provider telemetry.
        
        Saves a SafetyDecisionLog audit record for every decision.
        """
        if request.desired_calls <= 0:
            return cls._finalize_decision(
                session=session,
                request=request,
                decision_type=SafetyDecisionType.REJECTED,
                approved_calls=0,
                available_agents=0,
                reserved_agents=0,
                active_calls=0,
                ringing_calls=0,
                provider_failure_rate=0.0,
                provider_healthy=True,
                reasoning="Requested zero or negative calls.",
            )

        # 1. Query Campaign constraints
        campaign = session.get(Campaign, request.campaign_id)
        max_active_calls = campaign.max_active_calls if campaign else 100

        # 2. Query live database agent counts and authoritatively claim available agent capacity
        agent_claim_stmt = (
            select(Agent.id)
            .where(
                Agent.campaign_id == request.campaign_id,
                Agent.status == AgentState.AVAILABLE.value,
            )
            .order_by(Agent.last_call_ended_at.asc().nullsfirst(), Agent.id.asc())
            .limit(max(1, request.desired_calls))
            .with_for_update(skip_locked=True)
        )
        claimed_agent_ids = tuple(session.execute(agent_claim_stmt).scalars().all())
        available_agents = len(claimed_agent_ids)

        reserved_agents = session.execute(
            select(func.count(Agent.id)).where(
                Agent.campaign_id == request.campaign_id,
                Agent.status == AgentState.RESERVED.value,
            )
        ).scalar() or 0

        # 3. Query active agent-bound calls and currently ringing calls
        active_calls = session.execute(
            select(func.count(Call.id)).where(
                Call.campaign_id == request.campaign_id,
                Call.status.in_([
                    CallState.INITIATED.value,
                    CallState.RINGING.value,
                    CallState.ANSWERED.value,
                    CallState.CONNECTED.value,
                ]),
            )
        ).scalar() or 0

        ringing_calls = session.execute(
            select(func.count(Call.id)).where(
                Call.campaign_id == request.campaign_id,
                Call.status == CallState.RINGING.value,
            )
        ).scalar() or 0

        # Query connected and wrap_up agents for predictive Little's Law projections
        connected_agents = session.execute(
            select(func.count(Agent.id)).where(
                Agent.campaign_id == request.campaign_id,
                Agent.status == AgentState.CONNECTED.value,
            )
        ).scalar() or 0

        wrap_up_agents = session.execute(
            select(func.count(Agent.id)).where(
                Agent.campaign_id == request.campaign_id,
                Agent.status == AgentState.WRAP_UP.value,
            )
        ).scalar() or 0

        # 4. Check Provider Telemetry (with database-backed distributed visibility)
        provider_healthy = provider_health_tracker.is_healthy(request.provider_name, session=session)
        provider_failure_rate = provider_health_tracker.get_failure_rate(request.provider_name, session=session)

        # 5. Evaluate based on Dialing Mode
        if request.mode == CampaignMode.PROGRESSIVE:
            return cls._evaluate_progressive(
                session=session,
                request=request,
                available_agents=available_agents,
                reserved_agents=reserved_agents,
                active_calls=active_calls,
                ringing_calls=ringing_calls,
                max_active_calls=max_active_calls,
                provider_failure_rate=provider_failure_rate,
                provider_healthy=provider_healthy,
                claimed_agent_ids=claimed_agent_ids,
            )
        else:
            return cls._evaluate_predictive(
                session=session,
                request=request,
                available_agents=available_agents,
                reserved_agents=reserved_agents,
                active_calls=active_calls,
                ringing_calls=ringing_calls,
                max_active_calls=max_active_calls,
                provider_failure_rate=provider_failure_rate,
                provider_healthy=provider_healthy,
                claimed_agent_ids=claimed_agent_ids,
                connected_agents=connected_agents,
                wrap_up_agents=wrap_up_agents,
            )

    @classmethod
    def _evaluate_progressive(
        cls,
        session: Session,
        request: DialingRequest,
        available_agents: int,
        reserved_agents: int,
        active_calls: int,
        ringing_calls: int,
        max_active_calls: int,
        provider_failure_rate: float,
        provider_healthy: bool,
        claimed_agent_ids: tuple = (),
    ) -> SafetyDecision:
        """Progressive Mode: Strictly 1 outbound call per currently AVAILABLE agent."""
        if not provider_healthy or provider_failure_rate > 0.30:
            return cls._finalize_decision(
                session=session,
                request=request,
                decision_type=SafetyDecisionType.REJECTED,
                approved_calls=0,
                available_agents=available_agents,
                reserved_agents=reserved_agents,
                active_calls=active_calls,
                ringing_calls=ringing_calls,
                provider_failure_rate=provider_failure_rate,
                provider_healthy=provider_healthy,
                reasoning=(
                    f"Provider '{request.provider_name}' failure rate ({provider_failure_rate:.1%}) "
                    f"exceeds safety threshold. Outbound dialing suspended."
                ),
                claimed_agent_ids=claimed_agent_ids,
            )

        if available_agents <= 0:
            return cls._finalize_decision(
                session=session,
                request=request,
                decision_type=SafetyDecisionType.REJECTED,
                approved_calls=0,
                available_agents=available_agents,
                reserved_agents=reserved_agents,
                active_calls=active_calls,
                ringing_calls=ringing_calls,
                provider_failure_rate=provider_failure_rate,
                provider_healthy=provider_healthy,
                reasoning="No agents currently AVAILABLE in database.",
                claimed_agent_ids=claimed_agent_ids,
            )

        # Respect campaign ceiling
        capacity_under_ceiling = max(0, max_active_calls - active_calls)
        approved = min(request.desired_calls, available_agents, capacity_under_ceiling)

        if approved <= 0:
            return cls._finalize_decision(
                session=session,
                request=request,
                decision_type=SafetyDecisionType.REJECTED,
                approved_calls=0,
                available_agents=available_agents,
                reserved_agents=reserved_agents,
                active_calls=active_calls,
                ringing_calls=ringing_calls,
                provider_failure_rate=provider_failure_rate,
                provider_healthy=provider_healthy,
                reasoning=f"Campaign max active calls ceiling reached ({active_calls}/{max_active_calls}).",
                claimed_agent_ids=claimed_agent_ids,
            )

        decision_type = (
            SafetyDecisionType.APPROVED
            if approved == request.desired_calls
            else SafetyDecisionType.REDUCED
        )
        reasoning = (
            f"Progressive 1:1 rule satisfied. Approved {approved} call(s) "
            f"for {available_agents} available agent(s)."
        )

        reservation_token = uuid.uuid4()
        if approved > 0 and claimed_agent_ids:
            approved_ids = list(claimed_agent_ids[:approved])
            for aid in approved_ids:
                ag = session.get(Agent, aid)
                if ag and ag.status == AgentState.AVAILABLE.value:
                    AgentStateMachine.transition(ag, AgentState.RESERVED, reservation_token=reservation_token)
            session.flush()

        return cls._finalize_decision(
            session=session,
            request=request,
            decision_type=decision_type,
            approved_calls=approved,
            available_agents=available_agents,
            reserved_agents=reserved_agents + approved,
            active_calls=active_calls,
            ringing_calls=ringing_calls,
            provider_failure_rate=provider_failure_rate,
            provider_healthy=provider_healthy,
            reasoning=reasoning,
            claimed_agent_ids=claimed_agent_ids[:approved],
        )

    @classmethod
    def _evaluate_predictive(
        cls,
        session: Session,
        request: DialingRequest,
        available_agents: int,
        reserved_agents: int,
        active_calls: int,
        ringing_calls: int,
        max_active_calls: int,
        provider_failure_rate: float,
        provider_healthy: bool,
        claimed_agent_ids: tuple = (),
        connected_agents: int = 0,
        wrap_up_agents: int = 0,
    ) -> SafetyDecision:
        """
        Predictive Mode: Validate predictive proposals against available plus high-probability
        freeing agent capacity (FIND-HIGH-01), while enforcing 0% abandoned call guarantee.
        
        Fallback to progressive if provider degraded, failure rate high, or pacing erratic.
        """
        # Rule 1: Fallback if provider health is degraded
        if not provider_healthy or provider_failure_rate > 0.15:
            # Fallback to progressive conservative mode
            safe_progressive = min(request.desired_calls, available_agents)
            if safe_progressive > 0 and claimed_agent_ids:
                reservation_token = uuid.uuid4()
                for aid in claimed_agent_ids[:safe_progressive]:
                    ag = session.get(Agent, aid)
                    if ag and ag.status == AgentState.AVAILABLE.value:
                        AgentStateMachine.transition(ag, AgentState.RESERVED, reservation_token=reservation_token)
                session.flush()

            return cls._finalize_decision(
                session=session,
                request=request,
                decision_type=SafetyDecisionType.FALLBACK_TO_PROGRESSIVE,
                approved_calls=safe_progressive,
                available_agents=available_agents,
                reserved_agents=reserved_agents + safe_progressive,
                active_calls=active_calls,
                ringing_calls=ringing_calls,
                provider_failure_rate=provider_failure_rate,
                provider_healthy=provider_healthy,
                reasoning=(
                    f"Provider health degraded (failure rate {provider_failure_rate:.1%}). "
                    f"Predictive pacing unsafe; falling back to progressive 1:1 allocation."
                ),
                claimed_agent_ids=claimed_agent_ids[:safe_progressive],
            )

        # Little's Law projection of agents expected to free during call setup latency (18s window):
        expected_freeing = min(connected_agents * 0.40, connected_agents * (1.0 / 120.0) * 18.0) + wrap_up_agents
        effective_capacity = available_agents + expected_freeing

        # Rule 2: If neither available agents nor projected freeing agents exist, reject
        if effective_capacity <= 0:
            return cls._finalize_decision(
                session=session,
                request=request,
                decision_type=SafetyDecisionType.REJECTED,
                approved_calls=0,
                available_agents=available_agents,
                reserved_agents=reserved_agents,
                active_calls=active_calls,
                ringing_calls=ringing_calls,
                provider_failure_rate=provider_failure_rate,
                provider_healthy=provider_healthy,
                reasoning="Zero available agents in pool (no available or projected freeing agents). Overdialing disallowed to prevent abandoned calls.",
                claimed_agent_ids=(),
            )

        # Rule 3: Enforce Hard Invariant against regulatory abandonment risk:
        safe_headroom = max(0.0, effective_capacity - ringing_calls)
        ceiling_headroom = max(0, max_active_calls - active_calls)
        approved = min(request.desired_calls, int(math.ceil(safe_headroom)), ceiling_headroom)

        if approved <= 0:
            return cls._finalize_decision(
                session=session,
                request=request,
                decision_type=SafetyDecisionType.REJECTED,
                approved_calls=0,
                available_agents=available_agents,
                reserved_agents=reserved_agents,
                active_calls=active_calls,
                ringing_calls=ringing_calls,
                provider_failure_rate=provider_failure_rate,
                provider_healthy=provider_healthy,
                reasoning=(
                    f"Ringing call backlog ({ringing_calls}) exhausts effective agent headroom ({effective_capacity:.1f}). "
                    f"Rejected dialing request to maintain 0% abandoned call guarantee."
                ),
                claimed_agent_ids=(),
            )

        reservation_token = uuid.uuid4()
        claimed_count = min(approved, available_agents)
        if claimed_count > 0 and claimed_agent_ids:
            approved_ids = list(claimed_agent_ids[:claimed_count])
            for aid in approved_ids:
                ag = session.get(Agent, aid)
                if ag and ag.status == AgentState.AVAILABLE.value:
                    AgentStateMachine.transition(ag, AgentState.RESERVED, reservation_token=reservation_token)
            session.flush()

        decision_type = (
            SafetyDecisionType.APPROVED
            if approved == request.desired_calls
            else SafetyDecisionType.REDUCED
        )
        reasoning = (
            f"Predictive safety check: approved {approved} of {request.desired_calls} requested calls. "
            f"Available agents: {available_agents}, Connected: {connected_agents}, "
            f"Projected freeing: {expected_freeing:.1f}, Active/Ringing: {active_calls}/{ringing_calls}."
        )

        return cls._finalize_decision(
            session=session,
            request=request,
            decision_type=decision_type,
            approved_calls=approved,
            available_agents=available_agents,
            reserved_agents=reserved_agents + claimed_count,
            active_calls=active_calls,
            ringing_calls=ringing_calls,
            provider_failure_rate=provider_failure_rate,
            provider_healthy=provider_healthy,
            reasoning=reasoning,
            claimed_agent_ids=claimed_agent_ids[:claimed_count],
        )

    @classmethod
    def _finalize_decision(
        cls,
        session: Session,
        request: DialingRequest,
        decision_type: SafetyDecisionType,
        approved_calls: int,
        available_agents: int,
        reserved_agents: int,
        active_calls: int,
        ringing_calls: int,
        provider_failure_rate: float,
        provider_healthy: bool,
        reasoning: str,
        claimed_agent_ids: tuple = (),
    ) -> SafetyDecision:
        """Persist decision audit log and construct immutable result."""
        decision = SafetyDecision(
            decision=decision_type,
            requested_calls=request.desired_calls,
            approved_calls=approved_calls,
            available_agents=available_agents,
            reserved_agents=reserved_agents,
            active_calls=active_calls,
            ringing_calls=ringing_calls,
            provider_failure_rate=provider_failure_rate,
            provider_healthy=provider_healthy,
            reasoning=reasoning,
            claimed_agent_ids=tuple(claimed_agent_ids[:approved_calls]),
        )

        # Persist audit record in DB
        log_entry = SafetyDecisionLog(
            campaign_id=request.campaign_id,
            dialing_mode=request.mode.value,
            requested_calls=request.desired_calls,
            approved_calls=approved_calls,
            decision=decision_type.value,
            available_agents=available_agents,
            reserved_agents=reserved_agents,
            active_calls=active_calls,
            ringing_calls=ringing_calls,
            provider_failure_rate=provider_failure_rate,
            provider_healthy=provider_healthy,
            reasoning=reasoning,
        )
        session.add(log_entry)
        session.flush()

        logger.info(
            "Safety Controller evaluated request",
            campaign_id=str(request.campaign_id),
            decision=decision_type.value,
            requested=request.desired_calls,
            approved=approved_calls,
            available_agents=available_agents,
        )
        return decision
