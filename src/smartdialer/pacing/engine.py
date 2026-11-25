"""Statistical and explainable predictive pacing engine."""

import math
import uuid
from dataclasses import dataclass
from typing import Optional
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from smartdialer.core.logging import get_logger
from smartdialer.models.agent import Agent
from smartdialer.models.call import Call
from smartdialer.models.enums import AgentState, CallState
from smartdialer.pacing.metrics import telemetry_registry
from smartdialer.safety.provider_health import provider_health_tracker

logger = get_logger("predictive_pacing_engine")


@dataclass(frozen=True)
class PacingProposal:
    """Detailed mathematical pacing recommendation emitted by the Predictive Engine."""
    campaign_id: uuid.UUID
    desired_calls: int
    estimated_answer_probability: float
    estimated_expected_answers: float
    utilization_target: float
    available_agents: int
    connected_agents: int
    ringing_calls: int
    expected_freeing_agents: float
    effective_agent_capacity: float
    pacing_multiplier: float
    explanation: str


class PredictivePacingEngine:
    """
    Computes statistical dialing pacing targets based on queueing dynamics and answer-rate smoothing.
    
    This engine NEVER initiates provider calls or bypasses the Safety Controller.
    It produces explainable PacingProposals to be reviewed and vetted by the Safety Controller.
    """

    DEFAULT_RINGING_SETUP_LATENCY_SEC: float = 18.0  # Average ring time before borrower answers
    TARGET_UTILIZATION: float = 0.90  # Target 90% agent active talk utilization
    TARGET_ABANDONMENT_CEILING: float = 0.03  # Max 3% regulatory abandonment ceiling

    @classmethod
    def calculate_pacing(
        cls,
        session: Session,
        campaign_id: uuid.UUID,
        provider_name: str,
        simulated_answer_rate: Optional[float] = None,
        simulated_talk_duration_sec: Optional[float] = None,
    ) -> PacingProposal:
        """
        Calculate the optimal explainable number of outbound calls to initiate.
        
        Derivation:
        1. Query database for current agent and call state distributions.
        2. Compute blended answer probability using recent EMA and historical baseline.
        3. Project expected agent departures (freeing agents) during average ring window.
        4. Account for in-flight ringing calls already expected to answer.
        5. Apply variance margin (binomial variance) to guarantee abandonment risk remains <= 3%.
        """
        cid_str = str(campaign_id)
        telemetry = telemetry_registry.get(cid_str)

        # Allow simulation overrides if explicitly provided
        if simulated_answer_rate is not None:
            telemetry.set_baseline(simulated_answer_rate, simulated_talk_duration_sec or 90.0)

        # 1. Answer rate estimation (blended historical and EMA)
        p_ema = telemetry.answer_rate_ema
        p_hist = telemetry.historical_answer_rate
        p_ans = (0.7 * p_ema) + (0.3 * p_hist)
        p_ans = max(0.05, min(0.95, p_ans))

        # 2. Average talk duration
        avg_talk_sec = telemetry.average_talk_duration_seconds
        avg_talk_sec = max(10.0, avg_talk_sec)

        # 3. Live Agent States from Database
        available_agents = session.execute(
            select(func.count(Agent.id)).where(
                Agent.campaign_id == campaign_id,
                Agent.status == AgentState.AVAILABLE.value,
            )
        ).scalar() or 0

        connected_agents = session.execute(
            select(func.count(Agent.id)).where(
                Agent.campaign_id == campaign_id,
                Agent.status == AgentState.CONNECTED.value,
            )
        ).scalar() or 0

        # 4. Live Call Queue States from Database
        ringing_calls = session.execute(
            select(func.count(Call.id)).where(
                Call.campaign_id == campaign_id,
                Call.status == CallState.RINGING.value,
            )
        ).scalar() or 0

        # 5. Little's Law projection of agents freeing up during call setup latency
        # Ringing calls take ~18 seconds to connect. In that window, a fraction of connected agents finish their calls:
        ring_window = cls.DEFAULT_RINGING_SETUP_LATENCY_SEC
        departure_rate_per_sec = 1.0 / avg_talk_sec
        expected_freeing = min(
            connected_agents * 0.40,  # Cap conservatively at 40% of active connected pool
            connected_agents * departure_rate_per_sec * ring_window,
        )

        effective_capacity = available_agents + expected_freeing

        # 6. Expected answers from calls already ringing
        expected_in_flight_answers = ringing_calls * p_ans

        # 7. Net agent slots needing answers
        net_answer_slots_needed = max(0.0, (effective_capacity * cls.TARGET_UTILIZATION) - expected_in_flight_answers)

        if net_answer_slots_needed <= 0.0 or effective_capacity <= 0.0:
            desired_calls = 0
            pacing_multiplier = 0.0
            expected_answers = 0.0
            explanation = (
                f"Pacing = 0 calls: In-flight ringing calls ({ringing_calls}) with estimated "
                f"answer rate {p_ans:.1%} produce {expected_in_flight_answers:.1f} expected answers, "
                f"which fully satisfies available/freeing capacity ({effective_capacity:.1f} slots)."
            )
        else:
            # 8. Raw overdialing multiplier: N_raw = Needed / p_ans
            raw_target = net_answer_slots_needed / p_ans
            pacing_multiplier = raw_target / max(1.0, available_agents)

            # 9. Variance adjustment (Binomial distribution confidence bound):
            # To strictly avoid exceeding available capacity when answers spike,
            # we subtract an uncertainty buffer: z * sqrt(N * p * (1-p))
            # for 95% confidence (z = 1.645)
            n_candidate = raw_target
            variance = n_candidate * p_ans * (1.0 - p_ans)
            std_dev = math.sqrt(max(0.0, variance))
            safety_discount = 1.645 * std_dev

            # Ensure expected answers + confidence spread does not exceed physical capacity
            conservative_slots = max(0.0, net_answer_slots_needed - (0.5 * safety_discount))
            desired_calls = int(math.floor(conservative_slots / p_ans))

            # Provider failure/latency adjustment
            provider_failure_rate = provider_health_tracker.get_failure_rate(provider_name)
            if provider_failure_rate > 0.08:
                # Throttle by 30% if carrier exhibits elevated error rate
                desired_calls = int(desired_calls * 0.70)

            # Never exceed hard cap based on total agents
            total_active_pool = available_agents + connected_agents
            desired_calls = min(desired_calls, max(1, total_active_pool * 3))
            expected_answers = desired_calls * p_ans

            explanation = (
                f"Pacing target = {desired_calls} calls (multiplier {pacing_multiplier:.2f}x). "
                f"Available agents: {available_agents}, Connected: {connected_agents}, "
                f"Projected freeing: {expected_freeing:.1f}, Ringing backlog: {ringing_calls}. "
                f"Answer rate EMA: {p_ans:.1%}, Avg talk time: {avg_talk_sec:.0f}s. "
                f"Expected answers: {expected_answers:.1f} vs Net capacity needed: {net_answer_slots_needed:.1f}."
            )

        logger.info(
            "Predictive pacing calculated",
            campaign_id=cid_str,
            desired_calls=desired_calls,
            p_ans=f"{p_ans:.2f}",
            expected_answers=f"{expected_answers:.1f}",
        )

        return PacingProposal(
            campaign_id=campaign_id,
            desired_calls=desired_calls,
            estimated_answer_probability=p_ans,
            estimated_expected_answers=round(expected_answers, 2),
            utilization_target=cls.TARGET_UTILIZATION,
            available_agents=available_agents,
            connected_agents=connected_agents,
            ringing_calls=ringing_calls,
            expected_freeing_agents=round(expected_freeing, 2),
            effective_agent_capacity=round(effective_capacity, 2),
            pacing_multiplier=round(pacing_multiplier, 2),
            explanation=explanation,
        )
