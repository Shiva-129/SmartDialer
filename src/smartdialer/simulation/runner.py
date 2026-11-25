"""
SmartDialer Simulation Engine:
Executes the quantitative predictive scenarios (A, B, C, D) and distributed failure
demonstrations specified in the Tech Assignment against real PostgreSQL transactions.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
import random
import uuid
from typing import Any, Dict, List, Optional
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from smartdialer.allocator.allocator import CallAllocator
from smartdialer.core.logging import get_logger
from smartdialer.dialer.predictive import PredictiveDialer
from smartdialer.events.processor import EventProcessor
from smartdialer.models.agent import Agent
from smartdialer.models.borrower import Borrower
from smartdialer.models.call import Call
from smartdialer.models.campaign import Campaign
from smartdialer.models.enums import AgentState, BorrowerState, CallState, CampaignMode
from smartdialer.models.provider_request import ProviderRequestLedger
from smartdialer.pacing.metrics import telemetry_registry
from smartdialer.providers.registry import provider_registry
from smartdialer.recovery.outbox_dispatcher import OutboxDispatcher
from smartdialer.recovery.reconciler import ReconciliationSweeper
from smartdialer.safety.provider_health import provider_health_tracker
from smartdialer.safety.safety_controller import DialingRequest, SafetyController
from smartdialer.state.agent_state_machine import AgentStateMachine

logger = get_logger("simulation_runner")


@dataclass
class ScenarioMetrics:
    scenario: str
    description: str
    answer_rate_target: float
    avg_talk_time_sec: float
    provider_name: str
    num_agents: int
    calls_initiated: int = 0
    calls_answered: int = 0
    calls_connected: int = 0
    calls_completed: int = 0
    calls_abandoned: int = 0
    calls_failed: int = 0
    provider_healthy: bool = True
    safety_invariant_violations: int = 0
    agent_utilization_pct: float = 0.0
    abandonment_rate_pct: float = 0.0
    pacing_decisions: List[Dict[str, Any]] = field(default_factory=list)
    safety_decisions: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SimulationRunner:
    """Runs end-to-end campaign simulations through real Safety Controller & Dialers."""

    SCENARIOS: Dict[str, Dict[str, Any]] = {
        "A": {
            "description": "Low Answer Rate (20%), Standard Talk Time (120s)",
            "answer_rate": 0.20,
            "talk_time_sec": 120.0,
            "dynamic": False,
        },
        "B": {
            "description": "Balanced Answer Rate (50%), Short Talk Time (90s)",
            "answer_rate": 0.50,
            "talk_time_sec": 90.0,
            "dynamic": False,
        },
        "C": {
            "description": "High Answer Rate (70%), Long Talk Time (180s)",
            "answer_rate": 0.70,
            "talk_time_sec": 180.0,
            "dynamic": False,
        },
        "D": {
            "description": "Changing Regime (70% -> 10% shock + Provider failures)",
            "answer_rate": 0.70,
            "talk_time_sec": 90.0,
            "dynamic": True,
        },
    }

    @classmethod
    def run_scenario(
        cls,
        session_factory: sessionmaker,
        scenario_key: str,
        num_agents: int = 20,
        num_borrowers: int = 80,
        cycles: int = 4,
        provider_name: str = "provider_a",
        seed: int = 42,
    ) -> ScenarioMetrics:
        """Execute a predictive scenario end-to-end against PostgreSQL."""
        rng = random.Random(seed)
        cfg = cls.SCENARIOS[scenario_key]
        base_answer_rate = cfg["answer_rate"]
        base_talk_time = cfg["talk_time_sec"]
        is_dynamic = cfg["dynamic"]

        # Ensure clean provider health for scenario start
        provider = provider_registry.get(provider_name)
        orig_failure_rate = getattr(provider, "failure_rate", 0.0) if provider else 0.0
        if provider:
            provider.failure_rate = 0.15 if is_dynamic else 0.0

        with session_factory() as session:
            provider_health_tracker.reset(provider_name, session=session)
            campaign = Campaign(
                name=f"Sim Scenario {scenario_key} ({uuid.uuid4().hex[:6]})",
                mode=CampaignMode.PREDICTIVE.value,
                max_active_calls=max(50, num_agents * 4),
            )
            session.add(campaign)
            session.flush()

            # Pre-seed half agents CONNECTED and half AVAILABLE for realistic steady state
            agents = []
            for i in range(num_agents):
                st = AgentState.AVAILABLE.value if i < (num_agents // 2) else AgentState.CONNECTED.value
                agents.append(Agent(campaign_id=campaign.id, name=f"SimAgent-{i+1}", status=st))
            session.add_all(agents)

            borrowers = [
                Borrower(
                    campaign_id=campaign.id,
                    phone_number=f"+1555{rng.randint(1000000, 9999999)}",
                    name=f"SimBorrower-{i+1}",
                    priority=i % 5,
                )
                for i in range(num_borrowers)
            ]
            session.add_all(borrowers)
            session.commit()
            campaign_id = campaign.id

        # Seed telemetry baseline
        telem = telemetry_registry.get(str(campaign_id))
        telem.set_baseline(answer_rate=base_answer_rate, talk_duration_sec=base_talk_time)

        metrics = ScenarioMetrics(
            scenario=scenario_key,
            description=cfg["description"],
            answer_rate_target=base_answer_rate,
            avg_talk_time_sec=base_talk_time,
            provider_name=provider_name,
            num_agents=num_agents,
        )

        busy_slots_samples: List[float] = []

        try:
            for cycle_idx in range(1, cycles + 1):
                # In Scenario D, inject regime shock halfway through + agent availability drop!
                current_ans_rate = base_answer_rate
                current_talk = base_talk_time
                if is_dynamic and cycle_idx > max(1, cycles // 2):
                    current_ans_rate = 0.10
                    current_talk = 150.0
                    with session_factory() as session:
                        avail_to_drop = session.execute(
                            select(Agent).where(
                                Agent.campaign_id == campaign_id,
                                Agent.status == AgentState.AVAILABLE.value,
                            ).limit(max(1, num_agents // 5))
                        ).scalars().all()
                        for ag in avail_to_drop:
                            AgentStateMachine.transition(ag, AgentState.PAUSED)
                        session.commit()

                # Simulate some connected agents finishing wrap-up between cycles
                with session_factory() as session:
                    conn_agents = session.execute(
                        select(Agent).where(
                            Agent.campaign_id == campaign_id,
                            Agent.status == AgentState.CONNECTED.value,
                        )
                    ).scalars().all()
                    for ag in conn_agents:
                        if rng.random() < 0.35:
                            AgentStateMachine.transition(ag, AgentState.WRAP_UP)
                            AgentStateMachine.transition(ag, AgentState.AVAILABLE)
                    session.commit()

                # 1. Execute Predictive Dialing Cycle through Safety Controller
                with session_factory() as session:
                    dial_res = PredictiveDialer.execute_cycle(
                        session=session,
                        campaign_id=campaign_id,
                        provider_name=provider_name,
                        simulated_answer_rate=current_ans_rate,
                        simulated_talk_duration_sec=current_talk,
                    )
                    session.commit()

                    metrics.pacing_decisions.append({
                        "cycle": cycle_idx,
                        "answer_rate_est": round(dial_res.proposal.estimated_answer_probability, 3),
                        "desired_calls": dial_res.proposal.desired_calls,
                        "approved_calls": dial_res.safety_decision.approved_calls,
                        "explanation": dial_res.proposal.explanation,
                    })
                    metrics.safety_decisions.append({
                        "cycle": cycle_idx,
                        "decision": dial_res.safety_decision.decision.value,
                        "requested": dial_res.safety_decision.requested_calls,
                        "approved": dial_res.safety_decision.approved_calls,
                        "reasoning": dial_res.safety_decision.reasoning,
                    })
                    allocated_call_ids = [a.call.id for a in dial_res.allocations]

                # 2. Dispatch via Outbox Dispatcher (scoped to calls allocated in this scenario cycle)
                with session_factory() as session:
                    if allocated_call_ids:
                        dispatched_ids = OutboxDispatcher.dispatch_pending(
                            session=session,
                            limit=max(200, len(allocated_call_ids)),
                            call_ids=allocated_call_ids,
                        )
                        metrics.calls_initiated += len(dispatched_ids)

                # 3. Simulate Carrier Webhooks for initiated calls
                for cid in allocated_call_ids:
                    with session_factory() as session:
                        c = session.get(Call, cid)
                        if not c or c.status != CallState.INITIATED.value:
                            continue

                        EventProcessor.process_event(
                            session=session,
                            provider_name=provider_name,
                            event_id=f"sim-ring-{cid}-{cycle_idx}",
                            call_id=cid,
                            event_type="RINGING",
                        )

                        answered = rng.random() < current_ans_rate
                        if answered:
                            metrics.calls_answered += 1
                            EventProcessor.process_event(
                                session=session,
                                provider_name=provider_name,
                                event_id=f"sim-ans-{cid}-{cycle_idx}",
                                call_id=cid,
                                event_type="ANSWERED",
                                raw_payload={"answer_timeout_sec": 0.02},
                            )
                            c_after = session.get(Call, cid)
                            # If progressive/pre-assigned agent answered, send CONNECTED webhook to complete bridge
                            if c_after.status == CallState.ANSWERED.value:
                                EventProcessor.process_event(
                                    session=session,
                                    provider_name=provider_name,
                                    event_id=f"sim-conn-{cid}-{cycle_idx}",
                                    call_id=cid,
                                    event_type="CONNECTED",
                                )
                                c_after = session.get(Call, cid)

                            if c_after.status == CallState.CONNECTED.value:
                                metrics.calls_connected += 1
                                # Complete a portion of calls immediately, keep others busy to model occupancy
                                if rng.random() < 0.65:
                                    talk_dur = max(15.0, rng.gauss(current_talk, current_talk * 0.2))
                                    EventProcessor.process_event(
                                        session=session,
                                        provider_name=provider_name,
                                        event_id=f"sim-comp-{cid}-{cycle_idx}",
                                        call_id=cid,
                                        event_type="COMPLETED",
                                        raw_payload={"duration_seconds": round(talk_dur, 1)},
                                    )
                                    metrics.calls_completed += 1
                                    # Agent finishes wrap-up before next wave
                                    if c_after.agent_id:
                                        ag_done = session.get(Agent, c_after.agent_id)
                                        if ag_done and ag_done.status == AgentState.WRAP_UP.value:
                                            AgentStateMachine.transition(ag_done, AgentState.AVAILABLE)
                            elif c_after.termination_reason == "ABANDONED":
                                metrics.calls_abandoned += 1
                        else:
                            metrics.calls_failed += 1
                            EventProcessor.process_event(
                                session=session,
                                provider_name=provider_name,
                                event_id=f"sim-fail-{cid}-{cycle_idx}",
                                call_id=cid,
                                event_type="FAILED",
                                raw_payload={"reason": "NO_ANSWER"},
                            )
                        session.commit()

                # Record cycle agent utilization & verify core safety invariant
                with session_factory() as session:
                    all_agents = session.execute(
                        select(Agent).where(Agent.campaign_id == campaign_id)
                    ).scalars().all()
                    busy = sum(
                        1
                        for ag in all_agents
                        if ag.status in (AgentState.CONNECTED.value, AgentState.DIALING.value, AgentState.RESERVED.value)
                    )
                    busy_slots_samples.append(busy / max(1, len(all_agents)))
                    non_offline = sum(1 for ag in all_agents if ag.status not in (AgentState.OFFLINE.value, AgentState.PAUSED.value))
                    connected_calls_now = session.execute(
                        select(Call).where(Call.campaign_id == campaign_id, Call.status == CallState.CONNECTED.value)
                    ).scalars().all()
                    if len(connected_calls_now) > len(all_agents):
                        metrics.safety_invariant_violations += 1
                    metrics.provider_healthy = provider_health_tracker.is_healthy(provider_name, session=session)
        finally:
            if provider:
                provider.failure_rate = orig_failure_rate
            with session_factory() as session:
                provider_health_tracker.reset(provider_name, session=session)
                session.commit()

        if busy_slots_samples:
            metrics.agent_utilization_pct = round(
                (sum(busy_slots_samples) / len(busy_slots_samples)) * 100.0, 2
            )
        if metrics.calls_answered > 0:
            metrics.abandonment_rate_pct = round(
                (metrics.calls_abandoned / metrics.calls_answered) * 100.0, 2
            )
        return metrics

    @classmethod
    def run_all_scenarios(
        cls,
        session_factory: sessionmaker,
        num_agents: int = 20,
        num_borrowers: int = 80,
        cycles: int = 4,
        provider_name: str = "provider_a",
    ) -> List[ScenarioMetrics]:
        return [
            cls.run_scenario(
                session_factory=session_factory,
                scenario_key=key,
                num_agents=num_agents,
                num_borrowers=num_borrowers,
                cycles=cycles,
                provider_name=provider_name,
            )
            for key in ("A", "B", "C", "D")
        ]

    @classmethod
    def run_failure_demos(cls, session_factory: sessionmaker) -> Dict[str, Any]:
        """Run all 6 failure mode demonstrations (A-F) required by Section 11 of the assignment."""
        results: Dict[str, Any] = {}

        # A. Worker Crash recovery demo (before & after carrier initiation without duplicate calls)
        with session_factory() as session:
            c = Campaign(name="FailDemo-A-Crash")
            session.add(c)
            session.flush()
            ag = Agent(campaign_id=c.id, name="CrashAgent", status=AgentState.AVAILABLE.value)
            b = Borrower(campaign_id=c.id, phone_number="+15550101001", name="CrashBorrower")
            session.add_all([ag, b])
            session.flush()
            alloc = CallAllocator.allocate_call(session, c.id, "provider_a", agent_id=ag.id, borrower_id=b.id)
            alloc.call.created_at = datetime.now(timezone.utc) - timedelta(seconds=45)
            ag.reserved_at = datetime.now(timezone.utc) - timedelta(seconds=45)
            session.commit()
            call_id, ag_id, b_id = alloc.call.id, ag.id, b.id

        with session_factory() as session:
            sweep = ReconciliationSweeper.run_sweep(session)
            session.commit()
            c_row = session.get(Call, call_id)
            ag_row = session.get(Agent, ag_id)
            b_row = session.get(Borrower, b_id)
            results["worker_crash_recovery"] = {
                "call_status": c_row.status,
                "agent_status": ag_row.status,
                "borrower_status": b_row.status,
                "reclaimed_agents": sweep.reclaimed_agents_count,
            }

        # B. Provider Outage & Recovery demo
        with session_factory() as session:
            c = Campaign(name="FailDemo-B-Outage", mode=CampaignMode.PREDICTIVE.value)
            session.add(c)
            session.flush()
            ag1 = Agent(campaign_id=c.id, name="OutageAgent-1", status=AgentState.AVAILABLE.value)
            ag2 = Agent(campaign_id=c.id, name="OutageAgent-2", status=AgentState.AVAILABLE.value)
            session.add_all([ag1, ag2])
            session.commit()
            cid = c.id

        with session_factory() as session:
            provider_health_tracker.set_manual_status("provider_a", degraded=True, session=session)
            session.commit()

        with session_factory() as session:
            outage_decision = SafetyController.evaluate(
                session,
                DialingRequest(
                    campaign_id=cid,
                    mode=CampaignMode.PREDICTIVE,
                    desired_calls=1,
                    estimated_answer_rate=0.5,
                    provider_name="provider_a",
                    pacing_reasoning="Outage demo request",
                ),
            )
            provider_health_tracker.set_manual_status("provider_a", degraded=False, session=session)
            session.commit()

        with session_factory() as session:
            recovered_decision = SafetyController.evaluate(
                session,
                DialingRequest(
                    campaign_id=cid,
                    mode=CampaignMode.PREDICTIVE,
                    desired_calls=1,
                    estimated_answer_rate=0.5,
                    provider_name="provider_a",
                    pacing_reasoning="Recovered demo request",
                ),
            )
            session.commit()
            results["provider_outage_and_recovery"] = {
                "outage_decision": outage_decision.decision.value,
                "outage_approved": outage_decision.approved_calls,
                "recovered_decision": recovered_decision.decision.value,
                "recovered_approved": recovered_decision.approved_calls,
            }

        # C. Sudden Agent Availability Drop (100 available -> 40 disappear -> 60 safe limit)
        with session_factory() as session:
            c = Campaign(name="FailDemo-C-AgentDrop", mode=CampaignMode.PROGRESSIVE.value, max_active_calls=150)
            session.add(c)
            session.flush()
            agents_100 = [
                Agent(campaign_id=c.id, name=f"DropAg-{i}", status=AgentState.AVAILABLE.value)
                for i in range(100)
            ]
            session.add_all(agents_100)
            session.flush()
            # 40 agents suddenly become unavailable (20 OFFLINE, 20 PAUSED)
            for ag in agents_100[:20]:
                AgentStateMachine.transition(ag, AgentState.OFFLINE)
            for ag in agents_100[20:40]:
                AgentStateMachine.transition(ag, AgentState.PAUSED)
            session.commit()
            drop_cid = c.id

        with session_factory() as session:
            drop_decision = SafetyController.evaluate(
                session,
                DialingRequest(
                    campaign_id=drop_cid,
                    mode=CampaignMode.PROGRESSIVE,
                    desired_calls=100,  # Stale pacing asks for 100
                    estimated_answer_rate=1.0,
                    provider_name="provider_a",
                    pacing_reasoning="Stale request before seeing 40-agent drop",
                ),
            )
            session.commit()
            results["agent_availability_drop_100_to_60"] = {
                "requested": 100,
                "approved": drop_decision.approved_calls,
                "decision": drop_decision.decision.value,
                "available_observed": drop_decision.available_agents,
            }

        # D & E. Duplicate & Out-of-order events demo
        with session_factory() as session:
            c = Campaign(name="FailDemo-DE-Events")
            session.add(c)
            session.flush()
            ag = Agent(campaign_id=c.id, name="EventAgent", status=AgentState.AVAILABLE.value)
            b = Borrower(campaign_id=c.id, phone_number="+15550101002", name="EventBorrower")
            session.add_all([ag, b])
            session.flush()
            alloc = CallAllocator.allocate_call(session, c.id, "provider_a", agent_id=ag.id, borrower_id=b.id)
            session.commit()
            call_id = alloc.call.id

        with session_factory() as session:
            OutboxDispatcher.dispatch_pending(session)
            EventProcessor.process_event(session, "provider_a", f"dup-evt-{call_id}", call_id=call_id, event_type="ANSWERED")
            dup_res = EventProcessor.process_event(session, "provider_a", f"dup-evt-{call_id}", call_id=call_id, event_type="ANSWERED")
            EventProcessor.process_event(session, "provider_a", f"comp-evt-{call_id}", call_id=call_id, event_type="COMPLETED")
            ooo_res = EventProcessor.process_event(session, "provider_a", f"late-evt-{call_id}", call_id=call_id, event_type="RINGING")
            session.commit()
            c_final = session.get(Call, call_id)
            results["duplicate_and_out_of_order_events"] = {
                "duplicate_acknowledged_safely": dup_res.is_duplicate,
                "final_call_status": c_final.status,
                "late_ringing_blocked": ooo_res.is_out_of_order,
            }

        # F. Worker crash after ANSWERED + subsequent COMPLETED reconciliation
        with session_factory() as session:
            c = Campaign(name="FailDemo-F-CrashAfterAnswered")
            session.add(c)
            session.flush()
            ag = Agent(campaign_id=c.id, name="AnsCrashAgent", status=AgentState.AVAILABLE.value)
            b = Borrower(campaign_id=c.id, phone_number="+15550101003", name="AnsCrashBorrower")
            session.add_all([ag, b])
            session.flush()
            alloc = CallAllocator.allocate_call(session, c.id, "provider_a", agent_id=ag.id, borrower_id=b.id)
            session.commit()
            f_call_id, f_ag_id = alloc.call.id, ag.id

        with session_factory() as session:
            OutboxDispatcher.dispatch_pending(session)
            EventProcessor.process_event(session, "provider_a", f"ans-f-{f_call_id}", call_id=f_call_id, event_type="ANSWERED")
            session.commit()

        # Worker crashed right after ANSWERED committed; new worker handles COMPLETED
        with session_factory() as session:
            EventProcessor.process_event(
                session,
                "provider_a",
                f"comp-f-{f_call_id}",
                call_id=f_call_id,
                event_type="COMPLETED",
                raw_payload={"duration_seconds": 65.0},
            )
            session.commit()
            c_f = session.get(Call, f_call_id)
            ag_f = session.get(Agent, f_ag_id)
            results["worker_crash_after_answered"] = {
                "call_status": c_f.status,
                "agent_status": ag_f.status,
            }

        return results


def _print_cli_report() -> None:
    from smartdialer.db.migrations import run_migrations
    from smartdialer.db.session import SyncSessionFactory, sync_engine

    run_migrations(sync_engine)
    results = SimulationRunner.run_all_scenarios(SyncSessionFactory)
    print("\n=== SmartDialer Predictive Simulation Results (PostgreSQL) ===")
    header = (
        f"{'Scenario':<8} | {'Ans%':<5} | {'Talk':<5} | {'Init':<5} | {'Ans':<4} | {'Conn':<5} | "
        f"{'Comp':<5} | {'Fail':<5} | {'Aband%':<7} | {'Util%':<6} | {'SafeViol':<8}"
    )
    print(header)
    print("-" * len(header))
    for m in results:
        print(
            f"{m.scenario:<8} | {m.answer_rate_target*100:>4.0f}% | {m.avg_talk_time_sec:>4.0f}s | "
            f"{m.calls_initiated:<5} | {m.calls_answered:<4} | {m.calls_connected:<5} | "
            f"{m.calls_completed:<5} | {m.calls_failed:<5} | {m.abandonment_rate_pct:>6.2f}% | "
            f"{m.agent_utilization_pct:>5.2f}% | {m.safety_invariant_violations:<8}"
        )
    demos = SimulationRunner.run_failure_demos(SyncSessionFactory)
    print("\n=== Failure Mode Demonstrations (A-F) ===")
    for name, info in demos.items():
        print(f"- {name}: {info}")


if __name__ == "__main__":
    _print_cli_report()
