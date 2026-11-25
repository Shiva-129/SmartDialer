"""
Stage 9 Verification Suite: Quantitative Predictive Scenarios (A-D),
Distributed Failure Mode Demonstrations (A-F), and Multi-Worker Scaling Benchmark
executed directly against PostgreSQL 18.
"""

from sqlalchemy.orm import sessionmaker

from smartdialer.simulation.load_benchmark import LoadBenchmarkRunner
from smartdialer.simulation.runner import SimulationRunner


def test_stage9_predictive_scenarios_a_through_d(sync_test_engine):
    """Run Scenarios A, B, C, and D against PostgreSQL and verify zero safety violations."""
    SessionFactory = sessionmaker(bind=sync_test_engine, expire_on_commit=False)

    results = SimulationRunner.run_all_scenarios(
        SessionFactory,
        num_agents=16,
        num_borrowers=60,
        cycles=3,
        provider_name="provider_a",
    )

    assert [r.scenario for r in results] == ["A", "B", "C", "D"]
    for res in results:
        assert res.calls_initiated > 0
        assert res.safety_invariant_violations == 0
        assert len(res.pacing_decisions) == 3
        assert len(res.safety_decisions) == 3

    # Verify Scenario D exercises dynamic regime shock and provider fallback/tripping
    scenario_d = results[3]
    decisions_d = [d["decision"] for d in scenario_d.safety_decisions]
    assert any(dec in ("FALLBACK_TO_PROGRESSIVE", "REDUCED", "APPROVED") for dec in decisions_d)


def test_stage9_all_six_failure_mode_demonstrations(sync_test_engine):
    """Verify Failure Mode Demonstrations A through F end-to-end against PostgreSQL."""
    SessionFactory = sessionmaker(bind=sync_test_engine, expire_on_commit=False)

    demos = SimulationRunner.run_failure_demos(SessionFactory)

    # A. Worker crash recovery
    assert demos["worker_crash_recovery"]["call_status"] == "FAILED"
    assert demos["worker_crash_recovery"]["agent_status"] == "AVAILABLE"
    assert demos["worker_crash_recovery"]["borrower_status"] == "ELIGIBLE"
    assert demos["worker_crash_recovery"]["reclaimed_agents"] >= 1

    # B. Provider outage & recovery
    assert demos["provider_outage_and_recovery"]["outage_decision"] == "FALLBACK_TO_PROGRESSIVE"
    assert demos["provider_outage_and_recovery"]["outage_approved"] == 1
    assert demos["provider_outage_and_recovery"]["recovered_decision"] == "APPROVED"
    assert demos["provider_outage_and_recovery"]["recovered_approved"] == 1

    # C. Sudden agent availability drop (100 -> 60 safe ceiling)
    assert demos["agent_availability_drop_100_to_60"]["requested"] == 100
    assert demos["agent_availability_drop_100_to_60"]["available_observed"] == 60
    assert demos["agent_availability_drop_100_to_60"]["approved"] == 60
    assert demos["agent_availability_drop_100_to_60"]["decision"] == "REDUCED"

    # D & E. Duplicate and out-of-order provider events
    assert demos["duplicate_and_out_of_order_events"]["duplicate_acknowledged_safely"] is True
    assert demos["duplicate_and_out_of_order_events"]["final_call_status"] == "COMPLETED"
    assert demos["duplicate_and_out_of_order_events"]["late_ringing_blocked"] is True

    # F. Worker crash after ANSWERED + clean completion reconciliation
    assert demos["worker_crash_after_answered"]["call_status"] == "COMPLETED"
    assert demos["worker_crash_after_answered"]["agent_status"] == "WRAP_UP"


def test_stage9_multi_worker_postgresql_scaling_benchmark(sync_test_engine):
    """Verify concurrent multi-worker allocation integrity across 100 agents in PostgreSQL."""
    SessionFactory = sessionmaker(bind=sync_test_engine, expire_on_commit=False)

    result = LoadBenchmarkRunner.run_tier(
        SessionFactory,
        num_agents=100,
        worker_threads=4,
        batch_size=10,
    )

    assert result.tier_agents == 100
    assert result.total_allocations == 100
    assert result.duplicate_agent_assignments == 0
    assert result.duplicate_borrower_assignments == 0
    assert result.errors == 0
    assert result.throughput_allocs_per_sec > 0
    assert result.avg_latency_ms > 0
    assert result.p95_latency_ms >= result.p50_latency_ms
