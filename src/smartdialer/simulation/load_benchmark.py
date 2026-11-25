"""
PostgreSQL Multi-Worker Scaling & Concurrency Benchmark (100, 1,000, and 10,000 Agents).

Measures real allocation throughput, mean/P50/P95/P99 latency, and verifies zero
double-reservations under concurrent worker contention directly against PostgreSQL 18.
"""

import concurrent.futures
from dataclasses import asdict, dataclass
import math
import time
import uuid
from typing import Any, Dict, List
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from smartdialer.allocator.allocator import CallAllocator
from smartdialer.models.agent import Agent
from smartdialer.models.borrower import Borrower
from smartdialer.models.call import Call
from smartdialer.models.campaign import Campaign
from smartdialer.models.enums import AgentState, CampaignMode
from smartdialer.safety.safety_controller import DialingRequest, SafetyController


@dataclass
class ScaleBenchmarkResult:
    tier_agents: int
    worker_threads: int
    batch_size: int
    total_allocations: int
    total_duration_sec: float
    throughput_allocs_per_sec: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    duplicate_agent_assignments: int
    duplicate_borrower_assignments: int
    errors: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _percentile(sorted_vals: List[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(math.ceil(pct * len(sorted_vals))) - 1))
    return sorted_vals[idx]


class LoadBenchmarkRunner:
    """Executes real PostgreSQL allocation concurrency benchmarks across pool scales."""

    @classmethod
    def run_tier(
        cls,
        session_factory: sessionmaker,
        num_agents: int,
        worker_threads: int = 8,
        batch_size: int = 25,
        max_batches: int | None = None,
    ) -> ScaleBenchmarkResult:
        """
        Seed `num_agents` AVAILABLE agents and borrowers in PostgreSQL, then unleash
        `worker_threads` concurrent workers racing through SafetyController + CallAllocator.
        """
        num_borrowers = int(num_agents * 1.2)
        with session_factory() as session:
            campaign = Campaign(
                name=f"ScaleBench-{num_agents}-{uuid.uuid4().hex[:6]}",
                mode=CampaignMode.PROGRESSIVE.value,
                max_active_calls=num_agents + 500,
            )
            session.add(campaign)
            session.flush()
            campaign_id = campaign.id

            # Bulk insert agents and borrowers cleanly
            agent_rows = [
                {"campaign_id": campaign_id, "name": f"BenchAg-{i}", "status": AgentState.AVAILABLE.value, "version": 1}
                for i in range(num_agents)
            ]
            borrower_rows = [
                {
                    "campaign_id": campaign_id,
                    "phone_number": f"+1666{i:07d}",
                    "name": f"BenchBorr-{i}",
                    "priority": i % 5,
                }
                for i in range(num_borrowers)
            ]
            session.bulk_insert_mappings(Agent, agent_rows)
            session.bulk_insert_mappings(Borrower, borrower_rows)
            session.commit()

        latencies_ms: List[float] = []
        total_allocs = 0
        errors = 0

        # Number of concurrent safety+allocation rounds to drain target capacity
        target_rounds = max_batches or max(1, math.ceil(num_agents / batch_size))

        def worker_batch() -> tuple[int, List[float], int]:
            local_lats: List[float] = []
            local_allocs = 0
            local_errs = 0
            t0 = time.perf_counter()
            try:
                with session_factory() as session:
                    decision = SafetyController.evaluate(
                        session,
                        DialingRequest(
                            campaign_id=campaign_id,
                            mode=CampaignMode.PROGRESSIVE,
                            desired_calls=batch_size,
                            estimated_answer_rate=1.0,
                            provider_name="provider_a",
                            pacing_reasoning=f"Scale benchmark batch ({batch_size})",
                        ),
                    )
                    for aid in decision.claimed_agent_ids:
                        CallAllocator.allocate_call(
                            session=session,
                            campaign_id=campaign_id,
                            provider_name="provider_a",
                            agent_id=aid,
                        )
                        local_allocs += 1
                    session.commit()
                    elapsed = (time.perf_counter() - t0) * 1000.0
                    if local_allocs > 0:
                        local_lats.append(elapsed)
            except Exception:
                local_errs += 1
            return local_allocs, local_lats, local_errs

        bench_start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_threads) as executor:
            futures = [executor.submit(worker_batch) for _ in range(target_rounds)]
            for fut in concurrent.futures.as_completed(futures):
                allocs, lats, errs = fut.result()
                total_allocs += allocs
                latencies_ms.extend(lats)
                errors += errs
        total_duration = max(0.001, time.perf_counter() - bench_start)

        # Audit PostgreSQL strictly for any duplicate agent or borrower assignments
        with session_factory() as session:
            dup_agents = session.execute(
                select(Call.agent_id, func.count(Call.id))
                .where(Call.campaign_id == campaign_id, Call.agent_id.is_not(None))
                .group_by(Call.agent_id)
                .having(func.count(Call.id) > 1)
            ).all()
            dup_borrowers = session.execute(
                select(Call.borrower_id, func.count(Call.id))
                .where(Call.campaign_id == campaign_id)
                .group_by(Call.borrower_id)
                .having(func.count(Call.id) > 1)
            ).all()

        sorted_lats = sorted(latencies_ms)
        avg_lat = sum(sorted_lats) / len(sorted_lats) if sorted_lats else 0.0

        return ScaleBenchmarkResult(
            tier_agents=num_agents,
            worker_threads=worker_threads,
            batch_size=batch_size,
            total_allocations=total_allocs,
            total_duration_sec=round(total_duration, 3),
            throughput_allocs_per_sec=round(total_allocs / total_duration, 2),
            avg_latency_ms=round(avg_lat, 2),
            p50_latency_ms=round(_percentile(sorted_lats, 0.50), 2),
            p95_latency_ms=round(_percentile(sorted_lats, 0.95), 2),
            p99_latency_ms=round(_percentile(sorted_lats, 0.99), 2),
            duplicate_agent_assignments=len(dup_agents),
            duplicate_borrower_assignments=len(dup_borrowers),
            errors=errors,
        )

    @classmethod
    def run_all_tiers(cls, session_factory: sessionmaker) -> List[ScaleBenchmarkResult]:
        """Run 100, 1,000, and 10,000 agent pool concurrency benchmarks."""
        return [
            cls.run_tier(session_factory, num_agents=100, worker_threads=8, batch_size=10),
            cls.run_tier(session_factory, num_agents=1000, worker_threads=8, batch_size=25),
            cls.run_tier(session_factory, num_agents=10000, worker_threads=8, batch_size=50, max_batches=40),
        ]


def _print_benchmark_report() -> None:
    from smartdialer.db.migrations import run_migrations
    from smartdialer.db.session import SyncSessionFactory, sync_engine

    run_migrations(sync_engine)
    results = LoadBenchmarkRunner.run_all_tiers(SyncSessionFactory)
    print("\n=== SmartDialer PostgreSQL Scaling Benchmark (100 / 1,000 / 10,000 Agents) ===")
    header = (
        f"{'Pool':<7} | {'Workers':<7} | {'Allocs':<7} | {'Time(s)':<8} | {'Throughput':<14} | "
        f"{'Avg(ms)':<8} | {'P95(ms)':<8} | {'DupAg':<5} | {'DupBorr':<7} | {'Err':<3}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.tier_agents:<7} | {r.worker_threads:<7} | {r.total_allocations:<7} | "
            f"{r.total_duration_sec:<8.3f} | {r.throughput_allocs_per_sec:>10.2f}/s   | "
            f"{r.avg_latency_ms:<8.2f} | {r.p95_latency_ms:<8.2f} | "
            f"{r.duplicate_agent_assignments:<5} | {r.duplicate_borrower_assignments:<7} | {r.errors:<3}"
        )


if __name__ == "__main__":
    _print_benchmark_report()
