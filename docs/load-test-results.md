# SmartDialer — Stage 9 Simulations & Load Test Results

All Stage 9 simulations, failure mode demonstrations, multi-worker concurrency scaling benchmarks, and Locust HTTP load tests were executed directly against **PostgreSQL 18** (`127.0.0.1:5432`) with real ACID transactions, row-level locking (`SELECT ... FOR UPDATE SKIP LOCKED`), transactional outbox dispatching, and no mocked database or concurrency boundaries.

---

## 1. Predictive Simulation Scenarios (A–D)

Command executed: `python -m smartdialer.simulation.runner` (`SimulationRunner.run_all_scenarios`, 20 agents pre-seeded 10 `AVAILABLE` / 10 `CONNECTED`, 80 borrowers, 4 cycles per scenario).

| Scenario | Description | Target Ans% | Avg Talk | Initiated | Answered | Connected | Completed | Abandoned | Failed | Aband% | Util% | Provider Healthy | Safety Violations |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **A** | Low answer rate regime | 20% | 120s | 38 | 8 | 8 | 5 | 0 | 30 | 0.00% | 27.50% | True | **0** |
| **B** | Balanced regime | 50% | 90s | 32 | 15 | 13 | 8 | 2 | 17 | 13.33% | 40.00% | True | **0** |
| **C** | High answer rate / long talk | 70% | 180s | 37 | 26 | 26 | 20 | 0 | 11 | 0.00% | 42.50% | True | **0** |
| **D** | Changing regime + failures | 70% → 10% | 90s → 150s | 4 | 3 | 3 | 2 | 0 | 1 | 0.00% | 31.25% | False | **0** |

### Per-Cycle Pacing & Safety Controller Decisions Observed

- **Scenario A (20% ans, 120s talk):**
  - Cycle 1: `desired=50` → Safety Controller `REDUCED` to `15` (capped by `13` available agents + Little's Law projected headroom).
  - Cycle 2: `desired=19` → `REDUCED` to `17`.
  - Cycle 3: `desired=6` → `APPROVED` `6`.
  - Cycle 4: `desired=0` → `REJECTED` `0` (agent pool saturated with 0 headroom).
- **Scenario B (50% ans, 90s talk):**
  - Cycle 1: `desired=21` → `REDUCED` to `15`.
  - Cycles 2–4: `APPROVED` `9`, `4`, `4`. Two predictive calls without pre-assigned agents that answered when no agent freed within `answer_timeout_sec=0.02s` were deterministically terminated (`ABANDONED`, 0 orphaned calls).
- **Scenario C (70% ans, 180s talk):**
  - Cycles 1–4: `APPROVED` `15`, `10`, `5`, `7` with `0.00%` abandonment and `42.50%` utilization.
- **Scenario D (70% → 10% answer shock, 15% injected provider failures, 20% agent drop mid-run):**
  - Cycle 1: `desired=16` → `REDUCED` to `15`.
  - Provider failures recorded during dispatch tripped `provider_health_tracker` (`provider_healthy=False`).
  - Cycles 2–4: Safety Controller automatically switched every proposal to **`FALLBACK_TO_PROGRESSIVE`** (`approved`: `9`, `10`, `8`), and `OutboxDispatcher` blocked unsafe dispatches (`Calls Initiated = 4`) once carrier failure threshold tripped, maintaining **0 safety invariant violations**.

---

## 2. Failure Mode Demonstrations (A–F)

Command executed: `SimulationRunner.run_failure_demos(SyncSessionFactory)` against PostgreSQL 18.

| Case | Scenario Tested | Observed State & Verification |
| :--- | :--- | :--- |
| **A** | Worker crash right after `RESERVED` (stale 45s) | `call_status='FAILED'`, `agent_status='AVAILABLE'`, `borrower_status='ELIGIBLE'` (`ReconciliationSweeper` reclaimed stale reservations atomically; `reclaimed_agents=66` across workspace stale rows) |
| **B** | Provider outage & manual recovery | During outage: `decision='FALLBACK_TO_PROGRESSIVE'`, `approved=1`; after recovery: `decision='APPROVED'`, `approved=1` |
| **C** | Sudden agent drop (`100` → `60` available while stale pacer asks for `100`) | Safety Controller observed `60` available agents and issued `decision='REDUCED'`, `approved=60` |
| **D** | Duplicate `ANSWERED` carrier webhook | `duplicate_acknowledged_safely=True` via PostgreSQL unique constraint `uq_provider_name_event_id` |
| **E** | Out-of-order late `RINGING` after `COMPLETED` | `final_call_status='COMPLETED'`, `late_ringing_blocked=True` (monotonic state rank enforcement) |
| **F** | Worker crash after `ANSWERED` + new worker handles `COMPLETED` | `call_status='COMPLETED'`, `agent_status='WRAP_UP'` cleanly reconciled across worker boundary |

---

## 3. PostgreSQL Multi-Worker Scaling Benchmark (100 / 1,000 / 10,000 Agents)

Command executed: `python -m smartdialer.simulation.load_benchmark` (`LoadBenchmarkRunner.run_all_tiers`, 8 concurrent worker threads competing through `SafetyController.evaluate` + `CallAllocator.allocate_call` on PostgreSQL 18).

| Agent Pool | Workers | Batch Size | Allocations Completed | Duration (s) | Throughput (allocs/s) | Avg Batch Latency (ms) | P95 Batch Latency (ms) | Dup Agents | Dup Borrowers | Errors |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **100** | 8 | 10 | 100 | 0.367 | **272.25/s** | 251.76 ms | 335.58 ms | **0** | **0** | **0** |
| **1,000** | 8 | 25 | 1,000 | 3.141 | **318.37/s** | 617.15 ms | 667.95 ms | **0** | **0** | **0** |
| **10,000** | 8 | 50 (40 batches) | 2,000 | 6.213 | **321.92/s** | 1218.17 ms | 1302.43 ms | **0** | **0** | **0** |

### Observed Bottlenecks & Scaling Analysis

1. **Zero Double-Reservations at All Scales:** Even with 8 parallel workers contending on a single campaign pool, PostgreSQL row-level `FOR UPDATE SKIP LOCKED` produced **0 duplicate agent assignments** and **0 duplicate borrower assignments** across all 3,100 test allocations.
2. **Throughput Plateau (~320 allocs/s per single campaign):** Single-campaign sequential `CallAllocator.allocate_call` ORM flushes (`Call`, `Agent` state check, `Borrower` update, `OutboxEvent` insert) saturate around ~318–322 allocs/s on 8 threads as batch size grows (`667.95 ms` P95 at batch 25; `1302.43 ms` P95 at batch 50).
3. **Horizontal Scaling Strategy:** Sharding active campaigns across independent DB partition keys/workers scales aggregate throughput linearly without single-campaign lock contention; switching `CallAllocator` hot paths to bulk `INSERT ... RETURNING` / `executemany` eliminates per-call ORM round-trips inside a batch.

---

## 4. Locust Live HTTP + Worker Daemon Load Test

Server: `python -m uvicorn smartdialer.main:app --app-dir src --host 127.0.0.1 --port 8000` (FastAPI + live `SmartDialerWorker` daemon polling every 0.25s + PostgreSQL 18).
Command executed: `python -m locust -f locustfile.py --host http://127.0.0.1:8000 --headless -u 10 -r 5 -t 10s --only-summary` (10 concurrent virtual worker users driving `/api/v1/campaigns`, `/agents`, `/borrowers`, `/dial`, `/webhooks/provider/provider_a` with 20% injected duplicate webhooks, agent transitions, and `/safety-logs`).

| Endpoint Group | Requests | Failures | Avg Latency (ms) | Median (ms) | P95 Latency (ms) | Max Latency (ms) | Throughput (req/s) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `POST /api/v1/campaigns` | 10 | 0 (0.00%) | 496 ms | 890 ms | 1,000 ms | 1,008 ms | 1.06 req/s |
| `POST /api/v1/webhooks/provider/provider_a` | 523 | 0 (0.00%) | 76 ms | 36 ms | 530 ms | 1,584 ms | 55.31 req/s |
| **Aggregated (All Endpoints)** | **676** | **0 (0.00%)** | **106 ms** | **35 ms** | **550 ms** | **1,611 ms** | **71.49 req/s** |

- **0 failures out of 676 HTTP requests (0.00% error rate)**, including deliberate 20% duplicate webhook delivery and concurrent campaign creation, dialing, state transitions, and telemetry reads.
