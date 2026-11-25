# SmartDialer — Complete Project & Architecture Overview

This document explains **what SmartDialer is, how every component works step by step, what every API and state transition does, and how the current dashboard UI maps to the backend**—so you can review the full system and specify the exact UI/UX changes you want.

---

## 1. What Problem SmartDialer Solves (Plain English)

In outbound debt-collection call centers, you have:
- **Agents** (human callers who answer and talk to borrowers)
- **Borrowers** (people waiting to be called)
- **Telecom Carriers** (`provider_a` and `provider_b`) that place outbound phone calls and send back webhook status events (`RINGING`, `ANSWERED`, `CONNECTED`, `COMPLETED`, `FAILED`)

Naive auto-dialers break in two dangerous ways:
1. **Over-dialing (Dropped / Abandoned Calls):** If a dialer places too many calls and borrowers answer when no agent is free, customers hang up or hear dead air—violating regulatory abandonment limits (target: under **3%**).
2. **Distributed Failures:** If a worker crashes mid-call, an agent suddenly goes offline during carrier network I/O, or a carrier sends the same webhook twice, naive systems **double-dial borrowers**, **leak agent reservations forever**, or **orphan ringing calls**.

### Non-Negotiable Core Invariant
> **The system must never create more agent-bound active calls than its safely reserved agent capacity.**

Neither dialer (`ProgressiveDialer` nor `PredictiveDialer`) is allowed to call a telecom provider directly. Every dialing cycle must pass through the **`SafetyController`**, commit an atomic PostgreSQL allocation + outbox record, and let **`OutboxDispatcher`** dial asynchronously with fencing tokens and an idempotency ledger.

---

## 2. How a Single Dial Cycle Works (Step-by-Step)

When you run a dial cycle on an `ACTIVE` campaign:

1. **Calculate How Many Calls to Place (Pacing)**
   - **Progressive Mode (`PROGRESSIVE`):** Strict **1:1**. Requests at most 1 outbound call per currently `AVAILABLE` agent.
   - **Predictive Mode (`PREDICTIVE`):** `PredictiveDialer` (`src/smartdialer/dialer/predictive.py`) checks campaign telemetry (`answer_rate_ema`, average talk time) and uses **Little's Law** + binomial variance bounds to estimate how many currently `CONNECTED`/`WRAP_UP` agents will finish within the ~18s call-setup window, discounting by active `RINGING` backlog.
2. **Mandatory Safety Gate (`SafetyController.evaluate`)**
   - File: `src/smartdialer/safety/safety_controller.py`
   - Locks candidate `AVAILABLE` agents in PostgreSQL with `SELECT ... FOR UPDATE SKIP LOCKED` (so two workers can never claim the same agent).
   - Checks live DB active/ringing counts, campaign `max_active_calls`, and distributed `provider_health_tracker` metrics.
   - Outputs one structured decision logged permanently to `safety_decision_logs`:
     - `APPROVED` — full requested count approved
     - `REDUCED` — capped to safe available/headroom capacity
     - `REJECTED` — 0 calls allowed (e.g., 0 headroom or unhealthy progressive provider)
     - `FALLBACK_TO_PROGRESSIVE` — carrier health degraded in Predictive mode, so predictive over-dialing is suspended and capped to strict 1:1 available agents
3. **Atomic DB Allocation (`CallAllocator.allocate_call`)**
   - File: `src/smartdialer/allocator/allocator.py`
   - Within one ACID PostgreSQL transaction:
     - Progressive / pre-assigned calls pair `1 RESERVED Agent + 1 RESERVED Borrower`.
     - Pure predictive extra calls reserve `1 Borrower` with `dialing_mode="PREDICTIVE"` and `agent_id=None`.
     - Inserts a `Call` (`RESERVED`), deterministic `idempotency_key` (`call-<campaign_id>-<borrower_id>-att<attempts>`), and an `OutboxEvent` (`PENDING`).
     - Any leftover claimed agents if borrowers run out are released back to `AVAILABLE` in cleanup.
4. **Safe Carrier Dispatch (`OutboxDispatcher.dispatch_pending`)**
   - File: `src/smartdialer/recovery/outbox_dispatcher.py`
   - Runs per-record in isolated transactions (never holds DB locks across slow HTTP requests) with a lease heartbeat (`LeaseHeartbeat`) and unique `dispatch_token` fencing tokens.
   - Re-verifies right before dialing that the campaign is active, the reservation hasn't expired (`30s` timeout), the agent hasn't gone `OFFLINE`/`PAUSED`, and carrier health is good.
   - Calls `provider_a` or `provider_b`, which first commits a `PENDING` row to `provider_request_ledger` (`UNIQUE(provider_name, idempotency_key)`), executes the dial, and autonomously commits `DISPATCHED` (or `UNCERTAIN` on timeout so retries never blind-dial twice).
   - If an agent disconnected during carrier I/O, `TerminationService` tears down the carrier call immediately so no borrower is left ringing without an agent.
5. **Incoming Carrier Webhooks (`EventProcessor.process_event`)**
   - File: `src/smartdialer/events/processor.py`
   - Resolves the call by `call_id` or `provider_call_id`.
   - Enforces DB-level webhook deduplication via `uq_provider_name_event_id`.
   - Enforces **monotonic call state rank** (`0..6`) so late out-of-order events (e.g., `RINGING` arriving after `COMPLETED`) are safely ignored and logged rather than regressing state.
   - **Predictive Answer Matching (`BridgeAllocator`):** When a predictive call (`agent_id=None`) sends `ANSWERED`, `BridgeAllocator` atomically waits/claims a freeing `AVAILABLE` agent and bridges them (`CONNECTED`). If no agent frees before `answer_timeout_sec`, it transitions the call to `FAILED` (`termination_reason="ABANDONED"`) and triggers carrier teardown so compliance stays intact.
6. **Continuous Self-Healing (`SmartDialerWorker` + `ReconciliationSweeper`)**
   - Files: `src/smartdialer/services/worker.py`, `src/smartdialer/recovery/reconciler.py`
   - FastAPI launches `SmartDialerWorker` automatically in its lifespan:
     - Outbox polling loop every **`0.25s`**
     - Reconciliation sweeper loop every **`5.0s`** (reclaims stale `RESERVED` calls past 30s, stuck `WRAP_UP` agents past 60s, and orphaned `RINGING` calls past 60s, while respecting active outbox leases).

---

## 3. Core Lifecycle State Machines (`src/smartdialer/models/enums.py`)

### Agent States (`AgentState`)
- `OFFLINE` <-> `AVAILABLE` -> `RESERVED` -> `DIALING` -> `CONNECTED` -> `WRAP_UP` -> `AVAILABLE`
- Agents can also move to `PAUSED` or `OFFLINE`; transitions are guarded by `AgentStateMachine`.

### Call States (`CallState` + Monotonic Rank)
| State | Rank | Meaning |
| :--- | :---: | :--- |
| `QUEUED` | 0 | Initial pre-allocation state |
| `RESERVED` | 1 | Agent/borrower reserved in DB, waiting on Outbox dispatch |
| `INITIATED` | 2 | Carrier accepted dispatch request |
| `RINGING` | 3 | Borrower phone is ringing |
| `ANSWERED` | 4 | Borrower picked up (pending bridge or already paired) |
| `ABANDONMENT_PENDING` / `CONNECTED` | 5 | Agent bridged & talking, or awaiting compliant teardown |
| `COMPLETED` / `FAILED` / `CANCELLED` | 6 | **Terminal states** (immutable; cannot regress) |

### Safety Controller Decisions (`SafetyDecisionType`)
- `APPROVED` | `REDUCED` | `REJECTED` | `FALLBACK_TO_PROGRESSIVE`

---

## 4. Codebase Directory Map (`src/smartdialer/`)

| Folder / File | Responsibility |
| :--- | :--- |
| `main.py` | FastAPI app entrypoint, serves `index.html` at `/`, WebSocket at `/ws/campaigns/{campaign_id}`, starts/stops `SmartDialerWorker` |
| `api/routes.py`, `schemas.py`, `deps.py`, `ws.py` | REST endpoints (`/api/v1/...`), Pydantic validation schemas, DB session dependency, WebSocket room broadcaster |
| `static/index.html` | Current single-file Operations Console web UI |
| `safety/safety_controller.py`, `provider_health.py` | Mandatory Safety Gate, Little's Law headroom math, DB-persisted distributed carrier health & circuit breaker |
| `dialer/progressive.py`, `predictive.py` | Progressive 1:1 dialer and Predictive statistical dialer |
| `pacing/engine.py`, `metrics.py` | EMA answer rate + talk duration tracker and Little's Law call pacing calculator |
| `allocator/allocator.py`, `bridge.py` | Atomic PostgreSQL agent/borrower reservation and predictive answer-to-agent bridging |
| `providers/base.py`, `provider_a.py`, `provider_b.py`, `registry.py` | Telecom provider interface, Provider A/B simulators (with latency/failure/reordering knobs), autonomous `provider_request_ledger` |
| `events/processor.py` | Carrier webhook ingestion, DB deduplication, monotonic state updates, telemetry updates |
| `recovery/outbox_dispatcher.py`, `reconciler.py` | Per-record outbox dispatcher with leases/fencing tokens + stale reservation/wrap-up/ringing sweeper |
| `services/worker.py`, `termination_service.py` | Background async polling daemon + durable carrier teardown confirmation |
| `models/*.py` | SQLAlchemy models: `Campaign`, `Agent`, `Borrower`, `Call`, `OutboxEvent`, `ProviderEvent`, `ProviderRequestLedger`, `ProviderHealthRecord`, `SafetyDecisionLog` |
| `db/base.py`, `session.py`, `migrations.py` | SQLAlchemy base/engines for PostgreSQL 18 + idempotent schema migrations |
| `simulation/runner.py`, `load_benchmark.py` | Predictive Scenarios A–D runner, Failure Demos A–F, and multi-worker `100 / 1,000 / 10,000` agent scaling benchmark |

---

## 5. Backend Endpoints Connected to the Dashboard

| Method & Path | Purpose |
| :--- | :--- |
| `GET /` | Serves the 4-tab Light SaaS Operations Console (`static/index.html`) |
| `GET /health` | DB connection check, `provider_a` / `provider_b` / `twilio` health + `configured` flag, WS connection count |
| `POST /api/v1/campaigns` | Create a new `PREDICTIVE` or `PROGRESSIVE` campaign |
| `GET /api/v1/campaigns` | List campaigns |
| `GET /api/v1/campaigns/{id}` | Live metrics: agent/call/borrower counts, EMA answer rate, avg talk time, utilization %, abandonment % |
| `PATCH /api/v1/campaigns/{id}` | Update campaign mode (`PROGRESSIVE` <-> `PREDICTIVE`), status, or max active calls |
| `POST /api/v1/campaigns/{id}/agents` | Batch create agents (e.g., 10 `AVAILABLE` agents) |
| `GET /api/v1/campaigns/{id}/agents` | Enriched agent roster (`status`, `current_borrower_name`, `calls_handled`, `completed_calls`, `avg_talk_duration_sec`, `last_completed_call_id`) |
| `POST /api/v1/agents/{id}/transition` | Manually transition an agent state (`AVAILABLE`, `PAUSED`, `OFFLINE`, etc.) |
| `POST /api/v1/campaigns/{id}/borrowers` | Batch create borrowers with phone numbers & priority |
| `POST /api/v1/campaigns/{id}/dial` | Run one dial cycle (`PredictiveDialer` or `ProgressiveDialer`) across `provider_a`, `provider_b`, or `twilio` |
| `GET /api/v1/campaigns/{id}/calls` | List recent calls enriched with `agent_name`, `borrower_name`, `borrower_phone`, `disposition`, and `call_notes` |
| `POST /api/v1/calls/{id}/notes` | Save agent wrap-up disposition + call notes, optionally returning a `WRAP_UP` agent to `AVAILABLE` |
| `GET /api/v1/campaigns/{id}/safety-logs` | List recent `SafetyController` audit decisions & reasoning |
| `POST /api/v1/webhooks/provider/{name}` | Ingest JSON carrier webhook (`RINGING`, `ANSWERED`, `CONNECTED`, `COMPLETED`, `FAILED`) |
| `POST /api/v1/webhooks/twilio/status` | Ingest Twilio form-urlencoded `CallStatus` webhooks cleanly through `EventProcessor` |
| `POST /api/v1/providers/{name}/health` | Manually trip or clear carrier outage (`{"degraded": true/false}`) |
| `POST /api/v1/simulations/run` | Execute Predictive Scenarios A–D + Failure Mode Demos A–F |
| `POST /api/v1/benchmarks/run` | Execute live 100 & 1,000-agent multi-worker PostgreSQL allocation benchmarks |
| `WS /ws/campaigns/{id}` | Real-time push updates (`dial_cycle`, `webhook_event`, `agent_state`, `agents_added`, `campaign_updated`, `call_notes_saved`) |

---

## 6. Light SaaS Operations Console (`src/smartdialer/static/index.html`)

The rebuilt dashboard is organized into 4 focused tabs backed by PostgreSQL & WebSockets:
1. **Live Operations:** Guided onboarding (`+ Add Agents`, `+ Add Borrowers`, `Demo Data`), `Auto-Dial` / `Run 1 Dial Wave`, carrier selector (`Provider A`, `Provider B`, `Twilio`), 4-step **Safety Controller Pipeline** narrative, KPI hero cards, live `Borrower ↔ Agent` call steppers, Quick Agent Status strip, and a plain-English activity feed.
2. **Agent Monitor & History:** Searchable per-agent live roster showing active borrower, completed call counts, average talk duration, and supervisor controls (`Mark Free Now`, `Add Notes`, `Pause`, `Offline`), paired with a **Who Talked to Whom** audit log displaying saved dispositions and Wrap-Up Call Notes.
3. **Agent Softphone:** Interactive agent headset screen to dial waiting or custom phone numbers, simulate pickup, track live call timer, run step-by-step customer dialogue (with optional Web Speech TTS), hang up, and save Wrap-Up Notes straight back to `AVAILABLE`.
4. **Stress Tests & Recovery:** Runs Scenarios A–D, renders visual `Before → Guardrail → After` fault-tolerance verification cards, and runs live PostgreSQL concurrency benchmarks.

---

## 7. Verification Baseline (PostgreSQL 18)

- **Pytest Suite:** `python -m pytest tests/ -v` → **90 passed** across unit, concurrency, crash recovery, Stage 8 API/WebSocket, Stage 9 simulation/scaling, and Twilio adapter tests.
- **Multi-Worker PostgreSQL Benchmark (`load_benchmark.py`):** Verified `100` (`272.25/s`), `1,000` (`318.37/s`), and `10,000` agent pool (`321.92/s`) runs with **0 duplicate agents**, **0 duplicate borrowers**, and **0 errors**.
- **Locust Live Load Test (`locustfile.py`):** `676` HTTP requests across 10 concurrent users against Uvicorn + `SmartDialerWorker` with **0 failures (0.00%)**, `35 ms` median latency, `71.49 req/s`.
- See `docs/load-test-results.md` for full measured Stage 9 tables.
