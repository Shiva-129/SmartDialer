# SmartDialer — Fault-Tolerant Automated Outbound Dialing System

A production-grade, fault-tolerant Python SmartDialer prototype built for debt collections and high-assurance telephony environments.

Engineered with strict mathematical safety bounds, atomic PostgreSQL concurrency control, transactional outbox pattern, state machine validation, distributed carrier health monitoring, and failure recovery.

---

## 1. Core Architectural Invariant

> **THE SYSTEM MUST NEVER CREATE MORE AGENT-BOUND ACTIVE CALLS THAN ITS SAFELY RESERVED AGENT CAPACITY.**
>
> Neither the Progressive Dialer nor the Predictive Pacing Engine may ever directly invoke telecom providers. The independent, non-bypassable **Safety Controller** is the sole authority governing call dispatches.

---

## 2. System Architecture

```
Campaign Manager
    |
    |---- Progressive Dialer (1:1 Ratio)
    |
    |---- Predictive Pacing Engine (Little's Law + EMA)
                 |
                 v
          Safety Controller [MANDATORY BOUNDARY]
                 |
                 v
           Call Allocator (Atomic Agent & Borrower Reservation)
                 |
                 v
         Transactional Outbox
                 |
                 v
         Outbox Dispatcher (Pre-Dispatch Validation & Independent TX)
                 |
                 v
          Provider Interface (Persistent Idempotency Ledger)
              /       \
       Provider A    Provider B
                 |
                 v
         Event Processor (DB-Enforced Deduplication & Monotonic Ranking)
                 |
                 v
       State Reconciliation Sweeper (30s Stale Reservations, 60s Wrap-Up)
                 |
                 v
         PostgreSQL Database (ACID, Row-Level Locking, SKIP LOCKED)
```

---

## 3. Key Components & Implementation

### 3.1 Independent Safety Controller (`src/smartdialer/safety/safety_controller.py`)
- Independent mandatory gateway evaluating all dialing requests before dispatch.
- Performs atomic capacity reservation directly in PostgreSQL using `SELECT id FROM agents WHERE status = 'AVAILABLE' LIMIT :desired FOR UPDATE SKIP LOCKED` to completely eliminate Time-of-Check to Time-of-Use (TOCTOU) races.
- Verifies live database counts of ringing calls, active calls, campaign ceilings, and carrier health.
- Emits structured decisions: `APPROVED`, `REDUCED`, `REJECTED`, or `FALLBACK_TO_PROGRESSIVE`.
- All evaluation decisions are immutably audited in `safety_decision_logs`.

### 3.2 Predictive Pacing Engine (`src/smartdialer/pacing/engine.py`)
- Statistical pacing engine without speculative black-box machine learning.
- Blends historical answer rates with Exponential Moving Average (EMA, $\alpha = 0.20$).
- Uses Little's Law to project agent departures during the average 18-second ringing setup window:
  $$C_{\text{eff}} = A_{\text{avail}} + \min\left(0.40 \cdot A_{\text{conn}}, A_{\text{conn}} \cdot \mu_{\text{depart}} \cdot T_{\text{ring}}\right) + A_{\text{wrap\_up}}$$
- Computes desired calls discounted by binomial variance bounds ($z = 1.645$) to guarantee compliance with the regulatory 3% abandonment ceiling.

### 3.3 Atomic Allocation & Bridge Allocator (`src/smartdialer/allocator/`)
- In Progressive mode: reserves an agent and borrower atomically in a single PostgreSQL transaction using `SELECT ... FOR UPDATE SKIP LOCKED`.
- In Predictive mode: decouples call placement from upfront agent reservation. Places calls with `dialing_mode = 'PREDICTIVE'` and `agent_id = None`.
- **Bridge Allocator (`src/smartdialer/allocator/bridge.py`):** When carrier sends `ANSWERED` or `CONNECTED`, atomically matches answering borrower to the next freeing available agent. Enforces configurable answer timeout (e.g. 2.0s); if no agent is free, compliantly terminates the call (`FAILED`, reason `ABANDONED`) to strictly prevent regulatory abandonment breaches.
- Formulates deterministic idempotency keys: `f"call-{campaign_id}-{borrower_id}-att{borrower.call_attempts}"`.
- Clean partial allocation rollback: wrapped in `try...finally` to immediately release any unallocated claimed agents back to `AVAILABLE`.

### 3.4 Transactional Outbox, Fencing Tokens & Orphan Prevention (`src/smartdialer/recovery/outbox_dispatcher.py`)
- Decouples local database transactions from slow external telecom HTTP requests.
- **Pre-Dispatch Validation:** Prior to dialing, locks Call, Agent, and Campaign rows. Verifies that the agent is still in `RESERVED` state and the reservation has not expired.
- **Dispatch Fencing Token:** Generates a unique `dispatch_token: UUID` on `Call` and `Agent`. If an agent disconnects (`OFFLINE`) or pauses (`PAUSED`) during external carrier I/O, dispatcher immediately terminates the carrier call via `provider.terminate_call(provider_call_id)` and marks the call `CANCELLED`, never leaving an orphaned ringing call.
- **Stale Worker Protection:** Fencing token mismatches reject stale workers whose leases expired, terminating rogue carrier calls.

### 3.5 Telecom Abstraction & Autonomous Idempotency (`src/smartdialer/providers/`)
- Abstract interface `TelecomProvider` isolating carrier specifics.
- Backed by `provider_request_ledger` in PostgreSQL with `UNIQUE(provider_name, idempotency_key)`.
- **Autonomous Transactions:** Dispatch intent (`PENDING`) is committed before carrier I/O, and carrier outcome (`DISPATCHED`) is committed autonomously immediately after response. Worker crashes or caller session rollbacks cannot destroy the ledger row.
- Retries return the identical `provider_call_id` and never trigger duplicate outbound calls. Uncertain outcomes (e.g. 504 timeouts) record `UNCERTAIN` and refuse blind retries.

### 3.6 Event Processor & Deduplication (`src/smartdialer/events/processor.py`)
- Ingests raw telephony webhooks.
- Enforces database-level event deduplication via `uq_provider_name_event_id` unique constraint inside a database savepoint.
- Supports callback lookup by either internal `call_id` or external `provider_call_id`.
- Reconciles state transitions monotonically; out-of-order webhooks (e.g., `COMPLETED` arriving before `ANSWERED`) cannot regress call or agent states.
- Automatically updates campaign telemetry on terminal call outcomes.

### 3.7 Self-Healing Reconciler & Background Daemon (`src/smartdialer/recovery/reconciler.py`, `src/smartdialer/services/worker.py`)
- Sweeps PostgreSQL to reclaim calls stuck in `RESERVED` past the 30-second reservation timeout.
- Reclaims agents abandoned in `WRAP_UP` past 60 seconds.
- Reclaims orphaned ringing calls past carrier timeout (60s).
- Background worker daemon `SmartDialerWorker` runs concurrent async polling loops with graceful OS signal handling (`SIGINT`/`SIGTERM`).

### 3.8 FastAPI REST API, Webhooks, WebSockets, Softphone & Twilio Adapter (`src/smartdialer/main.py`, `src/smartdialer/api/`, `src/smartdialer/providers/twilio_provider.py`)
- **REST & Webhook API:** Full FastAPI v1 surface (`/api/v1/campaigns`, `/agents`, `/borrowers`, `/dial`, `/calls/{id}/notes`, `/webhooks/provider/{provider_name}`, `/webhooks/twilio/status`, `/providers/{provider_name}/health`, `/simulations/run`, `/benchmarks/run`, `/health`).
- **WebSocket Real-Time Stream:** `/ws/campaigns/{campaign_id}` broadcasts dial cycles, safety decisions, agent transitions, call notes updates, and carrier webhook events in real time.
- **Light SaaS 4-Tab Operations Console (`/`):** Built-in web UI (`src/smartdialer/static/index.html`) featuring:
  1. **Live Operations:** Guided onboarding (`+ Add Agents`, `+ Add Borrowers`, `Demo Data`), `Auto-Dial` / `Run 1 Dial Wave`, 4-step **Safety Controller Pipeline** narrative, KPI cards, live `Borrower ↔ Agent` call steppers, and a plain-English WebSocket activity feed.
  2. **Agent Monitor & History:** Live per-agent roster (`Available`, `In Live Call`, `Dialing`, `Wrap-Up`, `Paused/Offline`), `Who Talked to Whom` call history, saved **Wrap-Up Call Notes** & dispositions, and supervisor controls (`Mark Free Now`, `Pause`, `Offline`).
  3. **Agent Softphone:** Headset view to dial waiting or custom numbers, simulate pickup, track live call timer, test interactive customer dialogue (with optional Web Speech TTS), hang up, and log wrap-up notes cleanly.
  4. **Stress Tests & Recovery:** Interactive runner for Scenarios A–D, plain-English `Before → Guardrail → After` failure-recovery cards, and live PostgreSQL concurrency benchmarking.
- **Twilio Real PSTN Adapter (`twilio`):** Optional `TwilioProvider` (`TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER`, `TWILIO_WEBHOOK_BASE_URL` in `.env.example`) backed by `provider_request_ledger` idempotency and `/api/v1/webhooks/twilio/status`. Fails safely if unconfigured.
- **Simulation & Load Testing (`src/smartdialer/simulation/runner.py`, `load_benchmark.py`, `locustfile.py`):** Quantitative simulator for Scenarios A–D + Failure Demos A–F, multi-worker PostgreSQL scaling benchmark (`100` / `1,000` / `10,000` agents), and Locust HTTP load suite.

See [docs/project-overview.md](docs/project-overview.md) for a plain-English architecture walkthrough, [docs/architecture-decisions.md](docs/architecture-decisions.md) for concurrency & recovery trade-offs, and [docs/assignment-answers.md](docs/assignment-answers.md) for design Q&A.

---

## 4. State Machines

### Agent Lifecycle
```
OFFLINE <--> AVAILABLE <--> RESERVED --> DIALING --> CONNECTED --> WRAP_UP --> AVAILABLE
               |              |                                      |
               v              v                                      v
             PAUSED        OFFLINE                                OFFLINE
```

### Call Lifecycle
```
QUEUED --> RESERVED --> INITIATED --> RINGING --> ANSWERED --> CONNECTED --> COMPLETED [TERMINAL]
              |             |            |           |            |
              v             v            v           v            v
          CANCELLED     CANCELLED    CANCELLED    FAILED       FAILED
          [TERMINAL]    [TERMINAL]   [TERMINAL]  [TERMINAL]  [TERMINAL]
```

---

## 5. Getting Started & Verification

### Prerequisites
- Python 3.12+ (tested on Python 3.14)
- PostgreSQL 16+ running locally on port 5432 (or via Docker Compose)

### Running the Test Suite
```bash
# Run the complete unit, concurrency, recovery, Stage 8 API, and Stage 9 simulation/scaling suite against PostgreSQL 18
python -m pytest tests/ -v
```

### Running the FastAPI Server, Dashboard & Background Worker
```bash
# Start PostgreSQL (local PostgreSQL 18 or docker-compose up -d postgres)
python -m uvicorn smartdialer.main:app --app-dir src --host 127.0.0.1 --port 8000
# Open http://127.0.0.1:8000/ for the Operations Console
# Open http://127.0.0.1:8000/docs for OpenAPI / Swagger UI
```

### Running Predictive Simulations, Failure Demos & Scaling Benchmarks
```bash
# Predictive Scenarios (A-D) and Failure Mode Demonstrations (A-F)
python -m smartdialer.simulation.runner

# PostgreSQL 100 / 1,000 / 10,000-Agent Multi-Worker Scaling Benchmark
python -m smartdialer.simulation.load_benchmark
```
See [docs/load-test-results.md](docs/load-test-results.md) for measured Stage 9 simulation, failure demo, PostgreSQL concurrency benchmark, and Locust HTTP load test results.

### Running Locust Load Tests
```bash
# With FastAPI server running on http://127.0.0.1:8000:
python -m locust -f locustfile.py --host http://127.0.0.1:8000 --headless -u 10 -r 5 -t 10s --only-summary
```

### Running the Standalone Background Worker Daemon
```bash
python -m smartdialer.services.worker
```
