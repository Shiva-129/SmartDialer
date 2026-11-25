# SmartDialer Architecture Decision Records (ADRs)

This document records the key architectural decisions, rationale, trade-offs, and alternatives considered during the design and construction of the SmartDialer system.

---

## ADR 1: Modular Monolith vs Distributed Microservices

### Context
Outbound collections dialing requires sub-50ms coordination across pacing algorithms, agent reservation pools, call queues, telecom provider APIs, and event callbacks.

### Decision
Implement SmartDialer as a **modular monolith** with clean bounded contexts (Safety, Pacing, Allocator, Providers, Events, Recovery) deployed against a unified PostgreSQL database, avoiding microservices, Kubernetes, Redis, or Kafka.

### Consequences
- **Positive:**
  - ACID transactions across agent reservation, borrower reservation, call creation, and outbox event persistence in a single local database transaction.
  - Zero network hops between pacing proposal and safety validation.
  - Zero distributed dual-write inconsistency or 2-phase commit overhead.
  - Operational simplicity and lower cloud hosting cost.
- **Negative:**
  - Horizontal scaling of specific modules (e.g., scaling only webhook processing) requires scaling the entire application process. (Mitigated by independent worker processes polling specific queues using `SKIP LOCKED`).

---

## ADR 2: PostgreSQL `FOR UPDATE SKIP LOCKED` vs Redis Distributed Locks

### Context
When multiple worker processes attempt to allocate agents and borrowers concurrently, they must not allocate the same entity twice or deadlock on locks.

### Decision
Use PostgreSQL row-level pessimistic locking with `SELECT ... FOR UPDATE SKIP LOCKED` rather than Redis Redlock or distributed memory locks.

### Consequences
- **Positive:**
  - Agent and borrower reservation lock is held *inside the exact database transaction* that creates the Call and Outbox records. If the worker crashes, PostgreSQL immediately releases the lock.
  - No lock timeout expiration races (the infamous Redis lock TTL expiry while a slow database transaction is still committing).
  - Eliminates Redis as an external infrastructure dependency.
- **Negative:**
  - Requires maintaining open database connections during allocation transactions. (Mitigated by keeping allocation transactions extremely short: $< 5\text{ms}$).

---

## ADR 3: Transactional Outbox Pattern for Telecom Dispatch

### Context
Invoking external telecom carriers (Twilio, Vonage, Plivo) via HTTP takes 50–500ms and can fail due to network timeouts, rate limits, or carrier outages. Holding a database transaction open across slow external HTTP requests exhausts database connection pools. Conversely, committing before sending network requests causes call loss if the process crashes.

### Decision
Implement the **Transactional Outbox Pattern**:
1. Phase 1: Atomically insert call record and an `outbox_events` record with status `PENDING` inside the allocation transaction.
2. Phase 2: An asynchronous `OutboxDispatcher` acquires a durable lease (`locked_until`), validates agent eligibility, invokes the provider outside any database locks, and records the result in an independent transaction.

### Consequences
- **Positive:**
  - Database row locks are never held across external HTTP requests.
  - At-least-once delivery guarantee: crashed dispatches are automatically re-claimed by recovery workers upon lease expiration.
- **Negative:**
  - Introduces a small latency buffer (10–50ms) between call allocation and provider dispatch. (Completely acceptable for outbound telephony).

---

## ADR 4: Independent Mandatory Safety Controller

### Context
Predictive pacing algorithms attempt to optimize agent utilization by overdialing based on statistical answer probabilities. If a predictive model miscalculates or encounters a sudden answer rate spike, multiple borrowers answer simultaneously with no agents available, violating regulatory abandonment limits (FTC/Ofcom 3% ceiling).

### Decision
Establish an **independent, non-bypassable Safety Controller** that acts as the sole authorized gatekeeper. Neither dialing strategy may directly invoke telecom providers. The Safety Controller performs independent database verification of physical agent capacity, active calls, campaign ceilings, and carrier health, rejecting or throttling any unsafe proposals.

### Consequences
- **Positive:**
  - Guarantees zero agent-bound calls are dispatched without safely reserved agent capacity.
  - Enforces automatic progressive fallback whenever predictive pacing exhibits elevated carrier error rates or erratic metrics.
  - Provides an immutable audit trail of every dialing decision and its mathematical explanation.
- **Negative:**
  - Dialing requests incur an additional validation step. (Validation execution is $< 2\text{ms}$).

---

## ADR 5: Statistical Pacing (Little's Law + EMA) vs Deep Learning

### Context
A predictive pacing engine is required to estimate dialing demand based on answer rates, talk durations, and queue states.

### Decision
Implement an explainable statistical model using Exponential Moving Average (EMA, $\alpha=0.20$) answer rate smoothing and Little's Law queueing projections:
$$C_{\text{eff}} = A_{\text{avail}} + \min\left(0.40 \cdot A_{\text{conn}}, A_{\text{conn}} \cdot \mu_{\text{depart}} \cdot T_{\text{ring}}\right) + A_{\text{wrap\_up}}$$
discounted by binomial distribution confidence intervals ($z = 1.645$). Avoid deep learning or black-box neural networks.

### Consequences
- **Positive:**
  - 100% explainable, deterministic mathematical model that can be defended in compliance audits and regulatory inquiries.
  - Zero training overhead, cold-start delays, or inference latency.
  - Immediate adaptation to sudden changes in campaign behavior.
- **Negative:**
  - Does not model non-linear time-of-day demographic patterns without explicit heuristic parameters.

---

## ADR 6: Persistent Database-Backed Provider Idempotency

### Context
When telecom carrier requests time out or fail, retry loops re-send the dispatch request. Real telecom providers guarantee that duplicate requests with the same `idempotency_key` return the existing call ID and do not place a second phone call.

### Decision
Store all outbound carrier requests in a PostgreSQL table `provider_request_ledger` with a unique constraint on `(provider_name, idempotency_key)`.

### Consequences
- **Positive:**
  - Re-dispatching an outbox event after a worker restart or network glitch always returns the original `provider_call_id`.
  - Zero duplicate phone calls to borrowers across worker restarts or concurrent workers.
- **Negative:**
  - Requires one extra table insert per initial dispatch. (Negligible overhead with PostgreSQL index).
