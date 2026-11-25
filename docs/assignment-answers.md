# SmartDialer Technical Defense & Architecture Q&A

This document provides authoritative answers to the architectural, concurrency, failure recovery, and mathematical questions expected during a Principal / Senior Backend Engineer technical defense.

---

### Q1: How does the system guarantee that two concurrent workers never reserve the same agent?

**Answer:**
We enforce mutual exclusion at the database engine level using PostgreSQL row-level pessimistic locking via `SELECT ... FOR UPDATE SKIP LOCKED`.

When a dialing cycle initiates:
1. The transaction begins and executes:
   ```sql
   SELECT id FROM agents
   WHERE campaign_id = :campaign_id AND status = 'AVAILABLE'
   ORDER BY id ASC
   LIMIT :desired
   FOR UPDATE SKIP LOCKED;
   ```
2. In PostgreSQL, `FOR UPDATE` places an exclusive row lock on the selected tuples.
3. The `SKIP LOCKED` clause instructs PostgreSQL to immediately skip any rows that are currently locked by another concurrent transaction, rather than waiting or blocking.
4. If Worker 1 and Worker 2 simultaneously query 10 available agents for 5 calls each, Worker 1 claims agents 1–5, while Worker 2 simultaneously skips rows 1–5 and claims agents 6–10.
5. In the exact same transaction, the agent status is updated to `RESERVED` with a unique `reservation_token` and `reserved_at` timestamp.
6. The optimistic version column `Agent.version` (mapped via SQLAlchemy's `version_id_col`) acts as an additional safety net: any concurrent write to an agent tuple triggers a `StaleDataError` rollback.

Application-level checks alone are never relied upon; concurrency safety is fully enforced by PostgreSQL ACID isolation.

---

### Q2: How does the system ensure no outbound call is dispatched without a valid, unique agent reservation?

**Answer:**
This is the core non-negotiable invariant of the system, enforced by a two-layer verification architecture:

1. **Layer 1 — Safety Controller Quota Pre-Reservation:**
   The Safety Controller acts as an independent mandatory boundary. During evaluation, it authoritatively locks available agents using `FOR UPDATE SKIP LOCKED`, transitions them to `RESERVED`, and embeds their unique IDs in `SafetyDecision.claimed_agent_ids`. The allocator is only permitted to construct calls bound to those pre-reserved agents.
2. **Layer 2 — Pre-Dispatch Outbox Verification (`OutboxDispatcher`):**
   Before the `OutboxDispatcher` makes any network call to a telecom carrier:
   - It acquires an exclusive database row-lock on the `Call`, `Agent`, and `Borrower`.
   - It verifies that `agent.status == 'RESERVED'`.
   - It verifies that the reservation has not timed out (`NOW() - agent.reserved_at <= 30s`).
   - It verifies that the campaign is in `ACTIVE` status and the carrier is healthy.
   - If the agent has disconnected, gone `OFFLINE`, or transitioned to `PAUSED` while the outbox event was waiting, **the dispatcher immediately cancels the dispatch**. It transitions the call to `CANCELLED`, releases the borrower to `ELIGIBLE`, and does NOT invoke the telecom provider.

Under no circumstances is an outbound call dispatched to a carrier network without a verified, live agent reservation.

---

### Q3: How does the system handle an agent going offline while an outbox record is pending?

**Answer:**
This scenario is thoroughly handled by Pre-Dispatch Validation in `OutboxDispatcher._dispatch_single_record`:

1. Suppose Agent X is reserved for Call Y, and an outbox event is queued.
2. Before the outbox worker processes the record, Agent X's workstation loses internet connectivity, or the agent logs off. The supervisor or agent state machine transitions Agent X:
   $$\text{Agent X} \longrightarrow \text{OFFLINE}$$
3. When `OutboxDispatcher` picks up the pending outbox event:
   - It locks Agent X with `SELECT ... FOR UPDATE`.
   - It inspects `agent.status` and observes it is `OFFLINE` (not `RESERVED`).
   - It logs a pre-dispatch validation failure: `Agent status is 'OFFLINE', expected 'RESERVED'`.
   - It sets `OutboxEvent.status = 'CANCELLED'`.
   - It transitions `Call.status = 'CANCELLED'` with reason `AGENT_STATE_OFFLINE`.
   - It releases the borrower: `Borrower.status = 'ELIGIBLE'`, `Borrower.reservation_token = None`.
   - It commits the cancellation and **does NOT invoke `provider.initiate_call`**.
4. The borrower's phone never rings, and zero abandoned calls occur.

---

### Q4: How does the system prevent duplicate calls when telecom providers time out or workers crash during batch dispatch?

**Answer:**
We solve this through three cooperating mechanisms:

1. **Independent Per-Record Outbox Transactions:**
   The outbox dispatcher never executes batch dispatches inside a single uncommitted transaction. Each outbox record is locked, lease-committed (`locked_until = NOW() + 20s`), dispatched, and post-committed independently. A crash on record #5 has zero effect on records #1–#4, which are already committed as `DISPATCHED`.
2. **Persistent Database-Backed Idempotency Ledger:**
   Both `ProviderA` and `ProviderB` are backed by the PostgreSQL table `provider_request_ledger` with a unique constraint on `(provider_name, idempotency_key)`.
   When an outbox record is retried (or re-picked up by a recovery worker), `provider.initiate_call` queries the database for `(provider_name, idempotency_key)`. Finding an existing entry, it returns the existing `provider_call_id` without dialing the carrier again.
3. **Deterministic Idempotency Keys:**
   `CallAllocator` formats idempotency keys deterministically:
   $$\text{key} = \text{"call-" } + \text{campaign\_id} + \text{"-" } + \text{borrower\_id} + \text{"-att" } + \text{call\_attempts}$$
   If a retry occurs for the same attempt, the exact same key is reused, enforcing end-to-end deduplication across both the internal database and external telecom carriers.

---

### Q5: How does the system handle duplicate and out-of-order carrier webhooks?

**Answer:**

#### 1. Database-Enforced Webhook Deduplication:
All provider callbacks are inserted into `provider_events`, which enforces:
```sql
CONSTRAINT uq_provider_name_event_id UNIQUE (provider_name, event_id)
```
Inside `EventProcessor.process_event`, the insertion runs inside a database savepoint (`session.begin_nested()`). When a duplicate webhook arrives (due to carrier retries), PostgreSQL raises an `IntegrityError`. The application intercepts this, acknowledges the webhook with `is_duplicate=True`, and leaves the call state untouched.

#### 2. Monotonic State Ranking & Terminal Immutability:
Each call state is assigned an integer rank:
`QUEUED (0)` $\to$ `RESERVED (1)` $\to$ `INITIATED (2)` $\to$ `RINGING (3)` $\to$ `ANSWERED (4)` $\to$ `CONNECTED (5)` $\to$ `TERMINAL (6)`.
- If an event arrives with rank $R_{\text{target}} \le R_{\text{current}}$, it is recognized as a regression or duplicate and discarded without altering state.
- An `ANSWERED` event arriving late can **never regress a call already in `COMPLETED`, `FAILED`, or `CANCELLED`**.
- If a carrier skips intermediate callbacks (e.g., delivers `CONNECTED` directly from `INITIATED`), `CallStateMachine.VALID_TRANSITIONS` explicitly accepts this legal forward jump, backfilling `answered_at` and `connected_at`.

---

### Q6: Explain the mathematical derivation of the Predictive Pacing Engine and how regulatory abandonment limits (3%) are maintained.

**Answer:**

The Predictive Pacing Engine uses a statistical model derived from queueing theory and Little's Law, avoiding unpredictable neural networks:

#### 1. Smoothed Answer Rate ($\hat{p}$):
Blends windowed empirical rate ($p_{\text{hist}}$) with Exponential Moving Average ($p_{\text{ema}}$, $\alpha = 0.20$):
$$\hat{p} = 0.70 \cdot p_{\text{ema}} + 0.30 \cdot p_{\text{hist}}, \quad \hat{p} \in [0.05, 0.95]$$

#### 2. Little's Law Freeing Capacity ($C_{\text{eff}}$):
Outbound calls take an average setup/ringing latency of $T_{\text{ring}} \approx 18\text{ seconds}$. In that window, active connected agents complete calls at departure rate $\mu_{\text{depart}} = 1 / \bar{T}_{\text{talk}}$. The projected capacity is:
$$A_{\text{freeing}} = \min\left(0.40 \cdot A_{\text{conn}}, \, A_{\text{conn}} \cdot \mu_{\text{depart}} \cdot T_{\text{ring}}\right) + A_{\text{wrap\_up}}$$
$$C_{\text{eff}} = A_{\text{avail}} + A_{\text{freeing}}$$

#### 3. Net Answer Slots Needed:
Accounting for in-flight ringing calls that are already expected to answer ($R \cdot \hat{p}$):
$$S_{\text{needed}} = \max\left(0, \, (C_{\text{eff}} \cdot U_{\text{target}}) - (R \cdot \hat{p})\right), \quad U_{\text{target}} = 0.85$$

#### 4. Binomial Variance Discount for Regulatory 3% Abandonment Ceiling:
The number of answers resulting from $N$ dialed calls follows a Binomial distribution $B(N, \hat{p})$ with variance $\sigma^2 = N \hat{p}(1 - \hat{p})$.
To guarantee that sudden answer spikes do not exceed available agent capacity at the 95% confidence level ($z = 1.645$):
$$S_{\text{conservative}} = \max\left(0, \, S_{\text{needed}} - 0.5 \cdot 1.645 \cdot \sqrt{S_{\text{needed}} (1 - \hat{p})}\right)$$
$$\text{Desired Calls } N = \left\lfloor \frac{S_{\text{conservative}}}{\hat{p}} \right\rfloor$$

#### 5. Safety Controller Final Gate:
The pacing proposal is submitted to the Safety Controller as a suggestion. The Safety Controller verifies physical database capacity and safe headroom. If carrier failure rate exceeds 15%, the Safety Controller trips a circuit breaker and automatically falls back to conservative 1:1 progressive dialing.

---

### Q7: What happens if a worker crashes immediately after reserving an agent?

**Answer:**
This was the exact defect investigated in **FIND-CRIT-01** and resolved in our repair mission:

1. When a worker crashes after atomic reservation commit, the `Call` record remains in `RESERVED`, the `Agent` is in `RESERVED`, and the `OutboxEvent` is in `PENDING`.
2. **Path A — Normal Outbox Recovery:** The background worker daemon runs `OutboxDispatcher.dispatch_pending`. The pending outbox event is picked up and dispatched to the carrier network.
3. **Path B — Abandoned Reservation Recovery:** If the outbox record is somehow lost or fails, the reservation age exceeds `RESERVATION_TIMEOUT_SEC` (30 seconds).
4. The background `ReconciliationSweeper` runs:
   ```sql
   SELECT id, agent_id, borrower_id FROM calls
   WHERE status = 'RESERVED' AND created_at < NOW() - INTERVAL '30 seconds'
   FOR UPDATE SKIP LOCKED;
   ```
5. The sweeper:
   - Transitions the stale call to `FAILED` with termination reason `STALE_RESERVATION_TIMEOUT`.
   - Transitions the locked agent back to `AVAILABLE` and clears `reservation_token`, `reserved_at`, and `current_call_id`.
   - Transitions the borrower back to `ELIGIBLE` and clears `reservation_token`.
   - Commits atomically, fully restoring cluster capacity with zero permanent agent leaks.

---

### Q8: Why is a Modular Monolith on PostgreSQL chosen over Redis and Kafka?

**Answer:**
1. **Atomic Dual-Writes:** In collections dialing, reserving an agent, reserving a borrower, creating a call, and queuing a dispatch MUST occur atomically. In a microservices + Kafka + Redis architecture, achieving this requires distributed two-phase commits (2PC) or outbox CDC (Debezium) with eventual consistency. In PostgreSQL, this is an instantaneous 4ms ACID transaction.
2. **Zero Split-Brain Failure Modes:** Redis distributed locks (Redlock) are vulnerable to process pause (GC pause) lock expiration races, where Redis expires a lock while a worker is still writing to the database, causing two workers to reserve the same agent. PostgreSQL row locks are tied to the lifetime of the TCP transaction: if a worker crashes or pauses, PostgreSQL holds or releases the lock with 100% mathematical certainty.
3. **Operational Simplicity & Cost:** Telephony systems must be auditable. Having all call states, agent transitions, outbox events, carrier logs, and safety decisions in PostgreSQL schemas enables instantaneous SQL auditing, ACID compliance reporting, and zero-infrastructure local test execution without managing external clusters.

---

### Q9: Scaling from 100 → 1,000 → 10,000 agents: What breaks first, and how do you fix it?

**Answer:**
1. **First Bottleneck (around 1,000+ agents / high webhook bursts): Single-Table Row Contention & Hot Index Scanning on `agents`, `borrowers`, and `outbox_events` + DB Connection Exhaustion.**
   - Even with `SELECT ... FOR UPDATE SKIP LOCKED`, if 50 workers simultaneously scan the same campaign's `AVAILABLE` index or poll `outbox_events` every 250ms, lock-skipping still scans skipped heap/index tuples, and each worker holds synchronous DB connections across bursts.
   - **Process-Local `CampaignTelemetry`:** At multiple API/worker replicas, in-memory `deque` telemetry diverges across nodes.
2. **How We Fix It (Targeted Evolution, Not Premature Microservices):**
   - **Campaign Sharding / Partitioning:** Partition worker polling and `agents`/`calls`/`outbox_events` by `campaign_id` hash (`MOD` worker shard) so workers rarely scan the same rows.
   - **PgBouncer + Async I/O Separation:** Place PgBouncer (transaction pooling) in front of PostgreSQL, keep DB transactions strictly sub-5ms (already enforced by the Outbox + autonomous ledger pattern), and separate webhook ingestion pods from dialer-loop pods.
   - **Shared Rollup Telemetry Table / Materialized Counters:** Persist rolling 5-minute campaign telemetry (`answered_count`, `talk_time_sum`) in PostgreSQL (or Redis Streams for telemetry only) so all workers read identical pacing metrics.
   - **Event Table Archival / Time-Range Partitioning:** Partition `calls`, `provider_events`, and `outbox_events` daily/weekly by `created_at` so active working sets stay entirely in PostgreSQL `shared_buffers`.

---

### Q10 (Final Question): How would you build a SmartDialer that gets as much of the utilization benefit of predictive dialing as possible, while retaining the deterministic safety characteristics of progressive dialing?

**Answer:**
Use a **Hybrid Progressive-First / Bounded-Lookahead Architecture**:
1. **Deterministic Core (Progressive Guarantee):** Whenever an agent is `AVAILABLE`, treat that slot as **1:1 deterministically reserved** — zero risk of abandonment for those calls because the agent is already physically locked.
2. **Bounded Lookahead Only for Maturing Departures (Little's Law + Binomial Lower Bound):** Only dial *extra* agent-less calls against agents who are already in late `CONNECTED` or `WRAP_UP` state, discounted by a 95% one-sided binomial confidence interval ($z = 1.645$) so we never bet on an aggressive mean.
3. **Hard Mandatory Safety Boundary + Instant Progressive Fallback:** Keep the non-bypassable `SafetyController` in front of every dial decision. The moment carrier latency spikes, answer-rate variance jumps, or a single call hits abandonment warning thresholds, the Safety Controller deterministically collapses lookahead headroom to zero (falling back instantaneously to 1:1 progressive mode) — giving **predictive utilization during steady-state queues** and **deterministic progressive safety whenever uncertainty rises**.
