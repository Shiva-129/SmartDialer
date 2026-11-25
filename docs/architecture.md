# SmartDialer Architecture & Concurrency Model

## 1. Architectural Philosophy

SmartDialer is structured as a **modular monolith** optimized for extreme correctness, concurrency control, and fault tolerance in high-volume debt collections. 

Distributed architectures often introduce distributed transactions (2PC, saga choreographies) across message brokers (Kafka, RabbitMQ) and caching layers (Redis), which introduce split-brain failure modes, out-of-order race conditions, and distributed state leakage. Instead, SmartDialer leverages **PostgreSQL as the single source of truth** combined with:
1. Row-level pessimistic locking (`SELECT ... FOR UPDATE SKIP LOCKED`).
2. The Transactional Outbox pattern.
3. Monotonic state ranking in state machines.
4. An independent, non-bypassable Safety Controller.

```
+-----------------------------------------------------------------------+
|                           Campaign Manager                            |
+-----------------------------------------------------------------------+
           |                                                |
           v                                                v
+-----------------------+                        +----------------------+
|  Progressive Dialer   |                        |  Predictive Pacing   |
| (Strict 1:1 Dialing)  |                        | Engine (Little's Law)|
+-----------------------+                        +----------------------+
           |                                                |
           +-----------------------+------------------------+
                                   |
                                   v
                    +-----------------------------+
                    |      Safety Controller      | <=== MANDATORY BOUNDARY
                    |  (Atomic Quota Reservation) |
                    +-----------------------------+
                                   |
                                   v
                    +-----------------------------+
                    |       Call Allocator        |
                    | (Agent & Borrower Lock TX)  |
                    +-----------------------------+
                                   |
                                   v
                    +-----------------------------+
                    |    Transactional Outbox     |
                    +-----------------------------+
                                   |
                                   v
                    +-----------------------------+
                    |      Outbox Dispatcher      |
                    | (Pre-Dispatch Verification) |
                    +-----------------------------+
                                   |
                                   v
                    +-----------------------------+
                    | Telecom Provider Interface  |
                    | (PostgreSQL Idemp. Ledger)  |
                    +-----------------------------+
                                   |
                                   v
                    +-----------------------------+
                    |       Event Processor       |
                    | (Deduplication & Monotonic) |
                    +-----------------------------+
                                   |
                                   v
                    +-----------------------------+
                    | State Reconciliation Engine |
                    | (Sweepers: Stale Res / Ring)|
                    +-----------------------------+
```

---

## 2. Concurrency Control & Row Locking

### 2.1 The Two-Worker Collision Problem
In high-concurrency environments, multiple background workers simultaneously attempt to claim available agents or borrowers. Application-level checks like:
```python
# DEFECTIVE ANTI-PATTERN (TOCTOU):
available_count = session.query(Agent).filter_by(status='AVAILABLE').count()
if available_count > 0:
    agent = session.query(Agent).filter_by(status='AVAILABLE').first()
    agent.status = 'RESERVED'
```
allow multiple workers to read the same `AVAILABLE` row, resulting in double-reservations and simultaneous outbound calls for a single agent.

### 2.2 PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED`
SmartDialer solves this at the database engine level:
```sql
SELECT id FROM agents
WHERE campaign_id = :campaign_id AND status = 'AVAILABLE'
ORDER BY id ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;
```
- `FOR UPDATE`: Locks the selected row in exclusive mode until the current transaction commits or rolls back.
- `SKIP LOCKED`: Skips rows currently locked by other concurrent transactions rather than blocking or waiting, ensuring $O(1)$ lock acquisition latency and zero deadlocks.

---

## 3. The Independent Safety Controller

The **Safety Controller** is an isolated architectural boundary. Neither the Progressive Dialer nor the Predictive Pacing Engine can initiate provider calls directly.

### Operational Sequence:
1. **Dialing Request Submitted:** Pacing strategy submits an immutable `DialingRequest` (mode, desired calls, estimated answer rate).
2. **Atomic Quota Reservation:** Evaluates database state and locks available agents using `FOR UPDATE SKIP LOCKED`.
3. **Capacity & Headroom Analysis:**
   - Active calls under campaign ceiling: $\text{Ceiling} - \text{Active}$.
   - Safe headroom accounting for in-flight ringing calls: $\text{Capacity} - \text{Ringing}$.
   - Provider health: Trips circuit breaker if provider failure rate $> 15\%$.
4. **Structured Decision:** Emits `APPROVED`, `REDUCED`, `REJECTED`, or `FALLBACK_TO_PROGRESSIVE`.
5. **Audit Logging:** Immutably logs the request, decision, available capacity, and reasoning to `safety_decision_logs`.

---

## 4. Failure Recovery & The Transactional Outbox Pattern

### 4.1 The Worker Crash Dilemma
If a worker commits a call record and immediately performs an HTTP request to Twilio or Vonage:
- If the worker crashes before sending the HTTP request, the call is orphaned in the database.
- If the HTTP request succeeds but the worker crashes before committing the status update, the next worker re-sends the call, causing duplicate dials.

### 4.2 Two-Phase Transactional Outbox Architecture
1. **Phase 1 (Atomic Reservation TX):**
   Inside a single database transaction, the Call Allocator reserves the Agent (`RESERVED`) in Progressive mode, reserves the Borrower (`RESERVED`), creates the Call (`RESERVED`), and inserts an `OutboxEvent` with status `PENDING`. In Predictive mode, calls may be created with `agent_id = None` (`dialing_mode = 'PREDICTIVE'`), allowing calls to dial ahead of agent freeing.
2. **Phase 2 (Outbox Dispatcher with Lease Locking & Fencing Tokens):**
   - Background dispatcher locks pending outbox records:
     ```sql
     SELECT * FROM outbox_events
     WHERE status = 'PENDING' AND (locked_until IS NULL OR locked_until < NOW())
     ORDER BY created_at ASC
     FOR UPDATE SKIP LOCKED;
     ```
   - **Pre-Dispatch Validation:** Locks the agent and call row to verify the agent is still in `RESERVED` status and the campaign is active. If the agent went offline or paused, cancels the call and releases the borrower without dialing.
   - **Dispatch Fencing Token:** Generates a unique `dispatch_token: UUID` assigned to `call.dispatch_token` and `agent.dispatch_token`, committing the lease (`locked_until = NOW() + 20s`).
   - Invokes telecom provider. Provider commits dispatch intent and completion in an **autonomous transaction**, guaranteeing ledger durability even under worker crashes.
   - **Post-Dispatch Fencing Check:** Reloads call and agent state with `session.expire_all()`. If the agent went offline/paused during external network I/O, immediately calls `provider.terminate_call(provider_call_id)` and cancels the call. If the worker lease expired and another process mutated the token, rejects the stale completion and terminates the carrier call.
   - If valid: updates call to `INITIATED`, agent (if assigned) to `DIALING`, and commits outbox to `DISPATCHED`.

### 4.3 Predictive Dialing & Answer-to-Agent Bridge Allocator
In Predictive dialing, calls are placed ahead of agent availability using Little's Law departure projections:
1. Calls dial out with `agent_id = None` and `dialing_mode = 'PREDICTIVE'`.
2. When the carrier delivers an `ANSWERED` or `CONNECTED` webhook, the **Bridge Allocator** is invoked.
3. The Bridge Allocator executes:
   ```sql
   SELECT * FROM agents
   WHERE campaign_id = :campaign_id AND status = 'AVAILABLE'
   ORDER BY last_call_ended_at ASC NULLS FIRST, id ASC
   LIMIT 1
   FOR UPDATE SKIP LOCKED;
   ```
4. If an agent is available: assigns `call.agent_id = agent.id` and transitions the agent directly to `CONNECTED`.
5. If no agent is available before `answer_timeout_sec` (e.g. 2.0s): triggers compliant abandonment, marking the call `FAILED` with `termination_reason = 'ABANDONED'` to enforce strict zero-abandonment regulations.

### 4.4 Self-Healing Reconciler Sweeper & Lease Coordination
Runs continuously in the background to handle crashed workers and network partitions:
- **Active Lease Protection:** Before sweeping stale calls in `RESERVED` status ($> 30\text{s}$), the reconciler checks if an outbox worker actively holds a lease (`locked_until > now`). If so, the sweeper respects the worker's lease and skips the call, preventing collisions with slow carrier dispatches.
- **Stale Reservation Sweeper (30s):** Reclaims calls and agents stuck in `RESERVED` status beyond 30 seconds whose outbox lease has expired.
- **Wrap-Up Sweeper (60s):** Reclaims agents left in `WRAP_UP` status beyond 60 seconds.
- **Orphaned Ringing Call Sweeper (60s):** Reclaims calls stuck in `RINGING` past telecom carrier timeout without answer callbacks.
