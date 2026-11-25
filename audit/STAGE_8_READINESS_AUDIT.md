# INDEPENDENT STAGE 8 READINESS AUDIT REPORT (REPAIR ROUND 2)

**Auditor:** Independent Principal Distributed Systems Engineer & Telecom Safety Auditor  
**Date:** November 24, 2026  
**Target Codebase:** SmartDialer Core Engine (Stages 1–7 Post-Repair Round 2)  
**Database Backend:** PostgreSQL 18.1 on x86_64-windows (64-bit), Database: `smartdialer_test`  
**Reference Documents:**
- [`audit/POST_REPAIR_AUDIT.md`](file:///c:/Users/shiva/Downloads/website/assignment/audit/POST_REPAIR_AUDIT.md)
- [`audit/REPAIR_2_REPORT.md`](file:///c:/Users/shiva/Downloads/website/assignment/audit/REPAIR_2_REPORT.md)

---

## 1. Executive Verdict

### **FINAL VERDICT: NOT READY FOR STAGE 8**

The implementation engineer claims in [`audit/REPAIR_2_REPORT.md`](file:///c:/Users/shiva/Downloads/website/assignment/audit/REPAIR_2_REPORT.md) that all five critical distributed flaws identified in [`audit/POST_REPAIR_AUDIT.md`](file:///c:/Users/shiva/Downloads/website/assignment/audit/POST_REPAIR_AUDIT.md) have been resolved and that 69/69 tests pass against PostgreSQL 18.

An exhaustive, independent adversarial distributed-systems audit of the modified source code, live PostgreSQL 18 database executions, and concurrent race simulations confirms that **this claim is FALSE**.

While the implementation engineer successfully eliminated the initial predictive allocation deadlock (allowing agent-less calls to be dispatched with `agent_id = None`) and added partial cleanup blocks, critical architectural flaws and distributed race conditions remain active in the codebase:

1. **CRITICAL DEFECT 1: Abandoned Predictive Calls Never Terminated at Carrier & Premature Borrower Release ([`src/smartdialer/events/processor.py#L180-L216`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/events/processor.py#L180-L216))**:  
   When an agent-less predictive call answers and `BridgeAllocator` times out (e.g. after 2.0s), `EventProcessor` transitions the call to `FAILED` with `termination_reason="ABANDONED"`. **It NEVER invokes `provider.terminate_call(call.provider_call_id)`**. The telecommunication carrier call remains **live and connected on the PSTN network**, leaving the consumer hearing silence. Simultaneously, the processor transitions `borrower.status` to `ELIGIBLE`, allowing subsequent dialer cycles to place a second concurrent phone call to a consumer who is still actively connected to the first call.
2. **HIGH DEFECT 2: Answering Calls Connected to Offline or Paused Agents ([`src/smartdialer/allocator/bridge.py#L49-L56`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/allocator/bridge.py#L49-L56), [`src/smartdialer/events/processor.py#L187-L205`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/events/processor.py#L187-L205))**:  
   If an assigned agent logs off (`OFFLINE`) or pauses (`PAUSED`) while a call is dialing or ringing, `BridgeAllocator` returns the offline/paused agent, and `EventProcessor` transitions the call to `CONNECTED`. The call enters `CONNECTED` status in the database with an `OFFLINE` agent and `current_call_id = None`, connecting the borrower to dead air.
3. **HIGH DEFECT 3: Absence of Lease Heartbeat Causes Sweeper Preemption on Slow Carrier Calls ([`src/smartdialer/recovery/outbox_dispatcher.py#L241-L253`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/recovery/outbox_dispatcher.py#L241-L253), [`src/smartdialer/recovery/reconciler.py#L86-L92`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/recovery/reconciler.py#L86-L92))**:  
   The outbox dispatcher sets a static 20-second lease lock without heartbeat renewal. When carrier API latency exceeds 20 seconds, the lease expires. When call age exceeds 30 seconds, `ReconciliationSweeper` marks the call `FAILED`, marks the outbox event `FAILED`, reclaims the agent to `AVAILABLE`, and reclaims the borrower to `ELIGIBLE`. When the carrier request returns at $t=35\text{s}$, the agent and borrower have already been re-allocated to a second call.
4. **MEDIUM DEFECT 4: Permanent Ledger Poisoning on Crashed Worker in PENDING State ([`src/smartdialer/providers/provider_a.py#L137-L164`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/providers/provider_a.py#L137-L164), [`src/smartdialer/providers/provider_b.py#L139-L166`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/providers/provider_b.py#L139-L166))**:  
   When a worker registers intent (`status = 'PENDING'`) and crashes before completing the carrier call, retrying workers see `status == 'PENDING'` and sleep in a 2-second loop. Because the crashed worker is dead, every retry times out with `CONCURRENT_DISPATCH_TIMEOUT`. After 3 retries, the call permanently fails. The `PENDING` ledger entry is never expired or reconciled.
5. **MEDIUM DEFECT 5: Unhandled Exceptions & Unchecked Return Values on Provider Call Termination ([`src/smartdialer/recovery/outbox_dispatcher.py#L276-L280`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/recovery/outbox_dispatcher.py#L276-L280), [`L295-L298`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/recovery/outbox_dispatcher.py#L295-L298))**:  
   `provider.terminate_call()` return values are completely ignored. If carrier termination returns `False` or raises an unhandled network error, no compensatory event is queued and the worker loop crashes, leaving rogue carrier calls ringing indefinitely.

Stage 8 implementation (Webhooks, Live Carrier Integration, WebSockets, Frontend) cannot proceed safely until these distributed and regulatory safety invariants are genuinely enforced.

---

## 2. Audit Area Verification Matrix

| Audit Area | Scope | Verification Status | Auditor Assessment & Residual Risk |
| :--- | :--- | :--- | :--- |
| **Audit A** | Predictive Bridge Correctness | **FAIL** | Predictive calls allocate with `agent_id = None`, and `SKIP LOCKED` prevents double-assignment. **However, abandoned calls are never hung up at the carrier, borrowers are prematurely released while calls are active, and answering calls can bridge to offline agents.** |
| **Audit B** | Provider Idempotency & Crash Recovery | **PARTIALLY VERIFIED** | Autonomous transactions prevent caller session rollback from deleting ledger entries. However, crashed workers leave permanent `PENDING` entries, and `UNCERTAIN` outcomes have no reconciliation path. |
| **Audit C** | Dispatch Fencing & Stale Workers | **VERIFIED WITH CAVEATS** | Stale worker writes are rejected by `dispatch_token` mismatch. Carrier call termination is attempted upon stale completion, but termination failures/exceptions are unhandled. |
| **Audit D** | Lease Heartbeat & Recovery | **FAIL** | **No lease heartbeat or renewal exists.** Slow provider calls ($>20\text{s}$) lose their lease. Calls $>30\text{s}$ are cancelled by the reconciler sweeper while the worker is actively waiting on the carrier. |
| **Audit E** | Reservation Cleanup | **PASS** | `try...finally` blocks in progressive and predictive dialers cleanly transition unallocated agents from `RESERVED` back to `AVAILABLE`. Cleanup is idempotent. |
| **Audit F** | PostgreSQL Test Credibility | **PASS (Test Harness) / FAIL (Coverage)** | Tests run against PostgreSQL 18.1 (`smartdialer_test`). However, tests mock carrier latency with millisecond sleeps and do not exercise abandonment carrier teardown or slow carrier lease expiration. |

---

## 3. Detailed Audit Findings

---

### FINDING 1 (CRITICAL): Abandoned Calls Never Terminated at Carrier & Premature Borrower Release

- **Severity:** CRITICAL (Regulatory & Telecom Invariant Violation)
- **Component:** `events/processor.py`
- **File & Line:** [`src/smartdialer/events/processor.py#L180-L216`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/events/processor.py#L180-L216)
- **Violation:** Audit A: *"Abandonment timeout and failed termination are handled safely. The borrower is not incorrectly released while the carrier call remains active."*

#### Reproduction Steps
Execute the following verification script against PostgreSQL 18:
```python
import uuid
from sqlalchemy.orm import sessionmaker
from smartdialer.db.session import sync_engine
from smartdialer.models import Campaign, Borrower, Call, CallState
from smartdialer.events.processor import EventProcessor

SessionMaker = sessionmaker(bind=sync_engine, expire_on_commit=False)
with SessionMaker() as s:
    cmp = Campaign(name="Test Abandonment Bug")
    s.add(cmp)
    s.flush()
    b = Borrower(campaign_id=cmp.id, phone_number="+15551230001", name="Borrower Bug")
    s.add(b)
    s.flush()
    c = Call(
        campaign_id=cmp.id,
        agent_id=None,
        borrower_id=b.id,
        status=CallState.INITIATED.value,
        idempotency_key=f"test-abandon-{uuid.uuid4()}",
        provider_name="provider_a",
        provider_call_id="prov-live-call-123",
        dialing_mode="PREDICTIVE",
    )
    s.add(c)
    s.commit()
    cid, bid = c.id, b.id

with SessionMaker() as s:
    res = EventProcessor.process_event(
        session=s,
        provider_name="provider_a",
        event_id=f"evt-{uuid.uuid4()}",
        call_id=cid,
        event_type="ANSWERED",
        raw_payload={"answer_timeout_sec": 0.01}
    )
    s.commit()

with SessionMaker() as s:
    c = s.get(Call, cid)
    b = s.get(Borrower, bid)
    print("Call Status:", c.status, "Termination Reason:", c.termination_reason)
    print("Borrower Status:", b.status)
    print("Carrier Call ID:", c.provider_call_id)
```

#### Actual Behavior & Evidence
```text
2026-11-24 21:16:54 [INFO] [call_state_machine]: Call state updated | call_id=... from_status=INITIATED to_status=ANSWERED state_rank=4
2026-11-24 21:16:54 [WARNING] [bridge_allocator]: Predictive bridge timeout: No available agent within answer timeout | timeout_sec=0.01
2026-11-24 21:16:54 [WARNING] [event_processor]: Predictive call abandoned: No agent available within answer timeout
2026-11-24 21:16:54 [INFO] [call_state_machine]: Call state updated | from_status=ANSWERED to_status=FAILED state_rank=6
Call Status: FAILED Termination Reason: ABANDONED
Borrower Status: ELIGIBLE
Carrier Call ID: prov-live-call-123
```
1. In `processor.py` line 186, when `BridgeAllocator` returns `None`, `CallStateMachine.transition(call, CallState.FAILED, termination_reason="ABANDONED")` is executed.
2. **`provider.terminate_call(call.provider_call_id)` is NEVER called.** The telecommunication carrier call `prov-live-call-123` remains connected on the PSTN network. The consumer is connected and hears dead air.
3. In lines 207–216, because `call.status == FAILED`, `borrower.status` is set to `BorrowerState.ELIGIBLE.value`.
4. While the consumer is actively connected to the carrier call, SmartDialer marks them `ELIGIBLE`. On the next dialer cycle, the dialer can place a second concurrent phone call to the same consumer.

#### Expected Behavior
1. When bridge timeout expires, `EventProcessor` must retrieve the provider via `provider_registry.get(provider_name)` and invoke `provider.terminate_call(call.provider_call_id)`.
2. The borrower must NOT be set to `ELIGIBLE` while the carrier call remains active. `borrower.status` must remain non-eligible (e.g. `FAILED` or a pending termination state) until the carrier confirms call teardown via `COMPLETED` webhook.

#### Recommended Fix
In `src/smartdialer/events/processor.py`:
```python
if agent:
    new_agent_state = agent.status
    if call.status == CallState.ANSWERED.value:
        CallStateMachine.transition(call, CallState.CONNECTED)
else:
    logger.warning("Predictive call abandoned: No agent available within answer timeout", call_id=str(call.id))
    CallStateMachine.transition(call, CallState.FAILED, termination_reason="ABANDONED")
    # Terminate call at telecom carrier immediately
    provider = provider_registry.get(provider_name)
    if provider and call.provider_call_id:
        provider.terminate_call(call.provider_call_id)
    # Do NOT release borrower to ELIGIBLE immediately while carrier connection lingers
    if borrower:
        borrower.status = BorrowerState.FAILED.value
```

---

### FINDING 2 (HIGH): Answering Calls Connected to Offline or Paused Agents

- **Severity:** HIGH (Call Integrity & Agent State Desynchronization)
- **Component:** `allocator/bridge.py` & `events/processor.py`
- **File & Line:** [`src/smartdialer/allocator/bridge.py#L49-L56`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/allocator/bridge.py#L49-L56) & [`src/smartdialer/events/processor.py#L187-L205`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/events/processor.py#L187-L205)
- **Violation:** Audit A: *"An answering call cannot be connected to an offline or paused agent."*

#### Reproduction Steps
1. Create a Call and link it to an Agent.
2. Advance the Agent to `DIALING`.
3. Transition the Agent to `OFFLINE` (simulating agent logout during ringing).
4. Deliver a carrier `CONNECTED` event via `EventProcessor.process_event`.

```python
with SessionMaker() as s:
    ag = s.get(Agent, aid)
    AgentStateMachine.transition(ag, AgentState.OFFLINE)
    s.commit()

with SessionMaker() as s:
    EventProcessor.process_event(
        session=s,
        provider_name="provider_a",
        event_id=f"evt-{uuid.uuid4()}",
        call_id=cid,
        event_type="CONNECTED",
    )
    s.commit()
```

#### Actual Behavior & Evidence
```text
2026-11-24 21:16:40 [INFO] [agent_state_machine]: Agent transitioned state | from_state=DIALING to_state=OFFLINE version=4
2026-11-24 21:16:40 [INFO] [call_state_machine]: Call state updated | from_status=INITIATED to_status=CONNECTED state_rank=5
2026-11-24 21:16:40 [INFO] [event_processor]: Provider event processed successfully | call_state=CONNECTED agent_state=None
VERIFICATION RESULTS:
Call status: CONNECTED
Agent status: OFFLINE
Agent current_call_id: None
```
- In `bridge.py` lines 49–56:
  ```python
  if call.agent_id is not None:
      stmt = select(Agent).where(Agent.id == call.agent_id).with_for_update()
      agent = session.execute(stmt).scalar_one_or_none()
      if agent:
          if agent.status in (AgentState.RESERVED.value, AgentState.DIALING.value):
              AgentStateMachine.transition(agent, AgentState.CONNECTED, current_call_id=call.id)
          return agent  # Returns the OFFLINE agent!
  ```
  If `agent.status == OFFLINE`, the transition is skipped, but `return agent` returns the offline agent object.
- In `processor.py` lines 187–205:
  ```python
  elif call.status in (CallState.ANSWERED.value, CallState.CONNECTED.value):
      if agent.status in (AgentState.RESERVED.value, AgentState.DIALING.value):
          AgentStateMachine.transition(agent, AgentState.CONNECTED, current_call_id=call.id)
  ```
  The call advances to `CONNECTED`, but the agent remains `OFFLINE`. The borrower is marked `IN_CALL`, but connected to an unstaffed line.

#### Expected Behavior
If an assigned agent becomes `OFFLINE` or `PAUSED` before the call is answered:
1. `BridgeAllocator` must reject the offline/paused agent.
2. The call must attempt to dynamically re-bridge to any available agent.
3. If no available agent can be found within the answer timeout, the call must be compliantly abandoned and terminated at the carrier. Under no circumstances may a call enter `CONNECTED` status when its assigned agent is `OFFLINE` or `PAUSED`.

#### Recommended Fix
In `src/smartdialer/allocator/bridge.py`:
```python
if call.agent_id is not None:
    stmt = select(Agent).where(Agent.id == call.agent_id).with_for_update()
    agent = session.execute(stmt).scalar_one_or_none()
    if agent and agent.status in (AgentState.RESERVED.value, AgentState.DIALING.value):
        AgentStateMachine.transition(agent, AgentState.CONNECTED, current_call_id=call.id)
        return agent
    # Agent was lost (OFFLINE/PAUSED/DISCONNECTED) -> detach and attempt dynamic bridge
    call.agent_id = None
```

---

### FINDING 3 (HIGH): Absence of Lease Heartbeat Causes Sweeper Preemption on Slow Carrier Calls

- **Severity:** HIGH (Worker Starvation & Premature Resource Theft)
- **Component:** `recovery/outbox_dispatcher.py` & `recovery/reconciler.py`
- **File & Line:** [`src/smartdialer/recovery/outbox_dispatcher.py#L241-L253`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/recovery/outbox_dispatcher.py#L241-L253) & [`src/smartdialer/recovery/reconciler.py#L86-L92`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/recovery/reconciler.py#L86-L92)
- **Violation:** Audit D: *"Whether leases are renewed during slow provider requests. Explicitly test a provider request that lasts longer than the original lease duration and longer than 30 seconds."*

#### Reproduction Steps
1. Create a Call (`created_at = now - 35s`) and OutboxEvent with an initial 20-second lease that expired 15s ago (`locked_until = now - 15s`), representing an active worker blocked on a 35-second carrier dispatch.
2. Execute `ReconciliationSweeper.run_sweep(session)`.

```python
with SessionMaker() as s:
    report = ReconciliationSweeper.run_sweep(s)
    s.commit()
```

#### Actual Behavior & Evidence
```text
2026-11-24 21:18:12 [INFO] [call_state_machine]: Call state updated | call_id=... from_status=RESERVED to_status=FAILED state_rank=6
2026-11-24 21:18:12 [INFO] [agent_state_machine]: Agent transitioned state | from_state=RESERVED to_state=AVAILABLE version=2
2026-11-24 21:18:12 [WARNING] [reconciler]: Reclaimed agent from stale RESERVED call | agent_id=... call_id=...
SWEEPER PREEMPTION RESULTS:
Sweeper report timed out calls: 1
Sweeper report reclaimed agents: 1
Call status: FAILED Termination Reason: RESERVATION_TIMEOUT_EXPIRED
Agent status: AVAILABLE
Borrower status: ELIGIBLE
OutboxEvent status: FAILED
```
- In `outbox_dispatcher.py`, line 241 sets `event.locked_until = now + 20s`, and line 246 calls `provider.initiate_call()` synchronously on the current thread. **There is no background heartbeat thread or lease renewal.**
- If carrier dispatch takes $>20\text{s}$, the lease expires.
- If the dispatch takes $>30\text{s}$, `ReconciliationSweeper` inspects line 86:
  `if outbox_event and outbox_event.locked_until and outbox_event.locked_until > now:`
  Because `locked_until < now`, this check evaluates to `False`.
- The sweeper cancels the call (`FAILED`), marks the outbox event `FAILED`, reclaims the agent to `AVAILABLE`, and reclaims the borrower to `ELIGIBLE`.
- When the worker's carrier call finishes at $t=35\text{s}$, the agent and borrower have already been re-allocated to a second call.

#### Expected Behavior
1. Workers executing in-flight dispatches must maintain an active lease heartbeat (renewing `event.locked_until` periodically during long network calls).
2. The sweeper must never preempt an outbox event while a worker holds an active heartbeat.

#### Recommended Fix
In `src/smartdialer/recovery/outbox_dispatcher.py`:
Implement a background heartbeat thread that renews `OutboxEvent.locked_until` every 5 seconds during `provider.initiate_call()`.

---

### FINDING 4 (MEDIUM): Permanent Ledger Poisoning on Crashed Worker in PENDING State

- **Severity:** MEDIUM (Idempotency Liveness & Recovery Lockup)
- **Component:** `providers/provider_a.py` & `providers/provider_b.py`
- **File & Line:** [`src/smartdialer/providers/provider_a.py#L137-L164`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/providers/provider_a.py#L137-L164) & [`src/smartdialer/providers/provider_b.py#L139-L166`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/providers/provider_b.py#L139-L166)
- **Violation:** Audit B: *"A PENDING ledger entry survives a worker crash."*

#### Reproduction Steps
1. Insert a row into `ProviderRequestLedger` with `status = 'PENDING'` (simulating Worker 1 writing intent and crashing).
2. Worker 2 attempts to dispatch the call with the same idempotency key using `provider.initiate_call()`.

#### Actual Behavior & Evidence
```text
Worker 2 initiate_call result:
success: False
error: CONCURRENT_DISPATCH_TIMEOUT: in-flight carrier dispatch did not complete within timeout.
```
- When a worker registers intent (`status = 'PENDING'`) and crashes, `ProviderRequestLedger` retains `status = 'PENDING'` indefinitely.
- Retrying workers check `existing.status == 'PENDING'` and sleep in a 2-second loop waiting for the row to become `DISPATCHED`.
- Because the crashed worker is dead, the row never transitions.
- Every retry fails with `CONCURRENT_DISPATCH_TIMEOUT`. After 3 retries, `OutboxDispatcher` marks the outbox event `FAILED`.
- The idempotency key is permanently poisoned.

#### Expected Behavior
`ProviderRequestLedger` must track an expiration or timestamp on `PENDING` entries. If a `PENDING` entry is older than a crash cutoff (e.g. 30 seconds), a retrying worker must reconcile the entry by querying the carrier API or taking ownership of the dispatch attempt.

#### Recommended Fix
In `src/smartdialer/providers/provider_a.py` and `provider_b.py`:
```python
elif existing and existing.status == "PENDING":
    age_seconds = (datetime.now(timezone.utc) - existing.created_at).total_seconds()
    if age_seconds > 30.0:
        logger.warning("Found orphaned PENDING ledger entry from crashed worker; claiming recovery", key=idempotency_key)
        # Attempt recovery or overwrite stale PENDING
```

---

### FINDING 5 (MEDIUM): Unhandled Exceptions & Lack of Retries on Failed Provider Call Termination

- **Severity:** MEDIUM (Rogue Call Leak on PSTN)
- **Component:** `recovery/outbox_dispatcher.py`
- **File & Line:** [`src/smartdialer/recovery/outbox_dispatcher.py#L276-L280`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/recovery/outbox_dispatcher.py#L276-L280), [`L295-L298`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/recovery/outbox_dispatcher.py#L295-L298), [`L378-L380`](file:///c:/Users/shiva/Downloads/website/assignment/src/smartdialer/recovery/outbox_dispatcher.py#L378-L380)
- **Violation:** Audit C: *"Also inspect what happens if provider termination fails after a rogue call has been placed."*

#### Actual Behavior & Evidence
In `outbox_dispatcher.py`:
```python
if res.success and res.provider_call_id:
    # Terminate rogue carrier call to avoid ghost call to borrower
    provider.terminate_call(res.provider_call_id)
session.rollback()
return None
```
1. `terminate_call()` returns a boolean (`bool`). The return value is completely ignored. If the carrier returns `False` (e.g. carrier API reject or call already answered), no warning is escalated and no record is preserved.
2. If `provider.terminate_call()` raises an unhandled network exception (e.g. HTTP timeout or 500 error), it is uncaught in `_dispatch_single_record()`, causing the worker loop to crash without rolling back cleanly.

#### Expected Behavior
Carrier termination must be wrapped in a robust try-except with retries or recorded in an outbox compensation table for guaranteed cancellation.

---

## 4. Audit Areas Deep Dive

### Audit A — Predictive Bridge Correctness

- **Decoupled Predictive Dispatch:** Verified. `PredictiveDialer.execute_cycle()` dispatches calls with `agent_id = None` and `dialing_mode = 'PREDICTIVE'`. `CallAllocator.allocate_call()` allows `agent = None` when `dialing_mode == 'PREDICTIVE'`.
- **Simultaneous Answer Concurrency:** Verified. In `BridgeAllocator.bridge_call_to_agent()`, agent selection executes `SELECT ... FROM agents WHERE status = 'AVAILABLE' ORDER BY ... LIMIT 1 FOR UPDATE SKIP LOCKED`. Concurrent answered calls cannot claim the same agent.
- **Offline / Paused Agent Protection:** **FAILED.** (Finding 2). If an agent assigned to a progressive call goes `OFFLINE` or `PAUSED`, the bridge returns the offline agent and `EventProcessor` marks the call `CONNECTED`.
- **Transactional Consistency:** **PARTIALLY FAILED.** Database call state advances to `CONNECTED` while the agent remains `OFFLINE` with `current_call_id = None`.
- **Failed Bridge Recovery:** **FAILED.** (Finding 1). When bridge times out, `EventProcessor` transitions the call to `FAILED` (`ABANDONED`), but never hangs up the carrier call and immediately marks the borrower `ELIGIBLE`.

### Audit B — Provider Idempotency & Crash Recovery

Simulations performed:
1. **Provider accepts call, application crashes before updating ledger:** Intent is committed in autonomous session as `PENDING`. On crash, row remains `PENDING`.
2. **Database disconnects immediately after provider acceptance:** Handled similarly: row remains `PENDING`.
3. **Two workers submit same idempotency key:** Handled by PostgreSQL composite unique constraint `uq_provider_idempotency_key` on `(provider_name, idempotency_key)`. Second worker catches `IntegrityError` and awaits `DISPATCHED` status.
4. **PENDING ledger entry survives worker crash:** **FAILED.** (Finding 4). Retrying workers poll for 2.0s, hit `CONCURRENT_DISPATCH_TIMEOUT`, and exhaust retries.
5. **UNCERTAIN outcome reconciled:** **FAILED.** No reconciliation path exists. Entries marked `UNCERTAIN` permanently reject retries with `PREVIOUS_DISPATCH_UNCERTAIN`.
6. **Provider does not support idempotency:** Autonomous database transactions cannot guarantee external exactly-once execution if the provider has no native deduplication and crashes occur during receipt transmission.

### Audit C — Dispatch Fencing & Stale Workers

- **Fencing Protection:** Verified. When an outbox event is claimed, `dispatch_token = uuid.uuid4()` is assigned to `call.dispatch_token` and `agent.dispatch_token`.
- **Stale Worker Modification:** Verified. If Worker A returns late after Worker B reclaims the event, Worker A checks `call.dispatch_token != dispatch_token` or `call.status != RESERVED`. Worker A rolls back and does not overwrite `call`, `agent`, or `borrower`.
- **Provider Termination on Stale Write:** **PARTIALLY VERIFIED / FAILED.** (Finding 5). Termination is attempted, but return value is ignored and network exceptions are unhandled.

### Audit D — Lease Heartbeat & Recovery

- **Lease Renewal / Heartbeat:** **DOES NOT EXIST.**
- **Sweeper Preemption on Slow Calls:** **FAILED.** (Finding 3). A carrier call exceeding 20s loses its lease. At 30s, the sweeper marks the call `FAILED` and steals the agent, even though the worker is still actively communicating with the carrier.
- **Atomic Lease Check:** Verified during claim via `SELECT ... FOR UPDATE SKIP LOCKED`.
- **Worker Ownership Check:** Verified via `dispatch_token`.

### Audit E — Reservation Cleanup

- **Progressive Cleanup:** Verified. `try...finally` block in `progressive.py` releases unallocated agents (`set(claimed_agent_ids) - allocated_agent_ids`) from `RESERVED` to `AVAILABLE`.
- **Predictive Cleanup:** Verified. Identical `finally` block in `predictive.py`.
- **Idempotency & Isolation:** Verified. Cleanup checks `ag.status == AgentState.RESERVED.value`.
- **Stranded Agent Prevention:** Verified. In `allocator.py`, if borrower reservation fails, agent is reverted before re-raising.

### Audit F — PostgreSQL Test Credibility

- **Database Engine:** PostgreSQL 18.1 (x86_64-windows, 64-bit).
- **Test Database:** `smartdialer_test`.
- **Session Isolation:** `conftest.py` uses independent database connections via psycopg 3.
- **Concurrency Overlap:** Multithreaded test cases in `test_allocation_concurrency.py`, `test_fix_predictive_bridge.py`, and `test_fix_durable_idempotency.py` successfully produce real PostgreSQL row lock contention.
- **Test Suite Credibility & Gaps:**  
  While 69/69 tests pass, they miss critical distributed failure modes:
  1. Tests mock carrier calls with 5–15ms sleeps; no test exercises a carrier call lasting $>20\text{s}$ or $>30\text{s}$.
  2. Abandonment tests assert `call.status == FAILED`, but never verify whether `provider.terminate_call()` was executed or whether `borrower.status` was released prematurely.
  3. Sweeper lease tests artificially backdate `created_at` while keeping `locked_until` in the future, disguising the lack of a lease heartbeat.

---

## 5. Required Actions for Stage 8 Approval

Before Stage 8 development can be authorized, the engineering team must resolve the following mandatory items:

1. **Carrier Teardown on Call Abandonment:**
   In `src/smartdialer/events/processor.py`, immediately invoke `provider.terminate_call(call.provider_call_id)` when `BridgeAllocator` returns `None`.
2. **Borrower State Retention on Abandonment:**
   Do NOT set `borrower.status = ELIGIBLE` when an abandonment occurs until carrier confirms call hang-up.
3. **Prevent Offline Agent Bridge Connections:**
   In `src/smartdialer/allocator/bridge.py` and `events/processor.py`, ensure that if an assigned agent is `OFFLINE` or `PAUSED`, the call dynamically re-bridges to an available agent or aborts compliantly.
4. **Implement Outbox Lease Heartbeat:**
   In `src/smartdialer/recovery/outbox_dispatcher.py`, implement a background heartbeat loop that renews `OutboxEvent.locked_until` during active carrier network dispatches.
5. **Reconcile Stale PENDING Ledger Entries:**
   In `ProviderA` and `ProviderB`, add age-based recovery for `PENDING` ledger entries older than 30 seconds.

---

**Auditor Sign-off:**  
*Independent Principal Distributed Systems Engineer & Telecom Safety Auditor*  
*November 24, 2026*
