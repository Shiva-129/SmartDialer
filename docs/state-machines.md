# SmartDialer State Machines & Progression Mechanics

SmartDialer implements two independent but strictly coordinated finite state machines:
1. **Agent State Machine**
2. **Call State Machine**

Both state machines are deterministic, validated, and resilient against out-of-order, delayed, and duplicate events.

---

## 1. Agent State Machine

### 1.1 States
- `OFFLINE`: Agent is disconnected from the dialer.
- `AVAILABLE`: Agent is logged in, idle, and eligible to receive outbound or inbound calls.
- `RESERVED`: Agent is temporarily locked for an impending outbound call.
- `DIALING`: Provider has initiated dialing on the carrier network; waiting for answer.
- `CONNECTED`: Call is answered and bridged; audio session is active.
- `WRAP_UP`: Call has terminated; agent is completing disposition, notes, or post-call work.
- `PAUSED`: Agent is on break or temporary administrative pause; unavailable for allocation.

### 1.2 Transition Matrix

| Source State | Target State | Trigger / Cause | Side Effects |
| :--- | :--- | :--- | :--- |
| `OFFLINE` | `AVAILABLE` | Agent logs into campaign. | Clears reservation tokens; sets `current_call_id = None`. |
| `AVAILABLE` | `RESERVED` | Safety Controller / Allocator claims agent. | Sets `reservation_token = UUID`, sets `reserved_at = NOW()`. |
| `AVAILABLE` | `CONNECTED` | Bridge Allocator bridges answering predictive call. | Sets `current_call_id = call.id`. |
| `AVAILABLE` | `PAUSED` | Agent goes on break. | Disallows call allocations. |
| `AVAILABLE` | `OFFLINE` | Agent logs off. | Cleans up session. |
| `RESERVED` | `DIALING` | Outbox dispatcher initiates call to carrier. | Sets `current_call_id = call.id`. |
| `RESERVED` | `CONNECTED` | Immediate bridge upon carrier answer webhook. | Sets `current_call_id = call.id`. |
| `RESERVED` | `AVAILABLE` | Allocation cancelled or stale reservation reclaimed. | Clears `reservation_token` and `reserved_at`. |
| `RESERVED` | `OFFLINE` | Emergency disconnect during dispatch. | Sweeper will fail the un-dispatched call. |
| `DIALING` | `CONNECTED` | Telecom carrier delivers `ANSWERED`/`CONNECTED`. | Bridges agent to borrower audio. |
| `DIALING` | `WRAP_UP` | Call failed, busy, or no-answer. | Sets `last_call_ended_at = NOW()`, clears token. |
| `CONNECTED` | `WRAP_UP` | Call hung up by agent, borrower, or network. | Sets `last_call_ended_at = NOW()`. |
| `WRAP_UP` | `AVAILABLE` | Disposition submitted or 60s timeout expires. | Resets agent to pool; clears `current_call_id`. |
| `WRAP_UP` | `OFFLINE` | Agent logs off during wrap-up. | Cleans up session. |
| `PAUSED` | `AVAILABLE` | Agent unpauses. | Re-enters available pool. |
| `PAUSED` | `OFFLINE` | Agent logs off from paused state. | Cleans up session. |

---

## 2. Call State Machine & Monotonic Progression

### 2.1 States & Monotonic State Rank

To handle network reordering (e.g. carrier delivering `COMPLETED` before `ANSWERED`), each state is assigned an integer rank:

```python
CALL_STATE_RANK = {
    CallState.QUEUED: 0,
    CallState.RESERVED: 1,
    CallState.INITIATED: 2,
    CallState.RINGING: 3,
    CallState.ANSWERED: 4,
    CallState.CONNECTED: 5,
    CallState.COMPLETED: 6,
    CallState.FAILED: 6,
    CallState.CANCELLED: 6,
}
```

States with rank 6 (`COMPLETED`, `FAILED`, `CANCELLED`) are **Terminal States**.

### 2.2 Reordering and Out-of-Order Rules

1. **Terminal Immutability:** Once a call enters a terminal state (`COMPLETED`, `FAILED`, `CANCELLED`), **no future event may ever alter or regress its status**. Late callbacks (`ANSWERED`, `RINGING`) arriving seconds or minutes after call completion are acknowledged idempotently as out-of-order events without modifying the Call record.
2. **Monotonic Forward Progression:** An incoming event with rank $R_{\text{target}} \le R_{\text{current}}$ is recognized as a duplicate ($R_{\text{target}} = R_{\text{current}}$) or delayed out-of-order event ($R_{\text{target}} < R_{\text{current}}$) and safely skipped.
3. **Resilience to Dropped Carrier Webhooks:** Real-world telecom carriers frequently drop intermediate webhooks:
   - `INITIATED -> ANSWERED` (quick pickup before carrier sends ringing).
   - `INITIATED -> CONNECTED` (direct bridge callback without ringing or answered).
   - `INITIATED -> COMPLETED` (immediate borrower rejection/busy signal).
   - `RINGING -> CONNECTED` (carrier skipped separate answered webhook).

All of these forward jumps are explicitly permitted by `CallStateMachine.VALID_TRANSITIONS`.

---

## 3. Database Event Deduplication

Telecom webhooks are guaranteed to deliver duplicates due to network retries. SmartDialer enforces deduplication at the PostgreSQL engine level:

```sql
CREATE TABLE provider_events (
    id UUID PRIMARY KEY,
    provider_name VARCHAR(50) NOT NULL,
    event_id VARCHAR(100) NOT NULL,
    call_id UUID,
    event_type VARCHAR(50) NOT NULL,
    raw_payload JSONB NOT NULL,
    received_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    CONSTRAINT uq_provider_name_event_id UNIQUE (provider_name, event_id)
);
```

When an event arrives in `EventProcessor.process_event`, it attempts insertion inside a database savepoint (`session.begin_nested()`). If a duplicate `event_id` is received, PostgreSQL raises a unique constraint violation, which the application catches, logs, and safely returns `is_duplicate=True` without failing the outer transaction.
