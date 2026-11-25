"""
STAGE 8 INTEGRATION TEST SUITE:
Tests FastAPI REST API, Webhook Ingestion, WebSocket Streaming, Operations Dashboard,
Provider Circuit Breaker Controls, and Predictive Simulation Scenarios (A-D) against PostgreSQL 18.
"""

import uuid
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker

from smartdialer.api.deps import get_db
from smartdialer.main import create_app
from smartdialer.models.enums import AgentState, CallState
from smartdialer.providers.registry import provider_registry
from smartdialer.safety.provider_health import provider_health_tracker


@pytest.fixture
def api_client(sync_test_engine):
    """Create a FastAPI TestClient wired to the PostgreSQL test database."""
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    app = create_app(enable_worker=False, run_startup_migrations=False)

    def override_get_db():
        session = SessionMaker()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db
    provider_registry.get("provider_a").failure_rate = 0.0
    provider_registry.get("provider_b").failure_rate = 0.0
    with SessionMaker() as s:
        provider_health_tracker.reset(session=s)
        s.commit()

    with TestClient(app) as client:
        yield client


def test_health_and_dashboard_served(api_client: TestClient):
    h_resp = api_client.get("/health")
    assert h_resp.status_code == 200
    h_data = h_resp.json()
    assert h_data["status"] == "ok"
    assert h_data["database"] == "connected"
    assert "provider_a" in h_data["providers"]

    ui_resp = api_client.get("/")
    assert ui_resp.status_code == 200
    assert "SmartDialer" in ui_resp.text
    assert "Safety Controller" in ui_resp.text


def test_progressive_campaign_rest_and_webhook_lifecycle(api_client: TestClient):
    # 1. Create PROGRESSIVE campaign
    c_resp = api_client.post(
        "/api/v1/campaigns",
        json={"name": "Stage8 Progressive API", "mode": "PROGRESSIVE", "max_active_calls": 20},
    )
    assert c_resp.status_code == 201
    campaign_id = c_resp.json()["id"]

    # 2. Seed 2 available agents and 2 borrowers
    a_resp = api_client.post(
        f"/api/v1/campaigns/{campaign_id}/agents",
        json={"agents": [{"name": "API-Agent-1"}, {"name": "API-Agent-2"}]},
    )
    assert a_resp.status_code == 201
    agents = a_resp.json()
    assert len(agents) == 2

    b_resp = api_client.post(
        f"/api/v1/campaigns/{campaign_id}/borrowers",
        json={
            "borrowers": [
                {"name": "API-Borrower-1", "phone_number": "+15552001001", "priority": 2},
                {"name": "API-Borrower-2", "phone_number": "+15552001002", "priority": 1},
            ]
        },
    )
    assert b_resp.status_code == 201

    # 3. Trigger Progressive Dial Cycle
    d_resp = api_client.post(
        f"/api/v1/campaigns/{campaign_id}/dial",
        json={"provider_name": "provider_a", "dispatch_immediately": True},
    )
    assert d_resp.status_code == 200
    d_data = d_resp.json()
    assert d_data["decision"] == "APPROVED"
    assert d_data["approved_calls"] == 2
    assert d_data["allocated_calls"] == 2
    assert set(d_data["allocated_call_ids"]).issubset(set(d_data["dispatched_call_ids"]))
    call_id = d_data["allocated_call_ids"][0]

    # 4. Ingest webhooks: RINGING -> ANSWERED -> Duplicate ANSWERED -> COMPLETED -> Late RINGING
    ans_evt_id = f"stage8-ans-{uuid.uuid4().hex[:8]}"
    r1 = api_client.post(
        "/api/v1/webhooks/provider/provider_a",
        json={"event_id": f"stage8-ring-{uuid.uuid4().hex[:8]}", "call_id": call_id, "event_type": "RINGING"},
    )
    assert r1.status_code == 200
    assert r1.json()["new_call_state"] == CallState.RINGING.value

    r2 = api_client.post(
        "/api/v1/webhooks/provider/provider_a",
        json={"event_id": ans_evt_id, "call_id": call_id, "event_type": "ANSWERED"},
    )
    assert r2.status_code == 200
    assert r2.json()["new_call_state"] == CallState.ANSWERED.value
    assert r2.json()["new_agent_state"] == AgentState.CONNECTED.value

    # Duplicate ANSWERED webhook
    r_dup = api_client.post(
        "/api/v1/webhooks/provider/provider_a",
        json={"event_id": ans_evt_id, "call_id": call_id, "event_type": "ANSWERED"},
    )
    assert r_dup.status_code == 200
    assert r_dup.json()["is_duplicate"] is True

    # CONNECTED webhook
    r_conn = api_client.post(
        "/api/v1/webhooks/provider/provider_a",
        json={"event_id": f"stage8-conn-{uuid.uuid4().hex[:8]}", "call_id": call_id, "event_type": "CONNECTED"},
    )
    assert r_conn.status_code == 200
    assert r_conn.json()["new_call_state"] == CallState.CONNECTED.value

    # COMPLETED webhook
    r3 = api_client.post(
        "/api/v1/webhooks/provider/provider_a",
        json={
            "event_id": f"stage8-comp-{uuid.uuid4().hex[:8]}",
            "call_id": call_id,
            "event_type": "COMPLETED",
            "payload": {"duration_seconds": 72.5},
        },
    )
    assert r3.status_code == 200
    assert r3.json()["new_call_state"] == CallState.COMPLETED.value
    assert r3.json()["new_agent_state"] == AgentState.WRAP_UP.value

    # Out-of-order late ANSWERED webhook cannot regress COMPLETED
    r_late = api_client.post(
        "/api/v1/webhooks/provider/provider_a",
        json={"event_id": f"stage8-late-{uuid.uuid4().hex[:8]}", "call_id": call_id, "event_type": "ANSWERED"},
    )
    assert r_late.status_code == 200
    assert r_late.json()["is_out_of_order"] is True
    assert r_late.json()["new_call_state"] == CallState.COMPLETED.value

    # 5. Verify SafetyDecisionLog audit endpoint & Campaign detail metrics
    logs_resp = api_client.get(f"/api/v1/campaigns/{campaign_id}/safety-logs")
    assert logs_resp.status_code == 200
    assert len(logs_resp.json()) >= 1

    detail_resp = api_client.get(f"/api/v1/campaigns/{campaign_id}")
    assert detail_resp.status_code == 200
    assert detail_resp.json()["call_counts"].get("COMPLETED", 0) == 1

    # 6. Verify enriched call & agent monitoring payloads
    calls_resp = api_client.get(f"/api/v1/campaigns/{campaign_id}/calls")
    assert calls_resp.status_code == 200
    completed_call = next(c for c in calls_resp.json() if c["id"] == call_id)
    assert completed_call["agent_name"] in ("API-Agent-1", "API-Agent-2")
    assert completed_call["borrower_name"] == "API-Borrower-1"
    assert completed_call["borrower_phone"] == "+15552001001"

    agents_resp = api_client.get(f"/api/v1/campaigns/{campaign_id}/agents")
    assert agents_resp.status_code == 200
    handled_agents = [a for a in agents_resp.json() if a["completed_calls"] == 1]
    assert len(handled_agents) == 1
    assert handled_agents[0]["avg_talk_duration_sec"] == 72.5
    assert handled_agents[0]["status"] == AgentState.WRAP_UP.value
    assert handled_agents[0]["last_completed_call_id"] == call_id

    # 7. Save wrap-up call notes and release agent back to AVAILABLE
    notes_resp = api_client.post(
        f"/api/v1/calls/{call_id}/notes",
        json={
            "disposition": "Promise to Pay",
            "call_notes": "Customer agreed to clear overdue balance on Friday.",
            "complete_wrap_up": True,
        },
    )
    assert notes_resp.status_code == 200
    n_data = notes_resp.json()
    assert n_data["disposition"] == "Promise to Pay"
    assert n_data["call_notes"] == "Customer agreed to clear overdue balance on Friday."

    agents_after = api_client.get(f"/api/v1/campaigns/{campaign_id}/agents").json()
    released = next(a for a in agents_after if a["id"] == handled_agents[0]["id"])
    assert released["status"] == AgentState.AVAILABLE.value


def test_agent_state_transition_validation_and_provider_circuit_breaker(api_client: TestClient):
    c_resp = api_client.post("/api/v1/campaigns", json={"name": "Circuit Breaker API", "mode": "PREDICTIVE"})
    cid = c_resp.json()["id"]
    a_resp = api_client.post(
        f"/api/v1/campaigns/{cid}/agents",
        json={"agents": [{"name": "CB-Agent-1", "status": "OFFLINE"}]},
    )
    agent_id = a_resp.json()[0]["id"]

    # Invalid transition OFFLINE -> CONNECTED must return 400
    bad_tr = api_client.post(f"/api/v1/agents/{agent_id}/transition", json={"target_state": "CONNECTED"})
    assert bad_tr.status_code == 400

    # Valid transition OFFLINE -> AVAILABLE
    ok_tr = api_client.post(f"/api/v1/agents/{agent_id}/transition", json={"target_state": "AVAILABLE"})
    assert ok_tr.status_code == 200
    assert ok_tr.json()["status"] == "AVAILABLE"

    api_client.post(
        f"/api/v1/campaigns/{cid}/borrowers",
        json={"borrowers": [{"name": "CB-Borrower", "phone_number": "+15553001001"}]},
    )

    # Trip provider_a outage -> SafetyController must reject/block predictive dial
    outage_resp = api_client.post("/api/v1/providers/provider_a/health", json={"degraded": True})
    assert outage_resp.status_code == 200
    assert outage_resp.json()["is_healthy"] is False

    dial_resp = api_client.post(f"/api/v1/campaigns/{cid}/dial", json={"provider_name": "provider_a"})
    assert dial_resp.status_code == 200
    assert dial_resp.json()["decision"] in ("REJECTED", "FALLBACK_TO_PROGRESSIVE")
    assert dial_resp.json()["approved_calls"] == 0

    # Restore health
    api_client.post("/api/v1/providers/provider_a/health", json={"degraded": False})


def test_websocket_receives_live_dial_and_webhook_events(api_client: TestClient):
    c_resp = api_client.post("/api/v1/campaigns", json={"name": "WS Live Campaign", "mode": "PROGRESSIVE"})
    cid = c_resp.json()["id"]
    api_client.post(f"/api/v1/campaigns/{cid}/agents", json={"agents": [{"name": "WS-Agent"}]})
    api_client.post(
        f"/api/v1/campaigns/{cid}/borrowers",
        json={"borrowers": [{"name": "WS-Borrower", "phone_number": "+15554001001"}]},
    )

    with api_client.websocket_connect(f"/ws/campaigns/{cid}") as ws:
        ws.send_text("ping")
        assert ws.receive_text() == "pong"

        d_resp = api_client.post(
            f"/api/v1/campaigns/{cid}/dial",
            json={"provider_name": "provider_a", "dispatch_immediately": True},
        )
        assert d_resp.status_code == 200
        call_id = d_resp.json()["dispatched_call_ids"][0]

        evt_msg = ws.receive_json()
        assert evt_msg["event_type"] == "dial_cycle"
        assert evt_msg["campaign_id"] == cid
        assert evt_msg["data"]["approved"] == 1

        api_client.post(
            "/api/v1/webhooks/provider/provider_a",
            json={"event_id": f"ws-evt-{uuid.uuid4().hex[:8]}", "call_id": call_id, "event_type": "RINGING"},
        )
        wh_msg = ws.receive_json()
        assert wh_msg["event_type"] == "webhook_event"
        assert wh_msg["data"]["new_call_state"] == "RINGING"


def test_simulation_endpoint_executes_scenarios_a_through_d(api_client: TestClient):
    sim_resp = api_client.post(
        "/api/v1/simulations/run",
        json={"scenario": "ALL", "num_agents": 10, "num_borrowers": 40, "cycles": 2, "provider_name": "provider_a"},
    )
    assert sim_resp.status_code == 200
    data = sim_resp.json()

    scenarios = {s["scenario"]: s for s in data["scenarios"]}
    assert set(scenarios.keys()) == {"A", "B", "C", "D"}
    for key, metrics in scenarios.items():
        assert metrics["calls_initiated"] >= 0
        assert len(metrics["safety_decisions"]) >= 1
        assert len(metrics["pacing_decisions"]) >= 1

    demos = data["failure_demos"]
    assert demos["worker_crash_recovery"]["call_status"] == "FAILED"
    assert demos["worker_crash_recovery"]["agent_status"] == "AVAILABLE"
    assert demos["duplicate_and_out_of_order_events"]["duplicate_acknowledged_safely"] is True
    assert demos["duplicate_and_out_of_order_events"]["final_call_status"] == "COMPLETED"
    assert demos["duplicate_and_out_of_order_events"]["late_ringing_blocked"] is True
