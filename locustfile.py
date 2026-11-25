"""
Locust Load Testing Suite for SmartDialer.

Simulates concurrent workers triggering dialing cycles, ingesting telecom provider
webhooks (including duplicate & out-of-order callbacks), churning agent states,
and polling campaign telemetry under load.

Usage:
    uvicorn smartdialer.main:app --host 127.0.0.1 --port 8000
    locust -f locustfile.py --host http://127.0.0.1:8000 --headless -u 10 -r 2 -t 30s
"""

import random
import uuid
from locust import HttpUser, between, task


class SmartDialerWorkerUser(HttpUser):
    """Simulates a concurrent dialer worker & carrier webhook stream."""

    wait_time = between(0.2, 0.8)

    def on_start(self) -> None:
        resp = self.client.post(
            "/api/v1/campaigns",
            json={
                "name": f"LoadTest-{uuid.uuid4().hex[:6]}",
                "mode": "PREDICTIVE",
                "max_active_calls": 100,
            },
        )
        if resp.status_code in (200, 201):
            self.campaign_id = resp.json()["id"]
        else:
            self.campaign_id = None
            return

        # Seed 20 available agents and 100 borrowers
        agents_payload = {
            "agents": [{"name": f"LoadAgent-{i}", "status": "AVAILABLE"} for i in range(20)]
        }
        self.client.post(f"/api/v1/campaigns/{self.campaign_id}/agents", json=agents_payload)

        borrowers_payload = {
            "borrowers": [
                {
                    "name": f"LoadBorrower-{i}",
                    "phone_number": f"+1555{random.randint(1000000, 9999999)}",
                    "priority": i % 5,
                }
                for i in range(100)
            ]
        }
        self.client.post(f"/api/v1/campaigns/{self.campaign_id}/borrowers", json=borrowers_payload)

    @task(4)
    def dial_and_process_webhooks(self) -> None:
        if not self.campaign_id:
            return

        dial_resp = self.client.post(
            f"/api/v1/campaigns/{self.campaign_id}/dial",
            json={"provider_name": "provider_a", "dispatch_immediately": True},
        )
        if dial_resp.status_code != 200:
            return

        data = dial_resp.json()
        for call_id in data.get("dispatched_call_ids", []):
            evt_ring = f"load-ring-{uuid.uuid4().hex[:10]}"
            self.client.post(
                "/api/v1/webhooks/provider/provider_a",
                json={"event_id": evt_ring, "call_id": call_id, "event_type": "RINGING"},
            )
            if random.random() < 0.45:
                evt_ans = f"load-ans-{uuid.uuid4().hex[:10]}"
                self.client.post(
                    "/api/v1/webhooks/provider/provider_a",
                    json={
                        "event_id": evt_ans,
                        "call_id": call_id,
                        "event_type": "ANSWERED",
                        "payload": {"answer_timeout_sec": 0.05},
                    },
                )
                # Deliberately inject duplicate ANSWERED webhook 20% of the time
                if random.random() < 0.20:
                    self.client.post(
                        "/api/v1/webhooks/provider/provider_a",
                        json={"event_id": evt_ans, "call_id": call_id, "event_type": "ANSWERED"},
                    )
                self.client.post(
                    "/api/v1/webhooks/provider/provider_a",
                    json={
                        "event_id": f"load-comp-{uuid.uuid4().hex[:10]}",
                        "call_id": call_id,
                        "event_type": "COMPLETED",
                        "payload": {"duration_seconds": random.uniform(45.0, 150.0)},
                    },
                )
            else:
                self.client.post(
                    "/api/v1/webhooks/provider/provider_a",
                    json={"event_id": f"load-fail-{uuid.uuid4().hex[:10]}", "call_id": call_id, "event_type": "FAILED"},
                )

    @task(2)
    def inspect_telemetry_and_safety(self) -> None:
        if not self.campaign_id:
            return
        self.client.get(f"/api/v1/campaigns/{self.campaign_id}")
        self.client.get(f"/api/v1/campaigns/{self.campaign_id}/safety-logs?limit=10")

    @task(1)
    def agent_wrap_up_and_pool_replenish(self) -> None:
        if not self.campaign_id:
            return
        ag_resp = self.client.get(f"/api/v1/campaigns/{self.campaign_id}/agents")
        if ag_resp.status_code == 200:
            for ag in ag_resp.json():
                if ag["status"] == "WRAP_UP":
                    self.client.post(
                        f"/api/v1/agents/{ag['id']}/transition",
                        json={"target_state": "AVAILABLE"},
                    )
        # Keep borrower pool topped up
        borrowers_payload = {
            "borrowers": [
                {
                    "name": f"Refill-{uuid.uuid4().hex[:6]}",
                    "phone_number": f"+1555{random.randint(1000000, 9999999)}",
                    "priority": 1,
                }
                for _ in range(10)
            ]
        }
        self.client.post(f"/api/v1/campaigns/{self.campaign_id}/borrowers", json=borrowers_payload)
