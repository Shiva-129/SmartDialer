"""Tests for TwilioProvider REST adapter, PostgreSQL ledger idempotency, and form webhook callback."""

import uuid
import httpx
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker

from smartdialer.api.deps import get_db
from smartdialer.main import create_app
from smartdialer.models import Agent, AgentState, Borrower, Call, CallState, Campaign, CampaignMode, ProviderRequestLedger
from smartdialer.providers.twilio_provider import TwilioProvider
from smartdialer.safety.provider_health import provider_health_tracker


def test_twilio_provider_unconfigured_fails_closed(sync_test_engine):
    provider = TwilioProvider(account_sid=None, auth_token=None, from_number=None)
    assert provider.is_configured is False
    res = provider.initiate_call(
        call_id=uuid.uuid4(),
        phone_number="+15550100001",
        idempotency_key=f"tw-unconf-{uuid.uuid4().hex[:8]}",
    )
    assert res.success is False
    assert "not configured" in (res.error or "").lower()


def test_twilio_provider_durable_ledger_idempotency_and_timeout(sync_test_engine):
    SessionFactory = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    http_calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        http_calls.append(str(request.url))
        if "timeout-key" in request.content.decode("utf-8", errors="ignore"):
            raise httpx.ReadTimeout("Simulated Twilio read timeout")
        return httpx.Response(201, json={"sid": "CA1234567890abcdef", "status": "queued"})

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    provider = TwilioProvider(
        account_sid="ACtest0001",
        auth_token="tok_secret",
        from_number="+15550001111",
        webhook_base_url="https://demo.example.com",
        http_client=mock_client,
    )
    assert provider.is_configured is True

    with SessionFactory() as session:
        camp = Campaign(name="Twilio Ledger Test", mode=CampaignMode.PROGRESSIVE.value)
        session.add(camp)
        session.flush()
        borr = Borrower(campaign_id=camp.id, phone_number="+15550198888", name="Twilio Borrower")
        session.add(borr)
        session.flush()
        call = Call(
            campaign_id=camp.id,
            borrower_id=borr.id,
            status=CallState.RESERVED.value,
            idempotency_key=f"tw-idem-{uuid.uuid4().hex[:8]}",
            provider_name="twilio",
        )
        session.add(call)
        session.commit()

        # First call hits Twilio HTTP and records DISPATCHED in PostgreSQL
        r1 = provider.initiate_call(
            call_id=call.id,
            phone_number="+15550198888",
            idempotency_key=call.idempotency_key,
            session=session,
        )
        assert r1.success is True
        assert r1.provider_call_id == "CA1234567890abcdef"
        assert len(http_calls) == 1

        # Retry with same idempotency_key hits PostgreSQL ledger and makes ZERO duplicate HTTP requests
        r2 = provider.initiate_call(
            call_id=call.id,
            phone_number="+15550198888",
            idempotency_key=call.idempotency_key,
            session=session,
        )
        assert r2.success is True
        assert r2.provider_call_id == "CA1234567890abcdef"
        assert len(http_calls) == 1

        # Timeout case: marks ledger UNCERTAIN and refuses blind retry
        call_timeout = Call(
            campaign_id=camp.id,
            borrower_id=borr.id,
            status=CallState.RESERVED.value,
            idempotency_key=f"tw-to-{uuid.uuid4().hex[:8]}",
            provider_name="twilio",
        )
        session.add(call_timeout)
        session.commit()

        r_to = provider.initiate_call(
            call_id=call_timeout.id,
            phone_number="+15550199999",
            idempotency_key=call_timeout.idempotency_key,
            callback_url="https://demo.example.com/timeout-key",
            session=session,
        )
        assert r_to.success is False

        r_to_retry = provider.initiate_call(
            call_id=call_timeout.id,
            phone_number="+15550199999",
            idempotency_key=call_timeout.idempotency_key,
            session=session,
        )
        assert r_to_retry.success is False
        assert "UNCERTAIN" in (r_to_retry.error or "")


def test_twilio_status_form_webhook_updates_call_and_agent(sync_test_engine):
    SessionFactory = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    app = create_app(enable_worker=False, run_startup_migrations=False)

    def override_get_db():
        session = SessionFactory()
        try:
            yield session
            session.commit()
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db

    with SessionFactory() as s:
        provider_health_tracker.reset("twilio", session=s)
        camp = Campaign(name="Twilio Webhook Flow", mode=CampaignMode.PROGRESSIVE.value)
        s.add(camp)
        s.flush()
        ag = Agent(campaign_id=camp.id, name="Twilio-Agent-1", status=AgentState.DIALING.value)
        borr = Borrower(campaign_id=camp.id, phone_number="+15550197777", name="Twilio Customer")
        s.add_all([ag, borr])
        s.flush()
        call_sid = f"CA{uuid.uuid4().hex[:16]}"
        call = Call(
            campaign_id=camp.id,
            agent_id=ag.id,
            borrower_id=borr.id,
            status=CallState.INITIATED.value,
            state_rank=2,
            idempotency_key=f"tw-wh-{uuid.uuid4().hex[:8]}",
            provider_name="twilio",
            provider_call_id=call_sid,
        )
        s.add(call)
        s.flush()
        ag.current_call_id = call.id
        s.commit()
        cid, aid = str(call.id), str(ag.id)

    with TestClient(app) as client:
        # Twilio sends form-urlencoded status callbacks
        resp_ans = client.post(
            f"/api/v1/webhooks/twilio/status?call_id={cid}",
            data={"CallSid": call_sid, "CallStatus": "in-progress", "SequenceNumber": "1"},
        )
        assert resp_ans.status_code == 200
        assert resp_ans.json()["new_call_state"] == CallState.ANSWERED.value
        assert resp_ans.json()["new_agent_state"] == AgentState.CONNECTED.value

        resp_comp = client.post(
            f"/api/v1/webhooks/twilio/status?call_id={cid}",
            data={"CallSid": call_sid, "CallStatus": "completed", "CallDuration": "42", "SequenceNumber": "2"},
        )
        assert resp_comp.status_code == 200
        assert resp_comp.json()["new_call_state"] == CallState.COMPLETED.value
        assert resp_comp.json()["new_agent_state"] == AgentState.WRAP_UP.value

    with SessionFactory() as s:
        ag_row = s.get(Agent, uuid.UUID(aid))
        c_row = s.get(Call, uuid.UUID(cid))
        assert ag_row.status == AgentState.WRAP_UP.value
        assert c_row.status == CallState.COMPLETED.value
        assert c_row.duration_seconds == 42.0
