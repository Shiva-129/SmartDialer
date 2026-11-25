"""Twilio REST API TelecomProvider implementation with durable PostgreSQL idempotency."""

from datetime import datetime, timedelta, timezone
import time
import uuid
from typing import Any, Dict, List, Optional
import httpx
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from smartdialer.core.config import settings
from smartdialer.core.logging import get_logger
from smartdialer.db.session import sync_engine
from smartdialer.models.provider_request import ProviderRequestLedger
from smartdialer.providers.base import (
    ProviderInitiateResult,
    ProviderTerminateResult,
    SimulatedWebhookEvent,
    TelecomProvider,
)

logger = get_logger("twilio_provider")


class TwilioProvider(TelecomProvider):
    """
    Real PSTN TelecomProvider adapter backed by Twilio's Programmable Voice REST API.

    Safety & Fault-Tolerance Guarantees:
    - Fails closed if TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN / TWILIO_FROM_NUMBER are unset.
    - Commits a 'PENDING' row to provider_request_ledger BEFORE the HTTP POST to Twilio.
    - Commits 'DISPATCHED' with Twilio's CallSid autonomously upon acceptance.
    - Marks timeouts/uncertain HTTP errors as 'UNCERTAIN' so retries never blind-dial twice.
    """

    STATUS_MAP: Dict[str, str] = {
        "queued": "INITIATED",
        "initiated": "INITIATED",
        "ringing": "RINGING",
        "in-progress": "ANSWERED",
        "answered": "ANSWERED",
        "completed": "COMPLETED",
        "busy": "FAILED",
        "no-answer": "FAILED",
        "failed": "FAILED",
        "canceled": "CANCELLED",
    }

    def __init__(
        self,
        account_sid: Optional[str] = None,
        auth_token: Optional[str] = None,
        from_number: Optional[str] = None,
        webhook_base_url: Optional[str] = None,
        http_client: Optional[httpx.Client] = None,
        api_base_url: str = "https://api.twilio.com/2010-04-01",
    ) -> None:
        self._name = "twilio"
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.from_number = from_number
        self.webhook_base_url = webhook_base_url
        self._http_client = http_client
        self.api_base_url = api_base_url.rstrip("/")

    @property
    def name(self) -> str:
        return self._name

    def _get_credentials(self) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        return (
            self.account_sid or settings.TWILIO_ACCOUNT_SID,
            self.auth_token or settings.TWILIO_AUTH_TOKEN,
            self.from_number or settings.TWILIO_FROM_NUMBER,
            self.webhook_base_url or settings.TWILIO_WEBHOOK_BASE_URL,
        )

    @property
    def is_configured(self) -> bool:
        sid, token, frm, _ = self._get_credentials()
        return bool(sid and token and frm)

    @classmethod
    def map_twilio_status(cls, twilio_status: str) -> str:
        """Map Twilio CallStatus webhook strings to SmartDialer CallState event types."""
        normalized = (twilio_status or "").strip().lower()
        return cls.STATUS_MAP.get(normalized, normalized.upper() or "FAILED")

    def initiate_call(
        self,
        call_id: uuid.UUID,
        phone_number: str,
        idempotency_key: str,
        callback_url: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> ProviderInitiateResult:
        """Place a real outbound voice call via Twilio after committing durable ledger intent."""
        sid, token, frm, base_url = self._get_credentials()
        if not sid or not token or not frm:
            return ProviderInitiateResult(
                success=False,
                error=(
                    "Twilio credentials not configured. Set TWILIO_ACCOUNT_SID, "
                    "TWILIO_AUTH_TOKEN, and TWILIO_FROM_NUMBER in environment or .env."
                ),
            )

        bind = session.get_bind() if session is not None else sync_engine
        AutonomousSession = sessionmaker(bind=bind, expire_on_commit=False)
        worker_id = f"twilio-worker-{uuid.uuid4().hex[:8]}"
        fencing_token = uuid.uuid4()
        now = datetime.now(timezone.utc)
        proceed_to_dispatch = False

        # 1. Check existing ledger entry autonomously
        with AutonomousSession() as auto_session:
            existing = auto_session.execute(
                select(ProviderRequestLedger).where(
                    ProviderRequestLedger.provider_name == self._name,
                    ProviderRequestLedger.idempotency_key == idempotency_key,
                )
            ).scalar_one_or_none()
            if existing:
                if existing.status in ("DISPATCHED", "SUCCEEDED"):
                    return ProviderInitiateResult(
                        success=existing.success,
                        provider_call_id=existing.provider_call_id,
                        latency_ms=0.0,
                        error=existing.error_message,
                    )
                if existing.status == "UNCERTAIN":
                    return ProviderInitiateResult(
                        success=False,
                        error="PREVIOUS_DISPATCH_UNCERTAIN: Twilio request outcome unconfirmed; refusing blind retry.",
                    )
                if existing.status == "FAILED":
                    return ProviderInitiateResult(
                        success=False,
                        error=existing.error_message or "Previous Twilio dispatch failed.",
                    )
                if existing.status == "PENDING" and existing.lease_expires_at and existing.lease_expires_at > now:
                    return ProviderInitiateResult(
                        success=False,
                        error="CONCURRENT_DISPATCH_IN_PROGRESS: Twilio call creation already in flight.",
                    )
                existing.worker_id = worker_id
                existing.fencing_token = fencing_token
                existing.lease_expires_at = now + timedelta(seconds=30)
                existing.attempt_count += 1
                auto_session.commit()
                proceed_to_dispatch = True

        # 2. Insert durable PENDING intent BEFORE carrier network I/O
        if not proceed_to_dispatch:
            try:
                with AutonomousSession() as auto_session:
                    ledger = ProviderRequestLedger(
                        provider_name=self._name,
                        idempotency_key=idempotency_key,
                        call_id=call_id,
                        phone_number=phone_number,
                        status="PENDING",
                        worker_id=worker_id,
                        fencing_token=fencing_token,
                        lease_expires_at=now + timedelta(seconds=30),
                        attempt_count=1,
                        success=False,
                    )
                    auto_session.add(ledger)
                    auto_session.commit()
            except IntegrityError:
                return ProviderInitiateResult(
                    success=False,
                    error="CONCURRENT_DISPATCH_CONFLICT: Another worker already claimed this Twilio idempotency key.",
                )

        # 3. Execute Twilio REST POST /Calls.json
        url = f"{self.api_base_url}/Accounts/{sid}/Calls.json"
        twiml = (
            "<Response>"
            "<Say voice=\"alice\">Hello, this is a call from SmartDialer collections. "
            "Connecting you to your specialist now.</Say>"
            "<Pause length=\"15\"/>"
            "</Response>"
        )
        form_data: Dict[str, Any] = {
            "To": phone_number,
            "From": frm,
            "Twiml": twiml,
            "Timeout": "30",
        }
        cb = callback_url or (
            f"{base_url.rstrip('/')}/api/v1/webhooks/twilio/status?call_id={call_id}"
            if base_url
            else None
        )
        if cb:
            form_data["StatusCallback"] = cb
            form_data["StatusCallbackMethod"] = "POST"
            form_data["StatusCallbackEvent"] = ["initiated", "ringing", "answered", "completed"]

        t0 = time.monotonic()
        try:
            client = self._http_client or httpx.Client(timeout=10.0)
            close_after = self._http_client is None
            try:
                resp = client.post(url, data=form_data, auth=(sid, token))
            finally:
                if close_after:
                    client.close()
            elapsed_ms = (time.monotonic() - t0) * 1000.0
        except httpx.TimeoutException as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            with AutonomousSession() as auto_session:
                entry = auto_session.execute(
                    select(ProviderRequestLedger).where(
                        ProviderRequestLedger.provider_name == self._name,
                        ProviderRequestLedger.idempotency_key == idempotency_key,
                    )
                ).scalar_one_or_none()
                if entry:
                    entry.status = "UNCERTAIN"
                    entry.error_message = f"Twilio HTTP timeout: {exc}"
                    entry.latency_ms = elapsed_ms
                    auto_session.commit()
            return ProviderInitiateResult(success=False, latency_ms=elapsed_ms, error=f"Twilio timeout: {exc}")
        except Exception as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            with AutonomousSession() as auto_session:
                entry = auto_session.execute(
                    select(ProviderRequestLedger).where(
                        ProviderRequestLedger.provider_name == self._name,
                        ProviderRequestLedger.idempotency_key == idempotency_key,
                    )
                ).scalar_one_or_none()
                if entry:
                    entry.status = "FAILED"
                    entry.error_message = str(exc)[:490]
                    entry.latency_ms = elapsed_ms
                    auto_session.commit()
            return ProviderInitiateResult(success=False, latency_ms=elapsed_ms, error=str(exc))

        if resp.status_code in (200, 201):
            data = resp.json()
            provider_call_id = data.get("sid")
            with AutonomousSession() as auto_session:
                entry = auto_session.execute(
                    select(ProviderRequestLedger).where(
                        ProviderRequestLedger.provider_name == self._name,
                        ProviderRequestLedger.idempotency_key == idempotency_key,
                    )
                ).scalar_one_or_none()
                if entry:
                    entry.status = "DISPATCHED"
                    entry.success = True
                    entry.provider_call_id = provider_call_id
                    entry.latency_ms = elapsed_ms
                    auto_session.commit()

            logger.info("Twilio call initiated", call_id=str(call_id), provider_call_id=provider_call_id)
            return ProviderInitiateResult(
                success=True,
                provider_call_id=provider_call_id,
                latency_ms=elapsed_ms,
            )

        err_msg = f"Twilio HTTP {resp.status_code}: {resp.text[:300]}"
        with AutonomousSession() as auto_session:
            entry = auto_session.execute(
                select(ProviderRequestLedger).where(
                    ProviderRequestLedger.provider_name == self._name,
                    ProviderRequestLedger.idempotency_key == idempotency_key,
                )
            ).scalar_one_or_none()
            if entry:
                entry.status = "UNCERTAIN" if resp.status_code >= 500 else "FAILED"
                entry.error_message = err_msg
                entry.latency_ms = elapsed_ms
                auto_session.commit()
        return ProviderInitiateResult(success=False, latency_ms=elapsed_ms, error=err_msg)

    def terminate_call(self, provider_call_id: str) -> bool:
        """Terminate an active Twilio call via REST API."""
        return self.terminate_call_with_status(provider_call_id).success

    def terminate_call_with_status(self, provider_call_id: str) -> ProviderTerminateResult:
        """Cancel or complete a live Twilio call and return explicit teardown status."""
        sid, token, _, _ = self._get_credentials()
        if not sid or not token:
            return ProviderTerminateResult(success=False, status="FAILED", error="Twilio credentials not configured.")

        url = f"{self.api_base_url}/Accounts/{sid}/Calls/{provider_call_id}.json"
        try:
            client = self._http_client or httpx.Client(timeout=8.0)
            close_after = self._http_client is None
            try:
                resp = client.post(url, data={"Status": "completed"}, auth=(sid, token))
            finally:
                if close_after:
                    client.close()
            if resp.status_code in (200, 201):
                return ProviderTerminateResult(success=True, status="CONFIRMED")
            return ProviderTerminateResult(success=False, status="FAILED", error=f"Twilio HTTP {resp.status_code}")
        except httpx.TimeoutException as exc:
            return ProviderTerminateResult(success=False, status="UNKNOWN", error=str(exc))
        except Exception as exc:
            return ProviderTerminateResult(success=False, status="FAILED", error=str(exc))

    def generate_simulated_lifecycle_events(
        self,
        call_id: uuid.UUID,
        provider_call_id: str,
        outcome: str = "COMPLETED",
        duration_seconds: float = 60.0,
    ) -> List[SimulatedWebhookEvent]:
        """Fallback helper if used inside unified provider harness."""
        return [
            SimulatedWebhookEvent(
                event_id=f"tw-{uuid.uuid4().hex[:12]}",
                provider_name=self._name,
                call_id=call_id,
                event_type="RINGING",
                payload={"provider_call_id": provider_call_id},
            ),
            SimulatedWebhookEvent(
                event_id=f"tw-{uuid.uuid4().hex[:12]}",
                provider_name=self._name,
                call_id=call_id,
                event_type=outcome,
                payload={"provider_call_id": provider_call_id, "duration_seconds": duration_seconds},
            ),
        ]
