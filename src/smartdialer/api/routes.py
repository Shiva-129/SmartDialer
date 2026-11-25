"""FastAPI REST API, Telecom Webhook Endpoints, and Simulation Controls."""

import uuid
from typing import Dict, List
from urllib.parse import parse_qs
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, joinedload, sessionmaker

from smartdialer.api.deps import get_db
from smartdialer.api.schemas import (
    AgentBatchCreate,
    AgentResponse,
    AgentTransitionRequest,
    BorrowerBatchCreate,
    BorrowerResponse,
    CallNotesUpdateRequest,
    CallResponse,
    CampaignCreate,
    CampaignDetailResponse,
    CampaignResponse,
    CampaignUpdate,
    DialCycleRequest,
    DialCycleResponse,
    ProviderHealthResponse,
    ProviderHealthToggleRequest,
    ProviderWebhookRequest,
    ProviderWebhookResponse,
    SafetyDecisionResponse,
    SimulationRunRequest,
)
from smartdialer.api.ws import ws_manager
from smartdialer.dialer.predictive import PredictiveDialer
from smartdialer.dialer.progressive import ProgressiveDialer
from smartdialer.events.processor import EventProcessor
from smartdialer.models.agent import Agent
from smartdialer.models.audit import SafetyDecisionLog
from smartdialer.models.borrower import Borrower
from smartdialer.models.call import Call
from smartdialer.models.campaign import Campaign
from smartdialer.models.enums import AgentState, CallState, CampaignMode
from smartdialer.pacing.metrics import telemetry_registry
from smartdialer.providers.registry import provider_registry
from smartdialer.providers.twilio_provider import TwilioProvider
from smartdialer.recovery.outbox_dispatcher import OutboxDispatcher
from smartdialer.safety.provider_health import provider_health_tracker
from smartdialer.simulation.load_benchmark import LoadBenchmarkRunner
from smartdialer.simulation.runner import SimulationRunner
from smartdialer.state.agent_state_machine import AgentStateMachine
from smartdialer.state.exceptions import InvalidStateTransitionError

router = APIRouter()
api_v1 = APIRouter(prefix="/api/v1", tags=["SmartDialer v1"])


# ---------------------------------------------------------
# Health & System Status
# ---------------------------------------------------------
@router.get("/health", tags=["Health"])
def health_check(db: Session = Depends(get_db)) -> Dict[str, object]:
    """Verify PostgreSQL connectivity and provider health status."""
    db.execute(text("SELECT 1"))
    providers = {}
    for name in ("provider_a", "provider_b", "twilio"):
        prov = provider_registry.get(name)
        providers[name] = {
            "healthy": provider_health_tracker.is_healthy(name, session=db),
            "failure_rate": round(provider_health_tracker.get_failure_rate(name, session=db), 4),
            "configured": getattr(prov, "is_configured", True) if prov else False,
        }
    return {
        "status": "ok",
        "database": "connected",
        "providers": providers,
        "ws_connections": ws_manager.active_connections_count(),
    }


# ---------------------------------------------------------
# Campaigns
# ---------------------------------------------------------
@api_v1.post("/campaigns", response_model=CampaignResponse, status_code=status.HTTP_201_CREATED)
def create_campaign(payload: CampaignCreate, db: Session = Depends(get_db)) -> Campaign:
    campaign = Campaign(
        name=payload.name,
        mode=payload.mode.value,
        max_active_calls=payload.max_active_calls,
        target_abandonment_rate=payload.target_abandonment_rate,
    )
    db.add(campaign)
    db.commit()
    db.refresh(campaign)
    return campaign


@api_v1.get("/campaigns", response_model=List[CampaignResponse])
def list_campaigns(db: Session = Depends(get_db)) -> List[Campaign]:
    stmt = select(Campaign).order_by(Campaign.created_at.desc())
    return list(db.execute(stmt).scalars().all())


@api_v1.get("/campaigns/{campaign_id}", response_model=CampaignDetailResponse)
def get_campaign_detail(campaign_id: uuid.UUID, db: Session = Depends(get_db)) -> CampaignDetailResponse:
    campaign = db.get(Campaign, campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    agent_rows = db.execute(
        select(Agent.status, func.count(Agent.id))
        .where(Agent.campaign_id == campaign_id)
        .group_by(Agent.status)
    ).all()
    agent_counts = {st: cnt for st, cnt in agent_rows}

    call_rows = db.execute(
        select(Call.status, func.count(Call.id))
        .where(Call.campaign_id == campaign_id)
        .group_by(Call.status)
    ).all()
    call_counts = {st: cnt for st, cnt in call_rows}

    borrower_rows = db.execute(
        select(Borrower.status, func.count(Borrower.id))
        .where(Borrower.campaign_id == campaign_id)
        .group_by(Borrower.status)
    ).all()
    borrower_counts = {st: cnt for st, cnt in borrower_rows}

    total_agents = sum(agent_counts.values())
    busy_agents = sum(
        agent_counts.get(s, 0)
        for s in (AgentState.RESERVED.value, AgentState.DIALING.value, AgentState.CONNECTED.value)
    )
    utilization_pct = round((busy_agents / total_agents) * 100.0, 2) if total_agents > 0 else 0.0

    abandoned_calls = db.execute(
        select(func.count(Call.id)).where(
            Call.campaign_id == campaign_id,
            Call.termination_reason == "ABANDONED",
        )
    ).scalar_one()
    answered_or_connected = (
        call_counts.get(CallState.ANSWERED.value, 0)
        + call_counts.get(CallState.CONNECTED.value, 0)
        + call_counts.get(CallState.COMPLETED.value, 0)
        + abandoned_calls
    )
    abandonment_pct = (
        round((abandoned_calls / answered_or_connected) * 100.0, 2)
        if answered_or_connected > 0
        else 0.0
    )

    telem = telemetry_registry.get(str(campaign_id))
    return CampaignDetailResponse(
        id=campaign.id,
        name=campaign.name,
        mode=campaign.mode,
        status=campaign.status,
        target_abandonment_rate=campaign.target_abandonment_rate,
        max_active_calls=campaign.max_active_calls,
        created_at=campaign.created_at,
        agent_counts=agent_counts,
        call_counts=call_counts,
        borrower_counts=borrower_counts,
        answer_rate_ema=round(telem.answer_rate_ema, 4),
        historical_answer_rate=round(telem.historical_answer_rate, 4),
        average_talk_duration_sec=round(telem.average_talk_duration_seconds, 2),
        agent_utilization_pct=utilization_pct,
        abandonment_rate_pct=abandonment_pct,
    )


@api_v1.patch("/campaigns/{campaign_id}", response_model=CampaignResponse)
async def update_campaign(
    campaign_id: uuid.UUID,
    payload: CampaignUpdate,
    db: Session = Depends(get_db),
) -> Campaign:
    campaign = db.get(Campaign, campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    if payload.mode is not None:
        campaign.mode = payload.mode.value
    if payload.status is not None:
        campaign.status = payload.status
    if payload.max_active_calls is not None:
        campaign.max_active_calls = payload.max_active_calls

    db.commit()
    db.refresh(campaign)

    await ws_manager.broadcast_to_campaign(
        str(campaign_id),
        "campaign_updated",
        {"mode": campaign.mode, "status": campaign.status, "max_active_calls": campaign.max_active_calls},
    )
    return campaign


# ---------------------------------------------------------
# Agents
# ---------------------------------------------------------
@api_v1.post("/campaigns/{campaign_id}/agents", response_model=List[AgentResponse], status_code=status.HTTP_201_CREATED)
async def create_agents(
    campaign_id: uuid.UUID,
    payload: AgentBatchCreate,
    db: Session = Depends(get_db),
) -> List[Agent]:
    campaign = db.get(Campaign, campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    created = [
        Agent(campaign_id=campaign_id, name=item.name, status=item.status.value)
        for item in payload.agents
    ]
    db.add_all(created)
    db.commit()
    for ag in created:
        db.refresh(ag)

    await ws_manager.broadcast_to_campaign(
        str(campaign_id),
        "agents_added",
        {"count": len(created)},
    )
    return created


@api_v1.get("/campaigns/{campaign_id}/agents", response_model=List[AgentResponse])
def list_agents(campaign_id: uuid.UUID, db: Session = Depends(get_db)) -> List[AgentResponse]:
    agents = list(
        db.execute(select(Agent).where(Agent.campaign_id == campaign_id).order_by(Agent.name.asc()))
        .scalars()
        .all()
    )
    calls = list(
        db.execute(
            select(Call)
            .options(joinedload(Call.borrower))
            .where(Call.campaign_id == campaign_id)
            .order_by(Call.created_at.desc())
        )
        .scalars()
        .all()
    )

    call_by_id = {c.id: c for c in calls}
    stats_by_agent: Dict[uuid.UUID, Dict[str, object]] = {}
    for c in calls:
        if not c.agent_id:
            continue
        st = stats_by_agent.setdefault(
            c.agent_id,
            {"handled": 0, "completed": 0, "durations": [], "last_completed": None},
        )
        st["handled"] = int(st["handled"]) + 1
        if c.status == CallState.COMPLETED.value:
            st["completed"] = int(st["completed"]) + 1
            if st["last_completed"] is None:
                st["last_completed"] = c
        if c.duration_seconds is not None:
            st["durations"].append(float(c.duration_seconds))

    enriched: List[AgentResponse] = []
    for ag in agents:
        st = stats_by_agent.get(ag.id, {"handled": 0, "completed": 0, "durations": [], "last_completed": None})
        durations = st["durations"]
        avg_dur = round(sum(durations) / len(durations), 1) if durations else 0.0
        active_call = call_by_id.get(ag.current_call_id) if ag.current_call_id else None
        last_comp = st["last_completed"]
        enriched.append(
            AgentResponse(
                id=ag.id,
                campaign_id=ag.campaign_id,
                name=ag.name,
                status=ag.status,
                current_call_id=ag.current_call_id,
                version=ag.version,
                current_borrower_name=active_call.borrower.name if active_call and active_call.borrower else None,
                current_borrower_phone=active_call.borrower.phone_number if active_call and active_call.borrower else None,
                last_completed_call_id=last_comp.id if last_comp else None,
                last_completed_borrower_name=last_comp.borrower.name if last_comp and last_comp.borrower else None,
                calls_handled=int(st["handled"]),
                completed_calls=int(st["completed"]),
                avg_talk_duration_sec=avg_dur,
                last_call_ended_at=ag.last_call_ended_at,
            )
        )
    return enriched


@api_v1.post("/agents/{agent_id}/transition", response_model=AgentResponse)
async def transition_agent_state(
    agent_id: uuid.UUID,
    payload: AgentTransitionRequest,
    db: Session = Depends(get_db),
) -> Agent:
    agent = db.execute(
        select(Agent).where(Agent.id == agent_id).with_for_update()
    ).scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    try:
        AgentStateMachine.transition(agent, payload.target_state)
    except InvalidStateTransitionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    db.commit()
    db.refresh(agent)

    await ws_manager.broadcast_to_campaign(
        str(agent.campaign_id),
        "agent_state",
        {"agent_id": str(agent.id), "name": agent.name, "status": agent.status},
    )
    return agent


# ---------------------------------------------------------
# Borrowers
# ---------------------------------------------------------
@api_v1.post("/campaigns/{campaign_id}/borrowers", response_model=List[BorrowerResponse], status_code=status.HTTP_201_CREATED)
def create_borrowers(
    campaign_id: uuid.UUID,
    payload: BorrowerBatchCreate,
    db: Session = Depends(get_db),
) -> List[Borrower]:
    campaign = db.get(Campaign, campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    created = [
        Borrower(
            campaign_id=campaign_id,
            phone_number=item.phone_number,
            name=item.name,
            priority=item.priority,
        )
        for item in payload.borrowers
    ]
    db.add_all(created)
    db.commit()
    for b in created:
        db.refresh(b)
    return created


@api_v1.get("/campaigns/{campaign_id}/borrowers", response_model=List[BorrowerResponse])
def list_borrowers(
    campaign_id: uuid.UUID,
    limit: int = 100,
    db: Session = Depends(get_db),
) -> List[Borrower]:
    stmt = (
        select(Borrower)
        .where(Borrower.campaign_id == campaign_id)
        .order_by(Borrower.priority.desc(), Borrower.created_at.asc())
        .limit(limit)
    )
    return list(db.execute(stmt).scalars().all())


# ---------------------------------------------------------
# Dialing Cycles & Safety Decisions
# ---------------------------------------------------------
@api_v1.post("/campaigns/{campaign_id}/dial", response_model=DialCycleResponse)
async def trigger_dial_cycle(
    campaign_id: uuid.UUID,
    payload: DialCycleRequest,
    db: Session = Depends(get_db),
) -> DialCycleResponse:
    campaign = db.get(Campaign, campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if campaign.status != "ACTIVE":
        raise HTTPException(status_code=400, detail=f"Campaign status is '{campaign.status}', must be ACTIVE")

    if campaign.mode == CampaignMode.PREDICTIVE.value:
        pred_res = PredictiveDialer.execute_cycle(
            session=db,
            campaign_id=campaign_id,
            provider_name=payload.provider_name,
            simulated_answer_rate=payload.simulated_answer_rate,
            simulated_talk_duration_sec=payload.simulated_talk_duration_sec,
        )
        decision = pred_res.safety_decision
        allocations = pred_res.allocations
        msg = pred_res.message
    else:
        prog_res = ProgressiveDialer.execute_cycle(
            session=db,
            campaign_id=campaign_id,
            provider_name=payload.provider_name,
        )
        decision = prog_res.safety_decision
        allocations = prog_res.allocations
        msg = prog_res.message

    db.commit()

    allocated_call_ids = [a.call.id for a in allocations]
    dispatched_call_ids: List[uuid.UUID] = []
    if payload.dispatch_immediately and allocated_call_ids:
        dispatched_call_ids = OutboxDispatcher.dispatch_pending(
            session=db,
            limit=max(20, len(allocated_call_ids)),
            call_ids=allocated_call_ids,
        )

    response = DialCycleResponse(
        campaign_id=campaign_id,
        mode=campaign.mode,
        decision=decision.decision.value,
        requested_calls=decision.requested_calls,
        approved_calls=decision.approved_calls,
        allocated_calls=len(allocations),
        dispatched_call_ids=dispatched_call_ids,
        allocated_call_ids=allocated_call_ids,
        reasoning=decision.reasoning,
        message=msg,
    )

    await ws_manager.broadcast_to_campaign(
        str(campaign_id),
        "dial_cycle",
        {
            "mode": campaign.mode,
            "decision": response.decision,
            "requested": response.requested_calls,
            "approved": response.approved_calls,
            "allocated": response.allocated_calls,
            "dispatched": len(dispatched_call_ids),
            "reasoning": response.reasoning,
        },
    )
    return response


@api_v1.get("/campaigns/{campaign_id}/calls", response_model=List[CallResponse])
def list_campaign_calls(
    campaign_id: uuid.UUID,
    limit: int = 100,
    db: Session = Depends(get_db),
) -> List[CallResponse]:
    stmt = (
        select(Call)
        .options(joinedload(Call.agent), joinedload(Call.borrower))
        .where(Call.campaign_id == campaign_id)
        .order_by(Call.created_at.desc())
        .limit(limit)
    )
    calls = list(db.execute(stmt).scalars().all())
    return [
        CallResponse(
            id=c.id,
            campaign_id=c.campaign_id,
            agent_id=c.agent_id,
            borrower_id=c.borrower_id,
            agent_name=c.agent.name if c.agent else None,
            borrower_name=c.borrower.name if c.borrower else None,
            borrower_phone=c.borrower.phone_number if c.borrower else None,
            status=c.status,
            dialing_mode=c.dialing_mode,
            provider_name=c.provider_name,
            provider_call_id=c.provider_call_id,
            termination_reason=c.termination_reason,
            disposition=c.disposition,
            call_notes=c.call_notes,
            termination_status=c.termination_status,
            duration_seconds=c.duration_seconds,
            answered_at=c.answered_at,
            connected_at=c.connected_at,
            completed_at=c.completed_at,
            created_at=c.created_at,
        )
        for c in calls
    ]


@api_v1.post("/calls/{call_id}/notes", response_model=CallResponse)
async def save_call_notes(
    call_id: uuid.UUID,
    payload: CallNotesUpdateRequest,
    db: Session = Depends(get_db),
) -> CallResponse:
    """Save agent wrap-up call notes and optional disposition; optionally transition agent from WRAP_UP to AVAILABLE."""
    call = db.execute(
        select(Call)
        .where(Call.id == call_id)
        .with_for_update()
    ).scalar_one_or_none()
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")

    call.disposition = payload.disposition.strip() if payload.disposition else None
    call.call_notes = payload.call_notes.strip()

    agent_released = False
    ag = None
    if call.agent_id:
        ag = db.execute(
            select(Agent).where(Agent.id == call.agent_id).with_for_update()
        ).scalar_one_or_none()
        if payload.complete_wrap_up and ag and ag.status == AgentState.WRAP_UP.value:
            AgentStateMachine.transition(ag, AgentState.AVAILABLE)
            agent_released = True

    borrower = db.get(Borrower, call.borrower_id)
    agent_name = ag.name if ag else None
    borrower_name = borrower.name if borrower else None
    borrower_phone = borrower.phone_number if borrower else None

    db.commit()
    db.refresh(call)

    await ws_manager.broadcast_to_campaign(
        str(call.campaign_id),
        "call_notes_saved",
        {
            "call_id": str(call.id),
            "agent_name": agent_name,
            "borrower_name": borrower_name,
            "disposition": call.disposition,
            "agent_released": agent_released,
        },
    )

    return CallResponse(
        id=call.id,
        campaign_id=call.campaign_id,
        agent_id=call.agent_id,
        borrower_id=call.borrower_id,
        agent_name=agent_name,
        borrower_name=borrower_name,
        borrower_phone=borrower_phone,
        status=call.status,
        dialing_mode=call.dialing_mode,
        provider_name=call.provider_name,
        provider_call_id=call.provider_call_id,
        termination_reason=call.termination_reason,
        disposition=call.disposition,
        call_notes=call.call_notes,
        termination_status=call.termination_status,
        duration_seconds=call.duration_seconds,
        answered_at=call.answered_at,
        connected_at=call.connected_at,
        completed_at=call.completed_at,
        created_at=call.created_at,
    )


@api_v1.get("/campaigns/{campaign_id}/safety-logs", response_model=List[SafetyDecisionResponse])
def list_safety_decisions(
    campaign_id: uuid.UUID,
    limit: int = 50,
    db: Session = Depends(get_db),
) -> List[SafetyDecisionLog]:
    stmt = (
        select(SafetyDecisionLog)
        .where(SafetyDecisionLog.campaign_id == campaign_id)
        .order_by(SafetyDecisionLog.created_at.desc())
        .limit(limit)
    )
    return list(db.execute(stmt).scalars().all())


# ---------------------------------------------------------
# Telecom Webhooks & Provider Health Controls
# ---------------------------------------------------------
@api_v1.post("/webhooks/provider/{provider_name}", response_model=ProviderWebhookResponse)
async def ingest_provider_webhook(
    provider_name: str,
    payload: ProviderWebhookRequest,
    db: Session = Depends(get_db),
) -> ProviderWebhookResponse:
    if not provider_registry.get(provider_name):
        raise HTTPException(status_code=404, detail=f"Unknown telecom provider '{provider_name}'")

    res = EventProcessor.process_event(
        session=db,
        provider_name=provider_name,
        event_id=payload.event_id,
        call_id=payload.call_id,
        event_type=payload.event_type,
        raw_payload=payload.payload,
        provider_call_id=payload.provider_call_id,
    )
    if not res.success:
        db.rollback()
        raise HTTPException(status_code=404, detail=res.message)

    db.commit()

    # Resolve campaign_id for live WebSocket push
    target_call = None
    if payload.call_id:
        target_call = db.get(Call, payload.call_id)
    elif payload.provider_call_id:
        target_call = db.execute(
            select(Call).where(
                Call.provider_name == provider_name,
                Call.provider_call_id == payload.provider_call_id,
            )
        ).scalar_one_or_none()

    if target_call:
        await ws_manager.broadcast_to_campaign(
            str(target_call.campaign_id),
            "webhook_event",
            {
                "provider": provider_name,
                "event_id": payload.event_id,
                "call_id": str(target_call.id),
                "event_type": payload.event_type,
                "new_call_state": res.new_call_state,
                "new_agent_state": res.new_agent_state,
                "is_duplicate": res.is_duplicate,
                "is_out_of_order": res.is_out_of_order,
            },
        )

    return ProviderWebhookResponse(
        success=res.success,
        is_duplicate=res.is_duplicate,
        is_out_of_order=res.is_out_of_order,
        new_call_state=res.new_call_state,
        new_agent_state=res.new_agent_state,
        message=res.message,
    )


@api_v1.post("/webhooks/twilio/status", response_model=ProviderWebhookResponse)
async def ingest_twilio_status_callback(
    request: Request,
    call_id: uuid.UUID | None = None,
    db: Session = Depends(get_db),
) -> ProviderWebhookResponse:
    """Ingest real Twilio application/x-www-form-urlencoded status callbacks."""
    raw_body = (await request.body()).decode("utf-8", errors="replace")
    parsed = {k: v[0] for k, v in parse_qs(raw_body).items() if v}

    call_sid = parsed.get("CallSid")
    twilio_status = parsed.get("CallStatus", "")
    seq = parsed.get("SequenceNumber", "0")
    duration_raw = parsed.get("CallDuration")
    event_type = TwilioProvider.map_twilio_status(twilio_status)
    event_id = f"twilio-{call_sid or call_id}-{twilio_status.lower()}-{seq}"

    extra_payload: Dict[str, object] = {
        "twilio_status": twilio_status,
        "provider_call_id": call_sid,
    }
    if duration_raw is not None:
        try:
            extra_payload["duration_seconds"] = float(duration_raw)
        except ValueError:
            pass

    res = EventProcessor.process_event(
        session=db,
        provider_name="twilio",
        event_id=event_id,
        call_id=call_id,
        event_type=event_type,
        raw_payload=extra_payload,
        provider_call_id=call_sid,
    )
    if not res.success:
        db.rollback()
        raise HTTPException(status_code=404, detail=res.message)

    db.commit()

    target_call = db.get(Call, call_id) if call_id else None
    if target_call is None and call_sid:
        target_call = db.execute(
            select(Call).where(Call.provider_name == "twilio", Call.provider_call_id == call_sid)
        ).scalar_one_or_none()

    if target_call:
        await ws_manager.broadcast_to_campaign(
            str(target_call.campaign_id),
            "webhook_event",
            {
                "provider": "twilio",
                "event_id": event_id,
                "call_id": str(target_call.id),
                "event_type": event_type,
                "new_call_state": res.new_call_state,
                "new_agent_state": res.new_agent_state,
                "is_duplicate": res.is_duplicate,
                "is_out_of_order": res.is_out_of_order,
            },
        )

    return ProviderWebhookResponse(
        success=res.success,
        is_duplicate=res.is_duplicate,
        is_out_of_order=res.is_out_of_order,
        new_call_state=res.new_call_state,
        new_agent_state=res.new_agent_state,
        message=res.message,
    )


@api_v1.get("/providers/health", response_model=List[ProviderHealthResponse])
def get_providers_health(db: Session = Depends(get_db)) -> List[ProviderHealthResponse]:
    out: List[ProviderHealthResponse] = []
    for name in ("provider_a", "provider_b", "twilio"):
        prov = provider_registry.get(name)
        out.append(
            ProviderHealthResponse(
                provider_name=name,
                is_healthy=provider_health_tracker.is_healthy(name, session=db),
                failure_rate=round(provider_health_tracker.get_failure_rate(name, session=db), 4),
                configured=getattr(prov, "is_configured", True) if prov else False,
            )
        )
    return out


@api_v1.post("/providers/{provider_name}/health", response_model=ProviderHealthResponse)
def set_provider_health(
    provider_name: str,
    payload: ProviderHealthToggleRequest,
    db: Session = Depends(get_db),
) -> ProviderHealthResponse:
    if not provider_registry.get(provider_name):
        raise HTTPException(status_code=404, detail=f"Unknown provider '{provider_name}'")

    provider_health_tracker.set_manual_status(provider_name, degraded=payload.degraded, session=db)
    db.commit()
    return ProviderHealthResponse(
        provider_name=provider_name,
        is_healthy=provider_health_tracker.is_healthy(provider_name, session=db),
        failure_rate=round(provider_health_tracker.get_failure_rate(provider_name, session=db), 4),
    )


# ---------------------------------------------------------
# Simulations (Scenarios A, B, C, D + Failure Demos)
# ---------------------------------------------------------
@api_v1.post("/simulations/run")
def run_simulation(
    payload: SimulationRunRequest,
    db: Session = Depends(get_db),
) -> Dict[str, object]:
    """Run assignment simulation scenarios against the active PostgreSQL database."""
    session_factory = sessionmaker(bind=db.get_bind(), expire_on_commit=False)

    if payload.scenario == "ALL":
        results = SimulationRunner.run_all_scenarios(
            session_factory=session_factory,
            num_agents=payload.num_agents,
            num_borrowers=payload.num_borrowers,
            cycles=payload.cycles,
            provider_name=payload.provider_name,
        )
    else:
        results = [
            SimulationRunner.run_scenario(
                session_factory=session_factory,
                scenario_key=payload.scenario,
                num_agents=payload.num_agents,
                num_borrowers=payload.num_borrowers,
                cycles=payload.cycles,
                provider_name=payload.provider_name,
            )
        ]

    failure_demos = SimulationRunner.run_failure_demos(session_factory=session_factory)
    return {
        "scenarios": [m.to_dict() for m in results],
        "failure_demos": failure_demos,
    }


@api_v1.post("/benchmarks/run")
def run_scaling_benchmark(db: Session = Depends(get_db)) -> Dict[str, object]:
    """Run live 100 & 1,000-agent PostgreSQL concurrency benchmark tiers."""
    session_factory = sessionmaker(bind=db.get_bind(), expire_on_commit=False)
    tiers = [
        LoadBenchmarkRunner.run_tier(session_factory, num_agents=100, worker_threads=4, batch_size=10),
        LoadBenchmarkRunner.run_tier(session_factory, num_agents=1000, worker_threads=4, batch_size=25),
    ]
    return {"tiers": [t.to_dict() for t in tiers]}


router.include_router(api_v1)
