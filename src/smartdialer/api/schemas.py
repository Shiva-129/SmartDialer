"""Pydantic v2 request and response schemas for SmartDialer REST & Webhook API."""

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from smartdialer.models.enums import AgentState, CampaignMode


# ---------------------------------------------------------
# Campaign Schemas
# ---------------------------------------------------------
class CampaignCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    mode: CampaignMode = Field(default=CampaignMode.PROGRESSIVE)
    max_active_calls: int = Field(default=100, ge=1, le=50000)
    target_abandonment_rate: float = Field(default=0.03, gt=0.0, le=0.25)


class CampaignUpdate(BaseModel):
    mode: Optional[CampaignMode] = None
    status: Optional[str] = Field(default=None, pattern="^(ACTIVE|PAUSED)$")
    max_active_calls: Optional[int] = Field(default=None, ge=1, le=50000)


class CampaignResponse(BaseModel):
    id: uuid.UUID
    name: str
    mode: str
    status: str
    target_abandonment_rate: float
    max_active_calls: int
    created_at: datetime

    model_config = {"from_attributes": True}


class CampaignDetailResponse(CampaignResponse):
    agent_counts: Dict[str, int]
    call_counts: Dict[str, int]
    borrower_counts: Dict[str, int]
    answer_rate_ema: float
    historical_answer_rate: float
    average_talk_duration_sec: float
    agent_utilization_pct: float
    abandonment_rate_pct: float


# ---------------------------------------------------------
# Agent Schemas
# ---------------------------------------------------------
class AgentCreateItem(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    status: AgentState = Field(default=AgentState.AVAILABLE)


class AgentBatchCreate(BaseModel):
    agents: List[AgentCreateItem] = Field(..., min_length=1, max_length=1000)


class AgentTransitionRequest(BaseModel):
    target_state: AgentState


class AgentResponse(BaseModel):
    id: uuid.UUID
    campaign_id: uuid.UUID
    name: str
    status: str
    current_call_id: Optional[uuid.UUID] = None
    version: int
    current_borrower_name: Optional[str] = None
    current_borrower_phone: Optional[str] = None
    last_completed_call_id: Optional[uuid.UUID] = None
    last_completed_borrower_name: Optional[str] = None
    calls_handled: int = 0
    completed_calls: int = 0
    avg_talk_duration_sec: float = 0.0
    last_call_ended_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ---------------------------------------------------------
# Borrower Schemas
# ---------------------------------------------------------
class BorrowerCreateItem(BaseModel):
    phone_number: str = Field(..., min_length=4, max_length=30)
    name: str = Field(..., min_length=1, max_length=100)
    priority: int = Field(default=0, ge=0, le=1000)


class BorrowerBatchCreate(BaseModel):
    borrowers: List[BorrowerCreateItem] = Field(..., min_length=1, max_length=5000)


class BorrowerResponse(BaseModel):
    id: uuid.UUID
    campaign_id: uuid.UUID
    phone_number: str
    name: str
    status: str
    priority: int
    call_attempts: int

    model_config = {"from_attributes": True}


# ---------------------------------------------------------
# Dialing & Safety Decision Schemas
# ---------------------------------------------------------
class DialCycleRequest(BaseModel):
    provider_name: str = Field(default="provider_a", pattern="^(provider_a|provider_b|twilio)$")
    dispatch_immediately: bool = Field(default=True)
    simulated_answer_rate: Optional[float] = Field(default=None, ge=0.01, le=0.99)
    simulated_talk_duration_sec: Optional[float] = Field(default=None, ge=1.0, le=3600.0)


class SafetyDecisionResponse(BaseModel):
    id: Optional[uuid.UUID] = None
    dialing_mode: str
    requested_calls: int
    approved_calls: int
    decision: str
    available_agents: int
    reserved_agents: int
    active_calls: int
    ringing_calls: int
    provider_failure_rate: float
    provider_healthy: bool
    reasoning: str
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class CallResponse(BaseModel):
    id: uuid.UUID
    campaign_id: uuid.UUID
    agent_id: Optional[uuid.UUID] = None
    borrower_id: uuid.UUID
    agent_name: Optional[str] = None
    borrower_name: Optional[str] = None
    borrower_phone: Optional[str] = None
    status: str
    dialing_mode: str
    provider_name: Optional[str] = None
    provider_call_id: Optional[str] = None
    termination_reason: Optional[str] = None
    disposition: Optional[str] = None
    call_notes: Optional[str] = None
    termination_status: str
    duration_seconds: Optional[float] = None
    answered_at: Optional[datetime] = None
    connected_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class CallNotesUpdateRequest(BaseModel):
    disposition: Optional[str] = Field(default=None, max_length=50)
    call_notes: str = Field(..., min_length=1, max_length=2000)
    complete_wrap_up: bool = Field(default=False)


class DialCycleResponse(BaseModel):
    campaign_id: uuid.UUID
    mode: str
    decision: str
    requested_calls: int
    approved_calls: int
    allocated_calls: int
    dispatched_call_ids: List[uuid.UUID]
    allocated_call_ids: List[uuid.UUID]
    reasoning: str
    message: str


# ---------------------------------------------------------
# Webhook & Provider Health Schemas
# ---------------------------------------------------------
class ProviderWebhookRequest(BaseModel):
    event_id: str = Field(..., min_length=1, max_length=100)
    call_id: Optional[uuid.UUID] = None
    provider_call_id: Optional[str] = None
    event_type: str = Field(..., min_length=1, max_length=50)
    payload: Dict[str, Any] = Field(default_factory=dict)


class ProviderWebhookResponse(BaseModel):
    success: bool
    is_duplicate: bool = False
    is_out_of_order: bool = False
    new_call_state: Optional[str] = None
    new_agent_state: Optional[str] = None
    message: str


class ProviderHealthToggleRequest(BaseModel):
    degraded: bool


class ProviderHealthResponse(BaseModel):
    provider_name: str
    is_healthy: bool
    failure_rate: float
    configured: bool = True


# ---------------------------------------------------------
# Simulation Schemas
# ---------------------------------------------------------
class SimulationRunRequest(BaseModel):
    scenario: str = Field(default="ALL", pattern="^(A|B|C|D|ALL)$")
    num_agents: int = Field(default=20, ge=2, le=200)
    num_borrowers: int = Field(default=60, ge=5, le=1000)
    cycles: int = Field(default=4, ge=1, le=20)
    provider_name: str = Field(default="provider_a", pattern="^(provider_a|provider_b)$")
