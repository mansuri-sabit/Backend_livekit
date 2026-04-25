"""
Pydantic models for the AI Voice Platform.
Ported from Project A (Pipecat version) — adapted to Project B conventions.

MongoDB stores documents with _id (ObjectId). Pydantic models use str for IDs.
Fields named user_id in code are serialised as tenant_id in DB for backward compatibility
where noted (CallRecord, Campaign).
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

# ── Role constants ────────────────────────────────────────────────────────────
ROLE_SUPER_ADMIN = 0
ROLE_ADMIN = 1
ROLE_RESELLER = 2
ROLE_USER = 3
ROLE_DEMO = 4


# ── User ──────────────────────────────────────────────────────────────────────

class User(BaseModel):
    """Dashboard user. Roles 0 and 4 use a single shared credential; 1/2/3 are admin-created."""
    id: Optional[str] = None                          # MongoDB _id as string
    email: str
    password_hash: str
    role: int = ROLE_USER                             # 0–4
    credits: int = 0
    concurrency: int = 0                              # Concurrent-call pool size
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_by: Optional[str] = None                  # user_id of creating admin
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Knowledge Base ────────────────────────────────────────────────────────────

class KnowledgeBaseItem(BaseModel):
    """Single uploaded KB file — stored as metadata in agent.knowledge_base_items."""
    id: str
    filename: str
    content: str                                      # Legacy: full content. New: empty (stored in kb_chunks)
    uploaded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Agent Persona ─────────────────────────────────────────────────────────────

class PersonaConfig(BaseModel):
    """Defines how the AI agent behaves on calls."""
    name: str = "Sarah"
    company: str = ""
    role: str = "Customer Service Executive"
    personality: str = "friendly"                     # friendly | professional | assertive | empathetic
    speaking_style: str = "casual"                    # formal | casual | hinglish
    tone: str = "warm"                                # warm | energetic | calm | authoritative
    response_length: str = "short"                    # short | medium | detailed
    language_mix: str = "english"                     # english | hindi | hinglish | regional
    dos: List[str] = []
    donts: List[str] = []
    objection_handling: str = ""
    escalation_trigger: str = ""
    closing_style: str = ""
    custom_instructions: str = ""
    # Pipeline config
    llm_model: str = "gpt-4o-mini"
    tts_model: str = "bulbul:v3"
    tts_pace_min: float = 0.78
    tts_pace_max: float = 1.12
    tts_temperature_min: float = 0.70
    tts_temperature_max: float = 1.00
    max_call_duration: int = 300                      # seconds — auto-hangup after this
    allow_interruptions: bool = True                  # barge-in
    lead_keywords: List[str] = []


# ── Tenant ────────────────────────────────────────────────────────────────────

class Tenant(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    tenant_id: str = Field(..., alias="_id")
    name: str
    max_concurrent_calls: int = 100
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Agent ─────────────────────────────────────────────────────────────────────

class Agent(BaseModel):
    agent_id: str = ""
    tenant_id: str
    name: str
    persona: PersonaConfig = Field(default_factory=PersonaConfig)
    system_prompt: str = ""
    virtual_number: str = ""                          # Exotel from-number for this agent
    voice_id: str = "priya"                           # Sarvam Bulbul v3 speaker
    language: str = "en-IN"                           # BCP-47 language code
    greeting: str = "Hello! How can I help you today?"
    cached_greeting: Optional[str] = None             # Pre-generated {{greeting}} template
    max_concurrent: int = 100
    tools: List[Dict] = []
    knowledge_base: str = ""                          # Legacy single-string KB (deprecated)
    knowledge_base_items: List[Dict[str, Any]] = []   # [{id, filename, uploaded_at}]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Call Record ───────────────────────────────────────────────────────────────

class CallRecord(BaseModel):
    """Call document. user_id is stored as tenant_id in DB for backward compatibility."""
    model_config = ConfigDict(populate_by_name=True)

    call_id: str
    user_id: str = Field(..., alias="tenant_id", serialization_alias="tenant_id")
    agent_id: str
    from_number: str
    to_number: str
    direction: str = "outbound"
    status: str = "in-progress"                       # in-progress | completed | failed | busy | no-answer | dnd | canceled | voicemail
    failure_reason: Optional[str] = None              # dnd | compliance | busy | no_answer | network_error | voicemail | user_inactivity
    transcript: List[Dict] = []                       # [{role, content, timestamp}]
    summary: str = ""
    outcome: str = ""                                 # interested | not-interested | callback | dnd
    duration: Optional[int] = None
    campaign_id: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Campaign ──────────────────────────────────────────────────────────────────

class CampaignContact(BaseModel):
    number: str
    name: Optional[str] = None


class Campaign(BaseModel):
    """Bulk calling campaign. user_id stored as tenant_id in DB for backward compatibility."""
    model_config = ConfigDict(populate_by_name=True)

    user_id: str = Field(..., alias="tenant_id", serialization_alias="tenant_id")
    name: str
    contacts: List[CampaignContact] = []
    active_calls: int = 1                             # concurrent calls at a time
    additional: bool = False
    status: str = "running"                           # scheduled | running | paused | completed
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total_numbers: int = 0
    completed_calls: int = 0
    failed_calls: int = 0
    voicemail_calls: int = 0
    retry_contact: int = 0
    retry_attempt: int = 0
    cached_greeting: Optional[str] = None
    transfer_enabled: Optional[bool] = None
    transfer_human_agent_number: Optional[str] = None
    proposal_enabled: Optional[bool] = None
    proposal_templates: Optional[str] = None         # JSON array overriding env PROPOSAL_TEMPLATES


# ── Scheduled Call ────────────────────────────────────────────────────────────

class ScheduledCall(BaseModel):
    user_id: str
    agent_id: str
    to_number: str
    scheduled_at: datetime
    status: str = "pending"                           # pending | dispatched | failed | cancelled
    campaign_id: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Lead ──────────────────────────────────────────────────────────────────────

class Lead(BaseModel):
    user_id: str
    name: Optional[str] = None
    phone: str
    email: Optional[str] = None
    source: str = "call"                              # call | campaign | manual
    status: str = "new"                               # new | contacted | qualified | converted | lost
    notes: str = ""
    campaign_id: Optional[str] = None
    call_id: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Phone Number ──────────────────────────────────────────────────────────────

class Phone(BaseModel):
    user_id: str
    number: str                                       # E.164 or 10-digit
    agent_id: Optional[str] = None                   # assigned agent ObjectId as string
    label: str = ""
    active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Appointment ───────────────────────────────────────────────────────────────

class Appointment(BaseModel):
    user_id: str
    agent_id: str
    call_id: Optional[str] = None
    contact_name: Optional[str] = None
    contact_phone: str
    scheduled_at: datetime
    duration_minutes: int = 30
    notes: str = ""
    status: str = "scheduled"                        # scheduled | completed | cancelled | no-show
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Credit Transaction ────────────────────────────────────────────────────────

class CreditTransaction(BaseModel):
    user_id: str
    action: str                                       # deduct | assign | adjust | call_deduction
    amount: int                                       # negative for deductions
    previous_credits: int
    new_credits: int
    reason: str = ""
    performed_by: str = "system"
    call_id: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── DIY Agent ─────────────────────────────────────────────────────────────────

class DIYAgent(BaseModel):
    user_id: str
    name: str
    prompt: str = ""
    gender: str = "female"
    supported_languages: List[str] = ["en"]
    inbound_config: Dict[str, Any] = {}
    outbound_config: Dict[str, Any] = {}
    call_transfer_enabled: bool = False
    call_transfer_settings: Dict[str, Any] = {}
    appointment_booking: Dict[str, Any] = {}
    proposal_settings: Dict[str, Any] = {}
    voicemail_detection: Dict[str, Any] = {"enabled": True}
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Audit Log ─────────────────────────────────────────────────────────────────

class AuditLog(BaseModel):
    action: str
    resource_type: str
    resource_id: str
    performed_by: str
    performed_by_role: int
    details: Optional[Dict[str, Any]] = None
    target_user_id: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
