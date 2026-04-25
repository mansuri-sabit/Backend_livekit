"""
routes/admin_agents.py — Admin agent CRUD (dashboard-facing).

Prefix: /api/v1/agents

This router manages agents keyed by MongoDB _id, designed for the admin dashboard.
Agents here use a rich config schema (voice provider, LLM, TTS settings, etc.)
and support role-scoped visibility (Super Admin/Admin see all; Reseller sees own).

Endpoints:
  GET    /api/v1/agents/config/providers                  — LLM/TTS/STT provider lists
  GET    /api/v1/agents/voice-sample/{provider}/{voice_id} — cached voice preview audio URL
  POST   /api/v1/agents/voice-preview                     — live Sarvam TTS preview (Pipecat)
  POST   /api/v1/agents                                   — create agent
  GET    /api/v1/agents                                   — list agents (paginated, role-scoped)
  GET    /api/v1/agents/{agent_id}                        — get agent by ID
  PUT    /api/v1/agents/{agent_id}                        — update agent (deep-merge config)
  POST   /api/v1/agents/{agent_id}/generate-greeting      — regenerate cached greeting
  DELETE /api/v1/agents/{agent_id}                        — delete agent
  PATCH  /api/v1/agents/{agent_id}/toggle                 — toggle active/inactive

NOTE: voice_preview delegates to routes.demo which is created in Phase 10.
Until then, POST /api/v1/agents/voice-preview returns 503.

NOTE: voice-sample endpoint uses services.s3_service created in Phase 9.
Until then, GET /api/v1/agents/voice-sample/... returns 503.
"""
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from config import get_settings
from models import ROLE_ADMIN, ROLE_RESELLER, ROLE_SUPER_ADMIN
from utils.audit import log_audit
from utils.db import db, require_db
from utils.greeting_service import generate_greeting_template
from utils.logger import logger
from utils.security import get_current_user

router = APIRouter(prefix="/api/v1/agents", tags=["Admin Agents"])

# ── Voice / LLM provider catalogues ──────────────────────────────────────────

DEEPGRAM_VOICE_MAP = {
    "asteria": "aura-asteria-en", "luna": "aura-luna-en", "stella": "aura-stella-en",
    "athena": "aura-athena-en", "hera": "aura-hera-en", "orion": "aura-orion-en",
    "arcas": "aura-arcas-en", "perseus": "aura-perseus-en", "angus": "aura-angus-en",
    "orpheus": "aura-orpheus-en", "helios": "aura-helios-en", "zeus": "aura-zeus-en",
    "luna-es": "aura-luna-es",
}

ELEVENLABS_VOICE_MAP = {
    "EXAVITQu4vr4xnSDxMaL": "EXAVITQu4vr4xnSDxMaL",
    "pNInz6obpgDQGcFmaJgB": "pNInz6obpgDQGcFmaJgB",
    "ThT5KcBeYPX3keUQqHPh": "ThT5KcBeYPX3keUQqHPh",
}

# Valid speakers for Sarvam bulbul:v2 (current model as of 2026)
SARVAM_VOICE_LIST = [
    "anushka", "manisha", "vidya", "arya",      # female
    "abhilash", "karun", "hitesh",               # male
]
# Female voices for gender tag
_SARVAM_FEMALE = {"anushka", "manisha", "vidya", "arya"}

GOOGLE_VOICE_LIST = [
    "en-IN-Chirp3-HD-Fenrir", "en-IN-Chirp3-HD-Kore", "en-IN-Chirp3-HD-Leda",
    "en-IN-Chirp3-HD-Orus", "en-IN-Chirp3-HD-Zephyr",
]

SUPPORTED_LLM_MODELS = [
    {"id": "gpt-4o-mini", "label": "GPT-4o Mini", "description": "Fast and cost-effective"},
    {"id": "gpt-4o", "label": "GPT-4o", "description": "Most capable GPT-4 model"},
    {"id": "gpt-4-turbo", "label": "GPT-4 Turbo", "description": "GPT-4 Turbo"},
    {"id": "gpt-4", "label": "GPT-4", "description": "GPT-4"},
    {"id": "gpt-3.5-turbo", "label": "GPT-3.5 Turbo", "description": "Fast and affordable"},
    {"id": "claude-haiku-4-5-20251001", "label": "Claude Haiku 4.5", "description": "Anthropic - Fastest and cheapest"},
]

SUPPORTED_TTS_PROVIDERS = [
    {
        "id": "deepgram", "label": "Deepgram", "description": "Fastest, English-focused",
        "voices": [
            {"id": "asteria", "fullId": "aura-asteria-en", "name": "Asteria", "gender": "female", "region": "American", "desc": "Warm, conversational American female"},
            {"id": "luna", "fullId": "aura-luna-en", "name": "Luna", "gender": "female", "region": "American", "desc": "Clear, professional American female"},
            {"id": "stella", "fullId": "aura-stella-en", "name": "Stella", "gender": "female", "region": "American", "desc": "Energetic American female"},
            {"id": "athena", "fullId": "aura-athena-en", "name": "Athena", "gender": "female", "region": "British", "desc": "Sophisticated British female"},
            {"id": "hera", "fullId": "aura-hera-en", "name": "Hera", "gender": "female", "region": "American", "desc": "Professional American female"},
            {"id": "orion", "fullId": "aura-orion-en", "name": "Orion", "gender": "male", "region": "American", "desc": "Confident American male"},
            {"id": "arcas", "fullId": "aura-arcas-en", "name": "Arcas", "gender": "male", "region": "American", "desc": "Friendly American male"},
            {"id": "perseus", "fullId": "aura-perseus-en", "name": "Perseus", "gender": "male", "region": "American", "desc": "Strong American male"},
            {"id": "angus", "fullId": "aura-angus-en", "name": "Angus", "gender": "male", "region": "Irish", "desc": "Warm Irish male"},
            {"id": "orpheus", "fullId": "aura-orpheus-en", "name": "Orpheus", "gender": "male", "region": "American", "desc": "Smooth American male"},
            {"id": "helios", "fullId": "aura-helios-en", "name": "Helios", "gender": "male", "region": "British", "desc": "Distinguished British male"},
            {"id": "zeus", "fullId": "aura-zeus-en", "name": "Zeus", "gender": "male", "region": "American", "desc": "Authoritative American male"},
            {"id": "luna-es", "fullId": "aura-luna-es", "name": "Luna (Spanish)", "gender": "female", "region": "Latin American", "desc": "Clear Latin American Spanish female"},
        ],
    },
    {
        "id": "elevenlabs", "label": "ElevenLabs", "description": "Multilingual, high quality",
        "voices": [
            {"id": "EXAVITQu4vr4xnSDxMaL", "name": "Rachel", "gender": "female", "region": "American", "desc": "Natural, expressive (MULTILINGUAL)"},
            {"id": "pNInz6obpgDQGcFmaJgB", "name": "Adam", "gender": "male", "region": "American", "desc": "Deep, confident (MULTILINGUAL)"},
            {"id": "ThT5KcBeYPX3keUQqHPh", "name": "Dorothy", "gender": "female", "region": "British", "desc": "Pleasant British (MULTILINGUAL)"},
        ],
    },
    {
        "id": "openai", "label": "OpenAI", "description": "OpenAI TTS voices",
        "voices": [
            {"id": "alloy", "name": "Alloy", "gender": "neutral", "region": "American", "desc": "Neutral, balanced"},
            {"id": "echo", "name": "Echo", "gender": "male", "region": "American", "desc": "Clear male"},
            {"id": "nova", "name": "Nova", "gender": "female", "region": "American", "desc": "Energetic female"},
        ],
    },
    {
        "id": "sarvam", "label": "Sarvam", "description": "Indian languages specialist (Bulbul v2)",
        "voices": [
            {"id": v, "name": v.capitalize(), "gender": "female" if v in _SARVAM_FEMALE else "male", "region": "Indian", "desc": f"Sarvam Bulbul v2 - {v}"}
            for v in SARVAM_VOICE_LIST
        ],
    },
    {
        "id": "google", "label": "Google Cloud", "description": "Google Cloud TTS - HD voices",
        "voices": [
            {"id": v, "fullId": v, "name": v.split("-")[-1], "gender": "female" if v.endswith(("Kore","Leda","Zephyr")) else "male", "region": "Indian English", "desc": "Chirp3 HD voice"}
            for v in GOOGLE_VOICE_LIST
        ],
    },
    {"id": "cartesia", "label": "Cartesia", "description": "Low-latency streaming TTS", "voices": []},
]

SUPPORTED_STT_PROVIDERS = [
    {"id": "deepgram", "label": "Deepgram", "description": "Multilingual, fast (Nova-3), $0.46/hr"},
    {"id": "sarvam", "label": "Sarvam", "description": "Indian languages, ultra-low latency, $0.36/hr"},
    {"id": "azure", "label": "Azure Speech", "description": "100+ languages, $1.00/hr"},
    {"id": "whisper", "label": "OpenAI Whisper", "description": "90+ languages, $0.006/min"},
]


# ── Pydantic request models ───────────────────────────────────────────────────

class VoiceConfig(BaseModel):
    provider: str = "deepgram"
    voiceId: str = "asteria"
    model: Optional[str] = None
    settings: Optional[Dict[str, Any]] = None


class LLMConfig(BaseModel):
    model: str = "gpt-4o-mini"
    temperature: float = 0.7
    maxTokens: Optional[int] = None


class DirectionConfig(BaseModel):
    greetingMessage: Optional[str] = None
    prompt: Optional[str] = None
    persona: Optional[str] = None


class VoicemailDetectionConfig(BaseModel):
    enabled: bool = True
    confidenceThreshold: float = 0.7
    minDetectionTime: int = 3
    keywords: Optional[List[str]] = None
    enableAudioAMD: bool = True
    audioAMDPriority: str = "both"


class TransferSettingsConfig(BaseModel):
    enabled: bool = False
    humanAgentNumber: Optional[str] = None
    detectionKeywords: List[str] = Field(default_factory=lambda: ["human agent", "real person", "transfer", "speak to someone", "talk to agent"])
    transferCondition: Optional[str] = None
    confirmationQuestion: str = "Would you like me to transfer you to a human agent?"
    confirmationKeywords: List[str] = Field(default_factory=lambda: ["yes", "sure", "okay", "yeah", "please", "confirm", "go ahead"])
    negativeKeywords: List[str] = Field(default_factory=lambda: ["no", "not now", "later", "cancel", "nope"])
    confirmationMessage: str = "Transferring you to a human agent now. Please hold for a moment."
    failureMessage: str = "I'm sorry, but our agents are currently unavailable. How else can I help you?"
    confirmationTimeoutMs: int = 15000
    ringTimeoutSeconds: int = 30
    onConfirmationTimeout: str = "assume_no"
    fallbackBehavior: str = "continue"


class AppointmentBookingQuestionsConfig(BaseModel):
    nameQuestion: str = "What's your name?"
    dateQuestion: str = "What date would you like to schedule the appointment?"
    timeQuestion: str = "What time works for you?"
    reasonQuestion: Optional[str] = None
    confirmationQuestion: str = "I've booked your appointment for [date] at [time]. Is this correct?"


class AppointmentBookingConfig(BaseModel):
    enabled: bool = False
    condition: str = "Activate appointment booking when user expresses interest in scheduling."
    questions: AppointmentBookingQuestionsConfig = Field(default_factory=AppointmentBookingQuestionsConfig)
    successMessage: str = "Great! Your appointment is confirmed for [date] at [time]. We'll see you then!"
    slotStartTime: str = "09:00"
    slotEndTime: str = "17:00"
    slotDurationMinutes: int = 30
    workingDays: List[int] = Field(default_factory=lambda: [1, 2, 3, 4, 5])


class AgentConfigBody(BaseModel):
    prompt: str = "You are a helpful AI assistant."
    persona: Optional[str] = None
    greetingMessage: str = "Hello! How can I help you today?"
    greetingLanguage: Optional[str] = None
    endCallPhrases: List[str] = Field(default_factory=lambda: ["goodbye", "bye", "end call"])
    inboundConfig: Optional[DirectionConfig] = None
    outboundConfig: Optional[DirectionConfig] = None
    voice: VoiceConfig = Field(default_factory=VoiceConfig)
    language: str = "en"
    supportedLanguages: List[str] = Field(default_factory=lambda: ["en"])
    sttProvider: Optional[str] = "deepgram"
    llm: LLMConfig = Field(default_factory=LLMConfig)
    voicemailDetection: Optional[VoicemailDetectionConfig] = None
    transferSettings: Optional[TransferSettingsConfig] = None
    leadKeywords: List[str] = Field(default_factory=list)
    followUpKeywords: List[str] = Field(default_factory=list)
    appointmentBooking: Optional[AppointmentBookingConfig] = None
    dataCollection: Optional[Dict[str, Any]] = None
    proposalSettings: Optional[Dict[str, Any]] = None


class CreateAgentBody(BaseModel):
    name: str
    description: Optional[str] = None
    gender: Optional[str] = "female"
    useDirectionSpecificKnowledgeBase: Optional[bool] = False
    userId: Optional[str] = None   # link this agent to a specific user (stored as user_id for voice worker lookup)
    config: AgentConfigBody = Field(default_factory=AgentConfigBody)


class UpdateAgentBody(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    gender: Optional[str] = None
    useDirectionSpecificKnowledgeBase: Optional[bool] = None
    config: Optional[Dict[str, Any]] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _require_agent_manager(current_user: dict) -> None:
    if current_user.get("role") not in (ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_RESELLER):
        raise HTTPException(status_code=403, detail="Admin access required")


def _agent_to_dict(doc: dict) -> dict:
    config = doc.get("config") or {}
    return {
        "id": str(doc["_id"]),
        "name": doc.get("name", ""),
        "description": doc.get("description", ""),
        "gender": doc.get("gender", "female"),
        "managedBy": doc.get("managed_by", "admin"),
        "ownerId": doc.get("owner_id"),
        "useDirectionSpecificKnowledgeBase": doc.get("use_direction_specific_kb", False),
        "isActive": doc.get("is_active", True),
        "approvalStatus": doc.get("approval_status", "approved"),
        "version": doc.get("version", 1),
        "config": config,
        "createdAt": doc.get("created_at"),
        "updatedAt": doc.get("updated_at"),
    }


def _extract_persona_from_config(config_data: dict, existing_persona: dict | None = None) -> dict:
    """Sync voice/proposal settings from agent config into the persona sub-doc."""
    persona = dict(existing_persona or {})
    if config_data.get("proposalSettings") is not None:
        ps = config_data["proposalSettings"]
        persona["proposal_enabled"] = bool(ps.get("enabled"))
        persona["proposal_templates"] = ps.get("templates") or []
    voice_cfg = config_data.get("voice") or {}
    voice_settings = voice_cfg.get("settings") or {}
    for key, persona_key in [("pace_min", "tts_pace_min"), ("pace_max", "tts_pace_max"),
                              ("temperature_min", "tts_temperature_min"), ("temperature_max", "tts_temperature_max")]:
        if voice_settings.get(key) is not None:
            persona[persona_key] = float(voice_settings[key])
    # Backward compat: single "pace" / "temperature" → min and max
    if voice_settings.get("pace") is not None and voice_settings.get("pace_min") is None:
        persona["tts_pace_min"] = persona["tts_pace_max"] = float(voice_settings["pace"])
    if voice_settings.get("temperature") is not None and voice_settings.get("temperature_min") is None:
        persona["tts_temperature_min"] = persona["tts_temperature_max"] = float(voice_settings["temperature"])
    return persona


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/config/providers")
async def get_provider_config(current_user: dict = Depends(get_current_user)):
    """Return all supported LLM models, TTS providers (with voices), and STT providers."""
    _require_agent_manager(current_user)
    return {
        "success": True,
        "data": {
            "llmModels": SUPPORTED_LLM_MODELS,
            "ttsProviders": SUPPORTED_TTS_PROVIDERS,
            "sttProviders": SUPPORTED_STT_PROVIDERS,
        },
    }


@router.get("/voice-sample/{provider}/{voice_id}")
async def get_voice_sample(provider: str, voice_id: str, current_user: dict = Depends(get_current_user)):
    """
    Return a presigned S3 URL for a cached voice sample.
    Requires Phase 9 (services/s3_service.py). Returns 503 until then.
    """
    _require_agent_manager(current_user)
    settings = get_settings()

    try:
        from services.s3_service import get_voice_sample_presigned_url, upload_voice_sample
    except ImportError:
        raise HTTPException(status_code=503, detail="Voice sample service not available until Phase 9 (S3 not configured)")

    provider_norm = (provider or "").strip().lower()
    voice_norm = (voice_id or "").strip()

    import httpx

    if provider_norm == "deepgram":
        voice_key = voice_norm.lower()
        if voice_key not in DEEPGRAM_VOICE_MAP:
            raise HTTPException(status_code=404, detail=f"Unknown Deepgram voice: {voice_norm}")
        url = get_voice_sample_presigned_url(provider_norm, voice_key)
        if url:
            return {"url": url, "cached": True}
        api_key = (getattr(settings, "DEEPGRAM_API_KEY", None) or "").strip()
        if not api_key:
            raise HTTPException(status_code=503, detail="DEEPGRAM_API_KEY not configured.")
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(
                    "https://api.deepgram.com/v1/speak",
                    params={"model": DEEPGRAM_VOICE_MAP[voice_key]},
                    headers={"Authorization": f"Token {api_key}", "Content-Type": "application/json"},
                    json={"text": "Hello! I'm your AI voice assistant. How can I help you today?"},
                )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Deepgram TTS request failed: {exc}")
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Deepgram error: {resp.text[:300]}")
        upload_voice_sample(provider_norm, voice_key, resp.content, "audio/mpeg")
        url = get_voice_sample_presigned_url(provider_norm, voice_key)
        return {"url": url, "cached": False}

    elif provider_norm == "sarvam":
        voice_key = voice_norm.lower()
        if voice_key not in SARVAM_VOICE_LIST:
            raise HTTPException(status_code=404, detail=f"Unknown Sarvam voice: {voice_norm}")
        url = get_voice_sample_presigned_url(provider_norm, voice_key)
        if url:
            return {"url": url, "cached": True}
        api_key = (getattr(settings, "SARVAM_API_KEY", None) or "").strip()
        if not api_key:
            raise HTTPException(status_code=503, detail="SARVAM_API_KEY not configured.")
        sample_text = "Hello! I'm your AI voice assistant. How can I help you today?"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(
                    "https://api.sarvam.ai/text-to-speech",
                    headers={"api-subscription-key": api_key},
                    json={
                        "text": sample_text,
                        "target_language_code": "en-IN",
                        "speaker": voice_key,
                        "pace": 1.0,
                        "model": "bulbul:v3",
                    },
                )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Sarvam TTS request failed: {exc}")
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Sarvam error {resp.status_code}: {resp.text[:300]}")
        try:
            import base64 as _b64
            data = resp.json()
            audio_b64 = (data.get("audios") or [None])[0]
            if not audio_b64:
                raise ValueError("No audio field in Sarvam response")
            audio_bytes = _b64.b64decode(audio_b64)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Sarvam response parse failed: {exc}")
        # Try S3 cache; if unavailable return as data URL so dashboard can play immediately
        try:
            upload_voice_sample(provider_norm, voice_key, audio_bytes, "audio/wav")
            cached_url = get_voice_sample_presigned_url(provider_norm, voice_key)
            if cached_url:
                return {"url": cached_url, "cached": False}
        except Exception:
            pass
        import base64 as _b64c
        data_url = f"data:audio/wav;base64,{_b64c.b64encode(audio_bytes).decode()}"
        return {"url": data_url, "cached": False}

    else:
        raise HTTPException(status_code=501, detail=f"Voice sample not implemented for provider: {provider_norm}")


@router.post("/voice-preview")
async def agent_voice_preview(body: dict, current_user: dict = Depends(get_current_user)):
    """
    Live TTS preview — generates a short audio clip for the selected voice/provider.
    Supports: sarvam, deepgram, openai.
    Returns audio bytes as a streaming response (audio/wav or audio/mpeg).
    """
    _require_agent_manager(current_user)
    settings = get_settings()

    provider = (body.get("provider") or "sarvam").lower()
    voice_id = (body.get("voiceId") or body.get("voice_id") or "").strip()
    text = (body.get("text") or "Hello! I'm your AI voice assistant. How can I help you today?")[:500]
    language = body.get("language") or "en-IN"
    preview_settings = body.get("settings") or {}

    from fastapi.responses import Response

    # ── Sarvam ────────────────────────────────────────────────────────────────
    if provider == "sarvam":
        api_key = (settings.SARVAM_API_KEY or "").strip()
        if not api_key:
            raise HTTPException(status_code=503, detail="SARVAM_API_KEY not configured")
        pace = float(
            preview_settings.get("pace_min")
            or preview_settings.get("pace")
            or 1.0
        )
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(
                    "https://api.sarvam.ai/text-to-speech",
                    headers={"api-subscription-key": api_key},
                    json={
                        "text": text,
                        "target_language_code": language,
                        "speaker": voice_id or "anushka",
                        "pace": pace,
                        "model": "bulbul:v3",
                    },
                )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Sarvam TTS error: {exc}")
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Sarvam error {resp.status_code}: {resp.text[:200]}")
        try:
            import base64 as _b64
            data = resp.json()
            audio_b64 = (data.get("audios") or [None])[0]
            if not audio_b64:
                raise ValueError("No audio in Sarvam response")
            return Response(content=_b64.b64decode(audio_b64), media_type="audio/wav")
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Sarvam response parse failed: {exc}")

    # ── Deepgram ──────────────────────────────────────────────────────────────
    if provider == "deepgram":
        api_key = (settings.DEEPGRAM_API_KEY or "").strip()
        if not api_key:
            raise HTTPException(status_code=503, detail="DEEPGRAM_API_KEY not configured")
        model = DEEPGRAM_VOICE_MAP.get(voice_id.lower(), f"aura-{voice_id.lower()}-en")
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(
                    "https://api.deepgram.com/v1/speak",
                    params={"model": model},
                    headers={"Authorization": f"Token {api_key}", "Content-Type": "application/json"},
                    json={"text": text},
                )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Deepgram TTS error: {exc}")
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Deepgram error {resp.status_code}: {resp.text[:200]}")
        return Response(content=resp.content, media_type="audio/mpeg")

    # ── OpenAI ────────────────────────────────────────────────────────────────
    if provider == "openai":
        from openai import AsyncOpenAI
        api_key = (settings.OPENAI_API_KEY or "").strip()
        if not api_key:
            raise HTTPException(status_code=503, detail="OPENAI_API_KEY not configured")
        valid_voices = {"alloy", "echo", "fable", "onyx", "nova", "shimmer"}
        voice = voice_id.lower() if voice_id.lower() in valid_voices else "alloy"
        try:
            oai = AsyncOpenAI(api_key=api_key)
            audio_resp = await oai.audio.speech.create(model="tts-1", voice=voice, input=text)
            return Response(content=audio_resp.content, media_type="audio/mpeg")
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"OpenAI TTS error: {exc}")

    raise HTTPException(
        status_code=501,
        detail=f"Voice preview not implemented for provider '{provider}'. Supported: sarvam, deepgram, openai.",
    )


@router.post("")
async def create_agent(body: CreateAgentBody, current_user: dict = Depends(get_current_user)):
    """Create a new agent. Accessible to Admin/SuperAdmin/Reseller."""
    require_db()
    _require_agent_manager(current_user)

    if not body.name or not body.name.strip():
        raise HTTPException(status_code=400, detail="Agent name is required")

    now = datetime.now(timezone.utc)
    config_data = body.config.model_dump(exclude_none=True) if body.config else {}

    # Strip empty direction-specific configs
    for dir_key in ("inboundConfig", "outboundConfig"):
        dir_cfg = config_data.get(dir_key)
        if isinstance(dir_cfg, dict):
            if not (dir_cfg.get("greetingMessage") or "").strip() and not (dir_cfg.get("prompt") or "").strip():
                config_data.pop(dir_key, None)

    role = current_user.get("role")
    doc: dict = {
        "name": body.name.strip(),
        "description": (body.description or "").strip(),
        "gender": body.gender or "female",
        "use_direction_specific_kb": body.useDirectionSpecificKnowledgeBase or False,
        "approval_status": "approved",
        "version": 1,
        "is_active": True,
        "config": config_data,
        "test_call_logs": [],
        "created_at": now,
        "updated_at": now,
    }
    if role in (ROLE_SUPER_ADMIN, ROLE_ADMIN):
        doc["managed_by"] = "admin"
        # Allow admin to explicitly assign this agent to a user (for voice worker scope resolution)
        if body.userId:
            doc["user_id"] = body.userId.strip()
    elif role == ROLE_RESELLER:
        doc["managed_by"] = "reseller"
        doc["owner_id"] = current_user.get("user_id")
        # Reseller-created agents are scoped to the reseller's user_id by default
        doc["user_id"] = body.userId.strip() if body.userId else current_user.get("user_id")

    persona = _extract_persona_from_config(config_data)
    if persona:
        doc["persona"] = persona

    result = await db.agents.insert_one(doc)
    doc["_id"] = result.inserted_id
    agent_id_str = str(result.inserted_id)

    # Ensure user_id is always set so agent_owner_scope() can resolve this agent.
    # Admin agents without an explicit userId use their own ObjectId as the scope —
    # get_tenant_config() handles 24-char hex strings as direct _id lookups.
    if not doc.get("user_id"):
        await db.agents.update_one(
            {"_id": result.inserted_id},
            {"$set": {"user_id": agent_id_str}}
        )
        doc["user_id"] = agent_id_str

    # Pre-generate greeting from persona text
    persona_text = config_data.get("prompt") or config_data.get("persona") or ""
    if persona_text:
        try:
            cached_greeting = await generate_greeting_template(persona_text)
            if cached_greeting:
                await db.agents.update_one({"_id": result.inserted_id}, {"$set": {"cached_greeting": cached_greeting}})
                doc["cached_greeting"] = cached_greeting
                logger.info(f"[Greeting] Pre-generated for new agent {agent_id_str}")
        except Exception as ge:
            logger.warning(f"[Greeting] Pre-generation failed for agent {agent_id_str}: {ge}")

    logger.info(f"Agent created: {body.name} by user {current_user['user_id']}")
    try:
        await log_audit(
            action="create", resource_type="agent", resource_id=agent_id_str,
            performed_by=current_user.get("user_id") or "",
            performed_by_role=current_user.get("role"),
            details={"name": body.name, "description": body.description, "gender": body.gender},
        )
    except Exception:
        pass

    return {"success": True, "data": {"agent": _agent_to_dict(doc)}}


@router.get("")
async def list_agents(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    search: Optional[str] = None,
    isActive: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
):
    """
    List agents (paginated).
    Super Admin/Admin: see all. Reseller: see only own agents.
    """
    require_db()
    _require_agent_manager(current_user)

    role = current_user.get("role")
    user_id = current_user.get("user_id")

    if role in (ROLE_SUPER_ADMIN, ROLE_ADMIN):
        query: dict = {}
    elif role == ROLE_RESELLER:
        query = {"managed_by": "reseller", "owner_id": user_id}
    else:
        raise HTTPException(status_code=403, detail="Admin access required")

    if search:
        query["name"] = {"$regex": search, "$options": "i"}
    if isActive == "true":
        query["is_active"] = True
    elif isActive == "false":
        query["is_active"] = False

    skip = (page - 1) * limit
    total = await db.agents.count_documents(query)
    cursor = db.agents.find(query).sort("created_at", -1).skip(skip).limit(limit)

    agents = []
    async for doc in cursor:
        agents.append(_agent_to_dict(doc))

    return {
        "success": True,
        "data": {
            "agents": agents,
            "total": total,
            "page": page,
            "totalPages": max(1, -(-total // limit)),
        },
    }


@router.get("/{agent_id}")
async def get_agent(agent_id: str, current_user: dict = Depends(get_current_user)):
    """Get agent by MongoDB _id."""
    require_db()
    _require_agent_manager(current_user)
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Agent not found")

    doc = await db.agents.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Agent not found")

    if current_user.get("role") == ROLE_RESELLER:
        if doc.get("managed_by") != "reseller" or doc.get("owner_id") != current_user.get("user_id"):
            raise HTTPException(status_code=404, detail="Agent not found")

    return {"success": True, "data": {"agent": _agent_to_dict(doc)}}


@router.put("/{agent_id}")
async def update_agent(agent_id: str, body: UpdateAgentBody, current_user: dict = Depends(get_current_user)):
    """Update agent. Deep-merges config dict."""
    require_db()
    _require_agent_manager(current_user)
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Agent not found")

    doc = await db.agents.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Agent not found")

    if current_user.get("role") == ROLE_RESELLER:
        if doc.get("managed_by") != "reseller" or doc.get("owner_id") != current_user.get("user_id"):
            raise HTTPException(status_code=404, detail="Agent not found")

    updates: dict = {"updated_at": datetime.now(timezone.utc)}

    if body.name is not None:
        updates["name"] = body.name.strip()
    if body.description is not None:
        updates["description"] = body.description.strip()
    if body.gender is not None:
        updates["gender"] = body.gender
    if body.useDirectionSpecificKnowledgeBase is not None:
        updates["use_direction_specific_kb"] = body.useDirectionSpecificKnowledgeBase

    if body.config is not None:
        existing_config = doc.get("config") or {}
        merged = {**existing_config, **body.config}

        # Direction-specific configs (inboundConfig / outboundConfig) are ONLY
        # removed when the frontend EXPLICITLY sends them:
        #   - as null → intentional clear
        #   - as {} (empty dict with no greetingMessage and no prompt) → intentional clear
        # If the key is absent entirely from body.config, the existing DB value is
        # preserved. This prevents partial tab saves from silently wiping configs
        # that were set from a different tab.
        for dir_key in ("inboundConfig", "outboundConfig"):
            if dir_key not in body.config:
                # Not in this save payload — keep whatever is already in the DB.
                continue
            incoming_val = body.config.get(dir_key)
            if incoming_val is None:
                # Frontend explicitly sent null → remove.
                merged.pop(dir_key, None)
            elif isinstance(incoming_val, dict):
                gm = (incoming_val.get("greetingMessage") or "").strip()
                pr = (incoming_val.get("prompt") or "").strip()
                if not gm and not pr:
                    # Frontend sent an empty dict — treat as intentional removal.
                    merged.pop(dir_key, None)
                else:
                    merged[dir_key] = incoming_val

        updates["config"] = merged
        persona = _extract_persona_from_config(merged, doc.get("persona"))
        if persona:
            updates["persona"] = persona

    await db.agents.update_one({"_id": oid}, {"$set": updates})

    # Invalidate the voice-agent's in-memory tenant config cache for this agent
    # so the next call picks up the new config from MongoDB immediately.
    try:
        from agents.tenant_config import reload_all as _reload_tenant_cache
        _reload_tenant_cache()
    except Exception:
        pass

    # Also clear the KB backend cache so new KB config takes effect
    try:
        from services.kb_service import clear_backend_cache_sync
        clear_backend_cache_sync()
    except Exception:
        pass

    # Re-generate greeting if persona text changed
    if body.config:
        new_persona_text = body.config.get("prompt") or body.config.get("persona")
        if new_persona_text:
            try:
                cached_greeting = await generate_greeting_template(new_persona_text)
                if cached_greeting:
                    await db.agents.update_one({"_id": oid}, {"$set": {"cached_greeting": cached_greeting}})
                    logger.info(f"[Greeting] Re-generated for agent {agent_id}")
            except Exception as ge:
                logger.warning(f"[Greeting] Re-generation failed for agent {agent_id}: {ge}")

    updated = await db.agents.find_one({"_id": oid})
    logger.info(f"Agent {agent_id} updated by user {current_user['user_id']}")
    try:
        await log_audit(
            action="update", resource_type="agent", resource_id=agent_id,
            performed_by=current_user.get("user_id") or "",
            performed_by_role=current_user.get("role"),
            details={"updated_fields": list(updates.keys())},
        )
    except Exception:
        pass

    return {"success": True, "data": {"agent": _agent_to_dict(updated)}, "message": "Agent updated successfully"}


@router.get("/{agent_id}/debug-greeting")
async def debug_greeting(agent_id: str, current_user: dict = Depends(get_current_user)):
    """
    Diagnostic: show exactly what greeting + persona the voice agent would use
    for this agent at call time, for both inbound and outbound directions.
    Reads the live MongoDB document so it reflects the current saved state.
    """
    require_db()
    _require_agent_manager(current_user)
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Agent not found")

    doc = await db.agents.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Agent not found")

    config = doc.get("config") or {}
    inbound_cfg = config.get("inboundConfig") or {}
    outbound_cfg = config.get("outboundConfig") or {}

    default_greeting = (
        doc.get("greeting")
        or config.get("greetingMessage")
        or "Hello! How can I help you today?"
    )
    inbound_greeting  = inbound_cfg.get("greetingMessage")  or default_greeting
    outbound_greeting = outbound_cfg.get("greetingMessage") or default_greeting

    # Simulate the full tenant_config resolution via the real loader
    try:
        from agents.tenant_config import _mongo_agent_to_tenant_config
        from agents.prompt_builder import build_greeting
        scope = str(doc.get("user_id") or doc.get("tenant_id") or agent_id)
        cfg = _mongo_agent_to_tenant_config(scope, doc)
        resolved_inbound  = build_greeting(cfg, call_direction="inbound")
        resolved_outbound = build_greeting(cfg, call_direction="outbound")
    except Exception as exc:
        resolved_inbound  = f"[resolution error: {exc}]"
        resolved_outbound = f"[resolution error: {exc}]"

    return {
        "success": True,
        "agent_id": agent_id,
        "agent_name": doc.get("name"),
        "raw_db": {
            "doc.greeting": doc.get("greeting"),
            "config.greetingMessage": config.get("greetingMessage"),
            "config.inboundConfig": inbound_cfg or None,
            "config.outboundConfig": outbound_cfg or None,
        },
        "greeting_resolution": {
            "default_greeting": default_greeting,
            "inbound_greeting_stored": inbound_cfg.get("greetingMessage"),
            "outbound_greeting_stored": outbound_cfg.get("greetingMessage"),
            "inbound_greeting_effective": inbound_greeting,
            "outbound_greeting_effective": outbound_greeting,
        },
        "live_build_greeting": {
            "inbound":  resolved_inbound,
            "outbound": resolved_outbound,
        },
        "voice_config": {
            "tts_provider": config.get("voice", {}).get("provider"),
            "tts_voice":    config.get("voice", {}).get("voiceId"),
            "language":     config.get("language"),
            "stt_provider": config.get("sttProvider"),
        },
    }


@router.post("/{agent_id}/generate-greeting")
async def manual_generate_greeting(agent_id: str, current_user: dict = Depends(get_current_user)):
    """Manually trigger greeting re-generation based on agent persona."""
    require_db()
    _require_agent_manager(current_user)
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Agent not found")

    agent = await db.agents.find_one({"_id": oid})
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    config = agent.get("config") or {}
    persona_text = config.get("prompt") or config.get("persona") or agent.get("system_prompt") or ""

    if not persona_text:
        raise HTTPException(status_code=400, detail="Cannot generate greeting: Agent has no persona or prompt.")

    logger.info(f"[Greeting] Manual regeneration request for agent {agent_id}")
    cached_greeting = await generate_greeting_template(persona_text)

    if not cached_greeting:
        raise HTTPException(status_code=500, detail="Failed to generate greeting using LLM.")

    await db.agents.update_one(
        {"_id": oid},
        {"$set": {"cached_greeting": cached_greeting, "updated_at": datetime.now(timezone.utc)}},
    )
    return {
        "success": True,
        "data": {"cached_greeting": cached_greeting},
        "message": "Greeting generated and cached successfully.",
    }


@router.delete("/{agent_id}")
async def delete_agent(agent_id: str, current_user: dict = Depends(get_current_user)):
    """Delete agent by MongoDB _id."""
    require_db()
    _require_agent_manager(current_user)
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Agent not found")

    doc = await db.agents.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Agent not found")

    if current_user.get("role") == ROLE_RESELLER:
        if doc.get("managed_by") != "reseller" or doc.get("owner_id") != current_user.get("user_id"):
            raise HTTPException(status_code=404, detail="Agent not found")

    await db.agents.delete_one({"_id": oid})
    logger.info(f"Agent {agent_id} deleted by user {current_user['user_id']}")
    try:
        await log_audit(
            action="delete", resource_type="agent", resource_id=agent_id,
            performed_by=current_user.get("user_id") or "",
            performed_by_role=current_user.get("role"),
            details={"name": doc.get("name")},
        )
    except Exception:
        pass

    return {"success": True, "message": "Agent deleted successfully"}


@router.patch("/{agent_id}/toggle")
async def toggle_agent(agent_id: str, current_user: dict = Depends(get_current_user)):
    """Toggle agent active/inactive status."""
    require_db()
    _require_agent_manager(current_user)
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Agent not found")

    doc = await db.agents.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Agent not found")

    if current_user.get("role") == ROLE_RESELLER:
        if doc.get("managed_by") != "reseller" or doc.get("owner_id") != current_user.get("user_id"):
            raise HTTPException(status_code=404, detail="Agent not found")

    new_status = not doc.get("is_active", True)
    await db.agents.update_one(
        {"_id": oid},
        {"$set": {"is_active": new_status, "updated_at": datetime.now(timezone.utc)}},
    )
    updated = await db.agents.find_one({"_id": oid})
    status_label = "activated" if new_status else "deactivated"
    try:
        await log_audit(
            action="toggle", resource_type="agent", resource_id=agent_id,
            performed_by=current_user.get("user_id") or "",
            performed_by_role=current_user.get("role"),
            details={"is_active": new_status},
        )
    except Exception:
        pass

    return {"success": True, "data": {"agent": _agent_to_dict(updated)}, "message": f"Agent {status_label} successfully"}
