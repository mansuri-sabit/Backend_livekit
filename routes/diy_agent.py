"""
DIY Agent Routes (User-facing)
API endpoints for DIY users to manage their own agent configuration.
Mirrors node-src/routes/agent.diy.routes.ts
"""

import asyncio
import base64
import io
import json
import time
import wave
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from config import get_settings
from models import ROLE_ADMIN, ROLE_SUPER_ADMIN, CallRecord
from utils.db import db
from utils.security import get_current_user
from utils.logger import logger

router = APIRouter(prefix="/api/v1/agent/diy", tags=["DIY Agent"])

SUPPORTED_LANGUAGES = [
    "en", "hi", "es", "fr", "de", "pt", "it", "ja", "ko", "zh",
    "ta", "te", "mr", "gu", "bn", "kn", "ml"
]

PERSONA_TEMPLATES = {
    "lead_generation": (
        "You are a professional lead generation specialist. Your primary goal is to engage prospects, "
        "qualify leads, and collect key information.\n\n"
        "**Your Responsibilities:**\n"
        "- Introduce yourself and the company warmly\n"
        "- Ask qualifying questions to assess interest and fit\n"
        "- Collect contact details and key information\n"
        "- Schedule follow-up calls or demos when appropriate\n"
        "- Handle objections professionally and positively\n"
        "- Keep conversations concise and focused on value\n\n"
        "**Guidelines:**\n"
        "- Always be polite and professional\n"
        "- Do not pressure or rush prospects\n"
        "- If the prospect is not interested, thank them and end the call politely"
    ),
    "customer_support": (
        "You are a helpful and empathetic customer support agent. Your primary goal is to resolve "
        "customer issues, answer questions, and ensure customer satisfaction.\n\n"
        "**Your Responsibilities:**\n"
        "- Greet customers warmly and ask how you can help\n"
        "- Listen patiently to their concerns or questions\n"
        "- Show empathy and acknowledge their frustration if they're upset\n"
        "- Ask clarifying questions to fully understand the issue\n"
        "- Provide clear, step-by-step solutions or answers\n"
        "- Explain technical concepts in simple, easy to understand language\n\n"
        "**Guidelines:**\n"
        "- Always prioritize customer satisfaction\n"
        "- If you cannot resolve an issue, escalate appropriately\n"
        "- Follow up to ensure the customer's issue is fully resolved"
    ),
    "appointment_booking": (
        "You are an efficient appointment scheduling assistant. Your primary goal is to schedule "
        "appointments efficiently while providing excellent service.\n\n"
        "**Your Responsibilities:**\n"
        "- Greet callers and identify the purpose of their call\n"
        "- Check available appointment slots\n"
        "- Collect necessary information (name, contact, reason for appointment)\n"
        "- Confirm appointment details clearly\n"
        "- Send confirmation and reminders when possible\n\n"
        "**Guidelines:**\n"
        "- Be efficient but not rushed\n"
        "- Confirm all details before ending the call\n"
        "- Handle rescheduling and cancellations gracefully"
    ),
    "enquiry_handling": (
        "You are a knowledgeable enquiry handling agent. Your primary goal is to provide accurate "
        "information and guide customers to the right solutions.\n\n"
        "**Your Responsibilities:**\n"
        "- Answer product and service questions accurately\n"
        "- Provide pricing and package information when asked\n"
        "- Guide customers through options based on their needs\n"
        "- Capture interest and direct qualified prospects appropriately\n\n"
        "**Guidelines:**\n"
        "- Always provide accurate information\n"
        "- If unsure, say so and offer to find out\n"
        "- Keep responses clear and concise"
    ),
    "sales_outreach": (
        "You are a professional sales representative making outbound calls. Your primary goal is to "
        "spark interest and create opportunities.\n\n"
        "**Your Responsibilities:**\n"
        "- Introduce yourself and your company confidently\n"
        "- Clearly articulate the value proposition\n"
        "- Identify customer pain points and needs\n"
        "- Present relevant solutions and benefits\n"
        "- Handle objections with confidence\n"
        "- Secure next steps (demo, callback, meeting)\n\n"
        "**Guidelines:**\n"
        "- Be professional and respectful of the prospect's time\n"
        "- Focus on value, not features\n"
        "- Always get a clear next step before ending the call"
    ),
}

PROVIDER_VOICES: Dict[str, List[Dict]] = {
    "sarvam": [
        {"id": "priya", "name": "Priya (female)", "gender": "female"},
        {"id": "neha", "name": "Neha (female)", "gender": "female"},
        {"id": "ritu", "name": "Ritu (female)", "gender": "female"},
        {"id": "pooja", "name": "Pooja (female)", "gender": "female"},
        {"id": "simran", "name": "Simran (female)", "gender": "female"},
        {"id": "kavya", "name": "Kavya (female)", "gender": "female"},
        {"id": "ishita", "name": "Ishita (female)", "gender": "female"},
        {"id": "shreya", "name": "Shreya (female)", "gender": "female"},
        {"id": "roopa", "name": "Roopa (female)", "gender": "female"},
        {"id": "suhani", "name": "Suhani (female)", "gender": "female"},
        {"id": "kavitha", "name": "Kavitha (female)", "gender": "female"},
        {"id": "shubh", "name": "Shubh (male)", "gender": "male"},
        {"id": "rahul", "name": "Rahul (male)", "gender": "male"},
        {"id": "rohan", "name": "Rohan (male)", "gender": "male"},
        {"id": "aditya", "name": "Aditya (male)", "gender": "male"},
        {"id": "amit", "name": "Amit (male)", "gender": "male"},
        {"id": "kabir", "name": "Kabir (male)", "gender": "male"},
        {"id": "varun", "name": "Varun (male)", "gender": "male"},
        {"id": "manan", "name": "Manan (male)", "gender": "male"},
        {"id": "advait", "name": "Advait (male)", "gender": "male"},
        {"id": "anand", "name": "Anand (male)", "gender": "male"},
    ],
    "deepgram": [
        {"id": "aura-asteria-en", "name": "Asteria (female)", "gender": "female"},
        {"id": "aura-luna-en", "name": "Luna (female)", "gender": "female"},
        {"id": "aura-stella-en", "name": "Stella (female)", "gender": "female"},
        {"id": "aura-hera-en", "name": "Hera (female)", "gender": "female"},
        {"id": "aura-orion-en", "name": "Orion (male)", "gender": "male"},
        {"id": "aura-arcas-en", "name": "Arcas (male)", "gender": "male"},
        {"id": "aura-perseus-en", "name": "Perseus (male)", "gender": "male"},
        {"id": "aura-angus-en", "name": "Angus (male)", "gender": "male"},
    ],
    "elevenlabs": [
        {"id": "rachel", "name": "Rachel (female)", "gender": "female"},
        {"id": "domi", "name": "Domi (female)", "gender": "female"},
        {"id": "bella", "name": "Bella (female)", "gender": "female"},
        {"id": "elli", "name": "Elli (female)", "gender": "female"},
        {"id": "adam", "name": "Adam (male)", "gender": "male"},
        {"id": "drew", "name": "Drew (male)", "gender": "male"},
        {"id": "clyde", "name": "Clyde (male)", "gender": "male"},
        {"id": "paul", "name": "Paul (male)", "gender": "male"},
    ],
    "google": [
        {"id": "en-US-Standard-C", "name": "Standard C (female)", "gender": "female"},
        {"id": "en-US-Standard-E", "name": "Standard E (female)", "gender": "female"},
        {"id": "en-US-Neural2-C", "name": "Neural2 C (female)", "gender": "female"},
        {"id": "en-US-Neural2-F", "name": "Neural2 F (female)", "gender": "female"},
        {"id": "en-US-Standard-A", "name": "Standard A (male)", "gender": "male"},
        {"id": "en-US-Standard-B", "name": "Standard B (male)", "gender": "male"},
        {"id": "en-US-Neural2-A", "name": "Neural2 A (male)", "gender": "male"},
        {"id": "en-US-Neural2-D", "name": "Neural2 D (male)", "gender": "male"},
    ],
    "openai": [
        {"id": "nova", "name": "Nova (female)", "gender": "female"},
        {"id": "shimmer", "name": "Shimmer (female)", "gender": "female"},
        {"id": "alloy", "name": "Alloy", "gender": "neutral"},
        {"id": "echo", "name": "Echo (male)", "gender": "male"},
        {"id": "fable", "name": "Fable", "gender": "neutral"},
        {"id": "onyx", "name": "Onyx (male)", "gender": "male"},
    ],
    "cartesia": [
        {"id": "sonic-english", "name": "Sonic English", "gender": "neutral"},
        {"id": "sonic-multilingual", "name": "Sonic Multilingual", "gender": "neutral"},
    ],
}


# ─── Pydantic Models ───

class InboundOutboundConfig(BaseModel):
    greetingMessage: Optional[str] = None
    prompt: Optional[str] = None


class DIYTransferSettings(BaseModel):
    enabled: Optional[bool] = None
    humanAgentNumber: Optional[str] = None
    transferCondition: Optional[str] = None
    confirmationQuestion: Optional[str] = None
    confirmationKeywords: Optional[List[str]] = Field(None, max_length=10)
    negativeKeywords: Optional[List[str]] = Field(None, max_length=10)
    confirmationMessage: Optional[str] = None
    failureMessage: Optional[str] = None
    confirmationTimeoutMs: Optional[int] = None
    ringTimeoutSeconds: Optional[int] = None
    onConfirmationTimeout: Optional[str] = None
    fallbackBehavior: Optional[str] = None


class DIYAppointmentQuestions(BaseModel):
    nameQuestion: Optional[str] = None
    dateQuestion: Optional[str] = None
    timeQuestion: Optional[str] = None
    reasonQuestion: Optional[str] = None
    confirmationQuestion: Optional[str] = None


class DIYAppointmentBooking(BaseModel):
    enabled: Optional[bool] = None
    condition: Optional[str] = None
    questions: Optional[DIYAppointmentQuestions] = None
    successMessage: Optional[str] = None
    slotStartTime: Optional[str] = None
    slotEndTime: Optional[str] = None
    slotDurationMinutes: Optional[int] = None
    workingDays: Optional[List[int]] = None


class DIYProposalTemplate(BaseModel):
    displayName: Optional[str] = None
    templateName: Optional[str] = None
    language: Optional[str] = None
    apiKey: Optional[str] = None
    baseUrl: Optional[str] = None
    campaignName: Optional[str] = None
    keywords: Optional[List[str]] = None
    isDefault: Optional[bool] = None


class DIYProposalSettings(BaseModel):
    enabled: Optional[bool] = None
    triggerKeywords: Optional[str] = None
    confirmationQuestion: Optional[str] = None
    singleTemplateSendMessage: Optional[str] = None
    multipleTemplateQuestion: Optional[str] = None
    templates: Optional[List[DIYProposalTemplate]] = Field(None, max_length=10)


class DIYAgentUpdateBody(BaseModel):
    name: Optional[str] = Field(None, min_length=2, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    gender: Optional[str] = None
    supportedLanguages: Optional[List[str]] = None
    inboundConfig: Optional[InboundOutboundConfig] = None
    outboundConfig: Optional[InboundOutboundConfig] = None
    prompt: Optional[str] = Field(None, min_length=50, max_length=10000)
    callTransferEnabled: Optional[bool] = None
    leadKeywords: Optional[List[str]] = Field(None, max_length=50)
    appointmentBookingEnabled: Optional[bool] = None
    transferSettings: Optional[DIYTransferSettings] = None
    appointmentBooking: Optional[DIYAppointmentBooking] = None
    proposalSettings: Optional[DIYProposalSettings] = None


class TestCallLogBody(BaseModel):
    callSid: str
    duration: float
    status: str


class VoiceGenderConfig(BaseModel):
    provider: str
    voiceId: str
    voiceName: str
    speakingRate: Optional[float] = 1.1
    model: Optional[str] = None
    settings: Optional[Dict[str, Any]] = None


class UserVoiceConfigBody(BaseModel):
    male: VoiceGenderConfig
    female: VoiceGenderConfig


# ─── Helpers ───

def _debug_log(hypothesis_id: str, location: str, message: str, data: Dict[str, Any], run_id: str = "pre-fix") -> None:
    """Best-effort NDJSON append logger for debugging."""
    try:
        log_path = Path(__file__).resolve().parents[1] / "debug-diy.log"
        payload = {
            "runId": run_id, "hypothesisId": hypothesis_id, "location": location,
            "message": message, "data": data, "timestamp": int(time.time() * 1000),
        }
        with log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=True) + "\n")
    except Exception:
        pass


async def _get_voice_config_for_user(user_id: str, gender: str = "female") -> dict:
    """Resolve voice config: user override → global admin → hardcoded default."""
    user_voice = await db.diy_voice_config.find_one({"type": "user", "user_id": user_id})
    if user_voice:
        gender_config = user_voice.get(gender)
        if gender_config:
            vc_settings = dict(gender_config.get("settings") or {})
            if "pace" not in vc_settings and "pace_min" not in vc_settings:
                vc_settings["pace"] = gender_config.get("speakingRate", 1.0)
            return {
                "provider": gender_config.get("provider", "sarvam"),
                "voiceId": gender_config.get("voiceId", "priya"),
                "voiceName": gender_config.get("voiceName", "Priya (female)"),
                "model": gender_config.get("model"),
                "settings": vc_settings,
            }
    global_voice = await db.diy_voice_config.find_one({"type": "global"})
    if global_voice:
        gender_config = global_voice.get(gender)
        if gender_config:
            vc_settings = dict(gender_config.get("settings") or {})
            if "pace" not in vc_settings and "pace_min" not in vc_settings:
                vc_settings["pace"] = gender_config.get("speakingRate", 1.0)
            return {
                "provider": gender_config.get("provider", "sarvam"),
                "voiceId": gender_config.get("voiceId", "priya"),
                "voiceName": gender_config.get("voiceName", "Priya (female)"),
                "model": gender_config.get("model"),
                "settings": vc_settings,
            }
    return {"provider": "sarvam", "voiceId": "priya", "voiceName": "Priya (female)", "settings": {"pace_min": 1.0, "pace_max": 1.2}}


async def _agent_to_dict(doc: dict) -> dict:
    appointment_booking = doc.get("appointment_booking") or {}
    call_transfer = doc.get("call_transfer_settings") or {}
    apt_questions = appointment_booking.get("questions") or {}
    proposal_settings = doc.get("proposal_settings") or {}

    user_id = doc.get("user_id")
    voice_config = await _get_voice_config_for_user(user_id, doc.get("gender", "female")) if user_id else {
        "provider": "sarvam", "voiceId": "priya", "voiceName": "Priya (female)", "settings": {"pace_min": 1.0, "pace_max": 1.2}
    }

    return {
        "id": str(doc["_id"]),
        "userId": doc.get("user_id", ""),
        "name": doc.get("name", ""),
        "description": doc.get("description", ""),
        "gender": doc.get("gender", "female"),
        "approvalStatus": doc.get("approval_status", "draft"),
        "version": doc.get("version", 1),
        "supportedLanguages": doc.get("supported_languages", ["en"]),
        "config": {
            "prompt": doc.get("prompt", ""),
            "inboundConfig": doc.get("inbound_config", {"greetingMessage": "Hello! How can I help you today?"}),
            "outboundConfig": doc.get("outbound_config", {"greetingMessage": "Hello! This is a call from our service."}),
            "callTransferEnabled": doc.get("call_transfer_enabled", call_transfer.get("enabled", False)),
            "llm": {"model": "gpt-4o-mini", "temperature": 0.7, "maxTokens": 150},
            "voice": voice_config,
            "leadKeywords": doc.get("lead_keywords", []),
            "transferSettings": {
                "enabled": call_transfer.get("enabled", False),
                "humanAgentNumber": call_transfer.get("human_agent_number") or "",
                "transferCondition": call_transfer.get("transfer_condition") or "",
                "confirmationQuestion": call_transfer.get("confirmation_question") or "Would you like me to transfer you to a human agent?",
                "confirmationKeywords": call_transfer.get("confirmation_keywords") or ["yes", "sure", "ok", "okay", "yeah", "please"],
                "negativeKeywords": call_transfer.get("negative_keywords") or ["no", "not now", "later", "cancel"],
                "confirmationMessage": call_transfer.get("confirmation_message") or "Transferring you to a human agent now. Please hold.",
                "failureMessage": call_transfer.get("failure_message") or "I'm sorry, but our agents are currently unavailable. How else can I help you?",
                "confirmationTimeoutMs": call_transfer.get("confirmation_timeout_ms", 15000),
                "ringTimeoutSeconds": call_transfer.get("ring_timeout_seconds", 30),
                "onConfirmationTimeout": call_transfer.get("on_confirmation_timeout", "assume_no"),
                "fallbackBehavior": call_transfer.get("fallback_behavior", "continue"),
            },
            "appointmentBooking": {
                "enabled": appointment_booking.get("enabled", False),
                "condition": appointment_booking.get("condition") or "",
                "questions": {
                    "nameQuestion": apt_questions.get("name_question") or "What's your name?",
                    "dateQuestion": apt_questions.get("date_question") or "What date would you like to schedule the appointment?",
                    "timeQuestion": apt_questions.get("time_question") or "What time works for you?",
                    "reasonQuestion": apt_questions.get("reason_question"),
                    "confirmationQuestion": apt_questions.get("confirmation_question") or "I've booked your appointment for [date] at [time]. Is this correct?",
                },
                "successMessage": appointment_booking.get("success_message") or "Great! Your appointment is confirmed for [date] at [time]. We'll see you then!",
                "slotStartTime": appointment_booking.get("slot_start_time") or "09:00",
                "slotEndTime": appointment_booking.get("slot_end_time") or "17:00",
                "slotDurationMinutes": appointment_booking.get("slot_duration_minutes", 30),
                "workingDays": appointment_booking.get("working_days") or [1, 2, 3, 4, 5],
            },
        },
        "proposalSettings": {
            "enabled": proposal_settings.get("enabled", False),
            "triggerKeywords": proposal_settings.get("trigger_keywords") or "proposal, send proposal, send me proposal, send details, share details",
            "confirmationQuestion": proposal_settings.get("confirmation_question") or "Would you like me to send you the proposal on WhatsApp?",
            "singleTemplateSendMessage": proposal_settings.get("single_template_send_message") or "Sure! I'll send you the proposal on WhatsApp right now.",
            "multipleTemplateQuestion": proposal_settings.get("multiple_template_question") or "Which proposal would you like? {templateNames}",
            "templates": proposal_settings.get("templates") or [],
        },
        "testCallLogs": doc.get("test_call_logs", []),
        "hasCompletedTestCall": doc.get("has_completed_test_call", False),
        "submittedAt": doc.get("submitted_at"),
        "approvedAt": doc.get("approved_at"),
        "rejectedAt": doc.get("rejected_at"),
        "rejectionReason": doc.get("rejection_reason"),
        "createdAt": doc.get("created_at"),
        "updatedAt": doc.get("updated_at"),
    }


async def _get_user_agent(user_id: str) -> Optional[dict]:
    return await db.diy_agents.find_one({"user_id": user_id})


async def _require_user_agent(user_id: str) -> dict:
    agent = await _get_user_agent(user_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found. Please contact admin to set up your agent.")
    return agent


def _require_editable(agent: dict):
    status = agent.get("approval_status", "draft")
    if status not in ("draft", "rejected"):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot edit agent with status: {status}. Only draft or rejected agents can be edited."
        )


async def _generate_sarvam_sample(voice_id: str, text: str, api_key: str, language: str = "en-IN", pace: float = 1.0) -> bytes:
    """Generate Sarvam TTS audio bytes via WebSocket streaming."""
    import websockets

    ws_url = "wss://api.sarvam.ai/text-to-speech/ws?model=bulbul:v3&send_completion_event=true"
    headers = {"Api-Subscription-Key": api_key}
    audio_chunks: list[bytes] = []

    async with websockets.connect(ws_url, additional_headers=headers) as ws:
        import json as _json
        await ws.send(_json.dumps({"type": "config", "data": {"speaker": voice_id, "target_language_code": language, "pace": pace}}))
        await ws.send(_json.dumps({"type": "text", "data": {"text": text}}))
        await ws.send(_json.dumps({"type": "flush"}))

        deadline = asyncio.get_event_loop().time() + 10
        try:
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                raw_msg = await asyncio.wait_for(ws.recv(), timeout=remaining)
                msg = _json.loads(raw_msg)
                audio_data = (msg.get("data") or {}).get("audio") or msg.get("audio")
                if msg.get("type") == "audio" and audio_data:
                    audio_chunks.append(base64.b64decode(audio_data))
                elif msg.get("type") == "event" and (msg.get("data") or {}).get("event_type") == "final":
                    break
                elif msg.get("type") == "error":
                    raise RuntimeError(f"Sarvam error: {(msg.get('data') or {}).get('message') or str(msg)}")
        except asyncio.TimeoutError:
            pass

    raw_pcm = b"".join(audio_chunks)
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(24000)
        w.writeframes(raw_pcm)
    return buf.getvalue()


async def _generate_google_sample(voice_id: str, text: str, creds_path: str, language_code: str = "en-IN") -> bytes:
    """Generate Google Cloud TTS audio bytes."""
    from google.cloud import texttospeech

    def _sync_call() -> bytes:
        client = texttospeech.TextToSpeechClient.from_service_account_file(creds_path)
        synthesis_input = texttospeech.SynthesisInput(text=text)
        voice = texttospeech.VoiceSelectionParams(language_code=language_code, name=voice_id)
        audio_config = texttospeech.AudioConfig(audio_encoding=texttospeech.AudioEncoding.LINEAR16, sample_rate_hertz=24000)
        response = client.synthesize_speech(input=synthesis_input, voice=voice, audio_config=audio_config)
        raw = response.audio_content
        buf = io.BytesIO()
        with wave.open(buf, "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)
            w.setframerate(24000)
            w.writeframes(raw)
        return buf.getvalue()

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_call)


def _invalidate_agent_cache(user_id: str):
    """Fire-and-forget Redis cache invalidation. Logs warning if Redis unavailable; does not fail the request."""
    async def _do():
        try:
            from utils.db import redis_client, AGENT_CACHE_PREFIX
            if redis_client:
                await redis_client.delete(f"{AGENT_CACHE_PREFIX}{user_id}")
        except Exception as e:
            logger.warning("Agent cache invalidation failed (Redis): %s", e)
    asyncio.ensure_future(_do())


# ─── Routes ───

@router.get("/templates")
async def get_persona_templates(current_user: dict = Depends(get_current_user)):
    """Return available persona templates."""
    return {
        "success": True,
        "data": {key: {"id": key, "prompt": val} for key, val in PERSONA_TEMPLATES.items()}
    }


@router.get("")
async def get_my_agent(current_user: dict = Depends(get_current_user)):
    """GET /api/v1/agent/diy — Get the current user's agent."""
    user_id = current_user["user_id"]
    agent = await _get_user_agent(user_id)
    if not agent:
        return {"success": False, "message": "Agent not found. Please contact admin.", "data": None}
    return {"success": True, "data": await _agent_to_dict(agent)}


@router.put("")
async def update_my_agent(
    body: DIYAgentUpdateBody,
    current_user: dict = Depends(get_current_user),
):
    """PUT /api/v1/agent/diy — Update agent config (only allowed for draft/rejected)."""
    user_id = current_user["user_id"]
    try:
        agent = await _require_user_agent(user_id)
        _require_editable(agent)

        updates: Dict[str, Any] = {"updated_at": datetime.now(timezone.utc)}

        if body.name is not None:
            updates["name"] = body.name.strip()
        if body.description is not None:
            updates["description"] = body.description.strip()
        if body.gender is not None:
            if body.gender not in ("male", "female"):
                raise HTTPException(status_code=400, detail="gender must be 'male' or 'female'")
            updates["gender"] = body.gender
        if body.supportedLanguages is not None:
            if len(body.supportedLanguages) < 1 or len(body.supportedLanguages) > 2:
                raise HTTPException(status_code=400, detail="Select 1 or 2 supported languages")
            updates["supported_languages"] = body.supportedLanguages
        if body.prompt is not None:
            updates["prompt"] = body.prompt.strip()

        if body.inboundConfig is not None:
            existing = agent.get("inbound_config") or {}
            if body.inboundConfig.greetingMessage is not None:
                if len(body.inboundConfig.greetingMessage.strip()) < 5:
                    raise HTTPException(status_code=400, detail="Inbound greeting must be at least 5 characters")
                existing["greetingMessage"] = body.inboundConfig.greetingMessage.strip()
            if body.inboundConfig.prompt is not None:
                existing["prompt"] = body.inboundConfig.prompt.strip()
            updates["inbound_config"] = existing

        if body.outboundConfig is not None:
            existing = agent.get("outbound_config") or {}
            if body.outboundConfig.greetingMessage is not None:
                if len(body.outboundConfig.greetingMessage.strip()) < 5:
                    raise HTTPException(status_code=400, detail="Outbound greeting must be at least 5 characters")
                existing["greetingMessage"] = body.outboundConfig.greetingMessage.strip()
            if body.outboundConfig.prompt is not None:
                existing["prompt"] = body.outboundConfig.prompt.strip()
            updates["outbound_config"] = existing

        if body.callTransferEnabled is not None:
            updates["call_transfer_enabled"] = body.callTransferEnabled
        if body.leadKeywords is not None:
            keywords = [str(k).strip() for k in body.leadKeywords if str(k).strip()]
            if len(keywords) > 50:
                raise HTTPException(status_code=400, detail="Maximum 50 lead keywords allowed")
            updates["lead_keywords"] = keywords
        if body.appointmentBookingEnabled is not None:
            existing_apt = dict(agent.get("appointment_booking") or {})
            existing_apt["enabled"] = body.appointmentBookingEnabled
            updates["appointment_booking"] = existing_apt

        if body.transferSettings is not None:
            existing_t = dict(agent.get("call_transfer_settings") or {})
            t = body.transferSettings
            if t.enabled is not None:
                existing_t["enabled"] = t.enabled
                updates["call_transfer_enabled"] = t.enabled
            if t.humanAgentNumber is not None:
                existing_t["human_agent_number"] = t.humanAgentNumber.strip()
            if t.transferCondition is not None:
                existing_t["transfer_condition"] = t.transferCondition.strip()
            if t.confirmationQuestion is not None:
                existing_t["confirmation_question"] = t.confirmationQuestion.strip()
            if t.confirmationKeywords is not None:
                existing_t["confirmation_keywords"] = [str(k).strip() for k in t.confirmationKeywords if str(k).strip()][:10]
            if t.negativeKeywords is not None:
                existing_t["negative_keywords"] = [str(k).strip() for k in t.negativeKeywords if str(k).strip()][:10]
            if t.confirmationMessage is not None:
                existing_t["confirmation_message"] = t.confirmationMessage.strip()
            if t.failureMessage is not None:
                existing_t["failure_message"] = t.failureMessage.strip()
            if t.confirmationTimeoutMs is not None:
                existing_t["confirmation_timeout_ms"] = int(t.confirmationTimeoutMs)
            if t.ringTimeoutSeconds is not None:
                existing_t["ring_timeout_seconds"] = int(t.ringTimeoutSeconds)
            if t.onConfirmationTimeout is not None:
                existing_t["on_confirmation_timeout"] = t.onConfirmationTimeout.strip()
            if t.fallbackBehavior is not None:
                existing_t["fallback_behavior"] = t.fallbackBehavior.strip()
            updates["call_transfer_settings"] = existing_t

        if body.appointmentBooking is not None:
            existing_apt = dict(agent.get("appointment_booking") or {})
            apt = body.appointmentBooking
            if apt.enabled is not None:
                existing_apt["enabled"] = apt.enabled
            if apt.condition is not None:
                existing_apt["condition"] = apt.condition.strip()
            if apt.questions is not None:
                q = apt.questions
                existing_q = dict(existing_apt.get("questions") or {})
                if q.nameQuestion is not None:
                    existing_q["name_question"] = q.nameQuestion.strip()
                if q.dateQuestion is not None:
                    existing_q["date_question"] = q.dateQuestion.strip()
                if q.timeQuestion is not None:
                    existing_q["time_question"] = q.timeQuestion.strip()
                if q.reasonQuestion is not None:
                    existing_q["reason_question"] = (q.reasonQuestion or "").strip() or None
                if q.confirmationQuestion is not None:
                    existing_q["confirmation_question"] = q.confirmationQuestion.strip()
                existing_apt["questions"] = existing_q
            if apt.successMessage is not None:
                existing_apt["success_message"] = apt.successMessage.strip()
            if apt.slotStartTime is not None:
                existing_apt["slot_start_time"] = apt.slotStartTime.strip()
            if apt.slotEndTime is not None:
                existing_apt["slot_end_time"] = apt.slotEndTime.strip()
            if apt.slotDurationMinutes is not None:
                existing_apt["slot_duration_minutes"] = int(apt.slotDurationMinutes)
            if apt.workingDays is not None:
                existing_apt["working_days"] = [int(d) for d in apt.workingDays][:7]
            updates["appointment_booking"] = existing_apt

        if body.proposalSettings is not None:
            existing_ps = agent.get("proposal_settings") or {}
            ps = body.proposalSettings
            if ps.enabled is not None:
                existing_ps["enabled"] = ps.enabled
            if ps.triggerKeywords is not None:
                existing_ps["trigger_keywords"] = ps.triggerKeywords.strip()
            if ps.confirmationQuestion is not None:
                existing_ps["confirmation_question"] = ps.confirmationQuestion.strip()
            if ps.singleTemplateSendMessage is not None:
                existing_ps["single_template_send_message"] = ps.singleTemplateSendMessage.strip()
            if ps.multipleTemplateQuestion is not None:
                existing_ps["multiple_template_question"] = ps.multipleTemplateQuestion.strip()
            if ps.templates is not None:
                existing_ps["templates"] = [
                    {
                        **({"displayName": t.displayName.strip()} if t.displayName and t.displayName.strip() else {}),
                        "templateName": (t.templateName or "").strip(),
                        "language": (t.language or "English").strip(),
                        "baseUrl": (t.baseUrl or "https://backend.api-wa.co").strip(),
                        "campaignName": (t.campaignName or "").strip(),
                        "keywords": [str(k).strip() for k in (t.keywords or []) if str(k).strip()],
                        "isDefault": bool(t.isDefault),
                        **({"apiKey": t.apiKey.strip()} if t.apiKey else {}),
                    }
                    for t in ps.templates
                ]
            updates["proposal_settings"] = existing_ps

        await db.diy_agents.update_one({"user_id": user_id}, {"$set": updates})

        # Keep db.agents in sync for fields the pipeline reads directly
        agents_sync: Dict[str, Any] = {}
        if "proposal_settings" in updates:
            agents_sync["proposal_settings"] = updates["proposal_settings"]
            agents_sync["config.proposalSettings"] = updates["proposal_settings"]
        if "voicemail_detection" in updates:
            agents_sync["config.voicemailDetection"] = updates["voicemail_detection"]
        if agents_sync:
            await db.agents.update_one({"user_id": user_id}, {"$set": agents_sync})

        _invalidate_agent_cache(user_id)

        updated = await _get_user_agent(user_id)
        logger.info(f"[DIY Agent] Updated: user={user_id}")
        return {"success": True, "data": await _agent_to_dict(updated), "message": "Agent updated successfully"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(f"[DIY Agent] update_my_agent error: user={user_id}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/submit-for-review")
async def submit_for_review(current_user: dict = Depends(get_current_user)):
    """POST /api/v1/agent/diy/submit-for-review — Submit agent for admin review."""
    user_id = current_user["user_id"]
    agent = await _require_user_agent(user_id)

    status = agent.get("approval_status", "draft")
    if status == "pending_approval":
        raise HTTPException(status_code=400, detail="Agent is already submitted for review.")
    if status == "approved":
        raise HTTPException(status_code=400, detail="Agent is already approved. Use 'request changes' to edit.")
    if status not in ("draft", "rejected"):
        raise HTTPException(status_code=400, detail=f"Cannot submit agent with status: {status}")

    prompt = (agent.get("prompt") or "").strip()
    if len(prompt) < 50:
        raise HTTPException(status_code=400, detail="Agent prompt must be at least 50 characters before submitting.")

    inbound_greeting = (agent.get("inbound_config") or {}).get("greetingMessage", "")
    outbound_greeting = (agent.get("outbound_config") or {}).get("greetingMessage", "")
    if len(inbound_greeting.strip()) < 5:
        raise HTTPException(status_code=400, detail="Inbound greeting is required before submitting.")
    if len(outbound_greeting.strip()) < 5:
        raise HTTPException(status_code=400, detail="Outbound greeting is required before submitting.")

    now = datetime.now(timezone.utc)
    current_version = agent.get("version") or 1
    if status == "rejected":
        new_version = current_version + 1
    else:
        existing_versions = await db.diy_agent_versions.count_documents({"agent_id": agent["_id"]})
        new_version = current_version if existing_versions == 0 else current_version + 1

    snapshot = {
        "agent_id": agent["_id"], "user_id": user_id, "version": new_version,
        "config_snapshot": {
            "prompt": agent.get("prompt", ""),
            "inbound_config": agent.get("inbound_config", {}),
            "outbound_config": agent.get("outbound_config", {}),
            "supported_languages": agent.get("supported_languages", ["en"]),
            "gender": agent.get("gender", "female"),
            "name": agent.get("name", ""),
        },
        "approval_status": "pending_approval",
        "created_at": now,
    }
    await db.diy_agent_versions.update_one(
        {"agent_id": agent["_id"], "version": new_version},
        {"$set": snapshot},
        upsert=True,
    )

    user_doc = await db.users.find_one({"_id": ObjectId(user_id)})
    user_name = (user_doc or {}).get("name") or (user_doc or {}).get("email", "Unknown")
    user_email = (user_doc or {}).get("email", "")

    await db.agent_review_queue.update_one(
        {"agent_id": agent["_id"]},
        {"$set": {
            "agent_id": agent["_id"], "user_id": user_id,
            "user_name": user_name, "user_email": user_email,
            "agent_name": agent.get("name", ""),
            "greeting_preview": inbound_greeting[:200],
            "change_type": "update" if status == "rejected" else "new",
            "submitted_at": now, "status": "pending",
            "reviewed_by": None, "reviewed_at": None, "review_notes": None,
        }},
        upsert=True,
    )

    await db.diy_agents.update_one(
        {"user_id": user_id},
        {"$set": {"approval_status": "pending_approval", "version": new_version, "submitted_at": now, "updated_at": now}}
    )

    _invalidate_agent_cache(user_id)
    updated = await _get_user_agent(user_id)
    logger.info(f"[DIY Agent] Submitted for review: user={user_id} version={new_version}")
    return {
        "success": True,
        "data": await _agent_to_dict(updated),
        "message": "Agent submitted for review successfully. You will be notified once the review is complete."
    }


@router.post("/request-changes")
async def request_changes(current_user: dict = Depends(get_current_user)):
    """POST /api/v1/agent/diy/request-changes — Unlock approved agent for editing."""
    user_id = current_user["user_id"]
    agent = await _require_user_agent(user_id)

    if agent.get("approval_status", "draft") != "approved":
        raise HTTPException(status_code=400, detail="Can only request changes on an approved agent.")

    now = datetime.now(timezone.utc)
    await db.diy_agents.update_one(
        {"user_id": user_id},
        {"$set": {"approval_status": "draft", "updated_at": now}}
    )
    _invalidate_agent_cache(user_id)
    updated = await _get_user_agent(user_id)
    logger.info(f"[DIY Agent] Unlocked for editing: user={user_id}")
    return {
        "success": True,
        "data": await _agent_to_dict(updated),
        "message": "Agent unlocked for editing. You can now make changes and resubmit for approval."
    }


@router.post("/test-call/initiate")
async def initiate_test_call(body: dict, current_user: dict = Depends(get_current_user)):
    """POST /api/v1/agent/diy/test-call/initiate — Initiate a test call via assigned phone."""
    user_id = current_user["user_id"]
    test_phone_number = (body.get("testPhoneNumber") or "").strip()
    if not test_phone_number:
        raise HTTPException(status_code=400, detail="testPhoneNumber is required")

    agent = await _require_user_agent(user_id)

    user_doc = await db.users.find_one({"_id": ObjectId(user_id)})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")

    assigned_phone_id = user_doc.get("assigned_phone_id")
    if not assigned_phone_id:
        raise HTTPException(status_code=400, detail="No phone number assigned to your account. Please contact admin.")

    try:
        phone_doc = await db.phones.find_one({"_id": ObjectId(str(assigned_phone_id))})
    except Exception:
        phone_doc = None

    if not phone_doc:
        raise HTTPException(status_code=400, detail="Assigned phone not found. Please contact admin.")

    from_number = phone_doc.get("number", "")
    if not from_number:
        raise HTTPException(status_code=400, detail="Assigned phone has no number configured.")

    exotel_data = phone_doc.get("exotel_data") or {}
    if not exotel_data.get("api_key") or not exotel_data.get("sid"):
        raise HTTPException(status_code=400, detail="Phone is not configured with Exotel credentials.")

    try:
        settings = get_settings()
        api_key = exotel_data.get("api_key", "")
        api_token = exotel_data.get("api_token", "")
        account_sid = exotel_data.get("sid", "")
        subdomain = (exotel_data.get("subdomain") or settings.EXOTEL_SUBDOMAIN or "api.exotel.com").strip()
        app_id = (exotel_data.get("app_id") or getattr(settings, "EXOTEL_APP_ID", "") or "").strip()

        if not app_id:
            raise HTTPException(status_code=503, detail="EXOTEL_APP_ID not configured for this phone.")
        ngrok_url = (getattr(settings, "NGROK_URL", None) or "").rstrip("/")
        if not ngrok_url:
            raise HTTPException(status_code=503, detail="Server callback URL not configured.")

        digits = "".join(c for c in test_phone_number if c.isdigit())
        from_for_exotel = ("0" + digits[-10:]) if len(digits) >= 10 else test_phone_number
        exoml_url = f"http://my.exotel.com/{account_sid}/exoml/start_voice/{app_id}"

        payload = {
            "From": from_for_exotel,
            "CallerId": from_number,
            "Url": exoml_url,
            "CallType": "trans",
            "StatusCallback": f"{ngrok_url}/api/v1/exotel/voice/status-callback",
        }

        api_url = f"https://{subdomain}/v1/Accounts/{account_sid}/Calls/connect.json"
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(api_url, data=payload, auth=httpx.BasicAuth(username=api_key, password=api_token))

        if resp.status_code not in (200, 201):
            raise HTTPException(status_code=502, detail=f"Exotel call failed: {resp.text[:200]}")

        resp_data = resp.json()
        call_info = resp_data.get("Call", resp_data)
        call_sid = (call_info.get("Sid") or "").strip()
        call_status = (call_info.get("Status") or "initiated").strip().lower()

        if not call_sid:
            raise HTTPException(status_code=502, detail="Exotel did not return call SID.")

        call_record = CallRecord(
            call_id=call_sid,
            user_id=user_id,
            agent_id=str(agent.get("_id", "")) or "diy",
            from_number=from_number,
            to_number=test_phone_number,
            direction="outbound",
            status=call_status,
        )
        call_dict = call_record.model_dump(by_alias=True)
        call_dict["is_test"] = True
        await db.calls.insert_one(call_dict)

        logger.info(f"[DIY Agent] Test call initiated: user={user_id} to={test_phone_number} call_sid={call_sid}")
        return {
            "success": True,
            "data": {"message": "Test call initiated successfully. You should receive a call shortly.", "callSid": call_sid}
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[DIY Agent] Test call error: user={user_id}")
        raise HTTPException(status_code=500, detail=f"Failed to initiate test call: {str(e)}")


@router.post("/test-call")
async def log_test_call(body: TestCallLogBody, current_user: dict = Depends(get_current_user)):
    """POST /api/v1/agent/diy/test-call — Log a completed test call."""
    user_id = current_user["user_id"]
    agent = await _require_user_agent(user_id)

    log_entry = {"callSid": body.callSid, "duration": body.duration, "status": body.status, "loggedAt": datetime.now(timezone.utc)}
    has_completed = body.status == "completed"

    await db.diy_agents.update_one(
        {"user_id": user_id},
        {
            "$push": {"test_call_logs": log_entry},
            "$set": {"updated_at": datetime.now(timezone.utc), **({"has_completed_test_call": True} if has_completed else {})},
        }
    )
    updated = await _get_user_agent(user_id)
    return {"success": True, "data": await _agent_to_dict(updated), "message": "Test call logged successfully"}


@router.get("/versions")
async def get_versions(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    current_user: dict = Depends(get_current_user),
):
    """GET /api/v1/agent/diy/versions — Get agent version history."""
    user_id = current_user["user_id"]
    agent = await _require_user_agent(user_id)

    skip = (page - 1) * limit
    cursor = db.diy_agent_versions.find({"agent_id": agent["_id"]}).sort("version", -1).skip(skip).limit(limit)
    versions = []
    async for v in cursor:
        v["_id"] = str(v["_id"])
        v["agent_id"] = str(v["agent_id"])
        versions.append(v)

    total = await db.diy_agent_versions.count_documents({"agent_id": agent["_id"]})
    return {
        "success": True,
        "data": {
            "currentVersion": agent.get("version", 1),
            "approvalStatus": agent.get("approval_status", "draft"),
            "versions": versions,
            "pagination": {"page": page, "limit": limit, "total": total, "pages": max(1, (total + limit - 1) // limit)},
        }
    }


# ─── Voice Config ───

@router.get("/voice-config")
async def get_my_voice_config(current_user: dict = Depends(get_current_user)):
    """GET /api/v1/agent/diy/voice-config — Get user's voice config (override or global default)."""
    user_id = current_user["user_id"]
    override = await db.diy_voice_config.find_one({"type": "user", "user_id": user_id})
    global_cfg = await db.diy_voice_config.find_one({"type": "global"})

    def _sanitize(gender_cfg: dict) -> dict:
        if not gender_cfg:
            return gender_cfg
        provider = gender_cfg.get("provider", "sarvam")
        voice_id = gender_cfg.get("voiceId", "")
        voices = PROVIDER_VOICES.get(provider, [])
        valid = next((v for v in voices if v["id"] == voice_id), None)
        if not valid and voices:
            valid = voices[0]
        if valid:
            return {**gender_cfg, "voiceId": valid["id"], "voiceName": valid["name"]}
        return gender_cfg

    def _cfg(doc):
        if not doc:
            return None
        return {"male": _sanitize(doc.get("male")), "female": _sanitize(doc.get("female"))}

    return {
        "success": True,
        "data": {
            "override": _cfg(override),
            "global": _cfg(global_cfg),
            "effective": _cfg(override) or _cfg(global_cfg) or {
                "male": {"provider": "sarvam", "voiceId": "shubh", "voiceName": "Shubh (male)", "speakingRate": 1.1, "settings": {"pace_min": 1.0, "pace_max": 1.2}},
                "female": {"provider": "sarvam", "voiceId": "priya", "voiceName": "Priya (female)", "speakingRate": 1.1, "settings": {"pace_min": 1.0, "pace_max": 1.2}},
            },
            "providers": PROVIDER_VOICES,
        }
    }


@router.put("/voice-config")
async def save_my_voice_config(body: UserVoiceConfigBody, current_user: dict = Depends(get_current_user)):
    """PUT /api/v1/agent/diy/voice-config — Save user's voice override."""
    user_id = current_user["user_id"]
    now = datetime.now(timezone.utc)
    config = {
        "male": {**body.male.model_dump(), "speakingRate": body.male.speakingRate or 1.1},
        "female": {**body.female.model_dump(), "speakingRate": body.female.speakingRate or 1.1},
    }
    await db.diy_voice_config.update_one(
        {"type": "user", "user_id": user_id},
        {"$set": {"type": "user", "user_id": user_id, **config, "updated_at": now}},
        upsert=True,
    )
    _invalidate_agent_cache(user_id)
    logger.info(f"[DIY Voice] User {user_id} saved voice config")
    return {"success": True, "message": "Voice config saved", "data": config}


@router.get("/voice-config/providers")
async def get_voice_providers(current_user: dict = Depends(get_current_user)):
    """GET /api/v1/agent/diy/voice-config/providers — List providers and their voices."""
    return {"success": True, "data": {"providers": PROVIDER_VOICES}}


@router.get("/voice-sample/{provider}/{voice_id}")
async def get_diy_voice_sample(
    provider: str,
    voice_id: str,
    current_user: dict = Depends(get_current_user),
):
    """
    GET /api/v1/agent/diy/voice-sample/:provider/:voiceId
    Returns a presigned S3 URL for a voice sample. Generates and caches on first request.
    Supported providers: sarvam, google.
    """
    from services.s3_service import get_voice_sample_presigned_url, upload_voice_sample

    settings = get_settings()
    provider_norm = provider.strip().lower()
    voice_norm = voice_id.strip().lower()

    url = get_voice_sample_presigned_url(provider_norm, voice_norm)
    if url:
        return {"success": True, "url": url, "cached": True}

    if provider_norm == "sarvam":
        api_key = (getattr(settings, "SARVAM_API_KEY", None) or "").strip()
        if not api_key:
            raise HTTPException(status_code=503, detail="Sarvam API key not configured.")
        try:
            audio_bytes = await _generate_sarvam_sample(
                voice_id=voice_norm,
                text="Hello! I am your AI voice assistant. How can I help you today?",
                api_key=api_key, language="en-IN", pace=1.1,
            )
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Sarvam TTS failed: {e}")
        upload_voice_sample(provider_norm, voice_norm, audio_bytes, "audio/wav")

    elif provider_norm == "google":
        creds_path = (getattr(settings, "GOOGLE_APPLICATION_CREDENTIALS", None) or "").strip()
        if not creds_path:
            raise HTTPException(status_code=503, detail="Google credentials not configured.")
        try:
            audio_bytes = await _generate_google_sample(
                voice_id=voice_id.strip(),
                text="Hello! I am your AI voice assistant. How can I help you today?",
                creds_path=creds_path, language_code="en-IN",
            )
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Google TTS failed: {e}")
        upload_voice_sample(provider_norm, voice_id.strip(), audio_bytes, "audio/wav")

    else:
        raise HTTPException(status_code=501, detail=f"Voice preview not available for provider: {provider_norm}")

    url = get_voice_sample_presigned_url(provider_norm, voice_norm)
    if not url:
        raise HTTPException(status_code=502, detail="Failed to generate voice sample URL.")
    return {"success": True, "url": url, "cached": False}
