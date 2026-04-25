"""
Demo dashboard API — virtual numbers, campaign helpers, TTS config and voice preview.

Voice preview:
  POST /api/demo/voice-preview
  Body: { voiceId, text, provider ("sarvam"|"google"), language (optional BCP-47/short code), settings (optional) }
  Returns binary audio/wav.

NOTE: llm_output_sanitizer is a Pipecat artifact (deleted). This file has no Pipecat dependencies.
"""

import base64
import csv
import io
import os
import re
import wave
from typing import Optional

from fastapi import APIRouter, Depends, Request, UploadFile
from fastapi.responses import Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from config import get_settings
from models import ROLE_DEMO
from utils.db import db
from utils.demo_voice import get_demo_voice_config
from utils.security import decode_token

_security_optional = HTTPBearer(auto_error=False)

router = APIRouter(prefix="/api/demo", tags=["Demo"])


class VoicePreviewBody(BaseModel):
    voiceId: str = ""
    text: str = ""
    provider: str = "sarvam"
    language: Optional[str] = None
    settings: Optional[dict] = None


ELEVENLABS_FEMALE = [
    {"value": "H8bdWZHK2OgZwTN7ponr", "label": "Premium 1"},
    {"value": "UbB19hYD8fvYxwJAVTY5", "label": "Premium 2"},
    {"value": "21m00Tcm4TlvDq8ikWAM", "label": "Premium 3"},
    {"value": "EXAVITQu4vr4xnSDxMaL", "label": "Premium 4"},
]
ELEVENLABS_MALE = [
    {"value": "TX3LPaxmHKxFdv7VOQHJ", "label": "Premium 1"},
    {"value": "onwK4e9ZLuTAKqWW03F9", "label": "Premium 2"},
    {"value": "N2lVS1w4EtoT3dr4eOWO", "label": "Premium 3"},
    {"value": "CwhRBWXzGAHq8TQ4Fs17", "label": "Premium 4"},
]

DEMO_SCRIPT_TEMPLATES = {
    "financial": "Hi, I'm {name} from {company}. I've called you regarding AI automation for your financial operations — we help with lead qualification and conversions. Can I share how in 30 seconds?",
    "realestate": "Hi, I'm {name} from {company}. I've called you regarding AI agents for real estate — we help capture more leads and schedule site visits automatically. Would you like a quick demo?",
    "education": "Hi, I'm {name} from {company}. I've called you regarding handling more admission inquiries with AI, round the clock. Can I share how it works for your institution?",
}

_SHORT_LANG_MAP = {
    "hi": "hi-IN", "en": "en-IN", "ta": "ta-IN", "bn": "bn-IN",
    "gu": "gu-IN", "kn": "kn-IN", "ml": "ml-IN", "mr": "mr-IN",
    "te": "te-IN", "pa": "pa-IN", "or": "or-IN", "od": "or-IN", "as": "as-IN",
}


def _resolve_lang_code(raw_lang: str) -> str:
    """Normalize language input (short code or BCP-47) to a full BCP-47 code."""
    lang_lower = (raw_lang or "").strip().lower()
    if not lang_lower:
        return "en-IN"
    if "-" in lang_lower:
        parts = lang_lower.split("-")
        return parts[0].lower() + "-" + parts[1].upper()
    return _SHORT_LANG_MAP.get(lang_lower, "en-IN")


def _extract_phone_numbers(text: str) -> list[str]:
    """Extract 10-digit Indian phone numbers from text."""
    digits = re.sub(r"\D", "", text)
    nums = []
    for i in range(0, len(digits) - 9):
        chunk = digits[i: i + 10]
        if len(chunk) == 10 and chunk[0] in "6789":
            nums.append(chunk)
    return list(dict.fromkeys(nums))


def _agent_industry(agent_doc: dict) -> str:
    """Derive script industry key from agent doc."""
    cfg = agent_doc.get("config") or {}
    industry = (cfg.get("industry") or "").strip().lower()
    if industry in ("financial", "realestate", "education"):
        return industry
    name = (agent_doc.get("name") or "").strip().lower()
    if "financial" in name or "finance" in name:
        return "financial"
    if "education" in name or "admission" in name:
        return "education"
    return "realestate"


def _sarvam_voice_lists(settings=None):
    """Build female and male voice lists from config."""
    if settings is None:
        settings = get_settings()
    female_ids = tuple(
        s.strip().lower()
        for s in (getattr(settings, "SARVAM_FEMALE_VOICE_IDS", "") or "priya").split(",")
        if s.strip()
    )
    v3 = tuple(
        s.strip().lower()
        for s in (getattr(settings, "SARVAM_V3_SPEAKERS", "") or "priya").split(",")
        if s.strip()
    )
    female = [{"value": v, "label": v.capitalize()} for v in v3 if v in female_ids]
    male = [{"value": v, "label": v.capitalize()} for v in v3 if v not in female_ids]
    default_female = (getattr(settings, "SARVAM_DEFAULT_VOICE", None) or (female_ids[0] if female_ids else "priya")).strip().lower()
    default_male = (getattr(settings, "SARVAM_DEFAULT_MALE_VOICE", None) or "advait").strip().lower()
    male_candidates = [v for v in v3 if v not in female_ids]
    if male_candidates:
        default_male = male_candidates[0]
    if not female:
        female = [{"value": default_female, "label": default_female.capitalize()}]
    if not male:
        male = [{"value": default_male, "label": default_male.capitalize()}]
    return female, male


async def _sarvam_ws_synthesize(api_key: str, text: str, speaker: str, language: str, pace: float) -> bytes:
    """Sarvam TTS via WebSocket streaming."""
    import asyncio
    import json as _json
    import websockets

    ws_url = "wss://api.sarvam.ai/text-to-speech/ws?model=bulbul:v3&send_completion_event=true"
    headers = {"Api-Subscription-Key": api_key}
    audio_chunks: list[bytes] = []

    async with websockets.connect(ws_url, additional_headers=headers) as ws:
        await ws.send(_json.dumps({"type": "config", "data": {"speaker": speaker, "target_language_code": language, "pace": pace}}))
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

    return b"".join(audio_chunks)


async def _voice_preview_google(creds_path: str, voice_id: str, text: str, language_code: str) -> Response:
    """Generate WAV using Google Cloud TTS."""
    try:
        from google.cloud import texttospeech
    except ImportError:
        return Response(status_code=503, content="Google TTS preview requires: pip install google-cloud-texttospeech")
    client = texttospeech.TextToSpeechClient.from_service_account_file(creds_path)
    synthesis_input = texttospeech.SynthesisInput(text=text)
    voice = texttospeech.VoiceSelectionParams(language_code=language_code, name=voice_id)
    audio_config = texttospeech.AudioConfig(audio_encoding=texttospeech.AudioEncoding.LINEAR16, sample_rate_hertz=24000)
    response = client.synthesize_speech(input=synthesis_input, voice=voice, audio_config=audio_config)
    raw = response.audio_content
    if not raw:
        return Response(status_code=502, content="No audio from Google TTS.")
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(24000)
        wav.writeframes(raw)
    return Response(content=buf.getvalue(), media_type="audio/wav")


@router.get("/virtual-numbers")
async def get_virtual_numbers(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_security_optional),
):
    """Return virtual numbers for the demo dashboard."""
    settings = get_settings()
    numbers = []

    if credentials and credentials.credentials:
        payload = decode_token(credentials.credentials)
        if payload:
            user_id = payload.get("sub")
            role = payload.get("role")
            if role == ROLE_DEMO and user_id:
                try:
                    uid_str = str(user_id).strip()
                    phone_doc = await db.phones.find_one({
                        "$or": [
                            {"assigned_to_user_id": uid_str},
                            {"assigned_to_user_id": "default"},
                        ]
                    })
                    if phone_doc and phone_doc.get("number"):
                        numbers.append({
                            "phone_number": phone_doc["number"],
                            "label": (phone_doc.get("label") or phone_doc.get("friendly_name") or "").strip() or None,
                        })
                except Exception:
                    pass

    if not numbers:
        from_num = getattr(settings, "EXOTEL_FROM_NUMBER", None) or ""
        if from_num:
            numbers.append({"phone_number": from_num, "label": None})

    return {"numbers": numbers}


@router.get("/tts-config")
async def get_tts_config():
    """Return TTS provider and voice lists for the demo page."""
    settings = get_settings()
    use_sarvam = getattr(settings, "USE_TTS_SARVAM", True)
    if use_sarvam:
        female, male = _sarvam_voice_lists(settings)
        demo_voice = await get_demo_voice_config(db)
        return {
            "useSarvam": True,
            "femaleVoices": female,
            "maleVoices": male,
            "defaultFemaleVoiceId": demo_voice["female"]["voiceId"],
            "defaultMaleVoiceId": demo_voice["male"]["voiceId"],
        }
    return {"useSarvam": False, "femaleVoices": ELEVENLABS_FEMALE, "maleVoices": ELEVENLABS_MALE}


@router.get("/demo-voice-config")
async def get_demo_voice_config_route():
    """Return admin-configured default female/male voice for demo users plus list of demo agents."""
    demo_voice = await get_demo_voice_config(db)
    agents = await db.agents.find({"demo_agent": True}).sort("created_at", -1).to_list(length=500)
    demo_agents = [
        {"id": str(a["_id"]), "name": (a.get("name") or "").strip() or "Demo Agent", "industry": _agent_industry(a)}
        for a in agents
    ]
    return {
        "female": demo_voice.get("female"),
        "male": demo_voice.get("male"),
        "demoAgents": demo_agents,
        "demoAgentName": demo_agents[0]["name"] if demo_agents else None,
    }


@router.get("/script-templates")
async def get_script_templates():
    """Return script template strings per industry for the demo script configurator."""
    return {"templates": DEMO_SCRIPT_TEMPLATES}


@router.post("/voice-preview")
async def voice_preview(request: Request, body: VoicePreviewBody):
    """
    Generate TTS preview audio.
    Body: { voiceId, text (required), provider ("sarvam"|"google"), language (optional), settings (optional) }
    Returns audio/wav or error response.
    """
    from loguru import logger
    import random as _random

    settings = get_settings()
    provider = (body.provider or "sarvam").strip().lower()
    voice_id = (body.voiceId or "").strip()
    if not voice_id:
        return Response(status_code=400, content="voiceId is required.")
    text = (body.text or "").strip()
    if not text:
        return Response(status_code=400, content="text is required for voice preview.")

    target_language_code = _resolve_lang_code(body.language or "")

    if provider == "google":
        creds_path = getattr(settings, "GOOGLE_APPLICATION_CREDENTIALS", None) or ""
        if not creds_path or not os.path.isfile(creds_path):
            return Response(status_code=503, content="Google TTS preview: set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON path.")
        try:
            return await _voice_preview_google(creds_path, voice_id, text, target_language_code)
        except Exception as e:
            return Response(status_code=502, content=f"Google TTS error: {e}")

    if provider not in ("sarvam", ""):
        return Response(status_code=501, content=f"Voice preview is not available for provider '{provider}'. Use Sarvam or Google Cloud.")

    if not getattr(settings, "USE_TTS_SARVAM", True):
        return Response(status_code=503, content="Sarvam voice preview is disabled. Set USE_TTS_SARVAM=true.")
    voice_id = voice_id.lower()
    api_key = getattr(settings, "SARVAM_API_KEY", "") or ""
    if not api_key:
        return Response(status_code=503, content="Sarvam API key not configured.")

    opts = body.settings or {}
    pace_min = float(opts.get("pace_min") or opts.get("pace", 0.95))
    pace_max = float(opts.get("pace_max") or opts.get("pace", 0.95))
    pace_min = max(0.5, min(2.0, pace_min))
    pace_max = max(0.5, min(2.0, pace_max))
    if pace_min > pace_max:
        pace_min, pace_max = pace_max, pace_min
    pace = round(_random.uniform(pace_min, pace_max), 2) if pace_min != pace_max else pace_min

    logger.info(f"[VOICE-PREVIEW] speaker={voice_id} lang={target_language_code} pace={pace} text={text[:80]!r}...")
    try:
        raw_pcm = await _sarvam_ws_synthesize(api_key, text, voice_id, target_language_code, pace)
    except Exception as e:
        logger.error(f"[VOICE-PREVIEW] Sarvam WS error: {e}")
        return Response(status_code=502, content=f"Sarvam TTS error: {e}")
    if not raw_pcm:
        return Response(status_code=502, content="No audio from Sarvam.")

    buf = io.BytesIO()
    with wave.open(buf, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(24000)
        wav.writeframes(raw_pcm)
    return Response(content=buf.getvalue(), media_type="audio/wav")


@router.post("/campaign/parse-excel")
async def parse_campaign_excel(file: UploadFile):
    """Parse Excel or CSV file and extract phone numbers."""
    numbers = []
    try:
        content = await file.read()
        if file.filename and file.filename.lower().endswith(".csv"):
            text = content.decode("utf-8", errors="ignore")
            reader = csv.reader(io.StringIO(text))
            for row in reader:
                for cell in row:
                    numbers.extend(_extract_phone_numbers(str(cell)))
        else:
            try:
                import openpyxl
                wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
                for sheet in wb.worksheets:
                    for row in sheet.iter_rows(values_only=True):
                        for cell in row:
                            if cell is not None:
                                numbers.extend(_extract_phone_numbers(str(cell)))
            except ImportError:
                text = content.decode("utf-8", errors="ignore")
                for line in text.splitlines():
                    numbers.extend(_extract_phone_numbers(line))
        numbers = list(dict.fromkeys(numbers))
    except Exception:
        pass
    return {"numbers": numbers}
