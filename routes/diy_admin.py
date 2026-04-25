"""
Admin DIY Routes
- Agent Review Queue (approve/reject submitted agents)
- DIY Voice Config (global + per-user voice overrides)
- DIY Users listing with agent status
- User provisioning, phone assignment, credits/concurrency management
"""

import re
import secrets
import string
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from models import ROLE_ADMIN, ROLE_SUPER_ADMIN, ROLE_USER
from utils.db import db
from utils.logger import logger
from utils.password import hash_password
from utils.security import get_current_user

EMAIL_REGEX = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

PREMIUM_FEATURES = [
    "call_transfer", "voicemail_detection", "lead_detection",
    "appointment_booking", "direction_specific_kb", "proposal_generation",
]

router = APIRouter(prefix="/api/v1/admin/diy", tags=["Admin DIY"])


def _generate_password(length: int = 12) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$*"
    while True:
        pwd = "".join(secrets.choice(alphabet) for _ in range(length))
        if any(c.isupper() for c in pwd) and any(c.islower() for c in pwd) and any(c.isdigit() for c in pwd):
            return pwd


def _require_admin(current_user: dict):
    if current_user.get("role") not in (ROLE_SUPER_ADMIN, ROLE_ADMIN):
        raise HTTPException(status_code=403, detail="Admin access required")


def _agent_summary(agent: dict) -> dict:
    return {
        "id": str(agent["_id"]),
        "userId": agent.get("user_id", ""),
        "name": agent.get("name", ""),
        "description": agent.get("description", ""),
        "gender": agent.get("gender", "female"),
        "approvalStatus": agent.get("approval_status", "draft"),
        "version": agent.get("version", 1),
        "supportedLanguages": agent.get("supported_languages", ["en"]),
        "prompt": agent.get("prompt", ""),
        "inboundConfig": agent.get("inbound_config", {}),
        "outboundConfig": agent.get("outbound_config", {}),
        "testCallLogs": agent.get("test_call_logs", []),
        "hasCompletedTestCall": agent.get("has_completed_test_call", False),
        "submittedAt": agent.get("submitted_at"),
        "approvedAt": agent.get("approved_at"),
        "rejectedAt": agent.get("rejected_at"),
        "rejectionReason": agent.get("rejection_reason"),
        "createdAt": agent.get("created_at"),
        "updatedAt": agent.get("updated_at"),
    }


def _voice_config_to_dict(doc: Optional[dict]) -> Optional[dict]:
    if not doc:
        return None
    return {
        "id": str(doc["_id"]),
        "type": doc.get("type"),
        "userId": doc.get("user_id"),
        "male": doc.get("male"),
        "female": doc.get("female"),
        "updatedBy": doc.get("updated_by"),
        "updatedAt": doc.get("updated_at"),
    }


async def _get_test_call_recordings(user_id: str) -> List[dict]:
    """Fetch test call recordings for a specific agent/user."""
    if not user_id:
        return []
    try:
        cursor = db.calls.find({"tenant_id": user_id, "is_test": True}).sort("created_at", -1).limit(20)
        recordings = []
        async for call in cursor:
            recordings.append({
                "callId": call.get("call_id"),
                "duration": call.get("duration", 0),
                "status": call.get("status", "unknown"),
                "direction": call.get("direction", "unknown"),
                "fromNumber": call.get("from_number", ""),
                "toNumber": call.get("to_number", ""),
                "createdAt": call.get("created_at"),
                "endedAt": call.get("ended_at"),
                "recordingStatus": call.get("recording_fetch_status", "pending"),
                "hasRecording": bool(call.get("s3_recording_key")),
                "recordingUrl": call.get("recording_url"),
                "s3RecordingKey": call.get("s3_recording_key"),
                "transcript": call.get("transcript", []),
                "isTest": call.get("is_test", False),
            })
        return recordings
    except Exception as e:
        logger.error(f"[DIY Admin] Error fetching test call recordings for user {user_id}: {e}")
        return []


async def _get_voice_config_synced(user_id: str, gender: str = "female") -> dict:
    """Resolve voice config: user override → global admin → hardcoded default."""
    user_voice = await db.diy_voice_config.find_one({"type": "user", "user_id": user_id})
    if user_voice:
        gender_config = user_voice.get(gender)
        if gender_config:
            vc_settings = dict(gender_config.get("settings") or {})
            if "pace" not in vc_settings and "pace_min" not in vc_settings:
                vc_settings["pace"] = gender_config.get("speakingRate", 1.1)
            return {"provider": gender_config.get("provider", "sarvam"), "voiceId": gender_config.get("voiceId", "priya"), "voiceName": gender_config.get("voiceName", "Priya (female)"), "settings": vc_settings}
    global_voice = await db.diy_voice_config.find_one({"type": "global"})
    if global_voice:
        gender_config = global_voice.get(gender)
        if gender_config:
            vc_settings = dict(gender_config.get("settings") or {})
            if "pace" not in vc_settings and "pace_min" not in vc_settings:
                vc_settings["pace"] = gender_config.get("speakingRate", 1.1)
            return {"provider": gender_config.get("provider", "sarvam"), "voiceId": gender_config.get("voiceId", "priya"), "voiceName": gender_config.get("voiceName", "Priya (female)"), "settings": vc_settings}
    return {"provider": "sarvam", "voiceId": "priya", "voiceName": "Priya (female)"}


# ═══════════════════════════════════════════════
#  AGENT REVIEW QUEUE
# ═══════════════════════════════════════════════

@router.get("/agent-review")
async def list_review_queue(
    status: str = Query("pending"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    current_user: dict = Depends(get_current_user),
):
    """GET /api/v1/admin/diy/agent-review — List agent review queue."""
    _require_admin(current_user)

    query: Dict[str, Any] = {} if status == "all" else {"status": status}
    skip = (page - 1) * limit
    cursor = db.agent_review_queue.find(query).sort([("submitted_at", 1)]).skip(skip).limit(limit)
    items = []
    async for doc in cursor:
        items.append({
            "id": str(doc["_id"]),
            "agentId": str(doc.get("agent_id", "")),
            "userId": doc.get("user_id", ""),
            "userName": doc.get("user_name", ""),
            "userEmail": doc.get("user_email", ""),
            "agentName": doc.get("agent_name", ""),
            "greetingPreview": doc.get("greeting_preview", ""),
            "changeType": doc.get("change_type", "new"),
            "submittedAt": doc.get("submitted_at"),
            "status": doc.get("status", "pending"),
            "reviewedBy": doc.get("reviewed_by"),
            "reviewedAt": doc.get("reviewed_at"),
            "reviewNotes": doc.get("review_notes"),
        })

    total = await db.agent_review_queue.count_documents(query)
    pending_count = await db.agent_review_queue.count_documents({"status": "pending"})
    return {
        "success": True,
        "data": {
            "items": items,
            "pendingCount": pending_count,
            "pagination": {"page": page, "limit": limit, "total": total, "pages": max(1, (total + limit - 1) // limit)},
        }
    }


@router.get("/agent-review/{agent_id}/test-recordings")
async def get_agent_test_recordings(agent_id: str, current_user: dict = Depends(get_current_user)):
    """GET /api/v1/admin/diy/agent-review/:agentId/test-recordings"""
    _require_admin(current_user)
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid agent ID")

    agent = await db.diy_agents.find_one({"_id": oid})
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    recordings = await _get_test_call_recordings(agent.get("user_id"))
    return {"success": True, "data": {"recordings": recordings, "agentId": agent_id, "userId": agent.get("user_id"), "agentName": agent.get("name", "")}}


@router.get("/agent-review/{agent_id}")
async def get_review_detail(agent_id: str, current_user: dict = Depends(get_current_user)):
    """GET /api/v1/admin/diy/agent-review/:agentId — Full agent detail for review."""
    _require_admin(current_user)
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid agent ID")

    agent = await db.diy_agents.find_one({"_id": oid})
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    queue_entry = await db.agent_review_queue.find_one({"agent_id": oid})

    user_doc = None
    if agent.get("user_id"):
        try:
            user_doc = await db.users.find_one({"_id": ObjectId(agent["user_id"])})
        except Exception:
            pass

    versions = []
    async for v in db.diy_agent_versions.find({"agent_id": oid}).sort("version", -1).limit(10):
        v["_id"] = str(v["_id"])
        v["agent_id"] = str(v["agent_id"])
        versions.append(v)

    test_call_recordings = await _get_test_call_recordings(agent.get("user_id"))

    kb_documents = []
    user_id_str = agent.get("user_id", "")
    async for doc in db.kb_documents.find({"user_id": user_id_str, "is_active": True}).sort("uploaded_at", -1):
        kb_documents.append({
            "id": str(doc["_id"]),
            "fileName": doc.get("file_name", ""),
            "fileType": doc.get("file_type", ""),
            "fileSize": doc.get("file_size", 0),
            "callDirection": doc.get("call_direction", "both"),
            "status": doc.get("status", ""),
            "totalChunks": doc.get("total_chunks", 0),
            "uploadedAt": doc.get("uploaded_at"),
        })

    return {
        "success": True,
        "data": {
            "agent": _agent_summary(agent),
            "user": {
                "id": str(user_doc["_id"]) if user_doc else None,
                "email": (user_doc or {}).get("email", ""),
                "name": (user_doc or {}).get("name", ""),
                "phone": (user_doc or {}).get("phone", ""),
            } if user_doc else None,
            "queueEntry": {
                "id": str(queue_entry["_id"]),
                "status": queue_entry.get("status", "pending"),
                "changeType": queue_entry.get("change_type", "new"),
                "submittedAt": queue_entry.get("submitted_at"),
                "reviewNotes": queue_entry.get("review_notes"),
            } if queue_entry else None,
            "versions": versions,
            "testCallRecordings": test_call_recordings,
            "documents": kb_documents,
        }
    }


class ApproveBody(BaseModel):
    notes: Optional[str] = None


@router.post("/agent-review/{agent_id}/approve")
async def approve_agent(agent_id: str, body: ApproveBody = ApproveBody(), current_user: dict = Depends(get_current_user)):
    """POST /api/v1/admin/diy/agent-review/:agentId/approve — Approve an agent."""
    _require_admin(current_user)
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid agent ID")

    agent = await db.diy_agents.find_one({"_id": oid})
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    if agent.get("approval_status") != "pending_approval":
        raise HTTPException(status_code=400, detail=f"Agent is not pending approval. Current: {agent.get('approval_status')}")

    now = datetime.now(timezone.utc)
    admin_id = current_user["user_id"]

    await db.diy_agents.update_one({"_id": oid}, {"$set": {"approval_status": "approved", "approved_at": now, "approved_by": admin_id, "rejection_reason": None, "updated_at": now}})
    await db.agent_review_queue.update_one({"agent_id": oid}, {"$set": {"status": "completed", "reviewed_by": admin_id, "reviewed_at": now, "review_notes": body.notes}})
    await db.diy_agent_versions.update_one(
        {"agent_id": oid, "version": agent.get("version", 1)},
        {"$set": {"approval_status": "approved", "approved_at": now, "approved_by": admin_id, "review_notes": body.notes}},
    )

    user_id = agent.get("user_id")
    if user_id:
        try:
            from utils.db import redis_client, AGENT_CACHE_PREFIX
            await redis_client.delete(f"{AGENT_CACHE_PREFIX}{user_id}")
        except Exception:
            pass

        linked = await db.agents.find_one({"user_id": user_id})
        if linked:
            voice_config = await _get_voice_config_synced(user_id, agent.get("gender", "female"))
            t = agent.get("call_transfer_settings") or {}
            apt = agent.get("appointment_booking") or {}
            apt_q = apt.get("questions") or {}
            config = {
                "prompt": (agent.get("prompt") or "").strip(),
                "inboundConfig": agent.get("inbound_config") or {"greetingMessage": "Hello! How can I help you today?"},
                "outboundConfig": agent.get("outbound_config") or {"greetingMessage": "Hello! This is a call from our service."},
                "supportedLanguages": agent.get("supported_languages") or ["en"],
                "llm": {"model": "gpt-4o-mini", "temperature": 0.7, "maxTokens": 150},
                "voice": voice_config,
                "transferSettings": {
                    "enabled": agent.get("call_transfer_enabled", t.get("enabled", False)),
                    "humanAgentNumber": (t.get("human_agent_number") or "").strip(),
                    "transferCondition": (t.get("transfer_condition") or "").strip(),
                    "confirmationQuestion": (t.get("confirmation_question") or "").strip() or "Would you like me to transfer you to a human agent?",
                    "confirmationKeywords": t.get("confirmation_keywords") or ["yes", "sure", "ok", "okay", "yeah", "please"],
                    "negativeKeywords": t.get("negative_keywords") or ["no", "not now", "later", "cancel"],
                    "confirmationMessage": (t.get("confirmation_message") or "").strip() or "Transferring you to a human agent now. Please hold.",
                    "failureMessage": (t.get("failure_message") or "").strip() or "I'm sorry, but our agents are currently unavailable. How else can I help you?",
                },
                "leadKeywords": agent.get("lead_keywords") or [],
                "appointmentBooking": {
                    "enabled": apt.get("enabled", False),
                    "condition": (apt.get("condition") or "").strip(),
                    "questions": {
                        "nameQuestion": apt_q.get("name_question") or "What's your name?",
                        "dateQuestion": apt_q.get("date_question") or "What date would you like to schedule the appointment?",
                        "timeQuestion": apt_q.get("time_question") or "What time works for you?",
                        "reasonQuestion": (apt_q.get("reason_question") or "").strip() or None,
                        "confirmationQuestion": apt_q.get("confirmation_question") or "I've booked your appointment for [date] at [time]. Is this correct?",
                    },
                    "successMessage": (apt.get("success_message") or "").strip() or "Great! Your appointment is confirmed for [date] at [time]. We'll see you then!",
                    "slotStartTime": apt.get("slot_start_time") or "09:00",
                    "slotEndTime": apt.get("slot_end_time") or "17:00",
                    "slotDurationMinutes": apt.get("slot_duration_minutes", 30),
                    "workingDays": apt.get("working_days") or [1, 2, 3, 4, 5],
                },
                "proposalSettings": agent.get("proposal_settings") or {},
                "voicemailDetection": agent.get("voicemail_detection") or {"enabled": True},
            }
            await db.agents.update_one(
                {"user_id": user_id},
                {"$set": {
                    "name": agent.get("name") or linked.get("name"),
                    "gender": agent.get("gender") or "female",
                    "config": config,
                    "persona": {"lead_keywords": agent.get("lead_keywords") or []},
                    "proposal_settings": agent.get("proposal_settings") or {},
                    "voicemail_detection": agent.get("voicemail_detection") or {"enabled": True},
                    "updated_at": now,
                }}
            )
            logger.info(f"[DIY Admin] Linked agent config synced for user={user_id} (approved)")

    logger.info(f"[DIY Admin] Agent approved: agent={agent_id} by admin={admin_id}")
    return {"success": True, "message": "Agent approved successfully"}


class RejectBody(BaseModel):
    reason: str


@router.post("/agent-review/{agent_id}/reject")
async def reject_agent(agent_id: str, body: RejectBody, current_user: dict = Depends(get_current_user)):
    """POST /api/v1/admin/diy/agent-review/:agentId/reject — Reject an agent."""
    _require_admin(current_user)
    if not body.reason or not body.reason.strip():
        raise HTTPException(status_code=400, detail="Rejection reason is required")
    try:
        oid = ObjectId(agent_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid agent ID")

    agent = await db.diy_agents.find_one({"_id": oid})
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    if agent.get("approval_status") != "pending_approval":
        raise HTTPException(status_code=400, detail=f"Agent is not pending approval. Current: {agent.get('approval_status')}")

    now = datetime.now(timezone.utc)
    admin_id = current_user["user_id"]

    await db.diy_agents.update_one({"_id": oid}, {"$set": {"approval_status": "rejected", "rejected_at": now, "rejected_by": admin_id, "rejection_reason": body.reason.strip(), "updated_at": now}})
    await db.agent_review_queue.update_one({"agent_id": oid}, {"$set": {"status": "completed", "reviewed_by": admin_id, "reviewed_at": now, "review_notes": body.reason.strip()}})
    await db.diy_agent_versions.update_one(
        {"agent_id": oid, "version": agent.get("version", 1)},
        {"$set": {"approval_status": "rejected", "rejected_at": now, "rejected_by": admin_id, "rejection_reason": body.reason.strip(), "review_notes": body.reason.strip()}},
    )

    user_id = agent.get("user_id")
    if user_id:
        try:
            from utils.db import redis_client, AGENT_CACHE_PREFIX
            await redis_client.delete(f"{AGENT_CACHE_PREFIX}{user_id}")
        except Exception:
            pass

    logger.info(f"[DIY Admin] Agent rejected: agent={agent_id} by admin={admin_id}")
    return {"success": True, "message": "Agent rejected successfully"}


# ═══════════════════════════════════════════════
#  TEST RECORDING ENDPOINTS
# ═══════════════════════════════════════════════

@router.get("/test-recordings/{call_id}/url")
async def get_test_recording_url(call_id: str, current_user: dict = Depends(get_current_user)):
    """GET /api/v1/admin/diy/test-recordings/:callId/url — Get presigned URL for test call recording."""
    _require_admin(current_user)
    call = await db.calls.find_one({"call_id": call_id})
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    if not call.get("is_test"):
        raise HTTPException(status_code=400, detail="Not a test call")

    s3_key = call.get("s3_recording_key")
    if not s3_key:
        rec_status = call.get("recording_fetch_status", "pending")
        return {"available": False, "status": rec_status, "message": "Recording not yet available" if rec_status == "pending" else f"Recording status: {rec_status}"}

    from services.s3_service import get_presigned_url
    from config import get_settings
    url = get_presigned_url(s3_key)
    if not url:
        raise HTTPException(status_code=500, detail="Failed to generate presigned URL")
    return {"available": True, "url": url, "expires_in": get_settings().AWS_S3_PRESIGNED_URL_EXPIRY, "s3_key": s3_key}


@router.get("/test-recordings/{call_id}/stream")
async def stream_test_recording(call_id: str, current_user: dict = Depends(get_current_user)):
    """GET /api/v1/admin/diy/test-recordings/:callId/stream — Stream test call recording audio."""
    _require_admin(current_user)
    call = await db.calls.find_one({"call_id": call_id})
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    if not call.get("is_test"):
        raise HTTPException(status_code=400, detail="Not a test call")

    s3_key = call.get("s3_recording_key")
    if not s3_key:
        raise HTTPException(status_code=404, detail="Recording not available")

    from config import get_settings
    settings = get_settings()
    s3 = boto3.client("s3", region_name=settings.AWS_REGION, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    try:
        s3_obj = s3.get_object(Bucket=settings.AWS_S3_BUCKET, Key=s3_key)
    except Exception as e:
        logger.error(f"[DIY Admin] S3 get_object error for test recording {s3_key}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch recording from S3")

    audio_bytes = s3_obj["Body"].read()
    content_type = s3_obj.get("ContentType", "audio/mpeg")

    async def iter_audio():
        yield audio_bytes

    return StreamingResponse(
        iter_audio(), media_type=content_type,
        headers={"Content-Length": str(len(audio_bytes)), "Content-Disposition": f'inline; filename="test_recording_{call_id}.mp3"'},
    )


@router.post("/test-recordings/{call_id}/fetch")
async def fetch_test_recording(call_id: str, current_user: dict = Depends(get_current_user)):
    """POST /api/v1/admin/diy/test-recordings/:callId/fetch — Manually fetch test call recording."""
    _require_admin(current_user)
    call = await db.calls.find_one({"call_id": call_id})
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    if not call.get("is_test"):
        raise HTTPException(status_code=400, detail="Not a test call")
    if call.get("s3_recording_key"):
        return {"status": "already_available", "s3_key": call["s3_recording_key"]}

    from config import get_settings
    settings = get_settings()
    if not settings.EXOTEL_API_KEY or not settings.EXOTEL_API_TOKEN:
        raise HTTPException(status_code=500, detail="Exotel credentials not configured")

    from services.recording_fetcher import fetch_recording_url, download_recording
    rec_url = call.get("recording_url") or await fetch_recording_url(call_id, settings)
    if not rec_url:
        await db.calls.update_one({"call_id": call_id}, {"$set": {"recording_fetch_status": "not_available", "last_recording_fetch_at": datetime.now(timezone.utc)}})
        return {"status": "not_available", "message": "Recording not found on Exotel"}

    audio = await download_recording(rec_url, settings)
    if not audio:
        return {"status": "download_failed", "message": "Could not download recording from Exotel"}

    if not settings.AWS_ACCESS_KEY_ID or not settings.AWS_SECRET_ACCESS_KEY:
        raise HTTPException(status_code=500, detail="AWS S3 credentials not configured")

    from services.s3_service import upload_recording
    result = upload_recording(call_id, audio)
    await db.calls.update_one(
        {"call_id": call_id},
        {"$set": {"recording_url": rec_url, "s3_recording_key": result["key"], "s3_recording_uploaded_at": datetime.now(timezone.utc), "recording_fetch_status": "available", "last_recording_fetch_at": datetime.now(timezone.utc)}},
    )
    return {"status": "available", "s3_key": result["key"]}


# ═══════════════════════════════════════════════
#  DIY VOICE CONFIG
# ═══════════════════════════════════════════════

class GenderVoiceConfig(BaseModel):
    provider: str
    voiceId: str
    voiceName: str
    speakingRate: Optional[float] = 1.1
    model: Optional[str] = None
    settings: Optional[Dict[str, Any]] = None


class VoiceConfigBody(BaseModel):
    male: GenderVoiceConfig
    female: GenderVoiceConfig


@router.get("/voice-config")
async def get_global_voice_config(current_user: dict = Depends(get_current_user)):
    """GET /api/v1/admin/diy/voice-config — Get global DIY voice config."""
    _require_admin(current_user)
    doc = await db.diy_voice_config.find_one({"type": "global"})
    return {"success": True, "data": _voice_config_to_dict(doc)}


@router.put("/voice-config")
async def set_global_voice_config(body: VoiceConfigBody, current_user: dict = Depends(get_current_user)):
    """PUT /api/v1/admin/diy/voice-config — Set global DIY voice config."""
    _require_admin(current_user)
    now = datetime.now(timezone.utc)
    data = {"type": "global", "male": body.male.model_dump(), "female": body.female.model_dump(), "updated_by": current_user["user_id"], "updated_at": now}
    await db.diy_voice_config.update_one({"type": "global"}, {"$set": data, "$setOnInsert": {"created_at": now}}, upsert=True)
    doc = await db.diy_voice_config.find_one({"type": "global"})
    return {"success": True, "data": _voice_config_to_dict(doc)}


@router.get("/voice-config/users/{user_id}")
async def get_user_voice_override(user_id: str, current_user: dict = Depends(get_current_user)):
    """GET /api/v1/admin/diy/voice-config/users/:userId — Get user voice override + global."""
    _require_admin(current_user)
    override = await db.diy_voice_config.find_one({"type": "user", "user_id": user_id})
    global_cfg = await db.diy_voice_config.find_one({"type": "global"})
    return {"success": True, "data": {"override": _voice_config_to_dict(override), "global": _voice_config_to_dict(global_cfg)}}


@router.put("/voice-config/users/{user_id}")
async def set_user_voice_override(user_id: str, body: VoiceConfigBody, current_user: dict = Depends(get_current_user)):
    """PUT /api/v1/admin/diy/voice-config/users/:userId — Set user voice override."""
    _require_admin(current_user)
    now = datetime.now(timezone.utc)
    data = {"type": "user", "user_id": user_id, "male": body.male.model_dump(), "female": body.female.model_dump(), "updated_by": current_user["user_id"], "updated_at": now}
    await db.diy_voice_config.update_one({"type": "user", "user_id": user_id}, {"$set": data, "$setOnInsert": {"created_at": now}}, upsert=True)
    try:
        from utils.db import redis_client, AGENT_CACHE_PREFIX
        await redis_client.delete(f"{AGENT_CACHE_PREFIX}{user_id}")
    except Exception:
        pass
    doc = await db.diy_voice_config.find_one({"type": "user", "user_id": user_id})
    return {"success": True, "data": _voice_config_to_dict(doc)}


@router.delete("/voice-config/users/{user_id}")
async def remove_user_voice_override(user_id: str, current_user: dict = Depends(get_current_user)):
    """DELETE /api/v1/admin/diy/voice-config/users/:userId — Remove user voice override."""
    _require_admin(current_user)
    await db.diy_voice_config.delete_one({"type": "user", "user_id": user_id})
    return {"success": True, "message": "Voice override removed"}


# ═══════════════════════════════════════════════
#  DIY USERS LIST
# ═══════════════════════════════════════════════

@router.get("/users")
async def list_diy_users(
    search: str = Query(""),
    agent_status: str = Query("all"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    current_user: dict = Depends(get_current_user),
):
    """GET /api/v1/admin/diy/users — List all is_diy users with their agent status."""
    _require_admin(current_user)

    user_query: Dict[str, Any] = {"is_diy": True}
    if search:
        user_query["$or"] = [{"name": {"$regex": search, "$options": "i"}}, {"email": {"$regex": search, "$options": "i"}}]

    skip = (page - 1) * limit
    total = await db.users.count_documents(user_query)
    diy_users: List[dict] = await db.users.find(user_query).sort("created_at", -1).skip(skip).limit(limit).to_list(length=limit)

    if not diy_users:
        counts = {"all": total, "draft": 0, "pending": 0, "approved": 0, "rejected": 0}
        return {"success": True, "data": {"users": [], "pagination": {"page": page, "limit": limit, "total": 0, "pages": 1}, "counts": counts}}

    uid_strs = [str(u["_id"]) for u in diy_users]
    agents_map: Dict[str, dict] = {}
    async for a in db.diy_agents.find({"user_id": {"$in": uid_strs}}):
        agents_map[a["user_id"]] = a

    phone_ids = [str(u["assigned_phone_id"]) for u in diy_users if u.get("assigned_phone_id")]
    phones_map: Dict[str, dict] = {}
    if phone_ids:
        try:
            async for p in db.phones.find({"_id": {"$in": [ObjectId(pid) for pid in phone_ids]}}):
                phones_map[str(p["_id"])] = p
        except Exception:
            pass

    results = []
    for user in diy_users:
        uid = str(user["_id"])
        agent = agents_map.get(uid)
        if agent_status != "all":
            if agent is None or agent.get("approval_status") != agent_status:
                continue
        assigned_phone_id = str(user.get("assigned_phone_id", "") or "")
        phone_doc = phones_map.get(assigned_phone_id)
        results.append({
            "userId": uid,
            "email": user.get("email", ""),
            "name": user.get("name", ""),
            "company": user.get("company_name", ""),
            "phone": user.get("phone", ""),
            "status": user.get("status", True),
            "statusExpiresAt": user.get("status_expires_at"),
            "credits": user.get("credits", 0),
            "concurrency": user.get("concurrency", 0),
            "assignedPhoneId": assigned_phone_id or None,
            "assignedPhoneNumber": phone_doc.get("number") if phone_doc else None,
            "agent": {
                "id": str(agent["_id"]), "name": agent.get("name", ""),
                "approvalStatus": agent.get("approval_status", "draft"), "version": agent.get("version", 1),
                "submittedAt": agent.get("submitted_at"), "approvedAt": agent.get("approved_at"),
                "rejectedAt": agent.get("rejected_at"), "rejectionReason": agent.get("rejection_reason"),
                "updatedAt": agent.get("updated_at"),
            } if agent else None,
        })

    all_uid_strs = [str(u["_id"]) async for u in db.users.find({"is_diy": True}, {"_id": 1})]
    counts = {
        "all": len(all_uid_strs),
        "draft": await db.diy_agents.count_documents({"user_id": {"$in": all_uid_strs}, "approval_status": "draft"}),
        "pending": await db.diy_agents.count_documents({"user_id": {"$in": all_uid_strs}, "approval_status": "pending_approval"}),
        "approved": await db.diy_agents.count_documents({"user_id": {"$in": all_uid_strs}, "approval_status": "approved"}),
        "rejected": await db.diy_agents.count_documents({"user_id": {"$in": all_uid_strs}, "approval_status": "rejected"}),
    }
    return {
        "success": True,
        "data": {
            "users": results,
            "pagination": {"page": page, "limit": limit, "total": total, "pages": max(1, (total + limit - 1) // limit)},
            "counts": counts,
        }
    }


@router.post("/users/{user_id}/create-agent")
async def admin_create_diy_agent(user_id: str, body: dict, current_user: dict = Depends(get_current_user)):
    """POST /api/v1/admin/diy/users/:userId/create-agent — Provision a DIY agent for a user."""
    _require_admin(current_user)
    try:
        user_doc = await db.users.find_one({"_id": ObjectId(user_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user ID")
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")
    if await db.diy_agents.find_one({"user_id": user_id}):
        raise HTTPException(status_code=400, detail="User already has a DIY agent")

    now = datetime.now(timezone.utc)
    agent_doc = {
        "user_id": user_id, "name": body.get("name") or f"{user_doc.get('name', 'My')} Agent",
        "description": body.get("description", ""), "gender": body.get("gender", "female"),
        "approval_status": "draft", "version": 1,
        "supported_languages": body.get("supportedLanguages", ["en"]),
        "prompt": body.get("prompt", ""),
        "inbound_config": {"greetingMessage": body.get("inboundGreeting", "Hello! How can I help you today?")},
        "outbound_config": {"greetingMessage": body.get("outboundGreeting", "Hello! This is a call from our service.")},
        "voicemail_detection": {"enabled": True, "confidenceThreshold": 0.7, "minDetectionTime": 3, "keywords": ["voicemail", "leave a message", "after the beep", "not available"]},
        "test_call_logs": [], "has_completed_test_call": False, "created_at": now, "updated_at": now,
    }
    result = await db.diy_agents.insert_one(agent_doc)
    agent_doc["_id"] = result.inserted_id

    if not await db.agents.find_one({"user_id": user_id}):
        linked_agent = {"user_id": user_id, "name": agent_doc["name"], "config": {}, "max_concurrent": 10, "managed_by": "diy", "is_active": True, "approval_status": "approved", "version": 1, "created_at": now, "updated_at": now}
        try:
            await db.agents.insert_one(linked_agent)
        except Exception as e:
            logger.exception(f"[DIY Admin] Failed to create linked agent for user {user_id}: {e}")
            raise HTTPException(status_code=500, detail=f"DIY agent draft created but linked agent could not be created: {str(e)}")

    logger.info(f"[DIY Admin] Agent created for user={user_id} by admin={current_user['user_id']}")
    return {"success": True, "data": _agent_summary(agent_doc), "message": "DIY agent created successfully"}


@router.put("/users/{user_id}/assign-phone")
async def assign_phone_to_diy_user(user_id: str, body: dict, current_user: dict = Depends(get_current_user)):
    """PUT /api/v1/admin/diy/users/:userId/assign-phone — Assign a phone to a DIY user."""
    _require_admin(current_user)
    phone_id = (body.get("phoneId") or "").strip()
    if not phone_id:
        raise HTTPException(status_code=400, detail="phoneId is required")

    try:
        user_doc = await db.users.find_one({"_id": ObjectId(user_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user ID")
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")

    try:
        phone_doc = await db.phones.find_one({"_id": ObjectId(phone_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid phone ID")
    if not phone_doc:
        raise HTTPException(status_code=404, detail="Phone not found")

    now = datetime.now(timezone.utc)
    prev_user_id = str(phone_doc.get("assigned_to_user_id") or "")
    if prev_user_id and prev_user_id != user_id:
        await db.users.update_one({"_id": ObjectId(prev_user_id)}, {"$unset": {"assigned_phone_id": ""}, "$set": {"updated_at": now}})

    await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": {"assigned_phone_id": phone_id, "updated_at": now}})
    linked_agent = await db.agents.find_one({"user_id": user_id})
    phone_update = {"assigned_to_user_id": user_id, "assignment_status": "assigned", "updated_at": now}
    if linked_agent:
        phone_update["agent_id"] = linked_agent["_id"]
    await db.phones.update_one({"_id": ObjectId(phone_id)}, {"$set": phone_update})

    logger.info(f"[DIY Admin] Phone {phone_id} assigned to user={user_id} by admin={current_user['user_id']}")
    return {"success": True, "message": f"Phone {phone_doc.get('number')} assigned to user"}


@router.delete("/users/{user_id}/assign-phone")
async def unassign_phone_from_diy_user(user_id: str, current_user: dict = Depends(get_current_user)):
    """DELETE /api/v1/admin/diy/users/:userId/assign-phone — Unassign phone from user."""
    _require_admin(current_user)
    try:
        user_doc = await db.users.find_one({"_id": ObjectId(user_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user ID")
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")

    phone_id = str(user_doc.get("assigned_phone_id") or "")
    now = datetime.now(timezone.utc)
    await db.users.update_one({"_id": ObjectId(user_id)}, {"$unset": {"assigned_phone_id": ""}, "$set": {"updated_at": now}})
    if phone_id:
        try:
            await db.phones.update_one({"_id": ObjectId(phone_id)}, {"$unset": {"assigned_to_user_id": "", "agent_id": ""}, "$set": {"assignment_status": "available", "updated_at": now}})
        except Exception:
            pass
    return {"success": True, "message": "Phone unassigned successfully"}


# ═══════════════════════════════════════════════
#  CREATE DIY USER
# ═══════════════════════════════════════════════

class CreateDiyUserBody(BaseModel):
    name: str
    email: str
    plan: Optional[str] = "Enterprise"
    status_expires_at: Optional[str] = None
    credits: Optional[int] = 0
    concurrency: Optional[int] = 0


@router.post("/users")
async def create_diy_user(body: CreateDiyUserBody, current_user: dict = Depends(get_current_user)):
    """POST /api/v1/admin/diy/users — Create a new DIY user with auto-generated password."""
    _require_admin(current_user)

    email = body.email.strip().lower()
    if not email or not EMAIL_REGEX.match(email):
        raise HTTPException(status_code=400, detail="Invalid email address")
    if not body.name.strip():
        raise HTTPException(status_code=400, detail="Name is required")
    if await db.users.find_one({"email": email}):
        raise HTTPException(status_code=400, detail="Email already registered")

    initial_credits = max(0, body.credits or 0)
    initial_concurrency = max(0, body.concurrency or 0)
    creator_id = current_user.get("impersonator") or current_user.get("user_id")
    creator_doc = await db.users.find_one({"_id": ObjectId(creator_id)})
    if not creator_doc:
        raise HTTPException(status_code=400, detail="Creator user not found")
    if initial_credits > 0 and creator_doc.get("credits", 0) < initial_credits:
        raise HTTPException(status_code=400, detail=f"Insufficient credits. You have {creator_doc.get('credits', 0)} but are trying to assign {initial_credits}.")
    if initial_concurrency > 0 and creator_doc.get("concurrency", 0) < initial_concurrency:
        raise HTTPException(status_code=400, detail=f"Insufficient concurrency slots. You have {creator_doc.get('concurrency', 0)} but are trying to assign {initial_concurrency}.")

    password = _generate_password()
    now = datetime.now(timezone.utc)
    doc = {
        "email": email, "password_hash": hash_password(password), "role": ROLE_USER,
        "name": body.name.strip(), "plan": body.plan or "Enterprise",
        "credits": initial_credits, "concurrency": initial_concurrency,
        "is_diy": True, "status": True, "created_at": now, "created_by": creator_id, "updated_at": now,
    }
    if body.status_expires_at:
        doc["status_expires_at"] = body.status_expires_at

    result = await db.users.insert_one(doc)
    user_id = str(result.inserted_id)

    if initial_credits > 0:
        creator_credits = creator_doc.get("credits", 0)
        creator_new = creator_credits - initial_credits
        await db.users.update_one({"_id": ObjectId(creator_id)}, {"$set": {"credits": creator_new, "updated_at": now}})
        for tx in [
            {"user_id": creator_id, "user_email": creator_doc["email"], "user_role": creator_doc.get("role"), "action": "deduct", "amount": initial_credits, "previous_credits": creator_credits, "new_credits": creator_new, "reason": f"Initial credits assigned to new DIY user {email}", "performed_by": creator_id, "performed_by_email": creator_doc["email"], "target_user_id": user_id, "target_user_email": email, "created_at": now},
            {"user_id": user_id, "user_email": email, "user_role": ROLE_USER, "action": "assign", "amount": initial_credits, "previous_credits": 0, "new_credits": initial_credits, "reason": "Initial credits on account creation", "performed_by": creator_id, "performed_by_email": creator_doc["email"], "created_at": now},
        ]:
            await db.credit_transactions.insert_one(tx)

    if initial_concurrency > 0:
        creator_pool = creator_doc.get("concurrency", 0)
        creator_new_pool = creator_pool - initial_concurrency
        await db.users.update_one({"_id": ObjectId(creator_id)}, {"$set": {"concurrency": creator_new_pool, "updated_at": now}})
        for tx in [
            {"user_id": creator_id, "user_email": creator_doc["email"], "user_role": creator_doc.get("role"), "action": "deduct", "amount": initial_concurrency, "previous_concurrency": creator_pool, "new_concurrency": creator_new_pool, "reason": f"Initial concurrency assigned to new DIY user {email}", "performed_by": creator_id, "target_user_id": user_id, "target_user_email": email, "created_at": now},
            {"user_id": user_id, "user_email": email, "user_role": ROLE_USER, "action": "assign", "amount": initial_concurrency, "previous_concurrency": 0, "new_concurrency": initial_concurrency, "reason": "Initial concurrency on account creation", "performed_by": creator_id, "created_at": now},
        ]:
            await db.concurrency_transactions.insert_one(tx)
        await db.users.update_one({"_id": result.inserted_id}, {"$set": {"concurrency_assigned_by": creator_id}})

    now2 = datetime.now(timezone.utc)
    agent_doc = {
        "user_id": user_id, "name": f"{body.name.strip()}'s Agent", "description": "", "gender": "female",
        "approval_status": "draft", "version": 1, "supported_languages": ["en"], "prompt": "",
        "inbound_config": {"greetingMessage": "Hello! How can I help you today?"},
        "outbound_config": {"greetingMessage": "Hello! This is a call from our service."},
        "voicemail_detection": {"enabled": True, "confidenceThreshold": 0.7, "minDetectionTime": 3, "keywords": ["voicemail", "leave a message", "after the beep", "not available"]},
        "test_call_logs": [], "has_completed_test_call": False, "created_at": now2, "updated_at": now2,
    }
    await db.diy_agents.insert_one(agent_doc)

    linked_agent = {"user_id": user_id, "name": agent_doc["name"], "config": {}, "max_concurrent": 10, "managed_by": "diy", "is_active": True, "approval_status": "approved", "version": 1, "created_at": now2, "updated_at": now2}
    try:
        await db.agents.insert_one(linked_agent)
    except Exception as e:
        logger.exception(f"[DIY Admin] Failed to create linked agent for DIY user {user_id}: {e}")
        raise HTTPException(status_code=500, detail=f"DIY user created but linked agent could not be created: {str(e)}")

    logger.info(f"[DIY Admin] DIY user created: {email} by admin={creator_id}")
    return {"success": True, "data": {"id": user_id, "email": email, "name": body.name.strip(), "password": password}, "message": "DIY user created successfully"}


# ═══════════════════════════════════════════════
#  DIY USER DETAIL
# ═══════════════════════════════════════════════

@router.get("/users/{user_id}/detail")
async def get_diy_user_detail(user_id: str, current_user: dict = Depends(get_current_user)):
    """GET /api/v1/admin/diy/users/:userId/detail — Full user detail for DIY user detail page."""
    _require_admin(current_user)
    try:
        user_doc = await db.users.find_one({"_id": ObjectId(user_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user ID")
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")

    assigned_phone_id = str(user_doc.get("assigned_phone_id") or "")
    phone_doc = None
    if assigned_phone_id:
        try:
            phone_doc = await db.phones.find_one({"_id": ObjectId(assigned_phone_id)})
        except Exception:
            pass

    agent = await db.diy_agents.find_one({"user_id": user_id})
    campaigns_count = await db.campaigns.count_documents({"user_id": user_id, "status": "active"})
    total_calls = await db.calls.count_documents({"user_id": user_id})

    return {
        "success": True,
        "data": {
            "id": user_id, "name": user_doc.get("name", ""), "email": user_doc.get("email", ""),
            "company": user_doc.get("company_name", ""), "phone": user_doc.get("phone", ""),
            "plan": user_doc.get("plan", ""), "credits": user_doc.get("credits", 0),
            "concurrency": user_doc.get("concurrency", 0), "status": user_doc.get("status", True),
            "statusExpiresAt": user_doc.get("status_expires_at"), "createdAt": user_doc.get("created_at"),
            "lastLogin": user_doc.get("last_login"), "onboardingComplete": bool(user_doc.get("onboarding_complete", False)),
            "isDiy": user_doc.get("is_diy", False),
            "assignedPhone": {"id": assigned_phone_id, "number": phone_doc.get("number") if phone_doc else None, "provider": phone_doc.get("provider") if phone_doc else None} if phone_doc else None,
            "agent": {"id": str(agent["_id"]), "name": agent.get("name", ""), "approvalStatus": agent.get("approval_status", "draft"), "version": agent.get("version", 1)} if agent else None,
            "stats": {"activeCampaigns": campaigns_count, "totalCalls": total_calls},
            "features": user_doc.get("features", {}),
        }
    }


# ═══════════════════════════════════════════════
#  UPDATE CREDITS / CONCURRENCY
# ═══════════════════════════════════════════════

class UpdateCreditsBody(BaseModel):
    amount: int
    reason: Optional[str] = "Admin update"


class UpdateConcurrencyBody(BaseModel):
    concurrency: int


@router.put("/users/{user_id}/credits")
async def update_diy_user_credits(user_id: str, body: UpdateCreditsBody, current_user: dict = Depends(get_current_user)):
    """PUT /api/v1/admin/diy/users/:userId/credits — Add/remove credits for a DIY user."""
    _require_admin(current_user)
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user ID")

    user_doc = await db.users.find_one({"_id": oid})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")

    now = datetime.now(timezone.utc)
    prev = user_doc.get("credits", 0)
    new_credits = max(0, prev + body.amount)

    assigner_doc = None
    if body.amount > 0:
        assigner_id = current_user.get("user_id")
        assigner_doc = await db.users.find_one({"_id": ObjectId(assigner_id)})
        if not assigner_doc:
            raise HTTPException(status_code=400, detail="Assigner user not found")
        assigner_current = assigner_doc.get("credits", 0)
        if assigner_current < body.amount:
            raise HTTPException(status_code=400, detail=f"Insufficient credits. You have {assigner_current} but are trying to assign {body.amount}.")
        assigner_new = assigner_current - body.amount
        await db.users.update_one({"_id": ObjectId(assigner_id)}, {"$set": {"credits": assigner_new, "updated_at": now}})
        await db.credit_transactions.insert_one({
            "user_id": assigner_id, "user_email": assigner_doc["email"], "user_role": assigner_doc.get("role"),
            "action": "deduct", "amount": body.amount, "previous_credits": assigner_current, "new_credits": assigner_new,
            "reason": f"Credits assigned to DIY user {user_doc['email']}: {body.reason}",
            "performed_by": assigner_id, "performed_by_email": assigner_doc["email"],
            "target_user_id": user_id, "target_user_email": user_doc["email"], "created_at": now,
        })

    await db.users.update_one({"_id": oid}, {"$set": {"credits": new_credits, "updated_at": now}})
    assigner_email = assigner_doc["email"] if assigner_doc else current_user.get("email")
    await db.credit_transactions.insert_one({
        "user_id": user_id, "user_email": user_doc["email"], "user_role": user_doc.get("role"),
        "action": "assign" if body.amount > 0 else "remove", "amount": abs(body.amount),
        "previous_credits": prev, "new_credits": new_credits, "reason": body.reason,
        "performed_by": current_user.get("user_id"), "performed_by_email": assigner_email,
        "created_at": now,
    })
    return {"success": True, "data": {"credits": new_credits, "previous": prev}}


@router.put("/users/{user_id}/concurrency")
async def update_diy_user_concurrency(user_id: str, body: UpdateConcurrencyBody, current_user: dict = Depends(get_current_user)):
    """PUT /api/v1/admin/diy/users/:userId/concurrency — Set concurrency limit for a DIY user."""
    _require_admin(current_user)
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user ID")

    user_doc = await db.users.find_one({"_id": oid})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")

    now = datetime.now(timezone.utc)
    new_val = max(0, body.concurrency)
    current_val = user_doc.get("concurrency", 0)
    delta = new_val - current_val

    assigner_id = current_user.get("user_id")
    assigner_doc = await db.users.find_one({"_id": ObjectId(assigner_id)})
    if not assigner_doc:
        raise HTTPException(status_code=400, detail="Assigner user not found")

    assigner_pool = assigner_doc.get("concurrency", 0)
    if delta > 0:
        if assigner_pool < delta:
            raise HTTPException(status_code=400, detail=f"Insufficient concurrency slots. You have {assigner_pool} but need {delta} more.")
        assigner_new_pool = assigner_pool - delta
        await db.users.update_one({"_id": ObjectId(assigner_id)}, {"$set": {"concurrency": assigner_new_pool, "updated_at": now}})
        await db.concurrency_transactions.insert_one({
            "user_id": assigner_id, "user_email": assigner_doc["email"], "user_role": assigner_doc.get("role"),
            "action": "deduct", "amount": delta, "previous_concurrency": assigner_pool, "new_concurrency": assigner_new_pool,
            "reason": f"Assigned to DIY user {user_doc['email']}", "performed_by": assigner_id,
            "target_user_id": user_id, "target_user_email": user_doc["email"], "created_at": now,
        })
    elif delta < 0:
        return_amount = abs(delta)
        assigner_new_pool = assigner_pool + return_amount
        await db.users.update_one({"_id": ObjectId(assigner_id)}, {"$set": {"concurrency": assigner_new_pool, "updated_at": now}})
        await db.concurrency_transactions.insert_one({
            "user_id": assigner_id, "user_email": assigner_doc["email"], "user_role": assigner_doc.get("role"),
            "action": "return", "amount": return_amount, "previous_concurrency": assigner_pool, "new_concurrency": assigner_new_pool,
            "reason": f"Returned from DIY user {user_doc['email']}", "performed_by": assigner_id,
            "target_user_id": user_id, "target_user_email": user_doc["email"], "created_at": now,
        })

    await db.users.update_one({"_id": oid}, {"$set": {"concurrency": new_val, "concurrency_assigned_by": assigner_id, "updated_at": now}})
    await db.concurrency_transactions.insert_one({
        "user_id": user_id, "user_email": user_doc["email"], "user_role": user_doc.get("role"),
        "action": "assign" if delta >= 0 else "revoke", "amount": abs(delta),
        "previous_concurrency": current_val, "new_concurrency": new_val,
        "reason": f"Set by admin {assigner_doc['email']}", "performed_by": assigner_id,
        "created_at": now,
    })
    return {"success": True, "data": {"concurrency": new_val, "previous": current_val}}


# ═══════════════════════════════════════════════
#  PREMIUM FEATURES
# ═══════════════════════════════════════════════

class FeaturesBody(BaseModel):
    features: Dict[str, bool]


@router.get("/users/{user_id}/features")
async def get_diy_user_features(user_id: str, current_user: dict = Depends(get_current_user)):
    """GET /api/v1/admin/diy/users/:userId/features"""
    _require_admin(current_user)
    try:
        user_doc = await db.users.find_one({"_id": ObjectId(user_id)}, {"features": 1})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user ID")
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")
    features = user_doc.get("features", {f: False for f in PREMIUM_FEATURES})
    return {"success": True, "data": {"features": features}}


@router.put("/users/{user_id}/features")
async def update_diy_user_features(user_id: str, body: FeaturesBody, current_user: dict = Depends(get_current_user)):
    """PUT /api/v1/admin/diy/users/:userId/features"""
    _require_admin(current_user)
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user ID")
    features = {k: bool(body.features.get(k, False)) for k in PREMIUM_FEATURES}
    await db.users.update_one({"_id": oid}, {"$set": {"features": features, "updated_at": datetime.now(timezone.utc)}})
    return {"success": True, "data": {"features": features}}
