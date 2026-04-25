"""Recording endpoints: presigned URL for playback, manual fetch trigger, list.
Role filter: user=own, reseller=created users, admin=all."""

from datetime import datetime, timezone
from typing import Any, Optional

import boto3
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from utils.db import db
from utils.security import get_current_user
from utils.logger import logger
from models import ROLE_USER, ROLE_DEMO, ROLE_RESELLER

router = APIRouter(prefix="/api/recordings", tags=["Recordings"])


async def _can_access_recordings(tenant_id: str, current_user: dict) -> bool:
    """Same access rules as call records."""
    role = current_user.get("role", ROLE_USER)
    uid = current_user.get("user_id")
    if role == ROLE_USER:
        return tenant_id == uid
    if role == ROLE_DEMO:
        return tenant_id == "default"
    if role == ROLE_RESELLER:
        if not uid:
            return False
        created = await db.users.find({"created_by": uid}).to_list(length=None)
        return tenant_id in [str(u["_id"]) for u in created]
    return True


async def _recordings_filter(current_user: dict) -> dict[str, Any]:
    """MongoDB filter: user=own, reseller=created users, super_admin/admin=all."""
    role = current_user.get("role", ROLE_USER)
    uid = current_user.get("user_id")
    if role == ROLE_USER:
        return {"tenant_id": uid} if uid else {}
    if role == ROLE_DEMO:
        return {"tenant_id": "default"}
    if role == ROLE_RESELLER:
        if not uid:
            return {}
        created = await db.users.find({"created_by": uid}).to_list(length=None)
        tenant_ids = [str(u["_id"]) for u in created]
        if not tenant_ids:
            return {"tenant_id": "__none__"}
        return {"tenant_id": {"$in": tenant_ids}}
    return {}


@router.get("/{call_id}/url")
async def get_recording_url(call_id: str, current_user: dict = Depends(get_current_user)):
    """Return a presigned S3 URL for streaming/downloading the call recording."""
    call = await db.calls.find_one({"call_id": call_id})
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    if not await _can_access_recordings(call.get("tenant_id", ""), current_user):
        raise HTTPException(status_code=404, detail="Not found")

    s3_key = call.get("s3_recording_key")
    if not s3_key:
        rec_status = call.get("recording_fetch_status", "pending")
        return {
            "available": False,
            "status": rec_status,
            "message": "Recording not yet available" if rec_status == "pending" else f"Recording status: {rec_status}",
        }

    from services.s3_service import get_presigned_url
    url = get_presigned_url(s3_key)
    if not url:
        raise HTTPException(status_code=500, detail="Failed to generate presigned URL")

    from config import get_settings
    return {
        "available": True,
        "url": url,
        "expires_in": get_settings().AWS_S3_PRESIGNED_URL_EXPIRY,
        "s3_key": s3_key,
    }


@router.get("/{call_id}/stream")
async def stream_recording(call_id: str, current_user: dict = Depends(get_current_user)):
    """Stream the recording audio from S3 through the backend (bypasses CORS)."""
    call = await db.calls.find_one({"call_id": call_id})
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    if not await _can_access_recordings(call.get("tenant_id", ""), current_user):
        raise HTTPException(status_code=404, detail="Not found")

    s3_key = call.get("s3_recording_key")
    if not s3_key:
        raise HTTPException(status_code=404, detail="Recording not available")

    from config import get_settings
    settings = get_settings()

    s3 = boto3.client(
        "s3",
        region_name=settings.AWS_REGION,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    )
    try:
        s3_obj = s3.get_object(Bucket=settings.AWS_S3_BUCKET, Key=s3_key)
    except Exception as e:
        logger.error(f"[Recordings] S3 get_object error for {s3_key}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch recording from S3")

    audio_bytes = s3_obj["Body"].read()
    content_type = s3_obj.get("ContentType", "audio/mpeg")
    headers = {
        "Content-Length": str(len(audio_bytes)),
        "Content-Disposition": f'inline; filename="recording_{call_id}.mp3"',
    }
    logger.info(f"[Recordings] Streaming {len(audio_bytes)} bytes for call_id={call_id}")

    async def iter_audio():
        yield audio_bytes

    return StreamingResponse(iter_audio(), media_type=content_type, headers=headers)


@router.post("/{call_id}/fetch")
async def fetch_recording(call_id: str, current_user: dict = Depends(get_current_user)):
    """Manually trigger recording fetch + S3 upload for a specific call."""
    call = await db.calls.find_one({"call_id": call_id})
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    if not await _can_access_recordings(call.get("tenant_id", ""), current_user):
        raise HTTPException(status_code=404, detail="Not found")

    if call.get("s3_recording_key"):
        return {"status": "already_available", "s3_key": call["s3_recording_key"]}

    from config import get_settings
    settings = get_settings()

    if not settings.EXOTEL_API_KEY or not (settings.EXOTEL_AUTH_TOKEN or settings.EXOTEL_API_TOKEN):
        raise HTTPException(status_code=500, detail="Exotel credentials not configured")

    from services.recording_fetcher import fetch_recording_url, download_recording
    rec_url = call.get("recording_url") or await fetch_recording_url(call_id, settings)
    if not rec_url:
        await db.calls.update_one(
            {"call_id": call_id},
            {"$set": {"recording_fetch_status": "not_available", "last_recording_fetch_at": datetime.now(timezone.utc)}},
        )
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
        {"$set": {
            "recording_url": rec_url,
            "s3_recording_key": result["key"],
            "s3_recording_uploaded_at": datetime.now(timezone.utc),
            "recording_fetch_status": "available",
            "last_recording_fetch_at": datetime.now(timezone.utc),
        }},
    )
    return {"status": "available", "s3_key": result["key"]}


@router.get("")
async def list_recordings(
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None, description="Filter: available, pending, not_available"),
    limit: int = Query(25, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """List calls with recording status. User=own, reseller=created users, super_admin/admin=all. Paginated."""
    filt = await _recordings_filter(current_user)

    if status == "available":
        filt["s3_recording_key"] = {"$exists": True}
    elif status == "pending":
        filt["s3_recording_key"] = {"$exists": False}
        filt["recording_fetch_status"] = {"$nin": ["not_available", "permanent_fail"]}
    elif status == "not_available":
        filt["recording_fetch_status"] = "not_available"

    total = await db.calls.count_documents(filt)
    calls = await db.calls.find(
        filt,
        projection={
            "_id": 0, "call_id": 1, "tenant_id": 1, "to_number": 1, "from_number": 1,
            "direction": 1, "status": 1, "created_at": 1, "duration": 1,
            "recording_url": 1, "s3_recording_key": 1, "recording_fetch_status": 1,
        },
    ).sort("created_at", -1).skip(skip).limit(limit).to_list(length=limit)

    return {"recordings": calls, "count": len(calls), "total": total}
