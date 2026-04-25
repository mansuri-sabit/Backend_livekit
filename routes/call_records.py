"""
Call record list and detail endpoints.

NOTE: Named call_records.py (not calls.py) to avoid ANY confusion with the
PROTECTED file routers/calls.py which handles Exotel outbound triggering.
These are two completely separate concerns:
  routers/calls.py     → /calls/outgoing  (PROTECTED: triggers live Exotel calls)
  routes/call_records.py → /call-records  (call history, transcripts, translation)

User isolation: user=own, reseller=created users only, super_admin/admin=all.
"""

import re
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query

from utils.db import db
from utils.security import get_current_user
from models import ROLE_USER, ROLE_DEMO, ROLE_RESELLER, ROLE_ADMIN, ROLE_SUPER_ADMIN

_IST = timezone(timedelta(hours=5, minutes=30))
_DATETIME_FIELDS = ("created_at", "updated_at", "started_at", "ended_at")

# Fields required by admin dashboard CallLogs.jsx / CallDetailsModal
_REQUIRED_CALL_TOP_LEVEL = {
    "call_id", "tenant_id", "to_number", "from_number", "direction",
    "status", "outcome", "transcript", "summary", "duration", "credits",
    "campaign_id", "created_at", "started_at", "ended_at", "is_test",
    "recording_url", "s3_recording_key", "recording_fetch_status",
    "metadata",
}
_REQUIRED_METADATA_KEYS = ("transferRequested", "transferToNumber", "userInactivity", "voicemailDetected")


def _ensure_call_response_shape(c: dict) -> None:
    """Ensure call dict has all fields expected by admin dashboard; missing keys set to None."""
    for k in _REQUIRED_CALL_TOP_LEVEL:
        if k not in c:
            c[k] = None
    meta = c.get("metadata")
    if meta is None or not isinstance(meta, dict):
        c["metadata"] = {}
        meta = c["metadata"]
    for k in _REQUIRED_METADATA_KEYS:
        if k not in meta:
            meta[k] = None


def _to_ist_str(dt: datetime) -> str:
    """Convert a naive UTC datetime (as stored by MongoDB) to an IST ISO string."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_IST).isoformat()


def _normalize_datetimes(c: dict) -> None:
    """Convert all datetime fields in a call dict to IST ISO strings."""
    for field in _DATETIME_FIELDS:
        val = c.get(field)
        if isinstance(val, datetime):
            c[field] = _to_ist_str(val)


router = APIRouter(prefix="/call-records", tags=["Call Records"])

CALLS_MAX_LIMIT = 500


def _normalize_credits(c: dict) -> None:
    """1 sec = 1 credit; set credits from duration when credits missing or 0."""
    duration = c.get("duration")
    if duration is not None and (c.get("credits") is None or c.get("credits") == 0):
        c["credits"] = duration


def _set_transferred(c: dict) -> None:
    """
    Set transferred boolean for list/detail responses.
    Relies on metadata.transferRequested (set when a real transfer is triggered for Exotel).
    """
    meta = c.get("metadata") or {}
    c["transferred"] = meta.get("transferRequested") is True


async def _calls_filter(current_user: dict) -> dict[str, Any]:
    """MongoDB filter for list_calls: user=own, reseller=created users, super_admin/admin=all."""
    role = current_user.get("role", ROLE_USER)
    user_id = current_user.get("user_id")
    if role == ROLE_USER:
        return {"tenant_id": user_id} if user_id else {}
    if role == ROLE_DEMO:
        return {"tenant_id": "default"}
    if role == ROLE_RESELLER:
        if not user_id:
            return {}
        created = await db.users.find({"created_by": user_id}).to_list(length=None)
        tenant_ids = [str(u["_id"]) for u in created]
        tenant_ids.append(user_id)
        return {"tenant_id": {"$in": tenant_ids}}
    # ROLE_SUPER_ADMIN, ROLE_ADMIN: see all
    return {}


async def _can_access_tenant(tenant_id: str, current_user: dict) -> bool:
    """True if user can access this tenant's calls."""
    role = current_user.get("role", ROLE_USER)
    user_id = current_user.get("user_id")
    if role == ROLE_USER:
        return tenant_id == user_id
    if role == ROLE_DEMO:
        return tenant_id == "default"
    if role == ROLE_RESELLER:
        if not user_id:
            return False
        created = await db.users.find({"created_by": user_id}).to_list(length=None)
        tenant_ids = [str(u["_id"]) for u in created]
        return tenant_id in tenant_ids
    return True


def _apply_list_filters(
    filt: dict,
    direction: Optional[str],
    status: Optional[str],
    transferred: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    campaign_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
) -> None:
    """Apply optional query params to filter dict for list_calls."""
    if direction and direction.lower() in ("inbound", "outbound"):
        filt["direction"] = direction.lower()
    if status and status.lower() not in ("all", "all status"):
        s = status.lower().replace(" ", "-")
        filt["status"] = {"$regex": re.escape(s), "$options": "i"}
    if transferred and transferred.lower() in ("transferred", "not transferred"):
        if transferred.lower() == "transferred":
            filt["metadata.transferRequested"] = True
        else:
            filt["$or"] = [
                {"metadata.transferRequested": {"$ne": True}},
                {"metadata.transferRequested": {"$exists": False}},
            ]
    if campaign_id:
        filt["campaign_id"] = campaign_id
    if tenant_id:
        filt["tenant_id"] = tenant_id
    if from_date:
        try:
            d = datetime.fromisoformat(from_date.split("T")[0])
            filt.setdefault("created_at", {})["$gte"] = d
        except Exception:
            pass
    if to_date:
        try:
            d = datetime.fromisoformat(to_date.split("T")[0] + "T23:59:59.999")
            filt.setdefault("created_at", {})["$lte"] = d
        except Exception:
            pass


@router.get("")
async def list_calls(
    current_user: dict = Depends(get_current_user),
    limit: int = Query(25, ge=1, le=CALLS_MAX_LIMIT),
    skip: int = Query(0, ge=0),
    direction: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None, description="Filter by campaign ID"),
    tenant_id: Optional[str] = Query(None, description="Filter by tenant (admin/super_admin only)"),
    transferred: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
):
    """Get call history. Returns { calls, count, total }. User=own, reseller=created users, super_admin/admin=all."""
    limit = min(limit, CALLS_MAX_LIMIT)
    filt = await _calls_filter(current_user)
    tenant_filter = tenant_id if current_user.get("role") in (ROLE_ADMIN, ROLE_SUPER_ADMIN) and tenant_id else None
    _apply_list_filters(filt, direction, status, transferred, from_date, to_date, campaign_id=campaign_id, tenant_id=tenant_filter)
    total = await db.calls.count_documents(filt)
    calls = await db.calls.find(filt).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    for c in calls:
        c.pop("_id", None)
        _ensure_call_response_shape(c)
        _normalize_credits(c)
        _set_transferred(c)
        _normalize_datetimes(c)
    # Enrich with campaign names
    campaign_ids = list({c["campaign_id"] for c in calls if c.get("campaign_id")})
    if campaign_ids:
        camps = await db.campaigns.find(
            {"_id": {"$in": [ObjectId(cid) for cid in campaign_ids if ObjectId.is_valid(cid)]}},
            {"_id": 1, "name": 1},
        ).to_list(length=None)
        camp_map = {str(c["_id"]): c.get("name") for c in camps}
        for c in calls:
            if c.get("campaign_id"):
                c["campaign_name"] = camp_map.get(c["campaign_id"])
    return {"calls": calls, "count": len(calls), "total": total}


@router.get("/by-id/{call_id}")
async def get_call_by_id(call_id: str, current_user: dict = Depends(get_current_user)):
    """Get single full call document by call_id. Access: user=own, reseller=created users, admin=all."""
    call = await db.calls.find_one({"call_id": call_id})
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    tenant_id = call.get("tenant_id", "")
    if not await _can_access_tenant(tenant_id, current_user):
        raise HTTPException(status_code=404, detail="Not found")
    call.pop("_id", None)
    _ensure_call_response_shape(call)
    _normalize_credits(call)
    _set_transferred(call)
    _normalize_datetimes(call)
    return call


@router.get("/{tenant_id}")
async def get_calls_by_tenant(
    tenant_id: str,
    current_user: dict = Depends(get_current_user),
    limit: int = Query(25, ge=1, le=CALLS_MAX_LIMIT),
    skip: int = 0,
):
    """Get call history for a specific tenant."""
    if not await _can_access_tenant(tenant_id, current_user):
        raise HTTPException(status_code=404, detail="Not found.")
    limit = min(limit, CALLS_MAX_LIMIT)
    calls = await db.calls.find({"tenant_id": tenant_id}).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    for c in calls:
        c.pop("_id", None)
        _normalize_credits(c)
        _set_transferred(c)
        _normalize_datetimes(c)
    return {"calls": calls, "count": len(calls)}


@router.get("/{tenant_id}/{call_id}")
async def get_call(tenant_id: str, call_id: str, current_user: dict = Depends(get_current_user)):
    """Get single call details by tenant + call_id."""
    if not await _can_access_tenant(tenant_id, current_user):
        raise HTTPException(status_code=404, detail="Not found.")
    call = await db.calls.find_one({"tenant_id": tenant_id, "call_id": call_id})
    if call:
        call.pop("_id", None)
        _normalize_credits(call)
        _set_transferred(call)
        _normalize_datetimes(call)
    return call or {"error": "Call not found"}


@router.post("/{call_id}/translate")
async def translate_call_content(
    call_id: str,
    body: dict,
    current_user: dict = Depends(get_current_user),
):
    """Translate call transcript and summary to target language via Azure Translator."""
    import httpx
    from config import get_settings
    settings = get_settings()

    target_lang = body.get("language") or body.get("target_lang") or "English"

    lang_map = {
        "Hindi": "hi",
        "English": "en",
        "Marathi": "mr",
        "Tamil": "ta",
        "Telugu": "te",
        "Bengali": "bn",
        "Kannada": "kn",
        "Gujarati": "gu",
    }
    lang_code = lang_map.get(target_lang, "en")

    call = await db.calls.find_one({"call_id": call_id})
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    if not await _can_access_tenant(call.get("tenant_id", ""), current_user):
        raise HTTPException(status_code=404, detail="Call not found")

    raw_transcript = call.get("transcript")
    summary = call.get("summary") or ""

    if isinstance(raw_transcript, list):
        transcript = "\n".join(
            (item.get("content") or "").strip()
            for item in raw_transcript
            if isinstance(item, dict) and (item.get("content") or "").strip()
        )
    else:
        transcript = (raw_transcript or "").strip() if raw_transcript else ""

    if not transcript and not summary:
        return {"transcript": "", "summary": ""}

    texts = []
    if transcript:
        texts.append({"text": transcript})
    if summary:
        texts.append({"text": summary})

    url = f"{settings.AZURE_TRANSLATOR_ENDPOINT}/translate"
    headers = {
        "Ocp-Apim-Subscription-Key": settings.AZURE_TRANSLATOR_KEY or "",
        "Ocp-Apim-Subscription-Region": settings.AZURE_TRANSLATOR_REGION or "",
        "Content-Type": "application/json",
    }
    params = {"api-version": "3.0", "to": lang_code}

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, headers=headers, params=params, json=texts)
        resp.raise_for_status()
        results = resp.json()

    translated_texts = [r["translations"][0]["text"] for r in results]

    result: dict = {}
    idx = 0
    if transcript:
        result["transcript"] = translated_texts[idx]
        idx += 1
    else:
        result["transcript"] = ""
    if summary:
        result["summary"] = translated_texts[idx]
    else:
        result["summary"] = ""

    return result
