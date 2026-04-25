"""Scheduled calls list and cancel endpoints. User isolation: role 3 = user_id, role 4 = default, admin = all."""

from typing import Optional
from datetime import datetime, timezone

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query

from utils.db import db
from utils.security import get_current_user
from utils.logger import logger
from models import ROLE_USER, ROLE_DEMO

router = APIRouter(prefix="/api/scheduled-calls", tags=["Scheduled Calls"])

MAX_LIMIT = 200


def _effective_user_scope(current_user: dict) -> Optional[str]:
    """Owner scope: role 3 = user_id, role 4 = default, admin = None (all)."""
    role = current_user.get("role", ROLE_USER)
    user_id = current_user.get("user_id")
    if role == ROLE_USER:
        return user_id
    if role == ROLE_DEMO:
        return "default"
    return None


def _safe_iso(val) -> str:
    if isinstance(val, datetime):
        return val.isoformat()
    return str(val) if val else ""


@router.get("")
async def list_scheduled_calls(
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None, description="Filter by status: pending, completed, failed, processing"),
    limit: int = Query(50, ge=1, le=MAX_LIMIT),
    skip: int = Query(0, ge=0),
):
    """List scheduled calls for the logged-in user."""
    try:
        scope = _effective_user_scope(current_user)
        filt: dict = {}
        if scope:
            filt["user_id"] = scope
        if status and status.strip().lower() != "all":
            filt["status"] = status.strip().lower()

        limit = min(limit, MAX_LIMIT)
        cursor = db.scheduled_calls.find(filt).sort("scheduledFor", -1).skip(skip).limit(limit)
        docs = await cursor.to_list(length=limit)
        total = await db.scheduled_calls.count_documents(filt)

        # Batch-fetch agent names
        user_ids = list({d.get("user_id") for d in docs if d.get("user_id")})
        agents_map: dict[str, str] = {}
        if user_ids:
            async for agent in db.agents.find(
                {"$or": [{"user_id": {"$in": user_ids}}, {"tenant_id": {"$in": user_ids}}]},
                {"user_id": 1, "tenant_id": 1, "name": 1},
            ):
                s = (agent.get("user_id") or agent.get("tenant_id") or "").strip()
                if s:
                    agents_map[s] = agent.get("name") or "AI Agent"

        # Batch-fetch call durations for completed/triggered scheduled calls
        call_sids = [str((doc.get("metadata") or {}).get("callSid") or "") for doc in docs]
        call_sids = [c for c in call_sids if c]
        duration_map: dict[str, int] = {}
        if call_sids:
            async for call_doc in db.calls.find(
                {"call_id": {"$in": call_sids}},
                {"call_id": 1, "duration": 1},
            ):
                cid = call_doc.get("call_id")
                if cid is not None:
                    duration_map[str(cid)] = call_doc.get("duration") if call_doc.get("duration") is not None else 0

        items = []
        now = datetime.now(timezone.utc)
        for doc in docs:
            try:
                scheduled_for = doc.get("scheduledFor")
                time_until = None
                if scheduled_for and isinstance(scheduled_for, datetime):
                    sf = scheduled_for if scheduled_for.tzinfo else scheduled_for.replace(tzinfo=timezone.utc)
                    diff = (sf - now).total_seconds()
                    if diff > 0:
                        mins = int(diff // 60)
                        hrs = mins // 60
                        mins_rem = mins % 60
                        time_until = f"{hrs}h {mins_rem}m" if hrs > 0 else f"{mins_rem}m"

                doc_tenant = doc.get("user_id") or ""
                meta = doc.get("metadata") or {}
                call_sid = str(meta.get("callSid") or "")
                duration_sec = duration_map.get(call_sid) if call_sid else None

                items.append({
                    "id": str(doc.get("_id", "")),
                    "phoneNumber": doc.get("phoneNumber") or "",
                    "agent": agents_map.get(doc_tenant, "AI Agent"),
                    "scheduledFor": _safe_iso(scheduled_for),
                    "timeUntil": time_until,
                    "status": doc.get("status") or "pending",
                    "createdAt": _safe_iso(doc.get("createdAt")),
                    "sourceCallId": str(meta.get("sourceCallId") or ""),
                    "reason": str(meta.get("reason") or ""),
                    "callId": call_sid,
                    "duration": duration_sec,
                })
            except Exception as e:
                logger.warning(f"[ScheduledCalls] Skipped doc {doc.get('_id')}: {e}")

        return {"scheduled_calls": items, "total": total}
    except Exception as e:
        logger.exception(f"[ScheduledCalls] list_scheduled_calls error: {e}")
        return {"scheduled_calls": [], "total": 0, "error": str(e)}


@router.delete("/{scheduled_call_id}")
async def cancel_scheduled_call(
    scheduled_call_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Cancel a pending scheduled call by setting status to cancelled."""
    try:
        oid = ObjectId(scheduled_call_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid scheduled call ID.")

    current_user_id = current_user.get("user_id")
    role = current_user.get("role", 3)

    doc = await db.scheduled_calls.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Scheduled call not found.")

    if role not in (0, 1) and doc.get("user_id") != current_user_id:
        raise HTTPException(status_code=403, detail="Not allowed.")

    if doc.get("status") != "pending":
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel a call with status '{doc.get('status')}'. Only pending calls can be cancelled.",
        )

    await db.scheduled_calls.update_one(
        {"_id": oid},
        {"$set": {"status": "cancelled", "updatedAt": datetime.now(timezone.utc)}},
    )

    return {"status": "cancelled", "id": scheduled_call_id}
