"""Leads list and status toggle. User isolation: role 3=user_id, role 4=default, admin=all."""

from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from models import ROLE_USER, ROLE_DEMO
from utils.db import db
from utils.security import get_current_user
from utils.logger import logger

router = APIRouter(prefix="/api/leads", tags=["Leads"])

MAX_LIMIT = 200


def _effective_user_scope(current_user: dict) -> Optional[str]:
    """Owner scope: role 3=user_id, role 4=default, admin=None (all)."""
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
async def list_leads(
    current_user: dict = Depends(get_current_user),
    limit: int = Query(25, ge=1, le=MAX_LIMIT),
    skip: int = Query(0, ge=0),
):
    """List leads for the logged-in user."""
    try:
        scope = _effective_user_scope(current_user)
        filt: dict = {}
        if scope:
            filt["user_id"] = scope

        limit = min(limit, MAX_LIMIT)
        total = await db.leads.count_documents(filt)
        docs = await db.leads.find(filt).sort("created_at", -1).skip(skip).limit(limit).to_list(length=limit)

        leads = []
        for doc in docs:
            try:
                leads.append({
                    "id": str(doc.get("_id", "")),
                    "call_id": doc.get("call_id", ""),
                    "from_number": doc.get("from_number", ""),
                    "to_number": doc.get("to_number", ""),
                    "direction": doc.get("direction", "outbound"),
                    "status": doc.get("status", ""),
                    "outcome": doc.get("outcome", ""),
                    "duration": doc.get("duration"),
                    "transcript": doc.get("transcript", []),
                    "summary": doc.get("summary", ""),
                    "campaign_id": doc.get("campaign_id", ""),
                    "action_status": doc.get("action_status", "pending"),
                    "detected_keywords": doc.get("detected_keywords", []),
                    "created_at": _safe_iso(doc.get("created_at")),
                    "updated_at": _safe_iso(doc.get("updated_at")),
                })
            except Exception as e:
                logger.warning(f"[Leads] Skipped doc {doc.get('_id')}: {e}")

        return {"leads": leads, "total": total}
    except Exception as e:
        logger.exception(f"[Leads] list_leads error: {e}")
        return {"leads": [], "total": 0, "error": str(e)}


class StatusUpdate(BaseModel):
    action_status: str


@router.patch("/{lead_id}/status")
async def update_lead_status(
    lead_id: str,
    body: StatusUpdate,
    current_user: dict = Depends(get_current_user),
):
    """Toggle lead action_status (pending/completed). Updates all leads with same phone."""
    if body.action_status not in ("pending", "completed"):
        raise HTTPException(400, "action_status must be 'pending' or 'completed'")

    try:
        oid = ObjectId(lead_id)
    except Exception:
        raise HTTPException(400, "Invalid lead ID")

    scope = _effective_user_scope(current_user)
    filt: dict = {"_id": oid}
    if scope:
        filt["user_id"] = scope

    result = await db.leads.find_one_and_update(
        filt,
        {"$set": {"action_status": body.action_status, "updated_at": datetime.now(timezone.utc)}},
    )
    if not result:
        raise HTTPException(404, "Lead not found")

    # Sync all leads with the same phone number
    phone = result.get("to_number") if result.get("direction") == "outbound" else result.get("from_number")
    if phone and scope:
        try:
            await db.leads.update_many(
                {
                    "user_id": scope,
                    "_id": {"$ne": oid},
                    "$or": [{"to_number": phone}, {"from_number": phone}],
                },
                {"$set": {"action_status": body.action_status, "updated_at": datetime.now(timezone.utc)}},
            )
        except Exception as e:
            logger.warning(f"[Leads] Failed to update sibling leads for phone {phone}: {e}")

    return {"status": "updated", "action_status": body.action_status, "id": lead_id}
