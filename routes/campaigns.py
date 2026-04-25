"""
Campaign CRUD and create/schedule endpoints — matches frontend Campaigns page.

User-based isolation:
- role 3 (user): sees only own campaigns (tenant_id=user_id)
- role 4 (demo): uses tenant_id=default
- admin/super-admin: all campaigns

NOTE: Campaign dispatch calls routes/outbound._trigger_single_call which
uses services/exotel_service.py settings via ExotelService's config in
config.py — no direct Pipecat pipeline dependency.
"""

import re
from datetime import datetime, timezone, timedelta
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from models import Campaign, CampaignContact, CallRecord, ROLE_USER, ROLE_DEMO, ROLE_RESELLER
from utils.db import db, redis_client
from utils.logger import logger
from utils.security import get_current_user
from utils.audit import log_audit

router = APIRouter(prefix="/api/campaigns", tags=["Campaigns"])

CAMPAIGN_LIST_MAX_PER_PAGE = 100


def _campaign_access_ok(doc: dict, current_user: dict) -> bool:
    """True if current user can access this campaign."""
    if not doc:
        return False
    role = current_user.get("role", ROLE_USER)
    user_id = current_user.get("user_id")
    tenant = doc.get("tenant_id") or ""
    if role == ROLE_USER:
        return tenant == user_id
    if role == ROLE_DEMO:
        return tenant == "default"
    return True


_IST = timezone(timedelta(hours=5, minutes=30))


def _to_ist_str(dt) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_IST).isoformat()
    return None


_RETRY_MODE_LABELS = {
    "10min": "Retry in 10 min",
    "1hour": "Retry in 1 hour",
    "custom": "Custom Retry",
    "scheduled": "Scheduled Retry",
}


def _retry_display_status(doc: dict) -> str | None:
    """If campaign status is retry_scheduled, return a human-friendly label based on retry_config.mode."""
    status = (doc.get("status") or "").lower()
    if status != "retry_scheduled":
        return None
    rc = doc.get("retry_config") or {}
    mode = rc.get("mode", "10min")
    return _RETRY_MODE_LABELS.get(mode, "Retry Scheduled")


def _safe_date_str(d) -> str:
    """Format date for list view; handles None or non-datetime."""
    if d is None:
        return ""
    if isinstance(d, datetime):
        return f"{d.month}/{d.day}/{d.year}"
    return ""


async def _campaign_progress_from_calls(campaign_ids: list[str]) -> dict[str, dict]:
    """
    Derive per-contact campaign stats from db.calls for campaign progress.

    Aggregates by (campaign_id, to_number) — only latest status per contact.
    Returns {campaign_id: {"completed", "failed", "transferred", "in_progress", "voicemail"}}
    """
    if not campaign_ids:
        return {}
    status_lower = {"$toLower": {"$ifNull": ["$status", ""]}}
    failure_reason_lower = {"$toLower": {"$ifNull": ["$failure_reason", ""]}}
    outcome_lower = {"$toLower": {"$ifNull": ["$outcome", ""]}}
    transfer_flag = {"$ifNull": ["$metadata.transferRequested", False]}

    pipeline = [
        {"$match": {"campaign_id": {"$in": campaign_ids}}},
        {
            "$addFields": {
                "_status_lower": status_lower,
                "_failure_reason": failure_reason_lower,
                "_outcome_lower": outcome_lower,
                "_transfer_flag": transfer_flag,
            }
        },
        {"$sort": {"created_at": -1}},
        {
            "$group": {
                "_id": {"campaign_id": "$campaign_id", "to": "$to_number"},
                "latest_status": {"$first": "$_status_lower"},
                "latest_outcome": {"$first": "$_outcome_lower"},
                "latest_transfer_flag": {"$first": "$_transfer_flag"},
            }
        },
        {
            "$group": {
                "_id": "$_id.campaign_id",
                "completed": {
                    "$sum": {
                        "$cond": [{"$in": ["$latest_status", ["completed", "positive"]]}, 1, 0]
                    }
                },
                "failed": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$not": {"$in": ["$latest_status", ["ringing", "in-progress", "inprogress", "queued"]]}},
                                    {"$not": {"$in": ["$latest_status", ["completed", "positive"]]}},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                "transferred": {
                    "$sum": {
                        "$cond": [
                            {
                                "$or": [
                                    {"$eq": ["$latest_outcome", "transferred"]},
                                    {"$eq": ["$latest_transfer_flag", True]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                "in_progress": {
                    "$sum": {
                        "$cond": [
                            {"$in": ["$latest_status", ["ringing", "in-progress", "inprogress", "queued"]]},
                            1,
                            0,
                        ]
                    }
                },
                "voicemail": {
                    "$sum": {"$cond": [{"$eq": ["$latest_status", "voicemail"]}, 1, 0]}
                },
            }
        },
    ]
    out = {
        cid: {"completed": 0, "failed": 0, "transferred": 0, "in_progress": 0, "voicemail": 0}
        for cid in campaign_ids
    }
    try:
        async for row in db.calls.aggregate(pipeline):
            cid = row.get("_id")
            if cid and cid in out:
                out[cid] = {
                    "completed": int(row.get("completed") or 0),
                    "failed": int(row.get("failed") or 0),
                    "transferred": int(row.get("transferred") or 0),
                    "in_progress": int(row.get("in_progress") or 0),
                    "voicemail": int(row.get("voicemail") or 0),
                }
    except Exception as e:
        logger.debug(f"Campaign progress from calls failed: {e}")
    return out


async def _outbound_capacity_for_scope(scope: str) -> int:
    """Sum of outbound_concurrent_limit for active phones assigned to this scope."""
    if not scope:
        return 1
    query = {"status": "active", "$or": [{"assigned_to_user_id": scope}, {"user_id": scope}]}
    cursor = db.phones.find(query, {"outbound_concurrent_limit": 1})
    total = 0
    async for doc in cursor:
        total += max(0, int(doc.get("outbound_concurrent_limit") or 0))
    return max(1, total)


async def _effective_concurrency_cap(scope: str) -> int:
    """Effective max concurrent calls for a tenant based on phones + user concurrency setting."""
    phone_cap = await _outbound_capacity_for_scope(scope)
    try:
        oid = ObjectId(scope)
    except Exception:
        return phone_cap
    user_doc = await db.users.find_one({"_id": oid}, {"concurrency": 1})
    if not user_doc:
        return phone_cap
    limit = int(user_doc.get("concurrency") or 0)
    if limit <= 0:
        return phone_cap
    return max(1, min(phone_cap, limit))


def parse_phone_lines(text: str) -> list[dict]:
    """Parse 'number, name' or 'number' per line. Returns list of {number, name?}."""
    contacts = []
    seen = set()
    for line in (text or "").strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",", 1)]
        raw_num = "".join(c for c in parts[0] if c.isdigit())
        if len(raw_num) >= 10:
            num = raw_num[-10:]
            if num[0] not in ("6", "7", "8", "9"):
                continue
            if num in seen:
                continue
            seen.add(num)
            num = "+91" + num
            name = parts[1] if len(parts) > 1 and parts[1] else None
            contacts.append({"number": num, "name": name})
    return contacts


def to_e164_list(contacts: list[dict]) -> list[str]:
    """Return list of E.164 numbers from contacts."""
    return [
        c["number"] if c["number"].startswith("+") else "+91" + c["number"][-10:]
        for c in contacts
    ]


# ── Request/Response models ──────────────────────────────────────────────────


class RetryConfig(BaseModel):
    """Per-campaign retry configuration."""
    enabled: bool = False
    mode: str = Field("10min", description="10min | 1hour | custom | scheduled")
    custom_delay_minutes: Optional[int] = None
    scheduled_time: Optional[str] = None
    filter_dnd: bool = True


class CreateCampaignBody(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(..., min_length=1)
    phone_numbers: str = Field(..., description="One per line: number, name or just number")
    active_calls: int = Field(1, ge=1)
    additional: bool = False
    user_id: str = Field("", alias="tenant_id", description="Owner scope; role 3/4 override from JWT")
    retry_config: Optional[RetryConfig] = None


class ScheduleCampaignBody(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(..., min_length=1)
    phone_numbers: str = Field(...)
    active_calls: int = Field(1, ge=1)
    additional: bool = False
    schedule_date: str = Field(..., description="dd-mm-yyyy")
    schedule_time: str = Field(..., description="HH:MM or HH:MM AM/PM")
    user_id: str = Field("", alias="tenant_id", description="Owner scope; role 3/4 override from JWT")
    retry_config: Optional[RetryConfig] = None


# ── Endpoints ────────────────────────────────────────────────────────────────


@router.post("", status_code=201)
async def create_campaign(body: CreateCampaignBody, current_user: dict = Depends(get_current_user)):
    """
    Create campaign and start calling immediately.
    Role 3: scope = JWT user_id. Role 4: scope = default. Admin: body.user_id or own.
    """
    current_user_id = current_user.get("user_id")
    role = current_user.get("role", ROLE_USER)
    if role == ROLE_USER:
        scope = current_user_id
    elif role == ROLE_DEMO:
        scope = "default"
    else:
        scope = (body.user_id or "").strip() or current_user_id

    contacts = parse_phone_lines(body.phone_numbers)
    if not contacts:
        raise HTTPException(status_code=400, detail="Provide at least one valid phone number (one per line).")

    cap = await _effective_concurrency_cap(scope)
    if body.active_calls > cap:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Active calls (concurrency) cannot exceed your allowed concurrency ({cap}). "
                f"Set Active Calls to {cap} or less."
            ),
        )

    from utils.db import get_agent_for_scope_cached
    agent_doc = await get_agent_for_scope_cached(scope, redis_client)

    now = datetime.now(timezone.utc)
    campaign_doc = {
        "tenant_id": scope,
        "created_by": current_user_id,
        "name": body.name,
        "contacts": [{"number": c["number"], "name": c.get("name")} for c in contacts],
        "active_calls": body.active_calls,
        "additional": body.additional,
        "status": "running",
        "scheduled_at": None,
        "started_at": now,
        "ended_at": None,
        "created_at": now,
        "total_numbers": len(contacts),
        "completed_calls": 0,
        "failed_calls": 0,
        "voicemail_calls": 0,
        "retry_contact": 0,
        "retry_attempt": 0,
        "pending_retries": 0,
        "retry_config": body.retry_config.model_dump() if body.retry_config else None,
        "retry_round": 0,
        "retry_scheduled_at": None,
        "cached_greeting": (
            agent_doc.get("config", {}).get("outboundConfig", {}).get("greetingMessage")
            or agent_doc.get("cached_greeting")
        ) if agent_doc else None,
    }
    result = await db.campaigns.insert_one(campaign_doc)
    campaign_id = str(result.inserted_id)

    numbers = to_e164_list(contacts)
    from routes.outbound import start_campaign_with_id
    try:
        await start_campaign_with_id(
            scope,
            numbers,
            campaign_id,
            active_calls=body.active_calls,
        )
    except HTTPException as e:
        if e.status_code == 409:
            await db.campaigns.update_one(
                {"_id": result.inserted_id},
                {"$set": {"status": "paused"}},
            )
        else:
            await db.campaigns.delete_one({"_id": result.inserted_id})
        raise
    except Exception as e:
        await db.campaigns.delete_one({"_id": result.inserted_id})
        logger.exception(f"Campaign start_campaign_with_id failed: {e}")
        detail = str(e)
        if "401" in detail or "Unauthorized" in detail:
            raise HTTPException(status_code=502, detail="Exotel authentication failed. Check API key, token and subdomain in settings.")
        raise HTTPException(status_code=502, detail=detail)

    logger.info(f"Campaign created and started: id={campaign_id}, name={body.name}, numbers={len(numbers)}")

    try:
        await log_audit(
            action="create",
            resource_type="campaign",
            resource_id=campaign_id,
            performed_by=current_user_id or "",
            performed_by_role=role,
            details={"name": body.name, "total_numbers": len(contacts), "scope": scope, "additional": body.additional},
            target_user_id=scope,
        )
    except Exception:
        pass
    return {
        "id": campaign_id,
        "name": body.name,
        "status": "running",
        "total_numbers": len(contacts),
        "message": "Campaign created and first call started.",
    }


@router.post("/schedule", status_code=201)
async def schedule_campaign(body: ScheduleCampaignBody, current_user: dict = Depends(get_current_user)):
    """Schedule a campaign for later execution."""
    current_user_id = current_user.get("user_id")
    role = current_user.get("role", ROLE_USER)
    if role == ROLE_USER:
        scope = current_user_id
    elif role == ROLE_DEMO:
        scope = "default"
    else:
        scope = (body.user_id or "").strip() or current_user_id

    contacts = parse_phone_lines(body.phone_numbers)
    if not contacts:
        raise HTTPException(status_code=400, detail="Provide at least one valid phone number (one per line).")

    cap = await _effective_concurrency_cap(scope)
    if body.active_calls > cap:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Active calls (concurrency) cannot exceed your allowed concurrency ({cap}). "
                f"Set Active Calls to {cap} or less."
            ),
        )

    try:
        d, m, y = body.schedule_date.strip().split("-")
        scheduled_at = datetime(int(y), int(m), int(d))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid schedule_date. Use dd-mm-yyyy.")
    try:
        time_part = body.schedule_time.strip()
        if " " in time_part:
            hour_min, ampm = time_part.rsplit(" ", 1)
            h, mins = map(int, hour_min.split(":"))
            if ampm.upper().startswith("P") and h != 12:
                h += 12
            elif ampm.upper().startswith("A") and h == 12:
                h = 0
        else:
            h, mins = map(int, time_part.split(":"))
        scheduled_at = scheduled_at.replace(hour=h, minute=mins, second=0, microsecond=0, tzinfo=_IST)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid schedule_time. Use HH:MM or HH:MM AM/PM.")

    from utils.db import get_agent_for_scope_cached
    agent_doc = await get_agent_for_scope_cached(scope, redis_client)

    campaign_doc = {
        "tenant_id": scope,
        "created_by": current_user_id,
        "name": body.name,
        "contacts": [{"number": c["number"], "name": c.get("name")} for c in contacts],
        "active_calls": body.active_calls,
        "additional": body.additional,
        "status": "scheduled",
        "scheduled_at": scheduled_at,
        "started_at": None,
        "ended_at": None,
        "created_at": datetime.now(timezone.utc),
        "total_numbers": len(contacts),
        "completed_calls": 0,
        "failed_calls": 0,
        "voicemail_calls": 0,
        "retry_contact": 0,
        "retry_attempt": 0,
        "pending_retries": 0,
        "retry_config": body.retry_config.model_dump() if body.retry_config else None,
        "retry_round": 0,
        "retry_scheduled_at": None,
        "cached_greeting": (
            agent_doc.get("config", {}).get("outboundConfig", {}).get("greetingMessage")
            or agent_doc.get("cached_greeting")
        ) if agent_doc else None,
    }
    result = await db.campaigns.insert_one(campaign_doc)
    campaign_id = str(result.inserted_id)
    logger.info(f"Campaign scheduled: id={campaign_id}, name={body.name}, at={scheduled_at}")

    try:
        await log_audit(
            action="schedule",
            resource_type="campaign",
            resource_id=campaign_id,
            performed_by=current_user_id or "",
            performed_by_role=role,
            details={
                "name": body.name,
                "total_numbers": len(contacts),
                "scope": scope,
                "scheduled_at": scheduled_at.isoformat(),
                "additional": body.additional,
            },
            target_user_id=scope,
        )
    except Exception:
        pass
    return {
        "campaign_id": campaign_id,
        "id": campaign_id,
        "name": body.name,
        "status": "scheduled",
        "scheduled_at": scheduled_at.isoformat(),
        "total_numbers": len(contacts),
    }


@router.post("/{campaign_id}/pause")
async def pause_campaign(campaign_id: str, current_user: dict = Depends(get_current_user)):
    """Pause a running campaign: stop triggering next calls but keep queue intact."""
    from routes.outbound import _active_key

    try:
        oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid campaign ID.")
    doc = await db.campaigns.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Campaign not found.")
    if not _campaign_access_ok(doc, current_user):
        raise HTTPException(status_code=404, detail="Campaign not found.")
    status = (doc.get("status") or "").lower()
    if status != "running":
        raise HTTPException(status_code=400, detail="Only running campaigns can be paused.")

    scope = doc.get("tenant_id") or "default"
    active_key = _active_key(scope)
    await redis_client.delete(active_key)
    await db.campaigns.update_one({"_id": oid}, {"$set": {"status": "paused"}})
    logger.info(f"Campaign paused: id={campaign_id}, scope={scope}")

    try:
        await log_audit(
            action="pause",
            resource_type="campaign",
            resource_id=campaign_id,
            performed_by=current_user.get("user_id") or "",
            performed_by_role=current_user.get("role"),
            details={"scope": scope},
            target_user_id=scope,
        )
    except Exception:
        pass
    return {"status": "paused"}


@router.post("/{campaign_id}/resume")
async def resume_campaign(campaign_id: str, current_user: dict = Depends(get_current_user)):
    """Resume a paused campaign: re-enable triggering and immediately start the next queued call."""
    from routes.outbound import (
        _queue_key,
        _active_key,
        _campaign_id_key,
        _trigger_single_call,
        _parse_demo_queue_item,
        DEMO_INDUSTRY_REDIS_KEY,
        DEMO_GREETING_REDIS_KEY,
        DEMO_INDUSTRY_TTL_SECONDS,
        DEMO_GREETING_TTL_SECONDS,
        VALID_DEMO_INDUSTRIES,
    )

    try:
        oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid campaign ID.")
    doc = await db.campaigns.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Campaign not found.")
    if not _campaign_access_ok(doc, current_user):
        raise HTTPException(status_code=404, detail="Campaign not found.")
    status = (doc.get("status") or "").lower()
    if status != "paused":
        raise HTTPException(status_code=400, detail="Only paused campaigns can be resumed.")

    scope = doc.get("tenant_id") or "default"
    queue_key = _queue_key(scope)
    active_key = _active_key(scope)
    campaign_id_key = _campaign_id_key(scope)

    await redis_client.set(active_key, "1", ex=7200)
    await redis_client.set(campaign_id_key, campaign_id, ex=7200)

    next_raw = await redis_client.lpop(queue_key)
    next_num, next_industry, next_greeting = _parse_demo_queue_item(next_raw)
    if next_num:
        if scope == "default" and (next_industry or next_greeting):
            if next_industry and next_industry in VALID_DEMO_INDUSTRIES:
                await redis_client.set(DEMO_INDUSTRY_REDIS_KEY, next_industry, ex=DEMO_INDUSTRY_TTL_SECONDS)
            if next_greeting:
                await redis_client.set(DEMO_GREETING_REDIS_KEY, next_greeting, ex=DEMO_GREETING_TTL_SECONDS)
        await _trigger_single_call(scope, next_num, campaign_id)
        logger.info(f"Campaign resumed: triggered next call to {next_num} for scope={scope}")
        await db.campaigns.update_one({"_id": oid}, {"$set": {"status": "running"}})

        try:
            await log_audit(
                action="resume",
                resource_type="campaign",
                resource_id=campaign_id,
                performed_by=current_user.get("user_id") or "",
                performed_by_role=current_user.get("role"),
                details={"scope": scope, "next_number": next_num},
                target_user_id=scope,
            )
        except Exception:
            pass
        return {"status": "running", "next_number": next_num}

    # No queued numbers left — mark as completed (return "completed", not "cancelled")
    await redis_client.delete(active_key)
    await redis_client.delete(campaign_id_key)
    await db.campaigns.update_one(
        {"_id": oid},
        {"$set": {"status": "completed", "ended_at": datetime.now(timezone.utc)}},
    )
    logger.info(f"Campaign resume requested but queue empty: id={campaign_id}, marked completed")

    try:
        await log_audit(
            action="resume_empty",
            resource_type="campaign",
            resource_id=campaign_id,
            performed_by=current_user.get("user_id") or "",
            performed_by_role=current_user.get("role"),
            details={"scope": scope, "reason": "queue_empty"},
            target_user_id=scope,
        )
    except Exception:
        pass
    return {"status": "completed"}


@router.post("/{campaign_id}/cancel")
async def cancel_campaign(campaign_id: str, current_user: dict = Depends(get_current_user)):
    """Cancel a campaign: clear Redis queue/active flags and mark as completed."""
    import asyncio
    from routes.outbound import _queue_key, _active_key, _campaign_id_key, _inflight_key

    try:
        oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid campaign ID.")
    doc = await db.campaigns.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Campaign not found.")
    if not _campaign_access_ok(doc, current_user):
        raise HTTPException(status_code=404, detail="Campaign not found.")

    scope = doc.get("tenant_id") or "default"
    await redis_client.delete(_queue_key(scope))
    await redis_client.delete(_active_key(scope))
    await redis_client.delete(_campaign_id_key(scope))
    await redis_client.delete(_inflight_key(scope))
    await redis_client.delete(f"campaign_paused:{scope}")

    await db.campaigns.update_one(
        {"_id": oid},
        {"$set": {"status": "cancelled", "ended_at": datetime.now(timezone.utc)}},
    )
    logger.info(f"Campaign cancelled: id={campaign_id}, scope={scope}")

    try:
        from scheduled_campaigns_worker import try_process_scheduled_campaigns_for_scope
        asyncio.create_task(try_process_scheduled_campaigns_for_scope(scope))
    except Exception as e:
        logger.warning(f"Campaign cancel: could not trigger scheduled-campaigns check: {e}")

    try:
        await log_audit(
            action="cancel",
            resource_type="campaign",
            resource_id=campaign_id,
            performed_by=current_user.get("user_id") or "",
            performed_by_role=current_user.get("role"),
            details={"scope": scope},
            target_user_id=scope,
        )
    except Exception:
        pass
    return {"status": "cancelled"}


@router.get("")
async def list_campaigns(
    current_user: dict = Depends(get_current_user),
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None, description="Completed, Running, Paused, Scheduled or empty for all"),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
):
    """List campaigns scoped by role."""
    role = current_user.get("role", ROLE_USER)
    user_id = current_user.get("user_id")

    if role == ROLE_USER:
        tenant_filter = {"tenant_id": user_id}
    elif role == ROLE_DEMO:
        tenant_filter = {"tenant_id": "default"}
    elif role == ROLE_RESELLER and user_id:
        created_users = await db.users.find({"created_by": user_id}).to_list(length=None)
        tenant_ids = [str(u["_id"]) for u in created_users]
        tenant_ids.append(str(user_id))
        if not tenant_ids:
            tenant_filter = {"tenant_id": "__none__"}
        else:
            tenant_filter = {"tenant_id": {"$in": tenant_ids}}
    else:
        tenant_filter = {}

    per_page = min(per_page, CAMPAIGN_LIST_MAX_PER_PAGE)
    filt = {**tenant_filter}
    if search and search.strip():
        filt["name"] = {"$regex": re.escape(search.strip()), "$options": "i"}
    if status and status.strip() and status != "All Status":
        filt["status"] = status.strip().lower()

    active_count = 0
    active_filt = {**filt, "status": "running"}

    total = await db.campaigns.count_documents(filt)
    skip = (page - 1) * per_page
    cursor = (
        db.campaigns.find(filt)
        .sort("created_at", -1)
        .skip(skip)
        .limit(per_page)
    )
    docs = []
    async for doc in cursor:
        docs.append(doc)
    campaign_ids = [str(d.get("_id")) for d in docs if d.get("_id")]
    progress_map = await _campaign_progress_from_calls(campaign_ids)

    # Aggregate credits used per campaign
    credits_map: dict[str, int] = {}
    try:
        credits_agg = await db.calls.aggregate([
            {"$match": {"campaign_id": {"$in": campaign_ids}}},
            {"$group": {
                "_id": "$campaign_id",
                "total": {"$sum": {"$ifNull": ["$credits", {"$ifNull": ["$duration", 0]}]}}
            }},
        ]).to_list(length=len(campaign_ids) + 1)
        credits_map = {row["_id"]: int(row["total"]) for row in credits_agg if row.get("_id")}
    except Exception as e:
        logger.debug(f"Campaign credits aggregation failed: {e}")

    campaigns = []
    for doc in docs:
        oid = doc.get("_id")
        cid = str(oid) if oid else None
        total_numbers = doc.get("total_numbers", 0)
        prog = progress_map.get(
            cid,
            {"completed": 0, "failed": 0, "transferred": 0, "in_progress": 0, "voicemail": 0},
        )
        completed = prog["completed"]
        failed_calls_raw = prog["failed"]
        transferred_calls = prog.get("transferred", 0)
        in_progress = prog.get("in_progress", 0)
        current_status = (doc.get("status") or "running").lower()
        retry_contact = int(doc.get("retry_contact") or 0)

        if current_status in {"running", "retry_scheduled"} and retry_contact > 0:
            failed_calls = max(0, failed_calls_raw - retry_contact)
        else:
            failed_calls = failed_calls_raw
        processed = completed + failed_calls
        if current_status not in {"cancelled", "paused", "retry_scheduled"} and total_numbers > 0 and processed >= total_numbers:
            display_status = "Completed"
            if current_status == "running":
                await db.campaigns.update_one(
                    {"_id": oid},
                    {"$set": {"status": "completed", "ended_at": doc.get("ended_at") or datetime.now(timezone.utc)}},
                )
        else:
            retry_label = _retry_display_status(doc)
            raw_status = doc.get("status")
            if raw_status is None or (isinstance(raw_status, str) and not raw_status.strip()):
                display_status = "Draft" if processed < total_numbers else "Completed"
            else:
                display_status = retry_label if retry_label else raw_status.capitalize()

        started = doc.get("started_at") or doc.get("created_at")
        d = started or doc.get("created_at")
        date_str = _safe_date_str(d)
        campaigns.append({
            "id": cid,
            "name": doc.get("name", ""),
            "date": date_str,
            "total": total_numbers,
            "completed": completed,
            "failedCalls": failed_calls,
            "transferredCalls": transferred_calls,
            "inProgress": in_progress,
            "voicemailCalls": doc.get("voicemail_calls", 0),
            "concurrent": doc.get("active_calls", 1),
            "retryContact": doc.get("retry_contact", 0),
            "retryAttempt": doc.get("retry_attempt", 0),
            "status": display_status,
            "scheduledAt": _to_ist_str(doc.get("scheduled_at")),
            "deductedCredits": credits_map.get(cid, 0),
            "retryConfig": doc.get("retry_config"),
            "retryRound": doc.get("retry_round", 0),
        })
    try:
        active_count = await db.campaigns.count_documents(active_filt)
    except Exception as e:
        logger.debug("Campaign active_count failed: %s", e)
    return {"campaigns": campaigns, "total": total, "active_count": active_count}


@router.get("/{campaign_id}")
async def get_campaign(campaign_id: str, current_user: dict = Depends(get_current_user)):
    """Get one campaign for details modal + recent call logs."""
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid campaign ID.")
    doc = await db.campaigns.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Campaign not found.")
    if not _campaign_access_ok(doc, current_user):
        raise HTTPException(status_code=404, detail="Campaign not found.")

    progress_map = await _campaign_progress_from_calls([campaign_id])
    prog = progress_map.get(
        campaign_id,
        {"completed": 0, "failed": 0, "transferred": 0, "in_progress": 0, "voicemail": 0},
    )
    completed_calls = prog["completed"]
    failed_calls_raw = prog["failed"]
    transferred_calls = prog.get("transferred", 0)
    in_progress = prog.get("in_progress", 0)
    total_numbers = doc.get("total_numbers", 0)
    current_status = (doc.get("status") or "running").lower()
    retry_contact = int(doc.get("retry_contact") or 0)

    if current_status in {"running", "retry_scheduled"} and retry_contact > 0:
        failed_calls = max(0, failed_calls_raw - retry_contact)
    else:
        failed_calls = failed_calls_raw
    processed_calls = completed_calls + failed_calls

    if current_status not in {"cancelled", "paused", "retry_scheduled"} and total_numbers > 0 and processed_calls >= total_numbers:
        display_status = "Completed"
        if current_status == "running":
            await db.campaigns.update_one(
                {"_id": oid},
                {"$set": {"status": "completed", "ended_at": doc.get("ended_at") or datetime.now(timezone.utc)}},
            )
    else:
        retry_label = _retry_display_status(doc)
        display_status = retry_label if retry_label else (doc.get("status") or "running").capitalize()

    def _dt_fmt(d):
        if not d or not isinstance(d, datetime):
            return ""
        return d.strftime("%B %d, %Y at %I:%M %p")

    # Creator name lookup
    created_by_name = ""
    creator_id = doc.get("created_by") or doc.get("tenant_id") or ""
    if creator_id and creator_id != "default":
        try:
            creator_oid = ObjectId(creator_id)
            user_doc = await db.users.find_one({"_id": creator_oid}, projection={"name": 1, "email": 1})
            if user_doc:
                created_by_name = (user_doc.get("name") or "").strip() or (user_doc.get("email") or "").strip()
        except Exception:
            pass
    if not created_by_name and creator_id == "default":
        created_by_name = "Default"

    # Total credits deducted
    deducted_credits = 0
    try:
        agg = await db.calls.aggregate([
            {"$match": {"campaign_id": campaign_id}},
            {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$credits", {"$ifNull": ["$duration", 0]}]}}}},
        ]).to_list(length=1)
        if agg and agg[0].get("total") is not None:
            deducted_credits = int(agg[0]["total"])
    except Exception as e:
        logger.debug(f"Campaign deducted credits aggregation failed: {e}")

    # Exotel caller number
    exotel_number = ""
    try:
        first_call = await db.calls.find_one(
            {"campaign_id": campaign_id, "from_number": {"$exists": True, "$ne": ""}},
            projection={"from_number": 1},
            sort=[("created_at", 1)],
        )
        if first_call:
            exotel_number = first_call.get("from_number", "")
    except Exception as e:
        logger.debug(f"Campaign exotel number lookup failed: {e}")

    # Recent call logs
    recent = []
    cursor = db.calls.find({"campaign_id": campaign_id}).sort("created_at", -1).limit(20)
    async for c in cursor:
        dur = c.get("duration")
        if dur is not None and isinstance(dur, (int, float)):
            d_m = int(dur) // 60
            d_s = int(dur) % 60
            duration_str = f"{d_m}m {d_s}s"
        else:
            duration_str = "—"
        cr_at = c.get("created_at")
        date_str = cr_at.strftime("%m/%d/%Y %I:%M %p") if cr_at and isinstance(cr_at, datetime) else ""
        raw_status = (c.get("status") or "unknown").strip().lower()
        failure_reason = (c.get("failure_reason") or "").strip().lower()
        display_s = (
            "Voicemail"
            if (raw_status == "voicemail" or failure_reason == "voicemail")
            else (c.get("status", "unknown").title())
        )
        recent.append({
            "date": date_str,
            "numbers": c.get("to_number", ""),
            "status": display_s,
            "duration": duration_str,
        })

    return {
        "campaignId": campaign_id,
        "campaignName": doc.get("name", ""),
        "createdAt": _dt_fmt(doc.get("created_at")),
        "createdBy": created_by_name or "",
        "exotelNumber": exotel_number,
        "deductedCredits": deducted_credits,
        "startTime": _dt_fmt(doc.get("started_at")) or _dt_fmt(doc.get("created_at")),
        "endTime": _dt_fmt(doc.get("ended_at")),
        "status": display_status,
        "totalNumbers": total_numbers,
        "activeCalls": 0,
        "completedCalls": completed_calls,
        "failedCalls": failed_calls,
        "transferredCalls": transferred_calls,
        "inProgress": in_progress,
        "voicemailCalls": doc.get("voicemail_calls", 0),
        "retryContact": doc.get("retry_contact", 0),
        "retryAttempt": doc.get("retry_attempt", 0),
        "retryConfig": doc.get("retry_config"),
        "retryRound": doc.get("retry_round", 0),
        "recentLogs": recent,
    }


@router.get("/{campaign_id}/contacts")
async def get_campaign_contacts(
    campaign_id: str,
    current_user: dict = Depends(get_current_user),
    limit: int = Query(100, ge=1, le=5000),
    skip: int = Query(0, ge=0),
    format: Optional[str] = Query(None, description="csv for CSV download"),
):
    """Return the contacts array from the campaign document. Supports limit/skip. Use ?format=csv for CSV download."""
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid campaign ID.")
    doc = await db.campaigns.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Campaign not found.")
    if not _campaign_access_ok(doc, current_user):
        raise HTTPException(status_code=404, detail="Campaign not found.")
    contacts_full = doc.get("contacts") or []
    total = len(contacts_full)
    contacts_slice = contacts_full[skip : skip + limit]

    if (format or "").strip().lower() == "csv":
        def gen():
            yield "number,name\n"
            for c in contacts_slice:
                num = c.get("number", "")
                name = (c.get("name") or "").replace('"', '""')
                yield f'"{num}","{name}"\n'

        return StreamingResponse(
            gen(),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="campaign-{campaign_id}-contacts.csv"'},
        )

    return {
        "contacts": contacts_slice,
        "count": len(contacts_slice),
        "total": total,
    }
