"""Phone number management — CRUD, agent/user assignment, outbound capacity."""

from datetime import datetime, timezone
from typing import List, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from models import ROLE_ADMIN, ROLE_SUPER_ADMIN, ROLE_RESELLER, ROLE_USER, ROLE_DEMO
from utils.db import db
from utils.security import get_current_user
from utils.logger import logger

router = APIRouter(prefix="/api/phones", tags=["Phones"])


class ExotelConfig(BaseModel):
    apiKey: str
    apiToken: str
    sid: str
    subdomain: str
    appId: Optional[str] = None


class ImportPhoneBody(BaseModel):
    number: str
    country: str = "IN"
    exotelConfig: Optional[ExotelConfig] = None
    tags: Optional[List[str]] = None


class UpdatePhoneBody(BaseModel):
    tags: Optional[List[str]] = None
    isActive: Optional[bool] = None
    inboundConcurrentLimit: Optional[int] = None
    outboundConcurrentLimit: Optional[int] = None


class AssignAgentBody(BaseModel):
    agentId: str


class AssignUserBody(BaseModel):
    """Accept either userId (camelCase) or user_id (snake_case) for compatibility."""
    userId: Optional[str] = None
    user_id: Optional[str] = None


def _require_admin(current_user: dict) -> None:
    if current_user.get("role") not in (ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_RESELLER):
        raise HTTPException(status_code=403, detail="Admin access required")


def _phone_to_dict(doc: dict) -> dict:
    """Convert a MongoDB phone document to a JSON-serialisable dict."""
    return {
        "id": str(doc["_id"]),
        "number": doc.get("number", ""),
        "country": doc.get("country", "IN"),
        "provider": doc.get("provider", "exotel"),
        "status": doc.get("status", "active"),
        "assignmentStatus": doc.get("assignment_status", "available"),
        "tags": doc.get("tags", []),
        "agentId": str(doc["agent_id"]) if doc.get("agent_id") else None,
        "assignedToUserId": str(doc["assigned_to_user_id"]) if doc.get("assigned_to_user_id") else None,
        "inboundConcurrentLimit": doc.get("inbound_concurrent_limit", 2),
        "outboundConcurrentLimit": doc.get("outbound_concurrent_limit", 2),
        "hasExotelConfig": bool(doc.get("exotel_data")),
        "createdAt": doc.get("created_at"),
        "updatedAt": doc.get("updated_at"),
    }


@router.post("")
async def import_phone(body: ImportPhoneBody, current_user: dict = Depends(get_current_user)):
    """Import a new phone number (admin only)."""
    _require_admin(current_user)
    number = body.number.strip()
    if not number:
        raise HTTPException(status_code=400, detail="Phone number is required")

    if await db.phones.find_one({"number": number}):
        raise HTTPException(status_code=409, detail="Phone number already exists in the system")

    now = datetime.now(timezone.utc)
    doc: dict = {
        "number": number,
        "country": body.country.upper(),
        "provider": "exotel",
        "status": "active",
        "assignment_status": "available",
        "tags": body.tags or [],
        "agent_id": None,
        "assigned_to_user_id": None,
        "inbound_concurrent_limit": 2,
        "outbound_concurrent_limit": 2,
        "created_by": current_user["user_id"],
        "created_at": now,
        "updated_at": now,
    }
    if body.exotelConfig:
        doc["exotel_data"] = {
            "api_key": body.exotelConfig.apiKey,
            "api_token": body.exotelConfig.apiToken,
            "sid": body.exotelConfig.sid,
            "subdomain": body.exotelConfig.subdomain,
            "app_id": body.exotelConfig.appId,
        }

    result = await db.phones.insert_one(doc)
    doc["_id"] = result.inserted_id
    logger.info(f"Phone imported: {number} by user {current_user['user_id']}")
    return {"success": True, "data": {"phone": _phone_to_dict(doc)}, "message": "Phone number imported successfully"}


@router.get("")
async def list_phones(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    search: Optional[str] = None,
    isActive: Optional[str] = None,
    hasAgent: Optional[str] = None,
    agentId: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
):
    """List phones. Super admins see all; resellers see their own + sub-users'; regular users see their own."""
    role = current_user.get("role")
    uid = current_user["user_id"]
    is_super_admin = role in (ROLE_SUPER_ADMIN, ROLE_ADMIN)
    query: dict = {}

    if is_super_admin:
        pass
    elif role == ROLE_RESELLER:
        sub_users = await db.users.find({"created_by": uid}, {"_id": 1}).to_list(length=None)
        sub_user_ids = [str(u["_id"]) for u in sub_users]
        query["$or"] = [
            {"created_by": uid},
            {"assigned_to_user_id": {"$in": sub_user_ids}},
        ]
    else:
        query["$or"] = [{"assigned_to_user_id": uid}, {"user_id": uid}]

    if search:
        query["number"] = {"$regex": search, "$options": "i"}
    if isActive == "true":
        query["status"] = "active"
    elif isActive == "false":
        query["status"] = "inactive"
    if agentId:
        query["agent_id"] = ObjectId(agentId)
    elif hasAgent == "true":
        query["agent_id"] = {"$ne": None}
    elif hasAgent == "false":
        query["agent_id"] = None

    skip = (page - 1) * limit
    total = await db.phones.count_documents(query)
    cursor = db.phones.find(query).sort("created_at", -1).skip(skip).limit(limit)
    phones = []
    async for doc in cursor:
        assigned_uid = doc.get("assigned_to_user_id")
        if assigned_uid is not None:
            try:
                user_oid = assigned_uid if isinstance(assigned_uid, ObjectId) else ObjectId(assigned_uid)
                if not await db.users.find_one({"_id": user_oid}):
                    await db.phones.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"assigned_to_user_id": None, "assignment_status": "available",
                                  "updated_at": datetime.now(timezone.utc)}},
                    )
                    doc = {**doc, "assigned_to_user_id": None, "assignment_status": "available"}
            except Exception:
                pass
        phones.append(_phone_to_dict(doc))

    return {
        "success": True,
        "data": {
            "phones": phones,
            "total": total,
            "page": page,
            "totalPages": max(1, -(-total // limit)),
        },
    }


@router.get("/outbound-capacity")
async def get_outbound_capacity(current_user: dict = Depends(get_current_user)):
    """Max concurrent outbound calls allowed for the current user."""
    role = current_user.get("role", ROLE_USER)
    uid = current_user.get("user_id")
    if role in (ROLE_SUPER_ADMIN, ROLE_ADMIN):
        return {"maxConcurrent": 100}
    query: dict = {"status": "active"}
    if role == ROLE_USER:
        query["$or"] = [{"assigned_to_user_id": uid}, {"user_id": uid}]
    elif role == ROLE_DEMO:
        query["$or"] = [{"assigned_to_user_id": "default"}, {"user_id": "default"}]
    cursor = db.phones.find(query, {"outbound_concurrent_limit": 1})
    total = 0
    async for doc in cursor:
        total += max(0, int(doc.get("outbound_concurrent_limit") or 0))
    return {"maxConcurrent": max(1, total)}


@router.get("/{phone_id}")
async def get_phone(phone_id: str, current_user: dict = Depends(get_current_user)):
    """Get a single phone by ID."""
    try:
        oid = ObjectId(phone_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Phone not found")

    doc = await db.phones.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Phone not found")

    role = current_user.get("role")
    uid = current_user["user_id"]
    if role not in (ROLE_SUPER_ADMIN, ROLE_ADMIN):
        if role == ROLE_RESELLER:
            sub_users = await db.users.find({"created_by": uid}, {"_id": 1}).to_list(length=None)
            sub_user_ids = [str(u["_id"]) for u in sub_users]
            if doc.get("created_by") != uid and str(doc.get("assigned_to_user_id") or "") not in sub_user_ids:
                raise HTTPException(status_code=403, detail="Not authorised to access this phone")
        else:
            owner = str(doc.get("assigned_to_user_id") or doc.get("user_id") or "")
            if owner and owner != uid:
                raise HTTPException(status_code=403, detail="Not authorised to access this phone")

    return {"success": True, "data": {"phone": _phone_to_dict(doc)}}


@router.put("/{phone_id}")
async def update_phone(phone_id: str, body: UpdatePhoneBody, current_user: dict = Depends(get_current_user)):
    """Update phone tags / status / limits (admin only)."""
    _require_admin(current_user)
    try:
        oid = ObjectId(phone_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Phone not found")

    if not await db.phones.find_one({"_id": oid}):
        raise HTTPException(status_code=404, detail="Phone not found")

    updates: dict = {"updated_at": datetime.now(timezone.utc)}
    if body.tags is not None:
        if len(body.tags) > 10:
            raise HTTPException(status_code=400, detail="Maximum 10 tags allowed")
        updates["tags"] = body.tags
    if body.isActive is not None:
        updates["status"] = "active" if body.isActive else "inactive"
    if body.inboundConcurrentLimit is not None:
        if body.inboundConcurrentLimit < 1:
            raise HTTPException(status_code=400, detail="Limit must be >= 1")
        updates["inbound_concurrent_limit"] = body.inboundConcurrentLimit
    if body.outboundConcurrentLimit is not None:
        if body.outboundConcurrentLimit < 1:
            raise HTTPException(status_code=400, detail="Limit must be >= 1")
        updates["outbound_concurrent_limit"] = body.outboundConcurrentLimit

    await db.phones.update_one({"_id": oid}, {"$set": updates})
    updated = await db.phones.find_one({"_id": oid})
    return {"success": True, "data": {"phone": _phone_to_dict(updated)}, "message": "Phone updated successfully"}


@router.put("/{phone_id}/assign")
async def assign_agent(phone_id: str, body: AssignAgentBody, current_user: dict = Depends(get_current_user)):
    """Assign an AI agent to a phone (admin only)."""
    _require_admin(current_user)
    try:
        phone_oid = ObjectId(phone_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Phone not found")

    if not await db.phones.find_one({"_id": phone_oid}):
        raise HTTPException(status_code=404, detail="Phone not found")

    try:
        agent_oid = ObjectId(body.agentId)
    except Exception:
        raise HTTPException(status_code=404, detail="Agent not found")

    if not await db.agents.find_one({"_id": agent_oid}):
        raise HTTPException(status_code=404, detail="Agent not found")

    await db.phones.update_one(
        {"_id": phone_oid},
        {"$set": {"agent_id": agent_oid, "updated_at": datetime.now(timezone.utc)}},
    )
    updated = await db.phones.find_one({"_id": phone_oid})
    logger.info(f"Agent {body.agentId} assigned to phone {phone_id}")
    return {"success": True, "data": {"phone": _phone_to_dict(updated)}, "message": "Agent assigned successfully"}


@router.delete("/{phone_id}/assign")
async def unassign_agent(phone_id: str, current_user: dict = Depends(get_current_user)):
    """Unassign agent from phone (admin only)."""
    _require_admin(current_user)
    try:
        oid = ObjectId(phone_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Phone not found")

    if not await db.phones.find_one({"_id": oid}):
        raise HTTPException(status_code=404, detail="Phone not found")

    await db.phones.update_one(
        {"_id": oid},
        {"$set": {"agent_id": None, "updated_at": datetime.now(timezone.utc)}},
    )
    updated = await db.phones.find_one({"_id": oid})
    return {"success": True, "data": {"phone": _phone_to_dict(updated)}, "message": "Agent unassigned successfully"}


@router.put("/{phone_id}/assign-user")
async def assign_phone_to_user(phone_id: str, body: AssignUserBody, current_user: dict = Depends(get_current_user)):
    """Assign phone to a user (admin only)."""
    _require_admin(current_user)
    target_user_id = (body.userId or body.user_id or "").strip()
    if not target_user_id:
        raise HTTPException(status_code=400, detail="userId is required")

    try:
        phone_oid = ObjectId(phone_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Phone not found")

    phone = await db.phones.find_one({"_id": phone_oid})
    if not phone:
        raise HTTPException(status_code=404, detail="Phone not found")

    current_owner = phone.get("assigned_to_user_id")
    if current_owner and str(current_owner) != target_user_id:
        raise HTTPException(status_code=409, detail="Phone is already assigned to another user")

    try:
        user_oid = ObjectId(target_user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user id")

    user = await db.users.find_one({"_id": user_oid})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    now = datetime.now(timezone.utc)
    prev_owner = str(phone.get("assigned_to_user_id") or "")
    if prev_owner and prev_owner != target_user_id:
        try:
            await db.users.update_one(
                {"_id": ObjectId(prev_owner)},
                {"$unset": {"assigned_phone_id": ""}, "$set": {"updated_at": now}},
            )
        except Exception:
            pass

    phone_update: dict = {
        "assigned_to_user_id": target_user_id,
        "assignment_status": "assigned",
        "updated_at": now,
    }
    if user.get("is_diy"):
        linked_agent = await db.agents.find_one({"user_id": target_user_id})
        if linked_agent:
            phone_update["agent_id"] = linked_agent["_id"]
    await db.phones.update_one({"_id": phone_oid}, {"$set": phone_update})
    await db.users.update_one(
        {"_id": user_oid},
        {"$set": {"assigned_phone_id": phone_id, "updated_at": now}},
    )

    updated = await db.phones.find_one({"_id": phone_oid})
    return {"success": True, "data": {"phone": _phone_to_dict(updated)}, "message": "Phone assigned to user successfully"}


@router.delete("/{phone_id}/assign-user")
async def unassign_phone_from_user(phone_id: str, current_user: dict = Depends(get_current_user)):
    """Unassign phone from user (admin only)."""
    _require_admin(current_user)
    try:
        oid = ObjectId(phone_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Phone not found")

    phone = await db.phones.find_one({"_id": oid})
    if not phone:
        raise HTTPException(status_code=404, detail="Phone not found")

    now = datetime.now(timezone.utc)
    assigned_user_id = str(phone.get("assigned_to_user_id") or "")

    await db.phones.update_one(
        {"_id": oid},
        {"$unset": {"assigned_to_user_id": "", "agent_id": ""}, "$set": {"assignment_status": "available", "updated_at": now}},
    )
    if assigned_user_id:
        try:
            await db.users.update_one(
                {"_id": ObjectId(assigned_user_id)},
                {"$unset": {"assigned_phone_id": ""}, "$set": {"updated_at": now}},
            )
        except Exception:
            pass

    updated = await db.phones.find_one({"_id": oid})
    return {"success": True, "data": {"phone": _phone_to_dict(updated)}, "message": "Phone unassigned from user successfully"}


@router.delete("/{phone_id}")
async def delete_phone(phone_id: str, current_user: dict = Depends(get_current_user)):
    """Delete a phone number (admin only)."""
    _require_admin(current_user)
    try:
        oid = ObjectId(phone_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Phone not found")

    if not await db.phones.find_one({"_id": oid}):
        raise HTTPException(status_code=404, detail="Phone not found")

    await db.phones.delete_one({"_id": oid})
    logger.info(f"Phone {phone_id} deleted by user {current_user['user_id']}")
    return {"success": True, "message": "Phone deleted successfully"}
