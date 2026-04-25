"""
routes/users.py — User & RBAC management.

Prefix: /api/users

Endpoints:
  GET    /api/users                         — list users (scoped by caller role)
  POST   /api/users                         — create user (with credit/concurrency transfer)
  GET    /api/users/me                      — current user profile + credit balance
  GET    /api/users/credit-history          — paginated credit transactions (scoped by role)
  GET    /api/users/concurrency-history     — concurrency transaction history
  PUT    /api/users/{user_id}               — update user fields
  DELETE /api/users/{user_id}              — delete user (frees assigned phone numbers)
  POST   /api/users/{user_id}/credits       — assign/remove credits (wallet transfer)
  POST   /api/users/{user_id}/unlimited-credits — toggle unlimited credit flag
  POST   /api/users/{user_id}/concurrency   — assign concurrency from SA pool
  GET    /api/users/{user_id}/permissions   — get sidebar permissions
  PUT    /api/users/{user_id}/permissions   — update sidebar permissions

RBAC hierarchy (role integers):
  0 = Super Admin → manages 1, 2, 3, 4
  1 = Admin       → manages 2, 3, 4
  2 = Reseller    → manages 3 (only own-created users)
  3 = User
  4 = Demo

NOTE: GET /me and GET /credit-history MUST be declared before PUT /{user_id} and
DELETE /{user_id} to avoid FastAPI matching them as {user_id} path params.
"""
import re
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from pymongo import ReturnDocument

from models import ROLE_ADMIN, ROLE_DEMO, ROLE_RESELLER, ROLE_SUPER_ADMIN, ROLE_USER
from utils.db import db, require_db
from utils.password import hash_password
from utils.security import get_current_user

router = APIRouter(prefix="/api/users", tags=["Users"])

EMAIL_REGEX = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

# Role permission tables
ALLOWED_ROLES_SUPER_ADMIN = (ROLE_ADMIN, ROLE_RESELLER, ROLE_USER, ROLE_DEMO)
ALLOWED_ROLES_ADMIN       = (ROLE_RESELLER, ROLE_USER, ROLE_DEMO)
ALLOWED_ROLES_RESELLER    = (ROLE_USER,)
LIST_ROLES_SUPER_ADMIN    = (ROLE_ADMIN, ROLE_RESELLER, ROLE_USER, ROLE_DEMO)
LIST_ROLES_ADMIN          = (ROLE_RESELLER, ROLE_USER, ROLE_DEMO)
LIST_ROLES_RESELLER       = (ROLE_USER,)
VIEW_ROLES_ADMIN          = (ROLE_ADMIN, ROLE_RESELLER, ROLE_USER, ROLE_DEMO)


# ── Request models ────────────────────────────────────────────────────────────

class CreateUserBody(BaseModel):
    email: str
    password: str
    role: int
    initialCredits: Optional[int] = None
    initialConcurrency: Optional[int] = None
    name: Optional[str] = None
    company_name: Optional[str] = None
    plan: Optional[str] = None
    phone: Optional[str] = None
    status: Optional[bool] = True
    status_expires_at: Optional[str] = None
    is_diy: Optional[bool] = False


class UpdateUserBody(BaseModel):
    email: Optional[str] = None
    password: Optional[str] = None
    role: Optional[int] = None
    name: Optional[str] = None
    company_name: Optional[str] = None
    plan: Optional[str] = None
    phone: Optional[str] = None
    credits: Optional[int] = None
    concurrency: Optional[int] = None
    status: Optional[bool] = None
    status_expires_at: Optional[str] = None
    is_diy: Optional[bool] = None


class AssignCreditsBody(BaseModel):
    amount: int
    reason: str = "Admin"


class ToggleUnlimitedCreditsBody(BaseModel):
    enabled: bool
    reason: str = "Unlimited credits toggled"


class AssignConcurrencyBody(BaseModel):
    amount: int
    reason: str = "Concurrency assigned"


class UserPermissionsBody(BaseModel):
    permissions: dict


# ── Private helpers ───────────────────────────────────────────────────────────

def _is_valid_email(value: str) -> bool:
    return bool(value and EMAIL_REGEX.match(value.strip()))


def _allowed_roles(current_user: dict) -> tuple:
    role = current_user.get("role")
    if role == ROLE_SUPER_ADMIN:
        return ALLOWED_ROLES_SUPER_ADMIN
    if role == ROLE_ADMIN:
        return ALLOWED_ROLES_ADMIN
    if role == ROLE_RESELLER:
        return ALLOWED_ROLES_RESELLER
    return ()


def _list_roles(current_user: dict) -> tuple:
    role = current_user.get("role")
    if role == ROLE_SUPER_ADMIN:
        return LIST_ROLES_SUPER_ADMIN
    if role == ROLE_ADMIN:
        return LIST_ROLES_ADMIN
    if role == ROLE_RESELLER:
        return LIST_ROLES_RESELLER
    return ()


def _view_roles(current_user: dict) -> tuple:
    role = current_user.get("role")
    if role == ROLE_SUPER_ADMIN:
        return LIST_ROLES_SUPER_ADMIN
    if role == ROLE_ADMIN:
        return VIEW_ROLES_ADMIN
    if role == ROLE_RESELLER:
        return LIST_ROLES_RESELLER
    return ()


def _require_user_manager(current_user: dict) -> None:
    if current_user.get("role") not in (ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_RESELLER):
        raise HTTPException(status_code=403, detail="Admin, Super Admin, or Reseller access required")


def _safe_int(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


async def _atomic_increment(
    collection,
    user_oid: ObjectId,
    field: str,
    delta: int,
    *,
    minimum: Optional[int] = None,
    projection: Optional[dict] = None,
):
    """Atomically increment a numeric field. Returns the document BEFORE update, or None if minimum guard failed."""
    query: dict = {"_id": user_oid}
    if minimum is not None:
        query[field] = {"$gte": minimum}
    return await collection.find_one_and_update(
        query,
        {
            "$inc": {field: delta},
            "$set": {"updated_at": datetime.now(timezone.utc)},
        },
        projection=projection,
        return_document=ReturnDocument.BEFORE,
    )


# ── Routes ────────────────────────────────────────────────────────────────────
# IMPORTANT: static paths (/me, /credit-history, /concurrency-history) MUST be
# declared before parameterised paths (/{user_id}/...) to avoid mismatches.

@router.get("/me")
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """Return the current user's profile including credits and call stats."""
    require_db()
    try:
        oid = ObjectId(current_user["user_id"])
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user ID")

    doc = await db.users.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")

    role = int(doc.get("role", ROLE_USER))
    if role != ROLE_SUPER_ADMIN:
        if not doc.get("status", True):
            raise HTTPException(status_code=401, detail="Your account is inactive. Please contact support.")
        expiry_str = doc.get("status_expires_at")
        if expiry_str:
            try:
                expiry_dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
                if datetime.now(timezone.utc) > expiry_dt:
                    raise HTTPException(
                        status_code=401,
                        detail="Your plan has expired. Please renew your subscription.",
                    )
            except (ValueError, TypeError):
                pass

    total_call_duration = 0
    try:
        pipeline = [
            {"$match": {"tenant_id": str(oid)}},
            {"$group": {"_id": None, "total_duration": {"$sum": {"$ifNull": ["$duration", 0]}}}},
        ]
        stats = await db.calls.aggregate(pipeline).to_list(length=1)
        total_call_duration = stats[0]["total_duration"] if stats else 0
    except Exception:
        pass

    return {
        "id": str(doc["_id"]),
        "email": doc["email"],
        "role": doc["role"],
        "credits": doc.get("credits", 0),
        "concurrency": doc.get("concurrency", 0),
        "status": doc.get("status", True),
        "status_expires_at": doc.get("status_expires_at"),
        "total_call_duration": total_call_duration or 0,
        "created_at": doc.get("created_at"),
        "plan": doc.get("plan", "Basic"),
        "unlimited_credits": doc.get("unlimited_credits", False),
        "is_diy": doc.get("is_diy", False),
    }


@router.get("/credit-history")
async def get_credit_history(
    current_user: dict = Depends(get_current_user),
    limit: int = Query(25, ge=1, le=100),
    skip: int = Query(0, ge=0),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    action_filter: Optional[str] = Query(None, description="addition|deduction or empty for all"),
):
    """Paginated credit transaction history — scoped by caller role."""
    require_db()
    try:
        role = current_user.get("role")
        if role in (ROLE_SUPER_ADMIN, ROLE_ADMIN):
            query: dict = {}
        elif role == ROLE_RESELLER:
            reseller_id = current_user.get("user_id")
            created = await db.users.find({"created_by": reseller_id}).to_list(length=None)
            created_ids = [str(u["_id"]) for u in created]
            query = {"$or": [
                {"user_id": {"$in": created_ids}},
                {"user_id": reseller_id},
                {"performed_by": reseller_id},
            ]}
        else:
            query = {"user_id": current_user.get("user_id")}

        date_conds: dict = {}
        if from_date:
            try:
                date_conds["$gte"] = datetime.fromisoformat(from_date.strip()[:10] + "T00:00:00+00:00")
            except (ValueError, TypeError):
                pass
        if to_date:
            try:
                date_conds["$lte"] = datetime.fromisoformat(to_date.strip()[:10] + "T23:59:59.999999+00:00")
            except (ValueError, TypeError):
                pass
        if date_conds:
            query["created_at"] = date_conds

        if action_filter and action_filter.strip():
            a = action_filter.strip().lower()
            if a in ("addition", "assign"):
                query["action"] = "assign"
            elif a in ("deduction", "deduct"):
                query["action"] = {"$in": ["call_deduction", "deduct", "remove"]}
            else:
                query["action"] = a

        uid = current_user.get("user_id")
        current_balance = 0
        unlimited_credits = False
        if uid:
            try:
                u = await db.users.find_one({"_id": ObjectId(uid)}, projection={"credits": 1, "unlimited_credits": 1})
                if u:
                    current_balance = u.get("credits", 0)
                    unlimited_credits = u.get("unlimited_credits", False)
            except Exception:
                pass

        total = await db.credit_transactions.count_documents(query)
        cursor = db.credit_transactions.find(query).sort("created_at", -1).skip(skip).limit(limit)

        transactions = []
        async for doc in cursor:
            performed_by_email = doc.get("performed_by_email")
            if doc.get("performed_by") and not performed_by_email:
                try:
                    p = await db.users.find_one({"_id": ObjectId(doc["performed_by"])}, projection={"email": 1})
                    if p:
                        performed_by_email = p.get("email")
                except Exception:
                    pass

            action = doc["action"]
            raw = doc["amount"]
            signed = -abs(raw) if action in ("deduct", "call_deduction", "remove") else abs(raw)

            # Shape expected by CreditHistory.jsx
            transactions.append({
                "action": action,
                "amount": signed,
                "previous_credits": doc.get("previous_credits"),
                "new_credits": doc.get("new_credits"),
                "reason": doc.get("reason", ""),
                "performed_by": performed_by_email,
                "call_id": doc.get("call_id"),
                "campaign_id": doc.get("campaign_id"),
                "direction": doc.get("direction"),
                "duration": doc.get("duration"),
                "created_at": doc.get("created_at"),
                "user_email": doc.get("user_email"),
            })

        return {
            "transactions": transactions,
            "current_balance": current_balance,
            "total": total,
            "unlimited_credits": unlimited_credits,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Internal server error: {exc}")


@router.get("/concurrency-history")
async def get_concurrency_history(current_user: dict = Depends(get_current_user)):
    """Concurrency transaction history — scoped by caller role."""
    require_db()
    try:
        role = current_user.get("role")
        if role == ROLE_SUPER_ADMIN:
            query: dict = {}
        elif role == ROLE_ADMIN:
            manageable = list(_list_roles(current_user))
            mgmt_users = await db.users.find({"role": {"$in": manageable}}).to_list(length=None)
            mgmt_ids = [str(u["_id"]) for u in mgmt_users]
            query = {"$or": [
                {"user_id": {"$in": mgmt_ids}},
                {"performed_by": current_user.get("user_id")},
            ]}
        elif role == ROLE_RESELLER:
            created = await db.users.find({"created_by": current_user.get("user_id")}).to_list(length=None)
            created_ids = [str(u["_id"]) for u in created]
            query = {"$or": [
                {"user_id": {"$in": created_ids}},
                {"performed_by": current_user.get("user_id")},
            ]}
        else:
            query = {"user_id": current_user.get("user_id")}

        current_balance = 0
        uid = current_user.get("user_id")
        if uid:
            try:
                u = await db.users.find_one({"_id": ObjectId(uid)}, projection={"concurrency": 1})
                if u:
                    current_balance = u.get("concurrency", 0)
            except Exception:
                pass

        cursor = db.concurrency_transactions.find(query).sort("created_at", -1)
        transactions = []
        async for doc in cursor:
            performer_info = None
            if doc.get("performed_by"):
                try:
                    p = await db.users.find_one({"_id": ObjectId(doc["performed_by"])})
                    if p:
                        performer_info = {"email": p.get("email"), "role": p.get("role")}
                except Exception:
                    pass
            action = doc["action"]
            raw = doc["amount"]
            signed = -abs(raw) if action in ("deduct", "remove") else abs(raw)
            transactions.append({
                "id": str(doc["_id"]),
                "user_id": doc["user_id"],
                "user_email": doc.get("user_email"),
                "user_role": doc.get("user_role"),
                "action": action,
                "amount": signed,
                "previous_concurrency": doc.get("previous_concurrency"),
                "new_concurrency": doc.get("new_concurrency"),
                "reason": doc.get("reason", ""),
                "performed_by": performer_info,
                "created_at": doc.get("created_at"),
            })
        return {"transactions": transactions, "current_balance": current_balance}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Internal server error: {exc}")


@router.get("")
async def list_users(current_user: dict = Depends(get_current_user)):
    """List users visible to the caller (role-scoped)."""
    require_db()
    _require_user_manager(current_user)
    roles = _view_roles(current_user)
    if not roles:
        return {"users": []}

    query: dict = {"role": {"$in": list(roles)}}
    if current_user.get("role") == ROLE_RESELLER:
        query["created_by"] = current_user.get("user_id")

    cursor = db.users.find(query).sort("created_at", -1)
    users = []
    async for doc in cursor:
        uid_str = str(doc["_id"])
        user_data = {
            "id": uid_str,
            "email": doc["email"],
            "role": doc["role"],
            "credits": doc.get("credits", 0),
            "concurrency": doc.get("concurrency", 0),
            "created_at": doc.get("created_at"),
            "created_by": doc.get("created_by"),
            "name": doc.get("name"),
            "company_name": doc.get("company_name"),
            "plan": doc.get("plan"),
            "phone": doc.get("phone"),
            "status": doc.get("status", True),
            "status_expires_at": doc.get("status_expires_at"),
            "unlimited_credits": doc.get("unlimited_credits", False),
            "is_diy": doc.get("is_diy", False),
        }
        # Assigned phone lookup
        try:
            user_oid = ObjectId(uid_str)
            phone_q = {"$or": [{"assigned_to_user_id": uid_str}, {"assigned_to_user_id": user_oid}]}
        except Exception:
            phone_q = {"assigned_to_user_id": uid_str}
        phone_doc = await db.phones.find_one(phone_q)
        user_data["assignedPhone"] = (
            {"id": str(phone_doc["_id"]), "number": phone_doc.get("number", "")}
            if phone_doc else None
        )
        # Creator info
        created_by = doc.get("created_by")
        if created_by:
            try:
                creator = await db.users.find_one({"_id": ObjectId(created_by)})
                if creator:
                    user_data["created_by_id"] = str(creator["_id"])
                    user_data["created_by_email"] = creator.get("email")
                    user_data["created_by_role"] = creator.get("role")
            except Exception:
                pass
        users.append(user_data)
    return {"users": users}


@router.post("")
async def create_user(body: CreateUserBody, current_user: dict = Depends(get_current_user)):
    """Create a user. Credits and concurrency are transferred wallet-style from creator."""
    require_db()
    _require_user_manager(current_user)
    allowed = _allowed_roles(current_user)
    if body.role not in allowed:
        raise HTTPException(status_code=400, detail="Role not allowed for your account")

    email = body.email.strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email required")
    if not _is_valid_email(body.email):
        raise HTTPException(status_code=400, detail="Invalid email format")
    if await db.users.find_one({"email": email}):
        raise HTTPException(status_code=400, detail="Email already registered")

    password_hash = hash_password(body.password)
    now = datetime.now(timezone.utc)

    creator_candidate = current_user.get("impersonator") or current_user.get("user_id")
    creator_to_store = None
    if creator_candidate:
        try:
            if await db.users.find_one({"_id": ObjectId(creator_candidate)}):
                creator_to_store = creator_candidate
        except Exception:
            pass
    if not creator_to_store:
        try:
            sa = await db.users.find_one({"role": ROLE_SUPER_ADMIN})
            if sa:
                creator_to_store = str(sa["_id"])
        except Exception:
            pass

    initial_credits = (body.initialCredits or 0) if body.role in (ROLE_ADMIN, ROLE_RESELLER, ROLE_USER) else 0
    doc: dict = {
        "email": email,
        "password_hash": password_hash,
        "role": body.role,
        "credits": initial_credits,
        "created_at": now,
        "created_by": creator_to_store,
        "updated_at": now,
    }
    for attr, key in [("name", "name"), ("company_name", "company_name"), ("plan", "plan"), ("phone", "phone")]:
        val = getattr(body, attr)
        if val is not None:
            doc[key] = val.strip() or None
    if body.status is not None:
        doc["status"] = body.status
    if body.status_expires_at:
        doc["status_expires_at"] = body.status_expires_at
    if body.is_diy:
        doc["is_diy"] = True

    # Concurrency transfer from Super Admin pool
    initial_concurrency = max(0, body.initialConcurrency or 0)
    if initial_concurrency > 0 and current_user.get("role") == ROLE_SUPER_ADMIN:
        performer_id = current_user.get("impersonator") or current_user.get("user_id")
        try:
            sa_oid = ObjectId(performer_id)
            sa_doc = await _atomic_increment(
                db.users, sa_oid, "concurrency", -initial_concurrency,
                minimum=initial_concurrency, projection={"concurrency": 1, "email": 1, "role": 1},
            )
            if sa_doc is not None:
                doc["concurrency"] = initial_concurrency
                _rlabel = {ROLE_RESELLER: "reseller", ROLE_ADMIN: "admin", ROLE_USER: "user"}.get(body.role, "user")
                sa_cur = max(0, _safe_int(sa_doc.get("concurrency")))
                await db.concurrency_transactions.insert_one({
                    "user_id": performer_id, "user_email": sa_doc.get("email"),
                    "user_role": sa_doc.get("role"), "action": "deduct",
                    "amount": initial_concurrency,
                    "previous_concurrency": sa_cur, "new_concurrency": sa_cur - initial_concurrency,
                    "reason": f"Concurrency assigned to new {_rlabel} {email}",
                    "performed_by": performer_id, "performed_by_role": current_user.get("role"),
                    "created_at": now,
                })
            else:
                cur_sa = await db.users.find_one({"_id": sa_oid}, projection={"concurrency": 1})
                sa_cur = max(0, _safe_int((cur_sa or {}).get("concurrency")))
                raise HTTPException(
                    status_code=400,
                    detail=f"Insufficient concurrency. You have {sa_cur} but trying to assign {initial_concurrency}.",
                )
        except HTTPException:
            raise
        except Exception:
            doc["concurrency"] = 0
    else:
        doc["concurrency"] = initial_concurrency

    result = await db.users.insert_one(doc)
    new_id = str(result.inserted_id)
    _rlabel = {ROLE_RESELLER: "reseller", ROLE_ADMIN: "admin", ROLE_USER: "user"}.get(body.role, "user")

    # Log concurrency receipt for new user
    if initial_concurrency > 0 and current_user.get("role") == ROLE_SUPER_ADMIN:
        performer_id = current_user.get("impersonator") or current_user.get("user_id")
        await db.concurrency_transactions.insert_one({
            "user_id": new_id, "user_email": email, "user_role": body.role, "action": "assign",
            "amount": initial_concurrency, "previous_concurrency": 0, "new_concurrency": initial_concurrency,
            "reason": f"Initial concurrency assigned during {_rlabel} creation",
            "performed_by": performer_id, "performed_by_role": current_user.get("role"), "created_at": now,
        })

    # Credit transfer
    if initial_credits > 0:
        performer_id = current_user.get("impersonator") or current_user.get("user_id")
        performer_role = current_user.get("role")
        deduct_reason = f"Credits assigned to new {_rlabel} {email}"
        assign_reason = f"Initial credits assigned during {_rlabel} creation"

        if performer_role == ROLE_RESELLER:
            reseller_oid = ObjectId(current_user.get("user_id"))
            r_doc = await _atomic_increment(
                db.users, reseller_oid, "credits", -initial_credits,
                minimum=initial_credits, projection={"credits": 1, "email": 1, "role": 1},
            )
            if not r_doc:
                cur_r = await db.users.find_one({"_id": reseller_oid}, projection={"credits": 1})
                await db.users.delete_one({"_id": result.inserted_id})
                raise HTTPException(
                    status_code=400,
                    detail=f"Insufficient credits. You have {_safe_int((cur_r or {}).get('credits'))} credits.",
                )
            r_cur = _safe_int(r_doc.get("credits"))
            await db.credit_transactions.insert_one({
                "user_id": current_user.get("user_id"), "user_email": r_doc["email"],
                "user_role": r_doc["role"], "action": "deduct", "amount": initial_credits,
                "previous_credits": r_cur, "new_credits": r_cur - initial_credits,
                "reason": deduct_reason, "performed_by": performer_id,
                "performed_by_role": performer_role, "created_at": now,
            })

        elif performer_role in (ROLE_SUPER_ADMIN, ROLE_ADMIN):
            try:
                c_oid = ObjectId(performer_id)
                c_doc = await _atomic_increment(
                    db.users, c_oid, "credits", -initial_credits,
                    projection={"credits": 1, "email": 1, "role": 1},
                )
                if c_doc is not None:
                    c_cur = _safe_int(c_doc.get("credits"))
                    await db.credit_transactions.insert_one({
                        "user_id": performer_id, "user_email": c_doc.get("email"),
                        "user_role": c_doc.get("role"), "action": "deduct", "amount": initial_credits,
                        "previous_credits": c_cur, "new_credits": c_cur - initial_credits,
                        "reason": deduct_reason, "performed_by": performer_id,
                        "performed_by_role": performer_role, "created_at": now,
                    })
            except Exception:
                pass

        await db.credit_transactions.insert_one({
            "user_id": new_id, "user_email": email, "user_role": body.role,
            "action": "assign", "amount": initial_credits,
            "previous_credits": 0, "new_credits": initial_credits,
            "reason": assign_reason, "performed_by": performer_id,
            "performed_by_role": performer_role, "created_at": now,
        })

    return {"id": new_id, "email": email, "role": body.role, "created_at": now, "created_by": creator_to_store}


@router.put("/{user_id}")
async def update_user(
    user_id: str,
    body: UpdateUserBody,
    current_user: dict = Depends(get_current_user),
):
    """Update user fields. Only roles within caller's allowed scope."""
    require_db()
    _require_user_manager(current_user)
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="User not found")
    doc = await db.users.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")
    if doc["role"] not in _list_roles(current_user):
        raise HTTPException(status_code=403, detail="Cannot update this user")

    updates: dict = {"updated_at": datetime.now(timezone.utc)}
    if body.email is not None:
        email = body.email.strip().lower()
        if email:
            if not _is_valid_email(body.email):
                raise HTTPException(status_code=400, detail="Invalid email format")
            if await db.users.find_one({"email": email, "_id": {"$ne": oid}}):
                raise HTTPException(status_code=400, detail="Email already in use")
            updates["email"] = email
    if body.password is not None and body.password.strip():
        updates["password_hash"] = hash_password(body.password)
    if body.role is not None and body.role in _allowed_roles(current_user):
        updates["role"] = body.role
    for attr, key in [("name", "name"), ("company_name", "company_name"), ("plan", "plan"), ("phone", "phone")]:
        val = getattr(body, attr)
        if val is not None:
            updates[key] = val.strip() or None
    if body.credits is not None:
        updates["credits"] = body.credits
    if body.concurrency is not None:
        updates["concurrency"] = max(0, body.concurrency)
    if body.status is not None:
        updates["status"] = body.status
    if body.status_expires_at is not None:
        updates["status_expires_at"] = body.status_expires_at or None
    if body.is_diy is not None:
        updates["is_diy"] = body.is_diy

    await db.users.update_one({"_id": oid}, {"$set": updates})
    updated = await db.users.find_one({"_id": oid})
    return {
        "id": user_id,
        "email": updated["email"],
        "role": updated["role"],
        "created_at": updated.get("created_at"),
        "created_by": updated.get("created_by"),
    }


@router.delete("/{user_id}")
async def delete_user(user_id: str, current_user: dict = Depends(get_current_user)):
    """Delete user and free any assigned phone numbers."""
    require_db()
    _require_user_manager(current_user)
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="User not found")
    doc = await db.users.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")
    if doc["role"] not in _list_roles(current_user):
        raise HTTPException(status_code=403, detail="Cannot delete this user")

    if current_user.get("role") == ROLE_RESELLER:
        created_by = str(doc.get("created_by") or "").strip()
        current_id = str(current_user.get("user_id") or "").strip()
        if not created_by or created_by != current_id:
            raise HTTPException(
                status_code=403,
                detail=f"Can only delete users you created. You are {current_id}, user was created by {created_by}",
            )

    await db.phones.update_many(
        {"$or": [{"assigned_to_user_id": user_id}, {"assigned_to_user_id": oid}]},
        {"$set": {"assigned_to_user_id": None, "assignment_status": "available", "updated_at": datetime.now(timezone.utc)}},
    )
    await db.users.delete_one({"_id": oid})
    return {"message": "User deleted"}


@router.post("/{user_id}/credits")
async def assign_credits(
    user_id: str,
    body: AssignCreditsBody,
    current_user: dict = Depends(get_current_user),
):
    """
    Assign/remove credits. Positive amount deducts from assigner's wallet (SA/Admin/Reseller).
    Super Admin assigning to their own account is a free top-up (no deduction).
    """
    require_db()
    _require_user_manager(current_user)
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="User not found")
    doc = await db.users.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")

    is_self_super_admin = (
        current_user.get("role") == ROLE_SUPER_ADMIN
        and str(doc["_id"]) == str(current_user.get("user_id"))
        and doc.get("role") == ROLE_SUPER_ADMIN
    )
    if not is_self_super_admin and doc["role"] not in _list_roles(current_user):
        raise HTTPException(status_code=403, detail="Cannot assign credits to this user")

    assigner_deduction = 0
    assigner_doc = None
    assigner_old = assigner_new = 0
    assigner_oid = None
    assigner_role = current_user.get("role")

    if body.amount > 0 and not is_self_super_admin and assigner_role in (ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_RESELLER):
        assigner_oid = ObjectId(current_user.get("user_id"))
        assigner_doc = await _atomic_increment(
            db.users, assigner_oid, "credits", -body.amount,
            minimum=body.amount, projection={"credits": 1, "email": 1, "role": 1},
        )
        if not assigner_doc:
            lookup = await db.users.find_one({"_id": assigner_oid}, projection={"credits": 1})
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient credits. You have {_safe_int((lookup or {}).get('credits'))} but trying to assign {body.amount}.",
            )
        assigner_old = _safe_int(assigner_doc.get("credits"))
        assigner_new = assigner_old - body.amount
        assigner_deduction = body.amount

    recipient_doc = await _atomic_increment(
        db.users, oid, "credits", body.amount,
        projection={"credits": 1, "email": 1, "role": 1},
    )
    if not recipient_doc:
        if assigner_deduction > 0 and assigner_oid:
            await _atomic_increment(db.users, assigner_oid, "credits", assigner_deduction)
        raise HTTPException(status_code=404, detail="User not found")

    r_old = _safe_int(recipient_doc.get("credits"))
    r_new = r_old + body.amount
    now = datetime.now(timezone.utc)
    assigner_email = (assigner_doc or {}).get("email") or current_user.get("email")

    await db.credit_transactions.insert_one({
        "user_id": user_id, "user_email": recipient_doc.get("email") or doc["email"],
        "user_role": recipient_doc.get("role") if recipient_doc.get("role") is not None else doc["role"],
        "action": "assign" if body.amount > 0 else "remove",
        "amount": abs(body.amount), "previous_credits": r_old, "new_credits": r_new,
        "reason": body.reason, "performed_by": current_user.get("user_id"),
        "performed_by_role": assigner_role, "performed_by_email": assigner_email, "created_at": now,
    })
    if assigner_deduction > 0:
        await db.credit_transactions.insert_one({
            "user_id": current_user.get("user_id"), "user_email": assigner_doc["email"],
            "user_role": assigner_doc["role"], "action": "deduct",
            "amount": assigner_deduction, "previous_credits": assigner_old, "new_credits": assigner_new,
            "reason": f"Credits assigned to {recipient_doc.get('email') or doc['email']}",
            "performed_by": current_user.get("user_id"), "performed_by_role": assigner_role,
            "performed_by_email": assigner_email, "created_at": now,
        })

    return {
        "user_id": user_id, "previous_credits": r_old, "new_credits": r_new,
        "assigned_amount": body.amount, "assigner_deduction": assigner_deduction, "reason": body.reason,
    }


@router.post("/{user_id}/unlimited-credits")
async def toggle_unlimited_credits(
    user_id: str,
    body: ToggleUnlimitedCreditsBody,
    current_user: dict = Depends(get_current_user),
):
    """Toggle unlimited credit flag. Balance is preserved; deductions are skipped when enabled."""
    require_db()
    _require_user_manager(current_user)
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="User not found")
    doc = await db.users.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")

    is_self_super_admin = (
        current_user.get("role") == ROLE_SUPER_ADMIN
        and str(doc["_id"]) == str(current_user.get("user_id"))
        and doc.get("role") == ROLE_SUPER_ADMIN
    )
    if not is_self_super_admin and doc["role"] not in _list_roles(current_user):
        raise HTTPException(status_code=403, detail="Cannot modify this user")

    previous = doc.get("unlimited_credits", False)
    await db.users.update_one(
        {"_id": oid},
        {"$set": {"unlimited_credits": body.enabled, "updated_at": datetime.now(timezone.utc)}},
    )
    action = "unlimited_enabled" if body.enabled else "unlimited_disabled"
    await db.credit_transactions.insert_one({
        "user_id": user_id, "user_email": doc["email"], "user_role": doc["role"],
        "action": action, "amount": 0,
        "previous_credits": doc.get("credits", 0), "new_credits": doc.get("credits", 0),
        "reason": body.reason, "performed_by": current_user.get("user_id"),
        "performed_by_role": current_user.get("role"),
        "performed_by_email": current_user.get("email"), "created_at": datetime.now(timezone.utc),
    })
    return {"user_id": user_id, "unlimited_credits": body.enabled, "previous_state": previous, "reason": body.reason}


@router.post("/{user_id}/concurrency")
async def assign_concurrency(
    user_id: str,
    body: AssignConcurrencyBody,
    current_user: dict = Depends(get_current_user),
):
    """Assign concurrency to a user. Deducted from caller's (SA/Admin/Reseller) pool."""
    require_db()
    if current_user.get("role") not in (ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_RESELLER):
        raise HTTPException(status_code=403, detail="Only Super Admin, Admin, or Reseller can assign concurrency")
    if body.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be positive")
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="User not found")
    doc = await db.users.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")

    performer_id = current_user.get("impersonator") or current_user.get("user_id")
    is_self_assign = str(doc["_id"]) == str(performer_id)
    if is_self_assign and current_user.get("role") != ROLE_SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Super Admin can assign concurrency to themselves")
    is_self_sa = is_self_assign and doc.get("role") == ROLE_SUPER_ADMIN

    if not is_self_sa and doc["role"] not in _list_roles(current_user):
        raise HTTPException(status_code=403, detail="Cannot assign concurrency to this user")

    now = datetime.now(timezone.utc)

    if is_self_sa:
        recipient = await _atomic_increment(
            db.users, oid, "concurrency", body.amount,
            projection={"concurrency": 1, "email": 1, "role": 1},
        )
        if not recipient:
            raise HTTPException(status_code=404, detail="User not found")
        r_cur = max(0, _safe_int(recipient.get("concurrency")))
        r_new = r_cur + body.amount
        await db.concurrency_transactions.insert_one({
            "user_id": user_id, "user_email": recipient.get("email") or doc.get("email"),
            "user_role": recipient.get("role") if recipient.get("role") is not None else doc.get("role"),
            "action": "assign", "amount": body.amount,
            "previous_concurrency": r_cur, "new_concurrency": r_new,
            "reason": body.reason or "Concurrency assigned to self",
            "performed_by": performer_id, "performed_by_role": current_user.get("role"), "created_at": now,
        })
        return {
            "user_id": user_id, "previous_concurrency": r_cur, "new_concurrency": r_new,
            "assigned_amount": body.amount, "super_admin_new_concurrency": r_new,
        }

    sa_oid = ObjectId(performer_id)
    sa_doc = await _atomic_increment(
        db.users, sa_oid, "concurrency", -body.amount,
        minimum=body.amount, projection={"concurrency": 1, "email": 1, "role": 1},
    )
    if not sa_doc:
        lookup = await db.users.find_one({"_id": sa_oid}, projection={"concurrency": 1})
        raise HTTPException(
            status_code=400,
            detail=f"Insufficient concurrency. You have {max(0, _safe_int((lookup or {}).get('concurrency')))} but trying to assign {body.amount}.",
        )
    sa_cur = max(0, _safe_int(sa_doc.get("concurrency")))
    sa_new = sa_cur - body.amount

    recipient = await _atomic_increment(
        db.users, oid, "concurrency", body.amount,
        projection={"concurrency": 1, "email": 1, "role": 1},
    )
    if not recipient:
        await _atomic_increment(db.users, sa_oid, "concurrency", body.amount)
        raise HTTPException(status_code=404, detail="User not found")

    r_cur = max(0, _safe_int(recipient.get("concurrency")))
    r_new = r_cur + body.amount

    await db.concurrency_transactions.insert_one({
        "user_id": performer_id, "user_email": sa_doc.get("email"),
        "user_role": sa_doc.get("role"), "action": "deduct", "amount": body.amount,
        "previous_concurrency": sa_cur, "new_concurrency": sa_new,
        "reason": body.reason or f"Concurrency assigned to {doc.get('email')}",
        "performed_by": performer_id, "performed_by_role": current_user.get("role"), "created_at": now,
    })
    await db.concurrency_transactions.insert_one({
        "user_id": user_id, "user_email": recipient.get("email") or doc.get("email"),
        "user_role": recipient.get("role") if recipient.get("role") is not None else doc.get("role"),
        "action": "assign", "amount": body.amount,
        "previous_concurrency": r_cur, "new_concurrency": r_new,
        "reason": body.reason or "Concurrency assigned",
        "performed_by": performer_id, "performed_by_role": current_user.get("role"), "created_at": now,
    })

    return {
        "user_id": user_id, "previous_concurrency": r_cur, "new_concurrency": r_new,
        "assigned_amount": body.amount, "super_admin_new_concurrency": sa_new,
    }


@router.get("/{user_id}/permissions")
async def get_user_permissions(user_id: str, current_user: dict = Depends(get_current_user)):
    """Get sidebar/navigation permissions for a user."""
    require_db()
    _require_user_manager(current_user)
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="User not found")
    doc = await db.users.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")
    return {"permissions": doc.get("permissions") or {}}


@router.put("/{user_id}/permissions")
async def update_user_permissions(
    user_id: str,
    body: UserPermissionsBody,
    current_user: dict = Depends(get_current_user),
):
    """Update which navigation items this user can see."""
    require_db()
    _require_user_manager(current_user)
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="User not found")
    doc = await db.users.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="User not found")
    if doc["role"] not in _list_roles(current_user):
        raise HTTPException(status_code=403, detail="Cannot update this user")
    await db.users.update_one(
        {"_id": oid},
        {"$set": {"permissions": body.permissions, "updated_at": datetime.now(timezone.utc)}},
    )
    return {"permissions": body.permissions}
