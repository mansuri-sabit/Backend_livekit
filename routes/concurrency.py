"""
Concurrency slot management.

Hierarchy:
  Super Admin (0) → Admin (1), Reseller (2), User (3), Demo (4)
  Admin (1)       → Reseller (2), User (3), Demo (4)
  Reseller (2)    → User (3)

Super admin is seeded with 100 000 concurrency slots.
Assigning slots deducts from the giver and adds to the receiver.
Revoking slots removes from the user and returns to whoever assigned them
(tracked via `concurrency_assigned_by` on the user document).
"""

from datetime import datetime, timezone

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from models import ROLE_ADMIN, ROLE_DEMO, ROLE_RESELLER, ROLE_SUPER_ADMIN, ROLE_USER
from utils.db import db
from utils.security import get_current_user

router = APIRouter(prefix="/api/concurrency", tags=["Concurrency"])

_MANAGE_ROLES = {
    ROLE_SUPER_ADMIN: (ROLE_ADMIN, ROLE_RESELLER, ROLE_USER, ROLE_DEMO),
    ROLE_ADMIN:       (ROLE_RESELLER, ROLE_USER, ROLE_DEMO),
    ROLE_RESELLER:    (ROLE_USER,),
}


def _require_manager(current_user: dict) -> None:
    if current_user.get("role") not in (ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_RESELLER):
        raise HTTPException(status_code=403, detail="Admin, Super Admin, or Reseller access required")


def _can_manage(current_user: dict, target_role: int) -> bool:
    allowed = _MANAGE_ROLES.get(current_user.get("role"), ())
    return target_role in allowed


class AssignConcurrencyBody(BaseModel):
    amount: int   # positive = assign slots, negative = revoke slots
    reason: str = "Admin"


@router.post("/users/{user_id}")
async def assign_concurrency(
    user_id: str,
    body: AssignConcurrencyBody,
    current_user: dict = Depends(get_current_user),
):
    """
    Assign (amount > 0) or revoke (amount < 0) concurrency slots for a user.

    Assign:  deducts from current_user balance → adds to target.
    Revoke:  removes from target → returns to whoever originally assigned them
             (stored in target's `concurrency_assigned_by` field).
    """
    _require_manager(current_user)

    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=404, detail="User not found")

    target_doc = await db.users.find_one({"_id": oid})
    if not target_doc:
        raise HTTPException(status_code=404, detail="User not found")
    if not _can_manage(current_user, target_doc["role"]):
        raise HTTPException(status_code=403, detail="Cannot manage concurrency for this user")

    now = datetime.now(timezone.utc)
    target_current = target_doc.get("concurrency_limit", 0)

    # ── ASSIGN ────────────────────────────────────────────────────────────────
    if body.amount > 0:
        assigner_id = current_user.get("user_id")
        assigner_oid = ObjectId(assigner_id)
        assigner_doc = await db.users.find_one({"_id": assigner_oid})
        if not assigner_doc:
            raise HTTPException(status_code=400, detail="Assigner not found")

        assigner_current = assigner_doc.get("concurrency_limit", 0)
        if assigner_current < body.amount:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Insufficient concurrency balance. "
                    f"You have {assigner_current} slots but trying to assign {body.amount}."
                ),
            )

        assigner_new = assigner_current - body.amount
        target_new = target_current + body.amount

        await db.users.update_one(
            {"_id": assigner_oid},
            {"$set": {"concurrency_limit": assigner_new, "updated_at": now}},
        )
        await db.users.update_one(
            {"_id": oid},
            {"$set": {
                "concurrency_limit": target_new,
                "concurrency_assigned_by": assigner_id,
                "updated_at": now,
            }},
        )

        await db.concurrency_transactions.insert_one({
            "user_id": assigner_id,
            "user_email": assigner_doc["email"],
            "user_role": assigner_doc["role"],
            "action": "deduct",
            "amount": body.amount,
            "previous_concurrency": assigner_current,
            "new_concurrency": assigner_new,
            "reason": f"Assigned to {target_doc['email']}: {body.reason}",
            "performed_by": assigner_id,
            "performed_by_role": assigner_doc["role"],
            "target_user_id": user_id,
            "target_user_email": target_doc["email"],
            "created_at": now,
        })
        await db.concurrency_transactions.insert_one({
            "user_id": user_id,
            "user_email": target_doc["email"],
            "user_role": target_doc["role"],
            "action": "assign",
            "amount": body.amount,
            "previous_concurrency": target_current,
            "new_concurrency": target_new,
            "reason": body.reason,
            "performed_by": assigner_id,
            "performed_by_role": assigner_doc["role"],
            "target_user_id": None,
            "target_user_email": None,
            "created_at": now,
        })

        return {
            "user_id": user_id,
            "previous_concurrency": target_current,
            "new_concurrency": target_new,
            "assigned_amount": body.amount,
            "assigner_new_concurrency": assigner_new,
        }

    # ── REVOKE ────────────────────────────────────────────────────────────────
    else:
        revoke_amount = abs(body.amount)
        if target_current < revoke_amount:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot revoke {revoke_amount} slots. User only has {target_current}.",
            )

        target_new = target_current - revoke_amount
        await db.users.update_one(
            {"_id": oid},
            {"$set": {"concurrency_limit": target_new, "updated_at": now}},
        )

        await db.concurrency_transactions.insert_one({
            "user_id": user_id,
            "user_email": target_doc["email"],
            "user_role": target_doc["role"],
            "action": "revoke",
            "amount": revoke_amount,
            "previous_concurrency": target_current,
            "new_concurrency": target_new,
            "reason": body.reason,
            "performed_by": current_user.get("user_id"),
            "performed_by_role": current_user.get("role"),
            "target_user_id": None,
            "target_user_email": None,
            "created_at": now,
        })

        # Return slots to whoever assigned them
        assigner_id = target_doc.get("concurrency_assigned_by")
        if assigner_id:
            try:
                assigner_oid = ObjectId(assigner_id)
                assigner_doc = await db.users.find_one({"_id": assigner_oid})
                if assigner_doc:
                    assigner_current = assigner_doc.get("concurrency_limit", 0)
                    assigner_new = assigner_current + revoke_amount
                    await db.users.update_one(
                        {"_id": assigner_oid},
                        {"$set": {"concurrency_limit": assigner_new, "updated_at": now}},
                    )
                    await db.concurrency_transactions.insert_one({
                        "user_id": assigner_id,
                        "user_email": assigner_doc["email"],
                        "user_role": assigner_doc["role"],
                        "action": "return",
                        "amount": revoke_amount,
                        "previous_concurrency": assigner_current,
                        "new_concurrency": assigner_new,
                        "reason": f"Returned from {target_doc['email']}: {body.reason}",
                        "performed_by": current_user.get("user_id"),
                        "performed_by_role": current_user.get("role"),
                        "target_user_id": user_id,
                        "target_user_email": target_doc["email"],
                        "created_at": now,
                    })
            except Exception as e:
                logger_warn = f"Warning: failed to return slots to assigner {assigner_id}: {e}"
                try:
                    from utils.logger import logger
                    logger.warning(logger_warn)
                except Exception:
                    print(logger_warn)

        return {
            "user_id": user_id,
            "previous_concurrency": target_current,
            "new_concurrency": target_new,
            "revoked_amount": revoke_amount,
            "returned_to": assigner_id,
        }


@router.get("/history")
async def get_concurrency_history(current_user: dict = Depends(get_current_user)):
    """
    Concurrency transaction history — role-filtered same as credit history.

    Super Admin : all transactions
    Admin       : transactions for users they manage + their own performed actions
    Reseller    : transactions for users they created + their own
    User/Demo   : own transactions only
    """
    try:
        role = current_user.get("role")
        uid = current_user.get("user_id")

        if role == ROLE_SUPER_ADMIN:
            query: dict = {}

        elif role == ROLE_ADMIN:
            mgmt_roles = [ROLE_RESELLER, ROLE_USER, ROLE_DEMO]
            mgmt_users = await db.users.find({"role": {"$in": mgmt_roles}}).to_list(length=None)
            mgmt_ids = [str(u["_id"]) for u in mgmt_users]
            query = {
                "$or": [
                    {"user_id": {"$in": mgmt_ids}},
                    {"performed_by": uid},
                    {"user_id": uid},
                ]
            }

        elif role == ROLE_RESELLER:
            created_users = await db.users.find({"created_by": uid}).to_list(length=None)
            created_ids = [str(u["_id"]) for u in created_users]
            query = {
                "$or": [
                    {"user_id": {"$in": created_ids}},
                    {"performed_by": uid},
                    {"user_id": uid},
                ]
            }

        else:
            query = {"user_id": uid}

        current_concurrency = 0
        try:
            if uid:
                u = await db.users.find_one({"_id": ObjectId(uid)}, projection={"concurrency_limit": 1})
                if u:
                    current_concurrency = u.get("concurrency_limit", 0)
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
            raw_amount = doc["amount"]
            signed_amount = -abs(raw_amount) if action in ("deduct", "revoke") else abs(raw_amount)

            transactions.append({
                "id": str(doc["_id"]),
                "user_id": doc["user_id"],
                "user_email": doc["user_email"],
                "user_role": doc["user_role"],
                "action": action,
                "amount": signed_amount,
                "previous_concurrency": doc["previous_concurrency"],
                "new_concurrency": doc["new_concurrency"],
                "reason": doc.get("reason", ""),
                "performed_by": performer_info,
                "target_user_id": doc.get("target_user_id"),
                "target_user_email": doc.get("target_user_email"),
                "created_at": doc["created_at"],
            })

        return {"transactions": transactions, "current_concurrency": current_concurrency}

    except Exception as e:
        import traceback
        from utils.logger import logger
        logger.error(f"Error in get_concurrency_history: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/summary")
async def get_concurrency_summary(current_user: dict = Depends(get_current_user)):
    """Pool summary for the current user."""
    _require_manager(current_user)

    uid_str = current_user.get("user_id")
    uid = ObjectId(uid_str)
    my_doc = await db.users.find_one({"_id": uid}, projection={"concurrency_limit": 1})
    my_balance = my_doc.get("concurrency_limit", 0) if my_doc else 0

    if current_user.get("role") == ROLE_SUPER_ADMIN:
        pipeline = [
            {"$match": {"role": {"$in": [ROLE_ADMIN, ROLE_RESELLER, ROLE_USER, ROLE_DEMO]}}},
            {"$group": {"_id": None, "total_distributed": {"$sum": "$concurrency_limit"}}},
        ]
        result = await db.users.aggregate(pipeline).to_list(length=1)
        total_distributed = result[0]["total_distributed"] if result else 0
        total_pool = my_balance + total_distributed
        return {
            "my_balance": my_balance,
            "total_pool": total_pool,
            "total_distributed": total_distributed,
            "available": my_balance,
        }
    else:
        role = current_user.get("role")
        if role == ROLE_ADMIN:
            children_query = {"created_by": uid_str, "role": {"$in": [ROLE_RESELLER, ROLE_USER, ROLE_DEMO]}}
        else:
            children_query = {"created_by": uid_str, "role": ROLE_USER}

        pipeline = [
            {"$match": children_query},
            {"$group": {"_id": None, "total_given": {"$sum": "$concurrency_limit"}}},
        ]
        result = await db.users.aggregate(pipeline).to_list(length=1)
        total_given = result[0]["total_given"] if result else 0
        return {
            "my_balance": my_balance,
            "total_given": total_given,
            "available": my_balance,
        }
