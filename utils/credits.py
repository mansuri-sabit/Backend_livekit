"""
Credit deduction for call usage.
Admin (1), Reseller (2), and User (3) are charged 1 credit per second of call duration.
Demo (scope 'default') and Super Admin (0) are not charged.
"""
from datetime import datetime, timezone

from bson import ObjectId
from loguru import logger

from models import ROLE_ADMIN, ROLE_RESELLER, ROLE_USER
from utils.db import db


async def deduct_credits_for_call(
    owner_scope: str, duration_seconds: int, call_id: str = ""
) -> None:
    """
    Deduct 1 credit/second from owner. Applies to Admin/Reseller/User only.
    Balance never goes below 0. Unlimited-credit users are tracked but not charged.
    """
    if db is None or not owner_scope or duration_seconds <= 0:
        return
    if owner_scope.strip() == "default":
        return  # Demo user — no deduction

    try:
        oid = ObjectId(owner_scope)
    except Exception:
        logger.debug(f"[Credits] Invalid owner_scope: {owner_scope!r}")
        return

    user_doc = await db.users.find_one({"_id": oid})
    if not user_doc:
        return
    if user_doc.get("role") not in (ROLE_ADMIN, ROLE_RESELLER, ROLE_USER):
        return

    current = max(0, int(user_doc.get("credits") or 0))
    is_unlimited = user_doc.get("unlimited_credits", False)

    if is_unlimited:
        reason = f"Call usage (1 credit/sec), duration {duration_seconds}s [unlimited]"
        if call_id:
            reason += f", call_id={call_id}"
        await db.credit_transactions.insert_one({
            "user_id": owner_scope,
            "action": "call_deduction",
            "amount": -duration_seconds,
            "previous_credits": current,
            "new_credits": current,
            "reason": reason,
            "created_at": datetime.now(timezone.utc),
            "performed_by": "system",
            "call_id": call_id or None,
        })
        logger.info(
            f"[Credits] Unlimited user {owner_scope}: tracked {duration_seconds}s (balance unchanged at {current})"
        )
        return

    to_deduct = min(duration_seconds, current)
    if to_deduct <= 0:
        logger.info(f"[Credits] User {owner_scope} has 0 credits; skipping deduction")
        return

    new_credits = current - to_deduct
    await db.users.update_one(
        {"_id": oid},
        {"$set": {"credits": new_credits, "updated_at": datetime.now(timezone.utc)}},
    )
    reason = f"Call usage (1 credit/sec), duration {duration_seconds}s"
    if call_id:
        reason += f", call_id={call_id}"
    await db.credit_transactions.insert_one({
        "user_id": owner_scope,
        "action": "deduct",
        "amount": -to_deduct,
        "previous_credits": current,
        "new_credits": new_credits,
        "reason": reason,
        "created_at": datetime.now(timezone.utc),
        "performed_by": "system",
        "call_id": call_id or None,
    })
    logger.info(
        f"[Credits] Deducted {to_deduct} credits from {owner_scope} "
        f"(duration={duration_seconds}s, balance {current} → {new_credits})"
    )
