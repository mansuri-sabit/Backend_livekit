"""Audit log writer — never raises, never blocks the main operation."""
from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from utils.db import db


async def log_audit(
    action: str,
    resource_type: str,
    resource_id: str,
    performed_by: str,
    performed_by_role: int,
    details: Optional[dict] = None,
    target_user_id: Optional[str] = None,
) -> None:
    """
    Write an audit event to the audit_logs collection.
    Parameters:
        action          — "create", "update", "delete", "login", "assign_credits", etc.
        resource_type   — "user", "agent", "campaign", "call", "credit", etc.
        resource_id     — ID of the affected resource.
        performed_by    — user_id of the actor.
        performed_by_role — role int of the actor.
        details         — optional dict with old_value, new_value, reason, etc.
        target_user_id  — set when the action affects a different user.
    """
    if db is None:
        return
    try:
        doc: dict = {
            "action": action,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "performed_by": performed_by,
            "performed_by_role": performed_by_role,
            "created_at": datetime.now(timezone.utc),
        }
        if details:
            doc["details"] = details
        if target_user_id:
            doc["target_user_id"] = target_user_id
        await db.audit_logs.insert_one(doc)
    except Exception as exc:
        logger.error(
            f"[Audit] Failed to write audit log: action={action} resource_type={resource_type} error={exc}"
        )
