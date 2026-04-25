"""Audit log viewer with role-based access filtering."""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from models import ROLE_ADMIN, ROLE_RESELLER, ROLE_SUPER_ADMIN, ROLE_USER
from utils.db import db
from utils.security import get_current_user

router = APIRouter(prefix="/api/audit-logs", tags=["Audit Logs"])


def _get_audit_query_filter(current_user: dict) -> dict:
    """Build MongoDB query filter based on user's role."""
    user_role = current_user.get("role")
    user_id = current_user.get("user_id")

    if user_role == ROLE_SUPER_ADMIN:
        return {}

    if user_role == ROLE_ADMIN:
        manageable_roles = [ROLE_RESELLER, ROLE_USER, 4]
        return {
            "$or": [
                {"performed_by": user_id},
                {
                    "target_user_id": {"$exists": True},
                    "$or": [
                        {"details.role": {"$in": manageable_roles}},
                        {"performed_by_role": {"$in": manageable_roles}},
                    ],
                },
            ]
        }

    if user_role == ROLE_RESELLER:
        return {
            "$or": [
                {"performed_by": user_id},
                {"target_user_id": {"$exists": True}, "details.created_by": user_id},
            ]
        }

    return {"performed_by": user_id}


@router.get("")
async def get_audit_logs(
    current_user: dict = Depends(get_current_user),
    action: Optional[str] = Query(None, description="Filter by action (create, update, delete, login, etc.)"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type (user, agent, campaign, etc.)"),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    from_date: Optional[str] = Query(None, description="From date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="To date (YYYY-MM-DD)"),
):
    """
    Get audit logs with role-based filtering.

    Super Admin: all logs | Admin: manageable roles + own actions
    Reseller: own + actions on created users | User/Demo: own actions only
    """
    query_filter = _get_audit_query_filter(current_user)

    if action:
        query_filter["action"] = action
    if resource_type:
        query_filter["resource_type"] = resource_type

    if from_date or to_date:
        date_filter: dict = {}
        if from_date:
            try:
                from_dt = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                date_filter["$gte"] = from_dt
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid from_date format. Use YYYY-MM-DD")
        if to_date:
            try:
                to_dt = datetime.strptime(to_date, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc
                )
                date_filter["$lte"] = to_dt
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid to_date format. Use YYYY-MM-DD")
        if date_filter:
            query_filter["created_at"] = date_filter

    try:
        cursor = db.audit_logs.find(query_filter).sort("created_at", -1).skip(skip).limit(limit)
        logs = []
        async for log in cursor:
            log["_id"] = str(log["_id"])
            logs.append(log)
        total_count = await db.audit_logs.count_documents(query_filter)
        return {"logs": logs, "total": total_count, "limit": limit, "skip": skip}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving audit logs: {str(e)}")
