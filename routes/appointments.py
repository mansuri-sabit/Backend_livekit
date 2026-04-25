"""
Appointments API: list booked appointments for the logged-in user.
Appointments are stored with userId (tenant/scope from the call); filtered by current user's id.
"""

from bson import ObjectId
from fastapi import APIRouter, Depends

from utils.db import db
from utils.security import get_current_user
from utils.logger import logger

router = APIRouter(prefix="/api/appointments", tags=["Appointments"])


@router.get("")
async def list_appointments(current_user: dict = Depends(get_current_user)):
    """
    List appointments for the logged-in user.
    Supports both string and ObjectId userId in DB.
    """
    user_id = (current_user.get("user_id") or "").strip()
    logger.info(f"[Appointments] list_appointments: user_id={user_id!r}")
    if not user_id:
        logger.warning("[Appointments] No user_id in token, returning empty list")
        return {"success": True, "data": {"appointments": []}}

    try:
        oid = ObjectId(user_id)
        query = {"$or": [{"userId": user_id}, {"userId": oid}]}
    except Exception:
        query = {"userId": user_id}

    cursor = db.appointments.find(query).sort("createdAt", -1)
    items = []
    async for doc in cursor:
        apt_date = doc.get("appointmentDate")
        apt_time = doc.get("appointmentTime") or ""
        if apt_date:
            date_str = apt_date.strftime("%Y-%m-%d") if hasattr(apt_date, "strftime") else str(apt_date)[:10]
        else:
            date_str = ""
        date_display = f"{date_str} {apt_time}".strip() if date_str or apt_time else ""
        items.append({
            "id": str(doc["_id"]),
            "name": doc.get("customerName") or "",
            "phone": doc.get("customerPhone") or "",
            "date": date_display,
            "status": doc.get("status") or "scheduled",
            "reason": doc.get("reason") or "",
            "createdAt": doc.get("createdAt"),
        })
    logger.info(f"[Appointments] Found {len(items)} appointments for user_id={user_id!r}")
    return {"success": True, "data": {"appointments": items}}
