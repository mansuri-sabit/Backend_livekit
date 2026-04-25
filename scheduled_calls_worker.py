"""
Background scheduler for scheduled callbacks.

Polls scheduled_calls collection for due items and triggers outbound call
via routes.outbound._trigger_single_call.
Runs as a FastAPI lifespan/background task.
"""

import asyncio
from datetime import datetime, timezone

from utils.logger import logger

CHECK_INTERVAL_SECONDS = 30


async def run_scheduled_calls_worker():
    """
    Loop: every CHECK_INTERVAL_SECONDS, find pending scheduled_calls where
    scheduledFor <= now, trigger outbound call for each, mark as completed or failed.
    """
    while True:
        try:
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            await _process_due_scheduled_calls()
        except asyncio.CancelledError:
            logger.info("[ScheduledCalls] Worker cancelled")
            break
        except Exception as e:
            logger.exception("[ScheduledCalls] Worker error: %s", e)


async def _process_due_scheduled_calls():
    from bson import ObjectId
    from utils.db import db

    now = datetime.now(timezone.utc)
    cursor = db.scheduled_calls.find(
        {"status": "pending", "scheduledFor": {"$lte": now}}
    ).limit(50)
    docs = await cursor.to_list(length=50)
    if not docs:
        return

    for doc in docs:
        sid = str(doc["_id"])
        try:
            tenant_id = doc.get("user_id") or ""
            phone_number = doc.get("phoneNumber") or ""
            if not tenant_id or not phone_number:
                reason = "Missing user_id" if not tenant_id else "Missing phoneNumber"
                await db.scheduled_calls.update_one(
                    {"_id": ObjectId(sid)},
                    {
                        "$set": {
                            "status": "failed",
                            "metadata.failureReason": reason,
                            "updatedAt": datetime.now(timezone.utc),
                        }
                    },
                )
                logger.warning(f"[ScheduledCalls] Skipped {sid}: {reason}")
                continue

            await db.scheduled_calls.update_one(
                {"_id": ObjectId(sid)},
                {"$set": {"status": "processing", "updatedAt": datetime.now(timezone.utc)}},
            )

            from routes.outbound import _trigger_single_call
            call_sid, status = await _trigger_single_call(tenant_id, phone_number, campaign_id=None)
            await db.scheduled_calls.update_one(
                {"_id": ObjectId(sid)},
                {
                    "$set": {
                        "status": "completed",
                        "metadata.callSid": call_sid,
                        "metadata.triggeredStatus": status,
                        "updatedAt": datetime.now(timezone.utc),
                    }
                },
            )
            logger.info(
                "[ScheduledCalls] Triggered callback: scheduled_call_id=%s call_sid=%s to=%s",
                sid, call_sid, phone_number[:8] + "...",
            )
        except Exception as e:
            logger.exception("[ScheduledCalls] Failed to trigger %s: %s", sid, e)
            try:
                await db.scheduled_calls.update_one(
                    {"_id": ObjectId(sid)},
                    {
                        "$set": {
                            "status": "failed",
                            "metadata.failureReason": str(e)[:500],
                            "updatedAt": datetime.now(timezone.utc),
                        }
                    },
                )
            except Exception:
                pass
