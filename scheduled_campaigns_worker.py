"""
Background worker for scheduled campaigns (bulk campaigns with status=scheduled).

FCFS: when a campaign's scheduled_at time is reached, start it only if no campaign
is currently running for that scope (tenant_id). If one is running, hold until it
completes or is cancelled — then start the next due scheduled campaign.
Runs as a FastAPI lifespan/background task.
"""

import asyncio
from datetime import datetime, timezone
from bson import ObjectId

from utils.db import db, redis_client
from utils.logger import logger
from routes.campaigns import to_e164_list
from routes.outbound import _active_key, start_campaign_with_id


CHECK_INTERVAL_SECONDS = 30


def _now_for_scheduled_check():
    """Return current UTC time for comparison against scheduled_at."""
    return datetime.now(timezone.utc)


async def _scope_has_running(scope: str, exclude_campaign_id: str | None = None) -> bool:
    """True if this scope has any campaign with status running."""
    q = {"tenant_id": scope, "status": "running"}
    if exclude_campaign_id:
        try:
            q["_id"] = {"$ne": ObjectId(exclude_campaign_id)}
        except Exception:
            pass
    n = await db.campaigns.count_documents(q)
    return n > 0


async def try_process_scheduled_campaigns_for_scope(scope: str) -> None:
    """
    Process due scheduled campaigns for a single scope. Called when a running campaign
    ends so the next scheduled one starts within seconds (without waiting for the 30s poll).
    """
    try:
        now = _now_for_scheduled_check()
        cursor = db.campaigns.find(
            {"status": "scheduled", "tenant_id": scope, "scheduled_at": {"$lte": now}}
        ).sort("scheduled_at", 1).limit(1)
        doc = await cursor.to_list(length=1)
        if not doc:
            logger.info(
                "[ScheduledCampaigns] try_process scope=%s: no due scheduled campaign",
                scope[:8] + "...",
            )
            return
        doc = doc[0]
    except Exception as e:
        logger.warning("[ScheduledCampaigns] try_process scope=%s error: %s", scope[:8] + "...", e)
        return

    active_key = _active_key(scope)
    if await redis_client.get(active_key):
        logger.info(
            "[ScheduledCampaigns] try_process scope=%s: slot still active, skipping",
            scope[:8] + "...",
        )
        return
    campaign_id = str(doc["_id"])
    if await _scope_has_running(scope, exclude_campaign_id=campaign_id):
        logger.info(
            "[ScheduledCampaigns] try_process scope=%s: another campaign still running in DB, skipping",
            scope[:8] + "...",
        )
        return
    contacts = doc.get("contacts") or []
    numbers = to_e164_list(contacts)
    if not numbers:
        await db.campaigns.update_one(
            {"_id": ObjectId(campaign_id)},
            {
                "$set": {
                    "status": "cancelled",
                    "ended_at": datetime.now(timezone.utc),
                    "metadata.scheduled_skip_reason": "no valid numbers",
                }
            },
        )
        return
    active_calls = max(1, doc.get("active_calls", 1))
    try:
        await db.campaigns.update_one(
            {"_id": ObjectId(campaign_id)},
            {"$set": {"status": "running", "started_at": datetime.now(timezone.utc)}},
        )
        await start_campaign_with_id(scope, numbers, campaign_id, active_calls=active_calls)
        logger.info(
            "[ScheduledCampaigns] Started scheduled campaign (after previous ended): id=%s scope=%s numbers=%s",
            campaign_id, scope[:8] + "...", len(numbers),
        )
    except Exception as e:
        logger.exception(
            "[ScheduledCampaigns] Failed to start scheduled campaign %s: %s", campaign_id, e
        )
        await db.campaigns.update_one(
            {"_id": ObjectId(campaign_id)},
            {
                "$set": {
                    "status": "scheduled",
                    "started_at": None,
                    "metadata.scheduled_start_error": str(e)[:500],
                }
            },
        )


async def run_scheduled_campaigns_worker():
    """
    Loop: every CHECK_INTERVAL_SECONDS, find campaigns with status=scheduled and
    scheduled_at <= now. For each scope with no active campaign (Redis), start
    the earliest due scheduled campaign. Only one campaign runs per scope at a time.
    """
    while True:
        try:
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            await _process_due_scheduled_campaigns()
        except asyncio.CancelledError:
            logger.info("[ScheduledCampaigns] Worker cancelled")
            break
        except Exception as e:
            logger.exception("[ScheduledCampaigns] Worker error: %s", e)


async def _process_due_scheduled_campaigns():
    """
    Find due scheduled campaigns (scheduled_at <= now), grouped by tenant_id.
    For each scope, if no campaign is currently running (Redis active_key not set),
    start the earliest due campaign for that scope.
    """
    now = _now_for_scheduled_check()
    cursor = db.campaigns.find(
        {"status": "scheduled", "scheduled_at": {"$lte": now}}
    ).sort("scheduled_at", 1)
    due_docs = await cursor.to_list(length=100)
    if not due_docs:
        return

    # Group by tenant_id (scope); start at most one per scope if that scope is free
    by_scope: dict[str, list] = {}
    for doc in due_docs:
        scope = doc.get("tenant_id") or "default"
        if scope not in by_scope:
            by_scope[scope] = []
        by_scope[scope].append(doc)

    for scope, campaigns in by_scope.items():
        active_key = _active_key(scope)
        is_active = await redis_client.get(active_key)
        if is_active:
            logger.info(
                "[ScheduledCampaigns] Scope %s has active campaign; holding %s scheduled campaign(s)",
                scope[:8] + "...", len(campaigns),
            )
            continue

        doc = campaigns[0]
        campaign_id = str(doc["_id"])
        if await _scope_has_running(scope, exclude_campaign_id=campaign_id):
            logger.info(
                "[ScheduledCampaigns] Scope %s has running campaign in DB; holding %s scheduled",
                scope[:8] + "...", len(campaigns),
            )
            continue
        contacts = doc.get("contacts") or []
        numbers = to_e164_list(contacts)
        if not numbers:
            logger.warning(
                "[ScheduledCampaigns] Skipping campaign %s: no valid numbers", campaign_id
            )
            await db.campaigns.update_one(
                {"_id": ObjectId(campaign_id)},
                {
                    "$set": {
                        "status": "cancelled",
                        "ended_at": datetime.now(timezone.utc),
                        "metadata.scheduled_skip_reason": "no valid numbers",
                    }
                },
            )
            continue

        active_calls = max(1, doc.get("active_calls", 1))
        try:
            await db.campaigns.update_one(
                {"_id": ObjectId(campaign_id)},
                {"$set": {"status": "running", "started_at": datetime.now(timezone.utc)}},
            )
            await start_campaign_with_id(scope, numbers, campaign_id, active_calls=active_calls)
            logger.info(
                "[ScheduledCampaigns] Started scheduled campaign: id=%s scope=%s numbers=%s",
                campaign_id, scope[:8] + "...", len(numbers),
            )
        except Exception as e:
            logger.exception(
                "[ScheduledCampaigns] Failed to start scheduled campaign %s: %s", campaign_id, e
            )
            await db.campaigns.update_one(
                {"_id": ObjectId(campaign_id)},
                {
                    "$set": {
                        "status": "scheduled",
                        "started_at": None,
                        "metadata.scheduled_start_error": str(e)[:500],
                    }
                },
            )
