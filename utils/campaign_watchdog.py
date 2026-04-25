"""
Campaign Watchdog
=================
Background task that recovers stale campaign slots caused by dropped
status-callback webhooks.

Problem
-------
When Exotel fails to deliver a status callback (network glitch, their outage,
server restart), the call stays in "queued" status indefinitely and
campaign_inflight is never decremented. That concurrency slot is permanently
consumed. Once all max_concurrent slots are stuck, the campaign stops making
progress even though numbers remain in the Redis queue.

Solution
--------
Every WATCHDOG_INTERVAL_SECS seconds, scan MongoDB for calls that are:
  - status = "queued"                  (never got a status update)
  - created_at < now - THRESHOLD       (old enough to be genuinely stuck)
  - campaign_id is not None            (is part of a campaign)

For each stale call:
  1. Atomically claim it via find_one_and_update(status=queued → failed).
     If the real webhook arrives concurrently the update won't match, so
     we never double-decrement inflight.
  2. Call trigger_next_campaign_call(scope, call_sid) — the exact same
     function the real webhook calls.

Usage
-----
Start at app startup (main.py):
    from utils.campaign_watchdog import run_campaign_watchdog
    _watchdog_task = asyncio.create_task(run_campaign_watchdog())
"""

import asyncio
import os
from datetime import datetime, timedelta, timezone

from utils.db import db, redis_client
from utils.logger import logger

# A call sitting in "queued" for longer than this is considered stale.
# Default: 180s (3 minutes). Override via env var for testing: WATCHDOG_STALE_THRESHOLD_SECS=5
STALE_CALL_THRESHOLD_SECS: int = int(os.getenv("WATCHDOG_STALE_THRESHOLD_SECS", "180"))

# Gap between watchdog scans.
# Default: 60s. Override via env var for testing: WATCHDOG_INTERVAL_SECS=3
WATCHDOG_INTERVAL_SECS: int = int(os.getenv("WATCHDOG_INTERVAL_SECS", "60"))


async def _ensure_index() -> None:
    """
    Create a compound index on (status, created_at) for the calls collection.
    Safe to call multiple times — MongoDB ignores create_index if index already exists.
    """
    try:
        await db.calls.create_index(
            [("status", 1), ("created_at", 1)],
            background=True,
            name="watchdog_status_created",
        )
        logger.info("Campaign watchdog: index 'watchdog_status_created' ensured on calls collection")
    except Exception as e:
        logger.warning(f"Campaign watchdog: could not create index: {e}")


async def recover_stale_calls() -> int:
    """
    One scan pass: find stale queued campaign calls, recover each slot.
    Returns the number of slots recovered in this pass.
    """
    from routes.outbound import trigger_next_campaign_call

    cutoff = datetime.now(timezone.utc) - timedelta(seconds=STALE_CALL_THRESHOLD_SECS)
    recovered = 0

    async for call in db.calls.find(
        {
            "status": "queued",
            "created_at": {"$lt": cutoff},
            "campaign_id": {"$ne": None},
        },
        {"call_id": 1, "tenant_id": 1, "campaign_id": 1, "created_at": 1},
    ):
        scope = (call.get("tenant_id") or "").strip()
        call_sid = call.get("call_id")
        if not scope or not call_sid:
            continue

        # Skip if the campaign already ended
        is_active = await redis_client.get(f"campaign_active:{scope}")
        if not is_active:
            continue

        # Atomically claim this stale call — prevents double-decrement
        claimed = await db.calls.find_one_and_update(
            {"call_id": call_sid, "status": "queued"},
            {
                "$set": {
                    "status": "failed",
                    "duration": 0,
                    "watchdog_recovered": True,
                    "watchdog_recovered_at": datetime.now(timezone.utc),
                }
            },
        )
        if claimed is None:
            continue

        age_s = (datetime.now(timezone.utc) - call["created_at"]).total_seconds()
        campaign_id = call.get("campaign_id")
        logger.warning(
            f"Campaign watchdog: recovering stale slot — "
            f"call_sid={call_sid} scope={scope} "
            f"age={age_s:.0f}s (webhook was dropped or never delivered)"
        )

        # Keep campaign counters accurate
        if campaign_id:
            try:
                from bson import ObjectId
                await db.campaigns.update_one(
                    {"_id": ObjectId(campaign_id)},
                    {"$inc": {"failed_calls": 1, "completed_calls": 1}},
                )
            except Exception as e:
                logger.error(f"Campaign watchdog: failed to update campaign counters for {campaign_id}: {e}")

        try:
            await trigger_next_campaign_call(scope, call_sid=call_sid)
            recovered += 1
        except Exception as e:
            logger.error(
                f"Campaign watchdog: trigger_next_campaign_call failed "
                f"for call_sid={call_sid} scope={scope}: {e}"
            )

    return recovered


async def run_campaign_watchdog() -> None:
    """
    Long-running background worker. Sleep first so the app finishes startup
    before the first scan runs, then loop forever.
    """
    await _ensure_index()
    logger.info(
        f"Campaign watchdog started — "
        f"scan_interval={WATCHDOG_INTERVAL_SECS}s, "
        f"stale_threshold={STALE_CALL_THRESHOLD_SECS}s"
    )
    while True:
        try:
            await asyncio.sleep(WATCHDOG_INTERVAL_SECS)
        except asyncio.CancelledError:
            break
        try:
            recovered = await recover_stale_calls()
            if recovered > 0:
                logger.info(f"Campaign watchdog: recovered {recovered} stale slot(s)")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Campaign watchdog scan error (will retry next interval): {e}")
