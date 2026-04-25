"""
Background service that periodically:
1. Finds completed calls without recordings
2. Fetches recording URL from Exotel API
3. Downloads the audio file
4. Uploads to AWS S3
5. Updates the call doc with S3 key + metadata

Runs as an asyncio background task started from main.py.
Uses Project B's canonical Settings fields (EXOTEL_SID, EXOTEL_AUTH_TOKEN)
with automatic fallback to migration aliases (EXOTEL_ACCOUNT_SID, EXOTEL_API_TOKEN).
"""

import asyncio
from datetime import datetime, timedelta, timezone

import httpx

from config import get_settings
from utils.db import db
from utils.logger import logger


def _exotel_base_url(settings) -> str:
    subdomain = settings.EXOTEL_SUBDOMAIN or "api.exotel.com"
    # Use canonical SID field; fall back to alias if needed
    sid = settings.EXOTEL_SID or settings.EXOTEL_ACCOUNT_SID or ""
    return f"https://{subdomain}/v1/Accounts/{sid}"


def _exotel_auth(settings) -> tuple[str, str]:
    """Return (api_key, api_token) using canonical names with alias fallback."""
    key = settings.EXOTEL_API_KEY or ""
    token = settings.EXOTEL_AUTH_TOKEN or settings.EXOTEL_API_TOKEN or ""
    return key, token


async def fetch_recording_url(call_sid: str, settings) -> str | None:
    """
    GET /v1/Accounts/{sid}/Calls/{callSid}/Recordings.json
    Returns the recording URL or None.
    """
    base = _exotel_base_url(settings)
    url = f"{base}/Calls/{call_sid}/Recordings.json"
    auth = _exotel_auth(settings)

    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(url, auth=auth)
            if resp.status_code != 200:
                logger.warning(f"[RecFetcher] Exotel Recordings API {resp.status_code} for {call_sid}")
                return None
            data = resp.json()
            rec_url = None
            if isinstance(data, dict):
                call_obj = data.get("Call") or data.get("call") or data
                rec_url = call_obj.get("RecordingUrl") or call_obj.get("recording_url")
                if not rec_url:
                    recs = data.get("Recordings") or data.get("recordings") or []
                    if isinstance(recs, list) and recs:
                        rec_url = recs[0].get("RecordingUrl") or recs[0].get("recording_url")
            if rec_url:
                logger.info(f"[RecFetcher] Got recording URL for {call_sid}")
            return rec_url
        except Exception as e:
            logger.error(f"[RecFetcher] Error fetching recording URL for {call_sid}: {e}")
            return None


async def download_recording(recording_url: str, settings) -> bytes | None:
    """Download the audio file from Exotel as bytes."""
    auth = _exotel_auth(settings)
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        try:
            resp = await client.get(recording_url, auth=auth)
            if resp.status_code != 200:
                logger.warning(f"[RecFetcher] Download failed {resp.status_code}: {recording_url}")
                return None
            data = resp.content
            logger.info(f"[RecFetcher] Downloaded {len(data)} bytes from Exotel")
            return data
        except Exception as e:
            logger.error(f"[RecFetcher] Download error: {e}")
            return None


async def _process_batch(settings) -> int:
    """Find calls needing recording fetch and process them."""
    now = datetime.now(timezone.utc)
    min_end_time = now - timedelta(seconds=settings.RECORDING_DELAY)

    query = {
        "status": {"$in": ["completed", "user-ended", "agent-ended", "positive"]},
        "call_id": {"$exists": True, "$ne": None},
        "created_at": {"$lte": min_end_time},
        "s3_recording_key": {"$exists": False},
        "$or": [
            {"recording_fetch_attempts": {"$exists": False}},
            {"recording_fetch_attempts": {"$lt": settings.RECORDING_MAX_ATTEMPTS}},
        ],
        "recording_fetch_status": {"$nin": ["not_available", "permanent_fail"]},
    }

    calls = await db.calls.find(query).sort("created_at", 1).limit(settings.RECORDING_BATCH_SIZE).to_list(
        length=settings.RECORDING_BATCH_SIZE
    )
    if not calls:
        return 0

    logger.info(f"[RecFetcher] Processing {len(calls)} calls for recording fetch")
    processed = 0

    for call_doc in calls:
        call_id = call_doc.get("call_id", "")
        attempts = call_doc.get("recording_fetch_attempts", 0) + 1

        try:
            rec_url = call_doc.get("recording_url") or await fetch_recording_url(call_id, settings)

            if not rec_url:
                status = "not_available" if attempts >= settings.RECORDING_MAX_ATTEMPTS else "pending"
                await db.calls.update_one(
                    {"call_id": call_id},
                    {"$set": {
                        "recording_fetch_attempts": attempts,
                        "last_recording_fetch_at": datetime.now(timezone.utc),
                        "recording_fetch_status": status,
                    }},
                )
                if status == "not_available":
                    logger.info(f"[RecFetcher] Recording not available for {call_id} after {attempts} attempts")
                continue

            audio_data = await download_recording(rec_url, settings)
            if not audio_data:
                await db.calls.update_one(
                    {"call_id": call_id},
                    {"$set": {
                        "recording_url": rec_url,
                        "recording_fetch_attempts": attempts,
                        "last_recording_fetch_at": datetime.now(timezone.utc),
                        "recording_fetch_status": "download_failed",
                    }},
                )
                continue

            try:
                from services.s3_service import upload_recording
                result = upload_recording(call_id, audio_data)
                s3_key = result["key"]
            except Exception as s3_err:
                logger.error(f"[RecFetcher] S3 upload failed for {call_id}: {s3_err}")
                await db.calls.update_one(
                    {"call_id": call_id},
                    {"$set": {
                        "recording_url": rec_url,
                        "recording_fetch_attempts": attempts,
                        "last_recording_fetch_at": datetime.now(timezone.utc),
                        "recording_fetch_status": "s3_upload_failed",
                    }},
                )
                continue

            # Optional summary backfill — never overwrites an existing summary
            existing = await db.calls.find_one({"call_id": call_id}, {"summary": 1, "duration": 1})
            summary = (existing or {}).get("summary") or ""
            duration_val = (existing or {}).get("duration")
            summary_to_set = None
            if not summary:
                try:
                    dur_int = int(duration_val) if duration_val is not None else None
                except (TypeError, ValueError):
                    dur_int = None
                if dur_int is not None and dur_int >= 0:
                    summary_to_set = f"Call completed. Duration: {dur_int}s."

            update_fields: dict = {
                "recording_url": rec_url,
                "s3_recording_key": s3_key,
                "s3_recording_uploaded_at": datetime.now(timezone.utc),
                "recording_fetch_attempts": attempts,
                "last_recording_fetch_at": datetime.now(timezone.utc),
                "recording_fetch_status": "available",
            }
            if summary_to_set is not None:
                update_fields["summary"] = summary_to_set

            await db.calls.update_one({"call_id": call_id}, {"$set": update_fields})
            processed += 1
            logger.info(f"[RecFetcher] Recording saved for {call_id} -> {s3_key}")

        except Exception as e:
            logger.error(f"[RecFetcher] Error processing {call_id}: {e}")
            try:
                await db.calls.update_one(
                    {"call_id": call_id},
                    {"$set": {
                        "recording_fetch_attempts": attempts,
                        "last_recording_fetch_at": datetime.now(timezone.utc),
                        "recording_fetch_status": "error",
                    }},
                )
            except Exception:
                pass

        await asyncio.sleep(0.3)

    return processed


async def run_recording_fetcher() -> None:
    """Infinite loop: fetch recordings every RECORDING_FETCH_INTERVAL seconds."""
    settings = get_settings()

    if not settings.AWS_ACCESS_KEY_ID or not settings.AWS_SECRET_ACCESS_KEY:
        logger.warning("[RecFetcher] AWS S3 credentials not configured — recording fetcher disabled")
        return

    api_key, api_token = _exotel_auth(settings)
    sid = settings.EXOTEL_SID or settings.EXOTEL_ACCOUNT_SID or ""
    if not api_key or not api_token or not sid:
        logger.warning("[RecFetcher] Exotel credentials not configured — recording fetcher disabled")
        return

    logger.info(
        f"[RecFetcher] Started — interval={settings.RECORDING_FETCH_INTERVAL}s, "
        f"delay={settings.RECORDING_DELAY}s, batch={settings.RECORDING_BATCH_SIZE}, "
        f"bucket={settings.AWS_S3_BUCKET}"
    )

    while True:
        try:
            count = await _process_batch(settings)
            if count:
                logger.info(f"[RecFetcher] Processed {count} recordings this cycle")
        except Exception as e:
            logger.error(f"[RecFetcher] Cycle error: {e}")
        await asyncio.sleep(settings.RECORDING_FETCH_INTERVAL)
