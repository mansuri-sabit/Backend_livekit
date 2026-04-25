"""
transcript_writer.py — Persists call transcript messages to MongoDB.

Collection: db.call_transcripts
Document shape:
  {
    call_sid:   str,
    messages:   [{"role": "agent"|"user", "content": str}],
    created_at: datetime,
    updated_at: datetime,
  }
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List

logger = logging.getLogger(__name__)


async def save_call_transcript(call_sid: str, messages: List[Dict]) -> None:
    """
    Upsert transcript messages for a call into db.call_transcripts.

    Args:
        call_sid:  Exotel / LiveKit call identifier.
        messages:  List of {"role": "agent"|"user", "content": str} dicts.
    """
    try:
        from utils.db import db
        if db is None:
            logger.warning("[Transcript] MongoDB not available — transcript not saved")
            return

        now = datetime.now(timezone.utc)
        await db.call_transcripts.update_one(
            {"call_sid": call_sid},
            {
                "$set":         {"updated_at": now},
                "$push":        {"messages": {"$each": messages}},
                "$setOnInsert": {"call_sid": call_sid, "created_at": now},
            },
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"[Transcript] Save failed for call_sid={call_sid}: {exc}")
