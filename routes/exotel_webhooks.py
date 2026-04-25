"""
Exotel post-call webhook endpoints.

NOTE: Two Exotel webhook paths coexist and must remain separate:
  routers/webhook.py      → POST /webhook/voicebot  (PROTECTED: returns WSS URL to Exotel for live call)
  routes/exotel_webhooks  → /api/v1/exotel/voice/*  (post-call processing: status, transfer, credits)

These are configured as separate hooks in Exotel's dashboard and must NEVER be merged.
"""

import asyncio
import json
import re as _re
from datetime import datetime, timezone
from urllib.parse import parse_qs

from bson import ObjectId
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

from utils.db import db
from utils.logger import logger

router = APIRouter(prefix="/api/v1/exotel/voice", tags=["Exotel Webhooks"])

_NON_TERMINAL_STATUSES = frozenset({"ringing", "in-progress", "queued"})
_NO_RETRY_STATUSES = frozenset({"dnd", "compliance"})

_STATUS_MAP = {
    "queued": "initiated", "initiated": "initiated",
    "ringing": "ringing",
    "in-progress": "in-progress", "inprogress": "in-progress",
    "active": "in-progress", "answered": "in-progress", "connected": "in-progress",
    "completed": "completed",
    "busy": "busy",
    "failed": "failed", "failure": "failed", "error": "failed", "rejected": "failed",
    "no-answer": "no-answer", "noanswer": "no-answer", "unanswered": "no-answer",
    "canceled": "canceled", "cancelled": "canceled",
}

_DND_RE = _re.compile(
    r"trai.*ndnc|ndnc|dnd|compliance|do.not.call|do.not.disturb|"
    r"blocked|rejected|blacklist|opt.out|optout",
    _re.IGNORECASE,
)


def _normalize_status(raw: str) -> str:
    return _STATUS_MAP.get(raw.lower().strip(), raw.lower().strip())


def _detect_failure_reason(merged: dict, canonical_status: str) -> str | None:
    """
    Mirror Node V2 exotel.controller.ts failure-reason detection.
    Returns a failure_reason string or None for success/in-progress calls.
    """
    error_msg = (
        merged.get("ErrorMessage") or merged.get("Error") or
        merged.get("Reason") or merged.get("Message") or ""
    ).lower()
    error_code = (
        merged.get("ErrorCode") or merged.get("Code") or
        merged.get("errorCode") or ""
    ).lower()
    status_raw = (merged.get("Status") or "").lower()

    is_dnd = bool(
        _DND_RE.search(error_msg) or
        _DND_RE.search(error_code) or
        any(k in status_raw for k in ("dnd", "ndnc", "blocked", "rejected"))
    )
    if is_dnd:
        if "dnd" in error_msg or "dnd" in error_code or "ndnc" in error_msg or "ndnc" in error_code:
            return "dnd"
        return "compliance"

    call_type = (merged.get("CallType") or "").lower()
    if _re.search(r"voicemail|vm|voice.?mail", call_type):
        return "voicemail"

    if error_msg == "user_inactivity":
        return "user_inactivity"

    if canonical_status == "no-answer":
        return "no_answer"
    if canonical_status == "busy":
        return "busy"
    if canonical_status in ("failed",):
        if _re.search(r"invalid|not found|not exist", error_msg):
            return "invalid_number"
        return "network_error"
    if canonical_status in ("canceled", "cancelled"):
        return "cancelled"

    return None


async def _generate_and_save_call_summary(call_sid: str, doc: dict) -> None:
    """
    Generate a short summary from the call transcript (OpenAI) and save to db.calls.
    Transcript is read from doc.transcript (agent persist) or db.call_transcripts (main.py).
    Runs in background; does not block the status-callback response.
    """
    transcript = doc.get("transcript") or []
    if not transcript and db is not None:
        ct_doc = await db.call_transcripts.find_one({"call_sid": call_sid})
        if ct_doc and ct_doc.get("messages"):
            transcript = [
                {"role": m.get("role", "user"), "content": m.get("content", "")}
                for m in ct_doc["messages"]
            ]
    if not transcript:
        return
    try:
        from config import get_settings
        settings = get_settings()
        if not getattr(settings, "OPENAI_API_KEY", None):
            return
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
        conv_text = "\n".join(
            f"{(t.get('role') or 'user').upper()}: {(t.get('content') or '').strip()}"
            for t in transcript
        )
        if not conv_text.strip():
            return
        resp = await asyncio.wait_for(
            client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are summarizing a phone call transcript. "
                            "Write a concise 2-3 sentence summary covering what was discussed "
                            "and the outcome. Use plain language, no markdown or lists."
                        ),
                    },
                    {"role": "user", "content": conv_text},
                ],
                max_tokens=200,
                temperature=0.3,
            ),
            timeout=15.0,
        )
        summary = (resp.choices[0].message.content or "").strip()
        if summary and db is not None:
            await db.calls.update_one(
                {"call_id": call_sid},
                {"$set": {"summary": summary}},
            )
            logger.info(f"[PostCall] Summary saved for call={call_sid}: {summary[:80]}")
    except asyncio.TimeoutError:
        logger.warning(f"[PostCall] Summary generation timed out for call={call_sid}")
    except Exception as exc:
        logger.warning(f"[PostCall] Summary generation failed for call={call_sid}: {exc}")


def _parse_exotel_duration(merged: dict) -> int | None:
    """Parse call duration in seconds from Exotel webhook params."""
    for key in ("Duration", "Duration[]", "Stream[Duration]", "Stream%5BDuration%5D"):
        val = merged.get(key)
        if val is None or val == "":
            continue
        try:
            sec = int(float(str(val).strip()))
            if sec >= 0:
                return sec
        except (ValueError, TypeError):
            continue
    return None


async def _parse_webhook_body(request: Request) -> dict:
    """Parse Exotel webhook body regardless of content-type (multipart, form-urlencoded, JSON)."""
    content_type = (request.headers.get("content-type") or "").lower()
    data = {}

    if "multipart/form-data" in content_type:
        form = await request.form()
        for key in form.keys():
            val = form[key]
            if hasattr(val, "read"):
                data[key] = ""
            elif isinstance(val, bytes):
                data[key] = val.decode("utf-8", errors="replace").strip()
            else:
                data[key] = str(val).strip() if val is not None else ""
    else:
        body = await request.body()
        raw_body = body.decode("utf-8", errors="replace") if body else ""
        if body:
            if "application/json" in content_type:
                try:
                    data = json.loads(body)
                except Exception:
                    pass
            if not data and "application/x-www-form-urlencoded" in content_type:
                parsed = parse_qs(raw_body)
                data = {k: (v[0] if v else "") for k, v in parsed.items()}

    qp = dict(request.query_params)
    return {**qp, **data}  # body wins over query


async def _fetch_exotel_duration(call_sid: str) -> int | None:
    """Fetch call duration from Exotel API when status-callback doesn't include it."""
    import httpx, base64, os
    account_sid = os.getenv("EXOTEL_SID") or os.getenv("EXOTEL_ACCOUNT_SID", "troikaplus1")
    api_key = os.getenv("EXOTEL_API_KEY", "")
    api_token = os.getenv("EXOTEL_AUTH_TOKEN", "")
    if not api_key or not api_token:
        return None
    creds = base64.b64encode(f"{api_key}:{api_token}".encode()).decode()
    url = f"https://api.exotel.com/v1/Accounts/{account_sid}/Calls/{call_sid}.json"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, headers={"Authorization": f"Basic {creds}"})
            if r.status_code == 200:
                data = r.json()
                dur = data.get("Call", {}).get("Duration")
                if dur is not None:
                    return int(dur)
    except Exception as e:
        logger.warning(f"[Credits] Failed to fetch Exotel call duration for {call_sid}: {e}")
    return None


async def _deduct_credits_atomic(call_sid: str, doc: dict, effective_duration: int, label: str) -> None:
    """
    Atomically deduct credits once per call using $setOnInsert upsert.
    Prevents double-deduction if both should_transfer and status-callback fire.
    """
    tenant_id = doc.get("tenant_id")
    if not tenant_id or doc.get("is_test"):
        if doc.get("is_test"):
            logger.info(f"[Credits] Skipping deduction for test call ({label}) {call_sid}")
        return

    try:
        user_oid = ObjectId(tenant_id)
        user_doc = await db.users.find_one({"_id": user_oid})
        if not user_doc:
            return
        current_credits = user_doc.get("credits", 0)
        is_unlimited = user_doc.get("unlimited_credits", False)
        new_credits = current_credits if is_unlimited else current_credits - effective_duration

        tx_result = await db.credit_transactions.find_one_and_update(
            {"call_id": call_sid, "action": "call_deduction"},
            {"$setOnInsert": {
                "user_id": tenant_id,
                "user_email": user_doc.get("email", ""),
                "user_role": user_doc.get("role", ""),
                "action": "call_deduction",
                "amount": -effective_duration,
                "previous_credits": current_credits,
                "new_credits": new_credits,
                "reason": "call duration [unlimited]" if is_unlimited else "call duration",
                "direction": doc.get("direction"),
                "campaign_id": doc.get("campaign_id"),
                "duration": effective_duration,
                "call_id": call_sid,
                "created_at": datetime.now(timezone.utc),
            }},
            upsert=True,
            return_document=False,
        )

        if tx_result is not None:
            logger.info(f"[Credits] Duplicate deduction blocked for call {call_sid} ({label})")
        elif is_unlimited:
            logger.info(
                f"Credit tracked (unlimited, {label}): call_id={call_sid} user={tenant_id} "
                f"amount={effective_duration} balance_unchanged={current_credits}"
            )
        else:
            await db.users.update_one(
                {"_id": user_oid},
                {"$set": {"credits": new_credits, "updated_at": datetime.now(timezone.utc)}}
            )
            logger.info(
                f"Credit deduction ({label}): call_id={call_sid} user={tenant_id} "
                f"amount={effective_duration} new_balance={new_credits}"
            )
    except Exception as e:
        logger.warning(f"Credit deduction ({label}) failed for call {call_sid}: {e}")


@router.post("/status-callback")
async def exotel_status_callback(request: Request):
    """
    Exotel calls this when a call reaches a terminal state.
    For campaign calls, trigger the next number for ANY terminal status
    (completed, failed, busy, no-answer, canceled, etc.).
    The dedup guard in trigger_next_campaign_call prevents double-processing
    when the pipeline cleanup also fires for the same call.
    """
    try:
        merged = await _parse_webhook_body(request)

        call_sid = (
            merged.get("CallSid") or merged.get("CallSid[]") or
            merged.get("callSid") or merged.get("callSid[]") or ""
        ).strip()
        status = (
            merged.get("Status") or merged.get("Status[]") or
            merged.get("status") or merged.get("status[]") or ""
        ).strip().lower()

        canonical = _normalize_status(status) if status else "unknown"
        failure_reason = _detect_failure_reason(merged, canonical)

        if failure_reason in ("dnd", "compliance"):
            canonical = failure_reason

        logger.info(
            f"Exotel status-callback: CallSid={call_sid}, Status={status} → {canonical}"
            + (f" [{failure_reason}]" if failure_reason else "")
        )

        if not call_sid:
            return {"status": "ok", "message": "no CallSid"}

        update_payload: dict = {"status": canonical}
        if failure_reason:
            update_payload["failure_reason"] = failure_reason
        duration_sec = _parse_exotel_duration(merged)
        if not duration_sec or duration_sec <= 0:
            duration_sec = await _fetch_exotel_duration(call_sid)
            if duration_sec:
                logger.info(f"[Credits] Fetched duration from Exotel API: {duration_sec}s for {call_sid}")
        if duration_sec is not None:
            update_payload["duration"] = duration_sec
            update_payload["credits"] = duration_sec
        await db.calls.update_one({"call_id": call_sid}, {"$set": update_payload})

        if status and status not in _NON_TERMINAL_STATUSES:
            doc = await db.calls.find_one({"call_id": call_sid})
            if doc and doc.get("tenant_id"):
                # Generate summary in background (agent job exits on session close, so we do it here)
                asyncio.create_task(_generate_and_save_call_summary(call_sid, doc))
                tenant_id = doc["tenant_id"]
                campaign_id = doc.get("campaign_id")

                # Resolve effective duration (from webhook → call doc duration → call doc credits)
                effective_duration = duration_sec
                if not effective_duration or effective_duration <= 0:
                    for key in ("duration", "credits"):
                        val = doc.get(key)
                        if val is not None:
                            try:
                                effective_duration = int(val)
                                if effective_duration > 0:
                                    break
                            except (TypeError, ValueError):
                                pass

                if effective_duration and effective_duration > 0:
                    await _deduct_credits_atomic(call_sid, doc, effective_duration, "status-callback")

                # Campaign stats + completion check
                try:
                    oid = ObjectId(campaign_id) if campaign_id else None
                except Exception:
                    oid = None

                if oid:
                    camp_doc_for_retry = await db.campaigns.find_one(
                        {"_id": oid}, {"retry_config": 1, "status": 1, "retry_round": 1}
                    )
                    has_post_campaign_retry = (
                        camp_doc_for_retry
                        and isinstance(camp_doc_for_retry.get("retry_config"), dict)
                        and camp_doc_for_retry["retry_config"].get("enabled")
                    )

                    is_success = canonical in {"completed", "positive"}
                    try:
                        inc_final: dict = {"completed_calls": 1}
                        if not is_success:
                            inc_final["failed_calls"] = 1
                            if has_post_campaign_retry and int(camp_doc_for_retry.get("retry_round") or 0) == 0:
                                inc_final["retry_contact"] = 1
                        await db.campaigns.update_one({"_id": oid}, {"$inc": inc_final})

                        camp = await db.campaigns.find_one({"_id": oid})
                        if camp:
                            total = int(camp.get("total_numbers") or 0)
                            done = int(camp.get("completed_calls") or 0)
                            current_status = (camp.get("status") or "").lower()
                            if (
                                current_status not in {"cancelled", "paused"}
                                and total > 0
                                and done >= total
                            ):
                                if has_post_campaign_retry:
                                    try:
                                        from routes.outbound import schedule_post_campaign_retry
                                        await schedule_post_campaign_retry(campaign_id)
                                        logger.info(
                                            f"Campaign {campaign_id}: all numbers processed, "
                                            f"post-campaign retry scheduled"
                                        )
                                    except Exception as retry_err:
                                        logger.warning(f"Post-campaign retry scheduling failed: {retry_err}")
                                else:
                                    await db.campaigns.update_one(
                                        {"_id": oid},
                                        {"$set": {"status": "completed", "ended_at": datetime.now(timezone.utc)}},
                                    )
                    except Exception as agg_err:
                        logger.warning(f"Failed to update campaign completion stats: {agg_err}")

                # Trigger next queued call
                from routes.outbound import trigger_next_campaign_call
                await trigger_next_campaign_call(tenant_id, call_sid=call_sid)
                logger.info(f"Campaign: call {call_sid} status={status}, triggered next via webhook")
    except Exception as e:
        logger.warning(f"Exotel status-callback error: {e}")
    return {"status": "ok"}


@router.get("/pass")
@router.post("/pass")
async def exotel_pass(request: Request):
    """Passthru applet — Exotel sends call info here. Return 200 OK to continue flow."""
    return {"status": "ok"}


@router.get("/should_transfer")
@router.post("/should_transfer")
async def should_transfer(request: Request):
    """
    Passthru URL: Exotel calls this when Voicebot (WebSocket) step ends.
    If call had transfer requested → 302 → Exotel goes to Connect.
    Else → 200 OK → Exotel goes to Hangup.
    """
    try:
        call_sid = request.query_params.get("CallSid") or request.query_params.get("call_id") or ""
        if not call_sid and request.method == "POST":
            body = await request.body()
            if body:
                try:
                    ct = (request.headers.get("content-type") or "").lower()
                    raw = body.decode("utf-8", errors="replace")
                    if "application/x-www-form-urlencoded" in ct:
                        parsed = parse_qs(raw)
                        call_sid = (parsed.get("CallSid") or parsed.get("call_id") or [""])[0]
                    else:
                        data = json.loads(raw) if raw.strip() else {}
                        call_sid = data.get("CallSid") or data.get("call_id") or ""
                except Exception:
                    pass
        call_sid = (call_sid or "").strip()
        logger.info(f"should_transfer: CallSid={call_sid}")

        if not call_sid:
            return PlainTextResponse("OK", status_code=200)

        qp = dict(request.query_params)
        duration_sec = _parse_exotel_duration(qp)
        if duration_sec is not None:
            await db.calls.update_one(
                {"call_id": call_sid},
                {"$set": {"duration": duration_sec, "credits": duration_sec}},
            )

        doc = await db.calls.find_one({"call_id": call_sid})
        if not doc:
            logger.info(f"should_transfer: no call doc for CallSid={call_sid} → 200")
            return PlainTextResponse("OK", status_code=200)

        # Credit deduction on should_transfer (same atomic guard as status-callback)
        effective_duration = duration_sec if duration_sec and duration_sec > 0 else None
        if effective_duration is None:
            d = doc.get("duration")
            if d is not None:
                try:
                    effective_duration = int(d)
                except (TypeError, ValueError):
                    pass
        if effective_duration and effective_duration > 0:
            await _deduct_credits_atomic(call_sid, doc, effective_duration, "should_transfer")

        meta = doc.get("metadata") or {}
        req = meta.get("transferRequested")
        num = (meta.get("transferToNumber") or "").strip()
        if req and num:
            logger.info(f"should_transfer: transfer requested for CallSid={call_sid} → 302")
            return Response(status_code=302, content="Redirect to Connect applet")
        logger.info(
            f"should_transfer: CallSid={call_sid} "
            f"transferRequested={req} transferToNumber={'set' if num else 'empty'} → 200"
        )
        return PlainTextResponse("OK", status_code=200)
    except Exception as e:
        logger.warning(f"should_transfer error: {e}")
        return PlainTextResponse("OK", status_code=200)


@router.get("/get_transfer_number")
@router.post("/get_transfer_number")
async def get_transfer_number(request: Request):
    """
    Exotel Connect (Dial Whom from URL) calls this when transfer is requested.
    Returns JSON { destination: { numbers: [normalized_number] } }.
    """
    try:
        call_sid = request.query_params.get("CallSid") or request.query_params.get("call_id")
        if not call_sid and request.method == "POST":
            body = await request.body()
            if body:
                try:
                    data_raw = body.decode("utf-8", errors="replace")
                    if "application/x-www-form-urlencoded" in (request.headers.get("content-type") or ""):
                        parsed = parse_qs(data_raw)
                        call_sid = (parsed.get("CallSid") or parsed.get("call_id") or [""])[0]
                    else:
                        data_json = json.loads(data_raw) if data_raw.strip() else {}
                        call_sid = data_json.get("CallSid") or data_json.get("call_id") or ""
                except Exception:
                    pass
        call_sid = (call_sid or "").strip()
        if not call_sid:
            logger.warning("get_transfer_number: no CallSid/call_id in request")
            return PlainTextResponse("", status_code=200, media_type="text/plain")

        doc = await db.calls.find_one(
            {"call_id": call_sid},
            projection={"metadata.transferRequested": 1, "metadata.transferToNumber": 1},
        )
        if not doc:
            return PlainTextResponse("", status_code=200, media_type="text/plain")
        meta = doc.get("metadata") or {}
        if not meta.get("transferRequested"):
            return PlainTextResponse("", status_code=200, media_type="text/plain")
        num = (meta.get("transferToNumber") or "").strip()

        def _norm(n: str) -> str:
            if not n:
                return ""
            n = n.replace("+", "").replace(" ", "")
            if n.startswith("91") and len(n) == 12:
                n = "0" + n[2:]
            return n

        call_from = (request.query_params.get("CallFrom") or request.query_params.get("Callfrom") or "").strip()
        call_to = (request.query_params.get("CallTo") or request.query_params.get("Callto") or "").strip()

        # Never return a call leg as the transfer target
        if _norm(num) and (_norm(num) == _norm(call_from) or _norm(num) == _norm(call_to)):
            logger.warning(f"get_transfer_number: DB number is a call leg, using env for CallSid={call_sid}")
            num = ""

        if not num:
            from config import get_settings
            env_num = (get_settings().TRANSFER_HUMAN_AGENT_NUMBER or "").strip()
            if env_num:
                num = env_num
                logger.info(f"get_transfer_number: using TRANSFER_HUMAN_AGENT_NUMBER fallback for CallSid={call_sid}")

        if not num:
            logger.warning(f"get_transfer_number: no number available for CallSid={call_sid}")
            return PlainTextResponse("", status_code=200, media_type="text/plain")

        normalized = num.replace("+", "").replace(" ", "")
        if normalized.startswith("91") and len(normalized) == 12:
            normalized = "0" + normalized[2:]

        logger.info(
            f"get_transfer_number: returning ...{normalized[-4:] if len(normalized) >= 4 else normalized} "
            f"for CallSid={call_sid}"
        )
        return JSONResponse(
            status_code=200,
            content={"destination": {"numbers": [normalized]}},
        )
    except Exception as e:
        logger.warning(f"get_transfer_number error: {e}")
        return PlainTextResponse("", status_code=200, media_type="text/plain")
