"""
Outbound call trigger via Exotel API.

User isolation:
- Role 3 (user) uses user_id for agent scope.
- Role 4 (demo) uses 'default'.

Agent lookup is by user_id or tenant_id (get_agent_for_scope).

Settings field mapping (config.py canonical → aliases kept for compat):
  EXOTEL_SID          = account SID  (A alias: EXOTEL_ACCOUNT_SID)
  EXOTEL_AUTH_TOKEN   = API token    (A alias: EXOTEL_API_TOKEN)
  EXOTEL_VIRTUAL_NUMBER = ExoPhone   (A alias: EXOTEL_FROM_NUMBER)
"""

import asyncio
import json
import traceback
from datetime import datetime, timezone
from typing import Optional, Tuple

from bson import ObjectId
import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from config import get_settings
from models import CallRecord, ROLE_USER, ROLE_DEMO
from utils.db import db, redis_client, get_agent_for_scope, get_agent_for_phone
from utils.logger import logger
from utils.security import get_current_user

router = APIRouter(prefix="/outbound", tags=["Outbound"])

EXOTEL_TIMEOUT = 15.0
# Retry delays: 10, 60, 120 minutes.
RETRY_DELAYS_SECONDS = [10 * 60, 60 * 60, 120 * 60]

# Demo: industry-based RAG/persona + first-line greeting.
DEMO_INDUSTRY_REDIS_KEY = "demo_industry:default"
DEMO_GREETING_REDIS_KEY = "demo_greeting:default"
DEMO_INDUSTRY_TTL_SECONDS = 7200  # 2 hours
DEMO_GREETING_TTL_SECONDS = 300   # 5 minutes
VALID_DEMO_INDUSTRIES = frozenset({"financial", "realestate", "education"})


def _exotel_from_format(phone: str) -> str:
    """Exotel India expects mobile numbers as 0 + 10 digits (e.g. 08424868079)."""
    digits = "".join(c for c in phone.strip() if c.isdigit())
    if len(digits) >= 10:
        ten = digits[-10:]
        return "0" + ten
    return phone.strip()


def _queue_key(scope: str) -> str:
    """Redis key for this owner scope (user_id or 'default')."""
    return f"campaign_queue:{scope}"


def _active_key(scope: str) -> str:
    return f"campaign_active:{scope}"


def _campaign_id_key(scope: str) -> str:
    return f"campaign_active_id:{scope}"


def _inflight_key(scope: str) -> str:
    return f"campaign_inflight:{scope}"


def _schedule_campaign_callback_timeout(scope: str, call_sid: str) -> None:
    """
    Placeholder for campaign callback timeout (e.g. if Exotel never sends status callback).
    Next campaign call is triggered from Exotel webhook when a call ends; this can be extended
    to schedule a fallback trigger if no webhook is received within a timeout.
    """
    pass


def _effective_user_scope(user_id_from_request: str, current_user: dict) -> str:
    """
    Resolve owner scope:
    - role 3 (ROLE_USER): JWT user_id
    - role 4 (ROLE_DEMO): 'default'
    - admin: request user_id or own user_id
    """
    role = current_user.get("role", ROLE_USER)
    user_id = current_user.get("user_id")
    if role == ROLE_USER:
        return user_id
    if role == ROLE_DEMO:
        return "default"
    return (user_id_from_request or "").strip() or user_id


def _demo_queue_item(number: str, industry: Optional[str], greeting: Optional[str]) -> str:
    """Serialize one demo queue entry so each call gets its own industry + greeting."""
    return json.dumps(
        {
            "n": number,
            "i": (industry or "").strip().lower() or "",
            "g": (greeting or "").strip()[:2000] or "",
        }
    )


def _parse_demo_queue_item(raw: Optional[str | bytes]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse queue value:
    - if JSON: return (number, industry, greeting)
    - else: (raw, None, None) for backward compatibility
    """
    if not raw:
        return None, None, None
    s = raw.decode() if isinstance(raw, bytes) else raw
    s = (s or "").strip()
    if not s:
        return None, None, None
    if s.startswith("{"):
        try:
            data = json.loads(s)
            return (
                data.get("n") or None,
                (data.get("i") or "").strip() or None,
                (data.get("g") or "").strip() or None,
            )
        except Exception:
            return s, None, None
    return s, None, None


class OutboundCampaignRequest(BaseModel):
    """Request body for campaign (sequential calls)."""

    numbers: list[str] = Field(..., description="List of phone numbers in E.164 format")
    user_id: str = Field(..., alias="tenant_id", description="Owner scope: user_id or 'default' for demo")
    industry: Optional[str] = Field(
        None,
        description="Demo only: industry for RAG/persona (financial|realestate|education)",
    )
    greeting: Optional[str] = Field(
        None,
        description="Demo only: first-line script when call connects (~15 sec)",
    )


class OutboundTriggerRequest(BaseModel):
    """Request body for outbound call trigger."""

    to: str = Field(..., description="Customer phone number in E.164 format (e.g. +919876543210)")
    user_id: str = Field(..., alias="tenant_id", description="Owner scope for agent lookup and call routing")


class OutboundTriggerResponse(BaseModel):
    """Response from outbound call trigger."""

    call_sid: str
    status: str
    message: str


@router.get("/test-connection")
async def test_exotel_connection():
    """Diagnostic endpoint: test connectivity to Exotel API using BasicAuth."""
    import time

    settings = get_settings()
    # Resolve canonical field names with alias fallback
    account_sid = (getattr(settings, "EXOTEL_ACCOUNT_SID", None) or settings.EXOTEL_SID or "").strip()
    api_key = (settings.EXOTEL_API_KEY or "").strip()
    api_token = (getattr(settings, "EXOTEL_API_TOKEN", None) or settings.EXOTEL_AUTH_TOKEN or "").strip()

    if not account_sid or not api_key or not api_token:
        return {
            "error": "EXOTEL_SID, EXOTEL_API_KEY, or EXOTEL_AUTH_TOKEN not configured",
            "results": None,
        }

    auth = httpx.BasicAuth(username=api_key, password=api_token)
    subdomains_to_test = ["api.exotel.com", "api.in.exotel.com"]
    results: dict[str, dict] = {}

    for subdomain in subdomains_to_test:
        url = f"https://{subdomain}/v1/Accounts/{account_sid}"
        start = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url, auth=auth)
            latency = time.perf_counter() - start
            results[subdomain] = {
                "status": resp.status_code,
                "latency_seconds": round(latency, 2),
                "ok": resp.status_code < 400,
                "body_preview": resp.text[:200] if resp.text else None,
            }
        except httpx.TimeoutException as e:
            results[subdomain] = {
                "status": "timeout",
                "latency_seconds": 10.0,
                "ok": False,
                "error": str(e),
            }
        except Exception as e:
            results[subdomain] = {
                "status": "error",
                "latency_seconds": round(time.perf_counter() - start, 2),
                "ok": False,
                "error": str(e),
            }

    return {
        "account_sid": account_sid,
        "configured_subdomain": (settings.EXOTEL_SUBDOMAIN or "api.exotel.com").strip(),
        "results": results,
        "suggestion": "Use the subdomain with ok=true and lowest latency for EXOTEL_SUBDOMAIN",
    }


async def _trigger_single_call(
    scope: str,
    to_number: str,
    campaign_id: Optional[str] = None,
    skip_credit_check: bool = False,
    is_test: bool = False,
) -> Tuple[str, str]:
    """
    Trigger one outbound call via Exotel.

    scope = owner user_id or 'default'.
    Uses phone assigned to user when present; else agent from get_agent_for_scope(scope).
    Returns (call_sid, status). Raises HTTPException on failure.
    """
    settings = get_settings()
    # Resolve Exotel credentials — canonical names take precedence, aliases as fallback
    _acct_sid = (getattr(settings, "EXOTEL_ACCOUNT_SID", None) or settings.EXOTEL_SID or "").strip()
    _api_key = (settings.EXOTEL_API_KEY or "").strip()
    _api_tok = (getattr(settings, "EXOTEL_API_TOKEN", None) or settings.EXOTEL_AUTH_TOKEN or "").strip()

    if not _acct_sid or not _api_key or not _api_tok:
        raise HTTPException(status_code=503, detail="Outbound calling not configured.")

    to_number = to_number.strip()
    if not to_number or len(to_number) < 10:
        raise HTTPException(status_code=400, detail="Invalid phone number.")

    # Block outbound when credits are at/below threshold (unless explicitly skipped).
    if not skip_credit_check:
        try:
            scope_oid = ObjectId(scope)
            user_doc = await db.users.find_one({"_id": scope_oid}, projection={"credits": 1, "unlimited_credits": 1})
            if user_doc is not None:
                if user_doc.get("unlimited_credits", False):
                    pass  # Unlimited credits — allow call
                else:
                    credits = user_doc.get("credits", 0)
                    threshold = int(getattr(settings, "CREDITS_CAMPAIGN_PAUSE_THRESHOLD", 0) or 0)
                    limit = threshold if threshold > 0 else 0
                    if credits <= limit:
                        msg = (
                            f"Credits are below {threshold}. Please add credits to continue calls."
                            if threshold > 0
                            else "Credits are zero or negative. Please add credits to continue calls."
                        )
                        logger.warning(
                            "Blocking outbound call due to low credits: scope=%s credits=%s threshold=%s",
                            scope, credits, threshold,
                        )
                        raise HTTPException(status_code=400, detail=msg)
        except HTTPException:
            raise
        except Exception:
            # scope not a valid user id (e.g. "default" demo); allow
            pass

    # Prefer number assigned to user → use that phone's number, Exotel config, and agent for outbound
    phone_query = None
    if scope:
        try:
            scope_oid = ObjectId(scope)
            phone_query = {"$or": [{"assigned_to_user_id": scope}, {"assigned_to_user_id": scope_oid}]}
        except Exception:
            phone_query = {"assigned_to_user_id": scope}
    phone_doc = await db.phones.find_one(phone_query) if phone_query else None
    _phone_has_agent = bool(
        phone_doc and (phone_doc.get("assigned_to_agent_id") or phone_doc.get("agent_id"))
    )
    if _phone_has_agent:
        agent = await get_agent_for_phone(phone_doc)
        from_number = (phone_doc.get("number") or "").strip()
        exotel_raw = phone_doc.get("exotel_data")
        exotel_data = exotel_raw if isinstance(exotel_raw, dict) else {}
    else:
        agent = await get_agent_for_scope(scope)
        from_number = (agent.get("virtual_number") or "").strip() if agent else ""
        exotel_data = {}

    if not agent:
        try:
            user_doc = await db.users.find_one({"_id": ObjectId(scope)}) if scope else None
            email = (user_doc or {}).get("email") or scope
        except Exception:
            email = scope
        if phone_doc and not _phone_has_agent:
            detail_msg = "No agent is assigned to this phone number. Please contact support."
        else:
            detail_msg = (
                f"No agent for user {email}. User Management: assign a phone to this user. "
                "Then Phone Numbers: assign an Agent to that phone."
            )
        raise HTTPException(status_code=404, detail=detail_msg)

    _virt_num = (getattr(settings, "EXOTEL_FROM_NUMBER", None) or settings.EXOTEL_VIRTUAL_NUMBER or "").strip()
    from_number = from_number or _virt_num
    if not from_number:
        raise HTTPException(status_code=503, detail="Assign a phone to this user or set EXOTEL_VIRTUAL_NUMBER.")
    if not settings.EXOTEL_APP_ID:
        raise HTTPException(status_code=503, detail="EXOTEL_APP_ID not set.")

    # Per-phone Exotel config when available, else global settings.
    account_sid = (exotel_data.get("sid") or _acct_sid or "").strip()
    api_key = (exotel_data.get("api_key") or _api_key or "").strip()
    api_token = (exotel_data.get("api_token") or _api_tok or "").strip()
    subdomain = (exotel_data.get("subdomain") or settings.EXOTEL_SUBDOMAIN or "api.exotel.com").strip()
    app_id = (exotel_data.get("app_id") or settings.EXOTEL_APP_ID or "").strip()

    if not account_sid or not api_key or not api_token or not app_id:
        raise HTTPException(
            status_code=503,
            detail="Exotel credentials/app_id not configured for this phone or tenant.",
        )

    exoml_url = f"http://my.exotel.com/{account_sid}/exoml/start_voice/{app_id}"
    from_for_exotel = _exotel_from_format(to_number)

    payload: dict[str, str] = {
        "From": from_for_exotel,
        "CallerId": from_number,
        "Url": exoml_url,
        "CallType": "trans",
    }

    # Always set StatusCallback when NGROK_URL exists so Exotel notifies when call ends.
    if settings.NGROK_URL:
        base = settings.NGROK_URL.rstrip("/")
        payload["StatusCallback"] = f"{base}/api/v1/exotel/voice/status-callback"

    url = f"https://{subdomain}/v1/Accounts/{account_sid}/Calls/connect.json"
    auth = httpx.BasicAuth(username=api_key, password=api_token)

    logger.info(f"Exotel call: POST {url} | From={from_for_exotel} CallerId={from_number}")
    try:
        async with httpx.AsyncClient(timeout=EXOTEL_TIMEOUT) as client:
            response = await client.post(url, data=payload, auth=auth)
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Exotel API timed out.")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Exotel API error: {e}")

    if response.status_code == 401:
        body = (response.text or "").strip()[:400]
        raise HTTPException(
            status_code=502,
            detail=(
                "Exotel 401 Unauthorized. Verify credentials at "
                "https://my.exotel.com/apisettings/site#api-credentials. "
                f"Response: {body}"
            ),
        )
    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Exotel {response.status_code}: {response.text[:300]}",
        )

    try:
        data = response.json()
    except Exception as e:
        logger.error(f"Failed to parse Exotel response: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=502, detail="Invalid response from Exotel API.")

    call_data = data.get("Call", data)
    call_sid = (call_data.get("Sid") or "").strip()
    status_raw = call_data.get("Status", "unknown")
    status = (
        (status_raw or "unknown").strip().lower().replace(" ", "-")
        if isinstance(status_raw, str)
        else "unknown"
    )

    if not call_sid:
        raise HTTPException(status_code=502, detail="Exotel did not return call SID.")

    agent_id_str = str(agent.get("_id") or agent.get("agent_id") or "unknown")
    call_record = CallRecord(
        call_id=call_sid,
        user_id=scope,
        agent_id=agent_id_str,
        from_number=from_number,
        to_number=to_number,
        direction="outbound",
        status=status,
        campaign_id=campaign_id,
    )
    call_dict = call_record.model_dump(by_alias=True)
    if is_test:
        call_dict["is_test"] = True
    await db.calls.insert_one(call_dict)

    # Store call_sid → agent _id so webhook.py resolves the *exact* agent attached to
    # this phone, not just the first active agent for the user scope.
    # get_tenant_config() already handles 24-char hex strings as direct _id lookups,
    # so storing agent _id here is safe and eliminates ambiguity for users with
    # multiple agents.
    # Also store call_direction — Exotel may report "inbound" from the voicebot's
    # perspective even for outbound-initiated calls, so Redis is authoritative.
    # TTL 5 minutes — enough for Exotel to ring, answer, and reach the voicebot webhook.
    if redis_client is not None:
        try:
            agent_id_for_routing = str(agent.get("_id") or "") or scope
            await redis_client.set(f"call_tenant:{call_sid}", agent_id_for_routing, ex=300)
            await redis_client.set(f"call_direction:{call_sid}", "outbound", ex=300)
            logger.debug(
                f"[Outbound] Stored call_tenant:{call_sid} → agent_id={agent_id_for_routing} "
                f"scope={scope}, direction=outbound"
            )
        except Exception as _re:
            logger.debug(f"[Outbound] Redis call_tenant store failed (non-fatal): {_re}")

    logger.info(f"Outbound call initiated: call_sid={call_sid}, to={to_number}")
    return call_sid, status


@router.post("/trigger", response_model=OutboundTriggerResponse)
async def trigger_outbound(
    request: OutboundTriggerRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Trigger an outbound call via Exotel.
    Role 3: scope = user_id from JWT. Role 4: scope = 'default'.
    """
    scope = _effective_user_scope(request.user_id, current_user)
    call_sid, status = await _trigger_single_call(scope, request.to)
    return OutboundTriggerResponse(
        call_sid=call_sid,
        status=status,
        message=(
            f"Call initiated to {request.to}. "
            "Exotel will connect to your bot when the customer answers."
        ),
    )


async def trigger_next_campaign_call(scope: str, call_sid: Optional[str] = None) -> None:
    """
    Called ONLY from the Exotel status-callback webhook when a campaign call ends.

    Decrements inflight counter, then triggers enough new calls to fill up to
    the campaign's active_calls concurrency limit.
    """
    try:
        logger.info(f"Campaign trigger_next: scope={scope} call_sid={call_sid}")

        active_key = _active_key(scope)
        is_active = await redis_client.get(active_key)
        if not is_active:
            logger.info(f"Campaign trigger_next: no active campaign for scope={scope}, skipping")
            return

        campaign_id_key = _campaign_id_key(scope)
        campaign_id = await redis_client.get(campaign_id_key)
        inflight_key = _inflight_key(scope)
        queue_key = _queue_key(scope)

        # Decrement inflight counter (a call just ended)
        inflight = await redis_client.decr(inflight_key)
        if inflight < 0:
            # Negative means duplicate/stale webhook — reset and bail to avoid double-triggering.
            await redis_client.set(inflight_key, "0", ex=7200)
            logger.warning(
                "Campaign trigger_next: duplicate webhook detected (inflight went negative) "
                "for scope=%s call_sid=%s — skipping trigger to prevent over-firing",
                scope, call_sid,
            )
            return

        # Get concurrency limit from campaign doc
        max_concurrent = 1
        if campaign_id:
            try:
                campaign_doc = await db.campaigns.find_one(
                    {"_id": ObjectId(campaign_id)},
                    {"active_calls": 1},
                )
                if campaign_doc:
                    max_concurrent = max(1, campaign_doc.get("active_calls", 1))
            except Exception:
                pass

        queue_len = await redis_client.llen(queue_key)
        slots_available = max(0, max_concurrent - inflight)
        logger.info(
            "Campaign trigger_next: scope=%s active=True campaign_id=%s "
            "queue_len=%s inflight=%s max=%s slots=%s",
            scope, campaign_id, queue_len, inflight, max_concurrent, slots_available,
        )

        credit_exhausted = False
        skipped = 0
        triggered = 0
        MAX_SKIP = 50  # safety cap: stop if queue is all-DND/invalid
        for _ in range(slots_available + MAX_SKIP):
            if triggered >= slots_available:
                break
            if skipped >= MAX_SKIP:
                logger.warning("Campaign: hit skip cap (%s skipped) for scope=%s, stopping slot fill", MAX_SKIP, scope)
                break
            next_raw = await redis_client.lpop(queue_key)
            next_num, next_industry, next_greeting = _parse_demo_queue_item(next_raw)
            if not next_num:
                break

            # Demo: set Redis for this call so pipeline gets correct industry + greeting
            if scope == "default" and (next_industry or next_greeting):
                if next_industry and next_industry in VALID_DEMO_INDUSTRIES:
                    await redis_client.set(
                        DEMO_INDUSTRY_REDIS_KEY,
                        next_industry,
                        ex=DEMO_INDUSTRY_TTL_SECONDS,
                    )
                if next_greeting:
                    await redis_client.set(
                        DEMO_GREETING_REDIS_KEY,
                        next_greeting,
                        ex=DEMO_GREETING_TTL_SECONDS,
                    )
                logger.info(
                    "[DEMO] Next call: industry=%s, greeting=%s chars",
                    next_industry or "(none)", len(next_greeting or ""),
                )

            try:
                await _trigger_single_call(scope, next_num, campaign_id)
                await redis_client.incr(inflight_key)
                triggered += 1
                logger.info("Campaign: triggered next call to %s for user_id=%s", next_num, scope)
            except HTTPException as he:
                if "credit" in str(he.detail or "").lower():
                    if next_raw:
                        await redis_client.lpush(queue_key, next_raw)
                        logger.info(
                            "Campaign: pushed number back to queue (credits exhausted): %s", next_num,
                        )
                    logger.warning("Campaign: stopping — credits zero or negative: %s", he.detail)
                    credit_exhausted = True
                    break
                else:
                    # Per-number error (DND/NDNC, invalid number, Exotel rejection).
                    skipped += 1
                    logger.warning(f"Campaign: skipping {next_num} [{skipped}/{MAX_SKIP}] (per-number error): {he.detail}")
                    if campaign_id:
                        try:
                            await db.campaigns.update_one(
                                {"_id": ObjectId(campaign_id)},
                                {"$inc": {"completed_calls": 1, "failed_calls": 1}},
                            )
                        except Exception:
                            pass

        # If credits exhausted: pause campaign so user can add credits and resume
        if credit_exhausted and campaign_id:
            try:
                await db.campaigns.update_one(
                    {"_id": ObjectId(campaign_id), "status": "running"},
                    {
                        "$set": {
                            "status": "paused",
                            "pause_reason": "credits_exhausted",
                            "paused_at": datetime.now(timezone.utc),
                        }
                    },
                )
                logger.info(
                    "Campaign paused (credits exhausted): campaign_id=%s scope=%s",
                    campaign_id, scope,
                )
            except Exception as e:
                logger.warning("Campaign pause (credits exhausted) failed for %s: %s", campaign_id, e)
            await redis_client.delete(active_key)
            await redis_client.delete(inflight_key)
            await redis_client.delete(campaign_id_key)

        # If nothing triggered and no inflight calls, clear active flags
        elif (
            not credit_exhausted
            and (current_inflight := int(await redis_client.get(inflight_key) or 0)) <= 0
            and (remaining := await redis_client.llen(queue_key)) == 0
        ):
            logger.info(
                "Campaign trigger_next: queue empty and no inflight for scope=%s, clearing active flags",
                scope,
            )
            await redis_client.delete(active_key)
            await redis_client.delete(inflight_key)
            if campaign_id:
                await redis_client.delete(campaign_id_key)
                # Check if campaign has post-campaign retry enabled
                try:
                    camp_doc = await db.campaigns.find_one(
                        {"_id": ObjectId(campaign_id)},
                        {"retry_config": 1, "status": 1},
                    )
                    has_post_retry = (
                        camp_doc
                        and isinstance(camp_doc.get("retry_config"), dict)
                        and camp_doc["retry_config"].get("enabled")
                    )
                except Exception:
                    has_post_retry = False

                if has_post_retry:
                    try:
                        await schedule_post_campaign_retry(campaign_id)
                        logger.info(
                            "Campaign trigger_next: queue empty for %s, post-campaign retry scheduled",
                            campaign_id,
                        )
                    except Exception as retry_err:
                        logger.warning(
                            "Campaign trigger_next: post-campaign retry failed for %s: %s",
                            campaign_id, retry_err,
                        )
                else:
                    try:
                        await db.campaigns.update_one(
                            {"_id": ObjectId(campaign_id), "status": "running"},
                            {"$set": {"status": "completed", "ended_at": datetime.now(timezone.utc)}},
                        )
                        logger.info("Campaign trigger_next: marked campaign %s as completed", campaign_id)
                    except Exception as e:
                        logger.warning(
                            "Campaign trigger_next: failed to mark campaign %s completed: %s",
                            campaign_id, e,
                        )
            # Immediately try to start any due scheduled campaign for this scope
            try:
                from scheduled_campaigns_worker import try_process_scheduled_campaigns_for_scope
                asyncio.create_task(try_process_scheduled_campaigns_for_scope(scope))
            except Exception as e:
                logger.warning(
                    "Campaign trigger_next: could not trigger scheduled-campaigns check: %s", e,
                )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Campaign trigger_next failed for scope %s: %s\n%s",
            scope, e, traceback.format_exc(),
        )


async def schedule_post_campaign_retry(campaign_id: str) -> None:
    """
    Schedule a post-campaign retry. After the configured delay, collect all
    failed/unanswered numbers (excluding DND/compliance) and re-queue them
    so the campaign runs another round.
    """
    from datetime import datetime as _dt, timedelta
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        return
    campaign = await db.campaigns.find_one({"_id": oid})
    if not campaign:
        return
    rc = campaign.get("retry_config")
    if not rc or not rc.get("enabled"):
        return
    # Only allow one retry round per campaign
    if int(campaign.get("retry_round", 0)) >= 1:
        await db.campaigns.update_one(
            {"_id": oid},
            {"$set": {"status": "completed", "ended_at": _dt.utcnow()}},
        )
        logger.info(f"Campaign {campaign_id}: max retry rounds reached, marking completed")
        return
    scope = campaign.get("tenant_id") or "default"

    # Check if there are actually any failed numbers to retry BEFORE scheduling
    pipeline = [
        {"$match": {"campaign_id": campaign_id}},
        {"$sort": {"created_at": -1}},
        {
            "$group": {
                "_id": "$to_number",
                "latest_status": {"$first": {"$toLower": {"$ifNull": ["$status", ""]}}},
                "latest_failure_reason": {"$first": {"$ifNull": ["$failure_reason", ""]}},
            }
        },
        {
            "$match": {
                "latest_status": {"$nin": ["completed", "positive"]},
                "latest_failure_reason": {"$nin": ["dnd", "compliance"]},
            }
        },
    ]
    retry_numbers = []
    async for row in db.calls.aggregate(pipeline):
        num = row.get("_id")
        if num and len(num) >= 10:
            retry_numbers.append(num)

    if not retry_numbers:
        await db.campaigns.update_one(
            {"_id": oid},
            {"$set": {"status": "completed", "ended_at": _dt.utcnow()}},
        )
        logger.info(f"Campaign {campaign_id}: all calls succeeded, no retry needed — marking completed")
        return

    # Persist retry_contact count for dashboard display
    try:
        await db.campaigns.update_one(
            {"_id": oid},
            {"$set": {"retry_contact": len(retry_numbers)}},
        )
    except Exception as stats_err:
        logger.warning("Campaign retry: failed to set retry_contact for %s: %s", campaign_id, stats_err)

    mode = rc.get("mode", "10min")
    if mode == "10min":
        delay = 600
    elif mode == "1hour":
        delay = 3600
    elif mode == "custom":
        minutes = int(rc.get("custom_delay_minutes") or 30)
        delay = max(60, minutes * 60)
    elif mode == "scheduled":
        try:
            scheduled = _dt.fromisoformat(rc.get("scheduled_time", ""))
            delta = (scheduled - _dt.utcnow()).total_seconds()
            delay = max(60, int(delta))
        except Exception:
            delay = 600
    else:
        delay = 600

    retry_at = _dt.utcnow() + timedelta(seconds=delay)
    await db.campaigns.update_one(
        {"_id": oid},
        {"$set": {"status": "retry_scheduled", "retry_scheduled_at": retry_at}},
    )
    logger.info(f"Campaign {campaign_id}: {len(retry_numbers)} failed numbers, retry scheduled in {delay}s (mode={mode})")

    async def _retry_worker():
        try:
            await asyncio.sleep(delay)
            camp = await db.campaigns.find_one({"_id": oid})
            if not camp:
                return
            status = (camp.get("status") or "").lower()
            if status in {"cancelled", "paused", "running"}:
                logger.info("Campaign retry skipped: %s status is %s", campaign_id, status)
                return

            re_pipeline = [
                {"$match": {"campaign_id": campaign_id}},
                {"$sort": {"created_at": -1}},
                {
                    "$group": {
                        "_id": "$to_number",
                        "latest_status": {"$first": {"$toLower": {"$ifNull": ["$status", ""]}}},
                        "latest_failure_reason": {"$first": {"$ifNull": ["$failure_reason", ""]}},
                    }
                },
                {
                    "$match": {
                        "latest_status": {"$nin": ["completed", "positive"]},
                        "latest_failure_reason": {"$nin": ["dnd", "compliance"]},
                    }
                },
            ]
            retry_numbers_final = []
            async for row in db.calls.aggregate(re_pipeline):
                num = row.get("_id")
                if num and len(num) >= 10:
                    retry_numbers_final.append(num)

            if not retry_numbers_final:
                await db.campaigns.update_one(
                    {"_id": oid},
                    {"$set": {"status": "completed", "ended_at": _dt.utcnow()}},
                )
                logger.info("Campaign retry: no numbers to retry for %s, marking completed", campaign_id)
                return

            new_round = int(camp.get("retry_round", 0)) + 1
            active_calls = camp.get("active_calls", 1)
            await db.campaigns.update_one(
                {"_id": oid},
                {
                    "$set": {
                        "status": "running",
                        "retry_round": new_round,
                        "retry_scheduled_at": None,
                        "started_at": _dt.utcnow(),
                        "ended_at": None,
                    }
                },
            )
            logger.info(
                "Campaign retry: starting round %s for %s with %s numbers",
                new_round, campaign_id, len(retry_numbers_final),
            )
            try:
                await db.campaigns.update_one(
                    {"_id": oid},
                    {
                        "$set": {"retry_contact": len(retry_numbers_final)},
                        "$inc": {"retry_attempt": 1},
                    },
                )
            except Exception as stats_err:
                logger.warning("Campaign retry: failed to update retry stats for %s: %s", campaign_id, stats_err)
            try:
                await start_campaign_with_id(
                    scope, retry_numbers_final, campaign_id, active_calls=active_calls,
                )
            except Exception as e:
                logger.error("Campaign retry start failed for %s: %s", campaign_id, e)
                await db.campaigns.update_one(
                    {"_id": oid},
                    {"$set": {"status": "completed", "ended_at": _dt.utcnow()}},
                )
        except Exception as e:
            logger.error(
                "Campaign retry worker failed: scope=%s, campaign=%s, error=%s\n%s",
                scope, campaign_id, e, traceback.format_exc(),
            )

    asyncio.create_task(_retry_worker())


async def start_campaign_with_id(
    scope: str,
    numbers: list[str],
    campaign_id: str,
    active_calls: int = 1,
) -> dict:
    """
    Start campaign with given campaign_id for a scope (user_id or 'default').
    Triggers up to `active_calls` concurrent calls initially.
    """
    numbers = [n.strip() for n in numbers if n and len(n.strip()) >= 10]
    if not numbers:
        raise HTTPException(status_code=400, detail="Provide at least one valid phone number.")

    # Same agent resolution as _trigger_single_call: prefer phone assigned to user with agent
    phone_query = None
    if scope:
        try:
            scope_oid = ObjectId(scope)
            phone_query = {"$or": [{"assigned_to_user_id": scope}, {"assigned_to_user_id": scope_oid}]}
        except Exception:
            phone_query = {"assigned_to_user_id": scope}
    phone_doc = await db.phones.find_one(phone_query) if phone_query else None
    _phone_has_agent = bool(
        phone_doc and (phone_doc.get("assigned_to_agent_id") or phone_doc.get("agent_id"))
    )
    if _phone_has_agent:
        agent = await get_agent_for_phone(phone_doc)
    else:
        agent = await get_agent_for_scope(scope)
    if not agent:
        try:
            user_doc = await db.users.find_one({"_id": ObjectId(scope)}) if scope else None
            email = (user_doc or {}).get("email") or scope
        except Exception:
            email = scope
        if phone_doc and not _phone_has_agent:
            detail_msg = "No agent is assigned to this phone number. Please contact support."
        else:
            detail_msg = (
                f"No agent for user {email}. Assign a phone to this user in User Management, "
                "then assign an Agent to that phone in Phone Numbers."
            )
        raise HTTPException(status_code=404, detail=detail_msg)

    queue_key = _queue_key(scope)
    active_key = _active_key(scope)
    campaign_id_key = _campaign_id_key(scope)
    inflight_key = _inflight_key(scope)

    is_active = await redis_client.get(active_key)
    queue_len = await redis_client.llen(queue_key)

    # Stale state: active flag set but queue empty → clear and start
    if is_active and queue_len == 0:
        await redis_client.delete(active_key)
        await redis_client.delete(inflight_key)
        try:
            await redis_client.delete(campaign_id_key)
        except Exception:
            pass
        is_active = None

    if is_active and queue_len > 0:
        raise HTTPException(
            status_code=409,
            detail="Another campaign is in progress. Please wait for it to finish.",
        )

    # Clear stale phone slot entries from previous runs
    try:
        phone_doc_for_scope = await db.phones.find_one({"assigned_to_user_id": scope})
        if phone_doc_for_scope:
            phone_id_str = str(phone_doc_for_scope.get("_id") or "")
            if phone_id_str:
                slot_key = f"phone:{phone_id_str}:active-calls:outbound"
                deleted = await redis_client.delete(slot_key)
                if deleted:
                    logger.info("Campaign: cleared stale phone outbound slots before start: key=%s", slot_key)
    except Exception as e:
        logger.warning("Campaign: failed to clear stale phone slots: %s", e)

    # Clear any stale queue left over from a previously paused/stopped campaign.
    await redis_client.delete(queue_key)
    active_calls = max(1, active_calls)
    batch = numbers[:active_calls]
    rest = numbers[active_calls:]

    if rest:
        await redis_client.rpush(queue_key, *rest)

    await redis_client.set(active_key, "1", ex=7200)
    await redis_client.set(campaign_id_key, campaign_id, ex=7200)
    await redis_client.set(inflight_key, "0", ex=7200)

    triggered: list[str] = []
    last_credit_error: Optional[HTTPException] = None

    for num in batch:
        try:
            call_sid, _status = await _trigger_single_call(scope, num, campaign_id)
            await redis_client.incr(inflight_key)
            triggered.append(call_sid)
            logger.info(
                "Campaign call triggered: campaign_id=%s, to=%s, call_sid=%s",
                campaign_id, num, call_sid,
            )
        except HTTPException as he:
            if "credit" in str(he.detail or "").lower():
                last_credit_error = he
            logger.warning(f"Campaign batch call failed for {num}: {he.detail}")
            try:
                await db.campaigns.update_one(
                    {"_id": ObjectId(campaign_id)},
                    {"$inc": {"completed_calls": 1, "failed_calls": 1}},
                )
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"Campaign batch call failed for {num}: {e}")
            try:
                await db.campaigns.update_one(
                    {"_id": ObjectId(campaign_id)},
                    {"$inc": {"completed_calls": 1, "failed_calls": 1}},
                )
            except Exception:
                pass

    # Fill empty slots from the queue so we always start with up to active_calls concurrent calls.
    while len(triggered) < active_calls:
        next_raw = await redis_client.lpop(queue_key)
        if not next_raw:
            break
        next_num, _, _ = _parse_demo_queue_item(next_raw)
        if not next_num:
            break
        try:
            call_sid, _ = await _trigger_single_call(scope, next_num, campaign_id)
            await redis_client.incr(inflight_key)
            triggered.append(call_sid)
            logger.info(f"Campaign fill-slot triggered: campaign_id={campaign_id}, to={next_num}, call_sid={call_sid}")
        except HTTPException as he:
            logger.warning(f"Campaign fill-slot call failed for {next_num}: {he.detail}")
            try:
                await db.campaigns.update_one(
                    {"_id": ObjectId(campaign_id)},
                    {"$inc": {"completed_calls": 1, "failed_calls": 1}},
                )
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"Campaign fill-slot call failed for {next_num}: {e}")
            try:
                await db.campaigns.update_one(
                    {"_id": ObjectId(campaign_id)},
                    {"$inc": {"completed_calls": 1, "failed_calls": 1}},
                )
            except Exception:
                pass

    if not triggered:
        await redis_client.delete(active_key)
        await redis_client.delete(inflight_key)
        await redis_client.delete(campaign_id_key)
        await redis_client.delete(queue_key)
        if last_credit_error:
            raise last_credit_error
        raise HTTPException(
            status_code=400,
            detail="All phone numbers failed to connect. Check your Exotel configuration and phone numbers.",
        )

    remaining_in_queue = await redis_client.llen(queue_key)
    return {
        "triggered": len(triggered),
        "queued": remaining_in_queue,
        "campaign_id": campaign_id,
    }


@router.post("/campaign/reset")
async def reset_campaign_post(
    user_id: str = "",
    current_user: dict = Depends(get_current_user),
):
    """POST alias for DELETE /campaign/reset — matches dashboard 'Test Call' tab."""
    return await reset_campaign(user_id=user_id, current_user=current_user)


@router.delete("/campaign/reset")
async def reset_campaign(
    user_id: str = "",
    current_user: dict = Depends(get_current_user),
):
    """Clear campaign active state and queue for the user scope."""
    effective = _effective_user_scope(user_id, current_user)
    queue_key = _queue_key(effective)
    active_key = _active_key(effective)
    campaign_id_key = _campaign_id_key(effective)
    inflight_key = _inflight_key(effective)

    await redis_client.delete(active_key)
    await redis_client.delete(campaign_id_key)
    await redis_client.delete(queue_key)
    await redis_client.delete(inflight_key)

    logger.info(
        "[Campaign] Reset scope=%s: cleared active, campaign_id, and queue. Next launch will trigger.",
        effective,
    )
    return {
        "status": "reset",
        "user_id": effective,
        "message": "Campaign state cleared. Next launch will trigger the first call.",
    }


@router.get("/campaign/status")
async def campaign_status(
    user_id: str = "",
    current_user: dict = Depends(get_current_user),
):
    """Return whether a campaign is active and how many numbers are queued."""
    effective = _effective_user_scope(user_id, current_user)
    queue_key = _queue_key(effective)
    active_key = _active_key(effective)

    is_active = await redis_client.get(active_key)
    queued = await redis_client.llen(queue_key)

    return {"user_id": effective, "active": bool(is_active), "queued": queued}


@router.post("/campaign")
async def start_campaign(
    request: OutboundCampaignRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Start a campaign: enqueue numbers and trigger first call.
    Role 3: scope = user_id from JWT. Role 4: scope = 'default'.
    Demo safeguard: limit to 5 numbers when scope='default'.
    """
    scope = _effective_user_scope(request.user_id, current_user)
    numbers = [n.strip() for n in request.numbers if n and len(n.strip()) >= 10]

    if not numbers:
        raise HTTPException(status_code=400, detail="Provide at least one valid phone number.")

    if scope == "default" and len(numbers) > 5:
        raise HTTPException(
            status_code=400,
            detail="Maximum 5 numbers are allowed for demo user.",
        )

    # Agent resolution
    phone_query = None
    if scope:
        try:
            scope_oid = ObjectId(scope)
            phone_query = {"$or": [{"assigned_to_user_id": scope}, {"assigned_to_user_id": scope_oid}]}
        except Exception:
            phone_query = {"assigned_to_user_id": scope}
    phone_doc = await db.phones.find_one(phone_query) if phone_query else None
    _phone_has_agent = bool(
        phone_doc and (phone_doc.get("assigned_to_agent_id") or phone_doc.get("agent_id"))
    )
    if _phone_has_agent:
        agent = await get_agent_for_phone(phone_doc)
    else:
        agent = await get_agent_for_scope(scope)
    if not agent:
        try:
            user_doc = await db.users.find_one({"_id": ObjectId(scope)}) if scope else None
            email = (user_doc or {}).get("email") or scope
        except Exception:
            email = scope
        if phone_doc and not _phone_has_agent:
            detail_msg = "No agent is assigned to this phone number. Please contact support."
        else:
            detail_msg = (
                f"No agent for user {email}. Assign a phone to this user in User Management, "
                "then assign an Agent to that phone in Phone Numbers."
            )
        raise HTTPException(status_code=404, detail=detail_msg)

    is_demo = scope == "default"
    demo_industry = (request.industry or "").strip().lower() if request.industry else ""
    if demo_industry not in VALID_DEMO_INDUSTRIES:
        demo_industry = ""
    demo_greeting = (request.greeting or "").strip()[:2000] if request.greeting else ""

    def _queue_values(nums: list[str]) -> list[str]:
        if is_demo and (demo_industry or demo_greeting):
            return [_demo_queue_item(n, demo_industry or None, demo_greeting or None) for n in nums]
        return nums

    queue_key = _queue_key(scope)
    active_key = _active_key(scope)
    is_active = await redis_client.get(active_key)
    queue_len = await redis_client.llen(queue_key)

    # Stale state: active set but queue empty → clear and start
    if is_active and queue_len == 0:
        await redis_client.delete(active_key)
        try:
            await redis_client.delete(_campaign_id_key(scope))
        except Exception:
            pass
        is_active = None
        logger.info("[Campaign] Cleared stale active flag (queue was empty); starting new campaign.")

    if is_active:
        if numbers:
            await redis_client.rpush(queue_key, *_queue_values(numbers))
        logger.info(
            "[Campaign] Already active — queued %s numbers; next call when current ends.", len(numbers),
        )
        return {
            "status": "queued",
            "queued": len(numbers),
            "message": "Numbers added to campaign queue.",
        }
    # Clear any stale queue left over from a previously paused/stopped campaign.
    await redis_client.delete(queue_key)

    rest = numbers[1:]
    if rest:
        await redis_client.rpush(queue_key, *_queue_values(rest))

    # Set Redis for first call so pipeline uses this batch's industry + greeting
    if is_demo and (demo_industry or demo_greeting):
        if demo_industry:
            await redis_client.set(DEMO_INDUSTRY_REDIS_KEY, demo_industry, ex=DEMO_INDUSTRY_TTL_SECONDS)
        if demo_greeting:
            await redis_client.set(DEMO_GREETING_REDIS_KEY, demo_greeting, ex=DEMO_GREETING_TTL_SECONDS)
        logger.info("[DEMO] First call: industry=%s, greeting=%s chars", demo_industry or "(none)", len(demo_greeting))

    await redis_client.set(active_key, "1", ex=7200)
    first = numbers[0]
    logger.info("[Campaign] Triggering first call to %s (user_id=%s); %s queued.", first, scope, len(rest))

    try:
        call_sid, status = await _trigger_single_call(scope, first, skip_credit_check=True)
        _schedule_campaign_callback_timeout(scope, call_sid)
        return {
            "status": "started",
            "first_call_sid": call_sid,
            "queued": len(numbers) - 1,
            "message": (
                f"Campaign started. First call to {first}. "
                "Next calls will trigger when each call ends."
            ),
        }
    except Exception:
        await redis_client.delete(active_key)
        await redis_client.lpush(queue_key, first)
        raise
