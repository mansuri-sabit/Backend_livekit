"""
routers/webhook.py

Handles the Exotel Voicebot HTTP request (phase 1 of the two-phase protocol).

Exotel flow:
  1. HTTP POST/GET to /webhook/voicebot  → we return {"url": "<wss_url>"}
  2. Exotel opens a WebSocket to that URL → main.py /media handles audio

Tenant wiring:
  tenant_id is appended to the WebSocket URL as a query parameter.  main.py
  reads it from there and embeds it into the LiveKit room metadata so the
  agent can load the correct persona and KB without any hardcoded mapping.

  How to route a call to a specific tenant:
    Option A (recommended): Use a different Exotel webhook URL per tenant.
      Configure Exotel with:
        https://your-server.com/webhook/voicebot?tenant_id=<your_tenant_id>
      This file reads it from request.query_params automatically.

    Option B: Map the called number (To) to a tenant_id using a simple dict
      in PHONE_TO_TENANT below.  Zero config changes needed anywhere else.
"""
import logging
from fastapi import APIRouter, Request, Depends

from config import get_settings, Settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhook", tags=["webhooks"])

PHONE_TO_TENANT: dict[str, str] = {
    # "+918888000001": "hotel_sahara_star",
    # "+918888000002": "cipla_helpdesk",
}


def _resolve_tenant_id(
    query_tenant: str,
    to_number: str,
    default: str,
) -> str:
    """
    Static fallback resolution — used only when dynamic lookups (Redis/DB) fail.

    Priority:
      1. ?tenant_id= query param (set by Exotel App Bazaar webhook URL config)
      2. PHONE_TO_TENANT static mapping (called number)
      3. settings.DEFAULT_TENANT_ID (.env)
    """
    if query_tenant and query_tenant != "default":
        return query_tenant
    if to_number and to_number in PHONE_TO_TENANT:
        return PHONE_TO_TENANT[to_number]
    return default


async def _dynamic_resolve_tenant(
    call_sid: str,
    to_number: str,
    query_tenant: str,
    default: str,
) -> tuple[str, str | None]:
    """
    Dynamic tenant resolution — correct source of truth for both call directions.

    Returns:
        (tenant_id, agent_mongo_id_or_none)
        agent_mongo_id is passed to LiveKit room metadata so the voice worker loads
        the exact dashboard agent (persona + KB) even when tenant_id is user scope.

    Priority:
      1. Redis key `call_tenant:{call_sid}` — set by _trigger_single_call() for
         outbound calls so the correct user's agent is always used.
      2. DB phones collection by `to_number` — maps inbound calls to the agent
         assigned to that phone number in the dashboard.
      3. Static fallback via _resolve_tenant_id() (query param → PHONE_TO_TENANT → .env default).
    """
    # ── 1. Redis: outbound call mapping written by _trigger_single_call() ──────
    try:
        from utils.db import redis_client
        if redis_client is not None:
            stored = await redis_client.get(f"call_tenant:{call_sid}")
            if stored:
                raw = (
                    stored.decode() if isinstance(stored, bytes) else stored
                )
                s = str(raw).strip()
                logger.info(
                    f"[Webhook] Tenant resolved via Redis mapping: "
                    f"call_sid={call_sid} → tenant={s}"
                )
                return (s, s)
    except Exception as _re:
        logger.debug(f"[Webhook] Redis tenant lookup failed: {_re}")

    # ── 2. DB phones collection: find phone by destination number → get assigned agent ─
    try:
        from utils.db import db, get_phone_by_number, get_agent_for_phone, agent_owner_scope
        logger.info(f"[Webhook] Step 2: db={db is not None} to_number='{to_number}'")
        if db is not None and to_number:
            phone_doc = await get_phone_by_number(to_number)
            logger.info(f"[Webhook] phone_doc found={phone_doc is not None} for to={to_number}")
            if phone_doc:
                logger.info(
                    f"[Webhook] phone fields: agent_id={phone_doc.get('agent_id')} "
                    f"assigned_to_agent_id={phone_doc.get('assigned_to_agent_id')} "
                    f"assigned_to_user_id={phone_doc.get('assigned_to_user_id')}"
                )

                # Fast path: phone has a directly-assigned agent (dashboard field).
                # Same fields as get_agent_for_phone() — do not rely on legacy "agent_id" only.
                agent_id_raw = phone_doc.get("assigned_to_agent_id") or phone_doc.get("agent_id")
                if agent_id_raw:
                    scope = str(agent_id_raw)
                    logger.info(
                        f"[Webhook] Tenant resolved via phone → agent id: "
                        f"to={to_number} → scope={scope}"
                    )
                    return (scope, scope)

                # Slow path: no direct agent — try resolving via assigned_to_user_id.
                assigned_uid = str(phone_doc.get("assigned_to_user_id") or "")
                if assigned_uid:
                    logger.info(
                        f"[Webhook] Tenant resolved via phone.assigned_to_user_id: "
                        f"to={to_number} → user_scope={assigned_uid}"
                    )
                    return (assigned_uid, None)

                # Legacy path: look up agent doc and extract scope via agent_owner_scope.
                agent_doc = await get_agent_for_phone(phone_doc)
                logger.info(f"[Webhook] agent_doc found={agent_doc is not None}")
                if agent_doc:
                    scope = agent_owner_scope(agent_doc)
                    aid = str(agent_doc.get("_id") or "") or None
                    logger.info(
                        f"[Webhook] Tenant resolved via phone DB (legacy): "
                        f"to={to_number} → agent={agent_doc.get('_id')} name={agent_doc.get('name')} → scope={scope!r}"
                    )
                    if scope:
                        return (scope, aid)
                    logger.warning(
                        f"[Webhook] agent_owner_scope() returned empty for agent={agent_doc.get('_id')} "
                        f"name={agent_doc.get('name')} — check that user_id or tenant_id is set on the agent doc."
                    )
    except Exception as _de:
        logger.warning(f"[Webhook] DB phone→tenant lookup failed: {_de}")

    # ── 3. DB agents: match by virtual_number field on the agent document ──────
    # Covers the common case where a dashboard agent has virtual_number set
    # but the phone hasn't been registered separately in db.phones.
    try:
        from utils.db import db, agent_owner_scope, _normalize_phone_for_lookup
        if db is not None and to_number:
            _norm = _normalize_phone_for_lookup(to_number)
            _candidates = [to_number]
            if _norm and len(_norm) == 10:
                _candidates += [f"+91{_norm}", f"91{_norm}", f"0{_norm}", _norm]
            agent_doc = await db.agents.find_one(
                {"virtual_number": {"$in": _candidates}, "is_active": {"$ne": False}}
            )
            if agent_doc:
                scope = agent_owner_scope(agent_doc)
                if scope:
                    aid = str(agent_doc.get("_id") or "") or None
                    logger.info(
                        f"[Webhook] Tenant resolved via agent.virtual_number: "
                        f"to={to_number} → agent={agent_doc.get('_id')} → scope={scope}"
                    )
                    return (scope, aid)
    except Exception as _ve:
        logger.debug(f"[Webhook] agent.virtual_number lookup failed: {_ve}")

    # ── 4. Static fallback ─────────────────────────────────────────────────────
    resolved = _resolve_tenant_id(query_tenant, to_number, default)
    return (resolved, None)


@router.api_route("/voicebot", methods=["GET", "POST"])
async def handle_voicebot_webhook(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    """
    Handle Exotel Voicebot HTTP request and return the WebSocket URL.

    Exotel passes call metadata (CallSid, Direction, From, To) either as
    query params (GET) or JSON/form body (POST).  We read them all and embed
    tenant_id + call_direction into the WebSocket URL so main.py can pick
    them up without any shared state.
    """
    if request.method == "GET":
        data: dict = dict(request.query_params)
    else:
        try:
            data = await request.json()
        except Exception:
            form_data = await request.form()
            data = dict(form_data)

    call_sid    = data.get("CallSid",   data.get("call_sid",   "unknown"))
    direction   = data.get("Direction", data.get("direction",  "inbound")).lower()
    from_number = data.get("From",      data.get("CallFrom",   ""))
    to_number   = data.get("To",        data.get("CallTo",     ""))

    query_tenant = data.get("tenant_id", request.query_params.get("tenant_id", ""))
    campaign_id  = data.get("campaign_id", request.query_params.get("campaign_id", "default"))

    # ── Override direction from Redis for outbound calls ──────────────────────
    # The outbound trigger (routes/outbound.py) stores call_direction:{call_sid}
    # = "outbound" in Redis. Exotel may report "inbound" from the voicebot's
    # perspective even for calls the system initiated, so Redis is authoritative.
    try:
        from utils.db import redis_client
        if redis_client is not None:
            stored_direction = await redis_client.get(f"call_direction:{call_sid}")
            if stored_direction:
                resolved = stored_direction.decode() if isinstance(stored_direction, bytes) else stored_direction
                if resolved != direction:
                    logger.info(
                        f"[Webhook] Direction corrected via Redis: "
                        f"Exotel={direction} → actual={resolved} (call_sid={call_sid})"
                    )
                direction = resolved
    except Exception as _dir_exc:
        logger.debug(f"[Webhook] Redis direction lookup failed (non-fatal): {_dir_exc}")

    tenant_id, room_agent_id = await _dynamic_resolve_tenant(
        call_sid=call_sid,
        to_number=to_number,
        query_tenant=query_tenant,
        default=settings.DEFAULT_TENANT_ID,
    )

    logger.info(
        f"Voicebot webhook — CallSid={call_sid} Direction={direction} "
        f"From={from_number} To={to_number} "
        f"tenant={tenant_id} agent_id={room_agent_id or '-'} campaign={campaign_id}"
    )

    base_url = settings.WEBHOOK_BASE_URL.rstrip("/")
    if base_url.startswith("https://"):
        ws_base = "wss://" + base_url[len("https://"):]
    elif base_url.startswith("http://"):
        ws_base = "ws://" + base_url[len("http://"):]
    else:
        ws_base = base_url

    from urllib.parse import quote

    ws_url = (
        f"{ws_base}/media"
        f"?call_sid={quote(str(call_sid), safe='')}"
        f"&sample-rate=8000"
        f"&tenant_id={quote(str(tenant_id), safe='')}"
        f"&campaign_id={quote(str(campaign_id), safe='')}"
        f"&direction={quote(str(direction), safe='')}"
    )
    if room_agent_id:
        ws_url += f"&agent_id={quote(str(room_agent_id), safe='')}"

    logger.info(f"Returning WebSocket URL: {ws_url}")
    return {"url": ws_url}


@router.post("/exotel/status")
async def handle_call_status(request: Request):
    """Handle Exotel call-status callbacks (no action required)."""
    try:
        form_data = await request.form()
        data = dict(form_data)
        call_sid = data.get("CallSid", "")
        status   = data.get("Status", "")
        logger.info(f"Call status update — SID: {call_sid}, Status: {status}")
        return {"status": "ok"}
    except Exception as exc:
        logger.error(f"Error in status callback: {exc}")
        return {"status": "error"}


@router.get("/health")
async def webhook_health():
    """Health check endpoint for webhooks"""
    return {"status": "healthy", "service": "webhook"}
