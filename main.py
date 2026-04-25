"""
main.py — Exotel + LiveKit AI Voice System

Architecture:
  Exotel (phone) ──WS──► /media  ──► LiveKitBridge ──► LiveKit Room ◄── AI Agent
                                                                           (voice_agent.py)

Tenant wiring:
  1. Exotel hits /webhook/voicebot (webhook.py).
  2. webhook.py appends ?tenant_id=X&campaign_id=Y&direction=inbound to the WS URL.
  3. /media reads those query params and embeds them in LiveKit room metadata (JSON).
  4. voice_agent.py reads ctx.room.metadata → loads TenantConfig → builds persona + KB.
  No shared state, no database lookup on the hot path.
"""
import asyncio
import logging
import logging.handlers
import sys
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from config import get_settings
from routers import webhook, calls
from services.kb_service import clear_backend_cache
from services.livekit_bridge import LiveKitBridge
from services.livekit_service import LiveKitService, get_cached_livekit_service
from utils.transcript_writer import save_call_transcript

# ── Phase 3: Auth + User Management ──────────────────────────────────────────
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from utils.rate_limit import limiter
from routes import auth as auth_routes
from routes import users as users_routes

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Ensure logs/ exists before any FileHandler tries to open it
from pathlib import Path as _Path
_Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            "logs/exotel_ai_voice.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger.info("=" * 60)
    logger.info("Exotel + LiveKit AI Voice System starting")
    logger.info(f"  Server  : http://{settings.HOST}:{settings.PORT}")
    logger.info(f"  Webhook : {settings.exotel_webhook_url}")
    logger.info(f"  LiveKit : {settings.livekit_ws_url}")
    logger.info(f"  Tenant  : default={settings.DEFAULT_TENANT_ID}")
    logger.info("=" * 60)

    # ── Phase 3: verify DB connectivity (non-fatal; pipeline works without DB) ─
    try:
        from utils.db import mongo_client, db as _db
        if mongo_client is not None:
            await mongo_client.admin.command("ping")
            logger.info("[DB] MongoDB connected")
        else:
            logger.warning("[DB] MongoDB not configured — auth endpoints will return 503")
    except Exception as _db_exc:
        logger.warning(f"[DB] MongoDB ping failed (server will still start): {_db_exc}")

    try:
        from utils.db import redis_client as _redis
        from utils.redis_mock import MockRedis
        if _redis is not None and not isinstance(_redis, MockRedis):
            await _redis.ping()
            logger.info("[Cache] Redis connected")
        else:
            logger.warning("[Cache] Redis not configured — using in-memory mock")
    except Exception as _redis_exc:
        logger.warning(f"[Cache] Redis ping failed: {_redis_exc}")

    # ── Phase 7: Background workers (non-fatal if Redis unavailable) ─────────────
    _p7_tasks = []
    try:
        from scheduled_calls_worker import run_scheduled_calls_worker
        from scheduled_campaigns_worker import run_scheduled_campaigns_worker
        from utils.campaign_watchdog import run_campaign_watchdog
        _p7_tasks.append(asyncio.create_task(run_scheduled_calls_worker()))
        _p7_tasks.append(asyncio.create_task(run_scheduled_campaigns_worker()))
        _p7_tasks.append(asyncio.create_task(run_campaign_watchdog()))
        logger.info("[Phase 7] Campaign workers started: scheduled_calls, scheduled_campaigns, watchdog")
    except Exception as _p7_exc:
        logger.warning(f"[Phase 7] Campaign workers failed to start (non-fatal): {_p7_exc}")

    # ── Phase 9: Recording fetcher (non-fatal — skips if AWS/Exotel not configured) ──
    _p9_tasks = []
    try:
        from config import get_settings as _p9_settings
        if _p9_settings().RECORDING_FETCHER_ENABLED:
            from services.recording_fetcher import run_recording_fetcher
            _p9_tasks.append(asyncio.create_task(run_recording_fetcher()))
            logger.info("[Phase 9] Recording fetcher started")
        else:
            logger.info("[Phase 9] Recording fetcher disabled (RECORDING_FETCHER_ENABLED=False)")
    except Exception as _p9_exc:
        logger.warning(f"[Phase 9] Recording fetcher failed to start (non-fatal): {_p9_exc}")

    yield
    await clear_backend_cache()

    # ── Phase 9: Cancel recording fetcher on shutdown ──────────────────────────
    for _task in _p9_tasks:
        try:
            _task.cancel()
            await _task
        except Exception:
            pass

    # ── Phase 7: Cancel background workers on shutdown ────────────────────────
    for _task in _p7_tasks:
        try:
            _task.cancel()
            await _task
        except Exception:
            pass

    # ── Phase 3: graceful shutdown ────────────────────────────────────────────
    try:
        from utils.db import redis_client as _redis_shutdown
        from utils.redis_mock import MockRedis
        if _redis_shutdown and not isinstance(_redis_shutdown, MockRedis):
            await _redis_shutdown.aclose()
    except Exception:
        pass
    try:
        from utils.db import mongo_client as _mc_shutdown
        if _mc_shutdown:
            _mc_shutdown.close()
    except Exception:
        pass
    logger.info("Shutdown complete.")


app = FastAPI(
    title="Exotel + LiveKit AI Voice System",
    description="Phone calls via Exotel, real-time AI via LiveKit Agent",
    version="4.0.0",
    lifespan=lifespan,
)

_cors_origins = get_settings().cors_origins_list + [
    "http://localhost:5175",
    "http://127.0.0.1:5175",
    "http://localhost:5176",
    "http://127.0.0.1:5176",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(dict.fromkeys(_cors_origins)),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# ── Phase 3: SlowAPI rate limiter ─────────────────────────────────────────────
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.include_router(webhook.router)
app.include_router(calls.router)

# ── Phase 3: Auth + User Management routers ───────────────────────────────────
app.include_router(auth_routes.router)
app.include_router(users_routes.router)

# ── Phase 4: Agent Configuration routers ──────────────────────────────────────
from routes import agents as agents_routes
from routes import admin_agents as admin_agents_routes
app.include_router(agents_routes.router)
app.include_router(admin_agents_routes.router)

# ── Phase 5: Knowledge Base & RAG router ──────────────────────────────────────
from routes import knowledge_base as kb_routes
app.include_router(kb_routes.router)

# ── Phase 7: Campaign & Scheduling routers ─────────────────────────────────────
from routes import campaigns as campaigns_routes
from routes import outbound as outbound_routes
from routes import scheduled_calls as scheduled_calls_routes
app.include_router(campaigns_routes.router)
app.include_router(outbound_routes.router)
app.include_router(scheduled_calls_routes.router)

# ── Phase 8: Call records, Exotel webhooks, Concurrency ────────────────────────
# NOTE: routes/call_records.py (/call-records) is SEPARATE from PROTECTED routers/calls.py (/calls/outgoing)
from routes import call_records as call_records_routes
from routes import exotel_webhooks as exotel_webhook_routes
from routes import concurrency as concurrency_routes
app.include_router(call_records_routes.router)   # prefix: /call-records
app.include_router(exotel_webhook_routes.router)  # prefix: /api/v1/exotel/voice
app.include_router(exotel_webhook_routes.router, prefix="/webhook/exotel")  # alias for Exotel compatibility
app.include_router(concurrency_routes.router)    # prefix: /api/concurrency

# ── Phase 9: Advanced call features ────────────────────────────────────────────
from routes import phones as phones_routes
from routes import leads as leads_routes
from routes import appointments as appointments_routes
from routes import recordings as recordings_routes
from routes import audit as audit_routes
from routes import agent_chat as agent_chat_routes
app.include_router(phones_routes.router)        # prefix: /api/phones
app.include_router(leads_routes.router)         # prefix: /api/leads
app.include_router(appointments_routes.router)  # prefix: /api/appointments
app.include_router(recordings_routes.router)    # prefix: /api/recordings
app.include_router(audit_routes.router)         # prefix: /api/audit-logs
app.include_router(agent_chat_routes.router)    # prefix: /api/v1/agents

# ── Phase 10: DIY Agent & Demo System ─────────────────────────────────────────
from routes import diy_agent as diy_agent_routes
from routes import diy_admin as diy_admin_routes
from routes import demo as demo_routes
app.include_router(diy_agent_routes.router)   # prefix: /api/v1/agent/diy
app.include_router(diy_admin_routes.router)   # prefix: /api/v1/admin/diy
app.include_router(demo_routes.router)        # prefix: /api/demo

@app.websocket("/media")
async def exotel_media_websocket(websocket: WebSocket):
    """
    WebSocket endpoint for Exotel Voicebot audio streaming.

    Query params (set by webhook.py when using webhook-first flow):
      call_sid    — Exotel call identifier
      tenant_id   — Agent scope (user_id, tenant_id, or agent _id)
      campaign_id — Campaign identifier (default: "default")
      direction   — "inbound" | "outbound"

    When Exotel connects DIRECTLY (no webhook-first flow), query params are
    absent.  In that case we buffer the first Exotel messages, extract the
    real callSid from the {"event":"start"} frame, then resolve tenant +
    direction from Redis before creating the LiveKit room.
    """
    await websocket.accept()

    import json as _json

    qp = websocket.query_params
    call_sid       = (qp.get("call_sid") or qp.get("callSid") or qp.get("CallSid") or "").strip()
    tenant_id      = (qp.get("tenant_id") or "").strip()
    meta_agent_id  = (qp.get("agent_id") or "").strip()
    campaign_id    = qp.get("campaign_id", "default")
    call_direction = qp.get("direction", "inbound").lower()

    # ── Extract callSid from Exotel start message when not in URL ────────────
    # Exotel sends two frames on connect:
    #   {"event": "connected"}
    #   {"event": "start", "start": {"callSid": "...", "streamSid": "..."}}
    # Buffer these so we can replay them through the bridge after setup.
    buffered_messages: list[str] = []

    if not call_sid:
        try:
            for _ in range(10):
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
                buffered_messages.append(raw)
                try:
                    msg = _json.loads(raw)
                except Exception:
                    continue
                if msg.get("event") == "start":
                    sd = msg.get("start") or {}
                    exotel_sid = (
                        sd.get("callSid") or sd.get("call_sid") or
                        sd.get("CallSid") or ""
                    ).strip()
                    if exotel_sid:
                        call_sid = exotel_sid
                        logger.info(f"[Media] callSid from Exotel start event: {call_sid}")
                    break
        except asyncio.TimeoutError:
            logger.warning("[Media] Timeout waiting for Exotel start event — using UUID fallback")
        except Exception as exc:
            logger.warning(f"[Media] Failed to read Exotel start event: {exc}")

    # ── Resolve tenant + direction from Redis using the real callSid ─────────
    # This correctly handles:
    #   - Outbound calls: _trigger_single_call() stores call_tenant:{sid} + call_direction:{sid}
    #   - Inbound calls: webhook.py stores nothing; we fall through to phone-DB lookup below
    if call_sid and (not tenant_id or call_direction == "inbound"):
        try:
            from utils.db import redis_client as _rc
            if _rc is not None:
                _stored_tenant = await _rc.get(f"call_tenant:{call_sid}")
                if _stored_tenant:
                    resolved_tenant = (
                        _stored_tenant.decode() if isinstance(_stored_tenant, bytes) else _stored_tenant
                    ).strip()
                    if resolved_tenant and not tenant_id:
                        tenant_id = resolved_tenant
                        logger.info(f"[Media] tenant from Redis: {tenant_id}")
                    # Outbound trigger stores agents._id here — pass through so the voice worker
                    # pins the same dashboard agent even if tenant_id was also user scope.
                    if resolved_tenant and not meta_agent_id:
                        meta_agent_id = resolved_tenant
                        logger.info(f"[Media] agent_id from Redis: {meta_agent_id}")

                _stored_dir = await _rc.get(f"call_direction:{call_sid}")
                if _stored_dir:
                    resolved_dir = (
                        _stored_dir.decode() if isinstance(_stored_dir, bytes) else _stored_dir
                    ).strip().lower()
                    if resolved_dir:
                        call_direction = resolved_dir
                        logger.info(f"[Media] direction from Redis: {call_direction}")
        except Exception as _re:
            logger.warning(f"[Media] Redis lookup failed (non-fatal): {_re}")

    # ── For inbound calls: resolve tenant from phone number via DB ───────────
    # When Exotel connects directly (no webhook), there is no Redis key.
    # Use the called virtual number (embedded in the Exotel start message) to
    # look up the phone doc and derive the correct tenant scope.
    if not tenant_id and buffered_messages:
        try:
            for raw in buffered_messages:
                try:
                    msg = _json.loads(raw)
                except Exception:
                    continue
                if msg.get("event") == "start":
                    sd = msg.get("start") or {}
                    to_number = (
                        sd.get("to") or sd.get("To") or
                        sd.get("customParameters", {}).get("to") or ""
                    ).strip()
                    if to_number:
                        from utils.db import get_phone_by_number, get_agent_for_phone, agent_owner_scope
                        phone_doc = await get_phone_by_number(to_number)
                        if phone_doc:
                            agent_doc = await get_agent_for_phone(phone_doc)
                            if agent_doc:
                                scope = agent_owner_scope(agent_doc)
                                if scope:
                                    tenant_id = scope
                                    logger.info(f"[Media] tenant from start.to DB lookup: {tenant_id}")
                                if not meta_agent_id and agent_doc.get("_id"):
                                    meta_agent_id = str(agent_doc["_id"])
                                    logger.info(f"[Media] agent_id from phone assignment: {meta_agent_id}")
                    break
        except Exception as _de:
            logger.warning(f"[Media] DB phone lookup from start message failed: {_de}")

    # ── Final fallbacks ───────────────────────────────────────────────────────
    if not call_sid:
        call_sid = uuid.uuid4().hex[:12]
    if not tenant_id:
        tenant_id = get_settings().DEFAULT_TENANT_ID

    logger.info(
        f"WS accepted — call={call_sid} tenant={tenant_id} "
        f"campaign={campaign_id} direction={call_direction}"
    )

    settings   = get_settings()
    room_name  = f"call_{call_sid}"

    room_metadata = {
        "tenant_id":      tenant_id,
        "campaign_id":    campaign_id,
        "call_sid":       call_sid,
        "call_direction": call_direction,
    }
    if meta_agent_id:
        room_metadata["agent_id"] = meta_agent_id

    bridge = LiveKitBridge(
        livekit_url=settings.livekit_ws_url,
        api_key=settings.LIVEKIT_API_KEY,
        api_secret=settings.LIVEKIT_API_SECRET,
    )

    sender_task = None
    transcript_messages: list[dict] = []
    # Track all transcript tasks so we can await them on shutdown instead of
    # letting Python destroy them mid-execution (causes "Task destroyed but pending" warning
    # and drops the last few words of every transcript).
    _transcript_tasks: list[asyncio.Task] = []

    try:
        livekit_svc = LiveKitService(
            api_key=settings.LIVEKIT_API_KEY,
            api_secret=settings.LIVEKIT_API_SECRET,
            ws_url=settings.livekit_ws_url,
        )
        await livekit_svc.create_room(
            room_name,
            empty_timeout=120,
            metadata=room_metadata,
        )
        await livekit_svc.close()

        await bridge.start(room_name)
        logger.info(f"Bridge active: Exotel WS ↔ LiveKit room {room_name}")

        # ── Transcript capture ────────────────────────────────────────────────
        async def _on_transcript_stream(reader, participant_identity: str) -> None:
            """
            Consume one lk.transcription stream and append cleaned entries to
            the shared transcript_messages list.

            Two problems this solves:

            PROBLEM A — Deepgram partials for user turns:
              lk.transcription chunks do NOT have an is_final attribute at the
              stream level. Instead, Deepgram partials arrive as shorter strings
              that are strict prefixes of the final ("Can you tell me the" before
              "Can you tell me the name of your company?"). We detect partials by
              checking whether the last saved user entry is a prefix of the new
              text and replacing it in-place rather than appending.

            PROBLEM B — Word-by-word agent tokens:
              The agent TTS stream emits one entry per spoken word ("I", "can",
              "help", "you"...). We buffer consecutive same-role chunks and flush
              the joined sentence when the stream closes or the role switches.
              A 300ms inactivity timer also triggers a flush so long agent turns
              are saved progressively rather than only at stream end.
            """
            _buf: list[str] = []        # word buffer for the current role
            _buf_role: str = ""         # role the buffer belongs to
            _flush_handle = None        # delayed-flush timer handle

            def _flush_buffer():
                nonlocal _buf, _buf_role, _flush_handle
                if _flush_handle is not None:
                    _flush_handle.cancel()
                    _flush_handle = None
                if not _buf or not _buf_role:
                    return
                sentence = " ".join(_buf).strip()
                _buf = []
                if not sentence:
                    return

                # For user role: check if this is an updated/final version of
                # the last saved user entry (Deepgram sends growing partials).
                # If the last saved user entry is a prefix of the new text,
                # replace it rather than adding a duplicate.
                if (
                    _buf_role == "user"
                    and transcript_messages
                    and transcript_messages[-1]["role"] == "user"
                    and sentence.startswith(transcript_messages[-1]["content"])
                    and sentence != transcript_messages[-1]["content"]
                ):
                    transcript_messages[-1]["content"] = sentence
                    logger.info(f"[Transcript] user (updated): {sentence[:80]}")
                    return

                # Normal cross-task dedup: skip exact consecutive same-role repeat
                if (
                    transcript_messages
                    and transcript_messages[-1]["role"] == _buf_role
                    and transcript_messages[-1]["content"] == sentence
                ):
                    return

                transcript_messages.append({"role": _buf_role, "content": sentence})
                logger.info(f"[Transcript] {_buf_role}: {sentence[:80]}")

            try:
                loop = asyncio.get_event_loop()
                async for chunk in reader:
                    text = chunk.text if hasattr(chunk, "text") else str(chunk)
                    text = text.strip()
                    if not text:
                        continue
                    role = "agent" if "agent" in participant_identity.lower() else "user"

                    # If role switches, flush the previous buffer first
                    if _buf and role != _buf_role:
                        _flush_buffer()

                    _buf_role = role
                    _buf.append(text)

                    # Agent TTS tokens arrive word-by-word with natural pauses
                    # at clause/phrase boundaries — use a 1.5s window so full
                    # sentences like "I'm here to assist you with inquiries
                    # related to the Delta MS300 Series" buffer completely before
                    # flushing rather than splitting at every syntactic pause.
                    # User turns (Deepgram finals) arrive as complete utterances
                    # so a short 400ms window is fine to avoid holding them back.
                    flush_delay = 1.5 if role == "agent" else 0.4
                    if _flush_handle is not None:
                        _flush_handle.cancel()
                    _flush_handle = loop.call_later(flush_delay, _flush_buffer)

            except asyncio.CancelledError:
                pass  # Normal on session shutdown
            except Exception as _te:
                logger.warning(f"[Transcript] Stream read error: {_te}")
            finally:
                # Always flush remaining buffer when the stream closes
                _flush_buffer()

        # bridge._room is the rtc.Room connected during start(); access is
        # intentional — livekit_bridge.py is protected so no public property exists.
        if bridge._room is not None:
            def _make_transcript_handler():
                def _handler(reader, participant_info):
                    identity = (
                        participant_info.identity
                        if hasattr(participant_info, "identity")
                        else str(participant_info)
                    )
                    task = asyncio.create_task(_on_transcript_stream(reader, identity))
                    _transcript_tasks.append(task)
                    # Clean up completed tasks to avoid the list growing unbounded
                    # on long calls with many transcript chunks
                    _transcript_tasks[:] = [t for t in _transcript_tasks if not t.done()]
                return _handler

            bridge._room.register_text_stream_handler(
                "lk.transcription",
                _make_transcript_handler(),
            )

            # Register a silent no-op handler for lk.agent.events.
            # LiveKit Agents 1.x publishes agent state changes (thinking/speaking/
            # listening) on this topic. Without a handler, the SDK logs
            # "ignoring text stream with topic 'lk.agent.events'" for every single
            # state change — 50+ log lines per call that bury real signals.
            # We intentionally discard these events here because the bridge only
            # needs audio; agent state events are for frontend UI clients.
            async def _drain_stream(reader):
                try:
                    async for _ in reader:
                        pass  # intentionally discard
                except Exception:
                    pass

            def _noop_agent_events_handler(reader, participant_info):
                asyncio.create_task(_drain_stream(reader))

            bridge._room.register_text_stream_handler(
                "lk.agent.events",
                _noop_agent_events_handler,
            )

            logger.info(f"[Transcript] Handler registered for room {room_name}")
        # ─────────────────────────────────────────────────────────────────────

        # Replay buffered messages (connected + start) through the bridge
        for _buffered_msg in buffered_messages:
            await bridge.handle_exotel_message(_buffered_msg)

        sender_task = asyncio.create_task(bridge.drain_queue_to_ws(websocket))

        while bridge.running:
            message = await websocket.receive_text()
            await bridge.handle_exotel_message(message)

    except WebSocketDisconnect:
        logger.info(f"WS disconnected — call={call_sid}")
    except Exception as exc:
        logger.error(f"WS error — call={call_sid}: {exc}", exc_info=True)
    finally:
        await bridge.stop()

        if sender_task and not sender_task.done():
            sender_task.cancel()
            try:
                await sender_task
            except asyncio.CancelledError:
                pass

        # Drain any in-flight transcript stream tasks so the final words of the
        # conversation are captured before we write the transcript to DB.
        # Give them a short grace window — if they are still reading after 2s,
        # cancel them (the session is already gone at this point).
        if _transcript_tasks:
            pending = [t for t in _transcript_tasks if not t.done()]
            if pending:
                logger.debug(f"[Transcript] Waiting for {len(pending)} stream task(s) to finish...")
                try:
                    await asyncio.wait(pending, timeout=2.0)
                except Exception:
                    pass
                # Cancel anything still running after the grace window
                for t in pending:
                    if not t.done():
                        t.cancel()

        try:
            livekit_cleanup = LiveKitService(
                api_key=settings.LIVEKIT_API_KEY,
                api_secret=settings.LIVEKIT_API_SECRET,
                ws_url=settings.livekit_ws_url,
            )
            await livekit_cleanup.delete_room(room_name)
            await livekit_cleanup.close()
        except Exception:
            pass

        if transcript_messages:
            try:
                await save_call_transcript(call_sid, transcript_messages)
                logger.info(
                    f"[Transcript] Saved {len(transcript_messages)} messages for call {call_sid}"
                )
            except Exception as _tse:
                logger.warning(f"[Transcript] Failed to save transcript: {_tse}")

        logger.info(f"Session ended — call={call_sid} tenant={tenant_id}")


@app.get("/voice-chat")
async def voice_chat_page():
    """Browser-based voice chat page with tenant selection."""
    settings = get_settings()
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>AI Voice Chat — LiveKit</title>
    <style>
        body {{ font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; text-align: center; }}
        button {{ padding: 15px 40px; font-size: 18px; border: none; border-radius: 10px; cursor: pointer; margin: 10px; }}
        #startBtn {{ background: #28a745; color: white; }}
        #stopBtn  {{ background: #dc3545; color: white; display: none; }}
        #status {{ margin: 20px 0; padding: 15px; border-radius: 5px; font-size: 16px; }}
        .connected {{ background: #d4edda; color: #155724; }}
        .disconnected {{ background: #f8d7da; color: #721c24; }}
        .connecting {{ background: #fff3cd; color: #856404; }}
        #log {{ text-align: left; background: #f8f9fa; padding: 15px; border-radius: 5px;
                max-height: 300px; overflow-y: auto; font-size: 13px; font-family: monospace; margin-top: 20px; }}
        label {{ font-weight: bold; }}
        select, input {{ padding: 6px 10px; font-size: 14px; border-radius: 5px; border: 1px solid #ccc; margin: 4px; }}
    </style>
</head>
<body>
    <h1>AI Voice Chat</h1>

    <div style="margin-bottom: 16px;">
        <label>Tenant: </label>
        <input id="tenantInput" type="text" value="{settings.DEFAULT_TENANT_ID}" style="width: 180px;" />
        <label style="margin-left: 12px;">Direction: </label>
        <select id="directionSelect">
            <option value="inbound">Inbound</option>
            <option value="outbound">Outbound</option>
        </select>
    </div>

    <div id="status" class="disconnected">Click Start to begin</div>
    <button id="startBtn" onclick="startChat()">Start Voice Chat</button>
    <button id="stopBtn"  onclick="stopChat()">Stop</button>
    <div id="log"></div>

    <script src="https://cdn.jsdelivr.net/npm/livekit-client@2.18.1/dist/livekit-client.umd.js"></script>
    <script>
        let room = null;
        function log(msg) {{
            const d = document.getElementById('log');
            d.innerHTML += new Date().toLocaleTimeString() + ' — ' + msg + '<br>';
            d.scrollTop = d.scrollHeight;
        }}
        function setStatus(msg, cls) {{
            const el = document.getElementById('status');
            el.textContent = msg; el.className = cls;
        }}
        async function startChat() {{
            const tenantId  = document.getElementById('tenantInput').value.trim() || '{settings.DEFAULT_TENANT_ID}';
            const direction = document.getElementById('directionSelect').value;
            setStatus('Connecting...', 'connecting');
            log('Requesting token for tenant=' + tenantId + ' direction=' + direction);
            try {{
                const t0 = performance.now();
                const resp = await fetch('/api/voice-chat/token?tenant_id=' + tenantId + '&direction=' + direction);
                const data = await resp.json();
                if (!data.success) {{ setStatus('Error: ' + data.error, 'disconnected'); return; }}
                log('Token OK. Room: ' + data.room_name + ' (+' + (performance.now() - t0).toFixed(0) + 'ms token API)');
                // Voice-only app: no adaptiveStream/dynacast (those target video simulcast).
                // Audio: WebRTC constraints — EC/NS on, AGC off in noisy rooms.
                room = new LivekitClient.Room({{
                    audioCaptureDefaults: {{
                        echoCancellation: true,
                        noiseSuppression: true,
                        autoGainControl: false,
                    }},
                }});
                room.on('connectionQualityChanged', (quality, participant) => {{
                    log('Connection quality: ' + quality + ' (' + (participant && participant.identity) + ')');
                }});
                room.on('trackSubscribed', (track, pub, participant) => {{
                    if (track.kind !== 'audio') return;
                    log('Agent audio: ' + participant.identity);
                    document.body.appendChild(track.attach());
                }});
                room.on('disconnected', () => {{
                    setStatus('Disconnected', 'disconnected');
                    document.getElementById('startBtn').style.display = '';
                    document.getElementById('stopBtn').style.display = 'none';
                }});
                const tConnect = performance.now();
                // Pre-warm DNS/TLS + WebSocket path; with LiveKit Cloud + token, edge selection.
                if (typeof room.prepareConnection === 'function') {{
                    await room.prepareConnection(data.livekit_url, data.token);
                    log('prepareConnection +' + (performance.now() - tConnect).toFixed(0) + 'ms');
                }}
                // Tight handshake: peerConnectionTimeout caps wait for ICE (raise if flaky networks fail).
                await room.connect(data.livekit_url, data.token, {{
                    autoSubscribe: true,
                    peerConnectionTimeout: 12000,
                    websocketTimeout: 10000,
                    rtcConfig: {{ iceCandidatePoolSize: 10 }},
                }});
                log('room.connect +' + (performance.now() - tConnect).toFixed(0) + 'ms (total to connected)');
                await room.localParticipant.setMicrophoneEnabled(true, {{
                    echoCancellation: true,
                    noiseSuppression: true,
                    autoGainControl: false,
                }});
                log('Microphone enabled (EC+NS on, AGC off) — speak now!');
                setStatus('Connected to ' + tenantId + ' (' + direction + ')', 'connected');
                document.getElementById('startBtn').style.display = 'none';
                document.getElementById('stopBtn').style.display = '';
            }} catch (e) {{
                setStatus('Error: ' + e.message, 'disconnected');
                log('Error: ' + e.message);
            }}
        }}
        async function stopChat() {{
            if (room) {{ await room.disconnect(); room = null; }}
            setStatus('Disconnected', 'disconnected');
            document.getElementById('startBtn').style.display = '';
            document.getElementById('stopBtn').style.display = 'none';
        }}
    </script>
</body>
</html>"""
    return HTMLResponse(content=html)


@app.get("/api/voice-chat/token")
async def get_voice_chat_token(
    tenant_id: str = "",
    direction: str = "inbound",
):
    """Generate a LiveKit token with tenant metadata for browser voice chat."""
    settings = get_settings()
    resolved_tenant = tenant_id.strip() or settings.DEFAULT_TENANT_ID
    room_name = f"voice_chat_{uuid.uuid4().hex[:8]}"
    identity  = f"user_{uuid.uuid4().hex[:6]}"

    try:
        livekit_svc = get_cached_livekit_service(
            api_key=settings.LIVEKIT_API_KEY,
            api_secret=settings.LIVEKIT_API_SECRET,
            ws_url=settings.livekit_ws_url,
        )
        await livekit_svc.create_room(
            room_name,
            empty_timeout=120,
            metadata={
                "tenant_id":      resolved_tenant,
                "campaign_id":    "default",
                "call_direction": direction.lower(),
            },
        )
        token = livekit_svc.generate_token(room_name, identity)

        return {
            "success":     True,
            "token":       token,
            "room_name":   room_name,
            "identity":    identity,
            "livekit_url": settings.livekit_ws_url,
            "tenant_id":   resolved_tenant,
        }
    except Exception as exc:
        logger.error(f"Error creating voice chat: {exc}")
        return {"success": False, "error": str(exc)}


@app.get("/")
async def root():
    return {
        "message": "Exotel + LiveKit AI Voice System",
        "version": "4.0.0",
        "status":  "running",
        "endpoints": {
            "voice_chat":    "/voice-chat",
            "health":        "/health",
            "outgoing_call": "/calls/test-outgoing",
        },
    }


@app.get("/health")
async def health_check():
    return {
        "status":  "healthy",
        "service": "exotel-livekit-voice",
        "components": {
            "exotel_webhook": "ready",
            "livekit_bridge": "ready",
            "livekit_agent":  "check run_livekit_agent.py",
        },
    }


if __name__ == "__main__":
    import uvicorn
    s = get_settings()
    uvicorn.run("main:app", host=s.HOST, port=s.PORT, reload=s.DEBUG, log_level="info")
