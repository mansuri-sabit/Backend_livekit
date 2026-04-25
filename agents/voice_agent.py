"""
agents/voice_agent.py

LiveKit worker entrypoint.  Called once per room by the agent worker process.

What changed from the original:
  - Reads tenant_id, campaign_id, call_direction from ctx.room.metadata (JSON).
    When present on JobAssignment (ctx.job.room.metadata), tenant config is prefetched
    in parallel with VAD warmup + ctx.connect() to hide Mongo lookup latency.
  - Loads TenantConfig from YAML or MongoDB via get_tenant_config().
  - Builds system prompt via build_system_prompt() — Jinja2 template rendered.
  - Applies direction-specific prompt/greeting overrides from config.inboundConfig
    / config.outboundConfig (set in the dashboard "Call Directions" tab).
  - STT/TTS are always Sarvam (language/voice from tenant config / dashboard).
  - Creates a TenantAgent that injects MongoDB KB context on every LLM call.
  - Passes call_direction to TenantAgent so KB search filters direction-specific chunks.
  - Registers AgentSession.on('user_input_transcribed') for parallel RAG prefetch
    (starts retrieval on final STT before llm_node).
  - TenantAgent.llm_node uses non-blocking KB: the LLM stream starts without waiting
    on a cold Mongo search (see KB_PREFETCH_GRACE_SEC in kb_hooks).
"""
import asyncio
import json
import logging
import os
from typing import Any

from livekit.agents import AutoSubscribe, JobContext, AgentSession, UserInputTranscribedEvent
from livekit.plugins.openai import llm as openai_llm
from livekit.plugins.silero import VAD

from plugins.sarvam_tts import TTS as SarvamTTS
from plugins.sarvam_stt import STT as SarvamSTT
from agents.kb_hooks import TenantAgent
from agents.tenant_config import get_tenant_config, TenantConfig, warmup_mongo_sync
from agents.prompt_builder import build_system_prompt, build_greeting

logger = logging.getLogger(__name__)


async def _preload_worker_db_async() -> None:
    """
    Import utils.db off the event loop (Motor TLS + optional Redis init).

    Transcript/summary writes use sync pymongo (`calls_set_fields_sync`) to avoid
    Motor event-loop issues; this preload remains useful if other worker code
    touches `utils.db` or Redis during the call.
    """
    try:
        await asyncio.to_thread(lambda: __import__("utils.db"))
        try:
            from utils.db import db as _wdb
            if _wdb is not None:
                await _wdb.command("ping")
                logger.info("[DB] Worker Mongo ping OK (import off event loop)")
        except Exception as _pe:
            logger.debug(f"[DB] Mongo ping after preload skipped: {_pe}")
    except Exception as _ie:
        logger.debug(f"[DB] utils.db preload skipped: {_ie}")


def _metadata_from_job_assignment(ctx: JobContext) -> dict:
    """
    Parse room JSON metadata from the agent Job (available before ctx.connect()).

    LiveKit sends room metadata on the job assignment so we can start get_tenant_config
    while the WebRTC session is still connecting — same fields as create_room(metadata=...).
    """
    try:
        job = ctx.job
        if job is None:
            return {}
        raw = ""
        room = getattr(job, "room", None)
        if room is not None:
            raw = (getattr(room, "metadata", None) or "").strip()
        if not raw:
            raw = (getattr(job, "metadata", None) or "").strip()
        if raw:
            return json.loads(raw)
    except Exception as exc:
        logger.debug(f"[Voice] Early job metadata parse skipped: {exc}")
    return {}


def _voice_llm_temperature(cfg: TenantConfig) -> float:
    """
    Resolve LLM temperature for the voice worker.

    - VOICE_LLM_TEMPERATURE: if set, overrides dashboard/Mongo (e.g. 0.1 for support/RAG).
    - VOICE_LLM_TEMPERATURE_MAX: if set, caps dashboard value (e.g. 0.15) without a fixed override.
    """
    raw_override = os.environ.get("VOICE_LLM_TEMPERATURE")
    if raw_override is not None and str(raw_override).strip() != "":
        return max(0.0, min(2.0, float(raw_override)))
    t = float(cfg.llm.temperature)
    cap_raw = os.environ.get("VOICE_LLM_TEMPERATURE_MAX")
    if cap_raw is not None and str(cap_raw).strip() != "":
        t = min(t, float(cap_raw))
    return t

# Suppress noisy pymongo DEBUG logs in the LiveKit worker subprocess.
# cli.run_app() spawns a new process that doesn't inherit run_livekit_agent.py's config.
for _pymongo_logger_name in ("pymongo", "pymongo.topology", "pymongo.connection",
                              "pymongo.command", "pymongo.serverSelection"):
    logging.getLogger(_pymongo_logger_name).setLevel(logging.WARNING)


def _build_tts(cfg: TenantConfig, sarvam_api_key: str) -> Any:
    """Sarvam TTS only; language/voice come from tenant config (dashboard)."""
    logger.info(f"[TTS] Using Sarvam TTS language={cfg.voice.tts_language} voice={cfg.voice.tts_voice}")
    return SarvamTTS(
        api_key=sarvam_api_key,
        language=cfg.voice.tts_language,
        voice=cfg.voice.tts_voice,
    )


def _build_stt(cfg: TenantConfig, sarvam_api_key: str) -> Any:
    """Sarvam STT only; language from tenant config (dashboard)."""
    logger.info(f"[STT] Using Sarvam STT language={cfg.voice.stt_language}")
    return SarvamSTT(
        api_key=sarvam_api_key,
        language=cfg.voice.stt_language,
    )


async def _save_post_call_data(call_sid: str, agent: "TenantAgent", settings: Any) -> None:
    transcript: list[dict] = getattr(agent, "_transcript", [])
    if not transcript:
        return

    summary = ""
    if settings.OPENAI_API_KEY:
        try:
            from openai import AsyncOpenAI
            client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
            conv_text = "\n".join(f"{t['role'].upper()}: {t['content']}" for t in transcript)
            resp = await asyncio.wait_for(
                client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": (
                            "You are summarizing a phone call transcript. "
                            "Write a concise 2-3 sentence summary covering what was discussed "
                            "and the outcome. Use plain language, no markdown or lists."
                        )},
                        {"role": "user", "content": conv_text},
                    ],
                    max_tokens=200,
                    temperature=0.3,
                ),
                timeout=15.0,
            )
            summary = resp.choices[0].message.content.strip()
            logger.info(f"[PostCall] Summary generated for call={call_sid}: {summary[:80]}")
        except Exception as exc:
            logger.warning(f"[PostCall] Summary generation failed for call={call_sid}: {exc}")

    if summary:
        try:
            from agents.tenant_config import calls_set_fields_sync

            ok = await asyncio.to_thread(
                calls_set_fields_sync,
                call_sid,
                {"summary": summary},
            )
            if ok:
                logger.info(f"[PostCall] Summary saved for call={call_sid}")
        except Exception as exc:
            logger.error(f"[PostCall] DB save failed for call={call_sid}: {exc}")


async def entrypoint(ctx: JobContext) -> None:
    """
    LiveKit worker entrypoint — called when the agent is dispatched to a room.

    Works for:
      - Exotel phone callers (bridged via services/livekit_bridge.py)
      - Browser WebRTC users (via /voice-chat page)
    """
    logger.info(f"Agent joining room: {ctx.room.name}")

    # ── KB / tenant config: prefetch as soon as Job exposes room metadata ─────
    # Same JSON as create_room(metadata=...) — overlaps Mongo find with connect+VAD.
    early_meta = _metadata_from_job_assignment(ctx)
    prefetch_task: asyncio.Task | None = None
    early_config_key = ""
    early_campaign_id = "default"
    if early_meta.get("tenant_id") or early_meta.get("agent_id"):
        _e_tid = early_meta.get("tenant_id", "default")
        early_campaign_id = early_meta.get("campaign_id", "default")
        _e_agent = (early_meta.get("agent_id") or "").strip()
        early_config_key = _e_agent or str(_e_tid).strip() or "default"
        prefetch_task = asyncio.create_task(
            asyncio.to_thread(get_tenant_config, early_config_key, early_campaign_id)
        )
        logger.debug(
            f"[Voice] Tenant config prefetch started (key={early_config_key}, "
            f"campaign={early_campaign_id})"
        )

    # ── Silero VAD cold-start warmup ──────────────────────────────────────────
    # Silero loads its ONNX model on first inference. Without warming up,
    # the first call logs "inference is slower than realtime" with a 3s delay.
    #
    # IMPORTANT: do NOT use asyncio.run() inside the thread executor.
    # asyncio.run() creates a NEW event loop in the thread. If Motor (async
    # MongoDB driver) is first imported in that secondary loop's context, its
    # internal Futures get bound to the wrong loop, causing:
    #   "Future attached to a different loop"
    # in _persist_transcript on every subsequent call.
    #
    # The fix: call VAD.load() synchronously in a thread — it's a pure
    # synchronous ONNX load. Skip the stream.push_frame() warmup and instead
    # just force the model load, which is the expensive part. The first VAD
    # inference on live audio will be fast because the model is already loaded.
    #
    # Run Mongo client + ping in parallel with VAD so TLS/DNS to Atlas overlaps
    # ONNX load (saves serial latency vs doing DB only after VAD).
    try:
        import concurrent.futures as _cf
        from livekit.plugins.silero import VAD as _SileroVAD

        def _run_vad_warmup():
            _SileroVAD.load()

        loop = asyncio.get_event_loop()
        with _cf.ThreadPoolExecutor(max_workers=2) as _pool:
            _, mongo_ok = await asyncio.gather(
                loop.run_in_executor(_pool, _run_vad_warmup),
                loop.run_in_executor(_pool, warmup_mongo_sync),
            )
        logger.info(
            "[VAD] Silero pre-warmed in worker subprocess; "
            f"Mongo warmup={'ok' if mongo_ok else 'skipped'}"
        )
    except Exception as _ve:
        logger.debug(f"[VAD] Pre-warm skipped (non-fatal): {_ve}")

    try:
        await ctx.connect(auto_subscribe=AutoSubscribe.AUDIO_ONLY)
        logger.info(f"Agent connected: {ctx.room.name}")
    except Exception as exc:
        logger.error(f"Failed to connect to room {ctx.room.name}: {exc}", exc_info=True)
        return

    # ── Read tenant context from room metadata ────────────────────────────────
    meta: dict = {}
    if ctx.room.metadata:
        try:
            meta = json.loads(ctx.room.metadata)
        except Exception as exc:
            logger.warning(
                f"Could not parse room metadata for {ctx.room.name}: {exc}. "
                "Using defaults."
            )

    tenant_id      = meta.get("tenant_id",      "default")
    campaign_id    = meta.get("campaign_id",     "default")
    call_direction = meta.get("call_direction",  "inbound")
    call_sid = meta.get("call_sid", "")
    # Prefer explicit MongoDB agents._id from phone assignment / webhook so we load
    # the correct dashboard agent (e.g. "Support Bot") even when tenant_id is only user scope.
    agent_pk = (meta.get("agent_id") or "").strip()
    config_key = agent_pk or str(tenant_id).strip() or "default"

    logger.info(
        f"Room metadata — tenant={tenant_id} campaign={campaign_id} "
        f"direction={call_direction} agent_id={agent_pk or '-'} "
        f"config_lookup={config_key}"
    )

    # ── Load tenant config (reuse prefetch if keys match post-connect metadata) ─
    if (
        prefetch_task is not None
        and config_key == early_config_key
        and campaign_id == early_campaign_id
    ):
        try:
            cfg = await prefetch_task
            logger.info("[TenantConfig] Used job-assignment prefetch (overlapped with connect)")
        except Exception as exc:
            logger.warning(f"[TenantConfig] Prefetch failed, reloading: {exc}")
            cfg = get_tenant_config(config_key, campaign_id)
    else:
        if prefetch_task is not None:
            prefetch_task.cancel()
            try:
                await prefetch_task
            except (asyncio.CancelledError, Exception):
                pass
        cfg = get_tenant_config(config_key, campaign_id)

    # Apply direction-specific prompt override when configured in the dashboard
    # "Call Directions" tab (config.inboundConfig.prompt / config.outboundConfig.prompt)
    if call_direction == "inbound" and getattr(cfg.prompts, "inbound_extra_context", ""):
        cfg.prompts.extra_role_context = cfg.prompts.inbound_extra_context
        logger.debug(f"[Voice] Applied inbound prompt override for tenant={tenant_id}")
    elif call_direction == "outbound" and getattr(cfg.prompts, "outbound_extra_context", ""):
        cfg.prompts.extra_role_context = cfg.prompts.outbound_extra_context
        logger.debug(f"[Voice] Applied outbound prompt override for tenant={tenant_id}")

    # KB chunk load is CPU/IO heavy; start as early as possible so it overlaps
    # prompt build, API key resolution, VAD/STT/TTS construction, and DB preload.
    async def _warm_kb_chunks_bg():
        """
        Pre-load KB chunk embeddings into _CHUNK_CACHE before the user speaks.

        WHY THIS EXISTS:
        Each call runs in a fresh worker subprocess — _CHUNK_CACHE starts empty.
        The first KB search of every call triggers _load_chunks_into_cache()
        which fetches all chunk documents (text + 1536-dim embeddings) from
        Atlas via a synchronous pymongo cursor in run_in_executor().
        For 21 chunks this takes 3-4 seconds. For 76 chunks it takes 5-8 seconds.
        This delay hits every single call on every single turn 1.

        FIX:
        Start the chunk load immediately after ctx.connect() as a background task.
        The greeting synthesis takes ~2-3 seconds and the AEC warmup adds 3 more.
        By the time the caller finishes their first utterance (~8-10s total),
        the cache is warm and KB searches return in <200ms.

        IMPLEMENTATION:
        We call _cosine_fallback_search with a dummy zero-vector query.
        This triggers _load_chunks_into_cache() which populates _CHUNK_CACHE.
        The dummy search returns no results (scores all zero) but the side-effect
        of loading the cache is exactly what we need.
        """
        try:
            from agents.tenant_config import _PYMONGO_CLIENT, _PYMONGO_DB_NAME
            if _PYMONGO_CLIENT is None or not _PYMONGO_DB_NAME:
                return

            from services.kb_backends import mongo_backend
            from services.kb_backends.mongo_backend import MongoKBBackend
            from utils.rag import EMBEDDING_DIM

            _kb_agent = (cfg.kb.agent_id or config_key or tenant_id).strip()
            _warmup_backend = MongoKBBackend(
                agent_id=_kb_agent,
                use_direction_specific=False,
            )
            # Dummy zero-vector triggers cache load, returns no results (that's fine)
            dummy_emb = [0.0] * EMBEDDING_DIM
            await _warmup_backend._cosine_fallback_search(
                query_emb=dummy_emb,
                k=1,
                allowed_dirs=None,
                min_score=1.1,  # Impossible threshold — no results, but cache loads
                client=_PYMONGO_CLIENT,
                db_name=_PYMONGO_DB_NAME,
            )
            n_chunks = len(mongo_backend._CHUNK_CACHE.get(_kb_agent, []))
            logger.info(f"[KB] Chunk cache pre-warmed for agent={_kb_agent} ({n_chunks} chunks)")
        except Exception as _kb_ve:
            logger.debug(f"[KB] Chunk cache pre-warm skipped (non-fatal): {_kb_ve}")

    asyncio.get_running_loop().create_task(_warm_kb_chunks_bg())

    system_prompt = build_system_prompt(cfg)
    contact_name  = meta.get("contact_name", "")
    greeting      = build_greeting(cfg, call_direction=call_direction, contact_name=contact_name)

    logger.info(
        f"[Voice] Greeting text: \"{greeting}\" (direction={call_direction}, tenant={tenant_id})\n"
        f"  inbound_greeting={cfg.greetings.inbound!r}\n"
        f"  outbound_greeting={cfg.greetings.outbound!r}\n"
        f"  company={cfg.company_name} agent_gender={cfg.agent_gender}"
    )
    logger.debug(f"System prompt ({len(system_prompt)} chars) ready for tenant={tenant_id}")

    # ── Resolve API keys via settings (avoids raw os.environ lookups) ────────
    from config import get_settings as _get_settings
    settings = _get_settings()
    sarvam_key = settings.SARVAM_API_KEY or ""

    # Motor/Redis import overlaps agent construction + wait_for_participant (not the greeting TTS).
    db_preload_task = asyncio.create_task(_preload_worker_db_async())

    # ── Build the agent ───────────────────────────────────────────────────────
    # Silero VAD: increase VAD_CONFIDENCE (e.g. 0.75–0.8) or VAD_START_SECS in .env if
    # the pipeline picks up background noise; higher activation_threshold = stricter speech gate.
    logger.info(
        f"[VAD] Silero min_speech={settings.VAD_START_SECS}s min_silence={settings.VAD_STOP_SECS}s "
        f"activation_threshold={settings.VAD_CONFIDENCE}"
    )
    agent = TenantAgent(
        tenant_id=config_key,
        campaign_id=campaign_id,
        call_direction=call_direction,
        use_direction_specific_kb=getattr(cfg.kb, "use_direction_specific", False),
        call_sid=call_sid,
        instructions=system_prompt,
        vad=VAD.load(
            min_speech_duration=settings.VAD_START_SECS,
            min_silence_duration=settings.VAD_STOP_SECS,
            activation_threshold=settings.VAD_CONFIDENCE,
        ),
        stt=_build_stt(cfg, sarvam_key),
        llm=openai_llm.LLM(
            model=cfg.llm.model,
            temperature=_voice_llm_temperature(cfg),
            max_completion_tokens=cfg.llm.max_tokens,
        ),
        tts=_build_tts(cfg, sarvam_key),
    )

    # ── Start the session ─────────────────────────────────────────────────────
    logger.info("Waiting for participant...")
    participant = await ctx.wait_for_participant()
    logger.info(f"Participant connected: {participant.identity}")

    _aec = float(getattr(settings, "AEC_WARMUP_SEC", 0.7))
    _aec_opt = None if _aec <= 0 else _aec
    # Sarvam TTS uses streaming=True → LiveKit does not add StreamAdapter /
    # blingfire SentenceTokenizer on tts_node; phrase batching is in plugins/sarvam_tts.
    #
    # Preemptive generation can start the LLM before the user fully stops; very short
    # lead times (e.g. ~5ms in logs) interact badly with turn-taking — keep off for
    # stable Sarvam WS TTS. (VOICE_PREEMPTIVE_GENERATION in .env is ignored here.)
    session = AgentSession(
        aec_warmup_duration=_aec_opt,
        preemptive_generation=False,
    )
    # Start the session BEFORE calling session.say() so the TTS pipeline,
    # Sarvam STT, and VAD are all initialized in parallel while
    # the greeting is being synthesized. Previously the pipeline initialized
    # sequentially which added ~1s before TTS synthesis even began.
    await session.start(agent=agent, room=ctx.room)

    # Parallel RAG: start KB retrieval as soon as STT finalizes each utterance,
    # so embedding + Mongo search overlap pipeline work before llm_node runs.
    def _on_user_input_transcribed(ev: UserInputTranscribedEvent) -> None:
        if not ev.is_final:
            return
        agent.schedule_kb_prefetch(ev.transcript)

    session.on("user_input_transcribed", _on_user_input_transcribed)

    # ── Greet the caller ──────────────────────────────────────────────────────
    logger.info(f"Sending greeting — tenant={tenant_id} direction={call_direction}")
    _greet_results = await asyncio.gather(
        session.say(greeting, allow_interruptions=True),
        db_preload_task,
        return_exceptions=True,
    )
    for _i, _r in enumerate(_greet_results):
        if isinstance(_r, Exception):
            logger.warning(f"[Voice] Parallel greet/db task {_i} failed: {_r}")
    logger.info("Greeting sent — voice agent active")
    # Post-call summary is generated in routes/exotel_webhooks.py status-callback;
    # the agent job process exits when the session closes, so we cannot run it here.
