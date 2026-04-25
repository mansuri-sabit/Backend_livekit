"""
agents/tenant_config.py

Loads tenant configuration from MongoDB (primary) or YAML files (fallback).

Lookup order (TTL-cached per 30 seconds):
  1. In-memory cache (30s TTL).
  2. MongoDB agents collection — matched by tenant_id | user_id | _id.
  3. MongoDB last-resort — most recently updated active agent (for default tenant).
  4. YAML: config/tenants/{tenant_id}.yaml (if it exists).
  5. Generic safe default (no hardcoded persona).

All persona, greeting, and knowledge base configuration comes from the
dashboard ("Create / Edit Agent") and is stored in MongoDB.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Suppress pymongo heartbeat/topology/command DEBUG spam in both main and worker processes.
for _pml in ("pymongo", "pymongo.topology", "pymongo.connection",
             "pymongo.command", "pymongo.serverSelection"):
    logging.getLogger(_pml).setLevel(logging.WARNING)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_TENANTS_DIR = _PROJECT_ROOT / "config" / "tenants"

_CONFIG_CACHE: dict[str, "TenantConfig"] = {}
_CONFIG_CACHE_TS: dict[str, float] = {}
# USE_CACHE=false in .env disables caching entirely (dev mode — instant dashboard updates).
_USE_CACHE: bool = os.environ.get("USE_CACHE", "true").lower() in ("true", "1", "yes")
_CACHE_TTL_SECONDS: float = 300.0

# Sync pymongo client for get_tenant_config() (LiveKit worker has no event loop).
# Initialised lazily on first lookup so .env / cwd match the voice worker process
# and TLS options stay aligned with utils.db (Motor).
_PYMONGO_CLIENT: "Any | None" = None
_PYMONGO_DB_NAME: "str | None" = None
# Skip repeat serverStatus ping when another warmup ran recently (same worker process).
_LAST_MONGO_PING_MONO: float | None = None


def _ensure_pymongo_client() -> bool:
    """Create pymongo client if missing. Returns False if MongoDB cannot be used."""
    global _PYMONGO_CLIENT, _PYMONGO_DB_NAME
    if _PYMONGO_CLIENT is not None and _PYMONGO_DB_NAME:
        return True
    try:
        from config import get_settings
        from urllib.parse import urlparse
        import pymongo

        settings = get_settings()
        if not settings.MONGO_URI:
            logger.warning(
                "[TenantConfig] MONGO_URI is not set — voice worker cannot load dashboard agents; "
                "using YAML/generic defaults."
            )
            return False
        uri = settings.MONGO_URI
        parsed = urlparse(uri)
        kw: dict[str, Any] = {
            "serverSelectionTimeoutMS": 10_000,
            "connectTimeoutMS": 10_000,
        }
        host = parsed.hostname or ""
        if "localhost" not in host and "127.0.0.1" not in host:
            try:
                import certifi
                kw["tlsCAFile"] = certifi.where()
            except ImportError:
                pass
        if getattr(settings, "MONGO_TLS_ALLOW_INVALID_CERTIFICATES", False):
            kw["tlsAllowInvalidCertificates"] = True
        _PYMONGO_CLIENT = pymongo.MongoClient(uri, **kw)
        _PYMONGO_DB_NAME = (parsed.path or "").lstrip("/").split("?")[0] or "ai_calling"
        logger.info("[TenantConfig] MongoDB client initialised for tenant config loading")
        return True
    except Exception as exc:
        logger.warning(
            f"[TenantConfig] MongoDB client init failed — voice worker will use generic persona/KB: {exc}"
        )
        return False


def warmup_mongo_sync() -> bool:
    """
    Eagerly create the pymongo client and run server ping.

    Call from LiveKit worker `prewarm` and/or at the start of `entrypoint` so the
    first `get_tenant_config` does not pay full TLS/DNS + handshake latency
    during the greeting path.

    Repeated pings in the same process within MONGO_WARMUP_PING_MIN_INTERVAL_SEC
    (default 60s) are skipped — LiveKit idle prewarm + entrypoint both call this,
    and logs were showing duplicate \"MongoDB warmed\" noise and RTT overhead.
    """
    global _LAST_MONGO_PING_MONO
    if not _ensure_pymongo_client():
        return False
    try:
        assert _PYMONGO_CLIENT is not None and _PYMONGO_DB_NAME
        min_interval = float(os.environ.get("MONGO_WARMUP_PING_MIN_INTERVAL_SEC", "60"))
        now = time.monotonic()
        if (
            _LAST_MONGO_PING_MONO is not None
            and (now - _LAST_MONGO_PING_MONO) < min_interval
        ):
            logger.debug(
                "[TenantConfig] MongoDB ping skipped (recent warmup, "
                f"{now - _LAST_MONGO_PING_MONO:.1f}s < {min_interval}s interval)"
            )
            return True
        _PYMONGO_CLIENT[_PYMONGO_DB_NAME].command("ping")
        _LAST_MONGO_PING_MONO = now
        logger.info("[TenantConfig] MongoDB warmed (client + ping)")
        return True
    except Exception as exc:
        logger.warning(f"[TenantConfig] MongoDB ping during warmup failed: {exc}")
        return False


def calls_set_fields_sync(call_id: str, fields: dict) -> bool:
    """
    Update `calls` by call_id using synchronous pymongo.

    The LiveKit voice worker must not use Motor/async `utils.db` for writes:
    that client can bind to the wrong asyncio loop and raise
    "Future attached to a different loop". KB search already uses this pymongo
    client from a thread pool; transcript and post-call summary use the same path.
    """
    if not call_id or not fields:
        return False
    if not _ensure_pymongo_client():
        return False
    try:
        assert _PYMONGO_CLIENT is not None and _PYMONGO_DB_NAME
        _PYMONGO_CLIENT[_PYMONGO_DB_NAME]["calls"].update_one(
            {"call_id": call_id},
            {"$set": fields},
        )
        return True
    except Exception as exc:
        logger.debug(f"[TenantConfig] calls update failed call_id={call_id}: {exc}")
        return False


_PREWARM_RECENT_MAX = 500  # safety cap for PREWARM_RECENT_ACTIVE_AGENTS


def _prewarm_recent_active_agents(limit: int) -> None:
    """
    Load configs for the N most recently updated active agents (Mongo `agents`).

    Production-friendly: no hardcoded ids. Call volume can be 100k+ across many
    tenants; we only prime a small hot set per idle worker process so common
    agents hit the in-memory cache on first job in that process.
    """
    if limit <= 0:
        return
    limit = min(limit, _PREWARM_RECENT_MAX)
    try:
        assert _PYMONGO_CLIENT is not None and _PYMONGO_DB_NAME
        col = _PYMONGO_CLIENT[_PYMONGO_DB_NAME]["agents"]
        cursor = (
            col.find(
                {"is_active": {"$ne": False}},
                {"_id": 1},
            )
            .sort("updated_at", -1)
            .limit(limit)
        )
        n_ok = 0
        for doc in cursor:
            aid = str(doc.get("_id") or "").strip()
            if not aid:
                continue
            try:
                get_tenant_config(aid, "default")
                n_ok += 1
            except Exception as exc:
                logger.debug(f"[TenantConfig] Prewarm recent skip for '{aid}': {exc}")
        if n_ok:
            logger.info(
                f"[TenantConfig] Prewarmed {n_ok} recently updated active agent(s) "
                f"(limit={limit}, from Mongo sort by updated_at)"
            )
    except Exception as exc:
        logger.debug(f"[TenantConfig] Recent-agent prewarm failed: {exc}")


def prewarm_agent_configs_from_env() -> None:
    """
    Load tenant configs into the in-process cache at worker idle time.

    Always runs Mongo client + ping when MONGO_URI is set (cheap connection reuse).

    Optional sources (any combination):

    - PREWARM_AGENT_IDS — comma-separated agent/tenant ids (dev or a fixed VIP list).
    - PREWARM_RECENT_ACTIVE_AGENTS — integer; loads that many active agents with newest
      `updated_at` from Mongo (recommended for production instead of listing all ids).

    Jobs still resolve the correct agent from room metadata + get_tenant_config();
    prewarm only reduces cold latency for agents that appear in these lists.

    No-op for Mongo steps when MONGO_URI is missing.
    """
    if not warmup_mongo_sync():
        return

    raw = (os.environ.get("PREWARM_AGENT_IDS") or "").strip()
    for tid in [x.strip() for x in raw.split(",") if x.strip()]:
        try:
            get_tenant_config(tid, "default")
        except Exception as exc:
            logger.debug(f"[TenantConfig] Prewarm skip for '{tid}': {exc}")

    try:
        recent_n = int((os.environ.get("PREWARM_RECENT_ACTIVE_AGENTS") or "0").strip())
    except ValueError:
        recent_n = 0
    if recent_n > 0:
        _prewarm_recent_active_agents(recent_n)


@dataclass
class VoiceConfig:
    tts_provider: str = "sarvam"       # sarvam | deepgram | elevenlabs | openai | google | cartesia
    stt_provider: str = "sarvam"       # sarvam | deepgram | azure | openai
    tts_voice: str = "anushka"
    tts_language: str = "hi-IN"
    stt_language: str = "hi-IN"
    languages: str = "Hindi and English"


@dataclass
class LLMConfig:
    model: str = "gpt-4o-mini"
    max_tokens: int = 60
    temperature: float = 0.7


@dataclass
class KBConfig:
    backend: str = "none"
    file_path: str | None = None
    collection: str | None = None
    qdrant_url: str | None = None
    qdrant_api_key: str | None = None
    top_k: int = 3
    agent_id: str | None = None             # MongoDB agent _id — scopes MongoKBBackend searches
    use_direction_specific: bool = False    # filter kb_chunks by call_direction (inbound/outbound)


@dataclass
class PromptsConfig:
    template: str = "config/prompts/base_system.j2"
    extra_role_context: str = ""
    inbound_extra_context: str = ""   # inbound-specific prompt override (from config.inboundConfig.prompt)
    outbound_extra_context: str = ""  # outbound-specific prompt override (from config.outboundConfig.prompt)
    transfer_instructions: str = ""   # generated from config.transferSettings
    appointment_instructions: str = ""  # generated from config.appointmentBooking
    data_collection_instructions: str = ""  # generated from config.dataCollection
    end_call_phrases: list[str] = field(default_factory=list)  # from config.endCallPhrases


@dataclass
class GuardrailsConfig:
    forbidden_topics: list[str] = field(default_factory=list)
    hard_restrictions: list[str] = field(default_factory=list)
    escalation_keywords: list[str] = field(default_factory=lambda: [
        "legal", "court", "police", "emergency", "ambulance",
    ])


@dataclass
class GreetingsConfig:
    inbound: str = (
        "Namaste! Main {company_name} ka AI assistant hoon. "
        "Aap mujhse Hindi ya English mein baat kar sakte hain. "
        "Kripya bataiye main aapki kaise madad kar sakta hoon?"
    )
    outbound: str = (
        "Namaste! Main {company_name} ka AI assistant hoon. "
        "Main aapko {vertical} ke baare mein help karne ke liye call kar raha hoon. "
        "Kya aap abhi baat kar sakte hain?"
    )


@dataclass
class TenantConfig:
    tenant_id: str
    company_name: str
    vertical: str
    locale: str

    voice: VoiceConfig
    llm: LLMConfig
    kb: KBConfig
    prompts: PromptsConfig
    guardrails: GuardrailsConfig
    greetings: GreetingsConfig

    agent_gender: str = "female"  # "female" | "male" — used for gender grammar rules


def _parse(raw: dict[str, Any]) -> TenantConfig:
    """Parse a raw YAML dict into a validated TenantConfig."""
    def sub(key: str, cls, defaults: Any = None) -> Any:
        d = raw.get(key) or {}
        if not isinstance(d, dict):
            return cls()
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    return TenantConfig(
        tenant_id=raw.get("tenant_id", "default"),
        company_name=raw.get("company_name", "AI Assistant"),
        vertical=raw.get("vertical", "general customer support"),
        locale=raw.get("locale", "hi-IN"),
        voice=sub("voice", VoiceConfig),
        llm=sub("llm", LLMConfig),
        kb=sub("kb", KBConfig),
        prompts=sub("prompts", PromptsConfig),
        guardrails=sub("guardrails", GuardrailsConfig),
        greetings=sub("greetings", GreetingsConfig),
        agent_gender=raw.get("agent_gender", "female"),
    )


_LANG_TO_LOCALE: dict[str, str] = {
    "en": "en-IN",
    "hi": "hi-IN",
    "bn": "bn-IN",
    "gu": "gu-IN",
    "kn": "kn-IN",
    "ml": "ml-IN",
    "mr": "mr-IN",
    "od": "od-IN",
    "or": "od-IN",
    "pa": "pa-IN",
    "ta": "ta-IN",
    "te": "te-IN",
}

_LANG_DISPLAY: dict[str, str] = {
    "en-IN": "English",
    "hi-IN": "Hindi",
    "bn-IN": "Bengali",
    "gu-IN": "Gujarati",
    "kn-IN": "Kannada",
    "ml-IN": "Malayalam",
    "mr-IN": "Marathi",
    "od-IN": "Odia",
    "pa-IN": "Punjabi",
    "ta-IN": "Tamil",
    "te-IN": "Telugu",
}

# Canonical BCP-47 locale table — restores proper casing after .lower() normalization.
# Sarvam (and most STT/TTS APIs) reject all-lowercase codes like "hi-in".
_CANONICAL_LOCALE: dict[str, str] = {
    "bn-in": "bn-IN", "en-in": "en-IN", "gu-in": "gu-IN",
    "hi-in": "hi-IN", "kn-in": "kn-IN", "ml-in": "ml-IN",
    "mr-in": "mr-IN", "od-in": "od-IN", "pa-in": "pa-IN",
    "ta-in": "ta-IN", "te-in": "te-IN",
}


def _normalize_language(raw: str) -> str:
    """
    Normalize a raw language value from MongoDB to a full BCP-47 locale.

    The dashboard stores short ISO codes like "en" or "hi". Sarvam TTS
    and many STT providers require the full locale ("en-IN", "hi-IN").
    Values containing a hyphen are canonicalized to proper BCP-47 casing
    (e.g. "hi-in" → "hi-IN") since Sarvam rejects lowercase locale codes.
    """
    code = (raw or "en-IN").strip().lower()
    if "-" in code:
        return _CANONICAL_LOCALE.get(code, code)  # restore BCP-47 casing
    return _LANG_TO_LOCALE.get(code, f"{code}-IN")


def _mongo_agent_to_tenant_config(tenant_id: str, doc: dict) -> "TenantConfig":
    """
    Convert a MongoDB agent document to a TenantConfig dataclass.
    Handles both the old seed_agent.py format (persona sub-doc) and the
    new admin_agents.py format (config sub-doc from the dashboard).

    Extracts all fields that the dashboard "Create Agent" form writes:
      - Basic Info: name, gender
      - Persona & Greeting: config.prompt, config.greetingMessage
      - Call Directions: config.inboundConfig / config.outboundConfig
      - AI & Voice: config.voice.provider/voiceId/settings, config.llm, config.sttProvider
      - Knowledge Base: use_direction_specific_kb (scoped via agent _id)
    """
    persona = doc.get("persona") or {}
    config = doc.get("config") or {}
    voice_cfg = config.get("voice") or {}
    llm_raw = config.get("llm") or {}
    inbound_cfg = config.get("inboundConfig") or {}
    outbound_cfg = config.get("outboundConfig") or {}

    company_name = persona.get("company") or doc.get("name") or tenant_id

    # TTS / STT provider from dashboard AI & Voice tab
    tts_provider = (voice_cfg.get("provider") or "sarvam").lower()
    stt_provider = (config.get("sttProvider") or "sarvam").lower()

    # Voice ID: prefer new config.voice.voiceId, fallback to legacy fields
    tts_voice = (
        voice_cfg.get("voiceId")
        or persona.get("voice_id")
        or doc.get("voice_id")
        or "priya"
    )

    language = _normalize_language(
        doc.get("language") or config.get("language") or "en-IN"
    )

    # Build human-readable language list from supportedLanguages array
    # e.g. ["en", "hi", "ta"] → "English, Hindi and Tamil"
    supported_langs = config.get("supportedLanguages") or []
    if supported_langs:
        lang_names = []
        for code in supported_langs:
            locale = _normalize_language(code)
            display = _LANG_DISPLAY.get(locale, code)
            if display not in lang_names:
                lang_names.append(display)
        if len(lang_names) > 1:
            languages_str = ", ".join(lang_names[:-1]) + " and " + lang_names[-1]
        elif lang_names:
            languages_str = lang_names[0]
        else:
            languages_str = _LANG_DISPLAY.get(language, language)
    else:
        languages_str = _LANG_DISPLAY.get(language, language)

    llm_model = persona.get("llm_model") or llm_raw.get("model") or "gpt-4o-mini"
    llm_max_tokens = int(llm_raw.get("maxTokens") or 60)
    llm_temperature = float(llm_raw.get("temperature", 0.7))

    # Direction-specific greetings (fall back to default)
    default_greeting = (
        doc.get("greeting")
        or config.get("greetingMessage")
        or "Hello! How can I help you today?"
    )
    inbound_greeting = inbound_cfg.get("greetingMessage") or default_greeting
    outbound_greeting = outbound_cfg.get("greetingMessage") or default_greeting

    # Agent gender (for language grammar rules in system prompt)
    agent_gender = (
        persona.get("gender")
        or doc.get("gender")
        or "female"
    ).lower()

    # Direction-specific system prompts (fall back to default)
    # Priority: config.prompt → system_prompt → auto-generate from persona fields
    default_prompt = doc.get("system_prompt") or config.get("prompt") or ""
    if not default_prompt.strip():
        # Auto-generate persona from structured PersonaConfig fields
        # (mirrors reference backend's build_persona_from_fields)
        from agents.prompt_builder import build_persona_from_fields
        default_prompt = build_persona_from_fields(persona, agent_name=doc.get("name"))

    inbound_prompt = inbound_cfg.get("prompt") or ""
    outbound_prompt = outbound_cfg.get("prompt") or ""

    # ── Transfer, appointment, data collection, end-call from config ──────────
    transfer_instructions = ""
    transfer_cfg = config.get("transferSettings") or {}
    if transfer_cfg.get("enabled"):
        number = transfer_cfg.get("humanAgentNumber", "")
        condition = transfer_cfg.get("transferCondition", "")
        confirm_q = transfer_cfg.get("confirmationQuestion", "Would you like me to transfer you to a human agent?")
        transfer_instructions = (
            "CALL TRANSFER:\n"
            f"- When the caller asks to speak to a human or when: {condition}\n"
            f"- Ask: \"{confirm_q}\"\n"
            "- If they confirm, say the transfer message and the system will handle the transfer.\n"
            "- NEVER say you are 'just a bot' or apologize for being AI.\n"
        )

    appointment_instructions = ""
    appt_cfg = config.get("appointmentBooking") or {}
    if appt_cfg.get("enabled"):
        appt_condition = appt_cfg.get("condition", "when user wants to book an appointment")
        questions = appt_cfg.get("questions") or {}
        name_q = questions.get("nameQuestion", "What's your name?")
        date_q = questions.get("dateQuestion", "What date would you like?")
        time_q = questions.get("timeQuestion", "What time works for you?")
        reason_q = questions.get("reasonQuestion", "")
        confirm_q = questions.get("confirmationQuestion", "")
        success_msg = appt_cfg.get("successMessage", "Your appointment is confirmed!")
        appointment_instructions = (
            "APPOINTMENT BOOKING:\n"
            f"- Activate when: {appt_condition}\n"
            f"- Ask for name: \"{name_q}\"\n"
            f"- Ask for date: \"{date_q}\"\n"
            f"- Ask for time: \"{time_q}\"\n"
        )
        if reason_q:
            appointment_instructions += f"- Ask for reason: \"{reason_q}\"\n"
        if confirm_q:
            appointment_instructions += f"- Confirm: \"{confirm_q}\"\n"
        appointment_instructions += f"- On success say: \"{success_msg}\"\n"

    data_collection_instructions = ""
    dc_cfg = config.get("dataCollection") or {}
    if dc_cfg.get("enabled"):
        fields = dc_cfg.get("fields") or []
        if fields:
            data_collection_instructions = "DATA COLLECTION:\nCollect the following from the caller:\n"
            for f in fields:
                fname = f.get("name", "")
                ftype = f.get("type", "text")
                required = f.get("required", False)
                req_label = " (required)" if required else " (optional)"
                data_collection_instructions += f"- {fname} ({ftype}){req_label}\n"

    end_call_phrases = config.get("endCallPhrases") or []

    # MongoDB KB: always enable when agent is loaded from DB (agent _id scopes the chunks)
    agent_id_str = str(doc["_id"]) if doc.get("_id") else ""
    use_dir_kb = bool(doc.get("use_direction_specific_kb", False))

    _prompt_template = "config/prompts/base_system.j2"

    return TenantConfig(
        tenant_id=tenant_id,
        company_name=company_name,
        vertical=persona.get("role") or "customer support",
        locale=language,
        voice=VoiceConfig(
            tts_provider=tts_provider,
            stt_provider=stt_provider,
            tts_voice=tts_voice,
            tts_language=language,
            stt_language=language,
            languages=languages_str,
        ),
        llm=LLMConfig(
            model=llm_model,
            max_tokens=llm_max_tokens,
            temperature=llm_temperature,
        ),
        kb=KBConfig(
            backend="mongo" if agent_id_str else "none",
            agent_id=agent_id_str or None,
            use_direction_specific=use_dir_kb,
        ),
        prompts=PromptsConfig(
            template=_prompt_template,
            extra_role_context=default_prompt,
            inbound_extra_context=inbound_prompt,
            outbound_extra_context=outbound_prompt,
            transfer_instructions=transfer_instructions,
            appointment_instructions=appointment_instructions,
            data_collection_instructions=data_collection_instructions,
            end_call_phrases=end_call_phrases,
        ),
        guardrails=GuardrailsConfig(),
        greetings=GreetingsConfig(inbound=inbound_greeting, outbound=outbound_greeting),
        agent_gender=agent_gender,
    )


def _cache_set(tenant_id: str, cfg: "TenantConfig") -> None:
    """Write a config into the in-memory cache with a fresh TTL timestamp."""
    if not _USE_CACHE:
        return  # dev mode — never cache
    _CONFIG_CACHE[tenant_id] = cfg
    _CONFIG_CACHE_TS[tenant_id] = time.monotonic()


def get_tenant_config(
    tenant_id: str,
    campaign_id: str = "default",
) -> TenantConfig:
    """
    Return a TenantConfig for the given tenant_id.

    Lookup order (retried every 5 minutes via TTL cache):
        1. In-memory cache (5-min TTL — dashboard changes propagate without restart).
        2. MongoDB: exact match on tenant_id | user_id | _id.
        3. MongoDB: last-resort — most recently updated *active* agent.
           Activated only when tenant_id == DEFAULT_TENANT_ID and no specific match.
           Allows zero-config demo use-cases without phone/routing setup.
        4. YAML: config/tenants/{tenant_id}.yaml (if present).
    """
    # ── 1. TTL-aware cache (disabled when USE_CACHE=false in .env) ─────────
    if _USE_CACHE and tenant_id in _CONFIG_CACHE:
        age = time.monotonic() - _CONFIG_CACHE_TS.get(tenant_id, 0.0)
        if age < _CACHE_TTL_SECONDS:
            return _CONFIG_CACHE[tenant_id]
        del _CONFIG_CACHE[tenant_id]
        _CONFIG_CACHE_TS.pop(tenant_id, None)
        logger.debug(f"[TenantConfig] Cache expired for '{tenant_id}' — re-querying")

    # ── 2. MongoDB: exact identifier lookup ──────────────────────────────────
    _agent_doc: dict | None = None
    try:
        _ensure_pymongo_client()
        if _PYMONGO_CLIENT is not None and _PYMONGO_DB_NAME:
            _or_clauses: list = [
                {"tenant_id": tenant_id},
                {"user_id": tenant_id},
            ]
            # If tenant_id looks like a valid ObjectId (24-char hex), add direct _id lookup
            if len(tenant_id) == 24:
                try:
                    from bson import ObjectId as _ObjectId
                    _or_clauses.append({"_id": _ObjectId(tenant_id)})
                except Exception:
                    pass

            # Only match active agents — inactive/disabled agents must not be
            # used for calls even if their tenant_id/user_id matches.
            # Use $ne:False instead of True so agents without the field are included
            # (legacy/seeded agents created before is_active was introduced).
            _agent_doc = _PYMONGO_CLIENT[_PYMONGO_DB_NAME].agents.find_one(
                {"$or": _or_clauses, "is_active": {"$ne": False}}
            )

            if _agent_doc:
                cfg = _mongo_agent_to_tenant_config(tenant_id, _agent_doc)
                _cache_set(tenant_id, cfg)
                logger.info(
                    f"[TenantConfig] Loaded '{tenant_id}' from MongoDB "
                    f"(agent={_agent_doc.get('_id')} name={_agent_doc.get('name', '?')})"
                )
                return cfg

            logger.warning(
                f"[TenantConfig] No MongoDB agent found for tenant_id='{tenant_id}'. "
                "Falling back to YAML config — dashboard 'Edit Agent' changes will NOT "
                "take effect. Assign a phone number to a user in the dashboard."
            )

    except Exception as _exc:
        logger.warning(
            f"[TenantConfig] MongoDB lookup error for '{tenant_id}': {_exc}. "
            "Falling back to YAML."
        )
    # ── 4. YAML fallback ──────────────────────────────────────────────────────
    config = _load_yaml(tenant_id)
    _cache_set(tenant_id, config)
    return config


def _generic_default_config(tenant_id: str) -> TenantConfig:
    """
    Minimal safe fallback used when MongoDB is unreachable and no YAML exists.
    Contains no hardcoded persona — the agent will respond generically until
    MongoDB is available and an agent is configured in the dashboard.
    """
    return TenantConfig(
        tenant_id=tenant_id,
        company_name="AI Assistant",
        vertical="general customer support",
        locale="en-IN",
        voice=VoiceConfig(),
        llm=LLMConfig(),
        kb=KBConfig(),
        prompts=PromptsConfig(
            template="config/prompts/base_system.j2",
            extra_role_context=(
                "You are a helpful AI voice assistant. "
                "Answer the caller's questions clearly and concisely. "
                "Do not make up information you don't know."
            ),
        ),
        guardrails=GuardrailsConfig(),
        greetings=GreetingsConfig(
            inbound="Hello! How can I help you today?",
            outbound="Hello! I'm calling to assist you. Is this a good time to talk?",
        ),
    )


def _load_yaml(tenant_id: str) -> TenantConfig:
    tenant_path = _TENANTS_DIR / f"{tenant_id}.yaml"

    if not tenant_path.exists():
        logger.warning(
            f"[TenantConfig] No YAML config found for '{tenant_id}' at '{tenant_path}'. "
            "Returning generic default — configure agents in the dashboard."
        )
        return _generic_default_config(tenant_id)

    try:
        with tenant_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        cfg = _parse(raw)
        logger.info(f"[TenantConfig] Loaded YAML: {tenant_path.name} (tenant_id={cfg.tenant_id})")
        return cfg
    except Exception as exc:
        logger.error(
            f"[TenantConfig] Failed to parse '{tenant_path}': {exc}. Using generic default.",
            exc_info=True,
        )
        return _generic_default_config(tenant_id)


def reload_all() -> None:
    """
    Clear the in-memory config cache (both entries and TTL timestamps).

    Called by admin_agents.update_agent() so changes made in the dashboard
    propagate to the next call immediately (within the same worker process).
    For the cross-process case (FastAPI + voice worker), the 5-minute TTL
    handles propagation automatically.
    """
    _CONFIG_CACHE.clear()
    _CONFIG_CACHE_TS.clear()
    logger.info("[TenantConfig] Cache cleared — next call will reload from MongoDB / YAML.")
