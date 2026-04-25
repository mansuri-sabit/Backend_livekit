"""
Database layer: Motor async MongoDB client + Redis client + DB helper functions.

MongoDB and Redis are optional until Phase 3 (auth) activates them.
If MONGO_URI or REDIS_URL is not set, db / redis_client are None and
require_db() / require_redis() raise HTTP 503 rather than crashing startup.
"""
import json
import os
import socket
from typing import Optional

import redis.asyncio as aioredis
from fastapi import HTTPException
from loguru import logger

from config import get_settings
from utils.redis_mock import MockRedis

settings = get_settings()

# ── MongoDB ───────────────────────────────────────────────────────────────────

def _mongo_client_kwargs() -> dict:
    kwargs: dict = {}
    uri = settings.MONGO_URI or ""
    is_local = "localhost" in uri or "127.0.0.1" in uri
    if not is_local:
        try:
            import certifi
            kwargs["tlsCAFile"] = certifi.where()
        except ImportError:
            pass
    if settings.MONGO_TLS_ALLOW_INVALID_CERTIFICATES:
        kwargs["tlsAllowInvalidCertificates"] = True
    return kwargs


mongo_client = None
db = None

if settings.MONGO_URI:
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
        mongo_client = AsyncIOMotorClient(settings.MONGO_URI, **_mongo_client_kwargs())
        try:
            db = mongo_client.get_default_database()
        except Exception:
            # URI has no database name — fall back to "ai_calling"
            db = mongo_client["ai_calling"]
        logger.info(f"[DB] MongoDB client initialised ({settings.MONGO_URI[:30]}...)")
    except Exception as exc:
        logger.warning(f"[DB] MongoDB client init failed: {exc}")
else:
    logger.info("[DB] MONGO_URI not set — MongoDB disabled until Phase 3 config")


def require_db():
    """Call inside route handlers that need the database. Raises 503 if not available."""
    if db is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    return db


# ── Redis ─────────────────────────────────────────────────────────────────────

def _is_redis_available() -> bool:
    try:
        import urllib.parse
        url = urllib.parse.urlparse(settings.REDIS_URL or "")
        host = url.hostname or "localhost"
        port = url.port or 6379
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


redis_client = None

# LiveKit voice worker imports this module for transcript writes. A synchronous
# TCP probe to Redis (1s timeout) was blocking the greeting path for no benefit
# when Redis is down. Set LIVEKIT_AGENT_SKIP_REDIS_PROBE=true on the agent host to
# create the async client immediately; first Redis use will fail fast if misconfigured.
_SKIP_REDIS_PROBE = os.environ.get("LIVEKIT_AGENT_SKIP_REDIS_PROBE", "").lower() in (
    "1",
    "true",
    "yes",
)

if settings.REDIS_URL:
    if _SKIP_REDIS_PROBE:
        redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        logger.info(
            "[DB] Redis client created (LIVEKIT_AGENT_SKIP_REDIS_PROBE — no sync TCP check)"
        )
    elif _is_redis_available():
        redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        logger.info(f"[DB] Redis connected at {settings.REDIS_URL}")
    else:
        logger.warning("[DB] Redis configured but not reachable — using in-memory mock")
        redis_client = MockRedis()
else:
    logger.info("[DB] REDIS_URL not set — using in-memory mock")
    redis_client = MockRedis()


def require_redis():
    """Call inside route handlers that need a real Redis. Raises 503 on mock."""
    if isinstance(redis_client, MockRedis):
        raise HTTPException(status_code=503, detail="Redis not available")
    return redis_client


# ── Agent / scope helpers ─────────────────────────────────────────────────────

_DIY_LANG_MAP = {
    "en": "en-IN", "hi": "hi-IN", "ta": "ta-IN", "te": "te-IN",
    "mr": "mr-IN", "gu": "gu-IN", "bn": "bn-IN", "kn": "kn-IN", "ml": "ml-IN",
}


def _normalize_diy_agent_for_pipeline(diy: dict, voice_cfg: dict = None) -> dict:
    """Convert a diy_agents document to the pipeline-compatible agent format."""
    user_id = diy.get("user_id", "")
    gender = (diy.get("gender") or "female").lower()

    if voice_cfg:
        gender_voice = voice_cfg.get(gender) or {}
        provider = gender_voice.get("provider", "sarvam")
        voice_id = gender_voice.get("voiceId", "priya" if gender == "female" else "shubh")
        voice_settings = dict(gender_voice.get("settings") or {})
        if "pace" not in voice_settings and "pace_min" not in voice_settings:
            voice_settings["pace"] = gender_voice.get("speakingRate", 1.1)
    else:
        provider = "sarvam"
        voice_id = "priya" if gender == "female" else "shubh"
        voice_settings = {"pace_min": 1.0, "pace_max": 1.2}

    inbound_cfg = diy.get("inbound_config") or {}
    outbound_cfg = diy.get("outbound_config") or {}
    langs = diy.get("supported_languages") or ["en"]
    language = _DIY_LANG_MAP.get(langs[0], f"{langs[0]}-IN") if langs else "en-IN"
    outbound_greeting = outbound_cfg.get("greetingMessage") or "Hello! How can I help you?"

    return {
        "_id": diy["_id"],
        "user_id": user_id,
        "tenant_id": user_id,
        "name": diy.get("name", "Assistant"),
        "system_prompt": diy.get("prompt", ""),
        "voice_id": voice_id,
        "language": language,
        "greeting": outbound_greeting,
        "max_concurrent": 10,
        "config": {
            "prompt": diy.get("prompt", ""),
            "greetingMessage": outbound_greeting,
            "inboundConfig": inbound_cfg,
            "outboundConfig": outbound_cfg,
            "supportedLanguages": langs,
            "language": language,
            "voice": {"provider": provider, "voiceId": voice_id, "settings": voice_settings},
            "callTransferEnabled": diy.get("call_transfer_enabled", False),
            "transferSettings": diy.get("call_transfer_settings") or {},
            "appointmentBookingEnabled": (diy.get("appointment_booking") or {}).get("enabled", False),
            "appointmentBooking": diy.get("appointment_booking") or {},
            "proposalSettings": diy.get("proposal_settings") or {},
            "voicemailDetection": diy.get("voicemail_detection") or {"enabled": True},
        },
        "proposal_settings": diy.get("proposal_settings") or {},
        "_is_diy": True,
    }


async def get_agent_for_scope(scope: str) -> Optional[dict]:
    """
    Find agent by scope (user_id or tenant_id). Admin-created agents use user_id;
    legacy/seeded agents use tenant_id. Falls back to diy_agents if not found.
    Returns agent doc or None.
    """
    if db is None or not scope or not scope.strip():
        return None
    scope = scope.strip()
    agent = await db.agents.find_one({"$or": [{"user_id": scope}, {"tenant_id": scope}]})
    if agent:
        diy = await db.diy_agents.find_one(
            {"user_id": scope},
            projection={"gender": 1, "proposal_settings": 1, "voicemail_detection": 1},
        )
        if diy:
            voice_cfg = await db.diy_voice_config.find_one({"type": "user", "user_id": scope})
            if not voice_cfg:
                voice_cfg = await db.diy_voice_config.find_one({"type": "global"})
            if voice_cfg:
                gender = (diy.get("gender") or "female").lower()
                gender_voice = voice_cfg.get(gender) or {}
                vc_settings = dict(gender_voice.get("settings") or {})
                if "pace" not in vc_settings and "pace_min" not in vc_settings:
                    vc_settings["pace"] = gender_voice.get("speakingRate", 1.1)
                if vc_settings:
                    cfg = agent.setdefault("config", {})
                    voice = dict(cfg.get("voice") or {})
                    voice["settings"] = vc_settings
                    cfg["voice"] = voice
            if diy.get("proposal_settings"):
                agent["proposal_settings"] = diy["proposal_settings"]
                agent.setdefault("config", {})["proposalSettings"] = diy["proposal_settings"]
            if diy.get("voicemail_detection"):
                agent.setdefault("config", {})["voicemailDetection"] = diy["voicemail_detection"]
                agent["voicemailDetection"] = diy["voicemail_detection"]
            agent["_is_diy"] = True
        return agent
    diy = await db.diy_agents.find_one({"user_id": scope})
    if not diy:
        return None
    voice_cfg = await db.diy_voice_config.find_one({"type": "user", "user_id": scope})
    if not voice_cfg:
        voice_cfg = await db.diy_voice_config.find_one({"type": "global"})
    logger.info(f"[DB] Resolved DIY agent for user_id={scope}")
    return _normalize_diy_agent_for_pipeline(diy, voice_cfg)


# ── Redis-cached agent / campaign lookups ─────────────────────────────────────

AGENT_CACHE_PREFIX = "agent_scope:"
AGENT_CACHE_TTL = 120
CAMPAIGN_CACHE_PREFIX = "campaign:"
CAMPAIGN_CACHE_TTL = 120


def _serialize_doc(doc: dict) -> str:
    if not doc:
        return "null"
    from bson import ObjectId
    d = dict(doc)
    if "_id" in d and d["_id"] is not None:
        d["_id"] = str(d["_id"])
    return json.dumps(d, default=str)


def _deserialize_agent_doc(raw: str) -> Optional[dict]:
    if not raw or raw == "null":
        return None
    from bson import ObjectId
    d = json.loads(raw)
    if d.get("_id"):
        d["_id"] = ObjectId(d["_id"])
    return d


def _deserialize_campaign_doc(raw: str) -> Optional[dict]:
    if not raw or raw == "null":
        return None
    from bson import ObjectId
    d = json.loads(raw)
    if d.get("_id"):
        d["_id"] = ObjectId(d["_id"])
    return d


async def get_agent_for_scope_cached(
    scope: str, rc=None, ttl: int = AGENT_CACHE_TTL
) -> Optional[dict]:
    """Agent lookup with Redis TTL cache. rc defaults to module-level redis_client."""
    if not scope or not scope.strip():
        return None
    rc = rc or redis_client
    scope = scope.strip()
    key = f"{AGENT_CACHE_PREFIX}{scope}"
    try:
        raw = await rc.get(key)
        if raw:
            return _deserialize_agent_doc(raw)
    except Exception:
        pass
    agent = await get_agent_for_scope(scope)
    try:
        if agent:
            await rc.setex(key, ttl, _serialize_doc(agent))
    except Exception:
        pass
    return agent


async def get_campaign_cached(campaign_id, rc=None, ttl: int = CAMPAIGN_CACHE_TTL) -> Optional[dict]:
    """Campaign by _id with Redis cache. campaign_id can be ObjectId or str."""
    if db is None or not campaign_id:
        return None
    rc = rc or redis_client
    from bson import ObjectId
    oid = campaign_id if isinstance(campaign_id, ObjectId) else ObjectId(str(campaign_id))
    key = f"{CAMPAIGN_CACHE_PREFIX}{oid}"
    try:
        raw = await rc.get(key)
        if raw:
            return _deserialize_campaign_doc(raw)
    except Exception:
        pass
    campaign = await db.campaigns.find_one({"_id": oid})
    try:
        if campaign:
            await rc.setex(key, ttl, _serialize_doc(campaign))
    except Exception:
        pass
    return campaign


def agent_owner_scope(agent: dict, default: str = "") -> str:
    """
    Canonical owner scope for routing.

    Priority:
      1. user_id  — set when agent is created/linked to a user account.
      2. tenant_id — legacy field from seeded / YAML agents.
      3. str(_id) — last resort for admin-created agents that have neither
                    user_id nor tenant_id.  get_tenant_config() handles 24-char
                    hex strings as direct MongoDB _id lookups, so this correctly
                    loads the agent even without a named scope.
    """
    if not agent:
        return default
    scope = (agent.get("user_id") or agent.get("tenant_id") or "").strip()
    if scope:
        return scope
    _id = agent.get("_id")
    if _id:
        return str(_id).strip() or default
    return default


# ── Phone → Agent lookup ──────────────────────────────────────────────────────

def _normalize_phone_for_lookup(number: str) -> str:
    """Strip to last 10 digits; remove Indian +91 prefix."""
    if not number:
        return ""
    digits = "".join(c for c in str(number).strip() if c.isdigit())
    if len(digits) >= 10 and digits.startswith("91"):
        digits = digits[2:][:10]
    return digits[-10:] if len(digits) >= 10 else digits


async def get_phone_by_number(number: str) -> Optional[dict]:
    """
    Find phone document by virtual number.
    Tries exact match then normalized variants (+91, 91, 0 prefix, 10-digit).
    """
    if db is None or not number or not str(number).strip():
        return None
    num = str(number).strip()
    canonical = _normalize_phone_for_lookup(num)
    candidates = [num]
    if canonical and len(canonical) == 10:
        candidates += [f"+91{canonical}", f"91{canonical}", f"0{canonical}", canonical]
    for c in candidates:
        doc = await db.phones.find_one({"number": c})
        if doc:
            return doc
    return None


async def get_agent_for_phone(phone_doc: dict) -> Optional[dict]:
    """
    Return agent document for a phone assignment.

    Checks these fields in order (matching the dashboard's phone assignment schema):
      1. assigned_to_agent_id  → direct agent _id lookup
      2. agent_id              → legacy field name
      3. assigned_to_user_id   → find the active agent owned by this user
    """
    if db is None or not phone_doc:
        return None

    from bson import ObjectId

    # ── 1. assigned_to_agent_id (primary field set by dashboard) ─────────
    agent_id_raw = phone_doc.get("assigned_to_agent_id") or phone_doc.get("agent_id")
    if agent_id_raw:
        try:
            oid = (
                agent_id_raw
                if isinstance(agent_id_raw, ObjectId)
                else ObjectId(str(agent_id_raw))
            )
            doc = await db.agents.find_one({"_id": oid})
            if doc:
                return doc
        except Exception:
            pass

    # ── 2. assigned_to_user_id → find active agent owned by this user ────
    user_id_raw = phone_doc.get("assigned_to_user_id")
    if user_id_raw:
        try:
            user_id_str = str(user_id_raw)
            # Find an active agent whose user_id matches
            doc = await db.agents.find_one({
                "user_id": user_id_str,
                "is_active": True,
            })
            if doc:
                return doc
        except Exception:
            pass

    return None
