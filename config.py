"""
Unified configuration for AI Voice Platform
Merges LiveKit (Project B) settings with Pipecat platform (Project A) settings.

Naming convention: Project B field names are canonical for all Exotel/LiveKit keys
because they are used by protected files (services/exotel_service.py,
routers/webhook.py, routers/calls.py). Project A field names appear as Optional
migration aliases and will be removed once all ported routes are adapted.
"""
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Server ────────────────────────────────────────────────────────────────
    PORT: int = 8000
    HOST: str = "0.0.0.0"
    DEBUG: bool = True

    # ── Exotel — B's field names are canonical (used by protected files) ──────
    EXOTEL_SID: str                           # Account SID  (A calls this EXOTEL_ACCOUNT_SID)
    EXOTEL_API_KEY: str                       # API key      (shared name between A and B)
    EXOTEL_AUTH_TOKEN: str                    # API token    (A calls this EXOTEL_API_TOKEN)
    EXOTEL_VIRTUAL_NUMBER: str                # ExoPhone     (A calls this EXOTEL_FROM_NUMBER)
    EXOTEL_APP_ID: str = ""                   # App Bazaar flow ID
    EXOTEL_SUBDOMAIN: str = "api.exotel.com"  # api.exotel.com or api.in.exotel.com
    EXOTEL_BASE_URL: str = ""
    EXOTEL_STARTUP_CHECK_ENABLED: bool = False

    # Migration aliases — A's code uses these names; swap to canonical in Phases 6+
    # Leave None in .env; set them to the same values as the canonical fields if needed.
    EXOTEL_ACCOUNT_SID: Optional[str] = None  # alias → EXOTEL_SID
    EXOTEL_API_TOKEN: Optional[str] = None     # alias → EXOTEL_AUTH_TOKEN
    EXOTEL_FROM_NUMBER: Optional[str] = None   # alias → EXOTEL_VIRTUAL_NUMBER

    # ── Exotel campaign / webhook behaviour ───────────────────────────────────
    CAMPAIGN_RETRY_ENABLED: bool = True
    CAMPAIGN_CALLBACK_TIMEOUT_SECONDS: int = 90
    CREDITS_CAMPAIGN_PAUSE_THRESHOLD: int = 1000

    # ── LiveKit — PROTECTED: used by livekit_bridge.py and voice_agent.py ─────
    LIVEKIT_API_KEY: str
    LIVEKIT_API_SECRET: str
    LIVEKIT_URL: str
    LIVEKIT_WS_URL: str = ""   # fallback; see property below

    # ── LiveKit SIP Trunk ─────────────────────────────────────────────────────
    SIP_TRUNK_OUTBOUND_ADDRESS: str = ""
    SIP_TRUNK_OUTBOUND_USERNAME: str = ""
    SIP_TRUNK_OUTBOUND_PASSWORD: str = ""
    SIP_TRUNK_OUTBOUND_NUMBER: str = ""
    SIP_TRUNK_OUTBOUND_ID: str = ""
    SIP_TRUNK_INBOUND_ID: str = ""

    # ── Tunnel / webhook ──────────────────────────────────────────────────────
    NGROK_URL: Optional[str] = None
    WEBHOOK_BASE_URL: str = ""

    # ── Database (Optional during Phase 1; required from Phase 3 onwards) ─────
    MONGO_URI: Optional[str] = None
    REDIS_URL: Optional[str] = None
    # Set True ONLY on Windows dev when Atlas TLS handshake fails (TLSV1_ALERT_INTERNAL_ERROR)
    MONGO_TLS_ALLOW_INVALID_CERTIFICATES: bool = False

    # ── Auth / JWT (required from Phase 3 onwards) ───────────────────────────
    JWT_SECRET: Optional[str] = None
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_HOURS: int = 24

    # WebSocket /ws/exotel: require JWT in ?token= query param for handshake.
    # False by default — Exotel server does not send tokens.
    WS_EXOTEL_REQUIRE_AUTH: bool = False

    # ── Email OTP ─────────────────────────────────────────────────────────────
    RESEND_API_KEY: str = ""
    RESEND_FROM_EMAIL: str = ""
    OTP_EXPIRY_SECONDS: int = 300

    # ── CORS ──────────────────────────────────────────────────────────────────
    # Comma-separated origins. Unset → allow localhost dev ports only (never wildcard).
    CORS_ORIGINS: Optional[str] = None

    # ── AI providers ──────────────────────────────────────────────────────────
    OPENAI_API_KEY: Optional[str] = None
    SARVAM_API_KEY: str = ""
    ELEVENLABS_API_KEY: Optional[str] = None
    DEEPGRAM_API_KEY: str = ""
    ANTHROPIC_API_KEY: str = ""
    CARTESIA_API_KEY: Optional[str] = None
    GROQ_API_KEY: Optional[str] = None
    GEMINI_API_KEY: Optional[str] = None

    # ── Sarvam TTS/STT (LiveKit agent uses these directly) ────────────────────
    SARVAM_TTS_LANGUAGE: str = "hi-IN"
    SARVAM_TTS_VOICE: str = "anushka"
    SARVAM_STT_LANGUAGE: str = "hi-IN"

    # Sarvam bulbul:v3 valid speakers; fallback voice if agent voice not in list
    SARVAM_V3_SPEAKERS: str = (
        "aditya,ritu,priya,neha,rahul,pooja,rohan,simran,kavya,amit,dev,ishita,"
        "shreya,ratan,varun,manan,sumit,roopa,kabir,aayan,shubh,ashutosh,advait,"
        "amelia,sophia"
    )
    SARVAM_DEFAULT_VOICE: str = "priya"
    SARVAM_DEFAULT_MALE_VOICE: str = "advait"
    SARVAM_FEMALE_VOICE_IDS: str = (
        "priya,neha,ritu,pooja,simran,kavya,ishita,shreya,roopa,amelia,sophia"
    )
    # Override default Sarvam speaker when set (e.g. TTS_SPEAKER=kavya)
    TTS_SPEAKER: Optional[str] = None

    # ── TTS / STT provider selection (Pipecat pipeline) ───────────────────────
    USE_TTS_SARVAM: bool = True
    STT_PROVIDER: str = "sarvam"   # "sarvam" | "edge" | "vosk"
    TTS_PROVIDER: str = "sarvam"   # "sarvam" | "elevenlabs" | "google" | "edge"

    # ── Azure (Edge STT / Translator) ─────────────────────────────────────────
    AZURE_SPEECH_KEY: Optional[str] = None
    AZURE_SPEECH_REGION: Optional[str] = None
    AZURE_TRANSLATOR_KEY: str = ""
    AZURE_TRANSLATOR_REGION: str = ""
    AZURE_TRANSLATOR_ENDPOINT: str = "https://api.cognitive.microsofttranslator.com"

    # ── Google Cloud TTS ──────────────────────────────────────────────────────
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = None
    GOOGLE_TTS_VOICE_ID: str = "en-US-Chirp3-HD-Kore"

    # ── Vosk (offline STT) ────────────────────────────────────────────────────
    VOSK_MODEL_PATH: str = "./vosk models/vosk-model-en-in-0.5/vosk-model-en-in-0.5"
    VOSK_MIN_CONFIDENCE: float = 0.5

    # ── VAD (Silero) — tune via .env for noisy rooms (higher activation_threshold = stricter)
    VAD_MIN_VOLUME: float = 0.45
    VAD_CONFIDENCE: float = 0.72
    # min_speech_duration: ignore very short sounds (thumps/coughs); raise in loud environments
    VAD_START_SECS: float = 0.25
    VAD_STOP_SECS: float = 0.40

    # ── RNNoise noise suppression ─────────────────────────────────────────────
    RNNOISE_ENABLED: bool = False
    RNNOISE_VAD_ENABLED: bool = True
    RNNOISE_VAD_THRESHOLD: float = 0.55
    RNNOISE_VAD_ATTENUATION_DB: float = -32.0
    RNNOISE_VAD_SOFT_KNEE: bool = True
    KRISP_VIVA_FILTER_MODEL_PATH: Optional[str] = None

    # LiveKit AgentSession: after the agent speaks, ignore user "interruptions" while AEC
    # converges. Default SDK is 3.0s — too long for phone/SIP; users sound ignored or STT
    # lags badly. Use ~0.5–1.0 for calls; 0 or negative = disable (may get echo false triggers).
    AEC_WARMUP_SEC: float = 0.7

    # Start LLM/TTS as soon as a user transcript arrives (before turn fully settles).
    VOICE_PREEMPTIVE_GENERATION: bool = False

    # ── LLM settings ──────────────────────────────────────────────────────────
    DEFAULT_LLM_MODEL: str = "gpt-4o-mini"
    LLM_MODE: str = "single"   # "cascade" | "single"
    LLM_CACHE_ENABLED: bool = False
    LLM_CACHE_TTL: int = 86400

    # ── Mem0 persistent contact memory ───────────────────────────────────────
    MEM0_ENABLED: bool = False
    MEM0_DIR: str = "/tmp/mem0"

    # ── RAG / vector search ───────────────────────────────────────────────────
    RAG_RELEVANT_MIN_WORDS: int = 2
    RAG_SKIP_PHRASES: str = (
        "hello,hi,hey,good morning,good afternoon,good evening,thanks,thank you,"
        "thx,ok,okay,yes,no,nope,yep,bye,goodbye,cya,see ya,cool,great,awesome,"
        "haan,theek,accha,ji"
    )
    RAG_SEARCH_TIMEOUT_SEC: float = 2.5

    # ── Early LLM trigger ────────────────────────────────────────────────────
    EARLY_LLM_MIN_WORDS: int = 7

    # ── Fallback agent defaults (when no agent in DB / DB unreachable) ────────
    DEFAULT_TENANT_ID: str = "default"
    FALLBACK_AGENT_SYSTEM_PROMPT: str = "You are a helpful AI assistant."
    FALLBACK_AGENT_VOICE_ID: str = "priya"
    FALLBACK_AGENT_LANGUAGE: str = "en-IN"
    FALLBACK_AGENT_GREETING: str = "Hello! I am your AI assistant. Can you hear me?"

    # ── Proposal (WhatsApp via AISensity) ─────────────────────────────────────
    PROPOSAL_API_KEY: Optional[str] = None
    AISENSITY_CAMPAIGN_URL: str = "https://backend.api-wa.co/campaign/troika-tech-services/api/v2"
    AISENSITY_CAMPAIGN_NAME: Optional[str] = None
    AISENSITY_TEMPLATE_NAME: Optional[str] = None
    AISENSITY_PDF_URL: Optional[str] = None
    PROPOSAL_TEMPLATES: Optional[str] = None
    PROPOSAL_CONDITION: str = (
        "User is asking for a proposal, quote, pricing, or wants to see costs "
        "or get details on WhatsApp."
    )
    PROPOSAL_CONFIRMATION_QUESTION: str = (
        "Kispe bheju? Isi number pe ya kisi aur number pe? Aur English ya Hindi mein?"
    )
    PROPOSAL_CONFIRMATION_KEYWORDS: str = (
        "yes,sure,ok,send it,please,go ahead,haan,theek hai,okay,bhej do"
    )
    PROPOSAL_SUCCESS_MESSAGE: str = "Bhej diya WhatsApp pe."
    PROPOSAL_FAILURE_MESSAGE: str = (
        "Abhi bhej nahi paya; main details verbally bata deti hoon."
    )

    # ── Human transfer ────────────────────────────────────────────────────────
    TRANSFER_HUMAN_AGENT_NUMBER: Optional[str] = None

    # ── AWS S3 (call recording storage) ──────────────────────────────────────
    AWS_REGION: str = "ap-south-1"
    AWS_SECRETS_MANAGER_SECRET_ID: str = "ai-calling-platform-secrets"
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_S3_BUCKET: str = "ai-calling-recordings"
    AWS_S3_PRESIGNED_URL_EXPIRY: int = 3600

    # ── Recording fetcher background job ─────────────────────────────────────
    RECORDING_FETCHER_ENABLED: bool = False
    RECORDING_FETCH_INTERVAL: int = 300
    RECORDING_DELAY: int = 120
    RECORDING_BATCH_SIZE: int = 50
    RECORDING_MAX_ATTEMPTS: int = 3

    # ── Misc tooling ──────────────────────────────────────────────────────────
    FFMPEG_PATH: Optional[str] = None

    # ── Parler TTS (legacy — unused by active pipeline) ───────────────────────
    PARLER_TTS_URL: Optional[str] = None
    PARLER_TTS_VOICE: str = "default_female"
    PARLER_TTS_TIMEOUT: float = 3.0

    # ─────────────────────────────────────────────────────────────────────────
    # Computed properties
    # ─────────────────────────────────────────────────────────────────────────

    @property
    def exotel_webhook_url(self) -> str:
        """Full URL for Exotel Voicebot JSON endpoint (LiveKit path)."""
        base = self.WEBHOOK_BASE_URL.rstrip("/")
        return f"{base}/webhook/voicebot"

    @property
    def livekit_ws_url(self) -> str:
        """LiveKit WebSocket URL (prefers LIVEKIT_WS_URL, falls back to LIVEKIT_URL)."""
        return self.LIVEKIT_WS_URL or self.LIVEKIT_URL

    @property
    def cors_origins_list(self) -> list[str]:
        """Parsed CORS origins list. Falls back to localhost dev ports if not set."""
        if self.CORS_ORIGINS:
            return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]
        return [
            "http://localhost:5173",
            "http://localhost:5174",
            "http://localhost:5176",
            "http://localhost:3000",
        ]


@lru_cache()
def get_settings() -> Settings:
    """Return cached Settings instance. Call get_settings.cache_clear() to reload."""
    return Settings()
