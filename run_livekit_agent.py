"""
LiveKit AI Voice Agent Runner

This script starts the LiveKit worker that handles real-time voice conversations
using WebSocket Secure (WSS) connection to LiveKit rooms.

Usage:
    python run_livekit_agent.py

Environment Variables Required:
    - LIVEKIT_API_KEY
    - LIVEKIT_API_SECRET
    - LIVEKIT_WS_URL (wss://your-project.livekit.cloud)
    - OPENAI_API_KEY
"""
from __future__ import annotations

import os
import sys
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

from livekit.agents import JobProcess

# Configure logging
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Force UTF-8 encoding BEFORE creating handlers so they inherit it
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# Console handler with UTF-8 support for Windows
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)

# File handler for all logs
file_handler = RotatingFileHandler(
    logs_dir / "livekit_agent.log",
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding="utf-8",
)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

# File handler specifically for outgoing calls
outgoing_calls_handler = RotatingFileHandler(
    logs_dir / "outgoing_calls.log",
    maxBytes=10*1024*1024,  # 10MB
    backupCount=10,
    encoding="utf-8",
)
outgoing_calls_handler.setLevel(logging.INFO)
outgoing_calls_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)
outgoing_calls_handler.setFormatter(outgoing_calls_formatter)

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[console_handler, file_handler]
)

# Suppress noisy pymongo DEBUG/INFO logs (heartbeats, topology, commands)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("pymongo.topology").setLevel(logging.WARNING)
logging.getLogger("pymongo.connection").setLevel(logging.WARNING)
logging.getLogger("pymongo.command").setLevel(logging.WARNING)
logging.getLogger("pymongo.serverSelection").setLevel(logging.WARNING)

# Create outgoing calls logger
outgoing_calls_logger = logging.getLogger("outgoing_calls")
outgoing_calls_logger.setLevel(logging.INFO)
outgoing_calls_logger.addHandler(outgoing_calls_handler)
# Prevent propagation to avoid duplicate logs
outgoing_calls_logger.propagate = False

logger = logging.getLogger(__name__)


def _prewarm_sarvam_tcp() -> None:
    """Open a short TCP connection to api.sarvam.ai:443 to warm DNS/TLS before first TTS/STT."""
    if os.environ.get("SARVAM_TCP_PREWARM", "1").lower() in ("0", "false", "no"):
        return
    try:
        import socket

        with socket.create_connection(("api.sarvam.ai", 443), timeout=1.5):
            pass
        logger.debug("[Prewarm] Sarvam API TCP handshake done")
    except OSError as e:
        logger.debug(f"[Prewarm] Sarvam TCP prewarm skipped: {e}")


def prewarm(proc: JobProcess):
    """
    Pre-load Silero VAD and warm MongoDB (client + ping; optional PREWARM_AGENT_IDS /
    PREWARM_RECENT_ACTIVE_AGENTS).

    Runs in idle worker processes before jobs arrive, so the first get_tenant_config
    in a job often reuses an already-connected pymongo client. For production,
    Linux + a nearby Mongo replica typically beats Windows IocpProactor for RT latency.
    """
    from livekit.plugins.silero import VAD as _SileroVAD
    try:
        _SileroVAD.load()
        logger.info("[Prewarm] VAD loaded in idle worker")
    except Exception as e:
        logger.warning(f"[Prewarm] VAD load failed: {e}")
    try:
        from agents.tenant_config import prewarm_agent_configs_from_env
        prewarm_agent_configs_from_env()
    except Exception as e:
        logger.warning(f"[Prewarm] Mongo / tenant-config warmup failed: {e}")
    try:
        _prewarm_sarvam_tcp()
    except Exception as e:
        logger.debug(f"[Prewarm] Sarvam network warmup failed: {e}")


def check_environment():
    """Check if all required environment variables are set"""
    required_vars = [
        "LIVEKIT_API_KEY",
        "LIVEKIT_API_SECRET",
        "LIVEKIT_WS_URL",
        "OPENAI_API_KEY"
    ]
    
    missing = []
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)
    
    if missing:
        logger.error(f"❌ Missing environment variables: {', '.join(missing)}")
        logger.info("📝 Please set these variables in your .env file")
        return False
    
    return True


def main():
    """Main entry point for LiveKit agent"""
    logger.info("=" * 60)
    logger.info("🎙️  LiveKit AI Voice Agent - Starting...")
    logger.info("=" * 60)
    
    # Load environment variables
    from dotenv import load_dotenv
    env_path = Path(".env")
    if env_path.exists():
        load_dotenv()
        logger.info("✅ Loaded environment from .env file")
    else:
        logger.warning("⚠️  .env file not found")

    # Voice worker: skip synchronous Redis TCP probe (transcripts use Mongo; probe added ~1s when Redis is down).
    os.environ.setdefault("LIVEKIT_AGENT_SKIP_REDIS_PROBE", "1")
    
    # Check environment
    if not check_environment():
        sys.exit(1)
    
    # Import and run the agent
    try:
        from livekit.agents import WorkerOptions
        from agents.voice_agent import entrypoint
        
        livekit_url = os.getenv("LIVEKIT_WS_URL")
        api_key = os.getenv("LIVEKIT_API_KEY")
        api_secret = os.getenv("LIVEKIT_API_SECRET")
        
        logger.info(f"🔌 Connecting to LiveKit: {livekit_url}")
        logger.info("🤖 AI Voice Agent is ready!")
        logger.info("⏳ Waiting for calls...")
        logger.info("\nPress Ctrl+C to stop\n")
        
        _idle = int(os.environ.get("LIVEKIT_AGENT_NUM_IDLE_PROCESSES", "1"))
        _idle = max(0, min(_idle, 8))

        # Create worker options
        worker_options = WorkerOptions(
            entrypoint_fnc=entrypoint,
            prewarm_fnc=prewarm,
            num_idle_processes=_idle,
            ws_url=livekit_url,
            api_key=api_key,
            api_secret=api_secret,
        )
        
        # Create and start the agent server (WorkerOptions is passed implicitly via environment or use cli.run_app)
        # Use the CLI to run the worker properly
        from livekit.agents import cli
        cli.run_app(worker_options)
        
    except KeyboardInterrupt:
        logger.info("\n👋 Shutting down LiveKit agent...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Error running LiveKit agent: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
