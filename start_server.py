"""
Startup script for Exotel AI Voice System

This script:
1. Checks environment variables
2. Optionally starts ngrok tunnel
3. Starts the FastAPI server
"""
import os
import sys
import subprocess
import time
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

# Configure logging
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Console handler with UTF-8 support for Windows
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)
# Force UTF-8 encoding for the stream
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# File handler for all server logs
file_handler = RotatingFileHandler(
    logs_dir / "server.log",
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

# File handler specifically for outgoing calls
outgoing_calls_handler = RotatingFileHandler(
    logs_dir / "outgoing_calls.log",
    maxBytes=10*1024*1024,  # 10MB
    backupCount=10
)
outgoing_calls_handler.setLevel(logging.INFO)
outgoing_calls_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)
outgoing_calls_handler.setFormatter(outgoing_calls_formatter)

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[console_handler, file_handler]
)

# Create outgoing calls logger
outgoing_calls_logger = logging.getLogger("outgoing_calls")
outgoing_calls_logger.setLevel(logging.INFO)
outgoing_calls_logger.addHandler(outgoing_calls_handler)
# Prevent propagation to avoid duplicate logs
outgoing_calls_logger.propagate = False

logger = logging.getLogger(__name__)


def check_env_file():
    """Check if .env file exists and has required variables"""
    env_path = Path(".env")
    
    if not env_path.exists():
        logger.error("❌ .env file not found!")
        logger.info("📝 Please create .env file from .env.example")
        logger.info("   Copy command: copy .env.example .env")
        return False
    
    # Check required variables
    required_vars = [
        "EXOTEL_SID",
        "EXOTEL_AUTH_TOKEN",
        "EXOTEL_VIRTUAL_NUMBER",
        "LIVEKIT_API_KEY",
        "LIVEKIT_API_SECRET",
        "LIVEKIT_WS_URL",
        "OPENAI_API_KEY"
    ]
    
    missing = []
    for var in required_vars:
        value = os.getenv(var)
        if not value or value.startswith("your_"):
            missing.append(var)
    
    if missing:
        logger.error(f"❌ Missing or unset environment variables: {', '.join(missing)}")
        logger.info("📝 Please update your .env file with actual values")
        return False
    
    logger.info("✅ Environment variables configured")
    return True


def start_ngrok(port: int = 8000):
    """Start ngrok tunnel"""
    try:
        # Check if ngrok is installed
        result = subprocess.run(
            ["ngrok", "version"],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.warning("⚠️  ngrok not found. Please install ngrok first.")
            logger.info("   Download: https://ngrok.com/download")
            return None
        
        logger.info("🚀 Starting ngrok tunnel...")
        
        # Start ngrok in background
        ngrok_process = subprocess.Popen(
            ["ngrok", "http", str(port), "--log=stdout"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for ngrok to start
        time.sleep(3)
        
        # Get ngrok URL from API
        import urllib.request
        import json
        
        try:
            with urllib.request.urlopen("http://localhost:4040/api/tunnels") as response:
                data = json.loads(response.read().decode())
                public_url = data["tunnels"][0]["public_url"]
                logger.info(f"✅ ngrok tunnel started: {public_url}")
                return public_url
        except Exception as e:
            logger.warning(f"⚠️  Could not get ngrok URL: {e}")
            logger.info("   Please check ngrok manually at http://localhost:4040")
            return None
            
    except FileNotFoundError:
        logger.warning("⚠️  ngrok command not found")
        return None


def main():
    """Main startup function"""
    logger.info("=" * 60)
    logger.info("🎙️  Exotel AI Voice System - Startup")
    logger.info("=" * 60)
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Check environment
    if not check_env_file():
        sys.exit(1)
    
    # Ask about ngrok
    use_ngrok = input("\n🌐 Start ngrok tunnel? (y/n): ").lower().strip() == "y"
    
    ngrok_url = None
    if use_ngrok:
        ngrok_url = start_ngrok()
        if ngrok_url:
            logger.info(f"\n📋 Update your Exotel webhook URL to:")
            logger.info(f"   {ngrok_url}/webhook/voicebot\n")
            logger.info("⏳ Waiting 5 seconds for ngrok to stabilize...")
            time.sleep(5)
    
    # Start the server
    logger.info("🚀 Starting FastAPI server...")
    logger.info("   URL: http://localhost:8000")
    logger.info("   Health: http://localhost:8000/health")
    logger.info("   Webhook: http://localhost:8000/webhook/voicebot")
    logger.info("\nPress Ctrl+C to stop\n")
    
    try:
        import uvicorn
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level="info"
        )
    except KeyboardInterrupt:
        logger.info("\n👋 Shutting down...")
        sys.exit(0)


if __name__ == "__main__":
    main()
