"""Shared loguru logger. Import `logger` from this module throughout the platform."""
import sys
from loguru import logger

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    colorize=True,
)
logger.add("logs/app_{time:YYYY-MM-DD}.log", rotation="10 MB", retention="30 days")
