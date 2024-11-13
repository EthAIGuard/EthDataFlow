# src/utils/logger.py

from loguru import logger
import sys

logger.remove()  # Clear previous logging handlers
logger.add(sys.stdout, level="INFO")
