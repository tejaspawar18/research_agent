"""
Run the pipeline once. Intended for local testing / cron entry.
Usage:
    python scripts/run_daily.py
"""

from core.logger import get_logger
from pipeline.orchestrator import run_pipeline

logger = get_logger("run_daily")

if __name__ == "__main__":
    logger.info("Starting daily runner...")
    run_pipeline()
