"""
Simple scheduler using cron-like invocation:
This module exposes a `schedule_daily()` function which can be called from OS cron
or from a process supervisor. For Option A (local), simplest is to run scripts/run_daily.py
via crontab.

I keep this minimal: it's just a thin wrapper around run_pipeline.
"""

from pipeline.orchestrator import run_pipeline
from core.logger import get_logger

logger = get_logger("scheduler")

def schedule_daily():
    logger.info("Scheduled daily run triggered.")
    run_pipeline()
