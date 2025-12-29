"""
Simple file-backed failed queue.
Stores failed items to data/failed_queue.ndjson as newline-delimited json.
Allows pushing and reading for manual retries.
"""

import os
import json
from datetime import datetime
from core.logger import get_logger
from core.utils import ensure_dir

logger = get_logger("failed_queue")

class FailedQueue:
    QUEUE_FILE = ensure_dir("data") + "/failed_queue.ndjson"

    @staticmethod
    def push(item, error):
        payload = {
            "item": item,
            "error": error,
            "failed_at": datetime.utcnow().isoformat()
        }
        with open(FailedQueue.QUEUE_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
        logger.info(f"Pushed to failed queue: {item.get('url')}")

    @staticmethod
    def read_all():
        if not os.path.exists(FailedQueue.QUEUE_FILE):
            return []
        items = []
        with open(FailedQueue.QUEUE_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    items.append(json.loads(line))
                except:
                    continue
        return items

    @staticmethod
    def clear():
        if os.path.exists(FailedQueue.QUEUE_FILE):
            os.remove(FailedQueue.QUEUE_FILE)
            logger.info("Cleared failed queue.")
