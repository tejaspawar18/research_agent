"""
Simple test harness to run a single URL through the entire pipeline for debugging.

Usage:
    python scripts/test_source.py "<url>" "<source_name>"
"""

import sys
from storage.scylla_client import get_scylla_session
from core.logger import get_logger
from pipeline.article_worker import process_article

logger = get_logger("test_source")

def main(url, source="test"):
    item = {
        "url": url,
        "source": source,
        "title": None,
        "published": None,
        "selectors": {}
    }
    logger.info(f'Testing URL: {url} from source: {source}')
    # session = get_scylla_session()
    logger.info('Starting test processing...')
    session=None
    res = process_article(item, session)
    logger.info(f"Test result: {res}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_source.py <url> [source_name]")
        sys.exit(1)
    url = sys.argv[1]
    src = sys.argv[2] if len(sys.argv) > 2 else "test"
    main(url, src)
