"""
Main orchestrator:
 - Loads sources.yaml
 - Discovers new article URLs (RSS / sitemap / list pages)
 - Deduplicates
 - Submits jobs to ThreadPoolExecutor
 - Ensures Scylla schema
 - Collects simple metrics
"""

import yaml
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from core.logger import get_logger
from discovery import get_rss_articles, get_sitemap_urls, get_list_page_articles, dedupe_urls
from pipeline.article_worker import process_article
from storage.scylla_client import get_scylla_session
from storage.migrations import ensure_schema
from pipeline.failed_queue import FailedQueue

logger = get_logger("orchestrator")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def load_sources(config_path=None):
    config_path = config_path or os.path.join(BASE_DIR, "config", "sources.yaml")
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("sources", [])

def discover_all(sources):
    all_urls = []
    for s in sources:
        fetch_method = s.get("fetch_method", "rss")
        try:
            if fetch_method == "rss":
                items = get_rss_articles(s)
            elif fetch_method == "sitemap":
                items = get_sitemap_urls(s)
            elif fetch_method == "list_page":
                items = get_list_page_articles(s)
            else:
                logger.warning(f"Unknown fetch_method {fetch_method} for {s.get('name')}")
                items = []
            # annotate source for downstream
            for it in items:
                it.setdefault("source", s.get("name"))
                it.setdefault("selectors", s.get("selectors", {}))
            all_urls.extend(items)
        except Exception as e:
            logger.error(f"Discovery failed for {s.get('name')}: {e}")
    # dedupe
    deduped = dedupe_urls(all_urls)
    logger.info(f"Discovered {len(deduped)} unique items.")
    return deduped

def run_pipeline(max_workers=4, config_path=None):
    logger.info("Starting orchestrator")
    sources = load_sources(config_path)
    urls = discover_all(sources)

    # ensure scylla schema
    # session = get_scylla_session()
    # ensure_schema(session)
    session = None
    failed_q = FailedQueue()
    
    # Use ThreadPool for IO-heavy tasks (downloads, network)
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = {}
        for item in urls:
            future = exe.submit(process_article, item, session)
            futures[future] = item

        for fut in as_completed(futures):
            item = futures[fut]
            try:
                res = fut.result()
                if res.get("status") == "ok":
                    logger.info(f"Processed {item.get('url')} -> paper_id={res.get('paper_id')}")
                else:
                    logger.warning(f"Failed processing {item.get('url')}: {res.get('error')}")
                    failed_q.push(item, res.get("error"))
            except Exception as e:
                logger.exception(f"Unhandled exception for {item.get('url')}: {e}")
                failed_q.push(item, str(e))

    logger.info("Orchestrator run complete.")
