
"""
Crawler Service - Multi-source article crawler with direct DB storage.

New architecture:
1. Crawls all sources using AdaptiveCrawler (RSS → HTML → API)
2. Performs inline deduplication (fast hash checks)
3. Stores unique articles directly to ScyllaDB with status='unique'
4. Can run as cron job or on-demand via API
"""
import logging
import asyncio
import os
from contextlib import asynccontextmanager
from typing import List, Dict, Optional
from datetime import datetime, date

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import sys
sys.path.insert(0, '/app')

from shared.models import Article, CrawlJob, ArticleStatus
from shared.utils import (
    ScyllaDBManager, RedisManager, 
    compute_url_hash, compute_hash
)
from shared.config import config
from shared.utils.metrics import add_metrics_endpoint, ARTICLES_CRAWLED

from adaptive_crawler import AdaptiveCrawler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_manager = ScyllaDBManager()
redis_manager = RedisManager()
scheduler = AsyncIOScheduler()

# Configuration
CRON_SCHEDULE = os.getenv("CRON_SCHEDULE", config.pipeline.cron_schedule)
MAX_ARTICLES_PER_SOURCE = int(os.getenv("MAX_ARTICLES_PER_SOURCE", str(config.pipeline.default_max_articles_per_source)))
RUN_MODE = os.getenv("RUN_MODE", "cron")  # cron, api


class CrawlStats:
    """Track crawl statistics."""
    def __init__(self):
        self.total_found = 0
        self.total_unique = 0
        self.total_duplicates = 0
        self.by_source: Dict[str, Dict] = {}
        self.last_run: Optional[datetime] = None
        self.is_running = False
        self.current_source: Optional[str] = None


stats = CrawlStats()


async def is_duplicate(article: Article) -> bool:
    """
    Fast duplicate check using Redis cache and DB.
    Returns True if article is a duplicate.
    """
    # Check URL hash in Redis (fast)
    if article.url_hash:
        if await redis_manager.check_url_seen(article.url_hash):
            return True
    
    # Check content hash if available
    if article.content_hash:
        if await redis_manager.check_content_seen(article.content_hash):
            return True
    
    # Check title hash
    if article.title_hash:
        existing = db_manager.check_dedup_hash("title", article.title_hash)
        if existing:
            return True
    
    # Check DOI
    if article.doi:
        existing = db_manager.check_dedup_hash("doi", article.doi)
        if existing:
            return True
    
    # Check PMID
    if article.pmid:
        existing = db_manager.check_dedup_hash("pmid", article.pmid)
        if existing:
            return True
    
    return False


async def mark_as_seen(article: Article):
    """Mark article as seen in dedup indexes."""
    # Redis cache
    if article.url_hash:
        await redis_manager.mark_url_seen(article.url_hash, article.article_id)
    
    if article.content_hash:
        await redis_manager.mark_content_seen(article.content_hash, article.article_id)
    
    # ScyllaDB indexes
    if article.title_hash:
        db_manager.add_dedup_hash("title", article.title_hash, article.article_id, article.source_id)
    
    if article.doi:
        db_manager.add_dedup_hash("doi", article.doi, article.article_id, article.source_id)
    
    if article.pmid:
        db_manager.add_dedup_hash("pmid", article.pmid, article.article_id, article.source_id)


async def crawl_source(source) -> Dict:
    """Crawl a single source and store unique articles."""
    source_stats = {
        "source_id": source.source_id,
        "name": source.name,
        "found": 0,
        "unique": 0,
        "duplicates": 0,
        "errors": [],
    }
    
    try:
        logger.info(f"Crawling {source.name}...")
        stats.current_source = source.name
        
        # Use adaptive crawler
        crawler = AdaptiveCrawler(source)
        articles = await crawler.crawl(max_articles=MAX_ARTICLES_PER_SOURCE)
        
        source_stats["found"] = len(articles)

        # Track articles found per source for Grafana
        ARTICLES_CRAWLED.labels(
            source_id=source.source_id,
            source_name=source.name,
            quality_tier=source.quality_tier,
        ).inc(len(articles))

        for article in articles:
            # Compute hashes
            article.url_hash = compute_url_hash(article.url)
            article.title_hash = compute_hash(article.title) if article.title else None
            if article.abstract:
                article.content_hash = compute_hash(article.abstract)
            
            # Check for duplicates
            if await is_duplicate(article):
                source_stats["duplicates"] += 1
                continue
            
            # Mark as seen
            await mark_as_seen(article)
            
            # Update status and save to DB
            article.status = ArticleStatus.UNIQUE
            article.crawled_at = datetime.utcnow()
            
            try:
                db_manager.insert_article(article.model_dump())
                source_stats["unique"] += 1
            except Exception as e:
                logger.error(f"Failed to insert article: {e}")
                source_stats["errors"].append(str(e))
        
        logger.info(
            f"✓ {source.name}: {source_stats['unique']} unique / "
            f"{source_stats['found']} found ({source_stats['duplicates']} duplicates)"
        )
        
    except Exception as e:
        logger.error(f"✗ {source.name} failed: {e}")
        source_stats["errors"].append(str(e))
    
    return source_stats


async def run_crawl_cycle(source_ids: Optional[List[str]] = None):
    """Run one complete crawl cycle."""
    if stats.is_running:
        logger.warning("Crawl already running, skipping")
        return
    
    stats.is_running = True
    stats.last_run = datetime.utcnow()
    stats.total_found = 0
    stats.total_unique = 0
    stats.total_duplicates = 0
    stats.by_source = {}
    
    try:
        logger.info("=" * 60)
        logger.info("Starting crawl cycle...")
        logger.info("=" * 60)
        
        # Get sources to crawl
        sources = config.sources
        if source_ids:
            sources = [s for s in sources if s.source_id in source_ids]
        
        logger.info(f"Crawling {len(sources)} sources...")
        
        for source in sources:
            # Check rate limit
            rate_key = f"crawl_rate:{source.source_id}"
            allowed = await redis_manager.rate_limit_check(
                rate_key, source.rate_limit, config.pipeline.crawler_rate_limit_window
            )
            
            if not allowed:
                logger.warning(f"Rate limited: {source.name}")
                continue
            
            # Crawl source
            source_stats = await crawl_source(source)
            stats.by_source[source.source_id] = source_stats
            
            # Update totals
            stats.total_found += source_stats["found"]
            stats.total_unique += source_stats["unique"]
            stats.total_duplicates += source_stats["duplicates"]
            
            # Small delay between sources
            await asyncio.sleep(config.pipeline.crawler_source_delay)
        
        logger.info("=" * 60)
        logger.info(
            f"Crawl complete: {stats.total_unique} unique / "
            f"{stats.total_found} found ({stats.total_duplicates} duplicates)"
        )
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Crawl cycle failed: {e}")
    
    finally:
        stats.is_running = False
        stats.current_source = None


def setup_scheduler():
    """Setup cron scheduler for crawling."""
    try:
        parts = CRON_SCHEDULE.split()
        if len(parts) == 5:
            trigger = CronTrigger(
                minute=parts[0],
                hour=parts[1],
                day=parts[2],
                month=parts[3],
                day_of_week=parts[4]
            )
        else:
            trigger = CronTrigger(hour="*/2")  # Default: every 2 hours
        
        scheduler.add_job(
            run_crawl_cycle,
            trigger,
            id="crawler",
            replace_existing=True
        )
        logger.info(f"Scheduled crawl: {CRON_SCHEDULE}")
        
    except Exception as e:
        logger.error(f"Failed to setup scheduler: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    db_manager.connect()
    await redis_manager.connect()
    config.load()
    
    if RUN_MODE == "cron":
        setup_scheduler()
        scheduler.start()
    
    logger.info(f"Crawler service started (mode: {RUN_MODE})")
    yield
    
    if RUN_MODE == "cron":
        scheduler.shutdown()
    db_manager.disconnect()
    await redis_manager.disconnect()


app = FastAPI(
    title="Crawler Service",
    description="Multi-source article crawler with adaptive fallback",
    version="2.0.0",
    lifespan=lifespan,
)
add_metrics_endpoint(app)


# Request/Response models
class CrawlRequest(BaseModel):
    source_ids: Optional[List[str]] = None
    max_articles_per_source: int = 50


class CrawlResponse(BaseModel):
    status: str
    message: str


@app.post("/crawl/trigger", response_model=CrawlResponse)
async def trigger_crawl(
    request: CrawlRequest,
    background_tasks: BackgroundTasks
):
    """Trigger a crawl cycle."""
    if stats.is_running:
        return CrawlResponse(
            status="busy",
            message=f"Crawl already running: {stats.current_source}"
        )
    
    global MAX_ARTICLES_PER_SOURCE
    MAX_ARTICLES_PER_SOURCE = request.max_articles_per_source
    
    background_tasks.add_task(run_crawl_cycle, request.source_ids)
    
    sources_count = len(request.source_ids) if request.source_ids else len(config.sources)
    
    return CrawlResponse(
        status="started",
        message=f"Crawl triggered for {sources_count} sources"
    )


@app.get("/crawl/status")
async def get_status():
    """Get current crawl status."""
    return {
        "is_running": stats.is_running,
        "current_source": stats.current_source,
        "last_run": stats.last_run.isoformat() if stats.last_run else None,
        "total_found": stats.total_found,
        "total_unique": stats.total_unique,
        "total_duplicates": stats.total_duplicates,
        "by_source": stats.by_source,
    }


@app.get("/sources")
async def list_sources():
    """List all configured sources."""
    return {
        "total": len(config.all_sources),
        "enabled": len(config.sources),
        "sources": [
            {
                "source_id": s.source_id,
                "name": s.name,
                "url": s.url,
                "quality_tier": s.quality_tier,
                "crawl_method": s.crawl_method,
                "enabled": s.enabled,
            }
            for s in config.all_sources
        ]
    }


@app.post("/sources/{source_id}/crawl")
async def crawl_single_source(source_id: str):
    """Crawl a single source."""
    source = config.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    
    source_stats = await crawl_source(source)
    
    return source_stats


@app.get("/health")
async def health():
    """Health check."""
    return {
        "status": "healthy",
        "service": "crawler",
        "sources_configured": len(config.sources),
        "is_running": stats.is_running,
        "mode": RUN_MODE,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
