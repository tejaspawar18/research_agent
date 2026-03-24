"""
Extraction Worker - Parallel full-text extraction with independent scheduling.

This worker runs independently of the main pipeline and processes articles
that are in 'unique' status, extracting full text and updating to 'extracted'.

Can run as:
1. Cron job (every 30 minutes)
2. Continuous worker (processes queue)
3. On-demand via API
"""
import logging
import asyncio
import os
from typing import List, Optional
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import sys
sys.path.insert(0, '/app')

from shared.models import Article, ArticleStatus
from shared.utils import ScyllaDBManager, RedisManager, S3Manager
from shared.config import config

from pmc_extractor import PMCExtractor
from pdf_extractor import PDFExtractor
from html_extractor import HTMLExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_manager = ScyllaDBManager()
redis_manager = RedisManager()
s3_manager = S3Manager()
scheduler = AsyncIOScheduler()

# Configuration
WORKER_ID = os.getenv("WORKER_ID", "1")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", str(config.pipeline.extraction_worker_batch_size)))
PARALLEL_WORKERS = int(os.getenv("PARALLEL_WORKERS", str(config.pipeline.extraction_parallel_workers)))
CRON_SCHEDULE = os.getenv("CRON_SCHEDULE", config.pipeline.extraction_worker_cron)
RUN_MODE = os.getenv("RUN_MODE", "cron")  # cron, continuous, api


class ExtractionStats:
    """Track extraction statistics."""
    def __init__(self):
        self.total_processed = 0
        self.successful = 0
        self.failed = 0
        self.by_method = {"pmc": 0, "pdf": 0, "html": 0}
        self.last_run = None
        self.is_running = False


stats = ExtractionStats()


async def extract_single_article(article: Article) -> Article:
    """
    Extract full text for a single article.
    Tries methods in order: PMC → PDF → HTML
    """
    full_text = None
    method_used = None
    
    try:
        api_key = os.getenv("PUBMED_API_KEY")
        
        # Method 1: PMC API (best quality for PubMed articles)
        if article.pmid and not full_text:
            # Try to get PMC ID from PMID
            pmc_id = await get_pmc_id_from_pmid(article.pmid, api_key)
            if pmc_id:
                full_text = await PMCExtractor.extract(pmc_id, api_key)
                if full_text:
                    method_used = "pmc"
                    logger.debug(f"PMC extraction successful for {article.url}")
        
        # Method 2: PDF extraction
        if article.pdf_url and not full_text:
            full_text = await PDFExtractor.extract(article.pdf_url)
            if full_text:
                method_used = "pdf"
                logger.debug(f"PDF extraction successful for {article.url}")
        
        # Method 3: HTML extraction (most common)
        if not full_text:
            full_text = await HTMLExtractor.extract(article.url)
            if full_text:
                method_used = "html"
                logger.debug(f"HTML extraction successful for {article.url}")
        
        # Update article
        if full_text and len(full_text) > config.pipeline.min_fulltext_length:
            article.full_text = full_text
            article.status = ArticleStatus.EXTRACTED
            article.processed_at = datetime.utcnow()
            stats.successful += 1
            stats.by_method[method_used] += 1

            # Upload raw article to S3
            try:
                s3_manager.upload_article(
                    article_dict=article.model_dump(),
                    published_date=article.published_date,
                )
            except Exception as s3_err:
                logger.warning(f"S3 article upload failed for {article.url}: {s3_err}")
        else:
            article.status = ArticleStatus.FAILED
            article.error_message = "Could not extract full text"
            stats.failed += 1
            
    except Exception as e:
        logger.error(f"Extraction failed for {article.url}: {e}")
        article.status = ArticleStatus.FAILED
        article.error_message = str(e)
        stats.failed += 1
    
    stats.total_processed += 1
    return article


async def get_pmc_id_from_pmid(pmid: str, api_key: Optional[str] = None) -> Optional[str]:
    """Convert PMID to PMC ID using NCBI API."""
    import aiohttp
    
    try:
        url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
        params = {
            "ids": pmid,
            "format": "json",
        }
        if api_key:
            params["api_key"] = api_key
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=config.pipeline.pmid_lookup_timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    records = data.get("records", [])
                    if records and records[0].get("pmcid"):
                        return records[0]["pmcid"]
    except Exception as e:
        logger.debug(f"PMC ID lookup failed for {pmid}: {e}")
    
    return None


async def process_batch(articles: List[Article]) -> List[Article]:
    """Process a batch of articles in parallel."""
    semaphore = asyncio.Semaphore(PARALLEL_WORKERS)
    
    async def process_with_semaphore(article: Article) -> Article:
        async with semaphore:
            return await extract_single_article(article)
    
    tasks = [process_with_semaphore(article) for article in articles]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    processed = []
    for article, result in zip(articles, results):
        if isinstance(result, Exception):
            article.status = ArticleStatus.FAILED
            article.error_message = str(result)
            processed.append(article)
        else:
            processed.append(result)
    
    return processed


async def run_extraction_cycle():
    """Run one extraction cycle."""
    if stats.is_running:
        logger.warning("Extraction already running, skipping")
        return
    
    stats.is_running = True
    stats.last_run = datetime.utcnow()
    
    try:
        logger.info(f"[Worker {WORKER_ID}] Starting extraction cycle...")
        
        # Get articles pending extraction
        articles = db_manager.get_articles_by_status(
            status="unique",
            limit=BATCH_SIZE
        )
        
        if not articles:
            logger.info(f"[Worker {WORKER_ID}] No articles pending extraction")
            return
        
        logger.info(f"[Worker {WORKER_ID}] Processing {len(articles)} articles...")
        
        # Process in parallel
        processed = await process_batch(articles)
        
        # Update database
        for article in processed:
            try:
                db_manager.update_article_status(
                    source_id=article.source_id,
                    published_date=article.published_date,
                    article_id=article.article_id,
                    status=article.status.value,
                    full_text=article.full_text,
                    processed_at=article.processed_at,
                    error_message=article.error_message,
                )
            except Exception as e:
                logger.error(f"Failed to update article {article.article_id}: {e}")
        
        successful = len([a for a in processed if a.status == ArticleStatus.EXTRACTED])
        logger.info(
            f"[Worker {WORKER_ID}] Extraction complete: "
            f"{successful}/{len(articles)} successful"
        )
        
    except Exception as e:
        logger.error(f"[Worker {WORKER_ID}] Extraction cycle failed: {e}")
    
    finally:
        stats.is_running = False


async def run_continuous():
    """Run extraction continuously."""
    logger.info(f"[Worker {WORKER_ID}] Starting continuous mode...")
    
    while True:
        await run_extraction_cycle()
        
        # Wait before next cycle
        await asyncio.sleep(config.pipeline.extraction_continuous_interval)


def setup_scheduler():
    """Setup cron scheduler for extraction."""
    try:
        # Parse cron schedule
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
            # Default to every 30 minutes
            trigger = CronTrigger(minute="*/30")
        
        scheduler.add_job(
            run_extraction_cycle,
            trigger,
            id="extraction_worker",
            replace_existing=True
        )
        logger.info(f"[Worker {WORKER_ID}] Scheduled extraction: {CRON_SCHEDULE}")
        
    except Exception as e:
        logger.error(f"Failed to setup scheduler: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    db_manager.connect()
    await redis_manager.connect()
    s3_manager.connect()
    config.load()
    
    if RUN_MODE == "cron":
        setup_scheduler()
        scheduler.start()
    elif RUN_MODE == "continuous":
        asyncio.create_task(run_continuous())
    
    logger.info(f"[Worker {WORKER_ID}] Extraction worker started (mode: {RUN_MODE})")
    yield
    
    if RUN_MODE == "cron":
        scheduler.shutdown()
    db_manager.disconnect()
    await redis_manager.disconnect()


app = FastAPI(
    title=f"Extraction Worker {WORKER_ID}",
    description="Parallel full-text extraction service",
    version="2.0.0",
    lifespan=lifespan,
)


# API Endpoints
class TriggerRequest(BaseModel):
    batch_size: int = 50


class TriggerResponse(BaseModel):
    status: str
    message: str


@app.post("/extract/trigger", response_model=TriggerResponse)
async def trigger_extraction(
    request: TriggerRequest,
    background_tasks: BackgroundTasks
):
    """Manually trigger extraction cycle."""
    if stats.is_running:
        return TriggerResponse(
            status="busy",
            message="Extraction already running"
        )
    
    global BATCH_SIZE
    BATCH_SIZE = request.batch_size
    
    background_tasks.add_task(run_extraction_cycle)
    
    return TriggerResponse(
        status="started",
        message=f"Extraction triggered for {request.batch_size} articles"
    )


@app.get("/extract/stats")
async def get_stats():
    """Get extraction statistics."""
    return {
        "worker_id": WORKER_ID,
        "is_running": stats.is_running,
        "total_processed": stats.total_processed,
        "successful": stats.successful,
        "failed": stats.failed,
        "by_method": stats.by_method,
        "last_run": stats.last_run.isoformat() if stats.last_run else None,
        "config": {
            "batch_size": BATCH_SIZE,
            "parallel_workers": PARALLEL_WORKERS,
            "cron_schedule": CRON_SCHEDULE,
            "run_mode": RUN_MODE,
        }
    }


@app.get("/health")
async def health():
    """Health check."""
    return {
        "status": "healthy",
        "service": f"extraction-worker-{WORKER_ID}",
        "is_running": stats.is_running,
        "mode": RUN_MODE,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
