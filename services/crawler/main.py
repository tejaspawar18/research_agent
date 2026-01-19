"""
Crawler Service - Multi-source article crawler for Preventive Health Pipeline.
"""
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
from datetime import datetime, date

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

import sys
sys.path.insert(0, '/app')

from shared.models import Article, CrawlJob, SourceConfig, ArticleStatus
from shared.utils import RedisManager, compute_url_hash, compute_hash
from shared.config import config

from rss_crawler import RSSCrawler
from pubmed_crawler import PubMedCrawler
from html_crawler import HTMLCrawler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redis_manager = RedisManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    await redis_manager.connect()
    config.load()
    logger.info(f"Crawler service started with {len(config.sources)} sources")
    yield
    await redis_manager.disconnect()
    logger.info("Crawler service stopped")


app = FastAPI(
    title="Crawler Service",
    description="Multi-source article crawler for preventive health research",
    version="1.0.0",
    lifespan=lifespan,
)


# Request/Response models
class CrawlRequest(BaseModel):
    """Request to trigger a crawl."""
    source_ids: Optional[List[str]] = None  # None = all enabled sources
    max_articles_per_source: int = 50


class CrawlResponse(BaseModel):
    """Response from crawl trigger."""
    job_id: str
    status: str
    message: str
    sources_queued: int


class CrawlJobStatus(BaseModel):
    """Status of a crawl job."""
    job_id: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    articles_found: int
    articles_new: int
    error: Optional[str] = None


# Crawler registry
CRAWLERS = {
    "rss": RSSCrawler,
    "atom": RSSCrawler,
    "pubmed_api": PubMedCrawler,
    "html_scrape": HTMLCrawler,
    "sciencedirect_scrape": HTMLCrawler,  # Uses same base HTML scraper
}


class CrawlerOrchestrator:
    """Orchestrates crawling across multiple sources."""
    
    def __init__(self):
        self._jobs: Dict[str, CrawlJob] = {}
    
    async def crawl_source(
        self,
        source: Any,
        max_articles: int = 50,
    ) -> List[Article]:
        """Crawl a single source."""
        crawler_class = CRAWLERS.get(source.crawl_method)
        
        if not crawler_class:
            logger.warning(f"No crawler for method: {source.crawl_method}")
            return []
        
        try:
            crawler = crawler_class(source)
            articles = await crawler.crawl(max_articles=max_articles)
            
            # Add hashes for deduplication
            for article in articles:
                article.url_hash = compute_url_hash(article.url)
                if article.abstract:
                    article.content_hash = compute_hash(article.abstract)
                article.title_hash = compute_hash(article.title) if article.title else None
            
            logger.info(f"Crawled {len(articles)} articles from {source.name}")
            return articles
            
        except Exception as e:
            logger.error(f"Error crawling {source.name}: {e}")
            return []
    
    async def run_crawl_job(
        self,
        job: CrawlJob,
        source_ids: Optional[List[str]],
        max_articles_per_source: int,
    ):
        """Run a crawl job."""
        job.status = "running"
        job.started_at = datetime.utcnow()
        self._jobs[job.job_id] = job
        
        try:
            # Get sources to crawl
            sources = config.sources
            if source_ids:
                sources = [s for s in sources if s.source_id in source_ids]
            
            all_articles = []
            
            # Crawl sources with rate limiting
            for source in sources:
                # Check rate limit
                rate_key = f"crawl_rate:{source.source_id}"
                allowed = await redis_manager.rate_limit_check(
                    rate_key, source.rate_limit, 60
                )
                
                if not allowed:
                    logger.warning(f"Rate limited for {source.name}")
                    continue
                
                articles = await self.crawl_source(source, max_articles_per_source)
                
                # Check for duplicates using Redis cache
                new_articles = []
                for article in articles:
                    if article.url_hash:
                        is_seen = await redis_manager.check_url_seen(article.url_hash)
                        if not is_seen:
                            new_articles.append(article)
                            await redis_manager.mark_url_seen(
                                article.url_hash, article.article_id
                            )
                
                all_articles.extend(new_articles)
                job.articles_found += len(articles)
                job.articles_new = len(new_articles)
                
                # Small delay between sources
                await asyncio.sleep(1)
            
            # Push articles to queue for next pipeline step
            for article in all_articles:
                await redis_manager.push_queue(
                    "articles:crawled",
                    article.model_dump_json()
                )
            
            job.status = "completed"
            logger.info(
                f"Crawl job {job.job_id} completed: "
                f"{job.articles_found} found, {job.articles_new} new"
            )
            
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            logger.error(f"Crawl job {job.job_id} failed: {e}")
        
        finally:
            job.completed_at = datetime.utcnow()
            self._jobs[job.job_id] = job
    
    def get_job(self, job_id: str) -> Optional[CrawlJob]:
        """Get job by ID."""
        return self._jobs.get(job_id)


orchestrator = CrawlerOrchestrator()


@app.post("/crawl/trigger", response_model=CrawlResponse)
async def trigger_crawl(request: CrawlRequest, background_tasks: BackgroundTasks):
    """Trigger a crawl job."""
    sources = config.sources
    if request.source_ids:
        sources = [s for s in sources if s.source_id in request.source_ids]
    
    job = CrawlJob(
        source_id="all" if not request.source_ids else ",".join(request.source_ids),
        source_name="Multiple sources",
    )
    
    background_tasks.add_task(
        orchestrator.run_crawl_job,
        job,
        request.source_ids,
        request.max_articles_per_source,
    )
    
    return CrawlResponse(
        job_id=job.job_id,
        status="started",
        message=f"Crawl job started for {len(sources)} sources",
        sources_queued=len(sources),
    )


@app.get("/crawl/status/{job_id}", response_model=CrawlJobStatus)
async def get_crawl_status(job_id: str):
    """Get crawl job status."""
    job = orchestrator.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return CrawlJobStatus(
        job_id=job.job_id,
        status=job.status,
        started_at=job.started_at,
        completed_at=job.completed_at,
        articles_found=job.articles_found,
        articles_new=job.articles_new,
        error=job.error_message,
    )


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
                "source_type": s.source_type,
                "quality_tier": s.quality_tier,
                "crawl_method": s.crawl_method,
                "enabled": s.enabled,
            }
            for s in config.all_sources
        ]
    }


@app.post("/sources/{source_id}/crawl")
async def crawl_single_source(
    source_id: str,
    max_articles: int = 50,
    background_tasks: BackgroundTasks = None,
):
    """Crawl a single source."""
    source = config.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    
    articles = await orchestrator.crawl_source(source, max_articles)
    
    # Push to queue
    for article in articles:
        await redis_manager.push_queue(
            "articles:crawled",
            article.model_dump_json()
        )
    
    return {
        "source_id": source_id,
        "articles_found": len(articles),
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "crawler",
        "sources_configured": len(config.sources),
    }