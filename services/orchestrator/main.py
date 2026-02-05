"""
Orchestrator Service - Pipeline coordination and scheduling.
"""
import logging
import asyncio
import os
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timezone, timedelta
import json

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import sys
sys.path.insert(0, '/app')

from shared.models import Article, PipelineRun, ArticleStatus, ProjectArea
from shared.utils import ScyllaDBManager, RedisManager, KafkaManager, S3Manager, initialize_schema
from shared.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_manager = ScyllaDBManager()
redis_manager = RedisManager()
kafka_manager = KafkaManager()
s3_manager = S3Manager()
scheduler = AsyncIOScheduler()

CRAWLER_URL = os.getenv("CRAWLER_URL", "http://crawler:8001")
DEDUP_URL = os.getenv("DEDUP_URL", "http://dedup:8002")
EXTRACTION_URL = os.getenv("EXTRACTION_URL", "http://extraction:8003")
LLM_URL = os.getenv("LLM_URL", "http://llm:8004")
NOTIFICATION_URL = os.getenv("NOTIFICATION_URL", "http://notification:8005")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db_manager.connect()
    initialize_schema(db_manager)
    await redis_manager.connect()
    await kafka_manager.connect()
    s3_manager.connect()
    config.load()
    setup_scheduler()
    scheduler.start()
    logger.info("Orchestrator service started")
    yield
    scheduler.shutdown()
    await kafka_manager.disconnect()
    db_manager.disconnect()
    await redis_manager.disconnect()


app = FastAPI(title="Orchestrator Service", version="1.0.0", lifespan=lifespan)


class PipelineRequest(BaseModel):
    source_ids: Optional[List[str]] = None
    skip_crawl: bool = False
    skip_extraction: bool = False
    skip_llm: bool = False
    skip_notify: bool = False


class PipelineResponse(BaseModel):
    run_id: str
    status: str
    message: str


class PipelineOrchestrator:
    def __init__(self):
        self._runs: Dict[str, PipelineRun] = {}
        self._current: Optional[PipelineRun] = None
    
    async def run_pipeline(
        self,
        source_ids: Optional[List[str]] = None,
        skip_crawl: bool = False,
        skip_extraction: bool = False,
        skip_llm: bool = False,
        skip_notify: bool = False,
    ) -> PipelineRun:
        run = PipelineRun()
        self._runs[run.run_id] = run
        self._current = run
        
        logger.info(f"Starting pipeline run {run.run_id}")

        # Publish pipeline start event to Kafka
        await kafka_manager.publish_event("pipeline_started", {
            "run_id": run.run_id,
            "source_ids": source_ids,
            "skip_crawl": skip_crawl,
            "skip_extraction": skip_extraction,
            "skip_llm": skip_llm,
            "skip_notify": skip_notify,
        })

        try:
            # Step 1: Crawl new articles
            articles = []
            if not skip_crawl:
                run.status = "crawling"
                articles = await self._crawl(source_ids)
                run.articles_crawled = len(articles)
                logger.info(f"Crawled {len(articles)} new articles")
            else:
                articles = await self._load_from_queue("articles:crawled")
                run.articles_crawled = len(articles)

            # Step 1b: Load incomplete articles from previous runs (yesterday + today only)
            # This allows resuming processing for articles that failed mid-pipeline
            run.status = "loading_incomplete"
            incomplete_articles = self._load_incomplete_articles(days=3, limit=500)

            # Log count and limit to 100 if more
            if incomplete_articles:
                total_incomplete = len(incomplete_articles)
                logger.info(f"Found {total_incomplete} incomplete articles from previous runs")

            # Separate incomplete articles by status - they bypass dedup
            incomplete_needs_extraction = [a for a in incomplete_articles if a.status in (ArticleStatus.UNIQUE, "unique", None)]
            incomplete_extracted = [a for a in incomplete_articles if a.status in (ArticleStatus.EXTRACTED, "extracted")]
            incomplete_processed = [a for a in incomplete_articles if a.status in (ArticleStatus.PROCESSED, "processed")]

            # NOTE: Do NOT merge incomplete_needs_extraction into `articles` for dedup.
            # These articles already passed dedup in a previous run (status=unique).
            # They will be added directly to needs_extraction after dedup step.

            # Publish crawled articles to Kafka
            if articles:
                await kafka_manager.publish_articles(
                    "crawled", [a.model_dump(mode='json') for a in articles], run.run_id
                )

            # Step 2: Deduplicate (only for new/unique articles, not already-extracted ones)
            run.status = "deduplicating"
            unique_articles = await self._deduplicate(articles)
            run.articles_deduplicated = len(unique_articles)
            logger.info(f"Deduplicated to {len(unique_articles)} articles")

            # Publish deduplicated articles to Kafka
            if unique_articles:
                await kafka_manager.publish_articles(
                    "deduplicated", [a.model_dump(mode='json') for a in unique_articles], run.run_id
                )

            # Separate articles by status for appropriate processing stage
            # Articles from dedup need extraction + incomplete articles with status='unique'
            # Incomplete articles with status='extracted' skip extraction, go to LLM
            # Incomplete articles with status='processed' skip to notification
            needs_extraction = unique_articles + incomplete_needs_extraction  # Dedup output + already-deduped incomplete
            already_extracted = incomplete_extracted  # From incomplete articles
            already_processed = incomplete_processed  # From incomplete articles

            logger.info(f"Status breakdown: {len(needs_extraction)} need extraction, {len(already_extracted)} already extracted, {len(already_processed)} already processed")

            # Step 3: Extract Full Text (only for articles that need it)
            extracted_articles = already_extracted  # Start with already extracted
            if not skip_extraction and needs_extraction:
                run.status = "extracting"
                newly_extracted = await self._extract_fulltext(needs_extraction)
                # Add articles: full_text preferred, abstract fallback for 100+ chars
                full_text_count = 0
                abstract_fallback_count = 0
                for a in newly_extracted:
                    if a.full_text:
                        extracted_articles.append(a)
                        full_text_count += 1
                    elif a.abstract and len(a.abstract) >= 100:
                        a.status = ArticleStatus.EXTRACTED
                        extracted_articles.append(a)
                        abstract_fallback_count += 1
                        # Update status in DB for abstract-fallback articles
                        try:
                            db_manager.update_article_status(
                                source_id=a.source_id,
                                published_date=a.published_date,
                                article_id=a.article_id,
                                status="extracted",
                            )
                        except Exception as db_err:
                            logger.warning(f"Failed to update abstract-fallback status for {a.article_id}: {db_err}")
                logger.info(f"Extracted: {full_text_count} full text, {abstract_fallback_count} abstract-only")

                # Publish extracted articles to Kafka
                await kafka_manager.publish_articles(
                    "extracted", [a.model_dump() for a in extracted_articles], run.run_id
                )

            # Step 4: LLM Processing (only for articles that need it)
            processed_articles = list(already_processed)  # Start with already processed
            if not skip_llm:
                run.status = "processing"
                newly_processed = await self._llm_process(extracted_articles)
                processed_articles.extend(newly_processed)
                run.articles_filtered = len(processed_articles)
                run.articles_summarized = len([a for a in processed_articles if a.summary])
                logger.info(f"Processed {len(newly_processed)} articles (+ {len(already_processed)} already processed)")
            else:
                processed_articles.extend(extracted_articles)

            # Publish processed articles to Kafka
            if processed_articles:
                await kafka_manager.publish_articles(
                    "processed", [a.model_dump() for a in processed_articles], run.run_id
                )

            # Save articles to JSON file for inspection
            articles_dicts = [article.model_dump() for article in processed_articles]
            try:
                os.makedirs("/app/data", exist_ok=True)
                output_file = f"/app/data/processed_articles_{run.run_id}.json"
                with open(output_file, "w") as f:
                    json.dump(articles_dicts, f, indent=2, default=str)
                logger.info(f"Saved {len(processed_articles)} articles to {output_file}")
            except Exception as e:
                logger.warning(f"Failed to save articles to JSON file: {e}")

            # Step 5: Store in ScyllaDB
            run.status = "storing"
            for article in processed_articles:
                try:
                    db_manager.insert_article(article.model_dump())
                except Exception as e:
                    logger.warning(f"Failed to store article: {e}")

            logger.info(f"Stored {len(processed_articles)} articles in ScyllaDB")

            # Step 5b: Backup to S3
            if articles_dicts:
                s3_manager.upload_articles(articles_dicts, run.run_id, stage="processed")

            # Step 6: Notify
            if not skip_notify and processed_articles:
                run.status = "notifying"
                notified = await self._notify(processed_articles)
                run.articles_notified = notified
                logger.info(f"Notified {notified} articles")

            run.status = "completed"
            run.completed_at = datetime.utcnow()
            logger.info(f"Pipeline {run.run_id} completed successfully")

            # Publish pipeline completed event
            await kafka_manager.publish_event("pipeline_completed", {
                "run_id": run.run_id,
                "articles_crawled": run.articles_crawled,
                "articles_deduplicated": run.articles_deduplicated,
                "articles_filtered": run.articles_filtered,
                "articles_summarized": run.articles_summarized,
                "articles_notified": run.articles_notified,
            })

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            run.status = "failed"
            run.errors.append(str(e))

            # Publish pipeline failed event
            await kafka_manager.publish_event("pipeline_failed", {
                "run_id": run.run_id,
                "error": str(e),
            })

        finally:
            self._current = None
            self._runs[run.run_id] = run
            run_data = run.model_dump()
            db_manager.insert_pipeline_run(run_data)
            s3_manager.upload_pipeline_run(run_data, run.run_id)
        
        return run
    
    async def _crawl(self, source_ids: Optional[List[str]]) -> List[Article]:
        try:
            async with httpx.AsyncClient(timeout=600.0) as client:
                # Trigger crawl (v2 crawler stores directly to ScyllaDB)
                response = await client.post(
                    f"{CRAWLER_URL}/crawl/trigger",
                    json={"source_ids": source_ids, "max_articles_per_source": 50}
                )
                response.raise_for_status()
                trigger_result = response.json()

                if trigger_result.get("status") == "busy":
                    logger.warning("Crawler is busy, waiting...")

                # Poll /crawl/status until is_running becomes False
                for _ in range(120):
                    await asyncio.sleep(5)
                    status_resp = await client.get(f"{CRAWLER_URL}/crawl/status")
                    status = status_resp.json()
                    if not status.get("is_running", False):
                        logger.info(
                            f"Crawl finished: {status.get('total_unique', 0)} unique / "
                            f"{status.get('total_found', 0)} found"
                        )
                        break

                # Load unique articles from ScyllaDB (crawler stores with status='unique')
                articles = []
                today = date.today()
                if source_ids:
                    for sid in source_ids:
                        rows = db_manager.get_articles_by_date(sid, today, limit=100)
                        for row in rows:
                            if row.get("status") == "unique":
                                try:
                                    articles.append(Article.model_validate(row))
                                except Exception:
                                    pass
                else:
                    # Load from all sources for today
                    rows = db_manager.get_recent_articles(days=3, limit=500)
                    for row in rows:
                        if row.get("status") == "unique":
                            try:
                                articles.append(Article.model_validate(row))
                            except Exception:
                                pass

                logger.info(f"Loaded {len(articles)} unique articles from ScyllaDB")
                return articles

        except Exception as e:
            logger.error(f"Crawl failed: {e}")
            return []
    
    async def _deduplicate(self, articles: List[Article]) -> List[Article]:
        if not articles:
            return []

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    f"{DEDUP_URL}/dedup/batch",
                    json={"articles": [a.model_dump(mode='json') for a in articles]}
                )
                response.raise_for_status()
                result = response.json()

                unique = []
                for article, dedup_result in zip(articles, result["results"]):
                    if not dedup_result["is_duplicate"]:
                        unique.append(article)

                return unique
        except Exception as e:
            logger.error(f"Dedup failed: {e}")
            return articles

    async def _extract_fulltext(self, articles: List[Article]) -> List[Article]:
        """Extract full text for articles."""
        if not articles:
            return []

        extracted_count = 0

        try:
            # Use synchronous requests to avoid Windows Docker DNS issues
            import requests

            for article in articles:
                try:
                    # Prepare extraction request
                    extraction_data = {
                        "url": article.url,
                        "method": "auto"
                    }

                    # Add PMC ID if available
                    if hasattr(article, 'pmc_id') and article.pmc_id:
                        extraction_data["pmc_id"] = article.pmc_id

                    # Add PMID if available
                    if hasattr(article, 'pmid') and article.pmid:
                        extraction_data["pmid"] = article.pmid

                    # Add DOI if available (enables Unpaywall + DOI resolution)
                    if article.doi:
                        extraction_data["doi"] = article.doi

                    # Add PDF URL if available
                    if hasattr(article, 'pdf_url') and article.pdf_url:
                        extraction_data["pdf_url"] = article.pdf_url

                    # Call extraction service
                    response = requests.post(
                        f"{EXTRACTION_URL}/extract",
                        json=extraction_data,
                        timeout=30.0
                    )

                    if response.status_code == 200:
                        result = response.json()
                        if result.get("success") and result.get("full_text"):
                            article.full_text = result["full_text"]
                            article.status = ArticleStatus.EXTRACTED
                            extracted_count += 1

                            # Update published_date if extraction found a more accurate one
                            extracted_date = result.get("published_date")
                            if extracted_date:
                                try:
                                    article.published_date = date.fromisoformat(extracted_date)
                                except (ValueError, TypeError):
                                    pass

                            logger.debug(f"Extracted {result['char_count']} chars using {result['method_used']} for {article.url}")

                            # Upload raw article to S3
                            try:
                                s3_manager.upload_article(
                                    article_dict=article.model_dump(),
                                    published_date=article.published_date,
                                )
                            except Exception as s3_err:
                                logger.warning(f"S3 article upload failed for {article.url}: {s3_err}")

                            # Update status in DB to 'extracted'
                            try:
                                db_manager.update_article_status(
                                    source_id=article.source_id,
                                    published_date=article.published_date,
                                    article_id=article.article_id,
                                    status="extracted",
                                )
                            except Exception as db_err:
                                logger.warning(f"Failed to update status for {article.article_id}: {db_err}")
                        else:
                            logger.debug(f"Extraction failed for {article.url}: {result.get('error', 'Unknown error')}")

                except Exception as e:
                    logger.warning(f"Failed to extract full text for {article.url}: {e}")
                    continue

            logger.info(f"Successfully extracted full text for {extracted_count}/{len(articles)} articles")
            return articles

        except Exception as e:
            logger.error(f"Full text extraction failed: {e}")
            return articles
    
    async def _llm_process(self, articles: List[Article]) -> List[Article]:
        processed = []
        BATCH_SIZE = 10  # Process in smaller batches to avoid timeout

        try:
            async with httpx.AsyncClient(timeout=300.0) as client:
                # Process in batches
                for i in range(0, len(articles), BATCH_SIZE):
                    batch = articles[i:i + BATCH_SIZE]
                    logger.info(f"LLM processing batch {i // BATCH_SIZE + 1}/{(len(articles) + BATCH_SIZE - 1) // BATCH_SIZE} ({len(batch)} articles)")

                    try:
                        response = await client.post(
                            f"{LLM_URL}/llm/process-batch",
                            json={"articles": [a.model_dump(mode='json') for a in batch]}
                        )
                        response.raise_for_status()
                        result = response.json()

                        for article, proc_result in zip(batch, result["results"]):
                            if proc_result.get("status") == "processed":
                                article.project_area = proc_result.get("project_area")
                                article.sub_topic = proc_result.get("sub_topic")
                                article.summary = proc_result.get("summary")
                                article.key_findings = proc_result.get("key_findings", [])
                                evidence_level = proc_result.get("evidence_level")
                                # Cap evidence at 3 for abstract-only articles
                                if not article.full_text and evidence_level:
                                    evidence_level = min(evidence_level, 3)
                                article.evidence_level = evidence_level
                                article.study_type = proc_result.get("study_type")
                                article.relevance_score = proc_result.get("relevance_score")
                                article.status = ArticleStatus.PROCESSED
                                processed.append(article)

                                # Update status in DB to 'processed'
                                try:
                                    db_manager.update_article_status(
                                        source_id=article.source_id,
                                        published_date=article.published_date,
                                        article_id=article.article_id,
                                        status="processed",
                                        project_area=article.project_area,
                                        sub_topic=article.sub_topic,
                                        summary=article.summary,
                                        evidence_level=article.evidence_level,
                                    )
                                except Exception as db_err:
                                    logger.warning(f"Failed to update status for {article.article_id}: {db_err}")

                    except Exception as batch_err:
                        logger.warning(f"LLM batch {i // BATCH_SIZE + 1} failed: {batch_err}")
                        # Continue with next batch instead of failing entirely

                logger.info(f"LLM processing complete: {len(processed)}/{len(articles)} processed")
                return processed if processed else articles

        except Exception as e:
            logger.error(f"LLM processing failed: {e}")
            return articles
    
    async def _notify(self, articles: List[Article]) -> int:
        """Send each article as individual Slack message.

        Limits: max 40 articles per run, only sends between 9 AM - 7 PM IST.
        """
        # Check IST time window (9 AM - 7 PM)
        now_ist = datetime.now(IST)
        if now_ist.hour < 9 or now_ist.hour >= 19:
            logger.info(f"Outside notification window (9 AM - 7 PM IST). Current IST time: {now_ist.strftime('%H:%M')}. Skipping notifications.")
            return 0

        notified = 0
        MAX_NOTIFY = 40

        # Filter out articles without a specific project area (safety net)
        relevant_articles = [
            a for a in articles
            if a.project_area and a.project_area != "general"
            and (a.relevance_score is None or a.relevance_score >= 20)
        ]
        filtered_count = len(articles) - len(relevant_articles)
        if filtered_count:
            logger.info(f"Filtered out {filtered_count} articles with no specific project area or low relevance")

        # Sort by evidence level (highest first) and cap at MAX_NOTIFY
        sorted_articles = sorted(relevant_articles, key=lambda a: (a.evidence_level or 0), reverse=True)[:MAX_NOTIFY]

        if len(relevant_articles) > MAX_NOTIFY:
            logger.info(f"Capping notifications at {MAX_NOTIFY} articles (total: {len(relevant_articles)})")

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                for article in sorted_articles:
                    # Skip articles already notified (Redis guard against duplicates)
                    notify_key = f"notified:{article.article_id}"
                    try:
                        already_notified = await redis_manager.exists(notify_key)
                        if already_notified:
                            logger.debug(f"Skipping already-notified article: {article.title[:50]}")
                            continue
                    except Exception:
                        pass  # Redis unavailable - proceed with notification

                    area = article.project_area
                    channel = config.slack.channels.get(area, "#research-general")

                    try:
                        response = await client.post(
                            f"{NOTIFICATION_URL}/notify/article",
                            json={
                                "article": article.model_dump(mode='json'),
                                "channel": channel,
                            }
                        )

                        if response.status_code == 200:
                            result = response.json()
                            if result.get("success"):
                                article.status = ArticleStatus.NOTIFIED
                                # Mark as notified in Redis (permanent) to prevent duplicates
                                try:
                                    await redis_manager.set(notify_key, "1")
                                except Exception:
                                    pass
                                try:
                                    db_manager.update_article_status(
                                        source_id=article.source_id,
                                        published_date=article.published_date,
                                        article_id=article.article_id,
                                        status="notified",
                                    )
                                except Exception as db_err:
                                    logger.warning(f"Failed to update notified status for {article.article_id}: {db_err}")
                                notified += 1
                                logger.info(f"Notified: {article.title[:50]}...")

                        # Small delay between messages to avoid rate limiting
                        await asyncio.sleep(1)

                    except Exception as article_err:
                        logger.warning(f"Failed to notify article {article.article_id}: {article_err}")

        except Exception as e:
            logger.error(f"Notification failed: {e}")

        return notified
    
    async def _load_from_queue(self, queue_name: str) -> List[Article]:
        articles = []
        while True:
            data = await redis_manager.pop_queue(queue_name, timeout=1)
            if not data:
                break
            try:
                article = Article.model_validate_json(data)
                articles.append(article)
            except Exception as e:
                logger.warning(f"Failed to parse article: {e}")
        return articles

    def _load_incomplete_articles(self, days: int = 3, limit: int = 500) -> List[Article]:
        """
        Load articles from DB that haven't completed the full pipeline.

        Default: yesterday + today (2 days) to avoid reprocessing old articles.
        Caller should limit to 100 if needed to control batch size.

        This allows resuming processing for articles that were:
        - Crawled but not extracted (status='unique')
        - Extracted but not LLM processed (status='extracted')
        - Processed but not notified (status='processed')
        """
        articles = []
        try:
            rows = db_manager.get_unprocessed_articles(
                target_status="notified",
                days=days,
                limit=limit
            )
            logger.info(f"Fetched {len(rows)} incomplete articles from DB")
            for row in rows:
                try:
                    # Convert UUID to string if needed
                    if row.get("article_id") and not isinstance(row["article_id"], str):
                        row["article_id"] = str(row["article_id"])

                    # Convert ScyllaDB Date to Python date
                    if row.get("published_date"):
                        pd = row["published_date"]
                        if hasattr(pd, "date"):
                            row["published_date"] = pd.date()
                        elif not isinstance(pd, date):
                            # ScyllaDB Date object - days since epoch
                            from datetime import timedelta
                            epoch = date(1970, 1, 1)
                            row["published_date"] = epoch + timedelta(days=int(pd))

                    # Convert authors from list of strings to list of Author dicts
                    if row.get("authors") and isinstance(row["authors"], list):
                        if row["authors"] and isinstance(row["authors"][0], str):
                            row["authors"] = [{"name": name} for name in row["authors"]]
                    elif not row.get("authors"):
                        row["authors"] = []

                    # Handle null list fields
                    if row.get("keywords") is None:
                        row["keywords"] = []
                    if row.get("key_findings") is None:
                        row["key_findings"] = []

                    article = Article.model_validate(row)
                    articles.append(article)
                except Exception as e:
                    logger.warning(f"Failed to parse incomplete article {row.get('article_id')}: {e}")
            logger.info(f"Loaded {len(articles)} incomplete articles from DB")
        except Exception as e:
            logger.warning(f"Failed to load incomplete articles: {e}")
        return articles
    
    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        return self._runs.get(run_id)
    
    def get_current(self) -> Optional[PipelineRun]:
        return self._current


orchestrator = PipelineOrchestrator()


def setup_scheduler():
    schedule = config.pipeline.schedule
    try:
        parts = schedule.split()
        trigger = CronTrigger(minute=parts[0], hour=parts[1], day=parts[2], month=parts[3], day_of_week=parts[4])
        scheduler.add_job(scheduled_run, trigger, id="daily_pipeline", replace_existing=True)
        logger.info(f"Scheduled pipeline: {schedule}")
    except Exception as e:
        logger.error(f"Scheduler setup failed: {e}")


async def scheduled_run():
    logger.info("Starting scheduled pipeline run")
    await orchestrator.run_pipeline()


@app.post("/pipeline/run", response_model=PipelineResponse)
async def trigger_pipeline(request: PipelineRequest, background_tasks: BackgroundTasks):
    if orchestrator.get_current():
        raise HTTPException(status_code=409, detail="Pipeline already running")
    
    background_tasks.add_task(
        orchestrator.run_pipeline,
        request.source_ids, request.skip_crawl, request.skip_extraction, request.skip_llm, request.skip_notify
    )
    
    return PipelineResponse(run_id="pending", status="started", message="Pipeline initiated")


@app.get("/pipeline/status")
async def get_status():
    run = orchestrator.get_current()
    if not run:
        return {"status": "idle", "message": "No pipeline running"}
    return run.model_dump()


@app.get("/pipeline/status/{run_id}")
async def get_run_status(run_id: str):
    run = orchestrator.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run.model_dump()


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "orchestrator", "scheduler": scheduler.running}