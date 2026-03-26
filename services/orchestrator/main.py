"""
Orchestrator Service - Pipeline coordination and scheduling.
"""
import logging
import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
import json

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import sys
sys.path.insert(0, '/app')

from shared.models import Article, PipelineRun, ArticleStatus, ProjectArea
from shared.utils import (
    ScyllaDBManager,
    RedisManager,
    KafkaManager,
    S3Manager,
    build_top_feedback_users,
    build_weekly_report_sections,
    initialize_schema,
    render_weekly_feedback_pdf,
)
from shared.utils.slack_feedback import get_week_year, get_week_year_from_message_ts
from shared.utils.metrics import (
    add_metrics_endpoint,
    CONTENT_TYPE_COUNTER, PIPELINE_RUNS, PIPELINE_STAGE_ARTICLES, PIPELINE_DURATION,
)
from shared.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_manager = ScyllaDBManager()
redis_manager = RedisManager()
kafka_manager = KafkaManager()
s3_manager = S3Manager()
scheduler = AsyncIOScheduler()

CRAWLER_URL = config.settings.crawler_url
DEDUP_URL = config.settings.dedup_url
EXTRACTION_URL = config.settings.extraction_url
LLM_URL = config.settings.llm_url
NOTIFICATION_URL = config.settings.notification_url


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
add_metrics_endpoint(app)


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


class WeeklyReportRequest(BaseModel):
    week_year: Optional[str] = None
    send_to_slack: bool = True


class WeeklyReportResponse(BaseModel):
    success: bool
    report_created: bool
    week_year: str
    message: str
    report_path: Optional[str] = None
    s3_key: Optional[str] = None
    sections: int = 0
    articles: int = 0
    channels_targeted: int = 0
    channels_sent: int = 0
    failed_channels: List[str] = Field(default_factory=list)


class TestNotificationRequest(BaseModel):
    count: int = 2
    channel: str = "#research-general"
    title_prefix: str = "[TEST]"


class TestNotificationResult(BaseModel):
    article_id: str
    title: str
    channel: str
    slack_posted: bool
    article_stored: bool
    slack_message_stored: bool
    message_ts: Optional[str] = None
    error: Optional[str] = None


class TestNotificationResponse(BaseModel):
    success: bool
    channel: str
    requested: int
    sent: int
    stored_articles: int
    stored_messages: int
    results: List[TestNotificationResult]


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
            articles = []
            if not skip_crawl:
                run.status = "crawling"
                articles = await self._crawl(source_ids)
                run.articles_crawled = len(articles)
                PIPELINE_STAGE_ARTICLES.labels(stage="crawled").set(run.articles_crawled)
                logger.info(f"Crawled {len(articles)} new articles")
            else:
                articles = await self._load_from_queue("articles:crawled")
                run.articles_crawled = len(articles)
                PIPELINE_STAGE_ARTICLES.labels(stage="crawled").set(run.articles_crawled)

            run.status = "loading_incomplete"
            incomplete_articles = self._load_incomplete_articles(days=config.pipeline.incomplete_lookback_days, limit=config.pipeline.max_articles_per_run)

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

            if articles:
                await kafka_manager.publish_articles(
                    "crawled", [a.model_dump(mode='json') for a in articles], run.run_id
                )

            run.status = "deduplicating"
            unique_articles = await self._deduplicate(articles)
            run.articles_deduplicated = len(unique_articles)
            PIPELINE_STAGE_ARTICLES.labels(stage="deduplicated").set(run.articles_deduplicated)
            logger.info(f"Deduplicated to {len(unique_articles)} articles")

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

            extracted_articles = already_extracted
            # Run extraction if: not skipped OR there are incomplete articles that need it
            if needs_extraction and (not skip_extraction or incomplete_needs_extraction):
                run.status = "extracting"
                newly_extracted = await self._extract_fulltext(needs_extraction)
                full_text_count = 0
                abstract_fallback_count = 0
                for a in newly_extracted:
                    if a.full_text:
                        extracted_articles.append(a)
                        full_text_count += 1
                        CONTENT_TYPE_COUNTER.labels(content_type="full_text").inc()
                    elif a.abstract and len(a.abstract) >= config.pipeline.min_abstract_length:
                        a.status = ArticleStatus.EXTRACTED
                        extracted_articles.append(a)
                        abstract_fallback_count += 1
                        CONTENT_TYPE_COUNTER.labels(content_type="abstract_only").inc()
                        try:
                            db_manager.update_article_status(
                                source_id=a.source_id,
                                published_date=a.published_date,
                                article_id=a.article_id,
                                status="extracted",
                            )
                        except Exception as db_err:
                            logger.warning(f"Failed to update abstract-fallback status for {a.article_id}: {db_err}")
                dropped_count = len(newly_extracted) - full_text_count - abstract_fallback_count
                PIPELINE_STAGE_ARTICLES.labels(stage="extracted").set(len(extracted_articles))
                logger.info(f"Extracted: {full_text_count} full text, {abstract_fallback_count} abstract-only, {dropped_count} dropped (no content)")

                await kafka_manager.publish_articles(
                    "extracted", [a.model_dump() for a in extracted_articles], run.run_id
                )

            processed_articles = list(already_processed)
            # Run LLM if: not skipped OR there are extracted articles from incomplete pipeline
            if extracted_articles and (not skip_llm or (incomplete_needs_extraction or incomplete_extracted)):
                run.status = "processing"
                newly_processed = await self._llm_process(extracted_articles)
                processed_articles.extend(newly_processed)
                run.articles_filtered = len(processed_articles)
                run.articles_summarized = len([a for a in processed_articles if a.summary])
                PIPELINE_STAGE_ARTICLES.labels(stage="processed").set(run.articles_filtered)
                logger.info(f"Processed {len(newly_processed)} articles (+ {len(already_processed)} already processed)")
            elif not skip_llm:
                pass  # No extracted articles to process
            else:
                processed_articles.extend(extracted_articles)

            if processed_articles:
                await kafka_manager.publish_articles(
                    "processed", [a.model_dump() for a in processed_articles], run.run_id
                )

            articles_dicts = [article.model_dump() for article in processed_articles]
            try:
                os.makedirs("/app/data", exist_ok=True)
                output_file = f"/app/data/processed_articles_{run.run_id}.json"
                with open(output_file, "w") as f:
                    json.dump(articles_dicts, f, indent=2, default=str)
                logger.info(f"Saved {len(processed_articles)} articles to {output_file}")
            except Exception as e:
                logger.warning(f"Failed to save articles to JSON file: {e}")

            run.status = "storing"
            for article in processed_articles:
                try:
                    db_manager.insert_article(article.model_dump())
                except Exception as e:
                    logger.warning(f"Failed to store article: {e}")

            logger.info(f"Stored {len(processed_articles)} articles in ScyllaDB")

            if articles_dicts:
                s3_manager.upload_articles(articles_dicts, run.run_id, stage="processed")

            if not skip_notify and processed_articles:
                run.status = "notifying"
                notified = await self._notify(processed_articles)
                run.articles_notified = notified
                PIPELINE_STAGE_ARTICLES.labels(stage="notified").set(run.articles_notified)
                logger.info(f"Notified {notified} articles")

            run.status = "completed"
            run.completed_at = datetime.utcnow()
            PIPELINE_RUNS.labels(status="completed").inc()
            logger.info(f"Pipeline {run.run_id} completed successfully")

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
            PIPELINE_RUNS.labels(status="failed").inc()

            await kafka_manager.publish_event("pipeline_failed", {
                "run_id": run.run_id,
                "error": str(e),
            })

        finally:
            duration = (datetime.utcnow() - run.started_at).total_seconds()
            PIPELINE_DURATION.labels(status=run.status).observe(duration)
            self._current = None
            self._runs[run.run_id] = run
            run_data = run.model_dump()
            db_manager.insert_pipeline_run(run_data)
            s3_manager.upload_pipeline_run(run_data, run.run_id)
        
        return run
    
    async def _crawl(self, source_ids: Optional[List[str]]) -> List[Article]:
        try:
            async with httpx.AsyncClient(timeout=config.pipeline.crawler_timeout) as client:
                response = await client.post(
                    f"{CRAWLER_URL}/crawl/trigger",
                    json={"source_ids": source_ids, "max_articles_per_source": config.pipeline.max_articles_per_source_crawl}
                )
                response.raise_for_status()
                trigger_result = response.json()

                if trigger_result.get("status") == "busy":
                    logger.warning("Crawler is busy, waiting...")

                for _ in range(config.pipeline.crawler_poll_max_retries):
                    await asyncio.sleep(config.pipeline.crawler_poll_interval)
                    status_resp = await client.get(f"{CRAWLER_URL}/crawl/status")
                    status = status_resp.json()
                    if not status.get("is_running", False):
                        logger.info(
                            f"Crawl finished: {status.get('total_unique', 0)} unique / "
                            f"{status.get('total_found', 0)} found"
                        )
                        break

                articles = []
                today = date.today()
                lookback = config.pipeline.crawl_lookback_days
                per_source_limit = config.pipeline.max_articles_per_source_query
                if source_ids:
                    for sid in source_ids:
                        for i in range(lookback):
                            query_date = today - timedelta(days=i)
                            rows = db_manager.get_articles_by_date(sid, query_date, limit=per_source_limit)
                            for row in rows:
                                if row.get("status") == "unique":
                                    try:
                                        articles.append(Article.model_validate(row))
                                    except Exception:
                                        pass
                else:
                    rows = db_manager.get_recent_articles(days=lookback, limit=config.pipeline.max_articles_per_run)
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
            async with httpx.AsyncClient(timeout=config.pipeline.dedup_timeout) as client:
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
                    # Redis guard: skip if already extracted
                    extracted_key = f"extracted:{article.article_id}"
                    try:
                        if await redis_manager.exists(extracted_key):
                            logger.debug(f"Skipping already-extracted article (Redis guard): {article.article_id}")
                            article.status = ArticleStatus.EXTRACTED
                            continue
                    except Exception as redis_err:
                        logger.warning(f"Redis check failed for {extracted_key}, proceeding: {redis_err}")

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

                    if hasattr(article, 'pdf_url') and article.pdf_url:
                        extraction_data["pdf_url"] = article.pdf_url

                    response = requests.post(
                        f"{EXTRACTION_URL}/extract",
                        json=extraction_data,
                        timeout=config.pipeline.extraction_timeout
                    )

                    if response.status_code == 200:
                        result = response.json()
                        if result.get("success") and result.get("full_text"):
                            article.full_text = result["full_text"]
                            article.status = ArticleStatus.EXTRACTED
                            extracted_count += 1

                            extracted_date = result.get("published_date")
                            if extracted_date:
                                try:
                                    parsed_date = date.fromisoformat(extracted_date)
                                    # Cap future dates to today
                                    if parsed_date > date.today():
                                        parsed_date = date.today()
                                    article.published_date = parsed_date
                                except (ValueError, TypeError):
                                    pass

                            logger.debug(f"Extracted {result['char_count']} chars using {result['method_used']} for {article.url}")

                            try:
                                s3_manager.upload_article(
                                    article_dict=article.model_dump(),
                                    published_date=article.published_date,
                                )
                            except Exception as s3_err:
                                logger.warning(f"S3 article upload failed for {article.url}: {s3_err}")

                            try:
                                db_manager.update_article_status(
                                    source_id=article.source_id,
                                    published_date=article.published_date,
                                    article_id=article.article_id,
                                    status="extracted",
                                )
                            except Exception as db_err:
                                logger.warning(f"Failed to update status for {article.article_id}: {db_err}")

                            # Mark as extracted in Redis (no TTL) so it's never re-extracted
                            try:
                                await redis_manager.set(f"extracted:{article.article_id}", "1")
                            except Exception as redis_err:
                                logger.warning(f"Redis set failed for extracted:{article.article_id}: {redis_err}")
                        else:
                            logger.info(f"Extraction returned no full text for {article.url}: {result.get('error', 'no content')}")

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
        BATCH_SIZE = config.pipeline.llm_batch_size

        try:
            # Redis guard: filter out articles already LLM-processed
            unprocessed = []
            for article in articles:
                processed_key = f"processed:{article.article_id}"
                try:
                    if await redis_manager.exists(processed_key):
                        logger.debug(f"Skipping already-processed article (Redis guard): {article.article_id}")
                        article.status = ArticleStatus.PROCESSED
                        processed.append(article)
                        continue
                except Exception as redis_err:
                    logger.warning(f"Redis check failed for {processed_key}, proceeding: {redis_err}")
                unprocessed.append(article)

            if not unprocessed:
                logger.info("All articles already LLM-processed (Redis guard). Skipping LLM.")
                return processed

            async with httpx.AsyncClient(timeout=config.pipeline.llm_timeout) as client:
                for i in range(0, len(unprocessed), BATCH_SIZE):
                    batch = unprocessed[i:i + BATCH_SIZE]
                    logger.info(f"LLM processing batch {i // BATCH_SIZE + 1}/{(len(unprocessed) + BATCH_SIZE - 1) // BATCH_SIZE} ({len(batch)} articles)")

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
                                    evidence_level = min(evidence_level, config.pipeline.abstract_only_evidence_cap)
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

                                # Mark as processed in Redis (no TTL) so it's never re-processed
                                try:
                                    await redis_manager.set(f"processed:{article.article_id}", "1")
                                except Exception as redis_err:
                                    logger.warning(f"Redis set failed for processed:{article.article_id}: {redis_err}")
                            else:
                                reasons = proc_result.get("reasons", [])
                                logger.info(
                                    f"LLM rejected: {article.title[:60]} — "
                                    f"status={proc_result.get('status')}, reasons={reasons}"
                                )

                    except Exception as batch_err:
                        logger.warning(f"LLM batch {i // BATCH_SIZE + 1} failed: {batch_err}")

                logger.info(f"LLM processing complete: {len(processed)}/{len(articles)} processed")
                return processed if processed else unprocessed

        except Exception as e:
            logger.error(f"LLM processing failed: {e}")
            return articles
    
    async def _notify(self, articles: List[Article]) -> int:
        """Send each article as individual Slack message.

        Limits: max 40 articles per run, only sends between 9:30 AM - 6:30 PM IST.
        """
        now_ist = datetime.now(IST)
        notify_start = now_ist.replace(hour=config.pipeline.notify_start_hour, minute=config.pipeline.notify_start_minute, second=0, microsecond=0)
        notify_end = now_ist.replace(hour=config.pipeline.notify_end_hour, minute=config.pipeline.notify_end_minute, second=0, microsecond=0)
        if now_ist < notify_start or now_ist > notify_end:
            logger.info(f"Outside notification window ({config.pipeline.notify_start_hour}:{config.pipeline.notify_start_minute:02d} - {config.pipeline.notify_end_hour}:{config.pipeline.notify_end_minute:02d} IST). Current IST time: {now_ist.strftime('%H:%M')}. Skipping notifications.")
            return 0

        notified = 0
        MAX_NOTIFY = config.pipeline.max_notification_articles

        relevant_articles = []
        irrelevant_articles = []
        for a in articles:
            if a.project_area and a.project_area != "general" and a.relevance_score is not None and a.relevance_score >= config.pipeline.min_relevance_score:
                relevant_articles.append(a)
            else:
                irrelevant_articles.append(a)

        if irrelevant_articles:
            logger.info(f"Filtered out {len(irrelevant_articles)} articles with no specific project area or low relevance — marking as filtered_out")
            for a in irrelevant_articles:
                a.status = ArticleStatus.FILTERED_OUT
                try:
                    db_manager.update_article_status(
                        source_id=a.source_id,
                        published_date=a.published_date,
                        article_id=a.article_id,
                        status="filtered_out",
                    )
                except Exception as db_err:
                    logger.warning(f"Failed to mark irrelevant article {a.article_id} as filtered_out: {db_err}")
                try:
                    await redis_manager.set(f"filtered:{a.article_id}", "1")
                except Exception as redis_err:
                    logger.warning(f"Redis set failed for filtered:{a.article_id}: {redis_err}")

        # Pre-filter: remove articles already notified (Redis check BEFORE capping)
        # This prevents already-notified articles from eating up notification slots
        pending_articles = []
        already_notified_count = 0
        for article in relevant_articles:
            notify_key = f"notified:{article.article_id}"
            try:
                if await redis_manager.exists(notify_key):
                    already_notified_count += 1
                    # Sync DB status so _load_incomplete_articles stops reloading this article
                    article.status = ArticleStatus.NOTIFIED
                    try:
                        db_manager.update_article_status(
                            source_id=article.source_id,
                            published_date=article.published_date,
                            article_id=article.article_id,
                            status="notified",
                        )
                    except Exception as db_err:
                        logger.warning(f"DB status sync failed for already-notified {article.article_id}: {db_err}")
                    continue
            except Exception as redis_err:
                logger.warning(f"Redis check failed for {notify_key}, including in pending: {redis_err}")
            pending_articles.append(article)

        if already_notified_count:
            logger.info(f"Pre-filtered {already_notified_count} already-notified articles (Redis). {len(pending_articles)} pending.")

        # Sort by evidence level (highest first) and cap at MAX_NOTIFY
        all_sorted = sorted(pending_articles, key=lambda a: (a.evidence_level or 0), reverse=True)
        sorted_articles = all_sorted[:MAX_NOTIFY]

        if len(pending_articles) > MAX_NOTIFY:
            logger.info(f"Capping notifications at {MAX_NOTIFY} articles (total: {len(pending_articles)})")
            # Keep overflow articles as 'processed' so they get notified in the next cycle
            logger.info(f"Deferring {len(all_sorted[MAX_NOTIFY:])} overflow articles to next notification cycle")

        try:
            async with httpx.AsyncClient(timeout=config.pipeline.notification_timeout) as client:
                for article in sorted_articles:

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
                                message_ts = result.get("message_ts")

                                # Mark as notified in Redis (no TTL) to permanently prevent duplicates
                                try:
                                    await redis_manager.set(notify_key, "1")
                                except Exception as redis_err:
                                    logger.warning(f"Redis set failed for {notify_key}: {redis_err}")
                                try:
                                    db_manager.update_article_status(
                                        source_id=article.source_id,
                                        published_date=article.published_date,
                                        article_id=article.article_id,
                                        status="notified",
                                    )
                                except Exception as db_err:
                                    logger.warning(f"Failed to update notified status for {article.article_id}: {db_err}")

                                # Store Slack message metadata for feedback correlation
                                if message_ts:
                                    try:
                                        week_year = get_week_year_from_message_ts(message_ts)
                                        db_manager.insert_slack_message(
                                            week_year=week_year,
                                            message_ts=message_ts,
                                            channel=channel,
                                            article_id=article.article_id,
                                            source_id=article.source_id,
                                            published_date=article.published_date,
                                            project_area=article.project_area or "",
                                            title=article.title or "",
                                            url=article.url or "",
                                            summary=article.summary or "",
                                        )
                                    except Exception as slack_db_err:
                                        logger.warning(f"Failed to store Slack message metadata: {slack_db_err}")

                                notified += 1
                                logger.info(f"Notified: {article.title[:50]}...")
                            else:
                                logger.warning(f"Slack notification failed for {article.article_id}: {result.get('error', 'unknown')}")

                        # Small delay between messages to avoid rate limiting
                        await asyncio.sleep(config.pipeline.notification_sleep_interval)

                    except Exception as article_err:
                        logger.warning(f"Failed to notify article {article.article_id}: {article_err}")

        except Exception as e:
            logger.error(f"Notification failed: {e}")

        return notified
    
    async def _load_from_queue(self, queue_name: str) -> List[Article]:
        articles = []
        while True:
            data = await redis_manager.pop_queue(queue_name, timeout=config.pipeline.redis_queue_timeout)
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
                    # Mark unparseable articles as filtered_out so they don't retry every cycle
                    try:
                        aid = row.get('article_id')
                        if aid and row.get('source_id') and row.get('published_date'):
                            db_manager.update_article_status(
                                source_id=row['source_id'],
                                published_date=row['published_date'],
                                article_id=str(aid),
                                status="filtered_out",
                            )
                            logger.info(f"Marked unparseable article {aid} as filtered_out")
                    except Exception as db_err:
                        logger.warning(f"Failed to mark unparseable article as filtered_out: {db_err}")
            logger.info(f"Loaded {len(articles)} incomplete articles from DB")
        except Exception as e:
            logger.warning(f"Failed to load incomplete articles: {e}")
        return articles
    
    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        return self._runs.get(run_id)
    
    def get_current(self) -> Optional[PipelineRun]:
        return self._current

    def _get_week_year(self, dt: date = None) -> str:
        """Get week-year string like '2026-W06' for partition key."""
        return get_week_year(dt)

    def _resolve_report_week_year(self, explicit_week_year: Optional[str] = None) -> str:
        """Resolve the ISO week-year for the weekly feedback report."""
        if explicit_week_year:
            return explicit_week_year
        report_date = date.today() - timedelta(days=config.pipeline.weekly_report_lookback_days)
        return self._get_week_year(report_date)

    def _weekly_report_output_path(self, week_year: str) -> Path:
        report_dir = Path(config.pipeline.weekly_report_output_dir)
        filename = f"weekly_feedback_report_{week_year}.pdf"
        return (report_dir / filename).resolve()

    def _weekly_report_channels(self) -> List[str]:
        """Return unique configured Slack channels for weekly report delivery."""
        channels: List[str] = []
        seen = set()

        for channel in (config.slack.channels or {}).values():
            normalized = str(channel or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            channels.append(normalized)

        return channels

    async def _send_weekly_report_to_channels(
        self,
        report_path: Path,
        week_year: str,
        section_count: int,
        article_count: int,
    ) -> Dict[str, Any]:
        """Upload the weekly report PDF to all configured Slack channels."""
        channels = self._weekly_report_channels()
        if not channels:
            logger.info("Weekly report Slack delivery skipped: no configured channels")
            return {"channels_targeted": 0, "channels_sent": 0, "failed_channels": []}

        bot_token = config.settings.slack_bot_token or os.getenv("SLACK_BOT_TOKEN")
        if not bot_token:
            logger.warning("Weekly report Slack delivery skipped: SLACK_BOT_TOKEN is missing")
            return {
                "channels_targeted": len(channels),
                "channels_sent": 0,
                "failed_channels": channels,
            }

        try:
            report_bytes = report_path.read_bytes()
        except Exception as exc:
            logger.warning(f"Weekly report Slack delivery skipped: unable to read report file: {exc}")
            return {
                "channels_targeted": len(channels),
                "channels_sent": 0,
                "failed_channels": channels,
            }

        title = f"Weekly Feedback Report {week_year}"
        initial_comment = (
            f"Weekly feedback report for *{week_year}* is ready. "
            f"Sections: {section_count}, Articles: {article_count}."
        )

        sent_channels: List[str] = []
        failed_channels: List[str] = []

        async with httpx.AsyncClient() as client:
            for channel in channels:
                try:
                    response = await client.post(
                        "https://slack.com/api/files.upload",
                        headers={"Authorization": f"Bearer {bot_token}"},
                        data={
                            "channels": channel,
                            "filename": report_path.name,
                            "title": title,
                            "initial_comment": initial_comment,
                        },
                        files={"file": (report_path.name, report_bytes, "application/pdf")},
                        timeout=config.pipeline.slack_api_timeout,
                    )
                    result = response.json()
                    if result.get("ok"):
                        sent_channels.append(channel)
                    else:
                        error = result.get("error", "unknown_error")
                        failed_channels.append(channel)
                        logger.warning(f"Weekly report Slack upload failed for {channel}: {error}")
                except Exception as exc:
                    failed_channels.append(channel)
                    logger.warning(f"Weekly report Slack upload failed for {channel}: {exc}")

        return {
            "channels_targeted": len(channels),
            "channels_sent": len(sent_channels),
            "failed_channels": failed_channels,
        }

    async def generate_weekly_report(self, week_year: Optional[str] = None, send_to_slack: bool = True) -> Dict[str, Any]:
        """Generate a weekly PDF report from positive Slack feedback."""
        resolved_week_year = self._resolve_report_week_year(week_year)
        logger.info(f"Generating weekly feedback report for {resolved_week_year}")

        try:
            feedback_rows = db_manager.get_feedback_for_week(resolved_week_year)
            positive_feedback = [
                row for row in feedback_rows
                if (row.get("feedback_type") or "").strip().lower() == "positive"
            ]
            if not positive_feedback:
                logger.info(f"No positive feedback found for {resolved_week_year}")
                return {
                    "success": True,
                    "report_created": False,
                    "week_year": resolved_week_year,
                    "message": f"No positive feedback found for {resolved_week_year}.",
                    "report_path": None,
                    "s3_key": None,
                    "sections": 0,
                    "articles": 0,
                    "channels_targeted": 0,
                    "channels_sent": 0,
                    "failed_channels": [],
                }

            slack_messages = db_manager.get_slack_messages_for_week(resolved_week_year)
            sections = build_weekly_report_sections(
                positive_feedback=positive_feedback,
                slack_messages=slack_messages,
                top_n=config.pipeline.weekly_report_top_articles_per_channel,
            )
            top_feedback_users = build_top_feedback_users(
                feedback_rows,
                top_n=config.pipeline.weekly_report_top_users,
            )

            if not sections:
                logger.info(f"No reportable Slack message metadata found for {resolved_week_year}")
                return {
                    "success": True,
                    "report_created": False,
                    "week_year": resolved_week_year,
                    "message": f"No reportable Slack message metadata found for {resolved_week_year}.",
                    "report_path": None,
                    "s3_key": None,
                    "sections": 0,
                    "articles": 0,
                    "channels_targeted": 0,
                    "channels_sent": 0,
                    "failed_channels": [],
                }

            report_path = self._weekly_report_output_path(resolved_week_year)
            render_weekly_feedback_pdf(
                output_path=str(report_path),
                week_year=resolved_week_year,
                sections=sections,
                top_feedback_users=top_feedback_users,
                generated_at=datetime.utcnow(),
            )

            s3_key = None
            try:
                with open(report_path, "rb") as pdf_file:
                    s3_key = s3_manager.upload_weekly_report_pdf(
                        pdf_content=pdf_file.read(),
                        week_year=resolved_week_year,
                        filename=report_path.name,
                    )
            except Exception as upload_error:
                logger.warning(f"Weekly report generated locally but S3 upload failed: {upload_error}")

            article_count = sum(len(section.articles) for section in sections)
            slack_delivery = {
                "channels_targeted": 0,
                "channels_sent": 0,
                "failed_channels": [],
            }
            if send_to_slack:
                slack_delivery = await self._send_weekly_report_to_channels(
                    report_path=report_path,
                    week_year=resolved_week_year,
                    section_count=len(sections),
                    article_count=article_count,
                )

            logger.info(
                f"Weekly feedback report ready for {resolved_week_year}: "
                f"{len(sections)} sections, {article_count} articles"
            )

            return {
                "success": True,
                "report_created": True,
                "week_year": resolved_week_year,
                "message": "Weekly feedback report generated successfully.",
                "report_path": str(report_path),
                "s3_key": s3_key,
                "sections": len(sections),
                "articles": article_count,
                "channels_targeted": slack_delivery["channels_targeted"],
                "channels_sent": slack_delivery["channels_sent"],
                "failed_channels": slack_delivery["failed_channels"],
            }
        except Exception as e:
            logger.error(f"Weekly feedback report failed for {resolved_week_year}: {e}")
            return {
                "success": False,
                "report_created": False,
                "week_year": resolved_week_year,
                "message": f"Weekly feedback report failed: {e}",
                "report_path": None,
                "s3_key": None,
                "sections": 0,
                "articles": 0,
                "channels_targeted": 0,
                "channels_sent": 0,
                "failed_channels": [],
            }

    def _build_dummy_test_articles(self, count: int, title_prefix: str) -> List[Article]:
        """Build dummy articles for manual Slack notification testing."""
        safe_count = max(1, min(count, 5))
        batch_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        articles: List[Article] = []

        for index in range(safe_count):
            ordinal = index + 1
            article_id = str(uuid.uuid4())
            articles.append(
                Article(
                    article_id=article_id,
                    source_id="test_notification",
                    source_name="Manual Notification Test",
                    url=f"https://example.com/testing/slack-feedback/{batch_id}/{ordinal}",
                    title=f"{title_prefix} Feedback Button Test {ordinal} ({batch_id})",
                    abstract="Dummy abstract for manual Slack notification testing.",
                    full_text="Dummy full text for manual Slack notification testing.",
                    published_date=date.today(),
                    project_area=ProjectArea.GENERAL,
                    relevance_score=100.0,
                    evidence_level=4,
                    summary=(
                        "This is a dummy article sent to validate Slack delivery, "
                        "feedback buttons, and ScyllaDB storage."
                    ),
                    key_findings=[
                        "Dummy notification reached Slack",
                        "Feedback buttons can be clicked",
                    ],
                    preventive_implications="Testing only; not a real article.",
                    status=ArticleStatus.PROCESSED,
                    processed_at=datetime.utcnow(),
                )
            )

        return articles

    async def send_dummy_articles_for_notification_test(
        self,
        count: int = 2,
        channel: str = "#research-general",
        title_prefix: str = "[TEST]",
    ) -> Dict[str, Any]:
        """Post dummy articles to Slack and verify article/slack-message persistence."""
        articles = self._build_dummy_test_articles(count=count, title_prefix=title_prefix)
        results: List[Dict[str, Any]] = []

        async with httpx.AsyncClient(timeout=config.pipeline.notification_timeout) as client:
            for article in articles:
                article_stored = False
                slack_message_stored = False
                slack_posted = False
                message_ts = None
                error_message = None

                try:
                    db_manager.insert_article(article.model_dump())
                    stored_rows = db_manager.get_articles_by_date(
                        source_id=article.source_id,
                        published_date=article.published_date,
                        limit=100,
                    )
                    article_stored = any(str(row.get("article_id")) == article.article_id for row in stored_rows)

                    response = await client.post(
                        f"{NOTIFICATION_URL}/notify/article",
                        json={
                            "article": article.model_dump(mode="json"),
                            "channel": channel,
                        },
                    )
                    response.raise_for_status()
                    payload = response.json()

                    slack_posted = bool(payload.get("success"))
                    message_ts = payload.get("message_ts")
                    if not slack_posted:
                        error_message = payload.get("error") or "Slack notification failed"
                    else:
                        db_manager.update_article_status(
                            source_id=article.source_id,
                            published_date=article.published_date,
                            article_id=article.article_id,
                            status="notified",
                        )
                        if message_ts:
                            week_year = get_week_year_from_message_ts(message_ts)
                            db_manager.insert_slack_message(
                                week_year=week_year,
                                message_ts=message_ts,
                                channel=channel,
                                article_id=article.article_id,
                                source_id=article.source_id,
                                published_date=article.published_date,
                                project_area=article.project_area or "",
                                title=article.title or "",
                                url=article.url or "",
                                summary=article.summary or "",
                            )
                            slack_message_stored = db_manager.get_slack_message(week_year, message_ts) is not None
                except Exception as exc:
                    error_message = str(exc)

                results.append(
                    {
                        "article_id": article.article_id,
                        "title": article.title,
                        "channel": channel,
                        "slack_posted": slack_posted,
                        "article_stored": article_stored,
                        "slack_message_stored": slack_message_stored,
                        "message_ts": message_ts,
                        "error": error_message,
                    }
                )

        sent = sum(1 for result in results if result["slack_posted"])
        stored_articles = sum(1 for result in results if result["article_stored"])
        stored_messages = sum(1 for result in results if result["slack_message_stored"])

        return {
            "success": sent == len(results) and stored_articles == len(results) and stored_messages == len(results),
            "channel": channel,
            "requested": len(results),
            "sent": sent,
            "stored_articles": stored_articles,
            "stored_messages": stored_messages,
            "results": results,
        }

    async def send_weekly_digest(self):
        """Backward-compatible wrapper for the weekly report trigger."""
        return await self.generate_weekly_report()

    def _format_weekly_digest(self, articles: List[Dict], week_year: str, project_area: str) -> List[Dict]:
        """Format weekly digest as Slack blocks."""
        area_names = {
            "disease_prevention": "🏥 Disease Prevention",
            "behavioral_protocols": "🏃 Behavioral Protocols",
            "nutritional_protocols": "🥗 Nutritional Protocols",
            "government_interventions": "🏛️ Government Interventions",
            "youth_health": "🎓 Youth Health",
            "general": "📚 General Research",
        }

        area_name = area_names.get(project_area, project_area)

        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"⭐ Weekly Digest: {area_name}", "emoji": True}},
            {"type": "context", "elements": [
                {"type": "mrkdwn", "text": f"📅 Week {week_year}"},
                {"type": "mrkdwn", "text": f"👍 {len(articles)} top-rated articles"},
            ]},
            {"type": "divider"},
        ]

        for i, article in enumerate(articles[:config.pipeline.digest_max_articles], 1):
            title = article.get("title", "Unknown Title")[:100]
            url = article.get("url", "")
            summary = article.get("summary", "")[:200]

            if url:
                text = f"*{i}. <{url}|{title}>*"
            else:
                text = f"*{i}. {title}*"

            if summary:
                text += f"\n>{summary}..."

            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

        if len(articles) > config.pipeline.digest_max_articles:
            blocks.append({"type": "context", "elements": [
                {"type": "mrkdwn", "text": f"_...and {len(articles) - config.pipeline.digest_max_articles} more_"}
            ]})

        return blocks


orchestrator = PipelineOrchestrator()


def setup_scheduler():
    try:
        # Pipeline (crawl + extraction + LLM, no notification): 8 AM IST (2:30 AM UTC) and 2 PM IST (8:30 AM UTC), every day
        pipeline_morning = CronTrigger(hour=config.pipeline.pipeline_morning_hour, minute=config.pipeline.pipeline_morning_minute)
        scheduler.add_job(scheduled_pipeline_run, pipeline_morning, id="pipeline_morning", replace_existing=True)
        logger.info(f"Scheduled pipeline morning run: {config.pipeline.pipeline_morning_hour}:{config.pipeline.pipeline_morning_minute:02d} UTC")

        pipeline_afternoon = CronTrigger(hour=config.pipeline.pipeline_afternoon_hour, minute=config.pipeline.pipeline_afternoon_minute)
        scheduler.add_job(scheduled_pipeline_run, pipeline_afternoon, id="pipeline_afternoon", replace_existing=True)
        logger.info(f"Scheduled pipeline afternoon run: {config.pipeline.pipeline_afternoon_hour}:{config.pipeline.pipeline_afternoon_minute:02d} UTC")

        # Notification only: every 30 min, 9:30 AM - 6:30 PM IST (4:00 AM - 1:00 PM UTC), weekdays only
        notif_trigger = CronTrigger(hour=config.pipeline.notification_cron_hours, minute=config.pipeline.notification_cron_minutes, day_of_week='mon-fri')
        scheduler.add_job(scheduled_notification_run, notif_trigger, id="notification_run", replace_existing=True)
        logger.info(f"Scheduled notification run: hours={config.pipeline.notification_cron_hours}, minutes={config.pipeline.notification_cron_minutes}, Mon-Fri")

        # Weekly feedback PDF report: Monday 10:00 AM IST (4:30 AM UTC)
        weekly_report_trigger = CronTrigger(
            minute=config.pipeline.weekly_report_minute,
            hour=config.pipeline.weekly_report_hour,
            day_of_week=config.pipeline.weekly_report_day_of_week,
        )
        scheduler.add_job(scheduled_weekly_report, weekly_report_trigger, id="weekly_report", replace_existing=True)
        logger.info(
            f"Scheduled weekly report: {config.pipeline.weekly_report_day_of_week} "
            f"{config.pipeline.weekly_report_hour}:{config.pipeline.weekly_report_minute:02d} UTC"
        )
    except Exception as e:
        logger.error(f"Scheduler setup failed: {e}")


async def scheduled_pipeline_run():
    """Crawl + extraction + LLM only (no notification). Runs at 8 AM and 2 PM IST every day."""
    logger.info("Starting scheduled pipeline run (crawl + extraction + LLM)")
    await orchestrator.run_pipeline(skip_notify=True)


async def scheduled_notification_run():
    """Notification-only run. Fires every 30 min but is guarded to 9:30 AM - 6:30 PM IST, weekdays only."""
    now_ist = datetime.now(IST)
    notify_start = now_ist.replace(hour=config.pipeline.notify_start_hour, minute=config.pipeline.notify_start_minute, second=0, microsecond=0)
    notify_end = now_ist.replace(hour=config.pipeline.notify_end_hour, minute=config.pipeline.notify_end_minute, second=0, microsecond=0)
    if now_ist < notify_start or now_ist > notify_end:
        logger.debug(f"Outside notification window ({config.pipeline.notify_start_hour}:{config.pipeline.notify_start_minute:02d} - {config.pipeline.notify_end_hour}:{config.pipeline.notify_end_minute:02d} IST). Current IST: {now_ist.strftime('%H:%M')}. Skipping.")
        return
    logger.info(f"Starting scheduled notification run at IST {now_ist.strftime('%H:%M')}")
    await orchestrator.run_pipeline(skip_crawl=True, skip_extraction=True, skip_llm=True)


async def scheduled_run():
    """Full pipeline run (manual/legacy use)."""
    logger.info("Starting full scheduled pipeline run")
    await orchestrator.run_pipeline()


async def scheduled_weekly_report():
    logger.info("Starting scheduled weekly report")
    await orchestrator.generate_weekly_report()


async def scheduled_weekly_digest():
    """Backward-compatible alias for older scheduler references."""
    await scheduled_weekly_report()


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


@app.post("/pipeline/weekly-report", response_model=WeeklyReportResponse)
async def trigger_weekly_report(request: Optional[WeeklyReportRequest] = None):
    """Generate the weekly feedback PDF report immediately."""
    request = request or WeeklyReportRequest()
    result = await orchestrator.generate_weekly_report(request.week_year, request.send_to_slack)
    return WeeklyReportResponse(**result)


@app.post("/pipeline/weekly-digest", response_model=WeeklyReportResponse)
async def trigger_weekly_digest(request: Optional[WeeklyReportRequest] = None):
    """Backward-compatible alias for the weekly report endpoint."""
    request = request or WeeklyReportRequest()
    result = await orchestrator.generate_weekly_report(request.week_year, request.send_to_slack)
    return WeeklyReportResponse(**result)


@app.post("/pipeline/test-notification", response_model=TestNotificationResponse)
async def trigger_test_notification(request: Optional[TestNotificationRequest] = None):
    """Send dummy articles to Slack and verify they were stored in ScyllaDB."""
    request = request or TestNotificationRequest()
    result = await orchestrator.send_dummy_articles_for_notification_test(
        count=request.count,
        channel=request.channel,
        title_prefix=request.title_prefix,
    )
    return TestNotificationResponse(**result)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "orchestrator", "scheduler": scheduler.running}
