"""
Deduplication Service - Eliminates duplicate articles.
"""
import logging
from contextlib import asynccontextmanager
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import sys
sys.path.insert(0, '/app')

from shared.models import Article, DeduplicationResult
from shared.utils import (
    ScyllaDBManager, RedisManager, initialize_schema,
    compute_hash, compute_url_hash, title_similarity,
)
from shared.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_manager = ScyllaDBManager()
redis_manager = RedisManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    db_manager.connect()
    initialize_schema(db_manager)
    await redis_manager.connect()
    config.load()
    logger.info("Dedup service started")
    yield
    db_manager.disconnect()
    await redis_manager.disconnect()
    logger.info("Dedup service stopped")


app = FastAPI(
    title="Deduplication Service",
    description="Eliminates duplicate articles",
    version="1.0.0",
    lifespan=lifespan,
)


class DedupRequest(BaseModel):
    """Request to check if article is duplicate."""
    article: Article


class DedupBatchRequest(BaseModel):
    """Batch deduplication request."""
    articles: List[Article]


class DedupBatchResponse(BaseModel):
    """Batch deduplication response."""
    total: int
    unique: int
    duplicates: int
    results: List[DeduplicationResult]


TITLE_SIMILARITY_THRESHOLD = 0.85


async def check_duplicate(article: Article) -> DeduplicationResult:
    """Check if article is a duplicate using multiple strategies."""
    
    # 1. URL hash check (Redis cache - fast)
    if article.url_hash:
        is_seen = await redis_manager.check_url_seen(article.url_hash)
        if is_seen:
            return DeduplicationResult(
                is_duplicate=True,
                match_type="url_hash",
                similarity_score=1.0,
            )
    
    # 2. Content hash check (Redis cache)
    if article.content_hash:
        is_seen = await redis_manager.check_content_seen(article.content_hash)
        if is_seen:
            return DeduplicationResult(
                is_duplicate=True,
                match_type="content_hash",
                similarity_score=1.0,
            )
    
    # 3. DOI check (ScyllaDB)
    if article.doi:
        existing = db_manager.check_dedup_hash("doi", article.doi)
        if existing:
            return DeduplicationResult(
                is_duplicate=True,
                duplicate_of=str(existing.get('article_id')),
                match_type="doi",
                similarity_score=1.0,
            )
    
    # 4. PMID check (ScyllaDB)
    if article.pmid:
        existing = db_manager.check_dedup_hash("pmid", article.pmid)
        if existing:
            return DeduplicationResult(
                is_duplicate=True,
                duplicate_of=str(existing.get('article_id')),
                match_type="pmid",
                similarity_score=1.0,
            )
    
    # 5. Title similarity check
    if article.title_hash:
        existing = db_manager.check_dedup_hash("title", article.title_hash)
        if existing:
            return DeduplicationResult(
                is_duplicate=True,
                duplicate_of=str(existing.get('article_id')),
                match_type="title_hash",
                similarity_score=1.0,
            )
    
    # Not a duplicate - add to indexes
    await _add_to_indexes(article)
    
    return DeduplicationResult(
        is_duplicate=False,
        match_type=None,
        similarity_score=0.0,
    )


async def _add_to_indexes(article: Article):
    """Add article to deduplication indexes."""
    # Add to Redis cache
    if article.url_hash:
        await redis_manager.mark_url_seen(article.url_hash, article.article_id)
    
    if article.content_hash:
        await redis_manager.mark_content_seen(article.content_hash, article.article_id)
    
    # Add to ScyllaDB
    if article.doi:
        db_manager.add_dedup_hash("doi", article.doi, article.article_id, article.source_id)
    
    if article.pmid:
        db_manager.add_dedup_hash("pmid", article.pmid, article.article_id, article.source_id)
    
    if article.title_hash:
        db_manager.add_dedup_hash("title", article.title_hash, article.article_id, article.source_id)


@app.post("/dedup/check", response_model=DeduplicationResult)
async def check_duplicate_endpoint(request: DedupRequest):
    """Check if article is duplicate."""
    return await check_duplicate(request.article)


@app.post("/dedup/batch", response_model=DedupBatchResponse)
async def batch_deduplicate(request: DedupBatchRequest):
    """Check multiple articles for duplicates."""
    results = []
    unique_count = 0
    duplicate_count = 0
    
    for article in request.articles:
        result = await check_duplicate(article)
        results.append(result)
        
        if result.is_duplicate:
            duplicate_count += 1
        else:
            unique_count += 1
    
    return DedupBatchResponse(
        total=len(request.articles),
        unique=unique_count,
        duplicates=duplicate_count,
        results=results,
    )


@app.get("/dedup/stats")
async def dedup_stats():
    """Get dedup index summary: total keys, counts by type, and Redis cache stats."""
    stats = {
        "redis": {"url_keys": 0, "content_keys": 0, "total_cached": 0},
        "scylladb": {"doi": 0, "pmid": 0, "title": 0, "total_indexed": 0},
        "total_keys": 0,
    }

    # Redis key counts
    try:
        url_keys = 0
        async for _ in redis_manager.client.scan_iter(match="seen:url:*", count=1000):
            url_keys += 1
        content_keys = 0
        async for _ in redis_manager.client.scan_iter(match="seen:content:*", count=1000):
            content_keys += 1

        stats["redis"]["url_keys"] = url_keys
        stats["redis"]["content_keys"] = content_keys
        stats["redis"]["total_cached"] = url_keys + content_keys
    except Exception as e:
        logger.warning(f"Failed to get Redis stats: {e}")

    # ScyllaDB dedup_index counts by hash_type
    try:
        for hash_type in ("doi", "pmid", "title"):
            rows = db_manager.execute(
                "SELECT COUNT(*) as cnt FROM governance_dedup_index WHERE hash_type = %s",
                (hash_type,)
            )
            for row in rows:
                stats["scylladb"][hash_type] = row.cnt
        stats["scylladb"]["total_indexed"] = (
            stats["scylladb"]["doi"]
            + stats["scylladb"]["pmid"]
            + stats["scylladb"]["title"]
        )
    except Exception as e:
        logger.warning(f"Failed to get ScyllaDB stats: {e}")

    stats["total_keys"] = stats["redis"]["total_cached"] + stats["scylladb"]["total_indexed"]

    return stats


@app.get("/health")
async def health():
    """Health check."""
    return {"status": "healthy", "service": "dedup"}