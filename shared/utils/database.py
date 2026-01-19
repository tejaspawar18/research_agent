"""
ScyllaDB database utilities for the Preventive Health Pipeline.
"""
import os
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, date
import uuid
from contextlib import asynccontextmanager

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement
from cassandra import ConsistencyLevel
import redis.asyncio as redis
import json

logger = logging.getLogger(__name__)


class ScyllaDBManager:
    """ScyllaDB database manager with async-friendly interface."""
    
    def __init__(self):
        self.cluster: Optional[Cluster] = None
        self.session = None
        self.keyspace = os.getenv("SCYLLA_KEYSPACE", "preventive_health")
        self.hosts = os.getenv("SCYLLA_HOSTS", "scylladb").split(",")
        self.port = int(os.getenv("SCYLLA_PORT", 9042))
        self.username = os.getenv("SCYLLA_USERNAME", "")
        self.password = os.getenv("SCYLLA_PASSWORD", "")
    
    def connect(self):
        """Create connection to ScyllaDB."""
        auth_provider = None
        if self.username and self.password:
            auth_provider = PlainTextAuthProvider(
                username=self.username,
                password=self.password
            )
        
        self.cluster = Cluster(
            contact_points=self.hosts,
            port=self.port,
            auth_provider=auth_provider,
        )
        self.session = self.cluster.connect()
        
        # Create keyspace if not exists
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        self.session.set_keyspace(self.keyspace)
        
        logger.info(f"Connected to ScyllaDB keyspace: {self.keyspace}")
    
    def disconnect(self):
        """Close connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Disconnected from ScyllaDB")
    
    def execute(self, query: str, parameters: tuple = None) -> Any:
        """Execute a query."""
        stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        return self.session.execute(stmt, parameters)
    
    def execute_async(self, query: str, parameters: tuple = None):
        """Execute a query asynchronously."""
        stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        return self.session.execute_async(stmt, parameters)
    
    # Article operations
    def insert_article(self, article: Dict[str, Any]):
        """Insert an article."""
        query = """
            INSERT INTO articles (
                source_id, published_date, article_id, url, title, authors,
                abstract, full_text, project_area, sub_topic, keywords,
                source_quality, study_type, evidence_level, relevance_score,
                summary, key_findings, preventive_implications,
                content_hash, doi, pmid, crawled_at, processed_at, status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        
        pub_date = article.get('published_date') or date.today()
        
        self.execute(query, (
            article.get('source_id'),
            pub_date,
            uuid.UUID(article.get('article_id', str(uuid.uuid4()))),
            article.get('url'),
            article.get('title'),
            article.get('authors', []),
            article.get('abstract'),
            article.get('full_text'),
            article.get('project_area'),
            article.get('sub_topic'),
            article.get('keywords', []),
            article.get('source_quality'),
            article.get('study_type'),
            article.get('evidence_level'),
            article.get('relevance_score'),
            article.get('summary'),
            article.get('key_findings', []),
            article.get('preventive_implications'),
            article.get('content_hash'),
            article.get('doi'),
            article.get('pmid'),
            article.get('crawled_at', datetime.utcnow()),
            article.get('processed_at'),
            article.get('status', 'crawled'),
        ))
    
    def get_articles_by_date(
        self,
        source_id: str,
        published_date: date,
        limit: int = 100
    ) -> List[Dict]:
        """Get articles by source and date."""
        query = """
            SELECT * FROM articles
            WHERE source_id = %s AND published_date = %s
            LIMIT %s
        """
        rows = self.execute(query, (source_id, published_date, limit))
        return [dict(row._asdict()) for row in rows]
    
    def get_recent_articles(self, days: int = 7, limit: int = 1000) -> List[Dict]:
        """Get recent articles across all sources."""
        articles = []
        end_date = date.today()
        
        # Query each day (ScyllaDB partitioning)
        for i in range(days):
            query_date = date.fromordinal(end_date.toordinal() - i)
            query = """
                SELECT * FROM articles
                WHERE published_date = %s
                ALLOW FILTERING
            """
            rows = self.execute(query, (query_date,))
            articles.extend([dict(row._asdict()) for row in rows])
            
            if len(articles) >= limit:
                break
        
        return articles[:limit]
    
    def update_article_status(
        self,
        source_id: str,
        published_date: date,
        article_id: str,
        status: str,
        **kwargs
    ):
        """Update article status and optional fields."""
        updates = ["status = %s"]
        values = [status]
        
        for key, value in kwargs.items():
            if value is not None:
                updates.append(f"{key} = %s")
                values.append(value)
        
        values.extend([source_id, published_date, uuid.UUID(article_id)])
        
        query = f"""
            UPDATE articles SET {', '.join(updates)}
            WHERE source_id = %s AND published_date = %s AND article_id = %s
        """
        self.execute(query, tuple(values))
    
    # Dedup operations
    def add_dedup_hash(self, hash_type: str, hash_value: str, article_id: str, source_id: str):
        """Add a deduplication hash."""
        query = """
            INSERT INTO dedup_index (hash_type, hash_value, article_id, source_id, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        self.execute(query, (hash_type, hash_value, uuid.UUID(article_id), source_id, datetime.utcnow()))
    
    def check_dedup_hash(self, hash_type: str, hash_value: str) -> Optional[Dict]:
        """Check if a hash exists."""
        query = """
            SELECT * FROM dedup_index
            WHERE hash_type = %s AND hash_value = %s
        """
        rows = self.execute(query, (hash_type, hash_value))
        for row in rows:
            return dict(row._asdict())
        return None
    
    # Source operations
    def upsert_source(self, source: Dict[str, Any]):
        """Insert or update a source configuration."""
        query = """
            INSERT INTO sources (
                source_id, name, url, source_type, quality_tier,
                crawl_method, rate_limit, enabled, last_crawled, config
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.execute(query, (
            source.get('source_id'),
            source.get('name'),
            source.get('url'),
            source.get('source_type'),
            source.get('quality_tier'),
            source.get('crawl_method'),
            source.get('rate_limit', 10),
            source.get('enabled', True),
            source.get('last_crawled'),
            source.get('config', {}),
        ))
    
    def get_enabled_sources(self) -> List[Dict]:
        """Get all enabled sources."""
        query = "SELECT * FROM sources WHERE enabled = true ALLOW FILTERING"
        rows = self.execute(query)
        return [dict(row._asdict()) for row in rows]
    
    def update_source_last_crawled(self, source_id: str):
        """Update last crawled timestamp."""
        query = "UPDATE sources SET last_crawled = %s WHERE source_id = %s"
        self.execute(query, (datetime.utcnow(), source_id))
    
    # Pipeline run operations
    def insert_pipeline_run(self, run: Dict[str, Any]):
        """Insert a pipeline run record."""
        query = """
            INSERT INTO pipeline_runs (
                run_date, run_id, started_at, completed_at, status,
                articles_crawled, articles_deduplicated, articles_filtered,
                articles_summarized, articles_notified, errors
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.execute(query, (
            run.get('run_date', date.today()),
            uuid.UUID(run.get('run_id', str(uuid.uuid4()))),
            run.get('started_at', datetime.utcnow()),
            run.get('completed_at'),
            run.get('status', 'running'),
            run.get('articles_crawled', 0),
            run.get('articles_deduplicated', 0),
            run.get('articles_filtered', 0),
            run.get('articles_summarized', 0),
            run.get('articles_notified', 0),
            run.get('errors', []),
        ))
    
    def update_pipeline_run(self, run_date: date, run_id: str, **kwargs):
        """Update pipeline run."""
        updates = []
        values = []
        
        for key, value in kwargs.items():
            if value is not None:
                updates.append(f"{key} = %s")
                values.append(value)
        
        if not updates:
            return
        
        values.extend([run_date, uuid.UUID(run_id)])
        
        query = f"""
            UPDATE pipeline_runs SET {', '.join(updates)}
            WHERE run_date = %s AND run_id = %s
        """
        self.execute(query, tuple(values))


class RedisManager:
    """Redis cache and queue manager."""
    
    def __init__(self):
        self.client: Optional[redis.Redis] = None
        self.url = os.getenv("REDIS_URL", "redis://localhost:6379")
    
    async def connect(self):
        """Create Redis connection."""
        self.client = redis.from_url(self.url, decode_responses=True)
        logger.info("Redis connection created")
    
    async def disconnect(self):
        """Close Redis connection."""
        if self.client:
            await self.client.close()
            logger.info("Redis connection closed")
    
    # Cache operations
    async def get(self, key: str) -> Optional[str]:
        return await self.client.get(key)
    
    async def set(self, key: str, value: str, expire: int = None):
        await self.client.set(key, value, ex=expire)
    
    async def delete(self, key: str):
        await self.client.delete(key)
    
    async def exists(self, key: str) -> bool:
        return await self.client.exists(key) > 0
    
    # JSON operations
    async def get_json(self, key: str) -> Optional[Dict]:
        value = await self.get(key)
        return json.loads(value) if value else None
    
    async def set_json(self, key: str, value: Dict, expire: int = None):
        await self.set(key, json.dumps(value), expire)
    
    # Queue operations
    async def push_queue(self, queue_name: str, item: str):
        await self.client.lpush(queue_name, item)
    
    async def pop_queue(self, queue_name: str, timeout: int = 0) -> Optional[str]:
        result = await self.client.brpop(queue_name, timeout=timeout)
        return result[1] if result else None
    
    async def queue_length(self, queue_name: str) -> int:
        return await self.client.llen(queue_name)
    
    # Rate limiting
    async def rate_limit_check(self, key: str, limit: int, window: int) -> bool:
        """Check rate limit. Returns True if allowed."""
        current = await self.client.incr(key)
        if current == 1:
            await self.client.expire(key, window)
        return current <= limit
    
    # Dedup cache
    async def check_url_seen(self, url_hash: str) -> bool:
        return await self.exists(f"seen:url:{url_hash}")
    
    async def mark_url_seen(self, url_hash: str, article_id: str, ttl: int = 604800):
        await self.set(f"seen:url:{url_hash}", article_id, ttl)
    
    async def check_content_seen(self, content_hash: str) -> bool:
        return await self.exists(f"seen:content:{content_hash}")
    
    async def mark_content_seen(self, content_hash: str, article_id: str, ttl: int = 604800):
        await self.set(f"seen:content:{content_hash}", article_id, ttl)


# Schema initialization
SCYLLA_SCHEMA = """
-- Articles table (time-series partitioned)
CREATE TABLE IF NOT EXISTS articles (
    source_id text,
    published_date date,
    article_id uuid,
    url text,
    title text,
    authors list<text>,
    abstract text,
    full_text text,
    project_area text,
    sub_topic text,
    keywords list<text>,
    source_quality text,
    study_type text,
    evidence_level int,
    relevance_score float,
    summary text,
    key_findings list<text>,
    preventive_implications text,
    content_hash text,
    doi text,
    pmid text,
    crawled_at timestamp,
    processed_at timestamp,
    notified_at timestamp,
    status text,
    PRIMARY KEY ((source_id, published_date), article_id)
) WITH CLUSTERING ORDER BY (article_id DESC)
  AND default_time_to_live = 31536000;

-- Sources configuration
CREATE TABLE IF NOT EXISTS sources (
    source_id text PRIMARY KEY,
    name text,
    url text,
    source_type text,
    quality_tier text,
    crawl_method text,
    rate_limit int,
    enabled boolean,
    last_crawled timestamp,
    config map<text, text>
);

-- Deduplication index
CREATE TABLE IF NOT EXISTS dedup_index (
    hash_type text,
    hash_value text,
    article_id uuid,
    source_id text,
    created_at timestamp,
    PRIMARY KEY ((hash_type), hash_value)
) WITH default_time_to_live = 604800;

-- Pipeline runs
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_date date,
    run_id uuid,
    started_at timestamp,
    completed_at timestamp,
    status text,
    articles_crawled int,
    articles_deduplicated int,
    articles_filtered int,
    articles_summarized int,
    articles_notified int,
    errors list<text>,
    PRIMARY KEY ((run_date), run_id)
) WITH CLUSTERING ORDER BY (run_id DESC);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_articles_status ON articles (status);
CREATE INDEX IF NOT EXISTS idx_articles_project ON articles (project_area);
"""


def initialize_schema(db: ScyllaDBManager):
    """Initialize database schema."""
    for statement in SCYLLA_SCHEMA.split(';'):
        statement = statement.strip()
        if statement:
            try:
                db.execute(statement)
            except Exception as e:
                logger.warning(f"Schema statement failed (may already exist): {e}")
    
    logger.info("ScyllaDB schema initialized")