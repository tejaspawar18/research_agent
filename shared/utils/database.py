"""
ScyllaDB, Redis, Kafka, and S3 utilities for the Preventive Health Pipeline.
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
    """ScyllaDB database manager with environment-aware credentials."""

    def __init__(self):
        self.cluster: Optional[Cluster] = None
        self.session = None
        self.keyspace = os.getenv("SCYLLA_KEYSPACE", "ai_events")
        self.port = int(os.getenv("SCYLLA_PORT", 9042))
        self._prepared_stmts: Dict[str, Any] = {}

        # Environment-aware credential selection
        environment = os.getenv("ENVIRONMENT", "development").lower()

        if environment == "production":
            self.hosts = os.getenv("SCYLLA_HOST_PROD_LST", "scylladb").split(",")
            self.username = os.getenv("SCYLLA_USERNAME_PROD", "")
            self.password = os.getenv("SCYLLA_PASSWORD_PROD", "")
            logger.info("Using ScyllaDB PRODUCTION credentials")
        elif environment in ("beta", "staging"):
            self.hosts = os.getenv("SCYLLA_HOST_BETA", "scylladb").split(",")
            self.username = os.getenv("SCYLLA_USERNAME_BETA", "")
            self.password = os.getenv("SCYLLA_PASSWORD_BETA", "")
            logger.info("Using ScyllaDB BETA credentials")
        else:
            self.hosts = os.getenv("SCYLLA_HOSTS", "scylladb").split(",")
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
        # self.session.execute(f"""
        #     CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
        #     WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        # """)
        self.session.set_keyspace(self.keyspace)
        
        logger.info(f"Connected to ScyllaDB keyspace: {self.keyspace}")
    
    def disconnect(self):
        """Close connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Disconnected from ScyllaDB")
    
    def _get_prepared(self, query: str):
        """Get or create a prepared statement (protocol-level parameter binding)."""
        if query not in self._prepared_stmts:
            self._prepared_stmts[query] = self.session.prepare(query)
            self._prepared_stmts[query].consistency_level = ConsistencyLevel.LOCAL_QUORUM
        return self._prepared_stmts[query]

    def execute(self, query: str, parameters: tuple = None) -> Any:
        """Execute a query."""
        stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        return self.session.execute(stmt, parameters)

    def execute_prepared(self, query: str, parameters: tuple = None) -> Any:
        """Execute a query using a prepared statement (safe from CQL injection)."""
        prepared = self._get_prepared(query)
        return self.session.execute(prepared, parameters)

    def execute_async(self, query: str, parameters: tuple = None):
        """Execute a query asynchronously."""
        stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        return self.session.execute_async(stmt, parameters)
    
    # Article operations
    def insert_article(self, article: Dict[str, Any]):
        """Insert an article."""
        query = """
            INSERT INTO governance_articles (
                source_id, published_date, article_id, url, title, authors,
                abstract, full_text, project_area, sub_topic, keywords,
                source_quality, study_type, evidence_level, relevance_score,
                summary, key_findings, preventive_implications,
                content_hash, doi, pmid, crawled_at, processed_at, status
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """
        
        pub_date = article.get('published_date') or date.today()

        # Convert authors list of dicts to list of strings
        authors_raw = article.get('authors', [])
        if authors_raw and isinstance(authors_raw[0], dict):
            authors = [a.get('name', '') for a in authors_raw if a.get('name')]
        else:
            authors = authors_raw if isinstance(authors_raw, list) else []

        self.execute_prepared(query, (
            article.get('source_id'),
            pub_date,
            uuid.UUID(article.get('article_id', str(uuid.uuid4()))),
            article.get('url'),
            article.get('title'),
            authors,
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
            SELECT * FROM governance_articles
            WHERE source_id = ? AND published_date = ?
            LIMIT ?
        """
        rows = self.execute_prepared(query, (source_id, published_date, limit))
        return [dict(row._asdict()) for row in rows]
    
    def get_recent_articles(self, days: int = 7, limit: int = 1000) -> List[Dict]:
        """Get recent articles across all sources."""
        articles = []
        end_date = date.today()
        
        # Query each day (ScyllaDB partitioning)
        for i in range(days):
            query_date = date.fromordinal(end_date.toordinal() - i)
            query = """
                SELECT * FROM governance_articles
                WHERE published_date = ?
                ALLOW FILTERING
            """
            rows = self.execute_prepared(query, (query_date,))
            articles.extend([dict(row._asdict()) for row in rows])
            
            if len(articles) >= limit:
                break
        
        return articles[:limit]

    def get_unprocessed_articles(
        self,
        target_status: str = "notified",
        days: int = 7,
        limit: int = 500
    ) -> List[Dict]:
        """
        Get articles that haven't reached the target status.

        Status progression: unique → extracted → processed → notified

        This allows resuming the pipeline for articles that failed mid-processing.
        For example:
        - target_status='extracted' returns articles with status='unique'
        - target_status='processed' returns articles with status in ('unique', 'extracted')
        - target_status='notified' returns all articles not yet notified
        """
        # Status hierarchy for comparison
        status_order = {
            "crawled": 0,
            "unique": 1,
            "extracted": 2,
            "processed": 3,
            "notified": 4,
            "filtered_out": -1,  # Terminal state
            "failed": -2,        # Can be retried
        }

        target_level = status_order.get(target_status, 4)

        articles = []
        end_date = date.today()

        for i in range(days):
            query_date = date.fromordinal(end_date.toordinal() - i)
            query = """
                SELECT * FROM governance_articles
                WHERE published_date = ?
                ALLOW FILTERING
            """
            rows = self.execute_prepared(query, (query_date,))

            for row in rows:
                row_dict = dict(row._asdict())
                article_status = row_dict.get("status", "crawled")
                article_level = status_order.get(article_status, 0)

                # Include if article hasn't reached target status yet
                # Also include failed articles for retry
                if 0 <= article_level < target_level or article_status == "failed":
                    articles.append(row_dict)

            if len(articles) >= limit:
                break

        logger.info(f"Found {len(articles)} unprocessed articles (target: {target_status}, days: {days})")
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
        updates = ["status = ?"]
        values = [status]

        for key, value in kwargs.items():
            if value is not None:
                updates.append(f"{key} = ?")
                values.append(value)

        values.extend([source_id, published_date, uuid.UUID(article_id)])

        query = f"""
            UPDATE governance_articles SET {', '.join(updates)}
            WHERE source_id = ? AND published_date = ? AND article_id = ?
        """
        self.execute_prepared(query, tuple(values))
    
    # Dedup operations
    def add_dedup_hash(self, hash_type: str, hash_value: str, article_id: str, source_id: str):
        """Add a deduplication hash."""
        query = """
            INSERT INTO governance_dedup_index (hash_type, hash_value, article_id, source_id, created_at)
            VALUES (?, ?, ?, ?, ?)
        """
        self.execute_prepared(query, (hash_type, hash_value, uuid.UUID(article_id), source_id, datetime.utcnow()))
    
    def check_dedup_hash(self, hash_type: str, hash_value: str) -> Optional[Dict]:
        """Check if a hash exists."""
        query = """
            SELECT * FROM governance_dedup_index
            WHERE hash_type = ? AND hash_value = ?
        """
        rows = self.execute_prepared(query, (hash_type, hash_value))
        for row in rows:
            return dict(row._asdict())
        return None
    
    # Source operations
    def upsert_source(self, source: Dict[str, Any]):
        """Insert or update a source configuration."""
        query = """
            INSERT INTO governance_sources (
                source_id, name, url, source_type, quality_tier,
                crawl_method, rate_limit, enabled, last_crawled, config
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.execute_prepared(query, (
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
        query = "SELECT * FROM governance_sources WHERE enabled = true ALLOW FILTERING"
        rows = self.execute(query)
        return [dict(row._asdict()) for row in rows]
    
    def update_source_last_crawled(self, source_id: str):
        """Update last crawled timestamp."""
        query = "UPDATE governance_sources SET last_crawled = ? WHERE source_id = ?"
        self.execute_prepared(query, (datetime.utcnow(), source_id))
    
    # Pipeline run operations
    def insert_pipeline_run(self, run: Dict[str, Any]):
        """Insert a pipeline run record."""
        query = """
            INSERT INTO governance_pipeline_runs (
                run_date, run_id, started_at, completed_at, status,
                articles_crawled, articles_deduplicated, articles_filtered,
                articles_summarized, articles_notified, errors
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.execute_prepared(query, (
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
                updates.append(f"{key} = ?")
                values.append(value)

        if not updates:
            return

        values.extend([run_date, uuid.UUID(run_id)])

        query = f"""
            UPDATE governance_pipeline_runs SET {', '.join(updates)}
            WHERE run_date = ? AND run_id = ?
        """
        self.execute_prepared(query, tuple(values))

    # Slack feedback operations
    def insert_slack_message(
        self,
        week_year: str,
        message_ts: str,
        channel: str,
        article_id: str,
        source_id: str,
        published_date: date,
        project_area: str,
        title: str,
        url: str,
        summary: str,
    ):
        """Insert a Slack message record for feedback correlation."""
        query = """
            INSERT INTO governance_slack_messages (
                week_year, message_ts, channel, article_id, source_id,
                published_date, project_area, title, url, summary, sent_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.execute_prepared(query, (
            week_year,
            message_ts,
            channel,
            uuid.UUID(article_id),
            source_id,
            published_date,
            project_area,
            title,
            url,
            summary,
            datetime.utcnow(),
        ))

    def get_slack_message(self, week_year: str, message_ts: str) -> Optional[Dict]:
        """Get Slack message details by week_year and message_ts."""
        query = """
            SELECT * FROM governance_slack_messages
            WHERE week_year = ? AND message_ts = ?
        """
        rows = self.execute_prepared(query, (week_year, message_ts))
        for row in rows:
            return dict(row._asdict())
        return None

    def insert_article_feedback(
        self,
        week_year: str,
        article_id: str,
        message_ts: str,
        channel: str,
        user_id: str,
        user_name: str,
        feedback_type: str,
        feedback_id: Any = None,
        comment: str = None,
        created_at: datetime = None,
    ):
        """Insert article feedback from Slack user."""
        query = """
            INSERT INTO governance_article_feedback (
                week_year, feedback_id, article_id, message_ts, channel,
                user_id, user_name, feedback_type, comment, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        if feedback_id is None:
            feedback_uuid = uuid.uuid4()
        elif isinstance(feedback_id, uuid.UUID):
            feedback_uuid = feedback_id
        else:
            feedback_uuid = uuid.UUID(str(feedback_id))

        self.execute_prepared(query, (
            week_year,
            feedback_uuid,
            uuid.UUID(article_id),
            message_ts,
            channel,
            user_id,
            user_name,
            feedback_type,
            comment,
            created_at or datetime.utcnow(),
        ))

    def delete_article_feedback(self, week_year: str, feedback_id: Any):
        """Delete a feedback row by deterministic id."""
        query = """
            DELETE FROM governance_article_feedback
            WHERE week_year = ? AND feedback_id = ?
        """
        if isinstance(feedback_id, uuid.UUID):
            feedback_uuid = feedback_id
        else:
            feedback_uuid = uuid.UUID(str(feedback_id))
        self.execute_prepared(query, (week_year, feedback_uuid))

    def get_positive_feedback_articles(self, week_year: str) -> List[Dict]:
        """Get articles with positive feedback for a given week."""
        query = """
            SELECT * FROM governance_article_feedback
            WHERE week_year = ?
        """
        rows = self.execute_prepared(query, (week_year,))
        # Filter in Python since we need feedback_type='positive'
        positive = [dict(row._asdict()) for row in rows if row.feedback_type == "positive"]
        return positive

    def get_slack_messages_for_week(self, week_year: str) -> List[Dict]:
        """Get all Slack messages for a given week."""
        query = """
            SELECT * FROM governance_slack_messages
            WHERE week_year = ?
        """
        rows = self.execute_prepared(query, (week_year,))
        return [dict(row._asdict()) for row in rows]

    def insert_weekly_digest(
        self,
        year: int,
        week_number: int,
        project_area: str,
        channel: str,
        articles_count: int,
        message_ts: str = None,
    ):
        """Insert a weekly digest record."""
        query = """
            INSERT INTO governance_weekly_digests (
                year, week_number, digest_id, project_area, channel,
                articles_count, message_ts, sent_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.execute_prepared(query, (
            year,
            week_number,
            uuid.uuid4(),
            project_area,
            channel,
            articles_count,
            message_ts,
            datetime.utcnow(),
        ))

    def get_weekly_digest(self, year: int, week_number: int) -> List[Dict]:
        """Get weekly digest records for a given week."""
        query = """
            SELECT * FROM governance_weekly_digests
            WHERE year = ? AND week_number = ?
        """
        rows = self.execute_prepared(query, (year, week_number))
        return [dict(row._asdict()) for row in rows]


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
    
    async def mark_url_seen(self, url_hash: str, article_id: str):
        await self.set(f"seen:url:{url_hash}", article_id)

    async def check_content_seen(self, content_hash: str) -> bool:
        return await self.exists(f"seen:content:{content_hash}")

    async def mark_content_seen(self, content_hash: str, article_id: str):
        await self.set(f"seen:content:{content_hash}", article_id)


# Schema initialization
SCYLLA_SCHEMA = """
-- Articles table (time-series partitioned)
CREATE TABLE IF NOT EXISTS governance_articles (
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
CREATE TABLE IF NOT EXISTS governance_sources (
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

-- Deduplication index (no TTL — dedup records are permanent)
CREATE TABLE IF NOT EXISTS governance_dedup_index (
    hash_type text,
    hash_value text,
    article_id uuid,
    source_id text,
    created_at timestamp,
    PRIMARY KEY ((hash_type), hash_value)
);
-- Remove TTL on existing table if it was previously created with one
ALTER TABLE governance_dedup_index WITH default_time_to_live = 0;

-- Pipeline runs
CREATE TABLE IF NOT EXISTS governance_pipeline_runs (
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
CREATE INDEX IF NOT EXISTS idx_governance_articles_status ON governance_articles (status);
CREATE INDEX IF NOT EXISTS idx_governance_articles_project ON governance_articles (project_area);

-- Slack messages for feedback correlation (week-partitioned for efficient queries)
CREATE TABLE IF NOT EXISTS governance_slack_messages (
    week_year text,
    message_ts text,
    channel text,
    article_id uuid,
    source_id text,
    published_date date,
    project_area text,
    title text,
    url text,
    summary text,
    sent_at timestamp,
    PRIMARY KEY ((week_year), message_ts)
) WITH CLUSTERING ORDER BY (message_ts DESC);

-- Article feedback from Slack users
CREATE TABLE IF NOT EXISTS governance_article_feedback (
    week_year text,
    feedback_id uuid,
    article_id uuid,
    message_ts text,
    channel text,
    user_id text,
    user_name text,
    feedback_type text,
    comment text,
    created_at timestamp,
    PRIMARY KEY ((week_year), feedback_id)
) WITH CLUSTERING ORDER BY (feedback_id DESC);

-- Weekly digest tracking
CREATE TABLE IF NOT EXISTS governance_weekly_digests (
    year int,
    week_number int,
    digest_id uuid,
    project_area text,
    channel text,
    articles_count int,
    message_ts text,
    sent_at timestamp,
    PRIMARY KEY ((year), week_number, digest_id)
) WITH CLUSTERING ORDER BY (week_number DESC, digest_id DESC);

-- Index for looking up feedback by article
CREATE INDEX IF NOT EXISTS idx_governance_feedback_article ON governance_article_feedback (article_id);
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


class KafkaManager:
    """Kafka producer/consumer for reliable event streaming between pipeline stages."""

    def __init__(self):
        self.producer = None
        self.environment = os.getenv("ENVIRONMENT", "development").lower()

        if self.environment == "production":
            self.brokers = os.getenv("KAFKA_BROKER_PROD", "localhost:9092")
        elif self.environment in ("beta", "staging"):
            self.brokers = os.getenv("KAFKA_BROKER_BETA", "localhost:9092")
        else:
            self.brokers = os.getenv("KAFKA_BROKER", "localhost:9092")

        self.client_id = os.getenv("PRODUCER_CLIENT_ID", "preventive-health-pipeline-producer")
        self.topic_articles = os.getenv("KAFKA_TOPIC_ARTICLES", "preventive-health-articles")
        self.topic_events = os.getenv("KAFKA_TOPIC_EVENTS", "preventive-health-events")

    async def connect(self):
        """Initialize Kafka producer."""
        try:
            from aiokafka import AIOKafkaProducer
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.brokers,
                client_id=self.client_id,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # Wait for all replicas to acknowledge
                retries=3,
                retry_backoff_ms=500,
            )
            await self.producer.start()
            logger.info(f"Kafka producer connected to {self.brokers}")
        except Exception as e:
            logger.warning(f"Kafka connection failed (pipeline will continue without Kafka): {e}")
            self.producer = None

    async def disconnect(self):
        """Shutdown Kafka producer."""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer disconnected")

    async def publish_articles(self, stage: str, articles: List[Dict[str, Any]], run_id: str = None):
        """Publish articles to Kafka topic for a given pipeline stage."""
        if not self.producer:
            return

        try:
            for article in articles:
                message = {
                    "stage": stage,
                    "run_id": run_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "article": article,
                }
                key = article.get("article_id") or article.get("url", "unknown")
                await self.producer.send_and_wait(
                    self.topic_articles,
                    value=message,
                    key=key,
                )
            await self.producer.flush()
            logger.info(f"Published {len(articles)} articles to Kafka topic '{self.topic_articles}' [stage={stage}]")
        except Exception as e:
            logger.error(f"Failed to publish articles to Kafka: {e}")

    async def publish_event(self, event_type: str, data: Dict[str, Any]):
        """Publish a pipeline event to Kafka."""
        if not self.producer:
            return

        try:
            event = {
                "event_type": event_type,
                "timestamp": datetime.utcnow().isoformat(),
                "environment": self.environment,
                "data": data,
            }
            await self.producer.send_and_wait(
                self.topic_events,
                value=event,
                key=event_type,
            )
            logger.info(f"Published event '{event_type}' to Kafka")
        except Exception as e:
            logger.error(f"Failed to publish event to Kafka: {e}")


class S3Manager:
    """AWS S3 manager for backing up pipeline data."""

    def __init__(self):
        self.client = None
        self.bucket = os.getenv("S3_BUCKET", "pulse-narrative")
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        self.region = os.getenv("AWS_REGION", "ap-south-1")

    def connect(self):
        """Initialize S3 client."""
        try:
            import boto3
            self.client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.region,
            )
            logger.info(f"S3 client initialized for bucket '{self.bucket}'")
        except Exception as e:
            logger.warning(f"S3 connection failed (pipeline will continue without S3 backup): {e}")
            self.client = None

    def upload_articles(self, articles: List[Dict[str, Any]], run_id: str, stage: str = "processed"):
        """Upload articles JSON to S3 as backup."""
        if not self.client:
            return

        try:
            key = f"governance/pipeline/{stage}/{date.today().isoformat()}/{run_id}.json"
            body = json.dumps(articles, indent=2, default=str)

            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
            logger.info(f"Uploaded {len(articles)} articles to s3://{self.bucket}/{key}")
        except Exception as e:
            logger.error(f"Failed to upload articles to S3: {e}")

    def upload_pipeline_run(self, run_data: Dict[str, Any], run_id: str):
        """Upload pipeline run metadata to S3."""
        if not self.client:
            return

        try:
            key = f"governance/pipeline/runs/{date.today().isoformat()}/{run_id}_meta.json"
            body = json.dumps(run_data, indent=2, default=str)

            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
            logger.info(f"Uploaded pipeline run metadata to s3://{self.bucket}/{key}")
        except Exception as e:
            logger.error(f"Failed to upload pipeline run to S3: {e}")

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL for S3 folder organization."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Remove www. prefix
            if domain.startswith("www."):
                domain = domain[4:]
            # Replace dots with underscores for folder safety
            domain = domain.replace(".", "_")
            return domain or "unknown"
        except Exception:
            return "unknown"

    def upload_article_fulltext(
        self,
        article_id: str,
        domain: str,
        full_text: str,
        published_date: Optional[date] = None,
    ):
        """Upload extracted full text for a single article to S3."""
        if not self.client:
            return

        try:
            pub_date = (published_date or date.today()).isoformat()
            key = f"governance/pipeline/articles/{pub_date}/{domain}/{article_id}_fulltext.txt"

            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=full_text.encode("utf-8"),
                ContentType="text/plain; charset=utf-8",
            )
            logger.info(f"Uploaded full text to s3://{self.bucket}/{key}")
        except Exception as e:
            logger.error(f"Failed to upload article full text to S3: {e}")

    def upload_article_json(
        self,
        article_dict: Dict[str, Any],
        published_date: Optional[date] = None,
    ):
        """Upload complete article JSON to S3 (individual file per article)."""
        if not self.client:
            return

        try:
            article_id = str(article_dict.get("article_id", "unknown"))
            url = article_dict.get("url", "")
            domain = self._extract_domain(url)
            pub_date = (published_date or date.today()).isoformat()
            key = f"governance/pipeline/articles/{pub_date}/{domain}/{article_id}.json"
            body = json.dumps(article_dict, indent=2, default=str)

            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
            logger.info(f"Uploaded article JSON to s3://{self.bucket}/{key}")
        except Exception as e:
            logger.error(f"Failed to upload article JSON to S3: {e}")

    def upload_article(
        self,
        article_dict: Dict[str, Any],
        published_date: Optional[date] = None,
    ):
        """Upload article JSON and full text to S3 (organized by date/domain)."""
        if not self.client:
            return

        full_text = article_dict.get("full_text")
        article_id = str(article_dict.get("article_id", "unknown"))
        url = article_dict.get("url", "")
        domain = self._extract_domain(url)

        if full_text:
            self.upload_article_fulltext(article_id, domain, full_text, published_date)

        self.upload_article_json(article_dict, published_date)
