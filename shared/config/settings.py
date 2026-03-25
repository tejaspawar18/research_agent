"""
Configuration management for the Preventive Health Pipeline.
"""
import os
import yaml
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class SlackConfig(BaseModel):
    """Slack configuration."""
    bot_token: Optional[str] = None
    app_token: Optional[str] = None
    signing_secret: Optional[str] = None
    
    # Channel mappings for project areas
    channels: Dict[str, str] = Field(default_factory=lambda: {
        "disease_prevention": "#disease-prevention",
        "behavioral_protocols": "#behavioral-protocols",
        "nutritional_protocols": "#nutritional-protocols",
        "government_interventions": "#government-interventions",
        "youth_health": "#youth-health",
        "general": "#research-general",
    })


class LLMConfig(BaseModel):
    """LLM configuration."""
    provider: str = "gemini"
    api_key: Optional[str] = None
    model: str = "gemini-2.0-flash"
    max_tokens: int = 2000
    temperature: float = 0.3


class PipelineConfig(BaseModel):
    """Pipeline configuration."""
    schedule: str = "*/30 3-13 * * *"  # Every 30 minutes, 9 AM - 7 PM IST (UTC+5:30)
    max_articles_per_run: int = 5000
    max_articles_per_source: int = 1000
    relevance_threshold: float = 60.0
    parallel_crawlers: int = 5
    dedup_similarity_threshold: float = 0.85

    # Article date window
    crawl_lookback_days: int = 7          # How many days back to load articles from DB after crawl
    incomplete_lookback_days: int = 7     # How many days back to look for incomplete articles
    max_articles_per_source_query: int = 1000   # Per-source per-date DB query limit
    max_notification_articles: int = 20   # Max articles sent to Slack per run

    # Quality filters
    min_sample_size_observational: int = 150
    min_sample_size_rct: int = 50

    # Timeouts (seconds)
    crawler_timeout: float = 900.0
    dedup_timeout: float = 120.0
    extraction_timeout: float = 60.0
    llm_timeout: float = 300.0
    notification_timeout: float = 60.0
    slack_api_timeout: float = 30.0
    llm_api_timeout: float = 150.0
    gateway_proxy_timeout: float = 120.0
    health_check_timeout: float = 5.0
    doi_resolve_timeout: float = 30.0
    unpaywall_timeout: float = 30.0
    slack_modal_timeout: float = 30.0
    html_fetch_timeout: float = 60.0
    rss_fetch_timeout: float = 60.0
    pubmed_search_timeout: float = 60.0
    pubmed_fetch_timeout: float = 120.0
    pdf_download_timeout: float = 120.0
    pmc_fetch_timeout: float = 120.0
    pmid_lookup_timeout: float = 30.0

    # Crawler settings
    max_articles_per_source_crawl: int = 2000 # Max articles per source in orchestrator crawl trigger
    crawler_poll_interval: int = 20           # Seconds between crawler status polls
    crawler_poll_max_retries: int = 120       # Max number of poll retries
    crawler_source_delay: int = 3             # Seconds between crawling each source
    crawler_rate_limit_window: int = 120      # Rate limit window in seconds
    cron_schedule: str = "0 */2 * * *"        # Crawler cron schedule
    default_max_articles_per_source: int = 100  # Default max articles per source for crawler

    # Extraction settings
    min_abstract_length: int = 100            # Min abstract length to use as fallback
    min_fulltext_length: int = 100            # Min full text length to accept extraction
    llm_batch_size: int = 10                  # Articles per LLM batch
    llm_rate_limit_delay: float = 0.3         # Seconds between LLM API calls

    # Notification settings
    notification_sleep_interval: int = 1      # Seconds between Slack messages
    redis_queue_timeout: int = 1              # Redis queue pop timeout
    notify_start_hour: int = 9                # Notification window start hour (IST)
    notify_start_minute: int = 30             # Notification window start minute
    notify_end_hour: int = 18                 # Notification window end hour (IST)
    notify_end_minute: int = 30               # Notification window end minute
    min_relevance_score: int = 20             # Min relevance score for notification
    min_relevance_score_llm: int = 20         # Min relevance score from LLM summarization

    # Evidence and quality settings
    abstract_only_evidence_cap: int = 1       # Max evidence level for abstract-only articles

    # Digest settings
    weekly_digest_lookback_days: int = 7      # Days to look back for weekly digest
    digest_max_articles: int = 10             # Max articles in digest display

    # Weekly report settings
    weekly_report_lookback_days: int = 7
    weekly_report_top_articles_per_channel: int = 5
    weekly_report_top_users: int = 10
    weekly_report_output_dir: str = "data/weekly_reports"

    # Schedule settings (UTC times)
    pipeline_morning_hour: int = 2            # 8 AM IST = 2:30 AM UTC
    pipeline_morning_minute: int = 30
    pipeline_afternoon_hour: int = 8          # 2 PM IST = 8:30 AM UTC
    pipeline_afternoon_minute: int = 30
    notification_cron_hours: str = "4-13"     # UTC hours for notification cron
    notification_cron_minutes: str = "0,30"   # Minutes for notification cron
    digest_hour: int = 3                      # Weekly digest hour (UTC)
    digest_minute: int = 30                   # Weekly digest minute (UTC)
    digest_day_of_week: str = "mon"           # Weekly digest day
    weekly_report_hour: int = 4               # Monday 10:00 AM IST = 4:30 AM UTC
    weekly_report_minute: int = 30
    weekly_report_day_of_week: str = "mon"

    # Worker settings
    extraction_worker_batch_size: int = 50
    extraction_parallel_workers: int = 5
    extraction_worker_cron: str = "*/30 * * * *"
    extraction_continuous_interval: int = 60  # Seconds between continuous extraction cycles

    # PDF settings
    pdf_max_size_mb: int = 20
    pdf_max_pages: int = 100

    # Text limits
    abstract_max_length: int = 5000
    html_abstract_max_length: int = 3000
    classify_abstract_truncate: int = 2000
    summarize_fulltext_truncate: int = 6000
    summarize_abstract_truncate: int = 3000
    slack_section_text_limit: int = 2900
    digest_summary_truncate: int = 150
    title_min_length: int = 10
    keyword_max_count: int = 20

    # Keyword classification thresholds
    keyword_high_confidence_score: int = 5
    keyword_medium_confidence_score: int = 3
    keyword_low_confidence_score: int = 2

    # LLM settings
    llm_max_tokens: int = 16000
    llm_temperature: float = 0.3
    classify_temperature: float = 0.2

    # PubMed search settings
    pubmed_initial_lookback_days: int = 30
    pubmed_extended_lookback_days: int = 90


class Settings(BaseSettings):
    """Application settings from environment."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: str = "development"
    debug: bool = False
    log_level: str = "INFO"
    # ScyllaDB
    scylla_hosts: str = "scylladb"
    scylla_port: int = 9042
    scylla_keyspace: str = "ai_events"
    scylla_username: str = ""
    scylla_password: str = ""
    # Redis
    redis_url: str = "redis://redis:6379"
    # LLM
    llm_provider: str = "openai"
    llm_api_key: Optional[str] = None
    llm_model: str = "gpt-4o-mini"
    # Slack
    slack_bot_token: Optional[str] = None
    slack_app_token: Optional[str] = None
    slack_signing_secret: Optional[str] = None

    # Service URLs
    crawler_url: str = "http://crawler:8001"
    dedup_url: str = "http://dedup:8002"
    extraction_url: str = "http://extraction:8003"
    llm_url: str = "http://llm:8004"
    notification_url: str = "http://notification:8005"
    orchestrator_url: str = "http://orchestrator:8006"

    # Kafka
    kafka_broker_prod: str = ""
    kafka_broker_beta: str = ""
    kafka_broker: str = "localhost:9092"
    kafka_topic_articles: str = "preventive-health-articles"
    kafka_topic_events: str = "preventive-health-events"
    producer_client_id: str = "preventive-health-pipeline-producer"

    # AWS S3
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_region: str = "ap-south-1"
    s3_bucket: str = "pulse-narrative"

class SourceConfigItem(BaseModel):
    """Individual source configuration."""
    source_id: str
    name: str
    url: str
    source_type: str
    quality_tier: str
    crawl_method: str
    rate_limit: int = 10
    enabled: bool = True

    # Method-specific config
    rss_url: Optional[str] = None
    css_selectors: Optional[Dict[str, str]] = None
    search_terms: Optional[List[str]] = None


class ConfigManager:
    """Configuration manager."""

    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.settings = Settings()
        self._sources: List[SourceConfigItem] = []
        self._pipeline: PipelineConfig = PipelineConfig()
        self._slack: SlackConfig = SlackConfig()
        self._llm: LLMConfig = LLMConfig()

    def load(self):
        """Load all configuration."""
        self._load_sources()
        self._load_pipeline()
        self._load_slack()
        self._load_llm()
        logger.info("Configuration loaded")

    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        """Load YAML file."""
        path = self.config_dir / filename
        if path.exists():
            with open(path) as f:
                return yaml.safe_load(f) or {}
        return {}

    def _load_sources(self):
        """Load sources configuration."""
        # Load from sources directory
        sources_dir = self.config_dir / "sources"

        if sources_dir.exists():
            for yaml_file in sources_dir.glob("*.yaml"):
                data = self._load_yaml(f"sources/{yaml_file.name}")
                sources = data.get("sources", [])
                for s in sources:
                    self._sources.append(SourceConfigItem(**s))

        # Also load from main sources.yaml if exists
        data = self._load_yaml("sources.yaml")
        for s in data.get("sources", []):
            self._sources.append(SourceConfigItem(**s))

        logger.info(f"Loaded {len(self._sources)} source configurations")

    def _load_pipeline(self):
        """Load pipeline configuration."""
        data = self._load_yaml("pipeline.yaml")
        if data:
            self._pipeline = PipelineConfig(**data)

    def _load_slack(self):
        """Load Slack configuration."""
        self._slack = SlackConfig(
            bot_token=self.settings.slack_bot_token,
            app_token=self.settings.slack_app_token,
            signing_secret=self.settings.slack_signing_secret,
        )

        # Override channels from config if present
        data = self._load_yaml("slack.yaml")
        if data.get("channels"):
            self._slack.channels.update(data["channels"])

    def _load_llm(self):
        """Load LLM configuration."""
        self._llm = LLMConfig(
            provider=self.settings.llm_provider,
            api_key=self.settings.llm_api_key,
            model=self.settings.llm_model,
        )

    @property
    def sources(self) -> List[SourceConfigItem]:
        """Get enabled sources."""
        return [s for s in self._sources if s.enabled]

    @property
    def all_sources(self) -> List[SourceConfigItem]:
        """Get all sources including disabled."""
        return self._sources

    @property
    def pipeline(self) -> PipelineConfig:
        return self._pipeline

    @property
    def slack(self) -> SlackConfig:
        return self._slack

    @property
    def llm(self) -> LLMConfig:
        return self._llm

    def get_source(self, source_id: str) -> Optional[SourceConfigItem]:
        """Get source by ID."""
        for s in self._sources:
            if s.source_id == source_id:
                return s
        return None


# Global config instance
config = ConfigManager()
