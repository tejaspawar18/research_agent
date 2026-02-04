"""
Configuration management for the Preventive Health Pipeline.
"""
import os
import yaml
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class SlackConfig(BaseModel):
    """Slack configuration."""
    bot_token: Optional[str] = None
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
    provider: str = "openai"
    api_key: Optional[str] = None
    model: str = "gpt-4-turbo"
    max_tokens: int = 2000
    temperature: float = 0.3


class PipelineConfig(BaseModel):
    """Pipeline configuration."""
    schedule: str = "*/30 3-13 * * *"  # Every 30 minutes, 9 AM - 7 PM IST (UTC+5:30)
    max_articles_per_run: int = 500
    max_articles_per_source: int = 50
    relevance_threshold: float = 60.0
    parallel_crawlers: int = 5
    dedup_similarity_threshold: float = 0.85
    
    # Quality filters
    min_sample_size_observational: int = 150
    min_sample_size_rct: int = 50


class Settings(BaseSettings):
    """Application settings from environment."""
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
    llm_model: str = "gpt-4-turbo"
    
    # Slack
    slack_bot_token: Optional[str] = None
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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


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