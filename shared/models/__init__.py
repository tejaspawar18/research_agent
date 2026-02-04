"""Shared models package."""
from .article import (
    # Enums
    SourceQuality,
    StudyType,
    ProjectArea,
    SourceType,
    CrawlMethod,
    ArticleStatus,
    
    # Models
    Author,
    SourceConfig,
    Article,
    CrawlJob,
    PipelineRun,
    DeduplicationResult,
    QualityFilterResult,
    LLMClassificationResult,
    LLMSummaryResult,
    SlackMessage,
    DailyDigest,
    
    # Constants
    PROJECT_KEYWORDS,
    SUB_TOPIC_TAGS,
    HIGH_QUALITY_SOURCES,
    BAD_SCIENCE_INDICATORS,
)

__all__ = [
    # Enums
    "SourceQuality",
    "StudyType",
    "ProjectArea",
    "SourceType",
    "CrawlMethod",
    "ArticleStatus",

    # Models
    "Author",
    "SourceConfig",
    "Article",
    "CrawlJob",
    "PipelineRun",
    "DeduplicationResult",
    "QualityFilterResult",
    "LLMClassificationResult",
    "LLMSummaryResult",
    "SlackMessage",
    "DailyDigest",

    # Constants
    "PROJECT_KEYWORDS",
    "SUB_TOPIC_TAGS",
    "HIGH_QUALITY_SOURCES",
    "BAD_SCIENCE_INDICATORS",
]