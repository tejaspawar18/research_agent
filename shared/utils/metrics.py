"""
Prometheus metrics for the Preventive Health Research Pipeline.

Centralizes all metric definitions so every service uses consistent names and labels.
Each service calls add_metrics_endpoint(app) during startup to expose GET /metrics.
"""
import logging
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import FastAPI
from fastapi.responses import Response


class MetricsEndpointFilter(logging.Filter):
    """Filter to suppress /metrics endpoint logs from uvicorn access log."""

    def filter(self, record: logging.LogRecord) -> bool:
        # Suppress GET /metrics logs
        message = record.getMessage()
        if "GET /metrics" in message or "GET /health" in message:
            return False
        return True


def suppress_metrics_logs():
    """Apply filter to uvicorn access logger to suppress /metrics and /health logs."""
    uvicorn_access = logging.getLogger("uvicorn.access")
    uvicorn_access.addFilter(MetricsEndpointFilter())


# ── Articles per domain / source ──────────────────────────────────────────────
ARTICLES_CRAWLED = Counter(
    "articles_crawled_total",
    "Total articles found by the crawler, broken down by source",
    ["source_id", "source_name", "quality_tier"],
)

# ── Crawl method breakdown ────────────────────────────────────────────────────
CRAWL_METHOD_USED = Counter(
    "articles_crawled_by_method_total",
    "Articles retrieved by each crawling method",
    ["source_id", "crawl_method"],
)

# ── Content type (full text vs abstract only) ─────────────────────────────────
CONTENT_TYPE_COUNTER = Counter(
    "articles_content_type_total",
    "Articles by content type obtained during extraction",
    ["content_type"],
)

# ── Extraction method success ─────────────────────────────────────────────────
EXTRACTION_METHOD = Counter(
    "extraction_method_total",
    "Extraction attempts by method used",
    ["method"],
)

# ── Quality filter results ────────────────────────────────────────────────────
QUALITY_FILTER = Counter(
    "articles_quality_filter_total",
    "Articles that passed or failed the quality filter",
    ["result"],
)

# ── Relevance filter results ──────────────────────────────────────────────────
RELEVANCE_FILTER = Counter(
    "articles_relevance_filter_total",
    "Articles by relevance classification outcome",
    ["result"],
)

# ── Articles per project area ─────────────────────────────────────────────────
ARTICLES_BY_PROJECT = Counter(
    "articles_by_project_area_total",
    "Processed articles classified into each project area",
    ["project_area", "sub_topic"],
)

# ── LLM token usage ──────────────────────────────────────────────────────────
LLM_TOKENS = Counter(
    "llm_tokens_total",
    "LLM tokens consumed, broken down by provider, model, call type, and direction",
    ["provider", "model", "call_type", "token_type"],
)

# ── Pipeline run tracking ─────────────────────────────────────────────────────
PIPELINE_RUNS = Counter(
    "pipeline_runs_total",
    "Total pipeline runs by final status",
    ["status"],
)

PIPELINE_STAGE_ARTICLES = Gauge(
    "pipeline_stage_articles",
    "Number of articles at each pipeline stage for the current/latest run",
    ["stage"],
)

PIPELINE_DURATION = Histogram(
    "pipeline_duration_seconds",
    "Pipeline run duration in seconds",
    ["status"],
    buckets=[60, 120, 300, 600, 900, 1200, 1800, 3600],
)


def add_metrics_endpoint(app: FastAPI) -> None:
    """Register a GET /metrics endpoint on the given FastAPI app."""
    # Suppress /metrics and /health logs from uvicorn access log
    suppress_metrics_logs()

    @app.get("/metrics", include_in_schema=False)
    async def metrics():
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST,
        )
