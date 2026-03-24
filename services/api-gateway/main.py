"""
API Gateway - Unified entry point for the pipeline.
"""
import logging
import sys
from contextlib import asynccontextmanager
from typing import Optional, Dict

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx

sys.path.insert(0, '/app')
from shared.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ORCHESTRATOR_URL = config.settings.orchestrator_url
CRAWLER_URL = config.settings.crawler_url
DEDUP_URL = config.settings.dedup_url
LLM_URL = config.settings.llm_url
NOTIFICATION_URL = config.settings.notification_url


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API Gateway started")
    yield
    logger.info("API Gateway stopped")


app = FastAPI(
    title="Preventive Health Research Pipeline API",
    description="Unified API for the research article pipeline",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def proxy(method: str, url: str, json_data: dict = None, params: dict = None):
    try:
        async with httpx.AsyncClient(timeout=config.pipeline.gateway_proxy_timeout) as client:
            response = await client.request(method, url, json=json_data, params=params)
            return response.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="Service unavailable")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Pipeline endpoints
@app.post("/api/pipeline/run")
async def run_pipeline(request: dict = None):
    return await proxy("POST", f"{ORCHESTRATOR_URL}/pipeline/run", request or {})


@app.get("/api/pipeline/status")
async def pipeline_status():
    return await proxy("GET", f"{ORCHESTRATOR_URL}/pipeline/status")


@app.get("/api/pipeline/status/{run_id}")
async def run_status(run_id: str):
    return await proxy("GET", f"{ORCHESTRATOR_URL}/pipeline/status/{run_id}")


# Crawler endpoints
@app.get("/api/sources")
async def list_sources():
    return await proxy("GET", f"{CRAWLER_URL}/sources")


@app.post("/api/sources/{source_id}/crawl")
async def crawl_source(source_id: str, max_articles: int = 50):
    return await proxy("POST", f"{CRAWLER_URL}/sources/{source_id}/crawl", params={"max_articles": max_articles})


# Health check
@app.get("/health")
async def health():
    services = {}
    for name, url in [
        ("orchestrator", ORCHESTRATOR_URL),
        ("crawler", CRAWLER_URL),
        ("dedup", DEDUP_URL),
        ("llm", LLM_URL),
        ("notification", NOTIFICATION_URL),
    ]:
        try:
            async with httpx.AsyncClient(timeout=config.pipeline.health_check_timeout) as client:
                response = await client.get(f"{url}/health")
                services[name] = response.status_code == 200
        except:
            services[name] = False
    
    return {
        "status": "healthy" if all(services.values()) else "degraded",
        "service": "api-gateway",
        "services": services,
    }


@app.get("/")
async def root():
    return {
        "name": "Preventive Health Research Pipeline",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }