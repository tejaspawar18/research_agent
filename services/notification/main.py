"""
Notification Service - Slack integration for article delivery.
"""
import logging
import os
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
from datetime import datetime, date

from fastapi import FastAPI
from pydantic import BaseModel
import httpx

import sys
sys.path.insert(0, '/app')

from shared.models import Article, ProjectArea, DailyDigest
from shared.utils import RedisManager, truncate_text
from shared.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redis_manager = RedisManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await redis_manager.connect()
    config.load()
    logger.info("Notification service started")
    yield
    await redis_manager.disconnect()


app = FastAPI(
    title="Notification Service",
    description="Slack notifications for preventive health research",
    version="1.0.0",
    lifespan=lifespan,
)


class NotifyRequest(BaseModel):
    article: Article
    channel: str


class DigestRequest(BaseModel):
    articles: List[Article]
    project_area: str
    channel: str


class NotifyResponse(BaseModel):
    success: bool
    message_ts: Optional[str] = None
    error: Optional[str] = None


EVIDENCE_BADGES = {
    5: "🟢", 4: "🟢", 3: "🟡", 2: "🟠", 1: "🔴",
}

QUALITY_BADGES = {
    "high": "⭐", "medium": "📊", "low": "📰",
}


class SlackClient:
    def __init__(self):
        self.bot_token = os.getenv("SLACK_BOT_TOKEN")
    
    async def post_message(self, channel: str, blocks: List[Dict], text: str) -> Dict:
        if not self.bot_token:
            raise ValueError("SLACK_BOT_TOKEN not configured")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://slack.com/api/chat.postMessage",
                headers={"Authorization": f"Bearer {self.bot_token}", "Content-Type": "application/json"},
                json={"channel": channel, "blocks": blocks, "text": text},
                timeout=30.0,
            )
            return response.json()


class MessageFormatter:
    @staticmethod
    def format_article(article: Article) -> tuple:
        evidence_badge = EVIDENCE_BADGES.get(article.evidence_level or 2, "🟡")
        quality_badge = QUALITY_BADGES.get(article.source_quality or "medium", "📊")
        
        authors = article.authors[:2]
        author_str = ", ".join(a.name for a in authors) if authors else "Unknown"
        if len(article.authors) > 2:
            author_str += " et al."
        
        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"{quality_badge} *<{article.url}|{article.title}>*"}},
            {"type": "context", "elements": [
                {"type": "mrkdwn", "text": f"👥 {author_str}"},
                {"type": "mrkdwn", "text": f"📅 {article.published_date}"},
                {"type": "mrkdwn", "text": f"{evidence_badge} Evidence: {article.evidence_level or 'N/A'}/5"},
            ]},
        ]
        
        if article.summary:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Summary:* {truncate_text(article.summary, 400)}"}})
        
        if article.key_findings:
            findings = "\n".join(f"• {f}" for f in article.key_findings[:3])
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Key Findings:*\n{findings}"}})
        
        blocks.append({"type": "divider"})
        return blocks, article.title
    
    @staticmethod
    def format_digest(articles: List[Article], project_area: str, today: date) -> tuple:
        area_names = {
            "disease_prevention": "🏥 Disease Prevention",
            "behavioral_protocols": "🏃 Behavioral Protocols",
            "nutritional_protocols": "🥗 Nutritional Protocols",
            "government_interventions": "🏛️ Government Interventions",
            "youth_health": "🎓 Youth Health",
            "general": "📚 General Research",
        }
        
        area_name = area_names.get(project_area, project_area)
        high_evidence = sum(1 for a in articles if (a.evidence_level or 0) >= 4)
        
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"📰 Daily Digest: {area_name}", "emoji": True}},
            {"type": "context", "elements": [
                {"type": "mrkdwn", "text": f"📅 {today.strftime('%B %d, %Y')}"},
                {"type": "mrkdwn", "text": f"📄 {len(articles)} articles"},
                {"type": "mrkdwn", "text": f"🟢 {high_evidence} high-evidence"},
            ]},
            {"type": "divider"},
        ]
        
        sorted_articles = sorted(articles, key=lambda a: (a.evidence_level or 0), reverse=True)
        
        for i, article in enumerate(sorted_articles[:10], 1):
            badge = EVIDENCE_BADGES.get(article.evidence_level or 2, "🟡")
            author = article.authors[0].name if article.authors else "Unknown"
            text = f"*{i}. <{article.url}|{article.title}>*\n_{author}_ | {badge} Level {article.evidence_level or 'N/A'}"
            if article.summary:
                text += f"\n>{truncate_text(article.summary, 150)}"
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        
        if len(articles) > 10:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"_...and {len(articles) - 10} more_"}]})
        
        return blocks, f"Daily digest: {len(articles)} articles"


slack_client = SlackClient()
formatter = MessageFormatter()


@app.post("/notify/article", response_model=NotifyResponse)
async def notify_article(request: NotifyRequest):
    try:
        blocks, text = formatter.format_article(request.article)
        result = await slack_client.post_message(request.channel, blocks, text)
        return NotifyResponse(success=result.get("ok", False), message_ts=result.get("ts"), error=result.get("error"))
    except Exception as e:
        return NotifyResponse(success=False, error=str(e))


@app.post("/notify/digest", response_model=NotifyResponse)
async def notify_digest(request: DigestRequest):
    try:
        blocks, text = formatter.format_digest(request.articles, request.project_area, date.today())
        result = await slack_client.post_message(request.channel, blocks, text)
        return NotifyResponse(success=result.get("ok", False), message_ts=result.get("ts"), error=result.get("error"))
    except Exception as e:
        return NotifyResponse(success=False, error=str(e))


@app.get("/notify/channels")
async def list_channels():
    return {"channels": config.slack.channels}


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "notification", "slack_configured": bool(os.getenv("SLACK_BOT_TOKEN"))}