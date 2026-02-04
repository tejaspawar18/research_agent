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

from shared.models import Article, ProjectArea, DailyDigest, SUB_TOPIC_TAGS
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
    "high": "📄", "medium": "📊", "low": "📰",
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
                json={"channel": channel, "blocks": blocks, "text": text, "unfurl_links": False, "unfurl_media": False},
                timeout=30.0,
            )
            return response.json()


class MessageFormatter:
    @staticmethod
    def format_article(article: Article) -> tuple:
        """Format a single article message - full summary and key findings, no truncation."""
        evidence_badge = EVIDENCE_BADGES.get(article.evidence_level or 2, "🟡")

        # Get sub-topic tag for display
        sub_topic_tag = ""
        if article.sub_topic:
            tag_name = SUB_TOPIC_TAGS.get(article.sub_topic, article.sub_topic)
            sub_topic_tag = f" `{tag_name}`"

        # Title with sub-topic tag
        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"📄 *<{article.url}|{article.title}>*"}},
            {"type": "context", "elements": [
                e for e in [
                    {"type": "mrkdwn", "text": sub_topic_tag} if sub_topic_tag else None,
                    {"type": "mrkdwn", "text": f"📅 {article.published_date}"} if article.published_date else None,
                    {"type": "mrkdwn", "text": f"{evidence_badge} Confidence: {article.evidence_level or 'N/A'}/5"},
                    {"type": "mrkdwn", "text": "📋 Abstract only"} if not article.full_text else None,
                ] if e is not None
            ]},
        ]

        # Full summary (no truncation) - ensure bullet points are on new lines
        if article.summary:
            summary_text = article.summary
            # Replace escaped newlines with actual newlines
            summary_text = summary_text.replace('\\n', '\n')
            # Ensure bullet points start on new lines
            summary_text = summary_text.replace(' - ', '\n- ')
            if not summary_text.startswith('-'):
                summary_text = summary_text.replace('\n-', '\n• ').replace('- ', '• ', 1) if summary_text.startswith('- ') else summary_text
            else:
                summary_text = summary_text.replace('\n-', '\n• ').replace('-', '•', 1)
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Summary:*\n{summary_text}"}})

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

        # Group articles by sub-topic for summary
        sub_topic_counts = {}
        for a in articles:
            if a.sub_topic:
                tag = SUB_TOPIC_TAGS.get(a.sub_topic, a.sub_topic)
                sub_topic_counts[tag] = sub_topic_counts.get(tag, 0) + 1

        # Create tags summary string
        tags_summary = " ".join([f"`{tag}` ({count})" for tag, count in sorted(sub_topic_counts.items(), key=lambda x: -x[1])[:5]])

        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"📰 Daily Digest: {area_name}", "emoji": True}},
            {"type": "context", "elements": [
                {"type": "mrkdwn", "text": f"📅 {today.strftime('%B %d, %Y')}"},
                {"type": "mrkdwn", "text": f"📄 {len(articles)} articles"},
                {"type": "mrkdwn", "text": f"🟢 {high_evidence} high-evidence"},
            ]},
        ]

        # Add tags summary if available
        if tags_summary:
            blocks.append({"type": "context", "elements": [
                {"type": "mrkdwn", "text": f"🏷️ *Topics:* {tags_summary}"},
            ]})

        blocks.append({"type": "divider"})

        sorted_articles = sorted(articles, key=lambda a: (a.evidence_level or 0), reverse=True)

        for i, article in enumerate(sorted_articles[:10], 1):
            badge = EVIDENCE_BADGES.get(article.evidence_level or 2, "🟡")

            # Get sub-topic tag
            tag_str = ""
            if article.sub_topic:
                tag_name = SUB_TOPIC_TAGS.get(article.sub_topic, article.sub_topic)
                tag_str = f" `{tag_name}`"

            date_str = f"📅 {article.published_date} | " if article.published_date else ""
            text = f"*{i}. <{article.url}|{article.title}>*{tag_str}\n{date_str}{badge} Level {article.evidence_level or 'N/A'}"
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