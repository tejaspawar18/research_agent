"""
Notification Service - Slack integration for article delivery.
"""
import logging
import os
import hmac
import hashlib
import json
import urllib.parse
from contextlib import asynccontextmanager
from typing import List, Dict, Optional
from datetime import date

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import httpx

import sys
sys.path.insert(0, '/app')

from shared.models import Article, ProjectArea, DailyDigest, SUB_TOPIC_TAGS
from shared.utils import RedisManager, truncate_text, ScyllaDBManager
from shared.config import config
from shared.utils.metrics import add_metrics_endpoint

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redis_manager = RedisManager()
scylla_manager = ScyllaDBManager()

# Slack signing secret for verifying interaction requests
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")


def get_week_year(dt: date = None) -> str:
    """Get week-year string like '2026-W06' for partition key."""
    if dt is None:
        dt = date.today()
    iso_cal = dt.isocalendar()
    return f"{iso_cal[0]}-W{iso_cal[1]:02d}"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await redis_manager.connect()
    scylla_manager.connect()
    config.load()
    logger.info("Notification service started")
    yield
    await redis_manager.disconnect()
    scylla_manager.disconnect()


app = FastAPI(
    title="Notification Service",
    description="Slack notifications for preventive health research",
    version="1.0.0",
    lifespan=lifespan,
)
add_metrics_endpoint(app)


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
                timeout=config.pipeline.slack_api_timeout,
            )
            return response.json()


class MessageFormatter:
    @staticmethod
    def _clean_title(title: str) -> str:
        """Sanitize article title for Slack display.

        Strips link patterns, HTML artifacts, and Slack mrkdwn special chars
        that would break the <URL|Title> link format.
        """
        import re
        if not title:
            return "Untitled"
        # Extract display text from <URL|Text> patterns
        title = re.sub(r'<[^>|]+\|([^>]+)>', r'\1', title)
        # Remove any remaining angle brackets and pipe characters
        title = re.sub(r'[<>|]', '', title)
        # Remove HTML tags
        title = re.sub(r'&[a-zA-Z]+;', ' ', title)
        # Clean up whitespace
        title = re.sub(r'\s+', ' ', title).strip()
        return title or "Untitled"

    @staticmethod
    def format_article(article: Article) -> tuple:
        """Format a single article message - full summary and key findings, no truncation."""
        evidence_badge = EVIDENCE_BADGES.get(article.evidence_level or 2, "🟡")

        # Clean title for safe Slack rendering
        title = MessageFormatter._clean_title(article.title)

        # Get sub-topic tag for display
        sub_topic_tag = ""
        if article.sub_topic:
            tag_name = SUB_TOPIC_TAGS.get(article.sub_topic, article.sub_topic)
            sub_topic_tag = f" `{tag_name}`"
        domain = article.url.split('/')[2] if article.url else None
        # Title with sub-topic tag
        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"📄 *<{article.url}|{title}>*"}},
            {"type": "context", "elements": [
                e for e in [
                    {"type": "mrkdwn", "text": sub_topic_tag} if sub_topic_tag else None,
                    {"type": "mrkdwn", "text": f" `{domain}`"} if domain else None,
                    {"type": "mrkdwn", "text": f"📅 {article.published_date}"} if article.published_date else None,
                    # {"type": "mrkdwn", "text": f"{evidence_badge} Confidence: {article.evidence_level or 'N/A'}/5"},
                    {"type": "mrkdwn", "text": "📋 Abstract only"} if not article.full_text else None,
                ] if e is not None
            ]},
        ]

        # Summary with Slack section text limit (3000 chars max)
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
            full_text = f"*Summary:*\n{summary_text}"
            # Slack section text blocks have a 3000-char limit
            if len(full_text) > config.pipeline.slack_section_text_limit:
                full_text = full_text[:config.pipeline.slack_section_text_limit - 3] + "..."
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": full_text}})

        # Add feedback buttons
        # Encode article metadata in button value: article_id|source_id|published_date
        # button_value = f"{article.article_id}|{article.source_id}|{article.published_date}"
        # blocks.append({"type": "divider"})
        # blocks.append({
        #     "type": "actions",
        #     "block_id": f"feedback_{article.article_id}",
        #     "elements": [
        #         {
        #             "type": "button",
        #             "text": {"type": "plain_text", "text": "👍 Useful", "emoji": True},
        #             "style": "primary",
        #             "action_id": "feedback_positive",
        #             "value": button_value,
        #         },
        #         {
        #             "type": "button",
        #             "text": {"type": "plain_text", "text": "👎 Not Useful", "emoji": True},
        #             "action_id": "feedback_negative",
        #             "value": button_value,
        #         },
        #         {
        #             "type": "button",
        #             "text": {"type": "plain_text", "text": "💬 Comment", "emoji": True},
        #             "action_id": "feedback_comment",
        #             "value": button_value,
        #         },
        #     ],
        # })

        return blocks, title
    
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

        for i, article in enumerate(sorted_articles[:config.pipeline.digest_max_articles], 1):
            badge = EVIDENCE_BADGES.get(article.evidence_level or 2, "🟡")

            # Get sub-topic tag
            tag_str = ""
            if article.sub_topic:
                tag_name = SUB_TOPIC_TAGS.get(article.sub_topic, article.sub_topic)
                tag_str = f" `{tag_name}`"

            clean_title = MessageFormatter._clean_title(article.title)
            date_str = f"📅 {article.published_date} | " if article.published_date else ""
            text = f"*{i}. <{article.url}|{clean_title}>*{tag_str}\n{date_str}{badge} Level {article.evidence_level or 'N/A'}"
            if article.summary:
                text += f"\n>{truncate_text(article.summary, config.pipeline.digest_summary_truncate)}"
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

        if len(articles) > config.pipeline.digest_max_articles:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"_...and {len(articles) - config.pipeline.digest_max_articles} more_"}]})

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


def verify_slack_signature(timestamp: str, body: bytes, signature: str) -> bool:
    """Verify Slack request signature using signing secret."""
    if not SLACK_SIGNING_SECRET:
        logger.warning("SLACK_SIGNING_SECRET not configured, skipping verification")
        return True

    sig_basestring = f"v0:{timestamp}:{body.decode('utf-8')}"
    computed_sig = "v0=" + hmac.new(
        SLACK_SIGNING_SECRET.encode(),
        sig_basestring.encode(),
        hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(computed_sig, signature)


@app.post("/slack/interactions")
async def slack_interactions(request: Request):
    """Handle Slack interactive component callbacks (button clicks, modal submissions)."""
    body = await request.body()

    # Verify Slack signature
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    signature = request.headers.get("X-Slack-Signature", "")

    if not verify_slack_signature(timestamp, body, signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

    # Parse the payload
    try:
        parsed = urllib.parse.parse_qs(body.decode("utf-8"))
        payload = json.loads(parsed.get("payload", ["{}"])[0])
    except Exception as e:
        logger.error(f"Failed to parse Slack payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid payload")

    payload_type = payload.get("type")

    # Handle button clicks
    if payload_type == "block_actions":
        actions = payload.get("actions", [])
        user = payload.get("user", {})
        channel = payload.get("channel", {})
        message = payload.get("message", {})

        for action in actions:
            action_id = action.get("action_id", "")
            value = action.get("value", "")

            if action_id.startswith("feedback_"):
                # Parse button value: article_id|source_id|published_date
                parts = value.split("|")
                if len(parts) != 3:
                    logger.error(f"Invalid button value format: {value}")
                    continue

                article_id, source_id, published_date_str = parts
                feedback_type = action_id.replace("feedback_", "")  # positive, negative, comment

                if feedback_type == "comment":
                    # Open modal for comment input
                    trigger_id = payload.get("trigger_id")
                    if trigger_id:
                        await open_comment_modal(trigger_id, article_id, source_id, published_date_str, message.get("ts", ""), channel.get("id", ""))
                    return JSONResponse(content={})

                # Store feedback in ScyllaDB
                week_year = get_week_year()
                try:
                    scylla_manager.insert_article_feedback(
                        week_year=week_year,
                        article_id=article_id,
                        message_ts=message.get("ts", ""),
                        channel=channel.get("id", ""),
                        user_id=user.get("id", ""),
                        user_name=user.get("username", user.get("name", "")),
                        feedback_type=feedback_type,
                    )
                    logger.info(f"Stored {feedback_type} feedback for article {article_id} from user {user.get('id')}")
                except Exception as e:
                    logger.error(f"Failed to store feedback: {e}")

                # Update the message to show feedback received
                return JSONResponse(content={
                    "response_action": "update",
                    "text": f"Thanks for your feedback! ({feedback_type})",
                })

    # Handle modal submissions
    elif payload_type == "view_submission":
        view = payload.get("view", {})
        callback_id = view.get("callback_id", "")

        if callback_id.startswith("comment_modal_"):
            # Parse metadata from callback_id: comment_modal_articleId_sourceId_publishedDate_messageTs_channel
            metadata = view.get("private_metadata", "")
            parts = metadata.split("|")

            if len(parts) != 5:
                logger.error(f"Invalid comment modal metadata: {metadata}")
                return JSONResponse(content={"response_action": "clear"})

            article_id, source_id, published_date_str, message_ts, channel_id = parts

            # Get comment text from modal
            values = view.get("state", {}).get("values", {})
            comment_text = ""
            for block_id, block_values in values.items():
                for action_id, action_data in block_values.items():
                    if action_id == "comment_input":
                        comment_text = action_data.get("value", "")

            user = payload.get("user", {})
            week_year = get_week_year()

            try:
                scylla_manager.insert_article_feedback(
                    week_year=week_year,
                    article_id=article_id,
                    message_ts=message_ts,
                    channel=channel_id,
                    user_id=user.get("id", ""),
                    user_name=user.get("username", user.get("name", "")),
                    feedback_type="comment",
                    comment=comment_text,
                )
                logger.info(f"Stored comment for article {article_id} from user {user.get('id')}")
            except Exception as e:
                logger.error(f"Failed to store comment: {e}")

            return JSONResponse(content={"response_action": "clear"})

    return JSONResponse(content={})


async def open_comment_modal(trigger_id: str, article_id: str, source_id: str, published_date: str, message_ts: str, channel: str):
    """Open a Slack modal for entering a comment."""
    bot_token = os.getenv("SLACK_BOT_TOKEN")
    if not bot_token:
        logger.error("SLACK_BOT_TOKEN not configured for opening modal")
        return

    # Encode metadata in private_metadata
    private_metadata = f"{article_id}|{source_id}|{published_date}|{message_ts}|{channel}"

    modal = {
        "type": "modal",
        "callback_id": f"comment_modal_{article_id}",
        "title": {"type": "plain_text", "text": "Add Comment"},
        "submit": {"type": "plain_text", "text": "Submit"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "private_metadata": private_metadata,
        "blocks": [
            {
                "type": "input",
                "block_id": "comment_block",
                "element": {
                    "type": "plain_text_input",
                    "action_id": "comment_input",
                    "multiline": True,
                    "placeholder": {"type": "plain_text", "text": "Enter your comment about this article..."},
                },
                "label": {"type": "plain_text", "text": "Your Comment"},
            }
        ],
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://slack.com/api/views.open",
            headers={"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json"},
            json={"trigger_id": trigger_id, "view": modal},
            timeout=config.pipeline.slack_modal_timeout,
        )
        result = response.json()
        if not result.get("ok"):
            logger.error(f"Failed to open comment modal: {result.get('error')}")