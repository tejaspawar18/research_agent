import os
import requests

WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")

# def notify(article):
#     payload = {
#         "text": (
#             "🧪 *New Research Article*\n"
#             f"*Title:* {article.title}\n"
#             f"*Journal:* {article.journal}\n"
#             f"*Published:* {article.published}\n"
#             f"*DOI:* {article.doi}\n"
#             f"<{article.url}|Read Article>"
#         )
#     }
#     requests.post(WEBHOOK, json=payload, timeout=10)

from core.logger import get_logger

logger = get_logger("slack_notifier")

WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")

def send_article_notification(article_data: dict, summary_text: str):
    """
    Send a Slack notification for a processed article.
    article_data expects: title, url, journal, published_date, doi, source
    """
    if not WEBHOOK:
        logger.warning("SLACK_WEBHOOK_URL not set. Skipping notification.")
        return

    title = article_data.get("title") or "Unknown Title"
    journal = article_data.get("journal") or "Unknown Journal"
    published = article_data.get("published_date") or "Unknown Date"
    url = article_data.get("url") or "#"
    doi = article_data.get("doi")
    
    # Truncate summary if too long for Slack block
    if len(summary_text) > 1000:
        summary_text = summary_text[:1000] + "..."

    doi_line = f"DOI: {doi}\n" if doi else ""

    payload = {
        "text": (
            "🧪 *New Research Article*\n\n"
            f"*{title}*\n"
            f"_{journal} | {published}_\n"
            f"{doi_line}\n"
            f"{summary_text}\n\n"
            f"🔗 <{url}|Read full article>"
        )
    }
    
    try:
        r = requests.post(WEBHOOK, json=payload, timeout=10)
        if r.status_code != 200:
            logger.error(f"Slack notification failed: {r.status_code} {r.text}")
        else:
            logger.info(f"Slack notification sent for {title}")
    except Exception as e:
        logger.error(f"Slack notification error: {e}")
