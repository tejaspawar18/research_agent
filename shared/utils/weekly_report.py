"""Helpers for weekly feedback report aggregation and PDF rendering."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
import re
from typing import Any, Dict, List, Optional
from xml.sax.saxutils import escape


PROJECT_AREA_LABELS = {
    "disease_prevention": "Disease Prevention",
    "behavioral_protocols": "Behavioral Protocols",
    "nutritional_protocols": "Nutritional Protocols",
    "government_interventions": "Government Interventions",
    "youth_health": "Youth Health",
    "general": "General Research",
}


@dataclass
class WeeklyReportArticle:
    """Aggregated article row for the weekly feedback report."""

    article_id: str
    title: str
    url: str
    summary: str
    channel: str
    project_area: str = "general"
    positive_feedback_count: int = 0
    published_date: Optional[date] = None
    message_ts: Optional[str] = None
    sent_at: Optional[datetime] = None


@dataclass
class WeeklyReportSection:
    """A Slack-channel section in the weekly PDF report."""

    channel: str
    project_area: str = "general"
    articles: List[WeeklyReportArticle] = field(default_factory=list)


@dataclass
class WeeklyReportUser:
    """Aggregated feedback contributor row for the weekly report."""

    user_id: str
    user_name: str
    feedback_count: int = 0


def project_area_label(project_area: Optional[str]) -> str:
    """Return a human-friendly project area label."""
    normalized = (project_area or "general").strip().lower()
    return PROJECT_AREA_LABELS.get(normalized, normalized.replace("_", " ").title())


def _normalize_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "date"):
        try:
            converted = value.date()
            if isinstance(converted, date):
                return converted
        except Exception:
            pass
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _normalize_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _truncate_text(value: str, limit: int = 420) -> str:
    normalized = " ".join((value or "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def _humanize_token(value: str) -> str:
    cleaned = re.sub(r"[._\-]+", " ", (value or "").strip())
    cleaned = " ".join(cleaned.split())
    if not cleaned:
        return ""
    return cleaned.title()


def _format_section_heading(channel: str, project_area: str) -> str:
    label = project_area_label(project_area)
    if label and label != PROJECT_AREA_LABELS["general"]:
        return label

    normalized_channel = (channel or "").strip()
    if normalized_channel.startswith("#"):
        normalized_channel = normalized_channel[1:]
    humanized_channel = _humanize_token(normalized_channel)
    return humanized_channel or label or "General Research"


def _format_user_display_name(user: WeeklyReportUser) -> str:
    name = (user.user_name or "").strip()
    user_id = (user.user_id or "").strip()

    if name and name.lower() != "unknown user" and name != user_id:
        if "@" in name:
            return name
        pretty_name = _humanize_token(name)
        return pretty_name or name

    if user_id:
        if user_id.startswith("U") and user_id.upper() == user_id:
            return user_id
        if "@" in user_id:
            return user_id
        pretty_id = _humanize_token(user_id)
        return pretty_id or user_id

    return "Unknown User"


def week_year_bounds(week_year: str) -> tuple[date, date]:
    """Return the start/end dates for an ISO week key like ``2026-W13``."""
    year_part, week_part = week_year.split("-W", 1)
    year = int(year_part)
    week = int(week_part)
    start = date.fromisocalendar(year, week, 1)
    end = start + timedelta(days=6)
    return start, end


def build_weekly_report_sections(
    positive_feedback: List[Dict[str, Any]],
    slack_messages: List[Dict[str, Any]],
    top_n: int = 5,
) -> List[WeeklyReportSection]:
    """Aggregate positive feedback into Slack-channel report sections."""
    message_map: Dict[str, Dict[str, Any]] = {}
    for message in slack_messages:
        message_ts = str(message.get("message_ts") or "").strip()
        if message_ts:
            message_map[message_ts] = message

    aggregated: Dict[tuple[str, str], WeeklyReportArticle] = {}

    for feedback in positive_feedback:
        if (feedback.get("feedback_type") or "").strip().lower() != "positive":
            continue

        message_ts = str(feedback.get("message_ts") or "").strip()
        if not message_ts:
            continue

        message = message_map.get(message_ts)
        if not message:
            continue

        article_id = str(message.get("article_id") or feedback.get("article_id") or "").strip()
        channel = str(message.get("channel") or feedback.get("channel") or "").strip()
        if not article_id or not channel:
            continue

        key = (channel, article_id)
        if key not in aggregated:
            aggregated[key] = WeeklyReportArticle(
                article_id=article_id,
                title=str(message.get("title") or "Untitled").strip() or "Untitled",
                url=str(message.get("url") or "").strip(),
                summary=str(message.get("summary") or "").strip(),
                channel=channel,
                project_area=str(message.get("project_area") or "general").strip() or "general",
                published_date=_normalize_date(message.get("published_date")),
                message_ts=message_ts,
                sent_at=_normalize_datetime(message.get("sent_at")),
            )

        aggregated[key].positive_feedback_count += 1

    sections_by_channel: Dict[str, List[WeeklyReportArticle]] = defaultdict(list)
    for article in aggregated.values():
        sections_by_channel[article.channel].append(article)

    sections: List[WeeklyReportSection] = []
    for channel in sorted(sections_by_channel.keys(), key=lambda value: value.lower()):
        channel_articles = sections_by_channel[channel]
        channel_articles.sort(
            key=lambda item: (
                -item.positive_feedback_count,
                item.published_date is None,
                -(item.published_date.toordinal() if item.published_date else date.min.toordinal()),
                item.title.lower(),
            )
        )
        section_area = channel_articles[0].project_area if channel_articles else "general"
        sections.append(
            WeeklyReportSection(
                channel=channel,
                project_area=section_area,
                articles=channel_articles[:top_n],
            )
        )

    return sections


def build_top_feedback_users(
    feedback_rows: List[Dict[str, Any]],
    top_n: int = 10,
) -> List[WeeklyReportUser]:
    """Aggregate weekly feedback rows into top contributors."""
    if top_n <= 0:
        return []

    aggregated: Dict[str, WeeklyReportUser] = {}

    for feedback in feedback_rows:
        user_id = str(feedback.get("user_id") or "").strip()
        user_name = str(feedback.get("user_name") or "").strip()
        if not user_id and not user_name:
            continue

        key = user_id or user_name.lower()
        if key not in aggregated:
            aggregated[key] = WeeklyReportUser(
                user_id=user_id,
                user_name=user_name or user_id or "Unknown User",
                feedback_count=0,
            )
        elif not aggregated[key].user_name and user_name:
            aggregated[key].user_name = user_name

        aggregated[key].feedback_count += 1

    users = list(aggregated.values())
    users.sort(
        key=lambda item: (
            -item.feedback_count,
            (item.user_name or item.user_id).lower(),
            item.user_id.lower(),
        )
    )
    return users[:top_n]


def render_weekly_feedback_pdf(
    output_path: str,
    week_year: str,
    sections: List[WeeklyReportSection],
    top_feedback_users: Optional[List[WeeklyReportUser]] = None,
    generated_at: Optional[datetime] = None,
):
    """Render the weekly report PDF to ``output_path``."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_LEFT
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.platypus import HRFlowable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except ImportError as exc:
        raise RuntimeError(
            "reportlab is required to generate the weekly feedback PDF. "
            "Install it with the orchestrator dependencies."
        ) from exc

    top_feedback_users = top_feedback_users or []
    start_date, end_date = week_year_bounds(week_year)
    report_path = Path(output_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    heading_style = styles["Heading2"]
    body_style = styles["BodyText"]
    meta_style = ParagraphStyle(
        "WeeklyReportMeta",
        parent=styles["BodyText"],
        fontSize=9,
        leading=12,
        textColor=colors.HexColor("#4b5563"),
        alignment=TA_LEFT,
        spaceAfter=4,
    )
    article_title_style = ParagraphStyle(
        "WeeklyReportArticleTitle",
        parent=styles["Heading3"],
        fontSize=12,
        leading=15,
        textColor=colors.HexColor("#111827"),
        spaceAfter=4,
    )
    section_heading_style = ParagraphStyle(
        "WeeklyReportSectionHeading",
        parent=heading_style,
        fontSize=15,
        leading=18,
        textColor=colors.HexColor("#0f172a"),
        spaceAfter=6,
    )
    body_style.spaceAfter = 8
    body_style.leading = 13

    story = [
        Paragraph("Weekly Top Articles Report", title_style),
        Spacer(1, 8),
        Paragraph(
            f"Week {escape(week_year)} ({start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')})",
            heading_style,
        ),
        Paragraph(
            f"Slack channel sections: {len(sections)} | Articles included: {sum(len(section.articles) for section in sections)}",
            meta_style,
        ),
        Spacer(1, 10),
    ]

    if not sections:
        story.append(Paragraph("No positively rated articles were found for this report window.", body_style))
    else:
        for section_index, section in enumerate(sections, start=1):
            if section_index > 1:
                story.extend([Spacer(1, 8), HRFlowable(width="100%", color=colors.HexColor("#cbd5e1")), Spacer(1, 10)])

            section_heading = _format_section_heading(section.channel, section.project_area)
            story.append(Paragraph(escape(section_heading), section_heading_style))
            story.append(Spacer(1, 4))

            for article_index, article in enumerate(section.articles, start=1):
                title = escape(article.title or "Untitled")
                if article.url:
                    article_url = escape(article.url, {'"': "&quot;"})
                    story.append(
                        Paragraph(
                            f'{article_index}. <link href="{article_url}" color="#1d4ed8">{title}</link>',
                            article_title_style,
                        )
                    )
                else:
                    story.append(Paragraph(f"{article_index}. {title}", article_title_style))

                meta_parts = [f"Positive feedback: {article.positive_feedback_count}"]
                if article.published_date:
                    meta_parts.append(f"Published: {article.published_date.isoformat()}")
                story.append(Paragraph(" | ".join(meta_parts), meta_style))

                if article.summary:
                    summary_text = escape(article.summary).replace("\n", "<br/>")
                    story.append(Paragraph(summary_text, body_style))

                story.append(Spacer(1, 8))

    if top_feedback_users:
        if sections:
            story.extend([Spacer(1, 8), HRFlowable(width="100%", color=colors.HexColor("#cbd5e1")), Spacer(1, 10)])

        story.append(Paragraph("Top Contributors to the Report", section_heading_style))
        table_rows = [["Rank", "User", "Feedback Count"]]
        for rank, user in enumerate(top_feedback_users, start=1):
            table_rows.append([str(rank), _format_user_display_name(user), str(user.feedback_count)])

        users_table = Table(table_rows, colWidths=[46, 336, 92], hAlign="LEFT")
        users_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 10),
                    ("ALIGN", (0, 0), (0, -1), "CENTER"),
                    ("ALIGN", (2, 1), (2, -1), "RIGHT"),
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 9),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ]
            )
        )
        story.append(users_table)
        story.append(Spacer(1, 12))

    def add_page_number(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 9)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawRightString(A4[0] - doc.rightMargin, 18, f"Page {canvas.getPageNumber()}")
        canvas.restoreState()

    document = SimpleDocTemplate(
        str(report_path),
        pagesize=A4,
        leftMargin=40,
        rightMargin=40,
        topMargin=40,
        bottomMargin=30,
        title="Weekly Top Articles Report",
        author="Preventive Health Pipeline",
    )
    document.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
