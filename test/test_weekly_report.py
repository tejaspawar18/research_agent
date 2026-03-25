from datetime import date, datetime

import pytest

from shared.utils.weekly_report import (
    build_top_feedback_users,
    build_weekly_report_sections,
    render_weekly_feedback_pdf,
    week_year_bounds,
)


def _sample_weekly_report_sections():
    slack_messages = [
        {
            "message_ts": "1740000000.000100",
            "article_id": "article-1",
            "channel": "#disease-prevention",
            "project_area": "disease_prevention",
            "title": "Vitamin D and cardiometabolic outcomes",
            "url": "https://example.com/article-1",
            "summary": "Summary 1",
            "published_date": date(2026, 3, 20),
            "sent_at": datetime(2026, 3, 21, 8, 0, 0),
        },
        {
            "message_ts": "1740000001.000100",
            "article_id": "article-2",
            "channel": "#disease-prevention",
            "project_area": "disease_prevention",
            "title": "Sleep regularity and inflammatory markers",
            "url": "https://example.com/article-2",
            "summary": "Summary 2",
            "published_date": date(2026, 3, 19),
            "sent_at": datetime(2026, 3, 21, 9, 0, 0),
        },
        {
            "message_ts": "1740000002.000100",
            "article_id": "article-3",
            "channel": "#behavioral-protocols",
            "project_area": "behavioral_protocols",
            "title": "Interval walking for older adults",
            "url": "https://example.com/article-3",
            "summary": "Summary 3",
            "published_date": date(2026, 3, 18),
            "sent_at": datetime(2026, 3, 21, 10, 0, 0),
        },
    ]
    positive_feedback = [
        {"feedback_type": "positive", "message_ts": "1740000000.000100", "article_id": "article-1", "channel": "#disease-prevention"},
        {"feedback_type": "positive", "message_ts": "1740000000.000100", "article_id": "article-1", "channel": "#disease-prevention"},
        {"feedback_type": "positive", "message_ts": "1740000000.000100", "article_id": "article-1", "channel": "#disease-prevention"},
        {"feedback_type": "positive", "message_ts": "1740000001.000100", "article_id": "article-2", "channel": "#disease-prevention"},
        {"feedback_type": "positive", "message_ts": "1740000002.000100", "article_id": "article-3", "channel": "#behavioral-protocols"},
        {"feedback_type": "comment", "message_ts": "1740000002.000100", "article_id": "article-3", "channel": "#behavioral-protocols"},
    ]
    return build_weekly_report_sections(positive_feedback, slack_messages, top_n=5)


def test_build_weekly_report_sections_groups_by_channel_and_aggregates_positive_votes():
    sections = _sample_weekly_report_sections()

    by_channel = {section.channel: section for section in sections}

    assert set(by_channel.keys()) == {"#behavioral-protocols", "#disease-prevention"}
    assert [article.article_id for article in by_channel["#disease-prevention"].articles] == ["article-1", "article-2"]
    assert by_channel["#disease-prevention"].articles[0].positive_feedback_count == 3
    assert by_channel["#disease-prevention"].articles[0].url == "https://example.com/article-1"
    assert by_channel["#behavioral-protocols"].articles[0].positive_feedback_count == 1


def test_build_weekly_report_sections_limits_each_channel_to_top_five_articles():
    slack_messages = []
    positive_feedback = []

    for index in range(6):
        message_ts = f"174000010{index}.000100"
        slack_messages.append(
            {
                "message_ts": message_ts,
                "article_id": f"article-{index}",
                "channel": "#research-general",
                "project_area": "general",
                "title": f"Article {index}",
                "url": f"https://example.com/article-{index}",
                "summary": f"Summary {index}",
                "published_date": date(2026, 3, 10 + index),
            }
        )
        for _ in range(index + 1):
            positive_feedback.append(
                {
                    "feedback_type": "positive",
                    "message_ts": message_ts,
                    "article_id": f"article-{index}",
                    "channel": "#research-general",
                }
            )

    sections = build_weekly_report_sections(positive_feedback, slack_messages, top_n=5)

    assert len(sections) == 1
    assert [article.article_id for article in sections[0].articles] == [
        "article-5",
        "article-4",
        "article-3",
        "article-2",
        "article-1",
    ]


def test_week_year_bounds_returns_monday_to_sunday_for_iso_week():
    start, end = week_year_bounds("2026-W13")

    assert start == date(2026, 3, 23)
    assert end == date(2026, 3, 29)


def test_build_top_feedback_users_returns_ranked_users_by_feedback_count():
    feedback_rows = [
        {"feedback_type": "positive", "user_id": "U-ALICE", "user_name": "alice"},
        {"feedback_type": "comment", "user_id": "U-ALICE", "user_name": "alice"},
        {"feedback_type": "negative", "user_id": "U-BOB", "user_name": "bob"},
        {"feedback_type": "reaction", "user_id": "U-BOB", "user_name": "bob"},
        {"feedback_type": "positive", "user_id": "U-ALICE", "user_name": "alice"},
        {"feedback_type": "positive", "user_id": "U-CHARLIE", "user_name": "charlie"},
        {"feedback_type": "positive", "user_name": "guest-user"},
        {"feedback_type": "positive"},
    ]

    top_users = build_top_feedback_users(feedback_rows, top_n=3)

    assert [(user.user_name, user.feedback_count) for user in top_users] == [
        ("alice", 3),
        ("bob", 2),
        ("charlie", 1),
    ]


def test_render_weekly_feedback_pdf_writes_local_pdf_file(tmp_path):
    pytest.importorskip("reportlab")

    output_path = tmp_path / "weekly_feedback_report_2026-W13.pdf"
    sections = _sample_weekly_report_sections()
    top_users = build_top_feedback_users(
        [
            {"feedback_type": "positive", "user_id": "U-ALICE", "user_name": "alice"},
            {"feedback_type": "comment", "user_id": "U-ALICE", "user_name": "alice"},
            {"feedback_type": "negative", "user_id": "U-BOB", "user_name": "bob"},
        ]
    )

    render_weekly_feedback_pdf(
        output_path=str(output_path),
        week_year="2026-W13",
        sections=sections,
        top_feedback_users=top_users,
        generated_at=datetime(2026, 3, 30, 4, 30, 0),
    )

    assert output_path.exists()
    assert output_path.stat().st_size > 0
    assert output_path.read_bytes().startswith(b"%PDF")
