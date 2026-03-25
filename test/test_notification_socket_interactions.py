import asyncio

from services.notification.main import handle_interaction_payload


def test_handle_interaction_payload_stores_positive_feedback(monkeypatch):
    captured = {}

    def fake_insert_article_feedback(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(
        "services.notification.main.scylla_manager.insert_article_feedback",
        fake_insert_article_feedback,
    )

    payload = {
        "type": "block_actions",
        "user": {"id": "U123", "name": "tester"},
        "channel": {"id": "C123"},
        "message": {"ts": "1742900000.000100"},
        "actions": [
            {
                "action_id": "feedback_positive",
                "value": "article-1|source-1|2026-03-25",
                "action_ts": "1742900001.000100",
            }
        ],
    }

    result = asyncio.run(handle_interaction_payload(payload))

    assert result == {}
    assert captured["article_id"] == "article-1"
    assert captured["channel"] == "C123"
    assert captured["feedback_type"] == "positive"
    assert captured["user_id"] == "U123"


def test_handle_interaction_payload_opens_comment_modal(monkeypatch):
    captured = {}

    async def fake_open_comment_modal(trigger_id, article_id, source_id, published_date, message_ts, channel):
        captured["trigger_id"] = trigger_id
        captured["article_id"] = article_id
        captured["source_id"] = source_id
        captured["published_date"] = published_date
        captured["message_ts"] = message_ts
        captured["channel"] = channel

    monkeypatch.setattr(
        "services.notification.main.open_comment_modal",
        fake_open_comment_modal,
    )

    payload = {
        "type": "block_actions",
        "trigger_id": "trigger-123",
        "channel": {"id": "C123"},
        "message": {"ts": "1742900000.000100"},
        "actions": [
            {
                "action_id": "feedback_comment",
                "value": "article-1|source-1|2026-03-25",
                "action_ts": "1742900001.000100",
            }
        ],
    }

    result = asyncio.run(handle_interaction_payload(payload))

    assert result == {}
    assert captured == {
        "trigger_id": "trigger-123",
        "article_id": "article-1",
        "source_id": "source-1",
        "published_date": "2026-03-25",
        "message_ts": "1742900000.000100",
        "channel": "C123",
    }
