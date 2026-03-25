import asyncio

from services.notification.main import (
    SlackFeedbackIngestor,
    get_interaction_response_payload,
    handle_interaction_payload,
)


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


def test_get_interaction_response_payload_clears_comment_modal_submission():
    payload = {
        "type": "view_submission",
        "view": {"callback_id": "comment_modal_article-1"},
    }

    assert get_interaction_response_payload(payload) == {"response_action": "clear"}


def test_socket_mode_acknowledges_interactive_request_before_processing(monkeypatch):
    call_order = []

    class FakeClient:
        async def send_socket_mode_response(self, response):
            call_order.append(("ack", response.envelope_id))

    class FakeReq:
        type = "interactive"
        envelope_id = "env-123"
        payload = {"type": "block_actions", "actions": []}

    async def fake_handle_interaction_payload(payload):
        call_order.append(("process", payload["type"]))
        return {}

    monkeypatch.setattr(
        "services.notification.main.handle_interaction_payload",
        fake_handle_interaction_payload,
    )

    ingestor = SlackFeedbackIngestor(db=object())
    asyncio.run(ingestor._handle_socket_request(FakeClient(), FakeReq()))

    assert call_order == [
        ("ack", "env-123"),
        ("process", "block_actions"),
    ]
