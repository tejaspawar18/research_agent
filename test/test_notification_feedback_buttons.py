from services.notification.main import MessageFormatter
from shared.models import Article


def test_format_article_includes_feedback_buttons():
    article = Article(
        article_id="article-123",
        source_id="source-abc",
        url="https://example.com/articles/123",
        title="Exercise timing and cardiometabolic health",
        summary="A short summary for testing.",
    )

    blocks, text = MessageFormatter.format_article(article)

    assert text == "Exercise timing and cardiometabolic health"
    assert any(block.get("type") == "actions" for block in blocks)

    actions_block = next(block for block in blocks if block.get("type") == "actions")
    action_ids = [element["action_id"] for element in actions_block["elements"]]
    values = [element["value"] for element in actions_block["elements"]]

    assert action_ids == ["feedback_positive", "feedback_negative", "feedback_comment"]
    assert all(value.startswith("article-123|source-abc|") for value in values)
