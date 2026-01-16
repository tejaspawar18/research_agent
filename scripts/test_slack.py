from core.slack_notifier import send_article_notification
from unittest.mock import patch, MagicMock

def test_slack_notify():
    # Mock article data dict
    article_data = {
        "title": "Test Article",
        "journal": "Nature",
        "published_date": "2025-12-29",
        "doi": "10.1038/xxx",
        "url": "http://example.com/pdf",
        "source": "nature"
    }
    
    summary = "This is a summary of the article."
    
    with patch("requests.post") as mock_post:
        print("Testing send_article_notification()...")
        send_article_notification(article_data, summary)
        
        if mock_post.called:
            print("SUCCESS: requests.post called.")
            args, kwargs = mock_post.call_args
            print("Payload:", kwargs.get("json"))
        else:
            print("FAILURE: requests.post NOT called. (Did you set SLACK_WEBHOOK_URL?)")
            # We mock os.getenv to force it if needed, but the code checks env var.
            # actually code does: WEBHOOK = os.getenv(...) at module level.
            # if it's None, it returns early.
            # check if we need to patch module level variable or just patch os.getenv before import (too late).
            # We can patch core.slack_notifier.WEBHOOK
    
    # If failure due to missing webhook, let's enforce it for test
    import core.slack_notifier
    if not core.slack_notifier.WEBHOOK:
        print("Mocking WEBHOOK for test...")
        with patch("core.slack_notifier.WEBHOOK", "http://mock.webhook"):
             with patch("requests.post") as mock_post:
                send_article_notification(article_data, summary)
                if mock_post.called:
                     print("SUCCESS (with mocked webhook): requests.post called.")
                     print("Payload:", mock_post.call_args[1].get("json"))

if __name__ == "__main__":
    test_slack_notify()
