# core/playwright_client.py
from playwright.sync_api import sync_playwright
from core.logger import get_logger

logger = get_logger("playwright")

DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 ResearchAgent/1.0"

class PlaywrightClient:
    """Handles browser automation for complex PDF downloads."""

    def __init__(self, headless=True, user_agent: str = None):
        logger.info("Starting Playwright...")
        self.play = sync_playwright().start()
        # launch with stealth flags to avoid bot detection
        self.browser = self.play.chromium.launch(
            headless=headless, 
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars",
                "--disable-dev-shm-usage",
                "--disable-browser-side-navigation",
                "--disable-gpu"
            ]
        )

        self.user_agent = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

    def new_page(self, extra_http_headers: dict = None):
        ctx = self.browser.new_context(accept_downloads=True, user_agent=self.user_agent, extra_http_headers=extra_http_headers or {})
        page = ctx.new_page()
        return page, ctx

    def goto(self, url, timeout: int = 60000, extra_http_headers: dict = None):
        page, ctx = self.new_page(extra_http_headers=extra_http_headers)
        page.goto(url, timeout=timeout)
        return page, ctx

    def close(self):
        try:
            logger.info("Closing Playwright...")
            self.browser.close()
            self.play.stop()
        except Exception:
            pass
