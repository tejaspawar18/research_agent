# core/playwright_client.py
from playwright.sync_api import sync_playwright
try:
    from playwright_stealth import stealth_sync
except ImportError:
    stealth_sync = None
from core.logger import get_logger
import time
from fake_useragent import UserAgent

logger = get_logger("playwright")

class PlaywrightClient:
    """Handles browser automation for complex PDF downloads and page scraping."""

    def __init__(self, headless=True, user_agent: str = None):
        try:
            logger.info("Starting Playwright (Sync)...")
            self.play = sync_playwright().start()
            # launch with stealth flags to avoid bot detection
            self.browser = self.play.chromium.launch(
                headless=headless, 
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-infobars",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--window-size=1920,1080"
                ]
            )

            try:
                ua = UserAgent(platforms="pc", browsers="chrome")
                self.user_agent = user_agent or ua.random
            except Exception:
                self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        except Exception as e:
            if "It looks like you are using Playwright Sync API inside the asyncio loop" in str(e):
                logger.error("Playwright Sync API called inside asyncio loop. This is a known conflict.")
            raise e

    def new_page(self, extra_http_headers: dict = None):
        # Base headers to look more like a real browser
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
        if extra_http_headers:
            headers.update(extra_http_headers)

        ctx = self.browser.new_context(
            accept_downloads=True, 
            user_agent=self.user_agent, 
            extra_http_headers=headers,
            viewport={'width': 1920, 'height': 1080},
            java_script_enabled=True,
            ignore_https_errors=True
        )
        page = ctx.new_page()
        # Apply stealth
        if stealth_sync:
            stealth_sync(page)
        return page, ctx

    def goto(self, url, timeout: int = 3000, extra_http_headers: dict = None):
        page, ctx = self.new_page(extra_http_headers=extra_http_headers)
        
        # Add a realistic referer if possible
        referer = "https://www.google.com/"
        if "sciencedirect.com" in url:
            referer = "https://www.sciencedirect.com/"
            
        try:
            page.goto(url, timeout=timeout, wait_until="networkidle", referer=referer)
        except Exception as e:
            logger.warning(f"Initial goto failed or timed out: {e}. Retrying with domcontentloaded...")
            page.goto(url, timeout=timeout, wait_until="domcontentloaded", referer=referer)
            
        return page, ctx

    def get_html(self, url, timeout: int = 3000):
        """Fetch HTML content using Playwright."""
        page = None
        html = ""
        try:
            page, ctx = self.goto(url, timeout=timeout)
            
            # ScienceDirect and others often need a moment for the challenge to resolve
            if "sciencedirect.com" in url:
                try:
                    # Wait for a typical ScienceDirect element like the abstract or the title
                    page.wait_for_selector(".abstract", timeout=10000)
                except:
                    # Fallback: just wait a few seconds
                    time.sleep(5)
            else:
                time.sleep(3) 
            
            # If we see "unsupported_browser" or "captcha", we might need to wait longer
            if "unsupported_browser" in page.url:
                logger.warning("ScienceDirect redirected to unsupported_browser. Retrying with longer wait...")
                time.sleep(10)
            
            html = page.content()
            ctx.close()
        except Exception as e:
            logger.error(f"Playwright get_html failed: {e}")
            if page:
                try:
                    page.close()
                except:
                    pass
            raise e
        return html

    def close(self):
        try:
            logger.info("Closing Playwright...")
            if hasattr(self, 'browser'):
                self.browser.close()
            if hasattr(self, 'play'):
                self.play.stop()
        except Exception:
            pass

from playwright.async_api import async_playwright
from playwright_stealth import stealth

class AsyncPlaywrightClient:
    """Async version of PlaywrightClient for use inside asyncio loops."""

    def __init__(self, headless=True, user_agent: str = None):
        self.headless = headless
        self._user_agent = user_agent
        self.play = None
        self.browser = None

    async def start(self):
        logger.info("Starting Playwright (Async)...")
        self.play = await async_playwright().start()
        self.browser = await self.play.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-size=1920,1080"
            ]
        )
        if not self._user_agent:
            try:
                ua = UserAgent(platforms="pc", browsers="chrome")
                self._user_agent = ua.random
            except Exception:
                self._user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

    async def get_html(self, url, timeout: int = 6000):
        if not self.browser:
            await self.start()
        
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
        }
        
        ctx = await self.browser.new_context(
            user_agent=self._user_agent,
            extra_http_headers=headers,
            viewport={'width': 1920, 'height': 1080},
            ignore_https_errors=True
        )
        page = await ctx.new_page()
        
        # Apply stealth
        if stealth:
            await stealth(page)
        else:
            await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        html = ""
        try:
            referer = "https://www.google.com/"
            if "sciencedirect.com" in url:
                referer = "https://www.sciencedirect.com/"
                
            await page.goto(url, timeout=timeout, wait_until="networkidle", referer=referer)
            
            if "sciencedirect.com" in url:
                try:
                    await page.wait_for_selector(".abstract", timeout=10000)
                except:
                    await asyncio.sleep(5)
            else:
                await asyncio.sleep(3)
                
            html = await page.content()
        except Exception as e:
            logger.error(f"Async Playwright get_html failed: {e}")
            raise e
        finally:
            await ctx.close()
        return html

    async def close(self):
        try:
            if self.browser:
                await self.browser.close()
            if self.play:
                await self.play.stop()
        except Exception:
            pass

import asyncio

def get_html_universal(url, timeout: int = 6000):
    """
    Universal helper to fetch HTML using Playwright, 
    detecting if an asyncio loop is already running and using the appropriate client.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # Use a temporary thread to run the sync Playwright client 
        # to avoid the "sync API inside asyncio loop" error.
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=1) as executor:
            def _task():
                pw = PlaywrightClient()
                try:
                    return pw.get_html(url, timeout)
                finally:
                    pw.close()
            future = executor.submit(_task)
            return future.result()
    else:
        # No loop running in this thread, safe to use sync client directly
        pw = PlaywrightClient()
        try:
            return pw.get_html(url, timeout)
        finally:
            pw.close()
