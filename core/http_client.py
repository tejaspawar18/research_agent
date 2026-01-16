import requests
import cloudscraper
from tenacity import retry, stop_after_attempt, wait_exponential
from core.logger import get_logger

logger = get_logger("http_client")

# Create a cloudscraper instance which mimics a browser
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    }
)

# ... imports
from core.playwright_client import PlaywrightClient

# ... scraper setup

# NOTE: Removed @retry decorator because we have internal fallback logic now
# and retrying the same scraper call likely won't fix a 403.
def get(url, **kwargs):
    logger.info(f"GET → {url}")
    try:
        r = scraper.get(url, timeout=15, **kwargs)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning(f"Cloudscraper failed for {url}: {e}. Falling back to Playwright.")
        try:
            # Fallback to Playwright using the loop-safe helper
            from core.playwright_client import get_html_universal
            html = get_html_universal(url)
            # Create a mock response object to maintain compatibility
            resp = requests.Response()
            resp.status_code = 200
            resp._content = html.encode('utf-8')
            return resp
        except Exception as pw_e:
            logger.error(f"Playwright fallback also failed: {pw_e}")
            raise e

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def download(url, local_path):
    logger.info(f"Downloading {url} → {local_path}")
    r = scraper.get(url, stream=True, timeout=30)
    r.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in r.iter_content(4096):
            f.write(chunk)

    return local_path
if __name__ == "__main__":
    test_url = "https://www.nature.com/nature.rss"
    print(f"Testing with: {test_url}")
    try:
        r = get(test_url)
        print(f"Status: {r.status_code}")
        print(f"Content-Length: {len(r.text)}")
    except Exception as e:
        print(f"Failed: {e}")
