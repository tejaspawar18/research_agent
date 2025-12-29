import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from core.logger import get_logger

logger = get_logger("http_client")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ResearchBot/1.0; +https://example.com)"
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def get(url, **kwargs):
    logger.info(f"GET → {url}")
    r = requests.get(url, headers=HEADERS, timeout=15, **kwargs)
    r.raise_for_status()
    return r

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
def download(url, local_path):
    logger.info(f"Downloading {url} → {local_path}")
    r = requests.get(url, stream=True, headers=HEADERS, timeout=20)
    r.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in r.iter_content(4096):
            f.write(chunk)

    return local_path
