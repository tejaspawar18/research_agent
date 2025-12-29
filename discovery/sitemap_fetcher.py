import requests
from bs4 import BeautifulSoup
from datetime import datetime
from core.logger import get_logger

logger = get_logger("sitemap_fetcher")

def get_sitemap_urls(source):
    sitemap_url = source.get("sitemap")
    if not sitemap_url:
        return []

    logger.info(f"Fetching sitemap → {source['name']}")

    r = requests.get(sitemap_url, timeout=15)
    soup = BeautifulSoup(r.text, "xml")

    urls = []
    for loc in soup.find_all("loc"):
        urls.append({
            "url": loc.text,
            "source": source["name"],
            "title": None,
            "published": datetime.utcnow(),
            "selectors": source.get("selectors", {})
        })

    return urls
