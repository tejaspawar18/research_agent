import feedparser
from datetime import datetime
from core.logger import get_logger

logger = get_logger("rss_fetcher")

def parse_rss_time(entry):
    if "published_parsed" in entry and entry.published_parsed:
        return datetime(*entry.published_parsed[:6])
    return datetime.utcnow()

def get_rss_articles(source):
    url = source.get("rss")
    if not url:
        return []

    logger.info(f"Fetching RSS → {source['name']}")

    feed = feedparser.parse(url)
    results = []

    for entry in feed.entries:
        results.append({
            "url": entry.get("link"),
            "source": source["name"],
            "title": entry.get("title"),
            "published": parse_rss_time(entry),
            "selectors": source.get("selectors", {}),
            "doi": entry.get("doi", None),
            "authors": entry.get("authors", []),
        })

    return results
if __name__=="__main__":
    import yaml
    with open("config/sources.yaml", "r") as f:
        sources = yaml.safe_load(f).get("sources", [])
        for source in sources:
            if source.get("fetch_method") != "rss":
                continue
            res = get_rss_articles(source)
            print(len(res))
            print(res[0]) if len(res) > 0 else None 
            print("\n")
        

