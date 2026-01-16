import yaml
import requests
import feedparser
from urllib.parse import urlparse

def test_rss():
    with open("config/sources.yaml", "r") as f:
        config = yaml.safe_load(f)
        sources = config.get("sources", [])
        
    for source in sources:
        if source.get("fetch_method") == "rss" and "rss" in source:
            name = source["name"]
            url = source["rss"]
            print(f"Testing {name}: {url}")
            try:
                r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
                if r.status_code != 200:
                    print(f"  [FAIL] Status: {r.status_code}")
                    continue
                
                feed = feedparser.parse(r.content)
                if not feed.entries:
                    print(f"  [FAIL] No entries found in feed")
                else:
                    print(f"  [OK] Found {len(feed.entries)} entries")
            except Exception as e:
                print(f"  [ERROR] {e}")

if __name__ == "__main__":
    test_rss()
