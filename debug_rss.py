"""Quick debug script for RSS feed"""
import feedparser

feeds = [
    ('eurjmedres', 'https://eurjmedres.biomedcentral.com/articles/rss.xml'),
    ('nature', 'https://www.nature.com/nature.rss'),
    ('arxiv_cs', 'http://arxiv.org/rss/cs.AI'),
]

for name, url in feeds:
    print(f"\nTesting: {name}")
    print(f"URL: {url}")
    feed = feedparser.parse(url)
    print(f"  Status: {feed.get('status', 'N/A')}")
    print(f"  Bozo (error): {feed.get('bozo', False)}")
    if feed.bozo:
        print(f"  Bozo exception: {feed.get('bozo_exception', 'N/A')}")
    print(f"  Entries: {len(feed.entries)}")
    if feed.entries:
        print(f"  First entry title: {feed.entries[0].get('title', 'N/A')[:60]}...")
