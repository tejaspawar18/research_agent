import requests

urls = [
    'https://www.nih.gov/news/feed.xml',
    'http://www2c.cdc.gov/podcasts/feed.asp?feedid=183',
    'https://www.ecdc.europa.eu/en/publications-data/all-news/rss',
    'https://newsroom.heart.org/rss.xml',
    'https://peterattiamd.com/feed/',
    'https://www.obesityandenergetics.org/feed/',
    'https://news.mit.edu/rss/school/science',
    'https://med.stanford.edu/news/all/rss'
]

for url in urls:
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        print(f"{url}: {r.status_code}")
        if r.status_code == 200:
            print(f"  Snippet: {r.text[:100].strip()}")
    except Exception as e:
        print(f"{url}: ERROR {e}")
