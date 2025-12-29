"""
RSS Feed Discovery Script
Checks all sources that use list_page method to find available RSS feeds.
"""
import feedparser
import requests
from urllib.parse import urljoin, urlparse
import re
import time

# Known RSS feed patterns for common sites
KNOWN_RSS_PATTERNS = {
    "bmj.com": "https://www.bmj.com/rss/recent.xml",
    "nih.gov": "https://www.nih.gov/news-events/news-releases/feed",
    "cdc.gov": "https://tools.cdc.gov/api/v2/resources/media/rss",
    "mit.edu": "https://news.mit.edu/rss/feed",
    "stanford.edu": "https://med.stanford.edu/news.feed",
    "harvard.edu": "https://magazine.hms.harvard.edu/rss.xml",
    "yalemedicine.org": None,  # No RSS found
    "heart.org": "https://www.heart.org/en/rss",
}

# Sources using list_page that need RSS discovery
SOURCES_TO_CHECK = [
    # BMJ
    ("bmj_clinical_review", "https://www.bmj.com/education/clinical-review"),
    ("bmj_research", "https://www.bmj.com/research/research"),
    ("bmj_practice", "https://www.bmj.com/education/practice"),
    # Institutional
    ("nih", "https://www.nih.gov/news-events/news-releases"),
    ("cdc", "https://www.cdc.gov/media/"),
    ("ecdc", "https://www.ecdc.europa.eu"),
    ("ihme", "https://www.healthdata.org"),
    ("pib_india", "https://www.pib.gov.in"),
    # University
    ("harvard_medicine", "https://magazine.hms.harvard.edu/articles"),
    ("yale_medicine", "https://www.yalemedicine.org"),
    ("mit_science", "https://news.mit.edu/school/science"),
    ("stanford_medicine", "https://med.stanford.edu/news.html"),
    # Expert
    ("american_heart_assoc", "https://www.heart.org/en/around-the-aha"),
    ("peter_attia", "https://peterattiamd.com"),
    ("obesity_energetics", "https://www.obesityandenergetics.org"),
    # ScienceDirect (search pages, no RSS)
    # PubMed (search pages, has RSS but different format)
]

def extract_base_domain(url):
    parsed = urlparse(url)
    return parsed.netloc

def try_common_rss_paths(base_url):
    """Try common RSS feed paths."""
    common_paths = [
        "/rss",
        "/rss.xml",
        "/feed",
        "/feed.xml",
        "/feeds/rss",
        "/atom.xml",
        "/rss/recent.xml",
        "/news/rss",
        "/news-events/feed",
        "/index.xml",
    ]
    
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    
    for path in common_paths:
        rss_url = base + path
        try:
            resp = requests.head(rss_url, timeout=5, allow_redirects=True)
            if resp.status_code == 200:
                # Verify it's actually RSS
                feed = feedparser.parse(rss_url)
                if feed.entries and len(feed.entries) > 0:
                    return rss_url, len(feed.entries)
        except:
            continue
    return None, 0

def check_page_for_rss_link(url):
    """Scan HTML page for RSS link elements."""
    try:
        resp = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (compatible; ResearchBot/1.0)"
        })
        html = resp.text
        
        # Look for RSS link tags
        patterns = [
            r'<link[^>]*type=["\']application/rss\+xml["\'][^>]*href=["\']([^"\']+)["\']',
            r'<link[^>]*href=["\']([^"\']+)["\'][^>]*type=["\']application/rss\+xml["\']',
            r'<a[^>]*href=["\']([^"\']+\.rss)["\']',
            r'<a[^>]*href=["\']([^"\']+/rss)["\']',
            r'<a[^>]*href=["\']([^"\']+/feed)["\']',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            for match in matches:
                rss_url = urljoin(url, match)
                try:
                    feed = feedparser.parse(rss_url)
                    if feed.entries and len(feed.entries) > 0:
                        return rss_url, len(feed.entries)
                except:
                    continue
    except:
        pass
    return None, 0

def main():
    print("\n" + "="*70)
    print(" RSS FEED DISCOVERY FOR LIST_PAGE SOURCES")
    print("="*70 + "\n")
    
    results = {
        "found": [],
        "not_found": []
    }
    
    for name, url in SOURCES_TO_CHECK:
        print(f"Checking {name}...", end=" ", flush=True)
        domain = extract_base_domain(url)
        
        # Check known patterns first
        if domain in KNOWN_RSS_PATTERNS:
            if KNOWN_RSS_PATTERNS[domain]:
                rss_url = KNOWN_RSS_PATTERNS[domain]
                try:
                    feed = feedparser.parse(rss_url)
                    count = len(feed.entries)
                    if count > 0:
                        print(f"✓ Known RSS: {count} articles")
                        results["found"].append((name, rss_url, count))
                        continue
                except:
                    pass
        
        # Try common paths
        rss_url, count = try_common_rss_paths(url)
        if rss_url:
            print(f"✓ Found via path: {count} articles")
            results["found"].append((name, rss_url, count))
            continue
        
        # Scan page for RSS links
        rss_url, count = check_page_for_rss_link(url)
        if rss_url:
            print(f"✓ Found in page: {count} articles")
            results["found"].append((name, rss_url, count))
            continue
        
        print("✗ No RSS found")
        results["not_found"].append(name)
        
        time.sleep(0.5)  # Be nice to servers
    
    # Summary
    print("\n" + "="*70)
    print(" DISCOVERY RESULTS")
    print("="*70)
    
    if results["found"]:
        print(f"\n✓ RSS Feeds Found ({len(results['found'])}):\n")
        for name, rss_url, count in results["found"]:
            print(f"  - name: {name}")
            print(f"    rss: \"{rss_url}\"")
            print(f"    # {count} articles")
            print()
    
    if results["not_found"]:
        print(f"\n✗ No RSS Found ({len(results['not_found'])}):")
        for name in results["not_found"]:
            print(f"    - {name}")
    
    print("\n" + "="*70 + "\n")

if __name__ == "__main__":
    main()
