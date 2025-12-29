"""
Multi-source test script - validates different source types.
"""
import os
import sys
from dotenv import load_dotenv
load_dotenv()

from core.logger import get_logger
from discovery.rss_fetcher import get_rss_articles
from discovery.list_page_scraper import get_list_page_articles

logger = get_logger("test_sources")

# Test sources from different categories
TEST_SOURCES = [
    # RSS sources
    {
        "name": "nature",
        "type": "rss",
        "fetch_method": "rss",
        "rss": "https://www.nature.com/nature.rss",
        "selectors": {}
    },
    {
        "name": "nature_communications",
        "type": "rss",
        "fetch_method": "rss",
        "rss": "https://www.nature.com/ncomms.rss",
        "selectors": {}
    },
    # List page sources
    {
        "name": "yale_medicine_news",
        "type": "list_page",
        "fetch_method": "list_page",
        "url": "https://www.yalemedicine.org/search/news?tagId=1",
        "selectors": {"article_link": "a.search-result-card__link"}
    },
    {
        "name": "nih",
        "type": "list_page",
        "fetch_method": "list_page",
        "url": "https://www.nih.gov/news-events/news-releases",
        "selectors": {"article_link": "a.teaser__title-link"}
    },
    {
        "name": "mit_science",
        "type": "list_page",
        "fetch_method": "list_page",
        "url": "https://news.mit.edu/school/science",
        "selectors": {"article_link": "a.term-page--news-article--item--title--link"}
    },
]

def test_source(source):
    """Test a single source and return results."""
    name = source["name"]
    method = source["fetch_method"]
    
    print(f"\n{'='*60}")
    print(f"Testing: {name} ({method})")
    print(f"{'='*60}")
    
    try:
        if method == "rss":
            articles = get_rss_articles(source)
        elif method == "list_page":
            articles = get_list_page_articles(source)
        else:
            print(f"Unknown method: {method}")
            return None
        
        print(f"✓ Found {len(articles)} articles")
        
        if articles:
            print("\nSample articles (first 3):")
            for i, article in enumerate(articles[:3]):
                title = article.get("title", "No title")
                if title and len(title) > 70:
                    title = title[:70] + "..."
                url = article.get("url", "No URL")
                if len(url) > 70:
                    url = url[:70] + "..."
                print(f"  {i+1}. {title}")
                print(f"     {url}")
        
        return articles
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    print("\n" + "#"*60)
    print("# MULTI-SOURCE TEST")
    print("#"*60)
    
    results = {}
    for source in TEST_SOURCES:
        articles = test_source(source)
        results[source["name"]] = len(articles) if articles else 0
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    for name, count in results.items():
        status = "✓" if count > 0 else "✗"
        print(f"  {status} {name}: {count} articles")

if __name__ == "__main__":
    main()
