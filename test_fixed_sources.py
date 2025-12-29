"""
Test only the fixed sources to verify improvements.
"""
import os
import time
from dotenv import load_dotenv
load_dotenv()

from core.logger import get_logger
from discovery.rss_fetcher import get_rss_articles
from discovery.list_page_scraper import get_list_page_articles

logger = get_logger("test_fixed")

# Fixed sources to test
FIXED_SOURCES = [
    # Lancet - now using RSS
    {
        "name": "lancet_diabetes",
        "type": "journal",
        "fetch_method": "rss",
        "rss": "https://www.thelancet.com/rssfeed/landia_online.xml",
        "selectors": {}
    },
    {
        "name": "lancet_respiratory",
        "type": "journal",
        "fetch_method": "rss",
        "rss": "https://www.thelancet.com/rssfeed/lanres_online.xml",
        "selectors": {}
    },
    # Cell - now using RSS
    {
        "name": "cell_neuron",
        "type": "journal",
        "fetch_method": "rss",
        "rss": "https://www.cell.com/neuron/inpress.rss",
        "selectors": {}
    },
    # BMJ - removed bad selectors, using URL pattern filtering
    {
        "name": "bmj_clinical_review",
        "type": "journal",
        "fetch_method": "list_page",
        "url": "https://www.bmj.com/education/clinical-review",
        "selectors": {}
    },
    # JAHA - now using RSS  
    {
        "name": "jaha",
        "type": "journal",
        "fetch_method": "rss",
        "rss": "https://www.ahajournals.org/action/showFeed?type=etoc&feed=rss&jc=jaha",
        "selectors": {}
    },
]

def test_source(source):
    """Test a single source."""
    name = source["name"]
    method = source["fetch_method"]
    start = time.time()
    
    try:
        if method == "rss":
            articles = get_rss_articles(source)
        else:
            articles = get_list_page_articles(source)
        
        duration = round(time.time() - start, 2)
        
        # Show results
        icon = "✓" if len(articles) >= 5 else ("⚠" if len(articles) > 0 else "✗")
        print(f"{icon} {name:<25} | {len(articles):>4} articles | {duration}s | {method}")
        
        # Show sample titles for quality check
        if articles:
            print("   Sample titles:")
            for article in articles[:3]:
                title = article.get("title", "(no title)")
                if title:
                    title = title[:65] + "..." if len(title) > 65 else title
                print(f"     • {title}")
        print()
        
        return len(articles)
        
    except Exception as e:
        print(f"✗ {name}: ERROR - {str(e)[:60]}")
        return 0

def main():
    print("\n" + "="*70)
    print(" TESTING FIXED SOURCES")
    print("="*70 + "\n")
    
    total = 0
    for source in FIXED_SOURCES:
        count = test_source(source)
        total += count
    
    print("="*70)
    print(f" TOTAL: {total} articles from {len(FIXED_SOURCES)} fixed sources")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
