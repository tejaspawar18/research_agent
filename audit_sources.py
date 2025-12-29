"""
Comprehensive source discovery test.
Tests all sources from sources.yaml and reports:
- Article counts per source
- Sample article quality
- Errors/blocking issues
- No summarization - discovery only
"""
import os
import yaml
import time
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from core.logger import get_logger
from discovery.rss_fetcher import get_rss_articles
from discovery.list_page_scraper import get_list_page_articles
from discovery.sitemap_fetcher import get_sitemap_urls

logger = get_logger("source_audit")

def load_sources():
    """Load all sources from sources.yaml"""
    config_path = os.path.join(os.path.dirname(__file__), "config", "sources.yaml")
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("sources", [])

def test_source(source):
    """Test a single source and return detailed results."""
    name = source.get("name", "unknown")
    method = source.get("fetch_method", "unknown")
    start_time = time.time()
    
    result = {
        "name": name,
        "type": source.get("type", "unknown"),
        "method": method,
        "url": source.get("rss") or source.get("url") or source.get("sitemap"),
        "article_count": 0,
        "sample_articles": [],
        "error": None,
        "duration_sec": 0,
        "status": "unknown"
    }
    
    try:
        if method == "rss":
            articles = get_rss_articles(source)
        elif method == "list_page":
            articles = get_list_page_articles(source)
        elif method == "sitemap":
            articles = get_sitemap_urls(source)
        else:
            result["error"] = f"Unknown method: {method}"
            result["status"] = "error"
            return result
        
        result["article_count"] = len(articles)
        result["duration_sec"] = round(time.time() - start_time, 2)
        
        # Get sample articles for quality check
        for article in articles[:3]:
            sample = {
                "title": article.get("title", "")[:80] if article.get("title") else "(no title)",
                "url": article.get("url", "")[:100]
            }
            result["sample_articles"].append(sample)
        
        # Determine status
        if result["article_count"] == 0:
            result["status"] = "empty"
        elif result["article_count"] < 5:
            result["status"] = "low"
        else:
            result["status"] = "ok"
            
    except Exception as e:
        result["error"] = str(e)
        result["status"] = "error"
        result["duration_sec"] = round(time.time() - start_time, 2)
    
    return result

def print_result(result, verbose=False):
    """Print a single source result."""
    status_icons = {
        "ok": "✓",
        "low": "⚠",
        "empty": "✗",
        "error": "❌",
        "unknown": "?"
    }
    icon = status_icons.get(result["status"], "?")
    
    print(f"{icon} {result['name']:<35} | {result['article_count']:>4} articles | {result['duration_sec']:>5}s | {result['method']}")
    
    if result["error"]:
        print(f"    ERROR: {result['error'][:80]}")
    
    if verbose and result["sample_articles"]:
        for i, sample in enumerate(result["sample_articles"][:2], 1):
            print(f"    {i}. {sample['title'][:60]}")

def main():
    print("\n" + "="*80)
    print(" RESEARCH AGENT - SOURCE DISCOVERY AUDIT")
    print(" " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("="*80)
    
    sources = load_sources()
    print(f"\nLoaded {len(sources)} sources from sources.yaml\n")
    
    # Group results by category
    results = {
        "ok": [],
        "low": [],
        "empty": [],
        "error": []
    }
    
    total_articles = 0
    
    # Test each source
    for i, source in enumerate(sources, 1):
        print(f"[{i}/{len(sources)}] Testing {source['name']}...", end=" ", flush=True)
        result = test_source(source)
        results[result["status"]].append(result)
        total_articles += result["article_count"]
        
        # Clear line and print result
        print(f"\r", end="")
        print_result(result)
    
    # Summary
    print("\n" + "="*80)
    print(" SUMMARY")
    print("="*80)
    
    print(f"\n📊 Total Articles Discovered: {total_articles}")
    print(f"\n✓ Working Sources ({len(results['ok'])}):")
    for r in results["ok"]:
        print(f"    {r['name']}: {r['article_count']} articles")
    
    if results["low"]:
        print(f"\n⚠ Low Article Count ({len(results['low'])}):")
        for r in results["low"]:
            print(f"    {r['name']}: {r['article_count']} articles")
    
    if results["empty"]:
        print(f"\n✗ Empty/Blocked Sources ({len(results['empty'])}):")
        for r in results["empty"]:
            print(f"    {r['name']}: {r['url'][:60]}...")
    
    if results["error"]:
        print(f"\n❌ Failed Sources ({len(results['error'])}):")
        for r in results["error"]:
            print(f"    {r['name']}: {r['error'][:60]}")
    
    # Quality check
    print("\n" + "-"*80)
    print(" SAMPLE ARTICLE QUALITY CHECK")
    print("-"*80)
    
    # Pick a few good sources to show samples
    sample_sources = [r for r in results["ok"] if r["article_count"] >= 10][:5]
    for r in sample_sources:
        print(f"\n{r['name']} ({r['article_count']} articles):")
        for sample in r["sample_articles"]:
            title = sample["title"] if sample["title"] != "(no title)" else "[No title extracted]"
            print(f"    • {title}")
    
    print("\n" + "="*80)
    print(f" AUDIT COMPLETE - {len(sources)} sources tested")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
