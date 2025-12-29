"""
Test script for the research agent pipeline.
Tests: RSS fetching, PDF download, and summarization.
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from core.logger import get_logger
from discovery.rss_fetcher import get_rss_articles
from extraction import download_pdf, extract_html_content

logger = get_logger("test_pipeline")

def test_rss_fetcher():
    """Test RSS fetching from a journal."""
    print("\n" + "="*60)
    print("TEST 1: RSS Fetcher")
    print("="*60)
    
    source = {
        'name': 'nature',
        'rss': 'https://www.nature.com/nature.rss',
        'selectors': {'pdf_button': "a[aria-label='Download PDF']"}
    }
    
    try:
        articles = get_rss_articles(source)
        print(f"✓ Found {len(articles)} articles from {source['name']}")
        
        if articles:
            print("\nSample articles:")
            for i, article in enumerate(articles[1:4]):
                title = article.get('title', 'No title')
                url = article.get('url', 'No URL')
                print(f"  {i+1}. {title}...")
                print(f"     URL: {url}...")
        
        return articles
    except Exception as e:
        print(f"✗ RSS fetch failed: {e}")
        return []

def test_html_extraction(url):
    """Test HTML content extraction."""
    print("\n" + "="*60)
    print("TEST 2: HTML Content Extraction")
    print("="*60)
    
    try:
        html_data = extract_html_content(url)
        if html_data and html_data.get('text'):
            text = html_data['text']
            print(f"✓ Extracted {len(text)} characters of text")
            print(f"\nPreview (first 500 chars):\n{text}...")
            return html_data
        else:
            print("✗ No text extracted")
            return None
    except Exception as e:
        print(f"✗ HTML extraction failed: {e}")
        return None

def test_pdf_download(url, selectors):
    """Test PDF download."""
    print("\n" + "="*60)
    print("TEST 3: PDF Download")
    print("="*60)
    
    os.makedirs("data/temp", exist_ok=True)
    
    try:
        result = download_pdf(url, selectors=selectors, outdir="data/temp")
        # download_pdf now returns a dict with pdf_path, note, doi, fallback_abstract
        if isinstance(result, dict):
            pdf_path = result.get("pdf_path")
            note = result.get("note", "unknown")
            doi = result.get("doi")
            print(f"  DOI found: {doi}")
            print(f"  Download method: {note}")
            if pdf_path:
                print(f"✓ PDF downloaded to: {pdf_path}")
                return pdf_path
            elif result.get("fallback_abstract"):
                print(f"⚠ No PDF, but got abstract via Crossref")
                return None
            else:
                print(f"✗ PDF download failed (note: {note})")
                return None
        elif result:
            print(f"✓ PDF downloaded to: {result}")
            return result
        else:
            print("✗ PDF download returned no valid path")
            return None
    except Exception as e:
        print(f"✗ PDF download failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_summarizer(pdf_path=None, html_data=None):
    """Test the summarizer."""
    print("\n" + "="*60)
    print("TEST 4: Summarization")
    print("="*60)
    
    # Check if OpenAI API key is set
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or api_key.startswith("sk-proj-") == False:
        print("⚠ OPENAI_API_KEY may not be valid. Check your .env file.")
    
    try:
        if pdf_path:
            from processing.summarizer import summarize_pdf
            print(f"Summarizing PDF: {pdf_path}")
            summary = summarize_pdf(pdf_path)
        elif html_data:
            from processing.summarizer import summarize_html
            print("Summarizing HTML content...")
            summary = summarize_html(html_data)
        else:
            print("✗ No PDF or HTML data to summarize")
            return None
        
        if summary:
            print("✓ Summary generated successfully!")
            print(f"\nTitle: {summary.get('title', 'N/A')}")
            print(f"Authors: {summary.get('authors', 'N/A')}")
            print(f"\nFinal Summary:\n{summary.get('final_summary', 'N/A')[:500]}...")
            return summary
        else:
            print("✗ Summarizer returned None")
            return None
    except Exception as e:
        print(f"✗ Summarization failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    print("\n" + "#"*60)
    print("# RESEARCH AGENT PIPELINE TEST")
    print("#"*60)
    
    # Test 1: RSS Fetching
    articles = test_rss_fetcher()
    
    if not articles:
        print("\nNo articles found. Cannot continue testing.")
        return
    
    # Get first article URL for further tests
    test_url = articles[0].get('url')
    selectors = articles[0].get('selectors', {})
    
    print(f"\n→ Using test URL: {test_url}")
    
    # Test 2: HTML Extraction
    html_data = test_html_extraction(test_url)
    
    # Test 3: PDF Download (optional, may fail for paywalled content)
    pdf_path = test_pdf_download(test_url, selectors)
    
    # Test 4: Summarization (use PDF if available, otherwise HTML)
    if pdf_path:
        summary = test_summarizer(pdf_path=pdf_path)
    elif html_data:
        summary = test_summarizer(html_data=html_data)
    else:
        print("\n✗ Cannot test summarizer - no content available")
    
    print("\n" + "#"*60)
    print("# TEST COMPLETE")
    print("#"*60)

if __name__ == "__main__":
    main()
