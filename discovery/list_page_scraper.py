"""
List page scraper: Extract article URLs from listing pages.
Supports custom CSS selectors and smart article-link filtering.
"""
import re
from urllib.parse import urljoin, urlparse
from datetime import datetime
from core.logger import get_logger
from core.playwright_client import PlaywrightClient

logger = get_logger("list_scraper")

# URL patterns that typically indicate article pages
ARTICLE_URL_PATTERNS = [
    r'/article[s]?/',
    r'/news/',
    r'/pii/',           # ScienceDirect
    r'/PMC\d+',         # PubMed Central
    r'/doi/',
    r'/research/',
    r'/publication/',
    r'/press-release',
    r'/story/',
    r'/post/',
    r'/archive/',
    r'\d{4}/\d{2}/',    # Date-based URLs like /2025/01/
]

# URL patterns to exclude (navigation, login, etc.)
EXCLUDE_URL_PATTERNS = [
    r'/login',
    r'/signin',
    r'/signup',
    r'/register',
    r'/search\?',       # Don't include search links
    r'/tag/',
    r'/category/',
    r'/author/',
    r'/about',
    r'/contact',
    r'/faq',
    r'/privacy',
    r'/terms',
    r'/cookie',
    r'#',               # Anchor links
    r'javascript:',
    r'mailto:',
]

def _is_article_url(url, base_domain):
    """Check if URL looks like an article link."""
    if not url:
        return False
    
    # Must be same domain or relative
    parsed = urlparse(url)
    if parsed.netloc and base_domain not in parsed.netloc:
        return False
    
    # Check exclusion patterns
    for pattern in EXCLUDE_URL_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return False
    
    # Check article patterns
    for pattern in ARTICLE_URL_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    
    # Default: include if it has a reasonable path depth
    path = parsed.path
    if path and path.count('/') >= 2 and len(path) > 15:
        return True
    
    return False

def get_list_page_articles(source):
    """
    Extract article URLs from a listing page.
    Uses custom selector if provided, otherwise finds all links and filters.
    """
    url = source.get("url")
    if not url:
        return []

    logger.info(f"Fetching list page → {source['name']}")
    
    base_domain = urlparse(url).netloc
    selectors = source.get("selectors", {})
    article_selector = selectors.get("article_link")
    
    results = []
    
    try:
        # Use loop-safe helper to get HTML
        from core.playwright_client import get_html_universal
        from bs4 import BeautifulSoup
        
        html = get_html_universal(url)
        if not html:
            logger.warning(f"No HTML returned for {source['name']}")
            return []
            
        soup = BeautifulSoup(html, "lxml")
        links = []
        
        # Try custom selector first
        if article_selector:
            try:
                elements = soup.select(article_selector)
                for el in elements:
                    href = el.get("href")
                    title = el.get_text() if el else None
                    if href:
                        links.append((href, title))
                logger.info(f"Found {len(links)} links via selector '{article_selector}'")
            except Exception as e:
                logger.debug(f"Selector '{article_selector}' failed: {e}")
        
        # Fallback: get all anchor tags and filter
        if not links:
            all_anchors = soup.find_all("a")
            for anchor in all_anchors:
                try:
                    href = anchor.get("href")
                    title = anchor.get_text()
                    if href:
                        links.append((href, title))
                except:
                    continue
            logger.info(f"Found {len(links)} total links, filtering...")
        
        # Process and filter links
        seen_urls = set()
        for href, title in links:
            # Make absolute URL
            if href.startswith("/"):
                href = urljoin(url, href)
            elif not href.startswith("http"):
                continue
            
            # Skip duplicates
            if href in seen_urls:
                continue
            seen_urls.add(href)
            
            # Filter for article URLs (only if no custom selector was used)
            if not article_selector and not _is_article_url(href, base_domain):
                continue
            
            # Clean title
            title = title.strip() if title else None
            if title and len(title) > 200:
                title = title[:200] + "..."
            
            results.append({
                "url": href,
                "source": source["name"],
                "title": title,
                "published": datetime.utcnow(),
                "selectors": selectors
            })
        
        logger.info(f"Extracted {len(results)} article URLs from {source['name']}")
        
    except Exception as e:
        logger.error(f"List page scrape failed for {source['name']}: {e}")
    
    return results

