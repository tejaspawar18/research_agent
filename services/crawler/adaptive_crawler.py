"""
Adaptive crawler that automatically tries multiple crawling methods.
Falls back through different techniques to maximize success rate.
"""
import logging
from typing import List, Optional, Tuple
from datetime import datetime

import sys
sys.path.insert(0, '/app')

from shared.models import Article
from rss_crawler import RSSCrawler
from html_crawler import HTMLCrawler
from pubmed_crawler import PubMedCrawler

logger = logging.getLogger(__name__)


class AdaptiveCrawler:
    """
    Intelligent crawler that tries multiple methods automatically.

    Attempts crawling in this order:
    1. RSS feed (if rss_url configured)
    2. HTML scraping with configured selectors
    3. HTML scraping with generic selectors
    4. Auto-detect RSS/Atom feeds from HTML
    """

    def __init__(self, source):
        """
        Initialize adaptive crawler.

        Args:
            source: SourceConfig object
        """
        self.source = source
        self.attempts = []

    async def crawl(self, max_articles: int = 50) -> List[Article]:
        """
        Adaptively crawl using multiple fallback methods.

        Args:
            max_articles: Maximum articles to retrieve

        Returns:
            List of Article objects
        """
        articles = []

        # Debug logging
        logger.info(f"Crawling {self.source.name}, crawl_method: {getattr(self.source, 'crawl_method', 'NOT SET')}")

        # Method 0: Try PubMed API if crawl_method is pubmed_api
        if hasattr(self.source, 'crawl_method') and self.source.crawl_method == 'pubmed_api':
            logger.info(f"Using PubMed API for {self.source.name}")
            articles = await self._try_pubmed_api_crawl(max_articles)
            if articles:
                logger.info(f"✓ PubMed API crawl succeeded for {self.source.name}: {len(articles)} articles")
                return articles
            logger.warning(f"PubMed API returned no articles for {self.source.name}")

        # Method 1: Try RSS feed first (fast and reliable)
        if self.source.rss_url:
            articles = await self._try_rss_crawl(max_articles)
            if articles:
                logger.info(f"✓ RSS crawl succeeded for {self.source.name}: {len(articles)} articles")
                return articles

        # Method 2: Auto-detect RSS/Atom feeds from the page
        articles = await self._try_auto_detect_feed(max_articles)
        if articles:
            logger.info(f"✓ Auto-detected feed succeeded for {self.source.name}: {len(articles)} articles")
            return articles

        # Method 3: Try HTML scraping with configured selectors
        if self.source.css_selectors:
            articles = await self._try_html_crawl(max_articles, use_defaults=False)
            if articles:
                logger.info(f"✓ HTML crawl with custom selectors succeeded for {self.source.name}: {len(articles)} articles")
                return articles

        # Method 4: Try HTML scraping with generic/default selectors
        articles = await self._try_html_crawl(max_articles, use_defaults=True)
        if articles:
            logger.info(f"✓ HTML crawl with default selectors succeeded for {self.source.name}: {len(articles)} articles")
            return articles

        # All methods failed
        logger.error(
            f"✗ All crawl methods failed for {self.source.name}. "
            f"Attempts: {', '.join(self.attempts)}"
        )
        return []

    async def _try_rss_crawl(self, max_articles: int) -> List[Article]:
        """Try RSS/Atom feed crawling."""
        try:
            self.attempts.append("RSS")
            crawler = RSSCrawler(self.source)
            articles = await crawler.crawl(max_articles)
            return articles if articles else []
        except Exception as e:
            logger.debug(f"RSS crawl failed for {self.source.name}: {e}")
            return []

    async def _try_auto_detect_feed(self, max_articles: int) -> List[Article]:
        """Auto-detect RSS/Atom feeds from the HTML page."""
        try:
            self.attempts.append("Auto-detect feed")
            import aiohttp
            from bs4 import BeautifulSoup

            # Fetch the main page
            async with aiohttp.ClientSession() as session:
                headers = {
                    "User-Agent": "Mozilla/5.0 (compatible; PreventiveHealthBot/1.0)",
                }
                async with session.get(self.source.url, headers=headers, timeout=30) as response:
                    if response.status != 200:
                        return []

                    html = await response.text()

            soup = BeautifulSoup(html, 'html.parser')

            # Look for RSS/Atom feed links
            feed_links = []

            # Check <link> tags in <head>
            for link in soup.find_all('link', type=['application/rss+xml', 'application/atom+xml']):
                href = link.get('href')
                if href:
                    feed_links.append(href)

            # Check for common feed URLs
            if not feed_links:
                common_paths = ['/feed', '/rss', '/atom', '/feed.xml', '/rss.xml', '/atom.xml']
                for path in common_paths:
                    feed_links.append(self.source.url.rstrip('/') + path)

            # Try each detected feed
            for feed_url in feed_links[:3]:  # Limit attempts
                from urllib.parse import urljoin
                full_feed_url = urljoin(self.source.url, feed_url)

                # Create temporary source with detected feed
                temp_source = type('obj', (object,), {
                    **self.source.__dict__,
                    'rss_url': full_feed_url
                })()

                try:
                    crawler = RSSCrawler(temp_source)
                    articles = await crawler.crawl(max_articles)
                    if articles:
                        logger.info(f"Found working feed at: {full_feed_url}")
                        return articles
                except:
                    continue

            return []

        except Exception as e:
            logger.debug(f"Auto-detect feed failed for {self.source.name}: {e}")
            return []

    async def _try_html_crawl(self, max_articles: int, use_defaults: bool = False) -> List[Article]:
        """Try HTML scraping."""
        try:
            method_name = "HTML (default)" if use_defaults else "HTML (custom)"
            self.attempts.append(method_name)

            if use_defaults:
                # Temporarily remove custom selectors to force defaults
                original_selectors = self.source.css_selectors
                self.source.css_selectors = None

            crawler = HTMLCrawler(self.source)
            articles = await crawler.crawl(max_articles)

            if use_defaults:
                # Restore original selectors
                self.source.css_selectors = original_selectors

            return articles if articles else []

        except Exception as e:
            logger.debug(f"HTML crawl failed for {self.source.name}: {e}")
            return []

    async def _try_pubmed_api_crawl(self, max_articles: int) -> List[Article]:
        """Try PubMed E-utilities API crawling."""
        try:
            self.attempts.append("PubMed API")
            crawler = PubMedCrawler(self.source)
            articles = await crawler.crawl(max_articles)
            return articles if articles else []
        except Exception as e:
            logger.debug(f"PubMed API crawl failed for {self.source.name}: {e}")
            return []
