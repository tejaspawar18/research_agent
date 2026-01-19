"""
HTML scraper crawler for sites without RSS/API.
"""
import logging
from typing import List, Optional
from datetime import datetime, date
from urllib.parse import urljoin
import aiohttp
from bs4 import BeautifulSoup

import sys
sys.path.insert(0, '/app')

from shared.models import Article, Author, SourceQuality, ArticleStatus
from shared.utils import clean_text, parse_date_string, extract_doi, get_domain
from .base import BaseCrawler

logger = logging.getLogger(__name__)


class HTMLCrawler(BaseCrawler):
    """Crawler for HTML pages using CSS selectors."""
    
    # Default selectors for common patterns
    DEFAULT_SELECTORS = {
        "article_list": "article, .article, .post, .news-item, .views-row",
        "title": "h1, h2, h3, .title, .headline",
        "link": "a",
        "date": ".date, .published, time, .meta-date",
        "abstract": ".abstract, .summary, .excerpt, .teaser, p",
        "author": ".author, .byline, .meta-author",
    }
    
    async def crawl(self, max_articles: int = 50) -> List[Article]:
        """Crawl HTML page for articles."""
        articles = []
        
        try:
            # Fetch page
            async with aiohttp.ClientSession() as session:
                headers = {
                    "User-Agent": "Mozilla/5.0 (compatible; PreventiveHealthBot/1.0; Research)",
                    "Accept": "text/html,application/xhtml+xml",
                }
                
                async with session.get(
                    self.source.url,
                    headers=headers,
                    timeout=30
                ) as response:
                    if response.status != 200:
                        logger.error(f"HTML crawl failed: {response.status} for {self.source.url}")
                        return []
                    
                    html = await response.text()
            
            # Parse HTML
            soup = BeautifulSoup(html, 'html.parser')
            
            # Get selectors (from config or defaults)
            selectors = self.source.css_selectors or self.DEFAULT_SELECTORS
            
            # Find article elements
            article_selector = selectors.get("article_list", self.DEFAULT_SELECTORS["article_list"])
            article_elements = soup.select(article_selector)
            
            if not article_elements:
                # Try some fallback selectors
                for fallback in ["article", ".post", "li.item", ".news-card", ".article-card"]:
                    article_elements = soup.select(fallback)
                    if article_elements:
                        break
            
            logger.info(f"Found {len(article_elements)} potential articles on {self.source.url}")
            
            # Parse each article
            for elem in article_elements[:max_articles]:
                article = self._parse_element(elem, selectors)
                if article:
                    articles.append(article)
            
            logger.info(f"HTML crawl extracted {len(articles)} articles from {self.source.name}")
            
        except Exception as e:
            logger.error(f"HTML crawl error for {self.source.name}: {e}")
        
        return articles
    
    def _parse_element(self, elem, selectors: dict) -> Optional[Article]:
        """Parse an article element."""
        try:
            base_url = self.source.url
            
            # Extract title
            title_selector = selectors.get("title", self.DEFAULT_SELECTORS["title"])
            title_elem = elem.select_one(title_selector)
            
            if not title_elem:
                return None
            
            title = clean_text(title_elem.get_text())
            if not title or len(title) < 10:
                return None
            
            # Extract link
            link_selector = selectors.get("link", self.DEFAULT_SELECTORS["link"])
            link_elem = elem.select_one(link_selector) or title_elem.find_parent('a')
            
            if link_elem and link_elem.get('href'):
                url = urljoin(base_url, link_elem.get('href'))
            elif title_elem.get('href'):
                url = urljoin(base_url, title_elem.get('href'))
            else:
                return None
            
            # Extract date
            pub_date = None
            date_selector = selectors.get("date", self.DEFAULT_SELECTORS["date"])
            date_elem = elem.select_one(date_selector)
            
            if date_elem:
                date_text = date_elem.get_text() or date_elem.get('datetime', '')
                pub_date = parse_date_string(date_text)
            
            # Extract abstract/summary
            abstract = ""
            abstract_selector = selectors.get("abstract", self.DEFAULT_SELECTORS["abstract"])
            abstract_elem = elem.select_one(abstract_selector)
            
            if abstract_elem:
                abstract = clean_text(abstract_elem.get_text())
            
            # Extract author
            authors = []
            author_selector = selectors.get("author", self.DEFAULT_SELECTORS["author"])
            author_elem = elem.select_one(author_selector)
            
            if author_elem:
                author_name = clean_text(author_elem.get_text())
                if author_name:
                    authors.append(Author(name=author_name))
            
            # Extract DOI if present
            doi = extract_doi(url) or extract_doi(abstract)
            
            return Article(
                source_id=self.source.source_id,
                source_name=self.source.name,
                url=url,
                title=title,
                authors=authors,
                abstract=abstract[:3000] if abstract else None,
                published_date=pub_date.date() if pub_date else date.today(),
                doi=doi,
                source_quality=SourceQuality(self.source.quality_tier),
                status=ArticleStatus.CRAWLED,
                crawled_at=datetime.utcnow(),
            )
            
        except Exception as e:
            logger.warning(f"Failed to parse HTML element: {e}")
            return None