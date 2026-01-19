"""
RSS/Atom feed crawler.
"""
import logging
from typing import List, Optional
from datetime import datetime, date
import aiohttp
import feedparser

import sys
sys.path.insert(0, '/app')

from shared.models import Article, Author, SourceQuality, ArticleStatus
from shared.utils import clean_text, parse_date_string, extract_doi, get_domain
from .base import BaseCrawler

logger = logging.getLogger(__name__)


class RSSCrawler(BaseCrawler):
    """Crawler for RSS and Atom feeds."""
    
    async def crawl(self, max_articles: int = 50) -> List[Article]:
        """Crawl RSS/Atom feed."""
        articles = []
        
        feed_url = self.source.rss_url or self.source.url
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(feed_url, timeout=30) as response:
                    if response.status != 200:
                        logger.error(f"RSS feed returned {response.status}: {feed_url}")
                        return []
                    
                    content = await response.text()
            
            # Parse feed
            feed = feedparser.parse(content)
            
            if feed.bozo and not feed.entries:
                logger.error(f"Failed to parse RSS feed: {feed_url}")
                return []
            
            # Process entries
            for entry in feed.entries[:max_articles]:
                article = self._parse_entry(entry)
                if article:
                    articles.append(article)
            
            logger.info(f"RSS crawl found {len(articles)} articles from {self.source.name}")
            
        except Exception as e:
            logger.error(f"RSS crawl error for {self.source.name}: {e}")
        
        return articles
    
    def _parse_entry(self, entry) -> Optional[Article]:
        """Parse a single RSS entry."""
        try:
            # Get URL
            url = entry.get('link', '')
            if not url:
                return None
            
            # Get title
            title = entry.get('title', '')
            if not title:
                return None
            
            title = clean_text(title)
            
            # Get abstract/summary
            abstract = ""
            if entry.get('summary'):
                abstract = clean_text(entry.summary)
            elif entry.get('description'):
                abstract = clean_text(entry.description)
            elif entry.get('content'):
                content = entry.content[0] if isinstance(entry.content, list) else entry.content
                abstract = clean_text(content.get('value', ''))
            
            # Get publication date
            pub_date = None
            for date_field in ['published', 'updated', 'created']:
                if entry.get(date_field):
                    pub_date = parse_date_string(entry[date_field])
                    if pub_date:
                        break
            
            # Get authors
            authors = []
            if entry.get('authors'):
                for author in entry.authors:
                    name = author.get('name', '')
                    if name:
                        authors.append(Author(name=name))
            elif entry.get('author'):
                authors.append(Author(name=entry.author))
            
            # Extract DOI if present
            doi = extract_doi(url) or extract_doi(abstract or '')
            
            # Get keywords/tags
            keywords = []
            if entry.get('tags'):
                keywords = [tag.get('term', '') for tag in entry.tags if tag.get('term')]
            
            # Create article
            return Article(
                source_id=self.source.source_id,
                source_name=self.source.name,
                url=url,
                title=title,
                authors=authors,
                abstract=abstract[:5000] if abstract else None,  # Limit abstract length
                published_date=pub_date.date() if pub_date else date.today(),
                doi=doi,
                keywords=keywords,
                source_quality=SourceQuality(self.source.quality_tier),
                status=ArticleStatus.CRAWLED,
                crawled_at=datetime.utcnow(),
            )
            
        except Exception as e:
            logger.warning(f"Failed to parse RSS entry: {e}")
            return None