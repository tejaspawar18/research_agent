"""
Base crawler class for all source crawlers.
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Any
import logging

import sys
sys.path.insert(0, '/app')

from shared.models import Article

logger = logging.getLogger(__name__)


class BaseCrawler(ABC):
    """Abstract base class for source crawlers."""
    
    def __init__(self, source: Any):
        """
        Initialize crawler with source configuration.
        
        Args:
            source: SourceConfigItem with source details
        """
        self.source = source
    
    @abstractmethod
    async def crawl(self, max_articles: int = 50) -> List[Article]:
        """
        Crawl the source for articles.
        
        Args:
            max_articles: Maximum number of articles to return
        
        Returns:
            List of Article objects
        """
        pass
    
    async def health_check(self) -> bool:
        """Check if source is accessible."""
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(self.source.url, timeout=10) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(f"Health check failed for {self.source.name}: {e}")
            return False