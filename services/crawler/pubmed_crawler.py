"""
PubMed/PMC API crawler.
"""
import logging
import os
from typing import List, Optional
from datetime import datetime, date, timedelta
from urllib.parse import urlencode
import xml.etree.ElementTree as ET
import aiohttp

import sys
sys.path.insert(0, '/app')

from shared.models import Article, Author, SourceQuality, ArticleStatus
from shared.utils import clean_text, parse_date_string, extract_pmid
from .base import BaseCrawler

logger = logging.getLogger(__name__)


class PubMedCrawler(BaseCrawler):
    """Crawler for PubMed/PMC via E-utilities API."""
    
    BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    
    def __init__(self, source):
        super().__init__(source)
        self.api_key = os.getenv("PUBMED_API_KEY")
        self.rate_limit = 10 if self.api_key else 3
    
    async def crawl(self, max_articles: int = 50) -> List[Article]:
        """Crawl PubMed for recent articles."""
        articles = []
        
        # Get search terms from source config
        search_terms = self.source.search_terms or []
        if not search_terms:
            # Extract from URL if not explicitly set
            logger.warning(f"No search terms for PubMed source: {self.source.name}")
            return []
        
        try:
            # Build search query
            query = " OR ".join(search_terms)
            
            # Add date filter (last 7 days)
            end_date = date.today()
            start_date = end_date - timedelta(days=7)
            query += f" AND ({start_date.strftime('%Y/%m/%d')}:{end_date.strftime('%Y/%m/%d')}[Date - Publication])"
            
            # Add open access filter
            query += " AND open access[filter]"
            
            # Search for PMIDs
            pmids = await self._search(query, max_articles)
            
            if not pmids:
                logger.info(f"No PubMed results for: {self.source.name}")
                return []
            
            # Fetch article details
            articles = await self._fetch_details(pmids)
            
            logger.info(f"PubMed crawl found {len(articles)} articles for {self.source.name}")
            
        except Exception as e:
            logger.error(f"PubMed crawl error for {self.source.name}: {e}")
        
        return articles
    
    async def _search(self, query: str, max_results: int) -> List[str]:
        """Search PubMed and return PMIDs."""
        params = {
            "db": "pubmed",
            "term": query,
            "retmax": max_results,
            "retmode": "json",
            "sort": "date",
            "usehistory": "n",
        }
        
        if self.api_key:
            params["api_key"] = self.api_key
        
        url = f"{self.BASE_URL}/esearch.fcgi?{urlencode(params)}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30) as response:
                if response.status != 200:
                    logger.error(f"PubMed search failed: {response.status}")
                    return []
                
                data = await response.json()
                
                result = data.get("esearchresult", {})
                return result.get("idlist", [])
    
    async def _fetch_details(self, pmids: List[str]) -> List[Article]:
        """Fetch article details for list of PMIDs."""
        if not pmids:
            return []
        
        params = {
            "db": "pubmed",
            "id": ",".join(pmids),
            "retmode": "xml",
            "rettype": "abstract",
        }
        
        if self.api_key:
            params["api_key"] = self.api_key
        
        url = f"{self.BASE_URL}/efetch.fcgi?{urlencode(params)}"
        
        articles = []
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=60) as response:
                if response.status != 200:
                    logger.error(f"PubMed fetch failed: {response.status}")
                    return []
                
                xml_text = await response.text()
        
        # Parse XML
        try:
            root = ET.fromstring(xml_text)
            
            for article_elem in root.findall('.//PubmedArticle'):
                article = self._parse_article(article_elem)
                if article:
                    articles.append(article)
                    
        except ET.ParseError as e:
            logger.error(f"Failed to parse PubMed XML: {e}")
        
        return articles
    
    def _parse_article(self, article_elem: ET.Element) -> Optional[Article]:
        """Parse a single PubMed article."""
        try:
            medline = article_elem.find('.//MedlineCitation')
            if medline is None:
                return None
            
            # PMID
            pmid_elem = medline.find('.//PMID')
            pmid = pmid_elem.text if pmid_elem is not None else None
            
            if not pmid:
                return None
            
            article_data = medline.find('.//Article')
            if article_data is None:
                return None
            
            # Title
            title_elem = article_data.find('.//ArticleTitle')
            title = clean_text(title_elem.text) if title_elem is not None and title_elem.text else ""
            
            if not title:
                return None
            
            # Abstract
            abstract = ""
            abstract_elem = article_data.find('.//Abstract')
            if abstract_elem is not None:
                abstract_texts = []
                for text_elem in abstract_elem.findall('.//AbstractText'):
                    if text_elem.text:
                        label = text_elem.get('Label', '')
                        text = clean_text(text_elem.text)
                        if label:
                            abstract_texts.append(f"{label}: {text}")
                        else:
                            abstract_texts.append(text)
                abstract = " ".join(abstract_texts)
            
            # Authors
            authors = []
            for author in article_data.findall('.//Author'):
                last_name = author.find('LastName')
                first_name = author.find('ForeName')
                affil = author.find('.//Affiliation')
                
                name_parts = []
                if first_name is not None and first_name.text:
                    name_parts.append(first_name.text)
                if last_name is not None and last_name.text:
                    name_parts.append(last_name.text)
                
                if name_parts:
                    authors.append(Author(
                        name=" ".join(name_parts),
                        affiliation=affil.text if affil is not None else None,
                    ))
            
            # Publication date
            pub_date = None
            pub_date_elem = article_data.find('.//PubDate')
            if pub_date_elem is not None:
                year = pub_date_elem.find('Year')
                month = pub_date_elem.find('Month')
                day = pub_date_elem.find('Day')
                
                if year is not None and year.text:
                    date_str = year.text
                    if month is not None and month.text:
                        # Handle month names
                        month_text = month.text
                        if month_text.isdigit():
                            date_str += f"-{month_text.zfill(2)}"
                        else:
                            date_str += f" {month_text}"
                        if day is not None and day.text:
                            date_str += f"-{day.text.zfill(2)}"
                    
                    pub_date = parse_date_string(date_str)
            
            # Journal
            journal_elem = article_data.find('.//Journal/Title')
            journal = journal_elem.text if journal_elem is not None else None
            
            # DOI
            doi = None
            for id_elem in article_elem.findall('.//ArticleId'):
                if id_elem.get('IdType') == 'doi':
                    doi = id_elem.text
                    break
            
            # Keywords (MeSH terms)
            keywords = []
            for mesh in medline.findall('.//MeshHeading/DescriptorName'):
                if mesh.text:
                    keywords.append(mesh.text)
            
            # Build URL
            url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
            
            return Article(
                source_id=self.source.source_id,
                source_name=self.source.name,
                url=url,
                title=title,
                authors=authors,
                abstract=abstract[:5000] if abstract else None,
                published_date=pub_date.date() if pub_date else date.today(),
                doi=doi,
                pmid=pmid,
                keywords=keywords[:20],  # Limit keywords
                source_quality=SourceQuality.HIGH,  # PubMed is high quality
                status=ArticleStatus.CRAWLED,
                crawled_at=datetime.utcnow(),
            )
            
        except Exception as e:
            logger.warning(f"Failed to parse PubMed article: {e}")
            return None