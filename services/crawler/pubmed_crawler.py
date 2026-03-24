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
from shared.config import config
from base import BaseCrawler
from pdf_extractor import PDFExtractor

logger = logging.getLogger(__name__)


class PubMedCrawler(BaseCrawler):
    """Crawler for PubMed/PMC via E-utilities API."""
    
    BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    
    def __init__(self, source):
        super().__init__(source)
        self.api_key = os.getenv("PUBMED_API_KEY")
        self.rate_limit = 10 if self.api_key else 3
    
    async def crawl(self, max_articles: int = 50, extract_pdfs: bool = True) -> List[Article]:
        """
        Crawl PubMed for recent articles.

        Uses progressive fallback strategy:
        1. Try last 30 days with open access
        2. If insufficient, try last 90 days with open access
        3. If still insufficient, try last 30 days without OA filter
        """
        articles = []

        # Get search terms from source config
        search_terms = self.source.search_terms or []
        if not search_terms:
            logger.warning(f"No search terms for PubMed source: {self.source.name}")
            return []

        try:
            # Build base query from search terms
            base_query = " OR ".join(search_terms)

            # Strategy 1: Last 30 days with open access filter
            pmids = await self._search_with_strategy(
                base_query,
                days=config.pipeline.pubmed_initial_lookback_days,
                open_access=True,
                max_results=max_articles
            )

            # Strategy 2: If few results, try extended lookback with open access
            if len(pmids) < max_articles // 2:
                logger.info(f"Only {len(pmids)} results found, expanding to {config.pipeline.pubmed_extended_lookback_days} days")
                pmids = await self._search_with_strategy(
                    base_query,
                    days=config.pipeline.pubmed_extended_lookback_days,
                    open_access=True,
                    max_results=max_articles
                )

            # Strategy 3: If still few results, try initial lookback without OA filter
            if len(pmids) < max_articles // 2:
                logger.info(f"Only {len(pmids)} results found, removing OA filter")
                pmids = await self._search_with_strategy(
                    base_query,
                    days=config.pipeline.pubmed_initial_lookback_days,
                    open_access=False,
                    max_results=max_articles
                )

            if not pmids:
                logger.info(f"No PubMed results for: {self.source.name} (query: {base_query})")
                return []

            logger.info(f"Found {len(pmids)} PMIDs for {self.source.name}")

            # Fetch article details
            articles = await self._fetch_details(pmids, extract_pdfs)

            logger.info(f"PubMed crawl found {len(articles)} articles for {self.source.name}")

        except Exception as e:
            logger.error(f"PubMed crawl error for {self.source.name}: {e}")

        return articles

    async def _search_with_strategy(
        self,
        base_query: str,
        days: int,
        open_access: bool,
        max_results: int
    ) -> List[str]:
        """
        Search PubMed with specific strategy.

        Args:
            base_query: Base search query
            days: Number of days to look back
            open_access: Whether to filter for open access only
            max_results: Maximum results to return

        Returns:
            List of PMIDs
        """
        query = base_query

        # Add date filter
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        query += f" AND ({start_date.strftime('%Y/%m/%d')}:{end_date.strftime('%Y/%m/%d')}[Date - Publication])"

        # Add open access filter if requested
        if open_access:
            query += " AND open access[filter]"

        logger.debug(f"PubMed query: {query}")

        # Search for PMIDs
        return await self._search(query, max_results)
    
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
            async with session.get(url, timeout=config.pipeline.pubmed_search_timeout) as response:
                if response.status != 200:
                    logger.error(f"PubMed search failed: {response.status}")
                    return []
                
                data = await response.json()
                
                result = data.get("esearchresult", {})
                return result.get("idlist", [])
    
    async def _fetch_details(self, pmids: List[str], extract_pdfs: bool = True) -> List[Article]:
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
            async with session.get(url, timeout=config.pipeline.pubmed_fetch_timeout) as response:
                if response.status != 200:
                    logger.error(f"PubMed fetch failed: {response.status}")
                    return []

                xml_text = await response.text()

        # Parse XML
        try:
            root = ET.fromstring(xml_text)

            for article_elem in root.findall('.//PubmedArticle'):
                article = await self._parse_article(article_elem, extract_pdfs)
                if article:
                    articles.append(article)

            # Log PDF detection stats
            if extract_pdfs and articles:
                pdfs_found = sum(1 for a in articles if a.has_pdf)
                if pdfs_found > 0:
                    logger.info(f"Found PDFs for {pdfs_found}/{len(articles)} PubMed articles")

        except ET.ParseError as e:
            logger.error(f"Failed to parse PubMed XML: {e}")

        return articles
    
    async def _parse_article(self, article_elem: ET.Element, extract_pdfs: bool = True) -> Optional[Article]:
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
                        month_text = month.text
                        if month_text.isdigit():
                            # Numeric month: "2026-02-07" → matches %Y-%m-%d
                            date_str += f"-{month_text.zfill(2)}"
                            if day is not None and day.text:
                                date_str += f"-{day.text.zfill(2)}"
                        else:
                            # Named month: "2026 Feb 7" → matches %Y %b %d
                            date_str += f" {month_text}"
                            if day is not None and day.text:
                                date_str += f" {day.text}"
                    
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

            # Try to find PDF link (PMC or publisher)
            pdf_url = None
            has_pdf = False
            if extract_pdfs:
                try:
                    # Check for PMC ID (free full text)
                    pmc_id = None
                    for id_elem in article_elem.findall('.//ArticleId'):
                        if id_elem.get('IdType') == 'pmc':
                            pmc_id = id_elem.text
                            break

                    if pmc_id:
                        # PMC articles have free PDFs
                        # Ensure PMC ID has 'PMC' prefix
                        if not pmc_id.startswith('PMC'):
                            pmc_id = f"PMC{pmc_id}"
                        pdf_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}/pdf/"
                        has_pdf = True
                        logger.debug(f"Found PMC PDF: {pdf_url}")
                    else:
                        # Try to find PDF via DOI or PubMed link
                        pdf_url = await PDFExtractor.find_pdf_link(url, doi)
                        has_pdf = pdf_url is not None
                except Exception as e:
                    logger.debug(f"PDF extraction failed for PMID {pmid}: {e}")

            return Article(
                source_id=self.source.source_id,
                source_name=self.source.name,
                url=url,
                title=title,
                authors=authors,
                abstract=abstract[:config.pipeline.abstract_max_length] if abstract else None,
                published_date=pub_date.date() if pub_date else date.today(),
                doi=doi,
                pmid=pmid,
                pdf_url=pdf_url,
                has_pdf=has_pdf,
                keywords=keywords[:config.pipeline.keyword_max_count],
                source_quality=SourceQuality.HIGH,  # PubMed is high quality
                status=ArticleStatus.CRAWLED,
                crawled_at=datetime.utcnow(),
            )
            
        except Exception as e:
            logger.warning(f"Failed to parse PubMed article: {e}")
            return None