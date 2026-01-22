"""
PDF detection and extraction for research articles.
"""
import logging
import re
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse
import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class PDFExtractor:
    """
    Detects and extracts PDF links from article pages.

    Handles common patterns:
    - Direct PDF links
    - DOI-based PDF links
    - Publisher-specific PDF patterns (PMC, PubMed, journals)
    """

    # Common PDF link patterns
    PDF_PATTERNS = [
        r'\.pdf$',
        r'/pdf/',
        r'format=pdf',
        r'download.*pdf',
        r'getPDF',
    ]

    # Publisher-specific PDF endpoints
    PUBLISHER_PATTERNS = {
        'nature.com': '/articles/{doi}.pdf',
        'sciencedirect.com': '/science/article/pii/{pii}/pdfft',
        'nih.gov': '/pmc/articles/PMC{pmcid}/pdf/',
        'ncbi.nlm.nih.gov': '/pmc/articles/PMC{pmcid}/pdf/',
        'thelancet.com': '/pdfs/journals/{journal}/{pii}.pdf',
        'cell.com': '/action/showPdf?pii={pii}',
    }

    @staticmethod
    async def find_pdf_link(article_url: str, doi: Optional[str] = None) -> Optional[str]:
        """
        Find PDF download link for an article.

        Args:
            article_url: URL of the article page
            doi: DOI of the article (if available)

        Returns:
            PDF URL if found, None otherwise
        """
        try:
            # Try direct URL patterns first (fast)
            if PDFExtractor._is_pdf_url(article_url):
                return article_url

            # Fetch article page
            async with aiohttp.ClientSession() as session:
                headers = {
                    "User-Agent": "Mozilla/5.0 (compatible; PreventiveHealthBot/1.0; Research)",
                }

                async with session.get(article_url, headers=headers, timeout=30) as response:
                    if response.status != 200:
                        return None

                    # Check if response itself is a PDF
                    content_type = response.headers.get('Content-Type', '')
                    if 'application/pdf' in content_type:
                        return article_url

                    html = await response.text()

            soup = BeautifulSoup(html, 'html.parser')

            # Method 1: Look for explicit PDF links
            pdf_link = PDFExtractor._find_pdf_link_in_html(soup, article_url)
            if pdf_link:
                logger.info(f"Found PDF link via HTML: {pdf_link}")
                return pdf_link

            # Method 2: Try publisher-specific patterns
            domain = urlparse(article_url).netloc
            for publisher_domain, pattern in PDFExtractor.PUBLISHER_PATTERNS.items():
                if publisher_domain in domain:
                    pdf_link = PDFExtractor._try_publisher_pattern(
                        article_url, pattern, soup, doi
                    )
                    if pdf_link:
                        logger.info(f"Found PDF via publisher pattern: {pdf_link}")
                        return pdf_link

            # Method 3: Try DOI-based access
            if doi:
                pdf_link = await PDFExtractor._try_doi_pdf(doi)
                if pdf_link:
                    logger.info(f"Found PDF via DOI: {pdf_link}")
                    return pdf_link

            return None

        except Exception as e:
            logger.warning(f"PDF extraction failed for {article_url}: {e}")
            return None

    @staticmethod
    def _is_pdf_url(url: str) -> bool:
        """Check if URL directly points to a PDF."""
        url_lower = url.lower()
        return any(re.search(pattern, url_lower) for pattern in PDFExtractor.PDF_PATTERNS)

    @staticmethod
    def _find_pdf_link_in_html(soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Find PDF link in HTML content."""
        # Look for links with PDF indicators
        selectors = [
            'a[href*=".pdf"]',
            'a[href*="/pdf/"]',
            'a[href*="download"]',
            'a.pdf-download',
            'a.download-pdf',
            'a[title*="PDF"]',
            'a[aria-label*="PDF"]',
            'link[type="application/pdf"]',
        ]

        for selector in selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href:
                    # Filter out unwanted links
                    if 'supplementary' in href.lower() or 'supporting' in href.lower():
                        continue

                    full_url = urljoin(base_url, href)
                    if PDFExtractor._is_pdf_url(full_url):
                        return full_url

        # Look in meta tags
        meta_pdf = soup.find('meta', attrs={'name': 'citation_pdf_url'})
        if meta_pdf and meta_pdf.get('content'):
            return urljoin(base_url, meta_pdf.get('content'))

        return None

    @staticmethod
    def _try_publisher_pattern(
        article_url: str,
        pattern: str,
        soup: BeautifulSoup,
        doi: Optional[str]
    ) -> Optional[str]:
        """Try publisher-specific PDF URL pattern."""
        try:
            # Extract identifiers from page
            identifiers = {}

            # Try to get DOI
            if doi:
                identifiers['doi'] = doi

            # Try to get PII (Publisher Item Identifier)
            pii_meta = soup.find('meta', attrs={'name': 'citation_pii'})
            if pii_meta:
                identifiers['pii'] = pii_meta.get('content')

            # Try to get PMCID
            pmcid_match = re.search(r'PMC(\d+)', article_url)
            if pmcid_match:
                identifiers['pmcid'] = pmcid_match.group(1)

            # Format pattern with available identifiers
            for key, value in identifiers.items():
                pattern = pattern.replace(f'{{{key}}}', str(value))

            # Check if pattern is fully resolved
            if '{' not in pattern:
                return urljoin(article_url, pattern)

            return None

        except Exception as e:
            logger.debug(f"Publisher pattern failed: {e}")
            return None

    @staticmethod
    async def _try_doi_pdf(doi: str) -> Optional[str]:
        """Try to get PDF via DOI."""
        try:
            # Try unpaywall API (free, legal access)
            unpaywall_url = f"https://api.unpaywall.org/v2/{doi}?email=research@example.com"

            async with aiohttp.ClientSession() as session:
                async with session.get(unpaywall_url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Check for OA PDF location
                        if data.get('is_oa'):
                            oa_locations = data.get('oa_locations', [])
                            for location in oa_locations:
                                pdf_url = location.get('url_for_pdf')
                                if pdf_url:
                                    return pdf_url

            return None

        except Exception as e:
            logger.debug(f"DOI PDF lookup failed: {e}")
            return None

    @staticmethod
    async def download_pdf(pdf_url: str, max_size_mb: int = 50) -> Optional[bytes]:
        """
        Download PDF content.

        Args:
            pdf_url: URL of the PDF
            max_size_mb: Maximum file size in MB

        Returns:
            PDF content as bytes, or None if download fails
        """
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "User-Agent": "Mozilla/5.0 (compatible; PreventiveHealthBot/1.0; Research)",
                }

                async with session.get(pdf_url, headers=headers, timeout=60) as response:
                    if response.status != 200:
                        logger.warning(f"PDF download failed: {response.status}")
                        return None

                    # Check content type
                    content_type = response.headers.get('Content-Type', '')
                    if 'pdf' not in content_type.lower():
                        logger.warning(f"Not a PDF: {content_type}")
                        return None

                    # Check size
                    content_length = response.headers.get('Content-Length')
                    if content_length:
                        size_mb = int(content_length) / (1024 * 1024)
                        if size_mb > max_size_mb:
                            logger.warning(f"PDF too large: {size_mb:.1f}MB")
                            return None

                    # Download content
                    pdf_content = await response.read()
                    logger.info(f"Downloaded PDF: {len(pdf_content) / 1024:.1f}KB")
                    return pdf_content

        except Exception as e:
            logger.error(f"PDF download error: {e}")
            return None
