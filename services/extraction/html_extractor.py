"""
HTML full text extractor with journal-specific extraction strategies.
"""
import logging
import re
from typing import Optional, List, Tuple
from urllib.parse import urlparse
from datetime import datetime, date
import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


# Journal-specific selectors for extracting content from paywalled sites
# These target publicly visible sections: abstracts, key points, summaries
JOURNAL_SELECTORS = {
    "nature.com": {
        "selectors": [
            "#Abs1",                          # Abstract section
            "div[data-article-body]",         # Article body
            "#abstract",                      # Fallback abstract
            ".c-article-body",                # Nature article body
            ".c-article-section__content",    # Section content
            "section[data-title='Abstract']", # Named section
        ],
        "extra": [
            ".c-article-teaser-text",         # Teaser/summary
            "[data-test='article-title']",    # Title
        ]
    },
    "thelancet.com": {
        "selectors": [
            "#abstracts",                     # Abstract section
            ".abstract",                      # Abstract class
            "#summary",                       # Summary section
            ".article-content",              # Article body
            ".section-paragraph",             # Section paragraphs
            ".article__sections",             # Main sections
        ],
        "extra": [
            ".article-header__title",         # Title
            ".badge--type",                   # Article type
        ]
    },
    "bmj.com": {
        "selectors": [
            ".abstract",                      # Abstract
            "#abstract",                      # Abstract by ID
            ".article-content",              # Full content
            ".boxed-text",                   # Key messages box
            ".intro",                        # Introduction
            "#boxed-text-1",                 # What is already known
        ],
        "extra": [
            ".highwire-cite-title",           # Title
            ".article-type",                  # Type
        ]
    },
    "cell.com": {
        "selectors": [
            "#abstracts",                     # Abstract
            ".abstract",                      # Abstract class
            "#summary",                       # Summary
            ".article-body",                 # Article body
            ".section-paragraph",             # Section paragraphs
            ".hlFld-Abstract",               # Highlight field abstract
        ],
        "extra": [
            ".article-header__title",         # Title
            ".graphical-abstract",            # Graphical abstract
        ]
    },
    "nejm.org": {
        "selectors": [
            "#abstract",                      # Abstract
            ".abstract",                      # Abstract class
            "#article_body",                 # Article body
            ".o-article-body",               # Article body
            ".o-article__body",              # Alt article body
        ],
        "extra": [
            ".m-article-header__title",       # Title
        ]
    },
    "jamanetwork.com": {
        "selectors": [
            "#abstract",                      # Abstract
            ".abstract",                      # Abstract class
            "#article-content",              # Content
            ".article-full-text",            # Full text
        ],
        "extra": [
            ".meta-article-title",            # Title
        ]
    },
    "ahajournals.org": {
        "selectors": [
            ".abstractSection",               # Abstract section
            "#abstract",                      # Abstract
            ".article__body",                # Article body
            ".hlFld-Abstract",               # Abstract field
        ],
        "extra": [
            ".citation__title",               # Title
        ]
    },
    "biomedcentral.com": {
        "selectors": [
            "#Abs1",                          # Abstract
            ".c-article-body",               # Article body
            "#abstract",                      # Abstract fallback
            ".c-article-section__content",   # Section content
        ],
        "extra": [
            ".c-article-title",               # Title
        ]
    },
    "plos.org": {
        "selectors": [
            ".abstract",                      # Abstract
            ".article-content",              # Article body
            "#section1",                     # First section
        ],
        "extra": [
            "#artTitle",                      # Title
        ]
    },
    "frontiersin.org": {
        "selectors": [
            ".JournalAbstract",               # Abstract
            ".article-content",              # Article body
        ],
        "extra": []
    },
    "mdpi.com": {
        "selectors": [
            ".art-abstract",                  # Abstract
            ".html-body",                    # Full body
        ],
        "extra": []
    },
    "wiley.com": {
        "selectors": [
            ".article-section__abstract",     # Abstract
            "#abstract",                      # Abstract
            ".article-section__content",     # Section content
        ],
        "extra": []
    },
    "sciencedirect.com": {
        "selectors": [
            "#abstracts",                     # Abstract
            ".abstract",                      # Abstract class
            "#body",                         # Article body
        ],
        "extra": []
    },
    "springer.com": {
        "selectors": [
            "#Abs1",                          # Abstract
            ".c-article-body",               # Article body
            "#abstract",                      # Fallback
        ],
        "extra": []
    },
    "oup.com": {
        "selectors": [
            ".abstract",                      # Abstract
            "#abstract",                      # Abstract by ID
            ".article-body",                 # Body
        ],
        "extra": []
    },
}

# Browser-like headers to bypass basic bot detection
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


class HTMLExtractor:
    """Extract article content from HTML pages."""

    @staticmethod
    def _get_domain(url: str) -> str:
        """Extract domain from URL for journal-specific handling."""
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Remove www. prefix
        if domain.startswith("www."):
            domain = domain[4:]
        return domain

    @staticmethod
    def _match_journal(domain: str) -> Optional[dict]:
        """Find matching journal selectors for a domain."""
        for journal_domain, config in JOURNAL_SELECTORS.items():
            if journal_domain in domain:
                return config
        return None

    @staticmethod
    async def extract(url: str) -> Optional[str]:
        """
        Extract article content from HTML page.

        Strategy:
        1. Use browser-like headers to bypass bot detection
        2. Try journal-specific selectors for known domains
        3. Fall back to generic article selectors
        4. Fall back to largest text block heuristic
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=BROWSER_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=30),
                    allow_redirects=True,
                ) as response:
                    if response.status != 200:
                        logger.warning(f"HTML fetch failed: {response.status} for {url}")
                        return None

                    html = await response.text()

            if len(html) < 200:
                logger.warning(f"HTML too short: {len(html)} bytes")
                return None

            soup = BeautifulSoup(html, 'html.parser')

            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'footer', 'aside',
                                 'form', 'iframe', 'noscript', 'svg']):
                element.decompose()

            # Determine domain for journal-specific extraction
            domain = HTMLExtractor._get_domain(url)
            journal_config = HTMLExtractor._match_journal(domain)

            # Strategy 1: Journal-specific selectors
            if journal_config:
                result = HTMLExtractor._extract_journal_content(
                    soup, journal_config, domain
                )
                if result:
                    return result

            # Strategy 2: Generic article selectors
            result = HTMLExtractor._extract_generic_content(soup)
            if result:
                return result

            # Strategy 3: Largest text block heuristic
            result = HTMLExtractor._extract_largest_block(soup)
            if result:
                return result

            logger.warning(f"No content found in HTML: {url}")
            return None

        except Exception as e:
            logger.error(f"HTML extraction failed for {url}: {e}")
            return None

    @staticmethod
    async def extract_with_date(url: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract article content and published date from HTML page.

        Returns:
            Tuple of (full_text, published_date_iso_string)
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=BROWSER_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=30),
                    allow_redirects=True,
                ) as response:
                    if response.status != 200:
                        return None, None
                    html = await response.text()

            if len(html) < 200:
                return None, None

            soup = BeautifulSoup(html, 'html.parser')

            # Extract published date before removing elements
            published_date = HTMLExtractor._extract_published_date(soup)

            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'footer', 'aside',
                                 'form', 'iframe', 'noscript', 'svg']):
                element.decompose()

            domain = HTMLExtractor._get_domain(url)
            journal_config = HTMLExtractor._match_journal(domain)

            full_text = None
            if journal_config:
                full_text = HTMLExtractor._extract_journal_content(soup, journal_config, domain)
            if not full_text:
                full_text = HTMLExtractor._extract_generic_content(soup)
            if not full_text:
                full_text = HTMLExtractor._extract_largest_block(soup)

            return full_text, published_date

        except Exception as e:
            logger.error(f"HTML extraction with date failed for {url}: {e}")
            return None, None

    @staticmethod
    def _extract_published_date(soup: BeautifulSoup) -> Optional[str]:
        """
        Extract publication date from HTML meta tags and common elements.

        Checks in order of reliability:
        1. Citation meta tags (citation_publication_date, citation_date)
        2. Dublin Core meta tags (dc.date, dcterms.date)
        3. Open Graph meta tags (article:published_time)
        4. Schema.org meta tags (datePublished)
        5. <time> elements with datetime attribute
        """
        # Meta tag names to check (name or property attribute)
        meta_date_fields = [
            ("name", "citation_publication_date"),
            ("name", "citation_date"),
            ("name", "citation_online_date"),
            ("name", "dc.date"),
            ("name", "dcterms.date"),
            ("name", "DC.date"),
            ("name", "date"),
            ("name", "article:published_time"),
            ("property", "article:published_time"),
            ("property", "og:article:published_time"),
            ("name", "publication_date"),
            ("name", "sailthru.date"),
            ("name", "pubdate"),
        ]

        for attr, value in meta_date_fields:
            meta = soup.find("meta", attrs={attr: value})
            if meta and meta.get("content"):
                parsed = HTMLExtractor._parse_date(meta["content"].strip())
                if parsed:
                    return parsed

        # Check schema.org datePublished in meta tags
        meta = soup.find("meta", attrs={"itemprop": "datePublished"})
        if meta and meta.get("content"):
            parsed = HTMLExtractor._parse_date(meta["content"].strip())
            if parsed:
                return parsed

        # Check <time> elements with datetime attribute
        for time_elem in soup.find_all("time", attrs={"datetime": True}):
            parsed = HTMLExtractor._parse_date(time_elem["datetime"].strip())
            if parsed:
                return parsed

        # Check elements with itemprop="datePublished"
        for elem in soup.find_all(attrs={"itemprop": "datePublished"}):
            date_val = elem.get("content") or elem.get("datetime") or elem.get_text(strip=True)
            if date_val:
                parsed = HTMLExtractor._parse_date(date_val.strip())
                if parsed:
                    return parsed

        return None

    @staticmethod
    def _parse_date(date_str: str) -> Optional[str]:
        """Parse a date string and return ISO format (YYYY-MM-DD) or None."""
        if not date_str:
            return None

        # Try RFC 2822 first
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(date_str)
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

        # Try ISO 8601 with timezone
        try:
            if 'T' in date_str:
                clean = date_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(clean)
                return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%d %B %Y",
            "%d %b %Y",
            "%Y-%m",
            "%B %Y",
            "%Y",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                continue

        return None

    @staticmethod
    def _extract_journal_content(soup: BeautifulSoup, config: dict, domain: str) -> Optional[str]:
        """Extract content using journal-specific selectors."""
        sections = []

        # Try each journal-specific selector
        for selector in config.get("selectors", []):
            try:
                elements = soup.select(selector)
                for elem in elements:
                    text = HTMLExtractor._clean_element_text(elem)
                    if text and len(text) > 50:
                        sections.append(text)
            except Exception:
                continue

        # Also grab extra context (title, type, etc.)
        for selector in config.get("extra", []):
            try:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 5:
                        sections.insert(0, text)
            except Exception:
                continue

        if sections:
            full_text = "\n\n".join(sections)
            full_text = HTMLExtractor._clean_text(full_text)

            if len(full_text) > 200:
                logger.info(f"Extracted {len(full_text)} chars from {domain} (journal-specific)")
                return full_text

        return None

    @staticmethod
    def _extract_generic_content(soup: BeautifulSoup) -> Optional[str]:
        """Extract content using generic article selectors."""
        article_selectors = [
            'article[role="main"]',
            'article.article',
            'article',
            '.article-content',
            '.article-body',
            '.post-content',
            '.entry-content',
            '.main-content',
            '.content-body',
            '[role="main"]',
            'main article',
            'main',
            '#content article',
            '#content',
            '.content',
        ]

        article_content = None
        for selector in article_selectors:
            article_content = soup.select_one(selector)
            if article_content:
                break

        if not article_content:
            return None

        sections = []

        # Get title if present
        title = article_content.find(['h1'])
        if title:
            title_text = title.get_text(strip=True)
            if title_text and len(title_text) < 300:
                sections.append(f"{title_text}\n")

        # Extract paragraphs and headers
        for elem in article_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'ul', 'ol']):
            text = elem.get_text(strip=True)

            if len(text) < 20:
                continue
            if text.lower() in ['share', 'tweet', 'print', 'email', 'save', 'read more']:
                continue

            if elem.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
                sections.append(f"\n{text.upper()}\n")
            elif elem.name in ['ul', 'ol']:
                items = elem.find_all('li')
                if items:
                    list_text = '\n'.join(
                        ['- ' + li.get_text(strip=True) for li in items if li.get_text(strip=True)]
                    )
                    if list_text:
                        sections.append(list_text)
            else:
                sections.append(text)

        if sections:
            full_text = "\n\n".join(sections)
            full_text = HTMLExtractor._clean_text(full_text)

            if len(full_text) > 200:
                logger.info(f"Extracted {len(full_text)} chars from HTML (generic)")
                return full_text

        return None

    @staticmethod
    def _extract_largest_block(soup: BeautifulSoup) -> Optional[str]:
        """Extract content by finding the largest text block."""
        text_blocks = []
        for tag in soup.find_all(['div', 'section', 'article']):
            tag_class = ' '.join(tag.get('class', []))
            tag_id = tag.get('id', '')
            combined = (tag_class + tag_id).lower()

            if any(x in combined for x in [
                'sidebar', 'widget', 'comment', 'footer',
                'header', 'nav', 'menu', 'cookie', 'banner',
                'advertisement', 'social', 'share'
            ]):
                continue

            text = tag.get_text(strip=True)
            if len(text) > 200:
                text_blocks.append((len(text), tag))

        if text_blocks:
            text_blocks.sort(reverse=True, key=lambda x: x[0])
            best_block = text_blocks[0][1]
            text = HTMLExtractor._clean_element_text(best_block)

            if text and len(text) > 200:
                logger.info(f"Extracted {len(text)} chars from HTML (largest block)")
                return text

        return None

    @staticmethod
    def _clean_element_text(elem) -> str:
        """Extract and clean text from a BeautifulSoup element."""
        sections = []

        for child in elem.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'ul', 'ol', 'li', 'div']):
            text = child.get_text(strip=True)
            if not text or len(text) < 15:
                continue
            if text.lower() in ['share', 'tweet', 'print', 'email', 'save',
                                'read more', 'sign in', 'subscribe']:
                continue

            if child.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
                sections.append(f"\n{text.upper()}\n")
            else:
                sections.append(text)

        if not sections:
            # Fallback: just get all text from element
            text = elem.get_text(separator='\n', strip=True)
            if text:
                sections.append(text)

        return "\n\n".join(sections)

    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean extracted text."""
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        text = re.sub(r'\t+', ' ', text)

        # Remove common junk patterns
        text = re.sub(r'Cookie Policy.*?Cookies\.?', '', text,
                       flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'Newsletter Sign.*?Subscribe', '', text,
                       flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'Sign in.*?institution', '', text,
                       flags=re.IGNORECASE | re.DOTALL)

        return text.strip()
