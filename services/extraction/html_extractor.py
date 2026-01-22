"""
HTML full text extractor.
"""
import logging
import re
from typing import Optional
import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class HTMLExtractor:
    """Extract article content from HTML pages."""

    @staticmethod
    async def extract(url: str) -> Optional[str]:
        """
        Extract article content from HTML page.

        Uses common article content selectors and readability heuristics.

        Args:
            url: Article URL

        Returns:
            Extracted text or None
        """
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "User-Agent": "Mozilla/5.0 (compatible; PreventiveHealthBot/1.0)",
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "en-US,en;q=0.9",
                }

                async with session.get(url, headers=headers, timeout=30, allow_redirects=True) as response:
                    if response.status != 200:
                        logger.warning(f"HTML fetch failed: {response.status} for {url}")
                        return None

                    html = await response.text()

            if len(html) < 500:
                logger.warning(f"HTML too short: {len(html)} bytes")
                return None

            soup = BeautifulSoup(html, 'html.parser')

            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'form', 'iframe', 'noscript']):
                element.decompose()

            # Try common article content selectors
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
                    logger.debug(f"Found content using selector: {selector}")
                    break

            # If no article container found, try to find main content area
            if not article_content:
                # Look for the largest text block
                text_blocks = []
                for tag in soup.find_all(['div', 'section', 'article']):
                    # Skip likely non-content areas
                    tag_class = ' '.join(tag.get('class', []))
                    tag_id = tag.get('id', '')
                    if any(x in (tag_class + tag_id).lower() for x in ['sidebar', 'widget', 'comment', 'footer', 'header', 'nav']):
                        continue

                    text = tag.get_text(strip=True)
                    if len(text) > 500:  # Minimum meaningful content
                        text_blocks.append((len(text), tag))

                if text_blocks:
                    text_blocks.sort(reverse=True, key=lambda x: x[0])
                    article_content = text_blocks[0][1]
                    logger.debug(f"Found content using largest text block heuristic")

            if article_content:
                # Extract text with structure
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

                    # Skip short fragments and likely UI elements
                    if len(text) < 20:
                        continue
                    if text.lower() in ['share', 'tweet', 'print', 'email', 'save', 'read more']:
                        continue

                    # Add section headers in caps
                    if elem.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
                        sections.append(f"\n{text.upper()}\n")
                    # Handle lists
                    elif elem.name in ['ul', 'ol']:
                        items = elem.find_all('li')
                        if items:
                            list_text = '\n'.join(['• ' + li.get_text(strip=True) for li in items if li.get_text(strip=True)])
                            if list_text:
                                sections.append(list_text)
                    # Regular paragraphs
                    else:
                        sections.append(text)

                if sections:
                    full_text = "\n\n".join(sections)

                    # Clean up
                    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
                    full_text = re.sub(r' {2,}', ' ', full_text)
                    full_text = re.sub(r'\t+', ' ', full_text)

                    # Remove common junk patterns
                    full_text = re.sub(r'Cookie Policy.*?Cookies\.?', '', full_text, flags=re.IGNORECASE | re.DOTALL)
                    full_text = re.sub(r'Newsletter Sign.*?Subscribe', '', full_text, flags=re.IGNORECASE | re.DOTALL)

                    # Only return if we got substantial content
                    if len(full_text) > 500:
                        logger.info(f"Extracted {len(full_text)} chars from HTML")
                        return full_text.strip()
                    else:
                        logger.warning(f"Extracted text too short: {len(full_text)} chars")

            logger.warning(f"No content found in HTML: {url}")
            return None

        except Exception as e:
            logger.error(f"HTML extraction failed for {url}: {e}")
            return None
