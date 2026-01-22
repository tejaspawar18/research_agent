"""
PubMed Central full text extractor.
"""
import logging
import re
from typing import Optional
import aiohttp
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


class PMCExtractor:
    """Extract full text from PubMed Central API."""

    PMC_FETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

    @staticmethod
    async def extract(pmc_id: str, api_key: Optional[str] = None) -> Optional[str]:
        """
        Extract full text from PMC.

        Args:
            pmc_id: PMC ID (with or without 'PMC' prefix)
            api_key: Optional NCBI API key for higher rate limits

        Returns:
            Full text or None
        """
        try:
            # Ensure PMC prefix
            if not pmc_id.startswith('PMC'):
                pmc_id = f"PMC{pmc_id}"

            # Fetch full text XML from PMC
            params = {
                "db": "pmc",
                "id": pmc_id,
                "retmode": "xml",
            }

            if api_key:
                params["api_key"] = api_key

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    PMCExtractor.PMC_FETCH_URL,
                    params=params,
                    timeout=30
                ) as response:
                    if response.status != 200:
                        logger.warning(f"PMC fetch failed: {response.status} for {pmc_id}")
                        return None

                    xml_text = await response.text()

            # Parse XML and extract body text
            root = ET.fromstring(xml_text)

            # Extract article body
            body_sections = []

            # Get abstract
            abstract = root.find('.//abstract')
            if abstract is not None:
                abstract_text = PMCExtractor._extract_text_from_element(abstract)
                if abstract_text:
                    body_sections.append(f"ABSTRACT\n{abstract_text}")

            # Get main body
            body = root.find('.//body')
            if body is not None:
                for sec in body.findall('.//sec'):
                    section_title = sec.find('./title')
                    title_text = section_title.text if section_title is not None and section_title.text else ""

                    section_content = PMCExtractor._extract_text_from_element(sec)

                    if title_text and section_content:
                        body_sections.append(f"\n{title_text.upper()}\n{section_content}")
                    elif section_content:
                        body_sections.append(section_content)

            if body_sections:
                full_text = "\n\n".join(body_sections)
                # Clean up excessive whitespace
                full_text = re.sub(r'\n{3,}', '\n\n', full_text)
                full_text = re.sub(r' {2,}', ' ', full_text)

                logger.info(f"Extracted {len(full_text)} chars from {pmc_id}")
                return full_text.strip()

            return None

        except ET.ParseError as e:
            logger.error(f"PMC XML parse error for {pmc_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"PMC extraction failed for {pmc_id}: {e}")
            return None

    @staticmethod
    def _extract_text_from_element(element: ET.Element) -> str:
        """Extract all text from an XML element and its children."""
        texts = []
        for text in element.itertext():
            if text and text.strip():
                texts.append(text.strip())
        return ' '.join(texts)
