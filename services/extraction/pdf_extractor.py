"""
PDF full text extractor.
"""
import logging
import re
from typing import Optional
import aiohttp

logger = logging.getLogger(__name__)


class PDFExtractor:
    """Extract text from PDF files."""

    @staticmethod
    async def extract(pdf_url: str, max_size_mb: int = 10) -> Optional[str]:
        """
        Extract text from PDF.

        Args:
            pdf_url: URL to PDF file
            max_size_mb: Maximum file size in MB

        Returns:
            Extracted text or None
        """
        try:
            # Try to import PDF libraries
            try:
                import PyPDF2
                from io import BytesIO
            except ImportError:
                logger.warning("PyPDF2 not installed. Install with: pip install PyPDF2")
                return None

            # Download PDF
            async with aiohttp.ClientSession() as session:
                headers = {
                    "User-Agent": "Mozilla/5.0 (compatible; PreventiveHealthBot/1.0)",
                }
                async with session.get(pdf_url, headers=headers, timeout=60) as response:
                    if response.status != 200:
                        logger.warning(f"PDF download failed: {response.status} for {pdf_url}")
                        return None

                    # Check content type
                    content_type = response.headers.get('Content-Type', '')
                    if 'pdf' not in content_type.lower() and not pdf_url.lower().endswith('.pdf'):
                        logger.warning(f"Not a PDF: {content_type} for {pdf_url}")
                        return None

                    # Check size
                    content_length = response.headers.get('Content-Length')
                    if content_length:
                        size_mb = int(content_length) / (1024 * 1024)
                        if size_mb > max_size_mb:
                            logger.warning(f"PDF too large: {size_mb:.1f}MB for {pdf_url}")
                            return None

                    pdf_data = await response.read()

            # Extract text from PDF
            pdf_file = BytesIO(pdf_data)
            pdf_reader = PyPDF2.PdfReader(pdf_file)

            # Check page count
            num_pages = len(pdf_reader.pages)
            if num_pages == 0:
                logger.warning(f"PDF has no pages: {pdf_url}")
                return None

            if num_pages > 100:
                logger.warning(f"PDF has too many pages ({num_pages}), limiting to first 100")
                num_pages = 100

            text_parts = []
            for page_num in range(num_pages):
                try:
                    page = pdf_reader.pages[page_num]
                    text = page.extract_text()
                    if text and text.strip():
                        text_parts.append(text)
                except Exception as e:
                    logger.debug(f"Failed to extract page {page_num}: {e}")
                    continue

            if text_parts:
                full_text = "\n\n".join(text_parts)

                # Clean up common PDF extraction artifacts
                full_text = re.sub(r'\n{3,}', '\n\n', full_text)
                full_text = re.sub(r' {2,}', ' ', full_text)
                full_text = re.sub(r'(\w)-\n(\w)', r'\1\2', full_text)  # Fix hyphenated words

                # Remove very short lines (likely headers/footers)
                lines = full_text.split('\n')
                filtered_lines = [line for line in lines if len(line.strip()) > 10 or line.strip() == '']
                full_text = '\n'.join(filtered_lines)

                if len(full_text) > 200:  # Minimum meaningful content
                    logger.info(f"Extracted {len(full_text)} chars from PDF ({num_pages} pages)")
                    return full_text.strip()
                else:
                    logger.warning(f"Extracted text too short: {len(full_text)} chars")
                    return None

            logger.warning(f"No text extracted from PDF: {pdf_url}")
            return None

        except Exception as e:
            logger.error(f"PDF extraction failed for {pdf_url}: {e}")
            return None
