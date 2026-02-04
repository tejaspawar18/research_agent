"""
Extraction Service - Full text extraction for articles.
"""
import logging
import os
from typing import Optional
from contextlib import asynccontextmanager

import aiohttp
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl

from pmc_extractor import PMCExtractor
from pdf_extractor import PDFExtractor
from html_extractor import HTMLExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Extraction service started")
    yield
    logger.info("Extraction service stopped")


app = FastAPI(
    title="Extraction Service",
    description="Full text extraction for research articles",
    version="1.0.0",
    lifespan=lifespan,
)


# Request/Response models
class ExtractionRequest(BaseModel):
    """Request to extract full text."""
    url: str
    pmc_id: Optional[str] = None
    pmid: Optional[str] = None
    doi: Optional[str] = None
    pdf_url: Optional[str] = None
    method: Optional[str] = None  # 'auto', 'pmc', 'pdf', 'html', 'unpaywall'


class ExtractionResponse(BaseModel):
    """Response from extraction."""
    url: str
    method_used: Optional[str] = None
    full_text: Optional[str] = None
    published_date: Optional[str] = None
    char_count: int = 0
    success: bool = False
    error: Optional[str] = None


async def _unpaywall_lookup(doi: str) -> Optional[str]:
    """
    Use Unpaywall API to find open access version of a paper.
    Returns the best open access URL if available.
    """
    if not doi:
        return None

    email = os.getenv("UNPAYWALL_EMAIL", "research@preventivehealth.dev")
    api_url = f"https://api.unpaywall.org/v2/{doi}?email={email}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status != 200:
                    logger.debug(f"Unpaywall lookup failed: {response.status} for DOI {doi}")
                    return None

                data = await response.json()

                # Check if open access
                if not data.get("is_oa"):
                    logger.debug(f"Unpaywall: DOI {doi} is not open access")
                    return None

                # Try best OA location first
                best_oa = data.get("best_oa_location")
                if best_oa:
                    oa_url = best_oa.get("url_for_landing_page") or best_oa.get("url")
                    if oa_url:
                        logger.info(f"Unpaywall found OA version: {oa_url}")
                        return oa_url

                # Try other OA locations
                for location in data.get("oa_locations", []):
                    oa_url = location.get("url_for_landing_page") or location.get("url")
                    if oa_url:
                        logger.info(f"Unpaywall found OA location: {oa_url}")
                        return oa_url

    except Exception as e:
        logger.debug(f"Unpaywall error for DOI {doi}: {e}")

    return None


@app.post("/extract", response_model=ExtractionResponse)
async def extract_fulltext(request: ExtractionRequest):
    """
    Extract full text from an article.

    Methods tried in order (if method='auto'):
    1. PMC API (if pmc_id provided)
    2. PDF extraction (if pdf_url provided)
    3. HTML extraction from original URL
    4. Unpaywall lookup for open access version (if DOI provided)
    5. HTML extraction from Unpaywall OA URL

    You can force a specific method by setting the method parameter.
    """
    full_text = None
    method_used = None
    error_msg = None
    published_date = None

    try:
        # Get NCBI API key from environment
        api_key = os.getenv("PUBMED_API_KEY")

        # Force specific method if requested
        if request.method and request.method != 'auto':
            if request.method == 'pmc' and request.pmc_id:
                full_text = await PMCExtractor.extract(request.pmc_id, api_key)
                method_used = 'pmc'
            elif request.method == 'pdf' and request.pdf_url:
                full_text = await PDFExtractor.extract(request.pdf_url)
                method_used = 'pdf'
            elif request.method == 'html':
                full_text, published_date = await HTMLExtractor.extract_with_date(request.url)
                method_used = 'html'
            elif request.method == 'unpaywall' and request.doi:
                oa_url = await _unpaywall_lookup(request.doi)
                if oa_url:
                    full_text, published_date = await HTMLExtractor.extract_with_date(oa_url)
                    method_used = 'unpaywall'
            else:
                error_msg = f"Method '{request.method}' not available or missing required parameters"
        else:
            # Try methods in order (auto mode)
            # Method 1: PMC API (best quality)
            if request.pmc_id and not full_text:
                full_text = await PMCExtractor.extract(request.pmc_id, api_key)
                if full_text:
                    method_used = 'pmc'

            # Method 2: PDF extraction
            if request.pdf_url and not full_text:
                full_text = await PDFExtractor.extract(request.pdf_url)
                if full_text:
                    method_used = 'pdf'

            # Method 3: HTML extraction from original URL (also extracts date)
            if not full_text:
                full_text, published_date = await HTMLExtractor.extract_with_date(request.url)
                if full_text:
                    method_used = 'html'

            # Method 4: Unpaywall - find open access version via DOI
            if not full_text and request.doi:
                oa_url = await _unpaywall_lookup(request.doi)
                if oa_url and oa_url != request.url:
                    full_text, pub_date_oa = await HTMLExtractor.extract_with_date(oa_url)
                    if full_text:
                        method_used = 'unpaywall'
                        if not published_date:
                            published_date = pub_date_oa

            # If we got text but no date from non-HTML methods, try extracting date from HTML
            if full_text and not published_date and method_used in ('pmc', 'pdf'):
                _, published_date = await HTMLExtractor.extract_with_date(request.url)

        if not full_text:
            error_msg = error_msg or "Could not extract full text using any available method"

        return ExtractionResponse(
            url=request.url,
            method_used=method_used,
            full_text=full_text,
            published_date=published_date,
            char_count=len(full_text) if full_text else 0,
            success=full_text is not None,
            error=error_msg,
        )

    except Exception as e:
        logger.error(f"Extraction error for {request.url}: {e}")
        return ExtractionResponse(
            url=request.url,
            success=False,
            error=str(e),
        )


@app.post("/extract/pmc")
async def extract_from_pmc(pmc_id: str):
    """Extract full text from PubMed Central."""
    try:
        api_key = os.getenv("PUBMED_API_KEY")
        full_text = await PMCExtractor.extract(pmc_id, api_key)

        if full_text:
            return {
                "pmc_id": pmc_id,
                "full_text": full_text,
                "char_count": len(full_text),
                "success": True,
            }
        else:
            return {
                "pmc_id": pmc_id,
                "success": False,
                "error": "Could not extract text from PMC",
            }

    except Exception as e:
        logger.error(f"PMC extraction error for {pmc_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/extract/pdf")
async def extract_from_pdf(pdf_url: HttpUrl):
    """Extract text from PDF URL."""
    try:
        full_text = await PDFExtractor.extract(str(pdf_url))

        if full_text:
            return {
                "pdf_url": str(pdf_url),
                "full_text": full_text,
                "char_count": len(full_text),
                "success": True,
            }
        else:
            return {
                "pdf_url": str(pdf_url),
                "success": False,
                "error": "Could not extract text from PDF",
            }

    except Exception as e:
        logger.error(f"PDF extraction error for {pdf_url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/extract/html")
async def extract_from_html(url: HttpUrl):
    """Extract text from HTML page."""
    try:
        full_text = await HTMLExtractor.extract(str(url))

        if full_text:
            return {
                "url": str(url),
                "full_text": full_text,
                "char_count": len(full_text),
                "success": True,
            }
        else:
            return {
                "url": str(url),
                "success": False,
                "error": "Could not extract text from HTML",
            }

    except Exception as e:
        logger.error(f"HTML extraction error for {url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "extraction",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
