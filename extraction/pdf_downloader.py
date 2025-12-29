# extraction/pdf_downloader.py (updated)
import os
import re
import time
import json
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from core.logger import get_logger
from core.http_client import download, get as http_get
from core.playwright_client import PlaywrightClient
from core.utils import generate_uuid, ensure_dir
from extraction.doi_resolver import resolve_pdf_from_doi

logger = get_logger("pdf_downloader")

DEBUG_DIR = ensure_dir("data/debug")

COOKIE_SELECTORS = [
    "button#onetrust-accept-btn-handler",   # OneTrust common
    "button:has-text('Accept')",
    "button:has-text('I agree')",
    "button:has-text('Agree')",
    "button:has-text('Accept all')",
    "button[title*='Accept']",
    "button.cc-btn.cc-accept",               # some cookie libs
]

PDF_LINK_SELECTORS = [
    "a[href$='.pdf']",
    "a:has-text('Download PDF')",
    "a:has-text('Download full text PDF')",
    "a:has-text('Full Text PDF')",
    "a:has-text('PDF')",
    "a.pdf-download",
]

# Common OneTrust cookie value (non-personalized) - may or may not work
OPTANON_DUMMY = "isGpcEnabled=0&datestamp={ts}&consentId={id}&version=6.16.0&hosts=&groups=%3A1%3A1&geolocation=%3B&AwaitingReconsent=false".format

def _save_debug(page, name_prefix="playwright_fail"):
    try:
        ts = int(time.time())
        png = os.path.join(DEBUG_DIR, f"{name_prefix}_{ts}.png")
        htmlp = os.path.join(DEBUG_DIR, f"{name_prefix}_{ts}.html")
        try:
            page.screenshot(path=png, full_page=True)
            logger.info(f"Saved screenshot {png}")
        except Exception as e:
            logger.debug(f"Failed screenshot: {e}")
        try:
            html = page.content()
            with open(htmlp, "w", encoding="utf-8") as f:
                f.write(html)
            logger.info(f"Saved page HTML {htmlp}")
        except Exception as e:
            logger.debug(f"Failed HTML dump: {e}")
    except Exception:
        logger.debug("Debug save failed.")

def _find_pdf_link_in_html(html, base_url):
    try:
        soup = BeautifulSoup(html, "html.parser")
        # try meta tags first (some sites provide pdf in meta)
        for meta in soup.find_all("meta"):
            if meta.get("name", "").lower().startswith("citation_pdf_url"):
                return meta.get("content")
        # then anchors
        for a in soup.find_all("a"):
            href = a.get("href") or ""
            if ".pdf" in href.lower():
                if href.startswith("http"):
                    return href
                return urljoin(base_url, href)
    except Exception:
        pass
    return None

def _extract_doi_from_html_text(html):
    try:
        match = re.search(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", html)
        return match.group(0) if match else None
    except Exception:
        return None

def download_pdf(article_url, selectors=None, outdir="data/temp", headless=True):
    """
    Robust PDF download flow:
    1) direct .pdf
    2) HTML scan (requests)
    3) Playwright (try accept cookies, remove overlays, find hrefs, expect_download)
    4) DOI -> Unpaywall
    5) Crossref abstract fallback (no PDF)
    Returns dict with {pdf_path, fallback_abstract, doi, note}
    """
    os.makedirs(outdir, exist_ok=True)
    logger.info(f"Trying to download PDF → {article_url}")

    result = {"pdf_path": None, "fallback_abstract": None, "doi": None, "note": None}

    # 1) direct link
    if article_url.lower().endswith(".pdf"):
        file_path = os.path.join(outdir, f"{generate_uuid()}.pdf")
        try:
            download(article_url, file_path)
            result["pdf_path"] = file_path
            result["note"] = "direct"
            return result
        except Exception as e:
            logger.warning(f"Direct download failed: {e}")

    # 2) HTML scan using http_get (sets proper UA)
    try:
        r = http_get(article_url)
        html = r.text
        doi = _extract_doi_from_html_text(html)
        if doi:
            result["doi"] = doi

        pdf_link = _find_pdf_link_in_html(html, article_url)
        if pdf_link:
            try:
                file_path = os.path.join(outdir, f"{generate_uuid()}.pdf")
                download(pdf_link, file_path)
                result["pdf_path"] = file_path
                result["note"] = "html_anchor"
                return result
            except Exception as e:
                logger.warning(f"Download from HTML-discovered PDF link failed: {e}")
    except Exception as e:
        logger.debug(f"HTML scan failed: {e}")

    # 3) Playwright robust approach
    try:
        client = PlaywrightClient(headless=headless)
        page, ctx = client.goto(article_url, timeout=45000)

        # attempt to set a OneTrust cookie to pre-accept consent (may or may not help)
        try:
            # craft a simple OptanonConsent-like cookie
            opt_val = f"true;{int(time.time())}"
            ctx.add_cookies([{
                "name": "OptanonConsent",
                "value": str(opt_val),
                "domain": "." + article_url.split("//")[-1].split("/")[0],
                "path": "/",
                "httpOnly": False,
                "secure": True
            }])
        except Exception:
            # ignore domain errors
            pass

        # try multiple cookie selectors
        for sel in COOKIE_SELECTORS:
            try:
                el = page.query_selector(sel)
                if el:
                    logger.info(f"Clicking cookie button selector: {sel}")
                    el.click(timeout=3000)
                    page.wait_for_timeout(600)
            except Exception:
                pass

        # remove visible overlays / dialogs (generic)
        try:
            page.evaluate("""() => {
                const blockers = ['div.cookie', '.cookie-banner', '#cookie-consent', '.cc-window', '.overlay', '.modal', '.consent-banner', '.onetrust-banner']; 
                blockers.forEach(s => { document.querySelectorAll(s).forEach(e => e.remove()); });
                document.querySelectorAll('[role=dialog]').forEach(e => e.remove());
                document.body.style.overflow = 'auto';
            }""")
            # allow DOM to stabilize
            page.wait_for_timeout(800)
        except Exception:
            pass

        # attempt to find pdf via DOM selectors (custom selector preferred)
        selector_list = []
        if selectors and selectors.get("pdf_button"):
            selector_list.append(selectors.get("pdf_button"))
        selector_list += PDF_LINK_SELECTORS

        # Strategy: find href attributes then download via HTTP (avoid expect_download)
        for sel in selector_list:
            if not sel:
                continue
            try:
                loc = page.locator(sel)
                cnt = 0
                try:
                    cnt = loc.count()
                except Exception:
                    cnt = 1
                if cnt:
                    for i in range(cnt):
                        try:
                            href = None
                            try:
                                href = loc.nth(i).get_attribute("href")
                            except Exception:
                                # try attribute via evaluate
                                href = page.evaluate('el => el.getAttribute("href")', loc.nth(i))
                            if href:
                                pdf_url = urljoin(article_url, href)
                                logger.info(f"Found PDF link via selector '{sel}': {pdf_url}")
                                file_path = os.path.join(outdir, f"{generate_uuid()}.pdf")
                                download(pdf_url, file_path)
                                client.close()
                                result["pdf_path"] = file_path
                                result["note"] = f"playwright_href:{sel}"
                                return result
                        except Exception:
                            continue
            except Exception:
                continue

        # fallback: find any anchor with .pdf in DOM
        try:
            anchors = page.query_selector_all("a")
            for a in anchors:
                try:
                    href = a.get_attribute("href")
                    if href and ".pdf" in href.lower():
                        pdf_url = urljoin(article_url, href)
                        logger.info(f"Found PDF anchor: {pdf_url}")
                        file_path = os.path.join(outdir, f"{generate_uuid()}.pdf")
                        download(pdf_url, file_path)
                        client.close()
                        result["pdf_path"] = file_path
                        result["note"] = "playwright_anchor_any"
                        return result
                except Exception:
                    continue
        except Exception:
            pass

        # last resort: try click -> expect_download (may fail behind paywall)
        for sel in selector_list:
            try:
                with page.expect_download(timeout=8000) as dl_info:
                    page.click(sel, timeout=3000)
                download_obj = dl_info.value
                filename = download_obj.suggested_filename or f"{generate_uuid()}.pdf"
                file_path = os.path.join(outdir, filename)
                download_obj.save_as(file_path)
                client.close()
                result["pdf_path"] = file_path
                result["note"] = f"playwright_click:{sel}"
                return result
            except Exception:
                continue

        # save debug screenshot + html if none of the above worked
        _save_debug(page, name_prefix="playwright_nopdf")
        client.close()

    except Exception as e:
        logger.debug(f"Playwright step failed: {e}")

    # 4) DOI -> Unpaywall
    try:
        # re-scan HTML via requests for DOI if not found already
        try:
            r2 = http_get(article_url)
            html2 = r2.text
            doi = _extract_doi_from_html_text(html2)
            if doi:
                result["doi"] = doi
        except Exception:
            pass

        doi = result.get("doi")
        if doi:
            pdf_url = resolve_pdf_from_doi(doi)
            if pdf_url:
                file_path = os.path.join(outdir, f"{generate_uuid()}.pdf")
                download(pdf_url, file_path)
                result["pdf_path"] = file_path
                result["note"] = "unpaywall"
                return result
            else:
                # try Crossref to fetch abstract/title if available
                try:
                    meta = _crossref_metadata(doi)
                    if meta:
                        result["fallback_abstract"] = meta.get("abstract")
                        result["note"] = "crossref_no_pdf"
                        return result
                except Exception:
                    pass
    except Exception:
        pass

    result["note"] = result.get("note") or "no_pdf_found"
    logger.warning(f"No PDF found for: {article_url} (note={result['note']}, doi={result.get('doi')})")
    return result

def _crossref_metadata(doi):
    """
    Fetch metadata from Crossref (title, abstract) by DOI.
    """
    try:
        url = f"https://api.crossref.org/works/{doi}"
        r = requests.get(url, timeout=10, headers={"User-Agent": "ResearchAgent/1.0 (mailto:you@example.com)"})
        r.raise_for_status()
        data = r.json().get("message", {})
        # Crossref abstracts often contain HTML tags; return as-is
        return {
            "title": data.get("title", [None])[0] if data.get("title") else None,
            "abstract": data.get("abstract")
        }
    except Exception as e:
        logger.debug(f"Crossref call failed: {e}")
        return None

