import json
from bs4 import BeautifulSoup
from tenacity import RetryError
from core.logger import get_logger
from core.http_client import get as http_get

logger = get_logger("html_extractor")

# selectors likely to be cookie/paywall overlays (remove them before extraction)
OVERLAY_SELECTORS = [
    "div.cookie", ".cookie-banner", "#cookie-consent", ".cc-window", ".overlay", ".modal",
    ".consent-banner", ".onetrust-banner", "#onetrust-banner-sdk", ".paywall", ".paywall-overlay"
]

def _strip_overlays(soup):
    for sel in OVERLAY_SELECTORS:
        for node in soup.select(sel):
            try:
                node.decompose()
            except Exception:
                pass
    # also remove large dialogs
    for node in soup.select("[role=dialog]"):
        try:
            node.decompose()
        except Exception:
            pass

from core.playwright_client import PlaywrightClient

def extract_html_content(url):
    """
    Extract useful article text from HTML, trying meta tags, schema.org JSON-LD, citation meta tags,
    and finally fallback to paragraph extraction after removing overlays.
    Returns dict: {"text":..., "title":..., "authors":[], "abstract":...}
    """
    pw = None
    try:
        pw = PlaywrightClient()
        page, _ = pw.goto(url)
        html = page.content()
        soup = BeautifulSoup(html, "lxml")

        # Remove overlays that block content
        _strip_overlays(soup)

        # 1) Try schema.org JSON-LD
        jsonld = None
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                j = tag.string
                if not j:
                    continue
                obj = json.loads(j)
                # find article type
                if isinstance(obj, dict) and obj.get("@type") in ("ScholarlyArticle", "Article", "NewsArticle"):
                    jsonld = obj
                    break
                # sometimes it's a list
                if isinstance(obj, list):
                    for o in obj:
                        if isinstance(o, dict) and o.get("@type") in ("ScholarlyArticle", "Article"):
                            jsonld = o
                            break
            except json.JSONDecodeError:
                continue
            except Exception:
                continue
        title = None
        authors = []
        abstract = None
        if jsonld:
            title = jsonld.get("headline") or jsonld.get("name") or title
            if isinstance(jsonld.get("author"), list):
                for a in jsonld.get("author"):
                    if isinstance(a, dict):
                        authors.append(a.get("name"))
                    else:
                        authors.append(str(a))
            elif isinstance(jsonld.get("author"), dict):
                authors.append(jsonld.get("author").get("name"))
            abstract = jsonld.get("description") or jsonld.get("abstract") or abstract

        # 2) Try meta tags: citation_*
        if not title:
            t = soup.find("meta", attrs={"name": "citation_title"}) or soup.find("meta", attrs={"property": "og:title"})
            if t and t.get("content"):
                title = t["content"]
        # authors
        if not authors:
            authors_meta = soup.find_all("meta", attrs={"name": "citation_author"})
            for am in authors_meta:
                if am.get("content"):
                    authors.append(am["content"])
        # abstract
        if not abstract:
            meta_abs = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"name": "dc.Description"})
            if meta_abs and meta_abs.get("content"):
                abstract = meta_abs["content"]

        # 3) Try to find article content in common containers first
        ARTICLE_SELECTORS = [
            "article",
            "main article",
            "[role='main']",
            ".article-body",
            ".article__body",
            ".article-content",
            ".c-article-body",  # Nature
            ".c-article-section__content",  # Nature sections
            "#article-body",
            ".post-content",
            ".entry-content",
            ".content-body",
            ".full-text",
            ".paper-content",
            "section.body",
        ]
        
        article_container = None
        for sel in ARTICLE_SELECTORS:
            container = soup.select_one(sel)
            if container and len(container.get_text(strip=True)) > 200:
                article_container = container
                break
        
        if article_container:
            # Extract paragraphs from article container only
            paragraphs = [p.get_text(" ", strip=True) for p in article_container.find_all("p") if len(p.get_text(strip=True)) > 30]
        else:
            # Fallback: all paragraphs, but filter out likely navigation/footer text
            all_p = soup.find_all("p")
            paragraphs = []
            for p in all_p:
                text_content = p.get_text(" ", strip=True)
                # Skip short paragraphs and common boilerplate
                if len(text_content) < 50:
                    continue
                # Skip cookie/privacy notices
                lower = text_content.lower()
                if any(skip in lower for skip in ["cookie", "privacy", "subscribe", "newsletter", "sign up", "log in"]):
                    continue
                paragraphs.append(text_content)
        
        text = "\n\n".join(paragraphs)

        # If text is empty and abstract exists, use abstract as text
        if not text and abstract:
            text = abstract

        return {"text": text, "title": title, "authors": authors, "abstract": abstract}
    except (Exception, RetryError) as e:
        logger.error(f"HTML extraction failed for {url}: {e}")
        return {"text": None, "title": None, "authors": [], "abstract": None}
    finally:
        if pw:
            pw.close()