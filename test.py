# sd_extract_debug.py
import asyncio, re, json, time, os
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

SD_SEARCH_URL = "https://www.sciencedirect.com/search?qs=fatty+liver&years=2026%2C2025&lastSelectedFacet=accessTypes&articleTypes=FLA&langs=en&subjectAreas=2700%2C3000&accessTypes=openaccess"
OUT_DIR = "sd_debug"

# candidate selectors to find article tiles (tries several, covers different page layouts)
CANDIDATE_SELECTORS = [
    "ol#search-results-list li",
    ".ResultItem",
    ".search-result",
    ".js-result-item",
    "article.result-item",
    "div.ResultList div.ResultItem"
]

DOI_RE = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.I)

async def extract_articles(url, max_items=200):
    os.makedirs(OUT_DIR, exist_ok=True)
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        page = await context.new_page()

        print("goto:", url)
        resp = await page.goto(url, timeout=60000)
        if resp:
            print("response status:", resp.status)

        # wait for network to be idle (up to 30s)
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except PWTimeout:
            print("networkidle not reached within 30s — continuing anyway")

        # short scroll / load more attempts to trigger lazy loading
        for _ in range(6):
            await page.mouse.wheel(0, 4000)
            await asyncio.sleep(0.5)

        # try candidate selectors
        nodes = []
        for sel in CANDIDATE_SELECTORS:
            try:
                nodes = await page.query_selector_all(sel)
                if nodes and len(nodes) > 0:
                    print(f"found {len(nodes)} nodes with selector: {sel}")
                    break
            except Exception:
                continue

        # if nothing found, save debug screenshot + html and try regex extraction
        if not nodes:
            ts = int(time.time())
            ss_path = f"{OUT_DIR}/screenshot_{ts}.png"
            html_path = f"{OUT_DIR}/page_{ts}.html"
            print("No article nodes found. Saving screenshot and page HTML for debugging.")
            try:
                await page.screenshot(path=ss_path, full_page=True)
                content = await page.content()
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(content)
                print("Saved:", ss_path, html_path)
            except Exception as e:
                print("Failed to save debug files:", e)

            # fallback: scan page HTML for DOIs and links containing '/science/article/'
            content = await page.content()
            dois = set(DOI_RE.findall(content))
            links = set(re.findall(r'href="([^"]+)"', content))
            article_links = [l for l in links if "/science/article/" in l or "doi.org" in l]
            print("Found DOIs (regex):", list(dois)[:10])
            print("Found candidate article links (regex):", article_links[:10])

            # normalize links
            normalized = []
            for l in article_links:
                if l.startswith("/"):
                    l = "https://www.sciencedirect.com" + l
                normalized.append(l)
            # produce result items from DOIs & links
            for doi in sorted(dois)[:max_items]:
                results.append({"title": None, "article_url": None, "doi": doi})
            for l in normalized[:max_items]:
                results.append({"title": None, "article_url": l, "doi": (DOI_RE.search(l).group(1) if DOI_RE.search(l) else None)})
            await browser.close()
            return results

        # parse nodes found normally
        for n in nodes[:max_items]:
            try:
                # title tries
                title = None
                for tsel in ["h2", "h3", ".result-item-title", ".title", "a"]:
                    try:
                        el = await n.query_selector(tsel)
                        if el:
                            txt = (await el.inner_text()).strip()
                            if txt:
                                title = txt
                                break
                    except Exception:
                        pass

                # link: try to find first anchor with /science/article or doi.org
                link = None
                anchors = await n.query_selector_all("a")
                for a in anchors:
                    href = await a.get_attribute("href")
                    if not href:
                        continue
                    if "/science/article/" in href or "doi.org" in href or "sciencedirect.com" in href:
                        if href.startswith("/"):
                            href = "https://www.sciencedirect.com" + href
                        link = href
                        break
                    # fallback: any https link
                    if href.startswith("http"):
                        link = href

                # try find DOI text inside node
                doi = None
                txt = await n.inner_text()
                m = DOI_RE.search(txt)
                if m:
                    doi = m.group(1)
                # try DOI from link
                if not doi and link:
                    m2 = DOI_RE.search(link)
                    if m2:
                        doi = m2.group(1)

                results.append({"title": title, "article_url": link, "doi": doi})
            except Exception as e:
                # ignore node parsing errors
                continue

        await browser.close()
    return results

async def main():
    items = await extract_articles(SD_SEARCH_URL, max_items=200)
    print(json.dumps(items, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
