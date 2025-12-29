import re
from bs4 import BeautifulSoup
from core.logger import get_logger

logger = get_logger("metadata_parser")

def extract_metadata_from_html(html):
    """
    Extracts title, authors, abstract heuristically.
    """
    soup = BeautifulSoup(html, "html.parser")

    title = soup.find("h1")
    title = title.get_text(strip=True) if title else None

    authors = [a.get_text(strip=True) for a in soup.find_all("span", class_="author")]

    abstract = None
    for p in soup.find_all("p"):
        if "abstract" in p.get_text(strip=True).lower():
            abstract = p.get_text(" ", strip=True)
            break

    return {
        "title": title,
        "authors": authors,
        "abstract": abstract,
    }
