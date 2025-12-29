import requests
from core.logger import get_logger

logger = get_logger("doi_resolver")

def resolve_pdf_from_doi(doi):
    """
    Resolve open-access PDF via Unpaywall.
    """
    url = f"https://api.unpaywall.org/v2/{doi}?email=tejas.sanju@inverv.com"

    try:
        r = requests.get(url, timeout=10)
        data = r.json()

        pdf = data.get("best_oa_location", {}).get("url_for_pdf")
        if pdf:
            logger.info(f"Resolved PDF via DOI → {pdf}")
        return pdf
    except Exception as e:
        logger.error(f"DOI resolve failed: {e}")
        return None
