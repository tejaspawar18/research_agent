import requests
import os

UNPAYWALL_EMAIL = os.getenv("UNPAYWALL_EMAIL", 'tejas.sanju@inverv.com')

class UnpaywallClient:
    BASE_URL = "https://api.unpaywall.org/v2"

    def get(self, doi: str) -> dict | None:
        if not doi:
            return None

        url = f"{self.BASE_URL}/{doi}"
        params = {"email": UNPAYWALL_EMAIL}

        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            return None

        return r.json()

def resolve_oa(unpaywall_data):
    if not unpaywall_data:
        return None

    if not unpaywall_data.get("is_oa"):
        return None

    loc = unpaywall_data.get("best_oa_location")
    if not loc:
        return None

    pdf = loc.get("url_for_pdf")
    if not pdf:
        return None

    return {
        "pdf_url": pdf,
        "title": unpaywall_data.get("title"),
        "license": loc.get("license"),
        "host": loc.get("host_type"),
        "published_date": unpaywall_data.get("published_date")
    }


# def download_pdf(pdf_url, out_path):
#     headers = {"User-Agent": "research-agent/1.0"}
#     r = requests.get(pdf_url, stream=True, timeout=30)
#     r.raise_for_status()

#     with open(out_path, "wb") as f:
#         for chunk in r.iter_content(8192):
#             f.write(chunk)


if __name__ == "__main__":
    client = UnpaywallClient()
    data = client.get("10.1038/s41586-025-09880-5")
    print(resolve_oa(data)) 
    download_pdf(resolve_oa(data)["pdf_url"], "test.pdf")
