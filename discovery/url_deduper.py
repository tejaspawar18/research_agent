import hashlib

def normalize_url(url):
    return url.strip().lower().split("?")[0]

def dedupe_urls(url_list):
    seen = set()
    results = []

    for item in url_list:
        norm = normalize_url(item["url"])
        key = hashlib.md5(norm.encode()).hexdigest()

        if key not in seen:
            seen.add(key)
            results.append(item)

    return results
