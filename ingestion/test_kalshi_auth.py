import time
import requests
import trafilatura
from bs4 import BeautifulSoup

# Assume: feed.entries, seen_urls, new_items, new_urls, ingested_at are already defined

# ─────────────────────────────────────────────
# STEP 0: Pick one article for demonstration
# ─────────────────────────────────────────────

entry = feed.entries[0]  # just take the first article for step-by-step
title = entry.title
raw_html = entry.get("summary", "")
print("STEP 0 - Title and summary loaded")
print("Title:", title)
print("Raw HTML summary:", raw_html[:200], "...")  # print first 200 chars
print("="*50)

# ─────────────────────────────────────────────
# STEP 1: Extract Google URL from summary
# ─────────────────────────────────────────────

soup = BeautifulSoup(raw_html, "html.parser")
a_tag = soup.find("a", href=True)
google_url = a_tag["href"] if a_tag else None
print("STEP 1 - Extracted Google URL:", google_url)
print("="*50)

# ─────────────────────────────────────────────
# STEP 2: Resolve Google URL to real Reuters URL
# ─────────────────────────────────────────────

source_url = None
if google_url:
    try:
        time.sleep(0.5)  # avoid throttling
        resp = requests.get(google_url, allow_redirects=True, timeout=10)
        source_url = resp.url
    except Exception as e:
        print("STEP 2 - Failed to resolve URL:", e)

print("STEP 2 - Resolved Reuters URL:", source_url)
print("="*50)

# ─────────────────────────────────────────────
# STEP 3: Determine target URL for scraping
# ─────────────────────────────────────────────

target_url = source_url if source_url else entry.link
unique_id = source_url if source_url else entry.link
print("STEP 3 - Target URL for scraping:", target_url)
print("STEP 3 - Unique ID for deduplication:", unique_id)
print("="*50)

# ─────────────────────────────────────────────
# STEP 4: Check deduplication
# ─────────────────────────────────────────────

if unique_id in seen_urls:
    print("STEP 4 - Already seen, skipping article")
else:
    print("STEP 4 - Not seen, continuing")
print("="*50)

# ─────────────────────────────────────────────
# STEP 5: Scrape article text
# ─────────────────────────────────────────────

scraped_text = None
is_scraped = False

try:
    downloaded = trafilatura.fetch_url(target_url)
    if downloaded:
        scraped_text = trafilatura.extract(downloaded)
        if scraped_text:
            is_scraped = True
except Exception as e:
    print("STEP 5 - Scraping failed:", e)

print("STEP 5 - Scraped text (first 200 chars):", scraped_text[:200] if scraped_text else "None")
print("STEP 5 - Scraping successful:", is_scraped)
print("="*50)

# ─────────────────────────────────────────────
# STEP 6: Build record
# ─────────────────────────────────────────────

record = {
    "source": "Reuters",
    "title": title,
    "link": entry.link,
    "google_url": google_url,
    "source_url": source_url,
    "domain": source_url.split("/")[2] if source_url else None,
    "published_at": entry.get("published", ""),
    "summary": raw_html,
    "full_text": scraped_text,
    "scraped": is_scraped,
    "ingested_at": ingested_at
}

print("STEP 6 - Record built:")
for k,v in record.items():
    if isinstance(v, str) and len(v)>100:
        print(f"{k}: {v[:100]}...")
    else:
        print(f"{k}: {v}")
print("="*50)

# ─────────────────────────────────────────────
# STEP 7: Append to lists
# ─────────────────────────────────────────────

new_items.append(record)
new_urls.append(unique_id)
print("STEP 7 - Record appended to new_items and new_urls")
print("new_items length:", len(new_items))
print("new_urls length:", len(new_urls))
print("="*50)