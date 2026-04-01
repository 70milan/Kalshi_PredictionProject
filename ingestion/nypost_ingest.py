import os
import time
import feedparser
import trafilatura
import pandas as pd
import duckdb
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

NY_POST_POLITICS_RSS = "https://nypost.com/politics/feed/"

# Always write to project root — two levels up from script location
PROJECT_ROOT = "/app"
BRONZE_DIR   = os.path.join(PROJECT_ROOT, "data", "bronze", "nypost")
SEEN_URLS_FILE = os.path.join(BRONZE_DIR, ".seen_urls")

# ─────────────────────────────────────────────
# DEDUP — SEEN URLS CHECK
# ─────────────────────────────────────────────

def get_seen_urls():
    """Returns a set of all URLs already ingested."""
    if os.path.exists(SEEN_URLS_FILE):
        with open(SEEN_URLS_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def mark_as_seen(urls):
    """Appends new URLs to the seen_urls file."""
    os.makedirs(BRONZE_DIR, exist_ok=True)
    with open(SEEN_URLS_FILE, "a") as f:
        for url in urls:
            f.write(f"{url}\n")

# ─────────────────────────────────────────────
# PROCESSING
# ─────────────────────────────────────────────

def fetch_and_scrape():
    """Fetches RSS feed and scrapes new articles."""
    print(f"Fetching NY Post RSS: {NY_POST_POLITICS_RSS}")
    feed = feedparser.parse(NY_POST_POLITICS_RSS)
    
    # DEBUG: Check feed status
    status = getattr(feed, 'status', 'N/A')
    bozo = getattr(feed, 'bozo', False)
    print(f"   [DEBUG] HTTP Status: {status}")
    print(f"   [DEBUG] Bozo (Parsing Error): {bozo}")
    if bozo and hasattr(feed, 'bozo_exception'):
        print(f"   [DEBUG] Exception: {feed.bozo_exception}")

    if not hasattr(feed, 'entries'):
        print("   [DEBUG] Feed object has NO 'entries' attribute!")
        return [], []
        
    print(f"   [DEBUG] Entries found: {len(feed.entries)}")
    
    seen_urls = get_seen_urls()
    new_items = []
    new_urls  = []
    
    ingested_at = datetime.now(timezone.utc).isoformat()
    
    for entry in feed.entries:
        link = entry.link
        if link in seen_urls:
            continue
            
        print(f"New Article: {entry.title}")
        
        # Scrape full content
        scraped_text = None
        is_scraped = False
        
        try:
            downloaded = trafilatura.fetch_url(link)
            if downloaded:
                scraped_text = trafilatura.extract(downloaded)
                if scraped_text:
                    is_scraped = True
        except Exception as e:
            print(f"   Scraping failed for {link}: {e}")
            
        # Fallback to summary if scraping fails or is empty
        content = scraped_text if is_scraped else entry.get("summary", "")
        
        new_items.append({
            "source":      "NY Post",
            "title":       entry.title,
            "link":        link,
            "published_at": entry.get("published", ""),
            "summary":     entry.get("summary", ""),
            "full_text":   content,
            "scraped":     is_scraped,
            "ingested_at": ingested_at
        })
        new_urls.append(link)
        
    return new_items, new_urls

def save_to_bronze(items):
    """Saves any new items to Bronze Parquet via DuckDB."""
    if not items:
        print("No new articles found.")
        return False
        
    os.makedirs(BRONZE_DIR, exist_ok=True)
    df = pd.DataFrame(items)
    
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(BRONZE_DIR, f"nypost_{ts}.parquet").replace("\\", "/")
    latest_path = os.path.join(BRONZE_DIR, "latest.parquet").replace("\\", "/")
    
    conn = duckdb.connect()
    try:
        conn.execute(f"COPY (SELECT * FROM df) TO '{filepath}' (FORMAT 'PARQUET')")
        conn.execute(f"COPY (SELECT * FROM df) TO '{latest_path}' (FORMAT 'PARQUET')")
        print(f"Saved {len(items)} articles to {filepath} and latest.parquet")
        return True
    except Exception as e:
        print(f"Failed to save to Parquet: {e}")
        return False
    finally:
        conn.close()

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 65)
    print("PredictIQ — NY Post RSS Ingestion")
    print(f"Run time : {datetime.now(timezone.utc).isoformat()}")
    print("=" * 65)

    items, urls = fetch_and_scrape()
    
    if items:
        if save_to_bronze(items):
            mark_as_seen(urls)
            print(f"Successfully ingested {len(items)} new articles.")
    else:
        print("Everything is up to date.")

    print("=" * 65)

if __name__ == "__main__":
    print(f"Initializing Docker Polling Service ({int(900/60)}-min intervals)...")
    while True:
        try:
            main()
        except Exception as e:
            print(f"CRITICAL ERROR in ingestion loop: {e}")
        
        print("Sleeping for 900 seconds before poll...")
        time.sleep(900)
