import os
import feedparser
import trafilatura
import pandas as pd
import duckdb
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

NYT_POLITICS_RSS = "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml"

# Docker: /app is the project root (volume-mounted)
PROJECT_ROOT = "/app"
BRONZE_DIR   = os.path.join(PROJECT_ROOT, "data", "bronze", "nyt")
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
    print(f"📡 Fetching NYT RSS: {NYT_POLITICS_RSS}")
    feed = feedparser.parse(NYT_POLITICS_RSS)
    
    if not hasattr(feed, 'entries'):
        print("❌ Feed retrieval failed (no entries found).")
        return [], []
        
    print(f"   Entries found: {len(feed.entries)}")
    
    seen_urls = get_seen_urls()
    new_items = []
    new_urls  = []
    
    ingested_at = datetime.now(timezone.utc).isoformat()
    
    for entry in feed.entries:
        link = entry.link
        if link in seen_urls:
            continue
            
        print(f"📄 New Article: {entry.title}")
        
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
            print(f"   ⚠️ Scraping failed for {link}: {e}")
            
        # Fallback to summary if scraping fails or is empty
        content = scraped_text if is_scraped else entry.get("summary", "")
        
        new_items.append({
            "source":      "NYT",
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
        print("⏭️  No new articles found.")
        return False
        
    os.makedirs(BRONZE_DIR, exist_ok=True)
    df = pd.DataFrame(items)
    
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(BRONZE_DIR, f"nyt_{ts}.parquet").replace("\\", "/")
    
    conn = duckdb.connect()
    try:
        conn.execute(f"COPY (SELECT * FROM df) TO '{filepath}' (FORMAT 'PARQUET')")
        print(f"💾 Saved {len(items)} articles to {filepath}")
        return True
    except Exception as e:
        print(f"❌ Failed to save to Parquet: {e}")
        return False
    finally:
        conn.close()

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 65)
    print("PredictIQ — NYT RSS Ingestion (Docker)")
    print(f"Run time : {datetime.now(timezone.utc).isoformat()}")
    print("=" * 65)

    items, urls = fetch_and_scrape()
    
    if items:
        if save_to_bronze(items):
            mark_as_seen(urls)
            print(f"✅ Successfully ingested {len(items)} new articles.")
    else:
        print("☕ Everything is up to date.")

    print("=" * 65)

if __name__ == "__main__":
    main()
