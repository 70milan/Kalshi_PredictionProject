import os
import sys
import time
import argparse
import feedparser
import trafilatura
import pandas as pd
import duckdb
from datetime import datetime, timezone
# Upgraded network engine to bypass advanced Cloudflare JA3/JA4 fingerprint walls
from curl_cffi import requests

# ─────────────────────────────────────────────
# PRODUCTION FEED DIRECTORY (LEFT / MAINSTREAM)
# ─────────────────────────────────────────────
FEED_REGISTRY = {
    # From original unified configuration
    "politico_picks": {
        "url": "https://www.politico.com/rss/politicopicks.xml",
        "source_name": "Politico"
    },
    "politico_congress": {
        "url": "https://www.politico.com/rss/congress.xml",
        "source_name": "Politico"
    },
    "politico_healthcare": {
        "url": "https://www.politico.com/rss/healthcare.xml",
        "source_name": "Politico"
    },
    "abc_topstories": {
        "url": "https://abcnews.go.com/abcnews/topstories",
        "source_name": "ABC News"
    },
    "abc_politics": {
        "url": "https://abcnews.go.com/abcnews/politicsheadlines",
        "source_name": "ABC News"
    },
    "ap_politics": {
        "url": "https://news.google.com/rss/search?q=when:1d+source:%22Associated+Press%22+politics",
        "source_name": "Associated Press"
    },
    "npr_politics": {
        "url": "https://feeds.npr.org/1014/rss.xml",
        "source_name": "NPR"
    },
    "the_hill": {
        "url": "https://thehill.com/feed/",
        "source_name": "The Hill"
    },
    # Merged from standalone tracking files
    "nyt": {
        "url": "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
        "source_name": "New York Times"
    },
    "bbc": {
        "url": "http://feeds.bbci.co.uk/news/politics/rss.xml",
        "source_name": "BBC News"
    }
}

# ─────────────────────────────────────────────
# CONFIGURATION & RUNTIME ARGS
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
# CONFIGURATION & RUNTIME ARGS
# ─────────────────────────────────────────────
parser = argparse.ArgumentParser(description="PredictIQ — Unified Left Ingestion Engine")
parser.add_argument(
    "--source", 
    type=str, 
    required=True, 
    choices=list(FEED_REGISTRY.keys()),
    help="The target feed key from the left-wing registry."
)
parser.add_argument(
    "--daemon",
    action="store_true",
    default=False,
    help="Run in daemon mode (infinite loop with 900s sleep). Default: run once and exit."
)
args = parser.parse_args()

ACTIVE_KEY = args.source
RSS_URL = FEED_REGISTRY[ACTIVE_KEY]["url"]
SOURCE_NAME = FEED_REGISTRY[ACTIVE_KEY]["source_name"]

PROJECT_ROOT = "/app"
# CHANGED: Added "news" to push paths to /app/data/bronze/news/{medianame}/
BRONZE_DIR   = os.path.join(PROJECT_ROOT, "data", "bronze", "news", ACTIVE_KEY)
SEEN_URLS_FILE = os.path.join(BRONZE_DIR, ".seen_urls")
# ─────────────────────────────────────────────
# DEDUP — SEEN URLS CHECK
# ─────────────────────────────────────────────

def get_seen_urls():
    """Returns a set of all URLs already ingested for this specific feed."""
    if os.path.exists(SEEN_URLS_FILE):
        with open(SEEN_URLS_FILE, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def mark_as_seen(urls):
    """Appends newly discovered URLs to the feed-specific seen log."""
    os.makedirs(BRONZE_DIR, exist_ok=True)
    with open(SEEN_URLS_FILE, "a", encoding="utf-8") as f:
        for url in urls:
            f.write(f"{url}\n")

# ─────────────────────────────────────────────
# PROCESSING & SCRAPING ENGINE
# ─────────────────────────────────────────────

def fetch_and_scrape():
    """Fetches targeted RSS feed via impersonated curl sockets and extracts entries."""
    print(f"Fetching {SOURCE_NAME} Feed via [{ACTIVE_KEY}]: {RSS_URL}")
    
    try:
        response = requests.get(RSS_URL, impersonate="chrome", timeout=15)
        status = response.status_code
        print(f"   [DEBUG] HTTP Status: {status}")
        
        if status != 200:
            print(f"   [DEBUG] Error: Received unexpected status code {status}")
            return [], []
            
        feed = feedparser.parse(response.text)
        
    except Exception as e:
        print(f"   [CRITICAL] Network connection request failed: {e}")
        return [], []
    
    bozo = getattr(feed, 'bozo', False)
    print(f"   [DEBUG] Bozo (Parsing Error): {bozo}")

    if not hasattr(feed, 'entries') or not feed.entries:
        print("   [DEBUG] Error: Feed object contains empty or missing entries token.")
        return [], []
        
    print(f"   [DEBUG] Payload Entry Count: {len(feed.entries)}")
    
    seen_urls = get_seen_urls()
    new_items = []
    new_urls  = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    
    for entry in feed.entries:
        link = entry.link
        if link in seen_urls:
            continue
            
        print(f"New Item Isolated: {entry.title}")
        
        scraped_text = None
        is_scraped = False
        try:
            downloaded = trafilatura.fetch_url(link)
            if downloaded:
                scraped_text = trafilatura.extract(downloaded)
                if scraped_text:
                    is_scraped = True
        except Exception as e:
            print(f"   Scraping hook failed for target link {link}: {e}")
            
        content = scraped_text if is_scraped else entry.get("summary", "")
        
        new_items.append({
            "source":        SOURCE_NAME,
            "feed_key":      ACTIVE_KEY,
            "title":         entry.title,
            "link":          link,
            "published_at":  entry.get("published", ""),
            "summary":       entry.get("summary", ""),
            "full_text":     content,
            "scraped":       is_scraped,
            "ingested_at":   ingested_at
        })
        new_urls.append(link)
        
    return new_items, new_urls

# ─────────────────────────────────────────────
# STORAGE LAYER (BRONZE MEDALLION LAYER)
# ─────────────────────────────────────────────

def save_to_bronze(items):
    """Safely commits new batches to isolated Bronze Parquet files via DuckDB."""
    if not items:
        print(f"No new state changes discovered for {ACTIVE_KEY}.")
        return False
        
    os.makedirs(BRONZE_DIR, exist_ok=True)
    df = pd.DataFrame(items)
    
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(BRONZE_DIR, f"{ACTIVE_KEY}_{ts}.parquet").replace("\\", "/")
    latest_path = os.path.join(BRONZE_DIR, "latest.parquet").replace("\\", "/")
    
    if os.path.exists(latest_path):
        try:
            os.remove(latest_path)
        except Exception as e:
            print(f"   [WARNING] Lock retention error on latest target path: {e}")
            
    conn = duckdb.connect()
    try:
        conn.execute(f"COPY (SELECT * FROM df) TO '{filepath}' (FORMAT 'PARQUET')")
        conn.execute(f"COPY (SELECT * FROM df) TO '{latest_path}' (FORMAT 'PARQUET')")
        print(f"Saved {len(items)} rows to partition {filepath} and latest.parquet")
        return True
    except Exception as e:
        print(f"Pipeline write execution crashed on storage step: {e}")
        return False
    finally:
        conn.close()

# ─────────────────────────────────────────────
# AUTOMATED RUNTIME LOOP
# ─────────────────────────────────────────────

def main():
    print("=" * 65)
    print(f"PredictIQ — Left Stream Engine: {SOURCE_NAME} ({ACTIVE_KEY})")
    print(f"Execution Window : {datetime.now(timezone.utc).isoformat()}")
    print("=" * 65)

    items, urls = fetch_and_scrape()
    if items:
        if save_to_bronze(items):
            mark_as_seen(urls)
            print(f"Successfully processed {len(items)} target partitions.")
    else:
        print("Delta snapshot match. Delta lake state is up to date.")
    print("=" * 65)

if __name__ == "__main__":
    mode = "Daemon" if args.daemon else "Batch"
    print(f"Initializing Left Ingestion Engine [{mode}] — Ingest Key: [{ACTIVE_KEY}]...")
    
    if args.daemon:
        # Daemon mode: infinite loop with 900s sleep
        while True:
            try:
                main()
            except Exception as e:
                print(f"CRITICAL SYSTEM ERROR inside daemon run loop: {e}")
            
            print("Polling frequency delay. Sleeping for 900 seconds...")
            time.sleep(900)
    else:
        # Batch mode: run once and exit
        try:
            main()
        except Exception as e:
            print(f"CRITICAL SYSTEM ERROR: {e}")