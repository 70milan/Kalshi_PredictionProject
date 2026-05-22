import os
import sys
import time
import argparse
import pandas as pd
import duckdb
from datetime import datetime, timezone
from curl_cffi import requests
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# RUNTIME CONFIGURATION
# ─────────────────────────────────────────────
API_KEY = os.getenv("datadotgov_API_KEY")
if not API_KEY:
    print("[CRITICAL] Missing environment token context: datadotgov_API_KEY could not be found.")
    sys.exit(1)

PROJECT_ROOT = "/app"
# UPDATED: Nesting congress_bills cleanly inside the bronze/news/ directory
BRONZE_DIR   = os.path.join(PROJECT_ROOT, "data", "bronze", "news", "congress_bills")
SEEN_BILLS_FILE = os.path.join(BRONZE_DIR, ".seen_bills")

# Target the 119th Congress (Current 2026 Session)
TARGET_CONGRESS = "119"
BASE_URL = "https://api.congress.gov/v3"

# ─────────────────────────────────────────────
# RUNTIME ARGS
# ─────────────────────────────────────────────
parser = argparse.ArgumentParser(description="PredictIQ — Congress Legislative Ingestion Engine")
parser.add_argument(
    "--daemon",
    action="store_true",
    default=False,
    help="Run in daemon mode (infinite loop with 900s sleep). Default: run once and exit."
)
args = parser.parse_args()

# ─────────────────────────────────────────────
# STATE DEDUP TRACKING
# ─────────────────────────────────────────────

def get_seen_bills():
    """Tracks historical delta state using unique Bill Compound IDs (Type + Number + ActionDate)"""
    if os.path.exists(SEEN_BILLS_FILE):
        with open(SEEN_BILLS_FILE, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def mark_as_seen(bill_ids):
    """Appends processed state vectors to the delta tracking manifest."""
    os.makedirs(BRONZE_DIR, exist_ok=True)
    with open(SEEN_BILLS_FILE, "a", encoding="utf-8") as f:
        for b_id in bill_ids:
            f.write(f"{b_id}\n")

# ─────────────────────────────────────────────
# CORE API INGESTION LAYER
# ─────────────────────────────────────────────

def fetch_recent_legislation():
    """Queries Congress.gov REST endpoints for high-velocity delta changes."""
    endpoint = f"{BASE_URL}/bill/{TARGET_CONGRESS}?format=json&sort=updateDateDesc&limit=50&api_key={API_KEY}"
    print(f"Polling Congress.gov Live API Platform for Congress Session {TARGET_CONGRESS}...")
    
    try:
        response = requests.get(endpoint, impersonate="chrome", timeout=15)
        if response.status_code != 200:
            print(f"   [DEBUG] API Platform Connection Error: Status {response.status_code}")
            return [], []
            
        data = response.json()
    except Exception as e:
        print(f"   [CRITICAL] Endpoint handshake failed completely: {e}")
        return [], []
        
    bills = data.get("bills", [])
    print(f"   [DEBUG] Retrieved {len(bills)} recent bill metadata footprints.")
    
    seen_state = get_seen_bills()
    new_records = []
    new_ids = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    
    for bill in bills:
        bill_type = bill.get("type", "UNKNOWN").lower()
        bill_num  = bill.get("number", "0")
        
        # Defensive lookup block to intercept null/None actions safely
        latest_action = bill.get("latestAction")
        if latest_action and isinstance(latest_action, dict):
            action_dt = latest_action.get("actionDate", "0000-00-00")
            latest_action_text = latest_action.get("text", "")
        else:
            action_dt = "0000-00-00"
            latest_action_text = ""
        
        compound_id = f"{TARGET_CONGRESS}_{bill_type}_{bill_num}_{action_dt}"
        
        if compound_id in seen_state:
            continue
            
        title = bill.get("title", "")
        url_pointer = bill.get("url", "")
        
        print(f"New Status Shift Captured -> [{bill_type.upper()} {bill_num}]: {title[:60]}...")
        
        new_records.append({
            "source": "Congress.gov",
            "feed_key": "congress_bills",
            "congress": TARGET_CONGRESS,
            "bill_type": bill_type,
            "bill_number": bill_num,
            "title": title,
            "latest_action_date": action_dt,
            "latest_action_text": latest_action_text,
            "api_url": url_pointer,
            "ingested_at": ingested_at
        })
        new_ids.append(compound_id)
        
    return new_records, new_ids

# ─────────────────────────────────────────────
# STORAGE LAYER (BRONZE MEDALLION LAYER)
# ─────────────────────────────────────────────

def save_to_bronze(items):
    """Commits unique state snapshots to partition layers via DuckDB."""
    if not items:
        print("Delta snapshot match. Legislative state lake is up to date.")
        return False
        
    os.makedirs(BRONZE_DIR, exist_ok=True)
    df = pd.DataFrame(items)
    
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(BRONZE_DIR, f"congress_{ts}.parquet").replace("\\", "/")
    latest_path = os.path.join(BRONZE_DIR, "latest.parquet").replace("\\", "/")
    
    if os.path.exists(latest_path):
        try:
            os.remove(latest_path)
        except Exception as e:
            print(f"   [WARNING] Lock retention error on target path: {e}")
            
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
# DAEMON POLLING RUNTIME
# ─────────────────────────────────────────────

def main():
    print("=" * 65)
    print(f"PredictIQ — Legislative Ticker Engine: Congress.gov")
    print(f"Execution Window : {datetime.now(timezone.utc).isoformat()}")
    print("=" * 65)

    items, ids = fetch_recent_legislation()
    if items:
        if save_to_bronze(items):
            mark_as_seen(ids)
            print(f"Successfully processed {len(items)} legislative updates.")
    else:
        print("Delta snapshot match. Legislative state lake is up to date.")
    print("=" * 65)

if __name__ == "__main__":
    mode = "Daemon" if args.daemon else "Batch"
    print(f"Initializing Congress Ingestion Engine [{mode}]...")
    
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