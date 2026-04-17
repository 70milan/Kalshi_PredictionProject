import os
import requests
import time
import base64
import duckdb
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization

# ─────────────────────────────────────────────
# DAILY INCREMENTAL SETTLEMENT SWEEPER
# Run once per day (e.g. 11:59 PM via Prefect).
# Fetches markets that settled IN THE LAST 24 HOURS
# and appends them to the settled/ Bronze folder.
#
# Fills the CDC gap left by the one-time historical
# backfill — any market that was open today and settled
# today will be captured by this script.
# ─────────────────────────────────────────────

load_dotenv()

API_KEY    = os.getenv("KALSHI_API_KEY")
API_SECRET = os.getenv("KALSHI_API_SECRET").replace("\\n", "\n")
BASE_URL   = "https://api.elections.kalshi.com"
MARKETS_ENDPOINT = "/trade-api/v2/markets"
SERIES_ENDPOINT  = "/trade-api/v2/series"

# Docker: /app is the project root (volume-mounted)
PROJECT_ROOT = "/app"
SETTLED_DIR  = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "settled")
CLOSED_DIR   = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "closed")

# Only pull from political series
TARGET_CATEGORIES = {"Politics"}

# Text exclusions — catch mislabeled political series
TEXT_EXCLUDE = [
    "sports", "nba", "nfl", "mlb", "nhl", "nascar", "ufc",
    "soccer", "golf", "tennis", "basketball", "football",
    "grammy", "oscar", "emmy", "bafta", "golden globe",
    "game awards", "box office", "album", "skater",
    "frisbee", "champions league", "ballon d'or",
    "snow", "weather", "temperature", "rainfall",
]

MAX_PAGES  = 500   # 500 × 200 = 100,000 markets cap per status
PAGE_SIZE  = 200


# ─────────────────────────────────────────────
# RSA AUTH
# ─────────────────────────────────────────────

def build_headers(method: str, path: str) -> dict:
    timestamp   = str(int(time.time() * 1000))
    message     = timestamp + method + path
    private_key = serialization.load_pem_private_key(API_SECRET.encode(), password=None)
    signature   = private_key.sign(message.encode(), padding.PKCS1v15(), hashes.SHA256())

    return {
        "KALSHI-ACCESS-KEY":       API_KEY,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "Content-Type":            "application/json",
    }


def get(path: str, params: dict = None, max_retries: int = 5) -> dict:
    for attempt in range(max_retries):
        try:
            response = requests.get(
                f"{BASE_URL}{path}",
                headers=build_headers("GET", path),
                params=params or {},
                timeout=10,
            )

            if response.status_code == 429:
                wait_time = (2 ** attempt) * 10
                print(f"    Rate limited (429). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"    Request failed: {e}")
            if attempt == max_retries - 1:
                return {}
            time.sleep(5)

    return {}


# ─────────────────────────────────────────────
# STEP 1 — GET POLITICAL SERIES
# ─────────────────────────────────────────────

def fetch_political_series() -> list:
    """Fetch series that are open or settled and return only political series tickers."""
    political = []
    
    # Check both statuses to catch markets finalizing in active series
    for status in ["open", "settled"]:
        data = get(SERIES_ENDPOINT, params={"status": status})
        series = data.get("series", [])
        
        for s in series:
            category = (s.get("category", "") or "").strip()
            if category in TARGET_CATEGORIES:
                political.append(s["ticker"])
                
    # Unique tickers only
    political = list(set(political))
    print(f"   Relevant (open/settled) political series found: {len(political)}")
    return political


# ─────────────────────────────────────────────
# STEP 2 — FETCH MARKETS PER SERIES
# ─────────────────────────────────────────────

def fetch_status_markets(series_tickers: list, status: str, cutoff_ts: str) -> list:
    """Fetch markets by status for each series, filtering by recency."""
    print(f"\n   Fetching {status} markets per series...")
    ingested_at = datetime.now(timezone.utc).isoformat()
    filtered = []
    
    for i, st in enumerate(series_tickers, 1):
        cursor = None
        page = 0
        while True:
            page += 1
            if page > 10: 
                break
                
            params = {"limit": 200, "status": status, "series_ticker": st}
            if cursor: 
                params["cursor"] = cursor
                
            data = get(MARKETS_ENDPOINT, params)
            time.sleep(0.2)  # Smooth out 1,800 series requests
            if not data:
                break
                
            batch = data.get("markets", [])
            if not batch: 
                break
                
            for m in batch:
                title = (m.get("title", "") or "").lower()
                if any(ex in title for ex in TEXT_EXCLUDE): 
                    continue
                    
                # Recency check
                ts_str = m.get("settlement_ts") or m.get("close_time") or ""
                if ts_str and ts_str < cutoff_ts: 
                    continue
                    
                m["series_ticker"] = st
                m["ingested_at"]   = ingested_at
                m["status_pulled"] = status
                filtered.append(m)
                
            cursor = data.get("cursor")
            if not cursor: 
                break
                
            time.sleep(0.3)
            
        if i % 200 == 0:
            print(f"   [{status}] Checked {i}/{len(series_tickers)} series... ({len(filtered)} found so far)")
            
    print(f"   [{status}] Completed. Total {status} markets found: {len(filtered)}")
    return filtered


# ─────────────────────────────────────────────
# FLATTEN
# ─────────────────────────────────────────────

def flatten_market(m: dict) -> dict:
    """Serialize nested dicts/lists — Parquet can't handle them."""
    return {
        k: (str(v) if isinstance(v, (dict, list)) else v)
        for k, v in m.items()
    }


# ─────────────────────────────────────────────
# SAVE — APPEND TO EXISTING SETTLED/CLOSED FOLDERS
# ─────────────────────────────────────────────

def save_incremental(markets: list, output_dir: str, status: str) -> str:
    """
    Append today's settlements to the existing Bronze folder.
    Each daily run creates a new timestamped file — never overwrites.
    The DuckDB wildcard view (*.parquet) picks it up automatically.
    """
    if not markets:
        print(f"   No new [{status}] markets to save.")
        return ""

    os.makedirs(output_dir, exist_ok=True)

    flat = [flatten_market(m) for m in markets]
    df   = pd.DataFrame(flat)

    ts       = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(output_dir, f"daily_{status}_{ts}.parquet").replace("\\", "/")

    conn = duckdb.connect()
    conn.execute(f"COPY (SELECT * FROM df) TO '{filepath}' (FORMAT 'PARQUET')")
    conn.close()

    print(f"\n   Saved [{status}]:")
    print(f"   Path    -> {filepath}")
    print(f"   Rows    -> {len(flat)}")
    print(f"   Columns -> {len(df.columns)}")

    return filepath


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    run_ts = datetime.now(timezone.utc)
    cutoff = (run_ts - timedelta(hours=24)).isoformat()

    print("=" * 65)
    print("PredictIQ - Kalshi Daily Settlement Sweeper  [BULK v2]")
    print(f"Run time  : {run_ts.isoformat()}")
    print(f"Cutoff    : {cutoff}  (last 24 hours)")
    print(f"Settled   : {SETTLED_DIR}")
    print(f"Closed    : {CLOSED_DIR}")
    print("=" * 65)

    # 1. Get political series (single API call)
    print("\n Fetching political series list...")
    series_tickers = fetch_political_series()

    # 2. Fetch ALL closed and settled markets in two clean passes
    # (No double loop — fetch_status_markets handles all tickers internally)
    print("\n   Fetching finalized markets per series...")
    all_closed  = fetch_status_markets(series_tickers, "closed",  cutoff)
    all_settled = fetch_status_markets(series_tickers, "settled", cutoff)

    total = len(all_closed) + len(all_settled)
    print(f"\n   Completed. Found {len(all_closed)} closed and {len(all_settled)} settled markets.")

    # 3. Save incrementally (appends to Bronze, never overwrites)
    save_incremental(all_settled, SETTLED_DIR, "settled")
    save_incremental(all_closed,  CLOSED_DIR,  "closed")

    # 4. Summary
    print(f"\n{'='*65}")
    if total == 0:
        print("   Nothing new settled in the last 24 hours. Nothing to save.")
    else:
        print(f"   Daily settlement sweep complete.")
        print(f"   Closed  : {len(all_closed):>5} new markets")
        print(f"   Settled : {len(all_settled):>5} new markets")
        print(f"   Total   : {total:>5}")
        print(f"\n   These will automatically appear in your DuckDB views:")
        print(f"   - kalshi_closed  (via closed/*.parquet)")
        print(f"   - kalshi_settled (via settled/*.parquet)")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()
