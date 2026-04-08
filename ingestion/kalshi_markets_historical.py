import os
import requests
import time
import base64
import duckdb
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization

# ─────────────────────────────────────────────
# ⚠️  ONE-TIME SCRIPT — RUN ONCE THEN DONE
# Backfills ALL historical closed + settled
# Kalshi political markets into Bronze layer.
# Single loop — fetches both statuses per series
# in one pass to cut run time in half.
# ─────────────────────────────────────────────

load_dotenv()

API_KEY    = os.getenv("KALSHI_API_KEY")
API_SECRET = os.getenv("KALSHI_API_SECRET").replace("\\n", "\n")
BASE_URL   = "https://api.elections.kalshi.com"
MARKETS_ENDPOINT = "/trade-api/v2/markets"
SERIES_ENDPOINT  = "/trade-api/v2/series"

# Docker: /app is the project root (volume-mounted)
PROJECT_ROOT = "/app"
CLOSED_DIR   = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "closed")
SETTLED_DIR  = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "settled")

# Categories to pull
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
                print(f"    ⚠️  Rate limited (429). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"    ❌ Request failed: {e}")
            if attempt == max_retries - 1:
                return {}
            time.sleep(5)
            
    return {}


# ─────────────────────────────────────────────
# STEP 1 — BULK FETCH ALL MARKETS BY STATUS
# O(N_pages) instead of O(N_series)
# ─────────────────────────────────────────────

def fetch_markets_bulk(status: str) -> list:
    """
    Single paginated sweep of GET /markets?status={status}&limit=200.
    No series_ticker filter — fetches everything, then we filter locally.
    """
    all_markets = []
    cursor = None
    page = 0

    while True:
        page += 1
        if page > MAX_PAGES:
            print(f"   ⚠️  Hit page cap ({MAX_PAGES}) for status={status}.")
            break

        params = {"limit": PAGE_SIZE, "status": status}
        if cursor:
            params["cursor"] = cursor

        data = get(MARKETS_ENDPOINT, params)
        batch = data.get("markets", [])

        if not batch:
            break

        all_markets.extend(batch)

        if page % 10 == 0:
            print(f"   [{status}] Page {page}: running total {len(all_markets)}")

        cursor = data.get("cursor")
        if not cursor:
            break

        time.sleep(0.3)

    print(f"   [{status}] Fetched {len(all_markets)} total markets in {page} pages")
    return all_markets


# ─────────────────────────────────────────────
# STEP 2 — BUILD CATEGORY LOOKUP FROM /series
# Single API call → {series_ticker: category}
# ─────────────────────────────────────────────

def build_category_lookup() -> dict:
    """
    One call to /series (no status filter, includes resolved) —
    builds a lookup dict so we can filter bulk markets by category.
    """
    data = get(SERIES_ENDPOINT)
    series = data.get("series", [])
    lookup = {}
    for s in series:
        ticker   = s.get("ticker", "")
        category = (s.get("category", "") or "").strip()
        lookup[ticker] = category
    print(f"   Series lookup built: {len(lookup)} entries")
    return lookup


# ─────────────────────────────────────────────
# STEP 3 — LOCAL FILTER FOR POLITICS
# ─────────────────────────────────────────────

def filter_political(markets: list, category_lookup: dict) -> list:
    """
    Client-side filter: keep only political markets,
    exclude mislabeled sports/weather/entertainment.
    """
    political = []
    for m in markets:
        series_ticker = m.get("series_ticker", "")
        category = (m.get("category", "") or "").strip()
        if not category:
            category = category_lookup.get(series_ticker, "")

        if category not in TARGET_CATEGORIES:
            continue

        title = (m.get("title", "") or "").lower()
        if any(ex in title for ex in TEXT_EXCLUDE):
            continue

        political.append(m)

    return political


# ─────────────────────────────────────────────
# STEP 4 — BULK FETCH BOTH STATUSES
# ─────────────────────────────────────────────

def fetch_all_historical(category_lookup: dict) -> tuple:
    """
    Bulk fetch ALL closed and settled markets, filter locally.
    ~50 API calls total instead of ~3,600 per-series calls.
    Returns two separate lists: (closed_markets, settled_markets)
    """
    ingested_at = datetime.now(timezone.utc).isoformat()

    print(f"\n📡 Bulk-fetching closed markets...")
    raw_closed = fetch_markets_bulk("closed")
    all_closed = filter_political(raw_closed, category_lookup)
    for m in all_closed:
        m["ingested_at"]   = ingested_at
        m["status_pulled"] = "closed"
    print(f"   Closed: {len(raw_closed)} total → {len(all_closed)} political")

    print(f"\n📡 Bulk-fetching settled markets...")
    raw_settled = fetch_markets_bulk("settled")
    all_settled = filter_political(raw_settled, category_lookup)
    for m in all_settled:
        m["ingested_at"]   = ingested_at
        m["status_pulled"] = "settled"
    print(f"   Settled: {len(raw_settled)} total → {len(all_settled)} political")

    return all_closed, all_settled


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
# SAVE
# ─────────────────────────────────────────────

def save_to_bronze(markets: list, output_dir: str, status: str) -> str:
    """
    Save one status worth of historical markets to its own folder.
    Single timestamped file — this is a one-time backfill.
    Folders are created automatically if they don't exist.
    """
    os.makedirs(output_dir, exist_ok=True)

    flat = [flatten_market(m) for m in markets]
    df   = pd.DataFrame(flat)

    ts       = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(output_dir, f"historical_{status}_{ts}.parquet").replace("\\", "/")

    conn = duckdb.connect()
    conn.execute(f"COPY (SELECT * FROM df) TO '{filepath}' (FORMAT 'PARQUET')")
    conn.close()

    print(f"\n💾 Saved [{status}]:")
    print(f"   Path    → {filepath}")
    print(f"   Rows    → {len(flat)}")
    print(f"   Columns → {len(df.columns)}")

    return filepath


# ─────────────────────────────────────────────
# PREVIEW
# ─────────────────────────────────────────────

def preview(markets: list, status: str, n: int = 5):
    print(f"\n🔍 Sample of {n} [{status}] markets:")
    print(f"   {'SERIES':<25} {'TICKER':<35} {'RESULT':<10}  TITLE")
    print(f"   {'-'*25} {'-'*35} {'-'*10}  {'-'*40}")
    for m in markets[:n]:
        series = (m.get("series_ticker", "") or "")[:25]
        ticker = (m.get("ticker",        "") or "")[:35]
        title  = (m.get("title",         "") or "")[:50]
        result = (m.get("result",        "") or "pending")[:10]
        print(f"   {series:<25} {ticker:<35} {result:<10}  {title}")


# ─────────────────────────────────────────────
# VERIFY
# ─────────────────────────────────────────────

def verify_saved(closed_path: str, settled_path: str):
    """Quick DuckDB count query to confirm both files saved correctly."""
    print("\n📊 Verification:")
    conn = duckdb.connect()

    for label, path in [("Closed", closed_path), ("Settled", settled_path)]:
        if not path:
            print(f"   {label:<10} → skipped (no data)")
            continue
        try:
            row = conn.execute(
                f"SELECT COUNT(*) as total, COUNT(DISTINCT series_ticker) as series "
                f"FROM read_parquet('{path}')"
            ).fetchone()
            print(f"   {label:<10} → {row[0]:>6} markets across {row[1]} series")
        except Exception as e:
            print(f"   {label:<10} → ❌ {e}")

    conn.close()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 65)
    print("PredictIQ — Kalshi Historical Backfill (ONE-TIME)  [BULK v2]")
    print(f"Run time    : {datetime.now(timezone.utc).isoformat()}")
    print(f"Categories  : {', '.join(TARGET_CATEGORIES)}")
    print(f"Closed dir  : {CLOSED_DIR}")
    print(f"Settled dir : {SETTLED_DIR}")
    print(f"Page cap    : {MAX_PAGES} per status")
    print("=" * 65)
    print("\n⚠️  ONE-TIME run. Do not interrupt — files save at the end.")

    # 1. Build category lookup (1 API call)
    print("\n📋 Building series → category lookup...")
    category_lookup = build_category_lookup()

    # 2. Bulk fetch closed + settled, filter locally
    closed_markets, settled_markets = fetch_all_historical(category_lookup)

    closed_path  = None
    settled_path = None

    # 3. Save closed
    print(f"\n{'='*65}")
    if closed_markets:
        closed_path = save_to_bronze(closed_markets, CLOSED_DIR, "closed")
        preview(closed_markets, "closed")
    else:
        print("⚠️  No closed markets found.")

    # 4. Save settled
    if settled_markets:
        settled_path = save_to_bronze(settled_markets, SETTLED_DIR, "settled")
        preview(settled_markets, "settled")
    else:
        print("⚠️  No settled markets found.")

    # 5. Verify
    verify_saved(closed_path, settled_path)

    # 6. Final summary
    total = len(closed_markets) + len(settled_markets)
    print(f"\n{'='*65}")
    print(f"✅ Historical backfill complete.")
    print(f"   Closed markets  : {len(closed_markets):>6}")
    print(f"   Settled markets : {len(settled_markets):>6}")
    print(f"   Total           : {total:>6}")
    print(f"\n   Data is in:")
    print(f"   {CLOSED_DIR}")
    print(f"   {SETTLED_DIR}")
    print(f"\n   You do NOT need to run this script again.")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()