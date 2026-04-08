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
    One call to /series (no status filter) — builds a lookup dict
    so we can filter bulk markets by category locally.
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
# STEP 3 — LOCAL FILTER: POLITICS + RECENCY
# ─────────────────────────────────────────────

def filter_political_recent(markets: list, category_lookup: dict, cutoff_ts: str) -> list:
    """
    Client-side filter:
      1. Category must be Politics (from market or series lookup)
      2. Title must not match TEXT_EXCLUDE
      3. Settlement/close time must be after cutoff_ts (last 24h)
    """
    filtered = []
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

        # Recency check — only keep markets settled/closed after cutoff
        ts_str = m.get("settlement_ts") or m.get("close_time") or ""
        if ts_str and ts_str < cutoff_ts:
            continue

        filtered.append(m)

    return filtered


# ─────────────────────────────────────────────
# STEP 4 — SWEEP BOTH STATUSES IN BULK
# ─────────────────────────────────────────────

def fetch_todays_settlements(category_lookup: dict, cutoff_ts: str) -> tuple:
    """
    Bulk fetch settled + closed markets, filter locally.
    ~50 API calls total instead of ~3,600 per-series calls.
    """
    ingested_at = datetime.now(timezone.utc).isoformat()

    print(f"\n   Bulk-fetching settled markets...")
    raw_settled = fetch_markets_bulk("settled")
    settled = filter_political_recent(raw_settled, category_lookup, cutoff_ts)
    for m in settled:
        m["ingested_at"]   = ingested_at
        m["status_pulled"] = "settled"
    print(f"   Settled: {len(raw_settled)} total → {len(settled)} political + recent")

    print(f"\n   Bulk-fetching closed markets...")
    raw_closed = fetch_markets_bulk("closed")
    closed = filter_political_recent(raw_closed, category_lookup, cutoff_ts)
    for m in closed:
        m["ingested_at"]   = ingested_at
        m["status_pulled"] = "closed"
    print(f"   Closed: {len(raw_closed)} total → {len(closed)} political + recent")

    return closed, settled


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
    print(f"   Path    → {filepath}")
    print(f"   Rows    → {len(flat)}")
    print(f"   Columns → {len(df.columns)}")

    return filepath


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    run_ts   = datetime.now(timezone.utc)
    # Look back 24 hours — catches anything that resolved today
    cutoff   = (run_ts - timedelta(hours=24)).isoformat()

    print("=" * 65)
    print("PredictIQ — Kalshi Daily Settlement Sweeper  [BULK v2]")
    print(f"Run time  : {run_ts.isoformat()}")
    print(f"Cutoff    : {cutoff}  (last 24 hours)")
    print(f"Settled   : {SETTLED_DIR}")
    print(f"Closed    : {CLOSED_DIR}")
    print("=" * 65)

    # 1. Build category lookup (1 API call)
    print("\n📋 Building series → category lookup...")
    category_lookup = build_category_lookup()

    # 2. Bulk fetch + filter for both statuses
    closed_markets, settled_markets = fetch_todays_settlements(category_lookup, cutoff)

    total = len(closed_markets) + len(settled_markets)
    print(f"\n   Sweep complete — {len(closed_markets)} closed + {len(settled_markets)} settled found today.")

    if total == 0:
        print("   Nothing new settled in the last 24 hours. Nothing to save.")
        print("=" * 65)
        return

    # 3. Save each status to its own folder
    print(f"\n{'='*65}")
    save_incremental(closed_markets,  CLOSED_DIR,  "closed")
    save_incremental(settled_markets, SETTLED_DIR, "settled")

    # 4. Summary
    print(f"\n{'='*65}")
    print(f"   Daily settlement sweep complete.")
    print(f"   Closed  : {len(closed_markets):>5} new markets")
    print(f"   Settled : {len(settled_markets):>5} new markets")
    print(f"   Total   : {total:>5}")
    print(f"\n   These will automatically appear in your DuckDB views:")
    print(f"   - kalshi_closed  (via closed/*.parquet)")
    print(f"   - kalshi_settled (via settled/*.parquet)")
    print(f"{'='*65}")


if __name__ == "__main__":
    SLEEP_SECONDS = 86400  # 24 hours
    while True:
        try:
            main()
        except Exception as e:
            print(f"[Settlement Daemon] Run failed with error: {e}. Continuing to next cycle.")
        print(f"[Settlement Daemon] Sweep complete. Sleeping {SLEEP_SECONDS // 3600} hours until next run...")
        time.sleep(SLEEP_SECONDS)
