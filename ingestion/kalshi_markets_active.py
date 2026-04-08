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
# CONFIG
# ─────────────────────────────────────────────

load_dotenv()

API_KEY    = os.getenv("KALSHI_API_KEY")
API_SECRET = os.getenv("KALSHI_API_SECRET").replace("\\n", "\n")
BASE_URL   = "https://api.elections.kalshi.com"
MARKETS_ENDPOINT = "/trade-api/v2/markets"
SERIES_ENDPOINT  = "/trade-api/v2/series"

# Docker: /app is the project root (volume-mounted)
PROJECT_ROOT = "/app"
BRONZE_DIR   = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "open")

# Categories to pull from /series endpoint
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

MAX_PAGES  = 200   # 200 × 200 = 40,000 markets cap — generous safety limit
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
            
            # Handle 429 Rate Limit explicitly
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
# STEP 1 — BULK FETCH ALL OPEN MARKETS
# O(N_pages) instead of O(N_series)
# ─────────────────────────────────────────────

def fetch_all_open_markets() -> list:
    """
    Single paginated sweep of GET /markets?status=open&limit=200.
    No series_ticker filter — fetches everything in ~15-25 API calls
    instead of ~1,800 per-series calls.
    """
    all_markets = []
    cursor = None
    page = 0

    while True:
        page += 1
        if page > MAX_PAGES:
            print(f"   ⚠️  Hit page cap ({MAX_PAGES}). Stopping pagination.")
            break

        params = {"limit": PAGE_SIZE, "status": "open"}
        if cursor:
            params["cursor"] = cursor

        data = get(MARKETS_ENDPOINT, params)
        batch = data.get("markets", [])

        if not batch:
            break

        all_markets.extend(batch)
        print(f"   Page {page:>3}: +{len(batch)} markets  (running total: {len(all_markets)})")

        cursor = data.get("cursor")
        if not cursor:
            break

        time.sleep(0.3)

    return all_markets


# ─────────────────────────────────────────────
# STEP 2 — BUILD CATEGORY LOOKUP FROM /series
# Single API call → {series_ticker: category}
# ─────────────────────────────────────────────

def build_category_lookup() -> dict:
    """
    One call to /series — builds a quick lookup dict so we can
    filter bulk markets by category locally.
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
    Uses category from the market object if available,
    falls back to the series lookup.
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
# FLATTEN
# ─────────────────────────────────────────────

def flatten_market(m: dict) -> dict:
    return {
        k: (str(v) if isinstance(v, (dict, list)) else v)
        for k, v in m.items()
    }


# ─────────────────────────────────────────────
# SAVE TO BRONZE
# ─────────────────────────────────────────────

def save_to_bronze(markets: list):
    """
    Two files per run:
      latest.parquet              — overwritten, always current
      markets_TIMESTAMP.parquet   — append-only history, never overwritten
    """
    os.makedirs(BRONZE_DIR, exist_ok=True)

    flat = [flatten_market(m) for m in markets]
    df   = pd.DataFrame(flat)

    ts           = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    latest_path  = os.path.join(BRONZE_DIR, "latest.parquet").replace("\\", "/")
    history_path = os.path.join(BRONZE_DIR, f"markets_{ts}.parquet").replace("\\", "/")

    conn = duckdb.connect()
    conn.execute(f"COPY (SELECT * FROM df) TO '{latest_path}'  (FORMAT 'PARQUET')")
    conn.execute(f"COPY (SELECT * FROM df) TO '{history_path}' (FORMAT 'PARQUET')")
    conn.close()

    print(f"   Saved → {len(flat)} rows, {len(df.columns)} columns")
    print(f"   Latest   : latest.parquet")
    print(f"   Snapshot : markets_{ts}.parquet")


# ─────────────────────────────────────────────
# PREVIEW
# ─────────────────────────────────────────────

def preview(markets: list, n: int = 5):
    print(f"\n   {'SERIES':<20} {'TICKER':<35} {'YES_BID':>8}  TITLE")
    print(f"   {'-'*20} {'-'*35} {'-'*8}  {'-'*40}")
    for m in markets[:n]:
        series  = (m.get("series_ticker",   "") or "")[:20]
        ticker  = (m.get("ticker",          "") or "")[:35]
        title   = (m.get("title",           "") or "")[:60]
        yes_bid =  m.get("yes_bid_dollars", "?")
        print(f"   {series:<20} {ticker:<35} {str(yes_bid):>8}  {title}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 65)
    print("PredictIQ — Kalshi Active Markets Poll  [BULK v2]")
    print(f"Run time : {datetime.now(timezone.utc).isoformat()}")
    print(f"Output   : {BRONZE_DIR}")
    print("=" * 65)

    # 1. Build category lookup (1 API call)
    print("\n📋 Building series → category lookup...")
    category_lookup = build_category_lookup()

    # 2. Bulk fetch ALL open markets (paginated, ~15-25 API calls)
    print(f"\n📡 Bulk-fetching all open markets...")
    all_markets = fetch_all_open_markets()
    print(f"\n   Total open markets (all categories): {len(all_markets)}")

    if not all_markets:
        print("⚠️  No open markets returned from API.")
        return

    # 3. Filter for Politics locally
    print(f"\n🔍 Filtering for Politics...")
    political = filter_political(all_markets, category_lookup)
    print(f"   {len(all_markets)} total → {len(political)} political")

    if not political:
        print("⚠️  No political markets after filtering.")
        return

    # 4. Stamp metadata
    ingested_at = datetime.now(timezone.utc).isoformat()
    for m in political:
        m["ingested_at"]   = ingested_at
        m["status_pulled"] = "open"

    # 5. Save
    print(f"\n💾 Saving to Bronze...")
    save_to_bronze(political)

    # 6. Preview
    print(f"\n🔍 Sample:")
    preview(political)

    print("\n✅ Done.")
    print("=" * 65)


if __name__ == "__main__":
    import time
    import traceback

    SLEEP_SECONDS = 900  # 15 minutes
    
    print("[Kalshi Active] Polling Daemon Initialized (15-min intervals).")
    while True:
        try:
            main()
        except Exception:
            print("[Kalshi Active] FATAL ERROR in main loop:")
            traceback.print_exc()
            
        print(f"\n[Kalshi Active] Poll complete. Sleeping {SLEEP_SECONDS // 60} minutes...")
        time.sleep(SLEEP_SECONDS)