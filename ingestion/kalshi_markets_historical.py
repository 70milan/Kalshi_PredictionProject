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

# No practical cap — fetch everything for historical backfill
MAX_PAGES_PER_SERIES = 500  # 500 × 200 = 100,000 per series max
PAGE_SIZE            = 200


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
# STEP 1 — GET ALL POLITICAL SERIES
# No status=open — we need resolved series too
# because their markets have historical data
# ─────────────────────────────────────────────

def fetch_political_series() -> list:
    """
    Fetch all series with no status filter to include resolved ones.
    Filter to political categories only.
    """
    print(f"\n📋 Fetching all series (including resolved)...")
    data   = get(SERIES_ENDPOINT)
    series = data.get("series", [])
    print(f"   Total series found: {len(series)}")

    political = []
    for s in series:
        category = (s.get("category", "") or "").strip()
        title    = (s.get("title",    "") or "").lower()
        ticker   = (s.get("ticker",   "") or "")

        if category not in TARGET_CATEGORIES:
            continue

        if any(ex in title for ex in TEXT_EXCLUDE):
            continue

        political.append(ticker)

    print(f"   Political series to process: {len(political)}")
    return political


# ─────────────────────────────────────────────
# STEP 2 — FETCH MARKETS FOR ONE SERIES + STATUS
# ─────────────────────────────────────────────

def fetch_markets_for_series(series_ticker: str, status: str) -> list:
    """
    Fetch all markets of a given status for one series ticker.
    Paginates until no cursor returned or page cap hit.
    """
    markets    = []
    cursor     = None
    page_count = 0

    while True:
        page_count += 1

        if page_count > MAX_PAGES_PER_SERIES:
            print(f"      ⚠️  Hit page limit for {series_ticker} [{status}]")
            break

        params = {
            "limit":         PAGE_SIZE,
            "status":        status,
            "series_ticker": series_ticker,
        }
        if cursor:
            params["cursor"] = cursor

        data  = get(MARKETS_ENDPOINT, params)
        batch = data.get("markets", [])

        if not batch:
            break

        markets.extend(batch)

        cursor = data.get("cursor")
        if not cursor:
            break

        time.sleep(0.3)

    return markets


# ─────────────────────────────────────────────
# STEP 3 — SINGLE LOOP: BOTH STATUSES PER SERIES
# Cuts run time in half vs two separate passes
# ─────────────────────────────────────────────

def fetch_all_historical(series_tickers: list) -> tuple:
    """
    Single loop through all series.
    Fetches BOTH closed and settled markets per series in one pass.
    Returns two separate lists: (closed_markets, settled_markets)
    """
    all_closed  = []
    all_settled = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    closed_series_count  = 0
    settled_series_count = 0

    print(f"\n📡 Processing {len(series_tickers)} political series (closed + settled in one pass)...")
    print(f"   This will take a while — grab a coffee.\n")

    for i, ticker in enumerate(series_tickers, 1):

        # Fetch closed markets for this series
        closed = fetch_markets_for_series(ticker, "closed")
        if closed:
            for m in closed:
                m["ingested_at"]   = ingested_at
                m["series_ticker"] = ticker
                m["status_pulled"] = "closed"
            all_closed.extend(closed)
            closed_series_count += 1

        # Fetch settled markets for this series
        settled = fetch_markets_for_series(ticker, "settled")
        if settled:
            for m in settled:
                m["ingested_at"]   = ingested_at
                m["series_ticker"] = ticker
                m["status_pulled"] = "settled"
            all_settled.extend(settled)
            settled_series_count += 1

        # Only print progress when something was found
        if closed or settled:
            closed_label  = f"{len(closed)} closed"   if closed  else "—"
            settled_label = f"{len(settled)} settled"  if settled else "—"
            print(f"   [{i:>4}/{len(series_tickers)}] {ticker:<35} | {closed_label:<12} | {settled_label}")

        # Progress ping every 100 series regardless
        elif i % 100 == 0:
            print(f"   [{i:>4}/{len(series_tickers)}] ... {i} series checked, "
                  f"{len(all_closed)} closed + {len(all_settled)} settled so far")

        time.sleep(0.3)

    print(f"\n   ✅ Pass complete.")
    print(f"   Closed  — {len(all_closed):>6} markets from {closed_series_count} series")
    print(f"   Settled — {len(all_settled):>6} markets from {settled_series_count} series")

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
    print("PredictIQ — Kalshi Historical Backfill (ONE-TIME)")
    print(f"Run time    : {datetime.now(timezone.utc).isoformat()}")
    print(f"Categories  : {', '.join(TARGET_CATEGORIES)}")
    print(f"Closed dir  : {CLOSED_DIR}")
    print(f"Settled dir : {SETTLED_DIR}")
    print(f"Page cap    : {MAX_PAGES_PER_SERIES} per series per status")
    print("=" * 65)
    print("\n⚠️  ONE-TIME run. Do not interrupt — files save at the end.")
    print("   Estimated time: 15-25 minutes.\n")

    # 1. Get all political series (including resolved)
    series_tickers = fetch_political_series()
    if not series_tickers:
        print("⚠️  No political series found. Check TARGET_CATEGORIES.")
        return

    # 2. Single loop — fetch closed + settled per series
    closed_markets, settled_markets = fetch_all_historical(series_tickers)

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