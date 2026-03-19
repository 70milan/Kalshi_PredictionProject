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

# Always write to project root — not script's local folder
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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

MAX_PAGES_PER_SERIES = 10   # 10 × 200 = 2,000 per series max
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


def get(path: str, params: dict = None) -> dict:
    try:
        response = requests.get(
            f"{BASE_URL}{path}",
            headers=build_headers("GET", path),
            params=params or {},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"    ❌ Request failed: {e}")
        return {}


# ─────────────────────────────────────────────
# STEP 1 — GET OPEN POLITICAL SERIES
# ─────────────────────────────────────────────

def fetch_political_series() -> list[str]:
    """
    Fetch only open series — status=open skips resolved/dead series.
    Filter to Politics category only.
    """
    data   = get(SERIES_ENDPOINT, params={"status": "open"})
    series = data.get("series", [])

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

    print(f"   Series found: {len(series)} total → {len(political)} political")
    return political


# ─────────────────────────────────────────────
# STEP 2 — FETCH OPEN MARKETS PER SERIES
# ─────────────────────────────────────────────

def fetch_markets_for_series(series_ticker: str) -> list[dict]:
    markets    = []
    cursor     = None
    page_count = 0

    while True:
        page_count += 1
        if page_count > MAX_PAGES_PER_SERIES:
            break

        params = {
            "limit":         PAGE_SIZE,
            "status":        "open",
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

        time.sleep(0.2)

    return markets


def fetch_all_political_markets(series_tickers: list[str]) -> list[dict]:
    all_markets = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    for i, ticker in enumerate(series_tickers, 1):
        markets = fetch_markets_for_series(ticker)

        if markets:
            for m in markets:
                m["ingested_at"]   = ingested_at
                m["series_ticker"] = ticker
                m["status_pulled"] = "open"
            all_markets.extend(markets)
            print(f"   [{i:>3}/{len(series_tickers)}] {ticker:<30} → {len(markets)} markets")

        time.sleep(0.2)

    return all_markets


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

def save_to_bronze(markets: list[dict]):
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

def preview(markets: list[dict], n: int = 5):
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
    print("PredictIQ — Kalshi Active Markets Poll")
    print(f"Run time : {datetime.now(timezone.utc).isoformat()}")
    print(f"Output   : {BRONZE_DIR}")
    print("=" * 65)

    # 1. Get open political series
    print("\n📋 Fetching open political series...")
    series_tickers = fetch_political_series()
    if not series_tickers:
        print("⚠️  No political series found.")
        return

    # 2. Fetch open markets
    print(f"\n📡 Fetching open markets...")
    all_markets = fetch_all_political_markets(series_tickers)
    print(f"\n✅ Total markets fetched: {len(all_markets)}")
    if not all_markets:
        print("⚠️  No open markets found.")
        return

    # 3. Save
    print(f"\n💾 Saving to Bronze...")
    save_to_bronze(all_markets)

    # 4. Preview
    print(f"\n🔍 Sample:")
    preview(all_markets)

    print("\n✅ Done.")
    print("=" * 65)


if __name__ == "__main__":
    main()