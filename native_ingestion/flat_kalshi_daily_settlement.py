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

# Always write to project root — not script's local folder
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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

MAX_PAGES_PER_SERIES = 20  # Daily run: should need at most 1-2 pages per series
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
# STEP 1 — GET ALL POLITICAL SERIES (OPEN + RESOLVED)
# ─────────────────────────────────────────────

def fetch_political_series() -> list:
    """
    Fetch all series — no status filter — so we catch
    newly resolved series that settled today.
    """
    data   = get(SERIES_ENDPOINT)
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

    print(f"   Political series found: {len(political)}")
    return political


# ─────────────────────────────────────────────
# STEP 2 — FETCH RECENT MARKETS FOR ONE SERIES + STATUS
# Only returns markets settled/closed within cutoff_ts
# ─────────────────────────────────────────────

def fetch_recent_markets(series_ticker: str, status: str, cutoff_ts: str) -> list:
    """
    Fetch recently settled/closed markets for a given series.
    Paginates until we see markets older than the 24-hour cutoff.
    """
    markets    = []
    cursor     = None
    page_count = 0

    while True:
        page_count += 1
        if page_count > MAX_PAGES_PER_SERIES:
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

        new_markets = []
        stop_early  = False

        for m in batch:
            # Use settlement_ts or close_time to determine recency
            ts_str = (
                m.get("settlement_ts") or
                m.get("close_time")    or
                ""
            )
            if ts_str and ts_str < cutoff_ts:
                # This market and all subsequent are older than our window
                stop_early = True
                break
            new_markets.append(m)

        markets.extend(new_markets)

        if stop_early:
            break

        cursor = data.get("cursor")
        if not cursor:
            break

        time.sleep(0.3)

    return markets


# ─────────────────────────────────────────────
# STEP 3 — SWEEP ALL SERIES FOR RECENT SETTLEMENTS
# ─────────────────────────────────────────────

def fetch_todays_settlements(series_tickers: list, cutoff_ts: str) -> tuple:
    """
    Single pass through all political series.
    Captures anything that settled/closed in the last 24 hours.
    """
    all_closed  = []
    all_settled = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    print(f"\n   Sweeping {len(series_tickers)} series for markets settled after {cutoff_ts[:10]}...\n")

    for i, ticker in enumerate(series_tickers, 1):

        closed = fetch_recent_markets(ticker, "closed", cutoff_ts)
        if closed:
            for m in closed:
                m["ingested_at"]   = ingested_at
                m["series_ticker"] = ticker
                m["status_pulled"] = "closed"
            all_closed.extend(closed)

        settled = fetch_recent_markets(ticker, "settled", cutoff_ts)
        if settled:
            for m in settled:
                m["ingested_at"]   = ingested_at
                m["series_ticker"] = ticker
                m["status_pulled"] = "settled"
            all_settled.extend(settled)

        if closed or settled:
            closed_label  = f"{len(closed)} closed"   if closed  else "—"
            settled_label = f"{len(settled)} settled"  if settled else "—"
            print(f"\r   [{i:>4}/{len(series_tickers)}] {ticker:<35} | {closed_label:<12} | {settled_label}          ", flush=True)

        else:
            print(f"\r   [{i:>4}/{len(series_tickers)}] {ticker:<35} | checking...          ", end="", flush=True)

        time.sleep(0.3)

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
    print("PredictIQ — Kalshi Daily Settlement Sweeper")
    print(f"Run time  : {run_ts.isoformat()}")
    print(f"Cutoff    : {cutoff}  (last 24 hours)")
    print(f"Settled   : {SETTLED_DIR}")
    print(f"Closed    : {CLOSED_DIR}")
    print("=" * 65)

    # 1. Get all political series
    print("\n   Fetching political series...")
    series_tickers = fetch_political_series()
    if not series_tickers:
        print("   No political series found. Check TARGET_CATEGORIES.")
        return

    # 2. Sweep for today's settlements
    closed_markets, settled_markets = fetch_todays_settlements(series_tickers, cutoff)

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
    main()
