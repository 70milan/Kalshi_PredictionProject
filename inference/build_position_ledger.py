"""
PredictIQ - Position Ledger Builder
====================================
Joins Kalshi fills with the intelligence briefs that triggered each entry.
Output: data/gold/position_ledger.parquet — one row per open position with
entry context (price, time, original thesis, confidence).

The ledger is rebuilt every cycle (not append-only). It always reflects
the current state of open positions, so the exit evaluator can ask:
"given what we knew when we entered, is this trade still working?"

Modes:
- SAFE_MODE=true  : reads simulated_trades.csv (paper trades)
- SAFE_MODE=false : reads Kalshi /portfolio/positions + /portfolio/fills
"""

import os
import sys
import time
import base64
import requests
import pandas as pd
import duckdb
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

BRIEFS_PATH           = os.path.join(PROJECT_ROOT, "data", "gold", "intelligence_briefs")
LEDGER_PATH           = os.path.join(PROJECT_ROOT, "data", "gold", "position_ledger.parquet")
SIMULATED_TRADES_PATH = os.path.join(PROJECT_ROOT, "data", "gold", "simulated_trades.csv")

KALSHI_BASE_URL   = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_API_KEY    = os.getenv("KALSHI_API_KEY")
KALSHI_API_SECRET = os.getenv("KALSHI_API_SECRET", "")
SAFE_MODE         = os.getenv("SAFE_MODE", "true").lower() == "true"


# ─────────────────────────────────────────────
# KALSHI AUTH (duplicated from api/main.py to keep this script standalone)
# ─────────────────────────────────────────────

def _sign_kalshi_request(method: str, path: str) -> dict:
    ts_ms       = str(int(time.time() * 1000))
    msg_string  = ts_ms + method.upper() + path
    secret      = KALSHI_API_SECRET.replace("\\n", "\n")
    private_key = serialization.load_pem_private_key(secret.encode(), password=None)
    signature   = private_key.sign(
        msg_string.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )
    return {
        "KALSHI-ACCESS-KEY":       KALSHI_API_KEY,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "Content-Type":            "application/json",
    }


# ─────────────────────────────────────────────
# DATA SOURCES
# ─────────────────────────────────────────────

def fetch_live_positions_and_fills():
    """Pull current open positions + buy fills from Kalshi."""
    pos_headers = _sign_kalshi_request("GET", "/trade-api/v2/portfolio/positions")
    pos_r = requests.get(f"{KALSHI_BASE_URL}/portfolio/positions", headers=pos_headers, timeout=10)
    pos_r.raise_for_status()
    positions = pos_r.json().get("market_positions", [])
    positions = [p for p in positions if float(p.get("position_fp", 0)) != 0]

    fill_headers = _sign_kalshi_request("GET", "/trade-api/v2/portfolio/fills")
    fill_r = requests.get(
        f"{KALSHI_BASE_URL}/portfolio/fills",
        params={"limit": 500},
        headers=fill_headers,
        timeout=10,
    )
    fill_r.raise_for_status()
    fills = fill_r.json().get("fills", [])
    fills = [f for f in fills if f.get("action") == "buy"]

    return positions, fills


def fetch_safe_mode_positions():
    """Build pseudo-positions from simulated_trades.csv."""
    if not os.path.exists(SIMULATED_TRADES_PATH):
        return [], []
    df = pd.read_csv(SIMULATED_TRADES_PATH)
    if df.empty:
        return [], []

    fills = []
    for _, row in df.iterrows():
        fills.append({
            "ticker":            row["ticker"],
            "side":              row["side"],
            "action":            "buy",
            "count_fp":          str(row["count"]),
            "yes_price_dollars": str(row["price_dollars"]) if row["side"] == "yes" else "0",
            "no_price_dollars":  str(row["price_dollars"]) if row["side"] == "no" else "0",
            "created_time":      row["timestamp"],
        })

    grouped = df.groupby(["ticker", "side"]).agg(
        total_count=("count", "sum"),
        total_cost=("total_risk_usd", "sum"),
    ).reset_index()

    positions = []
    for _, g in grouped.iterrows():
        pos_fp = float(g["total_count"]) if g["side"] == "yes" else -float(g["total_count"])
        positions.append({
            "ticker":                  g["ticker"],
            "position_fp":             pos_fp,
            "market_exposure_dollars": float(g["total_cost"]),
        })

    return positions, fills


# ─────────────────────────────────────────────
# BRIEF MATCHING
# ─────────────────────────────────────────────

def find_matching_brief(con, ticker: str, fill_time: datetime):
    """
    Find the brief most likely to have triggered this entry.
    Rule: same ticker, ingested at or before fill_time, within the last 24h.
    Returns the most recent such brief, or None.
    """
    parquet_glob = os.path.join(BRIEFS_PATH, "*.parquet").replace("\\", "/")
    fill_iso  = fill_time.isoformat()
    floor_iso = (fill_time - timedelta(hours=24)).isoformat()
    try:
        df = con.execute(f"""
            SELECT ticker, verdict, recommended_side, confidence_score,
                   current_odds, mispricing_score, ingested_at, bull_case, bear_case
            FROM read_parquet('{parquet_glob}', union_by_name=true)
            WHERE ticker = '{ticker}'
              AND CAST(ingested_at AS TIMESTAMP) <= TIMESTAMP '{fill_iso}'
              AND CAST(ingested_at AS TIMESTAMP) >= TIMESTAMP '{floor_iso}'
            ORDER BY CAST(ingested_at AS TIMESTAMP) DESC
            LIMIT 1
        """).df()
        if df.empty:
            return None
        return df.iloc[0].to_dict()
    except Exception as e:
        print(f"    [WARN] Brief lookup failed for {ticker}: {e}")
        return None


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 60)
    print(" PredictIQ Position Ledger Builder")
    print("=" * 60)

    if SAFE_MODE:
        print("[Ledger] SAFE_MODE — reading simulated_trades.csv")
        positions, fills = fetch_safe_mode_positions()
    else:
        print("[Ledger] LIVE — fetching from Kalshi API")
        try:
            positions, fills = fetch_live_positions_and_fills()
        except Exception as e:
            print(f"[Ledger] ERROR fetching from Kalshi: {e}")
            return

    if not positions:
        print("[Ledger] No open positions. Writing empty ledger.")
        os.makedirs(os.path.dirname(LEDGER_PATH), exist_ok=True)
        pd.DataFrame(columns=[
            "ticker", "side", "qty", "entry_price", "entry_time", "cost_basis",
            "brief_id", "entry_confidence", "entry_thesis", "original_odds",
            "recommended_side", "bull_case", "bear_case", "evaluated_at"
        ]).to_parquet(LEDGER_PATH, index=False)
        return

    # Index fills by ticker for fast lookup
    fills_by_ticker = {}
    for f in fills:
        t = f.get("ticker")
        if t:
            fills_by_ticker.setdefault(t, []).append(f)

    con = duckdb.connect()
    ledger_rows = []

    for pos in positions:
        ticker = pos["ticker"]
        pos_fp = float(pos.get("position_fp", 0))
        side   = "yes" if pos_fp > 0 else "no"
        qty    = abs(pos_fp)

        # Match buy fills on (ticker, side)
        ticker_fills = [f for f in fills_by_ticker.get(ticker, []) if f.get("side") == side]
        if not ticker_fills:
            print(f"[Ledger] {ticker}: position exists but no matching buy fill — skipping")
            continue

        # Use the position's market_exposure_dollars for cost basis — this matches
        # Kalshi's displayed average price exactly (fills can have rounding drift).
        market_exposure = float(pos.get("market_exposure_dollars", 0))
        entry_price = market_exposure / qty if qty > 0 else 0.0

        # Earliest fill time = entry timestamp
        first_fill_time = min(
            pd.to_datetime(f.get("created_time"))
            for f in ticker_fills
        )
        if first_fill_time.tzinfo is None:
            first_fill_time = first_fill_time.tz_localize("UTC")

        brief = find_matching_brief(con, ticker, first_fill_time)

        ledger_rows.append({
            "ticker":            ticker,
            "side":              side,
            "qty":               qty,
            "entry_price":       round(entry_price, 4),
            "entry_time":        first_fill_time.isoformat(),
            "cost_basis":        round(market_exposure, 4),
            "brief_id":          str(brief["ingested_at"]) if brief else None,
            "entry_confidence":  float(brief["confidence_score"]) if brief else None,
            "entry_thesis":      str(brief["verdict"]) if brief else None,
            "original_odds":     float(brief["current_odds"]) if brief else None,
            "recommended_side":  str(brief["recommended_side"]) if brief else None,
            "bull_case":         str(brief["bull_case"]) if brief else None,
            "bear_case":         str(brief["bear_case"]) if brief else None,
            "evaluated_at":      datetime.now(timezone.utc).isoformat(),
        })

    con.close()

    os.makedirs(os.path.dirname(LEDGER_PATH), exist_ok=True)
    df = pd.DataFrame(ledger_rows)
    df.to_parquet(LEDGER_PATH, index=False)
    print(f"[Ledger] Wrote {len(df)} position(s) to {LEDGER_PATH}")

    if len(df) > 0:
        matched = df["brief_id"].notna().sum()
        print(f"[Ledger] {matched}/{len(df)} positions matched to a brief")


if __name__ == "__main__":
    main()
