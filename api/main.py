"""
PredictIQ - FastAPI Bridge Server
====================================
Serves intelligence briefs from the Gold Lakehouse to the React frontend.
Handles trade execution (with Safe Mode guard) via Kalshi API.

Brief storage: single append-only Parquet folder (intelligence_briefs/).
Dashboard query deduplicates by ticker and filters to today — so the
dashboard always shows ALL unique opportunities flagged today, not just
the last 3 from the most recent cycle.
"""

import os
import sys
import json
import time
import hashlib
import requests
import duckdb
import pandas as pd
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import base64

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kelly_math import calculate_kelly

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

PROJECT_ROOT          = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRIEFS_PATH           = os.path.join(PROJECT_ROOT, "data", "gold", "intelligence_briefs")
SIMULATED_TRADES_PATH = os.path.join(PROJECT_ROOT, "data", "gold", "simulated_trades.csv")
BRONZE_LATEST_PATH    = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "open", "latest.parquet")
KALSHI_BASE_URL       = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_API_KEY        = os.getenv("KALSHI_API_KEY")
KALSHI_API_SECRET     = os.getenv("KALSHI_API_SECRET", "")
SAFE_MODE             = os.getenv("SAFE_MODE", "true").lower() == "true"
DEFAULT_BANKROLL      = float(os.getenv("BANKROLL_USD", "1000.0"))

app = FastAPI(title="PredictIQ Intelligence API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://100.86.91.43:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────
# KALSHI AUTH
# ─────────────────────────────────────────────

def _sign_kalshi_request(method: str, path: str) -> dict:
    ts_ms      = str(int(time.time() * 1000))
    msg_string = ts_ms + method.upper() + path
    secret     = KALSHI_API_SECRET.replace("\\n", "\n")
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
# INTELLIGENCE ENDPOINT
# ─────────────────────────────────────────────

@app.get("/api/intelligence")
def get_intelligence():
    """
    Returns today's unique AI intelligence briefs.

    Key design decisions:
    - Reads from a single append-only Parquet folder (no current/history split)
    - Filters to TODAY only — dashboard always shows today's opportunities
    - Deduplicates by ticker — if a market was flagged 5 times today,
      show only the highest confidence brief for that ticker
    - No LIMIT — shows ALL unique markets flagged today (typically 10-50)
    """
    if not os.path.exists(BRIEFS_PATH):
        return {"briefs": [], "safe_mode": SAFE_MODE}

    con = duckdb.connect()
    try:
        # Read all Parquet files in the briefs folder
        parquet_glob = os.path.join(BRIEFS_PATH, "*.parquet").replace("\\", "/")
        bronze_latest = BRONZE_LATEST_PATH.replace("\\", "/")

        # Cross-reference against latest.parquet (overwritten every 15 min by ingestion).
        # We JOIN it to get the live market odds without spamming the Kalshi API on load.
        if os.path.exists(BRONZE_LATEST_PATH):
            query = f"""
                SELECT
                    b.ticker,
                    b.title,
                    b.current_odds,
                    b.odds_delta,
                    b.mispricing_score,
                    b.bull_case,
                    b.bear_case,
                    b.verdict,
                    b.confidence_score,
                    b.ingested_at,
                    b.recommended_side,
                    m.yes_bid_dollars AS live_yes_bid,
                    m.yes_ask_dollars AS live_yes_ask
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticker
                            ORDER BY confidence_score DESC, ingested_at DESC
                        ) AS rn
                    FROM read_parquet('{parquet_glob}', union_by_name=true)
                    WHERE CAST(ingested_at AS DATE) = CURRENT_DATE
                      AND verdict NOT LIKE '%Error%'
                      AND verdict NOT LIKE '%GHOST PUMP%'
                      AND verdict IS NOT NULL
                      AND verdict != ''
                ) b
                JOIN read_parquet('{bronze_latest}') m ON b.ticker = m.ticker
                WHERE b.rn = 1
                ORDER BY b.mispricing_score DESC, b.confidence_score DESC
            """
        else:
            query = f"""
                SELECT
                    ticker,
                    title,
                    current_odds,
                    odds_delta,
                    mispricing_score,
                    bull_case,
                    bear_case,
                    verdict,
                    confidence_score,
                    ingested_at,
                    recommended_side,
                    NULL AS live_yes_bid,
                    NULL AS live_yes_ask
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticker
                            ORDER BY confidence_score DESC, ingested_at DESC
                        ) AS rn
                    FROM read_parquet('{parquet_glob}', union_by_name=true)
                    WHERE CAST(ingested_at AS DATE) = CURRENT_DATE
                      AND verdict NOT LIKE '%Error%'
                      AND verdict NOT LIKE '%GHOST PUMP%'
                      AND verdict IS NOT NULL
                      AND verdict != ''
                )
                WHERE rn = 1
                ORDER BY mispricing_score DESC, confidence_score DESC
            """
        df = con.execute(query).df()
    except Exception as e:
        print(f"[API ERROR] {e}")
        return {"briefs": [], "safe_mode": SAFE_MODE, "error": str(e)}
    finally:
        con.close()

    # Fetch live Kalshi balance to use for Kelly Criterion. Fallback to DEFAULT_BANKROLL on error.
    active_bankroll = DEFAULT_BANKROLL
    try:
        balance_headers = _sign_kalshi_request("GET", "/trade-api/v2/portfolio/balance")
        r_bal = requests.get(f"{KALSHI_BASE_URL}/portfolio/balance", headers=balance_headers, timeout=5)
        if r_bal.status_code == 200:
            bal_cents = r_bal.json().get("balance", 0)
            if bal_cents > 0:
                active_bankroll = float(bal_cents) / 100.0
    except Exception as e:
        print(f"[API ERROR] Failed to fetch Kalshi balance: {e}")

    briefs = []
    for _, row in df.iterrows():
        yes_bid    = float(row.get("current_odds", 0.5) or 0.5)
        confidence = float(row.get("confidence_score", 0.0) or 0.0)
        kelly      = calculate_kelly(confidence, yes_bid, active_bankroll)
        # Derive recommended_side from the verdict text.
        # The LLM prompt always instructs the model to write "Buy YES" or "Buy NO".
        # This works for both old Parquet files (no recommended_side column) and new ones.
        verdict_lower = str(row.get("verdict", "")).lower()
        if "buy no" in verdict_lower:
            recommended_side = "no"
        elif "buy yes" in verdict_lower:
            recommended_side = "yes"
        else:
            # Final fallback: negative odds_delta means market overpriced → buy NO
            recommended_side = "no" if float(row.get("odds_delta", 0) or 0) < -0.05 else "yes"
        briefs.append({
            "ticker":            str(row.get("ticker", "")),
            "title":             str(row.get("title", "")),
            "current_odds":      yes_bid,
            "odds_delta":        float(row.get("odds_delta", 0) or 0),
            "mispricing_score":  float(row.get("mispricing_score", 0) or 0),
            "bull_case":         str(row.get("bull_case", "")),
            "bear_case":         str(row.get("bear_case", "")),
            "verdict":           str(row.get("verdict", "")),
            "recommended_side":  recommended_side,
            "confidence_score":  confidence,
            "ingested_at":       str(row.get("ingested_at", "")),
            "kelly":             kelly,
            "safe_mode":         SAFE_MODE,
            "live_yes_bid":      float(row["live_yes_bid"]) if row.get("live_yes_bid") is not None and not pd.isna(row.get("live_yes_bid")) else None,
            "live_yes_ask":      float(row["live_yes_ask"]) if row.get("live_yes_ask") is not None and not pd.isna(row.get("live_yes_ask")) else None,
        })

    # Report when the active-market list was last refreshed
    bronze_age = None
    if os.path.exists(BRONZE_LATEST_PATH):
        mtime = os.path.getmtime(BRONZE_LATEST_PATH)
        bronze_age = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()

    return {
        "briefs":               briefs,
        "safe_mode":            SAFE_MODE,
        "bankroll":             active_bankroll,
        "count":                len(briefs),
        "as_of":                datetime.now(timezone.utc).isoformat(),
        "active_markets_as_of": bronze_age,   # when latest.parquet was last written
        "active_market_filter": os.path.exists(BRONZE_LATEST_PATH),
    }


# ─────────────────────────────────────────────
# HISTORY ENDPOINT — all briefs, any date
# ─────────────────────────────────────────────

@app.get("/api/intelligence/history")
def get_intelligence_history(days: int = 7):
    """
    Returns all briefs from the last N days for review/backtesting.
    Deduplicated by ticker per day — one brief per market per day.
    """
    if not os.path.exists(BRIEFS_PATH):
        return {"briefs": [], "days": days}

    con = duckdb.connect()
    try:
        parquet_glob = os.path.join(BRIEFS_PATH, "*.parquet").replace("\\", "/")
        query = f"""
            SELECT
                ticker, title, current_odds, odds_delta,
                mispricing_score, verdict, confidence_score, ingested_at,
                CAST(ingested_at AS DATE) as brief_date
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY ticker, CAST(ingested_at AS DATE)
                        ORDER BY confidence_score DESC
                    ) AS rn
                FROM read_parquet('{parquet_glob}', union_by_name=true)
                WHERE CAST(ingested_at AS DATE) >= CURRENT_DATE - INTERVAL {days} DAY
                  AND verdict NOT LIKE '%Error%'
                  AND verdict IS NOT NULL
            )
            WHERE rn = 1
            ORDER BY ingested_at DESC
        """
        df = con.execute(query).df()
    except Exception as e:
        return {"briefs": [], "error": str(e)}
    finally:
        con.close()

    return {
        "briefs": df.to_dict(orient="records"),
        "count":  len(df),
        "days":   days,
    }


# ─────────────────────────────────────────────
# LIVE KALSHI ODDS
# ─────────────────────────────────────────────

@app.get("/api/market/{ticker}")
def get_market(ticker: str):
    """Fetches live Kalshi odds for a given ticker."""
    path = f"/markets/{ticker}"
    headers = _sign_kalshi_request("GET", f"/trade-api/v2{path}")
    try:
        r = requests.get(f"{KALSHI_BASE_URL}{path}", headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ─────────────────────────────────────────────
# TRADE EXECUTION
# ─────────────────────────────────────────────

class TradeRequest(BaseModel):
    ticker:        str
    side:          str    # "yes" or "no"
    count:         int    # number of contracts
    price_dollars: float  # ask price in dollars (e.g. 0.963)


@app.post("/api/trade")
def execute_trade(trade: TradeRequest):
    """Submits a trade to Kalshi. Guard-railed by SAFE_MODE."""
    if SAFE_MODE:
        file_exists = os.path.isfile(SIMULATED_TRADES_PATH)
        with open(SIMULATED_TRADES_PATH, "a", encoding="utf-8") as f:
            if not file_exists:
                f.write("timestamp,ticker,side,count,price_dollars,total_risk_usd\n")
            ts   = datetime.now(timezone.utc).isoformat()
            risk = trade.count * trade.price_dollars
            f.write(f"{ts},{trade.ticker},{trade.side},{trade.count},{trade.price_dollars:.4f},{risk:.2f}\n")
        print(f"[SAFE MODE] Logged: {trade.count}x {trade.ticker} @ {trade.price_dollars:.4f}")
        return {
            "status":  "SAFE_MODE_LOGGED",
            "message": f"Trade recorded in {os.path.basename(SIMULATED_TRADES_PATH)}",
            "trade":   trade.dict()
        }

    # Pre-flight: verify market is still active before placing order
    try:
        mkt_headers = _sign_kalshi_request("GET", f"/trade-api/v2/markets/{trade.ticker}")
        mkt_r = requests.get(f"{KALSHI_BASE_URL}/markets/{trade.ticker}", headers=mkt_headers, timeout=10)
        if mkt_r.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Market {trade.ticker} not found on Kalshi — it may have expired.")
        if mkt_r.status_code == 200:
            mkt_status = mkt_r.json().get("market", {}).get("status", "unknown")
            if mkt_status not in ("open", "active"):
                raise HTTPException(status_code=400, detail=f"Market {trade.ticker} is not tradeable (status: {mkt_status}). Brief is stale — refresh the dashboard.")
    except HTTPException:
        raise
    except Exception as e:
        print(f"[TRADE PRE-FLIGHT] Warning: could not verify market status: {e}")

    path    = "/portfolio/orders"
    headers = _sign_kalshi_request("POST", f"/trade-api/v2{path}")
    payload = {
        "ticker":           trade.ticker,
        "client_order_id":  hashlib.md5(f"{trade.ticker}-{time.time()}".encode()).hexdigest(),
        "type":             "limit",
        "action":           "buy",
        "side":             trade.side,
        "count":            trade.count,
    }
    if trade.side == "yes":
        payload["yes_price_dollars"] = f"{trade.price_dollars:.4f}"
    else:
        payload["no_price_dollars"] = f"{trade.price_dollars:.4f}"

    print(f"[TRADE] Submitting payload: {json.dumps(payload)}")
    try:
        r = requests.post(
            f"{KALSHI_BASE_URL}{path}",
            headers=headers,
            data=json.dumps(payload),
            timeout=10
        )
        if not r.ok:
            print(f"[TRADE ERROR] HTTP {r.status_code} — {r.text}")
        r.raise_for_status()
        resp = r.json()
        order = resp.get("order", {})
        order_status = order.get("status", "unknown")
        order_id = order.get("order_id", "")
        fill_count = order.get("fill_count_fp", "0")
        remaining = order.get("remaining_count_fp", str(trade.count))
        print(f"[TRADE] order_id={order_id} status={order_status} filled={fill_count} remaining={remaining}")
        return {
            "status":       "SUBMITTED",
            "order_status": order_status,
            "order_id":     order_id,
            "fill_count":   fill_count,
            "remaining":    remaining,
            "response":     resp,
        }
    except Exception as e:
        print(f"[TRADE ERROR] {e}")
        raise HTTPException(status_code=502, detail=str(e))


# ─────────────────────────────────────────────
# PORTFOLIO — inspect live Kalshi orders/balance
# ─────────────────────────────────────────────

@app.get("/api/portfolio/orders")
def get_portfolio_orders(status: str = "resting", limit: int = 20):
    """
    Fetches open orders from Kalshi demo portfolio.
    status options: resting, filled, cancelled, all
    """
    path = f"/portfolio/orders?status={status}&limit={limit}"
    headers = _sign_kalshi_request("GET", f"/trade-api/v2/portfolio/orders")
    try:
        r = requests.get(f"{KALSHI_BASE_URL}/portfolio/orders",
                         params={"status": status, "limit": limit},
                         headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/api/portfolio/positions")
def get_portfolio_positions():
    """Fetches open positions (filled contracts you currently hold) from Kalshi."""
    headers = _sign_kalshi_request("GET", "/trade-api/v2/portfolio/positions")
    try:
        r = requests.get(f"{KALSHI_BASE_URL}/portfolio/positions", headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.delete("/api/portfolio/orders/cancel/{ticker}")
def cancel_orders_for_ticker(ticker: str):
    """Finds and cancels all resting orders for a given ticker."""
    list_headers = _sign_kalshi_request("GET", "/trade-api/v2/portfolio/orders")
    try:
        r = requests.get(f"{KALSHI_BASE_URL}/portfolio/orders",
                         params={"ticker": ticker, "status": "resting", "limit": 50},
                         headers=list_headers, timeout=10)
        r.raise_for_status()
        resting = r.json().get("orders", [])
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch orders: {e}")

    if not resting:
        return {"cancelled": [], "message": "No resting orders found for this ticker."}

    cancelled = []
    for order in resting:
        order_id = order.get("order_id")
        del_path = f"/trade-api/v2/portfolio/orders/{order_id}"
        del_headers = _sign_kalshi_request("DELETE", del_path)
        try:
            del_r = requests.delete(f"{KALSHI_BASE_URL}/portfolio/orders/{order_id}",
                                    headers=del_headers, timeout=10)
            if del_r.ok:
                cancelled.append(order_id)
        except Exception:
            pass
    return {"cancelled": cancelled, "count": len(cancelled)}


@app.get("/api/portfolio/balance")
def get_portfolio_balance():
    """Fetches current Kalshi demo account balance."""
    headers = _sign_kalshi_request("GET", "/trade-api/v2/portfolio/balance")
    try:
        r = requests.get(f"{KALSHI_BASE_URL}/portfolio/balance", headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ─────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────

@app.get("/api/health")
def health():
    briefs_dir_exists = os.path.exists(BRIEFS_PATH)
    return {
        "status":            "ok",
        "safe_mode":         SAFE_MODE,
        "briefs_dir_exists": briefs_dir_exists,
        "ts":                datetime.now(timezone.utc).isoformat(),
    }