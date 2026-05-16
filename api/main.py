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
BRONZE_MARKETS_DIR    = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "open")
EXIT_SIGNALS_PATH     = os.path.join(PROJECT_ROOT, "data", "gold", "exit_signals")
LEDGER_PATH           = os.path.join(PROJECT_ROOT, "data", "gold", "position_ledger.parquet")
SETTLED_MARKETS_DIR   = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "settled")
KALSHI_BASE_URL       = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_API_KEY        = os.getenv("KALSHI_API_KEY")
KALSHI_API_SECRET     = os.getenv("KALSHI_API_SECRET", "")
SAFE_MODE             = os.getenv("SAFE_MODE", "true").lower() == "true"
DEFAULT_BANKROLL      = float(os.getenv("BANKROLL_USD", "1000.0"))

app = FastAPI(title="PredictIQ Intelligence API", version="2.0")


# ─────────────────────────────────────────────
# TITLE ENRICHMENT HELPER
# ─────────────────────────────────────────────

def _get_titles_from_history(tickers: list) -> dict:
    """
    Searches all markets_*.parquet files in Bronze to find titles for given tickers.
    Used for expired/settled markets not in latest.parquet.
    """
    if not tickers:
        return {}
    
    titles = {}
    con = duckdb.connect()
    try:
        # 1. Search latest.parquet (fastest)
        latest_path = BRONZE_LATEST_PATH.replace("\\", "/")
        if os.path.exists(BRONZE_LATEST_PATH):
            df_latest = con.execute(f"""
                SELECT ticker, title FROM read_parquet('{latest_path}')
                WHERE ticker IN ({','.join([f"'{t}'" for t in tickers])})
            """).df()
            titles.update(dict(zip(df_latest.ticker, df_latest.title)))
        
        # 2. Search history for remaining missing tickers
        missing = [t for t in tickers if t not in titles]
        if missing:
            history_glob = os.path.join(BRONZE_MARKETS_DIR, "markets_*.parquet").replace("\\", "/")
            # Use try-except block inside query for schema robustness
            df_hist = con.execute(f"""
                SELECT ticker, title FROM read_parquet('{history_glob}', union_by_name=true)
                WHERE ticker IN ({','.join([f"'{t}'" for t in missing])})
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ticker) = 1
            """).df()
            titles.update(dict(zip(df_hist.ticker, df_hist.title)))
            
    except Exception as e:
        print(f"[TITLE HELPER ERROR] {e}")
    finally:
        con.close()

    # 3. Live API Fallback for absolute missing
    final_missing = [t for t in tickers if t not in titles]
    for t in final_missing:
        try:
            print(f"[TITLE FALLBACK] Fetching live title for {t}")
            m_headers = _sign_kalshi_request("GET", f"/trade-api/v2/markets/{t}")
            r = requests.get(f"{KALSHI_BASE_URL}/markets/{t}", headers=m_headers, timeout=5)
            if r.status_code == 200:
                m_title = r.json().get("market", {}).get("title")
                if m_title:
                    titles[t] = m_title
        except Exception as e:
            print(f"[TITLE FALLBACK ERROR] {t}: {e}")

    return titles


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

        # rag_score was added after initial deployment — existing parquets lack the column.
        # union_by_name=true only merges schemas of files that HAVE the column, so if none
        # do yet the column simply doesn't exist. Check schema first and fall back to NULL.
        has_rag_score = False
        try:
            sd = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_glob}', union_by_name=true) LIMIT 1").df()
            has_rag_score = 'rag_score' in sd['column_name'].str.lower().tolist()
        except Exception:
            pass
        rag_b  = "b.rag_score,"             if has_rag_score else "NULL::DOUBLE AS rag_score,"
        rag_nb = "rag_score,"               if has_rag_score else "NULL::DOUBLE AS rag_score,"

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
                    b.decision_reason,
                    b.confidence_score,
                    {rag_b}
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
                    WHERE ingested_at >= CURRENT_TIMESTAMP - INTERVAL '36 hours'
                      AND verdict NOT LIKE '%Error%'
                      AND verdict NOT LIKE '%GHOST PUMP%'
                      AND verdict IS NOT NULL
                      AND verdict != ''
                      AND LOWER(TRIM(verdict)) NOT IN ('neutral', 'n/a', 'no signal', 'insufficient data', 'no trade')
                      AND LENGTH(TRIM(verdict)) > 5
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
                    decision_reason,
                    confidence_score,
                    {rag_nb}
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
                    WHERE ingested_at >= CURRENT_TIMESTAMP - INTERVAL '36 hours'
                      AND verdict NOT LIKE '%Error%'
                      AND verdict NOT LIKE '%GHOST PUMP%'
                      AND verdict IS NOT NULL
                      AND verdict != ''
                      AND LOWER(TRIM(verdict)) NOT IN ('neutral', 'n/a', 'no signal', 'insufficient data', 'no trade')
                      AND LENGTH(TRIM(verdict)) > 5
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
        # Derive recommended_side before Kelly so Kelly uses the correct side price.
        # The LLM prompt always instructs the model to write "Buy YES" or "Buy NO".
        verdict_lower = str(row.get("verdict", "")).lower()
        if "buy no" in verdict_lower:
            recommended_side = "no"
        elif "buy yes" in verdict_lower:
            recommended_side = "yes"
        else:
            # Final fallback: negative odds_delta means market overpriced → buy NO
            recommended_side = "no" if float(row.get("odds_delta", 0) or 0) < -0.05 else "yes"

        # Use live price if available (from latest.parquet JOIN), otherwise fall back to
        # the stale brief price. Live price is more accurate for edge detection.
        live_yes_bid_raw = row.get("live_yes_bid")
        live_yes_bid = float(live_yes_bid_raw) if live_yes_bid_raw is not None and not pd.isna(live_yes_bid_raw) else None
        current_yes_bid = live_yes_bid if live_yes_bid is not None else yes_bid

        # Calibration guard: LLM must beat market by at least MIN_EDGE percentage points (not
        # just any positive margin), accounting for LLM overconfidence. Mirror of predict_movements.
        MIN_EDGE = 0.10
        market_prob = current_yes_bid if recommended_side == "yes" else (1.0 - current_yes_bid)
        if confidence < market_prob + MIN_EDGE:
            continue  # edge too thin to trust given LLM calibration

        # Kelly must be computed on the actual side being purchased.
        # For NO bets: price = 1 - yes_bid (how much NO costs per contract).
        if recommended_side == "no":
            kelly = calculate_kelly(confidence, 1.0 - current_yes_bid, active_bankroll)
        else:
            kelly = calculate_kelly(confidence, current_yes_bid, active_bankroll)
        briefs.append({
            "ticker":            str(row.get("ticker", "")),
            "title":             str(row.get("title", "")),
            "current_odds":      yes_bid,
            "odds_delta":        float(row.get("odds_delta", 0) or 0),
            "mispricing_score":  float(row.get("mispricing_score", 0) or 0),
            "bull_case":         str(row.get("bull_case", "")),
            "bear_case":         str(row.get("bear_case", "")),
            "verdict":           str(row.get("verdict", "")),
            "decision_reason":   str(row.get("decision_reason", "") or ""),
            "recommended_side":  recommended_side,
            "confidence_score":  confidence,
            "rag_score":         float(row["rag_score"]) if row.get("rag_score") is not None and not pd.isna(row.get("rag_score")) else None,
            "ingested_at":       str(row.get("ingested_at", "")),
            "kelly":             kelly,
            "safe_mode":         SAFE_MODE,
            "live_yes_bid":      live_yes_bid,
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
# EXIT SIGNALS — read latest exit_evaluator output
# ─────────────────────────────────────────────

@app.get("/api/exits")
def get_exit_signals():
    """
    Returns the latest exit recommendations for all open positions.
    Generated by inference/exit_evaluator.py each ETL cycle.
    Joins with current ledger entries to surface entry context.
    """
    if not os.path.exists(EXIT_SIGNALS_PATH):
        return {"signals": [], "as_of": None}

    files = [f for f in os.listdir(EXIT_SIGNALS_PATH) if f.endswith(".parquet")]
    if not files:
        return {"signals": [], "as_of": None}

    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(EXIT_SIGNALS_PATH, f)))
    latest_path = os.path.join(EXIT_SIGNALS_PATH, latest_file).replace("\\", "/")
    ledger_path = LEDGER_PATH.replace("\\", "/") if os.path.exists(LEDGER_PATH) else None

    con = duckdb.connect()
    try:
        if ledger_path:
            df = con.execute(f"""
                SELECT
                    s.*,
                    l.entry_time, l.cost_basis, l.entry_confidence,
                    l.entry_thesis, l.original_odds, l.brief_id
                FROM read_parquet('{latest_path}') s
                LEFT JOIN read_parquet('{ledger_path}') l
                  ON s.ticker = l.ticker AND s.side = l.side
                ORDER BY
                    CASE WHEN s.action = 'HOLD' THEN 1 ELSE 0 END,
                    s.urgency DESC,
                    s.unrealized_pnl DESC
            """).df()
        else:
            df = con.execute(f"""
                SELECT * FROM read_parquet('{latest_path}')
                ORDER BY
                    CASE WHEN action = 'HOLD' THEN 1 ELSE 0 END,
                    urgency DESC
            """).df()
        
        # Enrich with titles from history
        tickers = df["ticker"].unique().tolist()
        title_map = _get_titles_from_history(tickers)
        df["title"] = df["ticker"].map(title_map)
        
    except Exception as e:
        print(f"[API EXITS ERROR] {e}")
        return {"signals": [], "error": str(e)}
    finally:
        con.close()


    mtime = os.path.getmtime(os.path.join(EXIT_SIGNALS_PATH, latest_file))
    as_of = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()

    # Replace any NaN/None with JSON-safe values
    df = df.where(pd.notna(df), None)

    signals = df.to_dict(orient="records")

    # Enrich with fresh Kalshi prices (the parquet may be minutes old — prices move).
    # At most ~10 open positions so this is cheap: one API call per ticker.
    tickers = list({s["ticker"] for s in signals if s.get("ticker")})
    for ticker in tickers:
        try:
            mkt_headers = _sign_kalshi_request("GET", f"/trade-api/v2/markets/{ticker}")
            r = requests.get(f"{KALSHI_BASE_URL}/markets/{ticker}", headers=mkt_headers, timeout=6)
            if not r.ok:
                continue
            m = r.json().get("market", {})
            yes_bid = float(m.get("yes_bid_dollars") or 0)
            yes_ask = float(m.get("yes_ask_dollars") or 0)
            for s in signals:
                if s.get("ticker") != ticker:
                    continue
                side     = s.get("side", "yes")
                current  = yes_bid if side == "yes" else (1.0 - yes_ask)
                entry    = float(s.get("entry_price") or 0)
                qty      = float(s.get("qty") or 0)
                pnl_per  = current - entry
                max_gain = 1.0 - entry
                s["current_price"]  = round(current, 4)
                s["unrealized_pnl"] = round(pnl_per * qty, 4)
                s["capture_pct"]    = round(pnl_per / max_gain, 4) if max_gain > 0 else 0.0
        except Exception:
            pass  # stale parquet values remain if API call fails

    return {
        "signals":     signals,
        "count":       len(signals),
        "as_of":       as_of,
        "actionable":  sum(1 for s in signals if s.get("action") != "HOLD"),
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
        data = r.json()
        
        # Enrich with titles from full history
        positions = data.get("market_positions", data.get("positions", []))
        if positions:
            tickers = [p["ticker"] for p in positions]
            title_map = _get_titles_from_history(tickers)
            for p in positions:
                p["title"] = title_map.get(p["ticker"])
        
        return data
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))



@app.get("/api/portfolio/fills")
def get_portfolio_fills(limit: int = 200):
    """Fetches fill history to compute cost basis for P&L."""
    headers = _sign_kalshi_request("GET", "/trade-api/v2/portfolio/fills")
    try:
        r = requests.get(f"{KALSHI_BASE_URL}/portfolio/fills",
                         params={"limit": limit},
                         headers=headers, timeout=10)
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
# BACKTEST ENDPOINT
# ─────────────────────────────────────────────

@app.get("/api/backtest")
def get_backtest(days: int = 30, sim_bankroll: float = 1000.0, current_system_only: bool = False):
    """
    Simulated backtest over all historical intelligence briefs.

    For each unique brief (one per ticker, highest confidence):
      - Joins against settled markets → WIN/LOSS outcome
      - Falls back to latest.parquet for open positions → OPEN with paper P&L
      - Computes Kelly-sized simulated P&L at a $1000 sim_bankroll

    current_system_only=True applies the SAME gates the live inference + API enforce today:
      - yes_bid must be in [0.25, 0.75]   (candidate price filter)
      - confidence > market-implied prob for the recommended side (edge pre-check)
    This excludes pre-guardrail historical artifacts so you see only what the CURRENT
    pipeline would have written.
    """
    if not os.path.exists(BRIEFS_PATH):
        return {"rows": [], "stats": {}}

    con = duckdb.connect()
    try:
        parquet_glob   = os.path.join(BRIEFS_PATH, "*.parquet").replace("\\", "/")
        bronze_latest  = BRONZE_LATEST_PATH.replace("\\", "/")
        settled_glob   = os.path.join(SETTLED_MARKETS_DIR, "*.parquet").replace("\\", "/")

        # Load all valid briefs — one per ticker (highest confidence)
        briefs_df = con.execute(f"""
            SELECT ticker, title, recommended_side, current_odds, confidence_score,
                   verdict, ingested_at, mispricing_score
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker
                           ORDER BY confidence_score DESC, ingested_at DESC
                       ) AS rn
                FROM read_parquet('{parquet_glob}', union_by_name=true)
                WHERE CAST(ingested_at AS DATE) >= CURRENT_DATE - INTERVAL {days} DAY
                  AND verdict NOT LIKE '%Error%'
                  AND verdict IS NOT NULL AND verdict != ''
                  AND recommended_side IN ('yes', 'no')
            )
            WHERE rn = 1
        """).df()

        if briefs_df.empty:
            return {"rows": [], "stats": {"total": 0}}

        tickers_sql = ",".join([f"'{t}'" for t in briefs_df["ticker"].tolist()])

        # Settled outcomes
        settled_df = pd.DataFrame(columns=["ticker", "result"])
        settled_files = [f for f in os.listdir(SETTLED_MARKETS_DIR) if f.endswith(".parquet")] if os.path.exists(SETTLED_MARKETS_DIR) else []
        if settled_files:
            try:
                settled_df = con.execute(f"""
                    SELECT ticker, LOWER(result) AS result
                    FROM read_parquet('{settled_glob}', union_by_name=true)
                    WHERE ticker IN ({tickers_sql})
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ingested_at DESC) = 1
                """).df()
            except Exception:
                pass

        # Live prices for open positions
        live_df = pd.DataFrame(columns=["ticker", "live_yes_bid"])
        if os.path.exists(BRONZE_LATEST_PATH):
            try:
                live_df = con.execute(f"""
                    SELECT ticker, yes_bid_dollars AS live_yes_bid
                    FROM read_parquet('{bronze_latest}')
                    WHERE ticker IN ({tickers_sql})
                """).df()
            except Exception:
                pass

    except Exception as e:
        print(f"[BACKTEST ERROR] {e}")
        return {"rows": [], "stats": {}, "error": str(e)}
    finally:
        con.close()

    settled_map = dict(zip(settled_df["ticker"], settled_df["result"])) if not settled_df.empty else {}
    live_map    = dict(zip(live_df["ticker"], live_df["live_yes_bid"])) if not live_df.empty else {}

    rows = []
    excluded = 0
    for _, b in briefs_df.iterrows():
        ticker     = b["ticker"]
        side       = b["recommended_side"]          # "yes" or "no"
        entry_yes  = float(b["current_odds"])        # YES bid when brief was written
        confidence = float(b["confidence_score"])
        entry_price = entry_yes if side == "yes" else (1.0 - entry_yes)  # cost of the contract we'd buy

        # Apply current-pipeline gates if requested (drops pre-guardrail briefs).
        # Mirrors predict_movements.py + the /api/intelligence guard.
        if current_system_only:
            if entry_yes < 0.25 or entry_yes > 0.75:
                excluded += 1
                continue
            market_prob = entry_yes if side == "yes" else (1.0 - entry_yes)
            if confidence < market_prob + 0.10:  # +10pp calibration guard
                excluded += 1
                continue

        # Outcome resolution
        result = settled_map.get(ticker)   # "yes", "no", or None
        live_yes = live_map.get(ticker)

        if result is not None:
            # Market has settled
            won = (result == side)
            outcome = "WIN" if won else "LOSS"
            pnl_per = (1.0 - entry_price) if won else -entry_price
            current_price = 1.0 if result == "yes" else 0.0  # final YES value
        elif live_yes is not None:
            outcome = "OPEN"
            live_yes = float(live_yes)
            current_price = live_yes if side == "yes" else (1.0 - live_yes)
            pnl_per = current_price - entry_price
        else:
            outcome = "UNKNOWN"
            current_price = None
            pnl_per = None

        # Kelly-sized simulated P&L
        sim_pnl = None
        kelly_info = calculate_kelly(confidence, entry_price, sim_bankroll)
        if pnl_per is not None and kelly_info["edge_detected"]:
            contracts = int(kelly_info["suggested_bet_usd"] / entry_price) if entry_price > 0 else 0
            sim_pnl = round(contracts * pnl_per, 2)
        elif pnl_per is not None:
            # No edge per Kelly, but still show flat-$10 simulation so the row has data
            contracts = int(10.0 / entry_price) if entry_price > 0 else 0
            sim_pnl = round(contracts * pnl_per, 2)

        rows.append({
            "ticker":          ticker,
            "title":           str(b.get("title", "")),
            "brief_date":      str(b["ingested_at"])[:10],
            "recommended_side": side,
            "entry_yes_bid":   round(entry_yes, 4),
            "entry_price":     round(entry_price, 4),
            "confidence":      round(confidence, 4),
            "outcome":         outcome,
            "current_price":   round(current_price, 4) if current_price is not None else None,
            "pnl_per_contract": round(pnl_per, 4) if pnl_per is not None else None,
            "kelly_edge":      kelly_info["edge_detected"],
            "kelly_fraction":  kelly_info["kelly_fraction"],
            "sim_pnl":         sim_pnl,
            "verdict":         str(b.get("verdict", ""))[:120],
        })

    # Aggregate stats
    resolved = [r for r in rows if r["outcome"] in ("WIN", "LOSS")]
    wins     = [r for r in resolved if r["outcome"] == "WIN"]
    open_pos = [r for r in rows if r["outcome"] == "OPEN"]
    with_pnl = [r for r in rows if r["sim_pnl"] is not None]

    stats = {
        "total":            len(rows),
        "resolved":         len(resolved),
        "wins":             len(wins),
        "losses":           len(resolved) - len(wins),
        "open":             len(open_pos),
        "unknown":          len(rows) - len(resolved) - len(open_pos),
        "hit_rate":         round(len(wins) / len(resolved), 4) if resolved else None,
        "sim_total_pnl":    round(sum(r["sim_pnl"] for r in with_pnl), 2),
        "sim_open_pnl":     round(sum(r["sim_pnl"] for r in open_pos if r["sim_pnl"] is not None), 2),
        "sim_resolved_pnl": round(sum(r["sim_pnl"] for r in resolved if r["sim_pnl"] is not None), 2),
        "sim_bankroll":     sim_bankroll,
        "days":             days,
        "current_system_only": current_system_only,
        "excluded":         excluded,
    }

    return {"rows": sorted(rows, key=lambda r: r["brief_date"], reverse=True), "stats": stats}


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