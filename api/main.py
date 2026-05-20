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
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import base64

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kelly_math import calculate_kelly
from risk_guardrails import apply_theme_caps

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
_KALSHI_LIVE_URL      = "https://api.elections.kalshi.com/trade-api/v2"
_KALSHI_DEMO_URL      = "https://demo-api.kalshi.co/trade-api/v2"
_USE_DEMO             = os.getenv("KALSHI_DEMO", "false").lower() == "true"
KALSHI_BASE_URL       = _KALSHI_DEMO_URL if _USE_DEMO else _KALSHI_LIVE_URL
KALSHI_API_KEY        = os.getenv("KALSHI_API_KEY")
KALSHI_API_SECRET     = os.getenv("KALSHI_API_SECRET", "")
SAFE_MODE             = os.getenv("SAFE_MODE", "true").lower() == "true"
READONLY_MODE         = os.getenv("READONLY_MODE", "false").lower() == "true"
DEFAULT_BANKROLL      = float(os.getenv("BANKROLL_USD", "500.0"))

def _is_public_request(request) -> bool:
    """Tailscale Funnel proxies public internet traffic as 127.0.0.1.
    Direct Tailscale device connections arrive as 100.x.x.x.
    Public requests are always read-only."""
    return request.client.host == "127.0.0.1"

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
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Required for browsers accessing a Tailscale (private-range) IP from a public origin (GitHub Pages)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest

class PrivateNetworkAccessMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        response.headers["Access-Control-Allow-Private-Network"] = "true"
        return response

app.add_middleware(PrivateNetworkAccessMiddleware)


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
                    m.yes_ask_dollars AS live_yes_ask,
                    m.close_time AS close_time
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticker
                            ORDER BY confidence_score DESC, ingested_at DESC
                        ) AS rn
                    FROM read_parquet('{parquet_glob}', union_by_name=true)
                    WHERE TRY_CAST(ingested_at AS TIMESTAMPTZ) >= CURRENT_TIMESTAMP - INTERVAL '36 hours'
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
                    NULL AS live_yes_ask,
                    NULL AS close_time
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticker
                            ORDER BY confidence_score DESC, ingested_at DESC
                        ) AS rn
                    FROM read_parquet('{parquet_glob}', union_by_name=true)
                    WHERE TRY_CAST(ingested_at AS TIMESTAMPTZ) >= CURRENT_TIMESTAMP - INTERVAL '36 hours'
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

    # Calibration sample for Kelly sizing: how many briefed markets have resolved.
    # Until this reaches MIN_CALIBRATION_SAMPLES, kelly_math clamps every bet flat
    # (fix A). Probe fails safe to 0 — staying clamped is the conservative default.
    resolved_count = 0
    try:
        settled_present = os.path.exists(SETTLED_MARKETS_DIR) and any(
            f.endswith(".parquet") for f in os.listdir(SETTLED_MARKETS_DIR)
        )
        if settled_present:
            settled_glob_r = os.path.join(SETTLED_MARKETS_DIR, "*.parquet").replace("\\", "/")
            briefs_glob_r  = os.path.join(BRIEFS_PATH, "*.parquet").replace("\\", "/")
            con_r = duckdb.connect()
            resolved_count = int(con_r.execute(f"""
                SELECT COUNT(DISTINCT s.ticker)
                FROM read_parquet('{settled_glob_r}', union_by_name=true) s
                WHERE s.ticker IN (
                    SELECT DISTINCT ticker
                    FROM read_parquet('{briefs_glob_r}', union_by_name=true)
                )
            """).fetchone()[0])
            con_r.close()
    except Exception as e:
        print(f"[API] resolved-count probe failed (staying clamped): {e}")

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

        # Days until the market resolves — feeds the short-dated haircut (fix E).
        close_time_raw = row.get("close_time")
        days_to_resolution = None
        if close_time_raw is not None and not pd.isna(close_time_raw):
            try:
                ct = pd.to_datetime(close_time_raw, utc=True)
                days_to_resolution = (ct - pd.Timestamp.now(tz="UTC")).total_seconds() / 86400.0
            except Exception:
                days_to_resolution = None

        # Kelly must be computed on the actual side being purchased.
        # For NO bets: price = 1 - yes_bid (how much NO costs per contract).
        # resolved_count + days_to_resolution drive the calibration cap and the
        # short-dated haircut inside calculate_kelly (fixes A, E).
        side_price = (1.0 - current_yes_bid) if recommended_side == "no" else current_yes_bid
        kelly = calculate_kelly(confidence, side_price, active_bankroll,
                                resolved_count=resolved_count,
                                days_to_resolution=days_to_resolution)
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

    # Fix C: cap total exposure per correlated theme. Without this, two Powell
    # bets each at the per-trade cap stacked to ~10% of bankroll on one event.
    briefs = apply_theme_caps(briefs, active_bankroll)

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
        "resolved_count":       resolved_count,
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
    action:        str = "buy"  # "buy" or "sell"


@app.post("/api/trade")
def execute_trade(trade: TradeRequest, request: Request):
    """Submits a trade to Kalshi. Guard-railed by SAFE_MODE."""
    if READONLY_MODE or _is_public_request(request):
        raise HTTPException(status_code=403, detail="Read-only mode — trading disabled.")
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
        "action":           trade.action,
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



@app.get("/api/paper/positions")
def get_paper_positions():
    """
    Live paper-trading positions computed on-the-fly from simulated_trades.csv
    and the latest bronze snapshot. No ETL cycle needed — always current.
    """
    if not os.path.exists(SIMULATED_TRADES_PATH):
        return {"positions": [], "summary": {"total_pnl": 0, "open": 0}}

    try:
        trades_df = pd.read_csv(SIMULATED_TRADES_PATH)
    except Exception:
        return {"positions": [], "summary": {"total_pnl": 0, "open": 0}}

    if trades_df.empty:
        return {"positions": [], "summary": {"total_pnl": 0, "open": 0}}

    # Load current prices from the bronze latest snapshot (updated ~every 2 min by ingestor)
    price_map = {}
    if os.path.exists(BRONZE_LATEST_PATH):
        try:
            con = duckdb.connect()
            latest = con.execute(f"""
                SELECT ticker, yes_bid_dollars, yes_ask_dollars
                FROM read_parquet('{BRONZE_LATEST_PATH.replace(chr(92), "/")}')
            """).df()
            con.close()
            for _, row in latest.iterrows():
                price_map[row["ticker"]] = {
                    "yes_bid": float(row["yes_bid_dollars"]),
                    "yes_ask": float(row["yes_ask_dollars"]),
                }
        except Exception:
            pass

    # Load settled outcomes for closed positions
    settled_map = {}
    if os.path.exists(SETTLED_MARKETS_DIR):
        settled_files = sorted(
            [f for f in os.listdir(SETTLED_MARKETS_DIR) if f.endswith(".parquet")],
            key=lambda f: os.path.getmtime(os.path.join(SETTLED_MARKETS_DIR, f)),
            reverse=True
        )[:30]
        if settled_files:
            try:
                con = duckdb.connect()
                paths_sql = ", ".join(
                    f"'{os.path.join(SETTLED_MARKETS_DIR, f).replace(chr(92), '/')}'"
                    for f in settled_files
                )
                s = con.execute(f"""
                    SELECT ticker, LOWER(result) AS result
                    FROM read_parquet([{paths_sql}])
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ingested_at DESC) = 1
                """).df()
                con.close()
                settled_map = dict(zip(s["ticker"], s["result"]))
            except Exception:
                pass

    title_map = _get_titles_from_history(trades_df["ticker"].unique().tolist())

    positions = []
    total_pnl = 0.0
    for _, trade in trades_df.iterrows():
        ticker     = str(trade["ticker"])
        side       = str(trade["side"])
        qty        = int(trade["count"])
        entry_price = float(trade["price_dollars"])
        cost_basis  = round(qty * entry_price, 2)
        ts          = str(trade.get("timestamp", ""))[:10]

        current_price = None
        unrealized_pnl = None
        status = "OPEN"

        if ticker in settled_map:
            result = settled_map[ticker]
            won = (result == side)
            exit_price = 1.0 if won else 0.0
            pnl = round((exit_price - entry_price) * qty, 2)
            status = "SETTLED_WIN" if won else "SETTLED_LOSS"
            current_price = exit_price
            unrealized_pnl = pnl
        elif ticker in price_map:
            p = price_map[ticker]
            current_price = p["yes_bid"] if side == "yes" else (1.0 - p["yes_ask"])
            unrealized_pnl = round((current_price - entry_price) * qty, 2)
            # Check TP/SL against current price
            roi = (current_price - entry_price) / entry_price if entry_price > 0 else 0
            from inference.exit_evaluator import TAKE_PROFIT_ROI, STOP_LOSS_ROI
            if roi >= TAKE_PROFIT_ROI:
                status = "TP_READY"
            elif roi <= -STOP_LOSS_ROI:
                status = "SL_HIT"

        total_pnl += unrealized_pnl or 0
        roi_pct = round((current_price - entry_price) / entry_price * 100, 1) if current_price is not None else None

        positions.append({
            "ticker":        ticker,
            "title":         title_map.get(ticker, ticker),
            "side":          side,
            "qty":           qty,
            "entry_price":   round(entry_price, 4),
            "current_price": round(current_price, 4) if current_price is not None else None,
            "cost_basis":    cost_basis,
            "unrealized_pnl": unrealized_pnl,
            "roi_pct":       roi_pct,
            "status":        status,
            "entered_at":    ts,
        })

    positions.sort(key=lambda p: (0 if p["status"] in ("SL_HIT", "TP_READY") else 1, p.get("roi_pct") or 0))

    return {
        "positions": positions,
        "summary": {
            "total_pnl":   round(total_pnl, 2),
            "open":        sum(1 for p in positions if p["status"] == "OPEN"),
            "tp_ready":    sum(1 for p in positions if p["status"] == "TP_READY"),
            "sl_hit":      sum(1 for p in positions if p["status"] == "SL_HIT"),
            "settled_win": sum(1 for p in positions if p["status"] == "SETTLED_WIN"),
            "settled_loss": sum(1 for p in positions if p["status"] == "SETTLED_LOSS"),
            "bankroll":    DEFAULT_BANKROLL,
            "as_of":       datetime.now(timezone.utc).isoformat(),
        }
    }


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
def cancel_orders_for_ticker(ticker: str, request: Request):
    """Finds and cancels all resting orders for a given ticker."""
    if READONLY_MODE or _is_public_request(request):
        raise HTTPException(status_code=403, detail="Read-only mode — cancellation disabled.")
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

        # Settled outcomes — cap to 50 most recent files to avoid slow glob over thousands
        settled_df = pd.DataFrame(columns=["ticker", "result"])
        settled_files = [f for f in os.listdir(SETTLED_MARKETS_DIR) if f.endswith(".parquet")] if os.path.exists(SETTLED_MARKETS_DIR) else []
        if settled_files:
            settled_files_recent = sorted(
                settled_files,
                key=lambda f: os.path.getmtime(os.path.join(SETTLED_MARKETS_DIR, f)),
                reverse=True
            )[:50]
            recent_paths_sql = ", ".join(
                f"'{os.path.join(SETTLED_MARKETS_DIR, f).replace(chr(92), '/')}'"
                for f in settled_files_recent
            )
            try:
                settled_df = con.execute(f"""
                    SELECT ticker, LOWER(result) AS result
                    FROM read_parquet([{recent_paths_sql}], union_by_name=true)
                    WHERE ticker IN ({tickers_sql})
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ingested_at DESC) = 1
                """).df()
            except Exception:
                pass

        # Live prices for open positions
        live_df = pd.DataFrame(columns=["ticker", "live_yes_bid", "close_time"])
        if os.path.exists(BRONZE_LATEST_PATH):
            try:
                live_df = con.execute(f"""
                    SELECT ticker, yes_bid_dollars AS live_yes_bid, close_time
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
    close_map   = (dict(zip(live_df["ticker"], live_df["close_time"]))
                   if not live_df.empty and "close_time" in live_df.columns else {})

    # Calibration sample for fix A: resolved markets the system briefed.
    resolved_count = len(settled_map)

    # Pass 1: resolve each brief's outcome and size it with Kelly (guardrails on).
    staged = []
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
        days_to_resolution = None

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
            ct = close_map.get(ticker)
            if ct is not None and not pd.isna(ct):
                try:
                    ctp = pd.to_datetime(ct, utc=True)
                    days_to_resolution = (ctp - pd.Timestamp.now(tz="UTC")).total_seconds() / 86400.0
                except Exception:
                    days_to_resolution = None
        else:
            outcome = "UNKNOWN"
            current_price = None
            pnl_per = None

        kelly_info = calculate_kelly(confidence, entry_price, sim_bankroll,
                                     resolved_count=resolved_count,
                                     days_to_resolution=days_to_resolution)

        staged.append({
            "ticker":        ticker,
            "title":         str(b.get("title", "")),
            "kelly":         kelly_info,
            "side":          side,
            "entry_yes":     entry_yes,
            "entry_price":   entry_price,
            "confidence":    confidence,
            "outcome":       outcome,
            "current_price": current_price,
            "pnl_per":       pnl_per,
            "brief_date":    str(b["ingested_at"])[:10],
            "verdict":       str(b.get("verdict", ""))[:120],
        })

    # Fix C: cap simulated exposure per correlated theme before computing P&L.
    staged = apply_theme_caps(staged, sim_bankroll)

    # Pass 2: simulated P&L from the guardrail- and theme-capped Kelly bet.
    rows = []
    for s in staged:
        kelly_info  = s["kelly"]
        entry_price = s["entry_price"]
        pnl_per     = s["pnl_per"]

        sim_pnl = None
        if pnl_per is not None:
            if kelly_info["kelly_fraction"] > 0:
                # Had a Kelly edge — size from the (guardrail/theme) capped bet.
                # A theme-zeroed bet correctly sims $0 here.
                contracts = int(kelly_info["suggested_bet_usd"] / entry_price) if entry_price > 0 else 0
            else:
                # No edge — flat-$10 reference simulation so the row still has data.
                contracts = int(10.0 / entry_price) if entry_price > 0 else 0
            sim_pnl = round(contracts * pnl_per, 2)

        rows.append({
            "ticker":          s["ticker"],
            "title":           s["title"],
            "brief_date":      s["brief_date"],
            "recommended_side": s["side"],
            "entry_yes_bid":   round(s["entry_yes"], 4),
            "entry_price":     round(entry_price, 4),
            "confidence":      round(s["confidence"], 4),
            "outcome":         s["outcome"],
            "current_price":   round(s["current_price"], 4) if s["current_price"] is not None else None,
            "pnl_per_contract": round(pnl_per, 4) if pnl_per is not None else None,
            "kelly_edge":      kelly_info["edge_detected"],
            "kelly_fraction":  kelly_info["kelly_fraction"],
            "theme":           s.get("theme"),
            "sim_pnl":         sim_pnl,
            "verdict":         s["verdict"],
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
# POSITION STRATEGY BACKTEST
# ─────────────────────────────────────────────

def _kalshi_fee_per_contract(price: float) -> float:
    """Kalshi trading fee formula: 0.07 * P * (1 - P) per contract."""
    return round(0.07 * price * (1.0 - price), 4)


@app.get("/api/backtest/positions")
def get_position_backtest(
    days: int = 30,
    sim_bankroll: float = 1000.0,
    current_system_only: bool = False,
    take_profit: float | None = None,
    stop_loss: float | None = None,
    realistic: bool = False,
    model: str = "all",
):
    """
    Strategy simulation: applies TAKE_PROFIT_ROI / STOP_LOSS_ROI triggers
    to the same signal backtest rows (from intelligence briefs), replaying
    the bronze price history from each brief date forward.

    Defaults to the values in inference.exit_evaluator (env-driven). Pass
    ?take_profit=0.30&stop_loss=0.17 to experiment without touching .env.
    Values are ROI fractions: 0.20 = 20%, 0.40 = 40%, etc.

    ?realistic=true layers three honesty corrections on top:
      - walk-forward only (no peeking at price action before the brief existed)
      - subtract Kalshi fees on entry + exit (0.07 * P * (1-P) per contract)
      - drop "stale briefs" whose stated entry price disagrees with the
        actual market snapshot at brief time by more than 10c
    """
    from inference.exit_evaluator import TAKE_PROFIT_ROI as TP_DEFAULT, STOP_LOSS_ROI as SL_DEFAULT
    TAKE_PROFIT_ROI = float(take_profit) if take_profit is not None else TP_DEFAULT
    STOP_LOSS_ROI   = float(stop_loss)   if stop_loss   is not None else SL_DEFAULT

    if not os.path.exists(BRIEFS_PATH):
        return {"trades": [], "stats": {}}

    con = duckdb.connect()
    try:
        parquet_glob  = os.path.join(BRIEFS_PATH, "*.parquet").replace("\\", "/")
        bronze_glob   = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "open", "*.parquet").replace("\\", "/")

        # Model filter clause
        model_filter = ""
        if model == "production":
            model_filter = "AND (replay_model IS NULL OR replay_model = '')"
        elif model in ("gpt-4o-mini", "gpt-4o"):
            model_filter = f"AND replay_model = '{model}'"

        # One brief per ticker (highest confidence), same window as signal backtest
        briefs_df = con.execute(f"""
            SELECT ticker, title, recommended_side, current_odds, confidence_score, ingested_at
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
                  {model_filter}
            )
            WHERE rn = 1
        """).df()

        if briefs_df.empty:
            return {"trades": [], "stats": {}}

        tickers = briefs_df["ticker"].tolist()
        tickers_sql = ", ".join(f"'{t}'" for t in tickers)

        # Full bronze price history for all signal tickers
        history_df = con.execute(f"""
            SELECT ticker,
                   yes_bid_dollars,
                   yes_ask_dollars,
                   CAST(ingested_at AS TIMESTAMP WITH TIME ZONE) AS ts
            FROM read_parquet('{bronze_glob}', union_by_name=true)
            WHERE ticker IN ({tickers_sql})
            ORDER BY ticker, ts
        """).df()

        # Settled outcomes (50 most recent files)
        settled_df = pd.DataFrame(columns=["ticker", "result"])
        settled_files = [f for f in os.listdir(SETTLED_MARKETS_DIR) if f.endswith(".parquet")] if os.path.exists(SETTLED_MARKETS_DIR) else []
        if settled_files:
            settled_files_recent = sorted(
                settled_files,
                key=lambda f: os.path.getmtime(os.path.join(SETTLED_MARKETS_DIR, f)),
                reverse=True
            )[:50]
            recent_paths_sql = ", ".join(
                f"'{os.path.join(SETTLED_MARKETS_DIR, f).replace(chr(92), '/')}'"
                for f in settled_files_recent
            )
            try:
                settled_df = con.execute(f"""
                    SELECT ticker, LOWER(result) AS result
                    FROM read_parquet([{recent_paths_sql}], union_by_name=true)
                    WHERE ticker IN ({tickers_sql})
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ingested_at DESC) = 1
                """).df()
            except Exception:
                pass

        # Live prices for still-open signals
        live_df = pd.DataFrame(columns=["ticker", "yes_bid_dollars", "yes_ask_dollars"])
        if os.path.exists(BRONZE_LATEST_PATH):
            try:
                live_df = con.execute(f"""
                    SELECT ticker, yes_bid_dollars, yes_ask_dollars
                    FROM read_parquet('{BRONZE_LATEST_PATH.replace(chr(92), "/")}')
                    WHERE ticker IN ({tickers_sql})
                """).df()
            except Exception:
                pass

    except Exception as e:
        print(f"[POSITION BACKTEST ERROR] {e}")
        return {"trades": [], "stats": {}, "error": str(e)}
    finally:
        con.close()

    settled_map = dict(zip(settled_df["ticker"], settled_df["result"])) if not settled_df.empty else {}
    live_map    = {r["ticker"]: r for _, r in live_df.iterrows()} if not live_df.empty else {}

    trades = []
    for _, b in briefs_df.iterrows():
        ticker      = str(b["ticker"])
        side        = str(b["recommended_side"])
        entry_yes   = float(b["current_odds"])
        confidence  = float(b["confidence_score"])
        entry_price = entry_yes if side == "yes" else (1.0 - entry_yes)
        title       = str(b.get("title", ticker))
        brief_date  = str(b["ingested_at"])[:10]

        # Apply current-pipeline gates if requested
        if current_system_only:
            if entry_yes < 0.25 or entry_yes > 0.75:
                continue
            market_prob = entry_yes if side == "yes" else (1.0 - entry_yes)
            if confidence < market_prob + 0.10:
                continue

        # Kelly viability gate — always applied (mirrors live inference filter).
        # Trades where payout ratio is too poor for positive Kelly are excluded;
        # they would be skipped by the pipeline and shouldn't pollute backtest P&L.
        kelly_info = calculate_kelly(confidence, entry_price, sim_bankroll, resolved_count=50)
        if not kelly_info["edge_detected"]:
            continue
        qty = int(kelly_info["suggested_bet_usd"] / entry_price) if entry_price > 0 else 0

        # Walk price history for this ticker. Default: walk ALL snapshots (no time gate);
        # exit at the threshold price the first time +TP% or -SL% is crossed.
        # When realistic=true: only walk post-brief snapshots, AND drop briefs whose
        # stated entry price disagrees with the actual market at brief time by >10c.
        entry_ts = None
        try:
            entry_ts = pd.to_datetime(b["ingested_at"], utc=True)
        except Exception:
            pass

        ticker_hist = history_df[history_df["ticker"] == ticker].copy().sort_values("ts")

        if realistic and entry_ts is not None and not ticker_hist.empty:
            # Stale-brief drop: nearest snapshot (in either direction) within 1h of brief
            diffs = (ticker_hist["ts"] - entry_ts).abs()
            nearest_idx = diffs.idxmin()
            nearest_row = ticker_hist.loc[nearest_idx]
            if abs((nearest_row["ts"] - entry_ts).total_seconds()) / 3600.0 < 1.0:
                actual_yes = float(nearest_row["yes_bid_dollars"])
                if abs(actual_yes - entry_yes) > 0.10:
                    continue  # stale brief — entry price was fiction at brief time

            # Walk-forward filter: only post-brief snapshots count
            ticker_hist = ticker_hist[ticker_hist["ts"] >= entry_ts]

        exit_action = None
        exit_price  = None
        exit_ts     = None
        peak_roi    = 0.0

        for _, snap in ticker_hist.iterrows():
            yes_bid = float(snap["yes_bid_dollars"])
            yes_ask = float(snap["yes_ask_dollars"])
            current_value = yes_bid if side == "yes" else (1.0 - yes_ask)
            roi = (current_value - entry_price) / entry_price if entry_price > 0 else 0.0
            if roi > peak_roi:
                peak_roi = roi

            if roi >= TAKE_PROFIT_ROI:
                exit_action = "PROFIT_EXIT"
                exit_price  = round(entry_price * (1 + TAKE_PROFIT_ROI), 4)
                exit_ts     = snap["ts"]
                break
            elif roi <= -STOP_LOSS_ROI:
                exit_action = "STOP_EXIT"
                exit_price  = round(entry_price * (1 - STOP_LOSS_ROI), 4)
                exit_ts     = snap["ts"]
                break

        settled_result = settled_map.get(ticker)

        if exit_action:
            outcome = exit_action
            pnl     = round((exit_price - entry_price) * qty, 2)
        elif settled_result:
            won     = (settled_result == side)
            outcome = "SETTLED_WIN" if won else "SETTLED_LOSS"
            pnl_per = (1.0 - entry_price) if won else -entry_price
            pnl     = round(pnl_per * qty, 2)
            exit_price = 1.0 if won else 0.0
        else:
            outcome = "OPEN"
            live = live_map.get(ticker)
            if live is not None:
                yes_bid = float(live["yes_bid_dollars"])
                yes_ask = float(live["yes_ask_dollars"])
                current_value = yes_bid if side == "yes" else (1.0 - yes_ask)
                pnl        = round((current_value - entry_price) * qty, 2)
                exit_price = round(current_value, 4)
            else:
                pnl        = None
                exit_price = None

        # Realistic mode: subtract Kalshi fees on entry + exit (only if we have a closed trade).
        # Fee formula: 0.07 * P * (1-P) per contract, charged on both sides.
        if realistic and pnl is not None and exit_price is not None and qty > 0:
            fee_in  = _kalshi_fee_per_contract(entry_price) * qty
            fee_out = _kalshi_fee_per_contract(exit_price)  * qty
            pnl = round(pnl - fee_in - fee_out, 2)

        trades.append({
            "ticker":       ticker,
            "title":        title,
            "side":         side,
            "entry_price":  round(entry_price, 4),
            "entry_yes":    round(entry_yes, 4),
            "confidence":   round(confidence, 4),
            "qty":          qty,
            "brief_date":   brief_date,
            "outcome":      outcome,
            "exit_price":   round(exit_price, 4) if exit_price is not None else None,
            "exit_time":    str(exit_ts)[:10] if exit_ts is not None else None,
            "pnl":          pnl,
            "peak_roi_pct": round(peak_roi * 100, 1),
        })

    trades.sort(key=lambda t: t["brief_date"], reverse=True)

    profit_exits   = [t for t in trades if t["outcome"] == "PROFIT_EXIT"]
    stop_exits     = [t for t in trades if t["outcome"] == "STOP_EXIT"]
    settled_wins   = [t for t in trades if t["outcome"] == "SETTLED_WIN"]
    settled_losses = [t for t in trades if t["outcome"] == "SETTLED_LOSS"]
    open_trades    = [t for t in trades if t["outcome"] == "OPEN"]

    wins   = profit_exits + settled_wins
    losses = stop_exits + settled_losses
    closed = wins + losses
    win_rate = round(len(wins) / len(closed), 4) if closed else None

    with_pnl  = [t for t in trades if t["pnl"] is not None]
    total_pnl = round(sum(t["pnl"] for t in with_pnl), 2)

    return {
        "trades": trades,
        "stats": {
            "total":           len(trades),
            "profit_exits":    len(profit_exits),
            "stop_exits":      len(stop_exits),
            "settled_wins":    len(settled_wins),
            "settled_losses":  len(settled_losses),
            "open":            len(open_trades),
            "wins":            len(wins),
            "losses":          len(losses),
            "win_rate":        win_rate,
            "total_pnl":       total_pnl,
            "take_profit_pct": round(TAKE_PROFIT_ROI * 100),
            "stop_loss_pct":   round(STOP_LOSS_ROI * 100),
            "realistic":       bool(realistic),
        },
    }


# ─────────────────────────────────────────────
# ORACLE STRATEGY BACKTEST
# ─────────────────────────────────────────────

@app.get("/api/backtest/oracle")
def get_oracle_backtest(
    take_profit: float | None = None,
    stop_loss:   float | None = None,
    sim_bankroll: float = 1000.0,
):
    """
    Upper-bound strategy backtest using settled markets as ground truth.

    For every settled market (binary yes/no outcome) that appeared in the
    bronze price snapshots inside the 25-75¢ entry zone, simulates:
      - Entry at the FIRST snapshot it was in range
      - Side = settlement outcome (perfect oracle — always correct direction)
      - TP/SL exit rules applied walking forward from entry

    Purpose: isolates whether the EXIT RULES add value vs. holding to settlement,
    using 200+ settled trades instead of the ~23 from LLM briefs.
    This is an upper bound — real results will be lower because our model is
    not a perfect oracle.
    """
    from inference.exit_evaluator import TAKE_PROFIT_ROI as TP_DEFAULT, STOP_LOSS_ROI as SL_DEFAULT
    TAKE_PROFIT_ROI = float(take_profit) if take_profit is not None else TP_DEFAULT
    STOP_LOSS_ROI   = float(stop_loss)   if stop_loss   is not None else SL_DEFAULT

    if not os.path.exists(SETTLED_MARKETS_DIR):
        return {"trades": [], "stats": {}}

    settled_files = [f for f in os.listdir(SETTLED_MARKETS_DIR) if f.endswith(".parquet")]
    if not settled_files:
        return {"trades": [], "stats": {}}

    con = duckdb.connect()
    try:
        settled_paths_sql = ", ".join(
            f"'{os.path.join(SETTLED_MARKETS_DIR, f).replace(chr(92), '/')}'"
            for f in settled_files
        )
        settled_df = con.execute(f"""
            SELECT ticker, title, LOWER(result) AS result
            FROM read_parquet([{settled_paths_sql}], union_by_name=true)
            WHERE LOWER(result) IN ('yes', 'no')
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ingested_at DESC) = 1
        """).df()

        if settled_df.empty:
            return {"trades": [], "stats": {}}

        tickers_sql  = ", ".join(f"'{t}'" for t in settled_df["ticker"].tolist())
        bronze_glob  = os.path.join(
            PROJECT_ROOT, "data", "bronze", "kalshi_markets", "open", "markets_*.parquet"
        ).replace("\\", "/")

        # First snapshot per ticker where price was in 25-75¢ zone
        entry_df = con.execute(f"""
            WITH ranked AS (
                SELECT ticker,
                       CAST(yes_bid_dollars AS DOUBLE) AS yes_bid,
                       CAST(yes_ask_dollars AS DOUBLE) AS yes_ask,
                       CAST(ingested_at AS TIMESTAMP WITH TIME ZONE) AS ts,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker
                           ORDER BY CAST(ingested_at AS TIMESTAMP WITH TIME ZONE)
                       ) AS rn
                FROM read_parquet('{bronze_glob}', union_by_name=true)
                WHERE ticker IN ({tickers_sql})
                  AND CAST(yes_bid_dollars AS DOUBLE) BETWEEN 0.25 AND 0.75
            )
            SELECT ticker, yes_bid, yes_ask, ts AS entry_ts
            FROM ranked WHERE rn = 1
        """).df()

        if entry_df.empty:
            return {"trades": [], "stats": {}}

        entry_tickers_sql = ", ".join(f"'{t}'" for t in entry_df["ticker"].tolist())
        history_df = con.execute(f"""
            SELECT ticker,
                   CAST(yes_bid_dollars AS DOUBLE) AS yes_bid,
                   CAST(yes_ask_dollars AS DOUBLE) AS yes_ask,
                   CAST(ingested_at AS TIMESTAMP WITH TIME ZONE) AS ts
            FROM read_parquet('{bronze_glob}', union_by_name=true)
            WHERE ticker IN ({entry_tickers_sql})
            ORDER BY ticker, ts
        """).df()

    except Exception as e:
        print(f"[ORACLE BACKTEST ERROR] {e}")
        return {"trades": [], "stats": {}, "error": str(e)}
    finally:
        con.close()

    settled_map = dict(zip(settled_df["ticker"], settled_df["result"]))
    title_map   = dict(zip(settled_df["ticker"], settled_df.get("title", settled_df["ticker"])))
    entry_map   = {r["ticker"]: r for _, r in entry_df.iterrows()}

    trades = []
    for ticker, entry_row in entry_map.items():
        settled_result = settled_map.get(ticker)
        if not settled_result:
            continue

        side        = settled_result          # oracle: always bet correct direction
        entry_yes   = float(entry_row["yes_bid"])
        entry_price = entry_yes if side == "yes" else (1.0 - float(entry_row["yes_ask"]))
        entry_ts    = entry_row["entry_ts"]

        if entry_price <= 0.05 or entry_price >= 0.95:
            continue

        qty = max(1, int((sim_bankroll * 0.05) / entry_price))

        ticker_hist = history_df[history_df["ticker"] == ticker].sort_values("ts")
        ticker_hist = ticker_hist[ticker_hist["ts"] >= entry_ts]

        exit_action = None
        exit_price  = None
        exit_ts     = None
        peak_roi    = 0.0

        for _, snap in ticker_hist.iterrows():
            yes_bid = float(snap["yes_bid"])
            yes_ask = float(snap["yes_ask"])
            current_value = yes_bid if side == "yes" else (1.0 - yes_ask)
            roi = (current_value - entry_price) / entry_price if entry_price > 0 else 0.0
            if roi > peak_roi:
                peak_roi = roi
            if roi >= TAKE_PROFIT_ROI:
                exit_action = "PROFIT_EXIT"
                exit_price  = round(entry_price * (1 + TAKE_PROFIT_ROI), 4)
                exit_ts     = snap["ts"]
                break
            elif roi <= -STOP_LOSS_ROI:
                exit_action = "STOP_EXIT"
                exit_price  = round(entry_price * (1 - STOP_LOSS_ROI), 4)
                exit_ts     = snap["ts"]
                break

        if exit_action:
            outcome = exit_action
            pnl     = round((exit_price - entry_price) * qty, 2)
        else:
            won     = True  # oracle always wins at settlement
            outcome = "SETTLED_WIN"
            pnl     = round((1.0 - entry_price) * qty, 2)
            exit_price = 1.0

        trades.append({
            "ticker":       ticker,
            "title":        title_map.get(ticker, ticker),
            "side":         side,
            "entry_price":  round(entry_price, 4),
            "entry_yes":    round(entry_yes, 4),
            "qty":          qty,
            "entry_date":   str(entry_ts)[:10],
            "outcome":      outcome,
            "exit_price":   round(exit_price, 4) if exit_price is not None else None,
            "exit_date":    str(exit_ts)[:10] if exit_ts is not None else None,
            "pnl":          pnl,
            "peak_roi_pct": round(peak_roi * 100, 1),
        })

    trades.sort(key=lambda t: t.get("entry_date", ""), reverse=True)

    profit_exits = [t for t in trades if t["outcome"] == "PROFIT_EXIT"]
    stop_exits   = [t for t in trades if t["outcome"] == "STOP_EXIT"]
    settled_wins = [t for t in trades if t["outcome"] == "SETTLED_WIN"]

    wins   = profit_exits + settled_wins
    losses = stop_exits
    closed = wins + losses
    win_rate  = round(len(wins)  / len(closed), 4) if closed else None
    total_pnl = round(sum(t["pnl"] for t in trades), 2)
    avg_win   = round(sum(t["pnl"] for t in wins) / len(wins), 2)   if wins   else 0.0
    avg_loss  = round(sum(t["pnl"] for t in losses) / len(losses), 2) if losses else 0.0
    rr        = round(abs(avg_win / avg_loss), 2) if avg_loss and avg_win else None

    return {
        "trades": trades,
        "stats": {
            "total":           len(trades),
            "profit_exits":    len(profit_exits),
            "stop_exits":      len(stop_exits),
            "settled_wins":    len(settled_wins),
            "wins":            len(wins),
            "losses":          len(losses),
            "win_rate":        win_rate,
            "total_pnl":       total_pnl,
            "avg_win":         avg_win,
            "avg_loss":        avg_loss,
            "rr":              rr,
            "take_profit_pct": round(TAKE_PROFIT_ROI * 100),
            "stop_loss_pct":   round(STOP_LOSS_ROI   * 100),
            "note":            "Oracle upper bound — direction always correct. Actual model results will be lower.",
        },
    }


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

@app.get("/api/config")
def get_config(request: Request):
    return {"readonly": READONLY_MODE or _is_public_request(request), "safe_mode": SAFE_MODE}


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