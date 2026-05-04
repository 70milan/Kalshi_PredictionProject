"""
PredictIQ - FastAPI Bridge Server
====================================
Serves intelligence briefs from the Gold Lakehouse to the React frontend.
Handles trade execution (with Safe Mode guard) via Kalshi API.
"""

import os
import sys
import json
import time
import hashlib
import requests
import duckdb
import glob
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import base64

# Ensure api/ directory is on sys.path for local imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kelly_math import calculate_kelly

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
PROJECT_ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRIEFS_PATH      = os.path.join(PROJECT_ROOT, "data", "gold", "intelligence_briefs")
SIMULATED_TRADES_PATH = os.path.join(PROJECT_ROOT, "data", "gold", "simulated_trades.csv")
KALSHI_BASE_URL  = "https://trading-api.kalshi.com/trade-api/v2"
KALSHI_API_KEY   = os.getenv("KALSHI_API_KEY")
KALSHI_API_SECRET= os.getenv("KALSHI_API_SECRET", "")
SAFE_MODE        = os.getenv("SAFE_MODE", "true").lower() == "true"   # Default: ON
DEFAULT_BANKROLL = float(os.getenv("BANKROLL_USD", "1000.0"))

app = FastAPI(title="PredictIQ Intelligence API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# KALSHI AUTH HELPER
# ─────────────────────────────────────────────
def _sign_kalshi_request(method: str, path: str) -> dict:
    """Generates RSA-signed headers for Kalshi API."""
    ts_ms = str(int(time.time() * 1000))
    msg_string = ts_ms + method.upper() + path
    # Clean the key string
    secret = KALSHI_API_SECRET.replace("\\n", "\n")
    private_key = serialization.load_pem_private_key(secret.encode(), password=None)
    signature = private_key.sign(msg_string.encode("utf-8"), padding.PKCS1v15(), hashes.SHA256())
    return {
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "Content-Type": "application/json",
    }

# ─────────────────────────────────────────────
# INTELLIGENCE ENDPOINT
# ─────────────────────────────────────────────
@app.get("/api/intelligence")
def get_intelligence():
    """Returns the latest unique AI Intelligence Briefs joined with Kelly sizing."""
    if not os.path.exists(BRIEFS_PATH):
        return {"briefs": [], "safe_mode": SAFE_MODE}

    con = duckdb.connect()
    try:
        con.execute("INSTALL delta; LOAD delta;")
        query = f"""
            SELECT
                ticker, title, current_odds, odds_delta,
                bull_case, bear_case, verdict, confidence_score,
                ingested_at
            FROM delta_scan('{BRIEFS_PATH}')
            WHERE verdict NOT LIKE '%GHOST PUMP%'
              AND verdict NOT LIKE '%Error%'
            ORDER BY odds_delta DESC
            LIMIT 50
        """
        df = con.execute(query).df()
    except Exception as e:
        print(f"[API ERROR] {e}")
        return {"briefs": [], "safe_mode": SAFE_MODE, "error": str(e)}
    finally:
        con.close()

    briefs = []
    for _, row in df.iterrows():
        yes_bid = float(row.get("current_odds", 0.5) or 0.5)
        confidence = float(row.get("confidence_score", 0.0) or 0.0)
        kelly = calculate_kelly(confidence, yes_bid, DEFAULT_BANKROLL)
        briefs.append({
            "ticker": row["ticker"],
            "title": row["title"],
            "current_odds": yes_bid,
            "odds_delta": float(row.get("odds_delta", 0) or 0),
            "bull_case": row.get("bull_case", ""),
            "bear_case": row.get("bear_case", ""),
            "verdict": row.get("verdict", ""),
            "confidence_score": confidence,
            "ingested_at": str(row.get("ingested_at", "")),
            "kelly": kelly,
            "safe_mode": SAFE_MODE,
        })

    return {"briefs": briefs, "safe_mode": SAFE_MODE, "bankroll": DEFAULT_BANKROLL}


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
# TRADE EXECUTION ENDPOINT
# ─────────────────────────────────────────────
class TradeRequest(BaseModel):
    ticker: str
    side: str         # "yes" or "no"
    count: int        # number of contracts
    price_cents: int  # limit price in cents (e.g. 30 = 30¢)

@app.post("/api/trade")
def execute_trade(trade: TradeRequest):
    """Submits a trade to Kalshi. Guard-railed by SAFE_MODE."""
    if SAFE_MODE:
        # ─────────────────────────────────────────────
        # LOG SIMULATED TRADE
        # ─────────────────────────────────────────────
        file_exists = os.path.isfile(SIMULATED_TRADES_PATH)
        with open(SIMULATED_TRADES_PATH, "a", encoding="utf-8") as f:
            if not file_exists:
                f.write("timestamp,ticker,side,count,price_cents,total_risk_usd\n")
            
            ts = datetime.now(timezone.utc).isoformat()
            risk = (trade.count * trade.price_cents) / 100.0
            f.write(f"{ts},{trade.ticker},{trade.side},{trade.count},{trade.price_cents},{risk:.2f}\n")

        print(f"[SAFE MODE] Logged simulated trade: {trade.count}x {trade.ticker} @ {trade.price_cents}¢")
        
        return {
            "status": "SAFE_MODE_LOGGED",
            "message": f"Trade recorded in {os.path.basename(SIMULATED_TRADES_PATH)}",
            "trade": trade.dict()
        }

    path = "/portfolio/orders"
    headers = _sign_kalshi_request("POST", f"/trade-api/v2{path}")
    order_payload = {
        "ticker": trade.ticker,
        "client_order_id": hashlib.md5(f"{trade.ticker}-{time.time()}".encode()).hexdigest(),
        "type": "limit",
        "action": "buy",
        "side": trade.side,
        "count": trade.count,
        "yes_price": trade.price_cents if trade.side == "yes" else (100 - trade.price_cents),
        "no_price": (100 - trade.price_cents) if trade.side == "yes" else trade.price_cents,
    }

    try:
        r = requests.post(f"{KALSHI_BASE_URL}{path}", headers=headers,
                          data=json.dumps(order_payload), timeout=10)
        r.raise_for_status()
        return {"status": "SUBMITTED", "response": r.json()}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/api/health")
def health():
    return {"status": "ok", "safe_mode": SAFE_MODE, "ts": datetime.now(timezone.utc).isoformat()}
