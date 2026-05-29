"""
PredictIQ - Paper Executor V2 (simulated limit fills against REAL prod bid/ask)
==============================================================================
Why this exists: the Kalshi DEMO exchange has ~no liquidity (probed 1,872 markets,
7 tradeable, all 0.02/0.98 spreads), so real demo limit orders never fill. Instead
we simulate limit-order fills against the REAL prod bid/ask we already ingest every
~5 min. This is honest (an order fills ONLY when the real market's ask actually
reaches our limit price) and fixes the original market-order spread bug by
construction — entry is recorded at a realistic fill price, marks use the bid.

Two arms write here with the SAME selection + SAME execution, differing ONLY in
direction source:
  - 'sentiment' : direction = sign(sentiment_signal)
  - 'llm'       : direction = LLM brief recommended_side

Order lifecycle (all simulated):
  place -> resting -> (ask<=limit) filled  | (timeout) expired
  filled position -> held to settlement -> closed at $1 (win) / $0 (loss)

Limit price is set by LIMIT_AGGRESSION in [0,1] on the chosen side:
  limit = side_bid + AGGRESSION * (side_ask - side_bid)
  0.0 = post at bid (cheapest, rarely fills) · 0.5 = mid · 1.0 = ask (marketable).
A resting BUY fills when the side's live ASK <= our limit (a seller meets our price);
fill price = that ask (price improvement vs the limit).
"""

import os
import json
import uuid
import pandas as pd
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ORDERS_PATH  = os.path.join(PROJECT_ROOT, "data", "gold", "paper_orders_v2.parquet")

LIMIT_AGGRESSION = float(os.getenv("PAPER_V2_AGGRESSION", "0.5"))   # 0=bid 0.5=mid 1=ask
FILL_TIMEOUT_MIN = float(os.getenv("PAPER_V2_FILL_TIMEOUT_MIN", "2880"))  # cancel unfilled after
RISK_PER_TRADE   = float(os.getenv("PAPER_V2_RISK_USD", "10.0"))    # identical sizing both arms

COLUMNS = [
    "order_id", "arm", "ticker", "side", "count", "limit_price", "status",
    "created_at", "entry_bid", "entry_ask", "entry_mid",
    "filled_price", "filled_at", "close_price", "closed_at", "close_reason", "meta",
]

# ─────────────────────────── persistence ───────────────────────────

def load_orders():
    if not os.path.exists(ORDERS_PATH):
        return pd.DataFrame(columns=COLUMNS)
    try:
        df = pd.read_parquet(ORDERS_PATH)
        for c in COLUMNS:
            if c not in df.columns:
                df[c] = pd.NA
        return df[COLUMNS]
    except Exception:
        return pd.DataFrame(columns=COLUMNS)

def save_orders(df):
    os.makedirs(os.path.dirname(ORDERS_PATH), exist_ok=True)
    df[COLUMNS].to_parquet(ORDERS_PATH, index=False)

# ─────────────────────────── price helpers ───────────────────────────
# All prices in dollars (0..1). NO side is the YES-complement.

def side_bid(side, yes_bid, yes_ask):
    return yes_bid if side == "yes" else (1.0 - yes_ask)

def side_ask(side, yes_bid, yes_ask):
    return yes_ask if side == "yes" else (1.0 - yes_bid)

def side_mid(side, yes_bid, yes_ask):
    return (side_bid(side, yes_bid, yes_ask) + side_ask(side, yes_bid, yes_ask)) / 2.0

def limit_for(side, yes_bid, yes_ask, aggression=LIMIT_AGGRESSION):
    b = side_bid(side, yes_bid, yes_ask)
    a = side_ask(side, yes_bid, yes_ask)
    return round(b + aggression * (a - b), 4)

def _now():
    return datetime.now(timezone.utc)

def held_tickers(df, arm):
    """Tickers this arm already has resting or filled (avoid double-entry)."""
    if df.empty:
        return set()
    m = (df["arm"] == arm) & (df["status"].isin(["resting", "filled"]))
    return set(df.loc[m, "ticker"].astype(str))

# ─────────────────────────── actions ───────────────────────────

def place_order(df, arm, ticker, side, yes_bid, yes_ask, meta=None):
    """Append a resting limit BUY. count sized so risk≈RISK_PER_TRADE at the limit price."""
    limit = limit_for(side, yes_bid, yes_ask)
    if limit <= 0 or limit >= 1:
        return df, None
    count = max(1, int(RISK_PER_TRADE / limit))
    row = {
        "order_id":   str(uuid.uuid4()),
        "arm":        arm,
        "ticker":     ticker,
        "side":       side,
        "count":      count,
        "limit_price": limit,
        "status":     "resting",
        "created_at": _now().isoformat(),
        "entry_bid":  round(side_bid(side, yes_bid, yes_ask), 4),
        "entry_ask":  round(side_ask(side, yes_bid, yes_ask), 4),
        "entry_mid":  round(side_mid(side, yes_bid, yes_ask), 4),
        "filled_price": pd.NA, "filled_at": pd.NA,
        "close_price": pd.NA, "closed_at": pd.NA, "close_reason": pd.NA,
        "meta":       json.dumps(meta or {}),
    }
    return pd.concat([df, pd.DataFrame([row])], ignore_index=True), row["order_id"]

def process_fills(df, price_map):
    """Resting orders: fill when live side-ask <= limit; else expire after timeout.
    price_map: ticker -> {'yes_bid':float,'yes_ask':float}. Returns (df, n_filled, n_expired)."""
    n_filled = n_expired = 0
    now = _now()
    for i, o in df[df["status"] == "resting"].iterrows():
        px = price_map.get(o["ticker"])
        if px is None:
            continue
        ask = side_ask(o["side"], px["yes_bid"], px["yes_ask"])
        if ask <= float(o["limit_price"]):
            df.at[i, "status"] = "filled"
            df.at[i, "filled_price"] = round(ask, 4)   # price improvement vs limit
            df.at[i, "filled_at"] = now.isoformat()
            n_filled += 1
        else:
            age_min = (now - pd.to_datetime(o["created_at"])).total_seconds() / 60.0
            if age_min > FILL_TIMEOUT_MIN:
                df.at[i, "status"] = "expired"
                df.at[i, "closed_at"] = now.isoformat()
                df.at[i, "close_reason"] = "fill_timeout"
                n_expired += 1
    return df, n_filled, n_expired

def process_settlements(df, settle_map):
    """Close filled positions whose market settled. settle_map: ticker -> 'yes'|'no'.
    Payoff $1 if our side matches result else $0. Returns (df, n_closed)."""
    n_closed = 0
    now = _now()
    for i, o in df[df["status"] == "filled"].iterrows():
        res = settle_map.get(o["ticker"])
        if res not in ("yes", "no"):
            continue
        won = (o["side"] == res)
        df.at[i, "status"] = "closed"
        df.at[i, "close_price"] = 1.0 if won else 0.0
        df.at[i, "closed_at"] = now.isoformat()
        df.at[i, "close_reason"] = "settled_win" if won else "settled_loss"
        n_closed += 1
    return df, n_closed

def unrealized(df, price_map):
    """Mark open (filled) positions at the side BID (honest exit value). Returns dict."""
    open_pos = df[df["status"] == "filled"]
    rows = []
    for _, o in open_pos.iterrows():
        px = price_map.get(o["ticker"])
        if px is None:
            continue
        mark = side_bid(o["side"], px["yes_bid"], px["yes_ask"])
        cost = float(o["filled_price"]) * float(o["count"])
        val  = mark * float(o["count"])
        rows.append({"arm": o["arm"], "ticker": o["ticker"], "side": o["side"],
                     "count": o["count"], "entry": float(o["filled_price"]),
                     "mark": round(mark, 4), "pnl": round(val - cost, 4)})
    return pd.DataFrame(rows)
