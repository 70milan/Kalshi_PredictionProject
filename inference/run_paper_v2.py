"""
Paper V2 cycle orchestrator — called once per ETL cycle.
  1. sentiment arm places mid limit orders (direction = sign(sentiment))
  2. LLM arm places mid limit orders (direction = brief recommended_side)
  3. manage(): process resting->filled/expired against live prices, and
     filled->closed for any market that has settled.
Both arms + the manager share data/gold/paper_orders_v2.parquet.
"""

import os
import sys
import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import paper_executor_v2 as px
import sentiment_trader
import llm_trader

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LATEST       = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "open", "latest.parquet").replace("\\", "/")
KALSHI_HIST  = os.path.join(PROJECT_ROOT, "data", "silver", "kalshi_markets_history", "*.parquet").replace("\\", "/")


def manage():
    con = duckdb.connect()
    df = px.load_orders()
    if df.empty:
        con.close()
        print("[Manage] no orders yet.")
        return

    # Live prices for open markets
    price_map = {}
    try:
        prices = con.execute(f"""
            SELECT ticker, TRY_CAST(yes_bid_dollars AS DOUBLE) yb, TRY_CAST(yes_ask_dollars AS DOUBLE) ya
            FROM read_parquet('{LATEST}')
        """).df()
        price_map = {r.ticker: {"yes_bid": float(r.yb), "yes_ask": float(r.ya)}
                     for r in prices.itertuples() if r.yb is not None and r.ya is not None}
    except Exception as e:
        print(f"[Manage] price load failed: {e}")

    df, nf, ne = px.process_fills(df, price_map)

    # Settlement for held positions (best-effort; most hold for weeks)
    held = df[df["status"] == "filled"]["ticker"].astype(str).unique().tolist()
    settle_map = {}
    if held:
        try:
            qlist = ",".join("'" + t.replace("'", "") + "'" for t in held)
            sdf = con.execute(f"""
                SELECT ticker, MIN(LOWER(CAST(result AS VARCHAR))) AS res
                FROM read_parquet('{KALSHI_HIST}', union_by_name=true)
                WHERE ticker IN ({qlist}) AND LOWER(CAST(result AS VARCHAR)) IN ('yes','no')
                GROUP BY ticker
            """).df()
            settle_map = {r.ticker: r.res for r in sdf.itertuples()}
        except Exception as e:
            print(f"[Manage] settlement lookup failed: {e}")

    df, nc = px.process_settlements(df, settle_map)
    px.save_orders(df)
    con.close()

    open_n = int((df["status"] == "filled").sum())
    rest_n = int((df["status"] == "resting").sum())
    print(f"[Manage] fills={nf} expired={ne} settled={nc} | now open={open_n} resting={rest_n}")


def main():
    print("=" * 60)
    print(" PredictIQ Paper V2 — sentiment + LLM arms (demo-data sim)")
    print("=" * 60)
    try:
        sentiment_trader.run()
    except Exception as e:
        print(f"[Sentiment] ERROR: {e}")
    try:
        llm_trader.run()
    except Exception as e:
        print(f"[LLM] ERROR: {e}")
    manage()


if __name__ == "__main__":
    main()
