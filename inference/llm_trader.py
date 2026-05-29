"""
LLM arm — direction = the LLM brief's recommended_side (skips "No Trade").
Draws from the SAME shared candidate universe as the sentiment arm; the only
difference is the direction source. Places mid limit orders tagged arm='llm'.

The LLM arm only trades candidates for which a fresh, actionable brief exists
(predict_movements writes these in Phase 3, budget-limited) — that selectivity
is itself part of what the A/B measures.
"""

import os
import duckdb
import paper_executor_v2 as px
from candidate_select import select_candidates

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRIEFS_PATH  = os.path.join(PROJECT_ROOT, "data", "gold", "intelligence_briefs")


def _latest_briefs(con):
    """Most recent brief per ticker (last 48h): ticker -> (recommended_side, verdict)."""
    glob = os.path.join(BRIEFS_PATH, "*.parquet").replace("\\", "/")
    try:
        df = con.execute(f"""
            SELECT ticker, recommended_side, verdict
            FROM (
                SELECT ticker, recommended_side, verdict,
                       ROW_NUMBER() OVER (PARTITION BY ticker
                           ORDER BY CAST(ingested_at AS TIMESTAMP) DESC) AS rn
                FROM read_parquet('{glob}', union_by_name=true)
                WHERE CAST(ingested_at AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
            ) WHERE rn = 1
        """).df()
        return {str(r["ticker"]): r for _, r in df.iterrows()}
    except Exception as e:
        print(f"[LLM] could not load briefs: {e}")
        return {}


def run(con=None):
    own = con is None
    if own:
        con = duckdb.connect()
    try:
        cands = select_candidates(con)
        briefs = _latest_briefs(con)
    finally:
        if own:
            con.close()

    df = px.load_orders()
    held = px.held_tickers(df, "llm")
    placed = no_brief = no_trade = 0

    for _, c in cands.iterrows():
        ticker = str(c["ticker"])
        if ticker in held:
            continue
        b = briefs.get(ticker)
        if b is None:
            no_brief += 1
            continue
        side = str(b.get("recommended_side") or "").lower().strip()
        verdict = str(b.get("verdict") or "").lower()
        if "no trade" in verdict or side not in ("yes", "no"):
            no_trade += 1
            continue
        yb, ya = c["yes_bid"], c["yes_ask"]
        if yb is None or ya is None:
            continue
        yb, ya = float(yb), float(ya)
        if not (0 < yb < ya < 1):
            continue
        df, oid = px.place_order(df, "llm", ticker, side, yb, ya,
                                 meta={"verdict": str(b.get("verdict"))[:80]})
        if oid:
            placed += 1
            held.add(ticker)

    px.save_orders(df)
    print(f"[LLM] candidates={len(cands)} placed={placed} no_brief={no_brief} no_trade={no_trade}")
    return placed


if __name__ == "__main__":
    run()
