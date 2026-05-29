"""
Sentiment arm — direction = sign(sentiment_signal), no LLM.
Draws from the shared candidate universe, places mid limit orders via paper_executor_v2,
tagged arm='sentiment'. Free and deterministic; this is the A/B counterpart to the LLM arm.
"""

import duckdb
import paper_executor_v2 as px
from candidate_select import select_candidates

MIN_CONVICTION = 0.05   # |sentiment_signal| floor to take a side


def run(con=None):
    own = con is None
    if own:
        con = duckdb.connect()
    try:
        cands = select_candidates(con)
    finally:
        if own:
            con.close()

    df = px.load_orders()
    held = px.held_tickers(df, "sentiment")
    placed = skipped = 0

    for _, c in cands.iterrows():
        ticker = str(c["ticker"])
        if ticker in held:
            continue
        sent = float(c["sentiment_signal"]) if c["sentiment_signal"] is not None else 0.0
        if abs(sent) < MIN_CONVICTION:
            skipped += 1
            continue
        yb, ya = c["yes_bid"], c["yes_ask"]
        if yb is None or ya is None:
            continue
        yb, ya = float(yb), float(ya)
        if not (0 < yb < ya < 1):
            continue
        side = "yes" if sent > 0 else "no"
        df, oid = px.place_order(df, "sentiment", ticker, side, yb, ya,
                                 meta={"sent": round(sent, 4), "score": float(c["mispricing_score"])})
        if oid:
            placed += 1
            held.add(ticker)

    px.save_orders(df)
    print(f"[Sentiment] candidates={len(cands)} placed={placed} skipped_low_conviction={skipped} "
          f"already_held={len(cands)-placed-skipped}")
    return placed


if __name__ == "__main__":
    run()
