"""
Shared candidate universe for BOTH paper-trading arms (sentiment + LLM).

This is the V2 selection gate WITHOUT the LLM-cost eligibility throttle that lives
in predict_movements.get_predictive_candidates (that throttle only governs WHEN to
spend LLM tokens, not what is tradeable). Both arms must see the identical universe
so the A/B isolates direction source only.

Gate: mispricing_score >= 80 (V2-anchored = validated top ~5%) · live yes_bid in
0.25-0.75 · close 30-365 days out · gold freshness <= 48h · <= SERIES_LIMIT per
series · top CANDIDATE_LIMIT overall. Returns live bid/ask from bronze latest.
"""

import os
import duckdb
import pandas as pd

PROJECT_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_MISPRICING = os.path.join(PROJECT_ROOT, "data", "gold", "mispricing_scores")
BRONZE_LATEST   = os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "open", "latest.parquet")

MIN_DAYS_TO_CLOSE = float(os.getenv("INFERENCE_MIN_DAYS_TO_CLOSE", "60"))
MAX_DAYS_TO_CLOSE = float(os.getenv("INFERENCE_MAX_DAYS_TO_CLOSE", "365"))
SERIES_LIMIT     = int(os.getenv("SERIES_LIMIT",     "3"))
CANDIDATE_LIMIT  = int(os.getenv("CANDIDATE_LIMIT",  "50"))

# Count / attendance / novelty series with no tradeable news edge (user-flagged junk:
# "how many hours", press-briefing attendance, passport-image, veto/action counts).
# Ticker-prefix exclusion is more reliable than title matching (per kalshi-api-debug).
# Env-tunable; binary event markets (e.g. "visit Israel") are deliberately NOT blocked.
_DEFAULT_BLOCK = ("KXWHPRESSBRIEFING,KXWHVISIT,KXTRUMPPASSPORT,KXTRUMPTIME,"
                  "KXVETOCOUNT,KXTRUMPACT,KXEOWEEK")
BLOCKLIST_SERIES = [s.strip() for s in
                    os.getenv("CANDIDATE_BLOCKLIST_SERIES", _DEFAULT_BLOCK).split(",")
                    if s.strip()]


def select_candidates(con=None):
    """Return a DataFrame of current candidates with live bid/ask + sentiment_signal."""
    own_con = con is None
    if own_con:
        con = duckdb.connect()
    gold   = GOLD_MISPRICING.replace("\\", "/")
    latest = BRONZE_LATEST.replace("\\", "/")
    block_sql = ""
    if BLOCKLIST_SERIES:
        quoted = ",".join("'" + s.replace("'", "") + "'" for s in BLOCKLIST_SERIES)
        block_sql = (f"AND SPLIT_PART(g.ticker,'-',1) NOT IN ({quoted})\n"
                     f"              AND NOT (LOWER(g.title) LIKE '%how many%' "
                     f"OR LOWER(g.title) LIKE '%number of%')")
    try:
        df = con.execute(f"""
        WITH latest AS (
            SELECT g.ticker, g.title, g.delta_15m, g.mispricing_score, g.sentiment_signal,
                   b.yes_bid_l AS yes_bid, b.yes_ask_l AS yes_ask,
                   SPLIT_PART(g.ticker, '-', 1) AS series
            FROM delta_scan('{gold}') g
            JOIN (
                SELECT ticker,
                       TRY_CAST(close_time AS TIMESTAMPTZ) AS close_ts,
                       TRY_CAST(yes_bid_dollars AS DOUBLE) AS yes_bid_l,
                       TRY_CAST(yes_ask_dollars AS DOUBLE) AS yes_ask_l
                FROM read_parquet('{latest}')
            ) b ON g.ticker = b.ticker
            WHERE g.flagged_candidate = TRUE
              AND TRY_CAST(g.ingested_at AS TIMESTAMPTZ) >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
              AND b.yes_bid_l BETWEEN 0.25 AND 0.75
              AND b.close_ts IS NOT NULL
              AND DATE_DIFF('day', CURRENT_TIMESTAMP, b.close_ts) BETWEEN {MIN_DAYS_TO_CLOSE} AND {MAX_DAYS_TO_CLOSE}
              {block_sql}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY g.ticker ORDER BY g.ingested_at DESC) = 1
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY series ORDER BY mispricing_score DESC, ABS(COALESCE(delta_15m,0)) DESC
            ) AS series_rank
            FROM latest
        )
        SELECT ticker, title, yes_bid, yes_ask, sentiment_signal, mispricing_score
        FROM ranked
        WHERE series_rank <= {SERIES_LIMIT}
        ORDER BY mispricing_score DESC, ABS(COALESCE(delta_15m,0)) DESC
        LIMIT {CANDIDATE_LIMIT}
        """).df()
        return df
    except Exception as e:
        print(f"[CandidateSelect] ERROR: {e}")
        return pd.DataFrame()
    finally:
        if own_con:
            con.close()
