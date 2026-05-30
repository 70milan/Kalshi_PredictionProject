#!/usr/bin/env python3
"""
Interactive DuckDB explorer — trace one market end-to-end.
Shows data flowing from Bronze → Silver → Gold → ChromaDB → LLM
"""

import duckdb
import pandas as pd
from datetime import datetime, timedelta
import sys

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────

def init_db():
    """Initialize DuckDB with Delta + views."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL delta; LOAD delta;")

    # Load views
    queries = [
        """CREATE OR REPLACE VIEW silver.kalshi_markets_history AS
           SELECT * FROM delta_scan('C:/Data Engineering/codeprep/predection_project/data/silver/kalshi_markets_history')""",

        """CREATE OR REPLACE VIEW silver.news_articles_enriched AS
           SELECT * FROM delta_scan('C:/Data Engineering/codeprep/predection_project/data/silver/news_articles_enriched')""",

        """CREATE OR REPLACE VIEW silver.gdelt_events_current AS
           SELECT * FROM delta_scan('C:/Data Engineering/codeprep/predection_project/data/silver/gdelt_events_current')""",

        """CREATE OR REPLACE VIEW silver.gdelt_gkg_current AS
           SELECT * FROM delta_scan('C:/Data Engineering/codeprep/predection_project/data/silver/gdelt_gkg_current')""",

        """CREATE OR REPLACE VIEW gold.intelligence_briefs AS
           SELECT * FROM read_parquet('C:/Data Engineering/codeprep/predection_project/data/gold/intelligence_briefs/*.parquet', union_by_name = true)""",

        """CREATE OR REPLACE VIEW gold.mispricing_scores AS
           SELECT * FROM delta_scan('C:/Data Engineering/codeprep/predection_project/data/gold/mispricing_scores')""",
    ]

    for q in queries:
        try:
            conn.execute(q)
        except Exception as e:
            print(f"⚠️  View setup warning: {e}")

    return conn

# ─────────────────────────────────────────────
# STEP 1: FIND ACTIVE MARKETS
# ─────────────────────────────────────────────

def step_1_find_markets(conn):
    """Show top liquid markets to trace."""
    print("\n" + "="*70)
    print("STEP 1: FIND ACTIVE MARKETS")
    print("="*70)

    query = """
    SELECT
      ticker,
      title,
      series_ticker,
      ROUND(yes_ask * 100, 1) as yes_ask_cents,
      volume,
      MAX(ingested_at) as last_update
    FROM silver.kalshi_markets_history
    WHERE status IN ('open', 'active')
    GROUP BY ticker, title, series_ticker, yes_ask, volume, status
    ORDER BY volume DESC
    LIMIT 15
    """

    df = conn.execute(query).df()
    print("\n📊 Top 15 most liquid markets:\n")
    print(df.to_string(index=False))

    print("\n💡 LEARNING: Pick one ticker (e.g., KXTW-20261231-TWJY) and trace it below.")
    print("   Question: What's the difference between 'ticker' and 'series_ticker'?")

    return df

# ─────────────────────────────────────────────
# STEP 2: NEWS SENTIMENT FOR A MARKET
# ─────────────────────────────────────────────

def step_2_news_for_market(conn, topic_keyword):
    """Show news sentiment for a specific market theme."""
    print("\n" + "="*70)
    print(f"STEP 2: NEWS SENTIMENT FOR '{topic_keyword}'")
    print("="*70)

    query = f"""
    SELECT
      DATE_TRUNC('minute', published_at) / 15 * INTERVAL '15 minutes' as window_15m,
      COUNT(*) as article_count,
      COUNT(DISTINCT feed_key) as sources,
      ROUND(AVG(sentiment_score), 3) as avg_sentiment,
      ROUND(MIN(sentiment_score), 3) as most_negative,
      ROUND(MAX(sentiment_score), 3) as most_positive
    FROM silver.news_articles_enriched
    WHERE LOWER(title) LIKE '%{topic_keyword}%'
      OR LOWER(full_text) LIKE '%{topic_keyword}%'
      AND published_at > NOW() - INTERVAL '48 hours'
    GROUP BY window_15m
    ORDER BY window_15m DESC
    LIMIT 20
    """

    df = conn.execute(query).df()
    print(f"\n📰 News sentiment windows (15-min aggregated):\n")
    print(df.to_string(index=False))

    print("\n💡 LEARNING: Each row is a '15-minute signal'.")
    print("   - avg_sentiment > 0.3 → Bullish signal (market underpriced)")
    print("   - avg_sentiment < -0.3 → Bearish signal (market overpriced)")
    print("   - sources: How many news sources mentioned this topic")

    return df

# ─────────────────────────────────────────────
# STEP 3: GDELT EVENTS (WHAT ACTUALLY HAPPENED)
# ─────────────────────────────────────────────

def step_3_gdelt_events(conn, actor_keyword):
    """Show GDELT events for a topic."""
    print("\n" + "="*70)
    print(f"STEP 3: GDELT EVENTS FOR '{actor_keyword}'")
    print("="*70)

    query = f"""
    SELECT
      DATE_TRUNC('hour', date) as event_hour,
      COUNT(*) as event_count,
      ROUND(AVG(goldstein_scale), 2) as avg_impact,  -- -10 to +10
      ROUND(AVG(tone), 2) as avg_tone,               -- sentiment
      STRING_AGG(DISTINCT event_root_code, ',') as event_codes
    FROM silver.gdelt_events_current
    WHERE (LOWER(actor1_name) LIKE '%{actor_keyword}%'
        OR LOWER(actor2_name) LIKE '%{actor_keyword}%'
        OR LOWER(event_description) LIKE '%{actor_keyword}%')
      AND date > NOW() - INTERVAL '48 hours'
    GROUP BY event_hour
    ORDER BY event_hour DESC
    LIMIT 20
    """

    try:
        df = conn.execute(query).df()
        print(f"\n🌍 GDELT events mentioning '{actor_keyword}':\n")
        print(df.to_string(index=False))

        print("\n💡 LEARNING: GDELT is 'structured news'.")
        print("   - goldstein_scale: -10 (conflict) to +10 (cooperation)")
        print("   - tone: sentiment of the event description")
        print("   - event_codes: Type of event (031=Accuse, 070=Appeal, etc.)")
    except Exception as e:
        print(f"   ⚠️  No GDELT data found: {e}")

# ─────────────────────────────────────────────
# STEP 4: BLEND SIGNALS (NEWS + GDELT)
# ─────────────────────────────────────────────

def step_4_blend_signals(conn, topic_keyword):
    """Show blended sentiment (70% news + 30% GDELT)."""
    print("\n" + "="*70)
    print(f"STEP 4: BLENDED SIGNAL (70% News + 30% GDELT) FOR '{topic_keyword}'")
    print("="*70)

    query = f"""
    WITH news_15m AS (
      SELECT
        DATE_TRUNC('minute', published_at) / 15 * INTERVAL '15 minutes' as window_time,
        ROUND(AVG(sentiment_score), 3) as news_sentiment,
        COUNT(*) as article_count
      FROM silver.news_articles_enriched
      WHERE LOWER(title) LIKE '%{topic_keyword}%'
        AND published_at > NOW() - INTERVAL '48 hours'
      GROUP BY window_time
    ),
    gdelt_15m AS (
      SELECT
        DATE_TRUNC('minute', date) / 15 * INTERVAL '15 minutes' as window_time,
        ROUND(AVG(tone) / 100.0, 3) as gdelt_sentiment,
        COUNT(*) as event_count
      FROM silver.gdelt_events_current
      WHERE (LOWER(actor1_name) LIKE '%{topic_keyword}%'
          OR LOWER(actor2_name) LIKE '%{topic_keyword}%')
        AND date > NOW() - INTERVAL '48 hours'
      GROUP BY window_time
    )
    SELECT
      COALESCE(news_15m.window_time, gdelt_15m.window_time) as signal_window,
      COALESCE(news_sentiment, 0) as news_signal,
      COALESCE(gdelt_sentiment, 0) as gdelt_signal,
      ROUND(0.7 * COALESCE(news_sentiment, 0) + 0.3 * COALESCE(gdelt_sentiment, 0), 3) as blended,
      COALESCE(article_count, 0) as articles,
      COALESCE(event_count, 0) as events
    FROM news_15m
    FULL OUTER JOIN gdelt_15m ON news_15m.window_time = gdelt_15m.window_time
    ORDER BY signal_window DESC
    LIMIT 20
    """

    df = conn.execute(query).df()
    print(f"\n📈 Blended signals (news + GDELT):\n")
    print(df.to_string(index=False))

    print("\n💡 LEARNING: This is the 'combined signal' sent to the market.")
    print("   - blended > 0.3 → Market is bearish (YES is overpriced)")
    print("   - blended < -0.3 → Market is bullish (NO is overpriced)")

# ─────────────────────────────────────────────
# STEP 5: KALSHI PRICES AT SAME TIME
# ─────────────────────────────────────────────

def step_5_kalshi_prices(conn, series_ticker):
    """Show Kalshi prices aligned with sentiment windows."""
    print("\n" + "="*70)
    print(f"STEP 5: KALSHI PRICES FOR {series_ticker}")
    print("="*70)

    query = f"""
    SELECT
      DATE_TRUNC('minute', ingested_at) / 15 * INTERVAL '15 minutes' as price_window,
      ticker,
      ROUND(yes_bid * 100, 1) as yes_bid_cents,
      ROUND(yes_ask * 100, 1) as yes_ask_cents,
      ROUND((yes_bid + yes_ask) / 2 * 100, 1) as mid_price_cents,
      ROUND(volume, 0) as volume,
      COUNT(*) as snapshots
    FROM silver.kalshi_markets_history
    WHERE series_ticker = '{series_ticker}'
      AND ingested_at > NOW() - INTERVAL '48 hours'
    GROUP BY price_window, ticker, yes_bid, yes_ask, volume
    ORDER BY price_window DESC
    LIMIT 20
    """

    df = conn.execute(query).df()
    print(f"\n💵 {series_ticker} price windows (aligned with signals):\n")
    print(df.to_string(index=False))

    print("\n💡 LEARNING: This is what the market was priced at when signals fired.")
    print("   - Match signal time to price time: same 15m window = synchronized")

# ─────────────────────────────────────────────
# STEP 6: MISPRICING SCORES (GOLD LAYER)
# ─────────────────────────────────────────────

def step_6_mispricing_scores(conn, series_ticker):
    """Show Gold layer scores."""
    print("\n" + "="*70)
    print(f"STEP 6: MISPRICING SCORES (GOLD) FOR {series_ticker}")
    print("="*70)

    query = f"""
    SELECT
      ticker,
      ROUND(mispricing_score, 1) as mispricing_score,
      ROUND(edge_pct * 100, 2) as edge_pct,
      verdict,
      ingested_at
    FROM gold.mispricing_scores
    WHERE series_ticker = '{series_ticker}'
      AND ingested_at > NOW() - INTERVAL '24 hours'
    ORDER BY ingested_at DESC
    LIMIT 10
    """

    try:
        df = conn.execute(query).df()
        print(f"\n🎯 {series_ticker} mispricing scores:\n")
        print(df.to_string(index=False))

        print("\n💡 LEARNING: Gold layer combines all signals into a score.")
        print("   - Score 0-100: how confident the system is about mispricing")
        print("   - edge_pct: How much YES/NO is overpriced vs fair value")
        print("   - verdict: What side to trade (BUY YES / SELL YES / HOLD)")
    except Exception as e:
        print(f"   ⚠️  No mispricing data: {e}")

# ─────────────────────────────────────────────
# STEP 7: LLM INTELLIGENCE BRIEF
# ─────────────────────────────────────────────

def step_7_llm_brief(conn, ticker_prefix):
    """Show LLM's written brief."""
    print("\n" + "="*70)
    print(f"STEP 7: LLM INTELLIGENCE BRIEF FOR {ticker_prefix}")
    print("="*70)

    query = f"""
    SELECT
      ticker,
      ROUND(confidence_score * 100, 1) as confidence_pct,
      verdict,
      bull_case,
      bear_case,
      ingested_at
    FROM gold.intelligence_briefs
    WHERE ticker LIKE '%{ticker_prefix}%'
      AND ingested_at > NOW() - INTERVAL '24 hours'
    ORDER BY ingested_at DESC, confidence_score DESC
    LIMIT 5
    """

    try:
        df = conn.execute(query).df()
        print(f"\n🤖 LLM briefs for {ticker_prefix}:\n")

        for _, row in df.iterrows():
            print(f"  Confidence: {row['confidence_pct']}% | Verdict: {row['verdict']}")
            print(f"  Bull: {row['bull_case']}")
            print(f"  Bear: {row['bear_case']}")
            print(f"  Written: {row['ingested_at']}\n")

        print("💡 LEARNING: LLM reads news/GDELT + retrieves from ChromaDB.")
        print("   - bull_case: Why YES should win (retrieved from ChromaDB)")
        print("   - bear_case: Why NO should win")
        print("   - verdict: Final recommendation")
    except Exception as e:
        print(f"   ⚠️  No intelligence briefs: {e}")

# ─────────────────────────────────────────────
# MAIN MENU
# ─────────────────────────────────────────────

def main():
    print("\n" + "="*70)
    print("🔍 PREDICTIQ DATA LINEAGE EXPLORER")
    print("   Trace one market through Bronze → Silver → Gold → ChromaDB → LLM")
    print("="*70)

    conn = init_db()
    print("✅ DuckDB initialized")

    while True:
        print("\n" + "-"*70)
        print("Choose a step to explore:")
        print("-"*70)
        print("1️⃣  Find active Kalshi markets")
        print("2️⃣  News sentiment for a topic")
        print("3️⃣  GDELT events for a topic")
        print("4️⃣  Blended signals (News + GDELT)")
        print("5️⃣  Kalshi prices (market snapshot)")
        print("6️⃣  Mispricing scores (Gold layer)")
        print("7️⃣  LLM intelligence brief")
        print("0️⃣  Exit")

        choice = input("\n👉 Enter choice (0-7): ").strip()

        if choice == "0":
            print("\n✋ Goodbye!\n")
            break

        elif choice == "1":
            df = step_1_find_markets(conn)

        elif choice == "2":
            topic = input("   Enter topic keyword (e.g., 'taiwan', 'fed', 'election'): ").strip()
            if topic:
                step_2_news_for_market(conn, topic)

        elif choice == "3":
            actor = input("   Enter actor/country (e.g., 'taiwan', 'us', 'china'): ").strip()
            if actor:
                step_3_gdelt_events(conn, actor)

        elif choice == "4":
            topic = input("   Enter topic keyword for blending: ").strip()
            if topic:
                step_4_blend_signals(conn, topic)

        elif choice == "5":
            series = input("   Enter series ticker (e.g., KXTW, KXFED, KXPOLITICS): ").strip()
            if series:
                step_5_kalshi_prices(conn, series)

        elif choice == "6":
            series = input("   Enter series ticker: ").strip()
            if series:
                step_6_mispricing_scores(conn, series)

        elif choice == "7":
            ticker = input("   Enter ticker prefix (e.g., KXTW, KXFED): ").strip()
            if ticker:
                step_7_llm_brief(conn, ticker)

        else:
            print("   ⚠️  Invalid choice")

if __name__ == "__main__":
    main()
