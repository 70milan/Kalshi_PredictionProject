"""
PredictIQ Strategic Backtest Engine
===================================
Retrospective "Point-in-Time" backtest to validate the Delayed Reaction hypothesis.
Proves that GDELT news volume spikes, when cross-referenced with local news embeddings,
can predict subsequent price movements on Kalshi.

Usage:
    python research/backtest_engine.py
    python research/backtest_engine.py --limit 10 --spike-threshold 5.0
    python research/backtest_engine.py --no-llm   # skip Groq calls, just compute signals + alpha
"""
import os
import sys
import platform
import re

# ChromaDB/SQLite patch -- only needed on Linux (Docker). Windows uses native SQLite.
if platform.system() == "Linux":
    try:
        __import__('pysqlite3')
        sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
    except ImportError:
        pass

import duckdb
import chromadb
import pandas as pd
import json
import argparse
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------
load_dotenv()
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

GDELT_HISTORY  = os.path.join(PROJECT_ROOT, "data", "gold", "gdelt_summaries_history")
KALSHI_HISTORY = os.path.join(PROJECT_ROOT, "data", "silver", "kalshi_markets_history")
THEME_MAP      = os.path.join(PROJECT_ROOT, "data", "reference", "kalshi_theme_map.csv")
CHROMA_PATH    = os.path.join(PROJECT_ROOT, "data", "chroma")

SIMILARITY_FLOOR = 0.45

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL   = "llama-3.3-70b-versatile"

# ---------------------------------------------------------
# 2. DATA LOADING (DuckDB)
# ---------------------------------------------------------
def load_mapped_signals(con, theme_map_df, spike_threshold=10.0):
    """
    Two-phase signal mapping:
      Phase 1 (DuckDB): Broad LIKE join — fast, casts a wide net.
      Phase 2 (Python): Regex gate — precision filter using required_regex column.
                        Prevents false positives like 'Brazilian Central Bank' -> KXFED.
    """
    # Phase 1: Broad DuckDB join
    con.register('theme_map', theme_map_df)
    query = f"""
    SELECT
        g.entity_name,
        g.entity_type,
        g.vol_spike_multiplier as vol_spike,
        g.tone_15m,
        g.ingested_at as gdelt_time,
        t.series_ticker,
        t.gdelt_entity_name as bridge_entity,
        t.required_regex as required_regex
    FROM read_parquet('{GDELT_HISTORY}/**/*.parquet', union_by_name=True) g
    JOIN theme_map t ON LOWER(g.entity_name) LIKE '%' || LOWER(t.gdelt_entity_name) || '%'
    WHERE g.vol_spike_multiplier IS NOT NULL
      AND g.vol_spike_multiplier >= {spike_threshold}
      AND g.entity_name IS NOT NULL
    ORDER BY g.ingested_at DESC
    """
    df = con.execute(query).df()
    broad_count = len(df)

    # Phase 2: Python regex gate
    if 'required_regex' in df.columns:
        def passes_regex(row):
            pattern = row.get('required_regex')
            # If no regex defined, pass through (backward compatible)
            if not pattern or str(pattern).strip() == '' or str(pattern) == 'nan':
                return True
            return bool(re.search(str(pattern), str(row['entity_name']), re.IGNORECASE))
        
        mask = df.apply(passes_regex, axis=1)
        df = df[mask].copy()
        filtered_count = broad_count - len(df)
        print(f"[Signal] {broad_count} broad matches -> {len(df)} after regex gate ({filtered_count} false positives removed)")
    else:
        print(f"[Signal] Found {len(df)} mapped GDELT spikes >= {spike_threshold}x (no regex column)")

    return df

def load_theme_map():
    """Load the bridge table mapping GDELT entities to Kalshi series tickers."""
    df = pd.read_csv(THEME_MAP)
    print(f"[Bridge] Loaded {len(df)} theme mappings across {df['series_ticker'].nunique()} series:")
    for _, row in df.iterrows():
        regex_hint = f" | regex: {str(row.get('required_regex',''))[:40]}" if 'required_regex' in df.columns else ""
        print(f"    [{row['series_ticker']}] {row['gdelt_entity_name']}{regex_hint}")
    return df



def get_kalshi_price_at(con, series_ticker, target_time):
    """
    Get the most recent Kalshi price snapshot active at target_time.
    Returns a DataFrame row with ticker, title, yes_bid, ingested_at.
    """
    t_end = target_time.strftime('%Y-%m-%d %H:%M:%S')

    query = f"""
    SELECT
        ticker, title, series_ticker, yes_bid, ingested_at
    FROM read_parquet('{KALSHI_HISTORY}/**/*.parquet', union_by_name=True)
    WHERE series_ticker LIKE '{series_ticker}%'
      AND ingested_at <= '{t_end}'
    ORDER BY ingested_at DESC
    LIMIT 1
    """
    try:
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame()


def get_kalshi_price_forward(con, series_ticker, target_time, hours_forward):
    """
    Get the most recent Kalshi price snapshot active at target_time + hours_forward.
    """
    t_target = target_time + timedelta(hours=hours_forward)
    t_end    = t_target.strftime('%Y-%m-%d %H:%M:%S')

    query = f"""
    SELECT
        ticker, title, series_ticker, yes_bid, ingested_at
    FROM read_parquet('{KALSHI_HISTORY}/**/*.parquet', union_by_name=True)
    WHERE series_ticker LIKE '{series_ticker}%'
      AND ingested_at <= '{t_end}'
    ORDER BY ingested_at DESC
    LIMIT 1
    """
    try:
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame()


def get_kalshi_price_before(con, series_ticker, target_time, minutes_before=15):
    """Get the most recent Kalshi price snapshot active at target_time - minutes_before."""
    t_target = target_time - timedelta(minutes=minutes_before)
    t_end    = t_target.strftime('%Y-%m-%d %H:%M:%S')

    query = f"""
    SELECT
        ticker, title, series_ticker, yes_bid, ingested_at
    FROM read_parquet('{KALSHI_HISTORY}/**/*.parquet', union_by_name=True)
    WHERE series_ticker LIKE '{series_ticker}%'
      AND ingested_at <= '{t_end}'
    ORDER BY ingested_at DESC
    LIMIT 1
    """
    try:
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------
# 3. POINT-IN-TIME RAG RETRIEVAL
# ---------------------------------------------------------
def fetch_rag_context_pit(collections, query_text, event_time, window_mins=120):
    """
    Point-in-Time RAG search. CRITICAL: Only retrieves documents
    where ingested_timestamp <= event_time to prevent data leakage.
    """
    end_ts   = int(event_time.timestamp())
    start_ts = int((event_time - timedelta(minutes=window_mins)).timestamp())
    scored_docs = []

    if not isinstance(collections, list):
        collections = [collections]

    for coll in collections:
        try:
            results = coll.query(
                query_texts=[query_text],
                n_results=5,
                where={
                    "$and": [
                        {"ingested_timestamp": {"$gte": start_ts}},
                        {"ingested_timestamp": {"$lte": end_ts}}
                    ]
                },
                include=["documents", "metadatas", "distances"]
            )

            if results['documents'] and results['documents'][0]:
                for i in range(len(results['documents'][0])):
                    distance = results['distances'][0][i]
                    score = 1.0 / (1.0 + distance)
                    if score >= SIMILARITY_FLOOR:
                        scored_docs.append({
                            "content": results['documents'][0][i],
                            "source": results['metadatas'][0][i].get('source', coll.name),
                            "score": score
                        })
        except Exception as e:
            print(f"    [RAG] Warning querying {coll.name}: {e}")

    scored_docs = sorted(scored_docs, key=lambda x: x['score'], reverse=True)
    return scored_docs


# ---------------------------------------------------------
# 4. LLM VERDICT (Groq)
# ---------------------------------------------------------
def generate_verdict(market_info, context_docs, groq_client):
    """
    Generate a Buy/Sell/Wait verdict using the LLM.
    Returns dict with keys: direction, confidence, reasoning.
    """
    if not groq_client:
        return {"direction": "SKIP", "confidence": 0, "reasoning": "No LLM configured"}

    rag_text = "\n\n".join([f"[{d['score']*100:.0f}% match] {d['content'][:500]}" for d in context_docs[:3]])

    prompt = f"""You are analyzing a prediction market for a retrospective backtest.

MARKET: {market_info.get('ticker', 'N/A')} | {market_info.get('title', 'N/A')}
CURRENT ODDS (yes_bid): {market_info.get('yes_bid', 'N/A')}
SERIES: {market_info.get('series_ticker', 'N/A')}

NEWS CONTEXT (at the time of the GDELT volume spike):
{rag_text if rag_text else 'No relevant news found in the time window.'}

Based ONLY on the news context available at this point in time, should you BUY (price will go UP),
SELL (price will go DOWN), or WAIT (insufficient signal)?

Respond with ONLY a JSON object:
{{"direction": "BUY" or "SELL" or "WAIT", "confidence": 0.0-1.0, "reasoning": "brief explanation"}}"""

    try:
        response = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.2
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        return {"direction": "ERROR", "confidence": 0, "reasoning": str(e)}


# ---------------------------------------------------------
# 5. MAIN BACKTEST LOOP
# ---------------------------------------------------------
def run_backtest(args):
    print("=" * 70)
    print("  PredictIQ Strategic Backtest Engine")
    print("  Hypothesis: Delayed Reaction (GDELT Spike -> Kalshi Price Move)")
    print("=" * 70)

    con = duckdb.connect()

    theme_map = load_theme_map()

    # --- Step A: Load and map signals via DuckDB ---
    mapped_df = load_mapped_signals(con, theme_map, args.spike_threshold)
    if mapped_df.empty:
        print("[ABORT] No mapped GDELT signals found above threshold.")
        return

    # Deduplicate: keep unique (series_ticker, gdelt_time) pairs
    mapped_df = mapped_df.drop_duplicates(subset=['series_ticker', 'gdelt_time'])
    print(f"[Dedup]  {len(mapped_df)} unique signal events after deduplication")

    if args.limit:
        mapped_df = mapped_df.head(args.limit)
        print(f"[Limit]  Processing first {args.limit} events")

    # --- ChromaDB setup ---
    chroma_client = None
    collections = []
    if not args.no_llm:
        try:
            chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
            collections = []
            for name in ["silver_news_enriched", "silver_gdelt_enriched"]:
                try:
                    collections.append(chroma_client.get_collection(name=name))
                except Exception:
                    print(f"    [RAG] Collection '{name}' not found, skipping.")
            print(f"[RAG] Loaded {len(collections)} ChromaDB collections")
        except Exception as e:
            print(f"[RAG] ChromaDB error: {e}. Continuing without RAG.")

    # Groq client
    groq_client = None
    if not args.no_llm and GROQ_API_KEY:
        from groq import Groq
        groq_client = Groq(api_key=GROQ_API_KEY)
        print(f"[LLM] Groq client initialized ({GROQ_MODEL})")

    # --- Step B & C: Process each event ---
    results = []
    print(f"\n{'='*70}")
    print(f"  PROCESSING {len(mapped_df)} EVENTS")
    print(f"{'='*70}\n")

    for idx, event in mapped_df.iterrows():
        event_num = len(results) + 1
        t0 = pd.to_datetime(event['gdelt_time'])
        if t0.tzinfo is None:
            t0 = t0.replace(tzinfo=timezone.utc)

        print(f"\n--- Event {event_num}/{len(mapped_df)} ---")
        print(f"    Entity: {event['entity_name']} ({event['entity_type']})")
        print(f"    Spike:  {event['vol_spike']:.1f}x | Tone: {event['tone_15m']:.2f}")
        print(f"    T0:     {t0.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"    Series: {event['series_ticker']}")

        # Get price at T0
        price_t0_df = get_kalshi_price_at(con, event['series_ticker'], t0)
        if price_t0_df.empty:
            print(f"    [SKIP] No Kalshi price data at T0 for {event['series_ticker']}")
            continue

        price_t0 = price_t0_df.iloc[0]['yes_bid']
        ticker = price_t0_df.iloc[0]['ticker']
        title = price_t0_df.iloc[0]['title']
        print(f"    Price@T0: {price_t0:.4f} ({price_t0*100:.1f}%) [{ticker}]")

        # Get price at T-15m for delayed reaction check
        price_before_df = get_kalshi_price_before(con, event['series_ticker'], t0)
        if not price_before_df.empty:
            price_before = price_before_df.iloc[0]['yes_bid']
            delta_15m = abs(price_t0 - price_before)
            print(f"    Price@T-15m: {price_before:.4f} | 15m Delta: {delta_15m*100:.1f}%")
        else:
            price_before = None
            delta_15m = 0.0
            print(f"    Price@T-15m: N/A (no prior snapshot)")

        # Delayed Reaction filter: delta < 2%
        is_delayed = delta_15m < 0.02
        print(f"    Delayed Reaction: {'YES' if is_delayed else 'NO (market already moved)'}")

        # Get future prices for alpha calculation
        price_4h_df = get_kalshi_price_forward(con, event['series_ticker'], t0, 4)
        price_12h_df = get_kalshi_price_forward(con, event['series_ticker'], t0, 12)

        price_4h  = price_4h_df.iloc[0]['yes_bid'] if not price_4h_df.empty else None
        price_12h = price_12h_df.iloc[0]['yes_bid'] if not price_12h_df.empty else None

        alpha_4h  = (price_4h - price_t0) if price_4h is not None else None
        alpha_12h = (price_12h - price_t0) if price_12h is not None else None

        if alpha_4h is not None and price_4h is not None:
            print(f"    Price@T+4h:  {price_4h:.4f} | Alpha: {alpha_4h*100:.1f}%")
        else:
            print(f"    Price@T+4h:  N/A")
            
        if alpha_12h is not None and price_12h is not None:
            print(f"    Price@T+12h: {price_12h:.4f} | Alpha: {alpha_12h*100:.1f}%")
        else:
            print(f"    Price@T+12h: N/A")

        # RAG + LLM verdict (if enabled)
        verdict = {"direction": "SKIP", "confidence": 0, "reasoning": "LLM disabled"}
        rag_count = 0
        if not args.no_llm and collections:
            query_text = f"{event['entity_name']} {title}"
            context_docs = fetch_rag_context_pit(collections, query_text, t0)
            rag_count = len(context_docs)
            print(f"    RAG Context: {rag_count} documents retrieved")

            if groq_client:
                market_info = {
                    'ticker': ticker,
                    'title': title,
                    'yes_bid': price_t0,
                    'series_ticker': event['series_ticker']
                }
                verdict = generate_verdict(market_info, context_docs, groq_client)
                print(f"    LLM Verdict: {verdict.get('direction', 'N/A')} (conf: {verdict.get('confidence', 0):.0%})")
                print(f"    Reasoning: {verdict.get('reasoning', 'N/A')[:120]}")

        result = {
            'event_num': event_num,
            'entity_name': event['entity_name'],
            'entity_type': event['entity_type'],
            'series_ticker': event['series_ticker'],
            'ticker': ticker,
            'title': title,
            'vol_spike': event['vol_spike'],
            'tone_15m': event['tone_15m'],
            'gdelt_time': str(t0),
            'price_t0': price_t0,
            'price_t_minus_15m': price_before,
            'delta_15m': delta_15m,
            'is_delayed_reaction': is_delayed,
            'price_t_plus_4h': price_4h,
            'price_t_plus_12h': price_12h,
            'alpha_4h': alpha_4h,
            'alpha_12h': alpha_12h,
            'rag_docs_found': rag_count,
            'llm_direction': verdict.get('direction', 'SKIP'),
            'llm_confidence': verdict.get('confidence', 0),
            'llm_reasoning': str(verdict.get('reasoning', ''))[:500]
        }
        results.append(result)

    # --- Step D: Generate Report ---
    if not results:
        print("\n[RESULT] No events could be processed. Check data availability.")
        return

    df = pd.DataFrame(results)

    print("\n")
    print("=" * 70)
    print("  BACKTEST RESULTS SUMMARY")
    print("=" * 70)

    print(f"\n  Total Events Processed:     {len(df)}")
    print(f"  Delayed Reactions Found:    {df['is_delayed_reaction'].sum()}")

    # Alpha Stats (all events with data)
    has_4h  = df[df['alpha_4h'].notna()]
    has_12h = df[df['alpha_12h'].notna()]

    print(f"\n  --- Alpha Performance (All Mapped Events) ---")
    if not has_4h.empty:
        print(f"  Events with T+4h data:      {len(has_4h)}")
        print(f"  Avg Alpha (4h):             {has_4h['alpha_4h'].mean()*100:+.2f}%")
        print(f"  Median Alpha (4h):          {has_4h['alpha_4h'].median()*100:+.2f}%")
        print(f"  Max Alpha (4h):             {has_4h['alpha_4h'].max()*100:+.2f}%")
        print(f"  Min Alpha (4h):             {has_4h['alpha_4h'].min()*100:+.2f}%")
        positive_4h = (has_4h['alpha_4h'] > 0).sum()
        print(f"  Price Increased (4h):       {positive_4h}/{len(has_4h)} ({positive_4h/len(has_4h)*100:.0f}%)")
    else:
        print(f"  No T+4h price data available.")

    if not has_12h.empty:
        print(f"\n  Events with T+12h data:     {len(has_12h)}")
        print(f"  Avg Alpha (12h):            {has_12h['alpha_12h'].mean()*100:+.2f}%")
        print(f"  Median Alpha (12h):         {has_12h['alpha_12h'].median()*100:+.2f}%")
        positive_12h = (has_12h['alpha_12h'] > 0).sum()
        print(f"  Price Increased (12h):      {positive_12h}/{len(has_12h)} ({positive_12h/len(has_12h)*100:.0f}%)")

    # Delayed Reaction specific stats
    delayed = df[df['is_delayed_reaction'] == True]
    if not delayed.empty:
        d4h = delayed[delayed['alpha_4h'].notna()]
        d12h = delayed[delayed['alpha_12h'].notna()]
        print(f"\n  --- Delayed Reaction Subset ---")
        print(f"  Total Delayed Reactions:    {len(delayed)}")
        if not d4h.empty:
            print(f"  Avg Alpha (4h):             {d4h['alpha_4h'].mean()*100:+.2f}%")
            dp4h = (d4h['alpha_4h'] > 0).sum()
            print(f"  Price Increased (4h):       {dp4h}/{len(d4h)} ({dp4h/len(d4h)*100:.0f}%)")
        if not d12h.empty:
            print(f"  Avg Alpha (12h):            {d12h['alpha_12h'].mean()*100:+.2f}%")

    # Ghost Pump Analysis
    ghost_pumps = df[(df['alpha_4h'].notna()) & (df['alpha_4h'].abs() < 0.005)]
    total_with_alpha = len(df[df['alpha_4h'].notna()])
    if total_with_alpha > 0:
        ghost_rate = len(ghost_pumps) / total_with_alpha
        print(f"\n  --- Ghost Pump Analysis ---")
        print(f"  Ghost Pumps (<0.5% move):   {len(ghost_pumps)}/{total_with_alpha} ({ghost_rate*100:.0f}%)")

    # LLM Verdict Stats
    llm_verdicts = df[df['llm_direction'].isin(['BUY', 'SELL', 'WAIT'])]
    if not llm_verdicts.empty:
        print(f"\n  --- LLM Verdict Performance ---")
        buys = llm_verdicts[llm_verdicts['llm_direction'] == 'BUY']
        sells = llm_verdicts[llm_verdicts['llm_direction'] == 'SELL']
        waits = llm_verdicts[llm_verdicts['llm_direction'] == 'WAIT']
        print(f"  BUY verdicts:  {len(buys)}")
        print(f"  SELL verdicts: {len(sells)}")
        print(f"  WAIT verdicts: {len(waits)}")

        # Precision: BUY verdicts where price actually went up
        buy_with_4h = buys[buys['alpha_4h'].notna()]
        if not buy_with_4h.empty:
            correct_buys = (buy_with_4h['alpha_4h'] > 0).sum()
            precision = correct_buys / len(buy_with_4h)
            print(f"  BUY Precision (4h):         {correct_buys}/{len(buy_with_4h)} ({precision*100:.0f}%)")
            if not buy_with_4h.empty:
                print(f"  Avg BUY Alpha (4h):         {buy_with_4h['alpha_4h'].mean()*100:+.2f}%")

        sell_with_4h = sells[sells['alpha_4h'].notna()]
        if not sell_with_4h.empty:
            correct_sells = (sell_with_4h['alpha_4h'] < 0).sum()
            print(f"  SELL Precision (4h):        {correct_sells}/{len(sell_with_4h)} ({correct_sells/len(sell_with_4h)*100:.0f}%)")

    # Save results
    out_dir = os.path.join(PROJECT_ROOT, "research")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"backtest_results_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv")
    df.to_csv(out_file, index=False)
    print(f"\n  Results saved to: {out_file}")

    # Per-event detail table
    print(f"\n  --- Event Detail ---")
    detail_cols = ['event_num', 'entity_name', 'series_ticker', 'vol_spike',
                   'price_t0', 'delta_15m', 'is_delayed_reaction',
                   'alpha_4h', 'alpha_12h', 'llm_direction']
    available = [c for c in detail_cols if c in df.columns]
    print(df[available].to_string(index=False))
    print("\n" + "=" * 70)


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PredictIQ Strategic Backtest Engine")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max number of events to process (default: all)")
    parser.add_argument("--spike-threshold", type=float, default=10.0,
                        help="Min vol_spike_multiplier to qualify as signal (default: 10.0)")
    parser.add_argument("--no-llm", action="store_true",
                        help="Skip LLM inference, only compute signal mapping + alpha")
    args = parser.parse_args()
    run_backtest(args)
