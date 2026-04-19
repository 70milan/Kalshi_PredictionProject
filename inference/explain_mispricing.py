import os
import sys
import duckdb
import chromadb
import google.generativeai as genai
import json
import pandas as pd
import argparse
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1. SETUP & CONFIGURATION
# ---------------------------------------------------------
load_dotenv()
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

KALSHI_HISTORY = os.path.join(PROJECT_ROOT, "data", "silver", "kalshi_markets_history")
GOLD_BRIEFS = os.path.join(PROJECT_ROOT, "data", "gold", "intelligence_briefs")
CHROMA_PATH  = os.path.join(PROJECT_ROOT, "data", "chroma")

# Threshold for "Master Suite" Filtering
SIMILARITY_FLOOR = 0.15 

# Configure Gemini 1.5 Pro
GEMINI_API_KEY = os.getenv("Gemini_API")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-flash-latest')

# ---------------------------------------------------------
# 2. THE GATEKEEPER (With Mock/Override logic)
# ---------------------------------------------------------
def get_candidate_markets(con, ticker_override=None, threshold=0.10):
    """
    Finds volatile markets. If ticker_override is provided, 
    bypasses the delta check for that specific ticker.
    """
    if ticker_override:
        print(f"[Inference] MOCK MODE: Forcing analysis for {ticker_override}")
        query = f"SELECT * FROM delta_scan('{KALSHI_HISTORY}') WHERE ticker = '{ticker_override}' ORDER BY ingested_at DESC LIMIT 1"
        df = con.execute(query).df()
        if not df.empty:
            # Set mock values to satisfy the schema
            df['current_odds'] = df['yes_bid']
            df['previous_odds'] = df['yes_bid']
            df['odds_delta'] = 0.0
            return df
        return pd.DataFrame()

    query = f"""
    WITH recent_snapshots AS (
        SELECT ticker, title, rules_primary, yes_bid, ingested_at,
               ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY ingested_at DESC) as rn
        FROM delta_scan('{KALSHI_HISTORY}')
    )
    SELECT current.ticker, current.title, current.rules_primary, current.ingested_at,
           current.yes_bid AS current_odds, previous.yes_bid AS previous_odds,
           ABS(current.yes_bid - previous.yes_bid) AS odds_delta
    FROM recent_snapshots current
    JOIN recent_snapshots previous ON current.ticker = previous.ticker AND previous.rn = 2
    WHERE current.rn = 1 AND ABS(current.yes_bid - previous.yes_bid) >= {threshold};
    """
    try:
        return con.execute(query).df()
    except Exception as e:
        print(f"[Inference] ERROR checking markets: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------
# 3. CASCADING RAG SEARCH (With Similarity Logic)
# ---------------------------------------------------------
def fetch_rag_context(collection, query_text, current_time, window_mins=15, debug=False):
    """
    Helper to search a specific window and return scored documents.
    """
    start_ts = int((current_time - timedelta(minutes=window_mins)).timestamp())
    results = collection.query(
        query_texts=[query_text],
        n_results=10,
        where={"ingested_timestamp": {"$gte": start_ts}},
        include=["documents", "metadatas", "distances"]
    )
    
    scored_docs = []
    if results['documents'] and results['documents'][0]:
        for i in range(len(results['documents'][0])):
            distance = results['distances'][0][i]
            score = 1.0 - distance # Similarity Approximation
            
            if score >= SIMILARITY_FLOOR:
                scored_docs.append({
                    "content": results['documents'][0][i],
                    "source": results['metadatas'][0][i].get('source', 'GDELT'),
                    "score": score
                })
            elif debug:
                print(f"      [REJECTED: {score*100:.1f}%] {results['metadatas'][0][i].get('source', '???')}: {results['documents'][0][i][:60]}...")
    return scored_docs

def cascading_rag_search(news_collection, query_text, current_time, debug=False):
    """
    Performs 15m -> 2h cascade. Rejects data below SIMILARITY_FLOOR.
    """
    # 1. Flash Window (15m) - Primary production reaction
    docs_15m = fetch_rag_context(news_collection, query_text, current_time, 15, debug)
    if len(docs_15m) >= 2:
        return docs_15m, "15-Minute Flash Reaction"

    # 2. Lagging Window (2h) - Secondary check for older headlines
    docs_2h = fetch_rag_context(news_collection, query_text, current_time, 120, debug)
    if len(docs_2h) >= 2:
        return docs_2h, "2-Hour Echo Chamber Check"

    # 3. Recovery Window (24h) - Secondary historical check
    docs_24h = fetch_rag_context(news_collection, query_text, current_time, 1440, debug)
    if len(docs_24h) >= 2:
        return docs_24h, "24-Hour Historical Recovery"

    # 4. Amnesty Window (7 Days) - Final fallback for deep historical audits
    docs_7d = fetch_rag_context(news_collection, query_text, current_time, 10080, debug)
    return docs_7d, "7-Day Amnesty Window"

# ---------------------------------------------------------
# 4. THE BOOKMAKER LLM
# ---------------------------------------------------------
def generate_intelligence_brief(market, context_docs, anomaly_type):
    if not GEMINI_API_KEY:
        return {"bull_case": "N/A", "bear_case": "N/A", "verdict": "Gemini key missing."}

    # Extract raw text for LLM
    rag_text = "\n\n".join([d['content'] for d in context_docs])
    
    prompt = f"""
    Analyze this prediction market anomaly.
    MARKET: {market['ticker']} | {market['title']}
    RULES: {market['rules_primary']}
    ODDS MOVE: {market['previous_odds']*100:.1f}% -> {market['current_odds']*100:.1f}% ({anomaly_type})
    
    EVIDENCE:
    {rag_text}
    
    Output JSON with: bull_case, bear_case, verdict.
    """
    try:
        response = model.generate_content(prompt, generation_config=genai.GenerationConfig(response_mime_type="application/json"))
        res_data = json.loads(response.text)
        # Ensure we return a dictionary, even if LLM wrapped it in a list
        if isinstance(res_data, list) and len(res_data) > 0:
            return res_data[0]
        return res_data if isinstance(res_data, dict) else {"bull_case": "N/A", "bear_case": "N/A", "verdict": "Invalid JSON format"}
    except Exception as e:
        return {"bull_case": "Error", "bear_case": "Error", "verdict": str(e)}

def save_briefs_to_gold(results):
    if not results: return
    os.makedirs(GOLD_BRIEFS, exist_ok=True)
    out_file = os.path.join(GOLD_BRIEFS, f"briefs_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet")
    pd.DataFrame(results).to_parquet(out_file, index=False)
    print(f"[Inference] Saved {len(results)} verdicts to Gold Ledger.")

# ---------------------------------------------------------
# 5. MAIN
# ---------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="PredictIQ Inference Engine (Phase 4.2)")
    parser.add_argument("--ticker", help="Force analysis of a specific ticker (Mock Mode)")
    parser.add_argument("--threshold", type=float, default=0.10, help="Min price delta to trigger (default 0.10)")
    parser.add_argument("--debug", action="store_true", help="Print X-Ray similarity scores")
    args = parser.parse_args()

    print("=" * 60)
    print(" PredictIQ Inference Engine: MASTER SUITE (X-Ray Mode)")
    print("=" * 60)

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    volatile_markets = get_candidate_markets(con, args.ticker, args.threshold)
    if volatile_markets.empty:
        print("[Inference] No target markets found. Sleeping.")
        return

    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
    try:
        news_collection = chroma_client.get_collection(name="silver_news_enriched")
    except:
        print("[Inference] ERROR: Chroma not ready.")
        return

    inference_results = []
    current_time = datetime.now(timezone.utc)

    for _, market in volatile_markets.iterrows():
        print(f"\n[Inference] ANALYZING: {market['ticker']} (Delta: {market['odds_delta']*100:.1f}%)")
        
        # Use the Market's own ingestion time as the anchor for context
        # This fixes the 'Empty Context' bug for historical audits.
        market_time = pd.to_datetime(market['ingested_at'])
        if market_time.tzinfo is None:
            market_time = market_time.replace(tzinfo=timezone.utc)
            
        # Use ONLY the title for search to maximize similarity (rules_primary is too noisy for RAG)
        query_text = market['title']
        context_docs, window_type = cascading_rag_search(news_collection, query_text, market_time, args.debug)
        
        if args.debug:
            print(f"    > X-Ray Window: {window_type}")
            for d in context_docs:
                print(f"      [{d['score']*100:.0f}% Match] {d['source']}: {d['content'][:80]}...")

        if not context_docs:
            print(f"    > No quality news found above {SIMILARITY_FLOOR*100:.0f}% floor. Skipping AI synthesis.")
            continue

        print(f"    > Synthesizing {len(context_docs)} signals via Gemini...")
        brief = generate_intelligence_brief(market, context_docs, window_type)
        brief.update({
            "ticker": market['ticker'], "ingested_at": current_time.isoformat(),
            "odds_delta": market['odds_delta'], "confidence_score": max([d['score'] for d in context_docs])
        })
        inference_results.append(brief)
        
        print(f"--- VERDICT [{market['ticker']}] ---")
        print(f"VERDICT: {brief['verdict']}")
    
    save_briefs_to_gold(inference_results)

if __name__ == "__main__":
    main()