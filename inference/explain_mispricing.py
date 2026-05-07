import os
import sys
import platform
# ChromaDB/SQLite patch — only needed on Linux (Docker). Windows uses native SQLite.
if platform.system() == "Linux":
    try:
        __import__('pysqlite3')
        sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
    except ImportError:
        pass

import duckdb
import chromadb
from groq import Groq
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
GOLD_BRIEFS    = os.path.join(PROJECT_ROOT, "data", "gold", "intelligence_briefs")
CHROMA_PATH    = os.path.join(PROJECT_ROOT, "data", "chroma")

# Similarity Tuning
# Chroma default distance is L2. We convert to similarity via 1/(1+d).
# With all-MiniLM-L6-v2 embeddings, L2 distances for related content
# cluster around 0.9-1.3, yielding similarities of ~0.43-0.53.
# A floor of 0.45 filters noise while admitting genuinely related content.
SIMILARITY_FLOOR = 0.45

# Configure Groq (free tier: 14,400 req/day, 30 req/min)
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
GROQ_MODEL = "llama-3.3-70b-versatile"

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
def fetch_rag_context(collections, query_text, current_time, window_mins=15, debug=False):
    """
    Search multiple Chroma collections and return merged, scored documents.
    Uses L2 distance -> similarity conversion: score = 1 / (1 + distance).
    """
    start_ts = int((current_time - timedelta(minutes=window_mins)).timestamp())
    end_ts   = int(current_time.timestamp())
    scored_docs = []
    
    # Handle single collection or list
    if not isinstance(collections, list):
        collections = [collections]

    for coll in collections:
        results = coll.query(
            query_texts=[query_text],
            n_results=3,
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
                # L2 distance to similarity: bounded [0, 1], higher = better
                score = 1.0 / (1.0 + distance)
                
                if score >= SIMILARITY_FLOOR:
                    scored_docs.append({
                        "content": results['documents'][0][i],
                        "source": results['metadatas'][0][i].get('source', coll.name),
                        "score": score
                    })
                elif debug:
                    safe_text = results['documents'][0][i][:60].encode('ascii', errors='replace').decode('ascii')
                    print(f"      [REJECTED: {score*100:.1f}%] {coll.name}: {safe_text}...")
    
    # Sort by descending score
    scored_docs = sorted(scored_docs, key=lambda x: x['score'], reverse=True)
    return scored_docs

def cascading_rag_search(collections, query_text, current_time, debug=False):
    """
    Performs 15m -> 2h cascade across all provided collections.
    Returns (docs, window_label) tuple.
    """
    # 1. Flash Window (15m)
    docs_15m = fetch_rag_context(collections, query_text, current_time, 15, debug)
    if len(docs_15m) >= 2:
        return docs_15m, "15-Minute Flash Reaction"

    # 2. Lagging Window (2h)
    docs_2h = fetch_rag_context(collections, query_text, current_time, 120, debug)
    if len(docs_2h) >= 2:
        return docs_2h, "2-Hour Echo Chamber Check"

    # 3. Final Fallback (Empty) - For Ghost Pump detection
    return [], "Ghost Pump / Echo Chamber Check"

# ---------------------------------------------------------
# 4. LLM OUTPUT CLEANUP
# ---------------------------------------------------------
def _flatten_llm_field(value):
    """Converts nested LLM output (dicts/lists) into a clean readable string.
    Prioritizes 'description', 'reason', 'analysis' keys when present."""
    if isinstance(value, str):
        # Check if the string is secretly JSON
        try:
            parsed = json.loads(value)
            return _flatten_llm_field(parsed)
        except (json.JSONDecodeError, TypeError):
            return value
    if isinstance(value, dict):
        # Priority keys that contain the actual readable content
        for key in ('description', 'reason', 'analysis', 'explanation', 'text', 'content'):
            if key in value and isinstance(value[key], str):
                return value[key]
        # Fallback: join all string values
        parts = [str(v) for v in value.values() if v]
        return ' '.join(parts)
    if isinstance(value, list):
        return ' '.join([_flatten_llm_field(item) for item in value])
    return str(value)

# ---------------------------------------------------------
# 5. THE BOOKMAKER LLM
# ---------------------------------------------------------
def generate_intelligence_brief(market, context_docs, anomaly_type):
    """
    Sends market anomaly + RAG context to Groq (Llama 3.3 70B) for bull/bear/verdict synthesis.
    Returns a dict with keys: bull_case, bear_case, verdict.
    """
    if not groq_client:
        return {"bull_case": "N/A", "bear_case": "N/A", "verdict": "GROQ_API_KEY missing."}

    # Extract raw text for LLM
    rag_text = "\n\n".join([d['content'] for d in context_docs])
    
    prompt = f"""Analyze this prediction market anomaly.
MARKET: {market['ticker']} | {market['title']}
RULES: {market['rules_primary']}
ODDS MOVE: {market['previous_odds']*100:.1f}% -> {market['current_odds']*100:.1f}% ({anomaly_type})

EVIDENCE:
{rag_text}

Respond with ONLY a JSON object containing exactly these keys: bull_case, bear_case, verdict."""
    try:
        response = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.3
        )
        res_data = json.loads(response.choices[0].message.content)
        
        # Ensure flat dictionary with string values to prevent Parquet schema errors
        if isinstance(res_data, list) and len(res_data) > 0:
            res_data = res_data[0]
            
        if isinstance(res_data, dict):
            # Flatten nested LLM outputs into clean readable strings
            for k in list(res_data.keys()):
                res_data[k] = _flatten_llm_field(res_data[k])
            return res_data
            
        return {"bull_case": "N/A", "bear_case": "N/A", "verdict": "Invalid JSON format"}
    except Exception as e:
        return {"bull_case": "Error", "bear_case": "Error", "verdict": str(e)}

def save_briefs_to_gold(results):
    """Save intelligence briefs to Gold layer as a timestamped Parquet file."""
    if not results:
        return
    os.makedirs(GOLD_BRIEFS, exist_ok=True)
    out_file = os.path.join(GOLD_BRIEFS, f"briefs_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet")
    pd.DataFrame(results).to_parquet(out_file, index=False)
    print(f"[Inference] Saved {len(results)} verdicts to Gold Ledger.")
    print(f"[Inference] Output file: {os.path.abspath(out_file)}")

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
        active_collections = [
            chroma_client.get_collection(name="silver_news_enriched"),
            chroma_client.get_collection(name="silver_gdelt_enriched")
        ]
    except Exception as e:
        print(f"[Inference] ERROR: Chroma collections not ready: {e}")
        return

    inference_results = []
    current_time = datetime.now(timezone.utc)

    for _, market in volatile_markets.iterrows():
      try:
        print(f"\n[Inference] ANALYZING: {market['ticker']} (Delta: {market['odds_delta']*100:.1f}%)")
        
        # Use the Market's own ingestion time as the anchor for context retrieval.
        # This fixes the 'Empty Context' bug for historical audits.
        market_time = pd.to_datetime(market['ingested_at'])
        if market_time.tzinfo is None:
            market_time = market_time.replace(tzinfo=timezone.utc)
            
        # Concatenate Title + Rules for maximum settlement precision (NotebookLM Standard)
        query_text = f"{market['title']} {market['rules_primary']}"
        context_docs, window_type = cascading_rag_search(active_collections, query_text, market_time, args.debug)
        
        if args.debug:
            print(f"    > X-Ray Window: {window_type}")
            for d in context_docs:
                safe_text = d['content'][:80].encode('ascii', errors='replace').decode('ascii')
                print(f"      [{d['score']*100:.0f}% Match] {d['source']}: {safe_text}...")

        if not context_docs:
            # GHOST PUMP PATH: No corroborating evidence found
            print(f"    > No quality news found above {SIMILARITY_FLOOR*100:.0f}% floor. Recording GHOST PUMP.")
            inference_results.append({
                "ticker": market['ticker'],
                "title": market['title'],
                "current_odds": market['current_odds'],
                "odds_delta": market['odds_delta'],
                "bull_case": "N/A - No Corroborating News Found",
                "bear_case": "N/A - No Corroborating News Found",
                "verdict": f"GHOST PUMP: Price moved {market['odds_delta']*100:.1f}%, but no fundamental news appeared in the {window_type} window above {SIMILARITY_FLOOR*100:.0f}% similarity. This is likely a retail-driven behavioral distortion.",
                "confidence_score": 0.0,
                "event_at": market['ingested_at'],
                "ingested_at": current_time.isoformat()
            })
            continue

        # EVIDENCE FOUND PATH: Synthesize intelligence via Gemini
        print(f"    > Synthesizing {len(context_docs)} signals via Gemini...")
        brief = generate_intelligence_brief(market, context_docs, window_type)
        confidence = max([d['score'] for d in context_docs])
        brief.update({
            "ticker": market['ticker'],
            "title": market['title'],
            "current_odds": market['current_odds'],
            "odds_delta": market['odds_delta'],
            "confidence_score": confidence,
            "event_at": market['ingested_at'],
            "ingested_at": current_time.isoformat()
        })
        inference_results.append(brief)
        
        print(f"--- VERDICT [{market['ticker']}] ---")
        print(f"VERDICT: {brief['verdict']}")

      except Exception as e:
        print(f"    > ERROR processing {market['ticker']}: {e}")
        continue
    
    save_briefs_to_gold(inference_results)

if __name__ == "__main__":
    main()