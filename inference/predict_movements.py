import os
import sys
# ChromaDB/SQLite patch — only needed on Linux (Docker). Windows uses native SQLite.
import platform
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

# Paths to the Gold Lakehouse
GOLD_MISPRICING = os.path.join(PROJECT_ROOT, "data", "gold", "mispricing_scores")
GOLD_BRIEFS     = os.path.join(PROJECT_ROOT, "data", "gold", "intelligence_briefs")
CHROMA_PATH     = os.path.join(PROJECT_ROOT, "data", "chroma")

# Similarity Tuning
SIMILARITY_FLOOR = 0.35

# Configure Groq
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
GROQ_MODEL = "llama-3.1-8b-instant"

# ---------------------------------------------------------
# 2. CANDIDATE SELECTION (PREDICTIVE LOGIC)
# ---------------------------------------------------------
def get_predictive_candidates(con, min_score=80.0):
    """
    Selects markets flagged by the Gold Layer as having a 
    News Spike > Price Move gap (The 'Delayed Reaction' signal).
    """
    print(f"[Predictive] Scanning Gold Mispricing Ledger for scores >= {min_score}...")
    query = f"""
    SELECT ticker, title, yes_bid as current_odds, delta_15m, mispricing_score, max_spike_multiplier, sentiment_signal, ingested_at
    FROM delta_scan('{GOLD_MISPRICING}')
    WHERE flagged_candidate = true AND mispricing_score >= {min_score}
    ORDER BY mispricing_score DESC, delta_15m ASC
    LIMIT 20;
    """
    try:
        df = con.execute(query).df()
        if not df.empty:
            # Add a mock 'previous_odds' for the LLM prompt consistency
            df['previous_odds'] = df['current_odds'] / (1 + df['delta_15m'])
        return df
    except Exception as e:
        print(f"[Predictive] ERROR checking Gold ledger: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------
# 3. RAG SEARCH & LLM SYNTHESIS
# ---------------------------------------------------------
def fetch_rag_context(collections, query_text, current_time, window_mins=120):
    """Search Chroma for relevant news within a 2-hour window."""
    start_ts = int((current_time - timedelta(minutes=window_mins)).timestamp())
    end_ts   = int(current_time.timestamp())
    scored_docs = []
    
    if not isinstance(collections, list):
        collections = [collections]

    for coll in collections:
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
    
    return sorted(scored_docs, key=lambda x: x['score'], reverse=True)

def generate_predictive_brief(market, context_docs):
    """Synthesizes the 'Delayed Reaction' verdict via Groq."""
    if not groq_client:
        return {"bull_case": "N/A", "bear_case": "N/A", "verdict": "GROQ_API_KEY missing."}

    rag_text = "\n\n".join([f"Source: {d['source']}\n{d['content'][:800]}" for d in context_docs])
    
    # Map raw sentiment score to a human-readable label
    sent = market['sentiment_signal']
    sent_label = "Strongly Positive" if sent > 0.5 else ("Strongly Negative" if sent < -0.5 else "Neutral/Mixed")

    prompt = f"""Analyze this PREDICTIVE opportunity for a 'Delayed Reaction' (Under-reaction). 
The news volume and sentiment signal are significantly stronger than the market's price movement.

MARKET: {market['ticker']} | {market['title']}
CURRENT ODDS: {market['current_odds']*100:.1f}%
NEWS SPIKE: {market['max_spike_multiplier']}x baseline volume
SENTIMENT INTENSITY: {sent:.2f} ({sent_label})
MISPRICING SCORE: {market['mispricing_score']}/100

EVIDENCE FOUND IN NEWS:
{rag_text}

TASK:
1. Identify if the news sentiment directionally supports the 'YES' or 'NO' outcome for this specific market.
2. Determine if the market is 'Under-reacting' (i.e., the price should have moved MORE than {market['delta_15m']*100:.1f}% based on this news).
3. Provide a Bull Case, Bear Case, and a final Verdict (Buy YES, Buy NO, or No Trade).

Respond ONLY with a JSON object containing: bull_case, bear_case, verdict."""

    try:
        response = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.3
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        return {"bull_case": "Error", "bear_case": "Error", "verdict": str(e)}


# ---------------------------------------------------------
# 4. MAIN
# ---------------------------------------------------------
def main():
    print("=" * 60)
    print(" PredictIQ Predictive Scanner: Delayed Reaction Detection")
    print("=" * 60)

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    candidates = get_predictive_candidates(con)
    if candidates.empty:
        print("[Predictive] No 'Delayed Reaction' opportunities found. Market is efficiently priced.")
        return

    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
    try:
        active_collections = [
            chroma_client.get_collection(name="silver_news_enriched"),
            chroma_client.get_collection(name="silver_gdelt_enriched")
        ]
    except Exception as e:
        print(f"[Predictive] ERROR: Chroma collections not ready: {e}")
        return

    inference_results = []
    current_time = datetime.now(timezone.utc)

    for _, market in candidates.iterrows():
        # Use the record's own timestamp as the anchor for context retrieval.
        # This prevents skipping valid signals during historical scans.
        market_time = pd.to_datetime(market['ingested_at'])
        if market_time.tzinfo is None:
            market_time = market_time.replace(tzinfo=timezone.utc)

        print(f"\n[Predictive] ANALYZING UNDER-REACTION: {market['ticker']} (Score: {market['mispricing_score']:.1f})")
        print(f"    > Event Time: {market_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        query_text = f"{market['title']}"
        context_docs = fetch_rag_context(active_collections, query_text, market_time, window_mins=1440)

        if not context_docs:
            print(f"    > Ticker has news volume spike but RAG found no specific text matches. Skipping.")
            continue

        print(f"    > Synthesizing {len(context_docs)} signals for predictive edge...")
        brief = generate_predictive_brief(market, context_docs)
        
        # Ensure all LLM outputs are flat strings to prevent PySpark MapType vs StringType schema crashes
        for k in list(brief.keys()):
            if not isinstance(brief[k], str):
                brief[k] = json.dumps(brief[k]) if isinstance(brief[k], (dict, list)) else str(brief[k])
        
        confidence = max([d['score'] for d in context_docs])
        brief.update({
            "ticker": market['ticker'],
            "title": market['title'],
            "current_odds": market['current_odds'],
            "odds_delta": market['delta_15m'],
            "confidence_score": confidence,
            "event_at": current_time.isoformat(),
            "ingested_at": current_time.isoformat()
        })
        inference_results.append(brief)
        print(f"--- PREDICTIVE VERDICT [{market['ticker']}] ---")
        print(f"VERDICT: {brief['verdict']}")

    if inference_results:
        # --- NEW: Delta Table Storage for Dashboard & History ---
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        import shutil

        print(f"\n[Predictive] Finalizing {len(inference_results)} briefs via Spark Delta Bridge...")
        
        # Initialize Spark Session (Match the Gold layer config)
        builder = SparkSession.builder \
            .appName("PredictIQ_Brief_Writer") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.parquet.enableVectorizedReader", "false")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        try:
            # 1. Prepare Data
            df_briefs = spark.createDataFrame(pd.DataFrame(inference_results))
            
            # 2. Write Current (The React Dashboard View - Overwrite)
            # This ensures the dashboard ONLY shows the latest signals
            if os.path.exists(GOLD_BRIEFS):
                shutil.rmtree(GOLD_BRIEFS)
            
            df_briefs.write.format("delta").mode("overwrite").save(GOLD_BRIEFS)
            print(f"    > Dashboard View Updated: {GOLD_BRIEFS}")

            # 3. Write History (The Audit Ledger - Append)
            history_path = GOLD_BRIEFS + "_history"
            df_briefs.write.format("delta").mode("append").option("mergeSchema", "true").save(history_path)
            print(f"    > Audit Ledger Appended: {history_path}")

        finally:
            spark.stop()

if __name__ == "__main__":
    main()
