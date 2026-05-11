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
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import time

# ---------------------------------------------------------
# 1. SETUP & CONFIGURATION
# ---------------------------------------------------------
load_dotenv()
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

GOLD_MISPRICING = os.path.join(PROJECT_ROOT, "data", "gold", "mispricing_scores")
GOLD_BRIEFS     = os.path.join(PROJECT_ROOT, "data", "gold", "intelligence_briefs")
CHROMA_PATH     = os.path.join(PROJECT_ROOT, "data", "chroma")

SIMILARITY_FLOOR = 0.35

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
groq_client  = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
GROQ_MODEL   = "llama-3.1-8b-instant"

# ---------------------------------------------------------
# 2. CANDIDATE SELECTION
# ---------------------------------------------------------
def get_predictive_candidates(con, min_score=80.0):
    print(f"[Predictive] Scanning Gold Mispricing Ledger for scores >= {min_score}...")
    query = f"""
    SELECT ticker, title, yes_bid as current_odds, delta_15m, mispricing_score,
           max_spike_multiplier, sentiment_signal, ingested_at
    FROM delta_scan('{GOLD_MISPRICING}')
    WHERE flagged_candidate = true
      AND mispricing_score >= {min_score}
      AND ingested_at >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
    ORDER BY mispricing_score DESC, delta_15m ASC
    LIMIT 3;
    """
    try:
        df = con.execute(query).df()
        if not df.empty:
            df['previous_odds'] = df['current_odds'] / (1 + df['delta_15m'])
        return df
    except Exception as e:
        print(f"[Predictive] ERROR checking Gold ledger: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------
# 3. LLM OUTPUT CLEANUP
# ---------------------------------------------------------
def _flatten_llm_field(value):
    if isinstance(value, str):
        if value.strip().startswith('{') or value.strip().startswith('['):
            try:
                return _flatten_llm_field(json.loads(value))
            except:
                pass
        return value
    if isinstance(value, dict):
        for key in ('description', 'reason', 'analysis', 'explanation', 'text', 'content', 'evidence', 'verdict'):
            if key in value:
                return _flatten_llm_field(value[key])
        parts = []
        for k, v in value.items():
            if k in ('direction', 'strength', 'sentiment'):
                parts.append(f"[{str(v).upper()}]")
            else:
                parts.append(_flatten_llm_field(v))
        return ' '.join([p for p in parts if p])
    if isinstance(value, list):
        return ' '.join([_flatten_llm_field(item) for item in value])
    return str(value)

# ---------------------------------------------------------
# 4. RAG SEARCH
# ---------------------------------------------------------
def fetch_rag_context(collections, query_text, current_time, window_mins=1440):
    start_ts    = int((current_time - timedelta(minutes=window_mins)).timestamp())
    end_ts      = int(current_time.timestamp())
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
                score    = 1.0 / (1.0 + distance)
                if score >= SIMILARITY_FLOOR:
                    scored_docs.append({
                        "content": results['documents'][0][i],
                        "source":  results['metadatas'][0][i].get('source', coll.name),
                        "score":   score
                    })

    return sorted(scored_docs, key=lambda x: x['score'], reverse=True)

# ---------------------------------------------------------
# 5. LLM SYNTHESIS
# ---------------------------------------------------------
def generate_predictive_brief(market, context_docs):
    """Returns a dict with bull_case/bear_case/verdict, or None on 429."""
    if not groq_client:
        return {"bull_case": "N/A", "bear_case": "N/A", "verdict": "GROQ_API_KEY missing."}

    rag_text   = "\n\n".join([f"Source: {d['source']}\n{d['content'][:800]}" for d in context_docs])
    sent       = market['sentiment_signal']
    sent_label = "Strongly Positive" if sent > 0.5 else ("Strongly Negative" if sent < -0.5 else "Neutral/Mixed")

    prompt = f"""Analyze this PREDICTIVE opportunity for a 'Delayed Reaction' (Under-reaction).

MARKET: {market['ticker']} | {market['title']}
CURRENT ODDS: {market['current_odds']*100:.1f}%
NEWS SPIKE: {market['max_spike_multiplier']:.1f}x baseline volume
SENTIMENT INTENSITY: {sent:.2f} ({sent_label})

EVIDENCE FOUND IN NEWS:
{rag_text}

TASK:
1. BULL CASE: Fundamental reasons why the 'YES' outcome might be more likely than the current {market['current_odds']*100:.0f}% odds.
2. BEAR CASE: Fundamental reasons why the 'NO' outcome might be more likely than the market implies.
3. VERDICT: If confidence in YES >> current odds, recommend 'Buy YES'. If << odds, 'Buy NO'. Otherwise 'No Trade'.

RULES:
- Do NOT repeat volume numbers. Explain the news fundamentals.
- STRICT RULE: Never recommend 'Buy YES' on a speculative basis just because the odds are low. If there is NO CONCRETE EVIDENCE in the text supporting the outcome, you MUST recommend 'Buy NO' or 'No Trade'. Do not act as a pundit.
- Be highly critical. If news is vague, recommend 'No Trade'.
- Respond ONLY with a JSON object: bull_case, bear_case, verdict, recommended_side, confidence_pct.
- recommended_side must be exactly "yes" or "no" (lowercase).
- confidence_pct must be an integer 0-100."""

    try:
        response = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.3
        )
        res_data = json.loads(response.choices[0].message.content)
        if isinstance(res_data, list) and len(res_data) > 0:
            res_data = res_data[0]

        if isinstance(res_data, dict):
            for k in list(res_data.keys()):
                if k not in ("recommended_side", "confidence_pct"):
                    res_data[k] = _flatten_llm_field(res_data[k])
            
            # Ensure valid side
            side = str(res_data.get("recommended_side", "")).strip().lower()
            if side not in ("yes", "no"):
                verdict_lower = str(res_data.get("verdict", "")).lower()
                side = "yes" if "buy yes" in verdict_lower else "no" if "buy no" in verdict_lower else "yes"
            res_data["recommended_side"] = side
            
        return res_data
    except Exception as e:
        if "429" in str(e):
            print(f"    [Rate Limit] Groq 429 — skipping this market, next cycle will retry.")
            return None
        return {"bull_case": "Error", "bear_case": "Error", "verdict": str(e)}

# ---------------------------------------------------------
# 6. MAIN
# ---------------------------------------------------------
def main():
    print("=" * 60)
    print(" PredictIQ Predictive Scanner: Delayed Reaction Detection")
    print("=" * 60)

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    candidates = get_predictive_candidates(con)
    if candidates.empty:
        print("[Predictive] No candidates found. Market is efficiently priced.")
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

    os.makedirs(GOLD_BRIEFS, exist_ok=True)
    current_time = datetime.now(timezone.utc)
    written      = 0

    for _, market in candidates.iterrows():
        market_time = pd.to_datetime(market['ingested_at'])
        if market_time.tzinfo is None:
            market_time = market_time.replace(tzinfo=timezone.utc)

        print(f"\n[Predictive] ANALYZING: {market['ticker']} (Score: {market['mispricing_score']:.1f})")

        context_docs = fetch_rag_context(active_collections, str(market['title']), market_time)
        if not context_docs:
            print(f"    > No RAG matches found. Skipping.")
            continue

        print(f"    > Synthesizing {len(context_docs)} signals...")
        brief = generate_predictive_brief(market, context_docs)

        if brief is None:
            continue  # 429 — skip, next cycle retries

        confidence = max([d['score'] for d in context_docs])
        row = {
            "ticker":           market['ticker'],
            "title":            market['title'],
            "bull_case":        _flatten_llm_field(brief.get("bull_case", "")),
            "bear_case":        _flatten_llm_field(brief.get("bear_case", "")),
            "verdict":          _flatten_llm_field(brief.get("verdict", "")),
            "recommended_side": brief.get("recommended_side", "yes"),
            "current_odds":     float(market['current_odds']),
            "odds_delta":       float(market['delta_15m']),
            "mispricing_score": float(market['mispricing_score']),
            "confidence_score": float(confidence),
            "event_at":         current_time.isoformat(),
            "ingested_at":      current_time.isoformat(),
        }

        print(f"--- VERDICT [{market['ticker']}]: {row['verdict']}")

        # Append as a timestamped Parquet file — no Spark, no JVM
        ts_str   = current_time.strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(GOLD_BRIEFS, f"brief_{ts_str}_{market['ticker']}.parquet")
        pd.DataFrame([row]).to_parquet(out_path, index=False)
        written += 1

        time.sleep(30)  # respect Groq TPM limits between calls

    print(f"\n[Predictive] Done. {written} brief(s) written to {GOLD_BRIEFS}")

if __name__ == "__main__":
    main()
