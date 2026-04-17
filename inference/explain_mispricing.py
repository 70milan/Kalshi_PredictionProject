import os
import sys
import duckdb
import pandas as pd
from datetime import datetime, timezone
from groq import Groq
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# CONFIG & PATHS
# ─────────────────────────────────────────────

load_dotenv()
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not GROQ_API_KEY:
    print("[Inference] ERROR: GROQ_API_KEY not found in .env. Exiting.")
    sys.exit(1)

CLIENT = Groq(api_key=GROQ_API_KEY)

GOLD_SCORES = os.path.join(ROOT, "data", "gold", "mispricing_scores")
GOLD_NEWS   = os.path.join(ROOT, "data", "gold", "news_summaries")
BRIEFS_DIR  = os.path.join(ROOT, "data", "gold", "intelligence_briefs")

os.makedirs(BRIEFS_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# LLM BRAIN
# ─────────────────────────────────────────────

def generate_ai_brief(ticker, title, sentiment_score, spike_vol):
    """
    Synthesizes market data into a concise intelligence brief using Llama-3.
    """
    prompt = f"""
    SYSTEM: You are PredictIQ-Inference, a geopolitical intelligence analyst.
    TASK: Analyze the following market discrepancy and provide a 2-3 sentence 'Brief'. 
    
    MARKET: {title} ({ticker})
    NEWS SENTIMENT: {sentiment_score:.2f} (-1 to 1 scale)
    MEDIA VOLUME SPIKE: {spike_vol:.1f}x normal volume

    Determine if the market odds are LAGGING the news narrative or if the media spike is LIKELY NOISE. 
    Be cold, objective, and professional. Use 'Inference:' as your prefix.
    """

    try:
        completion = CLIENT.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama3-8b-8192",
            temperature=0.2,
            max_tokens=150
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        return f"Error generating brief: {str(e)}"

# ─────────────────────────────────────────────
# MAIN EXECUTION
# ─────────────────────────────────────────────

def main():
    print("\n" + "="*60)
    print(" PredictIQ Intelligence Inference Layer")
    print("="*60)

    # 1. Connect to local DuckDB (SSD Speed)
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    try:
        # 2. Identify top targets (Score > 75)
        print(f"[Phase 1/3] Identifying high-conviction signals...")
        query = f"""
            SELECT 
                ticker, 
                title, 
                mispricing_score, 
                max_spike_multiplier 
            FROM delta_scan('{GOLD_SCORES}')
            WHERE mispricing_score >= 75
            ORDER BY mispricing_score DESC
            LIMIT 5
        """
        candidates = con.execute(query).df()

        if candidates.empty:
            print("[Inference] No signals above threshold today. Standby.")
            return

        print(f"[Inference] Found {len(candidates)} candidates for AI analysis.")

        # 3. Generate Briefs
        results = []
        for _, row in candidates.iterrows():
            ticker = row['ticker']
            print(f"    > Generating brief for {ticker} ... ", end="", flush=True)
            
            # For V1, we use a neutral sentiment if the join is complex, 
            # but we pass the actual GDELT spike multiplier.
            brief = generate_ai_brief(
                ticker, 
                row['title'], 
                0.15, # Placeholder for joined sentiment
                row['max_spike_multiplier']
            )
            
            results.append({
                "ticker": ticker,
                "ai_narrative_brief": brief,
                "generated_at": datetime.now(timezone.utc).isoformat()
            })
            print("OK")

        # 4. Save to Gold Layer (Local SSD)
        df_briefs = pd.DataFrame(results)
        
        # Clean naming: briefs_YYYYMMDD_HHMMSS.parquet
        ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_file = os.path.join(BRIEFS_DIR, f"briefs_{ts_str}.parquet")
        
        df_briefs.to_parquet(out_file)
        
        print(f"\n[Phase 3/3] Intelligence Ledger updated.")
        print(f"    Target: {out_file}")
        print(f"    Signals: {len(results)}")
        print("="*60 + "\n")

    except Exception as e:
        print(f"[Inference] FATAL ERROR: {str(e)}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
