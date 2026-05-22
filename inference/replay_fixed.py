"""
One-shot replay: run FIXED prompt + RAG query on all 269 historical candidate tickers.

Key fixes vs the old runs:
  - Prompt: symmetric YES/NO (old: "MUST Buy NO if no evidence")
  - RAG query: "{title} {ticker}" (old: generic boilerplate)
  - Model: gpt-4o-mini

Output: data/gold/intelligence_briefs/replay_fixed_<timestamp>.parquet
        + console summary comparing old vs new verdicts

Run: python inference/replay_fixed.py
"""

import os, sys, glob, json, time
from datetime import datetime, timedelta, timezone

import pandas as pd
import chromadb
from openai import OpenAI
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHROMA_PATH   = os.path.join(PROJECT_ROOT, "data", "chroma")
GOLD_BRIEFS   = os.path.join(PROJECT_ROOT, "data", "gold", "intelligence_briefs")
BRIEFS_GLOB   = os.path.join(GOLD_BRIEFS, "brief_*.parquet")

REPLAY_MODEL      = "gpt-4o-mini"
SIMILARITY_FLOOR  = 0.50
RAG_WINDOW_MINS   = 4320   # 72h — same as production

openai_client  = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
_minilm        = None


# ------------------------------------------------------------------
# Embedding helpers (mirrors predict_movements._get_query_embedding)
# ------------------------------------------------------------------
def _embed(collection_name, text):
    if collection_name == "silver_news_enriched":
        resp = openai_client.embeddings.create(model="text-embedding-3-small", input=[text])
        return [resp.data[0].embedding]
    global _minilm
    if _minilm is None:
        _minilm = SentenceTransformer("all-MiniLM-L6-v2")
    return _minilm.encode([text]).tolist()


def fetch_rag(collections, query, as_of):
    """Return scored docs from ChromaDB within RAG_WINDOW_MINS of as_of."""
    if as_of is None:
        as_of = datetime.now(timezone.utc)
    if hasattr(as_of, "tzinfo") and as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)

    start_ts = int((as_of - timedelta(minutes=RAG_WINDOW_MINS)).timestamp())
    end_ts   = int(as_of.timestamp())
    docs     = []

    for col in collections:
        emb = _embed(col.name, query)
        res = col.query(
            query_embeddings=emb,
            n_results=5,
            where={"$and": [
                {"ingested_timestamp": {"$gte": start_ts}},
                {"ingested_timestamp": {"$lte": end_ts}},
            ]},
            include=["documents", "metadatas", "distances"],
        )
        if res["documents"] and res["documents"][0]:
            for i, doc in enumerate(res["documents"][0]):
                score = 1.0 / (1.0 + res["distances"][0][i])
                if score >= SIMILARITY_FLOOR:
                    docs.append({
                        "source":  res["metadatas"][0][i].get("source", col.name),
                        "content": doc,
                        "score":   score,
                    })

    return sorted(docs, key=lambda x: x["score"], reverse=True)


# ------------------------------------------------------------------
# Fixed symmetric prompt
# ------------------------------------------------------------------
_PROMPT = """Analyze this PREDICTIVE opportunity for a 'Delayed Reaction' (Under-reaction).

MARKET: {ticker} | {title}
CURRENT ODDS: {odds_pct}%

EVIDENCE FOUND IN NEWS:
{rag_text}

TASK:
1. BULL CASE: Fundamental reasons why the 'YES' outcome might be more likely than the current {odds_pct_int}% odds.
2. BEAR CASE: Fundamental reasons why the 'NO' outcome might be more likely than the market implies.
3. VERDICT: If confidence in YES >> current odds, recommend 'Buy YES'. If << odds, 'Buy NO'. Otherwise 'No Trade'.

RULES:
- Be highly critical. If news is vague or absent for EITHER side, recommend 'No Trade'.
- Treat YES and NO symmetrically: lack of evidence is NOT a reason to prefer NO. Only recommend a side if concrete evidence shifts the probability materially from current odds.
- Respond ONLY with a JSON object: bull_case, bear_case, verdict, recommended_side, confidence_pct, decision_reason.
- recommended_side must be exactly "yes" or "no" (lowercase).
- confidence_pct must be an integer 0-100.
- verdict MUST start with one of: "Buy YES", "Buy NO", or "No Trade".
- decision_reason: 1-2 sentences. Reference the single most important piece of evidence. Be specific."""


def call_llm(ticker, title, current_odds, context_docs):
    odds_pct = round(current_odds * 100, 1)
    rag_text = "\n\n".join(
        f"Source: {d['source']}\n{d['content'][:800]}" for d in context_docs
    ) if context_docs else "(no matching news found)"

    prompt = _PROMPT.format(
        ticker      = ticker,
        title       = title,
        odds_pct    = odds_pct,
        odds_pct_int= int(odds_pct),
        rag_text    = rag_text,
    )

    for attempt in (1, 2):
        try:
            resp = openai_client.chat.completions.create(
                model           = REPLAY_MODEL,
                messages        = [{"role": "user", "content": prompt}],
                response_format = {"type": "json_object"},
            )
            raw = resp.choices[0].message.content
            return json.loads(raw), resp.usage.prompt_tokens, resp.usage.completion_tokens
        except Exception as e:
            if attempt == 1:
                time.sleep(10)
            else:
                print(f"    [ERROR] {e}")
                return None, 0, 0


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    if not os.getenv("OPENAI_API_KEY"):
        print("OPENAI_API_KEY not set. Aborting.")
        return

    # Load unique candidates from existing briefs
    files = sorted(glob.glob(BRIEFS_GLOB))
    if not files:
        print("No brief parquets found. Aborting."); return

    dfs  = [pd.read_parquet(f) for f in files]
    df   = pd.concat(dfs, ignore_index=True)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True, errors="coerce")
    # One row per ticker: most recent metadata
    cands = df.sort_values("ingested_at").groupby("ticker").last().reset_index()
    print(f"Candidates: {len(cands)} unique tickers | model: {REPLAY_MODEL}\n")

    chroma = chromadb.PersistentClient(path=CHROMA_PATH)
    try:
        cols = [
            chroma.get_collection("silver_news_enriched"),
            chroma.get_collection("silver_gdelt_enriched"),
        ]
    except Exception as e:
        print(f"ChromaDB error: {e}"); return

    results      = []
    total_cost   = 0.0
    no_rag_count = 0

    for i, row in cands.iterrows():
        ticker       = row["ticker"]
        title        = str(row.get("title", ""))
        current_odds = float(row.get("current_odds", 0.5))
        old_verdict  = str(row.get("verdict", ""))[:50]
        old_side     = str(row.get("recommended_side", ""))
        as_of        = row.get("ingested_at")

        # FIXED: title + ticker gives ChromaDB proper nouns to match against
        context_docs = fetch_rag(cols, f"{title} {ticker}", as_of)

        if not context_docs:
            no_rag_count += 1
            print(f"  [SKIP] {ticker} — no RAG context")
            continue

        brief, in_tok, out_tok = call_llm(ticker, title, current_odds, context_docs)
        if brief is None:
            continue

        cost        = (in_tok / 1_000_000) * 0.150 + (out_tok / 1_000_000) * 0.600
        total_cost += cost
        verdict     = str(brief.get("verdict", ""))
        new_side    = str(brief.get("recommended_side", "")).lower()
        confidence  = float(brief.get("confidence_pct", 50)) / 100.0
        reason      = str(brief.get("decision_reason", ""))

        changed = "CHANGED" if old_side != new_side else ""
        print(f"  [{i+1}/{len(cands)}] {ticker}")
        print(f"    OLD: {old_verdict}  NEW: {verdict[:55]}  {changed}")

        results.append({
            "ticker":          ticker,
            "title":           title,
            "current_odds":    current_odds,
            "old_verdict":     old_verdict,
            "old_side":        old_side,
            "new_verdict":     verdict,
            "new_side":        new_side,
            "new_confidence":  confidence,
            "rag_docs":        len(context_docs),
            "decision_reason": reason,
            "replay_model":    REPLAY_MODEL,
            "replayed_at":     datetime.now(timezone.utc).isoformat(),
        })

    # Summary
    print(f"\n{'='*60}")
    print(f"Replayed: {len(results)} | Skipped (no RAG): {no_rag_count} | Cost: ${total_cost:.4f}")
    print()

    if not results:
        print("No results to save."); return

    out_df = pd.DataFrame(results)

    v_new = out_df["new_verdict"].str[:8].value_counts()
    v_old = out_df["old_verdict"].str[:8].value_counts()

    print("OLD verdict distribution:")
    print(v_old.to_string())
    print()
    print("NEW verdict distribution:")
    print(v_new.to_string())
    print()
    print(f"YES (old): {(out_df['old_side']=='yes').sum()}  YES (new): {(out_df['new_side']=='yes').sum()}")
    print(f"NO  (old): {(out_df['old_side']=='no').sum()}   NO  (new): {(out_df['new_side']=='no').sum()}")
    print()

    changed = out_df[out_df["old_side"] != out_df["new_side"]]
    if not changed.empty:
        print(f"Verdict flips ({len(changed)}):")
        print(changed[["ticker", "current_odds", "old_side", "new_side", "new_confidence"]].to_string(index=False))

    ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out = os.path.join(GOLD_BRIEFS, f"replay_fixed_{ts}.parquet")
    out_df.to_parquet(out, index=False)
    print(f"\nSaved → {out}")


if __name__ == "__main__":
    main()
