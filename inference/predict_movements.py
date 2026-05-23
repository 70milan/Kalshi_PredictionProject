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
from openai import OpenAI
from sentence_transformers import SentenceTransformer
import json
import math
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import time

# ---------------------------------------------------------
# 1. SETUP & CONFIGURATION
# ---------------------------------------------------------
load_dotenv(override=True)  # .env values WIN over pre-existing OS env vars
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "api"))
from orchestration.notify import notify_failure, notify_brief
from kelly_math import calculate_kelly
from exit_evaluator import TAKE_PROFIT_ROI, STOP_LOSS_ROI

GOLD_MISPRICING        = os.path.join(PROJECT_ROOT, "data", "gold", "mispricing_scores")
GOLD_BRIEFS            = os.path.join(PROJECT_ROOT, "data", "gold", "intelligence_briefs")
CHROMA_PATH            = os.path.join(PROJECT_ROOT, "data", "chroma")
INFERENCE_HISTORY_PATH = os.path.join(PROJECT_ROOT, "data", "gold", "inference_history.parquet")
LEDGER_PATH            = os.path.join(PROJECT_ROOT, "data", "gold", "position_ledger.parquet")

SIMILARITY_FLOOR      = 0.50
SIMULATED_TRADES_PATH = os.path.join(PROJECT_ROOT, "data", "gold", "simulated_trades.csv")
PAPER_BANKROLL        = float(os.getenv("BANKROLL_USD", "500.0"))

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
OPENAI_MODEL  = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Per-cycle scan limits.
CANDIDATE_LIMIT      = int(os.getenv("CANDIDATE_LIMIT", "50"))

# Change-based re-eligibility: a ticker already analyzed stays suppressed until
# something MATERIAL changes — its price moved >= PRICE_DELTA, or news re-spiked to
# >= SPIKE_FACTOR x its last recorded spike (floored at SPIKE_MIN_FLOOR so a tiny prior
# spike can't make re-trigger trivial). MAX_SUPPRESS_HOURS is a safety ceiling so held
# positions still get a fresh daily brief (exit_evaluator's thesis-flip depends on it).
# MIN_RECHECK_MINUTES is an anti-thrash floor so a jittery price can't re-burn the LLM.
# This replaces the old pure-time cooldown that re-burned every ticker every 2h.
PRICE_DELTA          = float(os.getenv("INFERENCE_PRICE_DELTA",        "0.05"))
SPIKE_FACTOR         = float(os.getenv("INFERENCE_SPIKE_FACTOR",       "1.5"))
SPIKE_MIN_FLOOR      = float(os.getenv("INFERENCE_SPIKE_MIN_FLOOR",    "3.0"))
MAX_SUPPRESS_HOURS   = float(os.getenv("INFERENCE_MAX_SUPPRESS_HOURS", "24"))
MIN_RECHECK_MINUTES  = float(os.getenv("INFERENCE_MIN_RECHECK_MIN",    "15"))

# gpt-4o-mini list price (USD per 1M tokens). Override via env if it changes.
OPENAI_INPUT_PRICE_PER_MTOK  = float(os.getenv("OPENAI_INPUT_PRICE_PER_MTOK", "0.15"))
OPENAI_OUTPUT_PRICE_PER_MTOK = float(os.getenv("OPENAI_OUTPUT_PRICE_PER_MTOK", "0.60"))
# Hard daily ceiling on synthesis spend. Cycle stops calling the API beyond this.
BUDGET_GUARD_USD             = float(os.getenv("BUDGET_GUARD_USD", "1.0"))

_minilm_model = None


def _get_query_embedding(collection_name: str, text: str):
    """Returns query embedding matching the collection's stored dimensions."""
    if collection_name == "silver_news_enriched":
        resp = openai_client.embeddings.create(model="text-embedding-3-small", input=[text])
        return [resp.data[0].embedding]
    else:
        global _minilm_model
        if _minilm_model is None:
            _minilm_model = SentenceTransformer("all-MiniLM-L6-v2")
        return _minilm_model.encode([text]).tolist()


# ---------------------------------------------------------
# 2. CANDIDATE SELECTION
# ---------------------------------------------------------
SERIES_LIMIT = int(os.getenv("SERIES_LIMIT", "2"))  # max candidates per ticker series


def get_predictive_candidates(con, history_df, min_score=80.0):
    print(f"[Predictive] Scanning Gold Mispricing Ledger for scores >= {min_score} "
          f"(limit={CANDIDATE_LIMIT}, max {SERIES_LIMIT} per series)...")

    # Register the schema-normalised history (see _load_inference_history) as a temp view
    # so the eligibility SQL never references a column an older parquet might lack. The
    # 'eligible' CTE is the change-based gate: a previously-analyzed ticker only re-enters
    # the candidate pool if its price moved, news re-spiked, or the safety ceiling elapsed.
    try:
        con.register("inference_history_tmp", history_df)
    except Exception as e:
        print(f"[Predictive] WARN: could not register history ({e}) — treating all as eligible.")

    query = f"""
    WITH last_hist AS (
        SELECT
            ticker,
            MAX(TRY_CAST(analyzed_at AS TIMESTAMPTZ)) AS analyzed_at,
            ARG_MAX(TRY_CAST(last_price AS DOUBLE), TRY_CAST(analyzed_at AS TIMESTAMPTZ)) AS last_price,
            ARG_MAX(TRY_CAST(last_spike AS DOUBLE), TRY_CAST(analyzed_at AS TIMESTAMPTZ)) AS last_spike
        FROM inference_history_tmp
        GROUP BY ticker
    ),
    latest AS (
        SELECT ticker, title, yes_bid AS current_odds, delta_15m, mispricing_score,
               max_spike_multiplier, sentiment_signal, ingested_at,
               SPLIT_PART(ticker, '-', 1) AS series
        FROM delta_scan('{GOLD_MISPRICING}')
        WHERE mispricing_score >= 80
          AND TRY_CAST(ingested_at AS TIMESTAMPTZ) >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
          AND yes_bid BETWEEN 0.25 AND 0.75
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ingested_at DESC) = 1
    ),
    eligible AS (
        SELECT l.*
        FROM latest l
        LEFT JOIN last_hist h ON l.ticker = h.ticker
        WHERE
            h.ticker IS NULL                                       -- never analyzed
            OR h.analyzed_at IS NULL                               -- pre-migration row, no snapshot -> re-seed once
            OR (
                h.analyzed_at < CURRENT_TIMESTAMP - INTERVAL '{MIN_RECHECK_MINUTES} minutes'  -- anti-thrash floor
                AND (
                    h.last_price IS NULL                                                       -- no price snapshot yet
                    OR ABS(l.current_odds - h.last_price) >= {PRICE_DELTA}                     -- price moved
                    OR l.max_spike_multiplier >= GREATEST(COALESCE(h.last_spike, 0) * {SPIKE_FACTOR}, {SPIKE_MIN_FLOOR})  -- news re-spiked
                    OR h.analyzed_at < CURRENT_TIMESTAMP - INTERVAL '{MAX_SUPPRESS_HOURS} hours'  -- safety ceiling
                )
            )
    ),
    ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY series
                   ORDER BY mispricing_score DESC, ABS(COALESCE(delta_15m, 0)) DESC
               ) AS series_rank
        FROM eligible
    )
    SELECT ticker, title, current_odds, delta_15m, mispricing_score,
           max_spike_multiplier, sentiment_signal, ingested_at
    FROM ranked
    WHERE series_rank <= {SERIES_LIMIT}
    ORDER BY mispricing_score DESC, ABS(COALESCE(delta_15m, 0)) DESC
    LIMIT {CANDIDATE_LIMIT};
    """
    try:
        df = con.execute(query).df()
        if not df.empty:
            df['previous_odds'] = df['current_odds'] / (1 + df['delta_15m'].fillna(0))
        return df
    except Exception as e:
        print(f"[Predictive] ERROR checking Gold ledger: {e}")
        return pd.DataFrame()
    finally:
        try:
            con.unregister("inference_history_tmp")
        except Exception:
            pass


# ---------------------------------------------------------
# 3. COOLDOWN + BUDGET HELPERS
# ---------------------------------------------------------
HISTORY_COLS = ["ticker", "analyzed_at", "score", "last_price", "last_spike",
                "input_tokens", "output_tokens", "cost_usd"]


def _load_inference_history():
    if not os.path.exists(INFERENCE_HISTORY_PATH):
        return pd.DataFrame(columns=HISTORY_COLS)
    try:
        df = pd.read_parquet(INFERENCE_HISTORY_PATH)
    except Exception:
        return pd.DataFrame(columns=HISTORY_COLS)
    # Schema-safe: backfill any column an older parquet predates, then return a stable
    # column order. This is what lets `con.register` + the eligibility SQL reference
    # last_price / last_spike even on history files written before those existed.
    for col in HISTORY_COLS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[HISTORY_COLS]


def _load_open_tickers():
    """Tickers we currently hold — used to keep writing briefs (for exit-logic /
    thesis-flip) while suppressing a SECOND entry (monitor-only). Unions the position
    ledger with un-closed rows in simulated_trades.csv so a freshly-entered ticker is
    covered even before build_position_ledger.py has rebuilt the ledger."""
    held = set()
    if os.path.exists(LEDGER_PATH):
        try:
            led = pd.read_parquet(LEDGER_PATH)
            if not led.empty and "ticker" in led.columns:
                held.update(led["ticker"].astype(str).tolist())
        except Exception:
            pass
    if os.path.exists(SIMULATED_TRADES_PATH):
        try:
            tr = pd.read_csv(SIMULATED_TRADES_PATH)
            if not tr.empty and "ticker" in tr.columns:
                if "closed_at" in tr.columns:
                    tr = tr[tr["closed_at"].isna()]
                held.update(tr["ticker"].astype(str).tolist())
        except Exception:
            pass
    return held


def _today_spend(history_df):
    if history_df.empty:
        return 0.0
    today = datetime.now(timezone.utc).date().isoformat()
    try:
        today_rows = history_df[history_df["analyzed_at"].astype(str).str.startswith(today)]
        return float(today_rows["cost_usd"].fillna(0).sum())
    except Exception:
        return 0.0


def _append_inference(history_df, ticker, score, input_tokens, output_tokens,
                      last_price=None, last_spike=None):
    """Append a row to in-memory history + persist. Returns (new_df, call_cost_usd).
    last_price / last_spike snapshot the market state at analysis time so the next
    cycle's change-based eligibility can tell whether anything material has moved."""
    cost = (
        (input_tokens  / 1_000_000) * OPENAI_INPUT_PRICE_PER_MTOK +
        (output_tokens / 1_000_000) * OPENAI_OUTPUT_PRICE_PER_MTOK
    )
    row = {
        "ticker":        ticker,
        "analyzed_at":   datetime.now(timezone.utc).isoformat(),
        "score":         float(score),
        "last_price":    float(last_price) if last_price is not None else None,
        "last_spike":    float(last_spike) if last_spike is not None else None,
        "input_tokens":  int(input_tokens),
        "output_tokens": int(output_tokens),
        "cost_usd":      round(cost, 6),
    }
    new_df = pd.concat([history_df, pd.DataFrame([row])], ignore_index=True)
    try:
        os.makedirs(os.path.dirname(INFERENCE_HISTORY_PATH), exist_ok=True)
        new_df.to_parquet(INFERENCE_HISTORY_PATH, index=False)
    except Exception as e:
        print(f"    [WARN] could not persist inference_history.parquet: {e}")
    return new_df, cost


# ---------------------------------------------------------
# 4. LLM OUTPUT CLEANUP
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
# 5. RAG SEARCH
# ---------------------------------------------------------
def fetch_rag_context(collections, query_text, current_time, window_mins=4320):
    start_ts    = int((current_time - timedelta(minutes=window_mins)).timestamp())
    end_ts      = int(current_time.timestamp())
    scored_docs = []

    if not isinstance(collections, list):
        collections = [collections]

    for coll in collections:
        query_embedding = _get_query_embedding(coll.name, query_text)
        # No where= filter: ChromaDB metadata scans are O(n) — ~50s per query on large
        # collections. ANN (HNSW) without a filter is near-instant. Time-window filtering
        # and time-decay scoring are applied below in Python on the small result set.
        results = coll.query(
            query_embeddings=query_embedding,
            n_results=15,
            include=["documents", "metadatas", "distances"]
        )
        if results['documents'] and results['documents'][0]:
            for i in range(len(results['documents'][0])):
                meta = results['metadatas'][0][i]
                # Use published_timestamp (article publish time) when available — set by
                # embed_silver_data.py for news articles. Falls back to ingested_timestamp
                # for GDELT records and older ChromaDB entries that predate this field.
                pub_ts = int(meta.get("published_timestamp", meta.get("ingested_timestamp", 0)))
                if pub_ts < start_ts or pub_ts > end_ts:
                    continue
                distance       = results['distances'][0][i]
                semantic_score = 1.0 / (1.0 + distance)
                if semantic_score < SIMILARITY_FLOOR:
                    continue
                # Time-decay: blend semantic quality (60%) with recency (40%).
                # Half-life = 24h — an article published 24h ago contributes 80% of a fresh one.
                # Quality gate (MIN_RAG_SCORE) checks semantic_score so stale-but-topical
                # articles still clear the gate; decay only affects ranking order.
                hours_old = max(0.0, (end_ts - pub_ts) / 3600.0)
                recency   = math.exp(-0.693 * hours_old / 24.0)
                score     = semantic_score * (0.6 + 0.4 * recency)
                scored_docs.append({
                    "source":         meta.get("source", coll.name),
                    "content":        results['documents'][0][i],
                    "score":          score,
                    "semantic_score": semantic_score,
                })

    return sorted(scored_docs, key=lambda x: x['score'], reverse=True)


# ---------------------------------------------------------
# 6. LLM SYNTHESIS
# ---------------------------------------------------------
def _call_openai(prompt: str):
    """Single OpenAI chat completion. Returns (dict, in_tok, out_tok). On 429: retry once after 10s."""
    for attempt in (1, 2):
        try:
            response = openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.3,
            )
            text = response.choices[0].message.content
            try:
                res_data = json.loads(text)
            except Exception:
                res_data = {
                    "verdict": "Parse error", "bull_case": text, "bear_case": "",
                    "recommended_side": "no", "confidence_pct": 0,
                }
            usage = getattr(response, "usage", None)
            in_tok  = int(getattr(usage, "prompt_tokens", 0)     or 0)
            out_tok = int(getattr(usage, "completion_tokens", 0) or 0)
            return res_data, in_tok, out_tok
        except Exception as e:
            if attempt == 1 and "429" in str(e):
                print(f"    [Rate Limit] OpenAI 429 — backing off 10s and retrying...")
                time.sleep(10)
                continue
            print(f"    [LLM ERROR] {e}")
            return None, 0, 0


def generate_predictive_brief(market, context_docs):
    """Returns (brief_dict_or_None, input_tokens, output_tokens)."""
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
- Be highly critical. If news is vague or absent for EITHER side, recommend 'No Trade'.
- Treat YES and NO symmetrically: lack of evidence is NOT a reason to prefer NO. Only recommend a side if concrete evidence shifts the probability materially from current odds.
- Respond ONLY with a JSON object: bull_case, bear_case, verdict, recommended_side, confidence_pct, decision_reason.
- recommended_side must be exactly "yes" or "no" (lowercase).
- confidence_pct must be an integer 0-100.
- verdict MUST start with one of: "Buy YES", "Buy NO", or "No Trade". Never write "Neutral", "N/A", or any other value.
- decision_reason: 1-2 sentences explaining exactly why the recommended side was chosen over the other. Reference the single most important piece of evidence that tipped the scale. Be specific, not generic."""

    res_data, in_tok, out_tok = _call_openai(prompt)
    if res_data is None:
        return None, 0, 0

    if isinstance(res_data, list) and len(res_data) > 0:
        res_data = res_data[0]
    if isinstance(res_data, dict):
        for k in list(res_data.keys()):
            if k not in ("recommended_side", "confidence_pct"):
                res_data[k] = _flatten_llm_field(res_data[k])
        side = str(res_data.get("recommended_side", "")).strip().lower()
        if side not in ("yes", "no"):
            verdict_lower = str(res_data.get("verdict", "")).lower()
            side = "yes" if "buy yes" in verdict_lower else "no" if "buy no" in verdict_lower else "yes"
        res_data["recommended_side"] = side

    return res_data, in_tok, out_tok


# ---------------------------------------------------------
# 7. MAIN
# ---------------------------------------------------------
def main():
    print("=" * 60)
    print(" PredictIQ Predictive Scanner: Delayed Reaction Detection")
    print(f" Model: {OPENAI_MODEL}  |  Candidate limit: {CANDIDATE_LIMIT}")
    print(f" Re-eligible on: price move >= {PRICE_DELTA} or spike >= {SPIKE_FACTOR}x "
          f"(ceiling {MAX_SUPPRESS_HOURS}h, floor {MIN_RECHECK_MINUTES}m)  |  Budget: ${BUDGET_GUARD_USD}/day")
    print("=" * 60)

    if not os.getenv("OPENAI_API_KEY"):
        print("[Predictive] OPENAI_API_KEY missing — cannot synthesize briefs. Aborting.")
        return

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    # Load history + held positions BEFORE candidate selection: history feeds the
    # change-based eligibility gate, open_tickers feeds the monitor-only entry guard.
    history_df   = _load_inference_history()
    today_spent  = _today_spend(history_df)
    open_tickers = _load_open_tickers()
    print(f"[Predictive] Today's spend so far: ${today_spent:.4f} (cap: ${BUDGET_GUARD_USD})  |  "
          f"holding {len(open_tickers)} position(s)")

    candidates = get_predictive_candidates(con, history_df)
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
    written       = 0
    skipped_held  = 0
    skipped_bg    = 0

    for _, market in candidates.iterrows():
        ticker = market['ticker']
        score  = float(market['mispricing_score'])

        if today_spent >= BUDGET_GUARD_USD:
            skipped_bg += 1
            continue

        market_time = pd.to_datetime(market['ingested_at'])
        if market_time.tzinfo is None:
            market_time = market_time.replace(tzinfo=timezone.utc)

        print(f"\n[Predictive] ANALYZING: {ticker} (Score: {score:.1f})")

        # Build a query that leads with the title so ChromaDB can match proper nouns,
        # then appends the ticker (contains entity codes) as a secondary signal.
        rag_query = f"{market['title']} {ticker}"
        context_docs = fetch_rag_context(active_collections, rag_query, market_time)
        if not context_docs:
            print(f"    > No RAG matches found. Adding to cooldown to prevent repeated burns.")
            history_df, _ = _append_inference(history_df, ticker, score, 0, 0,
                                              market['current_odds'], market['max_spike_multiplier'])
            continue

        # RAG quality gate: checks raw semantic similarity, not the time-decayed score.
        # A topical article from 48h ago should still clear the gate — decay only affects
        # which docs the LLM sees first (ranking), not whether we call the LLM at all.
        MIN_RAG_SCORE = 0.63
        best_rag = max(d['semantic_score'] for d in context_docs)
        if best_rag < MIN_RAG_SCORE:
            print(f"    > RAG quality too low ({best_rag:.2f} < {MIN_RAG_SCORE}) — no signal, adding to cooldown.")
            history_df, _ = _append_inference(history_df, ticker, score, 0, 0,
                                              market['current_odds'], market['max_spike_multiplier'])
            continue

        print(f"    > Synthesizing {len(context_docs)} signals (best RAG: {best_rag:.2f})...")
        brief, in_tok, out_tok = generate_predictive_brief(market, context_docs)

        if brief is None:
            continue  # transient error / rate limit — next cycle retries

        # Record the call regardless of edge gate — we paid for it.
        history_df, call_cost = _append_inference(history_df, ticker, score, in_tok, out_tok,
                                                  market['current_odds'], market['max_spike_multiplier'])
        today_spent += call_cost

        rag_score      = max([d['score'] for d in context_docs])
        llm_pct        = float(brief.get("confidence_pct", 50)) / 100.0
        confidence     = round(llm_pct, 4)
        current_odds   = float(market['current_odds'])
        llm_confidence = float(brief.get("confidence_pct", 50)) / 100.0
        recommended    = brief.get("recommended_side", "yes")

        # Calibration guard: LLM must beat the market by at least MIN_EDGE percentage points,
        # not just any positive margin. A 51% LLM vs a 50% market is noise; +10pp is the
        # threshold where edge survives LLM miscalibration.
        MIN_EDGE       = 0.10
        MIN_CONFIDENCE = 0.65   # 50% LLM = shrug; only act on real conviction
        market_prob = current_odds if recommended == "yes" else (1.0 - current_odds)
        entry_price = market_prob
        if confidence < MIN_CONFIDENCE:
            print(f"    > Confidence too low: {confidence:.0%} < {MIN_CONFIDENCE:.0%} floor — skipping")
            continue
        if confidence < market_prob + MIN_EDGE:
            print(f"    > Edge too thin: LLM {confidence:.0%} vs market {market_prob:.0%} (need +{MIN_EDGE:.0%}) — skipping")
            continue

        # Kelly viability gate: skip if the payout ratio is too poor for positive Kelly.
        # This filters "trapped" bets where TP at {TAKE_PROFIT_ROI:.0%} ROI is physically
        # unreachable (entry_price * (1+TP) > $1.00) or reward/risk is unfavorable.
        kelly_check = calculate_kelly(confidence, entry_price, PAPER_BANKROLL)
        if not kelly_check["edge_detected"]:
            print(f"    > Kelly negative: entry_price {entry_price:.2f} on {recommended} side — payout ratio too poor, skipping")
            continue

        # odds_delta = implied move: LLM-implied YES probability minus market YES probability.
        implied_yes = llm_confidence if recommended == "yes" else (1.0 - llm_confidence)
        odds_delta  = round(implied_yes - current_odds, 4)
        row = {
            "ticker":           ticker,
            "title":            market['title'],
            "bull_case":        _flatten_llm_field(brief.get("bull_case", "")),
            "bear_case":        _flatten_llm_field(brief.get("bear_case", "")),
            "verdict":          _flatten_llm_field(brief.get("verdict", "")),
            "decision_reason":  _flatten_llm_field(brief.get("decision_reason", "")),
            "recommended_side": recommended,
            "current_odds":     current_odds,
            "odds_delta":       odds_delta,
            "mispricing_score": float(market['mispricing_score']),
            "confidence_score": float(confidence),
            "rag_score":        round(rag_score, 4),
            "replay_model":     OPENAI_MODEL,
            "event_at":         current_time.isoformat(),
            "ingested_at":      current_time.isoformat(),
        }

        print(f"--- VERDICT [{ticker}]: {row['verdict']}")

        ts_str   = current_time.strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(GOLD_BRIEFS, f"brief_{ts_str}_{ticker}.parquet")
        pd.DataFrame([row]).to_parquet(out_path, index=False)

        # Auto paper-trade: only on actionable verdicts (skip "No Trade").
        verdict_str = str(row.get("verdict", "")).strip()
        if verdict_str.lower().startswith("no trade"):
            written += 1
            continue  # brief is already written to parquet; no position, no Telegram

        # Monitor-only (Option A): if we already hold this ticker, keep the fresh brief so
        # exit_evaluator's thesis-flip can act on it, but never open a SECOND entry.
        if ticker in open_tickers:
            print(f"    > Already holding {ticker} — brief kept for exit logic; skipping new entry.")
            skipped_held += 1
            written += 1
            continue

        kelly_sized = calculate_kelly(confidence, entry_price, PAPER_BANKROLL)
        qty = int(kelly_sized["suggested_bet_usd"] / entry_price) if entry_price > 0 else 0
        if qty > 0:
            file_exists = os.path.isfile(SIMULATED_TRADES_PATH)
            with open(SIMULATED_TRADES_PATH, "a", encoding="utf-8") as f:
                if not file_exists:
                    f.write("timestamp,ticker,side,count,price_dollars,total_risk_usd\n")
                risk = round(qty * entry_price, 2)
                f.write(f"{current_time.isoformat()},{ticker},{recommended},{qty},{entry_price:.4f},{risk:.2f}\n")
            print(f"    [PAPER TRADE] {qty}x {ticker} {recommended.upper()} @ {entry_price:.4f}  risk=${qty*entry_price:.2f}")

        display_odds = current_odds if recommended == "yes" else (1.0 - current_odds)
        notify_brief(
            ticker=ticker,
            title=market['title'],
            side=recommended.upper(),
            odds=display_odds,
            confidence=confidence,
            edge=confidence - market_prob,
        )
        written += 1

    print(
        f"\n[Predictive] Done. {written} brief(s) written | "
        f"{skipped_held} held (monitor-only) | {skipped_bg} skipped (budget) | "
        f"spend today: ${today_spent:.4f}"
    )


if __name__ == "__main__":
    main()
