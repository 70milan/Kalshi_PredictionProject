# Mispricing Score System: End-to-End Guide

## Overview

The mispricing score is a quantitative measure (0-100) that identifies Kalshi markets trading at prices that don't yet reflect recent high-volume news or events. It's the **gatekeeper** for the LLM inference pipeline: only markets with scores ≥ 80 are considered "candidate markets" for detailed fundamental analysis.

The system operates on the principle of **delayed reaction** — markets initially underprice news due to retail behavioral biases, and the LLM can exploit that inefficiency before the market corrects.

---

## Data Pipeline Overview

### Bronze Layer (Ingestion)
Raw data from three streams:

| Stream | Source | Update Frequency | Key Fields |
|--------|--------|------------------|-----------|
| **Kalshi Markets** | Kalshi API | ~2 min | `ticker`, `title`, `yes_bid`, `volume`, `status`, `close_time` |
| **GDELT Events** | GDELT public API | ~15 min | `entity_type`, `entity_name`, `tone` (−10 to +10), `volume_spike` |
| **News Articles** | BBC, CNN, Fox, NYT, etc. | ~2–5 min per feed | `feed_key`, `title`, `content`, `published_at` |

### Silver Layer (Cleaning)
Each Bronze stream is deduplicated, validated, and enriched:

- **silver_kalshi_markets_history**: complete snapshots with timestamps
- **silver_gdelt_enriched**: deduplicated events with normalized tone
- **silver_news_enriched**: parsed headlines with VADER sentiment scores (−1 to +1)

### Gold Layer (Aggregation + Scoring)
The **gold_market_summaries_transform.py** script synthesizes all signals into a single **mispricing_score** per market.

---

## Mispricing Score Calculation (V2 Formula)

The formula evolved after a forward-test backtest proved the original formula was **backwards**: it penalized volatility when volatility actually predicted future movement.

### Step 1: Load Latest Market State (48h Lookback)

```python
df_k = spark.read.format("delta").load(kalshi_history_path)
df_k = df_k.filter(
    F.col("ingested_at").cast("timestamp") >= 48_hours_ago
)
```

- **Why 48h?** Enough lookback for velocity calculations (96 snapshots at ~15m intervals) without full-history scan.
- **Latest snapshot per ticker** is selected via window function.

### Step 2: Calculate Velocity Metrics

Three time-based deltas track price momentum:

```python
delta_15m  = (yes_bid[t] - yes_bid[t-1])  / yes_bid[t-1]
delta_1h   = (yes_bid[t] - yes_bid[t-4])  / yes_bid[t-4]  # lag by 4 snapshots (~15m each)
delta_24h  = (yes_bid[t] - yes_bid[t-96]) / yes_bid[t-96] # lag by 96 snapshots (~24h)
```

- These measure price direction and magnitude.
- Percentage deltas normalize across markets with different absolute price levels.

### Step 3: Route News & Events to Markets via Mapping

Three parallel matching strategies bridge high-volume events to Kalshi tickers:

#### A. GDELT Theme Joins

Maps GDELT entity types/names to Kalshi series via [kalshi_theme_map.csv](data/reference/kalshi_theme_map.csv):

```csv
series_ticker,gdelt_entity_type,gdelt_entity_name,required_regex
KXFED,organization,Federal Reserve,(Federal Reserve|FOMC|Jerome Powell)
KXPOLITICS,theme,ELECTION,(U\.S\. Election|American Election|Senate)
KXTW,location,Taiwan,(Taiwan|Taipei|TSMC|Cross-Strait)
```

**Join logic:**
- Match `series_ticker` (first part of market ticker, e.g., `KXFED` from `KXFED-250131`)
- Match GDELT entity (by type and loose name containment)
- Extract GDELT's `vol_spike_multiplier` (how many σ above baseline) and `tone_15m` (sentiment, −10 to +10)

#### B. GDELT Person Matches

For person entities (e.g., "Jerome Powell"), search all Kalshi titles for the name:

```python
df_kalshi_titles.crossJoin(df_g_persons) \
    .filter(F.expr("locate(entity_name, title) > 0"))
```

**Reasoning:** Political/finance markets often mention key figures; a spike in mentions of "Powell" suggests Fed-related market movement.

#### C. News Headline Keyword Routing

The most nuanced strategy: extract significant words from news headlines, match them against market titles, route the news's spike signal to matched markets.

**Example:** BBC publishes "FISA Reauthorization Passes Senate" → extract keywords `["fisa", "reauthorization", "passes", "senate"]` → find Kalshi market "Will FISA be reauthorized by end of 2024?" → match on "fisa" → route BBC's spike signal to that market.

**Why significant words?** CommonWords like "will", "the", "said" would match every article to every market. Only words with length > 3 and not in the STOPWORDS set are extracted.

**Deduplication:** News articles are deduplicated by (feed_key, word) before the join, so 50 BBC articles about FISA don't artificially amplify the spike 50×.

---

### Step 4: Select Strongest Signal per Market

Multiple events may match a single market. Pick the one with the **highest volume spike**:

```python
df_max_signal = df_all_matches \
    .withColumn("_rn", F.row_number() OVER (PARTITION BY ticker ORDER BY vol_spike_multiplier DESC)) \
    .filter(F.col("_rn") == 1) \
    .select("ticker", "max_spike_multiplier", "avg_sentiment")
```

- `max_spike_multiplier`: how many standard deviations above the ~15-minute moving average
- `avg_sentiment`: sentiment from that strongest signal (normalized −1 to +1)

### Step 5: Calculate Raw V2 Score

The core formula rewards **recent volatility** (not penalizes it):

```python
raw_v2 = max_spike_multiplier × |avg_sentiment| × (1 + |delta_24h|)
```

**Interpretation:**
- **max_spike_multiplier**: How abnormal is the event volume? (e.g., 8.5× baseline)
- **|avg_sentiment|**: How intense is sentiment? (0 = neutral, 1 = extreme)
- **(1 + |delta_24h|)**: Amplify if the market has **already moved** in the last 24h, because momentum predicts continued movement.

**Why V2 beats V1:**
- **V1 penalized:** `score = 20 * spike * |sentiment| × (1 - |delta_24h|)`
  - This discouraged betting on volatile markets.
  - Forward test: high-V1-score markets moved **0.63× baseline** at 24h (directional edge ~57%).
- **V2 rewards:** Volatility is autocorrelated; recent movement predicts future movement.
  - Forward test: high-V2-score markets moved **1.58× baseline** at 24h (directional edge ~68%).

### Step 6: Log-Normalize to [0, 100]

Raw V2 is **fat-tailed**: percentile stats:
- p50 (median) ≈ 0.5
- p95 ≈ 9.0
- max ≈ 22,000 (rare extremes during flash crashes or huge news events)

Naive ×20 + cap-100 saturates ~22% of all markets at exactly 100, making "score ≥ 80" too permissive.

**Solution:** Log-anchor the validated top-5% boundary (raw_v2 ≈ 9.0) to exactly 80:

```python
V2_TOP5_ANCHOR = float(os.getenv("MISPRICING_V2_ANCHOR", "9.0"))
ln_anchor = math.log1p(V2_TOP5_ANCHOR)  # ~2.197

mispricing_score = min(
    100.0,
    max(
        0.0,
        80.0 * log1p(raw_v2) / ln_anchor
    )
)
```

**Mapping:**
- `raw_v2 = 0.0` → `score = 0`
- `raw_v2 = 9.0` (p95) → `score = 80` ← the gatekeeper threshold
- `raw_v2 >> 9.0` → `score → 100` (clamped)

This ensures **downstream "score ≥ 80" gate admits the validated top-5% slice without code changes**.

---

## Candidate Selection: From Gold to LLM

After mispricing scores are computed, the **predict_movements.py** inference script selects candidates:

### Eligibility Gates

```python
def get_predictive_candidates(con, history_df):
    """
    WITH latest AS (
        SELECT g.ticker, g.mispricing_score, g.yes_bid, g.max_spike_multiplier, g.sentiment_signal
        FROM delta_scan('data/gold/mispricing_scores') g
        WHERE g.flagged_candidate = TRUE  -- score > 80
          AND g.yes_bid BETWEEN 0.25 AND 0.75  -- mid-range odds (avoid extremes)
          AND DATE_DIFF('day', NOW(), close_time) BETWEEN 60 AND 365  -- expiry window
    ),
    eligible AS (
        SELECT l.*
        FROM latest l
        LEFT JOIN inference_history h ON l.ticker = h.ticker
        WHERE
            h.ticker IS NULL  -- never analyzed
            OR h.analyzed_at < NOW() - INTERVAL '15 minutes'  -- anti-thrash floor
            AND (
                ABS(l.current_odds - h.last_price) >= 0.05  -- price moved ≥ 5%
                OR l.max_spike_multiplier >= MAX(h.last_spike * 1.5, 3.0)  -- spike re-triggered
                OR h.analyzed_at < NOW() - INTERVAL '24 hours'  -- safety ceiling
            )
    )
    """
```

**Key gates:**

| Gate | Purpose |
|------|---------|
| **score ≥ 80** | Only top-5% by signal strength |
| **0.25 ≤ odds ≤ 0.75** | Avoid extremes (edge is better on 50/50 splits) |
| **60–365 days to expiry** | Enough time for delayed reaction to play out; avoid imminent binaries |
| **Change-based re-eligibility** | Suppress re-analysis until price moves or news re-spikes (no thrashing on stale signals) |
| **Series cap (max 3 per series)** | Avoid over-concentration in one theme (e.g., KXFED) |
| **Candidate limit (max 50)** | Bound LLM cost per cycle |

---

## From Candidates to Briefs

For each eligible candidate, the system:

### 1. Fetch RAG Context
Query ChromaDB for relevant news/GDELT articles matching the market title and rules:

```python
context_docs = fetch_rag_context(
    collections=[silver_news_enriched, silver_gdelt_enriched],
    query_text=f"{market['title']} {market['rules']}",
    current_time=market['ingested_at'],
    window_mins=4320  # 3-day lookback for long-tail temporal support
)

# Filter by semantic similarity ≥ 0.50
# Rank by time-decay blend: 60% semantic, 40% recency (24h half-life)
```

### 2. Quality Gates
- **RAG floor (≥ 0.63 semantic similarity):** If even the best match is weak, the market is either: (a) a ghost pump with no corroborating news, or (b) the news is too vague to synthesize. Skip and add to cooldown.

### 3. LLM Synthesis
Send market + context to OpenAI (gpt-4o-mini):

```python
brief, in_tok, out_tok = generate_predictive_brief(market, context_docs)

# Returns:
# {
#     "bull_case": "...",
#     "bear_case": "...",
#     "verdict": "Buy YES|Buy NO|No Trade",
#     "recommended_side": "yes|no",
#     "confidence_pct": 65,
#     "decision_reason": "..."
# }
```

### 4. Edge Gate
LLM confidence must **beat the market by at least +10 percentage points**:

```python
MIN_CONFIDENCE = 0.65
MIN_EDGE = 0.10

market_prob = odds if side == "yes" else (1 - odds)

if confidence < MIN_CONFIDENCE:
    skip()  # "I have no strong opinion"
if confidence < market_prob + MIN_EDGE:
    skip()  # "The market is only slightly mispricedby LLM estimate"
```

Example:
- Market says 45% for YES
- LLM says 55% for YES
- Edge = +10pp → **Pass**, actionable trade
- LLM says 52% → Edge = +7pp → **Fail**, too thin to justify execution friction

### 5. Kelly Viability
Check that the payout ratio allows positive Kelly sizing:

```python
def calculate_kelly(confidence, entry_price, bankroll):
    """
    Kelly criterion: f* = (bp - q) / b
    where b = payout ratio, p = win prob, q = lose prob
    
    A 51% LLM vs a 50% market on YES is barely above Kelly min,
    because you win 1¢ per $1 risked. If spread is 2¢, you lose money.
    """
    if entry_price * (1 + TAKE_PROFIT_ROI) > 1.0:
        return {"edge_detected": False}  # TP unreachable
```

---

## Output: Intelligence Briefs

Each passing candidate generates a brief (parquet file):

```json
{
  "ticker": "KXFED-250131",
  "title": "Will the Federal Reserve cut rates by >50bp in 2025?",
  "bull_case": "Recent inflation data shows sustained pressure...",
  "bear_case": "Fed has signaled a gradual approach...",
  "verdict": "Buy YES",
  "recommended_side": "yes",
  "current_odds": 0.38,
  "odds_delta": 0.12,  // implied move: LLM 50% vs market 38%
  "mispricing_score": 87.3,
  "confidence_score": 0.72,
  "rag_score": 0.81,
  "decision_reason": "Recent inflation reports show stronger-than-expected CPI...",
  "event_at": "2025-01-15T14:32:00Z",
  "ingested_at": "2025-01-15T14:35:00Z"
}
```

---

## Key Metrics & Thresholds (Configurable)

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `MISPRICING_V2_ANCHOR` | 9.0 | raw_v2 p95 (maps to score=80) |
| `MIN_DAYS_TO_CLOSE` | 60 | Minimum days until market expiry |
| `MAX_DAYS_TO_CLOSE` | 365 | Maximum days until market expiry |
| `SERIES_LIMIT` | 3 | Max candidates per ticker series |
| `CANDIDATE_LIMIT` | 50 | Max total LLM calls per cycle |
| `MIN_EDGE` | 0.10 | Minimum edge to trade (10pp) |
| `MIN_CONFIDENCE` | 0.65 | Minimum LLM confidence to trade (65%) |
| `PRICE_DELTA` | 0.05 | Price move threshold for re-eligibility (5%) |
| `SPIKE_FACTOR` | 1.5 | Spike multiplier for re-eligibility (1.5×) |
| `MAX_SUPPRESS_HOURS` | 24 | Safety ceiling for suppression (all tickers reset after 24h) |
| `SIMILARITY_FLOOR` | 0.50 | Min semantic similarity to include RAG doc |
| `BUDGET_GUARD_USD` | 1.0 | Max LLM spend per cycle (stop after) |

---

## Architecture Principles

### Flat, Free, Native

- **Flat:** No cloud orchestration. Delta Lake on local filesystem, ChromaDB embedded.
- **Free:** Uses free-tier APIs (GDELT, Groq) and OSS tools (sentence-transformers).
- **Native:** All data stays local; no external databases.

### Why Delayed Reaction?

Prediction markets are ~2 weeks behind asset markets (stock prices, options) in pricing news. Humans are slow. The LLM exploits that inefficiency by:
1. Detecting high-spike events + sentiment via gold mispricing scores.
2. Retrieving corroborating news via RAG to reduce false positives.
3. Synthesizing a probability estimate via LLM.
4. Comparing LLM prob to market odds; if edge > 10pp, trade.

### Cost Efficiency

- **LLM calls are expensive:** Only call on high-confidence candidates (score ≥ 80, change-based eligibility).
- **RAG is cheap:** ChromaDB in-memory search is <100ms. Thousands of documents, instant retrieval.
- **Budget guard:** Daily LLM spend cap prevents runaway costs during volatile events.

---

## Common Debugging

### Q: Why is a market not flagged (score < 80)?
- Mispricing score = `spike × |sentiment| × (1 + |delta_24h|)` log-normalized.
- If no news matched: spike = 0, score = 0.
- If news matched but market hasn't moved: delta_24h ≈ 0, score capped lower.
- Solution: Check [kalshi_theme_map.csv](data/reference/kalshi_theme_map.csv) regex; verify GDELT events are flowing (data/gold/gdelt_summaries).

### Q: Why was a high-score market skipped (score ≥ 80 but no brief)?
- RAG quality too low (< 0.63 semantic similarity).
- Edge gate failed (LLM confidence not > market_prob + 0.10).
- Budget exceeded (daily LLM spend ≥ $BUDGET_GUARD_USD).
- Solution: Check logs in orchestration/run_etl.py for phase outputs.

### Q: Why are so many markets being re-analyzed (high LLM cost)?
- Change-based re-eligibility too permissive.
- Price moves or spike re-triggers frequently.
- Solution: Increase `PRICE_DELTA`, `SPIKE_FACTOR`, or `MAX_SUPPRESS_HOURS` env vars.

---

## Data Lineage

```
Bronze
├── kalshi_markets/open/ (ingest/kalshi_markets_active.py)
├── gdelt/gdelt_events/ (ingest/gdelt_events_ingest.py)
└── {news feeds}/ (ingest/bbc_ingest.py, cnn_ingest.py, etc.)
         ↓
Silver (orchestration/spark_pipeline.py)
├── kalshi_markets_history (silver_kalshi_transform)
├── gdelt_enriched (silver_gdelt_events_transform)
└── news_articles_enriched (silver_news_transform)
         ↓
Gold (transformation/gold_market_summaries_transform.py)
└── mispricing_scores  ← **YOU ARE HERE**
         ↓
ChromaDB Embeddings (rag/embed_silver_data.py)
├── silver_news_enriched
└── silver_gdelt_enriched
         ↓
Inference (inference/predict_movements.py)
└── intelligence_briefs
     ↓
Telegram + API Dashboard
```

---

## Related Files

- **Calculation:** [gold_market_summaries_transform.py](transformation/gold_market_summaries_transform.py)
- **Candidate selection:** [predict_movements.py](inference/predict_movements.py) lines 100–172
- **Mapping:** [kalshi_theme_map.csv](data/reference/kalshi_theme_map.csv)
- **Reference:** [CLAUDE.md](CLAUDE.md) — Infrastructure + env vars
