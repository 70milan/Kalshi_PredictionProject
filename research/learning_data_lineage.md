# PredictIQ Data Lineage — End-to-End Learning Guide

This guide traces **one prediction market** through the complete pipeline using DuckDB queries.

---

## Setup: Load the Schema

```sql
INSTALL delta;
LOAD delta;

-- Create schemas for views
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Silver layer views (what we'll mostly use)
CREATE OR REPLACE VIEW silver.kalshi_markets_history AS 
  SELECT * FROM delta_scan('C:/Data Engineering/codeprep/predection_project/data/silver/kalshi_markets_history');

CREATE OR REPLACE VIEW silver.news_articles_enriched AS 
  SELECT * FROM delta_scan('C:/Data Engineering/codeprep/predection_project/data/silver/news_articles_enriched');

CREATE OR REPLACE VIEW silver.gdelt_events_current AS 
  SELECT * FROM delta_scan('C:/Data Engineering/codeprep/predection_project/data/silver/gdelt_events_current');

CREATE OR REPLACE VIEW silver.gdelt_gkg_current AS 
  SELECT * FROM delta_scan('C:/Data Engineering/codeprep/predection_project/data/silver/gdelt_gkg_current');

CREATE OR REPLACE VIEW gold.intelligence_briefs AS 
  SELECT * FROM read_parquet('C:/Data Engineering/codeprep/predection_project/data/gold/intelligence_briefs/*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW gold.mispricing_scores AS 
  SELECT * FROM delta_scan('C:/Data Engineering/codeprep/predection_project/data/gold/mispricing_scores');
```

---

## STEP 1: Find an Active Kalshi Market

**Why**: Pick a single ticker to trace. Let's see what markets are currently active.

```sql
-- Find recent markets sorted by volume
SELECT 
  ticker,
  title,
  series_ticker,
  yes_bid,
  yes_ask,
  volume,
  status,
  MAX(ingested_at) as last_snapshot
FROM silver.kalshi_markets_history
WHERE status IN ('open', 'active')
GROUP BY ticker, title, series_ticker, yes_bid, yes_ask, volume, status
ORDER BY volume DESC
LIMIT 10;
```

**What to look for**: Pick a ticker with decent volume (>100 means liquid). Example: `KXTW-20261231-TWJY` (Taiwan elections).

---

## STEP 2: Map Kalshi Series to News Themes

**Why**: Not all news mentions market tickers directly. We need to understand which news TOPICS matter for which markets.

**Question**: What's in `kalshi_theme_map.csv`?

```sql
-- Read the theme mapping (if it exists)
SELECT * FROM read_csv_auto('C:/Data Engineering/codeprep/predection_project/data/reference/kalshi_theme_map.csv')
LIMIT 10;
```

**Output structure**:
- `series_ticker` — e.g., `KXTW`, `KXFED`, `KXPOLITICS`
- `gdelt_entity_type` — one of: `location`, `organization`, `person`, `theme`
- `gdelt_entity_name` — the entity to match (e.g., "Taiwan", "Federal Reserve")
- `required_regex` — pattern to match in article/event text

**Learning point**: This is the **bridge** between markets and signals. If a market is about Taiwan (`KXTW`), we look for news/GDELT mentioning Taiwan.

---

## STEP 3: Pull News Sentiment for Your Market's Theme

**Why**: VADER sentiment scores show how bullish/bearish coverage is for your market's topic.

```sql
-- Example: Find all news articles mentioning Taiwan (for KXTW market)
SELECT 
  published_at,
  feed_key as source,
  title,
  sentiment_score,
  sentiment_label,
  COUNT(*) OVER (PARTITION BY feed_key ORDER BY published_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as article_rank
FROM silver.news_articles_enriched
WHERE LOWER(title) LIKE '%taiwan%'
  OR LOWER(full_text) LIKE '%taiwan%'
ORDER BY published_at DESC
LIMIT 20;
```

**Columns you'll see**:
- `sentiment_score`: -1 (very negative) to +1 (very positive) — **this is VADER compound score**
- `sentiment_label`: "Positive" / "Neutral" / "Negative" — derived from score
- `feed_key`: which news source (bbc, cnn, nyt, etc.)
- `published_at`: when the article was published
- `ingested_at`: when PredictIQ ingested it

**Learning point**: This is **raw sentiment signal**. If many sources turn negative on Taiwan in 1 hour, the system should signal "YES price should drop" (market is overpriced on YES side).

---

## STEP 4: Aggregate Sentiment into Windows (15-min signals)

**Why**: You can't trade on individual articles. You need to ask: "What's the aggregate sentiment tone in the last 15 minutes?"

```sql
-- Aggregate news sentiment by 15-minute windows
SELECT 
  DATE_TRUNC('minute', published_at) / 15 * INTERVAL '15 minutes' as sentiment_window_15m,
  COUNT(*) as article_count,
  COUNT(DISTINCT feed_key) as unique_sources,
  ROUND(AVG(sentiment_score), 3) as avg_sentiment,
  ROUND(STDDEV(sentiment_score), 3) as sentiment_std,
  MIN(sentiment_score) as most_negative,
  MAX(sentiment_score) as most_positive
FROM silver.news_articles_enriched
WHERE LOWER(title) LIKE '%taiwan%'
  AND published_at > NOW() - INTERVAL '48 hours'
GROUP BY sentiment_window_15m
ORDER BY sentiment_window_15m DESC;
```

**What this tells you**:
- **avg_sentiment**: If > 0.3 → bullish sentiment (market underpriced on YES)
- **unique_sources**: If 5+ sources agree → high confidence
- **sentiment_std**: If low → consensus; if high → disagreement

---

## STEP 5: See the GDELT Side (Global Events)

**Why**: GDELT covers geopolitical/economic events. For Taiwan (`KXTW`), GDELT might mention US-China tensions.

```sql
-- Find GDELT events mentioning Taiwan
SELECT 
  date as event_date,
  actor1_name,
  actor2_name,
  event_root_code,
  goldstein_scale,  -- -10 (most negative) to +10 (most positive)
  tone,             -- sentiment of the event
  COUNT(*) OVER (ORDER BY date DESC) as event_count
FROM silver.gdelt_events_current
WHERE LOWER(actor1_name) LIKE '%taiwan%'
  OR LOWER(actor2_name) LIKE '%taiwan%'
  OR LOWER(actor1_name) LIKE '%us%'  -- US actions toward Taiwan
ORDER BY date DESC
LIMIT 20;
```

**Key columns**:
- `goldstein_scale`: -10 to +10 — impact of event (e.g., -8 = major conflict, +8 = major cooperation)
- `tone`: -100 to +100 — sentiment of coverage
- `event_root_code`: e.g., `031` = Accuse, `070` = Appeal

**Learning point**: GDELT is **structural** (what actually happened), while news is **sentiment** (how people feel about it). Together = fuller picture.

---

## STEP 6: See the GDELT GKG Side (Themes)

**Why**: GKG groups events into themes (`POLITICS`, `DEFENSE`, `FINANCE`). This lets you see which themes are "hot" right now.

```sql
-- GDELT GKG aggregates themes by 15-min windows
SELECT 
  date as gkg_date,
  themes,  -- semicolon-separated list of themes
  tone as gkg_tone,
  ingested_at
FROM silver.gdelt_gkg_current
WHERE LOWER(themes) LIKE '%TAIWAN%'
  OR LOWER(themes) LIKE '%POLITICS%'
ORDER BY date DESC
LIMIT 10;
```

**Output example**:
- `themes`: `"POLITICS;TRADE;DEFENSE;US"`
- `gkg_tone`: e.g., `-45` (moderately negative coverage)

**Learning point**: GKG is **theme-focused**, while Events are **actor-focused**. You use GKG for "is this topic hot?" and Events for "what specific actions happened?"

---

## STEP 7: Combine News + GDELT (The Bridge)

**Why**: Now we connect both signals to the same market. Do they agree?

```sql
-- For Taiwan market (KXTW), blend news sentiment + GDELT tone
WITH news_15m AS (
  SELECT 
    DATE_TRUNC('minute', published_at) / 15 * INTERVAL '15 minutes' as window_time,
    ROUND(AVG(sentiment_score), 3) as news_sentiment
  FROM silver.news_articles_enriched
  WHERE LOWER(title) LIKE '%taiwan%'
    AND published_at > NOW() - INTERVAL '48 hours'
  GROUP BY window_time
),
gdelt_15m AS (
  SELECT 
    DATE_TRUNC('minute', date) / 15 * INTERVAL '15 minutes' as window_time,
    ROUND(AVG(tone) / 100.0, 3) as gdelt_sentiment  -- Normalize tone to -1..+1
  FROM silver.gdelt_events_current
  WHERE (LOWER(actor1_name) LIKE '%taiwan%' OR LOWER(actor2_name) LIKE '%taiwan%')
    AND date > NOW() - INTERVAL '48 hours'
  GROUP BY window_time
)
SELECT 
  COALESCE(news_15m.window_time, gdelt_15m.window_time) as signal_time,
  ROUND(COALESCE(news_sentiment, 0), 3) as news_signal,
  ROUND(COALESCE(gdelt_sentiment, 0), 3) as gdelt_signal,
  ROUND(0.7 * COALESCE(news_sentiment, 0) + 0.3 * COALESCE(gdelt_sentiment, 0), 3) as blended_signal
FROM news_15m
FULL OUTER JOIN gdelt_15m ON news_15m.window_time = gdelt_15m.window_time
ORDER BY signal_time DESC
LIMIT 20;
```

**What you see**:
- `news_signal`: Sentiment from news coverage
- `gdelt_signal`: Tone from structured events
- `blended_signal`: 70% news + 30% GDELT (your weighting)

**Trading rule** (example):
- If blended_signal > 0.3 → "Market is bearish on YES" → trade NO
- If blended_signal < -0.3 → "Market is bullish on YES" → trade YES

---

## STEP 8: Match to Market Snapshot (Kalshi Price)

**Why**: Find the Kalshi price AT THE SAME TIME as your signal. If sentiment spikes at 2:15pm, what's the market price at 2:15pm?

```sql
-- For KXTW ticker, find price snapshots aligned with sentiment windows
SELECT 
  DATE_TRUNC('minute', k.ingested_at) / 15 * INTERVAL '15 minutes' as price_window,
  k.ticker,
  k.title,
  ROUND(k.yes_bid, 2) as yes_bid,
  ROUND(k.yes_ask, 2) as yes_ask,
  ROUND((k.yes_bid + k.yes_ask) / 2, 2) as mid_price,
  k.volume,
  COUNT(*) as snapshots_in_window
FROM silver.kalshi_markets_history k
WHERE k.series_ticker = 'KXTW'
  AND k.ingested_at > NOW() - INTERVAL '48 hours'
GROUP BY price_window, k.ticker, k.title, k.yes_bid, k.yes_ask, k.volume
ORDER BY price_window DESC
LIMIT 20;
```

**Trading scenario**: If sentiment window shows blended_signal = +0.5 (bullish) at 2:15pm, and price is yes_ask = 0.32 at 2:15pm, you'd signal "YES is underpriced, buy it."

---

## STEP 9: See the Gold Layer (Mispricing Scores)

**Why**: The Gold layer already combines all this and produces **mispricing_score** (0-100). Let's see how it scores your market.

```sql
-- Check if KXTW has a mispricing score
SELECT 
  ticker,
  series_ticker,
  title,
  mispricing_score,
  edge_pct,  -- How much the market is mispriced
  ingested_at as scored_at
FROM gold.mispricing_scores
WHERE series_ticker = 'KXTW'
  AND ingested_at > NOW() - INTERVAL '24 hours'
ORDER BY ingested_at DESC
LIMIT 10;
```

**Interpretation**:
- `mispricing_score`: 0-100 (higher = more mispriced)
- `edge_pct`: How much YES/NO is overpriced vs fair value
- **Example**: score=75, edge_pct=0.12 → Market is 12% overpriced on YES side

---

## STEP 10: See the Intelligence Brief (LLM Output)

**Why**: The LLM takes the signals + briefs and writes a recommendation.

```sql
-- Find the LLM brief for your market
SELECT 
  ticker,
  title,
  current_odds,
  bull_case,
  bear_case,
  verdict,
  confidence_score,
  mispricing_score,
  ingested_at
FROM gold.intelligence_briefs
WHERE ticker LIKE '%KXTW%'
  AND ingested_at > NOW() - INTERVAL '24 hours'
ORDER BY ingested_at DESC, confidence_score DESC
LIMIT 5;
```

**What you see**:
- `bull_case`: LLM's 1-2 sentence argument for why YES should win
- `bear_case`: LLM's argument for why NO should win
- `verdict`: Final recommendation (e.g., "BUY YES" or "SELL YES")
- `confidence_score`: 0-1 (how sure is the LLM?)

---

## STEP 11: Understanding ChromaDB (The Vector Store)

**Why**: ChromaDB stores **embeddings** of news + GDELT so the LLM can do RAG (Retrieval-Augmented Generation).

ChromaDB is a **local vector database** at `data/chroma/`. It stores:

```
documents: [
  "Taiwan's TSMC announces new US fab. Geopolitical tensions rise.",
  "US-Taiwan defense pact discussed by Biden administration...",
  ...
]
embeddings: [
  [0.123, -0.456, 0.789, ...],  -- OpenAI text-embedding-3-small (1536-dim)
  [0.234, -0.567, 0.890, ...],
  ...
]
metadatas: [
  { "source": "nyt", "sentiment": 0.45, "date": "2026-05-29", ... },
  ...
]
```

**How it's fed** (from `rag/embed_silver_data.py`):
1. Read new articles from `silver.news_articles_enriched`
2. Embed title+summary with **OpenAI text-embedding-3-small**
3. Insert into ChromaDB with metadata (sentiment, source, date)
4. Same for GDELT events (but using `sentence-transformers/all-MiniLM-L6-v2`)

**How LLM uses it** (from `inference/predict_movements.py`):
1. User asks about market KXTW
2. System queries ChromaDB: "Show me the 5 most similar articles about Taiwan"
3. LLM sees: documents + sentiments → writes bull/bear case

---

## STEP 12: Full End-to-End Query

**Why**: See the ENTIRE flow for one market in one query.

```sql
-- Complete lineage for KXTW market
WITH market_snapshot AS (
  SELECT 
    MAX(ingested_at) as latest_snapshot,
    ROUND(AVG(yes_bid), 3) as avg_yes_bid,
    ROUND(AVG(yes_ask), 3) as avg_yes_ask
  FROM silver.kalshi_markets_history
  WHERE series_ticker = 'KXTW'
    AND ingested_at > NOW() - INTERVAL '6 hours'
),
news_signal AS (
  SELECT 
    COUNT(*) as article_count,
    ROUND(AVG(sentiment_score), 3) as avg_sentiment
  FROM silver.news_articles_enriched
  WHERE LOWER(title) LIKE '%taiwan%'
    AND published_at > NOW() - INTERVAL '6 hours'
),
gdelt_signal AS (
  SELECT 
    COUNT(*) as event_count,
    ROUND(AVG(tone / 100.0), 3) as avg_tone
  FROM silver.gdelt_events_current
  WHERE (LOWER(actor1_name) LIKE '%taiwan%' OR LOWER(actor2_name) LIKE '%taiwan%')
    AND date > NOW() - INTERVAL '6 hours'
),
gold_scores AS (
  SELECT 
    mispricing_score,
    edge_pct
  FROM gold.mispricing_scores
  WHERE series_ticker = 'KXTW'
    AND ingested_at > NOW() - INTERVAL '6 hours'
  ORDER BY ingested_at DESC
  LIMIT 1
),
llm_brief AS (
  SELECT 
    verdict,
    ROUND(confidence_score, 2) as confidence
  FROM gold.intelligence_briefs
  WHERE ticker LIKE '%KXTW%'
    AND ingested_at > NOW() - INTERVAL '6 hours'
  ORDER BY ingested_at DESC, confidence_score DESC
  LIMIT 1
)
SELECT 
  'KXTW (Taiwan)' as market,
  (SELECT latest_snapshot FROM market_snapshot) as latest_market_snapshot,
  (SELECT ROUND(avg_yes_ask * 100, 1) FROM market_snapshot) as yes_price_cents,
  '━━━ SIGNALS ━━━' as sep1,
  (SELECT article_count FROM news_signal) as news_articles_6h,
  (SELECT avg_sentiment FROM news_signal) as avg_sentiment_vader,
  (SELECT event_count FROM gdelt_signal) as gdelt_events_6h,
  (SELECT avg_tone FROM gdelt_signal) as avg_gdelt_tone,
  '━━━ SCORING ━━━' as sep2,
  (SELECT mispricing_score FROM gold_scores) as mispricing_score,
  (SELECT edge_pct FROM gold_scores) as edge_pct,
  '━━━ LLM VERDICT ━━━' as sep3,
  (SELECT verdict FROM llm_brief) as final_verdict,
  (SELECT confidence FROM llm_brief) as confidence;
```

---

## YOUR TURN: Write Queries

Now that you understand the schema, try these:

1. **Pick 2 markets** (different series like KXTW, KXFED). Write queries to compare their news sentiment over the last 24h.

2. **Find a GDELT spike**: Which event in the last 48h had the highest goldstein_scale (most impactful)? Which market might it affect?

3. **Check ChromaDB alignment**: Are the articles stored in ChromaDB the same ones in `news_articles_enriched`? (Hint: compare counts)

4. **Trace a brief**: Pick a recent intelligence brief. Write a query that reconstructs which news articles likely triggered it.

5. **Sentiment divergence**: Find a time when news sentiment and GDELT tone DISAGREED. What happened to the market price?

