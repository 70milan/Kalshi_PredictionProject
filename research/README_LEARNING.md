# 🎓 PredictIQ Data Lineage — Learning Guide

This folder contains **interactive tools to understand your data pipeline end-to-end**.

---

## What You'll Learn

**The Big Picture**: How a prediction signal flows from news/GDELT → through Kalshi markets → into ChromaDB embeddings → to the LLM for a final verdict.

**By the end**, you'll understand:
- ✅ How **news sentiment** (VADER) connects to **market prices**
- ✅ How **GDELT events** (structural data) augment **news sentiment**
- ✅ How **ChromaDB** stores embeddings and powers RAG
- ✅ How **Kalshi market links** to news themes
- ✅ How the **LLM reads all of this** to write a trade recommendation

---

## Tools

### 1️⃣ `learning_data_lineage.md`

**Static guide with DuckDB queries and explanations.**

Open this in your editor and follow along. Each step shows:
- 📊 **What the query does** (English explanation)
- 🔍 **The DuckDB code** (ready to copy-paste)
- 💡 **What you should see** (example output + learning point)

**Run queries directly in DuckDB**:
```bash
duckdb
.read create_objects.sql
SELECT * FROM silver.kalshi_markets_history LIMIT 5;
```

---

### 2️⃣ `explore_lineage.py`

**Interactive menu-driven explorer. Pick a step, get data.**

```bash
# From the project root
python research/explore_lineage.py
```

Menu options:
1. Find active Kalshi markets (which ones have data?)
2. News sentiment for a topic ("taiwan" → shows 15m windows)
3. GDELT events for a topic
4. Blended signals (70% news + 30% GDELT)
5. Kalshi prices (synchronized market snapshots)
6. Mispricing scores (Gold layer intelligence)
7. LLM intelligence briefs (final verdicts)

**Example workflow**:
```
1. Run the script
2. Choose "1" → Find markets
3. Pick "KXTW" (Taiwan market)
4. Choose "2" → News sentiment for "taiwan"
5. Choose "4" → Blended signals for "taiwan"
6. Choose "5" → Check prices at same time
7. Choose "6" → See Gold layer score
8. Choose "7" → Read LLM's brief
```

**Benefit**: You're tracing **one data grain** through the entire pipeline in ~5 minutes.

---

### 3️⃣ `explore_chromadb.py`

**Interactive explorer for the vector store.**

```bash
python research/explore_chromadb.py
```

Menu options:
1. List ChromaDB collections (what's indexed?)
2. Inspect a collection (how many documents? sample documents?)
3. Similarity search (retrieve articles like the LLM does)
4. Metadata schema (what's stored alongside embeddings?)
5. Embedding statistics (storage, dimensions, models used)
6. Show data flow diagram (visual of entire pipeline)

**Example**: Query "Taiwan tensions" → ChromaDB returns top-5 most similar articles + metadata → you see why LLM picked them.

---

## The Data Flow (Simplified)

```
NEWS FEEDS                 GDELT
    ↓                        ↓
VADER Sentiment          Events + GKG (Tone)
(-1 to +1)               (-10 to +10)
    ↓                        ↓
    └──────────┬─────────────┘
               ↓
    Blended Sentiment Signal (70% news + 30% GDELT)
               ↓
       [Matched to Kalshi Price]
               ↓
    Gold Layer: Mispricing Score (0-100)
               ↓
    ┌──────────────┬──────────────┐
    ↓              ↓              ↓
  ChromaDB   Mispricing    Market Price
  (Embeddings)  Score        (Real-time)
    ↓              ↓              ↓
    └──────────────┬──────────────┘
               ↓
         Groq LLM (llama-3.3-70b)
         [RAG + Scoring + Reasoning]
               ↓
    Intelligence Brief
    (Bull case, Bear case, Verdict)
               ↓
    React Dashboard + Telegram Alert
```

---

## Sample Queries

### Find top news sources discussing Taiwan
```sql
SELECT 
  feed_key,
  COUNT(*) as article_count,
  ROUND(AVG(sentiment_score), 3) as avg_sentiment
FROM silver.news_articles_enriched
WHERE LOWER(title) LIKE '%taiwan%'
GROUP BY feed_key
ORDER BY article_count DESC;
```

### Find GDELT events with highest impact (goldstein_scale)
```sql
SELECT 
  actor1_name,
  actor2_name,
  goldstein_scale,
  tone,
  date
FROM silver.gdelt_events_current
WHERE goldstein_scale IS NOT NULL
ORDER BY ABS(goldstein_scale) DESC
LIMIT 10;
```

### Match sentiment windows to market prices
```sql
WITH sentiment AS (
  SELECT 
    DATE_TRUNC('minute', published_at) / 15 * INTERVAL '15 minutes' as window,
    AVG(sentiment_score) as signal
  FROM silver.news_articles_enriched
  WHERE published_at > NOW() - INTERVAL '48 hours'
  GROUP BY window
),
prices AS (
  SELECT 
    DATE_TRUNC('minute', ingested_at) / 15 * INTERVAL '15 minutes' as window,
    ROUND(AVG(yes_ask) * 100, 1) as mid_price
  FROM silver.kalshi_markets_history
  WHERE series_ticker = 'KXTW'
  GROUP BY window
)
SELECT *
FROM sentiment
JOIN prices USING (window)
ORDER BY window DESC;
```

### Retrieve articles from ChromaDB (like LLM does)
```python
import chromadb

client = chromadb.Client()
collection = client.get_collection("news_articles")

results = collection.query(
    query_texts=["Taiwan elections 2026"],
    n_results=5,
    include=["documents", "metadatas", "distances"]
)

for doc, meta, dist in zip(results['documents'], results['metadatas'], results['distances']):
    print(f"Similarity: {1 - dist/2:.3f}")
    print(f"Text: {doc}")
    print(f"Sentiment: {meta['sentiment']}\n")
```

---

## Questions to Answer While Exploring

1. **Market selection**: Which Kalshi series has the most liquid markets? Which themes matter most?
2. **Sentiment alignment**: When news and GDELT disagree, which signal is stronger?
3. **Time lag**: How long after a news spike does the market move?
4. **ChromaDB accuracy**: Do the articles retrieved by similarity search make sense for each market?
5. **LLM reasoning**: Read 5 intelligence briefs. Do the verdicts match the sentiment signals?

---

## Running from the Command Line

If you want to run **DuckDB queries standalone**:

```bash
# Start DuckDB REPL
duckdb

# Inside DuckDB:
.read create_objects.sql

-- Now run any query from learning_data_lineage.md:
SELECT * FROM silver.news_articles_enriched 
WHERE published_at > NOW() - INTERVAL '24 hours'
LIMIT 10;
```

Or use Python + duckdb:
```python
import duckdb

conn = duckdb.connect(":memory:")
conn.execute("INSTALL delta; LOAD delta;")

# Load a view
conn.execute("""
CREATE VIEW silver.news AS
  SELECT * FROM delta_scan('C:/Data Engineering/codeprep/predection_project/data/silver/news_articles_enriched')
""")

# Query
result = conn.execute("""
SELECT 
  feed_key, 
  COUNT(*) as count,
  ROUND(AVG(sentiment_score), 3) as avg_sentiment
FROM silver.news
GROUP BY feed_key
ORDER BY count DESC
""").df()

print(result)
```

---

## Architecture Reminder

| Layer | Format | Purpose | Tools |
|-------|--------|---------|-------|
| **Bronze** | Parquet (raw) | Raw ingestion from APIs | `read_parquet()` |
| **Silver** | Delta Lake | Cleaned, deduplicated, ACID | `delta_scan()` + Spark |
| **Gold** | Delta Lake | Aggregated scores, LLM outputs | `delta_scan()` + Spark |
| **Intelligence** | Parquet (append-only) | LLM briefs (one-writer) | `read_parquet()` |
| **ChromaDB** | Vector index | Embeddings + metadata | `chromadb` Python SDK |

---

## Next Steps

1. **Read** `learning_data_lineage.md` from top to bottom (takes 20 min)
2. **Run** `explore_lineage.py` and trace 2-3 markets end-to-end (takes 10 min)
3. **Run** `explore_chromadb.py` and understand RAG retrieval (takes 10 min)
4. **Write** your own queries to answer the 5 questions above

---

## Troubleshooting

### "View not found" error
**Cause**: Delta files don't exist for that layer yet.
**Fix**: Make sure the ETL has run: `orchestration/run_etl.py`

### "ChromaDB directory not found"
**Cause**: RAG hasn't run yet to initialize the vector store.
**Fix**: Run `rag/embed_silver_data.py` manually once, then explore.

### "No data in Silver layer"
**Cause**: Bronze layer is empty (ingestors haven't pulled data yet).
**Fix**: Check that containers are running: `list_containers()` from Portainer MCP

### DuckDB query hangs
**Cause**: Large Delta table scan (history layer can be 100k+ rows).
**Fix**: Add time filters: `WHERE ingested_at > NOW() - INTERVAL '24 hours'`

---

## Going Deeper

Once you understand the flow, you can:
- 🔬 **Backtest**: Run sentiment-only strategy on historical data
- 🎯 **Optimize**: Tune sentiment thresholds + GDELT/news weighting
- 📊 **Analyze**: Which themes drive the most profitable trades?
- 🤖 **Improve**: Add new data sources or embedding models to ChromaDB

---

Good luck! 🚀

