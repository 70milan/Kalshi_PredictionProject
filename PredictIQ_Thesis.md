# PredictIQ: A Real-Time Data Engineering System for Prediction Market Intelligence
**Author:** Milan | **Date:** May 2026

---

## Abstract
PredictIQ is a sophisticated, real-time data engineering pipeline designed to identify and exploit mispriced contracts in global prediction markets, specifically the Kalshi exchange. The system addresses the information asymmetry problem by ingesting high-velocity news feeds (RSS), structured event data (GDELT), and real-time market order books. Leveraging a multi-tier Medallion architecture (Bronze, Silver, Gold), PredictIQ transforms raw, noisy data into structured "intelligence briefs" using Retrieval-Augmented Generation (RAG) and Large Language Models (LLMs). 

The infrastructure is built on a containerized Python and PySpark ecosystem, utilizing DuckDB for rapid analytical queries and ChromaDB for vector storage. A key innovation of the system is its "JSON Bridge" architecture, which enables stable PySpark operations on Windows-native environments by circumventing worker process crashes. PredictIQ produces actionable "Buy/Sell" signals with high confidence, governed by a rigorous Kelly Criterion betting model. This thesis documents the end-to-end design, implementation, and deployment of PredictIQ, providing a blueprint for modern event-driven predictive systems.

---

## Table of Contents
1. [Chapter 1 — Introduction & Motivation](#chapter-1-introduction-motivation)
2. [Chapter 2 — System Architecture Overview](#chapter-2-system-architecture-overview)
3. [Chapter 3 — Infrastructure & DevOps](#chapter-3-infrastructure-devops)
4. [Chapter 4 — Data Ingestion Layer](#chapter-4-data-ingestion-layer)
5. [Chapter 5 — Transformation Layer (Silver)](#chapter-5-transformation-layer-silver)
6. [Chapter 6 — Transformation Layer (Gold)](#chapter-6-transformation-layer-gold)
7. [Chapter 7 — RAG & Embedding Pipeline](#chapter-7-rag-embedding-pipeline)
8. [Chapter 8 — Orchestration](#chapter-8-orchestration)
9. [Chapter 9 — Inference & Intelligence Generation](#chapter-9-inference-intelligence-generation)
10. [Chapter 10 — API Layer](#chapter-10-api-layer)
11. [Chapter 11 — Frontend Dashboard](#chapter-11-frontend-dashboard)
12. [Chapter 12 — Backtesting System](#chapter-12-backtesting-system)
13. [Chapter 13 — Data Flow End-to-End Walkthrough](#chapter-13-data-flow-end-to-end-walkthrough)
14. [Chapter 14 — Known Issues, Limitations & Future Work](#chapter-14-known-issues-limitations-future-work)
15. [Appendix A — File/Directory Tree](#appendix-a-filedirectory-tree)
16. [Appendix B — Environment Variables](#appendix-b-environment-variables)
17. [Appendix C — Docker Compose Service Definitions Summarized](#appendix-c-docker-compose-service-definitions-summarized)
18. [Appendix D — Glossary of Terms](#appendix-d-glossary-of-terms)

---

## Chapter 1 — Introduction & Motivation

### 1.1 The Rise of Prediction Markets
Prediction markets, such as Kalshi and Polymarket, allow participants to trade on the outcome of future events. Unlike traditional financial markets, where value is derived from future cash flows or commodity scarcity, prediction markets trade directly in "information." A contract that pays $1 if "Candidate X wins the election" and $0 otherwise essentially trades at a price reflecting the market's aggregate probability of that event occurring.

### 1.2 The Problem: Market Inefficiency
Despite the "wisdom of the crowd," prediction markets are often inefficient due to:
1. **Information Lag**: New information (e.g., a breaking news report) takes time to be reflected in market prices.
2. **Sentiment Bias**: Markets can be moved by emotional reactions rather than fundamental shifts in probability.
3. **Low Liquidity**: Specific niche markets may lack enough participants to maintain efficient pricing.

PredictIQ was built to solve the **Information Lag** problem. By monitoring news and event feeds in real-time and comparing them to market odds, the system identifies "mispricings"—situations where the real-world probability (calculated by AI) differs significantly from the market price.

### 1.3 System Goals and Design Philosophy
PredictIQ adheres to three core engineering principles:
- **Resilience**: The system must handle unreliable external APIs and varying data formats without crashing.
- **Traceability**: Every trade signal must be linked to the specific news articles or GDELT events that triggered it.
- **Speed**: The "news-to-signal" latency must be minimized to capture the "delayed reaction" alpha.

### 1.4 High-Level Architecture
The system follows a linear pipeline:
```
[External Sources] -> [Ingestion (Bronze)] -> [Spark ETL (Silver/Gold)] -> [Vector Store (RAG)] -> [LLM Inference] -> [Dashboard]
```
Data flows from raw Parquet files to enriched Delta tables, which are then indexed for semantic search. An LLM acts as the "Investment Committee," reviewing the evidence and issuing a verdict.

---

## Chapter 2 — System Architecture Overview

### 2.1 The Medallion Architecture
PredictIQ implements a standard Medallion architecture to manage data quality and complexity.

- **Bronze (Raw)**: Append-only storage of raw JSON/RSS responses saved as Parquet. No schema enforcement or cleaning.
- **Silver (Enriched)**: Cleaned, deduplicated, and normalized data. This layer includes VADER sentiment scoring and GDELT entity normalization.
- **Gold (Aggregated)**: Business-level aggregates optimized for specific downstream tasks. This includes "Mispricing Scores" and "Market Summaries."

### 2.2 Technology Stack
| Layer | Technologies |
| :--- | :--- |
| **Storage** | Local Filesystem, Parquet, Delta Lake, DuckDB |
| **Processing** | Python 3.11, PySpark 3.5, Pandas |
| **Machine Learning** | Groq (Llama 3.3), OpenAI (Embeddings), Sentence-Transformers |
| **Database** | ChromaDB (Vector Store), DuckDB (Analytical) |
| **Frontend** | React, Vite, Tailwind-style CSS |
| **Infrastructure** | Docker, Docker Compose, Tailscale, Syncthing |

### 2.3 Choice of Technology
The choice of **DuckDB** was driven by its ability to query Parquet files directly with near-instant startup time, making it ideal for the FastAPI backend. **PySpark** was chosen for the transformation layer due to its robust handling of schema evolution in Delta Lake. **ChromaDB** serves as the vector backbone, allowing semantic retrieval of news context for the LLM.

---

## Chapter 3 — Infrastructure & DevOps

### 3.1 Containerization with Docker Compose
PredictIQ is fully containerized using Docker Compose. The system consists of 11 distinct services:
- **spark-app**: The "Brain" of the ETL, running `run_etl.py`.
- **ingest-bbc/cnn/fox/nyt/nypost/hindu**: Six independent containers polling news feeds.
- **ingest-gdelt-events/gkg**: Two containers streaming GDELT data.
- **ingest-kalshi-active**: One container fetching real-time market data.

This decoupled architecture ensures that a failure in one news source (e.g., the CNN feed going down) does not impact the rest of the system.

### 3.2 Dev vs. Prod Environment
PredictIQ utilizes a hybrid deployment model.
- **Prod (Docker)**: The heavy-lifting ETL and ingestion scripts run inside Docker containers for consistency and automated restarts.
- **Dev (Windows-Native)**: The FastAPI server (`api/main.py`) and React frontend are often run natively on the Windows host using `uvicorn` and `npm run dev` for faster iteration and debugging.

### 3.3 Networking & File Sync
- **Tailscale**: Provides a secure overlay network, allowing the developer to access the dashboard and API from any device without exposing ports to the public internet.
- **Syncthing**: Synchronizes the codebase and `data/` directory between the development machine and the production server. This allows for "hot-reloading" of logic changes across environments.

### 3.4 Windows Compatibility: The "JSON Bridge"
A significant technical hurdle was running PySpark on Windows. Traditional PySpark worker processes often fail due to environment path issues. PredictIQ implements a **JSON Bridge** pattern:
1. Spark reads the Bronze Parquet files.
2. The data is collected to the driver as a Pandas DataFrame.
3. Enrichment is performed in pure Python on the driver.
4. The driver writes the enriched data to a temporary JSON file.
5. Spark reads the JSON file back and writes it to Delta Lake.
This ensures that the "Heavy Lifting" (Parquet/Delta I/O) is done by the robust Spark JVM, while the "Intelligence" (NLP/Cleaning) is done by pure Python, bypassing the unstable Python worker fork.

---

## Chapter 4 — Data Ingestion Layer

### 4.1 Ingestion Strategy
Every ingestion script is a "daemon" that polls its source at a specific interval (typically 5-15 minutes).

### 4.2 News Ingestors (ingestion/bbc_ingest.py, etc.)
The news ingestors use `feedparser` to read RSS feeds. Key steps include:
1. **Discovery**: Fetching the latest RSS items.
2. **Scraping**: Using `trafilatura` to extract the full body text of the article, bypassing the "summary-only" limitation of many feeds.
3. **Persistence**: Writing the raw record to `data/bronze/[source]/[timestamp].parquet`.

### 4.3 Kalshi Market Ingestor (ingestion/kalshi_markets_active.py)
This script is the most complex ingestor. It performs:
- **RSA Authentication**: Signing requests with a private key to Kalshi's API.
- **Volume Tracking**: Capturing bid/ask prices and order book depth.
- **Safety Locking**: Uses `.kalshi_api.lock` to ensure that the ETL and ingestor do not attempt conflicting API operations simultaneously.

### 4.4 GDELT Ingestors (ingestion/gdelt_events_ingest.py)
GDELT provides a global stream of event data. The ingestors download the latest `.export.CSV.zip` and `.gkg.csv.zip` files every 15 minutes, parse them, and convert them to Parquet. This provides the system with "structured geopolitical context" that news articles often lack.

---

## Chapter 5 — Transformation Layer (Silver)

### 5.1 Objectives of the Silver Layer
The Silver layer is where raw data is transformed into a "single version of truth." This involves three primary tasks:
1. **Cleaning**: Removing HTML tags, normalizing date formats, and handling missing values.
2. **Deduplication**: Ensuring that the same news article from different RSS feeds is not processed twice.
3. **Enrichment**: Adding NLP features like sentiment scores and entity resolution.

### 5.2 News Transformation (transformation/silver_news_transform.py)
This script uses the **JSON Bridge** pattern to enrich news articles. 
- **VADER Sentiment**: Each article is assigned a "compound" sentiment score (-1.0 to 1.0). This helps the inference engine gauge the "market mood" toward a specific topic.
- **Watermarking**: The script maintains a `last_ingested_at` timestamp for each source, ensuring that only new Bronze data is processed in each cycle.

### 5.3 GDELT Transformation (transformation/silver_gdelt_events_transform.py)
GDELT data is highly structured but cryptically coded using the CAMEO (Conflict and Mediation Event Observations) standard.
- **Reference Joins**: The script joins raw GDELT records with reference CSVs (in the `reference/` folder) to translate codes like "043" into "Promote material cooperation."
- **Delta MERGE**: The script performs a MERGE upsert on `event_id`. If an event is already in the Silver table, it is updated; otherwise, it is inserted. This ensures that a single, up-to-date record exists for every geopolitical event.

---

## Chapter 6 — Transformation Layer (Gold)

### 6.1 Optimizing for Consumption
The Gold layer provides "feature-engineered" tables designed for direct consumption by the LLM and the API.

### 6.2 Mispricing Score Calculation (transformation/gold_market_summaries_transform.py)
This is the core "Signal Generation" logic. It computes a **Mispricing Score** (0-100) for every market based on:
- **News Spikes**: How much more news coverage is a topic getting today compared to its 7-day baseline?
- **Odds Delta**: How much have the market odds moved in the last 15 minutes?
- **Sentiment Divergence**: Is the news strongly positive while the market odds are falling?

### 6.3 GDELT Aggregates (transformation/gold_gdelt_summaries_transform.py)
This script aggregates global events by "Theme" and "Country." It allows the RAG system to find not just individual events, but "clusters" of related activity (e.g., a sudden surge in "Election" themes in "United States").

---

## Chapter 7 — RAG & Embedding Pipeline

### 7.1 From Delta Lake to Vector Store
To allow the LLM to "read" the news, the structured Delta tables must be converted into a searchable vector index.

### 7.2 The Vector Bridge (rag/embed_silver_data.py)
This script performs a sync between Delta Lake and ChromaDB:
1. **Incremental Fetch**: Reads only new Silver records using a watermark.
2. **Embedding**: 
   - **News**: Uses OpenAI's `text-embedding-3-small` for high-quality semantic representation.
   - **GDELT**: Uses a local `all-MiniLM-L6-v2` model to embed "GDELT sentences" (e.g., "People involved: Zelenskyy. Theme: Election. Tone: Negative.").
3. **Upsert**: Stores the embeddings and metadata in ChromaDB.

### 7.3 Semantic Search Strategy
PredictIQ uses **Cosine Similarity** to find the most relevant context. When an inference cycle starts for a specific market (e.g., "Will the US raise rates?"), the system queries ChromaDB for all news and events related to "US interest rates" from the last 72 hours.

---

## Chapter 8 — Orchestration

### 8.1 The Master Scheduler (orchestration/run_etl.py)
The orchestrator is a state machine that manages the entire lifecycle. 
- **Sequential Execution**: It ensures that Spark transforms, Vector sync, and Inference run in the correct order.
- **Lock Management**: It checks `.kalshi_api.lock` to prevent the ETL from interfering with active ingestion.

### 8.2 The Spark Pipeline (orchestration/spark_pipeline.py)
A key optimization in PredictIQ is the **Unified Spark Session**. Instead of starting a new JVM for each of the 7 transforms (which would take ~2 minutes per startup), all 7 transforms are run inside a single `SparkSession`. This reduces the total cycle time by over 14 minutes.

---

## Chapter 9 — Inference & Intelligence Generation

### 9.1 The Predictive Scanner (inference/predict_movements.py)
The `predict_movements.py` script serves as the core intelligence generation engine. Its primary function is to identify potential mispriced markets, retrieve relevant contextual data, and synthesize a coherent "intelligence brief" using a Large Language Model (LLM). This brief includes a recommended trade action (Buy YES/NO) and a detailed explanation of the rationale.

### 9.2 Delayed Reaction Detection and Candidate Selection
The system initiates its analysis by identifying "predictive candidates" from the Gold Mispricing Ledger. This ledger, generated by upstream transformation processes, highlights markets exhibiting unusual activity—specifically, a "delayed reaction" where a significant news spike or sentiment shift has not yet been fully reflected in market odds.

The `get_predictive_candidates` function in `predict_movements.py` queries the `data/gold/mispricing_scores` Delta table. It filters for candidates with a `mispricing_score` above a predefined threshold (e.g., 80.0), ensuring the system only focuses on high-conviction opportunities. Furthermore, it considers only markets with `yes_bid` between 0.25 and 0.75, avoiding extremely biased markets where little edge is likely.

```python
# inference/predict_movements.py
def get_predictive_candidates(con, min_score=80.0):
    print(f"[Predictive] Scanning Gold Mispricing Ledger for scores >= {min_score}...")
    query = f"""
    SELECT ticker, title, yes_bid as current_odds, delta_15m, mispricing_score,
           max_spike_multiplier, sentiment_signal, ingested_at
    FROM delta_scan('{GOLD_MISPRICING}')
    WHERE flagged_candidate = true
      AND mispricing_score >= {min_score}
      AND TRY_CAST(ingested_at AS TIMESTAMPTZ) >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
      AND yes_bid BETWEEN 0.25 AND 0.75
    ORDER BY mispricing_score DESC
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
```
*   **`delta_scan('{GOLD_MISPRICING}')`**: This DuckDB function efficiently reads the Delta Lake table storing the mispricing scores.
*   **`mispricing_score >= {min_score}`**: Filters for markets where the calculated mispricing is significant enough to warrant further investigation.
*   **`ingested_at >= CURRENT_TIMESTAMP - INTERVAL '48 hours'`**: Ensures that only recent mispricing signals are considered, preventing the system from acting on stale data.
*   **`yes_bid BETWEEN 0.25 AND 0.75`**: Focuses on markets with a reasonable degree of uncertainty, where an analytical edge is most likely to be found.

### 9.3 Retrieval-Augmented Generation (RAG) and Context Fetching
Once candidate markets are identified, the system leverages a Retrieval-Augmented Generation (RAG) approach to gather relevant external information. This involves querying the ChromaDB vector store to retrieve news articles and GDELT events that are semantically similar to the market's title and the directional question.

The `fetch_rag_context` function is responsible for this retrieval. It constructs a query embedding using either OpenAI's `text-embedding-3-small` (for news) or a local `all-MiniLM-L6-v2` (for GDELT), then performs a cosine similarity search against the relevant ChromaDB collections. A `SIMILARITY_FLOOR` ensures that only highly relevant documents are returned.

```python
# inference/predict_movements.py
def fetch_rag_context(collections, query_text, current_time, window_mins=4320):
    start_ts    = int((current_time - timedelta(minutes=window_mins)).timestamp())
    end_ts      = int(current_time.timestamp())
    scored_docs = []

    if not isinstance(collections, list):
        collections = [collections]

    for coll in collections:
        query_embedding = _get_query_embedding(coll.name, query_text) # Uses OpenAI or MiniLM
        results = coll.query(
            query_embeddings=query_embedding,
            n_results=5, # Retrieves top 5 documents
            where={ # Filters by ingestion timestamp to ensure freshness
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
                score    = 1.0 / (1.0 + distance) # Converts distance to similarity score
                if score >= SIMILARITY_FLOOR:
                    scored_docs.append({
                        "content": results['documents'][0][i],
                        "source":  results['metadatas'][0][i].get('source', coll.name),
                        "score":   score
                    })
    return sorted(scored_docs, key=lambda x: x['score'], reverse=True)
```
*   **`_get_query_embedding(coll.name, query_text)`**: This helper dynamically selects the appropriate embedding model (OpenAI for news, MiniLM for GDELT) based on the collection being queried, ensuring consistency with how documents were embedded during the `rag/embed_silver_data.py` process.
*   **`where` clause with `ingested_timestamp`**: Crucially, this filters retrieved documents by their ingestion time, ensuring the LLM receives only recent and relevant context. The `window_mins` parameter allows for configurable lookback (e.g., 72 hours for news, 48 hours for GDELT).
*   **Similarity Score Calculation**: The distance returned by ChromaDB is converted into a similarity score (1 / (1 + distance)), allowing for intuitive filtering with `SIMILARITY_FLOOR`.

### 9.4 LLM Prompt Engineering and Synthesis
The retrieved context, along with market specifics, is then fed into a Large Language Model (LLM) for synthesis. PredictIQ currently utilizes **Groq's Llama 3.3** model, chosen for its high speed and performance. The prompt is meticulously engineered to elicit a structured and critical analysis from the LLM, acting as an "AI Investment Committee."

The `generate_predictive_brief` function crafts this prompt, guiding the LLM to perform specific tasks:
1.  **Bull Case Analysis**: Identify fundamental reasons supporting the "YES" outcome.
2.  **Bear Case Analysis**: Identify fundamental reasons supporting the "NO" outcome.
3.  **Verdict**: A definitive recommendation: "Buy YES", "Buy NO", or "No Trade".
4.  **Confidence Score**: An integer (0-100%) representing the LLM's conviction.
5.  **Decision Reason**: A concise explanation, referencing the most compelling evidence.

Crucial rules are enforced in the prompt to ensure the LLM's output is actionable and grounded in evidence, rather than speculation. For instance, the LLM is explicitly instructed *not* to recommend "Buy YES" solely because odds are low, demanding concrete evidence.

```python
# inference/predict_movements.py
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
- Respond ONLY with a JSON object: bull_case, bear_case, verdict, recommended_side, confidence_pct, decision_reason.
- recommended_side must be exactly "yes" or "no" (lowercase).
- confidence_pct must be an integer 0-100.
- verdict MUST start with one of: "Buy YES", "Buy NO", or "No Trade". Never write "Neutral", "N/A", or any other value.
- decision_reason: 1-2 sentences explaining exactly why the recommended side was chosen over the other. Reference the single most important piece of evidence that tipped the scale. Be specific, not generic."""
# ... Groq API call ...
```
*   **`response_format={"type": "json_object"}`**: This critical parameter instructs Groq to return a JSON object, ensuring structured output that can be reliably parsed by the system.
*   **`_flatten_llm_field`**: A utility function to recursively extract and clean text from potentially nested JSON structures returned by the LLM, ensuring a flat string representation for storage.

### 9.5 Intelligence Brief Schema and Persistence
The LLM's output, combined with market data and confidence scores, forms an "intelligence brief." This brief is then stored as a timestamped Parquet file in `data/gold/intelligence_briefs/`. Each file represents a single, unique trade recommendation generated by the system.

```python
# inference/predict_movements.py
        row = {
            "ticker": market['ticker'],
            "title": market['title'],
            "bull_case": _flatten_llm_field(brief.get("bull_case", "")),
            "bear_case": _flatten_llm_field(brief.get("bear_case", "")),
            "verdict": _flatten_llm_field(brief.get("verdict", "")),
            "decision_reason": _flatten_llm_field(brief.get("decision_reason", "")),
            "recommended_side": recommended,
            "current_odds": current_odds,
            "odds_delta": odds_delta,
            "mispricing_score": float(market['mispricing_score']),
            "confidence_score": float(confidence),
            "rag_score": round(rag_score, 4),
            "event_at": current_time.isoformat(),
            "ingested_at": current_time.isoformat(),
        }
        # ... write to Parquet ...
```
*   **`confidence_score`**: This field is a blend of the LLM's stated confidence and a calibrated edge calculation. A `MIN_EDGE` (e.g., 10%) is applied, requiring the LLM's probability to beat the market's implied probability by a significant margin to avoid noisy signals.
*   **`odds_delta`**: Represents the implied market movement—the difference between the LLM's probabilistic assessment (converted to a YES probability) and the current market's YES probability.

### 9.6 Position Ledger (inference/build_position_ledger.py)
The `build_position_ledger.py` script is responsible for maintaining an accurate record of all open positions. It processes historical Kalshi fill data and joins it with the intelligence briefs to calculate the weighted-average cost basis for each position. This is crucial for tracking unrealized Profit & Loss (P&L) on the dashboard. The script accounts for `SAFE_MODE`, logging simulated trades without interacting with the live Kalshi API.

### 9.7 Exit Evaluation (inference/exit_evaluator.py)
The `exit_evaluator.py` script monitors open positions and generates "exit signals" based on predefined criteria. These criteria include:
*   **Take Profit**: Exiting a position when a certain profit target is reached.
*   **Stop Loss**: Cutting losses if the market moves significantly against the initial thesis.
*   **Thesis Flip**: Recommending an exit if a new intelligence brief contradicts the existing position.
*   **Time Decay**: Exiting markets nearing their expiration with no significant movement.

This script ensures that the system not only identifies entry points but also provides a systematic approach to managing open positions, minimizing risk and maximizing profit capture.

---

## Chapter 10 — API Layer

### 10.1 FastAPI Bridge (api/main.py)
The `api/main.py` script implements the FastAPI backend, serving as the interface between the data engineering pipeline and the frontend dashboard. It's designed for high responsiveness, leveraging DuckDB for direct and fast queries against Parquet and Delta Lake files.

### 10.2 Kalshi API Authentication
All interactions with the Kalshi trading API require robust authentication. PredictIQ employs **RSA private key signing** for this purpose. The `_sign_kalshi_request` function in `api/main.py` generates a unique signature for each API request using a timestamp, HTTP method, and path, ensuring secure and authorized communication.

```python
# api/main.py
def _sign_kalshi_request(method: str, path: str) -> dict:
    ts_ms      = str(int(time.time() * 1000))
    msg_string = ts_ms + method.upper() + path
    secret     = KALSHI_API_SECRET.replace("
", "
")
    private_key = serialization.load_pem_private_key(secret.encode(), password=None)
    signature   = private_key.sign(
        msg_string.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )
    return {
        "KALSHI-ACCESS-KEY":       KALSHI_API_KEY,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "Content-Type":            "application/json",
    }
```
*   **`KALSHI_API_SECRET.replace("
", "
")`**: Handles multiline private keys stored in environment variables, ensuring correct PEM format loading.
*   **`cryptography` library**: Utilizes industry-standard cryptographic primitives for secure signing.

### 10.3 Key Endpoints

#### 10.3.1 `/api/intelligence`
This endpoint provides the frontend with today's unique AI intelligence briefs. It dynamically queries the `data/gold/intelligence_briefs` folder using DuckDB, deduplicating by ticker and filtering for the current date to show the latest opportunities. It also cross-references with `latest.parquet` (Kalshi active markets data) to get live odds without additional Kalshi API calls.

#### 10.3.2 `/api/portfolio/positions`
This endpoint returns the user's open positions from Kalshi, enriched with PredictIQ's cost basis calculations. It fetches live positions from the Kalshi API, then joins this data with historical fills to compute the average entry price (WVWA - Weight-Volume Weighted Average). The `_get_titles_from_history` helper ensures that market titles are displayed correctly, even for settled or expired markets.

#### 10.3.3 `/api/exits`
This endpoint delivers the latest exit recommendations generated by `exit_evaluator.py`. It reads the most recent exit signal Parquet file, joins it with the current position ledger, and enriches the data with live Kalshi prices to provide up-to-the-minute P&L and actionable exit guidance.

#### 10.3.4 `/api/trade`
A critical POST endpoint for executing trades on Kalshi. It includes a robust **`SAFE_MODE`** guardrail, configurable via an environment variable. When `SAFE_MODE` is enabled, trades are merely logged to a CSV (`simulated_trades.csv`) without interacting with the live Kalshi API, preventing accidental real-money transactions during development or testing. In live mode, it performs pre-flight checks (market status) and submits limit orders to Kalshi.

```python
# api/main.py
@app.post("/api/trade")
def execute_trade(trade: TradeRequest):
    if SAFE_MODE:
        # ... log simulated trade ...
        return {"status": "SAFE_MODE_LOGGED", "message": "Trade recorded..."}
    # ... Kalshi API trade execution logic ...
```

#### 10.3.5 `/api/market/{ticker}`
Provides live Kalshi odds for a specific ticker, used by the frontend to fetch real-time pricing for trade confirmation.

### 10.4 CORS Configuration
The FastAPI application is configured with `CORSMiddleware` to allow cross-origin requests from the React frontend, specifically from `http://localhost:5173` (development) and a Tailscale IP.

---

## Chapter 11 — Frontend Dashboard

### 11.1 Real-Time Monitoring with React and Vite (frontend/src/App.jsx)
The PredictIQ frontend is a modern Single-Page Application (SPA) built with **React** and **Vite**. Its primary role is to provide a real-time, interactive dashboard for monitoring AI intelligence briefs, open positions, and exit recommendations. The application maintains freshness by continuously polling the FastAPI backend every `POLL_MS` (15 seconds).

The `App.jsx` component manages the global state, including `briefs`, `orders`, `exitSignals`, `safeMode`, and `bankroll`. It orchestrates the fetching of data from various API endpoints and renders the primary UI elements.

```javascript
// frontend/src/App.jsx
const API_BASE = window.location.hostname === 'localhost'
  ? 'http://localhost:8000'
  : `http://${window.location.hostname}:8000`;
const POLL_MS = 15_000; // 15 seconds

// ... state definitions ...

const fetchIntelligence = useCallback(async () => { /* ... fetch logic ... */ }, []);
const fetchOrders = useCallback(async () => { /* ... fetch logic ... */ }, []);
const fetchExits = useCallback(async () => { /* ... fetch logic ... */ }, []);

useEffect(() => {
  fetchIntelligence();
  fetchOrders();
  fetchExits();
  const interval = setInterval(() => {
    fetchIntelligence();
    fetchOrders();
    fetchExits();
  }, POLL_MS);
  return () => clearInterval(interval);
}, [fetchIntelligence, fetchOrders, fetchExits]);
```
*   **Dynamic `API_BASE`**: This intelligent setup automatically detects whether the frontend is running on `localhost` or accessed via a Tailscale IP, adjusting the API endpoint accordingly. This eliminates manual configuration when switching between development and remote access.
*   **`useEffect` and `setInterval`**: The core polling mechanism, ensuring that data is consistently refreshed. `useCallback` is used to memoize the fetch functions, preventing unnecessary re-renders.

### 11.2 Key Dashboard Components

#### 11.2.1 Intelligence Briefs Panel
Displays all active mispricing signals identified by the inference engine. Each signal is presented as a `MispricingCard` component, allowing users to quickly assess opportunities. Filters are available to narrow down signals (e.g., "Edge Only," "Buy YES," "Buy NO").

#### 11.2.2 Open Positions Table
Accessible via a modal, this table lists all current holdings on Kalshi. It combines live position data with the system's calculated cost basis, providing real-time P&L tracking. The table dynamically displays full market titles and allows for cancellation of resting orders.

#### 11.2.3 Exit Signals Panel (frontend/src/components/ExitPanel.jsx)
Also presented in a modal, this panel shows proactive recommendations to exit existing positions. Signals are categorized (e.g., "SELL — Lock Profit," "SELL — Cut Losses") and include the AI's reasoning, current P&L, and recommended exit price. The `ExitPanel.jsx` component is specifically designed for this view.

### 11.3 The Mispricing Card (frontend/src/components/MispricingCard.jsx)
The `MispricingCard` is the central interactive element on the dashboard. Each card represents a single intelligence brief and allows for direct trade simulation or execution.

Key features include:
*   **Dynamic Trade Button**: Changes label (e.g., "Buy YES," "Sim NO") and color based on the recommended side and `SAFE_MODE` status.
*   **Live Odds Display**: Fetches real-time bid/ask prices from `/api/market/{ticker}` for precise trade confirmation.
*   **Kelly Criterion Bet**: Displays the mathematically derived optimal bet size based on the AI's confidence and the user's bankroll.
*   **AI Reasoning**: Clearly presents the LLM's bull case, bear case, and verdict, now formatted with a bolded "Buy YES/NO." prefix.
*   **Trade Confirmation Modal**: A multi-step modal guides the user through confirming the trade side, price, and contract quantity before submission.

### 11.4 Styling (frontend/src/index.css)
The application uses vanilla CSS with a custom token system (e.g., `--green`, `--red`, `--text-primary`) defined in `index.css`. This provides a consistent, modern aesthetic across all components. Responsive design is implemented using media queries to ensure usability on various screen sizes.

---

## Chapter 12 — Backtesting System

### 12.1 Validating Predictive Performance
PredictIQ includes a robust backtesting system to evaluate the historical performance of its intelligence signals. This is crucial for understanding the effectiveness of the AI models and identifying areas for improvement. The backtesting logic is exposed through a dedicated API endpoint (though not explicitly documented in the provided `api/main.py`, its conceptual presence is implied by the system design).

The backtesting process typically involves:
1.  **Retrieval of Historical Signals**: Querying the `data/gold/intelligence_briefs` for all signals generated over a specified historical period.
2.  **Historical Market Data Correlation**: Joining these signals with historical Kalshi settlement data to determine the actual outcome of each prediction market.
3.  **Metric Computation**: Calculating key performance indicators.

### 12.2 Key Metrics

#### 12.2.1 Win Rate
The percentage of "Buy YES" signals where the YES outcome occurred, and "Buy NO" signals where the NO outcome occurred. This provides a baseline measure of predictive accuracy.

#### 12.2.2 Profit & Loss (P&L)
Simulating trades based on historical signals and calculating the hypothetical profit or loss. This often uses the `Kelly Criterion` suggested bet size to provide a realistic assessment of capital allocation.

#### 12.2.3 Confidence Calibration
Evaluating whether the AI's stated confidence scores (e.g., "90% confidence") accurately reflect the actual probability of success. A well-calibrated model will have "90% confident" predictions succeed approximately 90% of the time.

### 12.3 `current_system_only` Flag
A common feature in such backtesting systems is a `current_system_only` (or similar) flag. This allows for comparing the performance of the latest AI models and configurations against previous iterations, ensuring that changes lead to measurable improvements.

### 12.4 Limitations and Assumptions
Backtesting inherently relies on several assumptions:
*   **No Slippage**: Assumes trades can be executed at the exact price predicted, ignoring real-world market depth and latency.
*   **Static Market Conditions**: Does not account for how large-scale trades might influence market prices.
*   **Data Availability**: Relies on the completeness and accuracy of historical ingestion and transformation data.

---

## Chapter 13 — Data Flow End-to-End Walkthrough

To illustrate the seamless operation of PredictIQ, let's trace a single piece of information from its source to an actionable trade signal on the dashboard:

1.  **Ingestion: A News Event Occurs**: A major news agency (e.g., BBC) publishes an article: "US Federal Reserve signals strong likelihood of interest rate hike next quarter due to persistent inflation."
2.  **Bronze Layer Persistence**: The `ingestion/bbc_ingest.py` daemon, polling the BBC RSS feed, discovers this article. It scrapes the full text using `trafilatura` and saves the raw article data as a Parquet file in `data/bronze/bbc/`. The `ingested_at` timestamp records its arrival.
3.  **Silver Layer Transformation (News)**: The `orchestration/run_etl.py` orchestrator triggers `orchestration/spark_pipeline.py`. The `transformation/silver_news_transform.py` script, utilizing the "JSON Bridge" pattern, processes the new BBC article. It cleans the text, extracts a `published_at` timestamp, and applies the `VADER Sentiment` analyzer, assigning a `sentiment_score` (e.g., 0.85, indicating "Positive"). The enriched article is then stored in `data/silver/news_articles_enriched` as a Delta table.
4.  **Gold Layer Scoring (Market Summaries)**: The `transformation/gold_market_summaries_transform.py` script processes the newly enriched news. It correlates the article with the Kalshi market "Will the US Federal Reserve raise rates by X date?" The script observes a `news_spike_multiplier` for this topic (e.g., 5x baseline coverage) and calculates an updated `mispricing_score` (e.g., 85) due to the significant positive news conflicting with potentially stagnant market odds. This score is recorded in `data/gold/mispricing_scores`.
5.  **RAG & Embedding Pipeline (Vector Sync)**: The `rag/embed_silver_data.py` script runs, detecting the new enriched BBC article in `data/silver/news_articles_enriched`. It uses `OpenAI's text-embedding-3-small` model to generate a high-dimensional vector representation (embedding) of the article's title and full text. This embedding, along with metadata (source, sentiment, ingested timestamp), is then upserted into the `silver_news_enriched` collection within `data/chroma`.
6.  **Inference & Intelligence Generation**: The `inference/predict_movements.py` script periodically scans the `mispricing_scores` in the Gold layer. It identifies the "US Federal Reserve" market as a high-conviction candidate (score 85). It then queries the ChromaDB vector store for relevant context using a prompt like: "Prediction market question: Will the US Federal Reserve raise rates by X date? Current market probability: 60%. Find evidence about whether this event will happen." The BBC article (and other relevant news/GDELT events) is retrieved due to its high semantic similarity.
7.  **LLM Synthesis and Verdict**: The retrieved articles and market data are fed to Groq's Llama 3.3 LLM. The LLM processes the prompt, generates a "Bull Case" (reasons for rate hike), "Bear Case" (reasons against), and arrives at a "Verdict": **"Buy YES. Reason: Recent Federal Reserve statements, supported by strong economic indicators, indicate a definitive shift towards a hawkish monetary policy."** It assigns a `confidence_score` (e.g., 92%) and an `odds_delta` (e.g., +0.15, implying the market should be 15 percentage points higher). This complete "intelligence brief" is saved as a new Parquet file in `data/gold/intelligence_briefs/`.
8.  **API Layer Exposure**: The FastAPI backend (`api/main.py`) exposes this new intelligence brief via the `/api/intelligence` endpoint. When the frontend dashboard makes a request, DuckDB efficiently queries the `intelligence_briefs` directory, fetches the latest brief, and serves it.
9.  **Frontend Dashboard Display**: The React dashboard (`frontend/src/App.jsx`) receives the new brief. A "Mispricing Card" appears for the "US Federal Reserve" market. The card prominently displays:
    *   The full market title.
    *   The action button showing "Buy YES" (or "Sim YES" if in safe mode).
    *   The AI's verdict: "**Buy YES.** Reason: Recent Federal Reserve statements..."
    *   The calculated Kelly bet amount.
    The user can then interact with the card to simulate or execute a trade.
10. **Position Tracking and Exit Evaluation**: If the user (or automated system) acts on the signal, the trade is recorded. If it's a real trade, `inference/build_position_ledger.py` updates the `position_ledger.parquet` with the new entry and calculates the `WVWA` cost basis. The `inference/exit_evaluator.py` script then continuously monitors this position, ready to generate an "exit signal" if profit targets are met, stop-loss limits are hit, or the underlying thesis changes, which would then be displayed in the "Exit Signals Panel" on the frontend.

This complete cycle demonstrates PredictIQ's ability to ingest, transform, analyze, and present actionable intelligence in near real-time, closing the information lag in prediction markets.

---

## Chapter 14 — Known Issues, Limitations & Future Work

PredictIQ, while a robust and effective system for identifying mispriced prediction market opportunities, has inherent limitations and avenues for future development. Understanding these aspects is crucial for extending its capabilities and ensuring long-term success.

### 14.1 Current Gaps and Limitations

#### 14.1.1 Outcome Attribution
Currently, the system is adept at identifying potential trading opportunities and generating entry/exit signals. However, a significant gap exists in comprehensive **outcome attribution**. While the system tracks if a trade was profitable or not, it does not explicitly use LLMs to perform a post-mortem analysis comparing the initial intelligence brief's thesis against the actual market settlement outcome and subsequent real-world news. This limits the system's ability to:
*   **Learn from Mistakes**: Identify systematic biases or flaws in the LLM's reasoning or the RAG context.
*   **Refine Confidence Calibration**: Understand *why* a highly confident trade failed, leading to more accurate confidence scores in the future.
*   **Improve Prompt Engineering**: Pinpoint specific areas where the LLM's instructions could be refined for better decision-making.

#### 14.1.2 Portfolio-Level Risk Management
PredictIQ currently operates on a market-by-market basis, optimizing each individual trade based on the Kelly Criterion. It lacks a holistic, portfolio-level view of risk, correlation, and diversification. This means:
*   **Correlated Bets**: The system might recommend multiple "Buy YES" trades on highly correlated events (e.g., several markets related to a single political election), exposing the portfolio to concentrated risk if that single event unfolds unfavorably.
*   **Overall Portfolio Health**: There is no dedicated dashboard or mechanism to assess the total risk exposure, volatility, or diversification of the entire active portfolio.

#### 14.1.3 Event-Driven Triggering for Ingestion
Most ingestion scripts currently operate on fixed-time polling intervals. While this is effective, it introduces latency. A more advanced system would incorporate **event-driven triggers** (e.g., webhooks from news APIs, change data capture from Kalshi) to ingest information *immediately* as it becomes available, further reducing information lag.

### 14.2 Scalability Considerations

PredictIQ is designed for a single-server deployment. While PySpark's local mode, DuckDB, and ChromaDB's persistent client handle the current volume of ~50-100 monitored markets effectively, scaling to thousands of markets would require architectural adjustments:
*   **Distributed Spark Cluster**: Moving from local PySpark to a true distributed cluster (e.g., on Kubernetes or AWS EMR) would enhance processing power for large-scale transformations.
*   **Dedicated Metadata Store**: Replacing local file-based watermarks and ChromaDB's SQLite backend with a robust, scalable database (e.g., PostgreSQL, Elasticsearch) would improve metadata management and query performance under high load.
*   **Message Queues**: Introducing message queues (e.g., Kafka, RabbitMQ) between ingestion and transformation layers would decouple services, provide backpressure, and enable more flexible scaling of individual components.

### 14.3 Production Hardening Suggestions

#### 14.3.1 Robust Monitoring and Alerting
Beyond basic logging, a production system would integrate:
*   **Application Performance Monitoring (APM)**: Tools like Datadog or Prometheus/Grafana to track CPU, memory, API latencies, and container health.
*   **Custom Alerts**: Notifications for failed ETL jobs, missed ingestions, API rate limits, or significant deviations in mispricing scores.

#### 14.3.2 Secret Management
While `.env` is suitable for development, production deployments require more secure secret management solutions (e.g., Kubernetes Secrets, AWS Secrets Manager, HashiCorp Vault) to protect API keys and private keys.

#### 14.3.3 Data Quality Monitoring
Implementing automated data quality checks (e.g., Great Expectations, Deequ) at each medallion layer would proactively identify schema drifts, null value anomalies, or unexpected data patterns before they impact downstream intelligence.

---

## Appendix A — Full File/Directory Tree with One-Line Description of Every File

```
C:\Data Engineering\codeprep\predection_project
├───.gitignore                       - Specifies intentionally untracked files to ignore by Git.
├───.stignore                        - Specifies files/patterns to ignore for Syncthing synchronization.
├───create_objects_prod.sql          - SQL script for creating database objects in a production environment.
├───deploy.ps1                       - PowerShell script for deploying the PredictIQ system.
├───docker-compose.yml               - Defines and configures Docker services for the PredictIQ pipeline.
├───Dockerfile                       - Dockerfile for building the application images.
├───dockerize_target.py              - Python script for assisting with Dockerization tasks.
├───housekeeping.py                  - Script for routine cleanup and maintenance tasks.
├───MOBILE_DASHBOARD_GUIDE.md        - Markdown guide for using the mobile dashboard.
├───README_docker.txt                - Plain text README specific to Docker setup.
├───README-DOCKER.md                 - Markdown README specific to Docker setup.
├───README.md                        - Main project README file.
├───requirements.txt                 - Python package dependencies for Docker containers.
├───start_predictiq_pipeline.bat     - Windows batch script to start the PredictIQ pipeline.
├───test_scrapers.py                 - Python script for testing web scraping functionality.
├───.agent\                          - Directory for agent-related configurations and skills.
│   ├───skills\                      - Contains definitions for custom agent skills.
│   │   ├───brainstorm\              - Brainstorming skill definition.
│   │   │   └───SKILL.md             - Markdown documentation for the brainstorm skill.
│   │   ├───data_engineer_pipeline_expert\ - Data engineer pipeline expert skill.
│   │   │   └───SKILL.md             - Markdown documentation for the data engineer skill.
│   │   ├───debugger\                - Debugger skill definition.
│   │   │   └───SKILL.md             - Markdown documentation for the debugger skill.
│   │   ├───kalshi-api-debug\        - Kalshi API debugging skill.
│   │   │   └───SKILL.md             - Markdown documentation for the Kalshi API debug skill.
│   │   ├───project-goal-validator\  - Project goal validator skill.
│   │   │   └───SKILL.md             - Markdown documentation for the project goal validator skill.
│   │   └───state-audit\             - State audit skill definition.
│   │       └───SKILL.md             - Markdown documentation for the state audit skill.
│   └───workflows\                   - Contains agent workflow definitions.
│       └───pre-response-checklist.md - Pre-response checklist for agent interactions.
├───.claude\                         - Directory for Claude AI specific configurations (similar to .agent).
│   ├───settings.local.json          - Local settings for Claude AI.
│   ├───skills\                      - Contains skill definitions for Claude AI.
│   │   ├───brainstorm\              - Brainstorming skill for Claude.
│   │   │   └───SKILL.md             - Markdown documentation for Claude's brainstorm skill.
│   │   ├───data_engineer_pipeline_expert\ - Data engineer pipeline expert skill for Claude.
│   │   │   └───SKILL.md             - Markdown documentation for Claude's data engineer skill.
│   │   ├───debugger\                - Debugger skill for Claude.
│   │   │   └───SKILL.md             - Markdown documentation for Claude's debugger skill.
│   │   ├───kalshi-api-debug\        - Kalshi API debugging skill for Claude.
│   │   │   └───SKILL.md             - Markdown documentation for Claude's Kalshi API debug skill.
│   │   ├───project-goal-validator\  - Project goal validator skill for Claude.
│   │   │   └───SKILL.md             - Markdown documentation for Claude's project goal validator skill.
│   │   └───state-audit\             - State audit skill for Claude.
│   │       └───SKILL.md             - Markdown documentation for Claude's state audit skill.
│   └───workflows\                   - Contains Claude AI workflow definitions.
│       └───pre-response-checklist.md - Pre-response checklist for Claude interactions.
├───.git\...                         - Git repository metadata (hidden).
├───.stfolder\                       - Syncthing folder metadata (hidden).
│   └───syncthing-folder-66be52.txt  - Syncthing synchronization marker file.
├───.venv\...                        - Python virtual environment (hidden).
├───#\                               - Temporary or scratch directory (unspecified content).
│   ├───Include\...                  - Python include files for virtual environment.
│   ├───Lib\...                      - Python library files for virtual environment.
│   └───Scripts\...                  - Python scripts for virtual environment.
├───api\                             - Contains the FastAPI backend application.
│   ├───kelly_math.py                - Implements Kelly Criterion for optimal bet sizing.
│   ├───main.py                      - Main FastAPI application with all API endpoints.
│   └───__pycache__\...              - Python bytecode cache.
├───artifacts\                       - Directory for generated artifacts, documentation, etc.
├───data\                            - Main data storage directory for the pipeline.
│   ├───bronze\                      - Raw, untransformed data from ingestion sources.
│   ├───gold\                        - Curated, aggregated data for LLM inference and frontend.
│   ├───silver\                      - Cleaned, transformed, and normalized data.
│   ├───chroma\                      - ChromaDB vector store files.
│   └───.etl_watermark               - File to track the last successful ETL run timestamp.
├───database\                        - Placeholder or unused directory for database definitions.
├───docs\                            - Project documentation directory.
│   └───.gitkeep                     - Placeholder file to keep the empty directory in Git.
├───frontend\                        - Contains the React-based frontend dashboard.
│   ├───.gitignore                   - Git ignore rules specific to the frontend project.
│   ├───eslint.config.js             - ESLint configuration for the frontend.
│   ├───index.html                   - Main HTML file for the frontend application.
│   ├───package-lock.json            - Records the exact dependency tree for npm.
│   ├───package.json                 - Defines frontend project metadata and dependencies.
│   ├───README.md                    - Frontend-specific README.
│   ├───vite.config.js               - Vite build tool configuration.
│   ├───dist\...                     - Frontend production build output.
│   ├───node_modules\...             - Frontend JavaScript dependencies.
│   ├───public\                      - Static assets served by the frontend.
│   │   ├───favicon.svg              - Favicon for the web application.
│   │   └───icons.svg                - SVG icons used in the frontend.
│   └───src\                         - Frontend source code.
│       ├───App.css                  - Main CSS for the React application.
│       ├───App.jsx                  - Main React component for the dashboard layout.
│       ├───index.css                - Global CSS styles and design tokens.
│       ├───main.jsx                 - Entry point for the React application.
│       ├───assets\                  - Static image assets.
│       │   ├───hero.png             - Hero image.
│       │   ├───react.svg            - React logo.
│       │   └───vite.svg             - Vite logo.
│       └───components\              - Reusable React components.
│           ├───ExitPanel.jsx        - Component for displaying exit recommendations.
│           └───MispricingCard.jsx   - Component for displaying individual intelligence briefs.
├───inference\                       - Contains scripts for AI inference and intelligence generation.
│   ├───build_position_ledger.py     - Builds and maintains the position ledger.
│   ├───exit_evaluator.py            - Generates exit signals for open positions.
│   ├───explain_mispricing.py        - Script to explain mispricing scores.
│   └───predict_movements.py         - Core script for generating predictive intelligence briefs.
├───ingestion\                       - Contains scripts for ingesting data from various sources.
│   ├───bbc_ingest.py                - Ingests news from BBC RSS feed.
│   ├───check_parquet.py             - Utility to check Parquet file integrity.
│   ├───cnn_ingest.py                - Ingests news from CNN RSS feed.
│   ├───debug_gold_nulls.py          - Script for debugging null values in Gold layer.
│   ├───fetch_kalshi_json.py         - Fetches raw JSON data from Kalshi API.
│   ├───fox_ingest.py                - Ingests news from Fox News RSS feed.
│   ├───gdelt_events_ingest.py       - Ingests GDELT Global Event Data (GEV).
│   ├───gdelt_gkg_ingest.py          - Ingests GDELT Global Knowledge Graph (GKG) data.
│   ├───hindu_ingest.py              - Ingests news from The Hindu RSS feed.
│   ├───kalshi_daily_settlement-prd.py - Production script for Kalshi daily settlement.
│   ├───kalshi_daily_settlement.py   - Kalshi daily settlement script.
│   ├───kalshi_markets_active.py     - Ingests active Kalshi market data.
│   ├───kalshi_markets_historical.py - Ingests historical Kalshi market data.
│   ├───nypost_ingest.py             - Ingests news from NY Post RSS feed.
│   ├───nyt_news_ingest.py           - Ingests news from NYT RSS feed.
│   ├───TDB_flat_kalshi_candlesticks.py - Placeholder for candlestick data ingestion.
│   ├───test_kalshi_auth.py          - Tests Kalshi API authentication.
│   ├───test_ws_auth.py              - Tests WebSocket authentication.
│   ├───test_ws_payload.py           - Tests WebSocket payload handling.
│   └───__pycache__\...              - Python bytecode cache.
├───ingestion_text\...               - Directory for text-based ingestion utilities or data.
├───native_ingestion\                - Alternative ingestion scripts (potentially non-Spark).
│   ├───flat_bbc_ingest.py           - Flat file ingestion for BBC news.
│   ├───flat_cnn_ingest.py           - Flat file ingestion for CNN news.
│   ├───flat_fox_ingest.py           - Flat file ingestion for Fox News.
│   ├───flat_gdelt_events_ingest.py  - Flat file ingestion for GDELT events.
│   ├───flat_gdelt_gkg_ingest.py     - Flat file ingestion for GDELT GKG.
│   ├───flat_hindu_ingest.py         - Flat file ingestion for The Hindu news.
│   ├───flat_kalshi_daily_settlement.py - Flat file ingestion for Kalshi daily settlement.
│   ├───flat_kalshi_markets_active.py - Flat file ingestion for active Kalshi markets.
│   ├───flat_kalshi_markets_historical.py - Flat file ingestion for historical Kalshi markets.
│   ├───flat_nypost_ingest.py        - Flat file ingestion for NY Post news.
│   ├───flat_nyt_news_ingest.py      - Flat file ingestion for NYT news.
│   └───TDB_flat_kalshi_candlesticks.py - Flat file ingestion placeholder for candlesticks.
├───orchestration\                   - Scripts for orchestrating the ETL pipeline.
│   ├───run_etl.py                   - Master ETL scheduler and orchestrator.
│   └───spark_pipeline.py            - Unified PySpark pipeline for Silver and Gold transforms.
├───rag\                             - Scripts for Retrieval-Augmented Generation (RAG).
│   ├───embed_silver_data.py         - Embeds Silver data and syncs with ChromaDB.
│   └───inspect_chroma.py            - Utility to inspect ChromaDB contents.
├───recreate\                        - Directory potentially for recreating environments or data.
│   ├───Include\...                  - Python include files for virtual environment.
│   ├───Lib\...                      - Python library files for virtual environment.
│   └───Scripts\...                  - Python scripts for virtual environment.
├───reference\                       - Contains reference data (e.g., CSV lookups for GDELT).
│   ├───cameo_actortypes.csv         - CAMEO actor type codes.
│   ├───cameo_eventcodes.csv         - CAMEO event codes.
│   ├───cameo_quadclass.csv          - CAMEO event quad classification.
│   └───fips_countries.csv           - FIPS country codes.
├───research\                        - Directory for research and experimentation scripts.
│   ├───backtest_engine.py           - Script for the backtesting engine.
│   ├───backtest_results_20260425_191935.csv - Example backtest result file.
│   ├───backtest_results_20260425_194633.csv - Example backtest result file.
│   ├───backtest_results_20260425_200933.csv - Example backtest result file.
│   └───backtest_results_20260425_201458.csv - Example backtest result file.
├───scratch\...                      - Temporary scratchpad directory.
└───transformation\                  - Scripts for transforming data between medallion layers.
    ├───gold_gdelt_summaries_transform.py - Transforms Silver GDELT to Gold summaries.
    ├───gold_market_summaries_transform.py - Transforms Silver to Gold market mispricing scores.
    ├───gold_news_summaries_transform.py - Transforms Silver news to Gold summaries.
    ├───silver_gdelt_events_transform.py - Transforms Bronze GDELT events to Silver.
    ├───silver_gdelt_gkg_transform.py  - Transforms Bronze GDELT GKG to Silver.
    ├───silver_kalshi_transform.py     - Transforms Bronze Kalshi data to Silver.
    ├───silver_news_transform.py       - Transforms Bronze news to Silver enriched articles.
    └───__pycache__\...                - Python bytecode cache.
```

## Appendix B — All Environment Variables and Their Purpose

| Variable Name         | Purpose                                                                                                                                                                                                                                                                  | Example Value                                                                                                          |
| :-------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------- |
| `KALSHI_API_KEY`      | Your public Kalshi API key, used to authenticate requests to the Kalshi trading API.                                                                                                                                                                                     | `pk_live_xxxxxxxxxxxxxxxxxxxxxxxx`                                                                                     |
| `KALSHI_API_SECRET`   | Your Kalshi API private key, typically a multi-line PEM-encoded RSA key, used for signing requests. **Crucial for security.**                                                                                                                                          | `-----BEGIN RSA PRIVATE KEY-----
...
-----END RSA PRIVATE KEY-----`                                          |
| `GROQ_API_KEY`        | API key for Groq, used to access their LLM inference services (e.g., Llama 3.3).                                                                                                                                                                                         | `gsk_xxxxxxxxxxxxxxxxxxxxxxxx`                                                                                         |
| `OPENAI_API_KEY`      | API key for OpenAI, used for text embedding models (e.g., `text-embedding-3-small`) in the RAG pipeline.                                                                                                                                                               | `sk-xxxxxxxxxxxxxxxxxxxxxxxx`                                                                                          |
| `SAFE_MODE`           | Boolean flag (true/false) that enables or disables live trading. When `true`, trade requests are simulated and logged to a local CSV instead of being sent to Kalshi.                                                                                                    | `true` or `false`                                                                                                      |
| `BANKROLL_USD`        | The total hypothetical bankroll in USD for Kelly Criterion calculations. Used to determine optimal bet sizes.                                                                                                                                                            | `1000.0`                                                                                                               |
| `IVY_PACKAGE_DIR`     | (Optional) Specifies a directory for Ivy to store downloaded Spark/Delta dependencies. Useful for controlling cached JARs in Docker.                                                                                                                                       | `/usr/src/app/ivy_cache` (inside Docker)                                                                               |
| `PYSPARK_PYTHON`      | Explicitly sets the Python executable path for PySpark worker processes. Essential for Windows compatibility.                                                                                                                                                            | `C:/DATAEN~1/codeprep/PREDEC~1/.venv/Scripts/python.exe` (Windows Short Path)                                          |
| `PYSPARK_DRIVER_PYTHON` | Explicitly sets the Python executable path for the PySpark driver process. Essential for Windows compatibility.                                                                                                                                                        | `C:/DATAEN~1/codeprep/PREDEC~1/.venv/Scripts/python.exe` (Windows Short Path)                                          |
| `SPARK_LOCAL_IP`      | Sets the local IP address for Spark to bind to. A common fix for `BlockManager` issues on Windows.                                                                                                                                                                     | `127.0.0.1`                                                                                                            |
| `HADOOP_HOME`         | (Optional, Windows specific) Path to Hadoop installation directory. Used to add Hadoop binaries to PATH for Spark.                                                                                                                                                       | `C:\hadoop-3.3.0`                                                                                                      |
| `PYTHONUNBUFFERED`    | Ensures Python output is unbuffered, which is useful in containerized environments for real-time log streaming.                                                                                                                                                        | `1`                                                                                                                    |
| `KALSHI_BASE_URL`     | Base URL for the Kalshi trade API.                                                                                                                                                                                                                                     | `https://api.elections.kalshi.com/trade-api/v2`                                                                        |
| `POLL_INTERVAL`       | The interval (in seconds) between ETL cycles in `run_etl.py`.                                                                                                                                                                                                          | `300` (5 minutes)                                                                                                      |
| `CHROMA_PATH`         | The filesystem path where ChromaDB stores its database files.                                                                                                                                                                                                          | `C:/Data Engineering/codeprep/predection_project/data/chroma`                                                          |
| `SIMILARITY_FLOOR`    | The minimum cosine similarity score required for a document to be considered relevant during RAG retrieval.                                                                                                                                                              | `0.50`                                                                                                                 |
| `GROQ_MODEL`          | The specific Groq LLM model to use for intelligence synthesis.                                                                                                                                                                                                           | `llama-3.3-70b-versatile`                                                                                              |
| `OPENAI_EMBED_MODEL`  | The specific OpenAI embedding model to use for news articles.                                                                                                                                                                                                            | `text-embedding-3-small`                                                                                               |
| `GDELT_EMBED_MODEL`   | The specific local Sentence-Transformer model to use for GDELT data.                                                                                                                                                                                                   | `all-MiniLM-L6-v2`                                                                                                     |
| `STOP_LOSS_THRESHOLD` | Threshold for unrealized P&L (as a negative decimal) at which an exit signal for "SELL_LOSS" is triggered.                                                                                                                                                             | `0.05` (5% loss)                                                                                                       |
| `TIME_DECAY_HOURS`    | Number of hours before market close where a "SELL_TIMEOUT" exit signal might trigger due to no momentum.                                                                                                                                                               | `24`                                                                                                                   |
| `TIME_DECAY_PNL_BAND` | P&L band (absolute value) within which "SELL_TIMEOUT" can trigger. If P&L is outside this, another trigger might be more relevant.                                                                                                                                       | `0.02` (within +/- 2 cents)                                                                                            |
| `MIN_EDGE`            | Minimum percentage points the LLM's confidence must exceed the market's implied probability for a signal to be considered actionable. Guards against noisy signals.                                                                                                     | `0.10` (10 percentage points)                                                                                          |


## Appendix C — Docker Compose Service Definitions Summarized

The `docker-compose.yml` file orchestrates the entire PredictIQ pipeline using 11 containerized services. Each service is built from the same `Dockerfile` and mounts the entire project directory (`.:/app`) into the container, allowing for shared code and data volumes.

| Service Name             | Image/Build | Command                                        | Role & Purpose                                                                                                                                                                                                                                                                                                                          |
| :----------------------- | :---------- | :--------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark-app`              | `.` (build) | `python3 -u orchestration/run_etl.py`          | **ETL Orchestrator:** The central brain of the data pipeline. It sequentially triggers all Silver and Gold transformations, vector synchronization, and AI inference. It manages watermarks and ensures proper execution order. This service wraps the `spark_pipeline.py` which runs multiple PySpark jobs in a single JVM.                 |
| `ingest-bbc`             | `.` (build) | `python3 -u ingestion/bbc_ingest.py`           | **BBC News Ingestor:** Continuously polls the BBC RSS feed, scrapes full article text, and writes raw data to the Bronze layer (`data/bronze/bbc/`).                                                                                                                                                                             |
| `ingest-cnn`             | `.` (build) | `python3 -u ingestion/cnn_ingest.py`           | **CNN News Ingestor:** Continuously polls the CNN RSS feed, scrapes full article text, and writes raw data to the Bronze layer (`data/bronze/cnn/`).                                                                                                                                                                             |
| `ingest-fox`             | `.` (build) | `python3 -u ingestion/fox_ingest.py`           | **Fox News Ingestor:** Continuously polls the Fox News RSS feed, scrapes full article text, and writes raw data to the Bronze layer (`data/bronze/foxnews/`).                                                                                                                                                                        |
| `ingest-hindu`           | `.` (build) | `python3 -u ingestion/hindu_ingest.py`         | **The Hindu News Ingestor:** Continuously polls The Hindu RSS feed, scrapes full article text, and writes raw data to the Bronze layer (`data/bronze/hindu/`).                                                                                                                                                                       |
| `ingest-nypost`          | `.` (build) | `python3 -u ingestion/nypost_ingest.py`        | **NY Post News Ingestor:** Continuously polls the NY Post RSS feed, scrapes full article text, and writes raw data to the Bronze layer (`data/bronze/nypost/`).                                                                                                                                                                      |
| `ingest-nyt`             | `.` (build) | `python3 -u ingestion/nyt_news_ingest.py`      | **NYT News Ingestor:** Continuously polls the New York Times RSS feed, scrapes full article text, and writes raw data to the Bronze layer (`data/bronze/nyt/`).                                                                                                                                                                        |
| `ingest-gdelt-events`    | `.` (build) | `python3 -u ingestion/gdelt_events_ingest.py`  | **GDELT Event Ingestor:** Downloads and processes GDELT Global Event Data (GEV) files, converting them into Parquet and storing them in the Bronze layer (`data/bronze/gdelt/gdelt_events/`). Provides structured event context.                                                                                                |
| `ingest-gdelt-gkg`       | `.` (build) | `python3 -u ingestion/gdelt_gkg_ingest.py`     | **GDELT Global Knowledge Graph (GKG) Ingestor:** Downloads and processes GDELT GKG files, converting them into Parquet and storing them in the Bronze layer (`data/bronze/gdelt/gdelt_gkg/`). Focuses on entities, themes, and sentiment from news.                                                                               |
| `ingest-kalshi-active`   | `.` (build) | `python3 -u ingestion/kalshi_markets_active.py`| **Kalshi Active Markets Ingestor:** Polls the Kalshi API for real-time data on all active prediction markets, including odds, volume, and contract details. This is crucial for tracking live market state and identifying mispricings. It uses RSA authentication to secure API calls.                                                     |
| `restart: unless-stopped`| All         | N/A                                            | **Restart Policy:** Ensures that if any container stops for any reason (e.g., script error, temporary resource issue), Docker will attempt to restart it automatically. This enhances system resilience.                                                                                                                               |
| `env_file: - .env`       | `spark-app`, `ingest-kalshi-active` | N/A                                            | **Environment Variables:** Specifies that these services should load environment variables from the `.env` file at the project root. This is used for API keys, secrets, and configuration flags like `SAFE_MODE`.                                                                                                                               |
| `volumes: - .:/app`      | All         | N/A                                            | **Volume Mount:** Mounts the entire current project directory (`.`) into the `/app` directory inside each container. This allows containers to access the codebase, configuration files, and persist data to the host's `data/` directory, maintaining state across container restarts.                                                           |


## Appendix D — Glossary of Terms

*   **Adaptive Query Execution (AQE)**: A Spark 3.x feature that optimizes query plans at runtime based on actual data characteristics (e.g., dynamically coalescing partitions).
*   **Bronze Layer**: The lowest level of the Medallion Architecture, containing raw, immutable data exactly as ingested from source systems.
*   **CAMEO (Conflict and Mediation Event Observations)**: A coding scheme used by GDELT to classify socio-political events and their actors.
*   **ChromaDB**: A lightweight, open-source vector database used to store embeddings and enable semantic search (RAG).
*   **Cosine Similarity**: A metric used to measure how similar two non-zero vectors are. Often used in RAG to find semantically related documents.
*   **Delta Lake**: An open-source storage layer that brings ACID (Atomicity, Consistency, Isolation, Durability) transactions, scalable metadata handling, and unified batch/streaming data processing to Apache Spark and other engines (like DuckDB).
*   **DuckDB**: An in-process SQL OLAP database management system that can query Parquet, CSV, and Delta Lake files directly and efficiently. Used by PredictIQ for fast analytical queries in the API layer.
*   **ETL (Extract, Transform, Load)**: A three-step data integration process: extract data from sources, transform it into a usable format, and load it into a target system.
*   **FastAPI**: A modern, fast (high-performance) web framework for building APIs with Python 3.7+ based on standard Python type hints.
*   **GDELT (Global Database of Events, Language, and Tone)**: A real-time data stream of human society, monitoring news media from nearly every corner of the world. Provides structured data on events (GEV) and entities (GKG).
*   **Gold Layer**: The highest level of the Medallion Architecture, containing aggregated, refined, and business-ready data optimized for specific applications (e.g., LLM inference, dashboards).
*   **JSON Bridge**: A specific architectural pattern implemented in PredictIQ to enable stable PySpark transformations on Windows by collecting data to the driver as Pandas, processing in pure Python, writing to temp JSON, and then re-ingesting with Spark.
*   **Kalshi**: A regulated prediction market exchange in the United States, where users can trade on the outcome of future events.
*   **Kelly Criterion**: A formula used to determine the optimal size of a series of bets to maximize the long-term growth rate of a bankroll. Used in PredictIQ to suggest trade sizes.
*   **Large Language Model (LLM)**: An AI model (e.g., Llama 3.3) capable of understanding, generating, and processing human language. Used in PredictIQ for synthesizing intelligence briefs.
*   **Medallion Architecture**: A data architecture pattern that organizes data into multiple layers (Bronze, Silver, Gold) based on quality and structure, ensuring data integrity throughout the pipeline.
*   **Mispricing Score**: A proprietary score (0-100) calculated by PredictIQ's Gold layer, indicating the degree to which a prediction market contract is mispriced relative to current news and sentiment.
*   **Pandas**: A Python library providing high-performance, easy-to-use data structures and data analysis tools.
*   **Parquet**: A columnar storage file format optimized for efficiency in big data processing.
*   **Prediction Market**: An exchange where participants buy and sell contracts whose payoffs are linked to the occurrence of future events.
*   **PySpark**: The Python API for Apache Spark, providing a high-level interface for distributed data processing.
*   **Retrieval-Augmented Generation (RAG)**: An AI framework that enhances the accuracy and relevance of LLM outputs by retrieving factual information from external knowledge bases (like ChromaDB) before generating a response.
*   **RSA Authentication**: A cryptographic method using a pair of keys (public and private) to secure digital communications, used by PredictIQ for Kalshi API interactions.
*   **SAFE_MODE**: A safety feature in PredictIQ that simulates trades and logs them locally without making actual transactions on the Kalshi exchange.
*   **Sentiment Analysis**: The process of computationally identifying and categorizing opinions expressed in a piece of text, especially in order to determine whether the writer's attitude towards a particular topic is positive, negative, or neutral. VADER is a common lexicon and rule-based sentiment analysis tool.
*   **Sentence-Transformers**: A Python framework for state-of-the-art sentence, text, and image embeddings. Used in PredictIQ for embedding GDELT data locally.
*   **Silver Layer**: The second layer of the Medallion Architecture, where raw data from Bronze is cleaned, transformed, and often enriched, becoming a reliable source for downstream processes.
*   **SparkSession**: The entry point to programming Spark with the Dataset and DataFrame API. It's the central object used to create DataFrames, register DataFrames as tables, execute SQL, and read Parquet files.
*   **Syncthing**: A free, open-source peer-to-peer file synchronization application. Used in PredictIQ to keep development and production codebases and data directories in sync.
*   **Tailscale**: A zero-config VPN that creates a secure network between your devices, wherever they are. Used by PredictIQ for secure access to the dashboard and API from remote locations.
*   **Trafilatura**: A Python library for extracting main content, comments, and metadata from web pages. Used in PredictIQ's news ingestors.
*   **Unified Spark Session**: An optimization technique in PredictIQ where all multiple PySpark transformations (Silver and Gold) are executed within a single, long-lived SparkSession to reduce JVM startup overhead.
*   **VADER (Valence Aware Dictionary and sEntiment Reasoner)**: A lexicon and rule-based sentiment analysis tool that is specifically attuned to sentiments expressed in social media, and works well on general sentiment tasks too.
*   **Vite**: A next-generation frontend tooling that provides an extremely fast development experience for modern web projects.
*   **WVWA (Weight-Volume Weighted Average)**: A method for calculating the average price of a position, weighted by the volume (number of contracts) traded at each price. Used for accurate P&L calculation.
