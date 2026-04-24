# PredictIQ — Political & Economic Intelligence Lakehouse

PredictIQ is a full-stack data engineering and AI platform that ingests multi-source political and economic data, refines it through a Medallion Lakehouse architecture, and uses LLM-powered inference to detect **mispriced prediction markets** on [Kalshi](https://kalshi.com). A React-based Human-in-the-Loop (HIL) trade terminal surfaces AI intelligence briefs with Kelly Criterion bet-sizing for final human approval.

---

## Architecture Overview

```
                          ┌───────────────────────────────────────────┐
                          │            BRONZE (Raw Ingest)            │
                          │  Kalshi · GDELT Events · GDELT GKG       │
                          │  BBC · CNN · Fox · NYT · NYPost · Hindu  │
                          └──────────────────┬────────────────────────┘
                                             │ PySpark + Delta Lake
                          ┌──────────────────▼────────────────────────┐
                          │      SILVER (Cleaned + Deduplicated)      │
                          │  kalshi_markets · news_articles_enriched  │
                          │  gdelt_events · gdelt_gkg (current/hist) │
                          └──────────┬───────────────┬────────────────┘
                                     │               │
                     ┌───────────────▼──┐   ┌────────▼───────────────┐
                     │   GOLD (Agg.)    │   │  ChromaDB (Vectors)    │
                     │  Market Scores   │   │  silver_news_enriched  │
                     │  GDELT Summaries │   │  silver_gdelt_enriched │
                     │  News Summaries  │   └────────┬───────────────┘
                     └────────┬─────────┘            │
                              │          ┌───────────▼───────────────┐
                              │          │  LLM INFERENCE ENGINE     │
                              │          │  Groq (Llama 3.3 70B)    │
                              │          │  Cascading RAG Search     │
                              │          │  Ghost Pump Detection     │
                              │          └───────────┬───────────────┘
                              │                      │
                              └──────────┬───────────┘
                          ┌──────────────▼────────────────────────────┐
                          │  GOLD: Intelligence Briefs (Parquet)      │
                          │  bull_case · bear_case · verdict          │
                          │  confidence_score · kelly_sizing          │
                          └──────────────────┬────────────────────────┘
                                             │
                          ┌──────────────────▼────────────────────────┐
                          │  FastAPI Bridge  →  React HIL Terminal    │
                          │  /api/intelligence · /api/trade           │
                          │  Safe Mode guard · Kelly Criterion        │
                          └───────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Storage** | DuckDB + Apache Parquet | OLAP queries on local SSD, Bronze raw storage |
| **Lakehouse** | Delta Lake (`delta_scan()`) | ACID transactions for Silver/Gold layers |
| **Processing** | PySpark 3.5.1 | Silver & Gold transformations |
| **Orchestration** | Custom ETL Orchestrator | 5-minute polling loop, watermark-based change detection |
| **Vector Store** | ChromaDB + `all-MiniLM-L6-v2` | Semantic search over news & GDELT for RAG |
| **Inference** | Groq API (Llama 3.3 70B) | Bull/bear/verdict synthesis per market |
| **API** | FastAPI | Serves intelligence briefs, proxies Kalshi trades |
| **Frontend** | React + Vite | HIL trade terminal with mispricing cards |
| **Auth** | RSA-signed Kalshi API (PKCS1v15) | Cryptographic request signing |
| **Deployment** | Docker Compose (10 containers) | Prod runs on headless Windows server over Tailscale |
| **Sync** | Syncthing + `deploy.ps1` | Automated data sync + manual code deployment |
| **Inspection** | DBeaver | Local Parquet/DuckDB visual inspection |

---

## Project Structure

```
PredictIQ/
├── native_ingestion/          # Flat Python ingestion scripts (dev/local)
│   ├── flat_kalshi_markets_active.py
│   ├── flat_kalshi_markets_historical.py
│   ├── flat_kalshi_daily_settlement.py
│   ├── flat_gdelt_events_ingest.py
│   ├── flat_gdelt_gkg_ingest.py
│   ├── flat_bbc_ingest.py
│   ├── flat_cnn_ingest.py
│   ├── flat_fox_ingest.py
│   ├── flat_nyt_news_ingest.py
│   ├── flat_nypost_ingest.py
│   └── flat_hindu_ingest.py
│
├── ingestion/                 # Docker-deployed ingestion scripts (prod)
│   ├── kalshi_markets_active.py
│   ├── kalshi_daily_settlement.py
│   ├── gdelt_events_ingest.py / gdelt_gkg_ingest.py
│   ├── bbc_ingest.py / cnn_ingest.py / fox_ingest.py
│   ├── nyt_news_ingest.py / nypost_ingest.py / hindu_ingest.py
│   └── test_kalshi_auth.py
│
├── transformation/            # PySpark Silver & Gold transforms
│   ├── silver_kalshi_transform.py
│   ├── silver_news_transform.py
│   ├── silver_gdelt_events_transform.py
│   ├── silver_gdelt_gkg_transform.py
│   ├── gold_market_summaries_transform.py
│   ├── gold_news_summaries_transform.py
│   └── gold_gdelt_summaries_transform.py
│
├── rag/                       # Vector Bridge (Silver → ChromaDB)
│   ├── embed_silver_data.py
│   └── inspect_chroma.py
│
├── inference/                 # LLM-powered mispricing detection
│   └── explain_mispricing.py
│
├── api/                       # FastAPI backend
│   ├── main.py
│   └── kelly_math.py
│
├── frontend/                  # React + Vite HIL trade terminal
│   └── src/
│       ├── App.jsx
│       └── components/
│           └── MispricingCard.jsx
│
├── orchestration/             # ETL scheduler
│   └── run_etl.py
│
├── database/                  # DuckDB persistent store
│   └── predictiq.duckdb
│
├── data/                      # Local Lakehouse (git-ignored)
│   ├── bronze/                #   Raw Parquet snapshots
│   ├── silver/                #   Delta Lake tables
│   ├── gold/                  #   Aggregated summaries + intelligence briefs
│   └── .etl_watermark         #   Change-detection marker
│
├── create_objects.sql         # DuckDB view definitions (Bronze/Silver/Gold)
├── docker-compose.yml         # 10-service production deployment
├── Dockerfile                 # Apache Spark 3.5.1 base image
├── deploy.ps1                 # Whitelist-sync deployment script
├── requirements.txt           # Python dependencies
├── .env                       # API keys (never committed)
└── .agent/skills/             # Custom AI expert instructions
```

---

## Data Ingestion (Bronze Layer)

The ingestion suite runs as 9 independent Docker containers, each on an `unless-stopped` restart policy.

### Kalshi Markets (Market Conviction)
- **Active Markets**: Surgical series-first discovery via `/series` endpoint, filtering to political & economic categories. Reduces API calls by ~98% vs full catalog scan.
- **Daily Settlement**: Once-per-day sweep of settled/closed markets with infinite-loop guard and lock-file coordination to avoid API rate-limit collisions with the active ingestor.
- **Historical Backfill**: One-time bulk import of all settled political history.

### GDELT (Global Signal)
- **Events**: Geopolitical event records filtered by CAMEO codes for conflict, diplomacy, and economic activity.
- **GKG (Global Knowledge Graph)**: Theme and entity extraction from global media coverage.

### News Sentiment (Narrative Context)
Six independent scrapers covering the political spectrum:
**BBC** · **CNN** · **Fox News** · **NYT** · **NY Post** · **The Hindu**

Each scraper uses `trafilatura` for full-text extraction, `vaderSentiment` for polarity scoring, and `feedparser` for RSS discovery. Articles are stored as Bronze Parquet with `ingested_at` timestamps.

---

## Transformation Pipeline (Silver & Gold)

Orchestrated by `run_etl.py` — a sequential batch controller polling every 5 minutes. Runs inside a single PySpark JVM (4 GB driver memory cap).

### Silver Layer (Delta Lake)
| Transform | Output |
|-----------|--------|
| `silver_kalshi_transform` | Deduplicated market snapshots with current/history split |
| `silver_news_transform` | Enriched articles with VADER sentiment + source metadata |
| `silver_gdelt_events_transform` | Cleaned events with current/history partitioning |
| `silver_gdelt_gkg_transform` | Parsed GKG themes, persons, organizations |

### Gold Layer (Delta Lake)
| Transform | Output |
|-----------|--------|
| `gold_market_summaries_transform` | Mispricing scores: odds delta, volume anomalies |
| `gold_news_summaries_transform` | Source-level sentiment aggregations |
| `gold_gdelt_summaries_transform` | Geopolitical tension indices |

---

## RAG Pipeline (Vector Bridge)

`rag/embed_silver_data.py` reads Silver Delta tables and embeds them into **ChromaDB** using `all-MiniLM-L6-v2` sentence embeddings:

- **`silver_news_enriched`** collection — headline + full-text chunks
- **`silver_gdelt_enriched`** collection — event/GKG descriptors

Documents are keyed by `ingested_timestamp` for time-windowed retrieval during inference.

---

## Inference Engine (LLM Intelligence)

`inference/explain_mispricing.py` detects volatile markets and synthesizes intelligence briefs:

1. **Gatekeeper**: Identifies markets with odds movement exceeding a configurable threshold (default: 10%).
2. **Cascading RAG Search**: Queries ChromaDB with a 15-minute flash window, falling back to a 2-hour echo window. Uses L2→similarity conversion with a configurable floor (default: 45%).
3. **Ghost Pump Detection**: Markets with significant price movement but zero corroborating news are flagged as behavioral distortions.
4. **Bookmaker LLM**: Sends market context + RAG evidence to **Groq (Llama 3.3 70B)** for structured `{bull_case, bear_case, verdict}` synthesis.
5. **Gold Output**: Briefs are written to `data/gold/intelligence_briefs/` as timestamped Parquet files.

---

## API & Frontend (HIL Terminal)

### FastAPI Backend (`api/main.py`)
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/intelligence` | GET | Latest deduplicated briefs with Kelly Criterion sizing |
| `/api/market/{ticker}` | GET | Live Kalshi odds (RSA-signed proxy) |
| `/api/trade` | POST | Submit trade to Kalshi (guarded by Safe Mode) |
| `/api/health` | GET | Health check with Safe Mode status |

### React Frontend (`frontend/`)
- Built with **Vite** and served on `localhost:5173`
- **MispricingCard** components display: ticker, verdict, bull/bear cases, confidence score, Kelly bet size, and a one-click trade button
- **Safe Mode** (default: ON) logs trade intent without executing — set `SAFE_MODE=false` in `.env` for live trading

---

## Deployment

### Production (Docker Compose)
```bash
docker compose up -d --build
```
Starts 10 containers:
- 1x Spark ETL Orchestrator
- 9x Bronze ingestors (Kalshi, GDELT Events, GDELT GKG, BBC, CNN, Fox, Hindu, NYPost, NYT)

### Local Development
```bash
# Run any native ingestion script directly
python native_ingestion/flat_kalshi_markets_active.py

# Start the API server
uvicorn api.main:app --reload

# Start the React frontend
cd frontend && npm run dev
```

### Prod Deployment (Code Sync)
```powershell
# Whitelist-sync to prod server over Tailscale
.\deploy.ps1
```
Only syncs `ingestion/`, `transformation/`, `orchestration/`, and root config files. Never touches `data/`, `.venv/`, or logs.

---

## Quick Start

1. **Clone**: `git clone <repo-url> && cd predection_project`
2. **Environment**: Create `.env` with your keys:
   ```
   KALSHI_API_KEY=<your-key>
   KALSHI_API_SECRET=<your-rsa-private-key>
   GROQ_API_KEY=<your-groq-key>
   NEWS_API_KEY=<your-newsapi-key>
   SAFE_MODE=true
   BANKROLL_USD=1000
   ```
3. **Dependencies**: `pip install -r requirements.txt`
4. **Backfill History**: `python native_ingestion/flat_kalshi_markets_historical.py`
5. **Start Ingestion**: `python native_ingestion/flat_kalshi_markets_active.py`
6. **DuckDB Views**: Run `create_objects.sql` in DBeaver to register all Bronze/Silver/Gold views
7. **Full Stack (Docker)**: `docker compose up -d --build`

---

## Design Principles

- **Flat**: Simple, procedural Python scripts. No brittle class hierarchies. Every script is independently runnable and debuggable.
- **Free**: Stays within free API limits via surgical category filtering, intelligent polling intervals, and lock-file coordination.
- **Native**: Runs entirely on local Windows infrastructure. DuckDB for OLAP, Parquet for storage, no cloud dependencies.
