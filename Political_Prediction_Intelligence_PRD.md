**POLITICAL PREDICTION**

**INTELLIGENCE SYSTEM**

Product Requirements Document | v1.0

Milan Barot • 2026 • Portfolio + Personal Use

# **1. Project Overview**

A fully automated, end-to-end data + AI platform that monitors Kalshi prediction markets on political and world events, ingests large-scale news data from GDELT, detects mispriced markets using a scoring engine, and surfaces LLM-generated research briefs to a personal dashboard for semi-automated bet approval.

This is simultaneously a Big Data Engineering project, an AI/RAG application, and a real utility you use daily for prediction market trading.

## **1.1 Goals**

* Ingest and process real, high-volume data (GDELT: millions of rows/day) using PySpark and Delta Lake locally
* Build a Medallion Architecture pipeline (Bronze → Silver → Gold) without a managed cloud platform
* Detect mispriced Kalshi markets by comparing market odds against news signal strength and historical resolution rates
* Generate plain-English research briefs via a RAG pipeline (ChromaDB + Groq/Llama 3)
* Surface recommendations to a personal React dashboard — you approve or reject each bet
* Containerize everything with Docker so any developer can clone and run it locally

## **1.2 What This Is NOT**

* Not a fully automated trading bot — you retain approval control
* Not a real-money platform — Kalshi paper trading only
* Not a cloud-dependent system — runs entirely on your local machine, free forever

# **2. System Architecture**

The system has 6 layers, each independently buildable and testable:

|  |  |  |  |
| --- | --- | --- | --- |
| **Layer** | **Name** | **What It Does** | **Key Tools** |
| 1 | Ingestion | Pull raw data from Kalshi API, GDELT, NewsAPI every 15 min / daily | Python, requests, schedule |
| 2 | ETL Pipeline | Transform raw data through Bronze → Silver → Gold medallion layers | PySpark, Delta Lake |
| 3 | Query Layer | SQL access to Delta tables for dev and debugging | DuckDB, Spark SQL |
| 4 | Scoring Engine | Detect mispriced markets, score confidence of each opportunity | Python, PySpark |
| 5 | RAG / LLM | Embed news + events, generate research briefs on anomalies | ChromaDB, Groq API |
| 6 | Dashboard | Display recommendations, approve/reject bets, track history | FastAPI, React |

## **2.1 Data Flow**

Raw sources → S3/local landing zone (Bronze) → normalized relational tables (Silver) → aggregated scoring tables (Gold) → ChromaDB embeddings → LLM brief → FastAPI → React dashboard → your approval → Kalshi paper trade execution

## **2.2 Storage Layout**

All Delta tables are stored as Parquet files on your local filesystem (or S3 free tier later). Structure:

|  |  |  |
| --- | --- | --- |
| **Layer** | **Table / Folder** | **Contents** |
| Bronze | bronze/kalshi\_markets/ | Raw market snapshots from Kalshi API (JSON → Delta) |
| Bronze | bronze/gdelt\_events/ | Raw GDELT CSV files landed as Delta |
| Bronze | bronze/news\_articles/ | Raw NewsAPI headlines |
| Silver | silver/markets/ | Normalized market rows with metadata |
| Silver | silver/odds\_history/ | Time-series odds per market (FK: market\_id) |
| Silver | silver/gdelt\_events/ | Parsed, deduplicated GDELT events |
| Silver | silver/news\_articles/ | Cleaned headlines with entity extraction |
| Gold | gold/mispricing\_scores/ | One row per market with computed mispricing score |
| Gold | gold/market\_summaries/ | Aggregated view: news volume + sentiment per market |
| Gold | gold/bet\_history/ | Your approved/rejected bets + outcomes (feedback loop) |

# **3. Data Sources**

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **Source** | **Data** | **Format** | **Volume** | **Cost** |
| Kalshi API | Live market odds, open positions, metadata | JSON REST | ~50-200 markets | Free |
| GDELT Project | Global news events, tone, actor mentions, themes | CSV (15-min drops) | Millions of rows/day | Free |
| NewsAPI | Breaking political headlines + article text | JSON REST | 100 req/day free tier | Free |
| Wikipedia Current Events | Structured daily event summaries | HTML scrape | Low volume | Free |

GDELT is the primary big data source. It drops a new file every 15 minutes, globally covering all major news events with actor codes, tone scores, geographic tags, and event categories — this is your genuine large-scale ingestion challenge.

# **4. Full Tech Stack**

|  |  |  |
| --- | --- | --- |
| **Component** | **Tool** | **Why** |
| Big Data ETL | PySpark (local) | pip install pyspark — full Spark without Databricks |
| Storage Format | Delta Lake (local) | pip install delta-spark — ACID, CDC, time travel on local files |
| Query / Debug | DuckDB | Instant SQL over Parquet/Delta files, like SSMS for files |
| Orchestration | Prefect (free tier) | Simpler than Airflow, free cloud dashboard, decorator-based |
| Vector Database | ChromaDB (local) | Runs as a Python library, no server needed |
| LLM | Groq API (Llama 3) | Free tier, fast inference, drop-in replacement for OpenAI |
| Backend API | FastAPI | Your existing stack from RealTime Context Engine |
| Frontend | React | Personal dashboard for bet approval |
| Containerization | Docker + docker-compose | One command to run entire stack on any machine |
| Prediction Market | Kalshi API | CFTC regulated, H1B accessible, paper trading supported |

**Total monthly cost: $0**

# **5. Build Phases**

|  |
| --- |
| **Phase 1: Foundation & Ingestion** | Week 1–2 |
| • Set up local project structure: /data/bronze, /silver, /gold folders |
| • Install PySpark + Delta Lake locally, confirm Spark session boots |
| • Write Kalshi API poller — fetch open markets every 15 mins, land to Bronze Delta table |
| • Write GDELT daily downloader — fetch CSV drops, land to Bronze Delta table |
| • Write NewsAPI scraper — fetch headlines, land to Bronze |
| • Confirm all three Bronze tables are queryable via DuckDB |
| • Set up Prefect flow to schedule all three ingestion jobs |

|  |
| --- |
| **Phase 2: ETL Pipeline — Silver Layer** | Week 3–4 |
| • Bronze → Silver: normalize Kalshi markets into relational schema (market\_id, title, category, close\_date, current\_odds) |
| • Bronze → Silver: build odds\_history table — one row per poll per market (time-series) |
| • Bronze → Silver: parse GDELT CSV — extract event codes, actor names, tone scores, geographic tags |
| • Bronze → Silver: clean NewsAPI headlines — deduplicate, extract entities (spaCy or simple regex) |
| • Add schema validation and bad-record quarantine to each Silver transform |
| • All Silver tables joinable on market\_id or event\_date — test joins in DuckDB |

|  |
| --- |
| **Phase 3: ETL Pipeline — Gold Layer + Scoring Engine** | Week 5–6 |
| • Build gold/market\_summaries: for each open Kalshi market, aggregate related news volume + avg GDELT tone over last 7 days |
| • Build gold/mispricing\_scores: compute gap between current market odds and signal-adjusted probability |
| • Mispricing logic v1 (rule-based): if news volume spike > 2x baseline AND tone positive AND market odds < 40% → flag as candidate |
| • Add historical resolution lookup: how did similar past markets resolve? (stored in gold/bet\_history) |
| • Score each candidate 0–100, rank by confidence |
| • Test end-to-end: GDELT event → Silver → Gold score in under 5 minutes |

|  |
| --- |
| **Phase 4: RAG Pipeline + LLM Briefs** | Week 7–8 |
| • Set up ChromaDB locally — embed all Silver news articles and GDELT event summaries |
| • Build embedding refresh job — runs daily via Prefect, adds new articles to ChromaDB |
| • Build RAG retrieval function: given a market\_id, retrieve top-5 most relevant news chunks |
| • Build LLM brief generator: RAG context + market odds + mispricing score → Groq API (Llama 3) → plain-English research brief |
| • Example output: 'Market priced at 32% but news volume on this topic tripled in 48hrs. 4 similar events in 2024 resolved YES at 68%. Suggested: YES position.' |
| • Store generated briefs in gold/mispricing\_scores table |

|  |
| --- |
| **Phase 5: Dashboard + Approval Workflow** | Week 9–10 |
| • FastAPI backend: /markets endpoint (all open scored markets), /brief/{market\_id} (LLM brief), /approve, /reject endpoints |
| • React frontend: table of open markets ranked by mispricing score, click to expand LLM brief, Approve / Reject / Watch buttons |
| • On approval: call Kalshi paper trading API to place bet |
| • Track all approvals/rejections + eventual outcomes in gold/bet\_history (feedback loop) |
| • Add accuracy dashboard: approved bets win rate vs rejected bets win rate over time |

|  |
| --- |
| **Phase 6: Containerization + Portfolio Polish** | Week 11–12 |
| • Write Dockerfile for FastAPI backend, React frontend, Prefect worker |
| • Write docker-compose.yml — single command spins up all services |
| • Volume mount ./data/ into containers so Delta tables persist on host filesystem |
| • .env.example file for API keys (Kalshi, Groq, NewsAPI) |
| • Write README: architecture diagram, setup instructions, example screenshots |
| • Add ARCHITECTURE.md with data flow diagram and tech decisions |
| • Record a 3-minute demo video showing a real mispricing alert and approval flow |

# **6. Portfolio & Interview Positioning**

One-liner for resume / portfolio site:

*"Multi-source political prediction intelligence platform — GDELT-scale news ingestion through a local PySpark Medallion pipeline with a RAG-powered research assistant that detects mispriced Kalshi markets and surfaces trade recommendations in plain English."*

## **6.1 What Each Layer Signals to Hiring Managers**

|  |  |  |
| --- | --- | --- |
| **Layer** | **Signal** | **Who Cares** |
| GDELT ingestion at scale | Big Data ETL with real volume, not toy data | Senior DE roles |
| PySpark + Delta Lake (local) | Deep understanding of lakehouse without needing managed platform | Data Architect roles |
| Medallion Architecture | Production data modeling — Bronze/Silver/Gold | Analytics Engineer roles |
| Prefect orchestration | Modern workflow tooling (Airflow successor) | Data platform roles |
| RAG pipeline + ChromaDB | Practical AI engineering — not just calling GPT | AI/ML Engineer roles |
| FastAPI + SSE streaming | Full-stack data product delivery | Startup / AI product roles |
| Docker + docker-compose | Production-ready, reproducible engineering | All technical roles |
| Kalshi paper trading loop | End-to-end product thinking with real feedback loop | Product-minded roles |

# **7. Risks & Mitigations**

|  |  |  |
| --- | --- | --- |
| **Risk** | **Likelihood** | **Mitigation** |
| Kalshi API rate limits | Medium | Cache responses locally, poll every 15 mins not real-time |
| GDELT files too large for RAM | Medium | Process in PySpark partitions — Spark handles this natively |
| Groq free tier limits hit | Low | Brief generation is infrequent (only on flagged markets) |
| NewsAPI 100 req/day limit | Low | GDELT covers most news signal — NewsAPI is supplementary |
| Local Spark session slow to start | High | Use DuckDB for all dev/debug queries, only use Spark for batch runs |
| Mispricing model too noisy | Medium | Start with high-confidence threshold (>0.25 gap), tune over time |

# **8. Recommended Repo Structure**

**prediction-intelligence/**

├── data/ ← Delta Lake storage (Bronze/Silver/Gold)

├── ingestion/ ← Kalshi, GDELT, NewsAPI pullers

├── pipeline/ ← PySpark Bronze→Silver→Gold transforms

├── scoring/ ← Mispricing detection engine

├── rag/ ← ChromaDB embedding + Groq LLM brief generation

├── api/ ← FastAPI backend

├── frontend/ ← React dashboard

├── flows/ ← Prefect orchestration flows

├── docker-compose.yml

├── .env.example

├── README.md

└── ARCHITECTURE.md

# **9. Definition of Done**

* All 6 Docker services start with docker-compose up on a fresh machine
* GDELT ingestion processes a full day of data end-to-end in under 10 minutes
* At least one real Kalshi market flagged as mispriced with a generated LLM brief
* Approve flow calls Kalshi paper trading API and logs result to bet\_history
* bet\_history tracks outcome after market resolves — win/loss accuracy visible on dashboard
* README includes architecture diagram and setup instructions
* Demo video recorded — 3 minutes, shows full flow from ingestion to approved bet

**Build it in phases. Ship the PRD. Start with Phase 1 today.**

milan barot • 70milan.github.io • 2026