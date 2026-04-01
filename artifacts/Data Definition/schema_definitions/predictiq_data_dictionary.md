# PredictIQ Master Data Dictionary

This document serves as the single source of truth for all data definitions, ingestion scripts, and schemas within the PredictIQ project.

---

## 1. Ingestion Catalog
Lists all active ingestion scripts and their target Bronze Layer storage.

| Script Name | Data Source | Frequency | Output (Bronze Layer) | Description |
| :--- | :--- | :--- | :--- | :--- |
| `gdelt_events_ingest.py` | GDELT 2.0 Events | 15 mins | `data/bronze/gdelt/gdelt_events` | Structured "actor-action" records. |
| `flat_gdelt_gkg_ingest.py` | GDELT 2.0 GKG | 15 mins | `data/bronze/gdelt/gdelt_gkg` | Themes, entities (V2Persons), and tone. |
| `flat_reuters_news_ingest.py`| Reuters Politics | Periodic | `data/bronze/reuters` | Scraped full-text news context. |
| `flat_nyt_news_ingest.py` | NYT Politics | Periodic | `data/bronze/nyt` | Scraped full-text news context. |
| `flat_kalshi_markets_active.py`| Kalshi API (V2) | Real-time | `data/bronze/kalshi_markets/open`| Currently open prediction markets. |
| `flat_kalshi_daily_settlement.py`| Kalshi API (V2) | Daily | `data/bronze/kalshi_markets/settled`| Settled/Resolved historical markets. |

---

## 2. Source Schemas (Bronze Layer)

### 🌍 GDELT Events
**Script:** `gdelt_events_ingest.py`  
**Description:** The "Action Profile" for the Scorer.

| Field | Type | Description |
| :--- | :--- | :--- |
| `GLOBALEVENTID` | BIGINT | Unique ID for the event. |
| `SQLDATE` | STRING | Date the event took place (YYYYMMDD). |
| `Actor1Name` | STRING | Entity name of the primary actor. |
| `Actor2Name` | STRING | Entity name of the secondary actor. |
| `EventCode` | STRING | CAMEO action code (e.g. `140` for Protest). |
| `GoldsteinScale` | DECIMAL | Stability impact score (-10.0 to +10.0). |
| `NumMentions` | INT | Total mentions across all documents. |
| `AvgTone` | DECIMAL | Average sentiment of reporting (-100 to +100). |
| `ActionGeo_FullName` | STRING | Human-readable name of the action location. |
| `SOURCEURL` | STRING | URL of the first reported news article. |
| `ingested_at` | STRING | UTC ISO timestamp showing when it entered PredictIQ. |

### 🧠 GDELT GKG (Global Knowledge Graph)
**Script:** `flat_gdelt_gkg_ingest.py`  
**Description:** The "Context/Entity Profile" using for RAG and Actor-to-Market joins.

| Field | Type | Description |
| :--- | :--- | :--- |
| `GKGRECORDID` | STRING | Unique GDELT observation ID. |
| `DATE` | STRING | GDELT news cycle timestamp (YYYYMMDDHHMMSS). |
| `V2Themes` | STRING | Semicolon-delimited global topics (e.g., `ECON_SANCTION`). |
| `V2Persons` | STRING | Semicolon-delimited list of persons mentioned (Critical). |
| `V2Organizations`| STRING | Semicolon-delimited list of entities mentioned. |
| `V2Tone` | STRING | Comma-delimited sentiment/tone scores. |
| `SOURCEURL` | STRING | Canonical URL of the news article. |
| `ingested_at` | STRING | UTC ISO timestamp showing when it entered PredictIQ. |

### 📰 News RSS (Reuters & NYT)
**Scripts:** `flat_reuters_news_ingest.py`, `flat_nyt_news_ingest.py`  
**Description:** Full-text context for high-confidence LLM Research Briefs.

| Field | Type | Description |
| :--- | :--- | :--- |
| `source` | STRING | Originating publisher ("Reuters", "NYT"). |
| `title` | STRING | Article headline. |
| `link` | STRING | Canonical URL (Join key). |
| `published_at` | STRING | Original publication timestamp. |
| `full_text` | STRING | The whole article body (scraped via Trafilatura). |
| `scraped` | BOOLEAN | `True` if extraction was successful. |
| `ingested_at` | STRING | UTC ISO timestamp showing when it entered PredictIQ. |

### 📈 Kalshi Prediction Markets
**Script:** `flat_kalshi_markets_active.py`  
**Description:** The primary prediction targets for the project.

| Field | Type | Description |
| :--- | :--- | :--- |
| `ticker` | STRING | Specific market ID (e.g.`KX-2024-PRES`). |
| `title` | STRING | Human-readable market question. |
| `yes_bid_dollars` | DECIMAL | "Yes" price ($0 - $1.00). Implied Probability. |
| `rules_primary` | STRING | Text describing exactly how the market settles. |
| `expiration_time`| STRING | When the market closes. |
| `ingested_at` | STRING | UTC ISO timestamp showing when it entered PredictIQ. |

---

## 3. Reference Data Definitions
Tables used in the **Silver Layer** to enrich and decode GDELT codes.

| Table Name | Storage Path | Key Columns |
| :--- | :--- | :--- |
| `cameo_eventcodes.csv` | `reference/` | `code`, `label` |
| `fips_countries.csv` | `reference/` | `code`, `country` |
| `cameo_actortypes.csv` | `reference/` | `code`, `type` |
| `cameo_quadclass.csv` | `reference/` | `code`, `class`|

---

## 4. Architectural Notes
*   **Independent Streams:** Every script shown here runs independently.
*   **Delayed Convergence:** No hard-joins happen in Bronze. The Scoring Engine links these via **ChromaDB Vector Similarity** and **Entity Matching** in the Gold phase.
