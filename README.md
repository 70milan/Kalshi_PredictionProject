# PredictIQ: Political & Economic Intelligence Lakehouse

PredictIQ is a sophisticated data engineering platform designed to ingest, refine, and analyze high-signal political and economic data. Use this tool to bridge the gap between **Real-World Narrative** (News/GDELT) and **Market Conviction** (Kalshi Prediction Markets).

---

## Project Vision
To build a state-of-the-art "Intelligence Lakehouse" that provides a quantitative edge in predicting geopolitical outcomes, while remaining **completely free to operate** and **native to local Windows environments**.

---

## Tech Stack & Architecture
This project follows the **Medallion Architecture** (Bronze → Silver → Gold) to ensure data reliability and scalability.

*   **Storage Engine**: [DuckDB](https://duckdb.org/) (Native OLAP performance on Windows).
*   **Data Format**: [Apache Parquet](https://parquet.apache.org/) (Columnar storage for fast analytical queries).
*   **Processing**: [Apache Spark](https://spark.apache.org/) (Production-grade transformation layer).
*   **Visualization**: [DBeaver](https://dbeaver.io/) (Universal database tool for local Parquet/DuckDB inspection).

---

## Data Ingestion Suite (Bronze Layer)
The ingestion suite is designed to be "Net-Positive" — providing the highest signal-to-noise ratio while staying strictly within free API limits.

### Kalshi Markets (The "Conviction" Data)
*   `flat_kalshi_markets_active.py`: Heartbeat script fetching current political/economic odds. Uses **surgical series-first discovery** to avoid scanning 40k+ irrelevant sports/weather markets.
*   `flat_kalshi_markets_historical.py`: One-time backfill of settled and closed political history.
*   `flat_kalshi_candlesticks.py`: Fetches historical price action (OHLCV) for identifying momentum.

### GDELT Global Events (The "Signal" Data)
*   `flat_gdelt_events_ingest.py`: Ingests real-world event data from the GDELT Project. Filters for Geopolitical events to identify real-world triggers for market movements.

### News Narrative (Coming Soon)
*   `flat_news_headlines_ingest.py`: Planned ingestion of political headlines from NewsAPI to provide "Narrative context" for AI sentiment analysis.

---

## Key Improvements Beyond the PRD
While the original PRD focused on basic ingestion, we have implemented several **industry-standard** refinements:

1.  **Surgical Category Filtering**: Instead of a "Haystack Scan," we hit the server-level `/series` endpoint first. This reduces API requests by **98%** and eliminates the infinite loops caused by Kalshi's massive sports parlay expansion.
2.  **Flat Code Architecture**: All scripts are designed for **Native Execution** on Windows. No heavy dependencies or brittle class structures — just clean, procedural, and highly debuggable Python.
3.  **Local Lakehouse Structure**: Standardized a central `/data` directory at the project root (`PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))`) to ensure all data is organized, ignored by Git, and ready for DBeaver inspection.
4.  **Parquet Safety-First**: Implemented automatic dictionary-to-string serialization (`flatten_market`) to ensure all nested JSON data is safely stored in Parquet without schema corruption.

---

## Project Structure
```bash
PredictIQ/
├── data/               # Local Lakehouse (ignored by Git)
│   └── bronze/         # Raw snapshots in Parquet
├── ingestion/          # Production Spark/Docker scripts
├── native_ingestion/   # Flat, Native Windows scripts (Fast & Free)
├── .agent/skills/      # Custom AI expert instructions
├── .env                # API Keys (MANDATORY: Never commit this file)
└── .gitignore          # Comprehensive safety rules for secrets/data
```

---

## Quick Start
1.  **Clone the Repo**: `git clone ...`
2.  **Setup Environment**: Create a `.env` file with your `KALSHI_API_KEY`, `KALSHI_API_SECRET`, and `NEWS_API_KEY`.
3.  **Initialize Data**: Run `python native_ingestion/flat_kalshi_markets_historical.py` to backfill political history.
4.  **Daily Pulse**: Schedule `python native_ingestion/flat_kalshi_markets_active.py` to capture market movements.

---

## Built with the "Mfer" Philosophy:
*   **Flat**: Simple code is better than complex code.
*   **Free**: Stay within API limits by being smart, not frequent.
*   **Native**: Run it anywhere, store it locally.
