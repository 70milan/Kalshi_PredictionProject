# PredictIQ — Project Architecture Reference

## What This Project Is

PredictIQ is a prediction-market intelligence pipeline. It ingests news and market data, runs Spark transformations through a Bronze→Silver→Gold lakehouse, embeds Silver data into ChromaDB, and uses an LLM (Groq Llama-3.3-70b) to detect mispriced markets on Kalshi. Signals are surfaced via a FastAPI + React dashboard and Telegram alerts.

---

## Infrastructure

### Two machines, one project directory

| Machine | Role | OS | Tailscale IP |
|---|---|---|---|
| `n` (this machine) | Dev / Claude Code | Windows 11 | `100.86.91.43` |
| `xeeeee` | Prod / Docker host | Windows (Docker Desktop) | `100.67.60.86` | (Path on prod: D:\MilanWork\projects_de\predection_project)

### Sync mechanism — Syncthing over Tailscale (no git push to deploy)

- **Dev → Prod**: code files sync automatically. Editing a file locally deploys it within seconds.
- **Prod → Dev**: `data/` directory syncs back so local reads reflect live data.
- **Do not** `git push` to deploy. Just save the file.

### Portainer

Portainer runs on the prod machine at `https://100.67.60.86:9443`. It manages all Docker containers.

**Portainer MCP** is configured for this project in `.claude.json`:
- Script: `C:\Users\milan\.claude\mcp-servers\portainer_mcp.py`
- Python: `C:\Users\milan\AppData\Local\Programs\Python\Python313\python.exe` (system Python — the `.venv` Python lacks `mcp`)
- Endpoint ID: `3`
- Tools: `list_containers`, `get_container_logs`, `check_failed_containers`, `list_stacks`, `list_endpoints`

If Portainer MCP shows as failed, check that the command points to the system Python (not `python` or the venv).

---

## Container Architecture (docker-compose.yml)

13 containers total. All use `restart: unless-stopped`.

### ETL / Pipeline

| Container | Script | Purpose |
|---|---|---|
| `predictiq_spark_etl` | `orchestration/run_etl.py` | Main orchestrator — runs all pipeline phases every 5 min |

### Bronze Ingestors (9 lightweight daemons)

| Container | Script | Source |
|---|---|---|
| `predictiq_ingest_kalshi_active` | `ingestion/kalshi_markets_active.py` | Kalshi open markets |
| `predictiq_ingest_bbc` | `ingestion/bbc_ingest.py` | BBC RSS |
| `predictiq_ingest_cnn` | `ingestion/cnn_ingest.py` | CNN RSS |
| `predictiq_ingest_fox` | `ingestion/fox_ingest.py` | Fox News RSS |
| `predictiq_ingest_nyt` | `ingestion/nyt_news_ingest.py` | NYT API |
| `predictiq_ingest_nypost` | `ingestion/nypost_ingest.py` | NY Post RSS |
| `predictiq_ingest_hindu` | `ingestion/hindu_ingest.py` | The Hindu RSS |
| `predictiq_ingest_gdelt_events` | `ingestion/gdelt_events_ingest.py` | GDELT Events |
| `predictiq_ingest_gdelt_gkg` | `ingestion/gdelt_gkg_ingest.py` | GDELT GKG |

### Serving

Frontend and API run directly (not via Docker):
- **API**: `uvicorn api.main:app --host 0.0.0.0 --port 8000`
- **Frontend**: `npm run dev -- --host` (in `frontend/`)
- Same commands on dev and prod machines

| Container | Script | Port | Notes |
|---|---|---|---|
| `portainer` | Portainer CE | `9000/9443` | Container management UI |

---

## Data Pipeline

### Storage layout (`data/`)

```
data/
  bronze/
    kalshi_markets/open/      ← live Kalshi markets (parquet snapshots)
    kalshi_markets/settled/   ← settled markets (daily sweep)
    kalshi_markets/closed/    ← closed markets
    bbc/ cnn/ foxnews/ nyt/ nypost/ hindu/  ← news parquet files
    gdelt/gdelt_events/       ← GDELT event records
    gdelt/gdelt_gkg/          ← GDELT knowledge graph
  silver/                     ← cleaned, deduplicated Delta tables
    silver_kalshi_markets/
    silver_news_enriched/
    silver_gdelt_enriched/
  gold/                       ← aggregated + scored Delta tables
    market_summaries/
    news_summaries/
    gdelt_summaries/
    mispricing_scores/        ← scored Kalshi markets
    intelligence_briefs/      ← LLM-generated trade recommendations (parquet, append-only)
    position_ledger.parquet   ← live Kalshi positions matched to briefs
    exit_signals/             ← per-cycle exit recommendations
    simulated_trades.csv      ← paper trading log
  chroma/                     ← ChromaDB vector store (news + GDELT embeddings)
  .etl_watermark              ← tracks last successful ETL run (mtime)
  .daily_settlement_marker    ← prevents duplicate daily settlement runs
  .kalshi_api.lock            ← ingestor lock file (stale after 30 min)
```

### ETL Phases (`orchestration/run_etl.py`) — runs every 5 minutes

```
Phase 1  Silver + Gold Spark pipeline (spark_pipeline.py)
         ├── silver_kalshi_transform
         ├── silver_news_transform
         ├── silver_gdelt_events_transform
         ├── silver_gdelt_gkg_transform      ← 4 Silver transforms
         ├── gold_market_summaries_transform
         ├── gold_news_summaries_transform
         └── gold_gdelt_summaries_transform  ← 3 Gold transforms
         All 7 run in ONE SparkSession (saves ~14 min of JVM startup)
         Timeout: 3000s (~50 min)

Phase 2  Vector Bridge (rag/embed_silver_data.py)
         Embeds new Silver rows into ChromaDB
         news → OpenAI text-embedding-3-small
         GDELT → sentence-transformers all-MiniLM-L6-v2
         Timeout: 2400s

Phase 3  Inference (inference/predict_movements.py)
         Queries Gold mispricing scores (threshold ≥ 80)
         RAG-retrieves relevant news/GDELT from ChromaDB
         Calls Groq (llama-3.3-70b-versatile) to synthesize verdict
         Writes briefs if edge > 10% vs market price
         Timeout: 1800s

Phase 4b Position Ledger + Exit Evaluator (runs EVERY cycle, no bronze gate)
         build_position_ledger.py  — fetches live Kalshi positions, matches to briefs
         exit_evaluator.py         — emits SELL_LOSS (stop-loss 17%) or HOLD signals
         Timeout: 120s each

Phase 5  Daily Settlement Sweep (once per UTC day)
         ingestion/kalshi_daily_settlement.py
         Skipped if .kalshi_api.lock is fresh (ingestor running)
         Timeout: 3600s
```

### Spark configuration

- Driver memory: 4 GB (`spark.driver.memory`)
- Delta Lake via `delta` pip package
- One JVM at a time (sequential guarantee in orchestrator)
- `PYSPARK_PYTHON` forced to `sys.executable` for container compatibility

---

## AI / Inference Stack

| Component | Model / Tool | Purpose |
|---|---|---|
| Embeddings (news) | OpenAI `text-embedding-3-small` | High-quality news semantic search |
| Embeddings (GDELT) | `all-MiniLM-L6-v2` (sentence-transformers) | GDELT event search |
| Vector store | ChromaDB (local, `data/chroma/`) | RAG retrieval |
| Synthesis LLM | Groq `llama-3.3-70b-versatile` | Trade verdict generation |
| Similarity floor | 0.50 cosine | Minimum RAG match to include signal |
| Edge threshold | +10% vs market | Minimum edge to write a brief |

---

## API (`api/main.py`) — FastAPI

- **Port 8000** (`api-live`): full access — trade execution allowed, `READONLY_MODE=false`
- **Port 8001** (`api-readonly`): `READONLY_MODE=true`, proxied via Tailscale Funnel for public access
- **SAFE_MODE**: env flag, defaults `true` — guards live trade execution
- **Tailscale detection**: requests from `127.0.0.1` = public Funnel traffic (read-only enforced); `100.x.x.x` = direct Tailscale device (trusted)
- **Trade sizing**: Kelly criterion (`api/kelly_math.py`) + theme concentration caps (`api/risk_guardrails.py`)
- **Kalshi auth**: RSA private key signing (`KALSHI_API_KEY` + `KALSHI_API_SECRET`)

---

## Frontend (`frontend/`) — React + Vite

- Served at `100.67.60.86:5173` (Tailscale only)
- Key components: `MispricingCard.jsx`, `ExitPanel.jsx`
- Talks to `api-live` at port 8000

---

## Alerts — Telegram

`orchestration/notify.py` sends Telegram messages for:
- `notify_failure(script, reason, phase)` — non-fatal phase failure (30 min cooldown per script)
- `notify_brief(ticker, title, ...)` — new intelligence brief written
- `notify_crash(traceback)` — orchestrator-level crash

Env vars: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

---

## Key Environment Variables (`.env` on prod)

| Variable | Purpose |
|---|---|
| `KALSHI_API_KEY` | Kalshi API key (RSA) |
| `KALSHI_API_SECRET` | Kalshi RSA private key |
| `OPENAI_API_KEY` | OpenAI embeddings |
| `GROQ_API_KEY` | Groq LLM inference |
| `TELEGRAM_BOT_TOKEN` | Telegram alerts bot |
| `TELEGRAM_CHAT_ID` | Telegram chat to send alerts |
| `BANKROLL_USD` | Kelly sizing base (default 1000.0) |
| `SAFE_MODE` | Guard for live trade execution (default `true`) |
| `READONLY_MODE` | Set `true` on api-readonly container |

---

## Common Tasks

### Check container status
Use the Portainer MCP tool `list_containers`, or via the API directly:
```python
# credentials are in .claude.json under mcpServers.portainer.env
```

### Read ETL logs
```
get_container_logs("predictiq_spark_etl", tail=200)
```

### Check for failed containers
```
check_failed_containers()
```

### Restart a crashed container
From the prod machine or Portainer UI — containers auto-restart (`unless-stopped`) but OOM kills need investigation before restart.

### Trigger a manual ETL cycle
The orchestrator loops indefinitely. To force a fresh cycle, touch any bronze file to update its mtime — the watermark check will pick it up.

### Deploy a code change
Save the file locally. Syncthing pushes it to prod within seconds. The running container picks up changes on the next cycle (Python subprocess model — no container restart needed for most changes).

---

## Architecture Principles

This project follows **Flat, Free, Native** constraints:
- **Flat**: no Kubernetes, no cloud orchestration, no managed services — everything runs in Docker Compose on one box
- **Free**: free-tier APIs and OSS tools where possible (GDELT is free, Groq has generous free tier, sentence-transformers runs locally)
- **Native**: Delta Lake via local filesystem, ChromaDB embedded, DuckDB for ad-hoc queries — no external databases
