# PredictIQ Docker Command Guide

This guide covers how to manage the PredictIQ data pipeline using Docker.

## 🚀 Starting the Pipeline

### Clean Build & Start (Full Automation)
Use this after any code changes to rebuild the images and start all 15 services.
```bash
docker compose up --build

or docker compose build
```

### Start in Background (Detached)
Keep the pipeline running without locking your terminal.
```bash
docker compose up -d
```

### Shutdown 
Stops all containers safely.
```bash
docker compose down
```

---

## 📋 Monitoring Logs

### View All Logs (Everything at once)
```bash
docker compose logs -f
```

### View a Specific Script (Example)
Follow the logs of just one job to debug it.
```bash
docker logs -f predictiq_ingest_kalshi_active
docker logs -f predictiq_transform_news
```

---docker compose stop ingest-kalshi-daily
docker compose stop ingest-kalshi-daily


## 🛠️ Individual Service Management

### Restart a Specific Job
If one script is acting up, you don't need to restart the whole pipeline.
```bash
docker compose restart ingest-nyt
docker compose restart transform-kalshi
```

### Run a One-Off Command
Run a script manually inside a fresh container and delete it when done.
```bash
# Run the historical ingestion manually
docker compose run --rm spark-app python3 ingestion/kalshi_markets_historical.py
```

---

## 🧹 Maintenance

### Check Container Status
Shows which scripts are running, how long they've been up, and their health.
```bash
docker ps
```

### Clean Up Old Images
Free up disk space from previous builds.
```bash
docker image prune -f
```

### Current Service Inventory
- **Ingestion**: `ingest-bbc`, `ingest-cnn`, `ingest-fox`, `ingest-hindu`, `ingest-nypost`, `ingest-nyt`, `ingest-gdelt-events`, `ingest-gdelt-gkg`, `ingest-kalshi-active`, `ingest-kalshi-daily`
- **Transformation**: `transform-gdelt-events`, `transform-gdelt-gkg`, `transform-kalshi`, `transform-news`
- **Utility**: `spark-app`

## Run RAG and Inference Scripts
```bash
docker compose run --rm spark-app python3 rag/embed_silver_data.py
docker compose run --rm spark-app python3 inference/predict_movements.py
```
