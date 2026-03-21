# PredictIQ — Docker Ingestion Commands
# Run these commands from the project root (C:\Data Engineering\codeprep\predection_project)

# ---------------------------------------------------------
# 1) BUILD THE IMAGE
# Run this once, or whenever you change the Dockerfile / requirements.txt
# ---------------------------------------------------------
docker compose build

# ---------------------------------------------------------
# 2) RUN GDELT INGESTION
# Fetches the latest 15-minute global events snapshot
# ---------------------------------------------------------
docker compose run spark-app python3 ingestion/gdelt_events_ingest.py

# ---------------------------------------------------------
# 3) RUN KALSHI ACTIVE MARKETS
# Fetches the current snapshot of all OPEN political markets
# ---------------------------------------------------------
docker compose run spark-app python3 ingestion/kalshi_markets_active.py

# ---------------------------------------------------------
# 4) RUN KALSHI HISTORICAL (ONE-TIME)
# Backfills all CLOSED and SETTLED political markets. 
# You only ever need to run this script once.
# ---------------------------------------------------------
docker compose run spark-app python3 ingestion/kalshi_markets_historical.py

# ---------------------------------------------------------
# 5) RUN KALSHI DAILY SETTLEMENT SWEEPER (RUN NIGHTLY)
# Captures markets that SETTLED in the last 24 hours.
# Fills the gap for markets that were open today and resolved.
# Schedule this to run once per day at ~11:59 PM via Prefect.
# ---------------------------------------------------------
docker compose run spark-app python3 ingestion/kalshi_daily_settlement.py

# ---------------------------------------------------------
# NOTE:
# The Python files inside the 'ingestion/' folder are live-mounted
# into the Docker container. If you edit the Python code on your
# Windows machine, you do NOT need to rebuild the image!
# Just run the 'docker compose run ...' command again and it will
# use your newest code.
# ---------------------------------------------------------
