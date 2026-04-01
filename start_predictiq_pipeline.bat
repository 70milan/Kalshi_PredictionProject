@echo off
echo ========================================================
echo PredictIQ Pipeline Initialization
echo ========================================================
echo.

echo Step 1: Rebuilding Docker Environment...
docker compose build

echo.
echo Step 2: Spinning up Ingestion Services...
docker compose up -d

echo.
echo Step 3: Verifying Services are Online...
docker ps

echo.
echo ========================================================
echo All background pipeline trackers are active!
echo Use 'docker compose logs -f' to monitor them securely.
echo ========================================================
pause
