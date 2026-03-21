import os
import requests
import time
import base64
import duckdb
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization

# 1. Environment Setup
load_dotenv()
api_key = os.getenv("KALSHI_API_KEY")
api_secret = os.getenv("KALSHI_API_SECRET").replace("\\n", "\n")

# 2. Get Tickers from Bronze
conn = duckdb.connect()
markets_path = "data/bronze/kalshi_markets/native_markets.parquet"

if not os.path.exists(markets_path):
    print(f"Error: Could not find {markets_path}. Run flat_kalshi_poller.py first.")
    exit()

print(f"Reading tickers from {markets_path}...")
# Fetch top 5 tickers for historical analysis
tickers_df = conn.execute(f"SELECT ticker FROM read_parquet('{markets_path}') LIMIT 5").fetchall()
all_tickers = [row[0] for row in tickers_df]

# 3. Fetch Historical Candlesticks
history_data = []
ingested_at = datetime.utcnow().isoformat()

for ticker in all_tickers:
    print(f"Fetching history for: {ticker}")
    # The endpoint for candlesticks (Daily history)
    # Correct endpoint for Kalshi V2: /trade-api/v2/series/markets/{ticker}/candlesticks
    path = f"/trade-api/v2/series/markets/{ticker}/candlesticks"
    url = f"https://api.elections.kalshi.com{path}"
    
    # RSA Authentication
    method = "GET"
    timestamp = str(int(time.time() * 1000))
    msg = timestamp + method + path

    private_key = serialization.load_pem_private_key(api_secret.encode(), password=None)
    signature = private_key.sign(msg.encode(), padding.PKCS1v15(), hashes.SHA256())
    base64_sig = base64.b64encode(signature).decode()

    headers = {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-SIGNATURE": base64_sig,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        candles = response.json().get("candlesticks", [])
        
        # Capture each day of history
        for c in candles:
            history_data.append({
                "ticker": ticker,
                "open": c.get("open"),
                "high": c.get("high"),
                "low": c.get("low"),
                "close": c.get("close"),
                "volume": c.get("volume"),
                "date": c.get("start_time"), # When this candle started
                "ingested_at": ingested_at
            })
        time.sleep(1) # Protect API from over-polling
        
    except Exception as e:
        print(f"API Error for {ticker}: {e}")

# 4. Native DuckDB Storage
if history_data:
    local_dir = os.path.join("data", "bronze", "kalshi_history")
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # Convert list to DataFrame for DuckDB
    df = pd.DataFrame(history_data)
    
    safe_path = os.path.join(local_dir, "native_history.parquet").replace("\\", "/")
    
    # Save the history list directly to parquet
    conn.execute(f"COPY (SELECT * FROM df) TO '{safe_path}' (FORMAT 'PARQUET')")
    
    print(f"Success: Saved {len(history_data)} historical data points to {safe_path}")
else:
    print("No historical data points retrieved.")
