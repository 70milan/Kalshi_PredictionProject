import os
import requests
import time
import base64
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization

load_dotenv()

def get_kalshi_auth_headers(method, path):
    """Generates RSA-signed headers for Kalshi V2 API."""
    api_key = os.getenv("KALSHI_API_KEY")
    api_secret = os.getenv("KALSHI_API_SECRET")
    if not api_key or not api_secret:
        raise ValueError("API Keys missing from .env")

    api_secret_bytes = api_secret.replace("\\n", "\n").encode()
    timestamp = str(int(time.time() * 1000))
    msg = timestamp + method + path
    
    private_key = serialization.load_pem_private_key(api_secret_bytes, password=None)
    signature = private_key.sign(msg.encode(), padding.PKCS1v15(), hashes.SHA256())
    base64_sig = base64.b64encode(signature).decode()

    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-SIGNATURE": base64_sig,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "Content-Type": "application/json"
    }

def get_filtered_markets():
    """Fetches markets and filters for Political/Economic relevance."""
    path = "/trade-api/v2/markets"
    url = f"https://api.elections.kalshi.com{path}"
    
    print(f"📡 Fetching markets from {url}...")
    headers = get_kalshi_auth_headers("GET", path)
    params = {"limit": 200, "status": "open"}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        all_markets = response.json().get("markets", [])
        
        # 🎯 SMART FILTER: Keep Politics, Econ, and World. Drop Sports/Games.
        keywords = ['politics', 'election', 'fed', 'interest', 'president', 'house', 'senate', 'world']
        
        focused = []
        for m in all_markets:
            text = f"{m.get('category', '')} {m.get('title', '')} {m.get('ticker', '')}".lower()
            if any(k in text for k in keywords):
                focused.append(m)
        
        print(f"✅ Filtered {len(all_markets)} down to {len(focused)} relevant markets.")
        return focused
    except Exception as e:
        print(f"❌ API Error: {e}")
        return []

def save_to_bronze(markets):
    if not markets:
        print("⚠️ No data to save.")
        return

    # Initialize Spark with Permission Fixes
    builder = SparkSession.builder \
        .appName("KalshiIngestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.ivy", "/opt/spark/work-dir/.ivy2") \
        .master("local[*]")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Add ingestion timestamp
    for m in markets:
        m["ingested_at"] = datetime.utcnow().isoformat()

    df = spark.createDataFrame(markets)

    bronze_path = "/app/data/bronze/kalshi_markets"
    print(f"💾 Saving to Delta: {bronze_path}")
    
    # We use 'append' so your data history grows over time!
    df.write.format("delta").mode("append").save(bronze_path)
    print("✅ Ingestion complete.")

if __name__ == "__main__":
    markets = get_filtered_markets()
    save_to_bronze(markets)
