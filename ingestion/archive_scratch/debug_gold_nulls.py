import duckdb
import pandas as pd

conn = duckdb.connect()
print("--- Silver Columns ---")
try:
    df_silver = conn.query("SELECT * FROM 'data/silver/kalshi_markets_history' LIMIT 0").df()
    print(df_silver.columns.tolist())
    
    print("\n--- Silver Data (Sample) ---")
    data_silver = conn.query("SELECT ticker, ingested_at, yes_bid_dollars, yes_ask_dollars FROM 'data/silver/kalshi_markets_history' WHERE yes_bid_dollars IS NOT NULL LIMIT 5").df()
    print(data_silver.to_string())
except Exception as e:
    print(f"Silver check failed: {e}")

print("\n--- Gold Data (Contents) ---")
try:
    df_gold = conn.query("SELECT * FROM 'data/gold/market_summaries' LIMIT 5").df()
    print(df_gold.to_string())
except Exception as e:
    print(f"Gold check failed: {e}")
