import duckdb
import os

print("--- latest_ws.parquet ---")
try:
    df1 = duckdb.query("SELECT * FROM 'data/bronze/kalshi_markets/ws_ticks/latest_ws.parquet' LIMIT 5").df()
    print(df1.to_string())
except Exception as e:
    print(e)
    
print("\n--- ws_*.parquet ---")
try:
    df2 = duckdb.query("SELECT * FROM 'data/bronze/kalshi_markets/ws_ticks/ws_*.parquet' LIMIT 5").df()
    print(df2.to_string())
except Exception as e:
    print(e)

print("\n--- latest.parquet (from metadata) ---")
try:
    df3 = duckdb.query("SELECT ticker, yes_bid, yes_ask FROM 'data/bronze/kalshi_markets/open/latest.parquet' LIMIT 5").df()
    print(df3.to_string())
except Exception as e:
    print(e)
