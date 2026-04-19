import duckdb
import os

db = duckdb.connect()

tables = {
    "bronze_kalshi": "data/bronze/kalshi_markets/*.parquet",
    "bronze_gdelt_events": "data/bronze/gdelt/gdelt_events/*.parquet",
    "bronze_gdelt_gkg": "data/bronze/gdelt/gdelt_gkg/*.parquet",
    "bronze_bbc": "data/bronze/bbc/*.parquet",
    "silver_kalshi": "data/silver/kalshi_markets_current/*.parquet",
    "silver_gdelt_events": "data/silver/gdelt_events_current/*.parquet",
    "silver_gdelt_gkg": "data/silver/gdelt_gkg_current/*.parquet",
}

for name, path in tables.items():
    print(f"--- {name} ---")
    try:
        res = db.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}', union_by_name=True) LIMIT 0").fetchall()
        for col in res:
            print(f"{col[0]}: {col[1]}")
    except Exception as e:
        print(f"Error reading {name}: {e}")
    print("\n")
