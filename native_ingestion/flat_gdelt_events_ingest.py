import os
import requests
import duckdb
from datetime import datetime
from dotenv import load_dotenv

# 1. Setup
load_dotenv()

# 2. Find the Latest GDELT Update URL
print("Checking for the latest GDELT global feed update...")
url_status = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
r = requests.get(url_status)
# The first line contains the 'Export' file (raw events)
export_url = r.text.split('\n')[0].split(' ')[2]

# 3. Native DuckDB Extraction and Transformation
print(f"Downloading and filtering global events from: {export_url}")
conn = duckdb.connect()

# GDELT is a Tab-Separated file with no headers. We name the key columns ourselves.
# We filter for events involving the 'USA' or the 'United States'
try:
    gdelt_query = f"""
    CREATE OR REPLACE TEMP TABLE raw_events AS 
    SELECT 
        column01 AS event_id,
        column02 AS event_day,
        column07 AS actor1_name,
        column17 AS actor2_name,
        column26 AS event_code,
        column30 AS goldstein_scale,
        column40 AS quad_class,
        column53 AS location_full,
        column57 AS source_url
    FROM read_csv('{export_url}', 
                 header=False, 
                 sep='\t', 
                 compression='zip', 
                 auto_detect=True)
    WHERE column07 = 'USA' OR column17 = 'USA' OR column53 LIKE '%United States%';
    """
    conn.execute(gdelt_query)
except Exception as e:
    print(f"Error reading GDELT data: {e}")
    exit()

# 4. Save to Bronze
local_dir = os.path.join("data", "bronze", "gdelt_events")
if not os.path.exists(local_dir):
    os.makedirs(local_dir)

safe_path = os.path.join(local_dir, "native_gdelt.parquet").replace("\\", "/")
conn.execute(f"COPY (SELECT * FROM raw_events) TO '{safe_path}' (FORMAT 'PARQUET')")

total_rows = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
print(f"Success: Saved {total_rows} GDELT events involving the USA to {safe_path}")
