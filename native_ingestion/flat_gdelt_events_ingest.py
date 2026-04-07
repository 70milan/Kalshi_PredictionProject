import os
import requests
import zipfile
import io
import pandas as pd
import duckdb
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

GDELT_LASTUPDATE_URL  = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# Always write to project root — two levels up from script location
PROJECT_ROOT  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR    = os.path.join(PROJECT_ROOT, "data", "bronze", "gdelt", "gdelt_events")
LAST_URL_FILE = os.path.join(BRONZE_DIR, ".last_url")  # tracks last ingested file URL

# Column names for GDELT 2.0 Event files (61 columns total)
GDELT_COLUMNS = [
    "GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode",
    "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
    "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code", "ActionGeo_ADM2Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL"
]

# Columns to keep for Bronze Layer
BRONZE_COLUMNS = [
    "GLOBALEVENTID", "SQLDATE", "Actor1Name", "Actor2Name",
    "Actor1CountryCode", "Actor2CountryCode",
    "Actor1Type1Code",
    "IsRootEvent",
    "EventCode", "EventBaseCode", "EventRootCode",
    "GoldsteinScale", "NumMentions", "NumSources", "NumArticles",
    "AvgTone", "Actor1Geo_CountryCode", "Actor2Geo_CountryCode",
    "ActionGeo_CountryCode", "ActionGeo_FullName",
    "SOURCEURL", "DATEADDED", "ingested_at"
]

# Political EventCode prefixes
POLITICAL_EVENT_CODES = [
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "20"
]

# ─────────────────────────────────────────────
# DEDUP — LAST URL CHECK
# ─────────────────────────────────────────────

def already_ingested(url):
    """Returns True if this exact GDELT file URL was already successfully ingested."""
    if os.path.exists(LAST_URL_FILE):
        with open(LAST_URL_FILE) as f:
            return f.read().strip() == url
    return False

def mark_ingested(url):
    """Writes the successfully ingested URL to disk. Called only after a clean save."""
    os.makedirs(BRONZE_DIR, exist_ok=True)
    with open(LAST_URL_FILE, "w") as f:
        f.write(url)

# ─────────────────────────────────────────────
# PROCESSING
# ─────────────────────────────────────────────

def fetch_latest_gdelt_url():
    """Fetches the latest GDELT export URL from lastupdate.txt"""
    try:
        response = requests.get(GDELT_LASTUPDATE_URL, timeout=10)
        response.raise_for_status()
        lines = response.text.strip().split("\n")

        # Search all 3 lines for the export file — don't assume line order
        for line in lines:
            parts = line.strip().split(" ")
            if len(parts) >= 3 and parts[2].endswith(".export.CSV.zip"):
                return parts[2]

        # No valid URL found in any line
        print("❌ Could not find .export.CSV.zip URL in lastupdate.txt")
        print(f"   Raw content: {response.text.strip()}")
        return None

    except Exception as e:
        print(f"❌ Failed to fetch GDELT lastupdate: {e}")
        return None

def download_and_parse_gdelt(url):
    """Downloads GDELT zip, unzips in memory, and parses CSV"""
    try:
        print(f"📡 Downloading: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                df = pd.read_csv(f, sep="\t", names=GDELT_COLUMNS, dtype=str)
                return df
    except Exception as e:
        print(f"❌ Failed to download or parse GDELT data: {e}")
        return None

def filter_political_events(df):
    """Filters GDELT events to political/world events only"""
    if df is None or df.empty:
        return None

    initial_rows = len(df)

    # Zero-pad EventCode so "1" matches "01" — GDELT stores some as bare integers
    df["EventCode"] = df["EventCode"].astype(str).str.zfill(2)

    mask = df["EventCode"].str[:2].isin(POLITICAL_EVENT_CODES)
    df_filtered = df[mask].copy()

    filtered_rows = len(df_filtered)
    print(f"📊 Filtering: {initial_rows:,} total rows → {filtered_rows:,} political events")

    return df_filtered

def save_to_bronze(df):
    """Saves the DataFrame to Parquet using DuckDB (latest and history)"""
    if df is None or df.empty:
        print("⚠️ No data to save.")
        return False

    os.makedirs(BRONZE_DIR, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    latest_path  = os.path.join(BRONZE_DIR, "latest.parquet").replace("\\", "/")
    history_path = os.path.join(BRONZE_DIR, f"gdelt_{ts}.parquet").replace("\\", "/")

    numeric_cols = ["GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df_to_save = df[BRONZE_COLUMNS]

    conn = duckdb.connect()
    try:
        conn.execute(f"COPY (SELECT * FROM df_to_save) TO '{latest_path}' (FORMAT 'PARQUET')")
        conn.execute(f"COPY (SELECT * FROM df_to_save) TO '{history_path}' (FORMAT 'PARQUET')")
        print(f"💾 Saved to Bronze:")
        print(f"   Latest   : {latest_path}")
        print(f"   Snapshot : {history_path}")
        return True  # signal success to main() so we can mark_ingested
    except Exception as e:
        print(f"❌ Failed to save to Parquet: {e}")
        return False
    finally:
        conn.close()

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 65)
    print("PredictIQ — GDELT Bronze Ingestion")
    print(f"Run time : {datetime.now(timezone.utc).isoformat()}")
    print(f"Output   : {BRONZE_DIR}")
    print("=" * 65)

    ingested_at = datetime.now(timezone.utc).isoformat()

    # 1. Get latest file URL
    url = fetch_latest_gdelt_url()
    if not url:
        return

    # 2. Skip if already ingested — handles container restarts and early wakeups
    if already_ingested(url):
        print(f"⏭️  Already ingested this file — skipping.")
        print(f"   URL: {url}")
        return

    # 3. Download and parse
    df = download_and_parse_gdelt(url)
    if df is None:
        return

    # 4. Add metadata
    df["ingested_at"] = ingested_at

    # 5. Filter to political events
    df_political = filter_political_events(df)

    # 6. Save — only mark as ingested if save succeeded
    saved = save_to_bronze(df_political)
    if saved:
        mark_ingested(url)

    print("\n✅ Done.")
    print("=" * 65)


if __name__ == "__main__":
    main()