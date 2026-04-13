import os
import requests
import zipfile
import io
import csv
import pandas as pd
import duckdb
import time
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

GDELT_LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# Docker: /app is the project root (volume-mounted)
PROJECT_ROOT = "/app"
BRONZE_DIR   = os.path.join(PROJECT_ROOT, "data", "bronze", "gdelt", "gdelt_gkg")
LAST_URL_FILE = os.path.join(BRONZE_DIR, ".last_url")

# GDELT 2.0 GKG Columns (27 total)
GKG_COLUMNS = [
    "GKGRECORDID", "DATE", "SourceCollectionIdentifier", "SourceCommonName",
    "DocumentIdentifier", "Counts", "V2Counts", "Themes", "V2Themes",
    "Locations", "V2Locations", "Persons", "V2Persons", "Organizations",
    "V2Organizations", "V2Tone", "Dates", "GCAM", "SharingImage",
    "RelatedImages", "SocialImageEmbeds", "SocialVideoEmbeds", "Quotations",
    "AllNames", "Amounts", "TranslationInfo", "Extras"
]

# Columns to keep for Bronze (Precisely per planning brief)
BRONZE_COLUMNS_GKG = [
    "GKGRECORDID", "DATE", "V2Counts", "V2Themes", "V2Persons", "V2Organizations",
    "V2Locations", "V2Tone", "GCAM", "AllNames", "Amounts",
    "SOURCEURL", "ingested_at"
]

# Political/Conflict Themes (Prefix list for matching)
POLITICAL_THEMES = [
    "ELECTION",           # elections, voting, ballots
    "GOV_",               # all government themes
    "LEADER_",            # world leaders
    "POLITICAL_",         # political events/prisoners/parties
    "ECON_SANCTION",      # sanctions
    "ECON_TARIFF",        # tariffs
    "ECON_TRADE",         # trade policy
    "PROTEST_",           # protests and civil unrest  
    "TERROR_",            # terrorism events
    "MILITARY_",          # military actions
    "LEGISLATION_",       # bills, laws, policy
    "DIPLOMACY_",         # diplomatic events
    "United_NATIONS",     # UN activity
    "WAR_",               # conflict
    "PEACE_",             # peace talks/treaties
]

# ─────────────────────────────────────────────
# DEDUP — LAST URL CHECK
# ─────────────────────────────────────────────

def already_ingested(url):
    """Returns True if this exact GDELT GKG file was already successfully ingested."""
    if os.path.exists(LAST_URL_FILE):
        with open(LAST_URL_FILE) as f:
            return f.read().strip() == url
    return False

def mark_ingested(url):
    """Writes the successfully ingested URL to disk."""
    os.makedirs(BRONZE_DIR, exist_ok=True)
    with open(LAST_URL_FILE, "w") as f:
        f.write(url)

# ─────────────────────────────────────────────
# PROCESSING
# ─────────────────────────────────────────────

def is_political(v2themes_raw):
    """Filter logic: True if any GKG theme starts with any political prefix."""
    if not v2themes_raw or not isinstance(v2themes_raw, str):
        return False
    
    themes = v2themes_raw.split(";")
    prefixes = tuple(POLITICAL_THEMES)
    for theme in themes:
        if theme.startswith(prefixes):
            return True
    return False

def fetch_latest_gkg_url():
    """Fetches the latest GKG export URL from lastupdate.txt"""
    try:
        response = requests.get(GDELT_LASTUPDATE_URL, timeout=10)
        response.raise_for_status()
        lines = response.text.strip().split("\n")
        
        for line in lines:
            parts = line.strip().split(" ")
            if len(parts) >= 3 and parts[2].endswith(".gkg.csv.zip"):
                return parts[2]
        return None
    except Exception as e:
        print(f"❌ Failed to fetch GDELT lastupdate: {e}")
        return None

def download_and_parse_gkg(url):
    """Downloads zip, unzips in memory, and filters to political events."""
    try:
        print(f"📡 Downloading GKG: {url}")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                df = pd.read_csv(
                    f, 
                    sep="\t", 
                    names=GKG_COLUMNS, 
                    dtype=str, 
                    quoting=csv.QUOTE_NONE,
                    on_bad_lines='skip',
                    encoding='utf-8'
                )
                
                initial_count = len(df)
                df = df[df["V2Themes"].apply(is_political)].copy()
                
                print(f"📊 Filtering GKG: {initial_count:,} → {len(df):,} political rows")
                return df
    except Exception as e:
        print(f"❌ Failed to download or parse GKG data: {e}")
        return None

def save_to_bronze(df):
    """Saves any processed items to Bronze Parquet via DuckDB."""
    if df is None or df.empty:
        print("⚠️ No political items found.")
        return False
        
    os.makedirs(BRONZE_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(BRONZE_DIR, f"gkg_{ts}.parquet").replace("\\", "/")
    latest_path = os.path.join(BRONZE_DIR, "latest.parquet").replace("\\", "/")
    
    conn = duckdb.connect()
    try:
        conn.execute(f"COPY (SELECT * FROM df) TO '{filepath}' (FORMAT 'PARQUET')")
        conn.execute(f"COPY (SELECT * FROM df) TO '{latest_path}' (FORMAT 'PARQUET')")
        print(f"Snapshot saved: {filepath}")
        print(f"Latest saved: {latest_path}")
        return True
    except Exception as e:
        print(f"Failed to save to Parquet: {e}")
        return False
    finally:
        conn.close()

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 65)
    print("PredictIQ — GDELT GKG Ingestion (v5)")
    print(f"Run time : {datetime.now(timezone.utc).isoformat()}")
    print("=" * 65)
    
    ingested_at = datetime.now(timezone.utc).isoformat()
    
    url = fetch_latest_gkg_url()
    if not url: return
    
    if already_ingested(url):
        print(f"☕ Already up to date: {url}")
        return
        
    df = download_and_parse_gkg(url)
    if df is not None:
        df.rename(columns={"DocumentIdentifier": "SOURCEURL"}, inplace=True)
        df["ingested_at"] = ingested_at
        df_to_save = df[BRONZE_COLUMNS_GKG]
        
        if save_to_bronze(df_to_save):
            mark_ingested(url)
            print("✅ GKG Bronze Ingestion successful.")

    print("=" * 65)

if __name__ == "__main__":
    # Poller loop for background execution
    while True:
        try:
            main()
        except Exception as e:
            print(f"CRITICAL ERROR in GKG poller: {e}")
        
        print("Sleeping for 900 seconds (15 mins) before next poll...")
        time.sleep(900)
