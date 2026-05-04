import os
import sys
import platform
# ChromaDB/SQLite patch — only needed on Linux (Docker). Windows uses native SQLite.
if platform.system() == "Linux":
    try:
        __import__('pysqlite3')
        sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
    except ImportError:
        pass

import duckdb
import chromadb
import json
from datetime import datetime, timezone, timedelta
from sentence_transformers import SentenceTransformer

# ─────────────────────────────────────────────
# CONFIG & PATHS
# ─────────────────────────────────────────────

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WATERMARK_FILE = os.path.join(PROJECT_ROOT, "data", ".vector_watermark.json")

# Delta Lake Paths
SILVER_NEWS  = os.path.join(PROJECT_ROOT, "data", "silver", "news_articles_enriched")
SILVER_GDELT = os.path.join(PROJECT_ROOT, "data", "silver", "gdelt_gkg_history")

# ChromaDB Path
CHROMA_PATH  = os.path.join(PROJECT_ROOT, "data", "chroma")

# Embedding Model (Local SSD)
# First run will download the ~80MB model to ~/.cache/torch/sentence_transformers
MODEL_NAME = "all-MiniLM-L6-v2"

# ─────────────────────────────────────────────
# VECTOR SYNC ENGINE
# ─────────────────────────────────────────────

def get_watermarks():
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_watermark(collection_name, timestamp):
    marks = get_watermarks()
    marks[collection_name] = str(timestamp)
    with open(WATERMARK_FILE, 'w') as f:
        json.dump(marks, f, indent=4)

def sync_collection(con, chroma_client, table_path, collection_name, text_cols, id_col, sentiment_col="sentiment", max_rows=None, lookback_hours=None):
    """
    Incremental sync from Delta table to ChromaDB collection using a persistent watermark.
    max_rows: hard cap on rows per run (prevents runaway embedding on large tables)
    lookback_hours: ignore records older than N hours even if watermark is missing
    """
    print(f"\n[Vector Sync] Syncing {collection_name} ...")
    
    # 1. Get/Create Collection
    try:
        collection = chroma_client.get_or_create_collection(name=collection_name)
    except Exception as e:
        print(f"    > FATAL ERROR: Could not get/create collection '{collection_name}': {e}")
        return
    
    # 2. Load Persistent Watermark
    watermarks = get_watermarks()
    latest_timestamp = watermarks.get(collection_name)
    
    print(f"    > Current Chroma count: {collection.count()}")
    if latest_timestamp:
        print(f"    > Watermark: {latest_timestamp}")

    # 3. Read from Delta using DuckDB
    # Use array_to_string for GDELT array columns and truncate to 2000 chars at SQL level
    # This prevents massive strings (40k+) from choking memory during transfer to Pandas.
    select_parts = []
    for c in text_cols:
        if "array" in c:
            select_parts.append(f"substring(array_to_string({c}, ', '), 1, 2000) as {c}")
        else:
            select_parts.append(f"substring({c}, 1, 2000) as {c}")
    
    # Define source/sentiment
    src_val = "source" if "news" in collection_name else "'GDELT' as source"
    select_list = ", ".join(set([id_col] + select_parts + ["ingested_at", src_val, f"{sentiment_col} as sentiment"]))
    
    try:
        query = f"SELECT {select_list} FROM delta_scan('{table_path}')"
        
        # Build WHERE clause
        filters = []
        if latest_timestamp:
            filters.append(f"ingested_at > '{latest_timestamp}'")
        if lookback_hours:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
            filters.append(f"ingested_at >= '{cutoff}'")
            print(f"    > Hard lookback cap: last {lookback_hours}h only (cutoff: {cutoff[:19]})")
        if filters:
            query += " WHERE " + " AND ".join(filters)
        
        query += " ORDER BY ingested_at ASC"
        if max_rows:
            query += f" LIMIT {max_rows}"
        
        print(f"    > Fetching data from Delta (this may take a few minutes for large tables)...")
        df = con.execute(query).df()
    except Exception as e:
        print(f"    > ERROR reading {collection_name}: {e}")
        return


    if df.empty:
        print("    > No new records found. Skipping.")
        return

    print(f"    > Found {len(df)} new records to embed.")

    # 4. Initialize Local Model
    model = SentenceTransformer(MODEL_NAME)

    # 5. Batch Upsert
    import time
    batch_size = 50
    total_new = len(df)
    print(f"    > Starting embedding of {total_new} records in batches of {batch_size}...")
    
    for i in range(0, total_new, batch_size):
        batch_start = time.time()
        batch = df.iloc[i : i + batch_size]
        batch = batch.drop_duplicates(subset=[id_col])
        
        # 5.1 Truncate
        combined_text = batch[text_cols].apply(
            lambda row: " | ".join(row.fillna("").astype(str))[:2000], 
            axis=1
        ).tolist()
        
        # 5.2 Encode
        encode_start = time.time()
        embeddings = model.encode(combined_text).tolist()
        encode_duration = time.time() - encode_start
        
        # 5.3 Upsert
        upsert_start = time.time()
        ids = [f"{collection_name}_{str(x)}" for x in batch[id_col].tolist()]
        metadatas = []
        for _, row in batch.iterrows():
            ingested_at = str(row["ingested_at"])
            try:
                ts = int(datetime.fromisoformat(ingested_at.replace('Z', '+00:00')).timestamp())
            except:
                ts = 0
            metadatas.append({
                "ingested_at": ingested_at,
                "ingested_timestamp": ts,
                "source": row.get("source", "GDELT"),
                "sentiment": float(row.get("sentiment", 0.0))
            })

        collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=combined_text,
            metadatas=metadatas
        )
        
        batch_duration = time.time() - batch_start
        print(f"    > Batch {i//batch_size + 1} complete | {len(batch)} rows | Encode: {encode_duration:.1f}s | Total: {batch_duration:.1f}s")

    # 6. Update Watermark
    new_max = df['ingested_at'].max()
    save_watermark(collection_name, new_max)
    print(f"    > Watermark updated to: {new_max}")

def main():
    print("=" * 60)
    print(" PredictIQ Vector Bridge (Local SSD Synchronization)")
    print("=" * 60)

    # 1. Connect to DuckDB (for Delta scanning)
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    # 2. Connect to ChromaDB
    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)

    # 3. Sync News Collection
    sync_collection(
        con, 
        chroma_client, 
        SILVER_NEWS, 
        "silver_news_enriched", 
        text_cols=["title", "full_text"],
        id_col="link",
        sentiment_col="sentiment_score"
    )

    # 4. Sync GDELT GKG Collection
    # Metadata includes 'source' and 'sentiment' (we fallback to 0.0/GDELT)
    sync_collection(
        con, 
        chroma_client, 
        SILVER_GDELT, 
        "silver_gdelt_enriched", 
        text_cols=["persons_array", "themes_array", "orgs_array"],
        id_col="gkg_record_id",
        sentiment_col="0.0",
        max_rows=5000,       # Cap per cycle — GDELT is a signal source, not a search corpus
        lookback_hours=48    # Never embed records older than 48h even on cold start
    )

    print("\n[Vector Sync] Bridge Synchronization Complete.")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()
