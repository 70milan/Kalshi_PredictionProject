import os
import sys
import duckdb
import chromadb
from datetime import datetime, timezone
from sentence_transformers import SentenceTransformer

# ─────────────────────────────────────────────
# CONFIG & PATHS
# ─────────────────────────────────────────────

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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

def sync_collection(con, chroma_client, table_path, collection_name, text_cols, id_col, sentiment_col="sentiment"):
    """
    Incremental sync from Delta table to ChromaDB collection.
    """
    print(f"\n[Vector Sync] Syncing {collection_name} ...")
    
    # 1. Get/Create Collection
    collection = chroma_client.get_or_create_collection(name=collection_name)
    
    # 2. Watermark: Get max ingested_at from Chroma metadata if available
    # Since Chroma didn't expose a native 'max', we query a subset of recent items
    latest_timestamp = None
    count = collection.count()
    if count > 0:
        # For large collections, fetching ALL metadata crashes Chroma (SQL variable limit)
        # We fetch the last 1000 items, assuming news/events are added somewhat in order.
        res = collection.get(include=['metadatas'], limit=1000)
        if res['metadatas']:
            timestamps = [m.get('ingested_at') for m in res['metadatas'] if m.get('ingested_at')]
            if timestamps:
                latest_timestamp = max(timestamps)
    
    print(f"    > Current Chroma count: {count}")
    if latest_timestamp:
        print(f"    > Watermark: {latest_timestamp}")

    # 3. Read from Delta using DuckDB
    # Use array_to_string for GDELT array columns
    select_parts = []
    for c in text_cols:
        if "array" in c:
            select_parts.append(f"array_to_string({c}, ', ') as {c}")
        else:
            select_parts.append(c)
    
    # Define source/sentiment
    src_val = "source" if "news" in collection_name else "'GDELT' as source"
    select_list = ", ".join(set([id_col] + select_parts + ["ingested_at", src_val, f"{sentiment_col} as sentiment"]))
    
    try:
        query = f"SELECT {select_list} FROM delta_scan('{table_path}')"
        if latest_timestamp:
            # Incremental Filter
            query += f" WHERE ingested_at > '{latest_timestamp}'"
        
        query += " ORDER BY ingested_at ASC"
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
    batch_size = 50
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i : i + batch_size]
        
        # Deduplicate within the batch to prevent Chroma errors
        batch = batch.drop_duplicates(subset=[id_col])
        
        # Concatenate text columns for embedding
        # We use a lambda to ensure every element is stringified to avoid join type errors
        combined_text = batch[text_cols].apply(lambda row: " | ".join(row.fillna("").astype(str)), axis=1).tolist()
        
        # Generate Embeddings
        embeddings = model.encode(combined_text).tolist()
        
        # Prepare Metadata
        ids = batch[id_col].astype(str).tolist()
        
        # Sanitize metadata values
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

        # Insert/Update into Chroma
        collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=combined_text,
            metadatas=metadatas
        )
        print(f"    > Batch {i//batch_size + 1} complete ({len(batch)} rows synced).")

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
        sentiment_col="0.0" # GDELT sentiment is complex, using 0.0 for now
    )

    print("\n[Vector Sync] Bridge Synchronization Complete.")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()
