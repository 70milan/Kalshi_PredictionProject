import os
import sys
import platform
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
from typing import List
from sentence_transformers import SentenceTransformer
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ─────────────────────────────────────────────
# CONFIG & PATHS
# ─────────────────────────────────────────────

PROJECT_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WATERMARK_FILE = os.path.join(PROJECT_ROOT, "data", ".vector_watermark.json")

SILVER_NEWS  = os.path.join(PROJECT_ROOT, "data", "silver", "news_articles_enriched")
SILVER_GDELT = os.path.join(PROJECT_ROOT, "data", "silver", "gdelt_gkg_history")
CHROMA_PATH  = os.path.join(PROJECT_ROOT, "data", "chroma")

# News → OpenAI text-embedding-3-small (better quality, ~$0.02/1M tokens)
# GDELT → local MiniLM (entity arrays benefit less from bigger model after text fix)
OPENAI_EMBED_MODEL = "text-embedding-3-small"
GDELT_EMBED_MODEL  = "all-MiniLM-L6-v2"

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ─────────────────────────────────────────────
# WATERMARK
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

# ─────────────────────────────────────────────
# GDELT TEXT BUILDER
# Converts structured entity arrays into readable sentences.
# all-MiniLM-L6-v2 is trained on natural language — entity lists like
# "ZELENSKYY | ELECTION | US_GOVERNMENT" embed poorly. Sentences embed well.
# ─────────────────────────────────────────────

def build_gdelt_sentence(row):
    parts = []
    persons = str(row.get("persons_array", "") or "").strip()
    themes  = str(row.get("themes_array",  "") or "").strip()
    orgs    = str(row.get("orgs_array",    "") or "").strip()
    tone    = row.get("tone_15m") or row.get("tone_score") or 0

    if persons:
        # "ZELENSKYY, VOLODYMYR, BIDEN, JOE" → "Zelenskyy Volodymyr, Biden Joe"
        names = [n.strip().title() for n in persons.split(",") if n.strip()][:5]
        parts.append("People involved: " + ", ".join(names))
    if themes:
        theme_list = [t.strip().replace("_", " ").title() for t in themes.split(",") if t.strip()][:5]
        parts.append("Themes: " + ", ".join(theme_list))
    if orgs:
        org_list = [o.strip().title() for o in orgs.split(",") if o.strip()][:4]
        parts.append("Organizations: " + ", ".join(org_list))
    sentiment_label = "positive" if float(tone) > 1 else ("negative" if float(tone) < -1 else "neutral")
    parts.append(f"Tone: {sentiment_label}")

    return ". ".join(parts) if parts else "GDELT event record."

# ─────────────────────────────────────────────
# EMBEDDING FUNCTIONS
# ─────────────────────────────────────────────

def embed_with_openai(texts: List[str]) -> List[List[float]]:
    """Batch embed with text-embedding-3-small. Max 2048 items per call."""
    all_embeddings = []
    batch_size = 100
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        response = openai_client.embeddings.create(
            model=OPENAI_EMBED_MODEL,
            input=batch
        )
        all_embeddings.extend([item.embedding for item in response.data])
    return all_embeddings

_gdelt_model = None
def embed_with_minilm(texts: List[str]) -> List[List[float]]:
    global _gdelt_model
    if _gdelt_model is None:
        _gdelt_model = SentenceTransformer(GDELT_EMBED_MODEL)
    return _gdelt_model.encode(texts).tolist()

# ─────────────────────────────────────────────
# COLLECTION SETUP
# Cosine distance is correct for sentence-transformer and OpenAI embeddings.
# ChromaDB distance is set at creation — existing L2 collections must be
# dropped and recreated once to migrate. This is a one-time operation.
# ─────────────────────────────────────────────

def get_or_recreate_collection(chroma_client, name, expected_distance="cosine"):
    try:
        col = chroma_client.get_collection(name=name)
        meta = col.metadata or {}
        current_distance = meta.get("hnsw:space", "l2")
        if current_distance != expected_distance:
            print(f"    [Migration] {name} uses '{current_distance}' distance — recreating with '{expected_distance}'.")
            chroma_client.delete_collection(name=name)
            marks = get_watermarks()
            if name in marks:
                del marks[name]
                with open(WATERMARK_FILE, 'w') as f:
                    json.dump(marks, f, indent=4)
            col = chroma_client.create_collection(
                name=name,
                metadata={"hnsw:space": expected_distance}
            )
            print(f"    [Migration] Collection recreated. Full re-embed will run this cycle.")
        return col
    except Exception:
        # Collection doesn't exist yet — create fresh
        try:
            chroma_client.delete_collection(name=name)
        except Exception:
            pass
        return chroma_client.create_collection(
            name=name,
            metadata={"hnsw:space": expected_distance}
        )

# ─────────────────────────────────────────────
# SYNC ENGINE
# ─────────────────────────────────────────────

def sync_collection(con, chroma_client, table_path, collection_name, text_cols, id_col,
                    sentiment_col="sentiment", embed_fn=None, max_rows=None, lookback_hours=None):
    print(f"\n[Vector Sync] Syncing {collection_name} ...")

    collection = get_or_recreate_collection(chroma_client, collection_name, expected_distance="cosine")

    watermarks      = get_watermarks()
    latest_timestamp = watermarks.get(collection_name)

    print(f"    > Current Chroma count: {collection.count()}")
    if latest_timestamp:
        print(f"    > Watermark: {latest_timestamp}")

    select_parts = []
    for c in text_cols:
        if "array" in c:
            select_parts.append(f"substring(array_to_string({c}, ', '), 1, 2000) as {c}")
        else:
            select_parts.append(f"substring({c}, 1, 2000) as {c}")

    src_val = "source" if "news" in collection_name else "'GDELT' as source"
    select_list = ", ".join(set([id_col] + select_parts + ["ingested_at", src_val, f"{sentiment_col} as sentiment"]))

    try:
        query = f"SELECT {select_list} FROM delta_scan('{table_path}')"
        filters = []
        if latest_timestamp:
            filters.append(f"ingested_at > '{latest_timestamp}'")
        if lookback_hours:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
            filters.append(f"ingested_at >= '{cutoff}'")
            print(f"    > Hard lookback cap: last {lookback_hours}h (cutoff: {cutoff[:19]})")
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY ingested_at ASC"
        if max_rows:
            query += f" LIMIT {max_rows}"

        print(f"    > Fetching data from Delta...")
        df = con.execute(query).df()
    except Exception as e:
        print(f"    > ERROR reading {collection_name}: {e}")
        return

    if df.empty:
        print("    > No new records found. Skipping.")
        return

    print(f"    > Found {len(df)} new records to embed.")

    import time
    batch_size = 50
    total_new  = len(df)

    for i in range(0, total_new, batch_size):
        batch_start = time.time()
        batch = df.iloc[i:i + batch_size].drop_duplicates(subset=[id_col])

        # Build text for embedding
        if collection_name == "silver_gdelt_enriched":
            combined_text = batch.apply(build_gdelt_sentence, axis=1).tolist()
        else:
            combined_text = batch[text_cols].apply(
                lambda row: " | ".join(row.fillna("").astype(str))[:2000], axis=1
            ).tolist()

        encode_start = time.time()
        embeddings   = embed_fn(combined_text)
        encode_time  = time.time() - encode_start

        ids = [f"{collection_name}_{str(x)}" for x in batch[id_col].tolist()]
        metadatas = []
        for _, row in batch.iterrows():
            ingested_at = str(row["ingested_at"])
            try:
                ts = int(datetime.fromisoformat(ingested_at.replace('Z', '+00:00')).timestamp())
            except Exception:
                ts = 0
            metadatas.append({
                "ingested_at":       ingested_at,
                "ingested_timestamp": ts,
                "source":            row.get("source", "GDELT"),
                "sentiment":         float(row.get("sentiment", 0.0) or 0.0),
            })

        collection.upsert(ids=ids, embeddings=embeddings, documents=combined_text, metadatas=metadatas)

        batch_time = time.time() - batch_start
        print(f"    > Batch {i // batch_size + 1} | {len(batch)} rows | Encode: {encode_time:.1f}s | Total: {batch_time:.1f}s")

    new_max = df['ingested_at'].max()
    save_watermark(collection_name, new_max)
    print(f"    > Watermark updated to: {new_max}")


def main():
    print("=" * 60)
    print(" PredictIQ Vector Bridge v2 (Cosine + OpenAI News Embeddings)")
    print("=" * 60)

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)

    # News → OpenAI text-embedding-3-small
    # lookback_hours=72 matches the inference RAG window — older news adds no signal.
    # max_rows hard-caps a migration-wipe backfill so it can't blow the OpenAI budget.
    sync_collection(
        con, chroma_client,
        SILVER_NEWS,
        "silver_news_enriched",
        text_cols=["title", "full_text"],
        id_col="link",
        sentiment_col="sentiment_score",
        embed_fn=embed_with_openai,
        lookback_hours=72,
        max_rows=4000,
    )

    # GDELT → local MiniLM + natural-language sentence builder
    sync_collection(
        con, chroma_client,
        SILVER_GDELT,
        "silver_gdelt_enriched",
        text_cols=["persons_array", "themes_array", "orgs_array"],
        id_col="gkg_record_id",
        sentiment_col="0.0",
        embed_fn=embed_with_minilm,
        max_rows=5000,
        lookback_hours=48,
    )

    print("\n[Vector Sync] Bridge Synchronization Complete.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
