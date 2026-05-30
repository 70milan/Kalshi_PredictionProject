#!/usr/bin/env python3
"""
Explore ChromaDB — the vector store that powers RAG.

ChromaDB stores embeddings of news articles + GDELT events.
The LLM uses these embeddings to retrieve relevant context when writing briefs.

Structure:
  data/chroma/
    ├── index.bin        (annoy index for similarity search)
    ├── data.parquet     (the actual documents + embeddings + metadata)
    └── ...

How it's fed:
  1. rag/embed_silver_data.py runs on each ETL cycle
  2. Reads new articles from silver.news_articles_enriched
  3. Embeds them with OpenAI text-embedding-3-small (1536 dimensions)
  4. Inserts into ChromaDB with metadata (sentiment, source, date)
  5. LLM queries: "Find articles similar to KXTW market context"
  6. ChromaDB returns K-nearest neighbors (top 5 articles)
  7. LLM writes brief based on retrieved context + embeddings
"""

import os
import sys
import json
from pathlib import Path

try:
    import chromadb
    from chromadb.config import Settings
except ImportError:
    print("❌ ChromaDB not installed. Install with: pip install chromadb")
    sys.exit(1)

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────

CHROMA_DIR = Path("C:/Data Engineering/codeprep/predection_project/data/chroma")

def init_chromadb():
    """Connect to the live ChromaDB instance."""
    if not CHROMA_DIR.exists():
        print(f"❌ ChromaDB directory not found: {CHROMA_DIR}")
        print("   First run: rag/embed_silver_data.py to initialize ChromaDB")
        sys.exit(1)

    settings = Settings(
        chroma_db_impl="duckdb+parquet",
        persist_directory=str(CHROMA_DIR),
        anonymized_telemetry=False
    )

    client = chromadb.Client(settings)
    return client

# ─────────────────────────────────────────────
# STEP 1: LIST COLLECTIONS
# ─────────────────────────────────────────────

def step_1_collections(client):
    """Show what collections (indices) exist."""
    print("\n" + "="*70)
    print("STEP 1: CHROMADB COLLECTIONS")
    print("="*70)

    collections = client.list_collections()
    print(f"\n📚 Found {len(collections)} collections:\n")

    for coll in collections:
        count = coll.count()
        print(f"  📖 {coll.name}")
        print(f"     └─ {count} documents stored")
        print(f"     └─ Metadata: {coll.metadata if hasattr(coll, 'metadata') else 'None'}")

    print("\n💡 LEARNING: Each collection is a separate vector index.")
    print("   - Typical collections: 'news_articles', 'gdelt_events'")
    print("   - Each embedding is 1536-dim (OpenAI) or 384-dim (sentence-transformers)")

    return collections

# ─────────────────────────────────────────────
# STEP 2: INSPECT COLLECTION METADATA
# ─────────────────────────────────────────────

def step_2_collection_stats(client, collection_name):
    """Show what's stored in a collection."""
    print("\n" + "="*70)
    print(f"STEP 2: COLLECTION STATS — {collection_name}")
    print("="*70)

    try:
        coll = client.get_collection(name=collection_name)
        count = coll.count()

        print(f"\n📊 {collection_name}:")
        print(f"   Total documents: {count}")

        # Sample a few documents
        print(f"\n   📄 Sample documents (first 5):")

        try:
            # Get all data (or limit to 5 for performance)
            results = coll.get(limit=5)

            for i, (doc_id, document, metadata) in enumerate(
                zip(results['ids'], results['documents'], results['metadatas'])
            ):
                print(f"\n      [{i+1}] ID: {doc_id}")
                print(f"          Text: {document[:100]}...")
                print(f"          Metadata: {metadata}")

        except Exception as e:
            print(f"      ⚠️  Could not retrieve documents: {e}")

        print("\n💡 LEARNING: Each document has:")
        print("   - id: unique identifier")
        print("   - document: the text (article/event title)")
        print("   - embedding: 1536-dim vector (computed by OpenAI)")
        print("   - metadata: source, sentiment, date, etc.")

    except Exception as e:
        print(f"❌ Could not access collection: {e}")

# ─────────────────────────────────────────────
# STEP 3: SIMILARITY SEARCH (WHAT LLM DOES)
# ─────────────────────────────────────────────

def step_3_similarity_search(client, collection_name, query_text, n_results=5):
    """Simulate what the LLM does: query for similar documents."""
    print("\n" + "="*70)
    print(f"STEP 3: SIMILARITY SEARCH — {collection_name}")
    print("="*70)

    try:
        coll = client.get_collection(name=collection_name)

        print(f"\n🔎 Query: '{query_text}'")
        print(f"   Looking for top {n_results} similar documents...\n")

        # ChromaDB does semantic search
        results = coll.query(
            query_texts=[query_text],
            n_results=n_results,
            include=["documents", "metadatas", "distances"]
        )

        if results['documents'] and len(results['documents'][0]) > 0:
            for i, (doc, metadata, distance) in enumerate(
                zip(results['documents'][0], results['metadatas'][0], results['distances'][0])
            ):
                similarity = 1 - (distance / 2)  # Convert distance to similarity (0-1)
                print(f"   [{i+1}] Similarity: {similarity:.3f}")
                print(f"       Text: {doc[:120]}...")
                print(f"       Metadata: {metadata}\n")
        else:
            print("   ⚠️  No similar documents found")

        print("💡 LEARNING: This is RAG (Retrieval-Augmented Generation).")
        print("   1. LLM needs context for market KXTW")
        print("   2. ChromaDB finds top-5 articles about Taiwan (by embeddings)")
        print("   3. LLM reads retrieved articles + sees their sentiment")
        print("   4. LLM writes: 'Bull case: Taiwan tensions rise → YES underpriced'")

    except Exception as e:
        print(f"❌ Query failed: {e}")

# ─────────────────────────────────────────────
# STEP 4: UNDERSTAND METADATA
# ─────────────────────────────────────────────

def step_4_metadata_schema(client, collection_name):
    """Show what metadata is stored (source, sentiment, date, etc.)."""
    print("\n" + "="*70)
    print(f"STEP 4: METADATA SCHEMA — {collection_name}")
    print("="*70)

    try:
        coll = client.get_collection(name=collection_name)

        # Sample metadata from a few docs
        results = coll.get(limit=10)

        if results['metadatas']:
            print("\n📋 Metadata fields found in documents:\n")

            all_keys = set()
            for meta in results['metadatas']:
                if meta:
                    all_keys.update(meta.keys())

            for key in sorted(all_keys):
                # Show example values
                examples = []
                for meta in results['metadatas']:
                    if meta and key in meta:
                        examples.append(meta[key])
                        if len(examples) >= 2:
                            break

                print(f"   📌 {key}")
                for ex in examples[:2]:
                    print(f"      Example: {ex}")
                print()

        print("💡 LEARNING: Common metadata fields:")
        print("   - source: 'nyt', 'bbc', 'cnn' (which news feed)")
        print("   - sentiment: -1.0 to +1.0 (VADER score)")
        print("   - published_at: '2026-05-29 14:32:00' (when article published)")
        print("   - feed_key: same as source, but standardized")
        print("   - theme: 'POLITICS', 'DEFENSE' (GDELT themes if from GKG)")

    except Exception as e:
        print(f"❌ Could not inspect metadata: {e}")

# ─────────────────────────────────────────────
# STEP 5: EMBEDDING STATS (WHAT OPENAI COMPUTED)
# ─────────────────────────────────────────────

def step_5_embedding_stats(client, collection_name):
    """Show embedding statistics."""
    print("\n" + "="*70)
    print(f"STEP 5: EMBEDDING STATISTICS — {collection_name}")
    print("="*70)

    try:
        coll = client.get_collection(name=collection_name)
        count = coll.count()

        print(f"\n🧠 Embedding info:")
        print(f"   Total documents embedded: {count}")
        print(f"   Embedding model: OpenAI text-embedding-3-small (for news)")
        print(f"                    sentence-transformers all-MiniLM-L6-v2 (for GDELT)")
        print(f"   Dimensionality: 1536 (OpenAI) or 384 (sentence-transformers)")
        print(f"   Indexing: Annoy (approximate nearest neighbor)")

        print(f"\n💡 LEARNING: How embeddings are used:")
        print(f"   1. Article text → OpenAI text-embedding-3-small → 1536-dim vector")
        print(f"   2. Vector stored in ChromaDB with metadata")
        print(f"   3. LLM asks: 'Find articles similar to Taiwan market'")
        print(f"   4. ChromaDB: Compute query embedding → Find K-nearest neighbors")
        print(f"   5. Return top-5 articles + their sentiment to LLM")

        # Try to estimate storage size
        print(f"\n📦 Estimated storage:")
        print(f"   ~{count * 1536 * 4 / 1024 / 1024:.1f} MB (embeddings only)")
        print(f"   +{count * 200 / 1024 / 1024:.1f} MB (text documents)")
        print(f"   +{count * 100 / 1024 / 1024:.1f} MB (metadata)")

    except Exception as e:
        print(f"❌ Could not compute stats: {e}")

# ─────────────────────────────────────────────
# STEP 6: FLOW DIAGRAM
# ─────────────────────────────────────────────

def step_6_flow_diagram():
    """Show how data flows into ChromaDB and out to LLM."""
    print("\n" + "="*70)
    print("STEP 6: DATA FLOW DIAGRAM")
    print("="*70)

    print("""
ETL PIPELINE → CHROMADB → LLM

Bronze Layer (Raw data)
  ├─ news/bbc/bbc_*.parquet
  ├─ news/nyt/nyt_*.parquet
  └─ gdelt/gdelt_events/gdelt_*.parquet

           ↓ (Transform + VADER sentiment)

Silver Layer (Cleaned, ACID Delta)
  ├─ silver/news_articles_enriched/
  │  └─ [id, title, full_text, sentiment_score, feed_key, ...]
  └─ silver/gdelt_events_current/
     └─ [date, actor1_name, tone, goldstein_scale, ...]

           ↓ (Phase 2: rag/embed_silver_data.py)

ChromaDB (Vector Index)
  ├─ Collection: "news_articles"
  │  ├─ [1536-dim embedding] → OpenAI text-embedding-3-small
  │  └─ metadata: sentiment, source, date, ...
  └─ Collection: "gdelt_events"
     ├─ [384-dim embedding] → sentence-transformers
     └─ metadata: tone, goldstein, actors, ...

           ↓ (Phase 3: inference/predict_movements.py)

Groq LLM (llama-3.3-70b-versatile)
  1. Takes market ticker: KXTW
  2. Queries ChromaDB: "Find articles about Taiwan"
  3. Gets top-5 articles + metadata (sentiment: +0.45, -0.32, ...)
  4. Reads Gold mispricing_score: 75 (bullish signal)
  5. Writes brief:
     Bull case: "Taiwan tensions easing, positive sentiment (avg +0.4)"
     Bear case: "War risk priced in, market already at 0.45"
     Verdict: "BUY YES (underpriced)"

           ↓ (Result)

Gold Layer (Intelligence Briefs)
  └─ gold/intelligence_briefs/*.parquet
     └─ [ticker, verdict, bull_case, bear_case, confidence, ...]

           ↓ (React Dashboard + Telegram Alerts)

API Endpoint: /api/intelligence → Market Pricing Recommendation
""")

    print("\n💡 KEY INSIGHT:")
    print("   ChromaDB is the 'memory' that gives the LLM context.")
    print("   Without it, LLM would write generic briefs.")
    print("   WITH it, LLM writes briefs grounded in recent news/events.")

# ─────────────────────────────────────────────
# MAIN MENU
# ─────────────────────────────────────────────

def main():
    print("\n" + "="*70)
    print("🗂️  CHROMADB EXPLORER")
    print("   Understand the vector store that powers RAG")
    print("="*70)

    try:
        client = init_chromadb()
        print("✅ Connected to ChromaDB")
    except Exception as e:
        print(f"❌ Failed to initialize ChromaDB: {e}")
        return

    collections = []

    while True:
        print("\n" + "-"*70)
        print("Choose a step to explore:")
        print("-"*70)
        print("1️⃣  List ChromaDB collections")
        print("2️⃣  Inspect a collection (stats, samples)")
        print("3️⃣  Similarity search (what LLM retrieves)")
        print("4️⃣  Metadata schema")
        print("5️⃣  Embedding statistics")
        print("6️⃣  Show data flow diagram")
        print("0️⃣  Exit")

        choice = input("\n👉 Enter choice (0-6): ").strip()

        if choice == "0":
            print("\n✋ Goodbye!\n")
            break

        elif choice == "1":
            collections = step_1_collections(client)

        elif choice == "2":
            if not collections:
                collections = client.list_collections()

            print(f"\n   Available collections: {[c.name for c in collections]}")
            coll_name = input("   Enter collection name: ").strip()
            if coll_name:
                step_2_collection_stats(client, coll_name)

        elif choice == "3":
            if not collections:
                collections = client.list_collections()

            print(f"\n   Available collections: {[c.name for c in collections]}")
            coll_name = input("   Enter collection name: ").strip()
            query = input("   Enter query text (e.g., 'Taiwan tensions'): ").strip()
            if coll_name and query:
                step_3_similarity_search(client, coll_name, query)

        elif choice == "4":
            if not collections:
                collections = client.list_collections()

            print(f"\n   Available collections: {[c.name for c in collections]}")
            coll_name = input("   Enter collection name: ").strip()
            if coll_name:
                step_4_metadata_schema(client, coll_name)

        elif choice == "5":
            if not collections:
                collections = client.list_collections()

            print(f"\n   Available collections: {[c.name for c in collections]}")
            coll_name = input("   Enter collection name: ").strip()
            if coll_name:
                step_5_embedding_stats(client, coll_name)

        elif choice == "6":
            step_6_flow_diagram()

        else:
            print("   ⚠️  Invalid choice")

if __name__ == "__main__":
    main()
