import os
import sys

__import__('pysqlite3')
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')

import chromadb
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHROMA_PATH  = os.path.join(PROJECT_ROOT, "data", "chroma")

def main():
    print("=" * 60)
    print(" PredictIQ Chroma Explorer (The Semantic Camera)")
    print("=" * 60)

    if not os.path.exists(CHROMA_PATH):
        print(f"[Error] Chroma directory not found at: {CHROMA_PATH}")
        return

    # 1. Initialize Client
    client = chromadb.PersistentClient(path=CHROMA_PATH)

    # 2. List Collections
    collections = client.list_collections()
    if not collections:
        print("[Inventory] No collections found. Is the Vector Bridge running?")
        return

    print(f"[Inventory] Found {len(collections)} collections:\n")
    
    for col in collections:
        count = col.count()
        print(f"--- COLLECTION: {col.name} ({count} items) ---")
        
        if count == 0:
            print("    > (Empty)")
            continue

        # 3. Peek at the latest items
        # Note: 'peek' returns the first N items by ID or internal order
        # For a true 'latest' view, we would query by ingested_timestamp
        # But 'peek' is the fastest way to verify data integrity.
        sample = col.peek(limit=3)
        
        for i in range(len(sample['ids'])):
            doc_id = sample['ids'][i]
            metadata = sample['metadatas'][i]
            content = sample['documents'][i][:100].replace('\n', ' ') + "..."
            
            source = metadata.get('source', 'Unknown')
            ingested = metadata.get('ingested_at', 'N/A')
            sentiment = metadata.get('sentiment', 'N/A')
            
            print(f"    [{i+1}] ID: {doc_id}")
            print(f"        Source: {source} | Sent: {sentiment} | Date: {ingested}")
            print(f"        Snippet: {content}")
        print("-" * 40 + "\n")

if __name__ == "__main__":
    main()
