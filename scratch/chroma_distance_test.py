"""Quick test to verify Chroma distance metric and embedding function alignment."""
import chromadb
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHROMA_PATH = os.path.join(PROJECT_ROOT, "data", "chroma")

client = chromadb.PersistentClient(path=CHROMA_PATH)

# Check what distance function the collections use
for name in ["silver_news_enriched", "silver_gdelt_enriched"]:
    col = client.get_collection(name)
    print(f"\n--- {name} ---")
    print(f"  Count: {col.count()}")
    print(f"  Metadata: {col.metadata}")
    
    # Query with a known political topic
    results = col.query(
        query_texts=["Will Donald Trump pardon people before April 2026"],
        n_results=5,
        include=["documents", "metadatas", "distances"]
    )
    
    if results["documents"] and results["documents"][0]:
        for i in range(len(results["documents"][0])):
            dist = results["distances"][0][i]
            sim_l2 = 1.0 / (1.0 + dist)
            sim_cosine = 1.0 - dist  # only valid for cosine
            src = results["metadatas"][0][i].get("source", "?")
            doc_preview = results["documents"][0][i][:80]
            print(f"  [{i}] dist={dist:.4f}  1/(1+d)={sim_l2:.4f}  1-d={sim_cosine:.4f}  src={src}")
            print(f"      {doc_preview}...")
