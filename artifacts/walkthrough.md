# Walkthrough: Phase 4 Inference Engine & Intelligence Layer

We have successfully transitioned PredictIQ from a batch ETL pipeline into a real-time **Geopolitical Intelligence Engine**. The system now identifies market anomalies and explains them using local semantic search and Gemini 1.5 Pro.

## Key Accomplishments

### 1. The Vector Bridge (Local RAG)
We implemented [embed_silver_data.py](file:///c:/Data%20Engineering/codeprep/predection_project/rag/embed_silver_data.py) which acts as a durable bridge between your Delta Lake and your semantic search layer.
*   **Local Processing**: Uses `all-MiniLM-L6-v2` for FREE, local embeddings (Zero API cost).
*   **Incremental Sync**: Uses a watermark pattern to only embed new news/GDELT events.
*   **Deduplication**: Automatically handles duplicate news articles to maintain index integrity.

### 2. Market-Led Inference Engine
We finalized [explain_mispricing.py](file:///c:/Data%20Engineering/codeprep/predection_project/inference/explain_mispricing.py) with the sophisticated "Odd-Delta First" strategy.
*   **Cascading Search**: Searches the last 15 minutes of news for "Flash" context, then cascades to a 2-hour window to find "Lagging" signals.
*   **Bookmaker Agent**: Integrated Gemini 1.5 Pro to synthesize polarized news into actionable **Bull/Bear/Verdict** briefs.

### 3. Full Automation
We updated [run_etl.py](file:///c:/Data%20Engineering/codeprep/predection_project/orchestration/run_etl.py) to include the new phases.
*   **Phase 3**: Vector Bridge Sync.
*   **Phase 4**: AI Inference & Mispricing detection.
*   The system now runs this entire loop every 5 minutes flawlessly.

---

## Infrastructure Updates

> [!IMPORTANT]
> **Persistent Connection**: We moved DBeaver to use `predictiq.duckdb` inside the `database/` folder. Your views and schemas are now permanent and will not disappear when you wipe your data folders.

> [!TIP]
> **Gemini API Key**: Your key (`Gemini_API`) is safely stored in the `.env` file and is being correctly loaded by the Inference Engine.

---

## How to Monitor the Intelligence briefs
Every 5 minutes, the orchestrator will scan for volatile markets. If it finds one (e.g., a tick move > 10%), it will output the brief to your console:

```json
--- INTELLIGENCE BRIEF [TICKER_NAME] ---
BULL: Why the YES side is underpriced...
BEAR: Why the NO side is underpriced...
VERDICT: The ultimate bookmaker ruling...
```

You can now use your **DBeaver** connection to query the Gold layer and see these signals in real-time as they are indexed!
