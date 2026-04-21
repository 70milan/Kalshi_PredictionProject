# Implementation Plan: GDELT Historical Unlock (RAG Expansion)

We will integrate your 36,000+ GDELT events into the Inference Engine to resolve the "Date Gap" and provide reasoning for historical March markets.

## User Review Required

> [!IMPORTANT]
> **Priority Merging**: We will search both News and GDELT collections. If both return matches, we will merge them, prioritizing News for readability but using GDELT as the primary "Historical Alibi". 

## Proposed Changes

### [MODIFY] [explain_mispricing.py](file:///c:/Data%20Engineering/codeprep/predection_project/inference/explain_mispricing.py)

#### 1. Dual Collection Connection
- Update the `main()` function to connect to **both** `silver_news_enriched` and `silver_gdelt_enriched` collections.

#### 2. Cross-Collection RAG Search
- Modify `fetch_rag_context` to accept a **list of collections** or iterate through both.
- Modify `cascading_rag_search` to pass both collections down the 15m -> 2h cascade.
- Aggregate all scored results before applying the 65% similarity high-pass filter.

#### 3. Source Attribution
- Ensure the `source` field in the final context reflects whether the data came from GDELT or a News RSS feed, allowing Gemini to cite its sources correctly.

## Verification Plan

### Automated Verification
1. **The Historical Run**: Execute `python inference/explain_mispricing.py`.
2. **Success Confirmation**: Verify that markets that were previously flagged as "GHOST PUMP" (like the SAVE America Act vote) now have **actual reasoning** based on GDELT events.

### Manual Verification
1. **DBeaver Audit**: Run `SELECT * FROM delta_scan('data/gold/intelligence_briefs')` and check for verdicts that reference GDELT news. 
