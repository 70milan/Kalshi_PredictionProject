# PredictIQ Technical Architecture Manifesto
## Idempotency, Watermarking, and Complexity Analysis

This document outlines the high-performance data engineering patterns used to ensure the PredictIQ pipeline is stable, scalable, and idempotent.

---

## 1. Idempotency & $O(1)$ Lookups
### Ingestion Layer: "Exactly-Once Ingestion"

**Pattern**: The `.seen_urls` Bloom-Filter Substitute.
To prevent duplicate article ingestion, RSS scripts use a persistent Set-based deduplication pattern.

> [!TIP]
> **Complexity: $O(1)$ Lookup**
> Scaling is constant. Whether the database has 1,000 or 1,000,000 URLs, checking if an article is new takes exactly the same amount of time because it uses a Hash-Set.

#### Code Snippet (`nyt_news_ingest.py`)
```python
def get_seen_urls():
    # Persistent state stored on disk
    if os.path.exists(".seen_urls"):
        with open(".seen_urls", "r") as f:
            return set(line.strip() for line in f) # Hash-Set for O(1) lookup
    return set()

# Filtering logic
new_items = [entry for entry in feed.entries if entry.link not in seen_urls]
```

---

## 2. High-Watermarking
### Transformation Layer: "Incremental Loading"

**Pattern**: The State-Aware Transformation.
Transform scripts do not "re-read the world." They calculate a High-Watermark (Timestamp) from the Silver table and only pull Bronze data that is newer than that mark.

> [!IMPORTANT]
> **Complexity: $O(N_{\Delta})$**
> The script only processes the "Delta" (new records), drastically reducing compute cost as the dataset grows over months.

#### Code Snippet (`silver_news_transform.py`)
```python
# Create High-Watermark
max_ingested_at = spark.read.format("delta").load(SILVER_PATH) \
    .select(max("ingested_at")).collect()[0][0]

# Filter Bronze Source
new_data = spark.read.parquet(BRONZE_PATH) \
    .filter(col("ingested_at") > max_ingested_at) # Incremental Scan
```

---

## 3. Idempotent Sinks
### "Fail-Safe Writing"

**Pattern**: Overwrite vs. Append Partitioning.
*   **Bronze**: Uses SNAPSHOT partitioning (`gdelt_{timestamp}.parquet`). If a job restarts, it simply writes a new file. The system is "Add-Only," making it physically impossible to corrupt existing data.
*   **Silver/Gold**: Uses Delta Lake's ACID transactions for transactional integrity.

#### Code Snippet (`silver_kalshi_transform.py`)
```python
# Deduplication before Sink
# Ensures that even if Ingestion ran twice, we only keep the latest price
df_clean = df_raw.withColumn("rn", row_number().over(
    Window.partitionBy("ticker").orderBy(desc("ingested_at"))
)).filter("rn = 1")

# Idempotent write
df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)
```

---

## 4. The JSON Bridge Pattern
### Enrichment Layer: "Driver-Side Vertical Scaling"

**Pattern**: Python-native enrichment inside a Spark context.
To avoid PySpark worker overhead and library conflicts (VADER/Groq), we pull data to the Spark Driver as a JSON structure, process it via high-performance Pandas/Vanilla Python, and send it back to Spark.

> [!NOTE]
> **Complexity: $O(N)$**
> This is a linear scan on the driver, optimized for enrichment tasks where distributed overhead would outweigh the benefits.

#### Code Snippet (`silver_news_transform.py`)
```python
# Bridge Pattern
rows = df.collect() # Collect to Driver
enriched_data = []

for row in rows:
    # High-performance local enrichment
    sentiment = analyzer.polarity_scores(row['full_text'])['compound']
    enriched_data.append(...)

# Send back to Distributed Layer
df_final = spark.createDataFrame(enriched_data)
```
