# Schema: Global News Intelligence
**Scripts:** `flat_bbc_ingest.py`, `flat_cnn_ingest.py`, `flat_fox_ingest.py`, `flat_nypost_ingest.py`, `flat_hindu_ingest.py`  
**Output:** `data/bronze/news/[source]/latest.parquet` & `data/bronze/news/[source]/[source]_[timestamp].parquet`

| Field | Type | Description |
| :--- | :--- | :--- |
| `source` | STRING | Originating news house (e.g., "BBC", "CNN", "Fox News", "NY Post", "The Hindu"). |
| `title` | STRING | Article headline. |
| `link` | STRING | Canonical URL of the news article. Used for programmatic deduplication. |
| `published_at` | STRING | Original publication datetime extracted from the RSS feed payload. |
| `summary` | STRING | Brief text snippet/summary provided natively by the RSS feed structure. |
| `full_text` | STRING | The main content extracted from the webpage. Crucial for Gold Layer VADER sentiment. |
| `scraped` | BOOLEAN | `True` if the full text was successfully extracted via Trafilatura. |
| `ingested_at` | STRING | UTC ISO timestamp showing when this record entered PredictIQ. |
