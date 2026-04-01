# Schema: News RSS (Reuters & NYT)

**Scripts:** `flat_reuters_news_ingest.py`, `flat_nyt_news_ingest.py`  
**Output:** `data/bronze/[reuters/nyt]/[source]_[timestamp].parquet`

| Field | Type | Description |
| :--- | :--- | :--- |
| `source` | STRING | Originating news house (e.g., "Reuters", "NYT"). |
| `title` | STRING | Article headline. |
| `link` | STRING | Canonical URL of the news article. Used for deduplication. |
| `published_at` | STRING | RFC 822 publication date from the RSS feed. |
| `summary` | STRING | Brief text snippet/summary provided by the RSS feed. |
| `full_text` | STRING | The main content extracted from the webpage. Falls back to summary if scraping fails. |
| `scraped` | BOOLEAN | `True` if the full text was successfully extracted via Trafilatura. |
| `ingested_at` | STRING | UTC ISO timestamp showing when this record entered PredictIQ. |
