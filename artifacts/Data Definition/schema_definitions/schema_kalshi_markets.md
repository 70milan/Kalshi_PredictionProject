# Schema: Kalshi Prediction Markets

**Script:** `flat_kalshi_markets_active.py` (and `flat_kalshi_markets_historical.py`)  
**Output:** `data/bronze/kalshi_markets/[open/historical]/latest.parquet`

| Field | Type | Description |
| :--- | :--- | :--- |
| `ticker` | STRING | ID for the specific prediction market (e.g. `KX-2024-PRES`). |
| `series_ticker` | STRING | Broad parent series ID (e.g. `Politics`). |
| `title` | STRING | Human-readable market question (e.g. "Will Trump win the election?"). |
| `yes_bid_dollars` | DECIMAL | Current "Yes" bid price ($0.00 to $1.00). Represents implied probability. |
| `yes_ask_dollars` | DECIMAL | Current "Yes" ask price ($0.00 to $1.00). |
| `last_price` | DECIMAL | The price of the most recent trade ($0.00 to $1.00). |
| `volume` | INT | Total contracts traded in the current cycle. |
| `open_interest` | INT | Number of open contracts yet to be settled. |
| `status` | STRING | State of the market (`open`, `closed`, `settled`). |
| `rules_primary` | STRING | Plain-text rules and conditions for market settlement. |
| `expiration_time` | STRING | DateTime when the market will close for trading. |
| `liquidity` | DECIMAL | Measure of market depth. |
| `ingested_at` | STRING | UTC ISO timestamp showing when this record entered PredictIQ. |
| `status_pulled` | STRING | The flag used during ingestion (`open` or `settled`). |
