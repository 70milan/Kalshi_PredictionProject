-- ─────────────────────────────────────────────────────────────────────────────
-- PREDICTIQ — DUCKDB VIEWS  |  MEDALLION LAKEHOUSE
-- ─────────────────────────────────────────────────────────────────────────────
--  BRONZE  →  read_parquet()   (raw .parquet files, no transaction log)
--  SILVER  →  delta_scan()     (Delta Lake managed by PySpark, has _delta_log)
--  GOLD    →  delta_scan()     (Delta Lake managed by PySpark, has _delta_log)
--
--  WARNING: Never use read_parquet wildcards on Silver/Gold tables.
--  Ghost/deleted Parquet files from MERGE/UPSERT operations will cause
--  duplicate and stale records in every downstream query.
-- ─────────────────────────────────────────────────────────────────────────────

-- Delta extension (required for Silver & Gold)
INSTALL delta;
LOAD delta;

-- Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- =============================================================================
-- 1. BRONZE LAYER — Raw Parquet Ingestion
--
--  kalshi_open   → Two-File Pattern: latest.parquet + markets_*.parquet
--  kalshi_closed → Multi-prefix pattern: daily_closed_* + historical_closed_*
--                  Captured with: *_closed_*.parquet
--  kalshi_settled→ Multi-prefix pattern: daily_settled_* + historical_settled_*
--                  Captured with: *_settled_*.parquet
-- =============================================================================

-- ── Kalshi Markets ───────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW bronze.kalshi_open_latest AS
    SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/open/latest.parquet');

-- open: standard Two-File Pattern (latest.parquet + markets_*.parquet)
CREATE OR REPLACE VIEW bronze.kalshi_open AS
    SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/open/markets_*.parquet', union_by_name = true);

-- closed: dual-prefix naming (daily_closed_* and historical_closed_*)
CREATE OR REPLACE VIEW bronze.kalshi_closed AS
    SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/closed/*_closed_*.parquet', union_by_name = true);

-- settled: dual-prefix naming (daily_settled_* and historical_settled_*)
CREATE OR REPLACE VIEW bronze.kalshi_settled AS
    SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/settled/*_settled_*.parquet', union_by_name = true);


-- ── GDELT Global Knowledge Graph ─────────────────────────────────────────────

CREATE OR REPLACE VIEW bronze.gdelt_events_latest AS
    SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_events/latest.parquet');

CREATE OR REPLACE VIEW bronze.gdelt_events AS
    SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_events/gdelt_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.gdelt_gkg_latest AS
    SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_gkg/latest.parquet');

CREATE OR REPLACE VIEW bronze.gdelt_gkg AS
    SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_gkg/gkg_*.parquet', union_by_name = true);


-- ── News Sentiment Feeds  (live under data/bronze/news/<outlet>/) ─────────────

CREATE OR REPLACE VIEW bronze.news_bbc_latest AS
    SELECT * FROM read_parquet('P:/data/bronze/bbc/latest.parquet');

CREATE OR REPLACE VIEW bronze.news_bbc AS
    SELECT * FROM read_parquet('P:/data/bronze/bbc/bbc_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_cnn_latest AS
    SELECT * FROM read_parquet('P:/data/bronze/cnn/latest.parquet');

CREATE OR REPLACE VIEW bronze.news_cnn AS
    SELECT * FROM read_parquet('P:/data/bronze/cnn/cnn_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_foxnews_latest AS
    SELECT * FROM read_parquet('P:/data/bronze/foxnews/latest.parquet');

CREATE OR REPLACE VIEW bronze.news_foxnews AS
    SELECT * FROM read_parquet('P:/data/bronze/foxnews/foxnews_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_nypost_latest AS
    SELECT * FROM read_parquet('P:/data/bronze/nypost/latest.parquet');

CREATE OR REPLACE VIEW bronze.news_nypost AS
    SELECT * FROM read_parquet('P:/data/bronze/nypost/nypost_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_nyt_latest AS
    SELECT * FROM read_parquet('P:/data/bronze/nyt/latest.parquet');

CREATE OR REPLACE VIEW bronze.news_nyt AS
    SELECT * FROM read_parquet('P:/data/bronze/nyt/nyt_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_thehindu_latest AS
    SELECT * FROM read_parquet('P:/data/bronze/thehindu/latest.parquet');

CREATE OR REPLACE VIEW bronze.news_thehindu AS
    SELECT * FROM read_parquet('P:/data/bronze/thehindu/thehindu_*.parquet', union_by_name = true);


-- =============================================================================
-- 2. SILVER LAYER — Delta Lake (PySpark MERGE / Overwrite)
--    MUST use delta_scan() pointed at the table root folder.
--    delta_scan() reads the _delta_log to reconstruct the correct live state.
-- =============================================================================

CREATE OR REPLACE VIEW silver.gdelt_events_history AS
    SELECT * FROM delta_scan('P:/data/silver/gdelt_events_history');

CREATE OR REPLACE VIEW silver.gdelt_events_current AS
    SELECT * FROM delta_scan('P:/data/silver/gdelt_events_current');

CREATE OR REPLACE VIEW silver.gdelt_gkg_history AS
    SELECT * FROM delta_scan('P:/data/silver/gdelt_gkg_history');

CREATE OR REPLACE VIEW silver.gdelt_gkg_current AS
    SELECT * FROM delta_scan('P:/data/silver/gdelt_gkg_current');

CREATE OR REPLACE VIEW silver.kalshi_markets_current AS
    SELECT * FROM delta_scan('P:/data/silver/kalshi_markets_current');

CREATE OR REPLACE VIEW silver.kalshi_markets_history AS
    SELECT * FROM delta_scan('P:/data/silver/kalshi_markets_history');

CREATE OR REPLACE VIEW silver.news_articles_enriched AS
    SELECT * FROM delta_scan('P:/data/silver/news_articles_enriched');


-- =============================================================================
-- 3. GOLD LAYER — Delta Lake (PySpark Overwrite + Append history)
--    Same rule as Silver: delta_scan() only — never read_parquet wildcards.
-- =============================================================================

CREATE OR REPLACE VIEW gold.gdelt_summaries AS
    SELECT * FROM delta_scan('P:/data/gold/gdelt_summaries');

CREATE OR REPLACE VIEW gold.gdelt_summaries_history AS
    SELECT * FROM delta_scan('P:/data/gold/gdelt_summaries_history');

CREATE OR REPLACE VIEW gold.news_summaries AS
    SELECT * FROM delta_scan('P:/data/gold/news_summaries');

CREATE OR REPLACE VIEW gold.news_summaries_history AS
    SELECT * FROM delta_scan('P:/data/gold/news_summaries_history');

CREATE OR REPLACE VIEW gold.mispricing_scores AS
    SELECT * FROM delta_scan('P:/data/gold/mispricing_scores');

CREATE OR REPLACE VIEW gold.mispricing_scores_history AS
    SELECT * FROM delta_scan('P:/data/gold/mispricing_scores_history');