-- ─────────────────────────────────────────────
-- PREDICTIQ — DUCKDB VIEWS WITH SCHEMAS
-- ─────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ==============================================                   
-- 1. BRONZE LAYER — RAW INGESTION
-- ==============================================

-- KALSHI MARKETS
CREATE OR REPLACE VIEW bronze.kalshi_open_latest AS SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/open/latest.parquet');
CREATE OR REPLACE VIEW bronze.kalshi_open AS SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/open/markets_*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW bronze.kalshi_closed AS SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/closed/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW bronze.kalshi_settled AS SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/settled/*.parquet', union_by_name = true);

-- GDELT GLOBAL MATRICES
CREATE OR REPLACE VIEW bronze.gdelt_events_latest AS SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_events/latest.parquet');
CREATE OR REPLACE VIEW bronze.gdelt_events AS SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_events/gdelt_*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW bronze.gdelt_gkg_latest AS SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_gkg/latest.parquet');
CREATE OR REPLACE VIEW bronze.gdelt_gkg AS SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_gkg/gkg_*.parquet', union_by_name = true);

-- GLOBAL NEWS SENTIMENT FEEDS
CREATE OR REPLACE VIEW bronze.news_bbc_latest AS SELECT * FROM read_parquet('P:/data/bronze/bbc/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_bbc AS SELECT * FROM read_parquet('P:/data/bronze/bbc/bbc_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_cnn_latest AS SELECT * FROM read_parquet('P:/data/bronze/cnn/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_cnn AS SELECT * FROM read_parquet('P:/data/bronze/cnn/cnn_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_foxnews_latest AS SELECT * FROM read_parquet('P:/data/bronze/foxnews/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_foxnews AS SELECT * FROM read_parquet('P:/data/bronze/foxnews/foxnews_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_nypost_latest AS SELECT * FROM read_parquet('P:/data/bronze/nypost/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_nypost AS SELECT * FROM read_parquet('P:/data/bronze/nypost/nypost_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_nyt_latest AS SELECT * FROM read_parquet('P:/data/bronze/nyt/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_nyt AS SELECT * FROM read_parquet('P:/data/bronze/nyt/nyt_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_thehindu_latest AS SELECT * FROM read_parquet('P:/data/bronze/thehindu/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_thehindu AS SELECT * FROM read_parquet('P:/data/bronze/thehindu/thehindu_*.parquet', union_by_name = true);


-- ==============================================
-- 2. SILVER LAYER — ENRICHED TABLES
-- ==============================================

CREATE OR REPLACE VIEW silver.gdelt_events_history AS SELECT * FROM read_parquet('P:/data/silver/gdelt_events_history/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW silver.gdelt_events_current AS SELECT * FROM read_parquet('P:/data/silver/gdelt_events_current/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW silver.gdelt_gkg_history AS SELECT * FROM read_parquet('P:/data/silver/gdelt_gkg_history/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW silver.gdelt_gkg_current AS SELECT * FROM read_parquet('P:/data/silver/gdelt_gkg_current/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW silver.kalshi_markets_current AS SELECT * FROM read_parquet('P:/data/silver/kalshi_markets_current/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW silver.kalshi_markets_history AS SELECT * FROM read_parquet('P:/data/silver/kalshi_markets_history/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW silver.news_articles_enriched AS SELECT * FROM read_parquet('P:/data/silver/news_articles_enriched/*.parquet', union_by_name = true);


-- ==============================================
-- 3. GOLD LAYER — MISPRICING SCORING ENGINE
-- ==============================================

CREATE OR REPLACE VIEW gold.gdelt_summaries AS SELECT * FROM read_parquet('P:/data/gold/gdelt_summaries/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW gold.gdelt_summaries_history AS SELECT * FROM read_parquet('P:/data/gold/gdelt_summaries_history/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW gold.news_summaries AS SELECT * FROM read_parquet('P:/data/gold/news_summaries/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW gold.news_summaries_history AS SELECT * FROM read_parquet('P:/data/gold/news_summaries_history/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW gold.mispricing_scores AS SELECT * FROM read_parquet('P:/data/gold/mispricing_scores/*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW gold.mispricing_scores_history AS SELECT * FROM read_parquet('P:/data/gold/mispricing_scores_history/*.parquet', union_by_name = true);