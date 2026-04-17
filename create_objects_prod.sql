-- ─────────────────────────────────────────────────────────────────────────────
-- PREDICTIQ — DUCKDB VIEWS  |  MEDALLION LAKEHOUSE
-- ─────────────────────────────────────────────────────────────────────────────

-- Delta extension (required for Silver & Gold)
INSTALL delta;
LOAD delta;

-- Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================================================
-- 1. BRONZE LAYER — Raw Parquet Ingestion (Using read_parquet)
-- =============================================================================

-- ── Kalshi Markets ──
CREATE OR REPLACE VIEW bronze.kalshi_open_latest AS SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/open/latest.parquet');
CREATE OR REPLACE VIEW bronze.kalshi_open AS SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/open/markets_*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW bronze.kalshi_closed AS SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/closed/closed*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW bronze.kalshi_settled AS SELECT * FROM read_parquet('P:/data/bronze/kalshi_markets/settled/settled*.parquet', union_by_name = true);

-- ── GDELT Global Knowledge Graph ──
CREATE OR REPLACE VIEW bronze.gdelt_events_latest AS SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_events/latest.parquet');
CREATE OR REPLACE VIEW bronze.gdelt_events AS SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_events/gdelt_*.parquet', union_by_name = true);
CREATE OR REPLACE VIEW bronze.gdelt_gkg_latest AS SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_gkg/latest.parquet');
CREATE OR REPLACE VIEW bronze.gdelt_gkg AS SELECT * FROM read_parquet('P:/data/bronze/gdelt/gdelt_gkg/gkg_*.parquet', union_by_name = true);

-- ── News Sentiment Feeds (Using your exact root paths) ──
CREATE OR REPLACE VIEW bronze.news_bbc_latest AS SELECT * FROM read_parquet('P:/data/bronze/bbc/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_bbc AS SELECT * FROM read_parquet('P:/data/bronze/bbc/bbc_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_cnn_latest AS SELECT * FROM read_parquet('P:/data/bronze/cnn/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_cnn AS SELECT * FROM read_parquet('P:/data/bronze/cnn/cnn_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_foxnews_latest AS SELECT * FROM read_parquet('P:/data/bronze/fox/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_foxnews AS SELECT * FROM read_parquet('P:/data/bronze/fox/fox_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_nypost_latest AS SELECT * FROM read_parquet('P:/data/bronze/nypost/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_nypost AS SELECT * FROM read_parquet('P:/data/bronze/nypost/nypost_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_thehindu_latest AS SELECT * FROM read_parquet('P:/data/bronze/hindu/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_thehindu AS SELECT * FROM read_parquet('P:/data/bronze/hindu/hindu_*.parquet', union_by_name = true);

CREATE OR REPLACE VIEW bronze.news_nyt_latest AS SELECT * FROM read_parquet('P:/data/bronze/nyt/latest.parquet');
CREATE OR REPLACE VIEW bronze.news_nyt AS SELECT * FROM read_parquet('P:/data/bronze/nyt/nyt_*.parquet', union_by_name = true);

-- ============================================================================= 
-- 2. SILVER LAYER — Delta Lake (Using delta_scan NO wildcards) 
-- =============================================================================

CREATE OR REPLACE VIEW silver.gdelt_events_history AS SELECT * FROM delta_scan('P:/data/silver/gdelt_events_history');
CREATE OR REPLACE VIEW silver.gdelt_events_current AS SELECT * FROM delta_scan('P:/data/silver/gdelt_events_current');

CREATE OR REPLACE VIEW silver.gdelt_gkg_history AS SELECT * FROM delta_scan('P:/data/silver/gdelt_gkg_history');
CREATE OR REPLACE VIEW silver.gdelt_gkg_current AS SELECT * FROM delta_scan('P:/data/silver/gdelt_gkg_current');

CREATE OR REPLACE VIEW silver.kalshi_markets_current AS SELECT * FROM delta_scan('P:/data/silver/kalshi_markets_current');
CREATE OR REPLACE VIEW silver.kalshi_markets_history AS SELECT * FROM delta_scan('P:/data/silver/kalshi_markets_history');

CREATE OR REPLACE VIEW silver.news_articles_enriched AS SELECT * FROM delta_scan('P:/data/silver/news_articles_enriched');

-- ============================================================================= 
-- 3. GOLD LAYER — Delta Lake (Using delta_scan NO wildcards) 
-- =============================================================================

CREATE OR REPLACE VIEW gold.mispricing_scores AS SELECT * FROM delta_scan('P:/data/gold/mispricing_scores');
CREATE OR REPLACE VIEW gold.mispricing_scores_history AS SELECT * FROM delta_scan('P:/data/gold/mispricing_scores_history');

-- 4. INTELLIGENCE LAYER — AI ANALYSIS (Phase 4)
-- ==============================================

CREATE OR REPLACE VIEW gold.intelligence_briefs AS 
    SELECT * FROM read_parquet('P:/data/gold/intelligence_briefs/*.parquet')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY generated_at DESC) = 1;

CREATE OR REPLACE VIEW gold.market_intelligence AS 
    SELECT 
        s.ticker,
        s.title,
        s.yes_bid as current_price,
        s.mispricing_score,
        s.max_spike_multiplier as narrative_spike,
        b.ai_narrative_brief
    FROM gold.mispricing_scores s
    LEFT JOIN gold.intelligence_briefs b ON s.ticker = b.ticker;