-- ============================================
-- Performance Optimization: Critical Indexes
-- ============================================
-- Purpose: Create indexes to optimize DSS queries for product recommendations and price predictions
-- Target tables: ml.fact_product_recommendation, dwh.dim_product, dwh.fact_product_daily
-- Expected impact: 70-90% reduction in query execution time
-- 
-- Run on: Render PostgreSQL database
-- Estimated execution time: 5-10 minutes (depends on table size)
--
-- IMPORTANT: Run during low-traffic period as index creation may briefly lock tables
-- ============================================

BEGIN;

-- ============================================
-- 1. ML.FACT_PRODUCT_RECOMMENDATION INDEXES
-- ============================================

-- Index for BY_PRODUCT queries
-- Used in: _query_recommendations_by_product() 
-- Query pattern: WHERE dp_src.product_key = $1 AND rec.similarity_score >= $2 ORDER BY rec.similarity_score DESC, rec.rank ASC
CREATE INDEX IF NOT EXISTS idx_fact_product_reco_src_sim_rank
ON ml.fact_product_recommendation (source_product_sk, similarity_score DESC, rank ASC);

COMMENT ON INDEX ml.idx_fact_product_reco_src_sim_rank IS 
'Optimizes product recommendation by_product queries - speeds up lookups by source product with similarity sorting';

-- Index for BY_CATEGORY queries
-- Used in: _query_recommendations_by_category()
-- Query pattern: WHERE rec.similarity_score >= $1 ORDER BY pm_rec.total_orders DESC, rec.similarity_score DESC
CREATE INDEX IF NOT EXISTS idx_fact_product_reco_sim_desc
ON ml.fact_product_recommendation (similarity_score DESC, source_product_sk, recommended_product_sk);

COMMENT ON INDEX ml.idx_fact_product_reco_sim_desc IS 
'Optimizes product recommendation by_category queries - speeds up filtering by similarity score';

-- ============================================
-- 2. DWH.DIM_PRODUCT INDEXES
-- ============================================

-- Unique index on product_key for fast lookups
-- Used in: All DSS queries that filter by product_key
-- Query pattern: WHERE dp.product_key = $1 or dp.product_key = ANY($1::text[])
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_product_product_key
ON dwh.dim_product(product_key);

COMMENT ON INDEX dwh.idx_dim_product_product_key IS 
'Unique index for fast product_key lookups - critical for all DSS queries';

-- Composite index for category filtering
-- Used in: Queries that filter by category_sk
-- Query pattern: WHERE category_sk = $1
CREATE INDEX IF NOT EXISTS idx_dim_product_category
ON dwh.dim_product(category_sk, product_sk);

COMMENT ON INDEX dwh.idx_dim_product_category IS 
'Composite index for filtering products by category';

-- ============================================
-- 3. DWH.FACT_PRODUCT_DAILY INDEXES
-- ============================================

-- Index for date-based aggregations
-- Used in: product_metrics CTE (to be replaced by materialized table)
-- Query pattern: WHERE dd.date_value BETWEEN $1 AND $2, GROUP BY f.product_sk
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_date_product
ON dwh.fact_product_daily(date_sk, product_sk);

COMMENT ON INDEX dwh.idx_fact_product_daily_date_product IS 
'Optimizes date-based aggregations for product metrics calculation';

-- Optional: Cover index for common metrics queries
-- Includes frequently accessed columns to enable index-only scans
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_metrics
ON dwh.fact_product_daily(product_sk, date_sk) 
INCLUDE (avg_price, total_review_count, avg_rating);

COMMENT ON INDEX dwh.idx_fact_product_daily_metrics IS 
'Cover index for product metrics - enables index-only scans for common aggregations';

-- ============================================
-- 4. ADDITIONAL SUPPORTING INDEXES
-- ============================================

-- Index on dim_date for date range queries
CREATE INDEX IF NOT EXISTS idx_dim_date_date_value
ON dwh.dim_date(date_value);

COMMENT ON INDEX dwh.idx_dim_date_date_value IS 
'Speeds up date range filtering in JOIN conditions';

-- Index on dim_platform for platform filtering
CREATE INDEX IF NOT EXISTS idx_dim_platform_code
ON dwh.dim_platform(platform_code);

COMMENT ON INDEX dwh.idx_dim_platform_code IS 
'Speeds up platform filtering queries';

-- ============================================
-- 5. VERIFICATION QUERIES
-- ============================================

-- Run these queries after index creation to verify:

-- Verify ml.fact_product_recommendation indexes
-- SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'fact_product_recommendation' AND schemaname = 'ml';

-- Verify dwh.dim_product indexes
-- SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'dim_product' AND schemaname = 'dwh';

-- Verify dwh.fact_product_daily indexes
-- SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'fact_product_daily' AND schemaname = 'dwh';

-- Check index sizes
-- SELECT schemaname, tablename, indexname, pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
-- FROM pg_stat_user_indexes
-- WHERE schemaname IN ('ml', 'dwh')
-- ORDER BY pg_relation_size(indexrelid) DESC;

COMMIT;

-- ============================================
-- ROLLBACK INSTRUCTIONS
-- ============================================
-- If issues occur, drop indexes with:
-- DROP INDEX CONCURRENTLY IF EXISTS ml.idx_fact_product_reco_src_sim_rank;
-- DROP INDEX CONCURRENTLY IF EXISTS ml.idx_fact_product_reco_sim_desc;
-- DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_product_product_key;
-- DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_product_category;
-- DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_fact_product_daily_date_product;
-- DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_fact_product_daily_metrics;
-- DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_date_date_value;
-- DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_platform_code;
-- ============================================
