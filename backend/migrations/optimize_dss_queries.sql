-- ============================================
-- DSS Query Performance Optimization
-- ============================================
-- Add indexes to speed up common query patterns

-- 1. Index on fact_product_daily for date filtering
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_date_sk 
ON dwh.fact_product_daily(date_sk);

-- 2. Index on fact_product_daily for product+platform grouping  
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_product_platform
ON dwh.fact_product_daily(product_sk, platform_sk);

-- 3. Composite index for common join pattern
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_date_product_platform
ON dwh.fact_product_daily(date_sk, product_sk, platform_sk);

-- 4. Index on fact_price_prediction for latest predictions
CREATE INDEX IF NOT EXISTS idx_fact_price_prediction_created_at
ON ml.fact_price_prediction(created_at DESC);

-- 5. Composite index for window function optimization
CREATE INDEX IF NOT EXISTS idx_fact_price_prediction_product_platform_created
ON ml.fact_price_prediction(product_sk, platform_sk, created_at DESC);

-- 6. Index on dim_date for date range queries
CREATE INDEX IF NOT EXISTS idx_dim_date_date_value
ON dwh.dim_date(date_value);

-- 7. Index on dim_product for category joins
CREATE INDEX IF NOT EXISTS idx_dim_product_category_sk
ON dwh.dim_product(category_sk) WHERE category_sk IS NOT NULL;

-- Analyze tables to update statistics
ANALYZE dwh.fact_product_daily;
ANALYZE ml.fact_price_prediction;
ANALYZE dwh.dim_date;
ANALYZE dwh.dim_product;
ANALYZE dwh.dim_platform;
ANALYZE dwh.dim_category;

-- Create comments to document optimization
COMMENT ON INDEX idx_fact_product_daily_date_sk IS 'Speeds up date range filtering in DSS queries';
COMMENT ON INDEX idx_fact_price_prediction_product_platform_created IS 'Optimizes window function for latest predictions';
