-- Analytics API Performance Optimization
-- Fix timeout issues in get_top_products endpoint

-- ANALYSIS: get_top_products query (analytics_service.py:542-572)
-- Query performs:
-- 1. JOIN fact_product_daily with dim_date, dim_product, dim_platform, dim_category
-- 2. WHERE date_value BETWEEN ... AND platform/category filters
-- 3. GROUP BY product, platform, category
-- 4. Aggregate: SUM(price * reviews), COUNT products, AVG rating/price
-- 5. ORDER BY metric (revenue/reviews/rating)
-- 6. LIMIT

-- BOTTLENECK: Full table scan on fact_product_daily with multiple JOINs
-- SOLUTION: Composite indexes for filtering + aggregation

-- Index 1: Date filtering (most common filter)
CREATE INDEX IF NOT EXISTS idx_fact_daily_date_product_platform 
ON dwh.fact_product_daily(date_sk, product_sk, platform_sk);

-- Index 2: Platform filtering
CREATE INDEX IF NOT EXISTS idx_fact_daily_platform_date 
ON dwh.fact_product_daily(platform_sk, date_sk);

-- Index 3: Covering index for aggregation (includes all needed columns)
CREATE INDEX IF NOT EXISTS idx_fact_daily_analytics_agg 
ON dwh.fact_product_daily(date_sk, product_sk, platform_sk, avg_price, total_review_count, avg_rating);

-- Index 4: Speed up category filtering via dim_product
CREATE INDEX IF NOT EXISTS idx_dim_product_category 
ON dwh.dim_product(category_sk, product_sk);

-- Verify indexes
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename IN ('fact_product_daily', 'dim_product')
  AND schemaname = 'dwh'
ORDER BY tablename, indexname;
