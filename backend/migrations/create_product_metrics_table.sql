-- ============================================
-- Performance Optimization: Product Metrics Materialized Table
-- ============================================
-- Purpose: Create pre-aggregated product metrics table to replace expensive CTE queries
-- Replaces: `WITH product_metrics AS (SELECT ... FROM dwh.fact_product_daily ... GROUP BY ...)` CTE pattern
-- Expected impact: 80-95% reduction in DSS query execution time
-- 
-- Refresh schedule: Daily at 2:00 AM via Airflow DAG
-- Run on: Render PostgreSQL database
-- ============================================

BEGIN;

-- ============================================
-- 1. CREATE PRODUCT METRICS GLOBAL TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS dwh.product_metrics_global (
    product_sk INTEGER PRIMARY KEY,
    avg_price DECIMAL(15, 2),
    total_orders INTEGER,
    avg_rating DECIMAL(3, 2),
    min_price DECIMAL(15, 2),
    max_price DECIMAL(15, 2),
    price_stddev DECIMAL(15, 2),
    last_updated TIMESTAMP DEFAULT NOW(),
    data_freshness_hours DECIMAL(5, 1)
);

COMMENT ON TABLE dwh.product_metrics_global IS 
'Pre-aggregated product metrics for DSS queries - refreshed daily by Airflow DAG';

COMMENT ON COLUMN dwh.product_metrics_global.product_sk IS 'Foreign key to dwh.dim_product';
COMMENT ON COLUMN dwh.product_metrics_global.avg_price IS 'Average price from fact_product_daily (last 30 days)';
COMMENT ON COLUMN dwh.product_metrics_global.total_orders IS 'Sum of total_review_count as proxy for orders (last 30 days)';
COMMENT ON COLUMN dwh.product_metrics_global.avg_rating IS 'Average rating (last 30 days)';
COMMENT ON COLUMN dwh.product_metrics_global.last_updated IS 'Timestamp of last refresh';
COMMENT ON COLUMN dwh.product_metrics_global.data_freshness_hours IS 'Hours since last data update';

-- ============================================
-- 2. CREATE INDEX FOR FAST LOOKUPS
-- ============================================

CREATE UNIQUE INDEX IF NOT EXISTS idx_product_metrics_global_pk
ON dwh.product_metrics_global(product_sk);

COMMENT ON INDEX dwh.idx_product_metrics_global_pk IS 
'Primary key index for fast product_sk lookups in JOIN operations';

-- ============================================
-- 3. INITIAL POPULATION
-- ============================================

-- Populate with last 30 days of data
INSERT INTO dwh.product_metrics_global (
    product_sk,
    avg_price,
    total_orders,
    avg_rating,
    min_price,
    max_price,
    price_stddev,
    last_updated,
    data_freshness_hours
)
SELECT
    f.product_sk,
    AVG(f.avg_price) AS avg_price,
    SUM(f.total_review_count) AS total_orders,
    AVG(f.avg_rating) AS avg_rating,
    MIN(f.avg_price) AS min_price,
    MAX(f.avg_price) AS max_price,
    STDDEV(f.avg_price) AS price_stddev,
    NOW() AS last_updated,
    EXTRACT(EPOCH FROM (NOW() - MAX(dd.date_value))) / 3600.0 AS data_freshness_hours
FROM dwh.fact_product_daily f
JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
WHERE dd.date_value >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY f.product_sk
ON CONFLICT (product_sk) DO UPDATE SET
    avg_price = EXCLUDED.avg_price,
    total_orders = EXCLUDED.total_orders,
    avg_rating = EXCLUDED.avg_rating,
    min_price = EXCLUDED.min_price,
    max_price = EXCLUDED.max_price,
    price_stddev = EXCLUDED.price_stddev,
    last_updated = EXCLUDED.last_updated,
    data_freshness_hours = EXCLUDED.data_freshness_hours;

-- ============================================
-- 4. REFRESH FUNCTION (Used by Airflow DAG)
-- ============================================

CREATE OR REPLACE FUNCTION dwh.refresh_product_metrics_global()
RETURNS TABLE(
    rows_inserted INTEGER,
    rows_updated INTEGER,
    execution_time_ms NUMERIC
) AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    v_rows_inserted INTEGER := 0;
    v_rows_updated INTEGER := 0;
BEGIN
    start_time := clock_timestamp();
    
    -- Delete old data (optional - or use UPSERT only)
    -- DELETE FROM dwh.product_metrics_global;
    
    -- Refresh metrics from last 30 days
    WITH refresh_data AS (
        SELECT
            f.product_sk,
            AVG(f.avg_price) AS avg_price,
            SUM(f.total_review_count) AS total_orders,
            AVG(f.avg_rating) AS avg_rating,
            MIN(f.avg_price) AS min_price,
            MAX(f.avg_price) AS max_price,
            STDDEV(f.avg_price) AS price_stddev,
            NOW() AS last_updated,
            EXTRACT(EPOCH FROM (NOW() - MAX(dd.date_value))) / 3600.0 AS data_freshness_hours
        FROM dwh.fact_product_daily f
        JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
        WHERE dd.date_value >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY f.product_sk
    ),
    upsert_result AS (
        INSERT INTO dwh.product_metrics_global
        SELECT * FROM refresh_data
        ON CONFLICT (product_sk) DO UPDATE SET
            avg_price = EXCLUDED.avg_price,
            total_orders = EXCLUDED.total_orders,
            avg_rating = EXCLUDED.avg_rating,
            min_price = EXCLUDED.min_price,
            max_price = EXCLUDED.max_price,
            price_stddev = EXCLUDED.price_stddev,
            last_updated = EXCLUDED.last_updated,
            data_freshness_hours = EXCLUDED.data_freshness_hours
        RETURNING 
            CASE WHEN xmax = 0 THEN 1 ELSE 0 END AS is_insert,
            CASE WHEN xmax > 0 THEN 1 ELSE 0 END AS is_update
    )
    SELECT SUM(is_insert)::INTEGER, SUM(is_update)::INTEGER
    INTO v_rows_inserted, v_rows_updated
    FROM upsert_result;
    
    end_time := clock_timestamp();
    
    RETURN QUERY SELECT 
        COALESCE(v_rows_inserted, 0),
        COALESCE(v_rows_updated, 0),
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION dwh.refresh_product_metrics_global() IS 
'Refreshes product_metrics_global table from fact_product_daily (last 30 days) - called by Airflow DAG';

-- ============================================
-- 5. VERIFICATION QUERIES
-- ============================================

-- Run these queries after table creation to verify:

-- Check table exists and has data
-- SELECT COUNT(*), MAX(last_updated), AVG(data_freshness_hours) FROM dwh.product_metrics_global;

-- Sample records
-- SELECT * FROM dwh.product_metrics_global LIMIT 10;

-- Check for products with null metrics
-- SELECT COUNT(*) FROM dwh.product_metrics_global WHERE avg_price IS NULL OR total_orders IS NULL;

-- Test refresh function
-- SELECT * FROM dwh.refresh_product_metrics_global();

COMMIT;

-- ============================================
-- ROLLBACK INSTRUCTIONS
-- ============================================
-- If issues occur, drop table with:
-- DROP TABLE IF EXISTS dwh.product_metrics_global CASCADE;
-- DROP FUNCTION IF EXISTS dwh.refresh_product_metrics_global();
-- ============================================
