-- ============================================================
-- DATA ENGINEER QUERIES
-- Collection of useful SQL queries for daily monitoring
-- ============================================================

-- ============================================================
-- 1. ETL OVERVIEW QUERIES
-- ============================================================

-- 1.1. ETL Runs trong 24h qua
SELECT 
    er.run_id,
    ej.job_code,
    ej.job_name,
    er.run_date,
    er.started_at,
    er.finished_at,
    er.status,
    er.rows_read,
    er.rows_written,
    er.error_message,
    EXTRACT(EPOCH FROM (er.finished_at - er.started_at)) / 60 as duration_minutes
FROM meta.etl_run er
JOIN meta.etl_job ej ON er.job_id = ej.job_id
WHERE er.run_date >= CURRENT_DATE - INTERVAL '1 day'
ORDER BY er.started_at DESC;

-- 1.2. ETL Summary theo ngày
WITH daily_summary AS (
    SELECT 
        er.run_date,
        ej.job_code,
        COUNT(*) as total_runs,
        COUNT(CASE WHEN er.status = 'SUCCESS' THEN 1 END) as success_count,
        COUNT(CASE WHEN er.status = 'FAILED' THEN 1 END) as failed_count,
        COUNT(CASE WHEN er.status = 'RUNNING' THEN 1 END) as running_count,
        SUM(er.rows_read) as total_rows_read,
        SUM(er.rows_written) as total_rows_written,
        AVG(EXTRACT(EPOCH FROM (er.finished_at - er.started_at)) / 60) as avg_duration_minutes,
        MAX(EXTRACT(EPOCH FROM (er.finished_at - er.started_at)) / 60) as max_duration_minutes
    FROM meta.etl_run er
    JOIN meta.etl_job ej ON er.job_id = ej.job_id
    WHERE er.run_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY er.run_date, ej.job_code
)
SELECT * FROM daily_summary
ORDER BY run_date DESC, job_code;

-- 1.3. Failed runs cần xử lý
SELECT 
    er.run_id,
    ej.job_code,
    ej.job_name,
    er.run_date,
    er.started_at,
    er.finished_at,
    er.status,
    er.error_message,
    er.airflow_run_id
FROM meta.etl_run er
JOIN meta.etl_job ej ON er.job_id = ej.job_id
WHERE er.status = 'FAILED'
  AND er.run_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY er.finished_at DESC;

-- ============================================================
-- 2. DATA VOLUME QUERIES
-- ============================================================

-- 2.1. Volume snapshot theo schema
SELECT 
    schemaname as layer,
    COUNT(*) as table_count,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))) as total_size
FROM pg_tables
WHERE schemaname IN ('staging', 'ods', 'dwh', 'ml')
GROUP BY schemaname
ORDER BY schemaname;

-- 2.2. Top 10 largest tables
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as indexes_size
FROM pg_tables
WHERE schemaname IN ('staging', 'ods', 'dwh', 'ml')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 10;

-- 2.3. Row count cho các fact tables chính
SELECT 
    'dwh.fact_product_daily' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT date_sk) as distinct_dates,
    COUNT(DISTINCT product_sk) as distinct_products,
    COUNT(DISTINCT platform_sk) as distinct_platforms,
    MIN((SELECT date_value FROM dwh.dim_date WHERE date_sk = MIN(fpd.date_sk))) as earliest_date,
    MAX((SELECT date_value FROM dwh.dim_date WHERE date_sk = MAX(fpd.date_sk))) as latest_date
FROM dwh.fact_product_daily fpd

UNION ALL

SELECT 
    'dwh.fact_review_daily' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT date_sk) as distinct_dates,
    COUNT(DISTINCT product_sk) as distinct_products,
    COUNT(DISTINCT platform_sk) as distinct_platforms,
    MIN((SELECT date_value FROM dwh.dim_date WHERE date_sk = MIN(frd.date_sk))) as earliest_date,
    MAX((SELECT date_value FROM dwh.dim_date WHERE date_sk = MAX(frd.date_sk))) as latest_date
FROM dwh.fact_review_daily frd

UNION ALL

SELECT 
    'dwh.fact_review' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT date_sk) as distinct_dates,
    COUNT(DISTINCT product_sk) as distinct_products,
    COUNT(DISTINCT platform_sk) as distinct_platforms,
    MIN((SELECT date_value FROM dwh.dim_date WHERE date_sk = MIN(fr.date_sk))) as earliest_date,
    MAX((SELECT date_value FROM dwh.dim_date WHERE date_sk = MAX(fr.date_sk))) as latest_date
FROM dwh.fact_review fr;

-- 2.4. Volume trend 7 ngày gần nhất
SELECT 
    dd.date_value as snapshot_date,
    COUNT(*) as row_count,
    COUNT(DISTINCT fpd.product_sk) as distinct_products,
    COUNT(DISTINCT fpd.platform_sk) as distinct_platforms,
    AVG(fpd.avg_price) as avg_price,
    SUM(fpd.total_review_count) as total_reviews
FROM dwh.fact_product_daily fpd
JOIN dwh.dim_date dd ON fpd.date_sk = dd.date_sk
WHERE dd.date_value >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY dd.date_value
ORDER BY dd.date_value DESC;

-- ============================================================
-- 3. DATA FRESHNESS QUERIES
-- ============================================================

-- 3.1. Latest data date cho mỗi fact table
SELECT 
    'fact_product_daily' as table_name,
    MAX(dd.date_value) as latest_date,
    CURRENT_DATE - MAX(dd.date_value) as days_behind
FROM dwh.fact_product_daily fpd
JOIN dwh.dim_date dd ON fpd.date_sk = dd.date_sk

UNION ALL

SELECT 
    'fact_review_daily' as table_name,
    MAX(dd.date_value) as latest_date,
    CURRENT_DATE - MAX(dd.date_value) as days_behind
FROM dwh.fact_review_daily frd
JOIN dwh.dim_date dd ON frd.date_sk = dd.date_sk

UNION ALL

SELECT 
    'fact_review' as table_name,
    MAX(dd.date_value) as latest_date,
    CURRENT_DATE - MAX(dd.date_value) as days_behind
FROM dwh.fact_review fr
JOIN dwh.dim_date dd ON fr.date_sk = dd.date_sk;

-- 3.2. Missing dates trong fact_product_daily (7 ngày gần nhất)
SELECT 
    dd.date_value as missing_date
FROM dwh.dim_date dd
WHERE dd.date_value >= CURRENT_DATE - INTERVAL '7 days'
  AND dd.date_value < CURRENT_DATE
  AND NOT EXISTS (
      SELECT 1 FROM dwh.fact_product_daily fpd
      WHERE fpd.date_sk = dd.date_sk
  )
ORDER BY dd.date_value DESC;

-- ============================================================
-- 4. DATA QUALITY QUERIES
-- ============================================================

-- 4.1. Open data quality issues
SELECT 
    issue_id,
    schema_name,
    table_name,
    issue_type,
    severity,
    status,
    affected_rows,
    issue_description,
    detected_at,
    resolved_at
FROM meta.data_quality_issue
WHERE status = 'OPEN'
ORDER BY 
    CASE severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
    END,
    detected_at DESC;

-- 4.2. Data quality issues summary
SELECT 
    status,
    severity,
    COUNT(*) as issue_count,
    SUM(affected_rows) as total_affected_rows
FROM meta.data_quality_issue
WHERE detected_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY status, severity
ORDER BY 
    CASE severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
    END,
    status;

-- 4.3. Check null values trong fact_product_daily (latest date)
WITH latest_date AS (
    SELECT MAX(date_sk) as date_sk FROM dwh.fact_product_daily
)
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN min_price IS NULL THEN 1 END) as null_min_price,
    COUNT(CASE WHEN max_price IS NULL THEN 1 END) as null_max_price,
    COUNT(CASE WHEN avg_price IS NULL THEN 1 END) as null_avg_price,
    COUNT(CASE WHEN product_sk IS NULL THEN 1 END) as null_product_sk,
    COUNT(CASE WHEN platform_sk IS NULL THEN 1 END) as null_platform_sk,
    COUNT(CASE WHEN date_sk IS NULL THEN 1 END) as null_date_sk
FROM dwh.fact_product_daily fpd
CROSS JOIN latest_date ld
WHERE fpd.date_sk = ld.date_sk;

-- 4.4. Check invalid prices
WITH latest_date AS (
    SELECT MAX(date_sk) as date_sk FROM dwh.fact_product_daily
)
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN min_price < 0 THEN 1 END) as negative_min_price,
    COUNT(CASE WHEN min_price = 0 THEN 1 END) as zero_min_price,
    COUNT(CASE WHEN min_price > 100000000 THEN 1 END) as outlier_min_price,
    COUNT(CASE WHEN max_price < min_price THEN 1 END) as invalid_price_range,
    COUNT(CASE WHEN avg_price < min_price OR avg_price > max_price THEN 1 END) as invalid_avg_price
FROM dwh.fact_product_daily fpd
CROSS JOIN latest_date ld
WHERE fpd.date_sk = ld.date_sk;

-- 4.5. Check duplicates trong fact_product_daily
SELECT 
    date_sk,
    product_sk,
    platform_sk,
    COUNT(*) as duplicate_count
FROM dwh.fact_product_daily
GROUP BY date_sk, product_sk, platform_sk
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;

-- 4.6. Check foreign key integrity
SELECT 
    'fact_product_daily -> dim_product' as relationship,
    COUNT(*) as orphaned_rows
FROM dwh.fact_product_daily fpd
WHERE NOT EXISTS (
    SELECT 1 FROM dwh.dim_product dp
    WHERE dp.product_sk = fpd.product_sk
)

UNION ALL

SELECT 
    'fact_product_daily -> dim_platform' as relationship,
    COUNT(*) as orphaned_rows
FROM dwh.fact_product_daily fpd
WHERE NOT EXISTS (
    SELECT 1 FROM dwh.dim_platform dp
    WHERE dp.platform_sk = fpd.platform_sk
)

UNION ALL

SELECT 
    'fact_product_daily -> dim_date' as relationship,
    COUNT(*) as orphaned_rows
FROM dwh.fact_product_daily fpd
WHERE NOT EXISTS (
    SELECT 1 FROM dwh.dim_date dd
    WHERE dd.date_sk = fpd.date_sk
);

-- ============================================================
-- 5. PERFORMANCE QUERIES
-- ============================================================

-- 5.1. Table statistics (cần analyze trước)
SELECT 
    schemaname,
    tablename,
    n_live_tup as row_count,
    n_dead_tup as dead_rows,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE schemaname IN ('staging', 'ods', 'dwh', 'ml')
ORDER BY n_live_tup DESC;

-- 5.2. Index usage statistics
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes
WHERE schemaname IN ('dwh', 'ml')
ORDER BY idx_scan DESC;

-- 5.3. Missing indexes (tables không có index)
SELECT 
    t.schemaname,
    t.tablename,
    pg_size_pretty(pg_total_relation_size(t.schemaname||'.'||t.tablename)) as table_size
FROM pg_tables t
LEFT JOIN pg_indexes i ON t.schemaname = i.schemaname AND t.tablename = i.tablename
WHERE t.schemaname IN ('dwh', 'ml')
  AND i.indexname IS NULL
  AND t.tablename LIKE 'fact_%'
ORDER BY pg_total_relation_size(t.schemaname||'.'||t.tablename) DESC;

-- ============================================================
-- 6. DIMENSION TABLES QUERIES
-- ============================================================

-- 6.1. Dimension counts
SELECT 
    'dim_product' as dimension,
    COUNT(*) as total_rows,
    COUNT(DISTINCT brand_sk) as distinct_brands,
    COUNT(DISTINCT category_sk) as distinct_categories
FROM dwh.dim_product

UNION ALL

SELECT 
    'dim_platform' as dimension,
    COUNT(*) as total_rows,
    NULL as distinct_brands,
    NULL as distinct_categories
FROM dwh.dim_platform

UNION ALL

SELECT 
    'dim_category' as dimension,
    COUNT(*) as total_rows,
    NULL as distinct_brands,
    NULL as distinct_categories
FROM dwh.dim_category

UNION ALL

SELECT 
    'dim_brand' as dimension,
    COUNT(*) as total_rows,
    NULL as distinct_brands,
    NULL as distinct_categories
FROM dwh.dim_brand

UNION ALL

SELECT 
    'dim_date' as dimension,
    COUNT(*) as total_rows,
    NULL as distinct_brands,
    NULL as distinct_categories
FROM dwh.dim_date;

-- 6.2. Top categories by product count
SELECT 
    dc.category_std_key,
    dc.category_lvl1,
    dc.category_lvl2,
    COUNT(DISTINCT dp.product_sk) as product_count
FROM dwh.dim_product dp
JOIN dwh.dim_category dc ON dp.category_sk = dc.category_sk
GROUP BY dc.category_std_key, dc.category_lvl1, dc.category_lvl2
ORDER BY product_count DESC
LIMIT 20;

-- 6.3. Top brands by product count
SELECT 
    db.brand_name,
    COUNT(DISTINCT dp.product_sk) as product_count
FROM dwh.dim_product dp
JOIN dwh.dim_brand db ON dp.brand_sk = db.brand_sk
GROUP BY db.brand_name
ORDER BY product_count DESC
LIMIT 20;

-- ============================================================
-- 7. UTILITY QUERIES
-- ============================================================

-- 7.1. Analyze all tables (chạy sau khi load data)
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN 
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE schemaname IN ('staging', 'ods', 'dwh', 'ml')
    LOOP
        EXECUTE format('ANALYZE %I.%I', r.schemaname, r.tablename);
        RAISE NOTICE 'Analyzed: %.%', r.schemaname, r.tablename;
    END LOOP;
END $$;

-- 7.2. Vacuum tables (chạy định kỳ)
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN 
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE schemaname IN ('staging', 'ods', 'dwh', 'ml')
    LOOP
        EXECUTE format('VACUUM ANALYZE %I.%I', r.schemaname, r.tablename);
        RAISE NOTICE 'Vacuumed: %.%', r.schemaname, r.tablename;
    END LOOP;
END $$;

-- 7.3. Check table bloat
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as overhead,
    n_dead_tup,
    n_live_tup,
    CASE 
        WHEN n_live_tup > 0 
        THEN ROUND(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
        ELSE 0
    END as bloat_percentage
FROM pg_stat_user_tables
WHERE schemaname IN ('staging', 'ods', 'dwh', 'ml')
  AND n_dead_tup > 0
ORDER BY bloat_percentage DESC;

