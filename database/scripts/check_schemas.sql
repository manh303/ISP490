-- ===================================================================
-- SCRIPT KIỂM TRA SCHEMAS (DWH + META)
-- ===================================================================

-- ===================================================================
-- 1. KIỂM TRA SCHEMA META
-- ===================================================================

SELECT '=== SCHEMA META ===' as check_section;

-- Kiểm tra schema meta tồn tại
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'meta')
        THEN '✅ Schema meta tồn tại'
        ELSE '❌ Schema meta CHƯA TỒN TẠI - Cần chạy: database/schema/meta_schema.sql'
    END as meta_schema_status;

-- Kiểm tra các bảng trong schema meta
SELECT 
    'meta.' || table_name as table_name,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'meta' AND table_name = t.table_name
        )
        THEN '✅'
        ELSE '❌ THIẾU'
    END as status
FROM (VALUES 
    ('etl_job'),
    ('etl_run'),
    ('etl_log'),
    ('table_stats'),
    ('data_quality_issue'),
    ('data_quality_rule'),
    ('data_quality_check_result')
) AS t(table_name)
ORDER BY t.table_name;

-- ===================================================================
-- 2. KIỂM TRA SCHEMA DWH
-- ===================================================================

SELECT '=== SCHEMA DWH ===' as check_section;

-- Kiểm tra schema dwh tồn tại
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'dwh')
        THEN '✅ Schema dwh tồn tại'
        ELSE '❌ Schema dwh CHƯA TỒN TẠI - Cần chạy: database/schema/datawarehouse.sql'
    END as dwh_schema_status;

-- Kiểm tra các bảng DIMENSION trong schema dwh
SELECT 
    'dwh.' || table_name as table_name,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'dwh' AND table_name = t.table_name
        )
        THEN '✅'
        ELSE '❌ THIẾU'
    END as status,
    'DIMENSION' as table_type
FROM (VALUES 
    ('dim_date'),
    ('dim_platform'),
    ('dim_brand'),
    ('dim_category'),
    ('dim_product')
) AS t(table_name)
ORDER BY t.table_name;

-- Kiểm tra các bảng FACT trong schema dwh
SELECT 
    'dwh.' || table_name as table_name,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'dwh' AND table_name = t.table_name
        )
        THEN '✅'
        ELSE '❌ THIẾU'
    END as status,
    'FACT' as table_type
FROM (VALUES 
    ('fact_product_daily'),
    ('fact_review'),
    ('fact_review_daily')
) AS t(table_name)
ORDER BY t.table_name;

-- ===================================================================
-- 3. KIỂM TRA SCHEMA ML
-- ===================================================================

SELECT '=== SCHEMA ML ===' as check_section;

-- Kiểm tra schema ml tồn tại
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'ml')
        THEN '✅ Schema ml tồn tại'
        ELSE '❌ Schema ml CHƯA TỒN TẠI - Cần chạy: database/schema/datawarehouse.sql'
    END as ml_schema_status;

-- Kiểm tra các bảng trong schema ml
SELECT 
    'ml.' || table_name as table_name,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'ml' AND table_name = t.table_name
        )
        THEN '✅'
        ELSE '❌ THIẾU'
    END as status
FROM (VALUES 
    ('dim_ml_model'),
    ('fact_price_prediction'),
    ('fact_product_recommendation')
) AS t(table_name)
ORDER BY t.table_name;

-- ===================================================================
-- 4. KIỂM TRA FOREIGN KEYS VÀ INDEXES
-- ===================================================================

SELECT '=== FOREIGN KEYS & INDEXES ===' as check_section;

-- Kiểm tra foreign keys trong fact_product_daily
SELECT 
    'fact_product_daily' as table_name,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM information_schema.table_constraints 
            WHERE table_schema = 'dwh' 
              AND table_name = 'fact_product_daily'
              AND constraint_type = 'FOREIGN KEY'
              AND constraint_name LIKE '%date_sk%'
        ) THEN '✅ FK date_sk'
        ELSE '❌ THIẾU FK date_sk'
    END as fk_date_sk,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM information_schema.table_constraints 
            WHERE table_schema = 'dwh' 
              AND table_name = 'fact_product_daily'
              AND constraint_type = 'FOREIGN KEY'
              AND constraint_name LIKE '%product_sk%'
        ) THEN '✅ FK product_sk'
        ELSE '❌ THIẾU FK product_sk'
    END as fk_product_sk,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM information_schema.table_constraints 
            WHERE table_schema = 'dwh' 
              AND table_name = 'fact_product_daily'
              AND constraint_type = 'FOREIGN KEY'
              AND constraint_name LIKE '%platform_sk%'
        ) THEN '✅ FK platform_sk'
        ELSE '❌ THIẾU FK platform_sk'
    END as fk_platform_sk;

-- Kiểm tra indexes quan trọng
SELECT 
    schemaname || '.' || tablename as table_name,
    indexname,
    CASE 
        WHEN indexname IS NOT NULL THEN '✅'
        ELSE '❌ THIẾU INDEX'
    END as status
FROM pg_tables t
LEFT JOIN pg_indexes i ON t.schemaname = i.schemaname AND t.tablename = i.tablename
WHERE t.schemaname IN ('dwh', 'ml')
  AND t.tablename IN (
      'fact_product_daily',
      'fact_review',
      'fact_review_daily',
      'fact_price_prediction',
      'fact_product_recommendation'
  )
  AND (
      i.indexname LIKE '%prod_plat_date%' 
      OR i.indexname LIKE '%date_sk%'
      OR i.indexname IS NULL
  )
ORDER BY t.schemaname, t.tablename, i.indexname;

-- ===================================================================
-- 5. TỔNG KẾT
-- ===================================================================

SELECT '=== TỔNG KẾT ===' as check_section;

SELECT 
    schema_name,
    COUNT(*) as table_count,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))) as total_size
FROM pg_tables
WHERE schemaname IN ('meta', 'dwh', 'ml')
GROUP BY schema_name
ORDER BY schema_name;

-- ===================================================================
-- 6. KIỂM TRA CẤU TRÚC CHI TIẾT CÁC BẢNG QUAN TRỌNG
-- ===================================================================

SELECT '=== CẤU TRÚC CHI TIẾT ===' as check_section;

-- Kiểm tra cột trong fact_product_daily
SELECT 
    'fact_product_daily' as table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'dwh' 
  AND table_name = 'fact_product_daily'
ORDER BY ordinal_position;

-- Kiểm tra cột trong dim_product
SELECT 
    'dim_product' as table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'dwh' 
  AND table_name = 'dim_product'
ORDER BY ordinal_position;

-- Kiểm tra cột trong meta.etl_run
SELECT 
    'meta.etl_run' as table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'meta' 
  AND table_name = 'etl_run'
ORDER BY ordinal_position;

