-- Dashboard Views for MVP DSS Demo
-- Simple aggregated views to support the Streamlit dashboard

-- Platform overview metrics
CREATE OR REPLACE VIEW v_platform_overview AS
SELECT
    source_platform,
    COUNT(*) as total_products,
    AVG(CAST(raw_data->>'price' AS DECIMAL)) as avg_price,
    AVG(CAST(raw_data->>'price_current' AS DECIMAL)) as avg_price_current,
    COUNT(CASE WHEN raw_data->>'rating' IS NOT NULL THEN 1 END) as rated_products,
    MAX(created_at) as last_updated
FROM stg_raw_products
WHERE source_platform IN ('tiki', 'lazada')
GROUP BY source_platform;

-- Price comparison across platforms
CREATE OR REPLACE VIEW v_price_comparison AS
SELECT
    source_platform,
    raw_data->>'product_name' as product_name,
    raw_data->>'brand' as brand,
    CAST(COALESCE(raw_data->>'price', raw_data->>'price_current', '0') AS DECIMAL) as price,
    CAST(raw_data->>'discount_percent' AS DECIMAL) as discount,
    created_at
FROM stg_raw_products
WHERE source_platform IN ('tiki', 'lazada')
    AND (raw_data->>'price' IS NOT NULL OR raw_data->>'price_current' IS NOT NULL)
    AND CAST(COALESCE(raw_data->>'price', raw_data->>'price_current', '0') AS DECIMAL) > 0;

-- Brand performance metrics
CREATE OR REPLACE VIEW v_brand_performance AS
SELECT
    COALESCE(raw_data->>'brand', raw_data->>'brand_name', 'Unknown') as brand,
    source_platform,
    COUNT(*) as product_count,
    AVG(CAST(COALESCE(raw_data->>'price', raw_data->>'price_current', '0') AS DECIMAL)) as avg_price,
    AVG(CAST(COALESCE(raw_data->>'rating', raw_data->>'rating_avg', '0') AS DECIMAL)) as avg_rating,
    MAX(created_at) as last_updated
FROM stg_raw_products
WHERE source_platform IN ('tiki', 'lazada')
GROUP BY brand, source_platform
HAVING COUNT(*) > 1 AND brand != 'Unknown' AND brand IS NOT NULL
ORDER BY product_count DESC;

-- Trending products based on reviews and ratings
CREATE OR REPLACE VIEW v_trending_products AS
SELECT
    raw_data->>'product_name' as product_name,
    COALESCE(raw_data->>'brand', raw_data->>'brand_name') as brand,
    source_platform,
    CAST(COALESCE(raw_data->>'price', raw_data->>'price_current', '0') AS DECIMAL) as price,
    CAST(COALESCE(raw_data->>'rating', raw_data->>'rating_avg', '0') AS DECIMAL) as rating,
    CAST(COALESCE(raw_data->>'review_count', '0') AS INTEGER) as review_count,
    CAST(COALESCE(raw_data->>'sold_count', '0') AS INTEGER) as sold_count,
    created_at
FROM stg_raw_products
WHERE source_platform IN ('tiki', 'lazada')
    AND raw_data->>'product_name' IS NOT NULL
    AND LENGTH(raw_data->>'product_name') > 5
ORDER BY
    CAST(COALESCE(raw_data->>'review_count', '0') AS INTEGER) DESC,
    CAST(COALESCE(raw_data->>'rating', raw_data->>'rating_avg', '0') AS DECIMAL) DESC
LIMIT 20;

-- Processing statistics for monitoring
CREATE OR REPLACE VIEW v_processing_stats AS
SELECT
    source_platform,
    DATE(created_at) as process_date,
    COUNT(*) as records_processed,
    COUNT(CASE WHEN raw_data->>'price' IS NOT NULL THEN 1 END) as records_with_price,
    COUNT(CASE WHEN raw_data->>'rating' IS NOT NULL THEN 1 END) as records_with_rating,
    MIN(created_at) as first_record,
    MAX(created_at) as last_record
FROM stg_raw_products
WHERE source_platform IN ('tiki', 'lazada')
GROUP BY source_platform, DATE(created_at)
ORDER BY process_date DESC, source_platform;

-- Grant permissions for dashboard user
GRANT SELECT ON v_platform_overview TO dss_user;
GRANT SELECT ON v_price_comparison TO dss_user;
GRANT SELECT ON v_brand_performance TO dss_user;
GRANT SELECT ON v_trending_products TO dss_user;
GRANT SELECT ON v_processing_stats TO dss_user;