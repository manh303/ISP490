-- Database Setup for Tiki & Lazada Batch Processing
-- Run this before executing the batch pipeline

-- Add missing constraints and indexes for performance optimization

-- Add unique constraints to prevent duplicates in staging
ALTER TABLE stg_raw_products
ADD CONSTRAINT IF NOT EXISTS uk_stg_raw_products
UNIQUE (source_platform, platform_product_id, checksum);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_stg_raw_products_platform_date
ON stg_raw_products (source_platform, DATE(created_at));

CREATE INDEX IF NOT EXISTS idx_stg_raw_products_platform_id
ON stg_raw_products (source_platform, platform_product_id);

CREATE INDEX IF NOT EXISTS idx_stg_raw_reviews_platform_date
ON stg_raw_reviews (source_platform, DATE(created_at));

-- ODS layer indexes
CREATE INDEX IF NOT EXISTS idx_ods_platform_ref_code
ON ods_platform_ref (platform_code);

CREATE INDEX IF NOT EXISTS idx_ods_product_clean_name
ON ods_product_clean (product_name);

CREATE INDEX IF NOT EXISTS idx_ods_product_clean_brand
ON ods_product_clean (brand_name);

CREATE INDEX IF NOT EXISTS idx_ods_price_point_product_platform
ON ods_price_point (global_product_id, platform_sk);

CREATE INDEX IF NOT EXISTS idx_ods_price_point_date
ON ods_price_point (DATE(captured_at));

CREATE INDEX IF NOT EXISTS idx_ods_rating_snapshot_product_platform
ON ods_rating_snapshot (global_product_id, platform_sk);

CREATE INDEX IF NOT EXISTS idx_ods_rating_snapshot_date
ON ods_rating_snapshot (DATE(captured_at));

-- DWH layer indexes
CREATE INDEX IF NOT EXISTS idx_dwh_dim_product_global_id
ON dwh_dim_product (global_product_id);

CREATE INDEX IF NOT EXISTS idx_dwh_dim_product_current
ON dwh_dim_product (is_current) WHERE is_current = true;

CREATE INDEX IF NOT EXISTS idx_dwh_fact_product_daily_date
ON dwh_fact_product_daily (date_sk);

CREATE INDEX IF NOT EXISTS idx_dwh_fact_product_daily_product
ON dwh_fact_product_daily (product_sk);

CREATE INDEX IF NOT EXISTS idx_dwh_fact_review_summary_date
ON dwh_fact_review_summary (date_sk);

-- Data Mart indexes
CREATE INDEX IF NOT EXISTS idx_dm_price_analytics_date
ON dm_price_analytics (date_sk);

CREATE INDEX IF NOT EXISTS idx_dm_price_analytics_product
ON dm_price_analytics (product_sk);

-- Initialize platform reference data for Tiki and Lazada
INSERT INTO ods_platform_ref (platform_code, platform_name, website_url, country_code, is_active)
VALUES
    ('tiki', 'Tiki.vn', 'https://tiki.vn', 'VN', true),
    ('lazada', 'Lazada Vietnam', 'https://lazada.vn', 'VN', true)
ON CONFLICT (platform_code) DO UPDATE SET
    platform_name = EXCLUDED.platform_name,
    website_url = EXCLUDED.website_url,
    is_active = EXCLUDED.is_active;

-- Copy to DWH platform dimension
INSERT INTO dwh_dim_platform (platform_code, platform_name, website_url, country_code, is_active)
SELECT platform_code, platform_name, website_url, country_code, is_active
FROM ods_platform_ref
WHERE platform_code IN ('tiki', 'lazada')
ON CONFLICT (platform_code) DO UPDATE SET
    platform_name = EXCLUDED.platform_name,
    website_url = EXCLUDED.website_url,
    is_active = EXCLUDED.is_active;

-- Initialize basic category taxonomy
INSERT INTO ods_category_taxonomy (category_code, category_name, level)
VALUES
    ('electronics', 'Electronics', 1),
    ('smartphones', 'Smartphones', 2),
    ('laptops', 'Laptops', 2),
    ('tablets', 'Tablets', 2),
    ('headphones', 'Headphones', 2),
    ('cameras', 'Cameras', 2)
ON CONFLICT (category_code) DO NOTHING;

-- Copy to DWH category dimension
INSERT INTO dwh_dim_category (category_code, category_name, category_level, category_path)
SELECT
    category_code,
    category_name,
    level,
    category_code
FROM ods_category_taxonomy
ON CONFLICT (category_code) DO UPDATE SET
    category_name = EXCLUDED.category_name,
    category_level = EXCLUDED.category_level;

-- Create a function to generate date dimension data
CREATE OR REPLACE FUNCTION generate_date_dimension(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    curr_date DATE := start_date;
    v_date_sk INTEGER;
BEGIN
    WHILE curr_date <= end_date LOOP
        v_date_sk := TO_CHAR(curr_date, 'YYYYMMDD')::INTEGER;

        INSERT INTO dwh_dim_date (
            date_sk, date_value, day, month, quarter, year,
            week_of_year, is_weekend, day_name, month_name
        ) VALUES (
            v_date_sk,
            curr_date,
            EXTRACT(DAY FROM curr_date)::INTEGER,
            EXTRACT(MONTH FROM curr_date)::INTEGER,
            EXTRACT(QUARTER FROM curr_date)::INTEGER,
            EXTRACT(YEAR FROM curr_date)::INTEGER,
            EXTRACT(WEEK FROM curr_date)::INTEGER,
            EXTRACT(DOW FROM curr_date) IN (0, 6),
            TO_CHAR(curr_date, 'Day'),
            TO_CHAR(curr_date, 'Month')
        ) ON CONFLICT (date_sk) DO NOTHING;

        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Generate date dimension for current year
SELECT generate_date_dimension(
    DATE_TRUNC('year', CURRENT_DATE)::DATE,
    (DATE_TRUNC('year', CURRENT_DATE)::DATE + INTERVAL '1 year' - INTERVAL '1 day')::DATE
);

-- Create a view for monitoring batch processing
CREATE OR REPLACE VIEW v_batch_processing_stats AS
WITH daily_stats AS (
    SELECT
        DATE(created_at) as batch_date,
        source_platform,
        COUNT(*) as products_count
    FROM stg_raw_products
    WHERE source_platform IN ('tiki', 'lazada')
    GROUP BY DATE(created_at), source_platform
),
processing_pipeline_stats AS (
    SELECT
        DATE(first_seen) as processing_date,
        COUNT(*) as ods_products_count
    FROM ods_product_clean
    GROUP BY DATE(first_seen)
),
dwh_stats AS (
    SELECT
        dd.date_value,
        COUNT(DISTINCT fp.product_sk) as dwh_products_count,
        COUNT(*) as fact_records_count
    FROM dwh_fact_product_daily fp
    JOIN dwh_dim_date dd ON fp.date_sk = dd.date_sk
    GROUP BY dd.date_value
)
SELECT
    COALESCE(ds.batch_date, ps.processing_date, dw.date_value) as date,
    ds.source_platform,
    ds.products_count as staging_products,
    ps.ods_products_count,
    dw.dwh_products_count,
    dw.fact_records_count
FROM daily_stats ds
FULL OUTER JOIN processing_pipeline_stats ps ON ds.batch_date = ps.processing_date
FULL OUTER JOIN dwh_stats dw ON ds.batch_date = dw.date_value
ORDER BY date DESC, source_platform;

-- Grant necessary permissions (adjust user as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dss_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dss_user;

COMMENT ON VIEW v_batch_processing_stats IS 'Monitoring view for Tiki & Lazada batch processing pipeline';

-- Show setup completion message
SELECT 'Database setup completed successfully for Tiki & Lazada batch processing!' as status;