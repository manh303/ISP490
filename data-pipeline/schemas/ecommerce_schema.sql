-- =====================================================
-- E-COMMERCE DATA WAREHOUSE SCHEMA
-- =====================================================
-- Thiết kế schema chuẩn cho dữ liệu Tiki & Lazada
-- Áp dụng Star Schema Design Pattern
-- Version: 1.0.0

-- =====================================================
-- STAGING TABLES (Raw Data Layer)
-- =====================================================

-- Bảng staging cho raw product data từ crawlers
CREATE TABLE IF NOT EXISTS staging.raw_products (
    id SERIAL PRIMARY KEY,
    source_platform VARCHAR(50) NOT NULL,           -- tiki, lazada
    crawl_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB NOT NULL,                        -- Dữ liệu JSON gốc từ crawler
    url TEXT NOT NULL,
    file_path TEXT,                                 -- Path của file JSON gốc
    data_hash VARCHAR(64),                          -- MD5 hash để dedup
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_products_platform_time ON staging.raw_products(source_platform, crawl_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_products_hash ON staging.raw_products(data_hash);

-- =====================================================
-- ODS TABLES (Operational Data Store)
-- =====================================================

-- Bảng ODS products chuẩn hóa
CREATE TABLE IF NOT EXISTS ods.products (
    product_id SERIAL PRIMARY KEY,
    source_platform VARCHAR(50) NOT NULL,
    external_product_id VARCHAR(255) NOT NULL,      -- ID từ platform gốc

    -- Basic Product Info
    title TEXT NOT NULL,
    description TEXT,
    category VARCHAR(255),
    subcategory VARCHAR(255),
    brand VARCHAR(255),

    -- URLs & Images
    product_url TEXT NOT NULL,
    image_url TEXT,

    -- Pricing
    current_price DECIMAL(15,2),
    original_price DECIMAL(15,2),
    discount_percentage DECIMAL(5,2),
    discount_amount DECIMAL(15,2),
    currency VARCHAR(10) DEFAULT 'VND',

    -- Ratings & Reviews
    rating DECIMAL(3,2),
    review_count INTEGER DEFAULT 0,

    -- Sales Info
    sold_count INTEGER,
    stock_status VARCHAR(50),

    -- Shop Info
    shop_name VARCHAR(255),
    shop_rating DECIMAL(3,2),
    shop_location VARCHAR(255),

    -- Technical Fields
    first_seen TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,

    UNIQUE(source_platform, external_product_id)
);

CREATE INDEX IF NOT EXISTS idx_products_platform ON ods.products(source_platform);
CREATE INDEX IF NOT EXISTS idx_products_category ON ods.products(category);
CREATE INDEX IF NOT EXISTS idx_products_brand ON ods.products(brand);
CREATE INDEX IF NOT EXISTS idx_products_price ON ods.products(current_price);

-- Bảng ODS shops
CREATE TABLE IF NOT EXISTS ods.shops (
    shop_id SERIAL PRIMARY KEY,
    source_platform VARCHAR(50) NOT NULL,
    external_shop_id VARCHAR(255),
    shop_name VARCHAR(255) NOT NULL,
    shop_url TEXT,
    shop_rating DECIMAL(3,2),
    follower_count INTEGER,
    location VARCHAR(255),
    badges JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(source_platform, shop_name)
);

-- =====================================================
-- DIMENSION TABLES (Star Schema)
-- =====================================================

-- Dimension Date
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_sk INTEGER PRIMARY KEY,                    -- YYYYMMDD format
    date_actual DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_actual INTEGER,
    month_name VARCHAR(20),
    quarter_actual INTEGER,
    year_actual INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Dimension Platform
CREATE TABLE IF NOT EXISTS dwh.dim_platform (
    platform_sk SERIAL PRIMARY KEY,
    platform_code VARCHAR(50) UNIQUE NOT NULL,     -- tiki, lazada
    platform_name VARCHAR(100) NOT NULL,
    platform_url TEXT,
    market_share DECIMAL(5,2),
    country VARCHAR(10) DEFAULT 'VN',
    currency VARCHAR(10) DEFAULT 'VND',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Dimension Category
CREATE TABLE IF NOT EXISTS dwh.dim_category (
    category_sk SERIAL PRIMARY KEY,
    category_code VARCHAR(100),
    category_name VARCHAR(255) NOT NULL,
    parent_category VARCHAR(255),
    category_level INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Dimension Brand
CREATE TABLE IF NOT EXISTS dwh.dim_brand (
    brand_sk SERIAL PRIMARY KEY,
    brand_code VARCHAR(100),
    brand_name VARCHAR(255) NOT NULL,
    brand_country VARCHAR(10),
    brand_type VARCHAR(50),                         -- premium, budget, mid-range
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Dimension Shop
CREATE TABLE IF NOT EXISTS dwh.dim_shop (
    shop_sk SERIAL PRIMARY KEY,
    platform_sk INTEGER REFERENCES dwh.dim_platform(platform_sk),
    shop_external_id VARCHAR(255),
    shop_name VARCHAR(255) NOT NULL,
    shop_type VARCHAR(50),                          -- official, verified, normal
    location VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Dimension Product
CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_sk SERIAL PRIMARY KEY,
    platform_sk INTEGER REFERENCES dwh.dim_platform(platform_sk),
    category_sk INTEGER REFERENCES dwh.dim_category(category_sk),
    brand_sk INTEGER REFERENCES dwh.dim_brand(brand_sk),
    shop_sk INTEGER REFERENCES dwh.dim_shop(shop_sk),

    external_product_id VARCHAR(255) NOT NULL,
    product_title TEXT NOT NULL,
    product_url TEXT,

    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- FACT TABLES
-- =====================================================

-- Fact Product Daily Snapshot
CREATE TABLE IF NOT EXISTS dwh.fact_product_daily (
    fact_id SERIAL PRIMARY KEY,
    date_sk INTEGER NOT NULL REFERENCES dwh.dim_date(date_sk),
    product_sk INTEGER NOT NULL REFERENCES dwh.dim_product(product_sk),
    platform_sk INTEGER NOT NULL REFERENCES dwh.dim_platform(platform_sk),
    category_sk INTEGER REFERENCES dwh.dim_category(category_sk),
    brand_sk INTEGER REFERENCES dwh.dim_brand(brand_sk),
    shop_sk INTEGER REFERENCES dwh.dim_shop(shop_sk),

    -- Price Metrics
    current_price DECIMAL(15,2),
    original_price DECIMAL(15,2),
    discount_amount DECIMAL(15,2),
    discount_percentage DECIMAL(5,2),

    -- Rating Metrics
    rating_score DECIMAL(3,2),
    review_count INTEGER DEFAULT 0,
    new_reviews_count INTEGER DEFAULT 0,

    -- Sales Metrics
    sold_count INTEGER DEFAULT 0,
    daily_sold_count INTEGER DEFAULT 0,
    stock_status VARCHAR(50),

    -- Flags
    is_on_promotion BOOLEAN DEFAULT FALSE,
    is_new_product BOOLEAN DEFAULT FALSE,
    is_trending BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(date_sk, product_sk)
);

-- Fact Price Changes
CREATE TABLE IF NOT EXISTS dwh.fact_price_changes (
    change_id SERIAL PRIMARY KEY,
    product_sk INTEGER NOT NULL REFERENCES dwh.dim_product(product_sk),
    change_date DATE NOT NULL,
    old_price DECIMAL(15,2),
    new_price DECIMAL(15,2),
    price_change_amount DECIMAL(15,2),
    price_change_percentage DECIMAL(5,2),
    change_type VARCHAR(20),                        -- increase, decrease
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- DATA MARTS
-- =====================================================

-- Data Mart: Price Analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS marts.price_analytics AS
SELECT
    dp.platform_sk,
    dpl.platform_name,
    dc.category_name,
    db.brand_name,
    DATE_TRUNC('month', dd.date_actual) as month_year,

    -- Price Statistics
    AVG(fpd.current_price) as avg_price,
    MIN(fpd.current_price) as min_price,
    MAX(fpd.current_price) as max_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fpd.current_price) as median_price,

    -- Discount Statistics
    AVG(fpd.discount_percentage) as avg_discount_pct,
    COUNT(CASE WHEN fpd.discount_percentage > 0 THEN 1 END) as products_on_sale,
    COUNT(*) as total_products,

    -- Rating Statistics
    AVG(fpd.rating_score) as avg_rating,
    SUM(fpd.review_count) as total_reviews

FROM dwh.fact_product_daily fpd
JOIN dwh.dim_date dd ON fpd.date_sk = dd.date_sk
JOIN dwh.dim_product dp ON fpd.product_sk = dp.product_sk
JOIN dwh.dim_platform dpl ON fpd.platform_sk = dpl.platform_sk
JOIN dwh.dim_category dc ON fpd.category_sk = dc.category_sk
JOIN dwh.dim_brand db ON fpd.brand_sk = db.brand_sk
WHERE dd.date_actual >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY dp.platform_sk, dpl.platform_name, dc.category_name, db.brand_name, DATE_TRUNC('month', dd.date_actual);

CREATE INDEX IF NOT EXISTS idx_price_analytics_platform_category ON marts.price_analytics(platform_sk, category_name);

-- =====================================================
-- METADATA & CONTROL TABLES
-- =====================================================

-- ETL Control Table
CREATE TABLE IF NOT EXISTS control.etl_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'RUNNING',           -- RUNNING, SUCCESS, FAILED
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB
);

-- Data Quality Rules
CREATE TABLE IF NOT EXISTS control.data_quality_rules (
    rule_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255),
    rule_type VARCHAR(50) NOT NULL,                 -- not_null, unique, range, format
    rule_definition JSONB NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    severity VARCHAR(20) DEFAULT 'ERROR',           -- ERROR, WARNING, INFO
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Results
CREATE TABLE IF NOT EXISTS control.data_quality_results (
    result_id SERIAL PRIMARY KEY,
    rule_id INTEGER REFERENCES control.data_quality_rules(rule_id),
    run_date DATE NOT NULL,
    records_checked INTEGER,
    records_failed INTEGER,
    pass_rate DECIMAL(5,2),
    status VARCHAR(20),                             -- PASSED, FAILED
    error_details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- INITIAL DATA SETUP
-- =====================================================

-- Insert initial platforms
INSERT INTO dwh.dim_platform (platform_code, platform_name, platform_url, market_share) VALUES
('tiki', 'Tiki', 'https://tiki.vn', 15.8),
('lazada', 'Lazada', 'https://lazada.vn', 28.5)
ON CONFLICT (platform_code) DO NOTHING;

-- Insert common categories
INSERT INTO dwh.dim_category (category_code, category_name, parent_category) VALUES
('electronics', 'Electronics', NULL),
('smartphones', 'Smartphones', 'Electronics'),
('laptops', 'Laptops', 'Electronics'),
('tablets', 'Tablets', 'Electronics'),
('headphones', 'Headphones', 'Electronics'),
('smartwatches', 'Smartwatches', 'Electronics'),
('cameras', 'Cameras', 'Electronics'),
('tvs', 'TVs', 'Electronics'),
('fashion', 'Fashion', NULL),
('beauty', 'Beauty & Personal Care', NULL),
('home', 'Home & Living', NULL),
('books', 'Books', NULL),
('sports', 'Sports & Outdoors', NULL)
ON CONFLICT DO NOTHING;

-- =====================================================
-- FUNCTIONS & PROCEDURES
-- =====================================================

-- Function to generate date dimension
CREATE OR REPLACE FUNCTION populate_date_dimension(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    current_date DATE;
BEGIN
    current_date := start_date;

    WHILE current_date <= end_date LOOP
        INSERT INTO dwh.dim_date (
            date_sk, date_actual, day_of_week, day_name, day_of_month,
            day_of_year, week_of_year, month_actual, month_name,
            quarter_actual, year_actual, is_weekend, is_holiday
        ) VALUES (
            TO_CHAR(current_date, 'YYYYMMDD')::INTEGER,
            current_date,
            EXTRACT(DOW FROM current_date),
            TO_CHAR(current_date, 'Day'),
            EXTRACT(DAY FROM current_date),
            EXTRACT(DOY FROM current_date),
            EXTRACT(WEEK FROM current_date),
            EXTRACT(MONTH FROM current_date),
            TO_CHAR(current_date, 'Month'),
            EXTRACT(QUARTER FROM current_date),
            EXTRACT(YEAR FROM current_date),
            CASE WHEN EXTRACT(DOW FROM current_date) IN (0, 6) THEN TRUE ELSE FALSE END,
            FALSE  -- Holiday detection can be added later
        ) ON CONFLICT (date_sk) DO NOTHING;

        current_date := current_date + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Populate date dimension for current year + 1 year future
SELECT populate_date_dimension('2024-01-01'::DATE, '2025-12-31'::DATE);

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

-- Fact table indexes
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_date ON dwh.fact_product_daily(date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_product ON dwh.fact_product_daily(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_platform ON dwh.fact_product_daily(platform_sk);
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_category ON dwh.fact_product_daily(category_sk);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_platform_date ON dwh.fact_product_daily(platform_sk, date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_category_date ON dwh.fact_product_daily(category_sk, date_sk);

COMMENT ON SCHEMA staging IS 'Raw data from crawlers - temporary storage';
COMMENT ON SCHEMA ods IS 'Operational Data Store - cleaned and standardized data';
COMMENT ON SCHEMA dwh IS 'Data Warehouse - star schema dimensions and facts';
COMMENT ON SCHEMA marts IS 'Data Marts - aggregated data for analytics';
COMMENT ON SCHEMA control IS 'ETL control and data quality monitoring';