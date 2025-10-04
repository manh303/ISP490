-- ====================================================================
-- DEPLOYMENT SCRIPT CHO VIETNAM DATA WAREHOUSE
-- ====================================================================
-- Description: Script deploy và setup hoàn chỉnh data warehouse Việt Nam
-- Run Order: Chạy sau khi database PostgreSQL đã được khởi tạo
-- Usage: psql -h localhost -U dss_user -d ecommerce_dss -f deploy_vietnam_datawarehouse.sql
-- ====================================================================

\echo '========================================='
\echo 'DEPLOYING VIETNAM DATA WAREHOUSE SCHEMA'
\echo '========================================='

-- Check PostgreSQL version
SELECT version();

-- Check current database and user
SELECT current_database(), current_user, now();

-- ====================================================================
-- 1. CREATE SCHEMA AND EXTENSIONS
-- ====================================================================

\echo 'Creating schema and extensions...'

-- Create Vietnam Data Warehouse schema
CREATE SCHEMA IF NOT EXISTS vietnam_dw;

-- Required extensions for Vietnam DW
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";    -- UUID generation
CREATE EXTENSION IF NOT EXISTS "pg_trgm";      -- Text similarity
CREATE EXTENSION IF NOT EXISTS "btree_gin";    -- GIN indexes
CREATE EXTENSION IF NOT EXISTS "btree_gist";   -- GIST indexes

-- Grant schema permissions
GRANT ALL PRIVILEGES ON SCHEMA vietnam_dw TO dss_user;
GRANT USAGE ON SCHEMA vietnam_dw TO public;

\echo 'Schema and extensions created successfully!'

-- ====================================================================
-- 2. CREATE DIMENSION TABLES
-- ====================================================================

\echo 'Creating dimension tables...'

-- Set search path
SET search_path TO vietnam_dw, public;

-- 2.1 Time Dimension with Vietnamese holidays
\echo 'Creating dim_time...'
\i 'vietnam_datawarehouse_schema.sql'

-- Verify tables were created
\echo 'Verifying dimension tables...'
SELECT schemaname, tablename, tableowner
FROM pg_tables
WHERE schemaname = 'vietnam_dw'
  AND tablename LIKE 'dim_%'
ORDER BY tablename;

\echo 'Dimension tables created successfully!'

-- ====================================================================
-- 3. POPULATE TIME DIMENSION (2020-2030)
-- ====================================================================

\echo 'Populating time dimension with Vietnamese holidays...'

INSERT INTO vietnam_dw.dim_time (
    date_value,
    day_of_week,
    day_name_vietnamese,
    day_name_english,
    day_of_month,
    day_of_year,
    week_of_year,
    week_start_date,
    week_end_date,
    month_number,
    month_name_vietnamese,
    month_name_english,
    month_short_vietnamese,
    month_start_date,
    month_end_date,
    quarter_number,
    quarter_name_vietnamese,
    quarter_name_english,
    quarter_start_date,
    quarter_end_date,
    year_number,
    year_start_date,
    year_end_date,
    is_vietnamese_holiday,
    vietnamese_holiday_name,
    holiday_type,
    is_tet_season,
    is_ghost_month,
    is_mid_autumn,
    is_christmas_season,
    is_shopping_festival,
    shopping_festival_name,
    is_weekend,
    is_business_day,
    is_school_day,
    fiscal_year,
    fiscal_quarter,
    fiscal_month
)
SELECT
    generate_series AS date_value,
    EXTRACT(DOW FROM generate_series) + 1 AS day_of_week,
    CASE EXTRACT(DOW FROM generate_series)
        WHEN 0 THEN 'Chủ Nhật'
        WHEN 1 THEN 'Thứ Hai'
        WHEN 2 THEN 'Thứ Ba'
        WHEN 3 THEN 'Thứ Tư'
        WHEN 4 THEN 'Thứ Năm'
        WHEN 5 THEN 'Thứ Sáu'
        WHEN 6 THEN 'Thứ Bảy'
    END AS day_name_vietnamese,
    CASE EXTRACT(DOW FROM generate_series)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name_english,
    EXTRACT(DAY FROM generate_series)::INTEGER AS day_of_month,
    EXTRACT(DOY FROM generate_series)::INTEGER AS day_of_year,
    EXTRACT(WEEK FROM generate_series)::INTEGER AS week_of_year,
    DATE_TRUNC('week', generate_series)::DATE AS week_start_date,
    (DATE_TRUNC('week', generate_series) + INTERVAL '6 days')::DATE AS week_end_date,
    EXTRACT(MONTH FROM generate_series)::INTEGER AS month_number,
    CASE EXTRACT(MONTH FROM generate_series)
        WHEN 1 THEN 'Tháng Một'
        WHEN 2 THEN 'Tháng Hai'
        WHEN 3 THEN 'Tháng Ba'
        WHEN 4 THEN 'Tháng Tư'
        WHEN 5 THEN 'Tháng Năm'
        WHEN 6 THEN 'Tháng Sáu'
        WHEN 7 THEN 'Tháng Bảy'
        WHEN 8 THEN 'Tháng Tám'
        WHEN 9 THEN 'Tháng Chín'
        WHEN 10 THEN 'Tháng Mười'
        WHEN 11 THEN 'Tháng Mười Một'
        WHEN 12 THEN 'Tháng Mười Hai'
    END AS month_name_vietnamese,
    TO_CHAR(generate_series, 'Month') AS month_name_english,
    CASE EXTRACT(MONTH FROM generate_series)
        WHEN 1 THEN 'T1' WHEN 2 THEN 'T2' WHEN 3 THEN 'T3'
        WHEN 4 THEN 'T4' WHEN 5 THEN 'T5' WHEN 6 THEN 'T6'
        WHEN 7 THEN 'T7' WHEN 8 THEN 'T8' WHEN 9 THEN 'T9'
        WHEN 10 THEN 'T10' WHEN 11 THEN 'T11' WHEN 12 THEN 'T12'
    END AS month_short_vietnamese,
    DATE_TRUNC('month', generate_series)::DATE AS month_start_date,
    (DATE_TRUNC('month', generate_series) + INTERVAL '1 month - 1 day')::DATE AS month_end_date,
    EXTRACT(QUARTER FROM generate_series)::INTEGER AS quarter_number,
    'Quý ' || EXTRACT(QUARTER FROM generate_series)::TEXT AS quarter_name_vietnamese,
    'Q' || EXTRACT(QUARTER FROM generate_series)::TEXT AS quarter_name_english,
    DATE_TRUNC('quarter', generate_series)::DATE AS quarter_start_date,
    (DATE_TRUNC('quarter', generate_series) + INTERVAL '3 months - 1 day')::DATE AS quarter_end_date,
    EXTRACT(YEAR FROM generate_series)::INTEGER AS year_number,
    DATE_TRUNC('year', generate_series)::DATE AS year_start_date,
    (DATE_TRUNC('year', generate_series) + INTERVAL '1 year - 1 day')::DATE AS year_end_date,

    -- Vietnamese holidays (simplified logic)
    CASE
        WHEN EXTRACT(MONTH FROM generate_series) = 1 AND EXTRACT(DAY FROM generate_series) = 1 THEN TRUE
        WHEN EXTRACT(MONTH FROM generate_series) = 4 AND EXTRACT(DAY FROM generate_series) = 30 THEN TRUE
        WHEN EXTRACT(MONTH FROM generate_series) = 5 AND EXTRACT(DAY FROM generate_series) = 1 THEN TRUE
        WHEN EXTRACT(MONTH FROM generate_series) = 9 AND EXTRACT(DAY FROM generate_series) = 2 THEN TRUE
        ELSE FALSE
    END AS is_vietnamese_holiday,

    CASE
        WHEN EXTRACT(MONTH FROM generate_series) = 1 AND EXTRACT(DAY FROM generate_series) = 1 THEN 'Tết Dương Lịch'
        WHEN EXTRACT(MONTH FROM generate_series) = 4 AND EXTRACT(DAY FROM generate_series) = 30 THEN 'Ngày Giải Phóng Miền Nam'
        WHEN EXTRACT(MONTH FROM generate_series) = 5 AND EXTRACT(DAY FROM generate_series) = 1 THEN 'Ngày Quốc Tế Lao Động'
        WHEN EXTRACT(MONTH FROM generate_series) = 9 AND EXTRACT(DAY FROM generate_series) = 2 THEN 'Ngày Quốc Khánh'
        ELSE NULL
    END AS vietnamese_holiday_name,

    CASE
        WHEN EXTRACT(MONTH FROM generate_series) = 1 AND EXTRACT(DAY FROM generate_series) = 1 THEN 'International'
        WHEN EXTRACT(MONTH FROM generate_series) = 4 AND EXTRACT(DAY FROM generate_series) = 30 THEN 'National'
        WHEN EXTRACT(MONTH FROM generate_series) = 5 AND EXTRACT(DAY FROM generate_series) = 1 THEN 'International'
        WHEN EXTRACT(MONTH FROM generate_series) = 9 AND EXTRACT(DAY FROM generate_series) = 2 THEN 'National'
        ELSE NULL
    END AS holiday_type,

    -- Tet season (January 15 - February 15, approximation)
    CASE
        WHEN (EXTRACT(MONTH FROM generate_series) = 1 AND EXTRACT(DAY FROM generate_series) >= 15)
          OR (EXTRACT(MONTH FROM generate_series) = 2 AND EXTRACT(DAY FROM generate_series) <= 15)
        THEN TRUE
        ELSE FALSE
    END AS is_tet_season,

    -- Ghost month (7th lunar month, approximated as August)
    CASE WHEN EXTRACT(MONTH FROM generate_series) = 8 THEN TRUE ELSE FALSE END AS is_ghost_month,

    -- Mid-autumn (September 15th approximation)
    CASE WHEN EXTRACT(MONTH FROM generate_series) = 9 AND EXTRACT(DAY FROM generate_series) = 15 THEN TRUE ELSE FALSE END AS is_mid_autumn,

    -- Christmas season (December 20-31)
    CASE WHEN EXTRACT(MONTH FROM generate_series) = 12 AND EXTRACT(DAY FROM generate_series) >= 20 THEN TRUE ELSE FALSE END AS is_christmas_season,

    -- Shopping festivals
    CASE
        WHEN EXTRACT(MONTH FROM generate_series) = 9 AND EXTRACT(DAY FROM generate_series) = 9 THEN TRUE
        WHEN EXTRACT(MONTH FROM generate_series) = 10 AND EXTRACT(DAY FROM generate_series) = 10 THEN TRUE
        WHEN EXTRACT(MONTH FROM generate_series) = 11 AND EXTRACT(DAY FROM generate_series) = 11 THEN TRUE
        WHEN EXTRACT(MONTH FROM generate_series) = 12 AND EXTRACT(DAY FROM generate_series) = 12 THEN TRUE
        ELSE FALSE
    END AS is_shopping_festival,

    CASE
        WHEN EXTRACT(MONTH FROM generate_series) = 9 AND EXTRACT(DAY FROM generate_series) = 9 THEN 'Ngày Mua Sắm 9.9'
        WHEN EXTRACT(MONTH FROM generate_series) = 10 AND EXTRACT(DAY FROM generate_series) = 10 THEN 'Ngày Mua Sắm 10.10'
        WHEN EXTRACT(MONTH FROM generate_series) = 11 AND EXTRACT(DAY FROM generate_series) = 11 THEN 'Ngày Độc Thân 11.11'
        WHEN EXTRACT(MONTH FROM generate_series) = 12 AND EXTRACT(DAY FROM generate_series) = 12 THEN 'Ngày Mua Sắm 12.12'
        ELSE NULL
    END AS shopping_festival_name,

    -- Weekend
    CASE WHEN EXTRACT(DOW FROM generate_series) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,

    -- Business day (not weekend and not major holiday)
    CASE
        WHEN EXTRACT(DOW FROM generate_series) IN (0, 6) THEN FALSE
        WHEN EXTRACT(MONTH FROM generate_series) = 1 AND EXTRACT(DAY FROM generate_series) = 1 THEN FALSE
        WHEN EXTRACT(MONTH FROM generate_series) = 4 AND EXTRACT(DAY FROM generate_series) = 30 THEN FALSE
        WHEN EXTRACT(MONTH FROM generate_series) = 5 AND EXTRACT(DAY FROM generate_series) = 1 THEN FALSE
        WHEN EXTRACT(MONTH FROM generate_series) = 9 AND EXTRACT(DAY FROM generate_series) = 2 THEN FALSE
        ELSE TRUE
    END AS is_business_day,

    -- School day (business day but not during summer vacation)
    CASE
        WHEN EXTRACT(DOW FROM generate_series) IN (0, 6) THEN FALSE
        WHEN EXTRACT(MONTH FROM generate_series) IN (6, 7, 8) THEN FALSE  -- Summer vacation
        ELSE TRUE
    END AS is_school_day,

    EXTRACT(YEAR FROM generate_series)::INTEGER AS fiscal_year,
    EXTRACT(QUARTER FROM generate_series)::INTEGER AS fiscal_quarter,
    EXTRACT(MONTH FROM generate_series)::INTEGER AS fiscal_month

FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL)
ON CONFLICT (date_value) DO NOTHING;

\echo 'Time dimension populated successfully!'

-- ====================================================================
-- 4. POPULATE GEOGRAPHY DIMENSION (63 Vietnamese Provinces)
-- ====================================================================

\echo 'Populating geography dimension with Vietnamese provinces...'

INSERT INTO vietnam_dw.dim_geography_vn (
    region_code, region_name_vietnamese, region_name_english,
    province_code, province_name_vietnamese, province_name_english, province_type,
    postal_code, population, economic_classification, is_major_ecommerce_hub
) VALUES
-- Miền Bắc (Northern Vietnam)
('MB', 'Miền Bắc', 'North', 'HN', 'Hà Nội', 'Hanoi', 'Thành phố TW', '100000', 8053663, 'Đô thị loại đặc biệt', TRUE),
('MB', 'Miền Bắc', 'North', 'HP', 'Hải Phòng', 'Hai Phong', 'Thành phố TW', '180000', 2028514, 'Đô thị loại I', TRUE),
('MB', 'Miền Bắc', 'North', 'QN', 'Quảng Ninh', 'Quang Ninh', 'Tỉnh', '200000', 1320324, 'Đô thị loại II', FALSE),
('MB', 'Miền Bắc', 'North', 'BG', 'Bắc Giang', 'Bac Giang', 'Tỉnh', '220000', 1803950, 'Nông thôn', FALSE),
('MB', 'Miền Bắc', 'North', 'BK', 'Bắc Kạn', 'Bac Kan', 'Tỉnh', '260000', 301957, 'Nông thôn', FALSE),

-- Miền Trung (Central Vietnam)
('MT', 'Miền Trung', 'Central', 'TH', 'Thanh Hóa', 'Thanh Hoa', 'Tỉnh', '440000', 3509543, 'Đô thị loại II', FALSE),
('MT', 'Miền Trung', 'Central', 'NA', 'Nghệ An', 'Nghe An', 'Tỉnh', '460000', 3327791, 'Nông thôn', FALSE),
('MT', 'Miền Trung', 'Central', 'HA', 'Hà Tĩnh', 'Ha Tinh', 'Tỉnh', '480000', 1288866, 'Nông thôn', FALSE),
('MT', 'Miền Trung', 'Central', 'QB', 'Quảng Bình', 'Quang Binh', 'Tỉnh', '510000', 857818, 'Nông thôn', FALSE),
('MT', 'Miền Trung', 'Central', 'QT', 'Quảng Trị', 'Quang Tri', 'Tỉnh', '520000', 608688, 'Nông thôn', FALSE),
('MT', 'Miền Trung', 'Central', 'HUE', 'Thừa Thiên Huế', 'Thua Thien Hue', 'Tỉnh', '530000', 1154812, 'Đô thị loại I', TRUE),
('MT', 'Miền Trung', 'Central', 'DN', 'Đà Nẵng', 'Da Nang', 'Thành phố TW', '550000', 1134310, 'Đô thị loại I', TRUE),
('MT', 'Miền Trung', 'Central', 'QN', 'Quảng Nam', 'Quang Nam', 'Tỉnh', '560000', 1495876, 'Đô thị loại II', FALSE),
('MT', 'Miền Trung', 'Central', 'QG', 'Quảng Ngãi', 'Quang Ngai', 'Tỉnh', '570000', 1240051, 'Nông thôn', FALSE),

-- Miền Nam (Southern Vietnam)
('MN', 'Miền Nam', 'South', 'HCM', 'TP. Hồ Chí Minh', 'Ho Chi Minh City', 'Thành phố TW', '700000', 9012781, 'Đô thị loại đặc biệt', TRUE),
('MN', 'Miền Nam', 'South', 'BD', 'Bình Dương', 'Binh Duong', 'Tỉnh', '750000', 2426561, 'Đô thị loại I', TRUE),
('MN', 'Miền Nam', 'South', 'DN', 'Đồng Nai', 'Dong Nai', 'Tỉnh', '760000', 3097107, 'Đô thị loại II', TRUE),
('MN', 'Miền Nam', 'South', 'BR', 'Bà Rịa - Vũng Tàu', 'Ba Ria - Vung Tau', 'Tỉnh', '790000', 1148313, 'Đô thị loại I', FALSE),
('MN', 'Miền Nam', 'South', 'CT', 'Cần Thơ', 'Can Tho', 'Thành phố TW', '900000', 1282028, 'Đô thị loại I', TRUE),
('MN', 'Miền Nam', 'South', 'AG', 'An Giang', 'An Giang', 'Tỉnh', '880000', 2053404, 'Nông thôn', FALSE),
('MN', 'Miền Nam', 'South', 'DT', 'Đồng Tháp', 'Dong Thap', 'Tỉnh', '870000', 1599504, 'Nông thôn', FALSE);

\echo 'Geography dimension populated with major Vietnamese provinces!'

-- ====================================================================
-- 5. POPULATE PLATFORM DIMENSION (Vietnamese E-commerce Platforms)
-- ====================================================================

\echo 'Populating platform dimension with Vietnamese e-commerce platforms...'

INSERT INTO vietnam_dw.dim_platform_vn (
    platform_id, platform_name, platform_name_vietnamese, platform_type,
    vietnam_entity_name, vietnam_market_share_percentage,
    commission_rate_percentage, supports_cod, supports_momo, supports_zalopay,
    supports_vnpay, has_mobile_app, customer_service_hours, launch_date_vietnam
) VALUES
('SHOPEE_VN', 'Shopee', 'Shopee Việt Nam', 'Marketplace',
 'Công ty TNHH Shopee', 35.2, 6.0, TRUE, TRUE, TRUE, FALSE, TRUE, '8:00-22:00', '2015-12-01'),

('LAZADA_VN', 'Lazada', 'Lazada Việt Nam', 'Marketplace',
 'Công ty TNHH Lazada Việt Nam', 28.5, 5.5, TRUE, TRUE, TRUE, TRUE, TRUE, '24/7', '2012-03-01'),

('TIKI_VN', 'Tiki', 'Tiki', 'B2C',
 'Công ty Cổ phần Ti Ki', 15.8, 8.0, TRUE, TRUE, FALSE, TRUE, TRUE, '8:00-21:00', '2010-03-01'),

('SENDO_VN', 'Sendo', 'Sen Đỏ', 'Marketplace',
 'Công ty Cổ phần Sen Đỏ', 10.3, 5.0, TRUE, TRUE, TRUE, FALSE, TRUE, '8:00-18:00', '2012-08-01'),

('FPTSHOP_VN', 'FPT Shop', 'FPT Shop', 'B2C',
 'Công ty Cổ phần Bán lẻ Kỹ thuật số FPT', 5.2, 4.0, TRUE, FALSE, FALSE, TRUE, TRUE, '8:00-22:00', '2012-12-01'),

('CELLPHONES_VN', 'CellphoneS', 'CellphoneS', 'B2C',
 'Công ty Cổ phần Viễn Thông Di Động', 3.5, 3.5, TRUE, TRUE, FALSE, FALSE, TRUE, '8:00-21:30', '2004-09-01'),

('THEGIOIDIDONG_VN', 'Thế Giới Di Động', 'Thế Giới Di Động', 'B2C',
 'Công ty Cổ phần Đầu tư Thế Giới Di Động', 1.5, 3.0, TRUE, FALSE, FALSE, FALSE, FALSE, '8:00-21:00', '2004-03-01');

\echo 'Platform dimension populated successfully!'

-- ====================================================================
-- 6. CREATE SAMPLE CUSTOMER AND PRODUCT DATA
-- ====================================================================

\echo 'Creating sample customer data...'

-- Sample customers (Vietnam-focused)
INSERT INTO vietnam_dw.dim_customer_vn (
    customer_id, full_name_vietnamese, email, phone_number, age, gender,
    geography_key, customer_segment_vietnamese, customer_segment_english,
    income_level_vietnamese, income_level_english, preferred_device,
    preferred_payment_method, registration_date, monthly_income_vnd, monthly_spending_vnd
)
SELECT
    'VN_CUST_' || LPAD(generate_series::TEXT, 6, '0'),
    'Khách hàng ' || generate_series,
    'customer' || generate_series || '@vietnam.com',
    '+84' || (900000000 + (random() * 99999999)::bigint),
    18 + (random() * 62)::integer,
    CASE WHEN random() > 0.5 THEN 'Nam' ELSE 'Nữ' END,
    (SELECT geography_key FROM vietnam_dw.dim_geography_vn ORDER BY random() LIMIT 1),
    CASE
        WHEN random() < 0.7 THEN 'Khách hàng thường'
        WHEN random() < 0.9 THEN 'Khách hàng VIP'
        ELSE 'Khách hàng cao cấp'
    END,
    CASE
        WHEN random() < 0.7 THEN 'Regular'
        WHEN random() < 0.9 THEN 'High_Value'
        ELSE 'Premium'
    END,
    CASE
        WHEN random() < 0.4 THEN 'Thu nhập thấp'
        WHEN random() < 0.8 THEN 'Thu nhập trung bình'
        ELSE 'Thu nhập cao'
    END,
    CASE
        WHEN random() < 0.4 THEN 'Low'
        WHEN random() < 0.8 THEN 'Middle'
        ELSE 'High'
    END,
    CASE
        WHEN random() < 0.75 THEN 'Mobile'
        WHEN random() < 0.95 THEN 'Desktop'
        ELSE 'Tablet'
    END,
    CASE
        WHEN random() < 0.5 THEN 'COD'
        WHEN random() < 0.75 THEN 'E_Wallet_MoMo'
        WHEN random() < 0.9 THEN 'Bank_Transfer'
        ELSE 'E_Wallet_ZaloPay'
    END,
    CURRENT_DATE - (random() * 1460)::integer, -- Random date within last 4 years
    5000000 + (random() * 45000000)::bigint, -- 5M - 50M VND monthly income
    500000 + (random() * 9500000)::bigint    -- 500K - 10M VND monthly spending
FROM generate_series(1, 1000);

\echo 'Sample customer data created!'

\echo 'Creating sample product data...'

-- Sample products (Vietnam e-commerce focused)
INSERT INTO vietnam_dw.dim_product_vn (
    product_id, product_name_vietnamese, product_name_english, brand_vietnamese, brand_english,
    category_level_1_vietnamese, category_level_1_english,
    category_level_2_vietnamese, category_level_2_english,
    price_vnd, original_price_vnd, discount_percentage, rating, review_count,
    price_tier_vietnamese, price_tier_english, vietnam_popularity_score,
    available_on_shopee, available_on_lazada, available_on_tiki,
    supports_cod, supports_momo, launch_date, data_source
)
SELECT
    'VN_PROD_' || LPAD(generate_series::TEXT, 6, '0'),
    products.name_vn,
    products.name_en,
    products.brand_vn,
    products.brand_en,
    products.cat1_vn,
    products.cat1_en,
    products.cat2_vn,
    products.cat2_en,
    (500000 + (random() * 49500000))::bigint, -- 500K - 50M VND
    (600000 + (random() * 54400000))::bigint, -- Original price higher
    (random() * 50)::numeric(5,2), -- 0-50% discount
    (1 + random() * 4)::numeric(3,2), -- 1-5 rating
    (random() * 5000)::integer, -- 0-5000 reviews
    CASE
        WHEN random() < 0.3 THEN 'Bình dân'
        WHEN random() < 0.7 THEN 'Tầm trung'
        WHEN random() < 0.9 THEN 'Cao cấp'
        ELSE 'Siêu cao cấp'
    END,
    CASE
        WHEN random() < 0.3 THEN 'Budget'
        WHEN random() < 0.7 THEN 'Mid-range'
        WHEN random() < 0.9 THEN 'Premium'
        ELSE 'Luxury'
    END,
    random()::numeric(3,2), -- Popularity score 0-1
    random() > 0.2, -- 80% available on Shopee
    random() > 0.3, -- 70% available on Lazada
    random() > 0.4, -- 60% available on Tiki
    random() > 0.1, -- 90% support COD
    random() > 0.3, -- 70% support MoMo
    CURRENT_DATE - (random() * 730)::integer, -- Launched within last 2 years
    'Vietnam_Market_Generator'
FROM generate_series(1, 500),
LATERAL (
    SELECT
        CASE (random() * 10)::integer
            WHEN 0 THEN 'Điện thoại ' || brands.brand_vn
            WHEN 1 THEN 'Laptop ' || brands.brand_vn
            WHEN 2 THEN 'Máy tính bảng ' || brands.brand_vn
            WHEN 3 THEN 'Tai nghe ' || brands.brand_vn
            WHEN 4 THEN 'Đồng hồ thông minh ' || brands.brand_vn
            WHEN 5 THEN 'Camera ' || brands.brand_vn
            WHEN 6 THEN 'Tivi ' || brands.brand_vn
            WHEN 7 THEN 'Tủ lạnh ' || brands.brand_vn
            WHEN 8 THEN 'Máy giặt ' || brands.brand_vn
            ELSE 'Điều hòa ' || brands.brand_vn
        END as name_vn,
        CASE (random() * 10)::integer
            WHEN 0 THEN 'Smartphone ' || brands.brand_en
            WHEN 1 THEN 'Laptop ' || brands.brand_en
            WHEN 2 THEN 'Tablet ' || brands.brand_en
            WHEN 3 THEN 'Headphones ' || brands.brand_en
            WHEN 4 THEN 'Smartwatch ' || brands.brand_en
            WHEN 5 THEN 'Camera ' || brands.brand_en
            WHEN 6 THEN 'Television ' || brands.brand_en
            WHEN 7 THEN 'Refrigerator ' || brands.brand_en
            WHEN 8 THEN 'Washing Machine ' || brands.brand_en
            ELSE 'Air Conditioner ' || brands.brand_en
        END as name_en,
        brands.brand_vn,
        brands.brand_en,
        'Điện tử' as cat1_vn,
        'Electronics' as cat1_en,
        CASE (random() * 5)::integer
            WHEN 0 THEN 'Điện thoại & Phụ kiện'
            WHEN 1 THEN 'Máy tính & Laptop'
            WHEN 2 THEN 'Gia dụng điện tử'
            WHEN 3 THEN 'Âm thanh & Hình ảnh'
            ELSE 'Thiết bị đeo thông minh'
        END as cat2_vn,
        CASE (random() * 5)::integer
            WHEN 0 THEN 'Mobile & Accessories'
            WHEN 1 THEN 'Computers & Laptops'
            WHEN 2 THEN 'Home Electronics'
            WHEN 3 THEN 'Audio & Video'
            ELSE 'Wearable Devices'
        END as cat2_en
    FROM (
        SELECT * FROM (VALUES
            ('Samsung', 'Samsung'),
            ('Apple', 'Apple'),
            ('Xiaomi', 'Xiaomi'),
            ('Oppo', 'Oppo'),
            ('Vivo', 'Vivo'),
            ('Huawei', 'Huawei'),
            ('LG', 'LG'),
            ('Sony', 'Sony'),
            ('Panasonic', 'Panasonic'),
            ('Bkav', 'Bkav')
        ) AS t(brand_vn, brand_en)
        ORDER BY random()
        LIMIT 1
    ) brands
) products;

\echo 'Sample product data created!'

-- ====================================================================
-- 7. VERIFY DEPLOYMENT
-- ====================================================================

\echo 'Verifying deployment...'

-- Check all tables
SELECT
    schemaname,
    tablename,
    tableowner,
    tablespace,
    hasindexes,
    hasrules,
    hastriggers
FROM pg_tables
WHERE schemaname = 'vietnam_dw'
ORDER BY tablename;

-- Check row counts
SELECT
    'dim_time' as table_name,
    count(*) as row_count,
    min(date_value) as min_date,
    max(date_value) as max_date
FROM vietnam_dw.dim_time
UNION ALL
SELECT
    'dim_geography_vn',
    count(*),
    null,
    null
FROM vietnam_dw.dim_geography_vn
UNION ALL
SELECT
    'dim_platform_vn',
    count(*),
    null,
    null
FROM vietnam_dw.dim_platform_vn
UNION ALL
SELECT
    'dim_customer_vn',
    count(*),
    null,
    null
FROM vietnam_dw.dim_customer_vn
UNION ALL
SELECT
    'dim_product_vn',
    count(*),
    null,
    null
FROM vietnam_dw.dim_product_vn;

-- Check indexes
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'vietnam_dw'
ORDER BY tablename, indexname;

-- ====================================================================
-- 8. CREATE MATERIALIZED VIEWS FOR PERFORMANCE
-- ====================================================================

\echo 'Creating materialized views...'

-- Monthly sales summary
CREATE MATERIALIZED VIEW vietnam_dw.mv_monthly_sales_summary AS
SELECT
    dt.year_number,
    dt.month_number,
    dt.month_name_vietnamese,
    dg.region_name_vietnamese,
    dp.platform_name,
    COUNT(*) as total_orders,
    SUM(fs.net_sales_amount_vnd) as total_revenue_vnd,
    AVG(fs.net_sales_amount_vnd) as avg_order_value_vnd,
    COUNT(DISTINCT fs.customer_key) as unique_customers
FROM vietnam_dw.fact_sales_vn fs
JOIN vietnam_dw.dim_time dt ON fs.time_key = dt.time_key
JOIN vietnam_dw.dim_geography_vn dg ON fs.geography_key = dg.geography_key
JOIN vietnam_dw.dim_platform_vn dp ON fs.platform_key = dp.platform_key
GROUP BY
    dt.year_number, dt.month_number, dt.month_name_vietnamese,
    dg.region_name_vietnamese, dp.platform_name;

-- Create index on materialized view
CREATE INDEX idx_mv_monthly_sales_year_month
ON vietnam_dw.mv_monthly_sales_summary(year_number, month_number);

\echo 'Materialized views created!'

-- ====================================================================
-- 9. GRANT PERMISSIONS
-- ====================================================================

\echo 'Setting up permissions...'

-- Grant all permissions to dss_user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA vietnam_dw TO dss_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA vietnam_dw TO dss_user;

-- Create read-only role for analysts
CREATE ROLE IF NOT EXISTS vietnam_analyst;
GRANT USAGE ON SCHEMA vietnam_dw TO vietnam_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA vietnam_dw TO vietnam_analyst;

\echo 'Permissions set up successfully!'

-- ====================================================================
-- 10. DEPLOYMENT SUMMARY
-- ====================================================================

\echo '================================================='
\echo 'VIETNAM DATA WAREHOUSE DEPLOYMENT COMPLETED!'
\echo '================================================='

\echo 'Summary:'
\echo '- Schema: vietnam_dw created'
\echo '- Dimension tables: 5 tables created and populated'
\echo '- Fact tables: 3 tables created (ready for data loading)'
\echo '- Aggregate tables: 2 tables created'
\echo '- Indexes: Performance indexes created'
\echo '- Materialized views: Created for reporting'
\echo '- Sample data: 1000 customers, 500 products'
\echo '- Time dimension: 2020-2030 with Vietnamese holidays'
\echo '- Geography: Major Vietnamese provinces'
\echo '- Platforms: Major Vietnamese e-commerce platforms'

\echo '================================================='
\echo 'READY TO RECEIVE PRODUCTION DATA!'
\echo '================================================='

-- Display connection info
SELECT
    'Database ready for Vietnam e-commerce analytics!' as status,
    current_database() as database,
    current_user as user,
    version() as postgresql_version,
    now() as deployed_at;