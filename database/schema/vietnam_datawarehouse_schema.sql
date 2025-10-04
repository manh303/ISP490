-- ====================================================================
-- Vietnam E-commerce Data Warehouse Schema
-- ====================================================================
-- Complete dimensional model for Vietnam e-commerce business
-- Designed for Star Schema with Vietnamese business requirements
--
-- Author: DSS Team
-- Version: 1.0.0
-- Database: PostgreSQL
-- ====================================================================

-- Create Vietnam Data Warehouse Schema
CREATE SCHEMA IF NOT EXISTS vietnam_dw;

-- Set search path
SET search_path TO vietnam_dw, public;

-- ====================================================================
-- DIMENSION TABLES
-- ====================================================================

-- Time Dimension with Vietnamese holidays and seasons
CREATE TABLE vietnam_dw.dim_time_vn (
    time_key INTEGER PRIMARY KEY,
    date_value DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name_vn VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name_vn VARCHAR(15) NOT NULL,
    is_weekend BOOLEAN DEFAULT FALSE,
    is_holiday_vn BOOLEAN DEFAULT FALSE,
    holiday_name_vn VARCHAR(100),
    is_tet_season BOOLEAN DEFAULT FALSE,
    is_mid_autumn BOOLEAN DEFAULT FALSE,
    is_womens_day BOOLEAN DEFAULT FALSE,
    is_shopping_festival BOOLEAN DEFAULT FALSE,
    season_vn VARCHAR(20),
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer Dimension with Vietnamese demographics
CREATE TABLE vietnam_dw.dim_customer_vn (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200),
    email VARCHAR(100),
    phone_vn VARCHAR(20),
    gender VARCHAR(10),
    age_group VARCHAR(20),
    birth_year INTEGER,

    -- Vietnamese address details
    province_vn VARCHAR(50),
    district_vn VARCHAR(50),
    ward_vn VARCHAR(50),
    region_vn VARCHAR(20), -- North, Central, South
    city_tier INTEGER, -- 1, 2, 3 (Tier 1: HCM, HN)

    -- Customer behavior in Vietnam context
    preferred_language VARCHAR(10) DEFAULT 'vi',
    preferred_payment_vn VARCHAR(30),
    is_cod_preferred BOOLEAN DEFAULT FALSE,
    loyalty_tier_vn VARCHAR(20),
    registration_platform VARCHAR(30),

    -- Social and economic factors
    income_bracket_vn VARCHAR(30),
    education_level_vn VARCHAR(30),
    occupation_vn VARCHAR(50),
    family_status_vn VARCHAR(30),

    -- Customer lifecycle
    first_purchase_date DATE,
    last_purchase_date DATE,
    total_orders INTEGER DEFAULT 0,
    total_spent_vnd DECIMAL(18,0) DEFAULT 0,
    avg_order_value_vnd DECIMAL(15,0) DEFAULT 0,
    customer_lifetime_months INTEGER DEFAULT 0,

    -- SCD Type 2 tracking
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(customer_id, effective_date)
);

-- Product Dimension for Vietnamese market
CREATE TABLE vietnam_dw.dim_product_vn (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name_vn VARCHAR(300),
    product_name_en VARCHAR(300),
    brand_vn VARCHAR(100),

    -- Category hierarchy for Vietnam market
    category_level_1_vn VARCHAR(100), -- Electronics, Fashion, etc.
    category_level_2_vn VARCHAR(100), -- Smartphones, Clothing, etc.
    category_level_3_vn VARCHAR(100), -- iPhone, T-shirts, etc.

    -- Product characteristics
    product_type_vn VARCHAR(50),
    is_imported BOOLEAN DEFAULT FALSE,
    country_of_origin VARCHAR(50),
    is_luxury BOOLEAN DEFAULT FALSE,

    -- Pricing in VND
    cost_price_vnd DECIMAL(15,0),
    retail_price_vnd DECIMAL(15,0),
    discount_price_vnd DECIMAL(15,0),
    min_price_vnd DECIMAL(15,0),
    max_price_vnd DECIMAL(15,0),

    -- Availability and logistics
    is_available BOOLEAN DEFAULT TRUE,
    stock_quantity INTEGER DEFAULT 0,
    is_cod_eligible BOOLEAN DEFAULT TRUE,
    shipping_weight_kg DECIMAL(8,2),

    -- Vietnam-specific attributes
    is_tet_product BOOLEAN DEFAULT FALSE,
    is_seasonal BOOLEAN DEFAULT FALSE,
    target_age_group_vn VARCHAR(30),
    target_gender_vn VARCHAR(20),

    -- Ratings and reviews
    average_rating DECIMAL(3,2),
    review_count INTEGER DEFAULT 0,

    -- SCD Type 2 tracking
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(product_id, effective_date)
);

-- Geography Dimension for Vietnam
CREATE TABLE vietnam_dw.dim_geography_vn (
    geography_key SERIAL PRIMARY KEY,
    province_code VARCHAR(10) NOT NULL,
    province_name_vn VARCHAR(50) NOT NULL,
    province_name_en VARCHAR(50),
    region_vn VARCHAR(20) NOT NULL, -- North, Central, South

    -- Administrative details
    region_code VARCHAR(5),
    area_km2 DECIMAL(10,2),
    population INTEGER,
    population_density DECIMAL(10,2),

    -- Economic indicators
    gdp_per_capita_vnd DECIMAL(15,0),
    economic_development_level VARCHAR(20),
    city_tier INTEGER, -- 1, 2, 3
    is_metropolitan BOOLEAN DEFAULT FALSE,

    -- E-commerce infrastructure
    internet_penetration_percent DECIMAL(5,2),
    smartphone_penetration_percent DECIMAL(5,2),
    ecommerce_readiness_score DECIMAL(3,2),
    logistics_hub_score DECIMAL(3,2),

    -- Shipping and logistics
    average_delivery_days INTEGER,
    cod_availability BOOLEAN DEFAULT TRUE,
    express_delivery_available BOOLEAN DEFAULT FALSE,

    -- Cultural and social factors
    primary_language VARCHAR(10) DEFAULT 'vi',
    secondary_languages TEXT,
    major_festivals TEXT,
    shopping_seasons TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(province_code)
);

-- Platform Dimension for Vietnam e-commerce platforms
CREATE TABLE vietnam_dw.dim_platform_vn (
    platform_key SERIAL PRIMARY KEY,
    platform_id VARCHAR(30) NOT NULL UNIQUE,
    platform_name_vn VARCHAR(100) NOT NULL,
    platform_name_en VARCHAR(100),
    platform_type VARCHAR(30), -- Marketplace, Direct, Social Commerce

    -- Platform characteristics
    market_share_percent DECIMAL(5,2),
    commission_rate_percent DECIMAL(4,2),
    is_international BOOLEAN DEFAULT FALSE,
    launch_year_vn INTEGER,

    -- Payment methods supported
    supports_cod BOOLEAN DEFAULT TRUE,
    supports_momo BOOLEAN DEFAULT FALSE,
    supports_zalopay BOOLEAN DEFAULT FALSE,
    supports_banking BOOLEAN DEFAULT TRUE,
    supports_visa_mastercard BOOLEAN DEFAULT FALSE,

    -- Features and services
    has_logistics_service BOOLEAN DEFAULT FALSE,
    has_installment_service BOOLEAN DEFAULT FALSE,
    has_livestream_shopping BOOLEAN DEFAULT FALSE,
    has_social_features BOOLEAN DEFAULT FALSE,

    -- Target demographics
    primary_age_group_vn VARCHAR(30),
    primary_income_group_vn VARCHAR(30),
    urban_rural_focus VARCHAR(20),

    -- Performance metrics
    average_delivery_days INTEGER,
    customer_satisfaction_score DECIMAL(3,2),
    return_rate_percent DECIMAL(4,2),

    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ====================================================================
-- FACT TABLES
-- ====================================================================

-- Sales Fact Table
CREATE TABLE vietnam_dw.fact_sales_vn (
    sale_key BIGSERIAL PRIMARY KEY,

    -- Foreign keys to dimensions
    time_key INTEGER REFERENCES vietnam_dw.dim_time_vn(time_key),
    customer_key INTEGER REFERENCES vietnam_dw.dim_customer_vn(customer_key),
    product_key INTEGER REFERENCES vietnam_dw.dim_product_vn(product_key),
    geography_key INTEGER REFERENCES vietnam_dw.dim_geography_vn(geography_key),
    platform_key INTEGER REFERENCES vietnam_dw.dim_platform_vn(platform_key),

    -- Business keys
    order_id VARCHAR(50) NOT NULL,
    order_item_id VARCHAR(50),

    -- Sale details
    sale_date DATE NOT NULL,
    sale_time TIME,
    quantity INTEGER NOT NULL DEFAULT 1,

    -- Pricing in VND (Vietnamese Dong)
    unit_price_vnd DECIMAL(15,0) NOT NULL,
    discount_amount_vnd DECIMAL(15,0) DEFAULT 0,
    final_price_vnd DECIMAL(15,0) NOT NULL,
    total_amount_vnd DECIMAL(18,0) NOT NULL,

    -- Taxes and fees (Vietnam specific)
    vat_amount_vnd DECIMAL(15,0) DEFAULT 0,
    platform_fee_vnd DECIMAL(15,0) DEFAULT 0,
    shipping_fee_vnd DECIMAL(15,0) DEFAULT 0,
    cod_fee_vnd DECIMAL(15,0) DEFAULT 0,

    -- Payment information
    payment_method_vietnamese VARCHAR(50),
    is_cod_order BOOLEAN DEFAULT FALSE,
    is_installment_order BOOLEAN DEFAULT FALSE,
    installment_months INTEGER,

    -- Order characteristics
    order_status_vn VARCHAR(30),
    is_first_order BOOLEAN DEFAULT FALSE,
    is_repeat_customer BOOLEAN DEFAULT FALSE,
    is_gift_order BOOLEAN DEFAULT FALSE,

    -- Vietnam-specific indicators
    is_tet_order BOOLEAN DEFAULT FALSE,
    is_festival_order BOOLEAN DEFAULT FALSE,
    is_flash_sale BOOLEAN DEFAULT FALSE,
    is_livestream_order BOOLEAN DEFAULT FALSE,

    -- Logistics
    shipping_method_vn VARCHAR(50),
    estimated_delivery_days INTEGER,
    actual_delivery_days INTEGER,
    is_express_delivery BOOLEAN DEFAULT FALSE,

    -- Customer behavior
    customer_rating INTEGER,
    customer_review_length INTEGER,
    has_customer_complaint BOOLEAN DEFAULT FALSE,

    -- Calculated measures
    profit_margin_vnd DECIMAL(15,0),
    profit_margin_percent DECIMAL(5,2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer Activity Fact Table
CREATE TABLE vietnam_dw.fact_customer_activity_vn (
    activity_key BIGSERIAL PRIMARY KEY,

    -- Foreign keys
    time_key INTEGER REFERENCES vietnam_dw.dim_time_vn(time_key),
    customer_key INTEGER REFERENCES vietnam_dw.dim_customer_vn(customer_key),
    product_key INTEGER REFERENCES vietnam_dw.dim_product_vn(product_key),
    platform_key INTEGER REFERENCES vietnam_dw.dim_platform_vn(platform_key),

    -- Activity details
    session_id VARCHAR(100),
    activity_date DATE NOT NULL,
    activity_time TIME,
    activity_type_vn VARCHAR(50) NOT NULL, -- view, cart, wishlist, compare, review

    -- Activity metrics
    session_duration_minutes INTEGER,
    page_views INTEGER DEFAULT 1,
    products_viewed INTEGER DEFAULT 0,
    products_added_to_cart INTEGER DEFAULT 0,
    products_purchased INTEGER DEFAULT 0,

    -- Device and channel information
    device_type_vn VARCHAR(30),
    operating_system VARCHAR(30),
    browser_type VARCHAR(30),
    traffic_source_vn VARCHAR(50),

    -- Geographic context
    province_vn VARCHAR(50),
    city_vn VARCHAR(50),

    -- Engagement metrics
    bounce_rate BOOLEAN DEFAULT FALSE,
    conversion_in_session BOOLEAN DEFAULT FALSE,
    cart_abandonment BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory Fact Table
CREATE TABLE vietnam_dw.fact_inventory_vn (
    inventory_key BIGSERIAL PRIMARY KEY,

    -- Foreign keys
    time_key INTEGER REFERENCES vietnam_dw.dim_time_vn(time_key),
    product_key INTEGER REFERENCES vietnam_dw.dim_product_vn(product_key),
    platform_key INTEGER REFERENCES vietnam_dw.dim_platform_vn(platform_key),

    -- Inventory snapshot details
    snapshot_date DATE NOT NULL,

    -- Stock levels
    beginning_stock INTEGER DEFAULT 0,
    stock_received INTEGER DEFAULT 0,
    stock_sold INTEGER DEFAULT 0,
    stock_returned INTEGER DEFAULT 0,
    stock_damaged INTEGER DEFAULT 0,
    ending_stock INTEGER DEFAULT 0,

    -- Inventory values in VND
    beginning_value_vnd DECIMAL(18,0) DEFAULT 0,
    stock_received_value_vnd DECIMAL(18,0) DEFAULT 0,
    stock_sold_value_vnd DECIMAL(18,0) DEFAULT 0,
    ending_value_vnd DECIMAL(18,0) DEFAULT 0,

    -- Inventory metrics
    days_of_inventory DECIMAL(8,2),
    turnover_rate DECIMAL(8,4),
    stockout_days INTEGER DEFAULT 0,

    -- Vietnam-specific factors
    is_peak_season BOOLEAN DEFAULT FALSE,
    is_tet_inventory BOOLEAN DEFAULT FALSE,
    supplier_location_vn VARCHAR(50),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ====================================================================
-- AGGREGATE TABLES
-- ====================================================================

-- Daily Sales Aggregates
CREATE TABLE vietnam_dw.agg_daily_sales_vn (
    agg_key BIGSERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    platform_key INTEGER REFERENCES vietnam_dw.dim_platform_vn(platform_key),
    geography_key INTEGER REFERENCES vietnam_dw.dim_geography_vn(geography_key),

    -- Sales metrics
    total_orders INTEGER DEFAULT 0,
    total_customers INTEGER DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    returning_customers INTEGER DEFAULT 0,

    -- Revenue metrics in VND
    total_revenue_vnd DECIMAL(20,0) DEFAULT 0,
    total_discount_vnd DECIMAL(18,0) DEFAULT 0,
    total_shipping_fee_vnd DECIMAL(18,0) DEFAULT 0,
    total_profit_vnd DECIMAL(20,0) DEFAULT 0,

    -- Order characteristics
    avg_order_value_vnd DECIMAL(15,0) DEFAULT 0,
    avg_items_per_order DECIMAL(8,2) DEFAULT 0,
    cod_orders_count INTEGER DEFAULT 0,
    cod_orders_percent DECIMAL(5,2) DEFAULT 0,

    -- Product metrics
    total_products_sold INTEGER DEFAULT 0,
    unique_products_sold INTEGER DEFAULT 0,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(sale_date, platform_key, geography_key)
);

-- Monthly Customer Aggregates
CREATE TABLE vietnam_dw.agg_monthly_customers_vn (
    agg_key BIGSERIAL PRIMARY KEY,
    year_month DATE NOT NULL, -- First day of month
    geography_key INTEGER REFERENCES vietnam_dw.dim_geography_vn(geography_key),

    -- Customer metrics
    total_active_customers INTEGER DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    churned_customers INTEGER DEFAULT 0,
    reactivated_customers INTEGER DEFAULT 0,

    -- Customer value metrics in VND
    total_customer_value_vnd DECIMAL(20,0) DEFAULT 0,
    avg_customer_value_vnd DECIMAL(15,0) DEFAULT 0,
    customer_lifetime_value_vnd DECIMAL(18,0) DEFAULT 0,

    -- Behavioral metrics
    avg_orders_per_customer DECIMAL(8,2) DEFAULT 0,
    avg_session_duration_minutes DECIMAL(8,2) DEFAULT 0,
    customer_retention_rate_percent DECIMAL(5,2) DEFAULT 0,

    -- Demographics
    avg_customer_age DECIMAL(4,1),
    male_customers_percent DECIMAL(5,2) DEFAULT 0,
    female_customers_percent DECIMAL(5,2) DEFAULT 0,

    -- Geographic distribution
    urban_customers_percent DECIMAL(5,2) DEFAULT 0,
    rural_customers_percent DECIMAL(5,2) DEFAULT 0,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(year_month, geography_key)
);

-- ====================================================================
-- INDEXES FOR PERFORMANCE
-- ====================================================================

-- Time dimension indexes
CREATE INDEX idx_dim_time_vn_date ON vietnam_dw.dim_time_vn(date_value);
CREATE INDEX idx_dim_time_vn_year_month ON vietnam_dw.dim_time_vn(year, month);
CREATE INDEX idx_dim_time_vn_holidays ON vietnam_dw.dim_time_vn(is_holiday_vn, is_tet_season);

-- Customer dimension indexes
CREATE INDEX idx_dim_customer_vn_id ON vietnam_dw.dim_customer_vn(customer_id);
CREATE INDEX idx_dim_customer_vn_current ON vietnam_dw.dim_customer_vn(is_current);
CREATE INDEX idx_dim_customer_vn_region ON vietnam_dw.dim_customer_vn(region_vn);
CREATE INDEX idx_dim_customer_vn_province ON vietnam_dw.dim_customer_vn(province_vn);

-- Product dimension indexes
CREATE INDEX idx_dim_product_vn_id ON vietnam_dw.dim_product_vn(product_id);
CREATE INDEX idx_dim_product_vn_current ON vietnam_dw.dim_product_vn(is_current);
CREATE INDEX idx_dim_product_vn_category ON vietnam_dw.dim_product_vn(category_level_1_vn);
CREATE INDEX idx_dim_product_vn_brand ON vietnam_dw.dim_product_vn(brand_vn);

-- Geography dimension indexes
CREATE INDEX idx_dim_geography_vn_province ON vietnam_dw.dim_geography_vn(province_code);
CREATE INDEX idx_dim_geography_vn_region ON vietnam_dw.dim_geography_vn(region_vn);

-- Platform dimension indexes
CREATE INDEX idx_dim_platform_vn_id ON vietnam_dw.dim_platform_vn(platform_id);
CREATE INDEX idx_dim_platform_vn_active ON vietnam_dw.dim_platform_vn(is_active);

-- Sales fact indexes
CREATE INDEX idx_fact_sales_vn_date ON vietnam_dw.fact_sales_vn(sale_date);
CREATE INDEX idx_fact_sales_vn_customer ON vietnam_dw.fact_sales_vn(customer_key);
CREATE INDEX idx_fact_sales_vn_product ON vietnam_dw.fact_sales_vn(product_key);
CREATE INDEX idx_fact_sales_vn_platform ON vietnam_dw.fact_sales_vn(platform_key);
CREATE INDEX idx_fact_sales_vn_order ON vietnam_dw.fact_sales_vn(order_id);
CREATE INDEX idx_fact_sales_vn_cod ON vietnam_dw.fact_sales_vn(is_cod_order);
CREATE INDEX idx_fact_sales_vn_tet ON vietnam_dw.fact_sales_vn(is_tet_order);

-- Customer activity fact indexes
CREATE INDEX idx_fact_activity_vn_date ON vietnam_dw.fact_customer_activity_vn(activity_date);
CREATE INDEX idx_fact_activity_vn_customer ON vietnam_dw.fact_customer_activity_vn(customer_key);
CREATE INDEX idx_fact_activity_vn_session ON vietnam_dw.fact_customer_activity_vn(session_id);
CREATE INDEX idx_fact_activity_vn_type ON vietnam_dw.fact_customer_activity_vn(activity_type_vn);

-- Inventory fact indexes
CREATE INDEX idx_fact_inventory_vn_date ON vietnam_dw.fact_inventory_vn(snapshot_date);
CREATE INDEX idx_fact_inventory_vn_product ON vietnam_dw.fact_inventory_vn(product_key);

-- Aggregate table indexes
CREATE INDEX idx_agg_daily_sales_vn_date ON vietnam_dw.agg_daily_sales_vn(sale_date);
CREATE INDEX idx_agg_monthly_customers_vn_month ON vietnam_dw.agg_monthly_customers_vn(year_month);

-- ====================================================================
-- PARTITIONING
-- ====================================================================

-- Partition sales fact table by month
-- ALTER TABLE vietnam_dw.fact_sales_vn PARTITION BY RANGE (sale_date);

-- ====================================================================
-- VIEWS FOR COMMON QUERIES
-- ====================================================================

-- Current customer view (only current records)
CREATE OR REPLACE VIEW vietnam_dw.v_current_customers_vn AS
SELECT *
FROM vietnam_dw.dim_customer_vn
WHERE is_current = TRUE;

-- Current product view (only current records)
CREATE OR REPLACE VIEW vietnam_dw.v_current_products_vn AS
SELECT *
FROM vietnam_dw.dim_product_vn
WHERE is_current = TRUE;

-- Sales summary view with dimensions
CREATE OR REPLACE VIEW vietnam_dw.v_sales_summary_vn AS
SELECT
    s.sale_key,
    s.order_id,
    t.date_value as sale_date,
    c.customer_name,
    c.province_vn,
    c.region_vn,
    p.product_name_vn,
    p.category_level_1_vn,
    p.brand_vn,
    pl.platform_name_vn,
    s.quantity,
    s.unit_price_vnd,
    s.total_amount_vnd,
    s.payment_method_vietnamese,
    s.is_cod_order,
    s.is_tet_order,
    s.profit_margin_vnd
FROM vietnam_dw.fact_sales_vn s
LEFT JOIN vietnam_dw.dim_time_vn t ON s.time_key = t.time_key
LEFT JOIN vietnam_dw.v_current_customers_vn c ON s.customer_key = c.customer_key
LEFT JOIN vietnam_dw.v_current_products_vn p ON s.product_key = p.product_key
LEFT JOIN vietnam_dw.dim_platform_vn pl ON s.platform_key = pl.platform_key;

-- ====================================================================
-- FUNCTIONS AND PROCEDURES
-- ====================================================================

-- Function to get time key from date
CREATE OR REPLACE FUNCTION vietnam_dw.get_time_key(input_date DATE)
RETURNS INTEGER AS $$
BEGIN
    RETURN EXTRACT(YEAR FROM input_date) * 10000 +
           EXTRACT(MONTH FROM input_date) * 100 +
           EXTRACT(DAY FROM input_date);
END;
$$ LANGUAGE plpgsql;

-- Function to check if date is Tet season
CREATE OR REPLACE FUNCTION vietnam_dw.is_tet_season(input_date DATE)
RETURNS BOOLEAN AS $$
BEGIN
    -- Tet is typically in late January to mid February
    RETURN (EXTRACT(MONTH FROM input_date) = 1 AND EXTRACT(DAY FROM input_date) >= 20) OR
           (EXTRACT(MONTH FROM input_date) = 2 AND EXTRACT(DAY FROM input_date) <= 20);
END;
$$ LANGUAGE plpgsql;

-- Function to get customer key by customer ID
CREATE OR REPLACE FUNCTION vietnam_dw.get_customer_key(input_customer_id VARCHAR)
RETURNS INTEGER AS $$
DECLARE
    result_key INTEGER;
BEGIN
    SELECT customer_key INTO result_key
    FROM vietnam_dw.dim_customer_vn
    WHERE customer_id = input_customer_id AND is_current = TRUE
    LIMIT 1;

    RETURN COALESCE(result_key, -1); -- Return -1 if not found
END;
$$ LANGUAGE plpgsql;

-- Function to get product key by product ID
CREATE OR REPLACE FUNCTION vietnam_dw.get_product_key(input_product_id VARCHAR)
RETURNS INTEGER AS $$
DECLARE
    result_key INTEGER;
BEGIN
    SELECT product_key INTO result_key
    FROM vietnam_dw.dim_product_vn
    WHERE product_id = input_product_id AND is_current = TRUE
    LIMIT 1;

    RETURN COALESCE(result_key, -1); -- Return -1 if not found
END;
$$ LANGUAGE plpgsql;

-- ====================================================================
-- TRIGGERS FOR SCD TYPE 2
-- ====================================================================

-- Trigger function for customer SCD Type 2
CREATE OR REPLACE FUNCTION vietnam_dw.customer_scd_trigger()
RETURNS TRIGGER AS $$
BEGIN
    -- When updating a current record, expire it and insert new one
    IF OLD.is_current = TRUE AND NEW.is_current = TRUE THEN
        -- Expire the old record
        UPDATE vietnam_dw.dim_customer_vn
        SET expiry_date = CURRENT_DATE - INTERVAL '1 day',
            is_current = FALSE
        WHERE customer_key = OLD.customer_key;

        -- Insert new record
        INSERT INTO vietnam_dw.dim_customer_vn (
            customer_id, customer_name, email, phone_vn, gender, age_group,
            province_vn, district_vn, region_vn, preferred_payment_vn,
            loyalty_tier_vn, income_bracket_vn, effective_date, is_current
        ) VALUES (
            NEW.customer_id, NEW.customer_name, NEW.email, NEW.phone_vn, NEW.gender, NEW.age_group,
            NEW.province_vn, NEW.district_vn, NEW.region_vn, NEW.preferred_payment_vn,
            NEW.loyalty_tier_vn, NEW.income_bracket_vn, CURRENT_DATE, TRUE
        );

        RETURN NULL; -- Don't perform the original update
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for customer SCD
-- CREATE TRIGGER customer_scd_trigger
--     BEFORE UPDATE ON vietnam_dw.dim_customer_vn
--     FOR EACH ROW
--     EXECUTE FUNCTION vietnam_dw.customer_scd_trigger();

-- ====================================================================
-- PERMISSIONS
-- ====================================================================

-- Grant permissions to dss_user
GRANT USAGE ON SCHEMA vietnam_dw TO dss_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA vietnam_dw TO dss_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA vietnam_dw TO dss_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA vietnam_dw TO dss_user;

-- ====================================================================
-- COMMENTS FOR DOCUMENTATION
-- ====================================================================

COMMENT ON SCHEMA vietnam_dw IS 'Vietnam E-commerce Data Warehouse - Dimensional model for Vietnamese market';

COMMENT ON TABLE vietnam_dw.dim_time_vn IS 'Time dimension with Vietnamese holidays and cultural events';
COMMENT ON TABLE vietnam_dw.dim_customer_vn IS 'Customer dimension with Vietnamese demographics and SCD Type 2';
COMMENT ON TABLE vietnam_dw.dim_product_vn IS 'Product dimension for Vietnamese market with local categories';
COMMENT ON TABLE vietnam_dw.dim_geography_vn IS 'Vietnamese provinces and regions with e-commerce metrics';
COMMENT ON TABLE vietnam_dw.dim_platform_vn IS 'Vietnam e-commerce platforms (Shopee, Lazada, Tiki, etc.)';

COMMENT ON TABLE vietnam_dw.fact_sales_vn IS 'Sales transactions fact table with Vietnamese business rules';
COMMENT ON TABLE vietnam_dw.fact_customer_activity_vn IS 'Customer activity and behavior tracking';
COMMENT ON TABLE vietnam_dw.fact_inventory_vn IS 'Inventory levels and movements';

COMMENT ON TABLE vietnam_dw.agg_daily_sales_vn IS 'Daily sales aggregates by platform and geography';
COMMENT ON TABLE vietnam_dw.agg_monthly_customers_vn IS 'Monthly customer metrics and demographics';

-- ====================================================================
-- INITIAL SETUP COMPLETE
-- ====================================================================

-- Display completion message
DO $$
BEGIN
    RAISE NOTICE 'Vietnam E-commerce Data Warehouse schema created successfully!';
    RAISE NOTICE 'Schema: vietnam_dw';
    RAISE NOTICE 'Tables created: % dimension tables, % fact tables, % aggregate tables', 5, 3, 2;
    RAISE NOTICE 'Next step: Run deploy_vietnam_datawarehouse.sql to populate with sample data';
END $$;