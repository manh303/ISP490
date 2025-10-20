-- =============================================================================
-- ENTERPRISE STAR SCHEMA DDL FOR VIETNAM E-COMMERCE DSS
-- =============================================================================
-- Standards:
-- - Kimball methodology with proper fact/dimension naming
-- - Standardized partitioning strategy
-- - Performance optimization with proper indexing
-- - SCD Type 1/2 implementation
-- - Enterprise naming conventions
-- =============================================================================

-- Create dedicated schema for star schema
CREATE SCHEMA IF NOT EXISTS dw_core;
CREATE SCHEMA IF NOT EXISTS dw_staging;
CREATE SCHEMA IF NOT EXISTS dw_audit;

-- Set search path
SET search_path TO dw_core, public;

-- =============================================================================
-- DIMENSION TABLES (Standard Kimball Naming: dim_*)
-- =============================================================================

-- DIM_DATE: Core time dimension with Vietnam business calendar
-- =============================================================================
CREATE TABLE dw_core.dim_date (
    date_key INTEGER PRIMARY KEY, -- YYYYMMDD format
    date_value DATE UNIQUE NOT NULL,

    -- Date hierarchy
    day_of_week_key INTEGER NOT NULL CHECK (day_of_week_key BETWEEN 1 AND 7),
    day_of_week_name_vn VARCHAR(15) NOT NULL, -- Thứ Hai, Thứ Ba
    day_of_week_name_en VARCHAR(15) NOT NULL, -- Monday, Tuesday
    day_of_week_abbr_vn CHAR(4) NOT NULL, -- T2, T3
    day_of_week_abbr_en CHAR(3) NOT NULL, -- Mon, Tue

    day_of_month INTEGER NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_year INTEGER NOT NULL CHECK (day_of_year BETWEEN 1 AND 366),
    day_suffix VARCHAR(4) NOT NULL, -- st, nd, rd, th

    -- Week hierarchy
    week_of_year INTEGER NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    week_of_month INTEGER NOT NULL CHECK (week_of_month BETWEEN 1 AND 6),
    week_begin_date DATE NOT NULL,
    week_end_date DATE NOT NULL,

    -- Month hierarchy
    month_key INTEGER NOT NULL, -- YYYYMM format
    month_number INTEGER NOT NULL CHECK (month_number BETWEEN 1 AND 12),
    month_name_vn VARCHAR(20) NOT NULL, -- Tháng Một
    month_name_en VARCHAR(20) NOT NULL, -- January
    month_abbr_vn VARCHAR(10) NOT NULL, -- T1, T2
    month_abbr_en VARCHAR(10) NOT NULL, -- Jan, Feb
    month_begin_date DATE NOT NULL,
    month_end_date DATE NOT NULL,
    days_in_month INTEGER NOT NULL CHECK (days_in_month BETWEEN 28 AND 31),

    -- Quarter hierarchy
    quarter_key INTEGER NOT NULL, -- YYYYQ format
    quarter_number INTEGER NOT NULL CHECK (quarter_number BETWEEN 1 AND 4),
    quarter_name_vn VARCHAR(10) NOT NULL, -- Quý 1
    quarter_name_en VARCHAR(10) NOT NULL, -- Q1
    quarter_begin_date DATE NOT NULL,
    quarter_end_date DATE NOT NULL,

    -- Year hierarchy
    year_key INTEGER NOT NULL, -- YYYY format
    year_begin_date DATE NOT NULL,
    year_end_date DATE NOT NULL,
    is_leap_year BOOLEAN NOT NULL DEFAULT FALSE,
    days_in_year INTEGER NOT NULL CHECK (days_in_year IN (365, 366)),

    -- Business calendar flags
    is_weekend BOOLEAN NOT NULL DEFAULT FALSE,
    is_business_day BOOLEAN NOT NULL DEFAULT TRUE,
    is_holiday_vn BOOLEAN NOT NULL DEFAULT FALSE,
    is_public_holiday BOOLEAN NOT NULL DEFAULT FALSE,

    -- Vietnam specific holidays
    holiday_name_vn VARCHAR(100),
    holiday_name_en VARCHAR(100),
    holiday_type VARCHAR(30), -- National, Traditional, International

    -- Vietnam business seasons
    is_tet_season BOOLEAN NOT NULL DEFAULT FALSE,
    is_ghost_month BOOLEAN NOT NULL DEFAULT FALSE, -- Tháng Cô Hồn
    is_mid_autumn BOOLEAN NOT NULL DEFAULT FALSE, -- Tết Trung Thu
    is_christmas_season BOOLEAN NOT NULL DEFAULT FALSE,
    is_back_to_school BOOLEAN NOT NULL DEFAULT FALSE,

    -- Shopping events
    is_shopping_festival BOOLEAN NOT NULL DEFAULT FALSE,
    shopping_event_name VARCHAR(50), -- 9/9, 10/10, 11/11, 12/12

    -- Fiscal calendar (Vietnam: Jan-Dec)
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER NOT NULL CHECK (fiscal_quarter BETWEEN 1 AND 4),
    fiscal_month INTEGER NOT NULL CHECK (fiscal_month BETWEEN 1 AND 12),
    fiscal_week INTEGER NOT NULL CHECK (fiscal_week BETWEEN 1 AND 53),

    -- Relative date calculations
    days_ago INTEGER NOT NULL DEFAULT 0,
    weeks_ago INTEGER NOT NULL DEFAULT 0,
    months_ago INTEGER NOT NULL DEFAULT 0,
    quarters_ago INTEGER NOT NULL DEFAULT 0,
    years_ago INTEGER NOT NULL DEFAULT 0,

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- DIM_CUSTOMER: Customer dimension with SCD Type 2
-- =============================================================================
CREATE TABLE dw_core.dim_customer (
    customer_sk BIGSERIAL PRIMARY KEY, -- Surrogate key
    customer_nk VARCHAR(50) NOT NULL, -- Natural key (business key)

    -- Customer attributes
    customer_name VARCHAR(200),
    customer_email VARCHAR(150),
    customer_phone VARCHAR(20),
    date_of_birth DATE,
    gender_code VARCHAR(10) CHECK (gender_code IN ('M', 'F', 'O', 'U')),
    gender_desc VARCHAR(20),

    -- Address information
    address_line_1 VARCHAR(200),
    address_line_2 VARCHAR(200),
    ward_name VARCHAR(100),
    district_name VARCHAR(100),
    province_code VARCHAR(10),
    province_name VARCHAR(100),
    postal_code VARCHAR(10),
    country_code CHAR(2) DEFAULT 'VN',
    country_name VARCHAR(50) DEFAULT 'Vietnam',

    -- Geographic groupings
    region_code VARCHAR(10), -- MB, MT, MN
    region_name VARCHAR(50), -- Miền Bắc, Miền Trung, Miền Nam
    geo_tier VARCHAR(20), -- Urban_Tier1, Urban_Tier2, Rural

    -- Customer classification
    customer_type VARCHAR(20), -- Individual, Business
    customer_status VARCHAR(20), -- Active, Inactive, Suspended
    customer_tier VARCHAR(20), -- Bronze, Silver, Gold, Platinum, Diamond

    -- RFM Segmentation
    rfm_segment VARCHAR(30), -- Champion, Loyal, At_Risk, etc.
    rfm_recency_score INTEGER CHECK (rfm_recency_score BETWEEN 1 AND 5),
    rfm_frequency_score INTEGER CHECK (rfm_frequency_score BETWEEN 1 AND 5),
    rfm_monetary_score INTEGER CHECK (rfm_monetary_score BETWEEN 1 AND 5),

    -- Behavioral attributes
    acquisition_channel VARCHAR(50),
    preferred_language VARCHAR(10) DEFAULT 'vi',
    preferred_currency CHAR(3) DEFAULT 'VND',
    preferred_payment_method VARCHAR(30),
    communication_preference VARCHAR(20), -- Email, SMS, App, None

    -- Demographic segmentation
    age_group VARCHAR(20), -- 18-24, 25-34, 35-44, 45-54, 55+
    income_bracket VARCHAR(30), -- Low, Lower_Mid, Upper_Mid, High
    education_level VARCHAR(30),
    occupation_category VARCHAR(50),

    -- Digital behavior
    device_preference VARCHAR(20), -- Mobile, Desktop, Tablet
    platform_preference VARCHAR(30), -- Mobile_App, Website, Both

    -- Lifecycle dates
    registration_date DATE,
    first_purchase_date DATE,
    last_purchase_date DATE,
    last_login_date DATE,

    -- SCD Type 2 fields
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(30) NOT NULL,
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0 AND 1)
);

-- DIM_PRODUCT: Product dimension with hierarchy
-- =============================================================================
CREATE TABLE dw_core.dim_product (
    product_sk BIGSERIAL PRIMARY KEY, -- Surrogate key
    product_nk VARCHAR(50) NOT NULL, -- Natural key (SKU)

    -- Product identification
    product_name VARCHAR(300) NOT NULL,
    product_description TEXT,
    brand_name VARCHAR(100),
    manufacturer_name VARCHAR(100),
    model_number VARCHAR(100),

    -- Product hierarchy (5 levels)
    category_l1_code VARCHAR(20), -- Electronics
    category_l1_name VARCHAR(100),
    category_l2_code VARCHAR(20), -- Mobile & Accessories
    category_l2_name VARCHAR(100),
    category_l3_code VARCHAR(20), -- Smartphones
    category_l3_name VARCHAR(100),
    category_l4_code VARCHAR(20), -- Android Phones
    category_l4_name VARCHAR(100),
    category_l5_code VARCHAR(20), -- Premium Android
    category_l5_name VARCHAR(100),

    -- Product attributes
    product_size VARCHAR(50),
    product_color VARCHAR(50),
    product_weight_kg DECIMAL(10,3),
    product_dimensions VARCHAR(100),

    -- Pricing information
    standard_cost_vnd DECIMAL(15,0),
    list_price_vnd DECIMAL(15,0),
    current_price_vnd DECIMAL(15,0),
    price_tier VARCHAR(20), -- Budget, Mid_Range, Premium, Luxury

    -- Product flags
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_discontinued BOOLEAN NOT NULL DEFAULT FALSE,
    is_featured BOOLEAN NOT NULL DEFAULT FALSE,
    is_digital_product BOOLEAN NOT NULL DEFAULT FALSE,
    is_perishable BOOLEAN NOT NULL DEFAULT FALSE,
    requires_shipping BOOLEAN NOT NULL DEFAULT TRUE,

    -- Quality and compliance
    quality_grade VARCHAR(10), -- A, B, C
    country_of_origin VARCHAR(50),
    warranty_months INTEGER DEFAULT 0,
    certification_standards TEXT[],

    -- Vietnam specific
    vietnam_import_duty_rate DECIMAL(5,2),
    vietnam_excise_tax_rate DECIMAL(5,2),
    vietnam_vat_rate DECIMAL(5,2) DEFAULT 10.00,
    customs_code VARCHAR(20),

    -- Lifecycle dates
    launch_date DATE,
    discontinue_date DATE,
    last_sale_date DATE,

    -- SCD Type 2 fields
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(30) NOT NULL,
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0 AND 1)
);

-- DIM_STORE: Sales channel/platform dimension
-- =============================================================================
CREATE TABLE dw_core.dim_store (
    store_sk BIGSERIAL PRIMARY KEY,
    store_nk VARCHAR(50) NOT NULL,

    -- Store identification
    store_name VARCHAR(100) NOT NULL,
    store_code VARCHAR(20) NOT NULL,

    -- Store type hierarchy
    channel_type VARCHAR(30), -- Online, Physical, Hybrid
    platform_type VARCHAR(30), -- Marketplace, Direct, Retail
    platform_name VARCHAR(50), -- Shopee, Lazada, Tiki, Own_Website

    -- Geographic information
    store_address TEXT,
    ward_name VARCHAR(100),
    district_name VARCHAR(100),
    province_code VARCHAR(10),
    province_name VARCHAR(100),
    region_code VARCHAR(10),
    region_name VARCHAR(50),

    -- Store attributes
    store_format VARCHAR(30), -- Flagship, Standard, Mini, Kiosk
    store_tier VARCHAR(20), -- Tier1, Tier2, Tier3
    opening_hours VARCHAR(100),
    time_zone VARCHAR(50) DEFAULT 'Asia/Ho_Chi_Minh',

    -- Operational flags
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    accepts_online_orders BOOLEAN NOT NULL DEFAULT FALSE,
    offers_pickup BOOLEAN NOT NULL DEFAULT FALSE,
    offers_delivery BOOLEAN NOT NULL DEFAULT FALSE,

    -- Commission and fees
    commission_rate DECIMAL(5,2),
    transaction_fee_rate DECIMAL(5,2),
    listing_fee_vnd DECIMAL(10,0),

    -- Performance metrics
    store_grade VARCHAR(10), -- A, B, C, D
    customer_rating DECIMAL(3,2),
    fulfillment_rating DECIMAL(3,2),

    -- Lifecycle dates
    opening_date DATE,
    closing_date DATE,

    -- SCD Type 1 fields
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(30) NOT NULL
);

-- DIM_PROMOTION: Promotion and discount dimension
-- =============================================================================
CREATE TABLE dw_core.dim_promotion (
    promotion_sk BIGSERIAL PRIMARY KEY,
    promotion_nk VARCHAR(50) NOT NULL,

    -- Promotion identification
    promotion_name VARCHAR(200) NOT NULL,
    promotion_code VARCHAR(50),
    promotion_description TEXT,

    -- Promotion hierarchy
    campaign_name VARCHAR(200),
    campaign_type VARCHAR(30), -- Seasonal, Flash_Sale, Loyalty, Welcome
    promotion_type VARCHAR(30), -- Percentage, Fixed_Amount, BOGO, Free_Shipping

    -- Promotion details
    discount_percentage DECIMAL(5,2),
    discount_amount_vnd DECIMAL(12,0),
    max_discount_vnd DECIMAL(12,0),
    min_purchase_amount_vnd DECIMAL(12,0),

    -- Usage limits
    usage_limit_total INTEGER,
    usage_limit_per_customer INTEGER DEFAULT 1,

    -- Targeting
    target_customer_segments TEXT[],
    target_product_categories TEXT[],
    target_geographic_regions TEXT[],

    -- Validity period
    valid_from_date DATE NOT NULL,
    valid_to_date DATE NOT NULL,

    -- Flags
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_stackable BOOLEAN NOT NULL DEFAULT FALSE,
    requires_coupon_code BOOLEAN NOT NULL DEFAULT FALSE,

    -- SCD Type 1 fields
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(30) NOT NULL
);

-- =============================================================================
-- FACT TABLES (Standard Kimball Naming: fact_*)
-- =============================================================================

-- FACT_SALES: Main sales fact table (partitioned by date)
-- =============================================================================
CREATE TABLE dw_core.fact_sales (
    -- Surrogate keys to dimensions
    date_key INTEGER NOT NULL REFERENCES dw_core.dim_date(date_key),
    customer_sk BIGINT NOT NULL REFERENCES dw_core.dim_customer(customer_sk),
    product_sk BIGINT NOT NULL REFERENCES dw_core.dim_product(product_sk),
    store_sk BIGINT NOT NULL REFERENCES dw_core.dim_store(store_sk),
    promotion_sk BIGINT REFERENCES dw_core.dim_promotion(promotion_sk),

    -- Degenerate dimensions (transaction-level attributes)
    order_number VARCHAR(50) NOT NULL,
    order_line_number INTEGER NOT NULL,
    invoice_number VARCHAR(50),
    transaction_id VARCHAR(50),

    -- Quantity measures
    quantity_ordered INTEGER NOT NULL CHECK (quantity_ordered > 0),
    quantity_shipped INTEGER DEFAULT 0 CHECK (quantity_shipped >= 0),
    quantity_returned INTEGER DEFAULT 0 CHECK (quantity_returned >= 0),
    quantity_cancelled INTEGER DEFAULT 0 CHECK (quantity_cancelled >= 0),

    -- Price measures (VND)
    unit_list_price_vnd DECIMAL(15,0) NOT NULL,
    unit_selling_price_vnd DECIMAL(15,0) NOT NULL,
    unit_cost_vnd DECIMAL(15,0),
    unit_discount_vnd DECIMAL(15,0) DEFAULT 0,

    -- Extended amounts (VND)
    gross_sales_amount_vnd DECIMAL(18,0) NOT NULL,
    discount_amount_vnd DECIMAL(15,0) DEFAULT 0,
    net_sales_amount_vnd DECIMAL(18,0) NOT NULL,
    tax_amount_vnd DECIMAL(15,0) DEFAULT 0,
    shipping_fee_vnd DECIMAL(12,0) DEFAULT 0,
    total_order_amount_vnd DECIMAL(18,0) NOT NULL,

    -- Cost and margin measures
    total_cost_vnd DECIMAL(18,0),
    gross_profit_vnd DECIMAL(18,0),
    gross_margin_percentage DECIMAL(5,2),

    -- Platform fees (VND)
    platform_commission_vnd DECIMAL(12,0) DEFAULT 0,
    payment_processing_fee_vnd DECIMAL(10,0) DEFAULT 0,
    fulfillment_fee_vnd DECIMAL(10,0) DEFAULT 0,
    advertising_fee_vnd DECIMAL(10,0) DEFAULT 0,
    net_profit_vnd DECIMAL(18,0),

    -- Payment information
    payment_method_code VARCHAR(20), -- COD, CARD, EWALLET, BANK
    payment_method_desc VARCHAR(50),
    payment_status VARCHAR(20), -- Pending, Paid, Failed, Refunded

    -- Shipping information
    shipping_method_code VARCHAR(20),
    shipping_method_desc VARCHAR(50),
    shipping_provider VARCHAR(30),

    -- Order status
    order_status_code VARCHAR(20),
    order_status_desc VARCHAR(50),
    fulfillment_status_code VARCHAR(20),
    fulfillment_status_desc VARCHAR(50),

    -- Vietnam specific flags
    is_cod_order BOOLEAN DEFAULT FALSE,
    is_installment_order BOOLEAN DEFAULT FALSE,
    is_gift_order BOOLEAN DEFAULT FALSE,
    is_cross_border BOOLEAN DEFAULT FALSE,
    is_b2b_order BOOLEAN DEFAULT FALSE,

    -- Timestamps (Vietnam timezone)
    order_timestamp TIMESTAMP NOT NULL,
    payment_timestamp TIMESTAMP,
    shipped_timestamp TIMESTAMP,
    delivered_timestamp TIMESTAMP,

    -- Calculated metrics
    order_processing_time_hours INTEGER,
    fulfillment_time_hours INTEGER,
    delivery_time_hours INTEGER,

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(30) NOT NULL,

    -- Primary key
    PRIMARY KEY (date_key, order_number, order_line_number),

    -- Check constraints
    CONSTRAINT chk_quantities CHECK (
        quantity_shipped <= quantity_ordered AND
        quantity_returned <= quantity_shipped AND
        quantity_cancelled <= quantity_ordered
    ),
    CONSTRAINT chk_amounts CHECK (
        gross_sales_amount_vnd >= net_sales_amount_vnd AND
        net_sales_amount_vnd >= 0 AND
        discount_amount_vnd >= 0
    )
) PARTITION BY RANGE (date_key);

-- Create monthly partitions for fact_sales
CREATE TABLE dw_core.fact_sales_202410 PARTITION OF dw_core.fact_sales
    FOR VALUES FROM (20241001) TO (20241101);

CREATE TABLE dw_core.fact_sales_202411 PARTITION OF dw_core.fact_sales
    FOR VALUES FROM (20241101) TO (20241201);

CREATE TABLE dw_core.fact_sales_202412 PARTITION OF dw_core.fact_sales
    FOR VALUES FROM (20241201) TO (20250101);

-- =============================================================================
-- PERFORMANCE INDEXES
-- =============================================================================

-- Dimension table indexes
CREATE INDEX idx_dim_date_date_value ON dw_core.dim_date(date_value);
CREATE INDEX idx_dim_date_fiscal_year_month ON dw_core.dim_date(fiscal_year, fiscal_month);
CREATE INDEX idx_dim_date_is_business_day ON dw_core.dim_date(is_business_day) WHERE is_business_day = TRUE;

CREATE INDEX idx_dim_customer_nk ON dw_core.dim_customer(customer_nk);
CREATE INDEX idx_dim_customer_current ON dw_core.dim_customer(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_customer_tier_segment ON dw_core.dim_customer(customer_tier, rfm_segment);

CREATE INDEX idx_dim_product_nk ON dw_core.dim_product(product_nk);
CREATE INDEX idx_dim_product_current ON dw_core.dim_product(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_product_category_hierarchy ON dw_core.dim_product(category_l1_code, category_l2_code, category_l3_code);

CREATE INDEX idx_dim_store_nk ON dw_core.dim_store(store_nk);
CREATE INDEX idx_dim_store_platform ON dw_core.dim_store(platform_name, channel_type);

-- Fact table indexes (applied to partition template)
CREATE INDEX idx_fact_sales_customer ON dw_core.fact_sales(customer_sk);
CREATE INDEX idx_fact_sales_product ON dw_core.fact_sales(product_sk);
CREATE INDEX idx_fact_sales_store ON dw_core.fact_sales(store_sk);
CREATE INDEX idx_fact_sales_order_timestamp ON dw_core.fact_sales(order_timestamp);
CREATE INDEX idx_fact_sales_composite_dims ON dw_core.fact_sales(date_key, customer_sk, product_sk);

-- =============================================================================
-- AGGREGATE TABLES FOR PERFORMANCE
-- =============================================================================

-- Daily sales summary
CREATE TABLE dw_core.agg_sales_daily (
    date_key INTEGER NOT NULL,
    store_sk BIGINT NOT NULL,

    -- Measures
    total_orders INTEGER DEFAULT 0,
    total_customers INTEGER DEFAULT 0,
    total_items_sold INTEGER DEFAULT 0,
    total_revenue_vnd DECIMAL(18,0) DEFAULT 0,
    total_profit_vnd DECIMAL(18,0) DEFAULT 0,
    avg_order_value_vnd DECIMAL(15,0) DEFAULT 0,
    avg_items_per_order DECIMAL(8,2) DEFAULT 0,

    -- Vietnam specific KPIs
    cod_orders_count INTEGER DEFAULT 0,
    cod_revenue_vnd DECIMAL(18,0) DEFAULT 0,
    mobile_orders_count INTEGER DEFAULT 0,
    mobile_revenue_vnd DECIMAL(18,0) DEFAULT 0,

    -- Performance metrics
    order_conversion_rate DECIMAL(5,4) DEFAULT 0,
    gross_margin_percentage DECIMAL(5,2) DEFAULT 0,
    return_rate DECIMAL(5,4) DEFAULT 0,

    -- Timestamp
    aggregated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (date_key, store_sk),
    FOREIGN KEY (date_key) REFERENCES dw_core.dim_date(date_key),
    FOREIGN KEY (store_sk) REFERENCES dw_core.dim_store(store_sk)
);

-- =============================================================================
-- VIEWS FOR BUSINESS INTELLIGENCE
-- =============================================================================

-- Sales performance view
CREATE VIEW dw_core.vw_sales_performance AS
SELECT
    d.date_value,
    d.month_name_en,
    d.quarter_name_en,
    d.year_key,
    c.customer_tier,
    c.region_name,
    p.category_l1_name,
    p.brand_name,
    s.platform_name,

    COUNT(*) as total_transactions,
    SUM(f.quantity_ordered) as total_quantity,
    SUM(f.net_sales_amount_vnd) as total_revenue_vnd,
    AVG(f.net_sales_amount_vnd) as avg_transaction_value_vnd,
    SUM(f.gross_profit_vnd) as total_profit_vnd,
    AVG(f.gross_margin_percentage) as avg_margin_percentage,

    COUNT(CASE WHEN f.is_cod_order THEN 1 END) as cod_transactions,
    SUM(CASE WHEN f.is_cod_order THEN f.net_sales_amount_vnd ELSE 0 END) as cod_revenue_vnd

FROM dw_core.fact_sales f
JOIN dw_core.dim_date d ON f.date_key = d.date_key
JOIN dw_core.dim_customer c ON f.customer_sk = c.customer_sk AND c.is_current = TRUE
JOIN dw_core.dim_product p ON f.product_sk = p.product_sk AND p.is_current = TRUE
JOIN dw_core.dim_store s ON f.store_sk = s.store_sk
WHERE d.date_value >= CURRENT_DATE - INTERVAL '365 days'
GROUP BY
    d.date_value, d.month_name_en, d.quarter_name_en, d.year_key,
    c.customer_tier, c.region_name, p.category_l1_name, p.brand_name, s.platform_name
ORDER BY d.date_value DESC, total_revenue_vnd DESC;

-- =============================================================================
-- METADATA AND AUDIT TABLES
-- =============================================================================

-- Data lineage tracking
CREATE TABLE dw_audit.data_lineage (
    lineage_id BIGSERIAL PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    transformation_logic TEXT,
    data_volume_processed BIGINT,
    processing_start_time TIMESTAMP NOT NULL,
    processing_end_time TIMESTAMP,
    processing_status VARCHAR(20) CHECK (processing_status IN ('Running', 'Success', 'Failed', 'Partial')),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data quality monitoring
CREATE TABLE dw_audit.data_quality_checks (
    check_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    check_type VARCHAR(50) NOT NULL, -- Completeness, Accuracy, Validity, Consistency
    check_rule TEXT NOT NULL,
    expected_value TEXT,
    actual_value TEXT,
    check_status VARCHAR(20) CHECK (check_status IN ('Pass', 'Fail', 'Warning')),
    severity VARCHAR(20) CHECK (severity IN ('Critical', 'High', 'Medium', 'Low')),
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    remediation_action TEXT
);

-- =============================================================================
-- GRANTS AND PERMISSIONS
-- =============================================================================

-- Create roles
CREATE ROLE dw_admin;
CREATE ROLE dw_analyst;
CREATE ROLE dw_readonly;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA dw_core TO dw_admin;
GRANT USAGE ON SCHEMA dw_core TO dw_analyst, dw_readonly;

GRANT SELECT ON ALL TABLES IN SCHEMA dw_core TO dw_analyst, dw_readonly;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw_core TO dw_admin;
GRANT SELECT, INSERT, UPDATE ON dw_core.agg_sales_daily TO dw_analyst;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dw_core TO dw_admin, dw_analyst;

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================

COMMENT ON SCHEMA dw_core IS 'Core data warehouse schema following Kimball methodology';
COMMENT ON TABLE dw_core.dim_date IS 'Date dimension with Vietnam business calendar and holidays';
COMMENT ON TABLE dw_core.dim_customer IS 'Customer dimension with SCD Type 2 for historical tracking';
COMMENT ON TABLE dw_core.dim_product IS 'Product dimension with 5-level hierarchy and Vietnam-specific attributes';
COMMENT ON TABLE dw_core.fact_sales IS 'Main sales fact table partitioned by date_key for performance';
COMMENT ON TABLE dw_core.agg_sales_daily IS 'Daily sales aggregates for dashboard performance';

-- =============================================================================
-- END OF STAR SCHEMA DDL
-- =============================================================================