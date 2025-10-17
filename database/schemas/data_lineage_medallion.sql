-- =============================================================================
-- MEDALLION ARCHITECTURE: BRONZE → SILVER → GOLD DATA LINEAGE
-- =============================================================================
-- Implementation of Databricks Medallion Architecture for Vietnam E-commerce DSS
-- Bronze: Raw data ingestion (append-only, immutable)
-- Silver: Cleaned, validated, and conformed data
-- Gold: Business-ready, aggregated, and feature-engineered data
-- =============================================================================

-- Create schemas for medallion architecture
CREATE SCHEMA IF NOT EXISTS bronze_layer;
CREATE SCHEMA IF NOT EXISTS silver_layer;
CREATE SCHEMA IF NOT EXISTS gold_layer;
CREATE SCHEMA IF NOT EXISTS lineage_metadata;

-- =============================================================================
-- BRONZE LAYER: Raw Data Ingestion
-- =============================================================================

-- Bronze: Raw order data from multiple sources
CREATE TABLE bronze_layer.raw_orders (
    -- Technical columns
    _bronze_id BIGSERIAL PRIMARY KEY,
    _source_system VARCHAR(50) NOT NULL,
    _file_path TEXT,
    _file_name VARCHAR(200),
    _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _record_hash VARCHAR(64), -- MD5 hash for deduplication
    _is_processed BOOLEAN DEFAULT FALSE,

    -- Raw data (JSONB for flexibility)
    raw_data JSONB NOT NULL,

    -- Parsed key fields for indexing
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date DATE,
    platform VARCHAR(50),

    -- Data quality indicators
    _data_quality_score DECIMAL(3,2),
    _validation_errors JSONB,
    _is_duplicate BOOLEAN DEFAULT FALSE,
    _duplicate_of BIGINT REFERENCES bronze_layer.raw_orders(_bronze_id)
);

-- Bronze: Raw customer data
CREATE TABLE bronze_layer.raw_customers (
    _bronze_id BIGSERIAL PRIMARY KEY,
    _source_system VARCHAR(50) NOT NULL,
    _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _record_hash VARCHAR(64),
    _is_processed BOOLEAN DEFAULT FALSE,

    raw_data JSONB NOT NULL,

    -- Parsed key fields
    customer_id VARCHAR(50),
    email VARCHAR(150),
    phone VARCHAR(20),
    registration_date DATE,

    _data_quality_score DECIMAL(3,2),
    _validation_errors JSONB,
    _is_duplicate BOOLEAN DEFAULT FALSE
);

-- Bronze: Raw product data
CREATE TABLE bronze_layer.raw_products (
    _bronze_id BIGSERIAL PRIMARY KEY,
    _source_system VARCHAR(50) NOT NULL,
    _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _record_hash VARCHAR(64),
    _is_processed BOOLEAN DEFAULT FALSE,

    raw_data JSONB NOT NULL,

    -- Parsed key fields
    product_id VARCHAR(50),
    sku VARCHAR(50),
    product_name TEXT,
    category VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(15,0),

    _data_quality_score DECIMAL(3,2),
    _validation_errors JSONB,
    _is_duplicate BOOLEAN DEFAULT FALSE
);

-- Bronze: Raw interaction/behavioral data
CREATE TABLE bronze_layer.raw_interactions (
    _bronze_id BIGSERIAL PRIMARY KEY,
    _source_system VARCHAR(50) NOT NULL,
    _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _record_hash VARCHAR(64),
    _is_processed BOOLEAN DEFAULT FALSE,

    raw_data JSONB NOT NULL,

    -- Parsed key fields
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    event_type VARCHAR(50),
    event_timestamp TIMESTAMP,
    session_id VARCHAR(50),

    _data_quality_score DECIMAL(3,2),
    _validation_errors JSONB
);

-- =============================================================================
-- SILVER LAYER: Cleaned and Conformed Data
-- =============================================================================

-- Silver: Cleansed orders
CREATE TABLE silver_layer.orders_clean (
    -- Technical columns
    _silver_id BIGSERIAL PRIMARY KEY,
    _bronze_source_id BIGINT NOT NULL,
    _processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _data_quality_score DECIMAL(3,2),
    _silver_record_hash VARCHAR(64),

    -- Business columns
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    order_date DATE NOT NULL,
    order_timestamp TIMESTAMP NOT NULL,

    -- Order details
    platform VARCHAR(50) NOT NULL,
    channel VARCHAR(30), -- Online, Mobile, Physical
    order_status VARCHAR(30),
    payment_method VARCHAR(30),
    payment_status VARCHAR(20),
    shipping_method VARCHAR(30),

    -- Financial data (standardized to VND)
    subtotal_vnd DECIMAL(15,0) NOT NULL,
    tax_amount_vnd DECIMAL(15,0) DEFAULT 0,
    shipping_fee_vnd DECIMAL(12,0) DEFAULT 0,
    discount_amount_vnd DECIMAL(15,0) DEFAULT 0,
    total_amount_vnd DECIMAL(15,0) NOT NULL,

    -- Geographic data (standardized)
    shipping_province_code VARCHAR(10),
    shipping_province_name VARCHAR(100),
    shipping_region_code VARCHAR(10), -- MB, MT, MN
    billing_province_code VARCHAR(10),

    -- Flags
    is_first_order BOOLEAN,
    is_weekend_order BOOLEAN,
    is_holiday_order BOOLEAN,
    is_cod_order BOOLEAN,
    is_mobile_order BOOLEAN,

    -- Calculated fields
    order_processing_time_hours INTEGER,
    fulfillment_time_hours INTEGER,

    -- Lineage
    _created_from_bronze BIGINT REFERENCES bronze_layer.raw_orders(_bronze_id),
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uk_silver_orders_order_id UNIQUE (order_id)
);

-- Silver: Order line items
CREATE TABLE silver_layer.order_items_clean (
    _silver_id BIGSERIAL PRIMARY KEY,
    _processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    order_id VARCHAR(50) NOT NULL REFERENCES silver_layer.orders_clean(order_id),
    order_line_number INTEGER NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    sku VARCHAR(50),

    -- Product details
    product_name VARCHAR(300),
    category_l1 VARCHAR(100),
    category_l2 VARCHAR(100),
    category_l3 VARCHAR(100),
    brand VARCHAR(100),

    -- Pricing
    unit_list_price_vnd DECIMAL(15,0),
    unit_selling_price_vnd DECIMAL(15,0),
    unit_cost_vnd DECIMAL(15,0),
    discount_amount_vnd DECIMAL(15,0) DEFAULT 0,

    -- Quantities
    quantity_ordered INTEGER NOT NULL,
    quantity_shipped INTEGER DEFAULT 0,
    quantity_returned INTEGER DEFAULT 0,

    -- Extended amounts
    line_total_vnd DECIMAL(15,0) NOT NULL,

    -- Lineage
    _created_from_bronze BIGINT,
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (order_id, order_line_number)
);

-- Silver: Customer master data
CREATE TABLE silver_layer.customers_clean (
    _silver_id BIGSERIAL PRIMARY KEY,
    _processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _data_quality_score DECIMAL(3,2),

    customer_id VARCHAR(50) NOT NULL,
    email VARCHAR(150),
    phone VARCHAR(20),

    -- Personal information (standardized)
    full_name VARCHAR(200),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(10), -- M, F, O, U
    age_calculated INTEGER,

    -- Address (standardized to Vietnam format)
    address_line_1 VARCHAR(200),
    address_line_2 VARCHAR(200),
    ward_name VARCHAR(100),
    district_name VARCHAR(100),
    province_code VARCHAR(10),
    province_name VARCHAR(100),
    region_code VARCHAR(10), -- MB, MT, MN
    postal_code VARCHAR(10),

    -- Registration and lifecycle
    registration_date DATE,
    registration_channel VARCHAR(50),
    acquisition_source VARCHAR(100),

    -- Customer status
    status VARCHAR(20), -- Active, Inactive, Suspended
    is_verified BOOLEAN DEFAULT FALSE,

    -- Preferences
    preferred_language VARCHAR(10) DEFAULT 'vi',
    communication_preference VARCHAR(20),
    marketing_opt_in BOOLEAN DEFAULT TRUE,

    -- Computed fields
    account_age_days INTEGER,
    is_vip_customer BOOLEAN DEFAULT FALSE,

    -- SCD Type 2 support
    effective_date DATE DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,

    -- Lineage
    _created_from_bronze BIGINT,
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uk_silver_customers_id_effective UNIQUE (customer_id, effective_date)
);

-- Silver: Product master data
CREATE TABLE silver_layer.products_clean (
    _silver_id BIGSERIAL PRIMARY KEY,
    _processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _data_quality_score DECIMAL(3,2),

    product_id VARCHAR(50) NOT NULL,
    sku VARCHAR(50),
    product_name VARCHAR(300) NOT NULL,
    product_description TEXT,

    -- Product hierarchy (standardized)
    category_l1_code VARCHAR(20),
    category_l1_name VARCHAR(100),
    category_l2_code VARCHAR(20),
    category_l2_name VARCHAR(100),
    category_l3_code VARCHAR(20),
    category_l3_name VARCHAR(100),

    -- Brand and manufacturer
    brand_name VARCHAR(100),
    manufacturer_name VARCHAR(100),

    -- Pricing (standardized to VND)
    cost_vnd DECIMAL(15,0),
    list_price_vnd DECIMAL(15,0),
    current_price_vnd DECIMAL(15,0),

    -- Product attributes
    weight_kg DECIMAL(10,3),
    dimensions VARCHAR(100),
    color VARCHAR(50),
    size VARCHAR(50),

    -- Status flags
    is_active BOOLEAN DEFAULT TRUE,
    is_discontinued BOOLEAN DEFAULT FALSE,
    requires_shipping BOOLEAN DEFAULT TRUE,

    -- Vietnam specific
    country_of_origin VARCHAR(50),
    import_duty_rate DECIMAL(5,2),
    vat_rate DECIMAL(5,2) DEFAULT 10.00,

    -- Dates
    launch_date DATE,
    discontinue_date DATE,

    -- SCD Type 2 support
    effective_date DATE DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,

    -- Lineage
    _created_from_bronze BIGINT,
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uk_silver_products_id_effective UNIQUE (product_id, effective_date)
);

-- =============================================================================
-- GOLD LAYER: Business-Ready Aggregated Data
-- =============================================================================

-- Gold: Daily sales summary
CREATE TABLE gold_layer.daily_sales_summary (
    date_key INTEGER NOT NULL,
    platform VARCHAR(50),
    region_code VARCHAR(10),
    category_l1 VARCHAR(100),

    -- Core metrics
    total_orders INTEGER DEFAULT 0,
    total_customers INTEGER DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    returning_customers INTEGER DEFAULT 0,
    total_items_sold INTEGER DEFAULT 0,

    -- Revenue metrics (VND)
    gross_revenue_vnd DECIMAL(18,0) DEFAULT 0,
    net_revenue_vnd DECIMAL(18,0) DEFAULT 0,
    discount_amount_vnd DECIMAL(18,0) DEFAULT 0,
    shipping_revenue_vnd DECIMAL(18,0) DEFAULT 0,
    tax_amount_vnd DECIMAL(18,0) DEFAULT 0,

    -- Average metrics
    avg_order_value_vnd DECIMAL(15,0) DEFAULT 0,
    avg_items_per_order DECIMAL(8,2) DEFAULT 0,
    avg_discount_per_order_vnd DECIMAL(12,0) DEFAULT 0,

    -- Payment method breakdown
    cod_orders INTEGER DEFAULT 0,
    cod_revenue_vnd DECIMAL(18,0) DEFAULT 0,
    online_payment_orders INTEGER DEFAULT 0,
    online_payment_revenue_vnd DECIMAL(18,0) DEFAULT 0,

    -- Device breakdown
    mobile_orders INTEGER DEFAULT 0,
    mobile_revenue_vnd DECIMAL(18,0) DEFAULT 0,
    desktop_orders INTEGER DEFAULT 0,
    desktop_revenue_vnd DECIMAL(18,0) DEFAULT 0,

    -- Performance metrics
    conversion_rate DECIMAL(5,4) DEFAULT 0,
    return_rate DECIMAL(5,4) DEFAULT 0,
    cancellation_rate DECIMAL(5,4) DEFAULT 0,

    -- Growth metrics (vs previous period)
    revenue_growth_percentage DECIMAL(8,2),
    order_growth_percentage DECIMAL(8,2),
    customer_growth_percentage DECIMAL(8,2),

    -- Metadata
    aggregated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_date DATE NOT NULL,

    PRIMARY KEY (date_key, platform, region_code, category_l1)
);

-- Gold: Customer 360 view
CREATE TABLE gold_layer.customer_360_view (
    customer_id VARCHAR(50) PRIMARY KEY,

    -- Basic info
    customer_name VARCHAR(200),
    email VARCHAR(150),
    registration_date DATE,
    customer_age_group VARCHAR(20),
    region_name VARCHAR(50),

    -- Transaction summary (last 365 days)
    total_orders INTEGER DEFAULT 0,
    total_items_purchased INTEGER DEFAULT 0,
    total_spent_vnd DECIMAL(18,0) DEFAULT 0,
    avg_order_value_vnd DECIMAL(15,0) DEFAULT 0,
    first_order_date DATE,
    last_order_date DATE,
    days_since_last_order INTEGER,

    -- RFM scores
    recency_score INTEGER CHECK (recency_score BETWEEN 1 AND 5),
    frequency_score INTEGER CHECK (frequency_score BETWEEN 1 AND 5),
    monetary_score INTEGER CHECK (monetary_score BETWEEN 1 AND 5),
    rfm_segment VARCHAR(30),

    -- Behavioral patterns
    preferred_platform VARCHAR(50),
    preferred_payment_method VARCHAR(30),
    preferred_category VARCHAR(100),
    is_mobile_user BOOLEAN DEFAULT FALSE,
    avg_session_duration_minutes INTEGER,

    -- Predictive metrics
    clv_predicted_vnd DECIMAL(18,0),
    churn_probability DECIMAL(3,2) CHECK (churn_probability BETWEEN 0 AND 1),
    next_purchase_probability DECIMAL(3,2),
    recommended_products JSONB,

    -- Customer tier
    customer_tier VARCHAR(20), -- Bronze, Silver, Gold, Platinum, Diamond
    tier_points INTEGER DEFAULT 0,

    -- Engagement metrics
    email_open_rate DECIMAL(5,4),
    click_through_rate DECIMAL(5,4),
    social_engagement_score DECIMAL(3,2),

    -- Satisfaction
    avg_rating DECIMAL(3,2),
    nps_score INTEGER CHECK (nps_score BETWEEN -100 AND 100),

    -- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Lineage tracking
    _source_silver_records JSONB -- Track which silver records contributed
);

-- Gold: Product performance metrics
CREATE TABLE gold_layer.product_performance_metrics (
    product_id VARCHAR(50) PRIMARY KEY,
    sku VARCHAR(50),
    product_name VARCHAR(300),
    category_hierarchy VARCHAR(300), -- L1 > L2 > L3
    brand_name VARCHAR(100),

    -- Sales performance (last 90 days)
    total_quantity_sold INTEGER DEFAULT 0,
    total_revenue_vnd DECIMAL(18,0) DEFAULT 0,
    total_orders INTEGER DEFAULT 0,
    avg_selling_price_vnd DECIMAL(15,0),

    -- Ranking metrics
    category_rank INTEGER,
    brand_rank INTEGER,
    overall_rank INTEGER,

    -- Performance indicators
    velocity_score DECIMAL(8,2), -- Sales velocity
    trend_direction VARCHAR(20), -- Up, Down, Stable
    seasonality_pattern VARCHAR(50),

    -- Customer metrics
    unique_customers INTEGER DEFAULT 0,
    repeat_purchase_rate DECIMAL(5,4),
    customer_satisfaction_score DECIMAL(3,2),
    review_count INTEGER DEFAULT 0,
    avg_rating DECIMAL(3,2),

    -- Inventory metrics
    current_stock_level INTEGER,
    stock_turn_rate DECIMAL(8,2),
    days_of_supply INTEGER,
    stockout_events INTEGER DEFAULT 0,

    -- Profitability
    gross_margin_percentage DECIMAL(5,2),
    contribution_margin_vnd DECIMAL(18,0),
    promotion_effectiveness_score DECIMAL(3,2),

    -- Predictive metrics
    forecasted_demand_30d INTEGER,
    demand_forecast_confidence DECIMAL(3,2),
    price_elasticity DECIMAL(6,4),
    optimal_price_vnd DECIMAL(15,0),

    -- Lifecycle stage
    product_lifecycle_stage VARCHAR(30), -- Introduction, Growth, Maturity, Decline
    days_since_launch INTEGER,

    -- Cross-sell analysis
    frequently_bought_together JSONB, -- Array of product_ids
    upsell_opportunities JSONB,

    -- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_as_of_date DATE NOT NULL
);

-- =============================================================================
-- LINEAGE METADATA TRACKING
-- =============================================================================

-- Track data lineage between layers
CREATE TABLE lineage_metadata.data_lineage_log (
    lineage_id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_run_id VARCHAR(50) NOT NULL,

    -- Source information
    source_layer VARCHAR(20) NOT NULL CHECK (source_layer IN ('bronze', 'silver', 'gold')),
    source_schema VARCHAR(50) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    source_record_count BIGINT,

    -- Target information
    target_layer VARCHAR(20) NOT NULL CHECK (target_layer IN ('silver', 'gold')),
    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    target_record_count BIGINT,

    -- Processing information
    transformation_logic TEXT,
    processing_start_time TIMESTAMP NOT NULL,
    processing_end_time TIMESTAMP,
    processing_duration_seconds INTEGER,
    processing_status VARCHAR(20) CHECK (processing_status IN ('Running', 'Success', 'Failed', 'Warning')),

    -- Data quality metrics
    records_processed BIGINT,
    records_inserted BIGINT,
    records_updated BIGINT,
    records_rejected BIGINT,
    data_quality_score DECIMAL(5,2),

    -- Error handling
    error_message TEXT,
    error_details JSONB,

    -- Resource usage
    cpu_usage_seconds INTEGER,
    memory_usage_mb INTEGER,
    disk_io_mb INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data transformation rules
CREATE TABLE lineage_metadata.transformation_rules (
    rule_id BIGSERIAL PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    source_layer VARCHAR(20) NOT NULL,
    target_layer VARCHAR(20) NOT NULL,
    transformation_type VARCHAR(50) NOT NULL, -- Clean, Validate, Aggregate, Enrich
    business_logic TEXT NOT NULL,
    sql_template TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Bronze layer indexes
CREATE INDEX idx_bronze_orders_source_timestamp ON bronze_layer.raw_orders(_source_system, _ingestion_timestamp);
CREATE INDEX idx_bronze_orders_processed ON bronze_layer.raw_orders(_is_processed) WHERE _is_processed = FALSE;
CREATE INDEX idx_bronze_orders_order_id ON bronze_layer.raw_orders(order_id);
CREATE INDEX idx_bronze_orders_hash ON bronze_layer.raw_orders(_record_hash);

-- Silver layer indexes
CREATE INDEX idx_silver_orders_customer_date ON silver_layer.orders_clean(customer_id, order_date);
CREATE INDEX idx_silver_orders_platform_date ON silver_layer.orders_clean(platform, order_date);
CREATE INDEX idx_silver_customers_current ON silver_layer.customers_clean(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_silver_products_current ON silver_layer.products_clean(is_current) WHERE is_current = TRUE;

-- Gold layer indexes
CREATE INDEX idx_gold_daily_sales_date ON gold_layer.daily_sales_summary(data_date);
CREATE INDEX idx_gold_customer_360_tier ON gold_layer.customer_360_view(customer_tier);
CREATE INDEX idx_gold_customer_360_segment ON gold_layer.customer_360_view(rfm_segment);

-- =============================================================================
-- VIEWS FOR DATA LINEAGE VISIBILITY
-- =============================================================================

-- Complete data lineage view
CREATE VIEW lineage_metadata.vw_data_lineage_complete AS
SELECT
    dll.lineage_id,
    dll.job_name,
    dll.job_run_id,

    -- Source details
    dll.source_layer || '.' || dll.source_schema || '.' || dll.source_table as source_full_name,
    dll.source_record_count,

    -- Target details
    dll.target_layer || '.' || dll.target_schema || '.' || dll.target_table as target_full_name,
    dll.target_record_count,

    -- Processing metrics
    dll.processing_start_time,
    dll.processing_end_time,
    dll.processing_duration_seconds,
    dll.processing_status,

    -- Data quality
    dll.data_quality_score,
    ROUND(dll.records_processed::DECIMAL / NULLIF(dll.source_record_count, 0) * 100, 2) as processing_coverage_percentage,
    ROUND(dll.records_rejected::DECIMAL / NULLIF(dll.records_processed, 0) * 100, 2) as rejection_rate_percentage,

    -- Performance metrics
    ROUND(dll.records_processed::DECIMAL / NULLIF(dll.processing_duration_seconds, 0), 0) as records_per_second,
    dll.cpu_usage_seconds,
    dll.memory_usage_mb

FROM lineage_metadata.data_lineage_log dll
ORDER BY dll.processing_start_time DESC;

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================

COMMENT ON SCHEMA bronze_layer IS 'Bronze layer: Raw data ingestion with minimal processing';
COMMENT ON SCHEMA silver_layer IS 'Silver layer: Cleaned, validated, and conformed data';
COMMENT ON SCHEMA gold_layer IS 'Gold layer: Business-ready aggregated and feature-engineered data';
COMMENT ON SCHEMA lineage_metadata IS 'Data lineage and transformation metadata tracking';

COMMENT ON TABLE bronze_layer.raw_orders IS 'Raw order data from all source systems with full audit trail';
COMMENT ON TABLE silver_layer.orders_clean IS 'Cleaned and standardized order data with business rules applied';
COMMENT ON TABLE gold_layer.daily_sales_summary IS 'Daily aggregated sales metrics for reporting and analytics';
COMMENT ON TABLE gold_layer.customer_360_view IS 'Complete customer view with predictive metrics and segmentation';

-- =============================================================================
-- END OF MEDALLION ARCHITECTURE
-- =============================================================================