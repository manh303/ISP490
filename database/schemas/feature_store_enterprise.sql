-- =============================================================================
-- ENTERPRISE FEATURE STORE - ONLINE/OFFLINE ML FEATURES
-- =============================================================================
-- Complete feature store implementation inspired by Feast, Tecton, and Databricks
-- Supports: Feature definitions, versioning, online/offline serving, monitoring
-- Use cases: Real-time predictions, batch training, feature discovery
-- =============================================================================

-- Create schemas for feature store
CREATE SCHEMA IF NOT EXISTS feature_store;
CREATE SCHEMA IF NOT EXISTS feature_registry;
CREATE SCHEMA IF NOT EXISTS feature_monitoring;
CREATE SCHEMA IF NOT EXISTS online_features;
CREATE SCHEMA IF NOT EXISTS offline_features;

-- =============================================================================
-- FEATURE REGISTRY - METADATA MANAGEMENT
-- =============================================================================

-- Feature groups (logical grouping of related features)
CREATE TABLE feature_registry.feature_groups (
    feature_group_id BIGSERIAL PRIMARY KEY,
    feature_group_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,

    -- Ownership and governance
    owner_team VARCHAR(50) NOT NULL,
    business_domain VARCHAR(50), -- Customer, Product, Order, Marketing
    data_classification VARCHAR(20) CHECK (data_classification IN ('Public', 'Internal', 'Confidential', 'Restricted')),

    -- Source information
    source_table VARCHAR(200), -- schema.table for batch features
    source_stream VARCHAR(200), -- Kafka topic for streaming features
    primary_key_columns TEXT[] NOT NULL, -- Entity key columns

    -- Freshness and SLA
    expected_freshness_minutes INTEGER, -- How fresh the data should be
    sla_hours INTEGER, -- SLA for feature availability

    -- Versioning
    version INTEGER NOT NULL DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,

    -- Metadata
    tags JSONB DEFAULT '{}',
    documentation_url TEXT,
    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Individual feature definitions
CREATE TABLE feature_registry.features (
    feature_id BIGSERIAL PRIMARY KEY,
    feature_group_id BIGINT REFERENCES feature_registry.feature_groups(feature_group_id),

    -- Feature identification
    feature_name VARCHAR(100) NOT NULL,
    feature_full_name VARCHAR(200) GENERATED ALWAYS AS (
        (SELECT fg.feature_group_name FROM feature_registry.feature_groups fg WHERE fg.feature_group_id = features.feature_group_id)
        || '__' || feature_name
    ) STORED,

    -- Feature metadata
    description TEXT,
    feature_type VARCHAR(30) CHECK (feature_type IN (
        'categorical', 'numerical', 'boolean', 'timestamp', 'text', 'embedding', 'json'
    )),
    data_type VARCHAR(50) NOT NULL, -- PostgreSQL data type

    -- Value constraints
    min_value DECIMAL(20,4),
    max_value DECIMAL(20,4),
    allowed_values TEXT[], -- For categorical features
    default_value TEXT,

    -- Computation details
    computation_logic TEXT, -- SQL or transformation logic
    aggregation_function VARCHAR(50), -- sum, avg, count, max, min, last, first
    window_size_hours INTEGER, -- For time-window features

    -- Serving configuration
    is_online_feature BOOLEAN DEFAULT FALSE, -- Served in real-time
    is_offline_feature BOOLEAN DEFAULT TRUE, -- Available for training
    online_store_ttl_hours INTEGER DEFAULT 24, -- TTL for online store

    -- Statistics and monitoring
    null_rate_threshold DECIMAL(5,4) DEFAULT 0.1, -- Max allowed null rate
    expected_cardinality INTEGER, -- Expected number of unique values
    monitoring_enabled BOOLEAN DEFAULT TRUE,

    -- Versioning and lifecycle
    version INTEGER NOT NULL DEFAULT 1,
    status VARCHAR(20) DEFAULT 'Active' CHECK (status IN ('Active', 'Deprecated', 'Archived')),
    deprecation_date DATE,
    retirement_date DATE,

    -- Lineage
    source_features TEXT[], -- Features this one depends on
    derived_from_feature_id BIGINT REFERENCES feature_registry.features(feature_id),

    -- Metadata
    tags JSONB DEFAULT '{}',
    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(feature_group_id, feature_name, version)
);

-- Feature entities (subjects that features describe)
CREATE TABLE feature_registry.entities (
    entity_id BIGSERIAL PRIMARY KEY,
    entity_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,

    -- Entity metadata
    entity_type VARCHAR(50), -- customer, product, order, session
    primary_key_type VARCHAR(50), -- string, integer, uuid
    example_values TEXT[],

    -- Governance
    data_retention_days INTEGER,
    pii_classification BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Feature sets (collections of features for specific use cases)
CREATE TABLE feature_registry.feature_sets (
    feature_set_id BIGSERIAL PRIMARY KEY,
    feature_set_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,

    -- Use case information
    use_case VARCHAR(100), -- churn_prediction, recommendation, fraud_detection
    model_type VARCHAR(50), -- classification, regression, ranking, clustering
    target_variable VARCHAR(100),

    -- Performance expectations
    expected_model_performance JSONB, -- AUC, Precision, Recall targets
    feature_importance_threshold DECIMAL(4,3), -- Minimum feature importance

    -- Access control
    owner_team VARCHAR(50) NOT NULL,
    allowed_teams TEXT[] DEFAULT '{}',

    -- Status
    is_production BOOLEAN DEFAULT FALSE,
    version INTEGER NOT NULL DEFAULT 1,

    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Mapping between feature sets and features
CREATE TABLE feature_registry.feature_set_features (
    feature_set_id BIGINT REFERENCES feature_registry.feature_sets(feature_set_id),
    feature_id BIGINT REFERENCES feature_registry.features(feature_id),

    -- Feature role in the set
    feature_role VARCHAR(30) CHECK (feature_role IN ('predictor', 'target', 'identifier', 'metadata')),
    importance_score DECIMAL(4,3), -- Feature importance (0-1)
    is_required BOOLEAN DEFAULT TRUE,

    -- Transformation for this use case
    transformation_logic TEXT, -- Any preprocessing specific to this feature set

    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    added_by VARCHAR(100),

    PRIMARY KEY (feature_set_id, feature_id)
);

-- =============================================================================
-- OFFLINE FEATURE STORE - BATCH SERVING
-- =============================================================================

-- Customer features (demographic, behavioral, transactional)
CREATE TABLE offline_features.customer_features (
    customer_id VARCHAR(50) NOT NULL,
    feature_timestamp TIMESTAMP NOT NULL,

    -- Demographic features
    age_group VARCHAR(20),
    gender VARCHAR(10),
    region_code VARCHAR(10),
    urban_tier VARCHAR(20),
    income_bracket VARCHAR(30),

    -- RFM features
    rfm_recency_days INTEGER,
    rfm_frequency_score INTEGER,
    rfm_monetary_score INTEGER,
    rfm_segment VARCHAR(30),

    -- Transactional features (last 30 days)
    orders_count_30d INTEGER DEFAULT 0,
    orders_amount_30d DECIMAL(18,0) DEFAULT 0,
    avg_order_value_30d DECIMAL(15,0) DEFAULT 0,
    unique_categories_30d INTEGER DEFAULT 0,
    unique_brands_30d INTEGER DEFAULT 0,

    -- Transactional features (last 90 days)
    orders_count_90d INTEGER DEFAULT 0,
    orders_amount_90d DECIMAL(18,0) DEFAULT 0,
    avg_order_value_90d DECIMAL(15,0) DEFAULT 0,

    -- Transactional features (last 365 days)
    orders_count_365d INTEGER DEFAULT 0,
    orders_amount_365d DECIMAL(18,0) DEFAULT 0,
    lifetime_value DECIMAL(18,0) DEFAULT 0,

    -- Behavioral features
    preferred_platform VARCHAR(50),
    preferred_payment_method VARCHAR(30),
    preferred_category VARCHAR(100),
    is_mobile_user BOOLEAN DEFAULT FALSE,
    avg_session_duration_minutes INTEGER,

    -- Temporal features
    avg_days_between_orders DECIMAL(8,2),
    last_order_days_ago INTEGER,
    first_order_days_ago INTEGER,
    seasonal_buyer_pattern VARCHAR(30), -- Spring, Summer, Fall, Winter, Holiday

    -- Engagement features
    email_open_rate_30d DECIMAL(5,4),
    click_through_rate_30d DECIMAL(5,4),
    cart_abandonment_rate_30d DECIMAL(5,4),
    support_tickets_30d INTEGER DEFAULT 0,

    -- Predictive features
    churn_probability DECIMAL(4,3),
    clv_prediction DECIMAL(18,0),
    next_purchase_probability_7d DECIMAL(4,3),
    price_sensitivity_score DECIMAL(3,2),

    -- Vietnam-specific features
    is_cod_user BOOLEAN DEFAULT FALSE,
    prefers_promotion BOOLEAN DEFAULT FALSE,
    festival_shopper BOOLEAN DEFAULT FALSE, -- Shops during Tet, etc.

    -- Feature versioning
    feature_version INTEGER NOT NULL DEFAULT 1,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (customer_id, feature_timestamp)
) PARTITION BY RANGE (feature_timestamp);

-- Product features (characteristics, performance, market position)
CREATE TABLE offline_features.product_features (
    product_id VARCHAR(50) NOT NULL,
    feature_timestamp TIMESTAMP NOT NULL,

    -- Basic product attributes
    category_l1 VARCHAR(100),
    category_l2 VARCHAR(100),
    category_l3 VARCHAR(100),
    brand_name VARCHAR(100),
    price_tier VARCHAR(20),
    is_premium_brand BOOLEAN DEFAULT FALSE,

    -- Pricing features
    current_price_vnd DECIMAL(15,0),
    price_vs_category_avg DECIMAL(8,4), -- Ratio to category average
    price_vs_brand_avg DECIMAL(8,4),
    discount_frequency_30d DECIMAL(5,4), -- How often discounted
    avg_discount_percentage_30d DECIMAL(5,2),

    -- Sales performance features (last 30 days)
    units_sold_30d INTEGER DEFAULT 0,
    revenue_30d DECIMAL(18,0) DEFAULT 0,
    unique_customers_30d INTEGER DEFAULT 0,
    repeat_purchase_rate_30d DECIMAL(5,4),

    -- Sales performance features (last 90 days)
    units_sold_90d INTEGER DEFAULT 0,
    revenue_90d DECIMAL(18,0) DEFAULT 0,
    market_share_category_90d DECIMAL(5,4),

    -- Inventory and availability
    stock_level INTEGER DEFAULT 0,
    days_out_of_stock_30d INTEGER DEFAULT 0,
    avg_fulfillment_time_hours DECIMAL(6,2),

    -- Customer feedback features
    avg_rating DECIMAL(3,2),
    review_count_30d INTEGER DEFAULT 0,
    sentiment_score DECIMAL(4,3), -- -1 to 1
    return_rate_30d DECIMAL(5,4),

    -- Ranking and positioning
    category_rank INTEGER,
    brand_rank INTEGER,
    bestseller_rank INTEGER,
    trending_score DECIMAL(8,2),

    -- Cross-sell and upsell features
    cross_sell_affinity_score DECIMAL(3,2),
    upsell_potential_score DECIMAL(3,2),
    frequently_bought_together_count INTEGER DEFAULT 0,

    -- Seasonality features
    seasonal_demand_pattern VARCHAR(50),
    holiday_performance_multiplier DECIMAL(4,2),
    weather_sensitivity_score DECIMAL(3,2),

    -- Lifecycle features
    days_since_launch INTEGER,
    lifecycle_stage VARCHAR(30), -- Introduction, Growth, Maturity, Decline
    predicted_demand_next_30d INTEGER,

    -- Vietnam-specific features
    vietnam_preference_score DECIMAL(3,2), -- How well it performs in VN
    supports_cod BOOLEAN DEFAULT TRUE,
    regional_popularity JSONB, -- Performance by region

    -- Feature versioning
    feature_version INTEGER NOT NULL DEFAULT 1,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (product_id, feature_timestamp)
) PARTITION BY RANGE (feature_timestamp);

-- Order context features (session, timing, environment)
CREATE TABLE offline_features.order_context_features (
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    order_timestamp TIMESTAMP NOT NULL,

    -- Temporal features
    hour_of_day INTEGER CHECK (hour_of_day BETWEEN 0 AND 23),
    day_of_week INTEGER CHECK (day_of_week BETWEEN 1 AND 7),
    day_of_month INTEGER CHECK (day_of_month BETWEEN 1 AND 31),
    month_of_year INTEGER CHECK (month_of_year BETWEEN 1 AND 12),
    is_weekend BOOLEAN DEFAULT FALSE,
    is_holiday BOOLEAN DEFAULT FALSE,
    is_payday_period BOOLEAN DEFAULT FALSE, -- Around salary payment dates

    -- Session context
    session_duration_minutes INTEGER,
    pages_viewed INTEGER DEFAULT 0,
    products_viewed INTEGER DEFAULT 0,
    cart_additions INTEGER DEFAULT 0,
    cart_removals INTEGER DEFAULT 0,
    search_queries INTEGER DEFAULT 0,

    -- Device and channel
    device_type VARCHAR(20), -- Mobile, Desktop, Tablet
    browser_type VARCHAR(50),
    platform VARCHAR(50), -- App, Website, Marketplace
    traffic_source VARCHAR(50), -- Organic, Paid, Direct, Social, Email

    -- Geographic context
    user_province_code VARCHAR(10),
    user_region_code VARCHAR(10),
    shipping_province_code VARCHAR(10),
    is_same_province BOOLEAN DEFAULT TRUE,

    -- Marketing context
    active_promotions_count INTEGER DEFAULT 0,
    campaign_exposure_count INTEGER DEFAULT 0,
    email_campaign_recent BOOLEAN DEFAULT FALSE,
    push_notification_recent BOOLEAN DEFAULT FALSE,

    -- User state features
    days_since_last_order INTEGER,
    orders_in_last_30d INTEGER DEFAULT 0,
    cart_value_before_order DECIMAL(15,0),
    wishlist_items_count INTEGER DEFAULT 0,

    -- Economic context
    exchange_rate_usd_vnd DECIMAL(8,2),
    inflation_index DECIMAL(6,4),
    consumer_confidence_index DECIMAL(6,2),

    -- Weather context (for relevant products)
    temperature_celsius DECIMAL(4,1),
    humidity_percentage DECIMAL(4,1),
    weather_condition VARCHAR(30), -- Sunny, Rainy, Cloudy
    air_quality_index INTEGER,

    -- Feature versioning
    feature_version INTEGER NOT NULL DEFAULT 1,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (order_id)
);

-- =============================================================================
-- ONLINE FEATURE STORE - REAL-TIME SERVING
-- =============================================================================

-- Real-time customer features (Redis-like key-value structure in PostgreSQL)
CREATE TABLE online_features.customer_features_online (
    customer_id VARCHAR(50) PRIMARY KEY,

    -- Core features for real-time prediction
    features JSONB NOT NULL,

    -- Specific commonly-used features (for fast access)
    rfm_segment VARCHAR(30),
    customer_tier VARCHAR(20),
    churn_probability DECIMAL(4,3),
    clv_prediction DECIMAL(18,0),
    preferred_category VARCHAR(100),
    preferred_payment_method VARCHAR(30),

    -- Freshness tracking
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ttl_expires_at TIMESTAMP, -- Time to live

    -- Source tracking
    source_batch_job_id VARCHAR(100),
    feature_version INTEGER DEFAULT 1
);

-- Real-time product features
CREATE TABLE online_features.product_features_online (
    product_id VARCHAR(50) PRIMARY KEY,

    features JSONB NOT NULL,

    -- Commonly-used features
    category_l1 VARCHAR(100),
    price_tier VARCHAR(20),
    trending_score DECIMAL(8,2),
    stock_level INTEGER,
    avg_rating DECIMAL(3,2),

    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ttl_expires_at TIMESTAMP,
    source_batch_job_id VARCHAR(100),
    feature_version INTEGER DEFAULT 1
);

-- Real-time session features
CREATE TABLE online_features.session_features_online (
    session_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),

    features JSONB NOT NULL,

    -- Session state
    pages_viewed INTEGER DEFAULT 0,
    products_viewed INTEGER DEFAULT 0,
    cart_value DECIMAL(15,0) DEFAULT 0,
    session_duration_minutes INTEGER DEFAULT 0,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ttl_expires_at TIMESTAMP
);

-- =============================================================================
-- FEATURE MONITORING AND OBSERVABILITY
-- =============================================================================

-- Feature quality metrics
CREATE TABLE feature_monitoring.feature_quality_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    feature_id BIGINT REFERENCES feature_registry.features(feature_id),

    -- Time scope
    metric_date DATE NOT NULL,
    metric_hour INTEGER CHECK (metric_hour BETWEEN 0 AND 23),

    -- Data quality metrics
    total_records BIGINT NOT NULL,
    null_count BIGINT DEFAULT 0,
    null_rate DECIMAL(5,4),
    distinct_count BIGINT,
    distinct_rate DECIMAL(5,4),

    -- Statistical metrics
    min_value DECIMAL(20,4),
    max_value DECIMAL(20,4),
    mean_value DECIMAL(20,4),
    median_value DECIMAL(20,4),
    std_deviation DECIMAL(20,4),

    -- Distribution metrics
    percentile_25 DECIMAL(20,4),
    percentile_75 DECIMAL(20,4),
    percentile_95 DECIMAL(20,4),
    percentile_99 DECIMAL(20,4),

    -- Data drift detection
    drift_score DECIMAL(5,4), -- Statistical drift from baseline
    distribution_shift_score DECIMAL(5,4),
    is_anomalous BOOLEAN DEFAULT FALSE,

    -- Freshness metrics
    latest_timestamp TIMESTAMP,
    staleness_hours DECIMAL(8,2),

    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Feature usage tracking
CREATE TABLE feature_monitoring.feature_usage_log (
    usage_id BIGSERIAL PRIMARY KEY,

    -- Feature identification
    feature_id BIGINT REFERENCES feature_registry.features(feature_id),
    feature_set_id BIGINT REFERENCES feature_registry.feature_sets(feature_set_id),

    -- Usage context
    usage_type VARCHAR(30) CHECK (usage_type IN ('training', 'inference', 'exploration', 'validation')),
    model_name VARCHAR(100),
    model_version VARCHAR(50),

    -- Request details
    request_id VARCHAR(100),
    user_id VARCHAR(100),
    service_name VARCHAR(100),

    -- Performance metrics
    latency_ms INTEGER,
    cache_hit BOOLEAN DEFAULT FALSE,
    error_occurred BOOLEAN DEFAULT FALSE,
    error_message TEXT,

    -- Volume metrics
    records_served BIGINT DEFAULT 1,

    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (timestamp);

-- Model performance tracking (features impact on model)
CREATE TABLE feature_monitoring.model_performance_metrics (
    metric_id BIGSERIAL PRIMARY KEY,

    -- Model identification
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    feature_set_id BIGINT REFERENCES feature_registry.feature_sets(feature_set_id),

    -- Time period
    metric_date DATE NOT NULL,

    -- Performance metrics
    accuracy DECIMAL(6,4),
    precision_score DECIMAL(6,4),
    recall_score DECIMAL(6,4),
    f1_score DECIMAL(6,4),
    auc_score DECIMAL(6,4),

    -- Feature-specific metrics
    feature_importance JSONB, -- Feature importance scores
    feature_drift_impact JSONB, -- How drift affects performance

    -- Business metrics
    prediction_volume BIGINT,
    prediction_accuracy_rate DECIMAL(6,4),
    business_impact_score DECIMAL(8,2),

    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- FEATURE ENGINEERING PIPELINES
-- =============================================================================

-- Pipeline definitions
CREATE TABLE feature_store.feature_pipelines (
    pipeline_id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,

    -- Pipeline configuration
    pipeline_type VARCHAR(30) CHECK (pipeline_type IN ('batch', 'streaming', 'hybrid')),
    source_type VARCHAR(30) CHECK (source_type IN ('database', 'kafka', 'file', 'api')),
    target_feature_group_id BIGINT REFERENCES feature_registry.feature_groups(feature_group_id),

    -- Execution configuration
    schedule_cron VARCHAR(100), -- Cron expression for batch pipelines
    execution_timeout_minutes INTEGER DEFAULT 60,
    retry_count INTEGER DEFAULT 3,

    -- Code and logic
    transformation_code TEXT NOT NULL, -- SQL, Python, or other transformation logic
    dependencies TEXT[], -- Other pipelines this depends on

    -- Resource requirements
    cpu_cores INTEGER DEFAULT 2,
    memory_gb INTEGER DEFAULT 4,
    max_parallelism INTEGER DEFAULT 10,

    -- Status and versioning
    is_active BOOLEAN DEFAULT TRUE,
    version INTEGER DEFAULT 1,

    -- Metadata
    owner_team VARCHAR(50) NOT NULL,
    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline execution history
CREATE TABLE feature_store.pipeline_execution_log (
    execution_id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT REFERENCES feature_store.feature_pipelines(pipeline_id),

    -- Execution details
    execution_start_time TIMESTAMP NOT NULL,
    execution_end_time TIMESTAMP,
    execution_duration_minutes INTEGER,
    execution_status VARCHAR(20) CHECK (execution_status IN ('Running', 'Success', 'Failed', 'Cancelled')),

    -- Data processing metrics
    input_records BIGINT,
    output_records BIGINT,
    records_processed BIGINT,
    records_skipped BIGINT,
    records_failed BIGINT,

    -- Resource usage
    cpu_usage_seconds INTEGER,
    memory_peak_gb DECIMAL(6,2),
    disk_io_gb DECIMAL(8,2),

    -- Error handling
    error_message TEXT,
    error_details JSONB,

    -- Trigger information
    triggered_by VARCHAR(50), -- Manual, Scheduled, Event
    trigger_context JSONB,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- FEATURE SERVING APIs AND FUNCTIONS
-- =============================================================================

-- Function to get online features for real-time prediction
CREATE OR REPLACE FUNCTION feature_store.get_online_customer_features(
    p_customer_id VARCHAR(50),
    p_feature_names TEXT[] DEFAULT NULL -- Specific features to retrieve
) RETURNS JSONB AS $$
DECLARE
    v_features JSONB;
    v_feature_name TEXT;
    v_result JSONB := '{}';
BEGIN
    -- Get features from online store
    SELECT features INTO v_features
    FROM online_features.customer_features_online
    WHERE customer_id = p_customer_id
    AND (ttl_expires_at IS NULL OR ttl_expires_at > CURRENT_TIMESTAMP);

    IF v_features IS NULL THEN
        RETURN '{}';
    END IF;

    -- If specific features requested, filter them
    IF p_feature_names IS NOT NULL THEN
        FOREACH v_feature_name IN ARRAY p_feature_names
        LOOP
            IF v_features ? v_feature_name THEN
                v_result := v_result || jsonb_build_object(v_feature_name, v_features->v_feature_name);
            END IF;
        END LOOP;
        RETURN v_result;
    ELSE
        RETURN v_features;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to batch update online features
CREATE OR REPLACE FUNCTION feature_store.update_online_customer_features(
    p_customer_id VARCHAR(50),
    p_features JSONB,
    p_ttl_hours INTEGER DEFAULT 24
) RETURNS BOOLEAN AS $$
BEGIN
    INSERT INTO online_features.customer_features_online (
        customer_id, features, ttl_expires_at,
        rfm_segment, customer_tier, churn_probability, clv_prediction
    ) VALUES (
        p_customer_id,
        p_features,
        CURRENT_TIMESTAMP + (p_ttl_hours || ' hours')::INTERVAL,
        p_features->>'rfm_segment',
        p_features->>'customer_tier',
        (p_features->>'churn_probability')::DECIMAL(4,3),
        (p_features->>'clv_prediction')::DECIMAL(18,0)
    )
    ON CONFLICT (customer_id)
    DO UPDATE SET
        features = EXCLUDED.features,
        ttl_expires_at = EXCLUDED.ttl_expires_at,
        rfm_segment = EXCLUDED.rfm_segment,
        customer_tier = EXCLUDED.customer_tier,
        churn_probability = EXCLUDED.churn_probability,
        clv_prediction = EXCLUDED.clv_prediction,
        last_updated = CURRENT_TIMESTAMP;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- PARTITIONING FOR PERFORMANCE
-- =============================================================================

-- Create monthly partitions for customer features
CREATE TABLE offline_features.customer_features_202410
    PARTITION OF offline_features.customer_features
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE offline_features.customer_features_202411
    PARTITION OF offline_features.customer_features
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

-- Create monthly partitions for product features
CREATE TABLE offline_features.product_features_202410
    PARTITION OF offline_features.product_features
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE offline_features.product_features_202411
    PARTITION OF offline_features.product_features
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

-- Create daily partitions for usage log
CREATE TABLE feature_monitoring.feature_usage_log_20241010
    PARTITION OF feature_monitoring.feature_usage_log
    FOR VALUES FROM ('2024-10-10') TO ('2024-10-11');

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Feature registry indexes
CREATE INDEX idx_features_group_name ON feature_registry.features(feature_group_id, feature_name);
CREATE INDEX idx_features_type_status ON feature_registry.features(feature_type, status);
CREATE INDEX idx_features_online ON feature_registry.features(is_online_feature) WHERE is_online_feature = TRUE;

-- Offline feature indexes
CREATE INDEX idx_customer_features_id_timestamp ON offline_features.customer_features(customer_id, feature_timestamp);
CREATE INDEX idx_product_features_id_timestamp ON offline_features.product_features(product_id, feature_timestamp);

-- Online feature indexes
CREATE INDEX idx_customer_online_ttl ON online_features.customer_features_online(ttl_expires_at);
CREATE INDEX idx_product_online_ttl ON online_features.product_features_online(ttl_expires_at);
CREATE INDEX idx_session_online_ttl ON online_features.session_features_online(ttl_expires_at);

-- Monitoring indexes
CREATE INDEX idx_quality_metrics_feature_date ON feature_monitoring.feature_quality_metrics(feature_id, metric_date);
CREATE INDEX idx_usage_log_feature_timestamp ON feature_monitoring.feature_usage_log(feature_id, timestamp);

-- =============================================================================
-- VIEWS FOR FEATURE DISCOVERY AND MONITORING
-- =============================================================================

-- Feature catalog view
CREATE VIEW feature_registry.vw_feature_catalog AS
SELECT
    f.feature_id,
    fg.feature_group_name,
    f.feature_name,
    f.feature_full_name,
    f.description,
    f.feature_type,
    f.data_type,
    fg.business_domain,
    f.is_online_feature,
    f.is_offline_feature,
    f.status,
    f.version,

    -- Usage statistics (last 30 days)
    COALESCE(usage_stats.usage_count, 0) as usage_count_30d,
    COALESCE(usage_stats.unique_models, 0) as unique_models_using,

    -- Quality metrics (latest)
    latest_quality.null_rate,
    latest_quality.drift_score,
    latest_quality.is_anomalous,

    f.created_by,
    f.created_at

FROM feature_registry.features f
JOIN feature_registry.feature_groups fg ON f.feature_group_id = fg.feature_group_id
LEFT JOIN (
    SELECT
        feature_id,
        COUNT(*) as usage_count,
        COUNT(DISTINCT model_name) as unique_models
    FROM feature_monitoring.feature_usage_log
    WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY feature_id
) usage_stats ON f.feature_id = usage_stats.feature_id
LEFT JOIN (
    SELECT DISTINCT ON (feature_id)
        feature_id,
        null_rate,
        drift_score,
        is_anomalous
    FROM feature_monitoring.feature_quality_metrics
    ORDER BY feature_id, metric_date DESC, metric_hour DESC
) latest_quality ON f.feature_id = latest_quality.feature_id
WHERE f.status = 'Active'
ORDER BY usage_stats.usage_count DESC NULLS LAST;

-- =============================================================================
-- SAMPLE FEATURE DEFINITIONS
-- =============================================================================

-- Insert sample feature groups
INSERT INTO feature_registry.feature_groups (
    feature_group_name, description, owner_team, business_domain,
    primary_key_columns, expected_freshness_minutes, created_by
) VALUES
('customer_demographic', 'Customer demographic and profile features', 'Data_Science', 'Customer',
 '{"customer_id"}', 1440, 'system'),
('customer_behavioral', 'Customer behavioral and transactional features', 'Data_Science', 'Customer',
 '{"customer_id"}', 60, 'system'),
('product_performance', 'Product performance and ranking features', 'Product_Analytics', 'Product',
 '{"product_id"}', 60, 'system'),
('order_context', 'Order and session context features', 'Data_Science', 'Order',
 '{"order_id"}', 5, 'system');

-- Insert sample features
INSERT INTO feature_registry.features (
    feature_group_id, feature_name, description, feature_type, data_type,
    is_online_feature, is_offline_feature, created_by
) VALUES
-- Customer demographic features
(1, 'age_group', 'Customer age group classification', 'categorical', 'VARCHAR(20)', TRUE, TRUE, 'system'),
(1, 'region_code', 'Customer region in Vietnam', 'categorical', 'VARCHAR(10)', TRUE, TRUE, 'system'),
(1, 'income_bracket', 'Estimated income bracket', 'categorical', 'VARCHAR(30)', FALSE, TRUE, 'system'),

-- Customer behavioral features
(2, 'orders_count_30d', 'Number of orders in last 30 days', 'numerical', 'INTEGER', TRUE, TRUE, 'system'),
(2, 'avg_order_value_30d', 'Average order value in last 30 days', 'numerical', 'DECIMAL(15,0)', TRUE, TRUE, 'system'),
(2, 'churn_probability', 'Probability of customer churning', 'numerical', 'DECIMAL(4,3)', TRUE, TRUE, 'system'),

-- Product performance features
(3, 'trending_score', 'Product trending score', 'numerical', 'DECIMAL(8,2)', TRUE, TRUE, 'system'),
(3, 'category_rank', 'Rank within product category', 'numerical', 'INTEGER', TRUE, TRUE, 'system'),
(3, 'stock_level', 'Current stock level', 'numerical', 'INTEGER', TRUE, FALSE, 'system');

-- =============================================================================
-- CLEANUP AND MAINTENANCE JOBS
-- =============================================================================

-- Function to clean up expired online features
CREATE OR REPLACE FUNCTION feature_store.cleanup_expired_online_features()
RETURNS INTEGER AS $$
DECLARE
    v_deleted_count INTEGER := 0;
BEGIN
    -- Clean up expired customer features
    DELETE FROM online_features.customer_features_online
    WHERE ttl_expires_at < CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

    -- Clean up expired product features
    DELETE FROM online_features.product_features_online
    WHERE ttl_expires_at < CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_deleted_count = v_deleted_count + ROW_COUNT;

    -- Clean up expired session features
    DELETE FROM online_features.session_features_online
    WHERE ttl_expires_at < CURRENT_TIMESTAMP;

    GET DIAGNOSTICS v_deleted_count = v_deleted_count + ROW_COUNT;

    RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================

COMMENT ON SCHEMA feature_store IS 'Enterprise feature store for ML model serving and training';
COMMENT ON SCHEMA feature_registry IS 'Feature metadata, definitions, and governance';
COMMENT ON SCHEMA online_features IS 'Real-time feature serving for online predictions';
COMMENT ON SCHEMA offline_features IS 'Historical features for model training and batch scoring';
COMMENT ON SCHEMA feature_monitoring IS 'Feature quality monitoring and observability';

COMMENT ON TABLE feature_registry.features IS 'Complete catalog of all features with metadata and governance';
COMMENT ON TABLE online_features.customer_features_online IS 'Real-time customer features for online ML inference';
COMMENT ON TABLE offline_features.customer_features IS 'Historical customer features for training and batch processing';

-- =============================================================================
-- END OF FEATURE STORE
-- =============================================================================