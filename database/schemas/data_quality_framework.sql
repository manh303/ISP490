-- =============================================================================
-- DATA QUALITY FRAMEWORK - GREAT EXPECTATIONS STYLE
-- =============================================================================
-- Comprehensive data quality validation framework inspired by Great Expectations
-- Includes: Completeness, Accuracy, Validity, Consistency, Timeliness checks
-- Supports: Rule definitions, validation execution, monitoring, alerting
-- =============================================================================

-- Create schemas for data quality framework
CREATE SCHEMA IF NOT EXISTS dq_framework;
CREATE SCHEMA IF NOT EXISTS dq_rules;
CREATE SCHEMA IF NOT EXISTS dq_monitoring;

-- =============================================================================
-- DATA QUALITY RULE DEFINITIONS
-- =============================================================================

-- Master table for data quality expectations
CREATE TABLE dq_framework.data_quality_expectations (
    expectation_id BIGSERIAL PRIMARY KEY,
    expectation_name VARCHAR(200) NOT NULL,
    expectation_type VARCHAR(50) NOT NULL CHECK (expectation_type IN (
        'completeness', 'accuracy', 'validity', 'consistency',
        'timeliness', 'uniqueness', 'referential_integrity', 'custom'
    )),

    -- Target information
    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    target_column VARCHAR(100),

    -- Expectation definition
    expectation_description TEXT NOT NULL,
    expectation_logic TEXT NOT NULL, -- SQL or Python code
    threshold_value DECIMAL(10,4),
    threshold_operator VARCHAR(10) CHECK (threshold_operator IN ('>', '>=', '<', '<=', '=', '!=')),

    -- Severity and actions
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('Critical', 'High', 'Medium', 'Low')),
    business_impact TEXT,

    -- Execution configuration
    execution_frequency VARCHAR(20) CHECK (execution_frequency IN ('Real_Time', 'Hourly', 'Daily', 'Weekly', 'On_Demand')),
    execution_schedule VARCHAR(100), -- Cron expression
    is_blocking BOOLEAN DEFAULT FALSE, -- Should block downstream processing if failed

    -- Status and metadata
    is_active BOOLEAN DEFAULT TRUE,
    created_by VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Tags for organization
    tags TEXT[] DEFAULT '{}',
    business_domain VARCHAR(50),
    data_owner VARCHAR(100)
);

-- Specific rule templates for common checks
CREATE TABLE dq_framework.completeness_rules (
    rule_id BIGSERIAL PRIMARY KEY,
    expectation_id BIGINT REFERENCES dq_framework.data_quality_expectations(expectation_id),

    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    target_column VARCHAR(100) NOT NULL,

    -- Completeness thresholds
    min_completeness_percentage DECIMAL(5,2) NOT NULL CHECK (min_completeness_percentage BETWEEN 0 AND 100),
    expected_completeness_percentage DECIMAL(5,2) DEFAULT 100,

    -- Special handling
    exclude_null_equivalent BOOLEAN DEFAULT TRUE, -- Exclude '', 'N/A', 'NULL' strings
    null_equivalent_values TEXT[] DEFAULT '{"", "N/A", "NULL", "null", "None"}',

    -- Time-based rules
    apply_to_recent_data_only BOOLEAN DEFAULT FALSE,
    recent_data_hours INTEGER DEFAULT 24,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dq_framework.validity_rules (
    rule_id BIGSERIAL PRIMARY KEY,
    expectation_id BIGINT REFERENCES dq_framework.data_quality_expectations(expectation_id),

    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    target_column VARCHAR(100) NOT NULL,

    -- Data type validation
    expected_data_type VARCHAR(50),
    allowed_values TEXT[], -- For enum-like fields
    value_pattern VARCHAR(500), -- Regex pattern

    -- Numeric range validation
    min_value DECIMAL(20,4),
    max_value DECIMAL(20,4),

    -- String validation
    min_length INTEGER,
    max_length INTEGER,

    -- Date validation
    min_date DATE,
    max_date DATE,
    future_date_allowed BOOLEAN DEFAULT TRUE,

    -- Custom validation
    custom_validation_sql TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dq_framework.consistency_rules (
    rule_id BIGSERIAL PRIMARY KEY,
    expectation_id BIGINT REFERENCES dq_framework.data_quality_expectations(expectation_id),

    -- Cross-table consistency
    primary_schema VARCHAR(50) NOT NULL,
    primary_table VARCHAR(100) NOT NULL,
    primary_column VARCHAR(100) NOT NULL,

    reference_schema VARCHAR(50),
    reference_table VARCHAR(100),
    reference_column VARCHAR(100),

    -- Consistency type
    consistency_type VARCHAR(30) CHECK (consistency_type IN (
        'referential_integrity', 'cross_table_sum', 'cross_table_count',
        'business_rule', 'temporal_consistency'
    )),

    -- Business rule definition
    business_rule_sql TEXT,
    tolerance_percentage DECIMAL(5,2) DEFAULT 0, -- Allowed variance

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- DATA QUALITY EXECUTION RESULTS
-- =============================================================================

-- Execution log for all data quality checks
CREATE TABLE dq_monitoring.quality_check_results (
    check_id BIGSERIAL PRIMARY KEY,
    expectation_id BIGINT REFERENCES dq_framework.data_quality_expectations(expectation_id),

    -- Execution metadata
    execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_duration_ms INTEGER,
    execution_triggered_by VARCHAR(50), -- Manual, Scheduled, Pipeline

    -- Target information
    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    target_column VARCHAR(100),

    -- Data scope
    total_records_evaluated BIGINT NOT NULL,
    records_in_scope BIGINT, -- For time-based or filtered checks
    sample_size BIGINT, -- If sampling was used

    -- Results
    check_status VARCHAR(20) NOT NULL CHECK (check_status IN ('Pass', 'Fail', 'Warning', 'Error')),
    actual_value DECIMAL(20,4),
    expected_value DECIMAL(20,4),
    threshold_value DECIMAL(20,4),

    -- Detailed results
    pass_count BIGINT DEFAULT 0,
    fail_count BIGINT DEFAULT 0,
    null_count BIGINT DEFAULT 0,

    -- Error handling
    error_message TEXT,
    error_details JSONB,

    -- Failure analysis
    failure_examples JSONB, -- Sample of failing records
    failure_patterns JSONB, -- Common patterns in failures

    -- Performance metrics
    data_drift_score DECIMAL(5,4), -- Statistical drift from baseline
    anomaly_score DECIMAL(5,4), -- Anomaly detection score

    -- Metadata
    execution_context JSONB, -- Additional context like job_id, pipeline_name
    data_version VARCHAR(50), -- Data version if applicable

    -- Partitioning key for performance
    check_date DATE GENERATED ALWAYS AS (DATE(execution_timestamp)) STORED
) PARTITION BY RANGE (check_date);

-- Create monthly partitions for performance
CREATE TABLE dq_monitoring.quality_check_results_202410
    PARTITION OF dq_monitoring.quality_check_results
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE dq_monitoring.quality_check_results_202411
    PARTITION OF dq_monitoring.quality_check_results
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

-- Data quality summary by table/day
CREATE TABLE dq_monitoring.daily_quality_summary (
    summary_id BIGSERIAL PRIMARY KEY,
    summary_date DATE NOT NULL,
    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(100) NOT NULL,

    -- Execution summary
    total_checks_executed INTEGER DEFAULT 0,
    checks_passed INTEGER DEFAULT 0,
    checks_failed INTEGER DEFAULT 0,
    checks_warned INTEGER DEFAULT 0,
    checks_errored INTEGER DEFAULT 0,

    -- Quality score (0-1 scale)
    overall_quality_score DECIMAL(5,4),
    completeness_score DECIMAL(5,4),
    accuracy_score DECIMAL(5,4),
    validity_score DECIMAL(5,4),
    consistency_score DECIMAL(5,4),

    -- Trend analysis
    quality_trend VARCHAR(20) CHECK (quality_trend IN ('Improving', 'Stable', 'Degrading')),
    quality_change_percentage DECIMAL(8,2),

    -- Record counts
    total_records BIGINT,
    valid_records BIGINT,
    invalid_records BIGINT,

    -- Issue summary
    critical_issues INTEGER DEFAULT 0,
    high_issues INTEGER DEFAULT 0,
    medium_issues INTEGER DEFAULT 0,
    low_issues INTEGER DEFAULT 0,

    -- Metadata
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(summary_date, target_schema, target_table)
);

-- =============================================================================
-- DATA QUALITY MONITORING AND ALERTING
-- =============================================================================

-- Quality alerts configuration
CREATE TABLE dq_monitoring.quality_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    alert_name VARCHAR(200) NOT NULL,

    -- Alert triggers
    trigger_condition VARCHAR(500) NOT NULL, -- SQL condition
    severity_threshold VARCHAR(20) CHECK (severity_threshold IN ('Critical', 'High', 'Medium', 'Low')),

    -- Scope
    target_schemas TEXT[], -- Empty array means all schemas
    target_tables TEXT[], -- Empty array means all tables
    expectation_types TEXT[], -- Empty array means all types

    -- Alert configuration
    is_active BOOLEAN DEFAULT TRUE,
    alert_frequency VARCHAR(20) CHECK (alert_frequency IN ('Immediate', 'Hourly', 'Daily', 'Weekly')),
    escalation_rules JSONB,

    -- Notification settings
    notification_channels TEXT[] DEFAULT '{"email"}', -- email, slack, teams, webhook
    notification_recipients TEXT[] NOT NULL,
    notification_template TEXT,

    -- Suppression rules
    suppression_conditions JSONB,
    max_alerts_per_hour INTEGER DEFAULT 10,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alert history
CREATE TABLE dq_monitoring.alert_history (
    alert_history_id BIGSERIAL PRIMARY KEY,
    alert_id BIGINT REFERENCES dq_monitoring.quality_alerts(alert_id),

    -- Trigger information
    triggered_by_check_id BIGINT REFERENCES dq_monitoring.quality_check_results(check_id),
    trigger_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    trigger_condition_met TEXT,

    -- Alert details
    alert_severity VARCHAR(20),
    alert_message TEXT,
    affected_tables TEXT[],
    failure_count INTEGER,

    -- Resolution
    alert_status VARCHAR(20) DEFAULT 'Open' CHECK (alert_status IN ('Open', 'Acknowledged', 'Resolved', 'Suppressed')),
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP,
    resolution_notes TEXT,

    -- Notification status
    notification_sent BOOLEAN DEFAULT FALSE,
    notification_timestamp TIMESTAMP,
    notification_recipients TEXT[],

    -- Escalation
    escalation_level INTEGER DEFAULT 0,
    escalated_at TIMESTAMP,
    escalated_to TEXT[]
);

-- =============================================================================
-- DATA PROFILING AND BASELINE ESTABLISHMENT
-- =============================================================================

-- Statistical profiles for baseline comparison
CREATE TABLE dq_monitoring.data_profiles (
    profile_id BIGSERIAL PRIMARY KEY,
    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    target_column VARCHAR(100) NOT NULL,

    -- Profile metadata
    profile_date DATE NOT NULL,
    profile_type VARCHAR(30) CHECK (profile_type IN ('Baseline', 'Regular', 'Comparison')),
    total_records BIGINT NOT NULL,

    -- Basic statistics
    null_count BIGINT,
    null_percentage DECIMAL(5,2),
    distinct_count BIGINT,
    distinct_percentage DECIMAL(5,2),

    -- Numeric statistics (if applicable)
    min_value DECIMAL(20,4),
    max_value DECIMAL(20,4),
    mean_value DECIMAL(20,4),
    median_value DECIMAL(20,4),
    std_deviation DECIMAL(20,4),

    -- String statistics (if applicable)
    min_length INTEGER,
    max_length INTEGER,
    avg_length DECIMAL(8,2),

    -- Date statistics (if applicable)
    min_date DATE,
    max_date DATE,
    future_date_count BIGINT,

    -- Top values and patterns
    top_values JSONB, -- Top 10 most frequent values
    value_patterns JSONB, -- Common regex patterns
    anomalous_values JSONB, -- Outliers and anomalies

    -- Data type analysis
    data_type_consistency DECIMAL(5,2), -- % matching expected type
    format_consistency DECIMAL(5,2), -- % matching expected format

    -- Comparative metrics (vs baseline)
    drift_score DECIMAL(5,4), -- Statistical drift
    change_significance DECIMAL(5,4), -- Statistical significance of change

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- PREDEFINED QUALITY RULES FOR VIETNAM E-COMMERCE
-- =============================================================================

-- Insert standard completeness rules
INSERT INTO dq_framework.data_quality_expectations (
    expectation_name, expectation_type, target_schema, target_table, target_column,
    expectation_description, expectation_logic, threshold_value, threshold_operator,
    severity, execution_frequency, business_domain, created_by
) VALUES
-- Customer data completeness
('Customer Email Completeness', 'completeness', 'silver_layer', 'customers_clean', 'email',
 'Customer email should be present for marketing and communication',
 'SELECT (COUNT(*) - COUNT(email))::DECIMAL / COUNT(*) * 100 FROM silver_layer.customers_clean WHERE is_current = TRUE',
 5.0, '<=', 'High', 'Daily', 'Customer', 'DQ_Admin'),

('Customer Phone Completeness', 'completeness', 'silver_layer', 'customers_clean', 'phone',
 'Customer phone should be present for delivery and support',
 'SELECT (COUNT(*) - COUNT(phone))::DECIMAL / COUNT(*) * 100 FROM silver_layer.customers_clean WHERE is_current = TRUE',
 10.0, '<=', 'Medium', 'Daily', 'Customer', 'DQ_Admin'),

-- Order data completeness
('Order Total Amount Completeness', 'completeness', 'silver_layer', 'orders_clean', 'total_amount_vnd',
 'Order total amount must be present for revenue calculation',
 'SELECT (COUNT(*) - COUNT(total_amount_vnd))::DECIMAL / COUNT(*) * 100 FROM silver_layer.orders_clean',
 0.1, '<=', 'Critical', 'Hourly', 'Order', 'DQ_Admin'),

-- Product data completeness
('Product Name Completeness', 'completeness', 'silver_layer', 'products_clean', 'product_name',
 'Product name is essential for catalog and search',
 'SELECT (COUNT(*) - COUNT(product_name))::DECIMAL / COUNT(*) * 100 FROM silver_layer.products_clean WHERE is_current = TRUE',
 1.0, '<=', 'High', 'Daily', 'Product', 'DQ_Admin');

-- Insert validity rules
INSERT INTO dq_framework.data_quality_expectations (
    expectation_name, expectation_type, target_schema, target_table, target_column,
    expectation_description, expectation_logic, threshold_value, threshold_operator,
    severity, execution_frequency, business_domain, created_by
) VALUES
-- Email format validation
('Valid Email Format', 'validity', 'silver_layer', 'customers_clean', 'email',
 'Customer email should follow valid email format',
 'SELECT COUNT(*) FROM silver_layer.customers_clean WHERE email IS NOT NULL AND email !~ ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$''',
 0, '=', 'High', 'Daily', 'Customer', 'DQ_Admin'),

-- Phone format validation (Vietnam)
('Valid Vietnam Phone Format', 'validity', 'silver_layer', 'customers_clean', 'phone',
 'Phone should follow Vietnam phone number format',
 'SELECT COUNT(*) FROM silver_layer.customers_clean WHERE phone IS NOT NULL AND phone !~ ''^(\+84|84|0)[0-9]{8,9}$''',
 0, '=', 'Medium', 'Daily', 'Customer', 'DQ_Admin'),

-- Order amount validation
('Positive Order Amount', 'validity', 'silver_layer', 'orders_clean', 'total_amount_vnd',
 'Order total amount must be positive',
 'SELECT COUNT(*) FROM silver_layer.orders_clean WHERE total_amount_vnd <= 0',
 0, '=', 'Critical', 'Hourly', 'Order', 'DQ_Admin'),

-- Date validation
('Valid Order Date', 'validity', 'silver_layer', 'orders_clean', 'order_date',
 'Order date should not be in the future',
 'SELECT COUNT(*) FROM silver_layer.orders_clean WHERE order_date > CURRENT_DATE',
 0, '=', 'High', 'Daily', 'Order', 'DQ_Admin');

-- Insert consistency rules
INSERT INTO dq_framework.data_quality_expectations (
    expectation_name, expectation_type, target_schema, target_table,
    expectation_description, expectation_logic, threshold_value, threshold_operator,
    severity, execution_frequency, business_domain, created_by
) VALUES
-- Order-Customer referential integrity
('Order Customer Reference Integrity', 'consistency', 'silver_layer', 'orders_clean',
 'All orders must reference valid customers',
 'SELECT COUNT(*) FROM silver_layer.orders_clean o LEFT JOIN silver_layer.customers_clean c ON o.customer_id = c.customer_id AND c.is_current = TRUE WHERE c.customer_id IS NULL',
 0, '=', 'Critical', 'Hourly', 'Order', 'DQ_Admin'),

-- Order items total consistency
('Order Items Total Consistency', 'consistency', 'silver_layer', 'orders_clean',
 'Order total should match sum of line items',
 'WITH order_totals AS (
    SELECT o.order_id, o.total_amount_vnd as order_total,
           COALESCE(SUM(oi.line_total_vnd), 0) as items_total
    FROM silver_layer.orders_clean o
    LEFT JOIN silver_layer.order_items_clean oi ON o.order_id = oi.order_id
    GROUP BY o.order_id, o.total_amount_vnd
 )
 SELECT COUNT(*) FROM order_totals WHERE ABS(order_total - items_total) > 1000', -- 1000 VND tolerance
 0, '=', 'High', 'Daily', 'Order', 'DQ_Admin');

-- =============================================================================
-- DATA QUALITY EXECUTION FUNCTIONS
-- =============================================================================

-- Function to execute a specific data quality check
CREATE OR REPLACE FUNCTION dq_framework.execute_quality_check(
    p_expectation_id BIGINT,
    p_execution_triggered_by VARCHAR(50) DEFAULT 'Manual'
) RETURNS BIGINT AS $$
DECLARE
    v_check_id BIGINT;
    v_expectation RECORD;
    v_result RECORD;
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration_ms INTEGER;
    v_actual_value DECIMAL(20,4);
    v_check_status VARCHAR(20);
    v_error_message TEXT;
BEGIN
    -- Get expectation details
    SELECT * INTO v_expectation
    FROM dq_framework.data_quality_expectations
    WHERE expectation_id = p_expectation_id AND is_active = TRUE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Expectation ID % not found or inactive', p_expectation_id;
    END IF;

    -- Execute the check logic
    BEGIN
        EXECUTE v_expectation.expectation_logic INTO v_actual_value;

        -- Determine check status
        IF v_expectation.threshold_operator = '>' AND v_actual_value > v_expectation.threshold_value THEN
            v_check_status := 'Pass';
        ELSIF v_expectation.threshold_operator = '>=' AND v_actual_value >= v_expectation.threshold_value THEN
            v_check_status := 'Pass';
        ELSIF v_expectation.threshold_operator = '<' AND v_actual_value < v_expectation.threshold_value THEN
            v_check_status := 'Pass';
        ELSIF v_expectation.threshold_operator = '<=' AND v_actual_value <= v_expectation.threshold_value THEN
            v_check_status := 'Pass';
        ELSIF v_expectation.threshold_operator = '=' AND v_actual_value = v_expectation.threshold_value THEN
            v_check_status := 'Pass';
        ELSIF v_expectation.threshold_operator = '!=' AND v_actual_value != v_expectation.threshold_value THEN
            v_check_status := 'Pass';
        ELSE
            v_check_status := 'Fail';
        END IF;

    EXCEPTION WHEN OTHERS THEN
        v_check_status := 'Error';
        v_error_message := SQLERRM;
        v_actual_value := NULL;
    END;

    v_end_time := CURRENT_TIMESTAMP;
    v_duration_ms := EXTRACT(EPOCH FROM (v_end_time - v_start_time)) * 1000;

    -- Insert check result
    INSERT INTO dq_monitoring.quality_check_results (
        expectation_id, execution_triggered_by, target_schema, target_table, target_column,
        check_status, actual_value, expected_value, threshold_value,
        execution_duration_ms, error_message
    ) VALUES (
        p_expectation_id, p_execution_triggered_by, v_expectation.target_schema,
        v_expectation.target_table, v_expectation.target_column,
        v_check_status, v_actual_value, v_expectation.threshold_value, v_expectation.threshold_value,
        v_duration_ms, v_error_message
    ) RETURNING check_id INTO v_check_id;

    RETURN v_check_id;
END;
$$ LANGUAGE plpgsql;

-- Function to execute all checks for a table
CREATE OR REPLACE FUNCTION dq_framework.execute_table_quality_checks(
    p_schema_name VARCHAR(50),
    p_table_name VARCHAR(100)
) RETURNS TABLE (
    check_id BIGINT,
    expectation_name VARCHAR(200),
    check_status VARCHAR(20),
    actual_value DECIMAL(20,4)
) AS $$
DECLARE
    v_expectation RECORD;
BEGIN
    FOR v_expectation IN
        SELECT expectation_id, expectation_name
        FROM dq_framework.data_quality_expectations
        WHERE target_schema = p_schema_name
        AND target_table = p_table_name
        AND is_active = TRUE
        ORDER BY severity DESC, expectation_name
    LOOP
        SELECT qcr.check_id, qcr.actual_value, qcr.check_status
        INTO check_id, actual_value, check_status
        FROM dq_framework.execute_quality_check(v_expectation.expectation_id, 'Batch_Check') eid
        JOIN dq_monitoring.quality_check_results qcr ON qcr.check_id = eid;

        expectation_name := v_expectation.expectation_name;

        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- VIEWS FOR MONITORING AND REPORTING
-- =============================================================================

-- Data quality dashboard view
CREATE VIEW dq_monitoring.vw_quality_dashboard AS
SELECT
    dqe.target_schema,
    dqe.target_table,
    dqe.expectation_type,
    dqe.severity,

    -- Recent execution summary (last 24 hours)
    COUNT(qcr.check_id) as total_checks_24h,
    COUNT(CASE WHEN qcr.check_status = 'Pass' THEN 1 END) as passed_checks_24h,
    COUNT(CASE WHEN qcr.check_status = 'Fail' THEN 1 END) as failed_checks_24h,
    COUNT(CASE WHEN qcr.check_status = 'Error' THEN 1 END) as error_checks_24h,

    -- Quality score calculation
    CASE
        WHEN COUNT(qcr.check_id) > 0 THEN
            ROUND(COUNT(CASE WHEN qcr.check_status = 'Pass' THEN 1 END)::DECIMAL / COUNT(qcr.check_id) * 100, 2)
        ELSE NULL
    END as quality_score_percentage,

    -- Latest check information
    MAX(qcr.execution_timestamp) as last_check_time,

    -- Critical issues
    COUNT(CASE WHEN qcr.check_status = 'Fail' AND dqe.severity = 'Critical' THEN 1 END) as critical_failures_24h

FROM dq_framework.data_quality_expectations dqe
LEFT JOIN dq_monitoring.quality_check_results qcr
    ON dqe.expectation_id = qcr.expectation_id
    AND qcr.execution_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
WHERE dqe.is_active = TRUE
GROUP BY dqe.target_schema, dqe.target_table, dqe.expectation_type, dqe.severity
ORDER BY critical_failures_24h DESC, quality_score_percentage ASC;

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

CREATE INDEX idx_dq_expectations_target ON dq_framework.data_quality_expectations(target_schema, target_table);
CREATE INDEX idx_dq_expectations_type_severity ON dq_framework.data_quality_expectations(expectation_type, severity);
CREATE INDEX idx_dq_expectations_active ON dq_framework.data_quality_expectations(is_active) WHERE is_active = TRUE;

CREATE INDEX idx_quality_results_expectation_time ON dq_monitoring.quality_check_results(expectation_id, execution_timestamp);
CREATE INDEX idx_quality_results_status_time ON dq_monitoring.quality_check_results(check_status, execution_timestamp);
CREATE INDEX idx_quality_results_target_time ON dq_monitoring.quality_check_results(target_schema, target_table, execution_timestamp);

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================

COMMENT ON SCHEMA dq_framework IS 'Data quality framework with Great Expectations-style validation rules';
COMMENT ON SCHEMA dq_monitoring IS 'Data quality monitoring, results, and alerting';

COMMENT ON TABLE dq_framework.data_quality_expectations IS 'Master table for all data quality validation rules and expectations';
COMMENT ON TABLE dq_monitoring.quality_check_results IS 'Execution results and history for all data quality checks';
COMMENT ON TABLE dq_monitoring.daily_quality_summary IS 'Daily aggregated quality metrics by table for trend analysis';

-- =============================================================================
-- END OF DATA QUALITY FRAMEWORK
-- =============================================================================