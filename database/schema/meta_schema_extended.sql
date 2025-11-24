-- ===================================================================
-- META SCHEMA EXTENSIONS - For Data Engineer API
-- ===================================================================
-- Run this AFTER meta_schema.sql to add additional tables

-- ===================================================================
-- 8. DATABASE CONNECTION HEALTH - Monitor database connections
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.db_connection_health (
    health_id       BIGSERIAL PRIMARY KEY,
    check_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    host            VARCHAR(255) NOT NULL,
    port            INT NOT NULL,
    database_name   VARCHAR(100) NOT NULL,
    status          VARCHAR(20) NOT NULL,  -- 'HEALTHY', 'DEGRADED', 'DOWN'
    active_connections INT DEFAULT 0,
    idle_connections   INT DEFAULT 0,
    max_connections    INT,
    connection_usage_pct DECIMAL(5,2),  -- % of max connections used
    avg_query_time_ms  DECIMAL(10,2),   -- Average query time
    slow_queries_count INT DEFAULT 0,   -- Queries > 1s
    response_time_ms   INT,             -- Connection test response time
    error_message   TEXT,
    metadata        JSONB
);

CREATE INDEX IF NOT EXISTS idx_db_health_check_time ON meta.db_connection_health(check_time);
CREATE INDEX IF NOT EXISTS idx_db_health_status ON meta.db_connection_health(status);
CREATE INDEX IF NOT EXISTS idx_db_health_host ON meta.db_connection_health(host, database_name);

-- ===================================================================
-- 9. SCHEMA VERSION - Track schema migrations
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.schema_version (
    version_id      SERIAL PRIMARY KEY,
    schema_name     VARCHAR(50) NOT NULL,
    version_number  VARCHAR(50) NOT NULL,  -- e.g., '1.0.0', '1.1.0'
    description     TEXT,
    migration_script TEXT,                 -- Script content or path
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by      VARCHAR(100),          -- User/system that applied
    rollback_script TEXT,                  -- Script to rollback if needed
    status          VARCHAR(20) DEFAULT 'APPLIED',  -- 'APPLIED', 'ROLLED_BACK', 'FAILED'
    UNIQUE(schema_name, version_number)
);

CREATE INDEX IF NOT EXISTS idx_schema_version_name ON meta.schema_version(schema_name);
CREATE INDEX IF NOT EXISTS idx_schema_version_applied_at ON meta.schema_version(applied_at);

-- ===================================================================
-- 10. DATA LINEAGE - Track data flow between tables
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.data_lineage (
    lineage_id      BIGSERIAL PRIMARY KEY,
    source_schema   VARCHAR(50) NOT NULL,
    source_table    VARCHAR(100) NOT NULL,
    target_schema   VARCHAR(50) NOT NULL,
    target_table    VARCHAR(100) NOT NULL,
    transformation_type VARCHAR(50),  -- 'DIRECT_COPY', 'AGGREGATION', 'JOIN', 'FILTER', 'TRANSFORM'
    transformation_logic TEXT,        -- SQL or description
    job_code        VARCHAR(100),     -- Reference to etl_job
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lineage_source ON meta.data_lineage(source_schema, source_table);
CREATE INDEX IF NOT EXISTS idx_lineage_target ON meta.data_lineage(target_schema, target_table);
CREATE INDEX IF NOT EXISTS idx_lineage_job ON meta.data_lineage(job_code);

-- ===================================================================
-- 11. PIPELINE DEPENDENCY - Track dependencies between pipelines
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.pipeline_dependency (
    dependency_id   SERIAL PRIMARY KEY,
    parent_job_code VARCHAR(100) NOT NULL REFERENCES meta.etl_job(job_code),
    child_job_code  VARCHAR(100) NOT NULL REFERENCES meta.etl_job(job_code),
    dependency_type VARCHAR(50) NOT NULL,  -- 'BLOCKING', 'SOFT', 'TRIGGER'
    schedule_offset_minutes INT,           -- Child runs X minutes after parent
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(parent_job_code, child_job_code)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_dep_parent ON meta.pipeline_dependency(parent_job_code);
CREATE INDEX IF NOT EXISTS idx_pipeline_dep_child ON meta.pipeline_dependency(child_job_code);

-- ===================================================================
-- 12. ALERT CONFIGURATION - Configure alerts for monitoring
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.alert_config (
    alert_id        SERIAL PRIMARY KEY,
    alert_name      VARCHAR(100) NOT NULL UNIQUE,
    alert_type      VARCHAR(50) NOT NULL,  -- 'ETL_FAILURE', 'DATA_QUALITY', 'FRESHNESS', 'VOLUME', 'PERFORMANCE'
    target_type     VARCHAR(50) NOT NULL,  -- 'JOB', 'TABLE', 'SCHEMA', 'SYSTEM'
    target_name     VARCHAR(200) NOT NULL, -- job_code or table_name or 'ALL'
    condition_sql   TEXT,                  -- SQL to check condition
    threshold_value DECIMAL,               -- Threshold for numeric checks
    severity        VARCHAR(20) NOT NULL,  -- 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'
    notification_channels JSONB,          -- e.g., ["email", "slack", "pagerduty"]
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_config_type ON meta.alert_config(alert_type);
CREATE INDEX IF NOT EXISTS idx_alert_config_target ON meta.alert_config(target_type, target_name);
CREATE INDEX IF NOT EXISTS idx_alert_config_active ON meta.alert_config(is_active);

-- ===================================================================
-- 13. ALERT HISTORY - Track alert triggers
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.alert_history (
    alert_history_id BIGSERIAL PRIMARY KEY,
    alert_id        INT REFERENCES meta.alert_config(alert_id),
    triggered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    severity        VARCHAR(20) NOT NULL,
    alert_message   TEXT NOT NULL,
    alert_details   JSONB,                -- Additional context
    status          VARCHAR(20) DEFAULT 'TRIGGERED',  -- 'TRIGGERED', 'ACKNOWLEDGED', 'RESOLVED'
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by VARCHAR(100),
    resolved_at     TIMESTAMPTZ,
    resolved_by     VARCHAR(100),
    resolution_notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_alert_history_alert_id ON meta.alert_history(alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_history_triggered_at ON meta.alert_history(triggered_at);
CREATE INDEX IF NOT EXISTS idx_alert_history_status ON meta.alert_history(status);

-- ===================================================================
-- 14. QUERY PERFORMANCE - Track slow queries
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.query_performance (
    query_id        BIGSERIAL PRIMARY KEY,
    query_hash      VARCHAR(64) NOT NULL,  -- MD5 hash of normalized query
    query_text      TEXT,                  -- Actual query (truncated if too long)
    schema_name     VARCHAR(50),
    tables_accessed VARCHAR(500),          -- Comma-separated list
    execution_time_ms DECIMAL(10,2) NOT NULL,
    rows_returned   BIGINT,
    rows_scanned    BIGINT,
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executed_by     VARCHAR(100),          -- User/app that ran query
    execution_plan  JSONB,                 -- EXPLAIN output
    is_slow         BOOLEAN DEFAULT FALSE  -- Mark if > threshold
);

CREATE INDEX IF NOT EXISTS idx_query_perf_hash ON meta.query_performance(query_hash);
CREATE INDEX IF NOT EXISTS idx_query_perf_time ON meta.query_performance(execution_time_ms DESC);
CREATE INDEX IF NOT EXISTS idx_query_perf_executed_at ON meta.query_performance(executed_at);
CREATE INDEX IF NOT EXISTS idx_query_perf_slow ON meta.query_performance(is_slow) WHERE is_slow = TRUE;

-- ===================================================================
-- 15. STORAGE USAGE - Track storage growth
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.storage_usage (
    storage_id      BIGSERIAL PRIMARY KEY,
    check_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    schema_name     VARCHAR(50) NOT NULL,
    table_name      VARCHAR(100) NOT NULL,
    table_size_bytes BIGINT NOT NULL,
    indexes_size_bytes BIGINT,
    total_size_bytes BIGINT,            -- table + indexes
    row_count       BIGINT,
    avg_row_size_bytes INT,
    growth_rate_mb_per_day DECIMAL(10,2),  -- Estimated growth
    last_vacuum     TIMESTAMPTZ,
    last_analyze    TIMESTAMPTZ,
    bloat_ratio     DECIMAL(5,2),       -- Table bloat %
    UNIQUE(schema_name, table_name, check_time)
);

CREATE INDEX IF NOT EXISTS idx_storage_schema_table ON meta.storage_usage(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_storage_check_time ON meta.storage_usage(check_time);
CREATE INDEX IF NOT EXISTS idx_storage_size ON meta.storage_usage(total_size_bytes DESC);

-- ===================================================================
-- COMMENTS
-- ===================================================================

COMMENT ON TABLE meta.db_connection_health IS 'Monitor database connection pool health';
COMMENT ON TABLE meta.schema_version IS 'Track schema migrations and versions';
COMMENT ON TABLE meta.data_lineage IS 'Track data flow between tables';
COMMENT ON TABLE meta.pipeline_dependency IS 'Define dependencies between ETL pipelines';
COMMENT ON TABLE meta.alert_config IS 'Configure monitoring alerts';
COMMENT ON TABLE meta.alert_history IS 'History of triggered alerts';
COMMENT ON TABLE meta.query_performance IS 'Track query performance and slow queries';
COMMENT ON TABLE meta.storage_usage IS 'Monitor storage usage and growth';

-- ===================================================================
-- SAMPLE CONFIGURATIONS
-- ===================================================================

-- Example: Alert for ETL failures
INSERT INTO meta.alert_config (
    alert_name, alert_type, target_type, target_name,
    severity, notification_channels, is_active
) VALUES (
    'DWH Pipeline Failure Alert',
    'ETL_FAILURE',
    'JOB',
    'MINIO_ECOMMERCE_DWH_PIPELINE',
    'CRITICAL',
    '["email", "slack"]'::jsonb,
    TRUE
) ON CONFLICT (alert_name) DO NOTHING;

-- Example: Alert for data freshness
INSERT INTO meta.alert_config (
    alert_name, alert_type, target_type, target_name,
    condition_sql, threshold_value, severity, notification_channels, is_active
) VALUES (
    'DWH Data Freshness Alert',
    'FRESHNESS',
    'TABLE',
    'dwh.fact_product_daily',
    'SELECT EXTRACT(EPOCH FROM (NOW() - MAX(last_loaded_at)))/3600 FROM meta.table_stats WHERE table_name = ''fact_product_daily''',
    24,  -- Alert if data older than 24 hours
    'HIGH',
    '["email"]'::jsonb,
    TRUE
) ON CONFLICT (alert_name) DO NOTHING;

-- Example: Data lineage
INSERT INTO meta.data_lineage (
    source_schema, source_table,
    target_schema, target_table,
    transformation_type, transformation_logic, job_code
) VALUES
    ('staging', 'raw_products', 'dwh', 'dim_product', 'TRANSFORM',
     'Clean, deduplicate, standardize product data', 'MINIO_ECOMMERCE_DWH_PIPELINE'),
    ('dwh', 'dim_product', 'dwh', 'fact_product_daily', 'AGGREGATION',
     'Aggregate product metrics by date', 'MINIO_ECOMMERCE_DWH_PIPELINE'),
    ('dwh', 'fact_review', 'ml', 'fact_review_sentiment', 'TRANSFORM',
     'Apply sentiment classification model', 'ML_TRAINING_PIPELINE')
ON CONFLICT DO NOTHING;

-- Example: Pipeline dependency
INSERT INTO meta.pipeline_dependency (
    parent_job_code, child_job_code, dependency_type, schedule_offset_minutes
) VALUES
    ('MINIO_ECOMMERCE_DWH_PIPELINE', 'ML_TRAINING_PIPELINE', 'BLOCKING', 60)
ON CONFLICT (parent_job_code, child_job_code) DO NOTHING;


