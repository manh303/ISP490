-- ===================================================================
-- META SCHEMA - ETL Monitoring & Data Quality
-- ===================================================================

-- ========= SCHEMA =========
-- Drop schema nếu tồn tại (để recreate với cấu trúc mới)
-- ⚠️ CẨN THẬN: Điều này sẽ xóa toàn bộ dữ liệu trong schema meta
DROP SCHEMA IF EXISTS meta CASCADE;

CREATE SCHEMA meta;

-- ===================================================================
-- 1. ETL JOB - Định nghĩa các ETL jobs
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.etl_job (
    job_id         SERIAL PRIMARY KEY,
    job_code       VARCHAR(100) NOT NULL UNIQUE,  -- 'MINIO_ECOMMERCE_DWH_PIPELINE'
    job_name       VARCHAR(255) NOT NULL,         -- 'Ecommerce DSS - Full DWH (Star Schema)'
    description    TEXT,
    is_active      BOOLEAN DEFAULT TRUE,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_job_code ON meta.etl_job(job_code);
CREATE INDEX IF NOT EXISTS idx_etl_job_active ON meta.etl_job(is_active);

-- ===================================================================
-- 2. ETL RUN - Lịch sử các lần chạy ETL
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.etl_run (
    run_id         SERIAL PRIMARY KEY,
    job_id         INT NOT NULL REFERENCES meta.etl_job(job_id),
    run_date       DATE NOT NULL,                 -- Ngày dữ liệu được xử lý (ds trong Airflow)
    started_at     TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ,
    status         VARCHAR(20) NOT NULL DEFAULT 'RUNNING',  -- RUNNING, SUCCESS, FAILED, CANCELLED
    rows_read      BIGINT,
    rows_written   BIGINT,
    error_message  TEXT,
    airflow_run_id VARCHAR(255),                  -- Airflow run_id để link với Airflow UI
    metadata       JSONB,                         -- Thông tin bổ sung (config, params...)
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_run_job_id ON meta.etl_run(job_id);
CREATE INDEX IF NOT EXISTS idx_etl_run_date ON meta.etl_run(run_date);
CREATE INDEX IF NOT EXISTS idx_etl_run_status ON meta.etl_run(status);
CREATE INDEX IF NOT EXISTS idx_etl_run_started_at ON meta.etl_run(started_at);
CREATE INDEX IF NOT EXISTS idx_etl_run_airflow_id ON meta.etl_run(airflow_run_id);

-- ===================================================================
-- 3. ETL LOG - Log chi tiết từng bước trong ETL run
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.etl_log (
    log_id         BIGSERIAL PRIMARY KEY,
    run_id         INT REFERENCES meta.etl_run(run_id),
    job_name       VARCHAR(100) NOT NULL,         -- Tên job/task (ví dụ: 'spark_build_star_dwh')
    stage          VARCHAR(50) NOT NULL,          -- Stage trong pipeline (ví dụ: 'LOAD_RAW', 'CLEAN_DATA', 'LOAD_DWH')
    log_level      VARCHAR(20) NOT NULL DEFAULT 'INFO',  -- DEBUG, INFO, WARN, ERROR
    log_message    TEXT NOT NULL,
    records_processed INT DEFAULT 0,
    records_failed    INT DEFAULT 0,
    error_message     TEXT,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_log_run_id ON meta.etl_log(run_id);
CREATE INDEX IF NOT EXISTS idx_etl_log_stage ON meta.etl_log(stage);
CREATE INDEX IF NOT EXISTS idx_etl_log_level ON meta.etl_log(log_level);
CREATE INDEX IF NOT EXISTS idx_etl_log_created_at ON meta.etl_log(created_at);

-- ===================================================================
-- 4. TABLE STATS - Thống kê volume và freshness theo bảng
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.table_stats (
    stat_id        BIGSERIAL PRIMARY KEY,
    schema_name    VARCHAR(50) NOT NULL,          -- 'staging', 'ods', 'dwh', 'ml'
    table_name     VARCHAR(100) NOT NULL,
    snapshot_date  DATE NOT NULL,                 -- Ngày snapshot
    row_count      BIGINT NOT NULL DEFAULT 0,
    size_bytes     BIGINT,                        -- Kích thước bảng (bytes)
    last_loaded_at TIMESTAMPTZ,                   -- Thời điểm load dữ liệu mới nhất
    metadata       JSONB,                         -- Thông tin bổ sung (column_count, index_count...)
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (schema_name, table_name, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_table_stats_schema_table ON meta.table_stats(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_table_stats_date ON meta.table_stats(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_table_stats_loaded_at ON meta.table_stats(last_loaded_at);

-- ===================================================================
-- 5. DATA QUALITY ISSUE - Các vấn đề về chất lượng dữ liệu
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.data_quality_issue (
    issue_id          SERIAL PRIMARY KEY,
    schema_name       VARCHAR(50) NOT NULL,       -- Schema chứa bảng có issue
    table_name        VARCHAR(100) NOT NULL,      -- Tên bảng
    issue_type        VARCHAR(50) NOT NULL,       -- 'NULL_VALUE', 'INVALID_DATA', 'DUPLICATE', 'FK_VIOLATION', 'OUTLIER'
    severity          VARCHAR(20) NOT NULL DEFAULT 'MEDIUM',  -- CRITICAL, HIGH, MEDIUM, LOW
    status            VARCHAR(20) NOT NULL DEFAULT 'OPEN',    -- OPEN, IN_PROGRESS, RESOLVED, IGNORED
    affected_rows     BIGINT DEFAULT 0,           -- Số dòng bị ảnh hưởng
    issue_description TEXT NOT NULL,              -- Mô tả chi tiết issue
    sample_rows       JSONB,                      -- Sample rows bị lỗi (để debug)
    detected_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at       TIMESTAMPTZ,
    resolved_by       VARCHAR(100),               -- User/script đã resolve
    resolution_notes  TEXT,                       -- Ghi chú về cách resolve
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_issue_schema_table ON meta.data_quality_issue(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_dq_issue_status ON meta.data_quality_issue(status);
CREATE INDEX IF NOT EXISTS idx_dq_issue_severity ON meta.data_quality_issue(severity);
CREATE INDEX IF NOT EXISTS idx_dq_issue_type ON meta.data_quality_issue(issue_type);
CREATE INDEX IF NOT EXISTS idx_dq_issue_detected_at ON meta.data_quality_issue(detected_at);

-- ===================================================================
-- 6. DATA QUALITY RULE - Định nghĩa các rules kiểm tra chất lượng
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.data_quality_rule (
    rule_id        SERIAL PRIMARY KEY,
    schema_name    VARCHAR(50) NOT NULL,
    table_name     VARCHAR(100) NOT NULL,
    column_name    VARCHAR(100),                  -- NULL nếu rule áp dụng cho toàn bảng
    rule_type      VARCHAR(50) NOT NULL,          -- 'NOT_NULL', 'UNIQUE', 'RANGE', 'FORMAT', 'FK_REFERENCE'
    rule_definition JSONB NOT NULL,               -- Định nghĩa rule (ví dụ: {"min": 0, "max": 100000000})
    is_active      BOOLEAN DEFAULT TRUE,
    severity       VARCHAR(20) DEFAULT 'ERROR',   -- ERROR, WARNING, INFO
    description    TEXT,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_rule_schema_table ON meta.data_quality_rule(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_dq_rule_active ON meta.data_quality_rule(is_active);

-- ===================================================================
-- 7. DATA QUALITY CHECK RESULT - Kết quả kiểm tra theo rule
-- ===================================================================
CREATE TABLE IF NOT EXISTS meta.data_quality_check_result (
    check_id       BIGSERIAL PRIMARY KEY,
    rule_id        INT REFERENCES meta.data_quality_rule(rule_id),
    run_id         INT REFERENCES meta.etl_run(run_id),
    schema_name    VARCHAR(50) NOT NULL,
    table_name     VARCHAR(100) NOT NULL,
    check_date     DATE NOT NULL,
    passed         BOOLEAN NOT NULL,
    failed_count   BIGINT DEFAULT 0,
    total_count    BIGINT DEFAULT 0,
    error_message  TEXT,
    sample_failed_rows JSONB,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_check_rule_id ON meta.data_quality_check_result(rule_id);
CREATE INDEX IF NOT EXISTS idx_dq_check_run_id ON meta.data_quality_check_result(run_id);
CREATE INDEX IF NOT EXISTS idx_dq_check_date ON meta.data_quality_check_result(check_date);
CREATE INDEX IF NOT EXISTS idx_dq_check_passed ON meta.data_quality_check_result(passed);

-- ===================================================================
-- COMMENTS
-- ===================================================================

COMMENT ON SCHEMA meta IS 'Schema chứa metadata và monitoring cho ETL pipeline';

COMMENT ON TABLE meta.etl_job IS 'Định nghĩa các ETL jobs trong hệ thống';
COMMENT ON TABLE meta.etl_run IS 'Lịch sử các lần chạy ETL, link với Airflow runs';
COMMENT ON TABLE meta.etl_log IS 'Log chi tiết từng bước trong ETL pipeline';
COMMENT ON TABLE meta.table_stats IS 'Thống kê volume và freshness của các bảng theo ngày';
COMMENT ON TABLE meta.data_quality_issue IS 'Các vấn đề về chất lượng dữ liệu được phát hiện';
COMMENT ON TABLE meta.data_quality_rule IS 'Định nghĩa các rules kiểm tra chất lượng dữ liệu';
COMMENT ON TABLE meta.data_quality_check_result IS 'Kết quả kiểm tra chất lượng theo từng rule';

-- ===================================================================
-- INITIAL DATA
-- ===================================================================

-- Insert default ETL job (nếu chưa có)
INSERT INTO meta.etl_job (job_code, job_name, description, is_active)
VALUES (
    'MINIO_ECOMMERCE_DWH_PIPELINE',
    'Ecommerce DSS - Full DWH (Star Schema)',
    'Full DWH pipeline: crawl -> MinIO -> Spark build star schema (products + reviews) -> ML',
    TRUE
)
ON CONFLICT (job_code) DO NOTHING;

-- Insert default ETL job cho ML training (nếu có)
INSERT INTO meta.etl_job (job_code, job_name, description, is_active)
VALUES (
    'ML_TRAINING_PIPELINE',
    'ML Training Pipeline',
    'ML model training pipeline: sentiment, recommendation, price prediction',
    TRUE
)
ON CONFLICT (job_code) DO NOTHING;

