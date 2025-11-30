-- ============================================
-- DSS (Decision Support System) Schema
-- ============================================
-- Create schema and tables for storing DSS decisions and action plans

-- Create dss schema
CREATE SCHEMA IF NOT EXISTS dss;

-- ============================================
-- Table: dss_analysis_session
-- Purpose: Store snapshots of DSS analysis runs
-- ============================================
CREATE TABLE IF NOT EXISTS dss.dss_analysis_session (
    session_id BIGSERIAL PRIMARY KEY,
    scenario_key VARCHAR(50) NOT NULL,
    user_id BIGINT NOT NULL,
    
    -- Analysis inputs and outputs (JSONB for flexibility)
    filters_json JSONB,
    kpi_summary_json JSONB,
    table_data_sample_json JSONB,
    ai_summary_insights JSONB,
    ai_recommended_actions JSONB,
    date_adjustment_info JSONB,
    
    -- Metadata
    run_started_at TIMESTAMP,
    run_finished_at TIMESTAMP,
    generated_at TIMESTAMP DEFAULT NOW(),
    source_endpoint VARCHAR(100),
    
    CONSTRAINT fk_session_user FOREIGN KEY (user_id) REFERENCES iam.iam_user(user_id) ON DELETE CASCADE,
    CONSTRAINT chk_scenario_key CHECK (scenario_key IN ('price_prediction', 'product_recommendation', 'review_sentiment'))
);

-- Indexes for dss_analysis_session
CREATE INDEX IF NOT EXISTS idx_session_user_id ON dss.dss_analysis_session(user_id);
CREATE INDEX IF NOT EXISTS idx_session_scenario ON dss.dss_analysis_session(scenario_key);
CREATE INDEX IF NOT EXISTS idx_session_generated_at ON dss.dss_analysis_session(generated_at DESC);

-- Comments for dss_analysis_session
COMMENT ON TABLE dss.dss_analysis_session IS 'Stores snapshots of DSS analysis runs for decision tracking';
COMMENT ON COLUMN dss.dss_analysis_session.scenario_key IS 'Type of DSS scenario: price_prediction, product_recommendation, or review_sentiment';
COMMENT ON COLUMN dss.dss_analysis_session.filters_json IS 'Filters used in the analysis (date range, platforms, categories, etc.)';
COMMENT ON COLUMN dss.dss_analysis_session.kpi_summary_json IS 'KPI summary from the analysis';
COMMENT ON COLUMN dss.dss_analysis_session.table_data_sample_json IS 'Sample of table data (top N rows to avoid excessive storage)';
COMMENT ON COLUMN dss.dss_analysis_session.ai_summary_insights IS 'AI-generated insights about the analysis';
COMMENT ON COLUMN dss.dss_analysis_session.ai_recommended_actions IS 'AI-recommended actions based on analysis';
COMMENT ON COLUMN dss.dss_analysis_session.date_adjustment_info IS 'Date adjustment information for price/review scenarios';

-- ============================================
-- Table: dss_decision
-- Purpose: Store analyst decisions based on DSS analysis
-- ============================================
CREATE TABLE IF NOT EXISTS dss.dss_decision (
    decision_id BIGSERIAL PRIMARY KEY,
    session_id BIGINT NOT NULL,
    scenario_key VARCHAR(50) NOT NULL,
    
    -- Decision information
    title TEXT NOT NULL,
    description TEXT,
    status VARCHAR(30) NOT NULL DEFAULT 'DRAFT',
    
    -- User tracking
    created_by BIGINT NOT NULL,
    approved_by BIGINT,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    approved_at TIMESTAMP,
    
    CONSTRAINT fk_decision_session FOREIGN KEY (session_id) REFERENCES dss.dss_analysis_session(session_id) ON DELETE CASCADE,
    CONSTRAINT fk_decision_created_by FOREIGN KEY (created_by) REFERENCES iam.iam_user(user_id) ON DELETE CASCADE,
    CONSTRAINT fk_decision_approved_by FOREIGN KEY (approved_by) REFERENCES iam.iam_user(user_id) ON DELETE SET NULL,
    CONSTRAINT chk_decision_status CHECK (status IN ('DRAFT', 'APPROVED', 'REJECTED', 'IMPLEMENTED')),
    CONSTRAINT chk_decision_scenario CHECK (scenario_key IN ('price_prediction', 'product_recommendation', 'review_sentiment'))
);

-- Indexes for dss_decision
CREATE INDEX IF NOT EXISTS idx_decision_session ON dss.dss_decision(session_id);
CREATE INDEX IF NOT EXISTS idx_decision_scenario ON dss.dss_decision(scenario_key);
CREATE INDEX IF NOT EXISTS idx_decision_status ON dss.dss_decision(status);
CREATE INDEX IF NOT EXISTS idx_decision_created_by ON dss.dss_decision(created_by);
CREATE INDEX IF NOT EXISTS idx_decision_created_at ON dss.dss_decision(created_at DESC);

-- Comments for dss_decision
COMMENT ON TABLE dss.dss_decision IS 'Stores analyst decisions based on DSS analysis results';
COMMENT ON COLUMN dss.dss_decision.scenario_key IS 'Denormalized scenario key for faster queries';
COMMENT ON COLUMN dss.dss_decision.status IS 'Decision status: DRAFT, APPROVED, REJECTED, or IMPLEMENTED';
COMMENT ON COLUMN dss.dss_decision.title IS 'Decision title describing the main action';
COMMENT ON COLUMN dss.dss_decision.description IS 'Detailed description or context for the decision';

-- ============================================
-- Table: dss_action_item
-- Purpose: Store specific action items for each decision
-- ============================================
CREATE TABLE IF NOT EXISTS dss.dss_action_item (
    action_id BIGSERIAL PRIMARY KEY,
    decision_id BIGINT NOT NULL,
    
    -- Action type and target
    action_type VARCHAR(50) NOT NULL,
    target_level VARCHAR(30) NOT NULL,
    
    -- Target references (nullable - depends on target_level)
    product_sk BIGINT,
    platform_sk INT,
    category_sk INT,
    
    -- Values
    current_value NUMERIC(18,4),
    recommended_value NUMERIC(18,4),
    chosen_value NUMERIC(18,4),
    unit VARCHAR(20),
    
    -- Planning
    planned_start_date DATE,
    planned_end_date DATE,
    status VARCHAR(30) NOT NULL DEFAULT 'PLANNED',
    
    -- Additional notes
    note TEXT,
    
    CONSTRAINT fk_action_decision FOREIGN KEY (decision_id) REFERENCES dss.dss_decision(decision_id) ON DELETE CASCADE,
    CONSTRAINT fk_action_product FOREIGN KEY (product_sk) REFERENCES dwh.dim_product(product_sk) ON DELETE SET NULL,
    CONSTRAINT fk_action_platform FOREIGN KEY (platform_sk) REFERENCES dwh.dim_platform(platform_sk) ON DELETE SET NULL,
    CONSTRAINT fk_action_category FOREIGN KEY (category_sk) REFERENCES dwh.dim_category(category_sk) ON DELETE SET NULL,
    CONSTRAINT chk_action_target_level CHECK (target_level IN ('product', 'category', 'platform')),
    CONSTRAINT chk_action_status CHECK (status IN ('PLANNED', 'IN_PROGRESS', 'DONE', 'CANCELLED'))
);

-- Indexes for dss_action_item
CREATE INDEX IF NOT EXISTS idx_action_decision ON dss.dss_action_item(decision_id);
CREATE INDEX IF NOT EXISTS idx_action_type ON dss.dss_action_item(action_type);
CREATE INDEX IF NOT EXISTS idx_action_status ON dss.dss_action_item(status);
CREATE INDEX IF NOT EXISTS idx_action_product ON dss.dss_action_item(product_sk) WHERE product_sk IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_action_platform ON dss.dss_action_item(platform_sk) WHERE platform_sk IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_action_category ON dss.dss_action_item(category_sk) WHERE category_sk IS NOT NULL;

-- Comments for dss_action_item
COMMENT ON TABLE dss.dss_action_item IS 'Stores specific action items for DSS decisions';
COMMENT ON COLUMN dss.dss_action_item.action_type IS 'Type of action: change_price, adjust_stock, marketing_campaign, fix_quality, review_monitoring, etc.';
COMMENT ON COLUMN dss.dss_action_item.target_level IS 'Target level: product, category, or platform';
COMMENT ON COLUMN dss.dss_action_item.current_value IS 'Current value (e.g., current price)';
COMMENT ON COLUMN dss.dss_action_item.recommended_value IS 'ML/AI recommended value';
COMMENT ON COLUMN dss.dss_action_item.chosen_value IS 'Value chosen by analyst';
COMMENT ON COLUMN dss.dss_action_item.unit IS 'Unit of measurement: VND, %, score, etc.';
COMMENT ON COLUMN dss.dss_action_item.status IS 'Action status: PLANNED, IN_PROGRESS, DONE, or CANCELLED';

-- ============================================
-- Grant permissions (adjust as needed)
-- ============================================
-- GRANT USAGE ON SCHEMA dss TO your_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dss TO your_app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dss TO your_app_user;
