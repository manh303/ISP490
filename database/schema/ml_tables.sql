-- ML Model Registry & Management

CREATE TABLE IF NOT EXISTS ml_model_registry (
    model_id BIGSERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_type VARCHAR(50) NOT NULL, -- demand_prediction|product_recommendation|price_prediction|customer_segmentation
    version VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'inactive', -- active|inactive|training|archived
    description TEXT,
    model_path VARCHAR(255),
    metrics JSONB, -- Store model metrics as JSON
    accuracy DECIMAL(5,4),
    precision DECIMAL(5,4),
    recall DECIMAL(5,4),
    f1_score DECIMAL(5,4),
    trained_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW(),
    triggered_by BIGINT, -- user_id who triggered training
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (triggered_by) REFERENCES "user"(user_id) ON DELETE SET NULL
);

CREATE INDEX idx_ml_model_type ON ml_model_registry(model_type);
CREATE INDEX idx_ml_model_status ON ml_model_registry(status);
CREATE INDEX idx_ml_model_created ON ml_model_registry(created_at DESC);

-- ML Model Output Tables

CREATE TABLE IF NOT EXISTS ml_product_recommendations (
    recommendation_id BIGSERIAL PRIMARY KEY,
    product_sk BIGINT NOT NULL,
    recommended_product_sk BIGINT NOT NULL,
    similarity_score DECIMAL(5,4),
    recommendation_type VARCHAR(50), -- collaborative|content_based|hybrid
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (product_sk) REFERENCES dwh_dim_product(product_sk),
    FOREIGN KEY (recommended_product_sk) REFERENCES dwh_dim_product(product_sk)
);

CREATE TABLE IF NOT EXISTS ml_price_predictions (
    prediction_id BIGSERIAL PRIMARY KEY,
    product_sk BIGINT NOT NULL,
    platform_sk INT NOT NULL,
    prediction_date DATE NOT NULL,
    predicted_price DECIMAL(15,2),
    confidence_interval_lower DECIMAL(15,2),
    confidence_interval_upper DECIMAL(15,2),
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (product_sk) REFERENCES dwh_dim_product(product_sk),
    FOREIGN KEY (platform_sk) REFERENCES dwh_dim_platform(platform_sk)
);

CREATE TABLE IF NOT EXISTS ml_demand_forecast (
    forecast_id BIGSERIAL PRIMARY KEY,
    product_sk BIGINT NOT NULL,
    forecast_date DATE NOT NULL,
    predicted_demand INT,
    confidence_level DECIMAL(3,2),
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (product_sk) REFERENCES dwh_dim_product(product_sk)
);

CREATE TABLE IF NOT EXISTS ml_customer_segments (
    segment_id BIGSERIAL PRIMARY KEY,
    segment_name VARCHAR(100),
    segment_description TEXT,
    avg_purchase_value DECIMAL(15,2),
    purchase_frequency DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT NOW()
);
