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
