-- Initialize DSS Database
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS processed_data;
CREATE SCHEMA IF NOT EXISTS ml_models;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw_data TO dss_user;
GRANT ALL PRIVILEGES ON SCHEMA processed_data TO dss_user;
GRANT ALL PRIVILEGES ON SCHEMA ml_models TO dss_user;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO dss_user;

-- Create basic tables
CREATE TABLE IF NOT EXISTS raw_data.products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    name TEXT NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2),
    rating DECIMAL(3,1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_products_category ON raw_data.products(category);
CREATE INDEX IF NOT EXISTS idx_products_price ON raw_data.products(price);
CREATE INDEX IF NOT EXISTS idx_products_created_at ON raw_data.products(created_at);