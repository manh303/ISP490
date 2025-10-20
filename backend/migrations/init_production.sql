-- Production Database Initialization Script for Render
-- Run this script after creating PostgreSQL database on Render

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS dw_core;
CREATE SCHEMA IF NOT EXISTS vietnam_dw;

-- Create basic tables for production
CREATE TABLE IF NOT EXISTS bronze.orders_raw (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    product_id VARCHAR(255),
    quantity INTEGER,
    price DECIMAL(15,2),
    order_date TIMESTAMP,
    status VARCHAR(100),
    payment_method VARCHAR(100),
    platform VARCHAR(100),
    city VARCHAR(100),
    province VARCHAR(100),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.orders_clean (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    product_id VARCHAR(255),
    quantity INTEGER,
    price DECIMAL(15,2),
    order_date TIMESTAMP,
    status VARCHAR(100),
    payment_method VARCHAR(100),
    platform VARCHAR(100),
    city VARCHAR(100),
    province VARCHAR(100),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.daily_sales_summary (
    date_key DATE PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(15,2),
    avg_order_value DECIMAL(15,2),
    unique_customers INTEGER,
    top_payment_method VARCHAR(100),
    top_platform VARCHAR(100),
    top_city VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.customer_summary (
    customer_id VARCHAR(255) PRIMARY KEY,
    customer_segment VARCHAR(100),
    total_spent DECIMAL(15,2),
    total_orders INTEGER,
    avg_order_value DECIMAL(15,2),
    first_order_date DATE,
    last_order_date DATE,
    days_since_last_order INTEGER,
    clv_score DECIMAL(10,2),
    churn_risk VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw_core.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(255) UNIQUE,
    product_name VARCHAR(500),
    category VARCHAR(200),
    subcategory VARCHAR(200),
    brand VARCHAR(200),
    price DECIMAL(15,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw_core.fact_orders (
    fact_key SERIAL PRIMARY KEY,
    order_id VARCHAR(255),
    customer_key INTEGER,
    product_key INTEGER,
    date_key INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(15,2),
    total_amount DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_orders_raw_date ON bronze.orders_raw(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_raw_customer ON bronze.orders_raw(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_clean_date ON silver.orders_clean(order_date);
CREATE INDEX IF NOT EXISTS idx_customer_summary_segment ON gold.customer_summary(customer_segment);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON dw_core.fact_orders(date_key);

-- Insert sample data for testing
INSERT INTO bronze.orders_raw (order_id, customer_id, product_id, quantity, price, order_date, status, payment_method, platform, city, province)
VALUES
    ('ORD001', 'CUST001', 'PROD001', 2, 1500000, CURRENT_TIMESTAMP - INTERVAL '1 day', 'completed', 'momo', 'mobile', 'Ho Chi Minh', 'HCM'),
    ('ORD002', 'CUST002', 'PROD002', 1, 800000, CURRENT_TIMESTAMP - INTERVAL '2 days', 'completed', 'zalopay', 'web', 'Hanoi', 'HN'),
    ('ORD003', 'CUST003', 'PROD003', 3, 2400000, CURRENT_TIMESTAMP - INTERVAL '3 days', 'pending', 'vnpay', 'mobile', 'Da Nang', 'DN')
ON CONFLICT (order_id) DO NOTHING;

-- Copy to silver layer
INSERT INTO silver.orders_clean
SELECT order_id, customer_id, product_id, quantity, price, order_date, status, payment_method, platform, city, province, CURRENT_TIMESTAMP
FROM bronze.orders_raw
ON CONFLICT (order_id) DO NOTHING;

-- Create daily summary
INSERT INTO gold.daily_sales_summary (date_key, total_orders, total_revenue, avg_order_value, unique_customers, top_payment_method, top_platform, top_city)
SELECT
    CURRENT_DATE,
    COUNT(*) as total_orders,
    SUM(price * quantity) as total_revenue,
    AVG(price * quantity) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers,
    'momo' as top_payment_method,
    'mobile' as top_platform,
    'Ho Chi Minh' as top_city
FROM silver.orders_clean
WHERE DATE(order_date) = CURRENT_DATE
ON CONFLICT (date_key) DO UPDATE SET
    total_orders = EXCLUDED.total_orders,
    total_revenue = EXCLUDED.total_revenue,
    avg_order_value = EXCLUDED.avg_order_value,
    unique_customers = EXCLUDED.unique_customers;

-- Create customer summary
INSERT INTO gold.customer_summary (customer_id, customer_segment, total_spent, total_orders, avg_order_value, first_order_date, last_order_date, days_since_last_order, clv_score, churn_risk)
SELECT
    customer_id,
    CASE
        WHEN SUM(price * quantity) > 2000000 THEN 'VIP'
        WHEN SUM(price * quantity) > 1000000 THEN 'Premium'
        ELSE 'Regular'
    END as customer_segment,
    SUM(price * quantity) as total_spent,
    COUNT(*) as total_orders,
    AVG(price * quantity) as avg_order_value,
    MIN(DATE(order_date)) as first_order_date,
    MAX(DATE(order_date)) as last_order_date,
    CURRENT_DATE - MAX(DATE(order_date)) as days_since_last_order,
    SUM(price * quantity) * 0.3 as clv_score,
    CASE
        WHEN CURRENT_DATE - MAX(DATE(order_date)) > 30 THEN 'High'
        WHEN CURRENT_DATE - MAX(DATE(order_date)) > 14 THEN 'Medium'
        ELSE 'Low'
    END as churn_risk
FROM silver.orders_clean
GROUP BY customer_id
ON CONFLICT (customer_id) DO UPDATE SET
    customer_segment = EXCLUDED.customer_segment,
    total_spent = EXCLUDED.total_spent,
    total_orders = EXCLUDED.total_orders,
    avg_order_value = EXCLUDED.avg_order_value,
    last_order_date = EXCLUDED.last_order_date,
    days_since_last_order = EXCLUDED.days_since_last_order,
    clv_score = EXCLUDED.clv_score,
    churn_risk = EXCLUDED.churn_risk;

-- Insert products
INSERT INTO dw_core.dim_products (product_id, product_name, category, subcategory, brand, price)
VALUES
    ('PROD001', 'iPhone 15 Pro Max', 'Electronics', 'Smartphones', 'Apple', 30000000),
    ('PROD002', 'Samsung Galaxy S24', 'Electronics', 'Smartphones', 'Samsung', 25000000),
    ('PROD003', 'MacBook Air M2', 'Electronics', 'Laptops', 'Apple', 28000000)
ON CONFLICT (product_id) DO NOTHING;

-- Grant permissions for application user
-- CREATE USER IF NOT EXISTS render_app_user WITH PASSWORD 'your_secure_password';
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO render_app_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO render_app_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO render_app_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dw_core TO render_app_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA vietnam_dw TO render_app_user;

-- Create functions for data processing (optional)
CREATE OR REPLACE FUNCTION refresh_daily_summary()
RETURNS VOID AS $$
BEGIN
    INSERT INTO gold.daily_sales_summary (date_key, total_orders, total_revenue, avg_order_value, unique_customers, top_payment_method, top_platform, top_city)
    SELECT
        DATE(order_date) as date_key,
        COUNT(*) as total_orders,
        SUM(price * quantity) as total_revenue,
        AVG(price * quantity) as avg_order_value,
        COUNT(DISTINCT customer_id) as unique_customers,
        MODE() WITHIN GROUP (ORDER BY payment_method) as top_payment_method,
        MODE() WITHIN GROUP (ORDER BY platform) as top_platform,
        MODE() WITHIN GROUP (ORDER BY city) as top_city
    FROM silver.orders_clean
    WHERE DATE(order_date) >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY DATE(order_date)
    ON CONFLICT (date_key) DO UPDATE SET
        total_orders = EXCLUDED.total_orders,
        total_revenue = EXCLUDED.total_revenue,
        avg_order_value = EXCLUDED.avg_order_value,
        unique_customers = EXCLUDED.unique_customers,
        top_payment_method = EXCLUDED.top_payment_method,
        top_platform = EXCLUDED.top_platform,
        top_city = EXCLUDED.top_city;
END;
$$ LANGUAGE plpgsql;

COMMIT;