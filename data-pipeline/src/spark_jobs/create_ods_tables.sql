-- Create ODS tables if not exists
CREATE TABLE IF NOT EXISTS ods_product_clean (
    product_id VARCHAR(255),
    product_name TEXT,
    price_current NUMERIC,
    rating_avg NUMERIC,
    review_count INTEGER,
    category VARCHAR(255),
    source_platform VARCHAR(50),
    crawl_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, crawl_date)
) PARTITION BY RANGE (crawl_date);

CREATE TABLE IF NOT EXISTS ods_review_clean (
    review_id VARCHAR(255),
    product_id VARCHAR(255),
    reviewer_name VARCHAR(255),
    rating INTEGER,
    content TEXT,
    review_time TIMESTAMP,
    source_platform VARCHAR(50),
    crawl_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (review_id, crawl_date)
) PARTITION BY RANGE (crawl_date);

-- Auto-create partition for current month
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('month', CURRENT_DATE);
    end_date DATE := start_date + INTERVAL '1 month';
    partition_name TEXT;
BEGIN
    partition_name := 'ods_product_clean_' || TO_CHAR(start_date, 'YYYY_MM');
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF ods_product_clean FOR VALUES FROM (%L) TO (%L)', 
                   partition_name, start_date, end_date);
    
    partition_name := 'ods_review_clean_' || TO_CHAR(start_date, 'YYYY_MM');
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF ods_review_clean FOR VALUES FROM (%L) TO (%L)', 
                   partition_name, start_date, end_date);
END $$;

CREATE INDEX IF NOT EXISTS idx_ods_product_platform ON ods_product_clean(source_platform, crawl_date);
CREATE INDEX IF NOT EXISTS idx_ods_review_product ON ods_review_clean(product_id, crawl_date);
