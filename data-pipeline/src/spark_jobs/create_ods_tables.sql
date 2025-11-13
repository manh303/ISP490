-- Create ODS tables if not exists
CREATE TABLE IF NOT EXISTS ods_product_clean (
    global_product_id VARCHAR NOT NULL,
    source_platform VARCHAR NOT NULL,
    platform_product_id TEXT,
    product_name TEXT NOT NULL,
    brand_name TEXT,
    category TEXT,
    category_sk INTEGER,
    seller_name TEXT,
    price_current NUMERIC,
    price_original NUMERIC,
    discount_percent NUMERIC,
    rating_avg NUMERIC,
    review_count INTEGER,
    url TEXT,
    image_url TEXT,
    crawled_at TIMESTAMP,
    created_at TIMESTAMP,
    last_seen TIMESTAMP,
    PRIMARY KEY (global_product_id, source_platform)
);

CREATE TABLE IF NOT EXISTS ods_review_clean (
    global_review_id VARCHAR NOT NULL,
    source_platform VARCHAR NOT NULL,
    platform_product_id TEXT,
    review_id TEXT,
    reviewer_name TEXT,
    rating INTEGER,
    review_text TEXT,
    review_time TIMESTAMP,
    helpful_count INTEGER,
    crawled_at TIMESTAMP,
    created_at TIMESTAMP,
    last_seen TIMESTAMP,
    PRIMARY KEY (global_review_id, source_platform)
);

CREATE INDEX IF NOT EXISTS idx_ods_product_platform ON ods_product_clean(source_platform);
CREATE INDEX IF NOT EXISTS idx_ods_review_product ON ods_review_clean(platform_product_id);
