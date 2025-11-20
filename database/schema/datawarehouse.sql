-- ===================================================================
-- STAR SCHEMA CHO E-COMMERCE DSS
-- ===================================================================

-- ========= SCHEMA =========
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS ml;

-- ========= DIMENSIONS =========

-- 1. dim_date
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_sk      SERIAL PRIMARY KEY,
    date_value   DATE NOT NULL UNIQUE,
    year         INT  NOT NULL,
    month        INT  NOT NULL,
    day          INT  NOT NULL,
    quarter      INT  NOT NULL,
    week_of_year INT  NOT NULL,
    day_of_week  INT  NOT NULL,       -- 1=Mon ... 7=Sun
    day_name     VARCHAR(10),
    is_weekend   BOOLEAN NOT NULL
);

-- 2. dim_platform
CREATE TABLE IF NOT EXISTS dwh.dim_platform (
    platform_sk    SERIAL PRIMARY KEY,
    platform_code  VARCHAR(50) NOT NULL UNIQUE,     -- 'tiki', 'lazada'
    platform_name  VARCHAR(100),
    country_code   CHAR(2),
    base_url       VARCHAR(255),
    is_active      BOOLEAN DEFAULT TRUE,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

-- 3. dim_brand
CREATE TABLE IF NOT EXISTS dwh.dim_brand (
    brand_sk         SERIAL PRIMARY KEY,
    brand_name       VARCHAR(150) NOT NULL UNIQUE,  -- 'SAMSUNG'
    brand_normalized VARCHAR(150),
    country          VARCHAR(100),
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- 4. dim_category
CREATE TABLE IF NOT EXISTS dwh.dim_category (
    category_sk       SERIAL PRIMARY KEY,
    category_std_key  VARCHAR(100) NOT NULL UNIQUE,  -- 'laptop_gaming'
    category_lvl1     VARCHAR(100),
    category_lvl2     VARCHAR(100),
    category_lvl3     VARCHAR(100),
    full_path         VARCHAR(400),
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- 5. dim_product
CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_sk        SERIAL PRIMARY KEY,
    product_key       VARCHAR(100) NOT NULL UNIQUE, -- global_product_id_synced
    product_master_id VARCHAR(256),
    product_name      VARCHAR(500),
    product_slug      VARCHAR(500),
    brand_sk          INT REFERENCES dwh.dim_brand(brand_sk),
    category_sk       INT REFERENCES dwh.dim_category(category_sk),
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ========= FACT TABLES =========

-- 6. fact_product_daily
CREATE TABLE IF NOT EXISTS dwh.fact_product_daily (
    date_sk       INT NOT NULL REFERENCES dwh.dim_date(date_sk),
    product_sk    INT NOT NULL REFERENCES dwh.dim_product(product_sk),
    platform_sk   INT NOT NULL REFERENCES dwh.dim_platform(platform_sk),

    currency_code     VARCHAR(10),
    min_price         NUMERIC(15,2),
    max_price         NUMERIC(15,2),
    avg_price         NUMERIC(15,2),
    median_price      NUMERIC(15,2),
    price_stddev      NUMERIC(15,2),

    total_review_count BIGINT,
    avg_rating         NUMERIC(3,2),

    snapshot_count     INT,

    CONSTRAINT fact_product_daily_pk PRIMARY KEY (date_sk, product_sk, platform_sk)
);

CREATE INDEX IF NOT EXISTS idx_fact_product_daily_prod_plat_date
ON dwh.fact_product_daily (product_sk, platform_sk, date_sk);

-- 7. fact_review (detail)
CREATE TABLE IF NOT EXISTS dwh.fact_review (
    review_sk        BIGSERIAL PRIMARY KEY,
    review_id_nk     VARCHAR(255) NOT NULL,
    product_sk       INT NOT NULL REFERENCES dwh.dim_product(product_sk),
    platform_sk      INT NOT NULL REFERENCES dwh.dim_platform(platform_sk),
    date_sk          INT NOT NULL REFERENCES dwh.dim_date(date_sk),

    rating           SMALLINT,
    helpful_votes    INT,
    sentiment_score  NUMERIC(4,3),

    review_title     TEXT,
    review_body      TEXT,
    reviewer_name    VARCHAR(255),
    is_verified_purchase BOOLEAN,
    raw_review_date  TIMESTAMPTZ,

    UNIQUE (review_id_nk, platform_sk)
);

CREATE INDEX IF NOT EXISTS idx_fact_review_prod_plat_date
ON dwh.fact_review (product_sk, platform_sk, date_sk);

-- 8. fact_review_daily (aggregate)
CREATE TABLE IF NOT EXISTS dwh.fact_review_daily (
    date_sk      INT NOT NULL REFERENCES dwh.dim_date(date_sk),
    product_sk   INT NOT NULL REFERENCES dwh.dim_product(product_sk),
    platform_sk  INT NOT NULL REFERENCES dwh.dim_platform(platform_sk),

    review_count   BIGINT,
    avg_rating     NUMERIC(3,2),
    rating_1_count BIGINT,
    rating_2_count BIGINT,
    rating_3_count BIGINT,
    rating_4_count BIGINT,
    rating_5_count BIGINT,
    avg_sentiment  NUMERIC(4,3),

    CONSTRAINT fact_review_daily_pk PRIMARY KEY (date_sk, product_sk, platform_sk)
);

CREATE INDEX IF NOT EXISTS idx_fact_review_daily_prod_plat_date
ON dwh.fact_review_daily (product_sk, platform_sk, date_sk);

-- ========= ML TABLES =========

-- 9. dim_ml_model
CREATE TABLE IF NOT EXISTS ml.dim_ml_model (
    model_sk      SERIAL PRIMARY KEY,
    model_name    VARCHAR(100) NOT NULL,
    model_type    VARCHAR(50) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    training_data_until DATE,
    metrics       JSONB,
    status        VARCHAR(20),
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (model_name, model_version)
);

-- 10. fact_price_prediction
CREATE TABLE IF NOT EXISTS ml.fact_price_prediction (
    prediction_sk   BIGSERIAL PRIMARY KEY,
    model_sk        INT NOT NULL REFERENCES ml.dim_ml_model(model_sk),
    date_sk         INT NOT NULL REFERENCES dwh.dim_date(date_sk),
    product_sk      INT NOT NULL REFERENCES dwh.dim_product(product_sk),
    platform_sk     INT NOT NULL REFERENCES dwh.dim_platform(platform_sk),

    predicted_price NUMERIC(15,2) NOT NULL,
    ci_lower        NUMERIC(15,2),
    ci_upper        NUMERIC(15,2),

    run_id          VARCHAR(100),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_price_pred_prod_plat_date
ON ml.fact_price_prediction (product_sk, platform_sk, date_sk);

-- 11. fact_product_recommendation
CREATE TABLE IF NOT EXISTS ml.fact_product_recommendation (
    recommendation_sk      BIGSERIAL PRIMARY KEY,
    model_sk               INT NOT NULL REFERENCES ml.dim_ml_model(model_sk),
    date_sk                INT NOT NULL REFERENCES dwh.dim_date(date_sk),

    source_product_sk      INT NOT NULL REFERENCES dwh.dim_product(product_sk),
    recommended_product_sk INT NOT NULL REFERENCES dwh.dim_product(product_sk),
    rank                   INT NOT NULL,

    similarity_score       NUMERIC(5,4),
    recommendation_type    VARCHAR(50),
    created_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rec_source_prod_date
ON ml.fact_product_recommendation (source_product_sk, date_sk, rank);
