-- =====================
-- STAGING (Data Integration Framework - Raw Landing)
-- =====================
CREATE TABLE stg_raw_products (
  id                BIGSERIAL PRIMARY KEY,
  source_platform   VARCHAR(20) NOT NULL,  -- lazada|fptshop|cellphones|tiki|hoanghamobile|thegioididong
  url               TEXT,
  platform_product_id TEXT,
  crawled_at        TIMESTAMP NOT NULL,
  raw_data          JSONB NOT NULL,        -- Full JSON payload from crawlers
  checksum          TEXT,
  load_id           VARCHAR(36),
  created_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE stg_raw_reviews (
  id                  BIGSERIAL PRIMARY KEY,
  source_platform     VARCHAR(20) NOT NULL,
  platform_product_id TEXT,
  crawled_at          TIMESTAMP NOT NULL,
  raw_data            JSONB NOT NULL,
  load_id             VARCHAR(36),
  created_at          TIMESTAMP DEFAULT NOW()
);

-- =====================
-- ODS (Data Standardization Tool Output)
-- =====================

-- Platform reference (after data cleaning)
CREATE TABLE ods_platform_ref (
  platform_sk   SERIAL PRIMARY KEY,
  platform_code VARCHAR(20) UNIQUE NOT NULL,  -- lazada|fptshop|cellphones|tiki|hoanghamobile|thegioididong
  platform_name TEXT NOT NULL,
  website_url   TEXT,
  country_code  VARCHAR(2) DEFAULT 'VN',
  is_active     BOOLEAN DEFAULT TRUE
);

-- Product ID mapping (synchronize identifier step)
CREATE TABLE ods_product_id_map (
  platform_sk          INT NOT NULL,
  platform_product_id  TEXT NOT NULL,
  global_product_id    VARCHAR(36) NOT NULL,
  first_seen           TIMESTAMP DEFAULT NOW(),
  last_seen            TIMESTAMP DEFAULT NOW(),
  is_active            BOOLEAN DEFAULT TRUE,
  PRIMARY KEY (platform_sk, platform_product_id),
  FOREIGN KEY (platform_sk) REFERENCES ods_platform_ref(platform_sk)
);

-- Category mapping (standardized categories)
CREATE TABLE ods_category_taxonomy (
  category_sk        SERIAL PRIMARY KEY,
  category_code      VARCHAR(100) UNIQUE NOT NULL,
  category_name      TEXT NOT NULL,
  parent_category_sk INT,
  level              INT DEFAULT 1,
  FOREIGN KEY (parent_category_sk) REFERENCES ods_category_taxonomy(category_sk)
);

-- Platform category mapping (category mapping step)
CREATE TABLE ods_platform_category_map (
  platform_sk           INT NOT NULL,
  platform_category_id  TEXT NOT NULL,
  category_sk           INT NOT NULL,
  confidence_score      DECIMAL(3,2) DEFAULT 1.0,
  PRIMARY KEY (platform_sk, platform_category_id),
  FOREIGN KEY (platform_sk) REFERENCES ods_platform_ref(platform_sk),
  FOREIGN KEY (category_sk) REFERENCES ods_category_taxonomy(category_sk)
);

-- Cleaned products (after data quality & dedup)
CREATE TABLE ods_product_clean (
  global_product_id VARCHAR(36) PRIMARY KEY,
  product_name      TEXT NOT NULL,
  brand_name        TEXT,
  category_sk       INT,
  description       TEXT,
  image_urls        TEXT[],
  seller_name       TEXT,
  seller_type       VARCHAR(50),
  attributes_json   JSONB,
  first_seen        TIMESTAMP DEFAULT NOW(),
  last_seen         TIMESTAMP DEFAULT NOW(),
  is_active         BOOLEAN DEFAULT TRUE,
  data_quality_score DECIMAL(3,2) DEFAULT 0.0,
  FOREIGN KEY (category_sk) REFERENCES ods_category_taxonomy(category_sk)
);

-- Price points (time-series data)
CREATE TABLE ods_price_point (
  id                BIGSERIAL PRIMARY KEY,
  global_product_id VARCHAR(36) NOT NULL,
  platform_sk       INT NOT NULL,
  captured_at       TIMESTAMP NOT NULL,
  price_current     DECIMAL(15,2),
  price_original    DECIMAL(15,2),
  discount_percent  DECIMAL(5,2),
  currency          VARCHAR(3) DEFAULT 'VND',
  is_available      BOOLEAN DEFAULT TRUE,
  FOREIGN KEY (global_product_id) REFERENCES ods_product_clean(global_product_id),
  FOREIGN KEY (platform_sk) REFERENCES ods_platform_ref(platform_sk)
);

-- Rating and reviews (aggregated)
CREATE TABLE ods_rating_snapshot (
  id                BIGSERIAL PRIMARY KEY,
  global_product_id VARCHAR(36) NOT NULL,
  platform_sk       INT NOT NULL,
  captured_at       TIMESTAMP NOT NULL,
  rating_avg        DECIMAL(3,2),
  rating_count      INT,
  review_count      INT,
  sold_count        INT,
  FOREIGN KEY (global_product_id) REFERENCES ods_product_clean(global_product_id),
  FOREIGN KEY (platform_sk) REFERENCES ods_platform_ref(platform_sk)
);

-- Individual reviews (cleaned)
CREATE TABLE ods_review_clean (
  id                BIGSERIAL PRIMARY KEY,
  global_product_id VARCHAR(36) NOT NULL,
  platform_sk       INT NOT NULL,
  reviewer_name     TEXT,
  rating            INT CHECK (rating >= 1 AND rating <= 5),
  review_content    TEXT,
  review_time       TIMESTAMP,
  helpful_count     INT DEFAULT 0,
  sku_info          TEXT,
  sentiment_score   DECIMAL(5,2),
  sentiment_label   VARCHAR(20),
  created_at        TIMESTAMP DEFAULT NOW(),
  FOREIGN KEY (global_product_id) REFERENCES ods_product_clean(global_product_id),
  FOREIGN KEY (platform_sk) REFERENCES ods_platform_ref(platform_sk)
);

-- =====================
-- DWH (Data Warehouse - Star Schema)
-- =====================

-- Dimension Tables
CREATE TABLE dwh_dim_date (
  date_sk       INT PRIMARY KEY,
  date_value    DATE UNIQUE NOT NULL,
  day           INT,
  month         INT,
  quarter       INT,
  year          INT,
  week_of_year  INT,
  is_weekend    BOOLEAN,
  day_name      VARCHAR(10),
  month_name    VARCHAR(12)
);

CREATE TABLE dwh_dim_platform (
  platform_sk   SERIAL PRIMARY KEY,
  platform_code VARCHAR(20) UNIQUE NOT NULL,
  platform_name TEXT NOT NULL,
  website_url   TEXT,
  country_code  VARCHAR(2) DEFAULT 'VN',
  is_active     BOOLEAN DEFAULT TRUE
);

CREATE TABLE dwh_dim_brand (
  brand_sk   SERIAL PRIMARY KEY,
  brand_code VARCHAR(100) UNIQUE,
  brand_name TEXT NOT NULL
);

CREATE TABLE dwh_dim_category (
  category_sk        SERIAL PRIMARY KEY,
  category_code      VARCHAR(100) UNIQUE NOT NULL,
  category_name      TEXT NOT NULL,
  parent_category_sk INT,
  category_level     INT DEFAULT 1,
  category_path      TEXT,
  FOREIGN KEY (parent_category_sk) REFERENCES dwh_dim_category(category_sk)
);

CREATE TABLE dwh_dim_product (
  product_sk        BIGSERIAL PRIMARY KEY,
  global_product_id VARCHAR(36) NOT NULL,
  product_name      TEXT NOT NULL,
  brand_sk          INT,
  category_sk       INT,
  seller_name       TEXT,
  seller_type       VARCHAR(50),
  effective_from    DATE NOT NULL,
  effective_to      DATE NOT NULL,
  is_current        BOOLEAN NOT NULL,
  UNIQUE (global_product_id, effective_from),
  FOREIGN KEY (brand_sk)    REFERENCES dwh_dim_brand(brand_sk),
  FOREIGN KEY (category_sk) REFERENCES dwh_dim_category(category_sk)
);

-- Fact Tables
CREATE TABLE dwh_fact_product_daily (
  date_sk          INT NOT NULL,
  product_sk       BIGINT NOT NULL,
  platform_sk      INT NOT NULL,
  price_current    DECIMAL(15,2),
  price_original   DECIMAL(15,2),
  discount_pct     DECIMAL(5,2),
  rating_avg       DECIMAL(3,2),
  rating_count     INT,
  review_count     INT,
  sold_count       INT,
  is_available     BOOLEAN DEFAULT TRUE,
  captured_at      TIMESTAMP,
  PRIMARY KEY (date_sk, product_sk, platform_sk),
  FOREIGN KEY (date_sk)     REFERENCES dwh_dim_date(date_sk),
  FOREIGN KEY (product_sk)  REFERENCES dwh_dim_product(product_sk),
  FOREIGN KEY (platform_sk) REFERENCES dwh_dim_platform(platform_sk)
);

CREATE TABLE dwh_fact_review_summary (
  date_sk          INT NOT NULL,
  product_sk       BIGINT NOT NULL,
  platform_sk      INT NOT NULL,
  total_reviews    INT DEFAULT 0,
  avg_rating       DECIMAL(3,2),
  positive_reviews INT DEFAULT 0,
  negative_reviews INT DEFAULT 0,
  neutral_reviews  INT DEFAULT 0,
  sentiment_score  DECIMAL(5,2),
  PRIMARY KEY (date_sk, product_sk, platform_sk),
  FOREIGN KEY (date_sk)     REFERENCES dwh_dim_date(date_sk),
  FOREIGN KEY (product_sk)  REFERENCES dwh_dim_product(product_sk),
  FOREIGN KEY (platform_sk) REFERENCES dwh_dim_platform(platform_sk)
);

-- =====================
-- DATA MART (Business Intelligence Layer)
-- =====================

-- Price Optimization Data Mart (chỉ về price analysis)
CREATE TABLE dm_price_analytics (
  product_sk       BIGINT NOT NULL,
  platform_sk      INT NOT NULL,
  date_sk          INT NOT NULL,
  price_current    DECIMAL(15,2),
  price_original   DECIMAL(15,2),
  discount_pct     DECIMAL(5,2),
  competitor_min_price DECIMAL(15,2),
  competitor_max_price DECIMAL(15,2),
  price_rank       INT,
  price_trend      VARCHAR(20), -- increasing|decreasing|stable
  PRIMARY KEY (product_sk, platform_sk, date_sk),
  FOREIGN KEY (product_sk)  REFERENCES dwh_dim_product(product_sk),
  FOREIGN KEY (platform_sk) REFERENCES dwh_dim_platform(platform_sk),
  FOREIGN KEY (date_sk)     REFERENCES dwh_dim_date(date_sk)
);

-- =====================
-- METADATA (technical + business)
-- =====================
CREATE TABLE meta_source_system (
  source_id     BIGSERIAL PRIMARY KEY,
  code          VARCHAR(32) UNIQUE NOT NULL,
  name          TEXT,
  owner_contact TEXT
);

CREATE TABLE meta_dataset (
  dataset_id    BIGSERIAL PRIMARY KEY,
  source_id     BIGINT,
  layer         VARCHAR(16) NOT NULL,     -- stg|ods|dwh|dm|external
  schema_name   TEXT NOT NULL,            -- chỉ để mô tả
  table_name    TEXT NOT NULL,
  dataset_type  VARCHAR(24) NOT NULL,     -- table|view|file|topic
  pii_class     VARCHAR(24),
  retention_days INT,
  created_at     TIMESTAMP,
  updated_at     TIMESTAMP,
  UNIQUE (schema_name, table_name),
  FOREIGN KEY (source_id) REFERENCES meta_source_system(source_id)
);

CREATE TABLE meta_column (
  column_id       BIGSERIAL PRIMARY KEY,
  dataset_id      BIGINT NOT NULL,
  column_name     TEXT NOT NULL,
  data_type       TEXT,
  nullable        BOOLEAN,
  description     TEXT,
  is_business_key BOOLEAN DEFAULT FALSE,
  is_surrogate_key BOOLEAN DEFAULT FALSE,
  FOREIGN KEY (dataset_id) REFERENCES meta_dataset(dataset_id)
);

CREATE TABLE meta_job (
  job_id    BIGSERIAL PRIMARY KEY,
  job_name  TEXT UNIQUE NOT NULL,
  owner     TEXT,
  schedule  TEXT,
  active    BOOLEAN DEFAULT TRUE
);

CREATE TABLE meta_job_run (
  run_id      BIGSERIAL PRIMARY KEY,
  job_id      BIGINT NOT NULL,
  started_at  TIMESTAMP,
  ended_at    TIMESTAMP,
  status      VARCHAR(16) NOT NULL,       -- success|failed|running
  rows_in     BIGINT,
  rows_out    BIGINT,
  error_message TEXT,
  FOREIGN KEY (job_id) REFERENCES meta_job(job_id)
);

CREATE TABLE meta_lineage_edge (
  edge_id         BIGSERIAL PRIMARY KEY,
  run_id          BIGINT,
  src_dataset_id  BIGINT,
  tgt_dataset_id  BIGINT,
  FOREIGN KEY (run_id)         REFERENCES meta_job_run(run_id),
  FOREIGN KEY (src_dataset_id) REFERENCES meta_dataset(dataset_id),
  FOREIGN KEY (tgt_dataset_id) REFERENCES meta_dataset(dataset_id)
);

CREATE TABLE meta_partition (
  partition_id    BIGSERIAL PRIMARY KEY,
  dataset_id      BIGINT NOT NULL,
  partition_name  TEXT NOT NULL,
  partition_value TEXT NOT NULL,
  row_count       BIGINT,
  size_mb         DECIMAL(18,2),
  last_loaded_at  TIMESTAMP,
  FOREIGN KEY (dataset_id) REFERENCES meta_dataset(dataset_id)
);

CREATE TABLE meta_expectation (
  exp_id     BIGSERIAL PRIMARY KEY,
  dataset_id BIGINT NOT NULL,
  name       TEXT NOT NULL,
  severity   VARCHAR(8) NOT NULL,         -- warn|error
  check_sql  TEXT NOT NULL,
  owner      TEXT,
  tags       TEXT,
  FOREIGN KEY (dataset_id) REFERENCES meta_dataset(dataset_id)
);

CREATE TABLE meta_expectation_result (
  result_id    BIGSERIAL PRIMARY KEY,
  exp_id       BIGINT NOT NULL,
  run_id       BIGINT,
  status       VARCHAR(8) NOT NULL,       -- pass|fail
  failed_rows  BIGINT,
  sample_rows_json TEXT,
  FOREIGN KEY (exp_id) REFERENCES meta_expectation(exp_id),
  FOREIGN KEY (run_id) REFERENCES meta_job_run(run_id)
);

CREATE TABLE meta_business_term (
  term_id   BIGSERIAL PRIMARY KEY,
  term_name TEXT UNIQUE NOT NULL,
  definition TEXT,
  steward   TEXT,
  status    VARCHAR(12)
);

CREATE TABLE meta_term_mapping (
  map_id     BIGSERIAL PRIMARY KEY,
  term_id    BIGINT NOT NULL,
  dataset_id BIGINT NOT NULL,
  column_name TEXT NOT NULL,
  FOREIGN KEY (term_id)   REFERENCES meta_business_term(term_id),
  FOREIGN KEY (dataset_id) REFERENCES meta_dataset(dataset_id)
);
