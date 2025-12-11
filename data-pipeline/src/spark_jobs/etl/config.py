# src/spark_jobs/etl/config.py

"""
Config & constants for ETL pipeline.

WHAT: Tập trung toàn bộ config (ENV, đường dẫn, bucket, flags) và constants (CATEGORY_MAPPINGS).
WHY: Dễ quản lý, không lặp lại ở nhiều file. Khi đổi host MinIO hay thêm mapping chỉ sửa tại đây.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ==== Schema Names ====
DWH_SCHEMA = os.getenv("DWH_SCHEMA", "dwh")
ML_SCHEMA = os.getenv("ML_SCHEMA", "ml")

# ==== MinIO / S3 config ====
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

S3_ENDPOINT = f"http://{MINIO_HOST}" if not MINIO_SECURE else f"https://{MINIO_HOST}"
S3_ACCESS_KEY = MINIO_ACCESS_KEY
S3_SECRET_KEY = MINIO_SECRET_KEY

# ==== Crawler outputs & cleaned paths ====
CRAWLER_OUTPUT_DIR = os.getenv("CRAWLER_OUTPUT_DIR", "/app/data/outputs")
MINIO_CLEANED_BUCKET = os.getenv("MINIO_CLEANED_BUCKET", "cleaned-data")
MINIO_PROCESSED_REVIEWS_BUCKET = os.getenv(
    "MINIO_PROCESSED_REVIEWS_BUCKET", "processed-reviews"
)
SAVE_TO_MINIO = os.getenv("SAVE_TO_MINIO", "true").lower() == "true"

# ==== Feature flags ====
PROCESS_REVIEWS = os.getenv("PROCESS_REVIEWS", "false").lower() == "true"

# ==== Postgres / Data Warehouse ====
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_dss")
DB_USER = os.getenv("DB_USER", "dss_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "dss_password_123")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ==== Category mapping constants ====
CATEGORY_MAPPINGS = [
    # ============================================================
    # PRIORITY 1: PLURAL FORMS (Critical for Lazada compatibility)
    # ============================================================
    
    # Headphones & Earphones - SPECIFIC FIRST, GENERAL LAST
    ("wireless earbuds", "Electronics|Audio|Headphones"),
    ("true wireless earbuds", "Electronics|Audio|Headphones"),
    ("galaxy buds", "Electronics|Audio|Headphones"),
    ("airpods pro", "Electronics|Audio|Headphones"),
    ("airpods", "Electronics|Audio|Headphones"),
    ("tai nghe không dây", "Electronics|Audio|Headphones"),
    ("tai nghe máy tính", "Electronics|Audio|Headphones"),
    ("tai nghe", "Electronics|Audio|Headphones"),
    ("headphones", "Electronics|Audio|Headphones"),
    ("earphones", "Electronics|Audio|Headphones"),
    ("earphone", "Electronics|Audio|Headphones"),
    
    # Speakers
    ("bluetooth speakers", "Electronics|Audio|Speakers"),
    ("bluetooth speaker", "Electronics|Audio|Speakers"),
    ("loa bluetooth", "Electronics|Audio|Speakers"),
    ("speakers", "Electronics|Audio|Speakers"),
    ("speaker", "Electronics|Audio|Speakers"),
    ("loa", "Electronics|Audio|Speakers"),
    
    # Laptops - SPECIFIC FIRST
    ("gaming laptop", "Electronics|Computers|Laptops"),
    ("gaming laptops", "Electronics|Computers|Laptops"),
    ("máy tính xách tay", "Electronics|Computers|Laptops"),
    ("macbook pro", "Electronics|Computers|Laptops"),
    ("macbook air", "Electronics|Computers|Laptops"),
    ("macbook", "Electronics|Computers|Laptops"),
    ("chromebook", "Electronics|Computers|Laptops"),
    ("ultrabook", "Electronics|Computers|Laptops"),
    ("notebook", "Electronics|Computers|Laptops"),
    ("laptops", "Electronics|Computers|Laptops"),
    ("laptop", "Electronics|Computers|Laptops"),
    
    # Smartwatches
    ("đồng hồ thông minh", "Electronics|Wearables|Smartwatches"),
    ("apple watch", "Electronics|Wearables|Smartwatches"),
    ("galaxy watch", "Electronics|Wearables|Smartwatches"),
    ("smart watch", "Electronics|Wearables|Smartwatches"),
    ("smartwatches", "Electronics|Wearables|Smartwatches"),
    ("smartwatch", "Electronics|Wearables|Smartwatches"),
    ("wearable devices", "Electronics|Wearables|Smartwatches"),
    
    # Tablets
    ("máy tính bảng", "Electronics|Tablets"),
    ("galaxy tab", "Electronics|Tablets"),
    ("ipad pro", "Electronics|Tablets"),
    ("ipad air", "Electronics|Tablets"),
    ("ipad", "Electronics|Tablets"),
    ("kindle", "Electronics|Tablets"),
    ("tablets", "Electronics|Tablets"),
    ("tablet", "Electronics|Tablets"),
    
    # Keyboards - SPECIFIC FIRST
    ("bàn phím máy tính", "Electronics|Computers|Accessories|Keyboard"),
    ("mechanical keyboard", "Electronics|Computers|Accessories|Keyboard"),
    ("gaming keyboard", "Electronics|Computers|Accessories|Keyboard"),
    ("bàn phím cơ", "Electronics|Computers|Accessories|Keyboard"),
    ("bàn phím", "Electronics|Computers|Accessories|Keyboard"),
    ("keyboards", "Electronics|Computers|Accessories|Keyboard"),
    ("keyboard", "Electronics|Computers|Accessories|Keyboard"),
    
    # Mouse
    ("chuột không dây", "Electronics|Computers|Accessories|Mouse"),
    ("chuột máy tính", "Electronics|Computers|Accessories|Mouse"),
    ("gaming mouse", "Electronics|Computers|Accessories|Mouse"),
    ("wireless mouse", "Electronics|Computers|Accessories|Mouse"),
    ("mice", "Electronics|Computers|Accessories|Mouse"),
    ("mouse", "Electronics|Computers|Accessories|Mouse"),
    
    # Monitors
    ("màn hình máy tính", "Electronics|Computers|Monitors"),
    ("gaming monitor", "Electronics|Computers|Monitors"),
    ("monitors", "Electronics|Computers|Monitors"),
    ("monitor", "Electronics|Computers|Monitors"),
    ("display", "Electronics|Computers|Monitors"),
    
    # Cameras
    ("action camera", "Electronics|Cameras"),
    ("digital camera", "Electronics|Cameras"),
    ("máy ảnh kỹ thuật số", "Electronics|Cameras"),
    ("máy ảnh", "Electronics|Cameras"),
    ("mirrorless", "Electronics|Cameras"),
    ("gopro", "Electronics|Cameras"),
    ("dslr", "Electronics|Cameras"),
    ("cameras", "Electronics|Cameras"),
    ("camera", "Electronics|Cameras"),
    
    # Printers & Scanners
    ("laser printer", "Electronics|Computers|Printers"),
    ("máy in", "Electronics|Computers|Printers"),
    ("inkjet", "Electronics|Computers|Printers"),
    ("printers", "Electronics|Computers|Printers"),
    ("printer", "Electronics|Computers|Printers"),
    ("máy quét", "Electronics|Computers|Scanners"),
    ("scanner", "Electronics|Computers|Scanners"),
    
    # Desktop PCs
    ("máy tính để bàn", "Electronics|Computers|Desktop"),
    ("gaming pc", "Electronics|Computers|Desktop"),
    ("mac mini", "Electronics|Computers|Desktop"),
    ("imac", "Electronics|Computers|Desktop"),
    ("desktop", "Electronics|Computers|Desktop"),
    ("pc", "Electronics|Computers|Desktop"),
    
    # Storage & Accessories
    ("ổ cứng di động", "Electronics|Computers|Storage"),
    ("ổ cứng", "Electronics|Computers|Storage"),
    ("hard drive", "Electronics|Computers|Storage"),
    ("ssd", "Electronics|Computers|Storage"),
    ("usb flash drive", "Electronics|Computers|Accessories|USB"),
    ("flash drive", "Electronics|Computers|Accessories|USB"),
    
    # Smartphones - SPECIFIC FIRST
    ("điện thoại thông minh", "Electronics|Mobile Phones|Smartphones"),
    ("samsung galaxy s", "Electronics|Mobile Phones|Smartphones"),
    ("samsung galaxy", "Electronics|Mobile Phones|Smartphones"),
    ("mobile phone", "Electronics|Mobile Phones|Smartphones"),
    ("điện thoại", "Electronics|Mobile Phones|Smartphones"),
    ("iphone", "Electronics|Mobile Phones|Smartphones"),
    ("smartphones", "Electronics|Mobile Phones|Smartphones"),
    ("smartphone", "Electronics|Mobile Phones|Smartphones"),
    ("xiaomi", "Electronics|Mobile Phones|Smartphones"),
    ("oppo", "Electronics|Mobile Phones|Smartphones"),
    ("vivo", "Electronics|Mobile Phones|Smartphones"),
    ("realme", "Electronics|Mobile Phones|Smartphones"),
    
    # Mobile Accessories
    ("sạc dự phòng", "Electronics|Mobile Phones|Accessories|PowerBank"),
    ("power bank", "Electronics|Mobile Phones|Accessories|PowerBank"),
    ("ốp lưng điện thoại", "Electronics|Mobile Phones|Accessories|Case"),
    ("ốp lưng", "Electronics|Mobile Phones|Accessories|Case"),
    ("phone case", "Electronics|Mobile Phones|Accessories|Case"),
    ("miếng dán màn hình", "Electronics|Mobile Phones|Accessories|ScreenProtector"),
    ("miếng dán", "Electronics|Mobile Phones|Accessories|ScreenProtector"),
    ("cáp sạc", "Electronics|Mobile Phones|Accessories|Cable"),
    ("charging cable", "Electronics|Mobile Phones|Accessories|Cable"),
    
    # Networking
    ("router wifi", "Electronics|Networking|Router"),
    ("access point", "Electronics|Networking|Access Points"),
    ("routers", "Electronics|Networking|Router"),
    ("router", "Electronics|Networking|Router"),
    ("modem", "Electronics|Networking|Modem"),
    
    # TVs & Accessories
    ("android tv", "Electronics|TVs|Smart TVs"),
    ("tivi smart", "Electronics|TVs|Smart TVs"),
    ("smart tv", "Electronics|TVs|Smart TVs"),
    ("apple tv", "Electronics|TVs|Smart TVs"),
    ("fire tv", "Electronics|TVs|Smart TVs"),
    ("television", "Electronics|TVs|Smart TVs"),
    ("tivi", "Electronics|TVs|Smart TVs"),
    ("máy chiếu", "Electronics|TVs|Projectors"),
    ("projector", "Electronics|TVs|Projectors"),
    
    # ============================================================
    # PRIORITY 2: LAZADA GENERIC CATEGORIES (Fallback)
    # ============================================================
    ("computer peripherals", "Electronics|Computers|Accessories|Mouse"),
    ("computer accessories", "Electronics|Computers|Accessories|Mouse"),
    ("gaming accessories", "Electronics|Computers|Accessories|Mouse"),
    ("mobile accessories", "Electronics|Mobile Phones|Accessories|Case"),
    ("audio accessories", "Electronics|Audio|Headphones"),
    ("tv accessories", "Electronics|TVs|Smart TVs"),
]

# ============================================================
#  STAR SCHEMA DDL TEMPLATE (DWH + ML)
# ============================================================
STAR_SCHEMA_SQL_TEMPLATE = """
-- ========= SCHEMAS =========
CREATE SCHEMA IF NOT EXISTS {dwh};
CREATE SCHEMA IF NOT EXISTS {ml};

-- ========= DIMENSIONS =========

-- 1. dim_date
CREATE TABLE IF NOT EXISTS {dwh}.dim_date (
    date_sk      SERIAL PRIMARY KEY,
    date_value   DATE NOT NULL UNIQUE,
    year         INT  NOT NULL,
    month        INT  NOT NULL,
    day          INT  NOT NULL,
    quarter      INT  NOT NULL,
    week_of_year INT  NOT NULL,
    day_of_week  INT  NOT NULL,
    day_name     VARCHAR(10),
    is_weekend   BOOLEAN NOT NULL
);

-- 2. dim_platform
CREATE TABLE IF NOT EXISTS {dwh}.dim_platform (
    platform_sk    SERIAL PRIMARY KEY,
    platform_code  VARCHAR(50) NOT NULL UNIQUE,
    platform_name  VARCHAR(100),
    country_code   CHAR(2),
    base_url       VARCHAR(255),
    is_active      BOOLEAN DEFAULT TRUE,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

-- 3. dim_brand
CREATE TABLE IF NOT EXISTS {dwh}.dim_brand (
    brand_sk         SERIAL PRIMARY KEY,
    brand_name       VARCHAR(150) NOT NULL UNIQUE,
    brand_normalized VARCHAR(150),
    country          VARCHAR(100),
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- 4. dim_category
CREATE TABLE IF NOT EXISTS {dwh}.dim_category (
    category_sk       SERIAL PRIMARY KEY,
    category_std_key  VARCHAR(100) NOT NULL UNIQUE,
    category_lvl1     VARCHAR(100),
    category_lvl2     VARCHAR(100),
    category_lvl3     VARCHAR(100),
    full_path         VARCHAR(400),
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- 5. dim_product
CREATE TABLE IF NOT EXISTS {dwh}.dim_product (
    product_sk        SERIAL PRIMARY KEY,
    product_key       VARCHAR(100) NOT NULL UNIQUE,
    product_master_id VARCHAR(256),
    product_name      VARCHAR(500),
    product_slug      VARCHAR(500),
    brand_sk          INT REFERENCES {dwh}.dim_brand(brand_sk),
    category_sk       INT REFERENCES {dwh}.dim_category(category_sk),
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ========= FACT TABLES =========

-- 6. fact_product_daily
CREATE TABLE IF NOT EXISTS {dwh}.fact_product_daily (
    date_sk       INT NOT NULL REFERENCES {dwh}.dim_date(date_sk),
    product_sk    INT NOT NULL REFERENCES {dwh}.dim_product(product_sk),
    platform_sk   INT NOT NULL REFERENCES {dwh}.dim_platform(platform_sk),

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
ON {dwh}.fact_product_daily (product_sk, platform_sk, date_sk);

-- 7. fact_review (detail)
CREATE TABLE IF NOT EXISTS {dwh}.fact_review (
    review_sk        BIGSERIAL PRIMARY KEY,
    review_id_nk     VARCHAR(255) NOT NULL,
    product_sk       INT NOT NULL REFERENCES {dwh}.dim_product(product_sk),
    platform_sk      INT NOT NULL REFERENCES {dwh}.dim_platform(platform_sk),
    date_sk          INT NOT NULL REFERENCES {dwh}.dim_date(date_sk),

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
ON {dwh}.fact_review (product_sk, platform_sk, date_sk);

-- 8. fact_review_daily (aggregate)
CREATE TABLE IF NOT EXISTS {dwh}.fact_review_daily (
    date_sk      INT NOT NULL REFERENCES {dwh}.dim_date(date_sk),
    product_sk   INT NOT NULL REFERENCES {dwh}.dim_product(product_sk),
    platform_sk  INT NOT NULL REFERENCES {dwh}.dim_platform(platform_sk),

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
ON {dwh}.fact_review_daily (product_sk, platform_sk, date_sk);

-- ========= ML TABLES =========

-- 9. dim_ml_model
CREATE TABLE IF NOT EXISTS {ml}.dim_ml_model (
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
CREATE TABLE IF NOT EXISTS {ml}.fact_price_prediction (
    prediction_sk   BIGSERIAL PRIMARY KEY,
    model_sk        INT NOT NULL REFERENCES {ml}.dim_ml_model(model_sk),
    date_sk         INT NOT NULL REFERENCES {dwh}.dim_date(date_sk),
    product_sk      INT NOT NULL REFERENCES {dwh}.dim_product(product_sk),
    platform_sk     INT NOT NULL REFERENCES {dwh}.dim_platform(platform_sk),

    predicted_price NUMERIC(15,2) NOT NULL,
    ci_lower        NUMERIC(15,2),
    ci_upper        NUMERIC(15,2),

    run_id          VARCHAR(100),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_price_pred_prod_plat_date
ON {ml}.fact_price_prediction (product_sk, platform_sk, date_sk);

-- 11. fact_product_recommendation
CREATE TABLE IF NOT EXISTS {ml}.fact_product_recommendation (
    recommendation_sk      BIGSERIAL PRIMARY KEY,
    model_sk               INT NOT NULL REFERENCES {ml}.dim_ml_model(model_sk),
    date_sk                INT NOT NULL REFERENCES {dwh}.dim_date(date_sk),

    source_product_sk      INT NOT NULL REFERENCES {dwh}.dim_product(product_sk),
    recommended_product_sk INT NOT NULL REFERENCES {dwh}.dim_product(product_sk),
    rank                   INT NOT NULL,
    similarity_score       NUMERIC(5,4),
    recommendation_type    VARCHAR(50),
    created_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rec_source_prod_date
ON {ml}.fact_product_recommendation (source_product_sk, date_sk, rank);
"""
