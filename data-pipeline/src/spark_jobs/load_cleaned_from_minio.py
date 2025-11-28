import os
import sys
import glob
import re
from datetime import datetime, timedelta
import pandas as pd

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from unidecode import unidecode
from pyspark.sql.functions import (
    col,
    when,
    regexp_replace,
    trim,
    concat,
    lit,
    lower,
    concat_ws,
    coalesce,
    upper,
    to_timestamp,
    sha2,
    split,
    element_at,
    to_date,
    countDistinct,
    avg,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    count,
    year,
    month,
    dayofmonth,
    dayofweek,
    row_number,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType, StringType, BooleanType
from pyspark.sql.functions import udf
from pyspark.sql import functions as F

from psycopg2.extras import execute_batch
import psycopg2

try:
    from textblob import TextBlob
except ImportError:
    TextBlob = None

load_dotenv()

DWH_SCHEMA = os.getenv("DWH_SCHEMA", "dwh")
ML_SCHEMA = os.getenv("ML_SCHEMA", "ml")

# Ép stdout dùng UTF-8 trên Windows
if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# --------------------------
# MinIO Configuration
# --------------------------
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

S3_ENDPOINT = f"http://{MINIO_HOST}" if not MINIO_SECURE else f"https://{MINIO_HOST}"
S3_ACCESS_KEY = MINIO_ACCESS_KEY
S3_SECRET_KEY = MINIO_SECRET_KEY

CRAWLER_OUTPUT_DIR = os.getenv("CRAWLER_OUTPUT_DIR", "/app/data/outputs")
MINIO_CLEANED_BUCKET = os.getenv("MINIO_CLEANED_BUCKET", "cleaned-data")
MINIO_PROCESSED_REVIEWS_BUCKET = os.getenv(
    "MINIO_PROCESSED_REVIEWS_BUCKET", "processed-reviews"
)
SAVE_TO_MINIO = os.getenv("SAVE_TO_MINIO", "true").lower() == "true"

# --------------------------
# Postgres / Data Warehouse
# --------------------------
DB_HOST = os.getenv("DB_HOST", "dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_dss_1")
DB_USER = os.getenv("DB_USER", "dss_user")
DB_PASSWORD = os.getenv(
    "DB_PASSWORD", "6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G"
)  # TODO: đổi khi lên prod

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ============================================================
#  Category mapping config
# ============================================================
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
    ("earphones", "Electronics|Audio|Headphones"),  # ✅ NEW: Plural
    ("earphone", "Electronics|Audio|Headphones"),
    
    # Speakers
    ("bluetooth speakers", "Electronics|Audio|Speakers"),  # ✅ NEW: Plural
    ("bluetooth speaker", "Electronics|Audio|Speakers"),
    ("loa bluetooth", "Electronics|Audio|Speakers"),
    ("speakers", "Electronics|Audio|Speakers"),  # ✅ NEW: Plural
    ("speaker", "Electronics|Audio|Speakers"),
    ("loa", "Electronics|Audio|Speakers"),
    
    # Laptops - SPECIFIC FIRST
    ("gaming laptop", "Electronics|Computers|Laptops"),  # ✅ NEW
    ("gaming laptops", "Electronics|Computers|Laptops"),  # ✅ NEW
    ("máy tính xách tay", "Electronics|Computers|Laptops"),
    ("macbook pro", "Electronics|Computers|Laptops"),  # ✅ NEW
    ("macbook air", "Electronics|Computers|Laptops"),  # ✅ NEW
    ("macbook", "Electronics|Computers|Laptops"),
    ("chromebook", "Electronics|Computers|Laptops"),  # ✅ NEW
    ("ultrabook", "Electronics|Computers|Laptops"),
    ("notebook", "Electronics|Computers|Laptops"),
    ("laptops", "Electronics|Computers|Laptops"),  # ✅ NEW: Plural - CRITICAL!
    ("laptop", "Electronics|Computers|Laptops"),
    
    # Smartwatches
    ("đồng hồ thông minh", "Electronics|Wearables|Smartwatches"),
    ("apple watch", "Electronics|Wearables|Smartwatches"),
    ("galaxy watch", "Electronics|Wearables|Smartwatches"),
    ("smart watch", "Electronics|Wearables|Smartwatches"),
    ("smartwatches", "Electronics|Wearables|Smartwatches"),  # ✅ NEW: Plural
    ("smartwatch", "Electronics|Wearables|Smartwatches"),
    ("wearable devices", "Electronics|Wearables|Smartwatches"),  # ✅ NEW: Lazada generic
    
    # Tablets
    ("máy tính bảng", "Electronics|Tablets"),
    ("galaxy tab", "Electronics|Tablets"),
    ("ipad pro", "Electronics|Tablets"),  # ✅ NEW
    ("ipad air", "Electronics|Tablets"),  # ✅ NEW
    ("ipad", "Electronics|Tablets"),
    ("kindle", "Electronics|Tablets"),  # ✅ NEW
    ("tablets", "Electronics|Tablets"),  # ✅ NEW: Plural
    ("tablet", "Electronics|Tablets"),
    
    # Keyboards - SPECIFIC FIRST
    ("bàn phím máy tính", "Electronics|Computers|Accessories|Keyboard"),  # ✅ NEW
    ("mechanical keyboard", "Electronics|Computers|Accessories|Keyboard"),
    ("gaming keyboard", "Electronics|Computers|Accessories|Keyboard"),
    ("bàn phím cơ", "Electronics|Computers|Accessories|Keyboard"),
    ("bàn phím", "Electronics|Computers|Accessories|Keyboard"),
    ("keyboards", "Electronics|Computers|Accessories|Keyboard"),  # ✅ NEW: Plural
    ("keyboard", "Electronics|Computers|Accessories|Keyboard"),
    
    # Mouse
    ("chuột không dây", "Electronics|Computers|Accessories|Mouse"),  # ✅ NEW
    ("chuột máy tính", "Electronics|Computers|Accessories|Mouse"),
    ("gaming mouse", "Electronics|Computers|Accessories|Mouse"),
    ("wireless mouse", "Electronics|Computers|Accessories|Mouse"),
    ("mice", "Electronics|Computers|Accessories|Mouse"),  # ✅ NEW: Plural
    ("mouse", "Electronics|Computers|Accessories|Mouse"),
    
    # Monitors
    ("màn hình máy tính", "Electronics|Computers|Monitors"),
    ("gaming monitor", "Electronics|Computers|Monitors"),
    ("monitors", "Electronics|Computers|Monitors"),  # ✅ NEW: Plural
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
    ("cameras", "Electronics|Cameras"),  # ✅ NEW: Plural
    ("camera", "Electronics|Cameras"),
    
    # Printers & Scanners
    ("laser printer", "Electronics|Computers|Printers"),
    ("máy in", "Electronics|Computers|Printers"),
    ("inkjet", "Electronics|Computers|Printers"),
    ("printers", "Electronics|Computers|Printers"),  # ✅ NEW: Plural
    ("printer", "Electronics|Computers|Printers"),
    ("máy quét", "Electronics|Computers|Scanners"),  # ✅ NEW
    ("scanner", "Electronics|Computers|Scanners"),  # ✅ NEW
    
    # Desktop PCs
    ("máy tính để bàn", "Electronics|Computers|Desktop"),
    ("gaming pc", "Electronics|Computers|Desktop"),
    ("mac mini", "Electronics|Computers|Desktop"),  # ✅ NEW
    ("imac", "Electronics|Computers|Desktop"),  # ✅ NEW
    ("desktop", "Electronics|Computers|Desktop"),
    ("pc", "Electronics|Computers|Desktop"),
    
    # Storage & Accessories
    ("ổ cứng di động", "Electronics|Computers|Storage"),  # ✅ NEW
    ("ổ cứng", "Electronics|Computers|Storage"),  # ✅ NEW
    ("hard drive", "Electronics|Computers|Storage"),  # ✅ NEW
    ("ssd", "Electronics|Computers|Storage"),  # ✅ NEW
    ("usb flash drive", "Electronics|Computers|Accessories|USB"),  # ✅ NEW
    ("flash drive", "Electronics|Computers|Accessories|USB"),  # ✅ NEW
    
    # Smartphones - SPECIFIC FIRST (place after other specific keywords)
    ("điện thoại thông minh", "Electronics|Mobile Phones|Smartphones"),
    ("samsung galaxy s", "Electronics|Mobile Phones|Smartphones"),  # ✅ NEW
    ("samsung galaxy", "Electronics|Mobile Phones|Smartphones"),
    ("mobile phone", "Electronics|Mobile Phones|Smartphones"),
    ("điện thoại", "Electronics|Mobile Phones|Smartphones"),
    ("iphone", "Electronics|Mobile Phones|Smartphones"),
    ("smartphones", "Electronics|Mobile Phones|Smartphones"),  # ✅ NEW: Plural
    ("smartphone", "Electronics|Mobile Phones|Smartphones"),
    ("xiaomi", "Electronics|Mobile Phones|Smartphones"),
    ("oppo", "Electronics|Mobile Phones|Smartphones"),
    ("vivo", "Electronics|Mobile Phones|Smartphones"),
    ("realme", "Electronics|Mobile Phones|Smartphones"),
    
    # Mobile Accessories
    ("sạc dự phòng", "Electronics|Mobile Phones|Accessories|PowerBank"),  # ✅ NEW
    ("power bank", "Electronics|Mobile Phones|Accessories|PowerBank"),  # ✅ NEW
    ("ốp lưng điện thoại", "Electronics|Mobile Phones|Accessories|Case"),  # ✅ NEW
    ("ốp lưng", "Electronics|Mobile Phones|Accessories|Case"),  # ✅ NEW
    ("phone case", "Electronics|Mobile Phones|Accessories|Case"),  # ✅ NEW
    ("miếng dán màn hình", "Electronics|Mobile Phones|Accessories|ScreenProtector"),  # ✅ NEW
    ("miếng dán", "Electronics|Mobile Phones|Accessories|ScreenProtector"),  # ✅ NEW
    ("cáp sạc", "Electronics|Mobile Phones|Accessories|Cable"),  # ✅ NEW
    ("charging cable", "Electronics|Mobile Phones|Accessories|Cable"),  # ✅ NEW
    
    # Networking
    ("router wifi", "Electronics|Networking|Router"),
    ("access point", "Electronics|Networking|Access Points"),
    ("routers", "Electronics|Networking|Router"),  # ✅ NEW: Plural
    ("router", "Electronics|Networking|Router"),
    ("modem", "Electronics|Networking|Modem"),
    
    # TVs & Accessories
    ("android tv", "Electronics|TVs|Smart TVs"),
    ("tivi smart", "Electronics|TVs|Smart TVs"),
    ("smart tv", "Electronics|TVs|Smart TVs"),
    ("apple tv", "Electronics|TVs|Smart TVs"),  # ✅ NEW
    ("fire tv", "Electronics|TVs|Smart TVs"),  # ✅ NEW
    ("television", "Electronics|TVs|Smart TVs"),
    ("tivi", "Electronics|TVs|Smart TVs"),
    ("máy chiếu", "Electronics|TVs|Projectors"),  # ✅ NEW
    ("projector", "Electronics|TVs|Projectors"),  # ✅ NEW
    
    # ============================================================
    # PRIORITY 2: LAZADA GENERIC CATEGORIES (Fallback)
    # ============================================================
    ("computer peripherals", "Electronics|Computers|Accessories|Mouse"),  # ✅ NEW
    ("computer accessories", "Electronics|Computers|Accessories|Mouse"),  # ✅ NEW
    ("gaming accessories", "Electronics|Computers|Accessories|Mouse"),  # ✅ NEW
    ("mobile accessories", "Electronics|Mobile Phones|Accessories|Case"),  # ✅ NEW
    ("audio accessories", "Electronics|Audio|Headphones"),  # ✅ NEW
    ("tv accessories", "Electronics|TVs|Smart TVs"),  # ✅ NEW
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


def ensure_star_schema(conn):
    """Tạo đầy đủ schema/bảng DWH + ML nếu chưa có."""
    ddl = STAR_SCHEMA_SQL_TEMPLATE.format(dwh=DWH_SCHEMA, ml=ML_SCHEMA)
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print("[INFO] DWH star schema ensured.")


# ============================================================
#  Spark Session
# ============================================================
def create_spark_session():
    print("[INFO] Creating Spark session...")

    if os.name == "nt":
        os.environ["HADOOP_HOME"] = os.environ.get("HADOOP_HOME", "C:/hadoop")
        os.environ["HADOOP_OPTS"] = (
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false"
        )

    spark = (
        SparkSession.builder.appName("EcommerceDSS-FullPipeline")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(MINIO_SECURE).lower(),
        )
        # Adaptive, timezone, datetime parser
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.debug.maxToStringFields", "100")
        # Fix nativeIO trên Windows / container
        .config(
            "spark.driver.extraJavaOptions",
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print(" Spark session created")
    return spark


# ============================================================
#  STEP 1 – Load raw data
# ============================================================
def load_raw_data(spark):
    print("\n" + "=" * 60)
    print(" STEP 1: LOADING RAW DATA")
    print("=" * 60)

    try:
        local_data_path = "/app/data/crawler_output"
        local_files = glob.glob(f"{local_data_path}/**/*.jsonl", recursive=True)
        if not local_files:
            local_files = glob.glob("/app/data/**/*.jsonl", recursive=True)

        if not local_files:
            print(f"\n No JSONL files found in {local_data_path}")
            print("Please ensure JSONL files are uploaded to:")
            print(f"  {local_data_path}")
            return None

        print(f"\n[INFO] Found {len(local_files)} local JSONL files")
        for f in local_files[:5]:
            print(f"   {f}")
        if len(local_files) > 5:
            print(f"   ... and {len(local_files) - 5} more")

        try:
            print("\n[INFO] Attempting to load with schema inference...")
            df = (
                spark.read.option("inferSchema", "true")
                .option("multiline", "false")
                .json(local_files)
            )
            count = df.count()
            print(f" Loaded {count:,} raw records")
            print(f"   Schema: {len(df.columns)} columns")
            df.printSchema()
            return df

        except Exception as e:
            print(f"  Schema inference failed: {e}")
            print("[INFO] Attempting without schema...")
            df = spark.read.option("multiline", "false").json(local_files)
            count = df.count()
            print(f" Loaded {count:,} raw records")
            print(f"   Schema: {len(df.columns)} columns")
            return df

    except Exception as e:
        print(f" Failed to load raw data: {e}")
        import traceback

        traceback.print_exc()
        return None


# ============================================================
#  STEP 2 – Cleaning & Transforming
# ============================================================
def clean_data(df):
    print("\n" + "=" * 60)
    print(" STEP 2: CLEANING & TRANSFORMING DATA")
    print("=" * 60)
    try:
        # ===== Đảm bảo product_id tồn tại dưới dạng string =====
        if "product_id" in df.columns:
            df = df.withColumn("product_id", col("product_id").cast(StringType()))
        else:
            # trường hợp hiếm nhưng để phòng thủ
            df = df.withColumn("product_id", lit(None).cast(StringType()))

        # ===== Tạo global_product_id & chuẩn hóa platform =====
        df_cleaned = (
            df
            .withColumn(
                "global_product_id",
                concat(col("source"), lit("_"), col("product_id"))
            )
            .withColumn("source_platform", col("source"))
        )

        # ===== Chuẩn hóa product_name =====
        df_cleaned = df_cleaned.withColumn(
            "product_name",
            when(
                (col("product_name").isNotNull()) & (trim(col("product_name")) != ""),
                trim(col("product_name")),
            ).otherwise(lit("Unknown"))
        )

        # ======================================================
        # 🔥 SỬA LỖI LỚN NHẤT: brand_name CHỈ XUẤT HIỆN NẾU CÓ
        # ======================================================

        if "brand_name" in df_cleaned.columns and "brand" in df_cleaned.columns:
            # Nếu cả hai đều có (trường hợp multi-source)
            df_cleaned = df_cleaned.withColumn(
                "brand_name",
                when(
                    (col("brand").isNotNull()) & (trim(col("brand")) != ""),
                    trim(col("brand"))
                )
                .when(
                    (col("brand_name").isNotNull()) & (trim(col("brand_name")) != ""),
                    trim(col("brand_name"))
                )
                .otherwise(lit("Unknown"))
            )
        elif "brand" in df_cleaned.columns:
            # Chỉ có brand (case Lazada)
            df_cleaned = df_cleaned.withColumn(
                "brand_name",
                when(
                    (col("brand").isNotNull()) & (trim(col("brand")) != ""),
                    trim(col("brand"))
                ).otherwise(lit("Unknown"))
            )
        elif "brand_name" in df_cleaned.columns:
            # Chỉ có brand_name (case Tiki cũ)
            df_cleaned = df_cleaned.withColumn(
                "brand_name",
                when(
                    (col("brand_name").isNotNull()) & (trim(col("brand_name")) != ""),
                    trim(col("brand_name"))
                ).otherwise(lit("Unknown"))
            )
        else:
            # Không có cột brand nào → tạo Unknown
            df_cleaned = df_cleaned.withColumn("brand_name", lit("Unknown"))

        # Chuẩn hóa brand_std
        df_cleaned = df_cleaned.withColumn(
            "brand_std",
            upper(trim(col("brand_name")))
        )

        # ===== Chuẩn hóa giá =====
        df_cleaned = df_cleaned.withColumn(
            "price_current",
            when(
                col("price_current").isNotNull(),
                regexp_replace(col("price_current"), "[^0-9]", "").cast(LongType())
            ).otherwise(lit(0))
        )

        df_cleaned = df_cleaned.withColumn(
            "price_original",
            when(
                col("price_original").isNotNull(),
                regexp_replace(col("price_original"), "[^0-9]", "").cast(LongType())
            ).otherwise(lit(0))
        )

        # ===== Chuẩn hóa discount_percent =====
        if "discount_percent" in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn(
                "discount_percent",
                when(
                    col("discount_percent").isNotNull(),
                    regexp_replace(col("discount_percent"), "[^0-9.]", "").cast(DoubleType())
                ).otherwise(lit(0.0))
            )
        else:
            df_cleaned = df_cleaned.withColumn("discount_percent", lit(0.0))

        # ===== data_quality_score =====
        df_cleaned = df_cleaned.withColumn(
            "data_quality_score",
            when(
                (col("product_name") != "Unknown") & (col("price_current") > 0),
                lit(1.0)
            ).otherwise(lit(0.0))
        )

        # ===== Select các cột thực sự tồn tại =====
        available_cols = df_cleaned.columns
        candidate_cols = [
            "global_product_id",
            "source_platform",
            "product_id",
            "product_name",
            "brand_name",
            "brand_std",
            "category",
            "price_current",
            "price_original",
            "discount_percent",
            "review_count",
            "rating",
            "seller_name",
            "url",
            "crawl_date",
            "data_quality_score",
        ]
        select_cols = [c for c in candidate_cols if c in available_cols]

        df_cleaned = df_cleaned.select(*select_cols)

        print(f" Cleaned {df_cleaned.count():,} records")
        print(f" Columns used: {select_cols}")

        return df_cleaned

    except Exception as e:
        print(f" Error during cleaning: {e}")
        import traceback
        traceback.print_exc()
        return None




# ============================================================
#  STEP 2.5 – Category Mapping
# ============================================================
def map_categories(df):
    print("\n" + "=" * 60)
    print(" STEP 2.5: CATEGORY MAPPING (using mapping table)")
    print("=" * 60)

    # DEBUG: Check sample categories before mapping
    print("\n[DEBUG] Sample raw categories (first 20):")
    sample_cats = df.select("category", "product_name").distinct().limit(20).collect()
    for i, row in enumerate(sample_cats[:20], 1):
        cat = row["category"] if row["category"] else "NULL"
        name = row["product_name"][:50] if row["product_name"] else "NULL"
        print(f"  {i}. Category: '{cat}' | Product: '{name}'")

    # ✅ NEW: Create sorted mapping dict (longest keywords first for better matching)
    print(f"\n[INFO] Total category mappings configured: {len(CATEGORY_MAPPINGS)}")
    mapping_dict = {k.lower(): v for (k, v) in CATEGORY_MAPPINGS}
    print(f"[INFO] Unique category patterns: {len(mapping_dict)}")
    
    # Count plural variants
    plural_count = sum(1 for k in mapping_dict.keys() if k.endswith('s') and k not in ['access', 'wireless'])
    print(f"[INFO] Including {plural_count} plural variants for Lazada compatibility")

    def _map_category_enhanced(category_text: str, product_name: str):
        """
        Try to map using category first, then fallback to product_name
        ✅ IMPROVED: Sort by keyword length (longest first) for best match
        """
        if not category_text and not product_name:
            return None
        
        # ✅ Sort mappings by keyword length DESC for better specificity
        sorted_mappings = sorted(mapping_dict.items(), 
                                key=lambda x: len(x[0]), 
                                reverse=True)
        
        # Try category text first
        if category_text:
            t = category_text.lower().strip()
            for key, path in sorted_mappings:
                if key in t:
                    return path
        
        # Fallback to product name if category didn't match
        if product_name:
            p = product_name.lower().strip()
            for key, path in sorted_mappings:
                if key in p:
                    return path
        
        return None

    map_category_udf = udf(_map_category_enhanced, StringType())

    # Prepare both category and product_name for mapping
    df_mapped = df.withColumn(
        "category_text",
        lower(trim(coalesce(col("category"), lit(""))))
    ).withColumn(
        "product_name_lower",
        lower(trim(coalesce(col("product_name"), lit(""))))
    )

    # ✅ DEBUG: Show before mapping stats
    print("\n[DEBUG] Before mapping - checking for potential matches:")
    before_sample = df_mapped.select("category_text").limit(1000).collect()
    plural_found = sum(1 for row in before_sample if row["category_text"] and row["category_text"].endswith('s'))
    print(f"  Found {plural_found}/1000 categories ending in 's' (likely plural)")

    # Map using both category and product_name
    df_mapped = df_mapped.withColumn(
        "category_path",
        map_category_udf(col("category_text"), col("product_name_lower")),
    ).drop("product_name_lower")

    # tách thành các level
    df_mapped = df_mapped.withColumn(
        "category_array", split(col("category_path"), r"\|")
    )

    df_mapped = (
        df_mapped.withColumn("category_lvl1", col("category_array").getItem(0))
        .withColumn("category_lvl2", col("category_array").getItem(1))
        .withColumn("category_lvl3", col("category_array").getItem(2))
        .withColumn("category_std", element_at(col("category_array"), -1))
    )

    # ép về OTHER nếu rỗng / null
    df_mapped = (
        df_mapped.withColumn(
            "category_lvl1",
            when(
                (col("category_lvl1").isNull()) | (col("category_lvl1") == ""),
                lit("OTHER"),
            ).otherwise(col("category_lvl1")),
        )
        .withColumn(
            "category_std",
            when(
                (col("category_std").isNull()) | (col("category_std") == ""),
                lit("OTHER"),
            ).otherwise(col("category_std")),
        )
    )

    # ✅ NEW: Enhanced unmapped category analysis
    print("\n[DEBUG] Analyzing unmapped categories (will become OTHER)...")
    
    # Get frequency distribution of unmapped categories
    unmapped_freq = (df_mapped.filter(col("category_std") == "OTHER")
                     .groupBy("category")
                     .count()
                     .orderBy(col("count").desc())
                     .limit(30)
                     .collect())
    
    print(f"\n[DEBUG] Top 30 unmapped category patterns by frequency:")
    for i, row in enumerate(unmapped_freq, 1):
        cat = row["category"] if row["category"] else "NULL"
        count = row["count"]
        print(f"  {i:2d}. '{cat}' → {count:,} products")
    
    # ✅ NEW: Platform distribution of OTHER categories
    print(f"\n[DEBUG] OTHER category distribution by platform:")
    other_by_platform = (df_mapped.filter(col("category_std") == "OTHER")
                        .groupBy("source_platform")
                        .count()
                        .orderBy(col("count").desc())
                        .collect())
    
    total_other = sum(row["count"] for row in other_by_platform)
    for row in other_by_platform:
        platform = row["source_platform"]
        count = row["count"]
        pct = (count / total_other * 100) if total_other > 0 else 0
        print(f"  {platform}: {count:,} ({pct:.1f}% of OTHER)")
    
    # Show sample products from unmapped categories
    print(f"\n[DEBUG] Sample unmapped products (first 50):")
    unmapped_samples = df_mapped.filter(col("category_std") == "OTHER").select("category", "product_name", "source_platform").limit(50).collect()
    for i, row in enumerate(unmapped_samples, 1):
        cat = row["category"] if row["category"] else "NULL"
        name = row["product_name"][:40] if row["product_name"] else "NULL"
        platform = row["source_platform"][:10] if row["source_platform"] else "?"
        print(f"  {i:2d}. [{platform}] Cat:'{cat}' | '{name}'")

    df_mapped = df_mapped.drop("category_array", "category_text")

    print("\n Category Mapping Summary:")
    dist = df_mapped.groupBy("category_std").count().orderBy(col("count").desc()).collect()
    
    # ✅ NEW: Calculate total and percentages
    total = sum(row['count'] for row in dist)
    other_count = next((row['count'] for row in dist if row['category_std'] == 'OTHER'), 0)
    other_pct = (other_count / total * 100) if total > 0 else 0
    
    print(f"  Total products: {total:,}")
    print(f"  Categories mapped: {len(dist)}")
    print(f"\n  Top categories:")
    for row in dist[:15]:  # Show top 15
        count = row['count']
        pct = (count / total * 100) if total > 0 else 0
        marker = "⚠️ " if row['category_std'] == 'OTHER' else "  "
        print(f"  {marker}{row['category_std']}: {count:,} ({pct:.1f}%)")
    
    if len(dist) > 15:
        print(f"  ... and {len(dist) - 15} more categories")
    
    # ✅ NEW: Summary statistics
    print(f"\n[RESULT] OTHER classification: {other_count:,} ({other_pct:.1f}%)")
    if other_pct > 15:
        print(f"[WARNING] OTHER rate is high (>15%). Check unmapped categories above.")
    elif other_pct <= 10:
        print(f"[SUCCESS] OTHER rate is acceptable (≤10%). Mapping is working well! ✅")
    else:
        print(f"[INFO] OTHER rate is moderate (10-15%). Could be improved further.")

    return df_mapped




# ============================================================
#  STEP 2.8 – Data Standardization
# ============================================================
def standardize_data(df):
    print("\n" + "=" * 60)
    print(" STEP 2.8: DATA STANDARDIZATION")
    print("=" * 60)

    df_std = (
        df.withColumn("platform_raw",
            when(col("source_platform").isNotNull(),
                lower(trim(col("source_platform")))
            ).otherwise(lit("unknown"))
        )
        .withColumn(
            "source_platform_std",
            when(col("platform_raw").isin("tiki", "tiki_mass_crawl"), lit("tiki"))
            .when(col("platform_raw").isin("lazada", "lazada_mass_crawl"), lit("lazada"))
            .otherwise(col("platform_raw"))
        )
        .drop("platform_raw")
        .withColumn(
            "brand_std",
            when(col("brand_name").isNotNull(),
                upper(trim(col("brand_name")))).otherwise(lit("UNKNOWN")),
        )
        .withColumn(
            "product_name_std",
            when(
                col("product_name").isNotNull(),
                regexp_replace(trim(col("product_name")), r"\s+", " "),
            ).otherwise(lit("Unknown")),
        )
        .withColumn("price_current_vnd", col("price_current").cast(DoubleType()))
        .withColumn("price_original_vnd", col("price_original").cast(DoubleType()))
    )


    df_std = df_std.withColumn(
        "crawl_ts",
        when(
            col("crawl_date").rlike(r"^\d{4}-\d{2}-\d{2}T"),
            to_timestamp(col("crawl_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        ).otherwise(
            to_timestamp(col("crawl_date"), "yyyy-MM-dd"),
        ),
    )

    print("\n Standardization Summary:")
    if "source_platform_std" in df_std.columns:
        src_dist = df_std.groupBy("source_platform_std").count().collect()
        print("  By source_platform_std:")
        for row in src_dist:
            print(f"    {row['source_platform_std']}: {row['count']:,}")
    return df_std


# ============================================================
#  STEP 2.9 – Identifier Synchronization
# ============================================================
def synchronize_identifiers(df):
    print("\n" + "=" * 60)
    print(" STEP 2.9: IDENTIFIER SYNCHRONIZATION")
    print("=" * 60)

    df_id = df.withColumn(
        "product_id_std",
        when(col("product_id").isNotNull(), trim(col("product_id").cast("string"))).otherwise(
            lit(None)
        ),
    )

    df_id = df_id.withColumn(
        "global_product_id_synced",
        when(
            col("product_id_std").isNotNull() & (col("product_id_std") != ""),
            concat(col("source_platform_std"), lit("_"), col("product_id_std")),
        ).otherwise(trim(col("global_product_id"))),
    )

    df_id = df_id.withColumn(
        "product_master_id",
        sha2(
            concat_ws(
                "||",
                lower(coalesce(col("brand_std"), lit(""))),
                lower(coalesce(col("product_name_std"), lit(""))),
                lower(coalesce(col("category_std"), lit(""))),
            ),
            256,
        ),
    )

    df_id = df_id.withColumn(
        "sku_id",
        sha2(
            concat_ws(
                "||",
                lower(coalesce(col("source_platform_std"), lit(""))),
                lower(coalesce(col("seller_name"), lit(""))),
                lower(coalesce(col("product_id_std"), lit(""))),
            ),
            256,
        ),
    )

    print("\n Identifier Sync Summary:")
    distinct_sync = df_id.select("global_product_id_synced").distinct().count()
    print(f"  Distinct global_product_id_synced: {distinct_sync:,}")
    return df_id


# ============================================================
#  STEP 3 – Deduplication
# ============================================================
def deduplicate_data(df):
    print("\n" + "=" * 60)
    print(" STEP 3: DEDUPLICATION")
    print("=" * 60)

    key_col = (
        "global_product_id_synced"
        if "global_product_id_synced" in df.columns
        else "global_product_id"
    )
    try:
        df_deduplicated = df.dropDuplicates([key_col])
        original_count = df.count()
        deduplicated_count = df_deduplicated.count()
        duplicates_removed = original_count - deduplicated_count

        print(" Deduplicated data:")
        print(f"   Key column: {key_col}")
        print(f"   Original: {original_count:,} records")
        print(f"   After dedup: {deduplicated_count:,} records")
        print(f"   Removed: {duplicates_removed:,} duplicates")
        return df_deduplicated

    except Exception as e:
        print(f" Error during deduplication: {e}")
        return None


# ============================================================
#  STEP 4 – Validation
# ============================================================
def validate_data(df):
    print("\n" + "=" * 60)
    print(" STEP 4: DATA VALIDATION")
    print("=" * 60)

    try:
        total_records = df.count()

        valid_records = df.filter(
            (col("product_name").isNotNull()) & (col("price_current") > 0)
        ).count()

        missing_product_name = df.filter(col("product_name").isNull()).count()
        missing_price = df.filter(col("price_current") <= 0).count()
        missing_brand = df.filter(col("brand_name").isNull()).count()

        print(f"\n Data Quality Report:")
        print(f"  Total records: {total_records:,}")
        print(
            f"  Valid records: {valid_records:,} ({valid_records/total_records*100:.1f}%)"
        )
        print(f"  Missing product_name: {missing_product_name:,}")
        print(f"  Missing/invalid price: {missing_price:,}")
        print(f"  Missing brand: {missing_brand:,}")
        return True

    except Exception as e:
        print(f"  Validation error: {e}")
        return True


# ============================================================
#  STEP 5.5 – Load Dimensions to DWH (Star Schema)
# ============================================================
def make_slug(name: str) -> str:
    """ 
    Tạo slug đơn giản từ product_name: bỏ dấu, thường hóa,
    thay chuỗi không phải [a-z0-9] bằng dấu gạch ngang.
    """
    if not name:
        return None
    s = unidecode(str(name))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or None

def truncate_str(value, max_len):
    """
    Cắt chuỗi về tối đa max_len ký tự để tránh lỗi varchar(n).
    Nếu None thì trả về None.
    """
    if value is None:
        return None
    s = str(value)
    return s[:max_len]

def load_dimensions(df_dedup, conn):
    """
    Load dim_date, dim_platform, dim_category, dim_brand, dim_product
    theo star schema.
    df_dedup phải có: snapshot_date, source_platform_std, category_*, brand_std,
                      global_product_id_synced, product_master_id, product_name_std.
    """
    cur = conn.cursor()

    # ========== DIM_DATE ==========
    print("[INFO] Loading dim_date...")
    date_pdf = (
        df_dedup.select("snapshot_date")
        .where(F.col("snapshot_date").isNotNull())
        .distinct()
        .toPandas()
    )
    insert_date_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_date (
            date_value,
            year,
            month,
            day,
            quarter,
            week_of_year,
            day_of_week,
            day_name,
            is_weekend
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date_value) DO NOTHING
    """

    insert_product_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_product (
            product_key, product_master_id, product_name, product_slug, brand_sk, category_sk
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (product_key) DO UPDATE SET
            product_master_id = COALESCE(EXCLUDED.product_master_id, {DWH_SCHEMA}.dim_product.product_master_id),
            product_name      = COALESCE(EXCLUDED.product_name,      {DWH_SCHEMA}.dim_product.product_name),
            product_slug      = COALESCE(EXCLUDED.product_slug,      {DWH_SCHEMA}.dim_product.product_slug),
            brand_sk          = COALESCE(EXCLUDED.brand_sk,          {DWH_SCHEMA}.dim_product.brand_sk),
            category_sk       = COALESCE(EXCLUDED.category_sk,       {DWH_SCHEMA}.dim_product.category_sk)
    """

    date_rows = []
    for _, r in date_pdf.iterrows():
        d = r["snapshot_date"]
        if isinstance(d, str):
            d = datetime.strptime(d, "%Y-%m-%d").date()
        year_ = d.year
        month_ = d.month
        day_ = d.day
        quarter_ = (month_ - 1) // 3 + 1
        week_of_year = d.isocalendar()[1]
        day_of_week = d.isoweekday()
        day_name = d.strftime("%a")
        is_weekend = day_of_week >= 6
        date_rows.append(
            (d, year_, month_, day_, quarter_, week_of_year, day_of_week, day_name, is_weekend)
        )

    if date_rows:
        execute_batch(cur, insert_date_sql, date_rows, page_size=500)
        conn.commit()
        print(f"  ✅ Loaded/ensured {len(date_rows)} dates")

    cur.execute(f"SELECT date_sk, date_value FROM {DWH_SCHEMA}.dim_date")
    date_map = {row[1]: row[0] for row in cur.fetchall()}

    # ========== DIM_PLATFORM ==========
    print("[INFO] Loading dim_platform...")
    plat_pdf = (
        df_dedup.select("source_platform_std")
        .where(F.col("source_platform_std").isNotNull())
        .distinct()
        .toPandas()
    )

    insert_platform_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_platform (platform_code, platform_name, country_code)
        VALUES (%s, %s, %s)
        ON CONFLICT (platform_code) DO NOTHING
    """

    plat_rows = []
    for _, r in plat_pdf.iterrows():
        code = str(r["source_platform_std"]).strip()
        plat_rows.append((code, code.upper(), "VN"))

    if plat_rows:
        execute_batch(cur, insert_platform_sql, plat_rows, page_size=100)
        conn.commit()
        print(f"  ✅ Loaded/ensured {len(plat_rows)} platforms")

    cur.execute(f"SELECT platform_sk, platform_code FROM {DWH_SCHEMA}.dim_platform")
    platform_map = {row[1]: row[0] for row in cur.fetchall()}

    # ========== DIM_CATEGORY ==========
    print("[INFO] Loading dim_category...")
    cat_pdf = (
        df_dedup.select("category_std", "category_lvl1", "category_lvl2", "category_lvl3")
        .where(F.col("category_std").isNotNull())
        .distinct()
        .toPandas()
    )

    insert_category_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_category (
            category_std_key, category_lvl1, category_lvl2, category_lvl3, full_path
        )
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (category_std_key) DO NOTHING
    """

    cat_rows = []
    for _, r in cat_pdf.iterrows():
        key = str(r["category_std"]).strip()
        l1 = r.get("category_lvl1")
        l2 = r.get("category_lvl2")
        l3 = r.get("category_lvl3")
        parts = [str(x) for x in [l1, l2, l3] if x]
        full_path = " > ".join(parts) if parts else None
        cat_rows.append((key, l1, l2, l3, full_path))

    if cat_rows:
        execute_batch(cur, insert_category_sql, cat_rows, page_size=500)
        conn.commit()
        print(f"  ✅ Loaded/ensured {len(cat_rows)} categories")

    cur.execute(f"SELECT category_sk, category_std_key FROM {DWH_SCHEMA}.dim_category")
    category_map = {row[1]: row[0] for row in cur.fetchall()}

    # ========== DIM_BRAND ==========
    print("[INFO] Loading dim_brand...")
    brand_pdf = (
        df_dedup.select("brand_std")
        .where(F.col("brand_std").isNotNull())
        .distinct()
        .toPandas()
    )

    insert_brand_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_brand (brand_name, brand_normalized)
        VALUES (%s, %s)
        ON CONFLICT (brand_name) DO NOTHING
    """

    brand_rows = []
    for _, r in brand_pdf.iterrows():
        name = str(r["brand_std"]).strip()
        norm = name.upper()
        brand_rows.append((name, norm))

    if brand_rows:
        execute_batch(cur, insert_brand_sql, brand_rows, page_size=500)
        conn.commit()
        print(f"  ✅ Loaded/ensured {len(brand_rows)} brands")

    cur.execute(f"SELECT brand_sk, brand_name FROM {DWH_SCHEMA}.dim_brand")
    brand_map = {row[1]: row[0] for row in cur.fetchall()}
    

    # ========== DIM_PRODUCT ==========
    print("[INFO] Loading dim_product...")
    
    # Get distinct products as Spark DataFrame (don't convert to pandas yet)
    prod_df = (
        df_dedup.select(
            "global_product_id_synced",
            "product_master_id",
            "product_name_std",
            "brand_std",
            "category_std",
        )
        .where(F.col("global_product_id_synced").isNotNull())
        .distinct()
    )
    
    total_products = prod_df.count()
    print(f"[INFO] Processing {total_products} products in batches...")

    insert_product_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_product (
            product_key,
            product_master_id,
            product_name,
            product_slug,
            brand_sk,
            category_sk
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (product_key) DO UPDATE SET
            product_master_id = COALESCE(EXCLUDED.product_master_id, {DWH_SCHEMA}.dim_product.product_master_id),
            product_name      = COALESCE(EXCLUDED.product_name,      {DWH_SCHEMA}.dim_product.product_name),
            product_slug      = COALESCE(EXCLUDED.product_slug,      {DWH_SCHEMA}.dim_product.product_slug),
            brand_sk          = COALESCE(EXCLUDED.brand_sk,          {DWH_SCHEMA}.dim_product.brand_sk),
            category_sk       = COALESCE(EXCLUDED.category_sk,       {DWH_SCHEMA}.dim_product.category_sk)
    """

    # Process using collect() which is more memory efficient than toPandas()
    # Collect returns list of Row objects
    prod_rows_all = prod_df.collect()
    
    print(f"[INFO] Collected {len(prod_rows_all)} product rows, now inserting in batches...")
    
    # Process in batches to avoid memory issues
    BATCH_SIZE = 5000
    total_loaded = 0
    
    for i in range(0, len(prod_rows_all), BATCH_SIZE):
        batch = prod_rows_all[i:i+BATCH_SIZE]
        
        prod_rows = []
        for r in batch:
            product_key = str(r["global_product_id_synced"])[:100]

            master_id_raw = r["product_master_id"] if "product_master_id" in r else None
            product_name_raw = r["product_name_std"] if "product_name_std" in r else None
            brand_name = r["brand_std"] if "brand_std" in r else None
            cat_key = r["category_std"] if "category_std" in r else None

            brand_sk = brand_map.get(brand_name)
            category_sk = category_map.get(cat_key)

            # slug + truncate theo giới hạn cột trong DB
            product_slug_raw = make_slug(product_name_raw)

            master_id = truncate_str(master_id_raw, 256)
            product_name = truncate_str(product_name_raw, 500)
            product_slug = truncate_str(product_slug_raw, 500)

            prod_rows.append(
                (
                    product_key,
                    master_id,
                    product_name,
                    product_slug,
                    brand_sk,
                    category_sk,
                )
            )

        if prod_rows:
            execute_batch(cur, insert_product_sql, prod_rows, page_size=1000)
            conn.commit()
            total_loaded += len(prod_rows)
            print(f"  [PROGRESS] Loaded {total_loaded}/{len(prod_rows_all)} products ({total_loaded/len(prod_rows_all)*100:.1f}%)")

    print(f"  ✅ Loaded/ensured {total_loaded} products total")

    cur.execute(f"SELECT product_sk, product_key FROM {DWH_SCHEMA}.dim_product")
    product_map = {row[1]: row[0] for row in cur.fetchall()}

    cur.close()

    return {
        "date_map": date_map,
        "platform_map": platform_map,
        "category_map": category_map,
        "brand_map": brand_map,
        "product_map": product_map,
    }


# ============================================================
#  STEP 6 – Load fact_product_daily (Star Schema)
# ============================================================
def load_fact_product_daily(df_dedup, conn, mappings):
    """
    Tạo và load dwh.fact_product_daily từ df_dedup.
    Grain: (snapshot_date, global_product_id_synced, source_platform_std)
    """
    print("[INFO] Loading fact_product_daily...")

    date_map = mappings["date_map"]
    platform_map = mappings["platform_map"]
    product_map = mappings["product_map"]

    # Đảm bảo có cột price / review_count / rating
    df = df_dedup
    if "price" not in df.columns:
        df = df.withColumn("price", col("price_current_vnd"))
    if "review_count" not in df.columns:
        df = df.withColumn("review_count", F.lit(0).cast("long"))
    if "rating" not in df.columns:
        df = df.withColumn("rating", F.lit(None).cast(DoubleType()))

    agg_df = (
        df.where(
            F.col("snapshot_date").isNotNull()
            & F.col("global_product_id_synced").isNotNull()
            & F.col("source_platform_std").isNotNull()
        )
        .groupBy("snapshot_date", "global_product_id_synced", "source_platform_std")
        .agg(
            F.count("*").alias("snapshot_count"),
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.expr("percentile_approx(price, 0.5)").alias("median_price"),
            F.stddev("price").alias("price_stddev"),
            F.sum(F.col("review_count")).alias("total_review_count"),
            F.avg("rating").alias("avg_rating"),
        )
    )

    agg_pdf = agg_df.toPandas()

    insert_fact_sql = f"""
        INSERT INTO {DWH_SCHEMA}.fact_product_daily (
            date_sk, product_sk, platform_sk,
            currency_code,
            min_price, max_price, avg_price, median_price, price_stddev,
            total_review_count, avg_rating, snapshot_count
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date_sk, product_sk, platform_sk)
        DO UPDATE SET
            min_price         = EXCLUDED.min_price,
            max_price         = EXCLUDED.max_price,
            avg_price         = EXCLUDED.avg_price,
            median_price      = EXCLUDED.median_price,
            price_stddev      = EXCLUDED.price_stddev,
            total_review_count= EXCLUDED.total_review_count,
            avg_rating        = EXCLUDED.avg_rating,
            snapshot_count    = EXCLUDED.snapshot_count
    """

    BIGINT_MAX = 9223372036854775807

    def safe_num(v):
        if v is None or (isinstance(v, float) and (v != v)):  # NaN
            return None
        if isinstance(v, (int, float)) and abs(v) > BIGINT_MAX:
            return None
        return v

    rows = []
    for _, r in agg_pdf.iterrows():
        d = r["snapshot_date"]
        if isinstance(d, str):
            d = datetime.strptime(d, "%Y-%m-%d").date()

        product_key = str(r["global_product_id_synced"])[:100]
        platform_code = str(r["source_platform_std"]).strip()

        date_sk = date_map.get(d)
        product_sk = product_map.get(product_key)
        platform_sk = platform_map.get(platform_code)

        if date_sk is None or product_sk is None or platform_sk is None:
            continue  # bỏ các dòng không map được key

        total_review_count = int(r["total_review_count"]) if not pd.isna(r["total_review_count"]) else 0
        snapshot_count = int(r["snapshot_count"]) if not pd.isna(r["snapshot_count"]) else 0

        rows.append(
            (
                date_sk,
                product_sk,
                platform_sk,
                "VND",
                safe_num(r["min_price"]),
                safe_num(r["max_price"]),
                safe_num(r["avg_price"]),
                safe_num(r["median_price"]),
                safe_num(r["price_stddev"]),
                total_review_count,
                safe_num(r["avg_rating"]),
                snapshot_count,
            )
        )

    if not rows:
        print("  ⚠ Không có rows nào để insert vào fact_product_daily")
        return

    # ✅ Tối ưu: commit theo batch nhỏ hơn để tránh long-running transaction
    cur = conn.cursor()
    batch_size = 1000
    total_rows = len(rows)
    
    for i in range(0, total_rows, batch_size):
        batch = rows[i:i+batch_size]
        execute_batch(cur, insert_fact_sql, batch, page_size=500)
        conn.commit()  # Commit từng batch để giải phóng connection
        if (i + batch_size) % 5000 == 0:
            print(f"  [PROGRESS] Loaded {min(i+batch_size, total_rows)}/{total_rows} rows...")
    
    cur.close()

    print(f"  ✅ Loaded/updated {len(rows)} rows into fact_product_daily")


from psycopg2.extras import execute_batch  # đã import trên đầu rồi, chỉ nhắc lại

def _ensure_dates_in_dim(conn, date_map, date_values):
    """
    Bổ sung thêm các ngày mới vào dim_date nếu chưa có.
    Trả về date_map mới (date_value -> date_sk).
    """
    # Chuẩn hóa list ngày thành set các string 'YYYY-MM-DD'
    normalized = set()
    for d in date_values:
        if d is None:
            continue
        s = str(d).strip()
        if len(s) >= 10:
            s = s[:10]
        if len(s) == 10 and s.count("-") == 2:
            normalized.add(s)

    if not normalized:
        return date_map

    cur = conn.cursor()
    insert_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_date (
            date_value, year, month, day, quarter,
            week_of_year, day_of_week, day_name, is_weekend
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date_value) DO NOTHING
    """

    rows = []
    for s in normalized:
        try:
            d = datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            continue

        if d in date_map:
            continue

        year_ = d.year
        month_ = d.month
        day_ = d.day
        quarter_ = (month_ - 1) // 3 + 1
        week_of_year = d.isocalendar()[1]
        day_of_week = d.isoweekday()
        day_name = d.strftime("%a")
        is_weekend = day_of_week >= 6

        rows.append(
            (d, year_, month_, day_, quarter_, week_of_year, day_of_week, day_name, is_weekend)
        )

    if rows:
        execute_batch(cur, insert_sql, rows, page_size=200)
        conn.commit()
        # reload date_map
        cur.execute(f"SELECT date_sk, date_value FROM {DWH_SCHEMA}.dim_date")
        date_map = {row[1]: row[0] for row in cur.fetchall()}

    cur.close()
    return date_map

def load_fact_review_star(df_reviews_time, conn, mappings):
    """
    Load dữ liệu review chi tiết vào dwh.fact_review (star schema).
    df_reviews_time: DataFrame sau bước add_review_time_features (có review_date_fmt).
    mappings: dict trả về từ load_dimensions (có date_map, platform_map, product_map).
    """
    print("[INFO] Loading dwh.fact_review (detail)...")

    date_map = mappings["date_map"]
    platform_map = mappings["platform_map"]
    product_map = mappings["product_map"]

    # lấy các cột cần thiết
    needed_cols = [
        "review_id_std",
        "global_review_id",
        "global_product_id",
        "source_platform_std",
        "review_date_fmt",
        "rating_std",
        "helpful_count",
        "sentiment_score",
        "review_text_std",
        "reviewer_name_std",
        "verified_purchase",
        "review_date",
    ]
    available = [c for c in needed_cols if c in df_reviews_time.columns]
    if not available:
        print("  ⚠ Không tìm thấy cột review nào phù hợp để load fact_review")
        return

    df_sel = df_reviews_time.select(*available)

    # Đảm bảo dim_date có đủ ngày review
    date_values = (
        df_sel.select("review_date_fmt")
        .where(F.col("review_date_fmt").isNotNull())
        .distinct()
        .toPandas()["review_date_fmt"]
        .tolist()
    )
    mappings["date_map"] = _ensure_dates_in_dim(conn, date_map, date_values)
    date_map = mappings["date_map"]

    pdf = df_sel.toPandas()
    rows = []

    for _, r in pdf.iterrows():
        product_key = r.get("global_product_id")
        platform_code = r.get("source_platform_std")
        review_date_fmt = r.get("review_date_fmt")

        if not product_key or not platform_code or not review_date_fmt:
            continue

        # map sang khóa surrogate
        try:
            d = datetime.strptime(str(review_date_fmt)[:10], "%Y-%m-%d").date()
        except Exception:
            continue

        date_sk = date_map.get(d)
        product_sk = product_map.get(str(product_key)[:100])
        platform_sk = platform_map.get(str(platform_code).strip())

        if date_sk is None or product_sk is None or platform_sk is None:
            continue

        # review_id_nk: ưu tiên review_id_std, fallback global_review_id
        review_id_nk = r.get("review_id_std") or r.get("global_review_id")
        if not review_id_nk:
            continue

        rating = r.get("rating_std")
        try:
            rating_val = int(rating) if rating is not None else None
        except Exception:
            rating_val = None

        helpful = r.get("helpful_count")
        try:
            helpful_val = int(helpful) if helpful is not None else 0
        except Exception:
            helpful_val = 0

        sentiment = r.get("sentiment_score")
        try:
            sentiment_val = float(sentiment) if sentiment is not None else None
        except Exception:
            sentiment_val = None

        review_body = r.get("review_text_std") or None
        reviewer_name = r.get("reviewer_name_std") or None
        verified = bool(r.get("verified_purchase")) if r.get("verified_purchase") is not None else False

        # 🔧 CHỈ DÙNG NGÀY ĐÃ CHUẨN HÓA LÀM raw_review_date
        from datetime import datetime as dt

        raw_review_date_val = None
        if review_date_fmt:
            try:
                # review_date_fmt là 'YYYY-MM-DD' → convert thành datetime
                raw_review_date_val = dt.strptime(str(review_date_fmt)[:10], "%Y-%m-%d")
            except Exception:
                raw_review_date_val = None

        rows.append(
            {
                "review_id_nk": str(review_id_nk)[:255],
                "product_sk": int(product_sk),
                "platform_sk": int(platform_sk),
                "date_sk": int(date_sk),
                "rating": rating_val,
                "helpful_votes": helpful_val,
                "sentiment_score": sentiment_val,
                "review_title": None,
                "review_body": review_body,
                "reviewer_name": reviewer_name,
                "is_verified_purchase": verified,
                "raw_review_date": raw_review_date_val,   # ⬅️ chỉ gửi datetime hợp lệ
            }
        )

    if not rows:
        print("  ⚠ Không có dòng nào hợp lệ để insert vào dwh.fact_review")
        return

    insert_sql = f"""
        INSERT INTO {DWH_SCHEMA}.fact_review (
            review_id_nk,
            product_sk,
            platform_sk,
            date_sk,
            rating,
            helpful_votes,
            sentiment_score,
            review_title,
            review_body,
            reviewer_name,
            is_verified_purchase,
            raw_review_date
        )
        VALUES (
            %(review_id_nk)s,
            %(product_sk)s,
            %(platform_sk)s,
            %(date_sk)s,
            %(rating)s,
            %(helpful_votes)s,
            %(sentiment_score)s,
            %(review_title)s,
            %(review_body)s,
            %(reviewer_name)s,
            %(is_verified_purchase)s,
            %(raw_review_date)s
        )
        ON CONFLICT (review_id_nk, platform_sk) DO UPDATE SET
            rating = EXCLUDED.rating,
            helpful_votes = EXCLUDED.helpful_votes,
            sentiment_score = EXCLUDED.sentiment_score,
            review_title = EXCLUDED.review_title,
            review_body = EXCLUDED.review_body,
            reviewer_name = EXCLUDED.reviewer_name,
            is_verified_purchase = EXCLUDED.is_verified_purchase,
            raw_review_date = EXCLUDED.raw_review_date
    """

    cur = conn.cursor()
    execute_batch(cur, insert_sql, rows, page_size=1000)
    conn.commit()
    cur.close()

    print(f"  ✅ Loaded/updated {len(rows)} rows into {DWH_SCHEMA}.fact_review")

def load_fact_review_daily_star(df_reviews_agg, conn, mappings):
    """
    Load dữ liệu aggregate review hằng ngày vào dwh.fact_review_daily (star schema).
    df_reviews_agg: output của aggregate_reviews_daily (có agg_date, global_product_id, source_platform_std, ...)
    """
    print("[INFO] Loading dwh.fact_review_daily (aggregate)...")

    if df_reviews_agg is None:
        print("  ⚠ Không có aggregate review để load")
        return

    date_map = mappings["date_map"]
    platform_map = mappings["platform_map"]
    product_map = mappings["product_map"]

    # Bổ sung ngày vào dim_date nếu thiếu
    date_values = (
        df_reviews_agg.select("agg_date")
        .where(F.col("agg_date").isNotNull())
        .distinct()
        .toPandas()["agg_date"]
        .tolist()
    )
    mappings["date_map"] = _ensure_dates_in_dim(conn, date_map, date_values)
    date_map = mappings["date_map"]

    agg_pdf = df_reviews_agg.toPandas()
    rows = []

    for _, r in agg_pdf.iterrows():
        agg_date = r.get("agg_date")
        product_key = r.get("global_product_id")
        platform_code = r.get("source_platform_std")

        if not agg_date or not product_key or not platform_code:
            continue

        try:
            d = datetime.strptime(str(agg_date)[:10], "%Y-%m-%d").date()
        except Exception:
            continue

        date_sk = date_map.get(d)
        product_sk = product_map.get(str(product_key)[:100])
        platform_sk = platform_map.get(str(platform_code).strip())

        if date_sk is None or product_sk is None or platform_sk is None:
            continue

        def safe_int(v):
            try:
                return int(v) if v is not None else None
            except Exception:
                return None

        def safe_float(v):
            try:
                return float(v) if v is not None else None
            except Exception:
                return None

        rows.append(
            {
                "date_sk": int(date_sk),
                "product_sk": int(product_sk),
                "platform_sk": int(platform_sk),
                "review_count": safe_int(r.get("total_reviews")) or 0,
                "avg_rating": safe_float(r.get("avg_rating")),
                "rating_1_count": safe_int(r.get("one_star_count")) or 0,
                "rating_2_count": safe_int(r.get("two_star_count")) or 0,
                "rating_3_count": safe_int(r.get("three_star_count")) or 0,
                "rating_4_count": safe_int(r.get("four_star_count")) or 0,
                "rating_5_count": safe_int(r.get("five_star_count")) or 0,
                "avg_sentiment": safe_float(r.get("avg_sentiment_score")),
            }
        )

    if not rows:
        print("  ⚠ Không có dòng nào hợp lệ để insert vào dwh.fact_review_daily")
        return

    insert_sql = f"""
        INSERT INTO {DWH_SCHEMA}.fact_review_daily (
            date_sk,
            product_sk,
            platform_sk,
            review_count,
            avg_rating,
            rating_1_count,
            rating_2_count,
            rating_3_count,
            rating_4_count,
            rating_5_count,
            avg_sentiment
        )
        VALUES (
            %(date_sk)s,
            %(product_sk)s,
            %(platform_sk)s,
            %(review_count)s,
            %(avg_rating)s,
            %(rating_1_count)s,
            %(rating_2_count)s,
            %(rating_3_count)s,
            %(rating_4_count)s,
            %(rating_5_count)s,
            %(avg_sentiment)s
        )
        ON CONFLICT (date_sk, product_sk, platform_sk) DO UPDATE SET
            review_count   = EXCLUDED.review_count,
            avg_rating     = EXCLUDED.avg_rating,
            rating_1_count = EXCLUDED.rating_1_count,
            rating_2_count = EXCLUDED.rating_2_count,
            rating_3_count = EXCLUDED.rating_3_count,
            rating_4_count = EXCLUDED.rating_4_count,
            rating_5_count = EXCLUDED.rating_5_count,
            avg_sentiment  = EXCLUDED.avg_sentiment
    """

    cur = conn.cursor()
    execute_batch(cur, insert_sql, rows, page_size=1000)
    conn.commit()
    cur.close()

    print(f"  ✅ Loaded/updated {len(rows)} rows into {DWH_SCHEMA}.fact_review_daily")


# ============================================================
#  STEP 7 – Save cleaned data to MinIO
# ============================================================
def save_cleaned_data(df, spark):
    print("\n" + "=" * 60)
    print(" STEP 7: SAVING CLEANED DATA")
    print("=" * 60)

    try:
        from pathlib import Path
        from minio import Minio

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_dir = "/tmp/cleaned_data"
        os.makedirs(local_dir, exist_ok=True)

        local_path = f"{local_dir}/cleaned_{timestamp}"
        print(f"[INFO] Writing to local: {local_path}")
        df.coalesce(4).write.mode("overwrite").parquet(local_path)

        count_ = df.count()
        print(" Saved cleaned data locally:")
        print(f"   Path: {local_path}")
        print("   Format: Parquet")
        print(f"   Total records: {count_:,}")

        if SAVE_TO_MINIO:
            minio_client = Minio(
                MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=MINIO_SECURE
            )
            if not minio_client.bucket_exists(MINIO_CLEANED_BUCKET):
                minio_client.make_bucket(MINIO_CLEANED_BUCKET)
                print(f"[INFO] Bucket created: {MINIO_CLEANED_BUCKET}")

            local_path_obj = Path(local_path)
            uploaded = 0
            prefix = f"cleaned_{timestamp}/"
            for parquet_file in local_path_obj.rglob("*.parquet"):
                remote_path = f"{prefix}{parquet_file.name}"
                print(f"[INFO] Uploading to MinIO: {remote_path}")
                minio_client.fput_object(
                    MINIO_CLEANED_BUCKET,
                    remote_path,
                    str(parquet_file),
                )
                uploaded += 1

            print(
                f" Uploaded {uploaded} files to MinIO: s3a://{MINIO_CLEANED_BUCKET}/{prefix}"
            )

        return True

    except Exception as e:
        print(f" Error saving data: {e}")
        import traceback

        traceback.print_exc()
        return False


# ============================================================
#  REVIEW PIPELINE (giữ nguyên cấu trúc cũ, chỉ sửa nhỏ)
# ============================================================
def load_review_data(spark):
    print("\n" + "=" * 60)
    print(" STEP 8: LOADING REVIEW DATA (LOCAL + MINIO)")
    print("=" * 60)

    import glob as glob_module
    import os as os_module

    dfs = []

    local_base = f"{CRAWLER_OUTPUT_DIR}"
    review_dirs = ["tiki_reviews", "lazada_reviews"]

    for review_dir in review_dirs:
        local_path = f"{local_base}/{review_dir}"
        if os_module.path.exists(local_path):
            print(f"\n[INFO] Loading from {local_path}")
            try:
                json_files = []

                json_files.extend(
                    glob_module.glob(f"{local_path}/date=*/*.json", recursive=False)
                )
                json_files.extend(
                    glob_module.glob(f"{local_path}/date=*/*.jsonl", recursive=False)
                )

                if not json_files:
                    json_files.extend(
                        glob_module.glob(f"{local_path}/**/*.json", recursive=True)
                    )
                    json_files.extend(
                        glob_module.glob(f"{local_path}/**/*.jsonl", recursive=True)
                    )

                if not json_files:
                    json_files.extend(glob_module.glob(f"{local_path}/*.json"))
                    json_files.extend(glob_module.glob(f"{local_path}/*.jsonl"))

                if json_files:
                    print(f"   Found {len(json_files)} JSON/JSONL files")
                    print(f"   Sample files: {json_files[:3]}")
                    df_local = spark.read.option("inferSchema", "true").json(json_files)
                    df_local = df_local.withColumn(
                        "source_platform", lit(review_dir.replace("_reviews", ""))
                    )
                    dfs.append(df_local)
                    print(f"   ✓ Loaded {df_local.count():,} reviews from {review_dir}")
                else:
                    print(f"   ⚠ No JSON/JSONL files found in {local_path}")
                    print(
                        f"      Directory contents: {os_module.listdir(local_path) if os_module.path.exists(local_path) else 'directory does not exist'}"
                    )
                    for date_dir in os_module.listdir(local_path):
                        date_path = os_module.path.join(local_path, date_dir)
                        if os_module.path.isdir(date_path):
                            contents = os_module.listdir(date_path)
                            print(f"      {date_dir}: {contents[:5]}")
            except Exception as e:
                print(f"   ✗ Error: {e}")
                import traceback

                traceback.print_exc()

    if not dfs:
        print(" ⚠ No review data found - skipping review pipeline")
        return None

    print("\n[INFO] Normalizing schemas for union...")
    normalized_dfs = []

    for idx, df in enumerate(dfs):
        print(f"\n  DataFrame {idx} columns: {df.columns}")
        df.printSchema()

        if idx == 0:  # tiki_reviews
            df_norm = (
                df.withColumn("review_id", col("review_id").cast("string"))
                .withColumn("product_id", col("product_id").cast("string"))
                .withColumn(
                    "reviewer_name",
                    coalesce(col("reviewer_name"), lit("Anonymous")),
                )
                .withColumn("rating", col("rating").cast(DoubleType()))
                .withColumn(
                    "review_text",
                    coalesce(col("content"), col("title"), lit("")),
                )
                .withColumn("review_date", coalesce(col("crawl_date"), lit("")))
                .withColumn("images", col("images").cast("string"))
                .select(
                    col("review_id"),
                    col("product_id"),
                    col("reviewer_name"),
                    col("rating"),
                    col("review_text"),
                    col("review_date"),
                    col("helpful_count"),
                    lit(False).alias("verified_purchase"),
                    col("source_platform"),
                    col("images").alias("extra_data"),
                    col("crawl_date"),
                )
            )
        else:  # lazada_reviews
            df_norm = (
                df.withColumn("review_id", col("review_id").cast("string"))
                .withColumn("product_id", col("product_id").cast("string"))
                .withColumn(
                    "reviewer_name",
                    coalesce(col("reviewer_name"), lit("Anonymous")),
                )
                .withColumn("rating", col("rating").cast(DoubleType()))
                .withColumn(
                    "review_text",
                    coalesce(col("review_text"), col("product_name"), lit("")),
                )
                .withColumn(
                    "review_date",
                    coalesce(col("review_date"), col("crawl_timestamp"), lit("")),
                )
                .select(
                    col("review_id"),
                    col("product_id"),
                    col("reviewer_name"),
                    col("rating"),
                    col("review_text"),
                    col("review_date"),
                    col("helpful_count"),
                    lit(False).alias("verified_purchase"),
                    col("source_platform"),
                    col("sku_info").alias("extra_data"),
                    col("crawl_timestamp").alias("crawl_date"),
                )
            )

        normalized_dfs.append(df_norm)

    df_reviews = normalized_dfs[0]
    for d in normalized_dfs[1:]:
        df_reviews = df_reviews.union(d)

    print(f"\n ✓ Total loaded: {df_reviews.count():,} raw reviews")
    print(f"   Final schema: {df_reviews.columns}")
    return df_reviews


def clean_review_data(df_reviews):
    print("\n" + "=" * 60)
    print(" STEP 8.1: CLEANING REVIEW DATA")
    print("=" * 60)

    if df_reviews is None:
        return None

    df_clean = (
        df_reviews.withColumn(
            "review_id",
            when(col("review_id").isNotNull(), trim(col("review_id").cast("string"))).otherwise(
                lit(None)
            ),
        )
        .withColumn(
            "product_id",
            when(col("product_id").isNotNull(), trim(col("product_id").cast("string"))).otherwise(
                lit(None)
            ),
        )
        .withColumn(
            "reviewer_name",
            when(col("reviewer_name").isNotNull(), trim(col("reviewer_name"))).otherwise(
                "Anonymous"
            ),
        )
        .withColumn(
            "rating",
            when(col("rating").isNotNull(), col("rating").cast(DoubleType())).otherwise(0.0),
        )
        .withColumn(
            "review_text",
            when(col("review_text").isNotNull(), trim(col("review_text"))).otherwise(""),
        )
        .withColumn(
            "review_date",
            when(col("review_date").isNotNull(), col("review_date")).otherwise(
                col("crawl_date")
            ),
        )
        .withColumn(
            "helpful_count",
            when(col("helpful_count").isNotNull(), col("helpful_count").cast(LongType())).otherwise(
                0
            ),
        )
        .withColumn(
            "verified_purchase",
            when(col("verified_purchase").isNotNull(), col("verified_purchase")).otherwise(False),
        )
    )

    print(f" ✓ Cleaned {df_clean.count():,} reviews")
    return df_clean


def standardize_review_data(df):
    print("\n" + "=" * 60)
    print(" STEP 8.1.5: STANDARDIZING REVIEW DATA")
    print("=" * 60)

    if df is None:
        return None

    df_std = (
        df.withColumn("platform_raw",
            when(col("source_platform").isNotNull(),
                lower(trim(col("source_platform")))
            ).otherwise(lit("unknown"))
        )
        .withColumn(
            "source_platform_std",
            when(col("platform_raw").isin("tiki", "tiki_mass_crawl"), lit("tiki"))
            .when(col("platform_raw").isin("lazada", "lazada_mass_crawl"), lit("lazada"))
            .otherwise(col("platform_raw"))
        )
        .drop("platform_raw")
        .withColumn(
            "reviewer_name_std",
            when(col("reviewer_name").isNotNull(), trim(col("reviewer_name"))).otherwise(
                "Anonymous"
            ),
        )
        .withColumn(
            "review_text_std",
            when(col("review_text").isNotNull(),
                regexp_replace(trim(col("review_text")), r"\s+", " ")).otherwise(""),
        )
        .withColumn("rating_std", col("rating").cast(DoubleType()))
    )

    print(f"\n ✓ Standardized {df_std.count():,} reviews")
    return df_std


def synchronize_review_identifiers(df):
    print("\n" + "=" * 60)
    print(" STEP 8.1.7: SYNCHRONIZING REVIEW IDENTIFIERS")
    print("=" * 60)

    if df is None:
        return None

    df_id = (
        df.withColumn(
            "review_id_std",
            when(col("review_id").isNotNull(), trim(col("review_id").cast("string"))).otherwise(
                lit(None)
            ),
        )
        .withColumn(
            "product_id_std",
            when(col("product_id").isNotNull(), trim(col("product_id").cast("string"))).otherwise(
                lit(None)
            ),
        )
        .withColumn(
            "global_review_id",
            when(
                col("review_id_std").isNotNull(),
                concat(col("source_platform_std"), lit("_"), col("review_id_std")),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "global_product_id",
            when(
                col("product_id_std").isNotNull(),
                concat(col("source_platform_std"), lit("_"), col("product_id_std")),
            ).otherwise(lit(None)),
        )
    )

    print(f"\n ✓ Synchronized identifiers for {df_id.count():,} reviews")
    return df_id


def deduplicate_review_data(df):
    print("\n" + "=" * 60)
    print(" STEP 8.1.8: DEDUPLICATING REVIEW DATA")
    print("=" * 60)

    if df is None:
        return None

    before_count = df.count()

    df_dedup = (
        df.withColumn("review_date_parsed", to_timestamp(col("review_date")))
        .withColumn(
            "row_num",
            row_number().over(
                Window.partitionBy("global_review_id").orderBy(
                    col("review_date_parsed").desc_nulls_last()
                )
            ),
        )
        .filter(col("row_num") == 1)
        .drop("row_num", "review_date_parsed")
    )

    after_count = df_dedup.count()
    duplicates = before_count - after_count

    print(f"\n ✓ Deduplication Summary:")
    print(f"   Before: {before_count:,}")
    print(f"   After: {after_count:,}")
    print(f"   Duplicates removed: {duplicates:,} ({100*duplicates/before_count:.2f}%)")

    return df_dedup


def validate_review_data(df):
    print("\n" + "=" * 60)
    print(" STEP 8.1.9: VALIDATING REVIEW DATA")
    print("=" * 60)

    if df is None:
        return

    total = df.count()
    valid_reviews = df.filter(
        col("review_id_std").isNotNull()
        & (col("rating_std") >= 1.0)
        & (col("rating_std") <= 5.0)
    ).count()

    print(f"\n ✓ Validation Summary:")
    print(f"   Total reviews: {total:,}")
    print(f"   Valid reviews: {valid_reviews:,} ({100*valid_reviews/total:.2f}%)")
    print(f"   Invalid reviews: {total - valid_reviews:,}")


def analyze_sentiment(df):
    print("\n" + "=" * 60)
    print(" STEP 8.2: SENTIMENT ANALYSIS")
    print("=" * 60)

    if df is None:
        return df

    if TextBlob is None:
        print(" ⚠ TextBlob not available, using default sentiment values")
        df_sentiment = (
            df.withColumn("sentiment_score", lit(0.0))
            .withColumn("sentiment_label", lit("neutral"))
            .withColumn("is_positive_review", lit(0))
            .withColumn("is_negative_review", lit(0))
            .withColumn("is_neutral_review", lit(1))
        )
        print(" ✓ Added default sentiment columns")
        return df_sentiment

    def _get_sentiment_score(text: str):
        if not text or len(str(text).strip()) == 0:
            return 0.0
        try:
            blob = TextBlob(str(text))
            return float(blob.sentiment.polarity)
        except Exception:
            return 0.0

    def _get_sentiment_label(score: float):
        if score < -0.1:
            return "negative"
        elif score > 0.1:
            return "positive"
        else:
            return "neutral"

    sentiment_udf = udf(_get_sentiment_score, DoubleType())
    label_udf = udf(_get_sentiment_label, StringType())

    df_sentiment = (
        df.withColumn("sentiment_score", sentiment_udf(col("review_text")))
        .withColumn("sentiment_label", label_udf(col("sentiment_score")))
        .withColumn(
            "is_positive_review",
            when(col("sentiment_score") > 0.1, 1).otherwise(0),
        )
        .withColumn(
            "is_negative_review",
            when(col("sentiment_score") < -0.1, 1).otherwise(0),
        )
        .withColumn(
            "is_neutral_review",
            when(
                (col("sentiment_score") >= -0.1) & (col("sentiment_score") <= 0.1),
                1,
            ).otherwise(0),
        )
    )

    print(" ✓ Sentiment Distribution:")
    for row in (
        df_sentiment.groupBy("sentiment_label")
        .count()
        .orderBy("sentiment_label")
        .collect()
    ):
        print(f"   {row['sentiment_label'].upper():10s}: {row['count']:>10,}")

    return df_sentiment


def add_review_time_features(df):
    print("\n" + "=" * 60)
    print(" STEP 8.3: ADDING TIME FEATURES")
    print("=" * 60)

    def _parse_relative_date(date_str: str):
        from datetime import datetime as dt, timedelta
        import re as re_module

        if not date_str:
            return None

        s = str(date_str).strip().lower()

        if len(s) == 10 and s.count("-") == 2:
            return s

        if "T" in s:
            return s[:10]

        try:
            match = re_module.search(r"(\d+)\s+(week|day|month|year)s?\s+ago", s)
            if match:
                num = int(match.group(1))
                unit = match.group(2)
                if unit == "week":
                    delta = timedelta(weeks=num)
                elif unit == "day":
                    delta = timedelta(days=num)
                elif unit == "month":
                    delta = timedelta(days=num * 30)
                elif unit == "year":
                    delta = timedelta(days=num * 365)
                else:
                    delta = timedelta(days=0)
                result_date = dt.now() - delta
                return result_date.strftime("%Y-%m-%d")
        except Exception:
            pass

        return None

    parse_relative_udf = udf(_parse_relative_date, StringType())

    df_with_parsed = df.withColumn(
        "review_date_parsed", parse_relative_udf(col("review_date"))
    )

    def _safe_to_date(date_str: str):
        from datetime import datetime as dt

        if not date_str:
            return None
        s = str(date_str).strip()
        if len(s) == 10 and s.count("-") == 2:
            try:
                dt.strptime(s, "%Y-%m-%d")
                return s
            except Exception:
                return None
        if "T" in s:
            return s[:10]
        return None

    safe_to_date_udf = udf(_safe_to_date, StringType())

    df_time = (
        df_with_parsed.withColumn(
            "review_date_fmt",
            coalesce(
                safe_to_date_udf(col("review_date_parsed")),
                safe_to_date_udf(col("crawl_date")),
                safe_to_date_udf(col("review_date")),
                lit(None),
            ),
        )
        .withColumn("review_year", year(to_date(col("review_date_fmt"))))
        .withColumn("review_month", month(to_date(col("review_date_fmt"))))
        .withColumn("review_day", dayofmonth(to_date(col("review_date_fmt"))))
        .withColumn("review_dow", dayofweek(to_date(col("review_date_fmt"))))
        .drop("review_date_parsed")
    )

    print(" ✓ Added time features")
    return df_time


def load_review_dimensions_to_dwh(df):
    print("\n" + "=" * 60)
    print(" STEP 8.4: LOADING REVIEW DIMENSIONS TO DWH")
    print("=" * 60)

    if df is None:
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()

        reviewer_df = (
            df.select(
                col("reviewer_name_std").alias("reviewer_name"),
                col("source_platform_std"),
            )
            .distinct()
            .limit(100000)
        ).toPandas()

        if not reviewer_df.empty:
            dim_reviewer_table = f"{DWH_SCHEMA}.dim_reviewer"
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {dim_reviewer_table} (
                    reviewer_id SERIAL PRIMARY KEY,
                    reviewer_name VARCHAR(500),
                    source_platform VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(reviewer_name, source_platform)
                );
            """
            cur.execute(create_table_sql)
            conn.commit()

            insert_sql = f"""
                INSERT INTO {dim_reviewer_table} (reviewer_name, source_platform)
                VALUES (%s, %s)
                ON CONFLICT (reviewer_name, source_platform) DO NOTHING
            """
            rows = [
                (row["reviewer_name"], row["source_platform_std"])
                for _, row in reviewer_df.iterrows()
            ]
            execute_batch(cur, insert_sql, rows, page_size=1000)
            conn.commit()
            print(f" ✓ Loaded {len(rows)} reviewers to {dim_reviewer_table}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f" Error loading review dimensions: {e}")
        import traceback

        traceback.print_exc()


def aggregate_reviews_daily(df):
    print("\n" + "=" * 60)
    print(" STEP 8.5: AGGREGATING REVIEWS DAILY")
    print("=" * 60)

    if df is None:
        return None

    has_sentiment = all(
        c in df.columns
        for c in [
            "sentiment_score",
            "is_positive_review",
            "is_negative_review",
            "is_neutral_review",
        ]
    )

    df_filtered = df.filter(col("review_date").isNotNull())
    df_with_date = df_filtered.withColumn("review_date_fmt", to_date(col("review_date")))

    df_agg = (
        df_with_date.groupBy("review_date_fmt", "global_product_id", "source_platform_std")
        .agg(
            count("review_id").alias("total_reviews"),
            avg("rating_std").alias("avg_rating"),
            count(when(col("rating_std") == 5.0, 1)).alias("five_star_count"),
            count(when(col("rating_std") == 4.0, 1)).alias("four_star_count"),
            count(when(col("rating_std") == 3.0, 1)).alias("three_star_count"),
            count(when(col("rating_std") == 2.0, 1)).alias("two_star_count"),
            count(when(col("rating_std") == 1.0, 1)).alias("one_star_count"),
            *(
                [
                    avg("sentiment_score").alias("avg_sentiment_score"),
                    spark_sum("is_positive_review").alias("positive_reviews"),
                    spark_sum("is_negative_review").alias("negative_reviews"),
                    spark_sum("is_neutral_review").alias("neutral_reviews"),
                ]
                if has_sentiment
                else [
                    lit(0.0).alias("avg_sentiment_score"),
                    lit(0).alias("positive_reviews"),
                    lit(0).alias("negative_reviews"),
                    lit(0).alias("neutral_reviews"),
                ]
            ),
            spark_sum("helpful_count").alias("total_helpful_count"),
        )
    )

    df_agg = (
        df_agg.withColumn(
            "negative_sentiment_pct",
            when(
                col("total_reviews") > 0,
                (col("negative_reviews") / col("total_reviews") * 100).cast(DoubleType()),
            ).otherwise(0.0),
        )
        .withColumn(
            "positive_sentiment_pct",
            when(
                col("total_reviews") > 0,
                (col("positive_reviews") / col("total_reviews") * 100).cast(DoubleType()),
            ).otherwise(0.0),
        )
        .withColumn(
            "review_quality_score",
            when(col("avg_sentiment_score") > 0.1, 1.0)
            .when(col("avg_sentiment_score") < -0.1, 0.5)
            .otherwise(0.75),
        )
    )

    final_cols = [
        col("review_date_fmt").alias("agg_date"),
        col("global_product_id"),
        col("source_platform_std"),
        col("total_reviews"),
        col("avg_rating"),
        col("five_star_count"),
        col("four_star_count"),
        col("three_star_count"),
        col("two_star_count"),
        col("one_star_count"),
        col("avg_sentiment_score"),
        col("positive_reviews"),
        col("negative_reviews"),
        col("neutral_reviews"),
        col("positive_sentiment_pct"),
        col("negative_sentiment_pct"),
        col("total_helpful_count"),
        col("review_quality_score"),
    ]

    df_agg = df_agg.select(*final_cols)
    df_agg = df_agg.filter(col("agg_date").isNotNull())

    print(
        f" ✓ Generated daily aggregates for {df_agg.count():,} product-date combinations"
    )
    return df_agg


def load_review_aggregation_to_dwh(agg_df):
    print("\n" + "=" * 60)
    print(" STEP 8.6: LOADING REVIEW AGGREGATION TO DWH")
    print("=" * 60)

    if agg_df is None:
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()

        table_full_name = f"{DWH_SCHEMA}.fact_review_daily_agg"
        pandas_df = agg_df.toPandas()

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_full_name} (
                agg_date DATE NOT NULL,
                global_product_id VARCHAR(100) NOT NULL,
                source_platform_std VARCHAR(50) NOT NULL,
                total_reviews BIGINT DEFAULT 0,
                avg_rating DOUBLE PRECISION,
                five_star_count BIGINT DEFAULT 0,
                four_star_count BIGINT DEFAULT 0,
                three_star_count BIGINT DEFAULT 0,
                two_star_count BIGINT DEFAULT 0,
                one_star_count BIGINT DEFAULT 0,
                avg_sentiment_score DOUBLE PRECISION,
                positive_reviews BIGINT DEFAULT 0,
                negative_reviews BIGINT DEFAULT 0,
                neutral_reviews BIGINT DEFAULT 0,
                positive_sentiment_pct DOUBLE PRECISION,
                negative_sentiment_pct DOUBLE PRECISION,
                total_helpful_count BIGINT DEFAULT 0,
                review_quality_score DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (agg_date, global_product_id, source_platform_std)
            );
        """
        cur.execute(create_table_sql)
        conn.commit()
        print(f"[INFO] Table {table_full_name} ready")

        columns = list(pandas_df.columns)
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        insert_query = f"""
            INSERT INTO {table_full_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (agg_date, global_product_id, source_platform_std) DO UPDATE SET
                total_reviews = EXCLUDED.total_reviews,
                avg_rating = EXCLUDED.avg_rating,
                avg_sentiment_score = EXCLUDED.avg_sentiment_score,
                positive_sentiment_pct = EXCLUDED.positive_sentiment_pct,
                negative_sentiment_pct = EXCLUDED.negative_sentiment_pct,
                review_quality_score = EXCLUDED.review_quality_score
        """

        rows = [tuple(row) for row in pandas_df.values]
        execute_batch(cur, insert_query, rows, page_size=1000)
        conn.commit()

        print(f" ✓ Loaded {len(rows)} rows into table: {table_full_name}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f" Error writing to DWH: {e}")
        import traceback

        traceback.print_exc()
        raise


def load_review_details_to_dwh(df):
    print("\n" + "=" * 60)
    print(" STEP 8.6.5: LOADING REVIEW DETAILS TO DWH")
    print("=" * 60)

    if df is None:
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()

        table_full_name = f"{DWH_SCHEMA}.fact_reviews_detail"

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_full_name} (
                review_id VARCHAR(100) NOT NULL,
                global_product_id VARCHAR(100) NOT NULL,
                source_platform_std VARCHAR(50),
                reviewer_name VARCHAR(500),
                rating DOUBLE PRECISION,
                review_text TEXT,
                review_date DATE,
                helpful_count BIGINT DEFAULT 0,
                verified_purchase BOOLEAN DEFAULT FALSE,
                sentiment_score DOUBLE PRECISION DEFAULT 0.0,
                sentiment_label VARCHAR(20),
                review_quality_score DOUBLE PRECISION DEFAULT 0.75,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (review_id, source_platform_std)
            );
        """
        cur.execute(create_table_sql)
        conn.commit()
        print(f"[INFO] Table {table_full_name} ready")

        col_mapping = {
            "global_review_id": "review_id",
            "global_product_id": "global_product_id",
            "source_platform_std": "source_platform_std",
            "reviewer_name_std": "reviewer_name",
            "rating_std": "rating",
            "review_text_std": "review_text",
            "review_date_fmt": "review_date",
            "helpful_count": "helpful_count",
            "verified_purchase": "verified_purchase",
            "sentiment_score": "sentiment_score",
            "sentiment_label": "sentiment_label",
            "review_quality_score": "review_quality_score",
        }

        available_cols = df.columns
        cols_to_select = []
        for src_col, alias in col_mapping.items():
            if src_col in available_cols:
                cols_to_select.append(col(src_col).alias(alias))

        if not cols_to_select:
            print(" ⚠ No compatible columns found for review detail table")
            cur.close()
            conn.close()
            return

        df_detail = df.select(*cols_to_select)

        def _is_valid_date(date_str):
            if not date_str:
                return False
            s = str(date_str).strip()
            if len(s) == 10 and s[4] == "-" and s[7] == "-":
                try:
                    datetime.strptime(s, "%Y-%m-%d")
                    return True
                except Exception:
                    return False
            return False

        is_valid_date_udf = udf(_is_valid_date, BooleanType())

        df_detail = df_detail.withColumn(
            "_date_valid", is_valid_date_udf(col("review_date"))
        ).filter(col("_date_valid") == True).drop("_date_valid")

        pandas_df = df_detail.toPandas()

        if pandas_df.empty:
            print(" ⚠ No review data to insert")
            cur.close()
            conn.close()
            return

        columns = list(pandas_df.columns)
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        insert_query = f"""
            INSERT INTO {table_full_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (review_id, source_platform_std) DO UPDATE SET
                rating = EXCLUDED.rating,
                review_text = EXCLUDED.review_text,
                review_date = EXCLUDED.review_date,
                helpful_count = EXCLUDED.helpful_count,
                sentiment_score = EXCLUDED.sentiment_score,
                sentiment_label = EXCLUDED.sentiment_label,
                review_quality_score = EXCLUDED.review_quality_score
        """

        rows = [tuple(row) for row in pandas_df.values]
        execute_batch(cur, insert_query, rows, page_size=1000)
        conn.commit()

        print(f" ✓ Loaded {len(rows)} review details into: {table_full_name}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f" Error loading review details: {e}")
        import traceback

        traceback.print_exc()


def save_review_results(df_reviews, df_agg):
    print("\n" + "=" * 60)
    print(" STEP 8.5: SAVING REVIEW RESULTS")
    print("=" * 60)

    if df_reviews is None:
        return False

    try:
        from pathlib import Path
        from minio import Minio

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_base = f"/tmp/reviews_processed_{ts}"
        os.makedirs(local_base, exist_ok=True)

        local_reviews = f"{local_base}/cleaned_reviews"
        print(f"[INFO] Writing cleaned reviews to {local_reviews}")
        df_reviews.coalesce(4).write.mode("overwrite").parquet(local_reviews)

        if df_agg is not None:
            local_agg = f"{local_base}/reviews_by_product"
            print(f"[INFO] Writing aggregates to {local_agg}")
            df_agg.coalesce(2).write.mode("overwrite").parquet(local_agg)

        if SAVE_TO_MINIO:
            minio_client = Minio(
                MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=MINIO_SECURE
            )

            if not minio_client.bucket_exists(MINIO_PROCESSED_REVIEWS_BUCKET):
                minio_client.make_bucket(MINIO_PROCESSED_REVIEWS_BUCKET)
                print(f"[INFO] Bucket created: {MINIO_PROCESSED_REVIEWS_BUCKET}")

            prefix = f"reviews_{ts}/"
            uploaded = 0

            for root, dirs, files in os.walk(local_base):
                for file in files:
                    if file.endswith(".parquet"):
                        local_file = os.path.join(root, file)
                        rel_path = os.path.relpath(root, local_base)
                        remote_path = f"{prefix}{rel_path}/{file}"
                        print(f"[INFO] Uploading: {remote_path}")
                        minio_client.fput_object(
                            MINIO_PROCESSED_REVIEWS_BUCKET,
                            remote_path,
                            local_file,
                        )
                        uploaded += 1

            print(
                f" ✓ Uploaded {uploaded} files to MinIO: s3a://{MINIO_PROCESSED_REVIEWS_BUCKET}/{prefix}"
            )

        return True

    except Exception as e:
        print(f" ✗ Error saving review results: {e}")
        import traceback

        traceback.print_exc()
        return False


# ============================================================
#  MAIN
# ============================================================
def main():
    print("\n" + "=" * 60)
    print(" FULL SPARK PIPELINE → DWH (STAR SCHEMA)")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = create_spark_session()

    try:
        df_raw = load_raw_data(spark)
        if df_raw is None:
            print(" Failed to load raw data")
            return 1

        df_cleaned = clean_data(df_raw)
        if df_cleaned is None:
            print(" Failed to clean data")
            return 1

        df_mapped = map_categories(df_cleaned)
        df_std = standardize_data(df_mapped)
        df_synced = synchronize_identifiers(df_std)

        df_dedup = deduplicate_data(df_synced)
        if df_dedup is None:
            print(" Failed to deduplicate data")
            return 1

        validate_data(df_dedup)

        # Chuẩn bị thêm cột snapshot_date + price cho star schema
        df_for_dwh = (
            df_dedup.withColumn(
                "snapshot_date",
                to_date(
                    coalesce(
                        col("crawl_ts"),
                        to_timestamp(col("crawl_date"), "yyyy-MM-dd"),
                    )
                ),
            ).withColumn("price", col("price_current_vnd"))
        )

        # Kết nối Postgres
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )

        # Đảm bảo star schema tồn tại
        ensure_star_schema(conn)

        # Load DIM
        mappings = load_dimensions(df_for_dwh, conn)

        # Load FACT product daily
        load_fact_product_daily(df_for_dwh, conn, mappings)

        conn.close()

        # Save cleaned detail data (parquet + MinIO)
        if not save_cleaned_data(df_dedup, spark):
            print(" Failed to save cleaned data")
            return 1

        # ===== REVIEW DATA PIPELINE =====
        print("\n" + "=" * 60)
        print(" STARTING REVIEW DATA PIPELINE")
        print("=" * 60)

        df_reviews_raw = load_review_data(spark)
        if df_reviews_raw is not None:
            df_reviews_clean = clean_review_data(df_reviews_raw)
            df_reviews_std = standardize_review_data(df_reviews_clean)
            df_reviews_synced = synchronize_review_identifiers(df_reviews_std)
            df_reviews_dedup = deduplicate_review_data(df_reviews_synced)
            validate_review_data(df_reviews_dedup)
            df_reviews_sentiment = analyze_sentiment(df_reviews_dedup)
            df_reviews_time = add_review_time_features(df_reviews_sentiment)

            # (OPTIONAL) dim_reviewer – anh muốn thì giữ, không muốn thì bỏ luôn dòng này
            load_review_dimensions_to_dwh(df_reviews_dedup)

            # Aggregate theo ngày (dùng cho fact_review_daily)
            df_reviews_agg = aggregate_reviews_daily(df_reviews_time)

            # === ĐẨY VÀO STAR SCHEMA FACT_REVIEW + FACT_REVIEW_DAILY ===
            try:
                conn_reviews = psycopg2.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD,
                )
                # dùng lại mappings của phần product (date_map, product_map, platform_map)
                load_fact_review_star(df_reviews_time, conn_reviews, mappings)
                load_fact_review_daily_star(df_reviews_agg, conn_reviews, mappings)
            finally:
                conn_reviews.close()

            # Lưu parquet + MinIO (giữ nguyên)
            save_review_results(df_reviews_dedup, df_reviews_agg)
        else:
            print(" ⚠ Skipping review pipeline - no review data found")

        print("\n" + "=" * 60)
        print(" PIPELINE COMPLETED SUCCESSFULLY! (STAR DWH + MinIO + Reviews)")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\n Pipeline failed: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        spark.stop()
        print("\n Spark session closed")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
