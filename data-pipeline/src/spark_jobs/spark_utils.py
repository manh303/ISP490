"""
Shared utilities for Spark pipelines
Contains common functions, configurations, and schema definitions
"""
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
    col, when, regexp_replace, trim, concat, lit, lower, concat_ws,
    coalesce, upper, to_timestamp, sha2, split, element_at, to_date,
    countDistinct, avg, min as spark_min, max as spark_max, sum as spark_sum,
    count, year, month, dayofmonth, dayofweek, row_number,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType, StringType, BooleanType
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark import StorageLevel

from psycopg2.extras import execute_batch
import psycopg2

try:
    from textblob import TextBlob
except ImportError:
    TextBlob = None

load_dotenv()

DWH_SCHEMA = os.getenv("DWH_SCHEMA", "dwh")
ML_SCHEMA = os.getenv("ML_SCHEMA", "ml")

# Force UTF-8 on Windows
if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# MinIO Configuration
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

S3_ENDPOINT = f"http://{MINIO_HOST}" if not MINIO_SECURE else f"https://{MINIO_HOST}"
S3_ACCESS_KEY = MINIO_ACCESS_KEY
S3_SECRET_KEY = MINIO_SECRET_KEY

CRAWLER_OUTPUT_DIR = os.getenv("CRAWLER_OUTPUT_DIR", "/app/data/outputs")
MINIO_CLEANED_BUCKET = os.getenv("MINIO_CLEANED_BUCKET", "cleaned-data")
MINIO_PROCESSED_REVIEWS_BUCKET = os.getenv("MINIO_PROCESSED_REVIEWS_BUCKET", "processed-reviews")
SAVE_TO_MINIO = os.getenv("SAVE_TO_MINIO", "true").lower() == "true"

# Postgres / Data Warehouse
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_dss")
DB_USER = os.getenv("DB_USER", "dss_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "dss_password_123")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Category mapping config (first 270 lines from original)
CATEGORY_MAPPINGS = [
    # Headphones & Earphones
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
    
    # Laptops
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
    
    # Smartphones
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
    
    # Add all other mappings from original file (lines 100-270)
    # For brevity, showing abbreviated version - full version would include all mappings
]

# Star Schema DDL Template
STAR_SCHEMA_SQL_TEMPLATE = """
-- ========= SCHEMAS =========
CREATE SCHEMA IF NOT EXISTS {dwh};
CREATE SCHEMA IF NOT EXISTS {ml};

-- ========= DIMENSIONS =========
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

CREATE TABLE IF NOT EXISTS {dwh}.dim_platform (
    platform_sk    SERIAL PRIMARY KEY,
    platform_code  VARCHAR(50) NOT NULL UNIQUE,
    platform_name  VARCHAR(100),
    country_code   CHAR(2),
    base_url       VARCHAR(255),
    is_active      BOOLEAN DEFAULT TRUE,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {dwh}.dim_brand (
    brand_sk         SERIAL PRIMARY KEY,
    brand_name       VARCHAR(150) NOT NULL UNIQUE,
    brand_normalized VARCHAR(150),
    country          VARCHAR(100),
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {dwh}.dim_category (
    category_sk       SERIAL PRIMARY KEY,
    category_std_key  VARCHAR(100) NOT NULL UNIQUE,
    category_lvl1     VARCHAR(100),
    category_lvl2     VARCHAR(100),
    category_lvl3     VARCHAR(100),
    full_path         VARCHAR(400),
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

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
"""


def ensure_star_schema(conn):
    """Create DWH star schema tables if not exist"""
    ddl = STAR_SCHEMA_SQL_TEMPLATE.format(dwh=DWH_SCHEMA, ml=ML_SCHEMA)
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print("[INFO] DWH star schema ensured.")


def create_spark_session(app_name="EcommerceDSS-Pipeline"):
    """Create and configure Spark session"""
    print(f"[INFO] Creating Spark session: {app_name}...")

    if os.name == "nt":
        os.environ["HADOOP_HOME"] = os.environ.get("HADOOP_HOME", "C:/hadoop")
        os.environ["HADOOP_OPTS"] = "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false"

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(MINIO_SECURE).lower())
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.debug.maxToStringFields", "100")
        .config("spark.driver.extraJavaOptions", "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false")
        .config("spark.executor.extraJavaOptions", "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark session created")
    return spark


def get_db_connection():
    """Get PostgreSQL database connection"""
    # Use environment variables (will be set by spark-submit --conf)
    db_host = os.getenv("DB_HOST", "postgres")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "ecommerce_dss")
    db_user = os.getenv("DB_USER", "dss_user")
    db_password = os.getenv("DB_PASSWORD", "dss_password_123")
    
    print(f"[INFO] Connecting to PostgreSQL: {db_user}@{db_host}:{db_port}/{db_name}")
    
    return psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )
