#!/usr/bin/env python3
"""
Spark-based Data Pipeline:
  - Cleaning & Transforming
  - Category Mapping (using mapping table)
  - Data Standardization
  - Identifier Synchronization
  - Deduplication
  - Aggregation (Daily)
  - Load Aggregation to Data Warehouse (Postgres)
  - Save cleaned data to MinIO

Input:  JSONL raw (local folder)
Output:
  - Cleaned parquet in MinIO (cleaned-data bucket)
  - Fact table in Postgres DWH: dwh.fact_product_daily_agg
"""

import os
import sys
import glob
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession
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
from pyspark.sql.types import DoubleType, LongType, StringType
from pyspark.sql.functions import udf

try:
    from textblob import TextBlob
except ImportError:
    TextBlob = None

load_dotenv()

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
MINIO_PROCESSED_REVIEWS_BUCKET = os.getenv("MINIO_PROCESSED_REVIEWS_BUCKET", "processed-reviews")
SAVE_TO_MINIO = os.getenv("SAVE_TO_MINIO", "true").lower() == "true"

# --------------------------
# Postgres / Data Warehouse
# --------------------------
DB_HOST = os.getenv("DB_HOST", "dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_dss")
DB_USER = os.getenv("DB_USER", "dss_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4")
DWH_SCHEMA = os.getenv("DWH_SCHEMA", "dwh")
FACT_TABLE = os.getenv("FACT_TABLE", "fact_product_daily_agg")  # dwh.fact_product_daily_agg

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ============================================================
#  Category mapping config (từ mapping JSON bạn gửi)
# ============================================================
CATEGORY_MAPPINGS = [
    ("headphones", "Electronics|Audio|Headphones"),
    ("tai nghe", "Electronics|Audio|Headphones"),
    ("tai nghe không dây", "Electronics|Audio|Headphones"),

    ("bluetooth speaker", "Electronics|Audio|Speakers"),
    ("speaker", "Electronics|Audio|Speakers"),
    ("loa", "Electronics|Audio|Speakers"),
    ("loa bluetooth", "Electronics|Audio|Speakers"),

    ("notebook", "Electronics|Computers|Laptops"),
    ("máy tính xách tay", "Electronics|Computers|Laptops"),
    ("laptop", "Electronics|Computers|Laptops"),

    ("đồng hồ thông minh", "Electronics|Wearables|Smartwatches"),
    ("smartwatch", "Electronics|Wearables|Smartwatches"),
    ("smart watch", "Electronics|Wearables|Smartwatches"),

    ("earphone", "Electronics|Audio|Earphones"),
    ("wireless earbuds", "Electronics|Audio|Earphones"),

    ("ipad", "Electronics|Tablets"),
    ("tablet", "Electronics|Tablets"),
    ("máy tính bảng", "Electronics|Tablets"),

    ("keyboard", "Electronics|Computers|Accessories|Keyboard"),
    ("mechanical keyboard", "Electronics|Computers|Accessories|Keyboard"),
    ("bàn phím", "Electronics|Computers|Accessories|Keyboard"),
    ("bàn phím cơ", "Electronics|Computers|Accessories|Keyboard"),

    ("mouse", "Electronics|Computers|Accessories|Mouse"),
    ("chuột máy tính", "Electronics|Computers|Accessories|Mouse"),

    ("màn hình máy tính", "Electronics|Computers|Monitors"),
    ("monitor", "Electronics|Computers|Monitors"),
    ("display", "Electronics|Computers|Monitors"),

    ("máy ảnh", "Electronics|Cameras"),
    ("máy ảnh kỹ thuật số", "Electronics|Cameras"),
    ("camera", "Electronics|Cameras"),
    ("digital camera", "Electronics|Cameras"),

    ("máy in", "Electronics|Computers|Printers"),
    ("printer", "Electronics|Computers|Printers"),

    ("máy tính để bàn", "Electronics|Computers|Desktop"),
    ("pc", "Electronics|Computers|Desktop"),
    ("desktop", "Electronics|Computers|Desktop"),

    ("mobile phone", "Electronics|Mobile Phones|Smartphones"),
    ("phone", "Electronics|Mobile Phones|Smartphones"),
    ("smartphone", "Electronics|Mobile Phones|Smartphones"),
    ("điện thoại", "Electronics|Mobile Phones|Smartphones"),
    ("điện thoại thông minh", "Electronics|Mobile Phones|Smartphones"),

    ("router wifi", "Electronics|Networking|Router"),
    ("modem", "Electronics|Networking|Modem"),
    ("access point", "Electronics|Networking|Access Points"),

    ("smart tv", "Electronics|TVs|Smart TVs"),
    ("television", "Electronics|TVs|Smart TVs"),
    ("tivi", "Electronics|TVs|Smart TVs"),
    ("tivi smart", "Electronics|TVs|Smart TVs"),
]




# ============================================================
#  Spark Session
# ============================================================
def create_spark_session():
    print("[INFO] Creating Spark session...")

    if os.name == "nt":
        os.environ["HADOOP_HOME"] = os.environ.get("HADOOP_HOME", "C:/hadoop")
        os.environ["HADOOP_OPTS"] = \
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false"

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
        df_cleaned = (
            df.withColumn("product_id", col("product_id"))
            .withColumn(
                "global_product_id", concat(col("source"), lit("_"), col("product_id"))
            )
            .withColumn("source_platform", col("source"))
            .withColumn(
                "product_name",
                when(col("product_name").isNotNull(), trim(col("product_name"))).otherwise(
                    "Unknown"
                ),
            )
            .withColumn(
                "brand_name",
                when(col("brand").isNotNull(), trim(col("brand"))).otherwise("Unknown"),
            )
            .withColumn(
                "price_current",
                when(
                    col("price_current").isNotNull(),
                    regexp_replace(col("price_current"), "[^0-9]", "").cast(LongType()),
                ).otherwise(0),
            )
            .withColumn(
                "price_original",
                when(
                    col("price_original").isNotNull(),
                    regexp_replace(col("price_original"), "[^0-9]", "").cast(LongType()),
                ).otherwise(0),
            )
            .withColumn(
                "discount_percent",
                when(
                    col("discount_percent").isNotNull(),
                    regexp_replace(col("discount_percent"), "[^0-9.]", "").cast(
                        DoubleType()
                    ),
                ).otherwise(0.0),
            )
            .withColumn(
                "data_quality_score",
                when(
                    (col("product_name").isNotNull()) & (col("price_current") > 0), 1.0
                ).otherwise(0.0),
            )
        )

        available_cols = df_cleaned.columns
        select_cols = [
            c
            for c in [
                "global_product_id",
                "source_platform",
                "product_id",
                "product_name",
                "brand_name",
                "category",
                "price_current",
                "price_original",
                "discount_percent",
                "review_count",
                "seller_name",
                "url",
                "crawl_date",
                "data_quality_score",
            ]
            if c in available_cols
        ]

        df_cleaned = df_cleaned.select(*select_cols)
        cleaned_count = df_cleaned.count()
        print(f" Cleaned {cleaned_count:,} records")
        return df_cleaned

    except Exception as e:
        print(f" Error during cleaning: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================
#  STEP 2.5 – Category Mapping (mapping table)
# ============================================================
def map_categories(df):
    print("\n" + "=" * 60)
    print(" STEP 2.5: CATEGORY MAPPING (using mapping table)")
    print("=" * 60)

    mapping_dict = {k.lower(): v for (k, v) in CATEGORY_MAPPINGS}

    def _map_category(text: str):
        if not text:
            return None
        t = text.lower()
        for key, path in mapping_dict.items():
            if key in t:
                return path
        return None

    map_category_udf = udf(_map_category, StringType())

    df_mapped = df.withColumn(
        "category_text",
        lower(
            concat_ws(
                " ",
                coalesce(col("category"), lit("")),
                coalesce(col("product_name"), lit("")),
            )
        ),
    )

    df_mapped = df_mapped.withColumn(
        "category_path",
        map_category_udf(col("category_text")),
    )

    df_mapped = df_mapped.withColumn(
        "category_array",
        split(col("category_path"), r"\|"),
    )

    df_mapped = (
        df_mapped
        .withColumn("category_lvl1", col("category_array").getItem(0))
        .withColumn("category_lvl2", col("category_array").getItem(1))
        .withColumn("category_lvl3", col("category_array").getItem(2))
        .withColumn("category_std", element_at(col("category_array"), -1))
    )

    df_mapped = (
        df_mapped
        .withColumn("category_lvl1", coalesce(col("category_lvl1"), lit("OTHER")))
        .withColumn("category_std", coalesce(col("category_std"), lit("OTHER")))
    )

    df_mapped = df_mapped.drop("category_array", "category_text")

    print("\n Category Mapping Summary:")
    dist = df_mapped.groupBy("category_std").count().collect()
    for row in dist:
        print(f"  {row['category_std']}: {row['count']:,}")
    return df_mapped


# ============================================================
#  STEP 2.8 – Data Standardization
# ============================================================
def standardize_data(df):
    print("\n" + "=" * 60)
    print(" STEP 2.8: DATA STANDARDIZATION")
    print("=" * 60)

    df_std = (
        df.withColumn(
            "source_platform_std",
            when(col("source_platform").isNotNull(),
                 lower(trim(col("source_platform")))).otherwise(lit("unknown")),
        )
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

    key_col = "global_product_id_synced" if "global_product_id_synced" in df.columns else "global_product_id"
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
#  STEP 5 – Aggregation (Daily)
# ============================================================
def aggregate_daily_metrics(df):
    """
    Tạo fact daily:
      - agg_date
      - source_platform_std
      - category hierarchy
      - số product distinct
      - avg / min / max price
      - tổng review_count
    """
    print("\n" + "=" * 60)
    print(" STEP 5: DAILY AGGREGATION")
    print("=" * 60)

    df_with_date = df.withColumn(
        "agg_date",
        to_date(
            coalesce(col("crawl_ts"), to_timestamp(col("crawl_date"), "yyyy-MM-dd"))
        ),
    )

    agg_df = (
        df_with_date.groupBy(
            "agg_date",
            "source_platform_std",
            "category_lvl1",
            "category_lvl2",
            "category_lvl3",
            "category_std",
        )
        .agg(
            countDistinct("global_product_id_synced").alias("distinct_products"),
            avg("price_current_vnd").alias("avg_price"),
            spark_min("price_current_vnd").alias("min_price"),
            spark_max("price_current_vnd").alias("max_price"),
            spark_sum(coalesce(col("review_count"), lit(0))).alias("total_review_count"),
        )
        .filter(col("agg_date").isNotNull())
    )

    count_rows = agg_df.count()
    print(f" Aggregation result: {count_rows:,} rows")
    return agg_df


# ============================================================
#  STEP 5.5 – Load Dimensions to Data Warehouse (Postgres)
# ============================================================
def load_dimensions_to_dwh(df_dedup):
    """Load dimension tables before loading fact table"""
    print("\n" + "=" * 60)
    print(" STEP 5.5: LOADING DIMENSIONS TO DATA WAREHOUSE")
    print("=" * 60)
    
    try:
        import psycopg2
        from psycopg2.extras import execute_batch
        
        conn = psycopg2.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode='require'
        )
        
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DWH_SCHEMA};")
        cur.execute(f"SET search_path TO {DWH_SCHEMA}, public;")
        
        # ===== DIM_PLATFORM =====
        print("\n[INFO] Loading dim_platform...")
        dim_platform_df = df_dedup.select("source_platform_std").distinct()
        dim_platform_pandas = dim_platform_df.toPandas()
        
        create_dim_platform = f"""
            CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_platform (
                platform_id SERIAL PRIMARY KEY,
                platform_name VARCHAR(50) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        cur.execute(create_dim_platform)
        conn.commit()
        
        insert_platform = f"""
            INSERT INTO {DWH_SCHEMA}.dim_platform (platform_name)
            VALUES (%s)
            ON CONFLICT (platform_name) DO NOTHING
        """
        platform_rows = [(row['source_platform_std'],) for _, row in dim_platform_pandas.iterrows()]
        if platform_rows:
            execute_batch(cur, insert_platform, platform_rows, page_size=100)
            conn.commit()
            print(f"  ✅ Loaded {len(platform_rows)} platforms")
        
        # ===== DIM_CATEGORY =====
        print("\n[INFO] Loading dim_category...")
        dim_category_df = df_dedup.select(
            "category_lvl1", "category_lvl2", "category_lvl3", "category_std"
        ).distinct()
        dim_category_pandas = dim_category_df.toPandas()
        
        create_dim_category = f"""
            CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_category (
                category_id SERIAL PRIMARY KEY,
                category_lvl1 VARCHAR(100),
                category_lvl2 VARCHAR(100),
                category_lvl3 VARCHAR(100),
                category_std VARCHAR(100) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        cur.execute(create_dim_category)
        conn.commit()
        
        insert_category = f"""
            INSERT INTO {DWH_SCHEMA}.dim_category (category_lvl1, category_lvl2, category_lvl3, category_std)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (category_std) DO NOTHING
        """
        category_rows = [
            (row['category_lvl1'], row['category_lvl2'], row['category_lvl3'], row['category_std'])
            for _, row in dim_category_pandas.iterrows()
        ]
        if category_rows:
            execute_batch(cur, insert_category, category_rows, page_size=100)
            conn.commit()
            print(f"  ✅ Loaded {len(category_rows)} categories")
        
        # ===== DIM_BRAND =====
        print("\n[INFO] Loading dim_brand...")
        dim_brand_df = df_dedup.select("brand_std").distinct()
        dim_brand_pandas = dim_brand_df.toPandas()
        
        create_dim_brand = f"""
            CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_brand (
                brand_id SERIAL PRIMARY KEY,
                brand_name VARCHAR(100) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        cur.execute(create_dim_brand)
        conn.commit()
        
        insert_brand = f"""
            INSERT INTO {DWH_SCHEMA}.dim_brand (brand_name)
            VALUES (%s)
            ON CONFLICT (brand_name) DO NOTHING
        """
        brand_rows = [(row['brand_std'],) for _, row in dim_brand_pandas.iterrows()]
        if brand_rows:
            execute_batch(cur, insert_brand, brand_rows, page_size=100)
            conn.commit()
            print(f"  ✅ Loaded {len(brand_rows)} brands")
        
        # ===== DIM_PRODUCT =====
        print("\n[INFO] Loading dim_product...")
        dim_product_df = df_dedup.select(
            "global_product_id_synced",
            "product_name_std",
            "brand_std",
            "category_std",
            "product_master_id"
        ).distinct()
        dim_product_pandas = dim_product_df.toPandas()
        
        # Drop old table if exists (to update schema)
        cur.execute(f"DROP TABLE IF EXISTS {DWH_SCHEMA}.dim_product CASCADE")
        conn.commit()
        
        create_dim_product = f"""
            CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_product (
                product_id SERIAL PRIMARY KEY,
                product_key VARCHAR(100) UNIQUE NOT NULL,
                product_name VARCHAR(500),
                brand_name VARCHAR(150),
                category_std VARCHAR(100),
                product_master_id VARCHAR(256),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        cur.execute(create_dim_product)
        conn.commit()
        
        insert_product = f"""
            INSERT INTO {DWH_SCHEMA}.dim_product (product_key, product_name, brand_name, category_std, product_master_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_key) DO NOTHING
        """
        product_rows = [
            (
                str(row['global_product_id_synced'])[:100],
                str(row['product_name_std'])[:500],
                str(row['brand_std'])[:150],
                str(row['category_std'])[:100],
                str(row['product_master_id'])[:256]
            )
            for _, row in dim_product_pandas.iterrows()
        ]
        if product_rows:
            execute_batch(cur, insert_product, product_rows, page_size=500)
            conn.commit()
            print(f"  ✅ Loaded {len(product_rows)} products")
        
        # ===== DIM_DATE =====
        print("\n[INFO] Loading dim_date...")
        from datetime import datetime, timedelta
        import pandas as pd
        
        create_dim_date = f"""
            CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_date (
                date_id SERIAL PRIMARY KEY,
                date_value DATE UNIQUE NOT NULL,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                quarter INTEGER,
                day_of_week VARCHAR(10),
                week_of_year INTEGER
            );
        """
        cur.execute(create_dim_date)
        conn.commit()
        
        # Get date range from cleaned data
        dim_date_df = df_dedup.select("crawl_ts").distinct()
        if dim_date_df.count() > 0:
            dim_date_pandas = dim_date_df.toPandas()
            dim_date_pandas['crawl_ts'] = pd.to_datetime(dim_date_pandas['crawl_ts'])
            
            # Generate all dates in range
            min_date = dim_date_pandas['crawl_ts'].min()
            max_date = dim_date_pandas['crawl_ts'].max()
            
            date_range = pd.date_range(start=min_date, end=max_date, freq='D')
            
            insert_date = f"""
                INSERT INTO {DWH_SCHEMA}.dim_date (date_value, year, month, day, quarter, day_of_week, week_of_year)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_value) DO NOTHING
            """
            
            date_rows = [
                (
                    d.date(),
                    d.year,
                    d.month,
                    d.day,
                    (d.month - 1) // 3 + 1,
                    d.strftime('%A'),
                    d.isocalendar()[1]
                )
                for d in date_range
            ]
            
            if date_rows:
                execute_batch(cur, insert_date, date_rows, page_size=500)
                conn.commit()
                print(f"  ✅ Loaded {len(date_rows)} dates ({min_date.date()} to {max_date.date()})")
        
        cur.close()
        conn.close()
        print("\n ✅ All dimensions loaded successfully")
        
    except ImportError:
        print("[WARN] psycopg2 not installed, skipping dimension loading")
    except Exception as e:
        print(f" Error loading dimensions: {e}")
        import traceback
        traceback.print_exc()
        raise


# ============================================================
#  STEP 6 – Load aggregation (Fact Table) to Data Warehouse
# ============================================================
def load_aggregation_to_dwh(agg_df):
    print("\n" + "=" * 60)
    print(" STEP 6: LOADING FACT TABLE TO DATA WAREHOUSE")
    print("=" * 60)

    table_full_name = f"{DWH_SCHEMA}.{FACT_TABLE}"

    try:
        import psycopg2
        from psycopg2.extras import execute_batch
        
        print("[INFO] Using psycopg2 for direct PostgreSQL insert...")
        
        # Convert Spark DF to Pandas for batch insert
        pandas_df = agg_df.toPandas()
        
        # Connect to PostgreSQL with SSL required for Render.com
        conn = psycopg2.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode='require'
        )
        
        cur = conn.cursor()
        
        # Set search path to dwh schema
        cur.execute(f"SET search_path TO {DWH_SCHEMA}, public;")
        
        # Create schema if not exists
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DWH_SCHEMA};")
        
        # Create table if not exists
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_full_name} (
                agg_date DATE,
                source_platform_std VARCHAR(50),
                category_lvl1 VARCHAR(100),
                category_lvl2 VARCHAR(100),
                category_lvl3 VARCHAR(100),
                category_std VARCHAR(100),
                distinct_products BIGINT,
                avg_price DOUBLE PRECISION,
                min_price DOUBLE PRECISION,
                max_price DOUBLE PRECISION,
                total_review_count BIGINT,
                PRIMARY KEY (agg_date, source_platform_std, category_std)
            );
        """
        cur.execute(create_table_sql)
        conn.commit()
        print(f"[INFO] Table {table_full_name} ready")
        
        # Get column names
        columns = list(pandas_df.columns)
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        
        insert_query = f"""
            INSERT INTO {table_full_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (agg_date, source_platform_std, category_std) DO UPDATE SET
                distinct_products = EXCLUDED.distinct_products,
                avg_price = EXCLUDED.avg_price,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                total_review_count = EXCLUDED.total_review_count
        """
        
        # Convert DataFrame rows to tuples
        rows = [tuple(row) for row in pandas_df.values]
        
        # Batch insert
        execute_batch(cur, insert_query, rows, page_size=1000)
        conn.commit()
        
        print(f" ✅ Loaded {len(rows)} rows into table: {table_full_name}")
        
        cur.close()
        conn.close()
        
    except ImportError:
        print("[WARN] psycopg2 not installed, falling back to Spark JDBC...")
        try:
            (
                agg_df.write
                .format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", table_full_name)
                .option("user", DB_USER)
                .option("password", DB_PASSWORD)
                .option("driver", "org.postgresql.Driver")
                .option("numPartitions", "1")
                .mode("append")
                .save()
            )
            print(f" ✅ Loaded fact table into: {table_full_name}")
        except Exception as e:
            print(f" Error writing to DWH: {e}")
            import traceback
            traceback.print_exc()
            raise
    except Exception as e:
        print(f" Error writing to DWH: {e}")
        import traceback
        traceback.print_exc()
        raise


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

        count = df.count()
        print(" Saved cleaned data locally:")
        print(f"   Path: {local_path}")
        print("   Format: Parquet")
        print(f"   Total records: {count:,}")

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
#  STEP 8 – Load Review Data (Local + MinIO)
# ============================================================
def load_review_data(spark):
    print("\n" + "=" * 60)
    print(" STEP 8: LOADING REVIEW DATA (LOCAL + MINIO)")
    print("=" * 60)

    import glob
    import os as os_module

    dfs = []

    # Load từ local directories - with date subdirectories
    local_base = f"{CRAWLER_OUTPUT_DIR}"
    review_dirs = ["tiki_reviews", "lazada_reviews"]

    for review_dir in review_dirs:
        local_path = f"{local_base}/{review_dir}"
        if os_module.path.exists(local_path):
            print(f"\n[INFO] Loading from {local_path}")
            try:
                # Search for JSON/JSONL files in date subdirectories
                json_files = []
                
                # Pattern 1: date=*/*.json or date=*/*.jsonl
                json_files.extend(glob.glob(f"{local_path}/date=*/*.json", recursive=False))
                json_files.extend(glob.glob(f"{local_path}/date=*/*.jsonl", recursive=False))
                
                # Pattern 2: date=**/*.json (deeper nesting)
                if not json_files:
                    json_files.extend(glob.glob(f"{local_path}/**/*.json", recursive=True))
                    json_files.extend(glob.glob(f"{local_path}/**/*.jsonl", recursive=True))
                
                # Pattern 3: direct files in root
                if not json_files:
                    json_files.extend(glob.glob(f"{local_path}/*.json"))
                    json_files.extend(glob.glob(f"{local_path}/*.jsonl"))

                if json_files:
                    print(f"   Found {len(json_files)} JSON/JSONL files")
                    print(f"   Sample files: {json_files[:3]}")
                    # Load all files and union them
                    df_local = spark.read.option("inferSchema", "true").json(json_files)
                    df_local = df_local.withColumn(
                        "source_platform",
                        lit(review_dir.replace("_reviews", ""))
                    )
                    dfs.append(df_local)
                    print(f"   ✓ Loaded {df_local.count():,} reviews from {review_dir}")
                else:
                    print(f"   ⚠ No JSON/JSONL files found in {local_path}")
                    print(f"      Directory contents: {os_module.listdir(local_path) if os_module.path.exists(local_path) else 'directory does not exist'}")
                    # Check inside date subdirectories
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

    # Normalize and union schemas
    print("\n[INFO] Normalizing schemas for union...")
    normalized_dfs = []
    
    for idx, df in enumerate(dfs):
        print(f"\n  DataFrame {idx} columns: {df.columns}")
        df.printSchema()
        
        # Normalize Tiki reviews schema
        if idx == 0:  # tiki_reviews
            df_norm = (
                df
                .withColumn("review_id", col("review_id").cast("string"))
                .withColumn("product_id", col("product_id").cast("string"))
                .withColumn("reviewer_name", coalesce(col("reviewer_name"), lit("Anonymous")))
                .withColumn("rating", col("rating").cast(DoubleType()))
                .withColumn("review_text", coalesce(col("content"), col("title"), lit("")))
                .withColumn("review_date", coalesce(col("crawl_date"), lit("")))
                .withColumn("images", col("images").cast("string"))  # Convert array to string
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
                    col("crawl_date")
                )
            )
        else:  # lazada_reviews
            df_norm = (
                df
                .withColumn("review_id", col("review_id").cast("string"))
                .withColumn("product_id", col("product_id").cast("string"))
                .withColumn("reviewer_name", coalesce(col("reviewer_name"), lit("Anonymous")))
                .withColumn("rating", col("rating").cast(DoubleType()))
                .withColumn("review_text", coalesce(col("review_text"), col("product_name"), lit("")))
                .withColumn("review_date", coalesce(col("review_date"), col("crawl_timestamp"), lit("")))
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
                    col("crawl_timestamp").alias("crawl_date")
                )
            )
        
        normalized_dfs.append(df_norm)
    
    # Union all normalized dataframes
    df_reviews = normalized_dfs[0]
    for d in normalized_dfs[1:]:
        df_reviews = df_reviews.union(d)

    print(f"\n ✓ Total loaded: {df_reviews.count():,} raw reviews")
    print(f"   Final schema: {df_reviews.columns}")
    return df_reviews


# ============================================================
#  STEP 8.1 – Clean Review Data
# ============================================================
def clean_review_data(df_reviews):
    print("\n" + "=" * 60)
    print(" STEP 8.1: CLEANING REVIEW DATA")
    print("=" * 60)

    if df_reviews is None:
        return None

    df_clean = (
        df_reviews
        .withColumn(
            "review_id",
            when(col("review_id").isNotNull(), trim(col("review_id").cast("string")))
            .otherwise(lit(None)),
        )
        .withColumn(
            "product_id",
            when(col("product_id").isNotNull(), trim(col("product_id").cast("string")))
            .otherwise(lit(None)),
        )
        .withColumn(
            "reviewer_name",
            when(col("reviewer_name").isNotNull(), trim(col("reviewer_name")))
            .otherwise("Anonymous"),
        )
        .withColumn(
            "rating",
            when(col("rating").isNotNull(), col("rating").cast(DoubleType()))
            .otherwise(0.0),
        )
        .withColumn(
            "review_text",
            when(col("review_text").isNotNull(), trim(col("review_text")))
            .otherwise(""),
        )
        .withColumn(
            "review_date",
            when(col("review_date").isNotNull(), col("review_date"))
            .otherwise(col("crawl_date")),
        )
        .withColumn(
            "helpful_count",
            when(col("helpful_count").isNotNull(), col("helpful_count").cast(LongType()))
            .otherwise(0),
        )
        .withColumn(
            "verified_purchase",
            when(col("verified_purchase").isNotNull(), col("verified_purchase"))
            .otherwise(False),
        )
    )

    print(f" ✓ Cleaned {df_clean.count():,} reviews")
    return df_clean


# ============================================================
#  STEP 8.1.5 – Standardize Review Data
# ============================================================
def standardize_review_data(df):
    print("\n" + "=" * 60)
    print(" STEP 8.1.5: STANDARDIZING REVIEW DATA")
    print("=" * 60)

    if df is None:
        return None

    df_std = (
        df
        .withColumn(
            "source_platform_std",
            when(col("source_platform").isNotNull(),
                 lower(trim(col("source_platform")))).otherwise(lit("unknown")),
        )
        .withColumn(
            "reviewer_name_std",
            when(col("reviewer_name").isNotNull(),
                 trim(col("reviewer_name"))).otherwise("Anonymous"),
        )
        .withColumn(
            "review_text_std",
            when(col("review_text").isNotNull(),
                 regexp_replace(trim(col("review_text")), r"\s+", " "))
            .otherwise(""),
        )
        .withColumn(
            "rating_std",
            col("rating").cast(DoubleType()),
        )
    )

    print(f"\n ✓ Standardized {df_std.count():,} reviews")
    return df_std


# ============================================================
#  STEP 8.1.7 – Synchronize Review Identifiers
# ============================================================
def synchronize_review_identifiers(df):
    print("\n" + "=" * 60)
    print(" STEP 8.1.7: SYNCHRONIZING REVIEW IDENTIFIERS")
    print("=" * 60)

    if df is None:
        return None

    df_id = (
        df
        .withColumn(
            "review_id_std",
            when(col("review_id").isNotNull(), trim(col("review_id").cast("string")))
            .otherwise(lit(None)),
        )
        .withColumn(
            "product_id_std",
            when(col("product_id").isNotNull(), trim(col("product_id").cast("string")))
            .otherwise(lit(None)),
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


# ============================================================
#  STEP 8.1.8 – Deduplicate Review Data
# ============================================================
def deduplicate_review_data(df):
    print("\n" + "=" * 60)
    print(" STEP 8.1.8: DEDUPLICATING REVIEW DATA")
    print("=" * 60)

    if df is None:
        return None

    before_count = df.count()

    # Deduplicate by global_review_id, keep latest by review_date
    df_dedup = (
        df
        .withColumn("review_date_parsed", to_timestamp(col("review_date")))
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


# ============================================================
#  STEP 8.1.9 – Validate Review Data
# ============================================================
def validate_review_data(df):
    print("\n" + "=" * 60)
    print(" STEP 8.1.9: VALIDATING REVIEW DATA")
    print("=" * 60)

    if df is None:
        return

    total = df.count()
    valid_reviews = df.filter(
        col("review_id_std").isNotNull() & 
        (col("rating_std") >= 1.0) & 
        (col("rating_std") <= 5.0)
    ).count()

    print(f"\n ✓ Validation Summary:")
    print(f"   Total reviews: {total:,}")
    print(f"   Valid reviews: {valid_reviews:,} ({100*valid_reviews/total:.2f}%)")
    print(f"   Invalid reviews: {total - valid_reviews:,}")


# ============================================================
#  STEP 8.2 – Sentiment Analysis
# ============================================================
def analyze_sentiment(df):
    print("\n" + "=" * 60)
    print(" STEP 8.2: SENTIMENT ANALYSIS")
    print("=" * 60)

    if df is None:
        return df

    if TextBlob is None:
        print(" ⚠ TextBlob not available, using default sentiment values")
        # Add default sentiment columns
        df_sentiment = (
            df
            .withColumn("sentiment_score", lit(0.0))
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
        except:
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
        df
        .withColumn("sentiment_score", sentiment_udf(col("review_text")))
        .withColumn("sentiment_label", label_udf(col("sentiment_score")))
        .withColumn(
            "is_positive_review",
            when(col("sentiment_score") > 0.1, 1).otherwise(0)
        )
        .withColumn(
            "is_negative_review",
            when(col("sentiment_score") < -0.1, 1).otherwise(0)
        )
        .withColumn(
            "is_neutral_review",
            when((col("sentiment_score") >= -0.1) & (col("sentiment_score") <= 0.1), 1)
            .otherwise(0)
        )
    )

    print(" ✓ Sentiment Distribution:")
    for row in df_sentiment.groupBy("sentiment_label").count().orderBy("sentiment_label").collect():
        print(f"   {row['sentiment_label'].upper():10s}: {row['count']:>10,}")

    return df_sentiment


# ============================================================
#  STEP 8.3 – Add Time Features
# ============================================================
def add_review_time_features(df):
    print("\n" + "=" * 60)
    print(" STEP 8.3: ADDING TIME FEATURES")
    print("=" * 60)

    df_time = (
        df
        .withColumn("review_date_fmt", to_date(col("review_date")))
        .withColumn("review_year", year(col("review_date_fmt")))
        .withColumn("review_month", month(col("review_date_fmt")))
        .withColumn("review_day", dayofmonth(col("review_date_fmt")))
        .withColumn("review_dow", dayofweek(col("review_date_fmt")))
    )

    print(f" ✓ Added time features")
    return df_time


# ============================================================
#  STEP 8.4 – Load Review Dimensions to DWH
# ============================================================
def load_review_dimensions_to_dwh(df):
    print("\n" + "=" * 60)
    print(" STEP 8.4: LOADING REVIEW DIMENSIONS TO DWH")
    print("=" * 60)

    if df is None:
        return

    try:
        import psycopg2
        from psycopg2.extras import execute_batch
        
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cur = conn.cursor()

        # Dimension: dim_reviewer
        reviewer_df = (
            df.select(col("reviewer_name_std").alias("reviewer_name"), col("source_platform_std"))
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
            rows = [(row['reviewer_name'], row['source_platform_std']) for _, row in reviewer_df.iterrows()]
            execute_batch(cur, insert_sql, rows, page_size=1000)
            conn.commit()
            print(f" ✓ Loaded {len(rows)} reviewers to {dim_reviewer_table}")

        cur.close()
        conn.close()

    except ImportError:
        print("[WARN] psycopg2 not available, skipping dimension load")
    except Exception as e:
        print(f" Error loading review dimensions: {e}")
        import traceback
        traceback.print_exc()


# ============================================================
#  STEP 8.5 – Aggregate Reviews Daily
# ============================================================
def aggregate_reviews_daily(df):
    print("\n" + "=" * 60)
    print(" STEP 8.5: AGGREGATING REVIEWS DAILY")
    print("=" * 60)

    if df is None:
        return None

    # Check if sentiment columns exist
    has_sentiment = all(col_name in df.columns for col_name in ["sentiment_score", "is_positive_review", "is_negative_review", "is_neutral_review"])
    
    # Filter out rows with NULL review_date before creating review_date_fmt
    df_filtered = df.filter(col("review_date").isNotNull())
    df_with_date = df_filtered.withColumn("review_date_fmt", to_date(col("review_date")))

    # Build aggregation dynamically
    agg_dict = {
        "review_id": "count",
        "rating_std": "avg",
        "helpful_count": "sum",
    }
    
    # Add sentiment aggregations if available
    if has_sentiment:
        agg_dict.update({
            "sentiment_score": "avg",
            "is_positive_review": "sum",
            "is_negative_review": "sum",
            "is_neutral_review": "sum",
        })

    df_agg = (
        df_with_date
        .groupBy("review_date_fmt", "global_product_id", "source_platform_std")
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
                ] if has_sentiment else [
                    lit(0.0).alias("avg_sentiment_score"),
                    lit(0).alias("positive_reviews"),
                    lit(0).alias("negative_reviews"),
                    lit(0).alias("neutral_reviews"),
                ]
            ),
            spark_sum("helpful_count").alias("total_helpful_count"),
        )
    )

    # Add percentage columns
    df_agg = (
        df_agg
        .withColumn(
            "negative_sentiment_pct",
            when(col("total_reviews") > 0,
                 (col("negative_reviews") / col("total_reviews") * 100).cast(DoubleType()))
            .otherwise(0.0),
        )
        .withColumn(
            "positive_sentiment_pct",
            when(col("total_reviews") > 0,
                 (col("positive_reviews") / col("total_reviews") * 100).cast(DoubleType()))
            .otherwise(0.0),
        )
        .withColumn(
            "review_quality_score",
            when(col("avg_sentiment_score") > 0.1, 1.0)
            .when(col("avg_sentiment_score") < -0.1, 0.5)
            .otherwise(0.75),
        )
    )

    # Select final columns in order
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
    
    # Filter out any remaining NULL agg_date values before returning
    df_agg = df_agg.filter(col("agg_date").isNotNull())

    print(f" ✓ Generated daily aggregates for {df_agg.count():,} product-date combinations")
    return df_agg


# ============================================================
#  STEP 8.6 – Load Review Aggregation to DWH
# ============================================================
def load_review_aggregation_to_dwh(agg_df):
    print("\n" + "=" * 60)
    print(" STEP 8.6: LOADING REVIEW AGGREGATION TO DWH")
    print("=" * 60)

    if agg_df is None:
        return

    try:
        import psycopg2
        from psycopg2.extras import execute_batch
        
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
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

    except ImportError:
        print("[WARN] psycopg2 not installed, falling back to Spark JDBC...")
        try:
            (
                agg_df.write
                .format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", f"{DWH_SCHEMA}.fact_review_daily_agg")
                .option("user", DB_USER)
                .option("password", DB_PASSWORD)
                .option("driver", "org.postgresql.Driver")
                .option("numPartitions", "1")
                .mode("append")
                .save()
            )
            print(f" ✓ Loaded review fact table via JDBC")
        except Exception as e:
            print(f" Error writing to DWH: {e}")
            import traceback
            traceback.print_exc()
            raise
    except Exception as e:
        print(f" Error writing to DWH: {e}")
        import traceback
        traceback.print_exc()
        raise


# ============================================================
#  STEP 8.7 – Aggregate Reviews by Product (Legacy)
# ============================================================
def aggregate_reviews_by_product(df):
    print("\n" + "=" * 60)
    print(" STEP 8.7: AGGREGATING REVIEWS BY PRODUCT (LEGACY)")
    print("=" * 60)

    if df is None:
        return None

    df_agg = (
        df
        .groupBy("product_id", "source_platform")
        .agg(
            count("review_id").alias("total_reviews"),
            avg("rating").alias("avg_rating"),
            count(when(col("rating") == 5.0, 1)).alias("five_star_count"),
            count(when(col("rating") == 4.0, 1)).alias("four_star_count"),
            count(when(col("rating") == 3.0, 1)).alias("three_star_count"),
            count(when(col("rating") == 2.0, 1)).alias("two_star_count"),
            count(when(col("rating") == 1.0, 1)).alias("one_star_count"),
            avg("sentiment_score").alias("avg_sentiment_score"),
            spark_sum("is_positive_review").alias("positive_reviews"),
            spark_sum("is_negative_review").alias("negative_reviews"),
            spark_sum("is_neutral_review").alias("neutral_reviews"),
            spark_sum("helpful_count").alias("total_helpful_count"),
            count(when(col("verified_purchase") == True, 1)).alias("verified_reviews"),
        )
        .withColumn("negative_sentiment_pct",
                   (col("negative_reviews") / col("total_reviews") * 100).cast(DoubleType()))
        .withColumn("positive_sentiment_pct",
                   (col("positive_reviews") / col("total_reviews") * 100).cast(DoubleType()))
        .withColumn("verified_purchase_pct",
                   (col("verified_reviews") / col("total_reviews") * 100).cast(DoubleType()))
    )

    print(f" ✓ Generated aggregates for {df_agg.count():,} products")
    return df_agg


# ============================================================
#  STEP 8.5 – Save Review Results to MinIO
# ============================================================
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

        # Save cleaned reviews
        local_reviews = f"{local_base}/cleaned_reviews"
        print(f"[INFO] Writing cleaned reviews to {local_reviews}")
        df_reviews.coalesce(4).write.mode("overwrite").parquet(local_reviews)

        # Save aggregates
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

            print(f" ✓ Uploaded {uploaded} files to MinIO: s3a://{MINIO_PROCESSED_REVIEWS_BUCKET}/{prefix}")

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
    print(" FULL SPARK PIPELINE → DWH")
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

        # Load Dimensions first
        load_dimensions_to_dwh(df_dedup)
        
        # Aggregation + Load Fact Table to DWH
        agg_df = aggregate_daily_metrics(df_dedup)
        load_aggregation_to_dwh(agg_df)

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
            
            # Load review dimensions & fact table
            load_review_dimensions_to_dwh(df_reviews_dedup)
            df_reviews_agg = aggregate_reviews_daily(df_reviews_time)
            load_review_aggregation_to_dwh(df_reviews_agg)
            
            # Save cleaned reviews to MinIO
            save_review_results(df_reviews_dedup, df_reviews_agg)
        else:
            print(" ⚠ Skipping review pipeline - no review data found")

        print("\n" + "=" * 60)
        print(" PIPELINE COMPLETED SUCCESSFULLY! (DWH + MinIO + Reviews)")
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
