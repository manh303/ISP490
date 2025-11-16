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
)
from pyspark.sql.types import DoubleType, LongType, StringType
from pyspark.sql.functions import udf

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

        print("\n" + "=" * 60)
        print(" PIPELINE COMPLETED SUCCESSFULLY! (DWH + MinIO)")
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
