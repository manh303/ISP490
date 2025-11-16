#!/usr/bin/env python3
"""
Job 1: Clean + Category Mapping + Standardization + Sync Identifier + Dedup
Input:  local JSONL (crawler output)
Output: cleaned parquet lên MinIO: s3a://cleaned-data/cleaned_<timestamp>/
"""

import os
import sys
import glob
from datetime import datetime
from etl_metadata import log_etl    

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, concat, lit,
    lower, concat_ws, coalesce, upper, to_timestamp,
    sha2, split, element_at, avg, count, sum as spark_sum,
    explode, array, collect_list, size, isnan, isnull,
)
from pyspark.sql.types import DoubleType, LongType, StringType, ArrayType
from pyspark.sql.functions import udf
from textblob import TextBlob

# ================== CONFIG ==================
load_dotenv()

if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

S3_ENDPOINT = f"http://{MINIO_HOST}" if not MINIO_SECURE else f"https://{MINIO_HOST}"
S3_ACCESS_KEY = MINIO_ACCESS_KEY
S3_SECRET_KEY = MINIO_SECRET_KEY

MINIO_CLEANED_BUCKET = os.getenv("MINIO_CLEANED_BUCKET", "cleaned-data")
SAVE_TO_MINIO = os.getenv("SAVE_TO_MINIO", "true").lower() == "true"

# Category mapping từ JSON bạn gửi
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


# ================== SPARK SESSION ==================
def create_spark_session():
    print("[INFO] Creating Spark session for CLEANING job...")

    if os.name == "nt":
        os.environ["HADOOP_HOME"] = os.environ.get("HADOOP_HOME", "C:/hadoop")
        os.environ["HADOOP_OPTS"] = \
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false"

    spark = (
        SparkSession.builder.appName("EcommerceDSS-CleanStandardize")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(MINIO_SECURE).lower(),
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ================== STEP 1: LOAD RAW ==================
def load_raw_data(spark):
    print("\n" + "=" * 60)
    print(" STEP 1: LOADING RAW DATA")
    print("=" * 60)

    local_data_path = "/app/data/crawler_output"
    local_files = glob.glob(f"{local_data_path}/**/*.jsonl", recursive=True)
    if not local_files:
        local_files = glob.glob("/app/data/**/*.jsonl", recursive=True)

    if not local_files:
        print(f"\n No JSONL files found in {local_data_path}")
        return None

    print(f"\n[INFO] Found {len(local_files)} local JSONL files")
    for f in local_files[:5]:
        print(f"   {f}")
    if len(local_files) > 5:
        print(f"   ... and {len(local_files) - 5} more")

    df = (
        spark.read.option("inferSchema", "true")
        .option("multiline", "false")
        .json(local_files)
    )
    print(f" Loaded {df.count():,} raw records")
    return df


# ================== STEP 2: CLEANING ==================
def clean_data(df):
    print("\n" + "=" * 60)
    print(" STEP 2: CLEANING & TRANSFORMING")
    print("=" * 60)

    df_cleaned = (
        df.withColumn("product_id", col("product_id"))
        .withColumn("global_product_id",
                    concat(col("source"), lit("_"), col("product_id")))
        .withColumn("source_platform", col("source"))
        .withColumn(
            "product_name",
            when(col("product_name").isNotNull(), trim(col("product_name")))
            .otherwise("Unknown"),
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

    cols = [
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
    df_cleaned = df_cleaned.select(*[c for c in cols if c in df_cleaned.columns])

    print(f" Cleaned {df_cleaned.count():,} records")
    return df_cleaned


# ================== STEP 2.5: CATEGORY MAPPING ==================
def map_categories(df):
    print("\n" + "=" * 60)
    print(" STEP 2.5: CATEGORY MAPPING")
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
    for row in df_mapped.groupBy("category_std").count().collect():
        print(f"  {row['category_std']}: {row['count']:,}")

    return df_mapped


# ================== STEP 2.6: LOAD & CLEAN REVIEWS ==================
def load_review_data(spark):
    """Load review data từ local directories + MinIO"""
    print("\n" + "=" * 60)
    print(" STEP 2.6: LOADING REVIEW DATA FROM LOCAL & MINIO")
    print("=" * 60)
    
    import glob
    import os as os_module
    
    dfs = []
    
    # Load từ local directories
    local_base = "/app/data/outputs"
    review_dirs = ["tiki_reviews", "lazada_reviews"]
    
    for review_dir in review_dirs:
        local_path = f"{local_base}/{review_dir}"
        if os_module.path.exists(local_path):
            print(f"\n[INFO] Loading from {local_path}")
            try:
                json_files = glob.glob(f"{local_path}/**/*.json", recursive=True)
                if not json_files:
                    json_files = glob.glob(f"{local_path}/*.json")
                
                if json_files:
                    print(f"   Found {len(json_files)} JSON files")
                    df_local = spark.read.option("inferSchema", "true").json(json_files)
                    df_local = df_local.withColumn(
                        "source_platform",
                        lit(review_dir.replace("_reviews", ""))
                    )
                    dfs.append(df_local)
                    print(f"   ✓ Loaded {df_local.count():,} reviews from {review_dir}")
            except Exception as e:
                print(f"   ✗ Error loading from {local_path}: {e}")
    
    # Load từ MinIO (backup)
    try:
        print(f"\n[INFO] Loading from MinIO s3a://reviews-data/")
        df_minio = spark.read.option("inferSchema", "true").json("s3a://reviews-data/")
        if "source_platform" not in df_minio.columns:
            df_minio = df_minio.withColumn("source_platform", lit("minio"))
        dfs.append(df_minio)
        print(f"   ✓ Loaded {df_minio.count():,} reviews from MinIO")
    except Exception as e:
        print(f"   ⚠ Could not load from MinIO: {e}")
    
    if not dfs:
        print(" ⚠ No review data found")
        return None
    
    df_reviews = dfs[0]
    for d in dfs[1:]:
        df_reviews = df_reviews.union(d)
    
    print(f"\n ✓ Total loaded: {df_reviews.count():,} raw reviews")
    return df_reviews


def clean_review_data(df_reviews):
    """Clean & standardize review data"""
    print("\n" + "=" * 60)
    print(" STEP 2.6.1: CLEANING REVIEWS")
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
            when(
                col("rating").isNotNull(),
                col("rating").cast(DoubleType())
            ).otherwise(0.0),
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
    
    print(f" Cleaned {df_clean.count():,} reviews")
    return df_clean


def sentiment_analysis(df_reviews):
    """Analyze sentiment từ review text"""
    print("\n" + "=" * 60)
    print(" STEP 2.6.2: SENTIMENT ANALYSIS")
    print("=" * 60)
    
    if df_reviews is None:
        return None
    
    def _get_sentiment_score(text: str):
        """Tính polarity (-1 to 1) từ review text"""
        if not text or len(text.strip()) == 0:
            return 0.0
        try:
            blob = TextBlob(text)
            return float(blob.sentiment.polarity)
        except:
            return 0.0
    
    def _get_sentiment_label(score: float):
        """Phân loại sentiment: negative, neutral, positive"""
        if score < -0.1:
            return "negative"
        elif score > 0.1:
            return "positive"
        else:
            return "neutral"
    
    sentiment_udf = udf(_get_sentiment_score, DoubleType())
    label_udf = udf(_get_sentiment_label, StringType())
    
    df_sentiment = (
        df_reviews
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
    )
    
    print(" Sentiment Distribution:")
    for row in df_sentiment.groupBy("sentiment_label").count().collect():
        print(f"   {row['sentiment_label'].upper()}: {row['count']:,}")
    
    return df_sentiment


def enrich_reviews(df_reviews, df_products):
    """Enrich reviews với product metadata"""
    print("\n" + "=" * 60)
    print(" STEP 2.6.3: ENRICHING REVIEWS WITH PRODUCT DATA")
    print("=" * 60)
    
    if df_reviews is None:
        return None
    
    # Join với product data
    product_cols = [
        "global_product_id_synced",
        "product_name_std",
        "brand_std",
        "category_std",
        "source_platform_std",
        "product_master_id",
        "sku_id",
    ]
    
    available_cols = [c for c in product_cols if c in df_products.columns]
    df_prod_subset = df_products.select("product_id", *available_cols).distinct()
    
    df_enriched = (
        df_reviews
        .join(
            df_prod_subset,
            on="product_id",
            how="left"
        )
        .withColumn("global_product_id_synced",
                   coalesce(col("global_product_id_synced"), 
                           concat(lit("unknown_"), col("product_id"))))
        .withColumn("product_name_std",
                   coalesce(col("product_name_std"), lit("Unknown")))
        .withColumn("brand_std",
                   coalesce(col("brand_std"), lit("UNKNOWN")))
        .withColumn("category_std",
                   coalesce(col("category_std"), lit("OTHER")))
    )
    
    print(f" Enriched {df_enriched.count():,} reviews")
    return df_enriched


def aggregate_review_metrics(df_reviews):
    """Tính aggregate metrics per product"""
    print("\n" + "=" * 60)
    print(" STEP 2.6.4: AGGREGATING REVIEW METRICS")
    print("=" * 60)
    
    if df_reviews is None:
        return None
    
    df_agg = (
        df_reviews
        .groupBy("global_product_id_synced", "product_name_std", "brand_std", "category_std")
        .agg(
            count("review_id").alias("total_reviews"),
            avg("rating").alias("avg_rating"),
            avg("sentiment_score").alias("avg_sentiment_score"),
            spark_sum("is_positive_review").alias("positive_reviews"),
            spark_sum("is_negative_review").alias("negative_reviews"),
            spark_sum("helpful_count").alias("total_helpful_count"),
            count(when(col("verified_purchase") == True, 1)).alias("verified_reviews"),
        )
        .withColumn("negative_sentiment_pct",
                   (col("negative_reviews") / col("total_reviews") * 100).cast(DoubleType()))
        .withColumn("positive_sentiment_pct",
                   (col("positive_reviews") / col("total_reviews") * 100).cast(DoubleType()))
    )
    
    print(f" Generated metrics for {df_agg.count():,} products")
    return df_agg


# ================== STEP 2.8: STANDARDIZATION ==================
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

    return df_std


# ================== STEP 2.9: SYNC IDENTIFIER ==================
def synchronize_identifiers(df):
    print("\n" + "=" * 60)
    print(" STEP 2.9: IDENTIFIER SYNCHRONIZATION")
    print("=" * 60)

    df_id = df.withColumn(
        "product_id_std",
        when(col("product_id").isNotNull(), trim(col("product_id").cast("string")))
        .otherwise(lit(None)),
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

    return df_id


# ================== STEP 3: DEDUP ==================
def deduplicate_data(df):
    print("\n" + "=" * 60)
    print(" STEP 3: DEDUPLICATION")
    print("=" * 60)

    key_col = (
        "global_product_id_synced"
        if "global_product_id_synced" in df.columns
        else "global_product_id"
    )
    df_dedup = df.dropDuplicates([key_col])

    print(" Deduplicated data:")
    print(f"   Key: {key_col}")
    print(f"   Original: {df.count():,}")
    print(f"   After:    {df_dedup.count():,}")
    return df_dedup


# ================== STEP 4: SAVE CLEANED TO MINIO ==================
def save_cleaned_data(df):
    """
    Lưu cleaned data vào local + MinIO
    Return:
        cleaned_prefix (str) nếu OK, None nếu fail
    """
    print("\n" + "=" * 60)
    print(" STEP 4: SAVING CLEANED DATA TO MINIO")
    print("=" * 60)

    try:
        from minio import Minio
        from pathlib import Path

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_dir = "/tmp/cleaned_data"
        os.makedirs(local_dir, exist_ok=True)

        local_path = f"{local_dir}/cleaned_{ts}"
        print(f"[INFO] Writing Parquet to local: {local_path}")
        df.coalesce(4).write.mode("overwrite").parquet(local_path)

        total = df.count()
        print(f"   Records: {total:,}")

        cleaned_prefix = f"cleaned_{ts}/"

        if SAVE_TO_MINIO:
            client = Minio(
                MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=MINIO_SECURE
            )

            if not client.bucket_exists(MINIO_CLEANED_BUCKET):
                client.make_bucket(MINIO_CLEANED_BUCKET)
                print(f"[INFO] Bucket created: {MINIO_CLEANED_BUCKET}")

            p = Path(local_path)
            uploaded = 0
            for f in p.rglob("*.parquet"):
                remote = f"{cleaned_prefix}{f.name}"
                print(f"[INFO] Uploading: {remote}")
                client.fput_object(MINIO_CLEANED_BUCKET, remote, str(f))
                uploaded += 1

            print(
                f" Uploaded {uploaded} files to s3a://{MINIO_CLEANED_BUCKET}/{cleaned_prefix}"
            )

        print(f"[OUTPUT] CLEANED_PREFIX={cleaned_prefix}")
        return cleaned_prefix, total

    except Exception as e:
        print(f" Error saving data: {e}")
        import traceback
        traceback.print_exc()
        return None, 0

# ================== MAIN ==================
def main():
    job_name = "spark_clean_standardize_syncid"
    stage = "CLEAN_STD_SYNC"
    start_time = datetime.utcnow()

    spark = create_spark_session()
    records_raw = 0
    records_final = 0
    cleaned_prefix = None

    try:
        # ===== PRODUCT DATA PIPELINE =====
        df_raw = load_raw_data(spark)
        if df_raw is None:
            log_etl(
                job_name, stage, "FAILED", start_time,
                records_processed=0,
                records_failed=0,
                error_message="No raw data found",
                load_id=None,
            )
            return 1

        records_raw = df_raw.count()

        df_clean = clean_data(df_raw)
        df_cat = map_categories(df_clean)
        df_std = standardize_data(df_cat)
        df_sync = synchronize_identifiers(df_std)
        df_dedup = deduplicate_data(df_sync)

        records_final = df_dedup.count()

        # ===== REVIEW DATA PIPELINE =====
        print("\n" + "=" * 60)
        print("STARTING REVIEW DATA PIPELINE")
        print("=" * 60)
        
        df_reviews_raw = load_review_data(spark)
        df_reviews_clean = clean_review_data(df_reviews_raw)
        df_reviews_sentiment = sentiment_analysis(df_reviews_clean)
        df_reviews_enriched = enrich_reviews(df_reviews_sentiment, df_dedup)
        df_review_metrics = aggregate_review_metrics(df_reviews_enriched)
        
        # Save both product and review data
        cleaned_prefix, saved_records = save_cleaned_data(df_dedup)
        
        if df_review_metrics is not None:
            review_prefix = f"{cleaned_prefix}reviews/"
            print(f"\n[INFO] Saving review metrics to {review_prefix}")
            if SAVE_TO_MINIO:
                try:
                    from minio import Minio
                    from pathlib import Path
                    
                    local_review_path = f"/tmp/cleaned_data/reviews"
                    df_review_metrics.coalesce(2).write.mode("overwrite").parquet(local_review_path)
                    
                    client = Minio(
                        MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=MINIO_SECURE
                    )
                    
                    p = Path(local_review_path)
                    for f in p.rglob("*.parquet"):
                        remote = f"{review_prefix}{f.name}"
                        print(f"[INFO] Uploading review data: {remote}")
                        client.fput_object(MINIO_CLEANED_BUCKET, remote, str(f))
                    
                    print(f" Uploaded review metrics to s3a://{MINIO_CLEANED_BUCKET}/{review_prefix}")
                except Exception as e:
                    print(f" Warning: Could not save review metrics: {e}")

        # Ghi metadata SUCCESS
        log_etl(
            job_name,
            stage,
            "SUCCESS",
            start_time,
            records_processed=records_final,
            records_failed=max(records_raw - records_final, 0),
            error_message=None,
            load_id=cleaned_prefix,
        )

        return 0

    except Exception as e:
        log_etl(
            job_name,
            stage,
            "FAILED",
            start_time,
            records_processed=records_final,
            records_failed=max(records_raw - records_final, 0),
            error_message=str(e),
            load_id=cleaned_prefix,
        )
        raise

    finally:
        spark.stop()
        print("\n Spark session closed")