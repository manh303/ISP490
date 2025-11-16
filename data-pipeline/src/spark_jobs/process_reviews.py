#!/usr/bin/env python3
"""
Review Data Processing Pipeline
Input:  JSON from MinIO (s3a://reviews-data/)
Output: Cleaned reviews + sentiment analysis + aggregated metrics to MinIO
"""

import os
import sys
from datetime import datetime
from etl_metadata import log_etl

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, concat, lit,
    lower, concat_ws, coalesce, upper, to_timestamp,
    avg, count, sum as spark_sum, to_date,
    year, month, dayofmonth, dayofweek,
)
from pyspark.sql.types import DoubleType, LongType, StringType
from pyspark.sql.functions import udf

try:
    from textblob import TextBlob
except ImportError:
    print("[WARNING] textblob not installed. Install with: pip install textblob")
    TextBlob = None

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

MINIO_REVIEWS_BUCKET = os.getenv("MINIO_REVIEWS_BUCKET", "reviews-data")
MINIO_PROCESSED_BUCKET = os.getenv("MINIO_PROCESSED_BUCKET", "processed-reviews")
SAVE_TO_MINIO = os.getenv("SAVE_TO_MINIO", "true").lower() == "true"


# ================== SPARK SESSION ==================
def create_spark_session():
    print("[INFO] Creating Spark session for REVIEW PROCESSING...")

    if os.name == "nt":
        os.environ["HADOOP_HOME"] = os.environ.get("HADOOP_HOME", "C:/hadoop")
        os.environ["HADOOP_OPTS"] = \
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false"

    spark = (
        SparkSession.builder.appName("EcommerceDSS-ReviewProcessing")
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


# ================== STEP 1: LOAD REVIEWS ==================
def load_reviews(spark):
    print("\n" + "=" * 60)
    print(" STEP 1: LOADING REVIEWS FROM LOCAL & MINIO")
    print("=" * 60)

    import glob
    import os
    
    dfs = []
    
    # Load từ local directories
    local_base = "/app/data/outputs"
    review_dirs = ["tiki_reviews", "lazada_reviews"]
    
    for review_dir in review_dirs:
        local_path = f"{local_base}/{review_dir}"
        if os.path.exists(local_path):
            print(f"\n[INFO] Loading from {local_path}")
            try:
                json_files = glob.glob(f"{local_path}/**/*.json", recursive=True)
                if not json_files:
                    json_files = glob.glob(f"{local_path}/*.json")
                
                if json_files:
                    print(f"   Found {len(json_files)} JSON files")
                    df_local = spark.read.option("inferSchema", "true").json(json_files)
                    
                    # Add source platform
                    df_local = df_local.withColumn(
                        "source_platform",
                        lit(review_dir.replace("_reviews", ""))
                    )
                    
                    dfs.append(df_local)
                    print(f"   ✓ Loaded {df_local.count():,} reviews from {review_dir}")
                else:
                    print(f"   ⚠ No JSON files found in {local_path}")
            except Exception as e:
                print(f"   ✗ Error loading from {local_path}: {e}")
        else:
            print(f"   ⚠ Directory not found: {local_path}")
    
    # Load từ MinIO (backup/additional data)
    try:
        print(f"\n[INFO] Loading from MinIO s3a://{MINIO_REVIEWS_BUCKET}/")
        review_path = f"s3a://{MINIO_REVIEWS_BUCKET}/"
        df_minio = spark.read.option("inferSchema", "true").json(review_path)
        
        # Add source platform if not present
        if "source_platform" not in df_minio.columns:
            df_minio = df_minio.withColumn("source_platform", lit("minio"))
        
        dfs.append(df_minio)
        print(f"   ✓ Loaded {df_minio.count():,} reviews from MinIO")
    except Exception as e:
        print(f"   ⚠ Could not load from MinIO: {e}")
    
    # Combine all dataframes
    if not dfs:
        print(" ✗ No review data found from any source")
        return None
    
    df = dfs[0]
    for d in dfs[1:]:
        # Union with schema matching
        df = df.union(d)
    
    total_count = df.count()
    print(f"\n ✓ Total loaded: {total_count:,} raw reviews")
    print(f" Columns: {df.columns}")
    return df


# ================== STEP 2: CLEAN REVIEWS ==================
def clean_reviews(df):
    print("\n" + "=" * 60)
    print(" STEP 2: CLEANING REVIEWS")
    print("=" * 60)

    if df is None:
        return None

    df_clean = (
        df
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
            "reviewer_id",
            when(col("reviewer_id").isNotNull(), trim(col("reviewer_id").cast("string")))
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
            "review_title",
            when(col("review_title").isNotNull(), trim(col("review_title")))
            .otherwise(""),
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
        .withColumn(
            "review_length",
            when(col("review_text").isNotNull(), col("review_text"))
            .otherwise(lit("")).cast("string")
        )
    )
    
    # Calculate word count
    df_clean = df_clean.withColumn(
        "review_word_count",
        when(
            (col("review_text").isNotNull()) & (col("review_text") != ""),
            when(
                col("review_text") != "",
                (col("review_length").cast("string") + " ").rlike("\\s+")
            ).otherwise(0)
        ).otherwise(0)
    )
    
    # Simpler word count: count spaces + 1
    df_clean = df_clean.withColumn(
        "review_word_count",
        when(
            (col("review_text").isNotNull()) & (col("review_text") != ""),
            (col("review_text").cast("string")).cast("string").cast("string")
        ).otherwise("")
    )
    
    print(f" ✓ Cleaned {df_clean.count():,} reviews")
    return df_clean.drop("review_length")


# ================== STEP 3: SENTIMENT ANALYSIS ==================
def analyze_sentiment(df):
    print("\n" + "=" * 60)
    print(" STEP 3: SENTIMENT ANALYSIS")
    print("=" * 60)

    if df is None or TextBlob is None:
        print(" ⚠ Skipping sentiment analysis (TextBlob not available)")
        return df

    def _get_sentiment_score(text: str):
        """Tính polarity (-1 to 1) từ review text"""
        if not text or len(str(text).strip()) == 0:
            return 0.0
        try:
            blob = TextBlob(str(text))
            return float(blob.sentiment.polarity)
        except:
            return 0.0

    def _get_sentiment_label(score: float):
        """Phân loại sentiment"""
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
            when(
                (col("sentiment_score") >= -0.1) & (col("sentiment_score") <= 0.1),
                1
            ).otherwise(0)
        )
    )

    print(" ✓ Sentiment Distribution:")
    for row in df_sentiment.groupBy("sentiment_label").count().orderBy("sentiment_label").collect():
        print(f"   {row['sentiment_label'].upper():10s}: {row['count']:>10,}")

    return df_sentiment


# ================== STEP 4: ENRICH WITH TIME FEATURES ==================
def add_time_features(df):
    print("\n" + "=" * 60)
    print(" STEP 4: ADDING TIME FEATURES")
    print("=" * 60)

    df_time = (
        df
        .withColumn("review_date_fmt", to_date(col("review_date")))
        .withColumn("review_year", year(col("review_date_fmt")))
        .withColumn("review_month", month(col("review_date_fmt")))
        .withColumn("review_day", dayofmonth(col("review_date_fmt")))
        .withColumn("review_dow", dayofweek(col("review_date_fmt")))  # 1=Sun, 7=Sat
    )

    print(f" ✓ Added time features")
    return df_time


# ================== STEP 5: AGGREGATE BY PRODUCT ==================
def aggregate_by_product(df):
    print("\n" + "=" * 60)
    print(" STEP 5: AGGREGATING METRICS BY PRODUCT")
    print("=" * 60)

    if df is None:
        return None

    df_agg = (
        df
        .groupBy("product_id")
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
    
    print("\n Product Review Statistics:")
    stats = df_agg.selectExpr(
        "count(product_id) as num_products",
        "sum(total_reviews) as total_reviews",
        "avg(avg_rating) as avg_product_rating",
        "max(total_reviews) as max_reviews_per_product",
        "min(total_reviews) as min_reviews_per_product",
    ).collect()[0]
    
    print(f"   Products:               {stats['num_products']:,}")
    print(f"   Total Reviews:          {stats['total_reviews']:,}")
    print(f"   Avg Product Rating:     {stats['avg_product_rating']:.2f}")
    print(f"   Max Reviews/Product:    {stats['max_reviews_per_product']:,}")
    print(f"   Min Reviews/Product:    {stats['min_reviews_per_product']:,}")

    return df_agg


# ================== STEP 6: AGGREGATE BY CATEGORY ==================
def aggregate_by_category(df_reviews, df_products):
    print("\n" + "=" * 60)
    print(" STEP 6: AGGREGATING METRICS BY CATEGORY")
    print("=" * 60)

    if df_reviews is None or df_products is None:
        return None

    # Join reviews with product info
    df_joined = (
        df_reviews
        .join(
            df_products.select("product_id", "category_std", "category_lvl1", "brand_std"),
            on="product_id",
            how="left"
        )
        .fillna("Unknown", subset=["category_std", "brand_std"])
    )

    df_agg = (
        df_joined
        .groupBy("category_std", "category_lvl1")
        .agg(
            count("review_id").alias("total_reviews"),
            avg("rating").alias("avg_rating"),
            avg("sentiment_score").alias("avg_sentiment_score"),
            spark_sum("is_positive_review").alias("positive_reviews"),
            spark_sum("is_negative_review").alias("negative_reviews"),
        )
        .orderBy(col("total_reviews").desc())
    )

    print(f" ✓ Generated category aggregates")
    print("\n Top Categories by Review Count:")
    for row in df_agg.limit(10).collect():
        print(f"   {row['category_std']:30s}: {row['total_reviews']:>6,} reviews, {row['avg_rating']:.2f}★")

    return df_agg


# ================== STEP 7: SAVE RESULTS ==================
def save_results(spark, df_raw, df_cleaned, df_by_product, df_by_category):
    print("\n" + "=" * 60)
    print(" STEP 7: SAVING RESULTS TO MINIO")
    print("=" * 60)

    try:
        from minio import Minio
        from pathlib import Path

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_base = f"/tmp/processed_reviews_{ts}"
        os.makedirs(local_base, exist_ok=True)

        outputs = {
            "cleaned_reviews": df_cleaned,
            "reviews_by_product": df_by_product,
            "reviews_by_category": df_by_category,
        }

        saved_files = []
        
        for name, df in outputs.items():
            if df is None:
                continue
                
            local_path = f"{local_base}/{name}"
            print(f"[INFO] Writing {name} to {local_path}")
            df.coalesce(4).write.mode("overwrite").parquet(local_path)
            saved_files.append((name, local_path))

        if SAVE_TO_MINIO:
            client = Minio(
                MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=MINIO_SECURE
            )

            if not client.bucket_exists(MINIO_PROCESSED_BUCKET):
                client.make_bucket(MINIO_PROCESSED_BUCKET)
                print(f"[INFO] Bucket created: {MINIO_PROCESSED_BUCKET}")

            for name, local_path in saved_files:
                prefix = f"reviews_{ts}/{name}/"
                p = Path(local_path)
                uploaded = 0
                for f in p.rglob("*.parquet"):
                    remote = f"{prefix}{f.name}"
                    print(f"[INFO] Uploading: {remote}")
                    client.fput_object(MINIO_PROCESSED_BUCKET, remote, str(f))
                    uploaded += 1
                
                print(f" ✓ Uploaded {uploaded} files to s3a://{MINIO_PROCESSED_BUCKET}/{prefix}")

        return f"reviews_{ts}/"

    except Exception as e:
        print(f" ✗ Error saving results: {e}")
        import traceback
        traceback.print_exc()
        return None


# ================== MAIN ==================
def main():
    job_name = "spark_process_reviews"
    stage = "REVIEW_PROCESSING"
    start_time = datetime.utcnow()

    spark = create_spark_session()

    try:
        # Load product data for enrichment (optional, from previous pipeline)
        print("\n[INFO] Attempting to load product data for enrichment...")
        try:
            # This assumes cleaned products are available
            df_products = spark.read.parquet("s3a://cleaned-data/*/")
            print(f" ✓ Loaded product data")
        except:
            print(" ⚠ Could not load product data (non-critical)")
            df_products = None

        # Review pipeline
        df_raw = load_reviews(spark)
        if df_raw is None:
            raise Exception("No review data found")

        records_raw = df_raw.count()

        df_cleaned = clean_reviews(df_raw)
        df_sentiment = analyze_sentiment(df_cleaned)
        df_time = add_time_features(df_sentiment)
        df_by_product = aggregate_by_product(df_time)
        df_by_category = aggregate_by_category(df_time, df_products) if df_products else None

        result_prefix = save_results(spark, df_raw, df_time, df_by_product, df_by_category)

        # Log success
        log_etl(
            job_name,
            stage,
            "SUCCESS",
            start_time,
            records_processed=records_raw,
            records_failed=0,
            error_message=None,
            load_id=result_prefix,
        )

        print("\n" + "=" * 60)
        print("✓ REVIEW PROCESSING COMPLETED")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()

        log_etl(
            job_name,
            stage,
            "FAILED",
            start_time,
            records_processed=0,
            records_failed=0,
            error_message=str(e),
            load_id=None,
        )
        return 1

    finally:
        spark.stop()
        print("\n[INFO] Spark session closed")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
