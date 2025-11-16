#!/usr/bin/env python3
"""
Spark-based Data Cleaning Pipeline
Đọc từ local hoặc MinIO → Clean & Transform → Deduplicate → Lưu vào local
"""
import os
import sys
import glob
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, concat, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, LongType, ArrayType
)

load_dotenv()

# Ép stdout dùng UTF-8 trên Windows
if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# MinIO Configuration (use 'minio:9000' for Docker, 'localhost:9000' for local)
MINIO_HOST = os.getenv('MINIO_HOST', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
MINIO_RAW_BUCKET = os.getenv('MINIO_RAW_BUCKET', 'crawler-data')
MINIO_SECURE = os.getenv('MINIO_SECURE', 'false').lower() == 'true'

# S3 Configuration for Spark
S3_ENDPOINT = f"http://{MINIO_HOST}" if not MINIO_SECURE else f"https://{MINIO_HOST}"
S3_ACCESS_KEY = MINIO_ACCESS_KEY
S3_SECRET_KEY = MINIO_SECRET_KEY

# Output Configuration
CRAWLER_OUTPUT_DIR = os.getenv('CRAWLER_OUTPUT_DIR', '/app/data/outputs')
# Save cleaned data to MinIO via S3A
MINIO_CLEANED_BUCKET = os.getenv('MINIO_CLEANED_BUCKET', 'cleaned-data')
SAVE_TO_MINIO = os.getenv('SAVE_TO_MINIO', 'true').lower() == 'true'


def create_spark_session():
    """Create Spark session with MinIO/S3 configuration"""
    print("[INFO] Creating Spark session...")

    # Suppress Hadoop native library warnings on Windows
    if os.name == "nt":
        os.environ["HADOOP_HOME"] = os.environ.get("HADOOP_HOME", "C:/hadoop")
        # Disable native Windows access0 toàn cục
        os.environ["HADOOP_OPTS"] = "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false"
        os.environ["JAVA_TOOL_OPTIONS"] = "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false"

    spark = (
        SparkSession.builder
        .appName("EcommerceDSS-DataCleaning")
        # S3 / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(MINIO_SECURE).lower())
        # TẮT native lib Hadoop để nó không dùng NativeIO$Windows
        .config("spark.hadoop.io.native.lib.available", "false")
        # Apply JVM option cho cả driver & executor
        .config(
            "spark.driver.extraJavaOptions",
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false"
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false"
        )
        # Các config khác của bạn giữ nguyên
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print(" Spark session created")

    return spark


def load_raw_data(spark):
    """Load raw JSONL data from local folder"""
    print("\n" + "="*60)
    print(" STEP 1: LOADING RAW DATA")
    print("="*60)
    
    try:
        # Try local folder first
        local_data_path = "/app/data/crawler_output"
        local_files = glob.glob(f"{local_data_path}/**/*.jsonl", recursive=True)
        
        if not local_files:
            # Try data folder
            local_files = glob.glob(f"/app/data/**/*.jsonl", recursive=True)
        
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
            # Try with inferSchema first
            print("\n[INFO] Attempting to load with schema inference...")
            df = spark.read \
                .option("inferSchema", "true") \
                .option("multiline", "false") \
                .json(local_files)
            
            count = df.count()
            print(f" Loaded {count:,} raw records")
            print(f"   Schema: {len(df.columns)} columns")
            df.printSchema()
            
            return df
            
        except Exception as e:
            print(f"  Schema inference failed: {e}")
            print("[INFO] Attempting without schema...")
            
            # Fallback: load without schema
            df = spark.read \
                .option("multiline", "false") \
                .json(local_files)
            
            count = df.count()
            print(f" Loaded {count:,} raw records")
            print(f"   Schema: {len(df.columns)} columns")
            
            return df
        
    except Exception as e:
        print(f" Failed to load raw data: {e}")
        import traceback
        traceback.print_exc()
        return None


def clean_data(df):
    """Clean and standardize product data"""
    print("\n" + "="*60)
    print(" STEP 2: CLEANING & TRANSFORMING DATA")
    print("="*60)
    
    try:
        # Extract numeric values from prices
        df_cleaned = df \
            .withColumn("product_id", col("product_id")) \
            .withColumn("global_product_id", 
                concat(col("source"), lit("_"), col("product_id"))) \
            .withColumn("source_platform", col("source")) \
            .withColumn("product_name", when(col("product_name").isNotNull(), trim(col("product_name"))).otherwise("Unknown")) \
            .withColumn("brand_name", 
                when(col("brand").isNotNull(), trim(col("brand"))).otherwise("Unknown")) \
            .withColumn("price_current", 
                when(col("price_current").isNotNull(),
                    regexp_replace(col("price_current"), "[^0-9]", "").cast(LongType())
                ).otherwise(0)) \
            .withColumn("price_original",
                when(col("price_original").isNotNull(),
                    regexp_replace(col("price_original"), "[^0-9]", "").cast(LongType())
                ).otherwise(0)) \
            .withColumn("discount_percent",
                when(col("discount_percent").isNotNull(),
                    regexp_replace(col("discount_percent"), "[^0-9.]", "").cast(DoubleType())
                ).otherwise(0.0)) \
            .withColumn("data_quality_score", 
                when((col("product_name").isNotNull()) & (col("price_current") > 0), 1.0).otherwise(0.0))
        
        # Select only available columns
        available_cols = df_cleaned.columns
        select_cols = [c for c in ["global_product_id", "source_platform", "product_id", "product_name",
                                    "brand_name", "category", "price_current", "price_original",
                                    "discount_percent", "review_count", "seller_name",
                                    "url", "crawl_date", "data_quality_score"] if c in available_cols]
        
        df_cleaned = df_cleaned.select(*select_cols)
        
        cleaned_count = df_cleaned.count()
        print(f" Cleaned {cleaned_count:,} records")
        
        return df_cleaned
        
    except Exception as e:
        print(f" Error during cleaning: {e}")
        import traceback
        traceback.print_exc()
        return None


def deduplicate_data(df):
    """Remove duplicates based on global_product_id"""
    print("\n" + "="*60)
    print(" STEP 3: DEDUPLICATION")
    print("="*60)
    
    try:
        # Remove complete duplicates
        df_deduplicated = df.dropDuplicates(['global_product_id'])
        
        # Count duplicates removed
        original_count = df.count()
        deduplicated_count = df_deduplicated.count()
        duplicates_removed = original_count - deduplicated_count
        
        print(f" Deduplicated data:")
        print(f"   Original: {original_count:,} records")
        print(f"   After dedup: {deduplicated_count:,} records")
        print(f"   Removed: {duplicates_removed:,} duplicates")
        
        return df_deduplicated
        
    except Exception as e:
        print(f" Error during deduplication: {e}")
        return None


def validate_data(df):
    """Validate cleaned data quality"""
    print("\n" + "="*60)
    print(" STEP 4: DATA VALIDATION")
    print("="*60)
    
    try:
        total_records = df.count()
        
        # Check required fields
        valid_records = df.filter(
            (col("product_name").isNotNull()) & 
            (col("price_current") > 0)
        ).count()
        
        # Check missing fields
        missing_product_name = df.filter(col("product_name").isNull()).count()
        missing_price = df.filter(col("price_current") <= 0).count()
        missing_brand = df.filter(col("brand_name").isNull()).count()
        
        print(f"\n Data Quality Report:")
        print(f"  Total records: {total_records:,}")
        print(f"  Valid records: {valid_records:,} ({valid_records/total_records*100:.1f}%)")
        print(f"  Missing product_name: {missing_product_name:,}")
        print(f"  Missing/invalid price: {missing_price:,}")
        print(f"  Missing brand: {missing_brand:,}")
        
        # Show data distribution by source
        print(f"\n Records by source:")
        source_dist = df.groupBy("source_platform").count().collect()
        for row in source_dist:
            print(f"  {row['source_platform']}: {row['count']:,}")
        
        return True
        
    except Exception as e:
        print(f"  Validation error: {e}")
        return True


def save_cleaned_data(df, spark):
    """Save cleaned data locally as Parquet, then upload to MinIO"""
    print("\n" + "="*60)
    print(" STEP 5: SAVING CLEANED DATA")
    print("="*60)
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_dir = '/tmp/cleaned_data'
        os.makedirs(local_dir, exist_ok=True)
        
        # Save locally as Parquet (more efficient)
        local_path = f"{local_dir}/cleaned_{timestamp}"
        print(f"[INFO] Writing to local: {local_path}")
        df.coalesce(4).write \
            .mode("overwrite") \
            .parquet(local_path)
        
        count = df.count()
        print(f" Saved cleaned data locally:")
        print(f"   Path: {local_path}")
        print(f"   Format: Parquet")
        print(f"   Total records: {count:,}")
        
        # Upload to MinIO if enabled
        if SAVE_TO_MINIO:
            upload_to_minio(local_path, f"cleaned_{timestamp}/")
        
        return True
        
    except Exception as e:
        print(f" Error saving data: {e}")
        import traceback
        traceback.print_exc()
        return False


def upload_to_minio(local_path, minio_prefix):
    """Upload local Parquet files to MinIO"""
    try:
        from minio import Minio
        from pathlib import Path
        
        minio_client = Minio(MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=MINIO_SECURE)
        
        # Create bucket if not exists
        if not minio_client.bucket_exists(MINIO_CLEANED_BUCKET):
            minio_client.make_bucket(MINIO_CLEANED_BUCKET)
            print(f"[INFO] Bucket created: {MINIO_CLEANED_BUCKET}")
        
        # Upload all parquet files
        local_path = Path(local_path)
        uploaded = 0
        
        for parquet_file in local_path.rglob("*.parquet"):
            remote_path = f"{minio_prefix}{parquet_file.name}"
            print(f"[INFO] Uploading to MinIO: {remote_path}")
            minio_client.fput_object(
                MINIO_CLEANED_BUCKET,
                remote_path,
                str(parquet_file)
            )
            uploaded += 1
        
        print(f" Uploaded {uploaded} files to MinIO: s3a://{MINIO_CLEANED_BUCKET}/{minio_prefix}")
        return True
        
    except Exception as e:
        print(f" Error uploading to MinIO: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main pipeline"""
    print("\n" + "="*60)
    print(" SPARK DATA CLEANING PIPELINE")
    print(f"Started: {datetime.now().isoformat()}")
    print("="*60)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Step 1: Load raw data
        df_raw = load_raw_data(spark)
        if df_raw is None:
            print(" Failed to load raw data")
            return 1
        
        # Step 2: Clean data
        df_cleaned = clean_data(df_raw)
        if df_cleaned is None:
            print(" Failed to clean data")
            return 1
        
        # Step 3: Deduplicate
        df_dedup = deduplicate_data(df_cleaned)
        if df_dedup is None:
            print(" Failed to deduplicate data")
            return 1
        
        # Step 4: Validate
        validate_data(df_dedup)
        
        # Step 5: Save cleaned data
        if not save_cleaned_data(df_dedup, spark):
            print(" Failed to save cleaned data")
            return 1
        
        print("\n" + "="*60)
        print(" PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        
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
