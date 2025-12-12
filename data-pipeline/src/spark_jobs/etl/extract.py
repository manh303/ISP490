# src/spark_jobs/etl/extract.py

"""
WHAT: Hàm đọc raw product/review từ MinIO (S3A) vào Spark DataFrame.
WHY: Load từ MinIO Parquet/JSON nhẹ hơn local JSONL.
"""

import os
import glob
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, coalesce
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F

# Import with fallback for both relative and absolute imports
try:
    from spark_jobs.etl.config import (
        MINIO_HOST, S3_ENDPOINT, CRAWLER_OUTPUT_DIR
    )
except ImportError:
    from .config import (
        MINIO_HOST, S3_ENDPOINT, CRAWLER_OUTPUT_DIR
    )

# MinIO bucket name - where crawler data is stored
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawler-data")


def load_raw_products(spark: SparkSession) -> DataFrame:
    """
    Load raw product data từ MinIO (S3A protocol).
    
    Priority:
    1. MinIO s3a://crawler-data/
    2. Fallback: local files
    
    Returns:
        DataFrame chứa raw product data hoặc None nếu không tìm thấy
    """
    print("\n" + "=" * 60)
    print(" STEP 1: LOADING RAW DATA FROM MINIO")
    print("=" * 60)

    df = None
    
    # ==== OPTION 1: Load from MinIO ====
    # Try Parquet first (faster), then fallback to JSON
    minio_paths = [
        # Parquet paths (faster)
        (f"s3a://{MINIO_BUCKET}/tiki_parquet/", "parquet"),
        (f"s3a://{MINIO_BUCKET}/lazada_parquet/", "parquet"),
        # JSON paths (fallback)
        (f"s3a://{MINIO_BUCKET}/tiki/", "json"),
        (f"s3a://{MINIO_BUCKET}/lazada/", "json"),
    ]
    
    print(f"\n[INFO] Attempting to load from MinIO bucket: {MINIO_BUCKET}")
    print(f"   Endpoint: {S3_ENDPOINT}")
    
    dfs = []
    loaded_sources = set()  # Track which data sources we've loaded
    
    for path, fmt in minio_paths:
        # Skip if we already loaded this data source (e.g. tiki from parquet)
        source_name = path.split("/")[-2].replace("_parquet", "")
        if source_name in loaded_sources:
            continue
            
        try:
            if fmt == "parquet":
                df_part = spark.read.parquet(path)
            else:
                df_part = spark.read.option("inferSchema", "true").json(path)
            
            count = df_part.count()
            if count > 0:
                print(f"   ✓ Loaded {fmt.upper()} from {path}: {count:,} records")
                dfs.append(df_part)
                loaded_sources.add(source_name)
        except Exception as e:
            print(f"   ⚠ Skipping {path}: {str(e)[:80]}")
    
    if dfs:
        # Union all DataFrames
        df = dfs[0]
        for d in dfs[1:]:
            df = df.unionByName(d, allowMissingColumns=True)
        
        print(f"\n ✓ Loaded {df.count():,} records from MinIO")
        return df
    
    # ==== OPTION 2: Fallback to local files ====
    print("\n[INFO] MinIO load failed, falling back to local files...")
    return _load_from_local(spark)


def _load_from_local(spark: SparkSession) -> DataFrame:
    """Fallback: Load từ local JSONL files."""
    search_paths = [
        CRAWLER_OUTPUT_DIR,
        "/app/data/outputs",
        "/app/data/crawler_output",
        "/app/raw_data",
        "/app/data",
    ]
    
    all_files = []
    for base_path in search_paths:
        if not os.path.exists(base_path):
            continue
        jsonl_files = glob.glob(f"{base_path}/**/*.jsonl", recursive=True)
        json_files = glob.glob(f"{base_path}/**/*.json", recursive=True)
        # Exclude review files
        product_files = [f for f in jsonl_files + json_files if "review" not in f.lower()]
        if product_files:
            print(f"[INFO] Found {len(product_files)} product files in {base_path}")
            all_files.extend(product_files)
    
    if not all_files:
        print(" ✗ No product files found")
        return None

    print(f"\n[INFO] Loading {len(all_files)} local files...")
    try:
        df = spark.read.option("inferSchema", "true").option("multiline", "false").json(all_files)
        print(f" ✓ Loaded {df.count():,} raw records")
        return df
    except Exception as e:
        print(f" ✗ Failed: {e}")
        return None


def load_raw_reviews(spark: SparkSession) -> DataFrame:
    """
    Load raw review data từ MinIO.
    
    Priority:
    1. MinIO s3a://crawler-data/tiki_reviews/, lazada_reviews/
    2. Fallback: local files
    
    Returns:
        DataFrame chứa raw review data hoặc None
    """
    print("\n" + "=" * 60)
    print(" STEP 8: LOADING REVIEW DATA FROM MINIO")
    print("=" * 60)

    dfs = []
    
    # ==== MinIO paths - Parquet first, JSON fallback ====
    review_sources = [
        # Parquet (faster)
        ("tiki", f"s3a://{MINIO_BUCKET}/tiki_reviews_parquet/", "parquet"),
        ("lazada", f"s3a://{MINIO_BUCKET}/lazada_reviews_parquet/", "parquet"),
        # JSON (fallback)
        ("tiki", f"s3a://{MINIO_BUCKET}/tiki_reviews/", "json"),
        ("lazada", f"s3a://{MINIO_BUCKET}/lazada_reviews/", "json"),
    ]
    
    loaded_platforms = set()
    
    for platform, minio_path, fmt in review_sources:
        # Skip if already loaded this platform
        if platform in loaded_platforms:
            continue
            
        try:
            if fmt == "parquet":
                df_part = spark.read.parquet(minio_path)
            else:
                df_part = spark.read.option("inferSchema", "true").json(minio_path)
            
            count = df_part.count()
            if count > 0:
                df_part = df_part.withColumn("source_platform", lit(platform))
                dfs.append((platform, df_part))
                loaded_platforms.add(platform)
                print(f"   ✓ Loaded {count:,} reviews from MinIO {fmt.upper()}: {platform}")
        except Exception as e:
            print(f"   ⚠ Skipping {minio_path}: {str(e)[:80]}")
    
    # ==== Fallback to local ====
    if not dfs:
        print("\n[INFO] MinIO reviews not found, falling back to local...")
        return _load_reviews_from_local(spark)
    
    return _normalize_and_union_reviews(dfs)


def _load_reviews_from_local(spark: SparkSession) -> DataFrame:
    """Fallback: Load reviews từ local files."""
    dfs = []
    review_dirs = [
        ("tiki", f"{CRAWLER_OUTPUT_DIR}/tiki_reviews"),
        ("lazada", f"{CRAWLER_OUTPUT_DIR}/lazada_reviews"),
    ]

    for platform, local_path in review_dirs:
        if not os.path.exists(local_path):
            continue
        json_files = glob.glob(f"{local_path}/**/*.json", recursive=True)
        json_files.extend(glob.glob(f"{local_path}/**/*.jsonl", recursive=True))
        
        if json_files:
            try:
                df_local = spark.read.option("inferSchema", "true").json(json_files)
                df_local = df_local.withColumn("source_platform", lit(platform))
                dfs.append((platform, df_local))
                print(f"   ✓ Loaded {df_local.count():,} reviews from local: {platform}")
            except Exception as e:
                print(f"   ✗ Error: {e}")

    if not dfs:
        print(" ⚠ No review data found")
        return None
    
    return _normalize_and_union_reviews(dfs)


def _normalize_and_union_reviews(dfs: list) -> DataFrame:
    """Normalize schema và union các review DataFrames."""
    print("\n[INFO] Normalizing review schemas...")
    
    normalized_dfs = []
    
    for platform, df in dfs:
        print(f"   Processing {platform}...")
        
        # Common normalization
        df_norm = (
            df
            .withColumn("review_id", col("review_id").cast("string") if "review_id" in df.columns else lit(None))
            .withColumn("product_id", col("product_id").cast("string") if "product_id" in df.columns else lit(None))
            .withColumn("reviewer_name", 
                coalesce(col("reviewer_name"), lit("Anonymous")) if "reviewer_name" in df.columns else lit("Anonymous"))
            .withColumn("rating", 
                col("rating").cast(DoubleType()) if "rating" in df.columns else lit(0.0))
            .withColumn("review_text",
                coalesce(
                    col("review_text") if "review_text" in df.columns else lit(None),
                    col("content") if "content" in df.columns else lit(None),
                    lit("")
                ))
            .withColumn("review_date",
                coalesce(
                    col("review_date") if "review_date" in df.columns else lit(None),
                    col("crawl_date") if "crawl_date" in df.columns else lit(None),
                    lit("")
                ))
            .withColumn("helpful_count",
                col("helpful_count").cast("long") if "helpful_count" in df.columns else lit(0))
            .withColumn("verified_purchase", lit(False))
            .withColumn("crawl_date",
                col("crawl_date") if "crawl_date" in df.columns else lit(None))
        )
        
        # Select standard columns
        standard_cols = [
            "review_id", "product_id", "reviewer_name", "rating", 
            "review_text", "review_date", "helpful_count", 
            "verified_purchase", "source_platform", "crawl_date"
        ]
        available_cols = [c for c in standard_cols if c in df_norm.columns]
        df_norm = df_norm.select(available_cols)
        
        normalized_dfs.append(df_norm)
    
    # Union all
    df_reviews = normalized_dfs[0]
    for d in normalized_dfs[1:]:
        df_reviews = df_reviews.unionByName(d, allowMissingColumns=True)
    
    print(f"\n ✓ Total loaded: {df_reviews.count():,} raw reviews")
    return df_reviews
