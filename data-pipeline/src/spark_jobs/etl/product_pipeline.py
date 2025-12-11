# src/spark_jobs/etl/product_pipeline.py

"""
Product ETL Pipeline - Separate job to avoid OOM.
Runs independently, creates own Spark session, stops when done.
"""

import sys
import os
from datetime import datetime

# PATH SETUP
_current_dir = os.path.dirname(os.path.abspath(__file__))
_spark_jobs_dir = os.path.dirname(_current_dir)
_src_dir = os.path.dirname(_spark_jobs_dir)
_app_dir = os.path.dirname(_src_dir)

for _path in [_current_dir, _spark_jobs_dir, _src_dir, _app_dir]:
    if _path not in sys.path:
        sys.path.insert(0, _path)

from spark_jobs.etl.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from spark_jobs.etl.spark_session import create_spark_session
from spark_jobs.etl.extract import load_raw_products
from spark_jobs.etl.product_transforms import (
    clean_products, map_product_categories, standardize_products,
    sync_product_identifiers, deduplicate_products, validate_products
)
from spark_jobs.etl.product_aggregation import save_cleaned_products
from spark_jobs.etl.dwh_loader import (
    ensure_star_schema, load_dimensions, load_fact_product_daily
)

import psycopg2
from pyspark import StorageLevel


def run_product_pipeline():
    """
    Product-only ETL pipeline.
    Creates Spark session, processes products, loads to DWH, then stops.
    """
    print("\n" + "=" * 60)
    print(" PRODUCT PIPELINE (Job 1/2)")
    print(f" Started: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = None
    conn = None
    
    try:
        spark = create_spark_session()
        
        # ===== 1. EXTRACT =====
        df_raw = load_raw_products(spark)
        if df_raw is None:
            print(" ✗ Failed to load raw data")
            return 1

        df_raw.persist(StorageLevel.MEMORY_AND_DISK)
        raw_count = df_raw.count()
        print(f"✓ Raw data loaded and cached: {raw_count:,} records")
        
        # ===== 2. TRANSFORM =====
        print("\n" + "=" * 60)
        print(" STEP 2: TRANSFORMING PRODUCTS")
        print("=" * 60)
        
        df_cleaned = clean_products(df_raw)
        if df_cleaned is None:
            print(" ✗ Failed to clean data")
            return 1
        
        # Free memory early
        df_raw.unpersist()
        
        df_mapped = map_product_categories(df_cleaned)
        df_std = standardize_products(df_mapped)
        df_synced = sync_product_identifiers(df_std)

        # ===== 3. DEDUPLICATE =====
        df_dedup = deduplicate_products(df_synced)
        if df_dedup is None:
            print(" ✗ Failed to deduplicate")
            return 1
        
        df_dedup.persist(StorageLevel.MEMORY_AND_DISK)
        dedup_count = df_dedup.count()
        print(f"✓ Deduplicated: {dedup_count:,} records")

        # ===== 4. VALIDATE =====
        validate_products(df_dedup)

        # Add price column for star schema
        from pyspark.sql.functions import col
        df_for_dwh = df_dedup.withColumn("price", col("price_current_vnd"))

        # ===== 5. LOAD TO DWH =====
        print("\n" + "=" * 60)
        print(" STEP 5: LOADING TO DWH")
        print("=" * 60)
        
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, 
            user=DB_USER, password=DB_PASSWORD
        )

        ensure_star_schema(conn)
        mappings = load_dimensions(df_for_dwh, conn)
        load_fact_product_daily(df_for_dwh, conn, mappings)

        # ===== 6. SAVE CLEANED DATA =====
        if not save_cleaned_products(df_dedup, spark):
            print(" ⚠ Warning: Failed to save cleaned products to MinIO")

        print("\n" + "=" * 60)
        print(" ✓ PRODUCT PIPELINE COMPLETED!")
        print(f" Finished: {datetime.now().isoformat()}")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\n ✗ Product pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        if conn:
            conn.close()
        if spark:
            spark.stop()
            print("\n Spark session stopped - memory released")


if __name__ == "__main__":
    exit_code = run_product_pipeline()
    sys.exit(exit_code)
