# src/spark_jobs/etl/pipeline_main.py

"""
Main pipeline orchestrator - thay thế main() cũ trong load_cleaned_from_minio.py.

WHAT: Điều phối toàn bộ ETL pipeline (Products + Reviews → DWH)
WHY: Entry point chính để chạy pipeline, tách riêng logic orchestration
"""

import sys
import os
from datetime import datetime

# ============================================================
# PATH SETUP: Enable imports when running as standalone script
# This is needed because spark-submit runs the file directly
# ============================================================
_current_dir = os.path.dirname(os.path.abspath(__file__))
_spark_jobs_dir = os.path.dirname(_current_dir)  # spark_jobs
_src_dir = os.path.dirname(_spark_jobs_dir)       # src
_app_dir = os.path.dirname(_src_dir)              # app (or data-pipeline)

# Add to path if not already there
for _path in [_current_dir, _spark_jobs_dir, _src_dir, _app_dir]:
    if _path not in sys.path:
        sys.path.insert(0, _path)

# Now import from the etl package using absolute imports
from spark_jobs.etl.config import (
    PROCESS_REVIEWS,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)
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
from spark_jobs.etl.review_transforms import run_review_transform_pipeline
from spark_jobs.etl.review_aggregation import save_review_results
from spark_jobs.etl.metadata_utils import (
    load_review_dimensions_to_dwh, load_fact_review_star, load_fact_review_daily_star
)

import psycopg2
from pyspark import StorageLevel


def run_etl():
    """
    Orchestrate toàn bộ ETL:

    1. Extract raw từ MinIO
    2. Transform product (clean -> map category -> standardize -> sync ID -> dedup -> validate)
    3. Aggregate product daily & lưu cleaned
    4. Load dim_* & fact_product_daily vào DWH
    5. (Optional) chạy pipeline review & load fact_review*
    """
    print("\n" + "=" * 60)
    print(" FULL SPARK PIPELINE → DWH (STAR SCHEMA)")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = create_spark_session()

    try:
        # ===== 1. EXTRACT =====
        df_raw = load_raw_products(spark)
        if df_raw is None:
            print(" Failed to load raw data")
            return 1

        # ✅ OPTIMIZATION: Cache raw data for reuse
        df_raw.persist(StorageLevel.MEMORY_AND_DISK)
        print("✓ Raw data cached (MEMORY_AND_DISK)")
        
        # ===== 2. TRANSFORM PRODUCTS =====
        df_cleaned = clean_products(df_raw)
        if df_cleaned is None:
            print(" Failed to clean data")
            return 1
        
        # ✅ OPTIMIZATION: Unpersist raw data (no longer needed)
        df_raw.unpersist()
        print("✓ Raw data unpersisted")

        df_mapped = map_product_categories(df_cleaned)
        df_std = standardize_products(df_mapped)
        df_synced = sync_product_identifiers(df_std)

        # ===== 3. DEDUPLICATE =====
        df_dedup = deduplicate_products(df_synced)
        if df_dedup is None:
            print(" Failed to deduplicate data")
            return 1
        
        df_dedup.persist(StorageLevel.MEMORY_AND_DISK)
        dedup_count = df_dedup.count()  # Force evaluation
        print(f"✓ Deduplication completed and cached: {dedup_count:,} records")

        # ===== 4. VALIDATE =====
        validate_products(df_dedup)

        # Chuẩn bị thêm cột price cho star schema
        from pyspark.sql.functions import col
        df_for_dwh = df_dedup.withColumn("price", col("price_current_vnd"))

        # ===== 5. LOAD DWH =====
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

        # ===== 6. SAVE CLEANED DATA =====
        if not save_cleaned_products(df_dedup, spark):
            print(" Failed to save cleaned data")
            return 1

        # ===== 7. REVIEW DATA PIPELINE (OPTIONAL) =====
        if PROCESS_REVIEWS:
            print("\n" + "=" * 60)
            print("✓ STARTING REVIEW DATA PIPELINE")
            print("=" * 60)

            df_reviews_time, df_reviews_agg = run_review_transform_pipeline(spark, mappings)
            
            if df_reviews_time is not None:
                # Load dim_reviewer
                load_review_dimensions_to_dwh(df_reviews_time)

                # Load FACT tables
                try:
                    conn_reviews = psycopg2.connect(
                        host=DB_HOST,
                        port=DB_PORT,
                        database=DB_NAME,
                        user=DB_USER,
                        password=DB_PASSWORD,
                    )
                    load_fact_review_star(df_reviews_time, conn_reviews, mappings)
                    load_fact_review_daily_star(df_reviews_agg, conn_reviews, mappings)
                finally:
                    conn_reviews.close()

                # Lưu parquet + MinIO
                save_review_results(df_reviews_time, df_reviews_agg)
            else:
                print(" ⚠ Skipping review pipeline - no review data found")
        else:
            print("\n" + "=" * 60)
            print("⚠️ REVIEW PIPELINE SKIPPED")
            print("=" * 60)
            print("ℹ️  Reason: PROCESS_REVIEWS environment variable is not set to 'true'")
            print("ℹ️  To enable: Set PROCESS_REVIEWS=true in environment")
            print("ℹ️  This is a temporary measure to prevent OOM errors")
            print("=" * 60)

        print("\n" + "=" * 60)
        if PROCESS_REVIEWS:
            print("✓ PIPELINE COMPLETED SUCCESSFULLY! (Products + Reviews → DWH)")
        else:
            print("✓ PIPELINE COMPLETED SUCCESSFULLY! (Products Only → DWH)")
            print("ℹ️  Reviews skipped - set PROCESS_REVIEWS=true to enable")
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
    exit_code = run_etl()
    sys.exit(exit_code)
