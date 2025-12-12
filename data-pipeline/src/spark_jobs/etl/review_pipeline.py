# src/spark_jobs/etl/review_pipeline.py

"""
Review ETL Pipeline - Separate job to avoid OOM.
Runs AFTER product pipeline completes. Creates own Spark session.
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
from spark_jobs.etl.review_transforms import run_review_transform_pipeline
from spark_jobs.etl.review_aggregation import save_review_results
from spark_jobs.etl.metadata_utils import (
    load_review_dimensions_to_dwh, load_fact_review_star, load_fact_review_daily_star
)
from spark_jobs.etl.dwh_loader import load_dimensions
from spark_jobs.etl.extract import load_raw_products

import psycopg2
from pyspark import StorageLevel


def run_review_pipeline():
    """
    Review-only ETL pipeline.
    Creates Spark session, processes reviews, loads to DWH, then stops.
    MUST run after product pipeline to have product mappings available.
    """
    print("\n" + "=" * 60)
    print(" REVIEW PIPELINE (Job 2/2)")
    print(f" Started: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = None
    conn = None
    
    try:
        spark = create_spark_session()
        
        # ===== 1. GET PRODUCT MAPPINGS =====
        print("\n[INFO] Loading product mappings from existing products...")
        
        # We need to reload products briefly just to get the mappings
        # This is needed for linking reviews to products
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, 
            user=DB_USER, password=DB_PASSWORD
        )
        
        # Get mappings directly from database (more efficient)
        mappings = _get_mappings_from_db(conn)
        print(f"✓ Loaded mappings: {len(mappings.get('product_map', {}))} products")
        
        # ===== 2. RUN REVIEW TRANSFORM PIPELINE =====
        print("\n" + "=" * 60)
        print(" STEP 2: PROCESSING REVIEWS")
        print("=" * 60)
        
        df_reviews_time, df_reviews_agg = run_review_transform_pipeline(spark, mappings)
        
        if df_reviews_time is None:
            print(" ⚠ No review data found - skipping review loading")
            return 0  # Not an error, just no data

        review_count = df_reviews_time.count()
        print(f"✓ Processed {review_count:,} reviews")

        # ===== 3. LOAD TO DWH =====
        print("\n" + "=" * 60)
        print(" STEP 3: LOADING REVIEWS TO DWH")
        print("=" * 60)
        
        # Load dim_reviewer
        load_review_dimensions_to_dwh(df_reviews_time)
        
        # Load FACT tables
        load_fact_review_star(df_reviews_time, conn, mappings)
        load_fact_review_daily_star(df_reviews_agg, conn, mappings)

        # ===== 4. SAVE RESULTS =====
        save_review_results(df_reviews_time, df_reviews_agg)

        print("\n" + "=" * 60)
        print(" ✓ REVIEW PIPELINE COMPLETED!")
        print(f" Finished: {datetime.now().isoformat()}")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\n ✗ Review pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        if conn:
            conn.close()
        if spark:
            spark.stop()
            print("\n Spark session stopped - memory released")


def _get_mappings_from_db(conn):
    """
    Get all mappings directly from database instead of reprocessing products.
    Much more memory efficient.
    """
    mappings = {}
    cursor = conn.cursor()
    
    try:
        # Date mapping
        cursor.execute("SELECT date_value, date_sk FROM dwh.dim_date")
        mappings['date_map'] = {str(row[0]): row[1] for row in cursor.fetchall()}
        
        # Platform mapping
        cursor.execute("SELECT platform_name, platform_sk FROM dwh.dim_platform")
        mappings['platform_map'] = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Category mapping
        cursor.execute("SELECT category_key, category_sk FROM dwh.dim_category")
        mappings['category_map'] = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Product mapping (product_key -> product_sk)
        cursor.execute("SELECT product_key, product_sk FROM dwh.dim_product")
        mappings['product_map'] = {row[0]: row[1] for row in cursor.fetchall()}
        
        print(f"   ✓ date_map: {len(mappings['date_map'])} entries")
        print(f"   ✓ platform_map: {len(mappings['platform_map'])} entries")
        print(f"   ✓ category_map: {len(mappings['category_map'])} entries")
        print(f"   ✓ product_map: {len(mappings['product_map'])} entries")
        
    finally:
        cursor.close()
    
    return mappings


if __name__ == "__main__":
    exit_code = run_review_pipeline()
    sys.exit(exit_code)
