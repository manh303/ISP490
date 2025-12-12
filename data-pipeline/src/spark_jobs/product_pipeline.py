"""
Product Pipeline - Processes product data only
Optimized for memory usage by focusing on products without reviews
"""

from spark_utils import *

print("=" * 60)
print("PRODUCT PIPELINE - Products Only")
print("=" * 60)

# Note: Import all product-specific functions from original load_cleaned_from_minio.py
# This is a simplified version - full implementation would include all functions

def main():
    """Main product pipeline workflow"""
    print("\n" + "=" * 60)
    print("✓ PRODUCT PIPELINE START")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = create_spark_session("EcommerceDSS-ProductPipeline")

    try:
        # Import product functions from original file
        # For now, using exec to import from original
        # In production, these would be copied here
        
        import importlib.util
        import sys
        
        # Load original file temporarily
        original_path = "/app/src/spark_jobs/load_cleaned_from_minio.py"
        spec = importlib.util.spec_from_file_location("original", original_path)
        original = importlib.util.module_from_spec(spec)
        sys.modules["original"] = original
        spec.loader.exec_module(original)
        
        print("\n✓ Loaded product processing functions")
        
        # Execute product pipeline (from original main, lines 2998-3059)
        df_raw = original.load_raw_data(spark)
        if df_raw is None:
            print("❌ Failed to load raw data")
            return 1

        # Cache raw data for reuse
        df_raw.persist(StorageLevel.MEMORY_AND_DISK)
        print("✓ Raw data cached (MEMORY_AND_DISK)")
        
        df_cleaned = original.clean_data(df_raw)
        if df_cleaned is None:
            print("❌ Failed to clean data")
            return 1
        
        # Unpersist raw data (no longer needed)
        df_raw.unpersist()
        print("✓ Raw data unpersisted")

        df_mapped = original.map_categories(df_cleaned)
        df_std = original.standardize_data(df_mapped)
        df_synced = original.synchronize_identifiers(df_std)

        # Persist and force evaluation to break lineage
        df_dedup = original.deduplicate_data(df_synced)
        if df_dedup is None:
            print("❌ Failed to deduplicate data")
            return 1
        
        df_dedup.persist(StorageLevel.MEMORY_AND_DISK)
        dedup_count = df_dedup.count()  # Force evaluation
        print(f"✓ Deduplication completed and cached: {dedup_count:,} records")

        original.validate_data(df_dedup)

        # Prepare for DWH
        df_for_dwh = df_dedup.withColumn("price", col("price_current_vnd"))

        # Connect and load to DWH
        conn = get_db_connection()
        ensure_star_schema(conn)
        
        mappings = original.load_dimensions(df_for_dwh, conn)
        original.load_fact_product_daily(df_for_dwh, conn, mappings)
        
        conn.close()

        # Save cleaned data to MinIO
        if not original.save_cleaned_data(df_dedup, spark):
            print("❌ Failed to save cleaned data")
            return 1

        print("\n" + "=" * 60)
        print("✓ PRODUCT PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        spark.stop()
        print("\n✓ Spark session closed")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
