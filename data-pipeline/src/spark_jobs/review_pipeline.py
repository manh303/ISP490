"""
Review Pipeline - Processes review data only
Optimized for memory usage by focusing on reviews without products
"""

from spark_utils import *

print("=" * 60)
print("REVIEW PIPELINE - Reviews Only")
print("=" * 60)


def load_existing_mappings(conn):
    """Load dimension mappings created by product pipeline"""
    mappings = {}
    
    with conn.cursor() as cur:
        # Load date mappings
        cur.execute(f"SELECT date_value, date_sk FROM {DWH_SCHEMA}.dim_date")
        mappings['date_map'] = {str(row[0]): row[1] for row in cur.fetchall()}
        
        # Load product mappings
        cur.execute(f"SELECT product_key, product_sk FROM {DWH_SCHEMA}.dim_product")
        mappings['product_map'] = {row[0]: row[1] for row in cur.fetchall()}
        
        # Load platform mappings
        cur.execute(f"SELECT platform_code, platform_sk FROM {DWH_SCHEMA}.dim_platform")
        mappings['platform_map'] = {row[0]: row[1] for row in cur.fetchall()}
    
    print(f"✓ Loaded mappings: {len(mappings['date_map'])} dates, "
          f"{len(mappings['product_map'])} products, "
          f"{len(mappings['platform_map'])} platforms")
    
    return mappings


def main():
    """Main review pipeline workflow"""
    print("\n" + "=" * 60)
    print("✓ REVIEW PIPELINE START")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = create_spark_session("EcommerceDSS-ReviewPipeline")

    try:
        # Import review functions from original file
        import importlib.util
        import sys
        
        # Load original file temporarily
        original_path = "/app/src/spark_jobs/load_cleaned_from_minio.py"
        spec = importlib.util.spec_from_file_location("original", original_path)
        original = importlib.util.module_from_spec(spec)
        sys.modules["original"] = original
        spec.loader.exec_module(original)
        
        print("\n✓ Loaded review processing functions")
        
        # Execute review pipeline
        df_reviews_raw = original.load_review_data(spark)
        if df_reviews_raw is None:
            print("⚠️  No review data found - skipping")
            return 0

        df_reviews_clean = original.clean_review_data(df_reviews_raw)
        df_reviews_std = original.standardize_review_data(df_reviews_clean)
        df_reviews_synced = original.synchronize_review_identifiers(df_reviews_std)
        df_reviews_dedup = original.deduplicate_review_data(df_reviews_synced)
        original.validate_review_data(df_reviews_dedup)
        
        # Persist and force evaluation after sentiment analysis
        df_reviews_sentiment = original.analyze_sentiment(df_reviews_dedup)
        df_reviews_sentiment.persist(StorageLevel.MEMORY_AND_DISK)
        sentiment_count = df_reviews_sentiment.count()  # Force evaluation
        print(f"✓ Sentiment analysis completed and cached: {sentiment_count:,} reviews")
        
        df_reviews_time = original.add_review_time_features(df_reviews_sentiment)

        # Load dimension mappings from product pipeline
        conn = get_db_connection()
        mappings = load_existing_mappings(conn)
        
        # ⚠️ SKIP: dim_reviewer has duplicate issues - optional table
        # try:
        #     original.load_review_dimensions_to_dwh(df_reviews_dedup)
        # except:
        #     print("⚠️  Skipped review dimensions")
        print("⚠️  Skipping dim_reviewer (optional - has duplicates)")

        # Aggregate by date
        df_reviews_agg = original.aggregate_reviews_daily(df_reviews_time)

        # Load to DWH
        original.load_fact_review_star(df_reviews_time, conn, mappings)
        original.load_fact_review_daily_star(df_reviews_agg, conn, mappings)
        
        conn.close()

        # Save to MinIO
        original.save_review_results(df_reviews_dedup, df_reviews_agg)

        print("\n" + "=" * 60)
        print("✓ REVIEW PIPELINE COMPLETED SUCCESSFULLY!")
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
