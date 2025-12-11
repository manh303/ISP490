# src/spark_jobs/etl/convert_json_to_parquet.py

"""
Convert JSON/JSONL files in MinIO to Parquet format.
Run this ONCE to convert all existing data.

Usage:
  docker exec spark-master spark-submit /app/src/spark_jobs/etl/convert_json_to_parquet.py
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

from spark_jobs.etl.spark_session import create_spark_session

# MinIO bucket
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawler-data")


def convert_to_parquet():
    """
    Convert all JSON/JSONL files in MinIO to Parquet format.
    Original files are kept, Parquet saved to _parquet suffix paths.
    """
    print("\n" + "=" * 60)
    print(" CONVERT JSON → PARQUET")
    print(f" Started: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = None
    
    try:
        spark = create_spark_session()
        
        # Paths to convert
        conversions = [
            # Products
            (f"s3a://{MINIO_BUCKET}/tiki/", f"s3a://{MINIO_BUCKET}/tiki_parquet/"),
            (f"s3a://{MINIO_BUCKET}/lazada/", f"s3a://{MINIO_BUCKET}/lazada_parquet/"),
            # Reviews
            (f"s3a://{MINIO_BUCKET}/tiki_reviews/", f"s3a://{MINIO_BUCKET}/tiki_reviews_parquet/"),
            (f"s3a://{MINIO_BUCKET}/lazada_reviews/", f"s3a://{MINIO_BUCKET}/lazada_reviews_parquet/"),
        ]
        
        for json_path, parquet_path in conversions:
            print(f"\n[INFO] Converting: {json_path}")
            print(f"   → {parquet_path}")
            
            try:
                # Read JSON
                df = spark.read.option("inferSchema", "true").json(json_path)
                count = df.count()
                
                if count == 0:
                    print(f"   ⚠ No data found, skipping")
                    continue
                
                print(f"   ✓ Read {count:,} records")
                
                # Write as Parquet (partitioned by date if available)
                if "crawl_date" in df.columns:
                    df.write \
                        .mode("overwrite") \
                        .partitionBy("crawl_date") \
                        .parquet(parquet_path)
                    print(f"   ✓ Written as Parquet (partitioned by crawl_date)")
                else:
                    df.write \
                        .mode("overwrite") \
                        .parquet(parquet_path)
                    print(f"   ✓ Written as Parquet")
                
            except Exception as e:
                print(f"   ✗ Failed: {str(e)[:100]}")
        
        print("\n" + "=" * 60)
        print(" ✓ CONVERSION COMPLETED!")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\n ✗ Conversion failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        if spark:
            spark.stop()
            print("\n Spark session stopped")


if __name__ == "__main__":
    exit_code = convert_to_parquet()
    sys.exit(exit_code)
