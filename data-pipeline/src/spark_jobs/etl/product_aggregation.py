# src/spark_jobs/etl/product_aggregation.py

"""
Aggregate dữ liệu product theo ngày & lưu cleaned dataset.
"""

import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Import with fallback for both relative and absolute imports
try:
    from spark_jobs.etl.config import SAVE_TO_MINIO, MINIO_CLEANED_BUCKET, MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE
except ImportError:
    from .config import SAVE_TO_MINIO, MINIO_CLEANED_BUCKET, MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE


def aggregate_products_daily(df: DataFrame) -> DataFrame:
    """
    WHAT:
        Tổng hợp dữ liệu product theo ngày / product / platform.

    WHY:
        Tạo dataset grain daily làm input trực tiếp cho fact_product_daily.

    HOW:
        Group by (snapshot_date, global_product_id_synced, source_platform_std)
        -> tính min/max/avg/median/stddev price, total_review_count, avg_rating, snapshot_count
    """
    # Đảm bảo có cột price / review_count / rating
    df_agg = df
    if "price" not in df_agg.columns:
        df_agg = df_agg.withColumn("price", col("price_current_vnd"))
    if "review_count" not in df_agg.columns:
        df_agg = df_agg.withColumn("review_count", F.lit(0).cast("long"))
    if "rating" not in df_agg.columns:
        df_agg = df_agg.withColumn("rating", F.lit(None).cast(DoubleType()))

    # Aggregate
    agg_df = (
        df_agg.where(
            F.col("snapshot_date").isNotNull()
            & F.col("global_product_id_synced").isNotNull()
            & F.col("source_platform_std").isNotNull()
        )
        .groupBy("snapshot_date", "global_product_id_synced", "source_platform_std")
        .agg(
            F.count("*").alias("snapshot_count"),
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.expr("percentile_approx(price, 0.5)").alias("median_price"),
            F.stddev("price").alias("price_stddev"),
            F.sum(F.col("review_count")).alias("total_review_count"),
            F.avg("rating").alias("avg_rating"),
        )
    )

    return agg_df


def save_cleaned_products(df_dedup: DataFrame, spark) -> bool:
    """
    WHAT:
        Lưu cleaned dataset (sau dedup) về dạng Parquet, (và upload MinIO nếu bật).

    WHY:
        - Là "golden copy" của dữ liệu chuẩn
        - Dùng cho debug, training ML, reload DWH sau này

    HOW:
        - Ghi Parquet ra thư mục /tmp/cleaned_data/cleaned_YYYYMMDD_HHMMSS
        - Nếu SAVE_TO_MINIO = True -> upload lên MINIO_CLEANED_BUCKET
    """
    print("\n" + "=" * 60)
    print(" STEP 7: SAVING CLEANED DATA")
    print("=" * 60)

    try:
        from minio import Minio

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_dir = "/tmp/cleaned_data"
        os.makedirs(local_dir, exist_ok=True)

        local_path = f"{local_dir}/cleaned_{timestamp}"
        print(f"[INFO] Writing to local: {local_path}")
        df_dedup.coalesce(4).write.mode("overwrite").parquet(local_path)

        count_ = df_dedup.count()
        print(" Saved cleaned data locally:")
        print(f"   Path: {local_path}")
        print("   Format: Parquet")
        print(f"   Total records: {count_:,}")

        if SAVE_TO_MINIO:
            minio_client = Minio(
                MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=MINIO_SECURE
            )
            if not minio_client.bucket_exists(MINIO_CLEANED_BUCKET):
                minio_client.make_bucket(MINIO_CLEANED_BUCKET)
                print(f"[INFO] Bucket created: {MINIO_CLEANED_BUCKET}")

            local_path_obj = Path(local_path)
            uploaded = 0
            prefix = f"cleaned_{timestamp}/"
            for parquet_file in local_path_obj.rglob("*.parquet"):
                remote_path = f"{prefix}{parquet_file.name}"
                print(f"[INFO] Uploading to MinIO: {remote_path}")
                minio_client.fput_object(
                    MINIO_CLEANED_BUCKET,
                    remote_path,
                    str(parquet_file),
                )
                uploaded += 1

            print(
                f" Uploaded {uploaded} files to MinIO: s3a://{MINIO_CLEANED_BUCKET}/{prefix}"
            )

        return True

    except Exception as e:
        print(f" Error saving data: {e}")
        import traceback
        traceback.print_exc()
        return False
