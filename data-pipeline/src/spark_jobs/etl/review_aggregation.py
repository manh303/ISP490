# src/spark_jobs/etl/review_aggregation.py

"""
Aggregate dữ liệu review theo ngày & lưu kết quả.
"""

import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, to_date, avg, count
)
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import sum as spark_sum

from .config import (
    SAVE_TO_MINIO, MINIO_PROCESSED_REVIEWS_BUCKET,
    MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE
)


def aggregate_reviews_daily(df: DataFrame) -> DataFrame:
    """
    WHAT: Aggregate review data theo ngày/product/platform.
    
    WHY: Tạo dữ liệu aggregate cho fact_review_daily.
    """
    print("\n" + "=" * 60)
    print(" STEP 8.5: AGGREGATING REVIEWS DAILY")
    print("=" * 60)

    if df is None:
        return None

    has_sentiment = all(
        c in df.columns
        for c in [
            "sentiment_score",
            "is_positive_review",
            "is_negative_review",
            "is_neutral_review",
        ]
    )

    df_filtered = df.filter(col("review_date").isNotNull())
    df_with_date = df_filtered.withColumn("review_date_fmt", to_date(col("review_date")))

    df_agg = (
        df_with_date.groupBy("review_date_fmt", "global_product_id", "source_platform_std")
        .agg(
            count("review_id").alias("total_reviews"),
            avg("rating_std").alias("avg_rating"),
            count(when(col("rating_std") == 5.0, 1)).alias("five_star_count"),
            count(when(col("rating_std") == 4.0, 1)).alias("four_star_count"),
            count(when(col("rating_std") == 3.0, 1)).alias("three_star_count"),
            count(when(col("rating_std") == 2.0, 1)).alias("two_star_count"),
            count(when(col("rating_std") == 1.0, 1)).alias("one_star_count"),
            *(
                [
                    avg("sentiment_score").alias("avg_sentiment_score"),
                    spark_sum("is_positive_review").alias("positive_reviews"),
                    spark_sum("is_negative_review").alias("negative_reviews"),
                    spark_sum("is_neutral_review").alias("neutral_reviews"),
                ]
                if has_sentiment
                else [
                    lit(0.0).alias("avg_sentiment_score"),
                    lit(0).alias("positive_reviews"),
                    lit(0).alias("negative_reviews"),
                    lit(0).alias("neutral_reviews"),
                ]
            ),
            spark_sum("helpful_count").alias("total_helpful_count"),
        )
    )

    df_agg = (
        df_agg.withColumn(
            "negative_sentiment_pct",
            when(
                col("total_reviews") > 0,
                (col("negative_reviews") / col("total_reviews") * 100).cast(DoubleType()),
            ).otherwise(0.0),
        )
        .withColumn(
            "positive_sentiment_pct",
            when(
                col("total_reviews") > 0,
                (col("positive_reviews") / col("total_reviews") * 100).cast(DoubleType()),
            ).otherwise(0.0),
        )
        .withColumn(
            "review_quality_score",
            when(col("avg_sentiment_score") > 0.1, 1.0)
            .when(col("avg_sentiment_score") < -0.1, 0.5)
            .otherwise(0.75),
        )
    )

    final_cols = [
        col("review_date_fmt").alias("agg_date"),
        col("global_product_id"),
        col("source_platform_std"),
        col("total_reviews"),
        col("avg_rating"),
        col("five_star_count"),
        col("four_star_count"),
        col("three_star_count"),
        col("two_star_count"),
        col("one_star_count"),
        col("avg_sentiment_score"),
        col("positive_reviews"),
        col("negative_reviews"),
        col("neutral_reviews"),
        col("positive_sentiment_pct"),
        col("negative_sentiment_pct"),
        col("total_helpful_count"),
        col("review_quality_score"),
    ]

    df_agg = df_agg.select(*final_cols)
    df_agg = df_agg.filter(col("agg_date").isNotNull())

    print(
        f" ✓ Generated daily aggregates for {df_agg.count():,} product-date combinations"
    )
    return df_agg


def save_review_results(df_reviews: DataFrame, df_agg: DataFrame) -> bool:
    """
    WHAT: Lưu kết quả review analysis về Parquet và MinIO.
    
    WHY: Tạo "golden copy" của review data đã xử lý.
    """
    print("\n" + "=" * 60)
    print(" STEP 8.5: SAVING REVIEW RESULTS")
    print("=" * 60)

    if df_reviews is None:
        return False

    try:
        from pathlib import Path
        from minio import Minio

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_base = f"/tmp/reviews_processed_{ts}"
        os.makedirs(local_base, exist_ok=True)

        local_reviews = f"{local_base}/cleaned_reviews"
        print(f"[INFO] Writing cleaned reviews to {local_reviews}")
        df_reviews.coalesce(4).write.mode("overwrite").parquet(local_reviews)

        if df_agg is not None:
            local_agg = f"{local_base}/reviews_by_product"
            print(f"[INFO] Writing aggregates to {local_agg}")
            df_agg.coalesce(2).write.mode("overwrite").parquet(local_agg)

        if SAVE_TO_MINIO:
            minio_client = Minio(
                MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=MINIO_SECURE
            )

            if not minio_client.bucket_exists(MINIO_PROCESSED_REVIEWS_BUCKET):
                minio_client.make_bucket(MINIO_PROCESSED_REVIEWS_BUCKET)
                print(f"[INFO] Bucket created: {MINIO_PROCESSED_REVIEWS_BUCKET}")

            prefix = f"reviews_{ts}/"
            uploaded = 0

            for root, dirs, files in os.walk(local_base):
                for file in files:
                    if file.endswith(".parquet"):
                        local_file = os.path.join(root, file)
                        rel_path = os.path.relpath(root, local_base)
                        remote_path = f"{prefix}{rel_path}/{file}"
                        print(f"[INFO] Uploading: {remote_path}")
                        minio_client.fput_object(
                            MINIO_PROCESSED_REVIEWS_BUCKET,
                            remote_path,
                            local_file,
                        )
                        uploaded += 1

            print(
                f" ✓ Uploaded {uploaded} files to MinIO: s3a://{MINIO_PROCESSED_REVIEWS_BUCKET}/{prefix}"
            )

        return True

    except Exception as e:
        print(f" ✗ Error saving review results: {e}")
        import traceback
        traceback.print_exc()
        return False
