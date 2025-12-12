# src/spark_jobs/etl/spark_session.py

"""
WHAT: Tạo SparkSession với cấu hình MinIO/S3, shuffle, memory...
WHY: Gom vào 1 chỗ để các pipeline khác (products, reviews) dùng chung.
"""

import os
from pyspark.sql import SparkSession

# Import with fallback for both relative and absolute imports
try:
    from spark_jobs.etl.config import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, MINIO_SECURE
except ImportError:
    from .config import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, MINIO_SECURE


def create_spark_session(app_name: str = "EcommerceDSS-FullPipeline") -> SparkSession:
    """
    Create and return a SparkSession configured to talk to MinIO (S3 compatible).

    - Thiết lập fs.s3a.* để Spark đọc/ghi được MinIO.
    - Set một số option performance (shuffle partitions, driver/executor memory...).
    """
    print("[INFO] Creating Spark session...")

    # Windows-specific Hadoop configuration
    if os.name == "nt":
        os.environ["HADOOP_HOME"] = os.environ.get("HADOOP_HOME", "C:/hadoop")
        os.environ["HADOOP_OPTS"] = (
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false"
        )

    spark = (
        SparkSession.builder.appName(app_name)
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(MINIO_SECURE).lower(),
        )
        # Adaptive, timezone, datetime parser
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.debug.maxToStringFields", "100")
        # Fix nativeIO on Windows / container
        .config(
            "spark.driver.extraJavaOptions",
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark session created")
    return spark
