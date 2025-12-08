#!/usr/bin/env python3
"""
Job 2: Aggregation → Data Warehouse
Input:  cleaned parquet trên MinIO (bucket cleaned-data)
Output: dwh.fact_product_daily_agg (Postgres)
"""

import os
import sys
from datetime import datetime
from etl_metadata import log_etl

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date, coalesce, to_timestamp, col,
    countDistinct, avg, min as spark_min,
    max as spark_max, sum as spark_sum, lit,
)

# ================== CONFIG ==================
load_dotenv()

if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

S3_ENDPOINT = f"http://{MINIO_HOST}" if not MINIO_SECURE else f"https://{MINIO_HOST}"
S3_ACCESS_KEY = MINIO_ACCESS_KEY
S3_SECRET_KEY = MINIO_SECRET_KEY
MINIO_CLEANED_BUCKET = os.getenv("MINIO_CLEANED_BUCKET", "cleaned-data")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "ecommerce_dss")
DB_USER = os.getenv("DB_USER", "dss_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "dss_password_123")

DWH_SCHEMA = os.getenv("DWH_SCHEMA", "dwh")
FACT_TABLE = os.getenv("FACT_TABLE", "fact_product_daily_agg")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

TARGET_DATE = os.getenv("TARGET_DATE")  # sẽ được set bởi Airflow: {{ ds }}


# ================== SPARK SESSION ==================
def create_spark_session():
    print("[INFO] Creating Spark session for AGGREGATION job...")

    if os.name == "nt":
        os.environ["HADOOP_HOME"] = os.environ.get("HADOOP_HOME", "C:/hadoop")
        os.environ["HADOOP_OPTS"] = \
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false"

    spark = (
        SparkSession.builder.appName("EcommerceDSS-AggregationToDWH")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(MINIO_SECURE).lower(),
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        # JDBC driver Postgres
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dorg.apache.hadoop.io.nativeio.NativeIO$Windows.access0=false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ================== LOAD CLEANED FROM MINIO ==================
def load_cleaned_data(spark):
    print("\n" + "=" * 60)
    print(" STEP 1: LOADING CLEANED DATA FROM MINIO")
    print("=" * 60)

    cleaned_path = f"s3a://{MINIO_CLEANED_BUCKET}/"
    print(f"[INFO] Reading from: {cleaned_path}")

    df = spark.read.parquet(cleaned_path)
    print(f" Loaded {df.count():,} cleaned records")
    return df


# ================== AGGREGATION ==================
def aggregate_daily(df):
    print("\n" + "=" * 60)
    print(" STEP 2: DAILY AGGREGATION")
    print("=" * 60)

    df_with_date = df.withColumn(
        "agg_date",
        to_date(
            coalesce(col("crawl_ts"),
                     to_timestamp(col("crawl_date"), "yyyy-MM-dd"))
        ),
    )

    agg_df = (
        df_with_date.groupBy(
            "agg_date",
            "source_platform_std",
            "category_lvl1",
            "category_lvl2",
            "category_lvl3",
            "category_std",
        )
        .agg(
            countDistinct("global_product_id_synced").alias("distinct_products"),
            avg("price_current_vnd").alias("avg_price"),
            spark_min("price_current_vnd").alias("min_price"),
            spark_max("price_current_vnd").alias("max_price"),
            spark_sum(coalesce(col("review_count"), lit(0))).alias("total_review_count"),
        )
        .filter(col("agg_date").isNotNull())
    )

    if TARGET_DATE:
        print(f"[INFO] Filtering aggregation for date = {TARGET_DATE}")
        agg_df = agg_df.filter(col("agg_date") == TARGET_DATE)

    print(f" Aggregation result: {agg_df.count():,} rows")
    return agg_df


# ================== LOAD TO DWH ==================
def load_to_dwh(agg_df):
    print("\n" + "=" * 60)
    print(" STEP 3: LOADING TO DATA WAREHOUSE")
    print("=" * 60)

    table_full = f"{DWH_SCHEMA}.{FACT_TABLE}"

    (
        agg_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_full)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(f" ✅ Loaded aggregation into: {table_full}")


# ================== MAIN ==================
def main():
    job_name = "spark_aggregate_to_dwh"
    stage = "AGG_DWH"
    start_time = datetime.utcnow()

    spark = create_spark_session()
    agg_rows = 0

    try:
        df_cleaned = load_cleaned_data(spark)
        agg_df = aggregate_daily(df_cleaned)
        agg_rows = agg_df.count()

        if agg_rows == 0:
            msg = f"No data to load to DWH (TARGET_DATE={TARGET_DATE})"
            print(msg)
            log_etl(
                job_name,
                stage,
                "SUCCESS",  # technically vẫn SUCCESS nhưng 0 row
                start_time,
                records_processed=0,
                records_failed=0,
                error_message=msg,
                load_id=TARGET_DATE,
            )
            return 0

        load_to_dwh(agg_df)

        log_etl(
            job_name,
            stage,
            "SUCCESS",
            start_time,
            records_processed=agg_rows,
            records_failed=0,
            error_message=None,
            load_id=TARGET_DATE,
        )

        return 0

    except Exception as e:
        log_etl(
            job_name,
            stage,
            "FAILED",
            start_time,
            records_processed=agg_rows,
            records_failed=0,
            error_message=str(e),
            load_id=TARGET_DATE,
        )
        raise

    finally:
        spark.stop()
        print("\n Spark session closed")
