#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
staging_to_curated.py
Đọc Parquet staging (/app/raw_data/staging) → chuẩn hoá & dedup → ghi Parquet curated (/app/raw_data/curated)
"""

import argparse, os, datetime
from pyspark.sql import SparkSession, functions as F, types as T

def init_spark():
    spark = (
        SparkSession.builder.appName("StagingToCurated")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main(staging_path, curated_path, ingest_dt):
    spark = init_spark()
    # đọc từng bảng staging theo partition ingest_dt
    stg_products = spark.read.parquet(os.path.join(staging_path, "products")) \
                      .where(F.col("ingest_dt") == ingest_dt)
    stg_price    = spark.read.parquet(os.path.join(staging_path, "price_snapshots")) \
                      .where(F.col("ingest_dt") == ingest_dt)
    stg_ratings  = spark.read.parquet(os.path.join(staging_path, "ratings")) \
                      .where(F.col("ingest_dt") == ingest_dt)
    stg_sellers  = spark.read.parquet(os.path.join(staging_path, "sellers")) \
                      .where(F.col("ingest_dt") == ingest_dt)

    # ID thống nhất (global)
    gid = F.sha1(F.concat_ws("|", F.col("platform"), F.col("platform_product_id")))
    # Chuẩn hoá brand
    brand_std = F.upper(F.trim(F.col("brand")))

    cur_products = (
        stg_products
        .dropDuplicates(["platform","platform_product_id"])
        .withColumn("global_product_id", gid)
        .withColumn("brand_std", brand_std)
        .select(
            "global_product_id","platform","platform_product_id",
            F.col("product_name").alias("name"),
            "brand_std","url","image_url","ingest_dt","ingest_ts"
        )
    )

    cur_price = (
        stg_price
        .withColumn("global_product_id", gid)
        .select("global_product_id","platform","platform_product_id",
                "price_current","price_original","discount_pct",
                "snapshot_ts","ingest_dt","ingest_ts")
    )

    cur_ratings = (
        stg_ratings
        .withColumn("global_product_id", gid)
        .select("global_product_id","platform","platform_product_id",
                "rating_avg","review_count","snapshot_ts",
                "ingest_dt","ingest_ts")
    )

    cur_sellers = (
        stg_sellers
        .withColumn("global_product_id", gid)
        .withColumn("seller_name", F.col("seller_name").cast(T.StringType()))
        .withColumn("seller_region", F.col("seller_region").cast(T.StringType()))
        .select("global_product_id","platform","platform_product_id",
                "seller_name","seller_region","ingest_dt","ingest_ts")
        .dropDuplicates(["platform","platform_product_id"])
    )

    # Ghi curated (Parquet), partition theo ingest_dt
    (cur_products.write.mode("append").partitionBy("ingest_dt")
        .parquet(os.path.join(curated_path, "products")))
    (cur_price.write.mode("append").partitionBy("ingest_dt")
        .parquet(os.path.join(curated_path, "price_snapshots")))
    (cur_ratings.write.mode("append").partitionBy("ingest_dt")
        .parquet(os.path.join(curated_path, "ratings")))
    (cur_sellers.write.mode("append").partitionBy("ingest_dt")
        .parquet(os.path.join(curated_path, "sellers")))

    # log
    print("[DONE] Curated written:",
          "products=", cur_products.count(),
          "price=", cur_price.count(),
          "ratings=", cur_ratings.count(),
          "sellers=", cur_sellers.count())
    spark.stop()

if __name__ == "__main__":
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging-path", required=True)
    parser.add_argument("--curated-path", required=True)
    parser.add_argument("--ingest-dt", default=datetime.date.today().isoformat())
    args = parser.parse_args()
    main(args.staging_path, args.curated_path, args.ingest_dt)
