#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
raw_to_staging.py
Chuẩn hoá raw files từ Tiki(JSON) & Lazada(CSV) → staging Parquet, partition by (platform, ingest_dt)

Staging outputs:
- staging/products:        platform_product_id, product_name, brand, url, image_url, platform, ingest_dt, ingest_ts
- staging/price_snapshots: platform_product_id, price_current, price_original, discount_pct, snapshot_ts, platform, ingest_dt, ingest_ts
- staging/ratings:         platform_product_id, rating_avg, review_count, snapshot_ts, platform, ingest_dt, ingest_ts
- staging/sellers:         platform_product_id, seller_name, seller_region, platform, ingest_dt, ingest_ts
"""

import argparse
import os
import datetime
import re
from pyspark.sql import SparkSession, functions as F, types as T

# ---------------------------
# Spark init
# ---------------------------
def init_spark(app_name="RawToStaging"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ---------------------------
# Helpers & UDFs
# ---------------------------
@F.udf("string")
def udf_sha1(text):
    if text is None:
        return None
    import hashlib
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

@F.udf("double")
def udf_parse_price(s):
    """Parse '595.000 ₫' → 595000.0 ; '1,290,000đ' → 1290000.0 ; ' ' → null"""
    if s is None:
        return None
    try:
        # remove currency and spaces
        cleaned = re.sub(r"[^\d,\.]", "", s.strip())
        # normalize thousand separators: prefer removing dots and commas except last decimal (VN rarely has decimals)
        # Strategy: drop all non-digits
        digits = re.sub(r"[^\d]", "", cleaned)
        if digits == "":
            return None
        return float(digits)
    except Exception:
        return None

@F.udf("double")
def udf_parse_percent(s):
    """Parse 'Voucher giảm 65%' → 65.0 ; '25' → 25.0 ; '' → null"""
    if s is None:
        return None
    try:
        m = re.search(r"(-?\d+(\.\d+)?)", s)
        return float(m.group(1)) if m else None
    except Exception:
        return None

def write_parquet(df, base_path, table):
    out = os.path.join(base_path, table)
    (
        df.write.mode("append")
        .partitionBy("platform", "ingest_dt")
        .parquet(out)
    )

# ---------------------------
# TIKI JSON → staging
# ---------------------------
def process_tiki(spark, raw_path, staging_path, ingest_dt):
    path = os.path.join(raw_path, "platform=tiki", f"ingest_dt={ingest_dt}", "*.json")
    df = spark.read.option("multiline", "true").json(path)
    if df.rdd.isEmpty():
        print(f"[TIKI] No files at {path}")
        return 0,0,0,0

    # Chuẩn hoá cột theo mẫu bạn cung cấp
    # Sample keys: source, product_id, product_name, price_current, price_original, discount_percent,
    # rating_avg, review_count, brand, url, image_urls[], crawl_date
    df = (
        df.withColumn("platform", F.lit("tiki"))
          .withColumn("ingest_dt", F.lit(ingest_dt))
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn("platform_product_id", F.col("product_id").cast("string"))
          .withColumn("product_name", F.col("product_name").cast("string"))
          .withColumn("brand", F.col("brand").cast("string"))
          .withColumn("url", F.col("url").cast("string"))
          .withColumn("image_url", F.when(F.col("image_urls").isNotNull(), F.col("image_urls").getItem(0)).otherwise(F.lit(None)).cast("string"))
          .withColumn("snapshot_ts", F.to_timestamp("crawl_date"))
          .withColumn("price_current_d", F.col("price_current").cast("double"))
          .withColumn("price_original_d", F.col("price_original").cast("double"))
          .withColumn("discount_pct_d", F.col("discount_percent").cast("double"))
          .withColumn("rating_avg_d", F.col("rating_avg").cast("double"))
          .withColumn("review_count_i", F.col("review_count").cast("int"))
    )

    # products
    products = df.select(
        "platform_product_id","product_name","brand","url","image_url","platform","ingest_dt","ingest_ts"
    ).dropDuplicates(["platform","platform_product_id"])

    # price_snapshots
    price_snapshots = df.select(
        "platform_product_id",
        F.col("price_current_d").alias("price_current"),
        F.col("price_original_d").alias("price_original"),
        F.col("discount_pct_d").alias("discount_pct"),
        "snapshot_ts","platform","ingest_dt","ingest_ts"
    )

    # ratings
    ratings = df.select(
        "platform_product_id",
        F.col("rating_avg_d").alias("rating_avg"),
        F.col("review_count_i").alias("review_count"),
        "snapshot_ts","platform","ingest_dt","ingest_ts"
    )

    # sellers (tiki sample không có shop_name/location → để trống)
    sellers = df.select(
        "platform_product_id",
        F.lit(None).cast("string").alias("seller_name"),
        F.lit(None).cast("string").alias("seller_region"),
        "platform","ingest_dt","ingest_ts"
    ).dropDuplicates(["platform","platform_product_id"])

    # Write out
    write_parquet(products, staging_path, "products")
    write_parquet(price_snapshots, staging_path, "price_snapshots")
    write_parquet(ratings, staging_path, "ratings")
    write_parquet(sellers, staging_path, "sellers")

    return products.count(), price_snapshots.count(), ratings.count(), sellers.count()

# ---------------------------
# LAZADA CSV → staging
# ---------------------------
def process_lazada(spark, raw_path, staging_path, ingest_dt):
    path = os.path.join(raw_path, "platform=lazada", f"ingest_dt={ingest_dt}", "*.csv")
    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")     # phòng trường hợp mô tả có xuống dòng
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )
    if df.rdd.isEmpty():
        print(f"[LAZADA] No files at {path}")
        return 0,0,0,0

    # Cột mẫu (theo bạn cung cấp):
    # title,url,image,price_text,price,original_price_text,original_price,discount,rating,review_count,location,shop_name
    # -> Một số cột có thể trống. Parse & cast.
    df = (
        df.withColumn("platform", F.lit("lazada"))
          .withColumn("ingest_dt", F.lit(ingest_dt))
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn("url", F.col("url").cast("string"))
          .withColumn("platform_product_id", udf_sha1(F.col("url")))  # hash từ URL
          .withColumn("product_name", F.col("title").cast("string"))
          .withColumn("brand", F.lit(None).cast("string"))  # thường khó lấy brand ở CSV; để null
          .withColumn("image_url", F.col("image").cast("string"))
          # Parse price
          .withColumn("price_current_d",
                      F.when(F.col("price").isNotNull() & (F.col("price") != ""),
                             F.col("price").cast("double")).otherwise(udf_parse_price("price_text")))
          .withColumn("price_original_d",
                      F.when(F.col("original_price").isNotNull() & (F.col("original_price") != ""),
                             F.col("original_price").cast("double")).otherwise(udf_parse_price("original_price_text")))
          .withColumn("discount_pct_d", udf_parse_percent("discount"))
          .withColumn("rating_avg_d", F.col("rating").cast("double"))
          .withColumn("review_count_i", F.col("review_count").cast("int"))
          .withColumn("seller_region", F.col("location").cast("string"))
          .withColumn("seller_name", F.col("shop_name").cast("string"))
          .withColumn("snapshot_ts", F.current_timestamp())  # không có crawl_time → dùng thời điểm nạp
    )

    # products
    products = df.select(
        "platform_product_id","product_name","brand","url","image_url","platform","ingest_dt","ingest_ts"
    ).dropDuplicates(["platform","platform_product_id"])

    # price_snapshots
    price_snapshots = df.select(
        "platform_product_id",
        F.col("price_current_d").alias("price_current"),
        F.col("price_original_d").alias("price_original"),
        F.col("discount_pct_d").alias("discount_pct"),
        "snapshot_ts","platform","ingest_dt","ingest_ts"
    )

    # ratings
    ratings = df.select(
        "platform_product_id",
        F.col("rating_avg_d").alias("rating_avg"),
        F.col("review_count_i").alias("review_count"),
        "snapshot_ts","platform","ingest_dt","ingest_ts"
    )

    # sellers
    sellers = df.select(
        "platform_product_id","seller_name","seller_region","platform","ingest_dt","ingest_ts"
    ).dropDuplicates(["platform","platform_product_id"])

    # Write out
    write_parquet(products, staging_path, "products")
    write_parquet(price_snapshots, staging_path, "price_snapshots")
    write_parquet(ratings, staging_path, "ratings")
    write_parquet(sellers, staging_path, "sellers")

    return products.count(), price_snapshots.count(), ratings.count(), sellers.count()

# ---------------------------
# Main
# ---------------------------
def main(raw_path, staging_path, ingest_dt):
    spark = init_spark("RawToStaging_Tiki_Lazada")

    total = {"prod":0,"price":0,"rate":0,"sell":0}

    # Tiki
    p, pr, r, s = process_tiki(spark, raw_path, staging_path, ingest_dt)
    total["prod"] += p; total["price"] += pr; total["rate"] += r; total["sell"] += s
    print(f"[TIKI] products={p} price_snapshots={pr} ratings={r} sellers={s}")

    # Lazada
    p, pr, r, s = process_lazada(spark, raw_path, staging_path, ingest_dt)
    total["prod"] += p; total["price"] += pr; total["rate"] += r; total["sell"] += s
    print(f"[LAZADA] products={p} price_snapshots={pr} ratings={r} sellers={s}")

    print(f"[DONE] TOTAL rows → products={total['prod']} | price_snapshots={total['price']} | ratings={total['rate']} | sellers={total['sell']}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Raw → Staging for Tiki(JSON) & Lazada(CSV)")
    parser.add_argument("--raw-path", required=True, help="Base path to raw data")
    parser.add_argument("--staging-path", required=True, help="Base path to staging")
    parser.add_argument("--ingest-dt", required=False, default=datetime.date.today().isoformat())
    args = parser.parse_args()
    main(args.raw_path, args.staging_path, args.ingest_dt)
