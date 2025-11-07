#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
raw_to_staging.py
Chuẩn hoá raw files từ Tiki(JSON) & Lazada(CSV) → staging Parquet, partition by (platform, ingest_dt)

Outputs:
- staging/products:        platform_product_id, product_name, brand, url, image_url, platform, ingest_dt, ingest_ts
- staging/price_snapshots: platform_product_id, price_current, price_original, discount_pct, snapshot_ts, platform, ingest_dt, ingest_ts
- staging/ratings:         platform_product_id, rating_avg, review_count, snapshot_ts, platform, ingest_dt, ingest_ts
- staging/sellers:         platform_product_id, seller_name, seller_region, platform, ingest_dt, ingest_ts
"""

import argparse
import os
import datetime
import re
import glob
from pyspark.sql import SparkSession, functions as F, types as T

def init_spark(app_name="RawToStaging_Tiki_Lazada"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def build_udfs():
    def _sha1(text):
        if text is None: return None
        import hashlib; return hashlib.sha1(text.encode("utf-8")).hexdigest()
    def _parse_price(s):
        if s is None: return None
        try:
            cleaned = re.sub(r"[^\d,\.]", "", s.strip())
            digits = re.sub(r"[^\d]", "", cleaned)
            return float(digits) if digits else None
        except: return None
    def _parse_percent(s):
        if s is None: return None
        try:
            m = re.search(r"(-?\d+(\.\d+)?)", s); return float(m.group(1)) if m else None
        except: return None
    return (F.udf(_sha1, T.StringType()),
            F.udf(_parse_price, T.DoubleType()),
            F.udf(_parse_percent, T.DoubleType()))

def write_parquet(df, base_path, table):
    out = os.path.join(base_path, table)
    (df.write.mode("append").partitionBy("platform","ingest_dt").parquet(out))

# ---------------- TIKI (progress_*.json) ----------------
def process_tiki(spark, raw_path, staging_path, ingest_dt):
    paths = glob.glob(os.path.join(raw_path, "progress_*.json"))
    if not paths:
        print(f"[TIKI] No files at {raw_path}/progress_*.json"); return 0,0,0,0

    df = spark.read.option("multiLine","true").json(paths)

    df = (
        df.withColumn("platform", F.lit("tiki"))
          .withColumn("ingest_dt", F.lit(ingest_dt))
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn("platform_product_id", F.col("product_id").cast("string"))
          .withColumn("product_name", F.col("product_name").cast("string"))
          .withColumn("brand", F.col("brand").cast("string"))
          .withColumn("url", F.col("url").cast("string"))
          .withColumn("image_url",
              F.when(F.col("image_urls").isNotNull(), F.col("image_urls").getItem(0)).otherwise(F.lit(None)).cast("string"))
          .withColumn("snapshot_ts", F.coalesce(F.to_timestamp("crawl_date"), F.current_timestamp()))
          .withColumn("price_current_d", F.col("price_current").cast(T.DoubleType()))
          .withColumn("price_original_d", F.col("price_original").cast(T.DoubleType()))
          .withColumn("discount_pct_d", F.col("discount_percent").cast(T.DoubleType()))
          .withColumn("rating_avg_d", F.col("rating_avg").cast(T.DoubleType()))
          .withColumn("review_count_i", F.col("review_count").cast(T.IntegerType()))
    )

    products = df.select("platform_product_id","product_name","brand","url","image_url","platform","ingest_dt","ingest_ts")\
                 .dropDuplicates(["platform","platform_product_id"])
    price_snapshots = df.select("platform_product_id",
                                F.col("price_current_d").alias("price_current"),
                                F.col("price_original_d").alias("price_original"),
                                F.col("discount_pct_d").alias("discount_pct"),
                                "snapshot_ts","platform","ingest_dt","ingest_ts")
    ratings = df.select("platform_product_id",
                        F.col("rating_avg_d").alias("rating_avg"),
                        F.col("review_count_i").alias("review_count"),
                        "snapshot_ts","platform","ingest_dt","ingest_ts")
    sellers = df.select("platform_product_id",
                        F.lit(None).cast("string").alias("seller_name"),
                        F.lit(None).cast("string").alias("seller_region"),
                        "platform","ingest_dt","ingest_ts")\
                .dropDuplicates(["platform","platform_product_id"])

    write_parquet(products, staging_path, "products")
    write_parquet(price_snapshots, staging_path, "price_snapshots")
    write_parquet(ratings, staging_path, "ratings")
    write_parquet(sellers, staging_path, "sellers")
    return products.count(), price_snapshots.count(), ratings.count(), sellers.count()

# -------------- LAZADA (/data/fixed_lazada_products*.csv) --------------
def process_lazada(spark, raw_path, staging_path, ingest_dt, udf_sha1, udf_parse_price, udf_parse_percent):
    # Ưu tiên file products; nếu bạn muốn đọc thêm file khác, chỉnh pattern ở đây
    paths = glob.glob(os.path.join(raw_path, "lazada_products*.csv"))
    if not paths:
        print(f"[LAZADA] No files at {raw_path}/fixed_lazada_products*.csv"); return 0,0,0,0

    df = (spark.read.option("header","true").option("multiLine","true")
          .option("quote",'"').option("escape",'"').csv(paths))

    df = (
        df.withColumn("platform", F.lit("lazada"))
          .withColumn("ingest_dt", F.lit(ingest_dt))
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn("url", F.col("url").cast(T.StringType()))
          .withColumn("platform_product_id", udf_sha1(F.col("url")))
          .withColumn("product_name", F.col("title").cast(T.StringType()))
          .withColumn("brand", F.lit(None).cast(T.StringType()))
          .withColumn("image_url", F.col("image").cast(T.StringType()))
          .withColumn("price_current_d",
                F.when(F.col("price").isNotNull() & (F.col("price")!=""), F.col("price").cast(T.DoubleType()))
                 .otherwise(udf_parse_price("price_text")))
          .withColumn("price_original_d",
                F.when(F.col("original_price").isNotNull() & (F.col("original_price")!=""), F.col("original_price").cast(T.DoubleType()))
                 .otherwise(udf_parse_price("original_price_text")))
          .withColumn("discount_pct_d", udf_parse_percent("discount"))
          .withColumn("rating_avg_d", F.col("rating").cast(T.DoubleType()))
          .withColumn("review_count_i", F.col("review_count").cast(T.IntegerType()))
          .withColumn("seller_region", F.col("location").cast(T.StringType()))
          .withColumn("seller_name", F.col("shop_name").cast(T.StringType()))
          .withColumn("snapshot_ts", F.current_timestamp())
    )

    products = df.select("platform_product_id","product_name","brand","url","image_url","platform","ingest_dt","ingest_ts")\
                 .dropDuplicates(["platform","platform_product_id"])
    price_snapshots = df.select("platform_product_id",
                                F.col("price_current_d").alias("price_current"),
                                F.col("price_original_d").alias("price_original"),
                                F.col("discount_pct_d").alias("discount_pct"),
                                "snapshot_ts","platform","ingest_dt","ingest_ts")
    ratings = df.select("platform_product_id",
                        F.col("rating_avg_d").alias("rating_avg"),
                        F.col("review_count_i").alias("review_count"),
                        "snapshot_ts","platform","ingest_dt","ingest_ts")
    sellers = df.select("platform_product_id","seller_name","seller_region","platform","ingest_dt","ingest_ts")\
                .dropDuplicates(["platform","platform_product_id"])

    write_parquet(products, staging_path, "products")
    write_parquet(price_snapshots, staging_path, "price_snapshots")
    write_parquet(ratings, staging_path, "ratings")
    write_parquet(sellers, staging_path, "sellers")
    return products.count(), price_snapshots.count(), ratings.count(), sellers.count()

def main(raw_path, staging_path, ingest_dt):
    spark = init_spark()
    udf_sha1, udf_parse_price, udf_parse_percent = build_udfs()

    total = {"prod":0,"price":0,"rate":0,"sell":0}

    p, pr, r, s = process_tiki(spark, raw_path, staging_path, ingest_dt)
    total["prod"] += p; total["price"] += pr; total["rate"] += r; total["sell"] += s
    print(f"[TIKI] products={p} price_snapshots={pr} ratings={r} sellers={s}")

    p, pr, r, s = process_lazada(spark, raw_path, staging_path, ingest_dt,
                                 udf_sha1, udf_parse_price, udf_parse_percent)
    total["prod"] += p; total["price"] += pr; total["rate"] += r; total["sell"] += s
    print(f"[LAZADA] products={p} price_snapshots={pr} ratings={r} sellers={s}")

    print(f"[DONE] TOTAL → products={total['prod']} | price_snapshots={total['price']} | ratings={total['rate']} | sellers={total['sell']}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Raw → Staging (Tiki progress_*.json, Lazada fixed_lazada_products*.csv)")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--staging-path", required=True)
    parser.add_argument("--ingest-dt", required=False, default=datetime.date.today().isoformat())
    args = parser.parse_args()
    main(args.raw_path, args.staging_path, args.ingest_dt)