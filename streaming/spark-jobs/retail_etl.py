# -*- coding: utf-8 -*-
"""
Spark job: đọc JSONL từ Tiki/Lazada, ghi Bronze/Silver, và đẩy dữ liệu sạch vào bảng staging (ods.stg_products)
Chạy bởi Airflow với tham số:
  --date YYYY-MM-DD
  --input /app/data/outputs
  --bronze /app/data/bronze
  --silver /app/data/silver
  --pg-url jdbc:postgresql://postgres:5432/ecommerce_dss_1
  --pg-user dss_user
  --pg-pass dss_password_123
"""

import argparse
import os
import re
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, types as T

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=False, help="snap date YYYY-MM-DD; default today UTC")
    p.add_argument("--input", required=True)
    p.add_argument("--bronze", required=True)
    p.add_argument("--silver", required=True)
    p.add_argument("--pg-url", required=True)
    p.add_argument("--pg-user", required=True)
    p.add_argument("--pg-pass", required=True)
    return p.parse_args()

def build_spark():
    return (SparkSession.builder
            .appName("retail_etl_tiki_lazada")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate())

def norm_text(col):
    # hạ chuẩn tối thiểu: trim + lower
    return F.lower(F.trim(col))

def extract_digits(col):
    return F.regexp_extract(col.cast("string"), r"(\d+)", 1)

def hash_key(*cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(c.cast("string"), F.lit("")) for c in cols]), 256)

def read_site(spark, base, site, date_str):
    # pattern: /input/{site}/date=YYYY-MM-DD/*/*.jsonl
    glob = f"{base}/{site}/date={date_str}/*/*.jsonl" if date_str else f"{base}/{site}/*/*/*.jsonl"
    df = spark.read.option("multiLine", "false").json(glob)
    if "url" not in df.columns:
        # đề phòng file rỗng
        df = spark.createDataFrame([], schema=T.StructType([T.StructField("url", T.StringType())]))
    return df.withColumn("source", F.lit(site))

def bronze_write(df, base, site, date_str):
    (df.withColumn("_ingest_ts", F.current_timestamp())
       .write.mode("append")
       .parquet(f"{base}/{site}/date={date_str}"))

def silver_transform(df):
    # Chuẩn hóa tối thiểu các cột
    has = lambda c: c in df.columns
    price_col = F.when(df["price"].cast("bigint").isNotNull(), df["price"].cast("bigint")) \
                 .otherwise(extract_digits(df["price"]).cast("bigint"))
    rating_col = df["rating"].cast("double") if has("rating") else F.lit(None).cast("double")
    review_col = (extract_digits(df["review_count"]).cast("bigint")
                  if has("review_count") else F.lit(None).cast("bigint"))
    title_col = F.coalesce(df["title"].cast("string"), F.lit("")).alias("title")
    brand_col = F.coalesce(df["brand"].cast("string"), F.lit("")).alias("brand")
    model_col = F.coalesce(df["model"].cast("string"), F.lit("")).alias("model")
    category_col = F.coalesce(df["category"].cast("string"), F.lit("")).alias("category")
    seller_col = F.coalesce(df["shop_name"].cast("string"), F.col("seller").cast("string"))
    image_col = F.coalesce(df["image_url"].cast("string"), F.lit(None))

    cleaned = (df
        .withColumn("title", title_col)
        .withColumn("brand", brand_col)
        .withColumn("model", model_col)
        .withColumn("raw_category", category_col)
        .withColumn("price", price_col)
        .withColumn("currency", F.lit("VND"))
        .withColumn("rating", rating_col)
        .withColumn("review_count", review_col)
        .withColumn("seller", seller_col)
        .withColumn("image_url", image_col)
        .withColumn("url", df["url"].cast("string"))
        .withColumn("title_norm", norm_text(F.regexp_replace("title", r"\s+", " ")))
        .withColumn("brand_norm", norm_text("brand"))
        .withColumn("model_norm", norm_text("model"))
        .withColumn("collected_at", F.current_timestamp())
    )

    # mapping category đơn giản (có thể thay bằng bảng tra từ DB)
    mapping = [
        ("phone|điện thoại|mobiles?", "smartphones"),
        ("laptop|notebook", "laptops"),
        ("tablet|ipad", "tablets"),
        ("tivi|tv", "tvs"),
        ("tai nghe|headphone|ear", "headphones"),
        ("camera|máy ảnh", "cameras"),
        ("màn hình|monitor", "monitors"),
        ("pc|desktop", "desktops-computers"),
        ("đồng hồ thông minh|smartwatch", "smartwatches"),
    ]
    cat = F.lower(F.coalesce(F.col("raw_category"), F.lit("")))
    canonical = F.lit("other")
    for pat, tgt in mapping:
        canonical = F.when(cat.rlike(pat), F.lit(tgt)).otherwise(canonical)

    cleaned = cleaned.withColumn("canonical_category", canonical)

    # product_key kết hợp các tín hiệu
    cleaned = cleaned.withColumn(
        "product_key",
        hash_key(F.col("source"), F.col("brand_norm"), F.col("model_norm"), F.col("title_norm"))
    )

    # DQ filter cơ bản
    cleaned = cleaned.where((F.col("url").isNotNull()) & (F.col("title") != "") & (F.col("price") > 0))

    # Dedup trong ngày theo url ưu tiên giá mới nhất
    w = F.window("collected_at", "1 day")
    cleaned = cleaned.dropDuplicates(["url", "source"])

    # Chọn projection cho staging
    out_cols = [
        "product_key", "title", "brand", "model", "canonical_category",
        "price", "currency", "rating", "review_count", "seller", "url", "image_url",
        "collected_at", "source"
    ]
    return cleaned.select(*out_cols)

def write_silver(df, base, site, date_str):
    (df.write.mode("overwrite")
       .parquet(f"{base}/{site}/date={date_str}"))

def write_staging(df, jdbc_url, user, password, snapshot_date):
    out = (df
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
        .select(
            "snapshot_date","source","product_key","title","brand","model","canonical_category",
            "price","currency","rating","review_count","seller","url","image_url","collected_at"
        ))
    (out.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "ods.stg_products")
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())

def main():
    args = parse_args()
    snap_date = args.date or datetime.utcnow().strftime("%Y-%m-%d")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    sites = ["lazada", "tiki"]
    for site in sites:
        df_raw = read_site(spark, args.input, site, snap_date)
        if df_raw.rdd.isEmpty():
            print(f"[WARN] Không tìm thấy dữ liệu {site} cho ngày {snap_date}")
            continue

        bronze_write(df_raw, args.bronze, site, snap_date)

        df_silver = silver_transform(df_raw)
        write_silver(df_silver, args.silver, site, snap_date)

        write_staging(df_silver, args.pg_url, args.pg_user, args.pg_pass, snap_date)

    spark.stop()

if __name__ == "__main__":
    main()
