# -*- coding: utf-8 -*-
# /app/src/spark_jobs/retail_etl.py
import argparse, re
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, types as T

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=False, help="YYYY-MM-DD (default: today UTC)")
    p.add_argument("--input", required=True)
    p.add_argument("--bronze", required=True)
    p.add_argument("--silver", required=True)
    p.add_argument("--pg-url", required=True)
    p.add_argument("--pg-user", required=True)
    p.add_argument("--pg-pass", required=True)
    return p.parse_args()

def spark():
    return (SparkSession.builder
            .appName("retail_etl_tiki_lazada")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())

def norm_text(col):
    return F.lower(F.trim(F.regexp_replace(col, r"\s+", " ")))

def extract_digits(col):
    return F.regexp_extract(col.cast("string"), r"(\d+)", 1)

def hash_key(*cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(c.cast("string"), F.lit("")) for c in cols]), 256)

def read_site(spark, base, site, date_str):
    glob = f"{base}/{site}/date={date_str}/*/*.jsonl" if date_str else f"{base}/{site}/*/*/*.jsonl"
    df = spark.read.json(glob)
    if "url" not in df.columns:
        df = spark.createDataFrame([], schema=T.StructType([T.StructField("url", T.StringType())]))
    return df.withColumn("source", F.lit(site))

def bronze_write(df, base, site, date_str):
    (df.withColumn("_ingest_ts", F.current_timestamp())
       .write.mode("append")
       .parquet(f"{base}/{site}/date={date_str}"))

def silver_transform(df):
    has = lambda c: c in df.columns
    price_col = F.when(df["price"].cast("bigint").isNotNull(), df["price"].cast("bigint")) \
                 .otherwise(extract_digits(df["price"]).cast("bigint"))
    rating_col = df["rating"].cast("double") if has("rating") else F.lit(None).cast("double")
    review_col = (extract_digits(df["review_count"]).cast("bigint")
                  if has("review_count") else F.lit(None).cast("bigint"))
    seller_col = F.coalesce(df["shop_name"].cast("string"), df["seller"].cast("string"))
    image_col  = F.coalesce(df["image_url"].cast("string"), F.lit(None))

    cleaned = (df
        .withColumn("title", F.coalesce(df["title"].cast("string"), F.lit("")))
        .withColumn("brand", F.coalesce(df["brand"].cast("string"), F.lit("")))
        .withColumn("model", F.coalesce(df["model"].cast("string"), F.lit("")))
        .withColumn("raw_category", F.coalesce(df["category"].cast("string"), F.lit("")))
        .withColumn("price", price_col)
        .withColumn("currency", F.lit("VND"))
        .withColumn("rating", rating_col)
        .withColumn("review_count", review_col)
        .withColumn("seller", seller_col)
        .withColumn("image_url", image_col)
        .withColumn("url", df["url"].cast("string"))
        .withColumn("title_norm", norm_text(F.col("title")))
        .withColumn("brand_norm", norm_text(F.col("brand")))
        .withColumn("model_norm", norm_text(F.col("model")))
        .withColumn("collected_at", F.current_timestamp())
    )

    # baseline mapping category
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
    cat = F.lower(F.col("raw_category"))
    canonical = F.lit("other")
    for pat, tgt in mapping:
        canonical = F.when(cat.rlike(pat), F.lit(tgt)).otherwise(canonical)
    cleaned = cleaned.withColumn("canonical_category", canonical)

    cleaned = cleaned.withColumn(
        "product_key",
        hash_key(F.col("source"), F.col("brand_norm"), F.col("model_norm"), F.col("title_norm"))
    )

    cleaned = cleaned.where((F.col("url").isNotNull()) & (F.col("title") != "") & (F.col("price") > 0))
    cleaned = cleaned.dropDuplicates(["url", "source"])

    cols = ["product_key","title","brand","model","canonical_category",
            "price","currency","rating","review_count","seller","url","image_url",
            "collected_at","source"]
    return cleaned.select(*cols)

def write_silver(df, base, site, date_str):
    (df.write.mode("overwrite").parquet(f"{base}/{site}/date={date_str}"))

def write_staging(df, jdbc_url, user, password, snapshot_date):
    out = (df
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
        .select("snapshot_date","source","product_key","title","brand","model","canonical_category",
                "price","currency","rating","review_count","seller","url","image_url","collected_at"))
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
    snap = args.date or datetime.utcnow().strftime("%Y-%m-%d")
    s = spark()
    s.sparkContext.setLogLevel("WARN")

    for site in ["lazada", "tiki"]:
        raw = read_site(s, args.input, site, snap)
        if raw.rdd.isEmpty():
            print(f"[WARN] No data for {site} @ {snap}")
            continue
        bronze_write(raw, args.bronze, site, snap)
        silver = silver_transform(raw)
        write_silver(silver, args.silver, site, snap)
        write_staging(silver, args.pg_url, args.pg_user, args.pg_pass, snap)

    s.stop()

if __name__ == "__main__":
    main()
