#!/usr/bin/env python3
"""DWH Build - Load ODS to Data Warehouse"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit, row_number, to_date
from pyspark.sql.window import Window
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pg-url', required=True)
    parser.add_argument('--pg-user', required=True)
    parser.add_argument('--pg-pass', required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("DWH Build") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar") \
        .getOrCreate()
    
    props = {"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"}
    
    print("=" * 60)
    print("DWH BUILD")
    print("=" * 60)
    
    # Load ODS
    ods_products = spark.read.jdbc(args.pg_url, "ods_product_clean", properties=props)
    ods_prices = spark.read.jdbc(args.pg_url, "ods_price_point", properties=props)
    
    print(f"✓ Loaded ODS: {ods_products.count()} products, {ods_prices.count()} prices")
    
    # Build dim_product (SCD Type 2)
    dim_product = ods_products.select(
        col("global_product_id"),
        col("product_name"),
        lit(1).alias("brand_sk"),
        col("category_sk"),
        col("seller_name"),
        lit("marketplace").alias("seller_type"),
        to_date(col("first_seen")).alias("effective_from"),
        lit("9999-12-31").cast("date").alias("effective_to"),
        lit(True).alias("is_current")
    )
    
    dim_product.write.jdbc(
        url=args.pg_url,
        table="dwh_dim_product",
        mode="append",
        properties=props
    )
    
    print(f"✓ Loaded dim_product: {dim_product.count()}")
    
    # Build fact_product_daily
    fact_daily = ods_prices.join(
        spark.read.jdbc(args.pg_url, "dwh_dim_product", properties=props),
        ods_prices.global_product_id == col("global_product_id"),
        "inner"
    ).select(
        (to_date(col("captured_at")).cast("string").replace("-", "").cast("int")).alias("date_sk"),
        col("product_sk"),
        col("platform_sk"),
        col("price_current"),
        col("price_original"),
        col("discount_percent").alias("discount_pct"),
        lit(None).cast("decimal(3,2)").alias("rating_avg"),
        lit(0).alias("rating_count"),
        lit(0).alias("review_count"),
        lit(0).alias("sold_count"),
        col("is_available"),
        col("captured_at")
    )
    
    fact_daily.write.jdbc(
        url=args.pg_url,
        table="dwh_fact_product_daily",
        mode="append",
        properties=props
    )
    
    print(f"✓ Loaded fact_product_daily: {fact_daily.count()}")
    
    spark.stop()
    print("✅ DWH build completed")

if __name__ == "__main__":
    main()
