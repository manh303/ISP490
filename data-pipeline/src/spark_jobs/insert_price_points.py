#!/usr/bin/env python3
"""Insert Price Points to ODS"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, to_timestamp, concat, lit, when
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pg-url', required=True)
    parser.add_argument('--pg-user', required=True)
    parser.add_argument('--pg-pass', required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("Insert Price Points") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar") \
        .getOrCreate()
    
    print("=" * 60)
    print("INSERT PRICE POINTS")
    print("=" * 60)
    
    # Read from staging
    stg_products = spark.read.jdbc(
        url=args.pg_url,
        table="stg_raw_products",
        properties={"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"}
    )
    
    print(f"✓ Read {stg_products.count()} products from staging")
    
    # Extract price points
    ods_prices = stg_products.select(
        concat(
            col("source_platform"),
            lit("_"),
            get_json_object(col("raw_data"), "$.product_id")
        ).alias("global_product_id"),
        when(col("source_platform") == "lazada", 1)
        .when(col("source_platform") == "tiki", 2)
        .otherwise(0).alias("platform_sk"),
        to_timestamp(get_json_object(col("raw_data"), "$.crawl_date")).alias("captured_at"),
        get_json_object(col("raw_data"), "$.price_current").cast("double").alias("price_current"),
        get_json_object(col("raw_data"), "$.price_original").cast("double").alias("price_original"),
        get_json_object(col("raw_data"), "$.discount_percent").cast("double").alias("discount_percent"),
        lit(True).alias("is_available")
    ).filter(
        col("global_product_id").isNotNull() & 
        (col("price_current") > 0)
    )
    
    print(f"✓ Extracted {ods_prices.count()} price points")
    
    ods_prices.write.jdbc(
        url=args.pg_url,
        table="ods_price_point",
        mode="append",
        properties={"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"}
    )
    
    print(f"✓ Wrote price points to ODS")
    
    spark.stop()
    print("✅ Price points insertion completed")

if __name__ == "__main__":
    main()
