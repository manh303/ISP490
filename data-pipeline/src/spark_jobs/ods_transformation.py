#!/usr/bin/env python3
"""
ODS Transformation - Transform staging data to ODS layer
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, get_json_object, to_timestamp
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pg-url', required=True)
    parser.add_argument('--pg-user', required=True)
    parser.add_argument('--pg-pass', required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("ODS Transformation") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar") \
        .getOrCreate()
    
    print("=" * 60)
    print("ODS TRANSFORMATION")
    print("=" * 60)
    
    # Read from staging
    stg_products = spark.read.jdbc(
        url=args.pg_url,
        table="stg_raw_products",
        properties={"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"}
    )
    
    print(f"✓ Read {stg_products.count()} products from staging")
    
    # Parse JSON from raw_data column
    from pyspark.sql.functions import concat, lit, when
    
    ods_products = stg_products.select(
        concat(
            col("source_platform"),
            lit("_"),
            get_json_object(col("raw_data"), "$.product_id")
        ).alias("global_product_id"),
        col("source_platform"),
        get_json_object(col("raw_data"), "$.product_id").alias("platform_product_id"),
        get_json_object(col("raw_data"), "$.product_name").alias("product_name"),
        get_json_object(col("raw_data"), "$.brand").alias("brand_name"),
        get_json_object(col("raw_data"), "$.category").alias("category"),
        when(col("source_platform") == "lazada", 1)
        .when(col("source_platform") == "tiki", 2)
        .otherwise(0).alias("category_sk"),
        get_json_object(col("raw_data"), "$.seller_name").alias("seller_name"),
        get_json_object(col("raw_data"), "$.price_current").cast("decimal(15,2)").alias("price_current"),
        get_json_object(col("raw_data"), "$.price_original").cast("decimal(15,2)").alias("price_original"),
        get_json_object(col("raw_data"), "$.discount_percent").cast("decimal(5,2)").alias("discount_percent"),
        get_json_object(col("raw_data"), "$.rating_avg").cast("decimal(3,2)").alias("rating_avg"),
        get_json_object(col("raw_data"), "$.review_count").cast("int").alias("review_count"),
        get_json_object(col("raw_data"), "$.url").alias("url"),
        get_json_object(col("raw_data"), "$.image_url").alias("image_url"),
        to_timestamp(get_json_object(col("raw_data"), "$.crawl_date")).alias("crawled_at"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("last_seen")
    ).filter(col("global_product_id").isNotNull())
    
    ods_products.write.jdbc(
        url=args.pg_url,
        table="ods_product_clean",
        mode="append",
        properties={"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"}
    )
    
    print(f"✓ Wrote {ods_products.count()} products to ODS")
    
    # Price Points
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
    
    ods_prices.write.jdbc(
        url=args.pg_url,
        table="ods_price_point",
        mode="append",
        properties={"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"}
    )
    
    print(f"✓ Wrote {ods_prices.count()} price points to ODS")
    
    # Reviews
    try:
        stg_reviews = spark.read.jdbc(
            url=args.pg_url,
            table="stg_raw_reviews",
            properties={"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"}
        )
        
        ods_reviews = stg_reviews.select(
            get_json_object(col("raw_data"), "$.review_id").alias("review_id"),
            get_json_object(col("raw_data"), "$.product_id").alias("product_id"),
            get_json_object(col("raw_data"), "$.reviewer_name").alias("reviewer_name"),
            get_json_object(col("raw_data"), "$.rating").cast("int").alias("rating"),
            get_json_object(col("raw_data"), "$.content").alias("content"),
            to_timestamp(get_json_object(col("raw_data"), "$.review_time")).alias("review_time"),
            col("source_platform"),
            to_timestamp(get_json_object(col("raw_data"), "$.crawl_date")).alias("crawled_at"),
            current_timestamp().alias("last_seen")
        ).filter(col("review_id").isNotNull())
        
        ods_reviews.write.jdbc(
            url=args.pg_url,
            table="ods_review_clean",
            mode="append",
            properties={"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"}
        )
        
        print(f"✓ Wrote {ods_reviews.count()} reviews to ODS")
    except Exception as e:
        print(f"⚠ No reviews: {e}")
    
    spark.stop()
    print("✅ ODS transformation completed")

if __name__ == "__main__":
    main()
