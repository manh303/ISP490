#!/usr/bin/env python3
"""Insert Reviews to ODS"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, to_timestamp, concat, lit, current_timestamp
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pg-url', required=True)
    parser.add_argument('--pg-user', required=True)
    parser.add_argument('--pg-pass', required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("Insert Reviews") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar") \
        .getOrCreate()
    
    print("=" * 60)
    print("INSERT REVIEWS")
    print("=" * 60)
    
    stg_reviews = spark.read.jdbc(
        url=args.pg_url,
        table="stg_raw_reviews",
        properties={"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"}
    )
    
    print(f"✓ Read {stg_reviews.count()} reviews from staging")
    
    ods_reviews = stg_reviews.select(
        concat(
            col("source_platform"),
            lit("_"),
            get_json_object(col("raw_data"), "$.review_id")
        ).alias("global_review_id"),
        col("source_platform"),
        get_json_object(col("raw_data"), "$.product_id").alias("platform_product_id"),
        get_json_object(col("raw_data"), "$.review_id").alias("review_id"),
        get_json_object(col("raw_data"), "$.reviewer_name").alias("reviewer_name"),
        get_json_object(col("raw_data"), "$.rating").cast("int").alias("rating"),
        get_json_object(col("raw_data"), "$.content").alias("review_text"),
        to_timestamp(get_json_object(col("raw_data"), "$.review_time")).alias("review_time"),
        lit(0).alias("helpful_count"),
        to_timestamp(get_json_object(col("raw_data"), "$.crawl_date")).alias("crawled_at"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("last_seen")
    ).filter(col("global_review_id").isNotNull())
    
    print(f"✓ Extracted {ods_reviews.count()} reviews")
    
    ods_reviews.write.jdbc(
        url=args.pg_url,
        table="ods_review_clean",
        mode="append",
        properties={"user": args.pg_user, "password": args.pg_pass, "driver": "org.postgresql.Driver"}
    )
    
    print(f"✓ Wrote reviews to ODS")
    
    spark.stop()
    print("✅ Reviews insertion completed")

if __name__ == "__main__":
    main()
