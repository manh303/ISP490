# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

def optimize_prices(spark, pg_url, pg_user, pg_pass):
    price_analytics = spark.read.jdbc(url=pg_url, table="dm_price_analytics", properties={"user": pg_user, "password": pg_pass})
    products = spark.read.jdbc(url=pg_url, table="dwh_dim_product", properties={"user": pg_user, "password": pg_pass})
    
    data = price_analytics.join(products, "product_sk").select("product_sk", "product_name", col("price_current").alias("current_price"), col("competitor_min_price").alias("min_competitor_price"), col("competitor_max_price").alias("max_competitor_price"), "price_trend")
    
    data = data.withColumn("avg_competitor_price", (col("min_competitor_price") + col("max_competitor_price")) / 2).withColumn("price_position", when(col("current_price") < col("min_competitor_price"), "Below Market").when(col("current_price") > col("max_competitor_price"), "Above Market").otherwise("At Market"))
    
    optimized = data.withColumn("optimal_price", when(col("price_position") == "Below Market", col("current_price") * 1.05).when(col("price_position") == "Above Market", col("avg_competitor_price")).otherwise(col("current_price"))).withColumn("expected_margin_change", ((col("optimal_price") - col("current_price")) / col("current_price")) * 100).withColumn("recommendation", when(col("expected_margin_change") > 3, "Increase Price").when(col("expected_margin_change") < -3, "Decrease Price").otherwise("Maintain Price"))
    
    result = optimized.select("product_sk", "product_name", "current_price", "optimal_price", "expected_margin_change", "recommendation", "price_position")
    
    result.write.jdbc(url=pg_url, table="mart_price_optimization", mode="overwrite", properties={"user": pg_user, "password": pg_pass})
    print(f"✅ Generated {result.count()} price optimizations")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--pg-user", required=True)
    parser.add_argument("--pg-pass", required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("PriceOptimization").getOrCreate()
    optimize_prices(spark, args.pg_url, args.pg_user, args.pg_pass)
    spark.stop()
