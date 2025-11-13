# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when
from pyspark.sql.window import Window

def forecast_demand(spark, pg_url, pg_user, pg_pass):
    fact_daily = spark.read.jdbc(url=pg_url, table="dwh_fact_product_daily", properties={"user": pg_user, "password": pg_pass})
    products = spark.read.jdbc(url=pg_url, table="dwh_dim_product", properties={"user": pg_user, "password": pg_pass})
    
    window_7d = Window.partitionBy("product_sk").orderBy("date_sk").rowsBetween(-6, 0)
    window_30d = Window.partitionBy("product_sk").orderBy("date_sk").rowsBetween(-29, 0)
    
    demand = fact_daily.join(products, "product_sk").select("product_sk", "product_name", "date_sk", "review_count", "rating_avg").withColumn("reviews_ma_7d", avg("review_count").over(window_7d)).withColumn("reviews_ma_30d", avg("review_count").over(window_30d)).withColumn("rating_trend", avg("rating_avg").over(window_7d))
    
    latest = demand.groupBy("product_sk", "product_name").agg(avg("reviews_ma_7d").alias("recent_demand"), avg("reviews_ma_30d").alias("baseline_demand"), avg("rating_trend").alias("quality_score"))
    
    forecast = latest.withColumn("demand_trend", when(col("recent_demand") > col("baseline_demand") * 1.2, "Growing").when(col("recent_demand") < col("baseline_demand") * 0.8, "Declining").otherwise("Stable")).withColumn("forecast_7d", col("recent_demand") * 7).withColumn("forecast_30d", col("baseline_demand") * 30).withColumn("stock_recommendation", when(col("demand_trend") == "Growing", "Increase Stock").when(col("demand_trend") == "Declining", "Reduce Stock").otherwise("Maintain Stock"))
    
    result = forecast.select("product_sk", "product_name", "recent_demand", "baseline_demand", "demand_trend", "forecast_7d", "forecast_30d", "quality_score", "stock_recommendation")
    
    result.write.jdbc(url=pg_url, table="mart_demand_forecast", mode="overwrite", properties={"user": pg_user, "password": pg_pass})
    print(f"✅ Generated forecasts for {result.count()} products")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--pg-user", required=True)
    parser.add_argument("--pg-pass", required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("DemandForecasting").getOrCreate()
    forecast_demand(spark, args.pg_url, args.pg_user, args.pg_pass)
    spark.stop()
