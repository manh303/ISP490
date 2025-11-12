# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, lag, when, dayofweek
from pyspark.sql.window import Window

def forecast_sales(spark, pg_url, pg_user, pg_pass):
    fact_daily = spark.read.jdbc(url=pg_url, table="dwh_fact_product_daily", properties={"user": pg_user, "password": pg_pass})
    dim_date = spark.read.jdbc(url=pg_url, table="dwh_dim_date", properties={"user": pg_user, "password": pg_pass})
    
    sales_data = fact_daily.join(dim_date, "date_sk").select("date_sk", "date_value", col("day").alias("day_of_week"), col("month").alias("month_num"), col("year").alias("year_num"), "review_count", "rating_avg")
    
    weekly = sales_data.groupBy("year_num", "day_of_week").agg(avg("review_count").alias("avg_weekly_reviews"), avg("rating_avg").alias("avg_weekly_rating"))
    
    monthly = sales_data.groupBy("year_num", "month_num").agg(sum("review_count").alias("total_monthly_reviews"), avg("rating_avg").alias("avg_monthly_rating"))
    
    seasonality = monthly.withColumn("season", when(col("month_num").isin(12, 1, 2), "Winter").when(col("month_num").isin(3, 4, 5), "Spring").when(col("month_num").isin(6, 7, 8), "Summer").otherwise("Fall")).groupBy("season").agg(avg("total_monthly_reviews").alias("avg_seasonal_reviews"), avg("avg_monthly_rating").alias("avg_seasonal_rating")).withColumn("seasonality_index", col("avg_seasonal_reviews") / avg("avg_seasonal_reviews").over(Window.partitionBy()))
    
    window_spec = Window.orderBy("year_num", "month_num")
    trend = monthly.withColumn("prev_month_reviews", lag("total_monthly_reviews", 1).over(window_spec)).withColumn("growth_rate", when(col("prev_month_reviews").isNotNull(), ((col("total_monthly_reviews") - col("prev_month_reviews")) / col("prev_month_reviews")) * 100).otherwise(0)).withColumn("trend", when(col("growth_rate") > 5, "Strong Growth").when(col("growth_rate") > 0, "Moderate Growth").when(col("growth_rate") > -5, "Slight Decline").otherwise("Strong Decline"))
    
    weekly.write.jdbc(url=pg_url, table="mart_sales_forecast_weekly", mode="overwrite", properties={"user": pg_user, "password": pg_pass})
    trend.write.jdbc(url=pg_url, table="mart_sales_trend", mode="overwrite", properties={"user": pg_user, "password": pg_pass})
    seasonality.write.jdbc(url=pg_url, table="mart_seasonality", mode="overwrite", properties={"user": pg_user, "password": pg_pass})
    
    print(f"✅ Generated sales forecasts: {weekly.count()} weekly, {trend.count()} trends, {seasonality.count()} seasonal patterns")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--pg-user", required=True)
    parser.add_argument("--pg-pass", required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("SalesForecasting").getOrCreate()
    forecast_sales(spark, args.pg_url, args.pg_user, args.pg_pass)
    spark.stop()
