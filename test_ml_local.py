# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

PG_HOST = "dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com"
PG_PORT = "5432"
PG_DB = "ecommerce_dss_1"
PG_USER = "dss_user"
PG_PASS = "6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G"
PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

def test_price_optimization():
    print("\n[PRICE] Testing Price Optimization...")
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when
    
    spark = SparkSession.builder.appName("TestPrice").master("local[*]").getOrCreate()
    price_analytics = spark.read.jdbc(url=PG_URL, table="dm_price_analytics", properties={"user": PG_USER, "password": PG_PASS})
    
    print(f"  Price Analytics: {price_analytics.count()} rows")
    
    optimized = price_analytics.select("product_name", "current_price", "avg_competitor_price", "price_position", "min_competitor_price", "max_competitor_price").withColumn("market_median", (col("min_competitor_price") + col("max_competitor_price")) / 2).withColumn("optimal_price", when(col("price_position") == "Below Market", col("current_price") * 1.05).when(col("price_position") == "Above Market", col("market_median")).otherwise(col("current_price"))).withColumn("expected_margin_change", ((col("optimal_price") - col("current_price")) / col("current_price")) * 100).withColumn("recommendation", when(col("expected_margin_change") > 3, "Increase Price").when(col("expected_margin_change") < -3, "Decrease Price").otherwise("Maintain Price"))
    
    result = optimized.select("product_name", "current_price", "optimal_price", "expected_margin_change", "recommendation", "price_position")
    
    print(f"  [OK] Generated {result.count()} price optimizations")
    result.show(5, truncate=False)
    spark.stop()

def test_demand_forecasting():
    print("\n[DEMAND] Testing Demand Forecasting...")
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, avg, when
    from pyspark.sql.window import Window
    
    spark = SparkSession.builder.appName("TestDemand").master("local[*]").getOrCreate()
    fact_daily = spark.read.jdbc(url=PG_URL, table="dwh_fact_product_daily", properties={"user": PG_USER, "password": PG_PASS})
    products = spark.read.jdbc(url=PG_URL, table="dwh_dim_product", properties={"user": PG_USER, "password": PG_PASS})
    
    print(f"  Daily Facts: {fact_daily.count()}, Products: {products.count()}")
    
    window_7d = Window.partitionBy("product_sk").orderBy("date_sk").rowsBetween(-6, 0)
    window_30d = Window.partitionBy("product_sk").orderBy("date_sk").rowsBetween(-29, 0)
    
    demand = fact_daily.join(products, "product_sk").select("product_sk", "product_name", "date_sk", "total_reviews", "avg_rating").withColumn("reviews_ma_7d", avg("total_reviews").over(window_7d)).withColumn("reviews_ma_30d", avg("total_reviews").over(window_30d))
    
    latest = demand.groupBy("product_sk", "product_name").agg(avg("reviews_ma_7d").alias("recent_demand"), avg("reviews_ma_30d").alias("baseline_demand"))
    
    forecast = latest.withColumn("demand_trend", when(col("recent_demand") > col("baseline_demand") * 1.2, "Growing").when(col("recent_demand") < col("baseline_demand") * 0.8, "Declining").otherwise("Stable")).withColumn("forecast_7d", col("recent_demand") * 7).withColumn("forecast_30d", col("baseline_demand") * 30)
    
    print(f"  [OK] Generated forecasts for {forecast.count()} products")
    forecast.show(5, truncate=False)
    spark.stop()

def test_sales_forecasting():
    print("\n[SALES] Testing Sales Forecasting...")
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, avg, sum, when
    from pyspark.sql.window import Window
    
    spark = SparkSession.builder.appName("TestSales").master("local[*]").getOrCreate()
    fact_daily = spark.read.jdbc(url=PG_URL, table="dwh_fact_product_daily", properties={"user": PG_USER, "password": PG_PASS})
    dim_date = spark.read.jdbc(url=PG_URL, table="dwh_dim_date", properties={"user": PG_USER, "password": PG_PASS})
    
    print(f"  Daily Facts: {fact_daily.count()}, Date Dimension: {dim_date.count()}")
    
    sales_data = fact_daily.join(dim_date, "date_sk").select("day_of_week", "month_num", "year_num", "total_reviews", "avg_rating")
    
    weekly = sales_data.groupBy("year_num", "day_of_week").agg(avg("total_reviews").alias("avg_weekly_reviews"), avg("avg_rating").alias("avg_weekly_rating"))
    
    monthly = sales_data.groupBy("year_num", "month_num").agg(sum("total_reviews").alias("total_monthly_reviews"), avg("avg_rating").alias("avg_monthly_rating"))
    
    seasonality = monthly.withColumn("season", when(col("month_num").isin(12, 1, 2), "Winter").when(col("month_num").isin(3, 4, 5), "Spring").when(col("month_num").isin(6, 7, 8), "Summer").otherwise("Fall")).groupBy("season").agg(avg("total_monthly_reviews").alias("avg_seasonal_reviews"))
    
    print(f"  [OK] Weekly: {weekly.count()}, Monthly: {monthly.count()}, Seasonality: {seasonality.count()}")
    print("\n  Seasonality:")
    seasonality.show()
    spark.stop()

if __name__ == "__main__":
    try:
        print("[TEST] Testing ML Models Locally\n" + "=" * 60)
        test_price_optimization()
        test_demand_forecasting()
        test_sales_forecasting()
        print("\n" + "=" * 60 + "\n[SUCCESS] All tests completed!")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
