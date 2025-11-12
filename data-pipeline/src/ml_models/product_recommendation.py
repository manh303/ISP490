# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer

def build_recommendations(spark, pg_url, pg_user, pg_pass):
    reviews = spark.read.jdbc(url=pg_url, table="ods_review_clean", properties={"user": pg_user, "password": pg_pass})
    products = spark.read.jdbc(url=pg_url, table="ods_product_clean", properties={"user": pg_user, "password": pg_pass})
    
    interactions = reviews.join(products, reviews.platform_product_id == products.platform_product_id).select("reviewer_name", products.global_product_id, "rating").filter(col("reviewer_name").isNotNull())
    
    user_indexer = StringIndexer(inputCol="reviewer_name", outputCol="user_id")
    product_indexer = StringIndexer(inputCol="global_product_id", outputCol="product_id")
    interactions = user_indexer.fit(interactions).transform(interactions)
    interactions = product_indexer.fit(interactions).transform(interactions)
    
    als = ALS(userCol="user_id", itemCol="product_id", ratingCol="rating", coldStartStrategy="drop", maxIter=10, regParam=0.1, rank=10)
    model = als.fit(interactions)
    
    product_recs = model.recommendForAllItems(10)
    recs_flat = product_recs.select(col("product_id").alias("source_product_id"), explode("recommendations").alias("rec")).select("source_product_id", col("rec.product_id").alias("recommended_product_id"), col("rec.rating").alias("score"))
    
    product_map = product_indexer.fit(interactions).labels
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    get_product = udf(lambda idx: product_map[int(idx)] if idx < len(product_map) else None, StringType())
    
    recs_flat = recs_flat.withColumn("source_global_id", get_product(col("source_product_id"))).withColumn("rec_global_id", get_product(col("recommended_product_id")))
    
    result = recs_flat.join(products.alias("src"), col("source_global_id") == col("src.global_product_id")).join(products.alias("rec"), col("rec_global_id") == col("rec.global_product_id")).select(col("src.product_name").alias("source_product"), col("rec.product_name").alias("recommended_product"), col("rec.global_product_id").alias("recommended_product_id"), col("score"))
    
    result.write.jdbc(url=pg_url, table="mart_product_recommendations", mode="overwrite", properties={"user": pg_user, "password": pg_pass})
    print(f"✅ Generated {result.count()} recommendations")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--pg-user", required=True)
    parser.add_argument("--pg-pass", required=True)
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("ProductRecommendation").getOrCreate()
    build_recommendations(spark, args.pg_url, args.pg_user, args.pg_pass)
    spark.stop()
