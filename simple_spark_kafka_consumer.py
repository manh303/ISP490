#!/usr/bin/env python3
"""
Simple Spark Kafka Consumer for Vietnam E-commerce Data
Shows real-time data processing from Kafka to console
"""
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    """Create Spark session with Kafka support"""

    spark = SparkSession.builder \
        .appName("VietnamKafkaConsumer") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.cores", "1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    print("🚀 Starting Simple Spark Kafka Consumer...")

    # Create Spark session
    spark = create_spark_session()

    try:
        print("✅ Spark session created")
        print(f"🎯 Spark Master: spark://spark-master:7077")
        print(f"📡 Reading from Kafka topics...")

        # Define schema for Vietnam sales events
        sales_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("total_amount", LongType(), True),
            StructField("payment_method", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("province", StringType(), True)
        ])

        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "vietnam_sales_events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        print("✅ Kafka stream created")

        # Parse JSON data
        parsed_df = kafka_df.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), sales_schema).alias("data")
        ).select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("data.*")
        )

        # Add processing timestamp and calculations
        processed_df = parsed_df.select(
            col("*"),
            current_timestamp().alias("processed_at"),
            (col("total_amount") / 24000).alias("total_amount_usd"),
            when(col("payment_method") == "COD", "Cash_On_Delivery")
                .when(col("payment_method") == "MoMo", "MoMo_Wallet")
                .otherwise(col("payment_method")).alias("payment_method_en")
        )

        print("✅ Data transformation defined")
        print("🔄 Starting streaming query...")

        # Write to console to see the data processing
        query = processed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "10") \
            .trigger(processingTime="10 seconds") \
            .start()

        print("✅ Streaming query started!")
        print("📊 Processing Vietnam sales data from Kafka...")
        print("⏰ Data will appear every 10 seconds...")
        print("🛑 Press Ctrl+C to stop")

        # Wait for the streaming to finish
        query.awaitTermination(300)  # Run for 5 minutes

    except KeyboardInterrupt:
        print("\n⌨️ Stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("🛑 Stopping Spark session...")
        spark.stop()
        print("✅ Simple Spark Kafka Consumer stopped")

if __name__ == "__main__":
    main()