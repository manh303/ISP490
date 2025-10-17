#!/usr/bin/env python3
"""
Simple Spark Kafka Test
Test connection from Spark to Kafka
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("Simple Kafka Test") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def test_kafka_connection():
    """Test Kafka connection"""

    spark = create_spark_session()

    logger.info("🚀 Starting Simple Spark Kafka Test")
    logger.info("=" * 50)

    try:
        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "ecommerce.orders.stream") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        logger.info("✅ Kafka connection successful!")

        # Simple transformation
        processed_df = kafka_df.select(
            col("key").cast("string").alias("order_key"),
            col("value").cast("string").alias("order_data"),
            col("topic").alias("kafka_topic"),
            col("timestamp").alias("kafka_timestamp")
        )

        # Write to console for testing
        query = processed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime="10 seconds") \
            .start()

        logger.info("🔥 Spark Streaming started successfully!")
        logger.info("📊 Processing Kafka messages every 10 seconds...")

        # Run for 2 minutes then stop
        query.awaitTermination(120)

        logger.info("✅ Spark Kafka test completed successfully!")

    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    test_kafka_connection()