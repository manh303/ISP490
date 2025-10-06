#!/usr/bin/env python3
"""
Production Spark Streaming Jobs for Big Data Pipeline
Real-time processing with multiple transformations, windowing, and dual storage
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Spark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, isnan, isnull, regexp_replace, split, trim,
    to_timestamp, date_format, year, month, dayofmonth, hour,
    window, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    first, last, collect_list, struct, to_json, from_json,
    udf, monotonically_increasing_id, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType,
    TimestampType, BooleanType, DecimalType, ArrayType
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.window import Window

# Add project paths
sys.path.append('/app/data-pipeline/src')
sys.path.append('/app/streaming')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkStreamingProcessor:
    """
    Complete Spark Streaming Processor for Big Data Pipeline
    Handles real-time stream processing with advanced transformations
    """
    
    def __init__(self):
        self.spark = None
        self.streaming_queries = []
        self.config = self._load_config()
        self._setup_spark()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load streaming configuration"""
        return {
            # Kafka Configuration
            'kafka_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'kafka_topics': {
                'products': 'products_stream',
                'customers': 'customers_stream',
                'orders': 'orders_stream', 
                'transactions': 'transactions_stream'
            },
            
            # Spark Configuration
            'checkpoint_location': os.getenv('SPARK_CHECKPOINT_LOCATION', '/app/checkpoints'),
            'batch_duration': os.getenv('SPARK_STREAMING_BATCH_DURATION', '30 seconds'),
            'watermark_delay': os.getenv('SPARK_WATERMARK_DELAY', '10 minutes'),
            
            # Database Configuration
            'postgres_url': os.getenv('DATABASE_URL', 'jdbc:postgresql://postgres:5432/dss_analytics'),
            'postgres_user': os.getenv('DB_USER', 'admin'),
            'postgres_password': os.getenv('DB_PASSWORD', 'admin123'),
            'mongo_uri': os.getenv('MONGODB_URL', 'mongodb://root:admin123@mongodb:27017/'),
            
            # Processing Configuration
            'window_duration': '5 minutes',
            'slide_duration': '1 minute',
            'enable_data_quality': True,
            'enable_real_time_alerts': True
        }
    
    def _setup_spark(self):
        """Initialize Spark Session with optimized configuration"""
        self.spark = SparkSession.builder \
            .appName("Big Data Streaming Pipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", self.config['checkpoint_location']) \
            .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.metricsEnabled", "true") \
            .config("spark.sql.streaming.ui.enabled", "true") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                   "org.postgresql:postgresql:42.5.0,"
                   "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark Session initialized successfully")

    def _define_schemas(self) -> Dict[str, StructType]:
        """Define schemas for different data streams"""
        schemas = {}
        
        # Products Schema
        schemas['products'] = StructType([
            StructField("product_id", StringType(), True),
            StructField("product_category_name", StringType(), True),
            StructField("product_name_length", IntegerType(), True),
            StructField("product_description_length", IntegerType(), True),
            StructField("product_photos_qty", IntegerType(), True),
            StructField("product_weight_g", FloatType(), True),
            StructField("product_length_cm", FloatType(), True),
            StructField("product_height_cm", FloatType(), True),
            StructField("product_width_cm", FloatType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("source", StringType(), True)
        ])
        
        # Customers Schema
        schemas['customers'] = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_unique_id", StringType(), True),
            StructField("customer_zip_code_prefix", StringType(), True),
            StructField("customer_city", StringType(), True),
            StructField("customer_state", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("source", StringType(), True)
        ])
        
        # Orders Schema  
        schemas['orders'] = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", TimestampType(), True),
            StructField("order_approved_at", TimestampType(), True),
            StructField("order_delivered_carrier_date", TimestampType(), True),
            StructField("order_delivered_customer_date", TimestampType(), True),
            StructField("order_estimated_delivery_date", TimestampType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("source", StringType(), True)
        ])
        
        # Order Items Schema
        schemas['order_items'] = StructType([
            StructField("order_id", StringType(), True),
            StructField("order_item_id", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("shipping_limit_date", TimestampType(), True),
            StructField("price", DecimalType(10,2), True),
            StructField("freight_value", DecimalType(10,2), True),
            StructField("timestamp", TimestampType(), True),
            StructField("source", StringType(), True)
        ])
        
        return schemas

    def _create_kafka_stream(self, topic: str, schema: StructType) -> DataFrame:
        """Create Kafka streaming DataFrame"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['kafka_servers']) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.consumer.group.id", f"spark_consumer_{topic}") \
            .load() \
            .select(
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("kafka_value"),
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("timestamp").alias("kafka_timestamp")
            ) \
            .select(
                "*",
                from_json(col("kafka_value"), schema).alias("data")
            ) \
            .select("kafka_*", "data.*") \
            .filter(col("data").isNotNull())

    def _clean_and_validate_data(self, df: DataFrame, stream_type: str) -> DataFrame:
        """Apply data cleaning and validation rules"""
        
        if stream_type == "products":
            return df.filter(col("product_id").isNotNull()) \
                    .withColumn("product_weight_g", 
                              when(col("product_weight_g") < 0, lit(None))
                              .otherwise(col("product_weight_g"))) \
                    .withColumn("product_category_name", 
                              trim(regexp_replace(col("product_category_name"), "[^a-zA-Z0-9_]", ""))) \
                    .filter(col("product_category_name") != "")
                    
        elif stream_type == "customers":
            return df.filter(col("customer_id").isNotNull()) \
                    .withColumn("customer_zip_code_prefix",
                              regexp_replace(col("customer_zip_code_prefix"), "[^0-9]", "")) \
                    .filter(col("customer_zip_code_prefix").rlike("^[0-9]{5}$")) \
                    .withColumn("customer_city", trim(col("customer_city"))) \
                    .withColumn("customer_state", trim(col("customer_state")))
                    
        elif stream_type == "orders":
            return df.filter(col("order_id").isNotNull()) \
                    .filter(col("customer_id").isNotNull()) \
                    .filter(col("order_purchase_timestamp").isNotNull()) \
                    .withColumn("order_status", trim(col("order_status"))) \
                    .filter(col("order_status").isin([
                        "delivered", "shipped", "processing", "invoiced", 
                        "canceled", "unavailable", "approved"
                    ]))
        
        return df

    def _create_real_time_analytics(self, df: DataFrame, stream_type: str) -> DataFrame:
        """Create real-time analytics aggregations"""
        
        # Add processing timestamp and extract time components
        df_with_time = df.withColumn("processing_time", lit(datetime.now())) \
                        .withColumn("hour", hour(col("timestamp"))) \
                        .withColumn("day", dayofmonth(col("timestamp"))) \
                        .withColumn("month", month(col("timestamp"))) \
                        .withColumn("year", year(col("timestamp")))
        
        if stream_type == "orders":
            # Orders analytics with windowing
            return df_with_time \
                .withWatermark("timestamp", self.config['watermark_delay']) \
                .groupBy(
                    window(col("timestamp"), self.config['window_duration'], self.config['slide_duration']),
                    "order_status",
                    "hour"
                ) \
                .agg(
                    count("*").alias("order_count"),
                    count("customer_id").alias("unique_customers"),
                    first("processing_time").alias("window_processed_at")
                ) \
                .withColumn("metric_type", lit("orders_hourly")) \
                .withColumn("window_start", col("window.start")) \
                .withColumn("window_end", col("window.end")) \
                .drop("window")
                
        elif stream_type == "products":
            # Products analytics
            return df_with_time \
                .withWatermark("timestamp", self.config['watermark_delay']) \
                .groupBy(
                    window(col("timestamp"), self.config['window_duration'], self.config['slide_duration']),
                    "product_category_name"
                ) \
                .agg(
                    count("*").alias("product_count"),
                    avg("product_weight_g").alias("avg_weight"),
                    spark_max("product_weight_g").alias("max_weight"),
                    first("processing_time").alias("window_processed_at")
                ) \
                .withColumn("metric_type", lit("products_hourly")) \
                .withColumn("window_start", col("window.start")) \
                .withColumn("window_end", col("window.end")) \
                .drop("window")
                
        elif stream_type == "customers":  
            # Customer analytics
            return df_with_time \
                .withWatermark("timestamp", self.config['watermark_delay']) \
                .groupBy(
                    window(col("timestamp"), self.config['window_duration'], self.config['slide_duration']),
                    "customer_state"
                ) \
                .agg(
                    count("*").alias("customer_count"),
                    first("processing_time").alias("window_processed_at")
                ) \
                .withColumn("metric_type", lit("customers_hourly")) \
                .withColumn("window_start", col("window.start")) \
                .withColumn("window_end", col("window.end")) \
                .drop("window")
        
        return df_with_time

    def _write_to_postgres(self, df: DataFrame, table_name: str, mode: str = "append"):
        """Write streaming data to PostgreSQL"""
        return df.writeStream \
            .outputMode(mode) \
            .foreachBatch(lambda batch_df, batch_id: self._postgres_batch_writer(batch_df, table_name, batch_id)) \
            .option("checkpointLocation", f"{self.config['checkpoint_location']}/postgres_{table_name}") \
            .trigger(processingTime=self.config['batch_duration']) \
            .start()

    def _postgres_batch_writer(self, batch_df: DataFrame, table_name: str, batch_id: int):
        """Custom batch writer for PostgreSQL"""
        if batch_df.count() > 0:
            try:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", self.config['postgres_url']) \
                    .option("dbtable", table_name) \
                    .option("user", self.config['postgres_user']) \
                    .option("password", self.config['postgres_password']) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                    
                logger.info(f"✅ Batch {batch_id}: Written {batch_df.count()} records to {table_name}")
            except Exception as e:
                logger.error(f"❌ Batch {batch_id}: Failed to write to {table_name}: {e}")

    def _write_to_mongodb(self, df: DataFrame, collection_name: str):
        """Write streaming data to MongoDB"""
        return df.writeStream \
            .outputMode("append") \
            .foreachBatch(lambda batch_df, batch_id: self._mongodb_batch_writer(batch_df, collection_name, batch_id)) \
            .option("checkpointLocation", f"{self.config['checkpoint_location']}/mongodb_{collection_name}") \
            .trigger(processingTime=self.config['batch_duration']) \
            .start()

    def _mongodb_batch_writer(self, batch_df: DataFrame, collection_name: str, batch_id: int):
        """Custom batch writer for MongoDB"""
        if batch_df.count() > 0:
            try:
                batch_df.write \
                    .format("mongo") \
                    .option("uri", f"{self.config['mongo_uri']}dss_streaming.{collection_name}") \
                    .mode("append") \
                    .save()
                    
                logger.info(f"✅ Batch {batch_id}: Written {batch_df.count()} records to MongoDB {collection_name}")
            except Exception as e:
                logger.error(f"❌ Batch {batch_id}: Failed to write to MongoDB {collection_name}: {e}")

    def _create_data_quality_checks(self, df: DataFrame, stream_type: str) -> DataFrame:
        """Create data quality validation checks"""
        
        quality_checks = df.select(
            lit(stream_type).alias("stream_type"),
            lit(datetime.now()).alias("check_timestamp"),
            
            # Null checks
            when(col("timestamp").isNull(), lit("timestamp_null")).alias("null_check_timestamp"),
            
            # Data type checks
            when(col("timestamp").isNull(), lit(0)).otherwise(lit(1)).alias("timestamp_valid"),
            
            # Business rule checks
            when(stream_type == "orders", 
                when(col("order_purchase_timestamp") > col("order_delivered_customer_date"), lit(0))
                .otherwise(lit(1))).alias("order_date_logic_valid"),
                
            # Row count for monitoring
            lit(1).alias("row_count")
        )
        
        return quality_checks \
            .withWatermark("check_timestamp", "5 minutes") \
            .groupBy(
                window(col("check_timestamp"), "5 minutes"),
                "stream_type"
            ) \
            .agg(
                spark_sum("row_count").alias("total_rows"),
                spark_sum("timestamp_valid").alias("valid_timestamps"),
                spark_sum("order_date_logic_valid").alias("valid_order_dates")
            ) \
            .withColumn("timestamp_validity_rate", 
                      col("valid_timestamps") / col("total_rows")) \
            .withColumn("order_date_validity_rate",
                      col("valid_order_dates") / col("total_rows"))

    def start_streaming_jobs(self):
        """Start all streaming jobs"""
        logger.info("🚀 Starting Spark Streaming Jobs...")
        
        schemas = self._define_schemas()
        
        # Process each stream type
        for stream_type, topic in self.config['kafka_topics'].items():
            logger.info(f"📊 Starting {stream_type} stream processing...")
            
            try:
                # Create Kafka stream
                raw_stream = self._create_kafka_stream(topic, schemas.get(stream_type))
                
                # Clean and validate data
                clean_stream = self._clean_and_validate_data(raw_stream, stream_type)
                
                # Create real-time analytics
                analytics_stream = self._create_real_time_analytics(clean_stream, stream_type)
                
                # Write raw data to MongoDB
                mongodb_query = self._write_to_mongodb(
                    clean_stream.select("*", lit(stream_type).alias("stream_type")),
                    f"{stream_type}_raw"
                )
                self.streaming_queries.append(mongodb_query)
                
                # Write analytics to PostgreSQL
                postgres_query = self._write_to_postgres(
                    analytics_stream,
                    f"{stream_type}_analytics",
                    "append"
                )
                self.streaming_queries.append(postgres_query)
                
                # Data quality monitoring
                if self.config['enable_data_quality']:
                    quality_stream = self._create_data_quality_checks(clean_stream, stream_type)
                    quality_query = self._write_to_postgres(
                        quality_stream,
                        "data_quality_metrics",
                        "append"
                    )
                    self.streaming_queries.append(quality_query)
                
                logger.info(f"✅ {stream_type} stream processing started successfully")
                
            except Exception as e:
                logger.error(f"❌ Failed to start {stream_type} stream: {e}")

    def monitor_streaming_queries(self):
        """Monitor all streaming queries"""
        logger.info("📈 Starting streaming queries monitoring...")
        
        while True:
            try:
                for i, query in enumerate(self.streaming_queries):
                    if query.isActive:
                        progress = query.lastProgress
                        if progress:
                            logger.info(f"Query {i}: Processed {progress.get('inputRowsPerSecond', 0)} rows/sec")
                    else:
                        logger.warning(f"Query {i} is not active!")
                        
                # Wait before next check
                self.spark.streams.awaitAnyTermination(60)  # 60 seconds
                
            except KeyboardInterrupt:
                logger.info("🛑 Stopping monitoring...")
                break
            except Exception as e:
                logger.error(f"❌ Monitoring error: {e}")

    def stop_all_streams(self):
        """Gracefully stop all streaming queries"""
        logger.info("🛑 Stopping all streaming queries...")
        
        for i, query in enumerate(self.streaming_queries):
            try:
                query.stop()
                logger.info(f"✅ Stopped query {i}")
            except Exception as e:
                logger.error(f"❌ Error stopping query {i}: {e}")
        
        if self.spark:
            self.spark.stop()
            logger.info("✅ Spark session stopped")

def main():
    """Main execution"""
    processor = None
    
    try:
        # Create and start processor
        processor = SparkStreamingProcessor()
        processor.start_streaming_jobs()
        
        # Monitor streams
        processor.monitor_streaming_queries()
        
    except KeyboardInterrupt:
        logger.info("🛑 Received shutdown signal")
    except Exception as e:
        logger.error(f"❌ Streaming processor error: {e}")
    finally:
        if processor:
            processor.stop_all_streams()

if __name__ == "__main__":
    main()