#!/usr/bin/env python3
"""
Vietnam E-commerce Spark Streaming Consumer
==========================================
Spark Structured Streaming consumer for Vietnam e-commerce data warehouse
Consumes Kafka streams and writes to PostgreSQL Vietnam DW

Features:
- Real-time data processing from Kafka
- Vietnam-specific transformations
- Direct writes to vietnam_dw schema
- Data quality validation
- Batch and micro-batch processing
- Error handling and recovery
- Performance monitoring

Author: DSS Team
Version: 1.0.0
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Spark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery

# Data processing
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2

# Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================================================================
# CONFIGURATION
# ====================================================================

class SparkStreamingConfig:
    """Spark Streaming Configuration for Vietnam DW"""

    # Kafka configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    KAFKA_TOPICS = {
        "sales": "vietnam_sales_events",
        "customers": "vietnam_customers",
        "products": "vietnam_products",
        "activities": "vietnam_user_activities"
    }

    # PostgreSQL configuration
    POSTGRES_HOST = os.getenv("DB_HOST", "postgres")
    POSTGRES_PORT = int(os.getenv("DB_PORT", "5432"))
    POSTGRES_DB = os.getenv("DB_NAME", "ecommerce_dss")
    POSTGRES_USER = os.getenv("DB_USER", "dss_user")
    POSTGRES_PASSWORD = os.getenv("DB_PASSWORD", "dss_password_123")
    POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    # Spark configuration
    APP_NAME = "Vietnam_Ecommerce_Streaming_Consumer"
    CHECKPOINT_LOCATION = "/app/streaming/checkpoints"
    BATCH_INTERVAL = "30 seconds"
    MAX_FILES_PER_TRIGGER = 1000

    # Performance tuning
    SPARK_EXECUTOR_MEMORY = "2g"
    SPARK_DRIVER_MEMORY = "1g"
    SPARK_EXECUTOR_CORES = 2
    SPARK_SQL_SHUFFLE_PARTITIONS = 200

config = SparkStreamingConfig()

# ====================================================================
# SPARK SESSION INITIALIZATION
# ====================================================================

def create_spark_session() -> SparkSession:
    """Create optimized Spark session for Vietnam streaming"""

    spark = SparkSession.builder \
        .appName(config.APP_NAME) \
        .master("spark://spark-master:7077") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", config.SPARK_SQL_SHUFFLE_PARTITIONS) \
        .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
        .config("spark.executor.cores", config.SPARK_EXECUTOR_CORES) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.checkpointLocation", config.CHECKPOINT_LOCATION) \
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    logger.info("✅ Spark session created successfully")
    logger.info(f"🎯 Application: {config.APP_NAME}")
    logger.info(f"🔗 Kafka servers: {config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"🗄️ PostgreSQL: {config.POSTGRES_URL}")

    return spark

# ====================================================================
# SCHEMA DEFINITIONS
# ====================================================================

# Sales event schema (from Kafka)
SALES_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price_vnd", LongType(), True),
    StructField("unit_price_usd", DoubleType(), True),
    StructField("total_amount_vnd", LongType(), True),
    StructField("total_amount_usd", DoubleType(), True),
    StructField("discount_amount_vnd", LongType(), True),
    StructField("tax_amount_vnd", LongType(), True),
    StructField("shipping_fee_vnd", LongType(), True),
    StructField("payment_method", StringType(), True),
    StructField("shipping_method", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("is_cod", BooleanType(), True),
    StructField("is_tet_order", BooleanType(), True),
    StructField("is_festival_order", BooleanType(), True),
    StructField("order_source", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("shipping_province", StringType(), True),
    StructField("shipping_region", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("order_timestamp", TimestampType(), True)
])

# Customer schema
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("province", StringType(), True),
    StructField("region", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("income_level", StringType(), True),
    StructField("preferred_platform", StringType(), True),
    StructField("preferred_payment", StringType(), True),
    StructField("preferred_device", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Product schema
PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name_vn", StringType(), True),
    StructField("product_name_en", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("category_l1", StringType(), True),
    StructField("category_l2", StringType(), True),
    StructField("price_vnd", LongType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("rating", DoubleType(), True),
    StructField("review_count", IntegerType(), True),
    StructField("available_platforms", ArrayType(StringType()), True),
    StructField("made_in_vietnam", BooleanType(), True),
    StructField("created_at", TimestampType(), True)
])

# Activity schema
ACTIVITY_SCHEMA = StructType([
    StructField("activity_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("activity_type", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

# ====================================================================
# VIETNAM DATA TRANSFORMATIONS
# ====================================================================

class VietnamDataTransformer:
    """Vietnam-specific data transformations for streaming"""

    @staticmethod
    def transform_sales_for_dw(df: DataFrame) -> DataFrame:
        """Transform sales events for Vietnam data warehouse"""

        # Add Vietnam-specific columns and transformations
        transformed_df = df.select(
            col("order_id").alias("order_id"),
            col("customer_id"),
            col("product_id"),
            col("platform"),

            # Quantities
            col("quantity").alias("quantity_ordered"),
            lit(0).alias("quantity_shipped"),
            lit(0).alias("quantity_returned"),
            lit(0).alias("quantity_cancelled"),

            # Pricing (VND primary)
            col("unit_price_vnd"),
            col("total_amount_vnd").alias("gross_sales_amount_vnd"),
            col("discount_amount_vnd"),
            (col("total_amount_vnd") - col("discount_amount_vnd")).alias("net_sales_amount_vnd"),
            col("tax_amount_vnd"),
            col("shipping_fee_vnd"),

            # USD conversion
            col("unit_price_usd"),
            col("total_amount_usd").alias("net_sales_amount_usd"),
            lit(24000.0).alias("exchange_rate_vnd_usd"),

            # Payment info (Vietnam)
            col("payment_method").alias("payment_method_english"),
            when(col("payment_method") == "COD", "Thanh toán khi nhận hàng")
                .when(col("payment_method") == "MoMo", "Ví MoMo")
                .when(col("payment_method") == "ZaloPay", "Ví ZaloPay")
                .when(col("payment_method") == "VNPay", "Ví VNPay")
                .when(col("payment_method") == "Banking", "Chuyển khoản ngân hàng")
                .otherwise("Thẻ tín dụng").alias("payment_method_vietnamese"),

            col("is_cod").alias("is_cod_order"),
            lit(false).alias("is_installment_order"),

            # Shipping
            col("shipping_method").alias("shipping_method_english"),
            when(col("shipping_method") == "Standard", "Giao hàng tiêu chuẩn")
                .when(col("shipping_method") == "Express", "Giao hàng nhanh")
                .when(col("shipping_method") == "Same_Day", "Giao hàng trong ngày")
                .otherwise("Giao hàng tiêu chuẩn").alias("shipping_method_vietnamese"),

            # Order status
            col("order_status").alias("order_status_english"),
            when(col("order_status") == "Pending", "Đang xử lý")
                .when(col("order_status") == "Confirmed", "Đã xác nhận")
                .when(col("order_status") == "Shipped", "Đang giao hàng")
                .when(col("order_status") == "Delivered", "Đã giao")
                .when(col("order_status") == "Cancelled", "Đã hủy")
                .otherwise("Đang xử lý").alias("order_status_vietnamese"),

            # Vietnam flags
            col("is_tet_order"),
            col("is_festival_order").alias("is_festival_order"),
            lit(false).alias("applied_vietnam_promotion"),

            # Geography
            col("shipping_province"),
            col("shipping_region"),

            # Timestamps
            col("order_timestamp"),
            col("event_timestamp").alias("created_at"),
            current_timestamp().alias("updated_at")
        )

        return transformed_df

    @staticmethod
    def transform_customers_for_dw(df: DataFrame) -> DataFrame:
        """Transform customer data for Vietnam data warehouse"""

        transformed_df = df.select(
            col("customer_id"),
            col("full_name").alias("full_name_vietnamese"),
            col("email"),
            col("phone").alias("phone_number"),
            col("age"),
            col("gender"),
            col("province"),
            col("region"),

            # Segments
            col("customer_segment").alias("customer_segment_english"),
            when(col("customer_segment") == "Regular", "Khách hàng thường")
                .when(col("customer_segment") == "High_Value", "Khách hàng VIP")
                .when(col("customer_segment") == "Premium", "Khách hàng cao cấp")
                .otherwise("Khách hàng thường").alias("customer_segment_vietnamese"),

            col("income_level").alias("income_level_english"),
            when(col("income_level") == "Low", "Thu nhập thấp")
                .when(col("income_level") == "Middle", "Thu nhập trung bình")
                .when(col("income_level") == "High", "Thu nhập cao")
                .otherwise("Thu nhập trung bình").alias("income_level_vietnamese"),

            # Preferences
            col("preferred_platform"),
            col("preferred_payment").alias("preferred_payment_method"),
            col("preferred_device"),

            # SCD Type 2 fields
            current_date().alias("effective_date"),
            lit(None).cast(DateType()).alias("expiry_date"),
            lit(true).alias("is_current"),

            # Timestamps
            col("created_at"),
            current_timestamp().alias("updated_at")
        )

        return transformed_df

    @staticmethod
    def transform_products_for_dw(df: DataFrame) -> DataFrame:
        """Transform product data for Vietnam data warehouse"""

        transformed_df = df.select(
            col("product_id"),
            col("product_name_vn").alias("product_name_vietnamese"),
            col("product_name_en").alias("product_name_english"),
            col("brand").alias("brand_vietnamese"),
            col("brand").alias("brand_english"),

            # Categories
            col("category_l1").alias("category_level_1_vietnamese"),
            col("category_l1").alias("category_level_1_english"),
            col("category_l2").alias("category_level_2_vietnamese"),
            col("category_l2").alias("category_level_2_english"),

            # Pricing (VND primary)
            col("price_vnd"),
            col("price_usd"),

            # Quality metrics
            col("rating"),
            col("review_count"),

            # Vietnam specific
            col("made_in_vietnam"),
            col("available_platforms").alias("platform_availability"),

            # SCD Type 2 fields
            current_date().alias("effective_date"),
            lit(None).cast(DateType()).alias("expiry_date"),
            lit(true).alias("is_current"),

            # Metadata
            lit("kafka_streaming").alias("data_source"),
            col("created_at"),
            current_timestamp().alias("updated_at")
        )

        return transformed_df

# ====================================================================
# DIMENSION KEY LOOKUP
# ====================================================================

class DimensionKeyLookup:
    """Lookup dimension keys for fact table inserts"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def lookup_keys_for_sales(self, sales_df: DataFrame) -> DataFrame:
        """Lookup dimension keys for sales fact table"""

        # Read dimension tables from PostgreSQL
        customer_dim = self.spark.read \
            .format("jdbc") \
            .option("url", config.POSTGRES_URL) \
            .option("dbtable", "vietnam_dw.dim_customer_vn") \
            .option("user", config.POSTGRES_USER) \
            .option("password", config.POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .filter(col("is_current") == True) \
            .select("customer_key", "customer_id")

        product_dim = self.spark.read \
            .format("jdbc") \
            .option("url", config.POSTGRES_URL) \
            .option("dbtable", "vietnam_dw.dim_product_vn") \
            .option("user", config.POSTGRES_USER) \
            .option("password", config.POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .filter(col("is_current") == True) \
            .select("product_key", "product_id")

        platform_dim = self.spark.read \
            .format("jdbc") \
            .option("url", config.POSTGRES_URL) \
            .option("dbtable", "vietnam_dw.dim_platform_vn") \
            .option("user", config.POSTGRES_USER) \
            .option("password", config.POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .select("platform_key", "platform_name")

        time_dim = self.spark.read \
            .format("jdbc") \
            .option("url", config.POSTGRES_URL) \
            .option("dbtable", "vietnam_dw.dim_time") \
            .option("user", config.POSTGRES_USER) \
            .option("password", config.POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .select("time_key", "date_value")

        geography_dim = self.spark.read \
            .format("jdbc") \
            .option("url", config.POSTGRES_URL) \
            .option("dbtable", "vietnam_dw.dim_geography_vn") \
            .option("user", config.POSTGRES_USER) \
            .option("password", config.POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .select("geography_key", "province_name_vietnamese")

        # Join with dimension keys
        sales_with_keys = sales_df \
            .join(customer_dim, "customer_id", "left") \
            .join(product_dim, "product_id", "left") \
            .join(platform_dim, col("platform") == col("platform_name"), "left") \
            .join(time_dim,
                  date_format(col("order_timestamp"), "yyyy-MM-dd") == col("date_value"),
                  "left") \
            .join(geography_dim,
                  col("shipping_province") == col("province_name_vietnamese"),
                  "left") \
            .drop("customer_id", "product_id", "platform", "platform_name",
                  "date_value", "province_name_vietnamese")

        return sales_with_keys

# ====================================================================
# STREAMING PROCESSORS
# ====================================================================

class VietnamStreamingProcessor:
    """Main streaming processor for Vietnam e-commerce data"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.transformer = VietnamDataTransformer()
        self.dimension_lookup = DimensionKeyLookup(spark)
        self.active_queries: List[StreamingQuery] = []

    def create_kafka_stream(self, topic: str, schema: StructType) -> DataFrame:
        """Create Kafka streaming DataFrame"""

        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", config.MAX_FILES_PER_TRIGGER) \
            .option("failOnDataLoss", "false") \
            .load()

        # Parse JSON value
        parsed_df = kafka_df.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), schema).alias("data")
        ).select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("data.*")
        )

        return parsed_df

    def write_to_postgres(self, df: DataFrame, table_name: str, mode: str = "append") -> StreamingQuery:
        """Write streaming DataFrame to PostgreSQL"""

        def write_batch(batch_df, batch_id):
            """Write each micro-batch to PostgreSQL"""
            try:
                if batch_df.count() > 0:
                    logger.info(f"📤 Writing batch {batch_id} to {table_name}: {batch_df.count()} records")

                    batch_df.write \
                        .format("jdbc") \
                        .option("url", config.POSTGRES_URL) \
                        .option("dbtable", f"vietnam_dw.{table_name}") \
                        .option("user", config.POSTGRES_USER) \
                        .option("password", config.POSTGRES_PASSWORD) \
                        .option("driver", "org.postgresql.Driver") \
                        .option("batchsize", "1000") \
                        .option("numPartitions", "4") \
                        .mode(mode) \
                        .save()

                    logger.info(f"✅ Batch {batch_id} written successfully to {table_name}")

            except Exception as e:
                logger.error(f"❌ Error writing batch {batch_id} to {table_name}: {e}")
                raise

        query = df.writeStream \
            .foreachBatch(write_batch) \
            .outputMode("append") \
            .trigger(processingTime=config.BATCH_INTERVAL) \
            .option("checkpointLocation", f"{config.CHECKPOINT_LOCATION}/{table_name}") \
            .start()

        return query

    def start_sales_streaming(self) -> StreamingQuery:
        """Start sales events streaming to fact table"""
        logger.info("🚀 Starting sales streaming...")

        # Create Kafka stream
        sales_stream = self.create_kafka_stream(
            config.KAFKA_TOPICS["sales"],
            SALES_EVENT_SCHEMA
        )

        # Transform for data warehouse
        transformed_sales = self.transformer.transform_sales_for_dw(sales_stream)

        # Lookup dimension keys
        sales_with_keys = self.dimension_lookup.lookup_keys_for_sales(transformed_sales)

        # Write to fact table
        query = self.write_to_postgres(sales_with_keys, "fact_sales_vn")
        self.active_queries.append(query)

        logger.info("✅ Sales streaming started")
        return query

    def start_customer_streaming(self) -> StreamingQuery:
        """Start customer streaming to dimension table"""
        logger.info("🚀 Starting customer streaming...")

        # Create Kafka stream
        customer_stream = self.create_kafka_stream(
            config.KAFKA_TOPICS["customers"],
            CUSTOMER_SCHEMA
        )

        # Transform for data warehouse
        transformed_customers = self.transformer.transform_customers_for_dw(customer_stream)

        # Write to dimension table
        query = self.write_to_postgres(transformed_customers, "dim_customer_vn")
        self.active_queries.append(query)

        logger.info("✅ Customer streaming started")
        return query

    def start_product_streaming(self) -> StreamingQuery:
        """Start product streaming to dimension table"""
        logger.info("🚀 Starting product streaming...")

        # Create Kafka stream
        product_stream = self.create_kafka_stream(
            config.KAFKA_TOPICS["products"],
            PRODUCT_SCHEMA
        )

        # Transform for data warehouse
        transformed_products = self.transformer.transform_products_for_dw(product_stream)

        # Write to dimension table
        query = self.write_to_postgres(transformed_products, "dim_product_vn")
        self.active_queries.append(query)

        logger.info("✅ Product streaming started")
        return query

    def start_activity_aggregation(self) -> StreamingQuery:
        """Start customer activity aggregation"""
        logger.info("🚀 Starting activity aggregation...")

        # Create Kafka stream
        activity_stream = self.create_kafka_stream(
            config.KAFKA_TOPICS["activities"],
            ACTIVITY_SCHEMA
        )

        # Aggregate activities by customer and platform
        activity_agg = activity_stream \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("customer_id"),
                col("platform"),
                col("device_type")
            ) \
            .agg(
                count("*").alias("total_activities"),
                countDistinct("activity_type").alias("distinct_activities"),
                sum("duration_seconds").alias("total_duration"),
                collect_set("activity_type").alias("activity_types")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("customer_id"),
                col("platform"),
                col("device_type"),
                col("total_activities"),
                col("distinct_activities"),
                col("total_duration"),
                col("activity_types"),
                current_timestamp().alias("aggregated_at")
            )

        # Write to activity fact table
        query = self.write_to_postgres(activity_agg, "fact_customer_activity_vn")
        self.active_queries.append(query)

        logger.info("✅ Activity aggregation started")
        return query

    def start_all_streams(self):
        """Start all streaming processes"""
        logger.info("🎯 Starting all Vietnam streaming processes...")

        try:
            # Start all streams
            self.start_sales_streaming()
            self.start_customer_streaming()
            self.start_product_streaming()
            self.start_activity_aggregation()

            logger.info(f"✅ All {len(self.active_queries)} streaming processes started successfully")

            # Wait for all queries
            for query in self.active_queries:
                logger.info(f"📊 Streaming query: {query.name} - Status: {query.status}")

        except Exception as e:
            logger.error(f"❌ Error starting streams: {e}")
            self.stop_all_streams()
            raise

    def stop_all_streams(self):
        """Stop all streaming processes"""
        logger.info("🛑 Stopping all streaming processes...")

        for query in self.active_queries:
            try:
                query.stop()
                logger.info(f"✅ Stopped query: {query.name}")
            except Exception as e:
                logger.error(f"❌ Error stopping query: {e}")

        self.active_queries.clear()
        logger.info("✅ All streaming processes stopped")

    def get_streaming_progress(self) -> Dict:
        """Get progress of all streaming queries"""
        progress = {}

        for i, query in enumerate(self.active_queries):
            try:
                query_progress = query.lastProgress
                progress[f"query_{i}"] = {
                    "id": query.id,
                    "name": query.name,
                    "timestamp": query_progress.get("timestamp"),
                    "batch_id": query_progress.get("batchId"),
                    "input_rows_per_second": query_progress.get("inputRowsPerSecond"),
                    "processed_rows_per_second": query_progress.get("processedRowsPerSecond"),
                    "batch_duration": query_progress.get("batchDuration"),
                    "state_operators": query_progress.get("stateOperators", [])
                }
            except Exception as e:
                progress[f"query_{i}"] = {"error": str(e)}

        return progress

# ====================================================================
# MAIN EXECUTION
# ====================================================================

def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='Vietnam E-commerce Spark Streaming Consumer')
    parser.add_argument('--mode', choices=['all', 'sales', 'customers', 'products', 'activities'],
                       default='all', help='Streaming mode')
    parser.add_argument('--duration', type=int, default=0,
                       help='Run duration in seconds (0 = indefinite)')

    args = parser.parse_args()

    # Create Spark session
    spark = create_spark_session()

    try:
        # Initialize processor
        processor = VietnamStreamingProcessor(spark)

        # Start streams based on mode
        if args.mode == 'all':
            processor.start_all_streams()
        elif args.mode == 'sales':
            processor.start_sales_streaming()
        elif args.mode == 'customers':
            processor.start_customer_streaming()
        elif args.mode == 'products':
            processor.start_product_streaming()
        elif args.mode == 'activities':
            processor.start_activity_aggregation()

        logger.info(f"🎯 Vietnam streaming started in {args.mode} mode")

        if args.duration > 0:
            logger.info(f"⏱️ Running for {args.duration} seconds...")
            import time
            time.sleep(args.duration)
        else:
            logger.info("⏱️ Running indefinitely (Ctrl+C to stop)...")

            # Monitor progress
            import time
            while True:
                time.sleep(30)
                progress = processor.get_streaming_progress()
                logger.info(f"📊 Streaming progress: {len(progress)} active queries")

                for query_name, query_progress in progress.items():
                    if "error" not in query_progress:
                        logger.info(f"  {query_name}: {query_progress.get('input_rows_per_second', 0):.2f} rows/sec")

    except KeyboardInterrupt:
        logger.info("⌨️ Interrupted by user")

    except Exception as e:
        logger.error(f"❌ Error: {e}")

    finally:
        # Stop all streams
        try:
            processor.stop_all_streams()
        except:
            pass

        # Stop Spark
        spark.stop()
        logger.info("🏁 Vietnam Spark Streaming Consumer stopped")

if __name__ == "__main__":
    main()