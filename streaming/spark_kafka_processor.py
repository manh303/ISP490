#!/usr/bin/env python3
"""
Spark Kafka Streaming Processor
Xử lý dữ liệu từ Kafka → Spark → Datawarehouse
Luồng: Kafka Topics → Spark Streaming → PostgreSQL Datawarehouse
"""

import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SparkKafkaProcessor:
    def __init__(self):
        # Spark configuration
        self.spark_config = {
            'app_name': 'EcommerceSparkKafkaProcessor',
            'kafka_bootstrap_servers': 'kafka:9092',
            'checkpoint_location': '/tmp/spark-checkpoint',
            'batch_duration': '10 seconds'
        }

        # PostgreSQL configuration
        self.postgres_config = {
            'host': 'postgres',
            'port': 5432,
            'database': 'ecommerce_dss',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        # Kafka topics to process
        self.kafka_topics = [
            'ecommerce.orders.stream',
            'ecommerce.customers.stream',
            'ecommerce.products.stream',
            'ecommerce.crawl.stream',
            'ecommerce.raw.stream'
        ]

        self.spark = None
        self.postgres_conn = None

    def init_spark_session(self):
        """Initialize Spark session với Kafka support"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.spark_config['app_name']) \
                .config("spark.jars.packages",
                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                       "org.postgresql:postgresql:42.3.1") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()

            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark session initialized successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark session: {e}")
            return False

    def connect_postgres(self):
        """Connect to PostgreSQL"""
        try:
            self.postgres_conn = psycopg2.connect(**self.postgres_config)
            self.postgres_conn.autocommit = True
            logger.info("✅ Connected to PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            return False

    def create_kafka_stream(self, topics):
        """Create Kafka streaming DataFrame"""
        try:
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.spark_config['kafka_bootstrap_servers']) \
                .option("subscribe", ",".join(topics)) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()

            logger.info(f"✅ Kafka stream created for topics: {topics}")
            return kafka_df

        except Exception as e:
            logger.error(f"❌ Failed to create Kafka stream: {e}")
            return None

    def parse_kafka_message(self, kafka_df):
        """Parse Kafka message and extract data"""
        try:
            # Define schema for Kafka message
            kafka_schema = StructType([
                StructField("data", StructType([
                    StructField("order_id", StringType(), True),
                    StructField("customer_id", StringType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("product_name", StringType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("total_amount", DoubleType(), True),
                    StructField("order_date", StringType(), True),
                    StructField("payment_method", StringType(), True),
                    StructField("platform", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("shipping_address", StructType([
                        StructField("city", StringType(), True),
                        StructField("district", StringType(), True),
                        StructField("street", StringType(), True)
                    ]), True),
                    StructField("customer_info", StructType([
                        StructField("age_group", StringType(), True),
                        StructField("gender", StringType(), True),
                        StructField("membership", StringType(), True)
                    ]), True),
                    StructField("source", StringType(), True),
                    StructField("data_type", StringType(), True)
                ]), True),
                StructField("metadata", StructType([
                    StructField("produced_at", StringType(), True),
                    StructField("producer_id", StringType(), True),
                    StructField("topic", StringType(), True),
                    StructField("message_id", StringType(), True)
                ]), True)
            ])

            # Parse JSON message
            parsed_df = kafka_df.select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp"),
                from_json(col("value").cast("string"), kafka_schema).alias("parsed_data")
            ).select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp"),
                col("parsed_data.data.*"),
                col("parsed_data.metadata.*")
            )

            # Add processing timestamp
            enriched_df = parsed_df.withColumn("processed_at", current_timestamp())

            logger.info("✅ Kafka messages parsed successfully")
            return enriched_df

        except Exception as e:
            logger.error(f"❌ Failed to parse Kafka messages: {e}")
            return None

    def write_to_postgres_bronze(self, df, epoch_id):
        """Write to PostgreSQL Bronze layer"""
        try:
            # Collect data from Spark DataFrame
            records = df.collect()

            if not records:
                logger.debug(f"Epoch {epoch_id}: No records to process")
                return

            cursor = self.postgres_conn.cursor()

            # Prepare data for Bronze layer
            bronze_data = []
            for row in records:
                try:
                    # Create JSON for original_data
                    original_data = {
                        'order_id': row['order_id'],
                        'customer_id': row['customer_id'],
                        'product_id': row['product_id'],
                        'product_name': row['product_name'],
                        'quantity': row['quantity'],
                        'total_amount': row['total_amount'],
                        'order_date': row['order_date'],
                        'payment_method': row['payment_method'],
                        'platform': row['platform'],
                        'status': row['status'],
                        'shipping_address': {
                            'city': getattr(row, 'city', None),
                            'district': getattr(row, 'district', None),
                            'street': getattr(row, 'street', None)
                        } if hasattr(row, 'city') else {},
                        'customer_info': {
                            'age_group': getattr(row, 'age_group', None),
                            'gender': getattr(row, 'gender', None),
                            'membership': getattr(row, 'membership', None)
                        } if hasattr(row, 'age_group') else {},
                        'source': row['source'],
                        'data_type': row['data_type']
                    }

                    bronze_record = (
                        row['order_id'],
                        row['customer_id'],
                        row['product_id'],
                        row['topic'],
                        row['partition'],
                        row['offset'],
                        json.dumps(original_data),
                        datetime.now(),
                        'spark_kafka_processor'
                    )
                    bronze_data.append(bronze_record)

                except Exception as e:
                    logger.warning(f"Failed to process record in epoch {epoch_id}: {e}")
                    continue

            # Batch insert to Bronze layer
            if bronze_data:
                execute_values(
                    cursor,
                    """
                    INSERT INTO bronze.orders_raw
                    (order_id, customer_id, product_id, kafka_topic, kafka_partition,
                     kafka_offset, original_data, processed_at, source)
                    VALUES %s
                    ON CONFLICT (order_id, kafka_offset) DO UPDATE SET
                        original_data = EXCLUDED.original_data,
                        processed_at = EXCLUDED.processed_at
                    """,
                    bronze_data
                )

                logger.info(f"Epoch {epoch_id}: Inserted {len(bronze_data)} records to Bronze layer")

            cursor.close()

        except Exception as e:
            logger.error(f"Failed to write to PostgreSQL Bronze in epoch {epoch_id}: {e}")

    def process_to_silver_layer(self, df, epoch_id):
        """Process Bronze to Silver layer transformation"""
        try:
            cursor = self.postgres_conn.cursor()

            # Transform Bronze to Silver
            cursor.execute("""
                INSERT INTO silver.orders_clean
                (order_id, customer_id, product_id, product_name, quantity, total_amount,
                 order_date, payment_method, platform, status, city, district,
                 customer_age_group, customer_gender, customer_membership, processed_at, source)
                SELECT DISTINCT
                    (original_data->>'order_id')::VARCHAR(100) as order_id,
                    (original_data->>'customer_id')::VARCHAR(100) as customer_id,
                    (original_data->>'product_id')::VARCHAR(100) as product_id,
                    (original_data->>'product_name')::VARCHAR(200) as product_name,
                    COALESCE((original_data->>'quantity')::INTEGER, 1) as quantity,
                    COALESCE((original_data->>'total_amount')::DECIMAL, 0) as total_amount,
                    CASE
                        WHEN original_data->>'order_date' IS NOT NULL
                        THEN (original_data->>'order_date')::TIMESTAMP
                        ELSE NOW()
                    END as order_date,
                    COALESCE((original_data->>'payment_method')::VARCHAR(50), 'unknown') as payment_method,
                    COALESCE((original_data->>'platform')::VARCHAR(50), 'web') as platform,
                    COALESCE((original_data->>'status')::VARCHAR(50), 'pending') as status,
                    COALESCE((original_data->'shipping_address'->>'city')::VARCHAR(100), 'Unknown') as city,
                    COALESCE((original_data->'shipping_address'->>'district')::VARCHAR(100), 'Unknown') as district,
                    (original_data->'customer_info'->>'age_group')::VARCHAR(20) as customer_age_group,
                    (original_data->'customer_info'->>'gender')::VARCHAR(10) as customer_gender,
                    (original_data->'customer_info'->>'membership')::VARCHAR(20) as customer_membership,
                    NOW() as processed_at,
                    'spark_streaming_processor' as source
                FROM bronze.orders_raw
                WHERE original_data->>'order_id' IS NOT NULL
                  AND inserted_at >= NOW() - INTERVAL '1 minute'
                  AND (original_data->>'total_amount')::DECIMAL > 0
                ON CONFLICT (order_id) DO UPDATE SET
                    total_amount = EXCLUDED.total_amount,
                    status = EXCLUDED.status,
                    product_name = EXCLUDED.product_name,
                    customer_age_group = EXCLUDED.customer_age_group,
                    customer_gender = EXCLUDED.customer_gender,
                    customer_membership = EXCLUDED.customer_membership,
                    updated_at = NOW();
            """)

            silver_count = cursor.rowcount
            if silver_count > 0:
                logger.info(f"Epoch {epoch_id}: Processed {silver_count} records to Silver layer")

            cursor.close()

        except Exception as e:
            logger.error(f"Failed to process Silver layer in epoch {epoch_id}: {e}")

    def process_to_gold_layer(self, df, epoch_id):
        """Process Silver to Gold layer aggregations"""
        try:
            cursor = self.postgres_conn.cursor()

            # Update Gold daily summary
            cursor.execute("""
                INSERT INTO gold.daily_sales_summary
                (date_key, total_orders, total_revenue, avg_order_value, unique_customers, unique_products,
                 top_payment_method, top_platform, top_city, updated_at)
                SELECT
                    DATE(order_date) as date_key,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as total_revenue,
                    AVG(total_amount) as avg_order_value,
                    COUNT(DISTINCT customer_id) as unique_customers,
                    COUNT(DISTINCT product_id) as unique_products,
                    MODE() WITHIN GROUP (ORDER BY payment_method) as top_payment_method,
                    MODE() WITHIN GROUP (ORDER BY platform) as top_platform,
                    MODE() WITHIN GROUP (ORDER BY city) as top_city,
                    NOW() as updated_at
                FROM silver.orders_clean
                WHERE processed_at >= NOW() - INTERVAL '2 minutes'
                  AND total_amount > 0
                GROUP BY DATE(order_date)
                ON CONFLICT (date_key) DO UPDATE SET
                    total_orders = EXCLUDED.total_orders,
                    total_revenue = EXCLUDED.total_revenue,
                    avg_order_value = EXCLUDED.avg_order_value,
                    unique_customers = EXCLUDED.unique_customers,
                    unique_products = EXCLUDED.unique_products,
                    top_payment_method = EXCLUDED.top_payment_method,
                    top_platform = EXCLUDED.top_platform,
                    top_city = EXCLUDED.top_city,
                    updated_at = NOW();
            """)

            # Update Gold customer summary
            cursor.execute("""
                INSERT INTO gold.customer_summary
                (customer_id, total_orders, total_spent, avg_order_value,
                 first_order_date, last_order_date, favorite_payment_method,
                 favorite_platform, primary_city, customer_age_group, customer_gender,
                 customer_membership, customer_segment, days_since_last_order, updated_at)
                SELECT
                    customer_id,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as total_spent,
                    AVG(total_amount) as avg_order_value,
                    MIN(DATE(order_date)) as first_order_date,
                    MAX(DATE(order_date)) as last_order_date,
                    MODE() WITHIN GROUP (ORDER BY payment_method) as favorite_payment_method,
                    MODE() WITHIN GROUP (ORDER BY platform) as favorite_platform,
                    MODE() WITHIN GROUP (ORDER BY city) as primary_city,
                    MODE() WITHIN GROUP (ORDER BY customer_age_group) as customer_age_group,
                    MODE() WITHIN GROUP (ORDER BY customer_gender) as customer_gender,
                    MODE() WITHIN GROUP (ORDER BY customer_membership) as customer_membership,
                    CASE
                        WHEN SUM(total_amount) > 50000000 THEN 'VIP'
                        WHEN SUM(total_amount) > 10000000 THEN 'Premium'
                        WHEN SUM(total_amount) > 1000000 THEN 'Standard'
                        ELSE 'Basic'
                    END as customer_segment,
                    CURRENT_DATE - MAX(DATE(order_date)) as days_since_last_order,
                    NOW() as updated_at
                FROM silver.orders_clean
                WHERE total_amount > 0
                GROUP BY customer_id
                ON CONFLICT (customer_id) DO UPDATE SET
                    total_orders = EXCLUDED.total_orders,
                    total_spent = EXCLUDED.total_spent,
                    avg_order_value = EXCLUDED.avg_order_value,
                    last_order_date = EXCLUDED.last_order_date,
                    favorite_payment_method = EXCLUDED.favorite_payment_method,
                    favorite_platform = EXCLUDED.favorite_platform,
                    primary_city = EXCLUDED.primary_city,
                    customer_age_group = EXCLUDED.customer_age_group,
                    customer_gender = EXCLUDED.customer_gender,
                    customer_membership = EXCLUDED.customer_membership,
                    customer_segment = EXCLUDED.customer_segment,
                    days_since_last_order = EXCLUDED.days_since_last_order,
                    updated_at = NOW();
            """)

            cursor.close()
            logger.info(f"Epoch {epoch_id}: Updated Gold layer aggregations")

        except Exception as e:
            logger.error(f"Failed to process Gold layer in epoch {epoch_id}: {e}")

    def process_streaming_batch(self, df, epoch_id):
        """Process each streaming batch"""
        logger.info(f"Processing batch epoch {epoch_id}")

        try:
            # Write to Bronze layer
            self.write_to_postgres_bronze(df, epoch_id)

            # Process to Silver layer
            self.process_to_silver_layer(df, epoch_id)

            # Process to Gold layer
            self.process_to_gold_layer(df, epoch_id)

            logger.info(f"✅ Successfully processed epoch {epoch_id}")

        except Exception as e:
            logger.error(f"❌ Failed to process epoch {epoch_id}: {e}")

    def run_spark_streaming(self):
        """Run Spark streaming process"""
        logger.info("🚀 Starting Spark Kafka Streaming Processor")
        logger.info("=" * 60)

        try:
            # Initialize components
            if not self.init_spark_session():
                return False

            if not self.connect_postgres():
                return False

            # Create Kafka stream
            kafka_df = self.create_kafka_stream(self.kafka_topics)
            if kafka_df is None:
                return False

            # Parse Kafka messages
            parsed_df = self.parse_kafka_message(kafka_df)
            if parsed_df is None:
                return False

            # Start streaming query
            logger.info("🔄 Starting streaming query...")
            query = parsed_df.writeStream \
                .foreachBatch(self.process_streaming_batch) \
                .option("checkpointLocation", self.spark_config['checkpoint_location']) \
                .trigger(processingTime=self.spark_config['batch_duration']) \
                .start()

            logger.info("✅ Spark streaming started successfully")
            logger.info("🔄 Processing Kafka streams → Spark → Datawarehouse...")

            # Wait for termination
            query.awaitTermination()

        except Exception as e:
            logger.error(f"❌ Spark streaming failed: {e}")
            return False

        finally:
            if self.spark:
                self.spark.stop()
            if self.postgres_conn:
                self.postgres_conn.close()

def main():
    """Main function"""
    processor = SparkKafkaProcessor()
    processor.run_spark_streaming()

if __name__ == "__main__":
    main()