#!/usr/bin/env python3
"""
Complete Kafka to Datawarehouse Processor
Processes streaming data from Kafka through Medallion Architecture:
Bronze (Raw) → Silver (Clean) → Gold (Aggregated) → DW_Core (Star Schema)
"""
import json
import time
import logging
from datetime import datetime, date
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import uuid
from decimal import Decimal

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CompleteDatawarehouseProcessor:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'group_id': 'datawarehouse_processor',
            'auto_offset_reset': 'earliest',  # Process from beginning to get all data
            'enable_auto_commit': True,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None,
            'consumer_timeout_ms': 30000  # 30 second timeout
        }

        self.postgres_config = {
            'host': 'localhost',
            'port': 5433,  # New Docker PostgreSQL port
            'database': 'ecommerce_dss',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        self.batch_size = 100  # Larger batch size for efficiency
        self.consumer = None
        self.db_conn = None

        # Statistics tracking
        self.stats = {
            'total_processed': 0,
            'bronze_inserts': 0,
            'silver_inserts': 0,
            'gold_updates': 0,
            'dw_core_inserts': 0,
            'start_time': None
        }

    def init_kafka_consumer(self):
        """Initialize Kafka consumer for all streaming topics"""
        try:
            self.consumer = KafkaConsumer(
                'ecommerce.orders.stream',
                'ecommerce.customers.stream',
                'ecommerce.products.stream',
                'vietnam_sales_events',
                'vietnam_customers',
                'vietnam_products',
                'vietnam_user_activities',
                **self.kafka_config
            )
            logger.info("Kafka consumer initialized for all streaming topics")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            return False

    def init_postgres_connection(self):
        """Initialize PostgreSQL connection"""
        try:
            self.db_conn = psycopg2.connect(**self.postgres_config)
            self.db_conn.autocommit = True
            logger.info("PostgreSQL connection established")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False

    def create_staging_tables(self):
        """Create staging tables for streaming data"""
        try:
            cursor = self.db_conn.cursor()

            # Create staging schema if not exists
            cursor.execute("CREATE SCHEMA IF NOT EXISTS streaming_data;")

            # Create streaming orders staging table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS streaming_data.orders_stream (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(50),
                    customer_id VARCHAR(50),
                    product_id VARCHAR(50),
                    quantity INTEGER,
                    total_amount DECIMAL(10,2),
                    order_date TIMESTAMP,
                    platform VARCHAR(50),
                    payment_method VARCHAR(50),
                    status VARCHAR(50),
                    city VARCHAR(100),
                    district VARCHAR(100),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_topic VARCHAR(100)
                );
            """)

            # Create streaming customers staging table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS streaming_data.customers_stream (
                    id SERIAL PRIMARY KEY,
                    customer_id VARCHAR(50),
                    customer_data JSONB,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_topic VARCHAR(100)
                );
            """)

            # Create streaming products staging table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS streaming_data.products_stream (
                    id SERIAL PRIMARY KEY,
                    product_id VARCHAR(50),
                    product_data JSONB,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_topic VARCHAR(100)
                );
            """)

            cursor.close()
            logger.info("Staging tables created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create staging tables: {e}")
            return False

    def process_order_message(self, message, topic):
        """Process an order message and transform for datawarehouse"""
        try:
            message_data = message.value
            if not message_data:
                return None

            # Handle both direct data and wrapped data from big data generator
            if 'data' in message_data and 'metadata' in message_data:
                # Big data generator format
                data = message_data['data']
            else:
                # Direct data format
                data = message_data

            # Extract shipping address
            shipping_addr = data.get('shipping_address', {})
            if shipping_addr:
                city = shipping_addr.get('city', '')
                district = shipping_addr.get('district', '')
            else:
                # For big data generator, city might be directly in data
                city = data.get('city', '')
                district = data.get('district', '')

            # Transform order data
            order_record = {
                'order_id': data.get('order_id'),
                'customer_id': data.get('customer_id'),
                'product_id': data.get('product_id'),
                'quantity': data.get('quantity', 1),
                'total_amount': data.get('total_amount', 0),
                'order_date': data.get('order_date'),
                'platform': data.get('platform', 'Unknown'),
                'payment_method': data.get('payment_method', 'Unknown'),
                'status': data.get('status', 'Unknown'),
                'city': city,
                'district': district,
                'source_topic': topic
            }

            return order_record
        except Exception as e:
            logger.error(f"Error processing order message: {e}")
            return None

    def insert_streaming_data(self, orders_batch, customers_batch, products_batch):
        """Insert batched streaming data into PostgreSQL"""
        try:
            cursor = self.db_conn.cursor()

            # Insert orders
            if orders_batch:
                order_query = """
                    INSERT INTO streaming_data.orders_stream
                    (order_id, customer_id, product_id, quantity, total_amount,
                     order_date, platform, payment_method, status, city, district, source_topic)
                    VALUES %s
                """
                order_values = [
                    (o['order_id'], o['customer_id'], o['product_id'], o['quantity'],
                     o['total_amount'], o['order_date'], o['platform'], o['payment_method'],
                     o['status'], o['city'], o['district'], o['source_topic'])
                    for o in orders_batch
                ]
                execute_values(cursor, order_query, order_values)
                logger.info(f"Inserted {len(orders_batch)} streaming orders")

            # Insert customers
            if customers_batch:
                customer_query = """
                    INSERT INTO streaming_data.customers_stream (customer_id, customer_data, source_topic)
                    VALUES %s
                """
                customer_values = [
                    (c['customer_id'], json.dumps(c['data']), c['source_topic'])
                    for c in customers_batch
                ]
                execute_values(cursor, customer_query, customer_values)
                logger.info(f"Inserted {len(customers_batch)} streaming customers")

            # Insert products
            if products_batch:
                product_query = """
                    INSERT INTO streaming_data.products_stream (product_id, product_data, source_topic)
                    VALUES %s
                """
                product_values = [
                    (p['product_id'], json.dumps(p['data']), p['source_topic'])
                    for p in products_batch
                ]
                execute_values(cursor, product_query, product_values)
                logger.info(f"Inserted {len(products_batch)} streaming products")

            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Failed to insert streaming data: {e}")
            return False

    def process_to_bronze_layer(self, orders_batch, customers_batch):
        """Process streaming data to Bronze layer (raw data)"""
        try:
            cursor = self.db_conn.cursor()
            bronze_count = 0

            # Insert orders to Bronze layer
            if orders_batch:
                bronze_query = """
                    INSERT INTO bronze.orders_raw (
                        order_id, original_data, kafka_metadata, source, processed_at
                    ) VALUES %s
                    ON CONFLICT (order_id) DO NOTHING;
                """
                bronze_values = []
                for order in orders_batch:
                    original_data = json.dumps(order)
                    kafka_metadata = json.dumps({
                        'topic': order.get('source_topic', 'unknown'),
                        'processed_timestamp': datetime.now().isoformat()
                    })
                    bronze_values.append((
                        order['order_id'],
                        original_data,
                        kafka_metadata,
                        order.get('source_topic', 'streaming'),
                        datetime.now()
                    ))

                execute_values(cursor, bronze_query, bronze_values)
                bronze_count += len(bronze_values)
                self.stats['bronze_inserts'] += len(bronze_values)
                logger.info(f"✅ Bronze: Inserted {len(bronze_values)} raw orders")

            cursor.close()
            return bronze_count > 0
        except Exception as e:
            logger.error(f"❌ Bronze layer processing failed: {e}")
            return False

    def process_to_silver_layer(self):
        """Process Bronze data to Silver layer (cleaned data)"""
        try:
            cursor = self.db_conn.cursor()

            # Clean and transform data from Bronze to Silver
            cursor.execute("""
                INSERT INTO silver.orders_clean (
                    order_id, customer_id, product_name, total_amount,
                    payment_method, city, order_date, data_quality_score, processed_at
                )
                SELECT
                    (original_data->>'order_id')::VARCHAR(100) as order_id,
                    (original_data->>'customer_id')::VARCHAR(100) as customer_id,
                    COALESCE((original_data->>'product_name')::VARCHAR(500), 'Unknown Product') as product_name,
                    COALESCE((original_data->>'total_amount')::NUMERIC, 0) as total_amount,
                    LOWER(TRIM(COALESCE((original_data->>'payment_method')::VARCHAR(50), 'unknown'))) as payment_method,
                    INITCAP(TRIM(COALESCE((original_data->>'city')::VARCHAR(100), 'Unknown'))) as city,
                    COALESCE((original_data->>'order_date')::TIMESTAMP, processed_at) as order_date,
                    -- Calculate data quality score
                    (
                        CASE WHEN (original_data->>'order_id') IS NOT NULL THEN 0.2 ELSE 0 END +
                        CASE WHEN (original_data->>'customer_id') IS NOT NULL THEN 0.2 ELSE 0 END +
                        CASE WHEN (original_data->>'total_amount')::NUMERIC > 0 THEN 0.2 ELSE 0 END +
                        CASE WHEN (original_data->>'payment_method') IS NOT NULL THEN 0.2 ELSE 0 END +
                        CASE WHEN (original_data->>'city') IS NOT NULL THEN 0.2 ELSE 0 END
                    ) as data_quality_score,
                    NOW() as processed_at
                FROM bronze.orders_raw
                WHERE (original_data->>'order_id') NOT IN (
                    SELECT order_id FROM silver.orders_clean WHERE order_id IS NOT NULL
                )
                AND processed_at >= NOW() - INTERVAL '1 hour'
                ON CONFLICT (order_id) DO NOTHING;
            """)

            silver_count = cursor.rowcount
            self.stats['silver_inserts'] += silver_count
            cursor.close()

            if silver_count > 0:
                logger.info(f"✅ Silver: Cleaned {silver_count} orders")
            return silver_count > 0
        except Exception as e:
            logger.error(f"❌ Silver layer processing failed: {e}")
            return False

    def process_to_gold_layer(self):
        """Process Silver data to Gold layer (business aggregates)"""
        try:
            cursor = self.db_conn.cursor()
            gold_updates = 0

            # Daily sales summary
            cursor.execute("""
                INSERT INTO gold.daily_sales_summary (
                    date_key, total_orders, total_revenue, avg_order_value,
                    top_city, top_payment_method, processed_at
                )
                SELECT
                    DATE(order_date) as date_key,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as total_revenue,
                    AVG(total_amount) as avg_order_value,
                    (
                        SELECT city
                        FROM silver.orders_clean s2
                        WHERE DATE(s2.order_date) = DATE(s1.order_date)
                        GROUP BY city
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                    ) as top_city,
                    (
                        SELECT payment_method
                        FROM silver.orders_clean s3
                        WHERE DATE(s3.order_date) = DATE(s1.order_date)
                        GROUP BY payment_method
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                    ) as top_payment_method,
                    NOW() as processed_at
                FROM silver.orders_clean s1
                WHERE DATE(order_date) >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY DATE(order_date)
                ON CONFLICT (date_key) DO UPDATE SET
                    total_orders = EXCLUDED.total_orders,
                    total_revenue = EXCLUDED.total_revenue,
                    avg_order_value = EXCLUDED.avg_order_value,
                    top_city = EXCLUDED.top_city,
                    top_payment_method = EXCLUDED.top_payment_method,
                    processed_at = EXCLUDED.processed_at;
            """)

            daily_updates = cursor.rowcount
            gold_updates += daily_updates

            # Customer summary
            cursor.execute("""
                INSERT INTO gold.customer_summary (
                    customer_id, total_orders, total_spent, avg_order_value,
                    first_order_date, last_order_date, favorite_payment_method,
                    customer_segment, processed_at
                )
                SELECT
                    customer_id,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as total_spent,
                    AVG(total_amount) as avg_order_value,
                    MIN(order_date) as first_order_date,
                    MAX(order_date) as last_order_date,
                    (
                        SELECT payment_method
                        FROM silver.orders_clean s2
                        WHERE s2.customer_id = s1.customer_id
                        GROUP BY payment_method
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                    ) as favorite_payment_method,
                    CASE
                        WHEN SUM(total_amount) >= 10000000 THEN 'Premium'
                        WHEN SUM(total_amount) >= 5000000 THEN 'Standard'
                        ELSE 'Basic'
                    END as customer_segment,
                    NOW() as processed_at
                FROM silver.orders_clean s1
                GROUP BY customer_id
                ON CONFLICT (customer_id) DO UPDATE SET
                    total_orders = EXCLUDED.total_orders,
                    total_spent = EXCLUDED.total_spent,
                    avg_order_value = EXCLUDED.avg_order_value,
                    last_order_date = EXCLUDED.last_order_date,
                    favorite_payment_method = EXCLUDED.favorite_payment_method,
                    customer_segment = EXCLUDED.customer_segment,
                    processed_at = EXCLUDED.processed_at;
            """)

            customer_updates = cursor.rowcount
            gold_updates += customer_updates

            self.stats['gold_updates'] += gold_updates
            cursor.close()

            if gold_updates > 0:
                logger.info(f"✅ Gold: Updated {daily_updates} daily summaries, {customer_updates} customer summaries")
            return gold_updates > 0
        except Exception as e:
            logger.error(f"❌ Gold layer processing failed: {e}")
            return False

    def process_to_dw_core(self):
        """Process data to DW_Core (Star Schema)"""
        try:
            cursor = self.db_conn.cursor()
            dw_inserts = 0

            # Insert into fact_orders
            cursor.execute("""
                INSERT INTO dw_core.fact_orders (
                    order_key, customer_key, product_key, time_key,
                    quantity, unit_price, total_amount, discount_amount
                )
                SELECT
                    s.order_id as order_key,
                    s.customer_id as customer_key,
                    COALESCE(s.product_name, 'Unknown') as product_key,
                    TO_CHAR(s.order_date, 'YYYYMMDD')::INTEGER as time_key,
                    1 as quantity,
                    s.total_amount as unit_price,
                    s.total_amount as total_amount,
                    0 as discount_amount
                FROM silver.orders_clean s
                WHERE s.order_id NOT IN (SELECT order_key FROM dw_core.fact_orders)
                AND s.data_quality_score >= 0.6
                ON CONFLICT (order_key) DO NOTHING;
            """)

            fact_inserts = cursor.rowcount
            dw_inserts += fact_inserts

            # Update dim_customers
            cursor.execute("""
                INSERT INTO dw_core.dim_customers (
                    customer_key, customer_id, customer_segment,
                    total_lifetime_value, registration_date
                )
                SELECT DISTINCT
                    s.customer_id as customer_key,
                    s.customer_id as customer_id,
                    g.customer_segment,
                    g.total_spent as total_lifetime_value,
                    g.first_order_date as registration_date
                FROM silver.orders_clean s
                LEFT JOIN gold.customer_summary g ON s.customer_id = g.customer_id
                WHERE s.customer_id NOT IN (SELECT customer_key FROM dw_core.dim_customers)
                ON CONFLICT (customer_key) DO UPDATE SET
                    customer_segment = EXCLUDED.customer_segment,
                    total_lifetime_value = EXCLUDED.total_lifetime_value;
            """)

            customer_inserts = cursor.rowcount
            dw_inserts += customer_inserts

            self.stats['dw_core_inserts'] += dw_inserts
            cursor.close()

            if dw_inserts > 0:
                logger.info(f"✅ DW_Core: Inserted {fact_inserts} facts, updated {customer_inserts} customers")
            return dw_inserts > 0
        except Exception as e:
            logger.error(f"❌ DW_Core processing failed: {e}")
            return False

    def run_streaming_processor(self):
        """Main streaming processor loop"""
        logger.info("Starting Kafka to Datawarehouse streaming processor")

        if not self.init_kafka_consumer():
            return False

        if not self.init_postgres_connection():
            return False

        if not self.create_staging_tables():
            return False

        orders_batch = []
        customers_batch = []
        products_batch = []

        try:
            logger.info("Starting to consume messages from Kafka...")
            for message in self.consumer:
                topic = message.topic
                logger.info(f"Processing message from topic: {topic}")

                if 'orders' in topic or 'sales_events' in topic:
                    order_record = self.process_order_message(message, topic)
                    if order_record:
                        orders_batch.append(order_record)

                elif 'customers' in topic:
                    if message.value:
                        customers_batch.append({
                            'customer_id': message.value.get('customer_id', f'STREAM_{int(time.time())}'),
                            'data': message.value,
                            'source_topic': topic
                        })

                elif 'products' in topic:
                    if message.value:
                        products_batch.append({
                            'product_id': message.value.get('product_id', f'STREAM_{int(time.time())}'),
                            'data': message.value,
                            'source_topic': topic
                        })

                # Process batch when it reaches batch_size
                if (len(orders_batch) + len(customers_batch) + len(products_batch)) >= self.batch_size:
                    self.process_complete_datawarehouse_pipeline(orders_batch, customers_batch, products_batch)

                    # Clear batches
                    orders_batch = []
                    customers_batch = []
                    products_batch = []

                    logger.info("Batch processed through complete datawarehouse pipeline")

        except KeyboardInterrupt:
            logger.info("Streaming processor stopped by user")
        except Exception as e:
            logger.error(f"Error in streaming processor: {e}")
        finally:
            # Process remaining batch
            if orders_batch or customers_batch or products_batch:
                self.process_complete_datawarehouse_pipeline(orders_batch, customers_batch, products_batch)

            if self.consumer:
                self.consumer.close()
            if self.db_conn:
                self.db_conn.close()
            logger.info("Streaming processor shutdown complete")

    def process_complete_datawarehouse_pipeline(self, orders_batch, customers_batch, products_batch):
        """Process data through complete datawarehouse pipeline: Bronze → Silver → Gold → DW_Core"""
        try:
            # Step 1: Insert to staging (for debugging)
            self.insert_streaming_data(orders_batch, customers_batch, products_batch)

            # Step 2: Process to Bronze layer (raw data with lineage)
            if self.process_to_bronze_layer(orders_batch, customers_batch):

                # Step 3: Process to Silver layer (cleaned data)
                if self.process_to_silver_layer():

                    # Step 4: Process to Gold layer (business aggregates)
                    self.process_to_gold_layer()

                    # Step 5: Process to DW_Core (star schema)
                    self.process_to_dw_core()

            self.stats['total_processed'] += len(orders_batch) + len(customers_batch) + len(products_batch)

        except Exception as e:
            logger.error(f"❌ Complete pipeline processing failed: {e}")

    def show_processing_stats(self):
        """Show processing statistics"""
        if self.stats['start_time']:
            elapsed = time.time() - self.stats['start_time']
            logger.info("\n" + "=" * 60)
            logger.info("📊 DATAWAREHOUSE PROCESSING STATISTICS")
            logger.info("=" * 60)
            logger.info(f"⏱️  Processing Time: {elapsed:.2f} seconds")
            logger.info(f"📨 Total Processed: {self.stats['total_processed']:,} messages")
            logger.info(f"🔵 Bronze Inserts: {self.stats['bronze_inserts']:,}")
            logger.info(f"🥈 Silver Inserts: {self.stats['silver_inserts']:,}")
            logger.info(f"🥇 Gold Updates: {self.stats['gold_updates']:,}")
            logger.info(f"⭐ DW_Core Inserts: {self.stats['dw_core_inserts']:,}")

            if elapsed > 0:
                throughput = self.stats['total_processed'] / elapsed
                logger.info(f"🚀 Throughput: {throughput:.0f} messages/second")

            logger.info("\n🎯 MEDALLION ARCHITECTURE PIPELINE COMPLETED!")
            logger.info("✅ Bronze → Silver → Gold → DW_Core processing successful")

if __name__ == "__main__":
    processor = CompleteDatawarehouseProcessor()
    processor.stats['start_time'] = time.time()

    try:
        processor.run_streaming_processor()
    finally:
        processor.show_processing_stats()