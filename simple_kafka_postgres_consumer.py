#!/usr/bin/env python3
"""
Simple Kafka to PostgreSQL Consumer
==================================
Direct streaming from Kafka to PostgreSQL with correct credentials
"""

import json
import logging
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleKafkaConsumer:
    """Simple Kafka consumer for real-time streaming"""

    def __init__(self):
        # PostgreSQL connection using correct credentials from docker-compose
        self.pg_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="ecommerce_dss",
            user="dss_user",
            password="dss_password_123"
        )
        self.pg_conn.autocommit = True
        self.pg_cursor = self.pg_conn.cursor()

        # Kafka consumer
        self.consumer = KafkaConsumer(
            'vietnam_products',
            'vietnam_sales_events',
            'ecommerce.orders.stream',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='simple_postgres_consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        self.create_streaming_tables()
        logger.info("✅ Simple Kafka-PostgreSQL Consumer initialized")

    def create_streaming_tables(self):
        """Create simple streaming tables"""
        try:
            # Create table for new streaming data
            create_streaming_data_table = """
            CREATE TABLE IF NOT EXISTS kafka_streaming_data (
                id SERIAL PRIMARY KEY,
                topic VARCHAR(100),
                data_type VARCHAR(50),
                product_id VARCHAR(100),
                event_data JSONB,
                stream_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """

            self.pg_cursor.execute(create_streaming_data_table)
            logger.info("✅ Streaming tables created/verified")

        except Exception as e:
            logger.error(f"❌ Failed to create tables: {e}")

    def process_message(self, topic, data):
        """Process incoming Kafka message"""
        try:
            # Insert into streaming table
            insert_query = """
            INSERT INTO kafka_streaming_data (topic, data_type, product_id, event_data)
            VALUES (%s, %s, %s, %s)
            """

            data_type = 'unknown'
            product_id = 'N/A'

            if topic == 'vietnam_products':
                data_type = 'product'
                product_id = data.get('product_id', 'N/A')
            elif topic == 'vietnam_sales_events':
                data_type = 'sales_event'
                product_id = data.get('product_id', 'N/A')
            elif topic == 'ecommerce.orders.stream':
                data_type = 'order'
                product_id = data.get('product_id', 'N/A')

            values = (topic, data_type, product_id, json.dumps(data))

            self.pg_cursor.execute(insert_query, values)
            logger.info(f"✅ Processed {data_type} from {topic}: {product_id}")

            # Also update existing tables if applicable
            if topic == 'vietnam_products' and data.get('product_id'):
                self.update_product_data(data)

        except Exception as e:
            logger.error(f"❌ Failed to process message from {topic}: {e}")

    def update_product_data(self, data):
        """Update vietnam_electronics_products table with new data"""
        try:
            # Check if product exists
            check_query = "SELECT COUNT(*) FROM vietnam_electronics_products WHERE product_id = %s"
            self.pg_cursor.execute(check_query, (data.get('product_id'),))
            exists = self.pg_cursor.fetchone()[0] > 0

            if not exists:
                # Insert new product
                insert_query = """
                INSERT INTO vietnam_electronics_products
                (product_id, name, brand, category, price_vnd, rating, review_count, stock_quantity, platform, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                values = (
                    data.get('product_id'),
                    data.get('product_name_vn', data.get('product_name_en', 'Unknown')),
                    data.get('brand', 'Unknown'),
                    data.get('category_l1', 'Unknown'),
                    data.get('price_vnd', 0),
                    data.get('rating', 0),
                    data.get('review_count', 0),
                    data.get('stock_quantity', 0),
                    'kafka_stream',
                    datetime.now()
                )

                self.pg_cursor.execute(insert_query, values)
                logger.info(f"📦 Added new product: {data.get('product_id')}")
            else:
                # Update existing product
                update_query = """
                UPDATE vietnam_electronics_products
                SET price_vnd = %s, rating = %s, review_count = %s,
                    stock_quantity = %s, processed_at = %s
                WHERE product_id = %s
                """

                values = (
                    data.get('price_vnd'),
                    data.get('rating'),
                    data.get('review_count'),
                    data.get('stock_quantity'),
                    datetime.now(),
                    data.get('product_id')
                )

                self.pg_cursor.execute(update_query, values)
                logger.info(f"🔄 Updated product: {data.get('product_id')}")

        except Exception as e:
            logger.error(f"❌ Failed to update product data: {e}")

    def get_streaming_stats(self):
        """Get streaming statistics"""
        try:
            # Count messages by topic
            stats_query = """
            SELECT topic, data_type, COUNT(*) as count,
                   MAX(stream_timestamp) as latest
            FROM kafka_streaming_data
            GROUP BY topic, data_type
            ORDER BY count DESC
            """

            self.pg_cursor.execute(stats_query)
            results = self.pg_cursor.fetchall()

            logger.info("📊 STREAMING STATISTICS:")
            total_messages = 0
            for topic, data_type, count, latest in results:
                logger.info(f"  {topic} ({data_type}): {count} messages (latest: {latest})")
                total_messages += count

            logger.info(f"  📈 Total messages processed: {total_messages}")

            # Check PostgreSQL record counts
            tables_query = """
            SELECT 'vietnam_electronics_products' as table_name, COUNT(*) as count
            FROM vietnam_electronics_products
            UNION ALL
            SELECT 'kafka_streaming_data' as table_name, COUNT(*) as count
            FROM kafka_streaming_data
            """

            self.pg_cursor.execute(tables_query)
            table_results = self.pg_cursor.fetchall()

            logger.info("📈 TABLE COUNTS:")
            for table_name, count in table_results:
                logger.info(f"  {table_name}: {count} records")

        except Exception as e:
            logger.error(f"❌ Failed to get stats: {e}")

    def start_consuming(self):
        """Start consuming messages from Kafka"""
        logger.info("🚀 Starting Kafka consumption...")

        message_count = 0
        last_stats_time = time.time()

        try:
            for message in self.consumer:
                topic = message.topic
                data = message.value

                # Process the message
                self.process_message(topic, data)
                message_count += 1

                # Show stats every 10 messages or 30 seconds
                current_time = time.time()
                if message_count % 10 == 0 or (current_time - last_stats_time) > 30:
                    self.get_streaming_stats()
                    last_stats_time = current_time

        except KeyboardInterrupt:
            logger.info("⚠️ Consumption interrupted by user")
        except Exception as e:
            logger.error(f"❌ Consumption error: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup resources"""
        logger.info("🧹 Cleaning up...")
        if hasattr(self, 'consumer'):
            self.consumer.close()
        if hasattr(self, 'pg_conn'):
            self.pg_conn.close()

if __name__ == "__main__":
    consumer = SimpleKafkaConsumer()
    consumer.start_consuming()