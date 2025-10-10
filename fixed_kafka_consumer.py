#!/usr/bin/env python3
"""
Fixed Kafka to PostgreSQL Consumer
==================================
Run inside Docker network to stream Kafka data to PostgreSQL
"""

import json
import logging
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import time
import signal
import sys

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FixedKafkaConsumer:
    """Fixed Kafka consumer working in Docker network"""

    def __init__(self):
        # PostgreSQL connection (container to container)
        self.pg_conn = psycopg2.connect(
            host="postgres",  # Docker service name
            port=5432,
            database="ecommerce_dss",
            user="dss_user",
            password="dss_password_123"
        )
        self.pg_conn.autocommit = True
        self.pg_cursor = self.pg_conn.cursor()

        # Kafka consumer (container to container)
        self.consumer = KafkaConsumer(
            'vietnam_products',
            'vietnam_sales_events',
            'ecommerce.orders.stream',
            bootstrap_servers=['kafka:29092'],  # Docker service name and internal port
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='fixed_postgres_consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000  # Timeout to check for shutdown
        )

        self.running = True
        self.create_streaming_tables()
        logger.info("✅ Fixed Kafka-PostgreSQL Consumer initialized")

    def create_streaming_tables(self):
        """Create tables for real-time streaming data"""
        try:
            # Main streaming data table
            create_streaming_table = """
            CREATE TABLE IF NOT EXISTS realtime_kafka_stream (
                id SERIAL PRIMARY KEY,
                topic VARCHAR(100),
                message_type VARCHAR(50),
                product_id VARCHAR(100),
                customer_id VARCHAR(100),
                order_id VARCHAR(100),
                event_data JSONB,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source VARCHAR(50) DEFAULT 'kafka_realtime'
            );
            """

            # Products updates table
            create_products_stream = """
            CREATE TABLE IF NOT EXISTS products_realtime_updates (
                id SERIAL PRIMARY KEY,
                product_id VARCHAR(100) UNIQUE,
                product_name VARCHAR(500),
                brand VARCHAR(100),
                category VARCHAR(100),
                price_vnd BIGINT,
                price_usd DECIMAL(10,2),
                rating DECIMAL(3,2),
                review_count INTEGER,
                stock_quantity INTEGER,
                platforms TEXT[],
                vietnam_popularity DECIMAL(5,3),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                update_count INTEGER DEFAULT 1
            );
            """

            # Sales events table
            create_sales_stream = """
            CREATE TABLE IF NOT EXISTS sales_events_realtime (
                id SERIAL PRIMARY KEY,
                event_id VARCHAR(100) UNIQUE,
                customer_id VARCHAR(100),
                product_id VARCHAR(100),
                event_type VARCHAR(50),
                event_value DECIMAL(12,2),
                platform VARCHAR(50),
                location VARCHAR(100),
                device_type VARCHAR(50),
                event_timestamp TIMESTAMP,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """

            # Orders stream table
            create_orders_stream = """
            CREATE TABLE IF NOT EXISTS orders_realtime_stream (
                id SERIAL PRIMARY KEY,
                order_id VARCHAR(100) UNIQUE,
                customer_id VARCHAR(100),
                product_id VARCHAR(100),
                quantity INTEGER,
                total_amount DECIMAL(12,2),
                order_date TIMESTAMP,
                platform VARCHAR(50),
                payment_method VARCHAR(50),
                status VARCHAR(50),
                shipping_address JSONB,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """

            self.pg_cursor.execute(create_streaming_table)
            self.pg_cursor.execute(create_products_stream)
            self.pg_cursor.execute(create_sales_stream)
            self.pg_cursor.execute(create_orders_stream)

            logger.info("✅ All streaming tables created/verified")

        except Exception as e:
            logger.error(f"❌ Failed to create tables: {e}")

    def process_vietnam_product(self, data):
        """Process Vietnam product data"""
        try:
            # Insert into products updates table
            insert_query = """
            INSERT INTO products_realtime_updates
            (product_id, product_name, brand, category, price_vnd, price_usd,
             rating, review_count, stock_quantity, platforms, vietnam_popularity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                price_vnd = EXCLUDED.price_vnd,
                price_usd = EXCLUDED.price_usd,
                rating = EXCLUDED.rating,
                review_count = EXCLUDED.review_count,
                stock_quantity = EXCLUDED.stock_quantity,
                vietnam_popularity = EXCLUDED.vietnam_popularity,
                last_updated = CURRENT_TIMESTAMP,
                update_count = products_realtime_updates.update_count + 1
            """

            platforms = data.get('available_platforms', [])
            if isinstance(platforms, list):
                platforms_array = platforms
            else:
                platforms_array = [str(platforms)]

            values = (
                data.get('product_id'),
                data.get('product_name_vn', data.get('product_name_en', 'Unknown')),
                data.get('brand'),
                data.get('category_l1'),
                data.get('price_vnd'),
                data.get('price_usd'),
                data.get('rating'),
                data.get('review_count'),
                data.get('stock_quantity'),
                platforms_array,
                data.get('vietnam_popularity')
            )

            self.pg_cursor.execute(insert_query, values)
            logger.info(f"📦 Processed product: {data.get('product_id')}")

        except Exception as e:
            logger.error(f"❌ Failed to process product: {e}")

    def process_sales_event(self, data):
        """Process sales event data"""
        try:
            insert_query = """
            INSERT INTO sales_events_realtime
            (event_id, customer_id, product_id, event_type, event_value,
             platform, location, device_type, event_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """

            event_timestamp = data.get('timestamp')
            if isinstance(event_timestamp, str):
                try:
                    event_timestamp = datetime.fromisoformat(event_timestamp.replace('Z', '+00:00'))
                except:
                    event_timestamp = datetime.now()
            else:
                event_timestamp = datetime.now()

            values = (
                data.get('event_id', f"evt_{int(time.time())}_{data.get('product_id', '')[:8]}"),
                data.get('customer_id'),
                data.get('product_id'),
                data.get('event_type', 'unknown'),
                data.get('event_value', data.get('amount', 0)),
                data.get('platform', 'unknown'),
                data.get('location', 'Vietnam'),
                data.get('device_type', 'unknown'),
                event_timestamp
            )

            self.pg_cursor.execute(insert_query, values)
            logger.info(f"💰 Processed sales event: {data.get('event_type')} - {data.get('product_id')}")

        except Exception as e:
            logger.error(f"❌ Failed to process sales event: {e}")

    def process_order_stream(self, data):
        """Process order stream data"""
        try:
            insert_query = """
            INSERT INTO orders_realtime_stream
            (order_id, customer_id, product_id, quantity, total_amount,
             order_date, platform, payment_method, status, shipping_address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE SET
                status = EXCLUDED.status,
                received_at = CURRENT_TIMESTAMP
            """

            order_date = data.get('order_date')
            if isinstance(order_date, str):
                try:
                    order_date = datetime.fromisoformat(order_date.replace('Z', '+00:00'))
                except:
                    order_date = datetime.now()
            else:
                order_date = datetime.now()

            shipping_address = data.get('shipping_address', {})

            values = (
                data.get('order_id'),
                data.get('customer_id'),
                data.get('product_id'),
                data.get('quantity', 1),
                data.get('total_amount', data.get('amount', 0)),
                order_date,
                data.get('platform', 'online'),
                data.get('payment_method', 'unknown'),
                data.get('status', 'new'),
                json.dumps(shipping_address) if shipping_address else None
            )

            self.pg_cursor.execute(insert_query, values)
            logger.info(f"🛒 Processed order: {data.get('order_id')}")

        except Exception as e:
            logger.error(f"❌ Failed to process order: {e}")

    def log_all_data(self, topic, data):
        """Log all data to main streaming table"""
        try:
            insert_query = """
            INSERT INTO realtime_kafka_stream
            (topic, message_type, product_id, customer_id, order_id, event_data)
            VALUES (%s, %s, %s, %s, %s, %s)
            """

            message_type = 'unknown'
            if topic == 'vietnam_products':
                message_type = 'product_update'
            elif topic == 'vietnam_sales_events':
                message_type = 'sales_event'
            elif topic == 'ecommerce.orders.stream':
                message_type = 'order_event'

            values = (
                topic,
                message_type,
                data.get('product_id'),
                data.get('customer_id'),
                data.get('order_id'),
                json.dumps(data)
            )

            self.pg_cursor.execute(insert_query, values)

        except Exception as e:
            logger.error(f"❌ Failed to log data: {e}")

    def get_streaming_stats(self):
        """Get and log streaming statistics"""
        try:
            # Get table counts
            stats_queries = [
                "SELECT 'realtime_kafka_stream' as table_name, COUNT(*) as count FROM realtime_kafka_stream",
                "SELECT 'products_realtime_updates' as table_name, COUNT(*) as count FROM products_realtime_updates",
                "SELECT 'sales_events_realtime' as table_name, COUNT(*) as count FROM sales_events_realtime",
                "SELECT 'orders_realtime_stream' as table_name, COUNT(*) as count FROM orders_realtime_stream"
            ]

            logger.info("📊 REAL-TIME STREAMING STATISTICS:")
            total_records = 0

            for query in stats_queries:
                self.pg_cursor.execute(query)
                result = self.pg_cursor.fetchone()
                table_name, count = result
                logger.info(f"  📈 {table_name}: {count:,} records")
                total_records += count

            logger.info(f"  🎯 Total streaming records: {total_records:,}")

            # Get recent activity
            recent_query = """
            SELECT topic, COUNT(*) as count, MAX(processed_at) as latest
            FROM realtime_kafka_stream
            WHERE processed_at >= NOW() - INTERVAL '5 minutes'
            GROUP BY topic
            """

            self.pg_cursor.execute(recent_query)
            recent_results = self.pg_cursor.fetchall()

            logger.info("  ⚡ Recent activity (last 5 minutes):")
            for topic, count, latest in recent_results:
                logger.info(f"    {topic}: {count} messages (latest: {latest})")

        except Exception as e:
            logger.error(f"❌ Failed to get stats: {e}")

    def start_streaming(self):
        """Start the streaming process"""
        logger.info("🚀 Starting real-time Kafka to PostgreSQL streaming...")

        message_count = 0
        last_stats_time = time.time()

        try:
            while self.running:
                try:
                    # Poll for messages with timeout
                    message_batch = self.consumer.poll(timeout_ms=1000)

                    if not message_batch:
                        continue

                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if not self.running:
                                break

                            topic = message.topic
                            data = message.value

                            # Log all data
                            self.log_all_data(topic, data)

                            # Process by topic
                            if topic == 'vietnam_products':
                                self.process_vietnam_product(data)
                            elif topic == 'vietnam_sales_events':
                                self.process_sales_event(data)
                            elif topic == 'ecommerce.orders.stream':
                                self.process_order_stream(data)

                            message_count += 1

                            # Show stats every 25 messages or 30 seconds
                            current_time = time.time()
                            if message_count % 25 == 0 or (current_time - last_stats_time) > 30:
                                self.get_streaming_stats()
                                last_stats_time = current_time

                except Exception as e:
                    logger.error(f"❌ Error in streaming loop: {e}")
                    time.sleep(1)

        except KeyboardInterrupt:
            logger.info("⚠️ Streaming interrupted by user")
        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup resources"""
        logger.info("🧹 Cleaning up resources...")
        self.running = False
        if hasattr(self, 'consumer'):
            self.consumer.close()
        if hasattr(self, 'pg_conn'):
            self.pg_conn.close()

def signal_handler(sig, frame):
    """Handle shutdown signals"""
    logger.info("🛑 Shutdown signal received")
    sys.exit(0)

if __name__ == "__main__":
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer = FixedKafkaConsumer()
    consumer.start_streaming()