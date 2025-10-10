#!/usr/bin/env python3
"""
Real-time Kafka to PostgreSQL Streaming
=======================================
Stream data from Kafka topics directly to PostgreSQL
"""

import json
import logging
import psycopg2
import pymongo
from kafka import KafkaConsumer
from datetime import datetime
import signal
import sys
import threading
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaPostgresStreamer:
    """Stream data from Kafka to PostgreSQL in real-time"""

    def __init__(self):
        # Database connections
        self.pg_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="ecommerce_dss",
            user="dss_user",
            password="dss_password_123"
        )
        self.pg_cursor = self.pg_conn.cursor()

        # MongoDB for metadata
        self.mongo_client = pymongo.MongoClient("mongodb://admin:admin_password@localhost:27017/")
        self.mongo_db = self.mongo_client['ecommerce_dss']

        # Kafka consumer
        self.consumer = KafkaConsumer(
            'vietnam_products',
            'vietnam_customers',
            'vietnam_sales_events',
            'ecommerce.orders.stream',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',  # Only get new messages
            enable_auto_commit=True,
            group_id='postgres_streaming_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        self.running = True
        logger.info("✅ Kafka-PostgreSQL Streamer initialized")

    def create_streaming_tables(self):
        """Create tables for streaming data if they don't exist"""

        # Real-time orders table
        create_realtime_orders = """
        CREATE TABLE IF NOT EXISTS realtime_orders (
            id SERIAL PRIMARY KEY,
            order_id VARCHAR(100) UNIQUE,
            customer_id VARCHAR(100),
            product_id VARCHAR(100),
            quantity INTEGER,
            total_amount DECIMAL(12,2),
            order_date TIMESTAMP,
            platform VARCHAR(50),
            payment_method VARCHAR(50),
            status VARCHAR(50) DEFAULT 'new',
            stream_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed BOOLEAN DEFAULT FALSE
        );
        """

        # Real-time vietnam products updates
        create_vietnam_products_stream = """
        CREATE TABLE IF NOT EXISTS vietnam_products_stream (
            id SERIAL PRIMARY KEY,
            product_id VARCHAR(100),
            product_name VARCHAR(500),
            brand VARCHAR(100),
            category VARCHAR(100),
            price_vnd BIGINT,
            price_usd DECIMAL(10,2),
            rating DECIMAL(3,2),
            review_count INTEGER,
            stock_quantity INTEGER,
            platform VARCHAR(50),
            vietnam_popularity DECIMAL(5,3),
            stream_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        # Real-time sales events
        create_sales_events = """
        CREATE TABLE IF NOT EXISTS realtime_sales_events (
            id SERIAL PRIMARY KEY,
            event_id VARCHAR(100) UNIQUE,
            customer_id VARCHAR(100),
            product_id VARCHAR(100),
            event_type VARCHAR(50),
            event_value DECIMAL(12,2),
            event_timestamp TIMESTAMP,
            platform VARCHAR(50),
            location VARCHAR(100),
            stream_received TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        try:
            self.pg_cursor.execute(create_realtime_orders)
            self.pg_cursor.execute(create_vietnam_products_stream)
            self.pg_cursor.execute(create_sales_events)
            self.pg_conn.commit()
            logger.info("✅ Streaming tables created/verified")
        except Exception as e:
            logger.error(f"❌ Failed to create streaming tables: {e}")
            self.pg_conn.rollback()

    def process_vietnam_products(self, data):
        """Process Vietnam products data from Kafka"""
        try:
            insert_query = """
            INSERT INTO vietnam_products_stream
            (product_id, product_name, brand, category, price_vnd, price_usd,
             rating, review_count, stock_quantity, platform, vietnam_popularity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                price_vnd = EXCLUDED.price_vnd,
                rating = EXCLUDED.rating,
                review_count = EXCLUDED.review_count,
                stock_quantity = EXCLUDED.stock_quantity,
                updated_at = CURRENT_TIMESTAMP
            """

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
                data.get('available_platforms', ['Unknown'])[0] if data.get('available_platforms') else 'Unknown',
                data.get('vietnam_popularity')
            )

            self.pg_cursor.execute(insert_query, values)
            self.pg_conn.commit()
            logger.info(f"✅ Inserted Vietnam product: {data.get('product_id')}")

        except Exception as e:
            logger.error(f"❌ Failed to process Vietnam product: {e}")
            self.pg_conn.rollback()

    def process_sales_event(self, data):
        """Process sales events from Kafka"""
        try:
            insert_query = """
            INSERT INTO realtime_sales_events
            (event_id, customer_id, product_id, event_type, event_value,
             event_timestamp, platform, location)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """

            values = (
                data.get('event_id', f"evt_{int(time.time())}"),
                data.get('customer_id'),
                data.get('product_id'),
                data.get('event_type', 'purchase'),
                data.get('amount', data.get('event_value', 0)),
                datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat())),
                data.get('platform', 'unknown'),
                data.get('location', 'Vietnam')
            )

            self.pg_cursor.execute(insert_query, values)
            self.pg_conn.commit()
            logger.info(f"✅ Inserted sales event: {data.get('event_id')}")

        except Exception as e:
            logger.error(f"❌ Failed to process sales event: {e}")
            self.pg_conn.rollback()

    def process_order_stream(self, data):
        """Process order stream from Kafka"""
        try:
            insert_query = """
            INSERT INTO realtime_orders
            (order_id, customer_id, product_id, quantity, total_amount,
             order_date, platform, payment_method, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE SET
                status = EXCLUDED.status,
                processed = FALSE
            """

            values = (
                data.get('order_id'),
                data.get('customer_id'),
                data.get('product_id'),
                data.get('quantity', 1),
                data.get('total_amount', data.get('amount', 0)),
                datetime.fromisoformat(data.get('order_date', datetime.now().isoformat())),
                data.get('platform', 'online'),
                data.get('payment_method', 'unknown'),
                data.get('status', 'new')
            )

            self.pg_cursor.execute(insert_query, values)
            self.pg_conn.commit()
            logger.info(f"✅ Inserted order: {data.get('order_id')}")

        except Exception as e:
            logger.error(f"❌ Failed to process order: {e}")
            self.pg_conn.rollback()

    def log_streaming_stats(self):
        """Log streaming statistics"""
        try:
            # Get table counts
            stats_query = """
            SELECT
                'vietnam_products_stream' as table_name, COUNT(*) as count,
                MAX(stream_timestamp) as latest
            FROM vietnam_products_stream
            UNION ALL
            SELECT
                'realtime_sales_events' as table_name, COUNT(*) as count,
                MAX(stream_received) as latest
            FROM realtime_sales_events
            UNION ALL
            SELECT
                'realtime_orders' as table_name, COUNT(*) as count,
                MAX(stream_timestamp) as latest
            FROM realtime_orders
            """

            self.pg_cursor.execute(stats_query)
            results = self.pg_cursor.fetchall()

            logger.info("📊 STREAMING STATISTICS:")
            for table_name, count, latest in results:
                logger.info(f"  {table_name}: {count} records (latest: {latest})")

            # Log to MongoDB for monitoring
            stats_doc = {
                "timestamp": datetime.now(),
                "streaming_stats": {
                    row[0]: {"count": row[1], "latest": row[2]}
                    for row in results
                }
            }
            self.mongo_db.streaming_monitoring.insert_one(stats_doc)

        except Exception as e:
            logger.error(f"❌ Failed to log streaming stats: {e}")

    def start_streaming(self):
        """Start the streaming process"""
        logger.info("🚀 Starting Kafka to PostgreSQL streaming...")

        # Create tables
        self.create_streaming_tables()

        messages_processed = 0
        last_stats_time = time.time()

        try:
            for message in self.consumer:
                if not self.running:
                    break

                topic = message.topic
                data = message.value

                # Route message to appropriate processor
                if topic == 'vietnam_products':
                    self.process_vietnam_products(data)
                elif topic == 'vietnam_sales_events':
                    self.process_sales_event(data)
                elif topic == 'ecommerce.orders.stream':
                    self.process_order_stream(data)

                messages_processed += 1

                # Log stats every 50 messages or 30 seconds
                current_time = time.time()
                if messages_processed % 50 == 0 or (current_time - last_stats_time) > 30:
                    self.log_streaming_stats()
                    last_stats_time = current_time

        except KeyboardInterrupt:
            logger.info("⚠️ Streaming interrupted by user")
        except Exception as e:
            logger.error(f"❌ Streaming error: {e}")
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
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()

def signal_handler(sig, frame):
    """Handle shutdown signals"""
    logger.info("🛑 Shutdown signal received")
    sys.exit(0)

if __name__ == "__main__":
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start streaming
    streamer = KafkaPostgresStreamer()

    try:
        streamer.start_streaming()
    except Exception as e:
        logger.error(f"❌ Failed to start streaming: {e}")
    finally:
        streamer.cleanup()
        logger.info("👋 Streaming service stopped")