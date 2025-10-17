#!/usr/bin/env python3
"""
Optimized Kafka Streaming Service for Datawarehouse
Memory-safe production streaming processor
"""
import json
import time
import logging
import sys
import gc
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedStreamingService:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['kafka:9092'],
            'group_id': 'optimized_dw_streaming',
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'max_poll_records': 5,
            'session_timeout_ms': 30000,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None
        }

        self.postgres_config = {
            'host': 'postgres',
            'port': 5432,
            'database': 'ecommerce_dss',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        self.processed_count = 0
        self.error_count = 0
        self.max_runtime = 300  # 5 minutes max runtime to prevent memory buildup

    def process_streaming_data(self):
        """Process streaming data with memory optimization"""
        consumer = None
        db_conn = None

        try:
            # Initialize Kafka consumer
            consumer = KafkaConsumer('ecommerce.orders.stream', **self.kafka_config)
            logger.info("✅ Kafka consumer initialized")

            # Initialize PostgreSQL connection
            db_conn = psycopg2.connect(**self.postgres_config)
            db_conn.autocommit = True
            logger.info("✅ PostgreSQL connection established")

            cursor = db_conn.cursor()
            start_time = time.time()
            last_gc = time.time()

            logger.info("🚀 Starting optimized streaming data processing...")

            for message in consumer:
                try:
                    # Check runtime limit
                    if time.time() - start_time > self.max_runtime:
                        logger.info("⏰ Max runtime reached, stopping to prevent memory issues")
                        break

                    # Process message
                    data = message.value
                    if not data or not data.get('order_id'):
                        continue

                    order_id = data.get('order_id')
                    payment_method = data.get('payment_method', 'Unknown')

                    # Parse order date safely
                    order_date_str = data.get('order_date', '2025-10-10')
                    order_date = order_date_str.split('T')[0] if 'T' in order_date_str else order_date_str

                    cursor.execute("""
                        INSERT INTO raw_data.orders (order_id, customer_id, order_date,
                            total_amount, payment_method, status, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (order_id) DO NOTHING;
                    """, (
                        order_id,
                        data.get('customer_id'),
                        order_date,
                        float(data.get('total_amount', 0)),
                        payment_method,
                        data.get('status', 'unknown'),
                        datetime.now()
                    ))

                    self.processed_count += 1

                    if self.processed_count % 5 == 0:
                        logger.info(f"✅ Processed {self.processed_count} streaming orders")

                    # Periodic garbage collection to prevent memory buildup
                    if time.time() - last_gc > 30:
                        gc.collect()
                        last_gc = time.time()

                except Exception as e:
                    self.error_count += 1
                    logger.warning(f"⚠️ Error processing message: {e}")
                    continue

            cursor.close()

        except Exception as e:
            logger.error(f"❌ Critical error in streaming service: {e}")
        finally:
            if consumer:
                consumer.close()
            if db_conn:
                db_conn.close()

            logger.info(f"🎯 Streaming service completed. Processed: {self.processed_count}, Errors: {self.error_count}")

if __name__ == "__main__":
    service = OptimizedStreamingService()
    service.process_streaming_data()