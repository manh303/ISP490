#!/usr/bin/env python3
"""
Lightweight Kafka to Datawarehouse Processor
Memory-optimized streaming processor for production use
"""
import json
import time
import logging
import signal
import sys
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LightweightKafkaProcessor:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['kafka:9092'],
            'group_id': 'lightweight_dw_processor',
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

        self.batch_size = 3
        self.consumer = None
        self.db_conn = None
        self.running = True
        self.processed_count = 0

    def init_kafka_consumer(self):
        try:
            self.consumer = KafkaConsumer('ecommerce.orders.stream', **self.kafka_config)
            logger.info("Lightweight Kafka consumer initialized")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            return False

    def init_postgres_connection(self):
        try:
            self.db_conn = psycopg2.connect(**self.postgres_config)
            self.db_conn.autocommit = True
            logger.info("PostgreSQL connection established")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False

    def process_message_batch(self, messages):
        if not messages:
            return 0

        try:
            cursor = self.db_conn.cursor()
            processed = 0

            for message in messages:
                try:
                    data = message.value
                    if not data or not data.get('order_id'):
                        continue

                    cursor.execute("""
                        INSERT INTO raw_data.orders (
                            order_id, customer_id, product_id, quantity,
                            total_amount, order_date, status, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (order_id) DO NOTHING;
                    """, (
                        data.get('order_id'),
                        data.get('customer_id'),
                        data.get('product_id'),
                        data.get('quantity', 1),
                        float(data.get('total_amount', 0)),
                        data.get('order_date'),
                        data.get('status', 'unknown')
                    ))
                    processed += 1
                except Exception as e:
                    logger.warning(f"Error processing message: {e}")
                    continue

            cursor.close()
            self.processed_count += processed

            if processed > 0:
                logger.info(f"Processed {processed} orders. Total: {self.processed_count}")
            return processed

        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return 0

    def run_lightweight_processor(self):
        logger.info("Starting lightweight Kafka processor")

        if not self.init_kafka_consumer():
            return False
        if not self.init_postgres_connection():
            return False

        message_batch = []
        start_time = time.time()

        try:
            for message in self.consumer:
                if not self.running:
                    break

                message_batch.append(message)

                if len(message_batch) >= self.batch_size:
                    self.process_message_batch(message_batch)
                    message_batch = []

                # Stop after processing for 60 seconds to avoid memory buildup
                if time.time() - start_time > 60:
                    logger.info("Stopping after 60 seconds to prevent memory issues")
                    break

        except KeyboardInterrupt:
            logger.info("Processor stopped by user")
        finally:
            if message_batch:
                self.process_message_batch(message_batch)

            if self.consumer:
                self.consumer.close()
            if self.db_conn:
                self.db_conn.close()

            logger.info(f"Processor shutdown. Total processed: {self.processed_count}")

if __name__ == "__main__":
    processor = LightweightKafkaProcessor()
    processor.run_lightweight_processor()
