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
from psycopg2.extras import execute_values
import threading
import queue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class LightweightKafkaProcessor:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'group_id': 'lightweight_dw_processor',
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'max_poll_records': 10,  # Process small batches
            'session_timeout_ms': 30000,
            'heartbeat_interval_ms': 10000,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None
        }

        self.postgres_config = {
            'host': '127.0.0.1',
            'port': 5432,
            'database': 'ecommerce_dss',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        self.batch_size = 5  # Small batch size to avoid memory issues
        self.consumer = None
        self.db_conn = None
        self.running = True
        self.processed_count = 0
        self.message_queue = queue.Queue(maxsize=50)

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Received shutdown signal, stopping processor...")
        self.running = False
        if self.consumer:
            self.consumer.close()

    def init_kafka_consumer(self):
        """Initialize lightweight Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                'ecommerce.orders.stream',
                **self.kafka_config
            )
            logger.info("Lightweight Kafka consumer initialized")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            return False

    def init_postgres_connection(self):
        """Initialize PostgreSQL connection with proper settings"""
        try:
            self.db_conn = psycopg2.connect(**self.postgres_config)
            self.db_conn.autocommit = True

            # Set connection parameters for better performance
            cursor = self.db_conn.cursor()
            cursor.execute("SET statement_timeout = '30s';")
            cursor.execute("SET lock_timeout = '10s';")
            cursor.close()

            logger.info("PostgreSQL connection established")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False

    def process_message_batch(self, messages):
        """Process a batch of messages efficiently"""
        if not messages:
            return 0

        try:
            cursor = self.db_conn.cursor()
            processed = 0

            for message in messages:
                try:
                    data = message.value
                    if not data:
                        continue

                    # Extract basic order data
                    order_id = data.get('order_id')
                    if not order_id:
                        continue

                    cursor.execute("""
                        INSERT INTO raw_data.orders (
                            order_id, customer_id, product_id, quantity,
                            total_amount, order_date, status, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (order_id) DO NOTHING;
                    """, (
                        order_id,
                        data.get('customer_id'),
                        data.get('product_id'),
                        data.get('quantity', 1),
                        float(data.get('total_amount', 0)),
                        data.get('order_date'),
                        data.get('status', 'unknown')
                    ))

                    processed += 1

                except Exception as e:
                    logger.warning(f"Error processing individual message: {e}")
                    continue

            cursor.close()
            self.processed_count += processed

            if processed > 0:
                logger.info(f"Processed batch of {processed} orders. Total: {self.processed_count}")

            return processed

        except Exception as e:
            logger.error(f"Error processing message batch: {e}")
            return 0

    def run_lightweight_processor(self):
        """Main lightweight processor loop"""
        logger.info("Starting lightweight Kafka to Datawarehouse processor")

        # Set up signal handlers
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

        if not self.init_kafka_consumer():
            return False

        if not self.init_postgres_connection():
            return False

        message_batch = []
        last_commit_time = time.time()

        try:
            logger.info("Starting message consumption...")

            while self.running:
                try:
                    # Poll for messages with timeout
                    message_batch_dict = self.consumer.poll(timeout_ms=1000)

                    if not message_batch_dict:
                        # No messages, check if we should process accumulated batch
                        if message_batch and (time.time() - last_commit_time > 10):
                            self.process_message_batch(message_batch)
                            message_batch = []
                            last_commit_time = time.time()
                        continue

                    # Extract messages from all partitions
                    for topic_partition, messages in message_batch_dict.items():
                        message_batch.extend(messages)

                    # Process batch when it reaches batch_size or timeout
                    if len(message_batch) >= self.batch_size or (time.time() - last_commit_time > 5):
                        self.process_message_batch(message_batch)
                        message_batch = []
                        last_commit_time = time.time()

                except Exception as e:
                    logger.error(f"Error in processing loop: {e}")
                    time.sleep(1)  # Brief pause on error

        except KeyboardInterrupt:
            logger.info("Processor stopped by user")
        finally:
            # Process remaining messages
            if message_batch:
                self.process_message_batch(message_batch)

            # Cleanup
            if self.consumer:
                self.consumer.close()
            if self.db_conn:
                self.db_conn.close()

            logger.info(f"Lightweight processor shutdown complete. Total processed: {self.processed_count}")

if __name__ == "__main__":
    processor = LightweightKafkaProcessor()
    processor.run_lightweight_processor()