#!/usr/bin/env python3
"""
Simple Spark-like processor for Vietnam e-commerce streaming data
Processes Kafka streams and writes to PostgreSQL
"""
import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VietnamDataProcessor:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['kafka:9092'],
            'group_id': 'vietnam_spark_processor',
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None
        }

        self.postgres_config = {
            'host': 'postgres',
            'port': 5432,
            'database': 'ecommerce_dss',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        self.batch_size = 100
        self.consumer = None
        self.db_conn = None

    def init_kafka_consumer(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                'vietnam_sales_events',
                'vietnam_customers',
                'vietnam_products',
                'vietnam_user_activities',
                **self.kafka_config
            )
            logger.info("Kafka consumer initialized successfully")
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

    def process_sales_event(self, data):
        """Process sales event data"""
        try:
            cursor = self.db_conn.cursor()

            # Insert into fact_sales_vn table
            insert_sql = """
            INSERT INTO vietnam_dw.fact_sales_vn
            (order_id, customer_id, product_id, order_date, quantity, unit_price, total_amount,
             payment_method, shipping_address, order_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            """

            cursor.execute(insert_sql, (
                data.get('order_id'),
                data.get('customer_id'),
                data.get('product_id'),
                data.get('order_date'),
                data.get('quantity', 1),
                data.get('unit_price'),
                data.get('total_amount'),
                data.get('payment_method'),
                data.get('shipping_address'),
                data.get('order_status', 'pending')
            ))

            cursor.close()
            return True

        except Exception as e:
            logger.error(f"Error processing sales event: {e}")
            return False

    def process_customer_data(self, data):
        """Process customer data"""
        try:
            cursor = self.db_conn.cursor()

            # Insert into dim_customer_vn table
            insert_sql = """
            INSERT INTO vietnam_dw.dim_customer_vn
            (customer_id, customer_name, email, phone, province_id, district, address,
             registration_date, customer_segment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
            """

            cursor.execute(insert_sql, (
                data.get('customer_id'),
                data.get('customer_name'),
                data.get('email'),
                data.get('phone'),
                1,  # Default province_id
                data.get('district'),
                data.get('address'),
                data.get('registration_date'),
                data.get('customer_segment', 'regular')
            ))

            cursor.close()
            return True

        except Exception as e:
            logger.error(f"Error processing customer data: {e}")
            return False

    def process_product_data(self, data):
        """Process product data"""
        try:
            cursor = self.db_conn.cursor()

            # Insert into dim_product_vn table
            insert_sql = """
            INSERT INTO vietnam_dw.dim_product_vn
            (product_id, product_name, category, brand, price, description)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
            """

            cursor.execute(insert_sql, (
                data.get('product_id'),
                data.get('product_name'),
                data.get('category'),
                data.get('brand'),
                data.get('price'),
                data.get('description')
            ))

            cursor.close()
            return True

        except Exception as e:
            logger.error(f"Error processing product data: {e}")
            return False

    def process_user_activity(self, data):
        """Process user activity data"""
        try:
            cursor = self.db_conn.cursor()

            # Insert into fact_customer_activity_vn table
            insert_sql = """
            INSERT INTO vietnam_dw.fact_customer_activity_vn
            (activity_id, customer_id, product_id, activity_type, activity_date,
             session_id, device_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (activity_id) DO NOTHING
            """

            cursor.execute(insert_sql, (
                data.get('activity_id'),
                data.get('customer_id'),
                data.get('product_id'),
                data.get('activity_type'),
                data.get('timestamp'),
                data.get('session_id'),
                data.get('device_type', 'web')
            ))

            cursor.close()
            return True

        except Exception as e:
            logger.error(f"Error processing user activity: {e}")
            return False

    def start_processing(self):
        """Start processing Kafka streams"""
        logger.info("Starting Vietnam e-commerce data processor...")

        # Initialize connections
        if not self.init_kafka_consumer():
            logger.error("Failed to initialize Kafka consumer")
            return

        if not self.init_postgres_connection():
            logger.error("Failed to initialize PostgreSQL connection")
            return

        logger.info("Processing streaming data...")
        processed_count = 0
        topic_counts = {}

        try:
            for message in self.consumer:
                topic = message.topic
                data = message.value

                if not data:
                    continue

                # Count messages by topic
                topic_counts[topic] = topic_counts.get(topic, 0) + 1
                processed_count += 1

                # Process based on topic
                success = False
                if topic == 'vietnam_sales_events':
                    success = self.process_sales_event(data)
                elif topic == 'vietnam_customers':
                    success = self.process_customer_data(data)
                elif topic == 'vietnam_products':
                    success = self.process_product_data(data)
                elif topic == 'vietnam_user_activities':
                    success = self.process_user_activity(data)

                # Log progress every 50 messages
                if processed_count % 50 == 0:
                    logger.info(f"Processed {processed_count} messages: {topic_counts}")

        except KeyboardInterrupt:
            logger.info(f"Stopping processor. Total processed: {processed_count}")
            logger.info(f"Final counts: {topic_counts}")
        except Exception as e:
            logger.error(f"Processing error: {e}")
        finally:
            if self.db_conn:
                self.db_conn.close()
            if self.consumer:
                self.consumer.close()

def main():
    processor = VietnamDataProcessor()
    processor.start_processing()

if __name__ == "__main__":
    main()