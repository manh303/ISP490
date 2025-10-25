#!/usr/bin/env python3
"""
Simple Host-based processor for Vietnam e-commerce streaming data
Runs on host machine to bypass container networking issues
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

class VietnamHostProcessor:
    def __init__(self):
        # Host-based configuration (localhost connections)
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'group_id': 'vietnam_host_processor',
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None
        }

        self.postgres_config = {
            'host': 'localhost',
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
            logger.info("✅ Kafka consumer initialized successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka consumer: {e}")
            return False

    def init_postgres_connection(self):
        """Initialize PostgreSQL connection"""
        try:
            self.db_conn = psycopg2.connect(**self.postgres_config)
            self.db_conn.autocommit = True
            logger.info("✅ PostgreSQL connection established")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            return False

    def process_sales_event(self, data):
        """Process sales event data"""
        try:
            cursor = self.db_conn.cursor()

            # Parse order timestamp for date and time
            order_timestamp = data.get('order_timestamp', data.get('event_timestamp'))
            if order_timestamp:
                if 'T' in order_timestamp:
                    order_datetime = datetime.fromisoformat(order_timestamp.replace('Z', '+00:00'))
                else:
                    order_datetime = datetime.now()
                sale_date = order_datetime.date()
                sale_time = order_datetime.time()
            else:
                order_datetime = datetime.now()
                sale_date = order_datetime.date()
                sale_time = order_datetime.time()

            # Insert into fact_sales_vn table
            insert_sql = """
            INSERT INTO vietnam_dw.fact_sales_vn
            (order_id, sale_date, sale_time, quantity, unit_price_vnd, discount_amount_vnd,
             final_price_vnd, total_amount_vnd, vat_amount_vnd, shipping_fee_vnd, cod_fee_vnd,
             payment_method_vietnamese, is_cod_order, order_status_vn, is_tet_order,
             is_festival_order, shipping_method_vn, customer_key, product_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            """

            # Calculate values
            unit_price_vnd = data.get('unit_price_vnd', 0)
            quantity = data.get('quantity', 1)
            discount_amount_vnd = data.get('discount_amount_vnd', 0)
            final_price_vnd = unit_price_vnd - (discount_amount_vnd // quantity) if quantity > 0 else unit_price_vnd
            total_amount_vnd = data.get('total_amount_vnd', unit_price_vnd * quantity)
            vat_amount_vnd = data.get('tax_amount_vnd', 0)
            shipping_fee_vnd = data.get('shipping_fee_vnd', 0)
            cod_fee_vnd = 5000 if data.get('is_cod', False) else 0

            cursor.execute(insert_sql, (
                data.get('order_id'),
                sale_date,
                sale_time,
                quantity,
                unit_price_vnd,
                discount_amount_vnd,
                final_price_vnd,
                total_amount_vnd,
                vat_amount_vnd,
                shipping_fee_vnd,
                cod_fee_vnd,
                data.get('payment_method'),
                data.get('is_cod', False),
                data.get('order_status'),
                data.get('is_tet_order', False),
                data.get('is_festival_order', False),
                data.get('shipping_method'),
                1,  # Default customer_key
                1   # Default product_key
            ))

            cursor.close()
            return True

        except Exception as e:
            logger.error(f"❌ Error processing sales event: {e}")
            return False

    def start_processing(self):
        """Start processing Kafka streams"""
        logger.info("🚀 Starting Vietnam e-commerce host processor...")

        # Initialize connections
        if not self.init_kafka_consumer():
            logger.error("❌ Failed to initialize Kafka consumer")
            return

        if not self.init_postgres_connection():
            logger.error("❌ Failed to initialize PostgreSQL connection")
            return

        logger.info("🔄 Processing streaming data...")
        processed_count = 0
        sales_inserted = 0
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
                    if success:
                        sales_inserted += 1

                # Log progress every 50 messages
                if processed_count % 50 == 0:
                    logger.info(f"📊 Processed {processed_count} messages | Sales inserted: {sales_inserted} | Topics: {topic_counts}")

        except KeyboardInterrupt:
            logger.info(f"⏹️ Stopping processor. Total processed: {processed_count}")
            logger.info(f"💾 Sales records inserted: {sales_inserted}")
            logger.info(f"📋 Final counts: {topic_counts}")
        except Exception as e:
            logger.error(f"❌ Processing error: {e}")
        finally:
            if self.db_conn:
                self.db_conn.close()
            if self.consumer:
                self.consumer.close()

def main():
    processor = VietnamHostProcessor()
    processor.start_processing()

if __name__ == "__main__":
    main()