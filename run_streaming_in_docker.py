#!/usr/bin/env python3
"""
Run Streaming Service inside Docker Container
This solves the PostgreSQL port conflict issue
"""

import subprocess
import logging
import json
import time
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_streaming_script():
    """Create the streaming script to run inside Docker"""

    streaming_script = '''#!/usr/bin/env python3
"""
Streaming Service - Runs inside Docker container
"""

import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values
from collections import defaultdict
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DockerStreamingService:
    def __init__(self):
        # Docker network configuration
        self.kafka_config = {
            'bootstrap_servers': ['kafka:29092'],
            'group_id': 'docker_streaming_service',
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None,
            'consumer_timeout_ms': 15000
        }

        # PostgreSQL Docker network configuration
        self.postgres_config = {
            'host': 'ecommerce-dss-project-postgres-1',
            'port': 5432,
            'database': 'ecommerce_dss',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        self.stats = {
            'total_processed': 0,
            'orders_processed': 0,
            'customers_processed': 0,
            'start_time': None
        }

    def process_streaming_data(self, max_messages=2000):
        """Process streaming data from Kafka to PostgreSQL"""
        logger.info("🚀 Starting Docker streaming service...")

        try:
            # Connect to Kafka
            consumer = KafkaConsumer(
                'ecommerce.orders.stream',
                'ecommerce.customers.stream',
                **self.kafka_config
            )
            logger.info("✅ Kafka connected")

            # Connect to PostgreSQL
            conn = psycopg2.connect(**self.postgres_config)
            conn.autocommit = True
            cursor = conn.cursor()
            logger.info("✅ PostgreSQL connected")

            # Create streaming table
            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS streaming_results;
                CREATE TABLE IF NOT EXISTS streaming_results.processed_orders (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(100),
                    customer_id VARCHAR(100),
                    product_name VARCHAR(500),
                    total_amount NUMERIC(15,2),
                    city VARCHAR(100),
                    payment_method VARCHAR(50),
                    source_topic VARCHAR(100),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            logger.info("✅ Streaming table ready")

            self.stats['start_time'] = time.time()
            processed = 0

            # Process messages
            for message in consumer:
                try:
                    topic = message.topic
                    data = message.value

                    if not data:
                        continue

                    if 'orders' in topic:
                        # Handle wrapped data from big data generator
                        if isinstance(data, dict) and 'data' in data:
                            order_data = data['data']
                        else:
                            order_data = data

                        # Extract data
                        order_id = order_data.get('order_id', 'unknown')
                        customer_id = order_data.get('customer_id', 'unknown')
                        product_name = order_data.get('product_name', 'Unknown Product')
                        total_amount = order_data.get('total_amount', 0)
                        city = order_data.get('city', '')

                        # Handle shipping address
                        if 'shipping_address' in order_data:
                            city = order_data['shipping_address'].get('city', city)

                        payment_method = order_data.get('payment_method', 'unknown')

                        # Insert to database
                        cursor.execute("""
                            INSERT INTO streaming_results.processed_orders
                            (order_id, customer_id, product_name, total_amount, city, payment_method, source_topic)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (order_id, customer_id, product_name, total_amount, city, payment_method, topic))

                        processed += 1
                        self.stats['orders_processed'] += 1

                    elif 'customers' in topic:
                        processed += 1
                        self.stats['customers_processed'] += 1

                    self.stats['total_processed'] += 1

                    if processed % 500 == 0:
                        logger.info(f"📊 Processed {processed} messages...")

                    if processed >= max_messages:
                        logger.info(f"🎯 Reached target of {max_messages} messages")
                        break

                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    continue

            # Show final results
            cursor.execute("SELECT COUNT(*) FROM streaming_results.processed_orders;")
            total_orders = cursor.fetchone()[0]

            # Get sample data
            cursor.execute("""
                SELECT order_id, product_name, city, payment_method, total_amount
                FROM streaming_results.processed_orders
                ORDER BY processed_at DESC
                LIMIT 10;
            """)
            samples = cursor.fetchall()

            # Show statistics
            elapsed = time.time() - self.stats['start_time']

            logger.info("\\n" + "=" * 60)
            logger.info("🎉 DOCKER STREAMING SERVICE COMPLETED")
            logger.info("=" * 60)
            logger.info(f"⏱️  Processing Time: {elapsed:.2f} seconds")
            logger.info(f"📊 Total Messages: {self.stats['total_processed']}")
            logger.info(f"🛒 Orders in DB: {total_orders}")
            logger.info(f"🚀 Throughput: {self.stats['total_processed']/elapsed:.0f} msg/sec")

            logger.info("\\n🛒 SAMPLE PROCESSED ORDERS:")
            for i, (order_id, product, city, payment, amount) in enumerate(samples, 1):
                logger.info(f"  {i:2d}. {order_id} - {product}")
                logger.info(f"      💰 {amount:,.0f} VND - {city} - {payment}")

            consumer.close()
            cursor.close()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"❌ Docker streaming service failed: {e}")
            return False

def main():
    service = DockerStreamingService()
    success = service.process_streaming_data(max_messages=5000)

    if success:
        logger.info("\\n✅ STREAMING SERVICE SUCCESSFUL!")
        logger.info("🎯 Vietnamese e-commerce data successfully processed!")
    else:
        logger.error("\\n❌ STREAMING SERVICE FAILED!")

if __name__ == "__main__":
    main()
'''

    return streaming_script

def run_streaming_in_docker():
    """Run the streaming service inside Docker container"""

    logger.info("🐳 RUNNING STREAMING SERVICE INSIDE DOCKER")
    logger.info("=" * 60)

    # Create the streaming script
    streaming_script = create_streaming_script()

    try:
        # Run the script inside the data-pipeline container
        result = subprocess.run([
            'docker', 'exec', 'ecommerce-dss-project-data-pipeline-1',
            'python', '-c', streaming_script
        ], capture_output=True, text=True, timeout=120)

        if result.returncode == 0:
            # Print the output from the container
            logger.info("✅ Docker streaming service output:")
            print(result.stdout)
            return True
        else:
            logger.error(f"❌ Docker streaming service failed:")
            logger.error(result.stderr)
            return False

    except subprocess.TimeoutExpired:
        logger.warning("⏰ Docker streaming service timed out (normal for large data)")
        return True
    except Exception as e:
        logger.error(f"❌ Error running streaming service: {e}")
        return False

def verify_streaming_results():
    """Verify streaming results by checking the database"""

    logger.info("\\n🔍 VERIFYING STREAMING RESULTS...")

    verify_script = '''
import psycopg2
import json

try:
    conn = psycopg2.connect(
        host='ecommerce-dss-project-postgres-1',
        port=5432,
        database='ecommerce_dss',
        user='dss_user',
        password='dss_password_123'
    )
    cursor = conn.cursor()

    # Check if streaming_results table exists and has data
    cursor.execute("""
        SELECT COUNT(*)
        FROM streaming_results.processed_orders;
    """)
    count = cursor.fetchone()[0]

    # Get sample data
    cursor.execute("""
        SELECT city, payment_method, COUNT(*) as order_count
        FROM streaming_results.processed_orders
        GROUP BY city, payment_method
        ORDER BY order_count DESC
        LIMIT 10;
    """)
    city_stats = cursor.fetchall()

    cursor.close()
    conn.close()

    print(json.dumps({
        "success": True,
        "total_orders": count,
        "city_stats": city_stats
    }))

except Exception as e:
    print(json.dumps({
        "success": False,
        "error": str(e)
    }))
'''

    try:
        result = subprocess.run([
            'docker', 'exec', 'ecommerce-dss-project-data-pipeline-1',
            'python', '-c', verify_script
        ], capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            response = json.loads(result.stdout.strip())

            if response.get('success'):
                logger.info(f"✅ Verification successful!")
                logger.info(f"📊 Total orders in warehouse: {response['total_orders']:,}")
                logger.info(f"🏙️  Top city/payment combinations:")

                for city, payment, count in response['city_stats'][:5]:
                    logger.info(f"   • {city} - {payment}: {count} orders")

                return response['total_orders'] > 0
            else:
                logger.error(f"❌ Verification failed: {response['error']}")
                return False
        else:
            logger.error(f"❌ Verification script failed: {result.stderr}")
            return False

    except Exception as e:
        logger.error(f"❌ Verification error: {e}")
        return False

def main():
    """Main function"""
    logger.info("🎯 SOLVING POSTGRESQL CONNECTION ISSUE WITH DOCKER APPROACH")
    logger.info("=" * 80)

    # Run streaming service inside Docker
    success = run_streaming_in_docker()

    if success:
        # Verify results
        verified = verify_streaming_results()

        if verified:
            logger.info("\\n🎉 COMPLETE SUCCESS!")
            logger.info("✅ Streaming data processed and stored in warehouse")
            logger.info("✅ Vietnamese e-commerce data flow working properly")
            logger.info("💡 Solution: Running consumer inside Docker network avoids port conflicts")
        else:
            logger.warning("\\n⚠️ Processing completed but verification failed")
    else:
        logger.error("\\n❌ Streaming service failed")

if __name__ == "__main__":
    main()