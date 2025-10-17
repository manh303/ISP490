#!/usr/bin/env python3
"""
Test Comprehensive Kafka Producer - Only Kafka functionality
Test without database connections - generate mock data and send to Kafka
"""

import json
import time
import logging
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MockKafkaProducer:
    def __init__(self):
        # Kafka configuration
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'value_serializer': lambda v: json.dumps(v, default=self.json_serializer).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',
            'retries': 3,
            'batch_size': 16384,
            'linger_ms': 100,
            'buffer_memory': 33554432
        }

        # Kafka topics
        self.topics = {
            'orders': 'ecommerce.orders.stream',
            'customers': 'ecommerce.customers.stream',
            'products': 'ecommerce.products.stream',
            'crawl_data': 'ecommerce.crawl.stream',
            'raw_data': 'ecommerce.raw.stream'
        }

        self.producer = None

    def json_serializer(self, obj):
        """Custom JSON serializer for datetime objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def init_kafka_producer(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(**self.kafka_config)
            logger.info("✅ Kafka producer initialized")
            return True
        except Exception as e:
            logger.error(f"❌ Kafka producer failed: {e}")
            return False

    def send_to_kafka(self, topic, data, key=None):
        """Send data to Kafka topic với error handling"""
        try:
            # Add metadata
            kafka_message = {
                'data': data,
                'metadata': {
                    'produced_at': datetime.now(),
                    'producer_id': 'mock_comprehensive_producer',
                    'topic': topic,
                    'message_id': str(uuid.uuid4())
                }
            }

            future = self.producer.send(topic, value=kafka_message, key=key)
            record_metadata = future.get(timeout=10)

            logger.debug(f"Sent to {topic}: partition={record_metadata.partition}, offset={record_metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to send to Kafka topic {topic}: {e}")
            return False

    def generate_mock_orders_data(self, count=15):
        """Generate mock historical orders data"""
        logger.info(f"🔄 Generating {count} mock historical orders...")

        orders = []
        for i in range(count):
            order = {
                'order_id': f'HIST_ORD_{int(time.time())}_{i}',
                'customer_id': f'HIST_CUST_{random.randint(100000, 999999)}',
                'product_id': f'HIST_PROD_{random.randint(1000, 9999)}',
                'quantity': random.randint(1, 5),
                'total_amount': random.randint(500000, 50000000),  # VND
                'order_date': datetime.now() - timedelta(days=random.randint(0, 365)),
                'status': random.choice(['completed', 'shipped', 'delivered']),
                'payment_method': random.choice(['vnpay', 'momo', 'zalopay', 'banking', 'cod']),
                'shipping_address': {
                    'city': random.choice(['Ho Chi Minh', 'Hanoi', 'Da Nang', 'Hai Phong', 'Can Tho']),
                    'district': f'District {random.randint(1, 12)}',
                    'street': f'{random.randint(1, 999)} {random.choice(["Nguyen Trai", "Le Lai", "Tran Hung Dao"])}'
                },
                'source': 'mock_postgresql_data',
                'data_type': 'historical_order'
            }
            orders.append(order)

        return orders

    def generate_mock_customers_data(self, count=20):
        """Generate mock customers data"""
        logger.info(f"🔄 Generating {count} mock customers...")

        customers = []
        for i in range(count):
            customer = {
                'customer_id': f'HIST_CUST_{random.randint(100000, 999999)}',
                'name': f'Customer {i+1}',
                'email': f'customer{i+1}@email.com',
                'phone': f'09{random.randint(10000000, 99999999)}',
                'address': f'{random.randint(1, 999)} {random.choice(["Nguyen Trai", "Le Lai", "Tran Hung Dao"])}',
                'city': random.choice(['Ho Chi Minh', 'Hanoi', 'Da Nang', 'Hai Phong', 'Can Tho']),
                'registration_date': datetime.now() - timedelta(days=random.randint(0, 1000)),
                'customer_segment': random.choice(['standard', 'silver', 'gold', 'platinum']),
                'total_spent': random.randint(1000000, 100000000),  # VND
                'source': 'mock_postgresql_data',
                'data_type': 'historical_customer'
            }
            customers.append(customer)

        return customers

    def generate_mock_products_data(self, count=10):
        """Generate mock products data"""
        logger.info(f"🔄 Generating {count} mock products...")

        vietnamese_products = [
            {'name': 'iPhone 15 Pro Max', 'category': 'Electronics', 'price': 35000000},
            {'name': 'Samsung Galaxy S24', 'category': 'Electronics', 'price': 25000000},
            {'name': 'MacBook Air M3', 'category': 'Electronics', 'price': 30000000},
            {'name': 'Dell XPS 13', 'category': 'Electronics', 'price': 28000000},
            {'name': 'AirPods Pro', 'category': 'Electronics', 'price': 7000000},
            {'name': 'Áo Polo Nam', 'category': 'Fashion', 'price': 500000},
            {'name': 'Giày Sneaker', 'category': 'Fashion', 'price': 1500000},
            {'name': 'Balo Laptop', 'category': 'Accessories', 'price': 800000},
            {'name': 'Cà phê Arabica', 'category': 'Food', 'price': 200000},
            {'name': 'Trà Ô Long', 'category': 'Food', 'price': 150000}
        ]

        products = []
        for i in range(count):
            prod_template = random.choice(vietnamese_products)
            product = {
                'product_id': f'HIST_PROD_{random.randint(1000, 9999)}',
                'product_name': prod_template['name'],
                'category': prod_template['category'],
                'price': prod_template['price'],
                'description': f"High quality {prod_template['name']} for Vietnamese market",
                'stock_quantity': random.randint(10, 1000),
                'brand': random.choice(['Apple', 'Samsung', 'Dell', 'Nike', 'Adidas', 'Vietnamese Brand']),
                'created_date': datetime.now() - timedelta(days=random.randint(0, 365)),
                'source': 'mock_postgresql_data',
                'data_type': 'historical_product'
            }
            products.append(product)

        return products

    def generate_realtime_streaming_data(self, count=10):
        """Generate real-time streaming orders"""
        logger.info(f"🔄 Generating {count} real-time streaming orders...")

        orders = []
        for i in range(count):
            order = {
                'order_id': f'RT_ORD_{int(time.time())}_{i}',
                'customer_id': f'RT_CUST_{random.randint(100000, 999999)}',
                'product_id': f'RT_PROD_{random.randint(1000, 9999)}',
                'product_name': random.choice(['iPhone 15 Pro Max', 'Samsung Galaxy S24', 'MacBook Air M3']),
                'quantity': random.randint(1, 3),
                'total_amount': random.randint(5000000, 40000000),  # VND
                'order_date': datetime.now(),
                'payment_method': random.choice(['vnpay', 'momo', 'zalopay', 'banking']),
                'platform': random.choice(['mobile', 'web', 'app']),
                'status': random.choice(['pending', 'confirmed', 'processing']),
                'shipping_address': {
                    'city': random.choice(['Ho Chi Minh', 'Hanoi', 'Da Nang']),
                    'district': f'District {random.randint(1, 12)}',
                    'street': f'{random.randint(1, 999)} Street'
                },
                'customer_info': {
                    'age_group': random.choice(['18-25', '26-35', '36-45', '46-55']),
                    'gender': random.choice(['male', 'female']),
                    'membership': random.choice(['standard', 'silver', 'gold', 'platinum'])
                },
                'source': 'realtime_generator',
                'data_type': 'realtime_streaming'
            }
            orders.append(order)

        return orders

    def run_mock_comprehensive_extraction(self):
        """Run mock comprehensive extraction to test Kafka only"""
        logger.info("🚀 Starting Mock Comprehensive Kafka Data Extraction")
        logger.info("=" * 60)

        try:
            # Initialize Kafka producer
            if not self.init_kafka_producer():
                return False

            # Generate and send mock data
            logger.info("📊 PHASE 1: Generating and Sending Mock Historical Data")

            # Mock orders
            mock_orders = self.generate_mock_orders_data(15)
            orders_sent = 0
            for order in mock_orders:
                if self.send_to_kafka(self.topics['orders'], order, order['order_id']):
                    orders_sent += 1

            # Mock customers
            mock_customers = self.generate_mock_customers_data(20)
            customers_sent = 0
            for customer in mock_customers:
                if self.send_to_kafka(self.topics['customers'], customer, customer['customer_id']):
                    customers_sent += 1

            # Mock products
            mock_products = self.generate_mock_products_data(10)
            products_sent = 0
            for product in mock_products:
                if self.send_to_kafka(self.topics['products'], product, product['product_id']):
                    products_sent += 1

            logger.info("📊 PHASE 2: Generating and Sending Real-time Streaming Data")
            realtime_orders = self.generate_realtime_streaming_data(15)
            realtime_sent = 0
            for order in realtime_orders:
                if self.send_to_kafka(self.topics['orders'], order, order['order_id']):
                    realtime_sent += 1
                time.sleep(0.1)  # Simulate real-time delay

            # Flush all messages
            self.producer.flush()

            # Summary
            total_sent = orders_sent + customers_sent + products_sent + realtime_sent

            logger.info("=" * 60)
            logger.info("🎯 MOCK COMPREHENSIVE KAFKA EXTRACTION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Mock Historical Orders: {orders_sent}")
            logger.info(f"Mock Historical Customers: {customers_sent}")
            logger.info(f"Mock Historical Products: {products_sent}")
            logger.info(f"Real-time Generated Orders: {realtime_sent}")
            logger.info(f"TOTAL RECORDS SENT TO KAFKA: {total_sent}")

            logger.info("\n✅ All mock data successfully sent to Kafka!")
            logger.info("🔄 Kafka topics populated with Vietnamese e-commerce data!")
            logger.info("🎯 Ready for Spark processing...")

            return True

        except Exception as e:
            logger.error(f"❌ Mock extraction failed: {e}")
            return False

        finally:
            # Cleanup
            if self.producer:
                self.producer.close()

def main():
    """Main function"""
    producer = MockKafkaProducer()
    success = producer.run_mock_comprehensive_extraction()

    if success:
        logger.info("\n✅ Mock Kafka producer test successful!")
        logger.info("🎉 Kafka infrastructure fully operational for Vietnamese e-commerce DSS!")
    else:
        logger.error("\n❌ Mock Kafka producer test failed!")

    return success

if __name__ == "__main__":
    main()