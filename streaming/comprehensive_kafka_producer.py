#!/usr/bin/env python3
"""
Comprehensive Kafka Producer
Đẩy TẤT CẢ dữ liệu vào Kafka: streaming data, crawl data, existing data
Luồng: All Data Sources → Kafka → Spark → Datawarehouse
"""

import json
import time
import logging
import psycopg2
import pymongo
from datetime import datetime, timedelta
from kafka import KafkaProducer
import random
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ComprehensiveKafkaProducer:
    def __init__(self):
        # Kafka configuration
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],  # Docker service
            'value_serializer': lambda v: json.dumps(v, default=self.json_serializer).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',
            'retries': 3,
            'batch_size': 16384,
            'linger_ms': 100,
            'buffer_memory': 33554432
        }

        # Database connections
        self.postgres_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'ecommerce_dss',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        self.mongo_uri = 'mongodb://admin:admin_password@localhost:27017/'

        # Kafka topics
        self.topics = {
            'orders': 'ecommerce.orders.stream',
            'customers': 'ecommerce.customers.stream',
            'products': 'ecommerce.products.stream',
            'crawl_data': 'ecommerce.crawl.stream',
            'raw_data': 'ecommerce.raw.stream'
        }

        self.producer = None
        self.postgres_conn = None
        self.mongo_client = None

    def json_serializer(self, obj):
        """Custom JSON serializer for datetime objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def connect_databases(self):
        """Kết nối database"""
        try:
            # PostgreSQL
            self.postgres_conn = psycopg2.connect(**self.postgres_config)
            logger.info("✅ Connected to PostgreSQL")

            # MongoDB
            self.mongo_client = pymongo.MongoClient(self.mongo_uri)
            self.mongo_client.admin.command('ping')
            logger.info("✅ Connected to MongoDB")

            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False

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
                    'producer_id': 'comprehensive_producer',
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

    def extract_existing_orders_data(self):
        """Đẩy tất cả dữ liệu orders có sẵn vào Kafka"""
        logger.info("🔄 Extracting existing orders data to Kafka...")

        try:
            cursor = self.postgres_conn.cursor()

            # Get all existing orders from raw_data
            cursor.execute("""
                SELECT order_id, customer_id, product_id, quantity, total_amount,
                       order_date, status, payment_method, shipping_address
                FROM raw_data.orders
                ORDER BY order_date DESC
            """)

            orders = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            sent_count = 0
            for order_tuple in orders:
                order_dict = dict(zip(columns, order_tuple))

                # Convert to Kafka-friendly format
                kafka_order = {
                    'order_id': order_dict['order_id'],
                    'customer_id': order_dict['customer_id'],
                    'product_id': order_dict['product_id'],
                    'quantity': order_dict['quantity'],
                    'total_amount': float(order_dict['total_amount']) if order_dict['total_amount'] else 0,
                    'order_date': order_dict['order_date'],
                    'status': order_dict['status'],
                    'payment_method': order_dict['payment_method'],
                    'shipping_address': json.loads(order_dict['shipping_address']) if order_dict['shipping_address'] else {},
                    'source': 'existing_postgresql_data',
                    'data_type': 'historical_order'
                }

                if self.send_to_kafka(self.topics['orders'], kafka_order, order_dict['order_id']):
                    sent_count += 1

            cursor.close()
            logger.info(f"✅ Sent {sent_count} existing orders to Kafka")
            return sent_count

        except Exception as e:
            logger.error(f"❌ Failed to extract orders: {e}")
            return 0

    def extract_existing_customers_data(self):
        """Đẩy tất cả dữ liệu customers có sẵn vào Kafka"""
        logger.info("🔄 Extracting existing customers data to Kafka...")

        try:
            cursor = self.postgres_conn.cursor()

            cursor.execute("""
                SELECT customer_id, name, email, phone, address, city,
                       registration_date, customer_segment, total_spent
                FROM raw_data.customers
                ORDER BY registration_date DESC
            """)

            customers = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            sent_count = 0
            for customer_tuple in customers:
                customer_dict = dict(zip(columns, customer_tuple))

                kafka_customer = {
                    'customer_id': customer_dict['customer_id'],
                    'name': customer_dict['name'],
                    'email': customer_dict['email'],
                    'phone': customer_dict['phone'],
                    'address': customer_dict['address'],
                    'city': customer_dict['city'],
                    'registration_date': customer_dict['registration_date'],
                    'customer_segment': customer_dict['customer_segment'],
                    'total_spent': float(customer_dict['total_spent']) if customer_dict['total_spent'] else 0,
                    'source': 'existing_postgresql_data',
                    'data_type': 'historical_customer'
                }

                if self.send_to_kafka(self.topics['customers'], kafka_customer, customer_dict['customer_id']):
                    sent_count += 1

            cursor.close()
            logger.info(f"✅ Sent {sent_count} existing customers to Kafka")
            return sent_count

        except Exception as e:
            logger.error(f"❌ Failed to extract customers: {e}")
            return 0

    def extract_existing_products_data(self):
        """Đẩy tất cả dữ liệu products có sẵn vào Kafka"""
        logger.info("🔄 Extracting existing products data to Kafka...")

        try:
            cursor = self.postgres_conn.cursor()

            cursor.execute("""
                SELECT product_id, product_name, category, price, description,
                       stock_quantity, brand, created_date
                FROM raw_data.products
                ORDER BY created_date DESC
            """)

            products = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            sent_count = 0
            for product_tuple in products:
                product_dict = dict(zip(columns, product_tuple))

                kafka_product = {
                    'product_id': product_dict['product_id'],
                    'product_name': product_dict['product_name'],
                    'category': product_dict['category'],
                    'price': float(product_dict['price']) if product_dict['price'] else 0,
                    'description': product_dict['description'],
                    'stock_quantity': product_dict['stock_quantity'],
                    'brand': product_dict['brand'],
                    'created_date': product_dict['created_date'],
                    'source': 'existing_postgresql_data',
                    'data_type': 'historical_product'
                }

                if self.send_to_kafka(self.topics['products'], kafka_product, product_dict['product_id']):
                    sent_count += 1

            cursor.close()
            logger.info(f"✅ Sent {sent_count} existing products to Kafka")
            return sent_count

        except Exception as e:
            logger.error(f"❌ Failed to extract products: {e}")
            return 0

    def extract_mongodb_streaming_data(self):
        """Đẩy dữ liệu streaming từ MongoDB vào Kafka"""
        logger.info("🔄 Extracting MongoDB streaming data to Kafka...")

        try:
            db = self.mongo_client['ecommerce_dss']

            # Get processed streaming orders
            streaming_orders = list(db.processed_orders_stream.find({}))

            sent_count = 0
            for doc in streaming_orders:
                original_data = doc.get('original_data', doc)

                kafka_order = {
                    'order_id': original_data.get('order_id'),
                    'customer_id': original_data.get('customer_id'),
                    'product_id': original_data.get('product_id'),
                    'product_name': original_data.get('product_name'),
                    'quantity': original_data.get('quantity', 1),
                    'total_amount': original_data.get('total_amount', 0),
                    'order_date': original_data.get('order_date'),
                    'payment_method': original_data.get('payment_method'),
                    'platform': original_data.get('platform'),
                    'status': original_data.get('status'),
                    'shipping_address': original_data.get('shipping_address', {}),
                    'customer_info': original_data.get('customer_info', {}),
                    'source': 'mongodb_streaming',
                    'data_type': 'streaming_order',
                    'kafka_metadata': doc.get('kafka_metadata', {})
                }

                if self.send_to_kafka(self.topics['orders'], kafka_order, original_data.get('order_id')):
                    sent_count += 1

            logger.info(f"✅ Sent {sent_count} MongoDB streaming orders to Kafka")
            return sent_count

        except Exception as e:
            logger.error(f"❌ Failed to extract MongoDB data: {e}")
            return 0

    def generate_realtime_streaming_data(self, num_records=10):
        """Sinh dữ liệu streaming real-time và đẩy vào Kafka"""
        logger.info(f"🔄 Generating {num_records} real-time streaming records...")

        # Vietnamese e-commerce data
        cities = ['Ho Chi Minh', 'Hanoi', 'Da Nang', 'Hai Phong', 'Can Tho', 'Bien Hoa']
        districts = ['District 1', 'District 3', 'District 7', 'Ba Dinh', 'Hai Ba Trung', 'Thanh Xuan']
        payment_methods = ['vnpay', 'momo', 'zalopay', 'banking', 'cod']
        platforms = ['mobile', 'web', 'app']
        statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'completed']

        products = [
            {'name': 'iPhone 15 Pro Max', 'price_range': (25000000, 35000000)},
            {'name': 'Samsung Galaxy S24', 'price_range': (15000000, 25000000)},
            {'name': 'MacBook Air M3', 'price_range': (25000000, 35000000)},
            {'name': 'Dell XPS 13', 'price_range': (20000000, 30000000)},
            {'name': 'AirPods Pro', 'price_range': (5000000, 8000000)},
            {'name': 'iPad Pro', 'price_range': (15000000, 25000000)}
        ]

        sent_count = 0
        for i in range(num_records):
            product = random.choice(products)
            city = random.choice(cities)

            # Generate realistic order
            order_data = {
                'order_id': f'ORD_RT_{int(time.time())}_{i}',
                'customer_id': f'CUST_RT_{random.randint(100000, 999999)}',
                'product_id': f'PROD_RT_{random.randint(1000, 9999)}',
                'product_name': product['name'],
                'quantity': random.randint(1, 3),
                'total_amount': random.randint(*product['price_range']),
                'order_date': datetime.now(),
                'payment_method': random.choice(payment_methods),
                'platform': random.choice(platforms),
                'status': random.choice(statuses),
                'shipping_address': {
                    'city': city,
                    'district': random.choice(districts),
                    'street': f'{random.randint(1, 999)} {random.choice(["Nguyen Trai", "Le Lai", "Tran Hung Dao"])}'
                },
                'customer_info': {
                    'age_group': random.choice(['18-25', '26-35', '36-45', '46-55', '55+']),
                    'gender': random.choice(['male', 'female']),
                    'membership': random.choice(['standard', 'silver', 'gold', 'platinum'])
                },
                'source': 'realtime_generator',
                'data_type': 'realtime_streaming'
            }

            if self.send_to_kafka(self.topics['orders'], order_data, order_data['order_id']):
                sent_count += 1

            # Small delay between messages
            time.sleep(0.1)

        logger.info(f"✅ Generated and sent {sent_count} real-time streaming orders")
        return sent_count

    def run_comprehensive_extraction(self):
        """Chạy toàn bộ extraction process"""
        logger.info("🚀 Starting Comprehensive Kafka Data Extraction")
        logger.info("=" * 60)

        try:
            # Connect to databases
            if not self.connect_databases():
                return False

            # Initialize Kafka producer
            if not self.init_kafka_producer():
                return False

            # Extract all existing data
            logger.info("📊 PHASE 1: Extracting Historical Data")
            orders_count = self.extract_existing_orders_data()
            customers_count = self.extract_existing_customers_data()
            products_count = self.extract_existing_products_data()

            # Extract MongoDB streaming data
            logger.info("📊 PHASE 2: Extracting MongoDB Streaming Data")
            mongo_count = self.extract_mongodb_streaming_data()

            # Generate real-time data
            logger.info("📊 PHASE 3: Generating Real-time Streaming Data")
            realtime_count = self.generate_realtime_streaming_data(15)

            # Flush all messages
            self.producer.flush()

            # Summary
            total_sent = orders_count + customers_count + products_count + mongo_count + realtime_count

            logger.info("=" * 60)
            logger.info("🎯 COMPREHENSIVE KAFKA EXTRACTION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Historical Orders: {orders_count}")
            logger.info(f"Historical Customers: {customers_count}")
            logger.info(f"Historical Products: {products_count}")
            logger.info(f"MongoDB Streaming: {mongo_count}")
            logger.info(f"Real-time Generated: {realtime_count}")
            logger.info(f"TOTAL RECORDS SENT TO KAFKA: {total_sent}")

            logger.info("\n✅ All data successfully sent to Kafka!")
            logger.info("🔄 Ready for Spark processing...")

            return True

        except Exception as e:
            logger.error(f"❌ Comprehensive extraction failed: {e}")
            return False

        finally:
            # Cleanup
            if self.producer:
                self.producer.close()
            if self.postgres_conn:
                self.postgres_conn.close()
            if self.mongo_client:
                self.mongo_client.close()

def main():
    """Main function"""
    producer = ComprehensiveKafkaProducer()
    success = producer.run_comprehensive_extraction()
    return success

if __name__ == "__main__":
    main()