#!/usr/bin/env python3
"""
Big Data Generator for Vietnamese E-commerce DSS
Generate millions of records for true Big Data processing
"""

import json
import time
import logging
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
import threading
from concurrent.futures import ThreadPoolExecutor
import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BigDataGenerator:
    def __init__(self):
        # Kafka configuration
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'value_serializer': lambda v: json.dumps(v, default=self.json_serializer).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 1,  # Faster for bulk data
            'retries': 1,
            'batch_size': 65536,  # Larger batches
            'linger_ms': 50,     # Wait for batching
            'compression_type': 'gzip',  # Compress for efficiency
            'buffer_memory': 134217728   # 128MB buffer
        }

        # Database config
        self.postgres_config = {
            'host': '127.0.0.1',
            'port': 5432,
            'database': 'ecommerce_dss',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        # Kafka topics
        self.topics = {
            'orders': 'ecommerce.orders.stream',
            'customers': 'ecommerce.customers.stream',
            'products': 'ecommerce.products.stream'
        }

        # Vietnamese e-commerce data templates
        self.vietnamese_cities = [
            'Ho Chi Minh', 'Hanoi', 'Da Nang', 'Hai Phong', 'Can Tho',
            'Bien Hoa', 'Hue', 'Nha Trang', 'Buon Ma Thuot', 'Quy Nhon',
            'Vung Tau', 'Nam Dinh', 'Phan Thiet', 'Long Xuyen', 'Ha Long',
            'Thai Nguyen', 'Thanh Hoa', 'Rach Gia', 'Cam Ranh', 'Vinh'
        ]

        self.districts = [
            'District 1', 'District 2', 'District 3', 'District 4', 'District 5',
            'District 7', 'District 10', 'Ba Dinh', 'Hai Ba Trung', 'Hoan Kiem',
            'Thanh Xuan', 'Cau Giay', 'Dong Da', 'Tay Ho', 'Long Bien'
        ]

        self.payment_methods = ['vnpay', 'momo', 'zalopay', 'banking', 'cod', 'shopeepay', 'airpay']
        self.platforms = ['mobile', 'web', 'app', 'social', 'marketplace']
        self.statuses = ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'completed', 'cancelled']

        self.vietnamese_products = [
            {'name': 'iPhone 15 Pro Max', 'category': 'Electronics', 'price_range': (25000000, 35000000)},
            {'name': 'Samsung Galaxy S24', 'category': 'Electronics', 'price_range': (15000000, 25000000)},
            {'name': 'MacBook Air M3', 'category': 'Electronics', 'price_range': (25000000, 35000000)},
            {'name': 'Dell XPS 13', 'category': 'Electronics', 'price_range': (20000000, 30000000)},
            {'name': 'AirPods Pro Gen 2', 'category': 'Electronics', 'price_range': (5000000, 8000000)},
            {'name': 'iPad Pro M4', 'category': 'Electronics', 'price_range': (15000000, 25000000)},
            {'name': 'Áo Polo Nam Cao Cấp', 'category': 'Fashion', 'price_range': (300000, 800000)},
            {'name': 'Giày Sneaker Nike', 'category': 'Fashion', 'price_range': (1500000, 4000000)},
            {'name': 'Balo Laptop Cao Cấp', 'category': 'Accessories', 'price_range': (500000, 1500000)},
            {'name': 'Đồng Hồ Thông Minh', 'category': 'Electronics', 'price_range': (2000000, 8000000)},
            {'name': 'Cà Phê Arabica Premium', 'category': 'Food', 'price_range': (150000, 500000)},
            {'name': 'Trà Ô Long Đặc Biệt', 'category': 'Food', 'price_range': (100000, 300000)},
            {'name': 'Nước Mắm Phú Quốc', 'category': 'Food', 'price_range': (80000, 200000)},
            {'name': 'Bánh Tráng Tây Ninh', 'category': 'Food', 'price_range': (30000, 80000)},
            {'name': 'Áo Dài Truyền Thống', 'category': 'Fashion', 'price_range': (800000, 3000000)}
        ]

        self.customer_names = [
            'Nguyễn Văn An', 'Trần Thị Bình', 'Lê Văn Cường', 'Phạm Thị Dung',
            'Hoàng Văn Em', 'Võ Thị Phương', 'Đặng Văn Giang', 'Bùi Thị Hoa',
            'Đinh Văn Khang', 'Lý Thị Lan', 'Vũ Văn Minh', 'Đỗ Thị Nga',
            'Tô Văn Phong', 'Ngô Thị Quỳnh', 'Phan Văn Sơn', 'Mai Thị Tâm'
        ]

        self.producer = None

    def json_serializer(self, obj):
        """Custom JSON serializer for datetime objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def init_kafka_producer(self):
        """Initialize Kafka producer for bulk data"""
        try:
            self.producer = KafkaProducer(**self.kafka_config)
            logger.info("✅ Big Data Kafka producer initialized")
            return True
        except Exception as e:
            logger.error(f"❌ Kafka producer failed: {e}")
            return False

    def generate_bulk_orders(self, count=100000):
        """Generate bulk orders for big data"""
        logger.info(f"🚀 Generating {count:,} bulk orders...")

        orders = []
        base_timestamp = datetime.now() - timedelta(days=365)  # Last year data

        for i in range(count):
            # Random time within last year
            random_days = random.randint(0, 365)
            random_hours = random.randint(0, 23)
            random_minutes = random.randint(0, 59)
            order_time = base_timestamp + timedelta(days=random_days, hours=random_hours, minutes=random_minutes)

            # Select random product
            product = random.choice(self.vietnamese_products)
            city = random.choice(self.vietnamese_cities)

            # Generate realistic Vietnamese order
            order = {
                'order_id': f'BIG_ORD_{int(time.time())}_{i:06d}',
                'customer_id': f'BIG_CUST_{random.randint(100000, 999999)}',
                'product_id': f'BIG_PROD_{random.randint(10000, 99999)}',
                'product_name': product['name'],
                'category': product['category'],
                'quantity': random.randint(1, 5),
                'unit_price': random.randint(*product['price_range']),
                'total_amount': 0,  # Will calculate
                'order_date': order_time,
                'payment_method': random.choice(self.payment_methods),
                'platform': random.choice(self.platforms),
                'status': random.choice(self.statuses),
                'shipping_address': {
                    'city': city,
                    'district': random.choice(self.districts),
                    'street': f'{random.randint(1, 999)} {random.choice(["Nguyen Trai", "Le Lai", "Tran Hung Dao", "Hai Ba Trung", "Ly Tu Trong"])}'
                },
                'customer_info': {
                    'name': random.choice(self.customer_names),
                    'age_group': random.choice(['18-25', '26-35', '36-45', '46-55', '55+']),
                    'gender': random.choice(['male', 'female']),
                    'membership': random.choice(['standard', 'silver', 'gold', 'platinum']),
                    'email': f'customer{i}@email.com',
                    'phone': f'09{random.randint(10000000, 99999999)}'
                },
                'source': 'big_data_generator',
                'data_type': 'bulk_historical_order',
                'metadata': {
                    'batch_id': f'BATCH_{int(time.time())}',
                    'generated_at': datetime.now(),
                    'generator_version': '1.0'
                }
            }

            # Calculate total amount
            order['total_amount'] = order['unit_price'] * order['quantity']

            orders.append(order)

            # Log progress every 10k records
            if (i + 1) % 10000 == 0:
                logger.info(f"   Generated {i+1:,} orders...")

        logger.info(f"✅ Generated {count:,} orders for big data processing")
        return orders

    def send_bulk_to_kafka(self, orders, batch_size=1000):
        """Send bulk orders to Kafka in batches"""
        logger.info(f"📤 Sending {len(orders):,} orders to Kafka in batches of {batch_size:,}...")

        sent_count = 0
        failed_count = 0

        for i in range(0, len(orders), batch_size):
            batch = orders[i:i + batch_size]

            for order in batch:
                try:
                    # Add Kafka metadata
                    kafka_message = {
                        'data': order,
                        'metadata': {
                            'produced_at': datetime.now(),
                            'producer_id': 'big_data_generator',
                            'topic': self.topics['orders'],
                            'message_id': str(uuid.uuid4()),
                            'batch_sequence': i // batch_size
                        }
                    }

                    # Send without waiting for response for speed
                    self.producer.send(self.topics['orders'], value=kafka_message, key=order['order_id'])
                    sent_count += 1

                except Exception as e:
                    failed_count += 1
                    if failed_count < 10:  # Only log first few errors
                        logger.error(f"Failed to send order {order['order_id']}: {e}")

            # Flush every batch and log progress
            self.producer.flush()
            logger.info(f"   📤 Sent batch {(i // batch_size) + 1}, total sent: {sent_count:,}")

        logger.info(f"✅ Bulk send completed: {sent_count:,} sent, {failed_count:,} failed")
        return sent_count

    def generate_big_customers(self, count=50000):
        """Generate big customer dataset"""
        logger.info(f"👥 Generating {count:,} customers...")

        customers = []
        for i in range(count):
            customer = {
                'customer_id': f'BIG_CUST_{i:06d}',
                'name': random.choice(self.customer_names),
                'email': f'customer{i}@email.com',
                'phone': f'09{random.randint(10000000, 99999999)}',
                'address': f'{random.randint(1, 999)} Street Name',
                'city': random.choice(self.vietnamese_cities),
                'district': random.choice(self.districts),
                'registration_date': datetime.now() - timedelta(days=random.randint(0, 1000)),
                'age_group': random.choice(['18-25', '26-35', '36-45', '46-55', '55+']),
                'gender': random.choice(['male', 'female']),
                'membership': random.choice(['standard', 'silver', 'gold', 'platinum']),
                'customer_segment': random.choice(['Basic', 'Standard', 'Premium', 'VIP']),
                'total_spent': random.randint(0, 100000000),  # VND
                'source': 'big_data_generator',
                'data_type': 'bulk_customer'
            }
            customers.append(customer)

            if (i + 1) % 10000 == 0:
                logger.info(f"   Generated {i+1:,} customers...")

        return customers

    def send_customers_to_kafka(self, customers, batch_size=1000):
        """Send customers to Kafka"""
        logger.info(f"📤 Sending {len(customers):,} customers to Kafka...")

        sent_count = 0
        for i, customer in enumerate(customers):
            try:
                kafka_message = {
                    'data': customer,
                    'metadata': {
                        'produced_at': datetime.now(),
                        'producer_id': 'big_data_generator',
                        'topic': self.topics['customers'],
                        'message_id': str(uuid.uuid4())
                    }
                }

                self.producer.send(self.topics['customers'], value=kafka_message, key=customer['customer_id'])
                sent_count += 1

                if (i + 1) % batch_size == 0:
                    self.producer.flush()
                    logger.info(f"   📤 Sent {i+1:,} customers...")

            except Exception as e:
                logger.error(f"Failed to send customer {customer['customer_id']}: {e}")

        self.producer.flush()
        logger.info(f"✅ Sent {sent_count:,} customers to Kafka")
        return sent_count

    def run_big_data_generation(self, orders_count=100000, customers_count=50000):
        """Run complete big data generation"""
        logger.info("🚀 STARTING BIG DATA GENERATION FOR VIETNAMESE E-COMMERCE DSS")
        logger.info("=" * 80)
        logger.info(f"Target: {orders_count:,} orders + {customers_count:,} customers")

        start_time = datetime.now()

        try:
            # Initialize Kafka producer
            if not self.init_kafka_producer():
                return False

            # Phase 1: Generate and send orders
            logger.info(f"📊 PHASE 1: Generating {orders_count:,} Orders")
            orders = self.generate_bulk_orders(orders_count)
            orders_sent = self.send_bulk_to_kafka(orders, batch_size=2000)

            # Phase 2: Generate and send customers
            logger.info(f"👥 PHASE 2: Generating {customers_count:,} Customers")
            customers = self.generate_big_customers(customers_count)
            customers_sent = self.send_customers_to_kafka(customers, batch_size=2000)

            # Final flush
            self.producer.flush()

            # Summary
            end_time = datetime.now()
            duration = end_time - start_time
            total_records = orders_sent + customers_sent

            logger.info("=" * 80)
            logger.info("🎯 BIG DATA GENERATION SUMMARY")
            logger.info("=" * 80)
            logger.info(f"⏱️  Total Duration: {duration}")
            logger.info(f"📊 Orders Generated: {orders_sent:,}")
            logger.info(f"👥 Customers Generated: {customers_sent:,}")
            logger.info(f"🎯 TOTAL RECORDS: {total_records:,}")
            logger.info(f"🚀 Throughput: {total_records / duration.total_seconds():.0f} records/second")
            logger.info(f"🇻🇳 Vietnamese E-commerce Data: Ready for Big Data processing")

            logger.info("\n🎉 BIG DATA GENERATION SUCCESSFUL!")
            logger.info("✅ Kafka topics populated with massive Vietnamese e-commerce dataset")
            logger.info("💫 Ready for Spark Big Data processing!")

            return True

        except Exception as e:
            logger.error(f"❌ Big data generation failed: {e}")
            return False

        finally:
            if self.producer:
                self.producer.close()

def main():
    """Main function"""
    generator = BigDataGenerator()

    # Generate Big Data - adjust numbers based on your system capacity
    # Start with smaller numbers and scale up
    orders_count = 100000   # 100K orders
    customers_count = 50000  # 50K customers

    logger.info(f"Starting Big Data generation: {orders_count:,} orders + {customers_count:,} customers")

    success = generator.run_big_data_generation(orders_count, customers_count)

    if success:
        logger.info("\n✅ Big Data generation completed successfully!")
        logger.info("🎯 Your Vietnamese e-commerce DSS now has sufficient data for Big Data processing!")
    else:
        logger.error("\n❌ Big Data generation failed!")

    return success

if __name__ == "__main__":
    main()