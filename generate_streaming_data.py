#!/usr/bin/env python3
"""
Generate Real-time Streaming Data
=================================
Generate and send real-time data to Kafka topics
"""

import json
import random
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
import uuid

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StreamingDataGenerator:
    """Generate real-time streaming data for Kafka"""

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Sample data for generation
        self.brands = ['Samsung', 'Apple', 'Huawei', 'Xiaomi', 'Nokia', 'VinSmart', 'BKAV']
        self.categories = ['Điện tử', 'Thời trang', 'Sách', 'Đồ gia dụng', 'Thể thao']
        self.platforms = ['Shopee', 'Tiki', 'Lazada', 'Sendo', 'FPT Shop']
        self.payment_methods = ['COD', 'Banking', 'MoMo', 'ZaloPay', 'VNPay']
        self.vietnam_cities = ['Hà Nội', 'TP.HCM', 'Đà Nẵng', 'Hải Phòng', 'Cần Thơ']

        logger.info("✅ Streaming data generator initialized")

    def generate_vietnam_product(self):
        """Generate a Vietnam product update"""
        product_id = f"VN_PROD_{uuid.uuid4().hex[:8].upper()}"

        return {
            "product_id": product_id,
            "product_name_vn": f"{random.choice(['Điện thoại', 'Laptop', 'Tai nghe', 'Túi xách'])} {random.choice(self.brands)} {random.randint(1, 100)}",
            "product_name_en": f"{random.choice(['Phone', 'Laptop', 'Headphone', 'Bag'])} {random.choice(self.brands)} {random.randint(1, 100)}",
            "brand": random.choice(self.brands),
            "category_l1": random.choice(self.categories),
            "category_l2": random.choice(['Phổ thông', 'Cao cấp', 'Giá rẻ']),
            "price_vnd": random.randint(100000, 50000000),
            "price_usd": round(random.uniform(5, 2000), 2),
            "discount_percent": round(random.uniform(0, 50), 2),
            "rating": round(random.uniform(1, 5), 2),
            "review_count": random.randint(0, 5000),
            "stock_quantity": random.randint(0, 1000),
            "is_featured": random.choice([True, False]),
            "available_platforms": random.sample(self.platforms, random.randint(1, 3)),
            "payment_methods": random.sample(self.payment_methods, random.randint(1, 4)),
            "vietnam_popularity": round(random.uniform(0, 1), 3),
            "made_in_vietnam": random.choice([True, False]),
            "launch_date": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
            "created_at": datetime.now().isoformat()
        }

    def generate_sales_event(self):
        """Generate a sales event"""
        event_id = f"SALE_{uuid.uuid4().hex[:8].upper()}"
        customer_id = f"CUST_{random.randint(10000, 99999)}"
        product_id = f"VN_PROD_{uuid.uuid4().hex[:8].upper()}"

        return {
            "event_id": event_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "event_type": random.choice(['view', 'add_to_cart', 'purchase', 'review']),
            "event_value": round(random.uniform(10, 5000), 2),
            "timestamp": datetime.now().isoformat(),
            "platform": random.choice(self.platforms),
            "location": random.choice(self.vietnam_cities),
            "session_id": f"sess_{uuid.uuid4().hex[:12]}",
            "device_type": random.choice(['mobile', 'desktop', 'tablet'])
        }

    def generate_order_stream(self):
        """Generate an order stream event"""
        order_id = f"ORD_{datetime.now().strftime('%Y%m%d')}_{random.randint(10000, 99999)}"
        customer_id = f"CUST_{random.randint(10000, 99999)}"
        product_id = f"VN_PROD_{uuid.uuid4().hex[:8].upper()}"

        return {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": random.randint(1, 5),
            "total_amount": round(random.uniform(50, 5000), 2),
            "order_date": datetime.now().isoformat(),
            "platform": random.choice(self.platforms),
            "payment_method": random.choice(self.payment_methods),
            "status": random.choice(['new', 'processing', 'shipped', 'delivered']),
            "shipping_address": {
                "city": random.choice(self.vietnam_cities),
                "district": f"Quận {random.randint(1, 12)}"
            }
        }

    def send_to_kafka(self, topic, data):
        """Send data to Kafka topic"""
        try:
            future = self.producer.send(topic, data)
            future.get(timeout=10)  # Wait for confirmation
            logger.info(f"✅ Sent to {topic}: {data.get('product_id', data.get('event_id', data.get('order_id')))}")
        except Exception as e:
            logger.error(f"❌ Failed to send to {topic}: {e}")

    def start_streaming(self, duration_minutes=5, messages_per_minute=10):
        """Start generating and streaming data"""
        logger.info(f"🚀 Starting data streaming for {duration_minutes} minutes")
        logger.info(f"📊 Generating {messages_per_minute} messages per minute")

        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        message_interval = 60 / messages_per_minute  # seconds between messages

        message_count = 0

        try:
            while time.time() < end_time:
                # Generate different types of data
                message_type = random.choice(['product', 'sales', 'order'])

                if message_type == 'product':
                    data = self.generate_vietnam_product()
                    self.send_to_kafka('vietnam_products', data)
                elif message_type == 'sales':
                    data = self.generate_sales_event()
                    self.send_to_kafka('vietnam_sales_events', data)
                else:  # order
                    data = self.generate_order_stream()
                    self.send_to_kafka('ecommerce.orders.stream', data)

                message_count += 1

                # Log progress every 50 messages
                if message_count % 50 == 0:
                    elapsed = (time.time() - start_time) / 60
                    logger.info(f"📈 Progress: {message_count} messages sent in {elapsed:.1f} minutes")

                # Wait before next message
                time.sleep(message_interval)

        except KeyboardInterrupt:
            logger.info("⚠️ Streaming interrupted by user")
        except Exception as e:
            logger.error(f"❌ Streaming error: {e}")
        finally:
            self.producer.close()
            elapsed = (time.time() - start_time) / 60
            logger.info(f"🎉 Streaming completed: {message_count} messages sent in {elapsed:.1f} minutes")

if __name__ == "__main__":
    generator = StreamingDataGenerator()

    # Generate data for 3 minutes, 20 messages per minute
    generator.start_streaming(duration_minutes=3, messages_per_minute=20)