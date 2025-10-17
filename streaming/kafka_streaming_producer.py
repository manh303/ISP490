#!/usr/bin/env python3
"""
Kafka Streaming Producer để sinh dữ liệu test cho pipeline
Chạy trong Docker container với proper network access
"""
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging
import signal
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StreamingProducer:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],  # Local connection từ trong kafka container
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',
            'retries': 3,
            'batch_size': 16384,
            'linger_ms': 10,
            'buffer_memory': 33554432
        }

        self.producer = None
        self.order_counter = 2000  # Start từ 2000 để tránh conflict
        self.running = True

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal, stopping producer...")
        self.running = False

    def init_producer(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(**self.kafka_config)
            logger.info("✅ Kafka streaming producer initialized")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            return False

    def generate_vietnamese_order_data(self):
        """Generate realistic Vietnamese e-commerce order data"""
        self.order_counter += 1

        # Vietnamese data
        cities = [
            'Ho Chi Minh', 'Hanoi', 'Da Nang', 'Hai Phong', 'Can Tho',
            'Bien Hoa', 'Hue', 'Nha Trang', 'Buon Ma Thuot', 'Quy Nhon'
        ]

        districts = [
            'District 1', 'District 3', 'District 7', 'District 10', 'Ba Dinh',
            'Hai Ba Trung', 'Thanh Xuan', 'Hai Chau', 'Ngu Hanh Son', 'Thanh Khe'
        ]

        payment_methods = ['vnpay', 'momo', 'zalopay', 'banking', 'cod']
        platforms = ['mobile', 'web', 'app']
        statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'completed']

        products = [
            'iPhone 15 Pro Max', 'Samsung Galaxy S24', 'MacBook Air M3',
            'Dell XPS 13', 'AirPods Pro', 'Sony WH-1000XM5', 'iPad Pro',
            'Nintendo Switch', 'Gaming Chair', 'Mechanical Keyboard'
        ]

        city = random.choice(cities)
        order_data = {
            'order_id': f'ORD_STREAM_{self.order_counter}_{int(time.time())}',
            'customer_id': f'CUST_{random.randint(10000, 99999)}',
            'product_id': f'PROD_{random.randint(1000, 9999)}',
            'product_name': random.choice(products),
            'quantity': random.randint(1, 5),
            'total_amount': round(random.uniform(50000, 5000000), 2),  # VND prices
            'order_date': datetime.now().isoformat(),
            'payment_method': random.choice(payment_methods),
            'platform': random.choice(platforms),
            'status': random.choice(statuses),
            'shipping_address': {
                'city': city,
                'district': random.choice(districts),
                'street': f'{random.randint(1, 999)} {random.choice(["Nguyen Trai", "Le Lai", "Tran Hung Dao", "Vo Van Tan"])}'
            },
            'customer_info': {
                'age_group': random.choice(['18-25', '26-35', '36-45', '46-55', '55+']),
                'gender': random.choice(['male', 'female']),
                'membership': random.choice(['standard', 'silver', 'gold', 'platinum'])
            },
            'created_at': datetime.now().isoformat(),
            'source': 'streaming_producer',
            'batch_id': f'streaming_batch_{int(time.time())}',
            'version': '2.0'
        }

        return order_data

    def send_streaming_orders(self, num_orders=5, interval=2):
        """Send streaming orders continuously"""
        if not self.producer:
            logger.error("❌ Producer not initialized")
            return False

        topic = 'ecommerce.orders.stream'
        sent_count = 0

        logger.info(f"🚀 Starting to send {num_orders} streaming orders every {interval}s to topic: {topic}")

        try:
            for i in range(num_orders):
                if not self.running:
                    break

                order_data = self.generate_vietnamese_order_data()

                # Send to Kafka
                future = self.producer.send(
                    topic,
                    value=order_data,
                    key=order_data['order_id']
                )

                # Wait for message to be sent
                record_metadata = future.get(timeout=10)
                sent_count += 1

                logger.info(f"📦 Sent order {i+1}/{num_orders}: {order_data['order_id']} "
                           f"(Amount: {order_data['total_amount']:,.0f} VND, "
                           f"City: {order_data['shipping_address']['city']}, "
                           f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset})")

                # Wait before sending next order
                if i < num_orders - 1:  # Don't wait after last order
                    time.sleep(interval)

        except Exception as e:
            logger.error(f"❌ Failed to send streaming orders: {e}")

        # Flush all messages
        self.producer.flush()
        logger.info(f"✅ Successfully sent {sent_count}/{num_orders} streaming orders")

        return sent_count > 0

    def run_continuous_streaming(self, orders_per_batch=5, batch_interval=10, max_batches=3):
        """Run continuous streaming for testing"""
        logger.info(f"🔄 Starting continuous streaming: {orders_per_batch} orders every {batch_interval}s, {max_batches} batches")

        # Set up signal handlers
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

        batch_count = 0

        try:
            while self.running and batch_count < max_batches:
                batch_count += 1
                logger.info(f"📊 Starting batch {batch_count}/{max_batches}")

                self.send_streaming_orders(num_orders=orders_per_batch, interval=1)

                if batch_count < max_batches:
                    logger.info(f"⏰ Waiting {batch_interval}s before next batch...")
                    time.sleep(batch_interval)

            logger.info(f"🏁 Completed {batch_count} batches of streaming data")

        except KeyboardInterrupt:
            logger.info("⏹️ Streaming stopped by user")
        finally:
            self.close()

    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.close()
            logger.info("🔌 Kafka producer closed")

def main():
    """Main function"""
    producer = StreamingProducer()

    try:
        if not producer.init_producer():
            logger.error("❌ Failed to initialize producer")
            return

        # Run continuous streaming
        producer.run_continuous_streaming(
            orders_per_batch=3,    # 3 orders per batch
            batch_interval=15,     # Wait 15s between batches
            max_batches=2          # Send 2 batches total
        )

        logger.info("✅ Streaming producer completed successfully!")

    except Exception as e:
        logger.error(f"❌ Producer error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()