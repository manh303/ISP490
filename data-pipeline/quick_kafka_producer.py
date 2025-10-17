#!/usr/bin/env python3
"""
Quick Kafka Producer để sinh dữ liệu test cho datawarehouse pipeline
"""
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QuickKafkaProducer:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',
            'retries': 3,
            'batch_size': 16384,
            'linger_ms': 10,
            'buffer_memory': 33554432
        }

        self.producer = None
        self.order_counter = 1000

    def init_producer(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(**self.kafka_config)
            logger.info("Kafka producer initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            return False

    def generate_order_data(self):
        """Generate realistic Vietnamese e-commerce order data"""
        self.order_counter += 1

        # Vietnamese cities and payment methods
        cities = ['Ho Chi Minh', 'Hanoi', 'Da Nang', 'Hai Phong', 'Can Tho', 'Bien Hoa', 'Hue', 'Nha Trang']
        districts = ['District 1', 'District 3', 'District 7', 'Ba Dinh', 'Hai Ba Trung', 'Thanh Xuan', 'Hai Chau', 'Ngu Hanh Son']
        payment_methods = ['vnpay', 'momo', 'zalopay', 'banking', 'cod']
        platforms = ['mobile', 'web', 'app']
        statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'completed']

        order_data = {
            'order_id': f'ORD_TEST_{self.order_counter}_{int(time.time())}',
            'customer_id': f'CUST_{random.randint(10000, 99999)}',
            'product_id': f'PROD_{random.randint(1000, 9999)}',
            'quantity': random.randint(1, 5),
            'total_amount': round(random.uniform(50000, 2000000), 2),  # VND prices
            'order_date': (datetime.now() - timedelta(minutes=random.randint(0, 1440))).isoformat(),
            'payment_method': random.choice(payment_methods),
            'platform': random.choice(platforms),
            'status': random.choice(statuses),
            'shipping_address': {
                'city': random.choice(cities),
                'district': random.choice(districts)
            },
            'created_at': datetime.now().isoformat(),
            'source': 'test_producer',
            'batch_id': f'batch_{int(time.time())}'
        }

        return order_data

    def send_orders_to_kafka(self, num_orders=10):
        """Send test orders to Kafka topic"""
        if not self.producer:
            logger.error("Producer not initialized")
            return False

        topic = 'ecommerce.orders.stream'
        sent_count = 0

        logger.info(f"Sending {num_orders} test orders to Kafka topic: {topic}")

        for i in range(num_orders):
            try:
                order_data = self.generate_order_data()

                # Send to Kafka
                future = self.producer.send(
                    topic,
                    value=order_data,
                    key=order_data['order_id']
                )

                # Wait for message to be sent
                record_metadata = future.get(timeout=10)
                sent_count += 1

                logger.info(f"Sent order {i+1}/{num_orders}: {order_data['order_id']} "
                           f"(Partition: {record_metadata.partition}, Offset: {record_metadata.offset})")

                # Small delay between messages
                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Failed to send order {i+1}: {e}")

        # Flush all messages
        self.producer.flush()
        logger.info(f"Successfully sent {sent_count}/{num_orders} orders to Kafka")

        return sent_count > 0

    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")

def main():
    """Main function to run the producer"""
    producer = QuickKafkaProducer()

    try:
        if not producer.init_producer():
            logger.error("Failed to initialize producer")
            return

        # Send test orders
        producer.send_orders_to_kafka(num_orders=20)

        logger.info("✅ Producer completed successfully!")

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Producer error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()