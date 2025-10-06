#!/usr/bin/env python3
"""
Simple Kafka Consumer to verify Vietnam e-commerce data streaming
"""
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError

def main():
    print("Starting simple Kafka consumer for Vietnam e-commerce data...")

    try:
        # Initialize consumer
        consumer = KafkaConsumer(
            'vietnam_sales_events',
            'vietnam_customers',
            'vietnam_products',
            'vietnam_user_activities',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='vietnam_verification_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            consumer_timeout_ms=5000
        )

        print("Consumer initialized successfully")
        print("Topics:", consumer.subscription())
        print("Listening for messages... (Ctrl+C to stop)")

        message_count = 0
        topic_counts = {}

        # Consume messages
        for message in consumer:
            topic = message.topic
            data = message.value

            # Count messages by topic
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
            message_count += 1

            # Print sample every 10 messages
            if message_count % 10 == 0:
                print(f"\nMessage #{message_count}")
                print(f"Topic: {topic}")
                print(f"Counts: {topic_counts}")

                # Show sample data
                if data:
                    if topic == 'vietnam_sales_events':
                        print(f"Sale: Order #{data.get('order_id')} - {data.get('total_amount')} VND")
                    elif topic == 'vietnam_customers':
                        print(f"Customer: {data.get('customer_name')} from {data.get('province')}")
                    elif topic == 'vietnam_products':
                        print(f"Product: {data.get('product_name')} - {data.get('price')} VND")
                    elif topic == 'vietnam_user_activities':
                        print(f"Activity: {data.get('activity_type')} on {data.get('product_name')}")

                print("-" * 50)

    except KeyboardInterrupt:
        print(f"\nStopped. Total messages consumed: {message_count}")
        print(f"Final counts: {topic_counts}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()