#!/usr/bin/env python3
"""
Test Kafka Consumer to verify data flow
"""
import json
import time
from kafka import KafkaConsumer

def test_kafka_connection():
    """Test basic Kafka connectivity and message consumption"""
    print("🔍 Testing Kafka connection...")

    try:
        consumer = KafkaConsumer(
            'vietnam_sales_events',
            bootstrap_servers=['kafka:9092'],
            group_id='test_consumer',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            consumer_timeout_ms=10000  # 10 second timeout
        )

        print("✅ Connected to Kafka successfully")
        print("📡 Listening for messages (10 second timeout)...")

        message_count = 0
        for message in consumer:
            message_count += 1
            print(f"📨 Message {message_count}:")
            print(f"   Topic: {message.topic}")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            print(f"   Data: {message.value}")
            print()

            if message_count >= 3:  # Stop after 3 messages
                break

        consumer.close()
        print(f"✅ Test completed. Received {message_count} messages")
        return True

    except Exception as e:
        print(f"❌ Kafka test failed: {e}")
        return False

if __name__ == "__main__":
    test_kafka_connection()