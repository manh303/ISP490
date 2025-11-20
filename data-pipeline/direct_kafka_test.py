#!/usr/bin/env python3
"""
Direct test to see Kafka data flow from container perspective
"""
import json
import socket
from kafka import KafkaConsumer, KafkaAdminClient

def test_kafka_network():
    """Test network connectivity to Kafka"""
    print("🔍 Testing Kafka network connectivity...")

    # Test socket connection
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('kafka', 9092))
        sock.close()

        if result == 0:
            print("✅ Can connect to kafka:9092")
        else:
            print("❌ Cannot connect to kafka:9092")
            return False
    except Exception as e:
        print(f"❌ Network test failed: {e}")
        return False

    # Test admin client
    try:
        admin = KafkaAdminClient(bootstrap_servers=['kafka:9092'])
        metadata = admin.describe_cluster()
        print(f"✅ Kafka cluster nodes: {len(metadata.brokers)}")

        # List topics
        topics = admin.list_topics()
        vietnam_topics = [t for t in topics if 'vietnam' in t]
        print(f"✅ Vietnam topics: {vietnam_topics}")

        admin.close()
        return True

    except Exception as e:
        print(f"❌ Admin client failed: {e}")
        return False

def test_consumer_with_earliest():
    """Test consumer from earliest offset"""
    print("🔍 Testing consumer from earliest offset...")

    try:
        consumer = KafkaConsumer(
            'vietnam_sales_events',
            bootstrap_servers=['kafka:9092'],
            group_id='direct_test_consumer',
            auto_offset_reset='earliest',  # Start from beginning
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            consumer_timeout_ms=15000  # 15 second timeout
        )

        print("✅ Consumer created, checking for messages...")

        message_count = 0
        for message in consumer:
            message_count += 1
            print(f"📨 Message {message_count}: {message.value}")

            if message_count >= 3:
                break

        consumer.close()
        print(f"✅ Received {message_count} messages from earliest offset")
        return message_count > 0

    except Exception as e:
        print(f"❌ Consumer test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Direct Kafka connectivity test from container...")

    if test_kafka_network():
        test_consumer_with_earliest()
    else:
        print("❌ Network connectivity failed")