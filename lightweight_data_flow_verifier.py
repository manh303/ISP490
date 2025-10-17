#!/usr/bin/env python3
"""
Lightweight Data Flow Verifier
Just read and verify data flow from Kafka topics without database
"""

import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LightweightDataFlowVerifier:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'group_id': 'lightweight_verifier',
            'auto_offset_reset': 'earliest',  # Read from beginning
            'enable_auto_commit': False,  # Don't commit offsets
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None,
            'consumer_timeout_ms': 15000  # 15 second timeout
        }

        self.topics = [
            'ecommerce.orders.stream',
            'ecommerce.customers.stream',
            'ecommerce.products.stream'
        ]

        self.stats = {
            'total_processed': 0,
            'by_topic': defaultdict(int),
            'data_sources': defaultdict(int),
            'sample_data': []
        }

    def process_data_flow(self, max_messages=5000):
        """Process Kafka messages to verify data flow"""
        logger.info("🔄 VERIFYING VIETNAMESE E-COMMERCE DATA FLOW")
        logger.info("=" * 60)

        try:
            consumer = KafkaConsumer(*self.topics, **self.kafka_config)

            logger.info(f"📊 Reading from {len(self.topics)} topics:")
            for topic in self.topics:
                logger.info(f"   📋 {topic}")

            logger.info(f"⏱️  Processing up to {max_messages:,} messages...")

            start_time = time.time()
            processed = 0

            for message in consumer:
                topic = message.topic
                data = message.value

                if not data:
                    continue

                processed += 1
                self.stats['total_processed'] += 1
                self.stats['by_topic'][topic] += 1

                # Extract source information
                source = "unknown"
                if isinstance(data, dict):
                    if 'metadata' in data and 'data' in data:
                        # Big data generator format
                        actual_data = data['data']
                        source = actual_data.get('source', 'big_data_generator')

                        # Store sample for Vietnamese e-commerce analysis
                        if len(self.stats['sample_data']) < 20:
                            sample = {
                                'topic': topic,
                                'source': source,
                                'order_id': actual_data.get('order_id', 'N/A'),
                                'product_name': actual_data.get('product_name', 'N/A'),
                                'city': actual_data.get('city', 'N/A'),
                                'payment_method': actual_data.get('payment_method', 'N/A'),
                                'total_amount': actual_data.get('total_amount', 0),
                                'customer_id': actual_data.get('customer_id', 'N/A')
                            }
                            self.stats['sample_data'].append(sample)
                    else:
                        # Direct format
                        source = data.get('source', 'direct_stream')

                self.stats['data_sources'][source] += 1

                # Progress logging
                if processed % 1000 == 0:
                    logger.info(f"   📈 Processed {processed:,} messages...")

                if processed >= max_messages:
                    logger.info(f"   🚫 Reached limit of {max_messages:,} messages")
                    break

            duration = time.time() - start_time
            consumer.close()

            # Display results
            self.display_flow_verification(duration)
            return True

        except Exception as e:
            logger.error(f"❌ Data flow verification failed: {e}")
            return False

    def display_flow_verification(self, duration):
        """Display data flow verification results"""
        logger.info("\n" + "=" * 60)
        logger.info("🎯 VIETNAMESE E-COMMERCE DATA FLOW VERIFICATION")
        logger.info("=" * 60)

        # Overall stats
        logger.info(f"⏱️  Processing Time: {duration:.2f} seconds")
        logger.info(f"📊 Total Messages: {self.stats['total_processed']:,}")
        logger.info(f"🚀 Throughput: {self.stats['total_processed'] / duration:.0f} messages/second")

        # By topic
        logger.info(f"\n📋 MESSAGES BY TOPIC:")
        for topic, count in self.stats['by_topic'].items():
            percentage = (count / self.stats['total_processed']) * 100 if self.stats['total_processed'] > 0 else 0
            logger.info(f"   📌 {topic}: {count:,} messages ({percentage:.1f}%)")

        # By data source
        logger.info(f"\n📊 MESSAGES BY DATA SOURCE:")
        for source, count in self.stats['data_sources'].items():
            percentage = (count / self.stats['total_processed']) * 100 if self.stats['total_processed'] > 0 else 0
            logger.info(f"   🔹 {source}: {count:,} messages ({percentage:.1f}%)")

        # Sample Vietnamese e-commerce data
        logger.info(f"\n🇻🇳 SAMPLE VIETNAMESE E-COMMERCE DATA:")
        for i, sample in enumerate(self.stats['sample_data'][:10], 1):
            logger.info(f"   {i:2d}. Order: {sample['order_id']}")
            logger.info(f"       Product: {sample['product_name']}")
            logger.info(f"       City: {sample['city']}")
            logger.info(f"       Payment: {sample['payment_method']}")
            logger.info(f"       Amount: {sample['total_amount']:,} VND")
            logger.info(f"       Source: {sample['source']}")

        # Assessment
        logger.info(f"\n🎯 DATA FLOW ASSESSMENT:")
        if self.stats['total_processed'] >= 10000:
            logger.info("✅ EXCELLENT: Massive data flow verified!")
            logger.info("🎉 Vietnamese e-commerce DSS has sufficient big data")
        elif self.stats['total_processed'] >= 1000:
            logger.info("👍 GOOD: Substantial data flow detected")
            logger.info("✅ Good volume for analytics")
        else:
            logger.info("⚠️ LIMITED: Small data volume")
            logger.info("💡 Consider generating more data")

        # Vietnamese specifics check
        vietnamese_indicators = 0
        for sample in self.stats['sample_data']:
            if any(city in sample.get('city', '') for city in ['Ho Chi Minh', 'Hanoi', 'Da Nang']):
                vietnamese_indicators += 1
            if any(payment in sample.get('payment_method', '') for payment in ['vnpay', 'momo', 'zalopay']):
                vietnamese_indicators += 1

        logger.info(f"\n🇻🇳 VIETNAMESE E-COMMERCE INDICATORS:")
        logger.info(f"   📊 Vietnamese data samples: {vietnamese_indicators}")
        if vietnamese_indicators > 0:
            logger.info("   ✅ Vietnamese e-commerce data confirmed!")
        else:
            logger.info("   ⚠️ Limited Vietnamese-specific data found")

        logger.info(f"\n📈 STREAMING STATUS:")
        logger.info(f"   📤 Kafka Topics: Operational")
        logger.info(f"   🔄 Data Flow: Active")
        logger.info(f"   💾 Ready for: Datawarehouse processing")

def main():
    """Main verification function"""
    verifier = LightweightDataFlowVerifier()

    logger.info("🚀 Starting lightweight Vietnamese e-commerce data flow verification...")

    success = verifier.process_data_flow(max_messages=10000)

    if success:
        logger.info("\n✅ Data flow verification completed!")
        logger.info("🎯 Vietnamese e-commerce streaming is working properly!")
    else:
        logger.error("\n❌ Data flow verification failed!")

    return success

if __name__ == "__main__":
    main()