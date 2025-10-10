#!/usr/bin/env python3
"""
Verify Streaming Data in PostgreSQL
===================================
Check if Kafka streaming data is flowing correctly to PostgreSQL
"""

import psycopg2
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_streaming_data():
    """Verify streaming data in PostgreSQL"""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="ecommerce_dss",
            user="dss_user",
            password="dss_password_123"
        )
        cursor = conn.cursor()

        logger.info("✅ Connected to PostgreSQL")

        # Check streaming tables
        tables_to_check = [
            'realtime_kafka_stream',
            'products_realtime_updates',
            'sales_events_realtime',
            'orders_realtime_stream'
        ]

        logger.info("📊 STREAMING DATA VERIFICATION:")
        logger.info("=" * 50)

        total_streaming_records = 0

        for table in tables_to_check:
            try:
                # Get count
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                total_streaming_records += count

                # Get latest record timestamp
                cursor.execute(f"""
                    SELECT COALESCE(MAX(processed_at), MAX(received_at), MAX(last_updated)) as latest
                    FROM {table}
                """)
                latest = cursor.fetchone()[0]

                logger.info(f"📈 {table}: {count:,} records")
                if latest:
                    logger.info(f"   Latest: {latest}")
                else:
                    logger.info(f"   Latest: No data yet")

                # Show sample data if available
                if count > 0:
                    cursor.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT 2")
                    samples = cursor.fetchall()
                    logger.info(f"   Recent samples: {len(samples)} records")
                    for i, sample in enumerate(samples, 1):
                        logger.info(f"     Sample {i}: {str(sample)[:100]}...")

            except Exception as e:
                logger.error(f"❌ Failed to check table {table}: {e}")

        logger.info("=" * 50)
        logger.info(f"🎯 Total streaming records: {total_streaming_records:,}")

        # Compare with main tables
        logger.info("\n📊 COMPARISON WITH MAIN TABLES:")
        logger.info("=" * 50)

        main_tables = [
            ('vietnam_electronics_products', 'products'),
            ('customer_data', 'customers'),
            ('order_data', 'orders')
        ]

        for table, description in main_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"📈 {description}: {count:,} records")
            except Exception as e:
                logger.info(f"❌ {description}: Table not found or error")

        # Check recent streaming activity
        logger.info("\n⚡ RECENT STREAMING ACTIVITY (last 10 minutes):")
        logger.info("=" * 50)

        try:
            cursor.execute("""
                SELECT topic, message_type, COUNT(*) as count,
                       MAX(processed_at) as latest
                FROM realtime_kafka_stream
                WHERE processed_at >= NOW() - INTERVAL '10 minutes'
                GROUP BY topic, message_type
                ORDER BY count DESC
            """)

            recent_activity = cursor.fetchall()
            if recent_activity:
                for topic, msg_type, count, latest in recent_activity:
                    logger.info(f"   {topic} ({msg_type}): {count} messages (latest: {latest})")
            else:
                logger.info("   No recent activity in last 10 minutes")

        except Exception as e:
            logger.error(f"❌ Failed to check recent activity: {e}")

        # Check data quality
        logger.info("\n🔍 DATA QUALITY CHECK:")
        logger.info("=" * 50)

        try:
            # Check for valid product IDs
            cursor.execute("""
                SELECT COUNT(*) FROM realtime_kafka_stream
                WHERE product_id IS NOT NULL AND product_id != 'N/A'
            """)
            valid_products = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM realtime_kafka_stream")
            total_messages = cursor.fetchone()[0]

            if total_messages > 0:
                quality_pct = (valid_products / total_messages) * 100
                logger.info(f"   Product ID quality: {valid_products}/{total_messages} ({quality_pct:.1f}%)")
            else:
                logger.info("   No messages to analyze")

        except Exception as e:
            logger.error(f"❌ Failed to check data quality: {e}")

        conn.close()
        logger.info("\n✅ Verification completed!")

        if total_streaming_records > 0:
            logger.info("🎉 SUCCESS: Kafka streaming pipeline is working!")
        else:
            logger.info("⚠️ WARNING: No streaming data found - pipeline may not be active")

    except Exception as e:
        logger.error(f"❌ Failed to verify streaming data: {e}")

if __name__ == "__main__":
    verify_streaming_data()