#!/usr/bin/env python3
"""
Test PostgreSQL Connection
Try different connection configurations
"""

import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_connections():
    """Test different PostgreSQL connection configurations"""

    configs = [
        {
            'name': 'postgres/postgres',
            'config': {
                'host': '127.0.0.1',
                'port': 5432,
                'database': 'ecommerce_dss',
                'user': 'postgres',
                'password': 'postgres'
            }
        },
        {
            'name': 'dss_user/dss_password_123',
            'config': {
                'host': '127.0.0.1',
                'port': 5432,
                'database': 'ecommerce_dss',
                'user': 'dss_user',
                'password': 'dss_password_123'
            }
        },
        {
            'name': 'postgres default database',
            'config': {
                'host': '127.0.0.1',
                'port': 5432,
                'database': 'postgres',
                'user': 'postgres',
                'password': 'postgres'
            }
        },
        {
            'name': 'localhost connection',
            'config': {
                'host': 'localhost',
                'port': 5432,
                'database': 'ecommerce_dss',
                'user': 'postgres',
                'password': ''
            }
        }
    ]

    for test_config in configs:
        logger.info(f"\n🔍 Testing: {test_config['name']}")
        try:
            conn = psycopg2.connect(**test_config['config'])
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"✅ SUCCESS: {version[0]}")

            # Check if database exists
            cursor.execute("SELECT current_database();")
            db_name = cursor.fetchone()
            logger.info(f"📊 Connected to database: {db_name[0]}")

            cursor.close()
            conn.close()
            return test_config['config']  # Return first working config

        except Exception as e:
            logger.error(f"❌ FAILED: {e}")

    return None

def create_database_if_needed(working_config):
    """Create database if it doesn't exist"""
    try:
        # Connect to postgres database to create ecommerce_dss
        postgres_config = working_config.copy()
        postgres_config['database'] = 'postgres'

        conn = psycopg2.connect(**postgres_config)
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname='ecommerce_dss'")
        exists = cursor.fetchone()

        if not exists:
            logger.info("Creating ecommerce_dss database...")
            cursor.execute("CREATE DATABASE ecommerce_dss")
            logger.info("✅ Database created successfully")
        else:
            logger.info("✅ Database ecommerce_dss already exists")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        logger.error(f"❌ Failed to create database: {e}")
        return False

if __name__ == "__main__":
    logger.info("🚀 PostgreSQL Connection Test")
    logger.info("=" * 50)

    working_config = test_connections()

    if working_config:
        logger.info(f"\n✅ Found working configuration!")
        logger.info(f"Config: {working_config}")

        # Try to create database if needed
        create_database_if_needed(working_config)

    else:
        logger.error("\n❌ No working PostgreSQL configuration found!")
        logger.error("Please check PostgreSQL installation and credentials")