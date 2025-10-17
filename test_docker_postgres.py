#!/usr/bin/env python3
"""
Test Docker PostgreSQL Connection
"""

import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_docker_postgres():
    """Test connection to Docker PostgreSQL"""

    # Docker PostgreSQL configuration
    config = {
        'host': 'localhost',  # From outside container
        'port': 5433,  # New Docker PostgreSQL port
        'database': 'ecommerce_dss',
        'user': 'dss_user',
        'password': 'dss_password_123'
    }

    try:
        logger.info("🔌 Testing Docker PostgreSQL connection...")
        logger.info(f"   Host: {config['host']}:{config['port']}")
        logger.info(f"   Database: {config['database']}")
        logger.info(f"   User: {config['user']}")

        # Connect to PostgreSQL
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()

        # Test queries
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info(f"✅ PostgreSQL Version: {version[0][:50]}...")

        cursor.execute("SELECT current_user, current_database();")
        user_db = cursor.fetchone()
        logger.info(f"✅ Connected as: {user_db[0]} to database: {user_db[1]}")

        # Check existing schemas
        cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """)
        schemas = cursor.fetchall()
        logger.info(f"📊 Available schemas: {[s[0] for s in schemas]}")

        # Check if raw_data schema exists and has tables
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'raw_data'
            ORDER BY table_name
        """)
        raw_tables = cursor.fetchall()
        if raw_tables:
            logger.info(f"📋 raw_data tables: {[t[0] for t in raw_tables]}")
        else:
            logger.info("📋 raw_data schema: No tables found")

        # Test write permission
        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS test_connection;")
            cursor.execute("DROP SCHEMA IF EXISTS test_connection;")
            logger.info("✅ Write permissions: OK")
        except Exception as e:
            logger.warning(f"⚠️ Write permissions issue: {e}")

        cursor.close()
        conn.close()

        logger.info("🎉 Docker PostgreSQL connection SUCCESSFUL!")
        return config

    except Exception as e:
        logger.error(f"❌ Docker PostgreSQL connection FAILED: {e}")
        return None

def main():
    """Main function"""
    logger.info("🚀 TESTING DOCKER POSTGRESQL CONNECTION")
    logger.info("=" * 50)

    working_config = test_docker_postgres()

    if working_config:
        logger.info("\n✅ DOCKER POSTGRESQL CONNECTION WORKS!")
        logger.info("-" * 40)
        logger.info("Use this configuration in your Python scripts:")
        for key, value in working_config.items():
            logger.info(f"  {key}: {value}")
    else:
        logger.error("\n❌ DOCKER POSTGRESQL CONNECTION FAILED!")

if __name__ == "__main__":
    main()