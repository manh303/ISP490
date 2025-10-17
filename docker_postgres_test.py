#!/usr/bin/env python3
"""
Test Docker PostgreSQL by running inside the Docker network
"""

import subprocess
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_postgres_from_inside_docker():
    """Test PostgreSQL connection from inside Docker network"""

    # Test connection using docker exec
    test_script = '''
import psycopg2
import json
import sys

try:
    conn = psycopg2.connect(
        host='ecommerce-dss-project-postgres-1',
        port=5432,
        database='ecommerce_dss',
        user='dss_user',
        password='dss_password_123'
    )
    cursor = conn.cursor()

    # Get basic info
    cursor.execute("SELECT current_user, current_database(), version();")
    result = cursor.fetchone()

    # Check schemas
    cursor.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name
    """)
    schemas = [row[0] for row in cursor.fetchall()]

    # Test write access
    cursor.execute("CREATE SCHEMA IF NOT EXISTS test_stream;")
    cursor.execute("CREATE TABLE IF NOT EXISTS test_stream.orders (id SERIAL PRIMARY KEY, data TEXT);")
    cursor.execute("INSERT INTO test_stream.orders (data) VALUES ('test');")
    cursor.execute("SELECT COUNT(*) FROM test_stream.orders;")
    count = cursor.fetchone()[0]
    cursor.execute("DROP SCHEMA test_stream CASCADE;")

    cursor.close()
    conn.close()

    print(json.dumps({
        "success": True,
        "user": result[0],
        "database": result[1],
        "version": result[2][:50],
        "schemas": schemas,
        "test_write": count > 0
    }))

except Exception as e:
    print(json.dumps({
        "success": False,
        "error": str(e)
    }))
'''

    try:
        # Run Python inside the data-pipeline container that has access to the network
        result = subprocess.run([
            'docker', 'exec', 'ecommerce-dss-project-data-pipeline-1',
            'python', '-c', test_script
        ], capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            response = json.loads(result.stdout.strip())

            if response.get('success'):
                logger.info("✅ Docker PostgreSQL connection successful!")
                logger.info(f"   User: {response['user']}")
                logger.info(f"   Database: {response['database']}")
                logger.info(f"   Version: {response['version']}")
                logger.info(f"   Schemas: {response['schemas']}")
                logger.info(f"   Write access: {response['test_write']}")
                return True
            else:
                logger.error(f"❌ PostgreSQL connection failed: {response['error']}")
                return False
        else:
            logger.error(f"❌ Docker exec failed: {result.stderr}")
            return False

    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}")
        return False

def main():
    """Main function"""
    logger.info("🐳 TESTING DOCKER POSTGRESQL FROM INSIDE NETWORK")
    logger.info("=" * 60)

    success = test_postgres_from_inside_docker()

    if success:
        logger.info("\n🎉 DOCKER POSTGRESQL IS WORKING!")
        logger.info("💡 The issue is port conflict with local PostgreSQL")
        logger.info("📝 Solution: Use Docker network hostnames or different ports")
    else:
        logger.error("\n❌ DOCKER POSTGRESQL TEST FAILED!")

if __name__ == "__main__":
    main()