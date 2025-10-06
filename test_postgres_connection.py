#!/usr/bin/env python3
"""
Test PostgreSQL connection from host
"""
import psycopg2

def test_postgres():
    """Test PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='ecommerce_dss',
            user='dss_user',
            password='dss_password_123'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        result = cursor.fetchone()
        print(f"✅ PostgreSQL connection successful: {result[0]}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False

if __name__ == "__main__":
    test_postgres()