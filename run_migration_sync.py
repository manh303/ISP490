#!/usr/bin/env python3
"""Run database migrations synchronously"""

import psycopg2
import os
import sys

def run_migration():
    # Database URL from environment or hardcoded
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1"
    )
    
    try:
        # Parse connection string
        import urllib.parse
        result = urllib.parse.urlparse(DATABASE_URL)
        
        # Connect to database
        conn = psycopg2.connect(
            host=result.hostname,
            port=result.port,
            database=result.path[1:],
            user=result.username,
            password=result.password
        )
        
        cursor = conn.cursor()
        
        # Read migration file
        migration_file = "backend/migrations/create_activity_logs_table.sql"
        with open(migration_file, 'r') as f:
            sql = f.read()
        
        # Execute migration
        print("Running migration...")
        cursor.execute(sql)
        conn.commit()
        print("✅ Migration completed successfully!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_migration()
