#!/usr/bin/env python3
"""Run database migrations"""

import asyncio
import os
import sys
from databases import Database

async def run_migration():
    # Database URL from environment or hardcoded
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1"
    )
    
    # Connect to database
    db = Database(DATABASE_URL)
    await db.connect()
    
    try:
        # Read migration file
        migration_file = "backend/migrations/create_activity_logs_table.sql"
        with open(migration_file, 'r') as f:
            sql = f.read()
        
        # Execute migration
        print("Running migration...")
        await db.execute(sql)
        print("✅ Migration completed successfully!")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)
    finally:
        await db.disconnect()

if __name__ == "__main__":
    asyncio.run(run_migration())
