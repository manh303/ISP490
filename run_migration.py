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
        "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"
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
