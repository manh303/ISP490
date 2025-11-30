"""
Run DSS Decision Schema Migration
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# Add app directory to path
sys.path.insert(0, str(Path(__file__).parent / "app"))

from db_config import DATABASE_URL

async def run_migration():
    # Read migration SQL
    migration_file = Path(__file__).parent / "migrations" / "create_dss_decision_schema.sql"
    
    with open(migration_file, "r", encoding="utf-8") as f:
        sql = f.read()
    
    print("Connecting to database...")
    print(f"Using DATABASE_URL: {DATABASE_URL[:50]}...")
    conn = await asyncpg.connect(dsn=DATABASE_URL)
    
    try:
        print("Running DSS Decision schema migration...")
        await conn.execute(sql)
        print("✅ Migration completed successfully!")
        
        # Verify tables were created
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'dss'
            ORDER BY table_name
        """)
        
        print(f"\n📊 Created {len(tables)} tables in dss schema:")
        for table in tables:
            print(f"  - {table['table_name']}")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        raise
    finally:
        await conn.close()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    asyncio.run(run_migration())
