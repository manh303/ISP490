import os
import sys
import asyncio

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from app.main import db_manager

async def check_tables():
    await db_manager.connect()
    result = await db_manager.execute_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
    print("Available tables:")
    for row in result:
        print(f"  - {row['table_name']}")

if __name__ == "__main__":
    asyncio.run(check_tables())
