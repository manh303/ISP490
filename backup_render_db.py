import asyncio
import asyncpg
import json
from datetime import datetime

SOURCE = {
    "host": "dpg-d4b1rger433s738l401g-a.singapore-postgres.render.com",
    "port": 5432,
    "database": "ecommerce_dss_bh5f",
    "user": "dss_user",
    "password": "0ZskEPwcL5kSjLPNfclbB3cCSbLZWFDY"
}

async def backup():
    conn = await asyncpg.connect(**SOURCE)
    backup_file = f"render_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # Get all tables
    tables = await conn.fetch("""
        SELECT tablename FROM pg_tables 
        WHERE schemaname = 'public'
    """)
    
    backup_data = {}
    for table_row in tables:
        table = table_row['tablename']
        rows = await conn.fetch(f"SELECT * FROM {table}")
        backup_data[table] = [dict(row) for row in rows]
        print(f"Backed up {table}: {len(rows)} rows")
    
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, ensure_ascii=False, default=str, indent=2)
    
    await conn.close()
    print(f"\nBackup saved: {backup_file}")
    return backup_file

if __name__ == "__main__":
    asyncio.run(backup())
