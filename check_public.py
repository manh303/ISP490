import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:dss_password_123@localhost/ecommerce_dss"

async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # List all tables in public
        public_tables = await conn.fetch("""
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        print(f"[PUBLIC] ({len(public_tables)} tables)")
        for t in public_tables:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM public.{t['table_name']}")
            print(f"  - {t['table_name']}: {count:,} rows ({t['table_type']})")
    
    finally:
        await conn.close()

asyncio.run(main())
