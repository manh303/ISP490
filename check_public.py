import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

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
