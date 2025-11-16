import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print('[STEP] Dropping dwh_fact_review_summary from public')
        await conn.execute('DROP TABLE IF EXISTS public.dwh_fact_review_summary CASCADE;')
        print('[OK] Table dropped')
        
        # Verify
        exists = await conn.fetchval("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'dwh_fact_review_summary'
            )
        """)
        
        if exists:
            print('[WARN] Table still exists')
        else:
            print('[OK] Verified: table successfully dropped')
    
    finally:
        await conn.close()

asyncio.run(main())
