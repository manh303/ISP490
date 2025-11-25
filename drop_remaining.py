import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1"

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
