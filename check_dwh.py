import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    
    print("=== dwh_fact_product_daily columns ===")
    cols = await conn.fetch("""
        SELECT *
        FROM dwh_dim_product

    """)
    for col in cols:
        print(f"  {col['column_name']}: {col['data_type']}")
    
    await conn.close()

asyncio.run(main())
