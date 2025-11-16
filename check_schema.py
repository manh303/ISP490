#!/usr/bin/env python3
import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Check ods_product_clean schema
    print("=== ods_product_clean columns ===")
    cols = await conn.fetch("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'ods_product_clean'
        ORDER BY ordinal_position
    """)
    for col in cols:
        print(f"  {col['column_name']}: {col['data_type']}")
    
    # Check stg_raw_products schema
    print("\n=== stg_raw_products columns ===")
    cols = await conn.fetch("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'stg_raw_products'
        ORDER BY ordinal_position
    """)
    for col in cols:
        print(f"  {col['column_name']}: {col['data_type']}")
    
    # Check ods_price_point schema
    print("\n=== ods_price_point columns ===")
    cols = await conn.fetch("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'ods_price_point'
        ORDER BY ordinal_position
    """)
    for col in cols:
        print(f"  {col['column_name']}: {col['data_type']}")
    
    await conn.close()

asyncio.run(main())
