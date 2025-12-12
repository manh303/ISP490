#!/usr/bin/env python3
"""
Check database schema to verify columns exist before creating indexes
"""
import asyncio
import asyncpg
import os

async def check_schema():
    """Check if required columns exist in tables"""
    
    database_url = "postgresql://dss_user:dss_password_123@localhost/ecommerce_dss"
    
    conn = await asyncpg.connect(database_url)
    
    try:
        # Check dwh.dim_product columns
        print("\n=== dwh.dim_product columns ===")
        rows = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'dwh' AND table_name = 'dim_product'
            ORDER BY ordinal_position;
        """)
        for row in rows:
            print(f"  {row['column_name']}: {row['data_type']}")
        
        # Check dwh.fact_product_daily columns
        print("\n=== dwh.fact_product_daily columns ===")
        rows = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'dwh' AND table_name = 'fact_product_daily'
            ORDER BY ordinal_position;
        """)
        for row in rows:
            print(f"  {row['column_name']}: {row['data_type']}")
        
        # Check ml.fact_product_recommendation columns
        print("\n=== ml.fact_product_recommendation columns ===")
        rows = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'ml' AND table_name = 'fact_product_recommendation'
            ORDER BY ordinal_position;
        """)
        for row in rows:
            print(f"  {row['column_name']}: {row['data_type']}")
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_schema())
