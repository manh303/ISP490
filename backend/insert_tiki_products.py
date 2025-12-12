import asyncio
import asyncpg
import sys
import io
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

async def insert_tiki_products():
    conn = await asyncpg.connect(
        host="localhost",
        port=5433,
        database="ecommerce_dss",
        user="dss_user",
        password="dss_password_123"
    )
    
    print("Checking stg_raw_products for Tiki data...")
    
    # Check Tiki products in staging
    tiki_count = await conn.fetchval(
        "SELECT COUNT(*) FROM stg_raw_products WHERE source_platform = 'tiki'"
    )
    print(f"Found {tiki_count:,} Tiki products in staging")
    
    if tiki_count == 0:
        print("❌ No Tiki products in staging!")
        await conn.close()
        return
    
    # Get table structure
    columns = await conn.fetch("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'ods_product_clean'
        ORDER BY ordinal_position
    """)
    
    print("\nods_product_clean columns:")
    for col in columns:
        print(f"  - {col['column_name']}: {col['data_type']}")
    
    # Insert Tiki products
    print("\nInserting Tiki products...")
    
    result = await conn.execute("""
        INSERT INTO ods_product_clean (
            product_id,
            product_name,
            price_current,
            rating_avg,
            review_count,
            category,
            source_platform,
            crawled_at,
            last_seen
        )
        SELECT 
            platform_product_id,
            raw_data->>'product_name',
            (raw_data->>'price_current')::numeric,
            (raw_data->>'rating_avg')::numeric,
            (raw_data->>'review_count')::integer,
            raw_data->>'category',
            source_platform,
            NOW(),
            NOW()
        FROM stg_raw_products
        WHERE source_platform = 'tiki'
          AND platform_product_id IS NOT NULL
          AND raw_data->>'product_name' IS NOT NULL
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            price_current = EXCLUDED.price_current,
            rating_avg = EXCLUDED.rating_avg,
            review_count = EXCLUDED.review_count,
            category = EXCLUDED.category,
            last_seen = NOW()
    """)
    
    print(f"✓ Inserted/Updated: {result}")
    
    # Verify
    final_count = await conn.fetchval(
        "SELECT COUNT(*) FROM ods_product_clean WHERE source_platform = 'tiki'"
    )
    print(f"✓ Total Tiki products in ODS: {final_count:,}")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(insert_tiki_products())
