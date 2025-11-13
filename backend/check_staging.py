import asyncio
import asyncpg
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

async def check_staging():
    conn = await asyncpg.connect(
        host="dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com",
        port=5432,
        database="ecommerce_dss",
        user="dss_user",
        password="IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4"
    )
    
    print("=" * 60)
    print("KIỂM TRA STAGING TABLES")
    print("=" * 60)
    
    # List all tables
    tables = await conn.fetch("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE '%raw%'
    """)
    print("\nTables with 'raw' in name:")
    for t in tables:
        print(f"  - {t['table_name']}")
    
    # Query stg_raw_products
    platforms = await conn.fetch("""
        SELECT source_platform, COUNT(*) as count 
        FROM stg_raw_products 
        GROUP BY source_platform
    """)
    
    print("\nstg_raw_products:")
    total = 0
    for row in platforms:
        print(f"  {row['source_platform']}: {row['count']:,}")
        total += row['count']
    print(f"  TOTAL: {total:,}")
    
    # Sample Tiki products
    tiki_samples = await conn.fetch("""
        SELECT platform_product_id, raw_data->>'product_name' as name, 
               raw_data->>'price_current' as price
        FROM stg_raw_products 
        WHERE source_platform = 'tiki'
        LIMIT 5
    """)
    
    if tiki_samples:
        print("\nSample Tiki products:")
        for i, row in enumerate(tiki_samples, 1):
            print(f"  {i}. {row['platform_product_id']}: {row['name'][:50] if row['name'] else 'N/A'} - {row['price']}")
    else:
        print("\n⚠️  No Tiki products found")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_staging())
