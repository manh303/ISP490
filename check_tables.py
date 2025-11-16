import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    
    tables = await conn.fetch("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    
    print("Tables in dwh schema:")
    if tables:
        for table in tables:
            print(f"  - {table['table_name']}")
    else:
        print("  (No tables found)")
    
    # Check data in aggregation table (if exists)
    print("\n=== Checking for aggregation data ===")
    agg_tables = await conn.fetch("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE '%agg%'
    """)
    
    for table in agg_tables:
        table_name = table['table_name']
        print(f"\nTable: {table_name}")
        
        cols = await conn.fetch(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
        """)
        
        if cols:
            print("  Columns:")
            for col in cols:
                print(f"    - {col['column_name']}: {col['data_type']}")
            
            # Check row count
            count = await conn.fetchval(f"SELECT COUNT(*) FROM dwh.{table_name}")
            print(f"  Row count: {count}")
            
            # Show sample data
            if count > 0:
                sample = await conn.fetch(f"SELECT * FROM dwh.{table_name} LIMIT 3")
                print(f"  Sample data (first 3 rows):")
                for row in sample:
                    print(f"    {dict(row)}")
    
    # Also check if the table was created in public schema
    print("\n=== Checking public schema ===")
    public_agg = await conn.fetch("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public 
        AND table_name LIKE '%agg%'
    """)
    
    if public_agg:
        for table in public_agg:
            print(f"  Found in public: {table['table_name']}")
    else:
        print("  No aggregation tables in public schema")
    
    await conn.close()

asyncio.run(main())
