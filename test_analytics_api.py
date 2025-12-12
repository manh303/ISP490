#!/usr/bin/env python3
"""
Test Analytics API - Check database and data availability
"""
import asyncio
import asyncpg
import os
from datetime import date, datetime, timedelta

# Database config (same as analytics.py)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "ecommerce_dss"),
    "user": os.getenv("DB_USER", "dss_user"),
    "password": os.getenv("DB_PASSWORD", "dss_password_123"),
}

async def test_connection():
    """Test database connection"""
    print("\n" + "="*60)
    print("1. TESTING DATABASE CONNECTION")
    print("="*60)
    
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        print("✅ Database connection successful!")
        
        # Test query
        result = await conn.fetchval("SELECT 1")
        print(f"✅ Test query result: {result}")
        
        await conn.close()
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

async def check_schemas():
    """Check if dwh schema exists"""
    print("\n" + "="*60)
    print("2. CHECKING SCHEMAS")
    print("="*60)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        # Check schemas
        schemas = await conn.fetch("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('dwh', 'ml', 'meta')
            ORDER BY schema_name
        """)
        
        print("\nFound schemas:")
        for row in schemas:
            print(f"  ✅ {row['schema_name']}")
            
        return len(schemas) > 0
    finally:
        await conn.close()

async def check_tables():
    """Check if required tables exist"""
    print("\n" + "="*60)
    print("3. CHECKING TABLES")
    print("="*60)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        tables = await conn.fetch("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'dwh'
            ORDER BY table_name
        """)
        
        print("\nTables in dwh schema:")
        for row in tables:
            print(f"  ✅ {row['table_schema']}.{row['table_name']}")
            
        if not tables:
            print("  ❌ No tables found in dwh schema!")
            
        return len(tables) > 0
    finally:
        await conn.close()

async def check_data_in_tables():
    """Check row counts in key tables"""
    print("\n" + "="*60)
    print("4. CHECKING DATA IN TABLES")
    print("="*60)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        required_tables = [
            'dim_date',
            'dim_platform', 
            'dim_category',
            'dim_brand',
            'dim_product',
            'fact_product_daily'
        ]
        
        for table in required_tables:
            try:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM dwh.{table}")
                status = "✅" if count > 0 else "❌"
                print(f"  {status} dwh.{table}: {count:,} rows")
            except Exception as e:
                print(f"  ❌ dwh.{table}: ERROR - {str(e)[:50]}")
                
    finally:
        await conn.close()

async def check_date_range():
    """Check date range in fact_product_daily"""
    print("\n" + "="*60)
    print("5. CHECKING DATE RANGE IN DATA")
    print("="*60)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        # Check if fact_product_daily exists
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'dwh' 
                AND table_name = 'fact_product_daily'
            )
        """)
        
        if not exists:
            print("  ❌ fact_product_daily table does not exist!")
            return
        
        # Check date range via dim_date join
        result = await conn.fetchrow("""
            SELECT 
                MIN(d.date_value) as min_date,
                MAX(d.date_value) as max_date,
                COUNT(*) as total_records
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d ON d.date_sk = f.date_sk
        """)
        
        if result and result['total_records'] > 0:
            print(f"  ✅ Date range: {result['min_date']} to {result['max_date']}")
            print(f"  ✅ Total records: {result['total_records']:,}")
        else:
            print("  ❌ No data found in fact_product_daily!")
            
    except Exception as e:
        print(f"  ❌ Error checking date range: {e}")
    finally:
        await conn.close()

async def test_overview_trends_query():
    """Test the actual overview trends query"""
    print("\n" + "="*60)
    print("6. TESTING OVERVIEW TRENDS QUERY")
    print("="*60)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        # Get date range from data
        date_range = await conn.fetchrow("""
            SELECT 
                MIN(d.date_value) as min_date,
                MAX(d.date_value) as max_date
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d ON d.date_sk = f.date_sk
        """)
        
        if not date_range or not date_range['max_date']:
            print("  ❌ No data available for testing!")
            return
            
        # Use last 7 days of available data
        to_date = date_range['max_date']
        from_date = to_date - timedelta(days=7)
        
        print(f"\n  Testing with date range: {from_date} to {to_date}")
        
        # Run the actual query from analytics_service
        sql = """
            SELECT
                d.date_value AS date,
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS revenue,
                COALESCE(SUM(f.total_review_count), 0) AS total_orders,
                AVG(f.avg_price) AS avg_price,
                AVG(f.avg_rating) AS avg_rating,
                COALESCE(SUM(f.total_review_count), 0) AS total_reviews
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            WHERE d.date_value BETWEEN $1 AND $2
            GROUP BY d.date_value
            ORDER BY d.date_value
        """
        
        rows = await conn.fetch(sql, from_date, to_date)
        
        if rows:
            print(f"\n  ✅ Query returned {len(rows)} data points:")
            for row in rows[:3]:  # Show first 3
                print(f"     Date: {row['date']}, Revenue: {row['revenue']:,.0f}, Orders: {row['total_orders']}")
            if len(rows) > 3:
                print(f"     ... and {len(rows) - 3} more rows")
        else:
            print("  ❌ Query returned no data!")
            
    except Exception as e:
        print(f"  ❌ Query failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await conn.close()

async def test_with_filters():
    """Test query with platform and category filters"""
    print("\n" + "="*60)
    print("7. TESTING WITH FILTERS")
    print("="*60)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        # Get available platforms
        platforms = await conn.fetch("SELECT platform_code, platform_name FROM dwh.dim_platform")
        print(f"\n  Available platforms: {len(platforms)}")
        for p in platforms:
            print(f"    - {p['platform_code']}: {p['platform_name']}")
            
        # Get available categories
        categories = await conn.fetch("""
            SELECT category_sk, category_lvl1, category_lvl2 
            FROM dwh.dim_category 
            LIMIT 5
        """)
        print(f"\n  Sample categories: {len(categories)}")
        for c in categories:
            print(f"    - SK {c['category_sk']}: {c['category_lvl1']} > {c['category_lvl2']}")
            
    except Exception as e:
        print(f"  ❌ Error: {e}")
    finally:
        await conn.close()

async def main():
    """Run all tests"""
    print("="*60)
    print("ANALYTICS API DATA TEST")
    print("="*60)
    print(f"Started at: {datetime.now()}")
    print(f"\nDatabase: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    
    # Run tests
    if not await test_connection():
        print("\n❌ Cannot connect to database. Stopping tests.")
        return
        
    await check_schemas()
    await check_tables()
    await check_data_in_tables()
    await check_date_range()
    await test_overview_trends_query()
    await test_with_filters()
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print("If all checks passed (✅), the API should work correctly.")
    print("If any checks failed (❌), you need to:")
    print("  1. Run data pipeline to populate dwh schema")
    print("  2. Check database schema and tables")
    print("  3. Verify date range matches your query parameters")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(main())

