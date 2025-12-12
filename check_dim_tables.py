#!/usr/bin/env python3
"""
Check dimension tables with size info
"""

import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:dss_password_123@localhost/ecommerce_dss"

DIM_TABLES = [
    "dim_platform",
    "dim_category",
    "dim_brand",
    "dim_product",
    "dim_date",
    "dim_reviewer",
    "fact_product_daily_agg",
    "fact_review_daily_agg"
]

async def check_table(conn, schema, table_name):
    print(f"\n{'='*70}")
    print(f"Table: {schema}.{table_name}")
    print('='*70)
    
    # Check if table exists
    exists = await conn.fetchval(f"""
        SELECT EXISTS(
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
            AND table_name = '{table_name}'
        )
    """)
    
    if not exists:
        print(f"[!] Table does not exist")
        return
    
    # Get table size
    size_info = await conn.fetchrow(f"""
        SELECT 
            pg_size_pretty(pg_total_relation_size('{schema}.{table_name}'::regclass)) as total_size,
            pg_size_pretty(pg_relation_size('{schema}.{table_name}'::regclass)) as table_size,
            pg_size_pretty(pg_total_relation_size('{schema}.{table_name}'::regclass) - pg_relation_size('{schema}.{table_name}'::regclass)) as indexes_size
    """)
    
    print(f"\n[SIZE]")
    print(f"  Total size: {size_info['total_size']}")
    print(f"  Table size: {size_info['table_size']}")
    print(f"  Indexes size: {size_info['indexes_size']}")
    
    # Get columns info
    cols = await conn.fetch(f"""
        SELECT 
            column_name, 
            data_type, 
            is_nullable,
            column_default,
            character_maximum_length
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    
    print(f"\n[COLUMNS] ({len(cols)})")
    for col in cols:
        nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
        char_len = f"({col['character_maximum_length']})" if col['character_maximum_length'] else ""
        default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
        print(f"  - {col['column_name']}: {col['data_type']}{char_len} {nullable}{default}")
    
    # Check row count
    count = await conn.fetchval(f"SELECT COUNT(*) FROM {schema}.{table_name}")
    print(f"\n[ROWS] Count: {count:,}")
    
    # Show sample data
    if count > 0:
        sample = await conn.fetch(f"SELECT * FROM {schema}.{table_name} LIMIT 2")
        print(f"\n[SAMPLE DATA] (first 2 rows):")
        for i, row in enumerate(sample, 1):
            print(f"\n  Row {i}:")
            for key, value in dict(row).items():
                value_str = str(value)[:60] + "..." if len(str(value)) > 60 else value
                print(f"    {key}: {value_str}")

async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("\n" + "=" * 70)
        print(" DIMENSION TABLES STATUS")
        print("=" * 70)
        
        for table_name in DIM_TABLES:
            await check_table(conn, 'dwh', table_name)
        
        print("\n" + "=" * 70)
        print(" DONE")
        print("=" * 70 + "\n")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
