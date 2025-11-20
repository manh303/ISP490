import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Domain tables
        domain_tables = [
            'admin_profile',
            'analyst_profile',
            'customer_profile',
            'customers',
            'user_activity_logs',
            'warehouse_load_stats',
        ]
        
        print("[STEP 1] Create domain schema")
        await conn.execute("CREATE SCHEMA IF NOT EXISTS domain;")
        print("  [OK] Schema 'domain' ready")
        
        print("\n[STEP 2] Move domain tables")
        print("="*60)
        
        for table in domain_tables:
            print(f"\n[MOVE] {table} -> domain")
            
            # Check if exists in public
            exists = await conn.fetchval(f"""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table}'
                )
            """)
            
            if not exists:
                print(f"  [SKIP] Not found in public")
                continue
            
            count = await conn.fetchval(f"SELECT COUNT(*) FROM public.{table}")
            print(f"  Rows: {count:,}")
            
            # Create in domain schema
            create_sql = f"""
                CREATE TABLE domain.{table} AS
                SELECT * FROM public.{table};
            """
            await conn.execute(create_sql)
            print(f"  [OK] Created in domain")
            
            # Drop from public
            await conn.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE;")
            print(f"  [OK] Dropped from public")
        
        print("\n" + "="*60)
        print("[SUMMARY] Final Schema Organization")
        print("="*60)
        
        schemas = await conn.fetch("""
            SELECT DISTINCT table_schema 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public')
            ORDER BY table_schema
        """)
        
        for schema in schemas:
            schema_name = schema['table_schema']
            tables = await conn.fetch(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{schema_name}'
                ORDER BY table_name
            """)
            
            print(f"\n[{schema_name.upper()}] ({len(tables)} tables)")
            for t in tables:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {schema_name}.{t['table_name']}")
                print(f"  - {t['table_name']}: {count:,} rows")
        
        # Check public
        public_tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        if public_tables:
            print(f"\n[PUBLIC] ({len(public_tables)} BASE TABLES)")
            for t in public_tables:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM public.{t['table_name']}")
                print(f"  - {t['table_name']}: {count:,} rows")
        else:
            print(f"\n[PUBLIC] (0 BASE TABLES) - Only system views remain")
        
        print("\n[SUCCESS] Schema organization completed!")
        
    except Exception as e:
        print(f"\n[ERROR] {str(e)}")
        raise
    finally:
        await conn.close()

asyncio.run(main())
