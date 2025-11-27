import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1"

async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Danh sach cac bang can move
        dim_tables = [
            'dwh_dim_product',
            'dwh_dim_category',
            'dwh_dim_brand',
            'dwh_dim_date',
            'dwh_dim_platform',
            'dwh_fact_product_daily'
        ]
        
        print("[STEP 1] Create schema dwh if not exists")
        await conn.execute("CREATE SCHEMA IF NOT EXISTS dwh;")
        print("  [OK] Schema dwh ready")
        
        for table in dim_tables:
            print(f"\n[STEP 2] Processing table: {table}")
            
            # Check if table exists in public
            exists = await conn.fetchval(f"""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table}'
                )
            """)
            
            if not exists:
                print(f"  [WARN] Table {table} not found in public schema")
                continue
            
            # Get row count
            count = await conn.fetchval(f"SELECT COUNT(*) FROM public.{table}")
            print(f"  Current rows in public: {count:,}")
            
            # Check if already exists in dwh
            dwh_exists = await conn.fetchval(f"""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'dwh' 
                    AND table_name = '{table}'
                )
            """)
            
            if dwh_exists:
                print(f"  [INFO] Table {table} already exists in dwh schema")
                print(f"  => Dropping old dwh.{table}")
                await conn.execute(f"DROP TABLE IF EXISTS dwh.{table} CASCADE;")
            
            # Create table in dwh schema with same structure
            print(f"  => Creating dwh.{table}")
            create_sql = f"""
                CREATE TABLE dwh.{table} AS
                SELECT * FROM public.{table};
            """
            await conn.execute(create_sql)
            
            # Verify
            new_count = await conn.fetchval(f"SELECT COUNT(*) FROM dwh.{table}")
            print(f"  [OK] Copied {new_count:,} rows to dwh.{table}")
            
            # Drop from public (CASCADE to handle dependencies)
            print(f"  => Dropping public.{table}")
            try:
                await conn.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE;")
                print(f"  [OK] Dropped public.{table}")
            except Exception as e:
                print(f"  [WARN] Could not drop public.{table}: {str(e)}")
                print(f"  => Truncating public.{table} instead")
                try:
                    await conn.execute(f"TRUNCATE TABLE public.{table} CASCADE;")
                    print(f"  [OK] Truncated public.{table}")
                except Exception as e2:
                    print(f"  [ERROR] Could not truncate: {str(e2)}")
        
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        
        # Show all tables in dwh
        dwh_tables = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables 
            WHERE table_schema = 'dwh'
            ORDER BY table_name
        """)
        
        print(f"\nTables in DWH schema ({len(dwh_tables)} tables):")
        for t in dwh_tables:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM dwh.{t['table_name']}")
            print(f"  - {t['table_name']}: {count:,} rows")
        
        # Show remaining tables in public
        public_tables = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_name LIKE 'dwh_%'
            ORDER BY table_name
        """)
        
        if public_tables:
            print(f"\nRemaining dwh_* tables in PUBLIC schema ({len(public_tables)} tables):")
            for t in public_tables:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM public.{t['table_name']}")
                print(f"  - {t['table_name']}: {count:,} rows")
        else:
            print("\nNo dwh_* tables remaining in PUBLIC schema")
        
        print("\n[SUCCESS] Migration completed!")
        
    except Exception as e:
        print(f"\n[ERROR] {str(e)}")
        raise
    finally:
        await conn.close()

asyncio.run(main())
