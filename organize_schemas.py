import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1"

# Map tables to their correct schemas
SCHEMA_MAPPING = {
    'ods': [
        'ods_category_mapping',
        'ods_platform_ref',
        'ods_price_point',
        'ods_product_clean',
        'ods_product_mapping',
        'ods_product_master',
        'ods_review_clean',
    ],
    'iam': [
        'iam_api_key',
        'iam_audit_log',
        'iam_email_verification_token',
        'iam_login_attempt',
        'iam_password_reset_token',
        'iam_permission',
        'iam_role',
        'iam_role_dataset_access',
        'iam_role_permission',
        'iam_user',
        'iam_user_role',
        'iam_user_session',
    ],
    'mart': [
        'mart_demand_forecast',
        'mart_price_optimization',
        'mart_sales_forecast_weekly',
        'mart_sales_trend',
        'mart_seasonality',
        'dm_price_analytics',
    ],
    'ml_models': [
        'ml_customer_segments',
        'ml_demand_forecast',
        'ml_price_predictions',
        'ml_product_recommendations',
    ],
    'metadata': [
        'meta_business_term',
        'meta_dataset',
        'meta_etl_log',
        'meta_expectation',
        'meta_job',
        'meta_source_system',
    ],
}

async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("[STEP 1] Create all schemas if not exist")
        print("="*60)
        for schema in SCHEMA_MAPPING.keys():
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            print(f"  [OK] Schema '{schema}' ready")
        
        print("\n[STEP 2] Move tables to correct schemas")
        print("="*60)
        
        total_moved = 0
        for target_schema, tables in SCHEMA_MAPPING.items():
            for table_name in tables:
                print(f"\n[MOVE] {table_name} -> {target_schema}")
                
                # Check if exists in public
                exists = await conn.fetchval(f"""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = '{table_name}'
                    )
                """)
                
                if not exists:
                    print(f"  [SKIP] Not found in public schema")
                    continue
                
                count = await conn.fetchval(f"SELECT COUNT(*) FROM public.{table_name}")
                print(f"  Rows: {count:,}")
                
                # Check if already exists in target schema
                target_exists = await conn.fetchval(f"""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = '{target_schema}' 
                        AND table_name = '{table_name}'
                    )
                """)
                
                if target_exists:
                    print(f"  [WARN] Already exists in {target_schema}, dropping old one")
                    await conn.execute(f"DROP TABLE IF EXISTS {target_schema}.{table_name} CASCADE;")
                
                # Create in target schema
                create_sql = f"""
                    CREATE TABLE {target_schema}.{table_name} AS
                    SELECT * FROM public.{table_name};
                """
                await conn.execute(create_sql)
                print(f"  [OK] Created in {target_schema}")
                
                # Drop from public
                await conn.execute(f"DROP TABLE IF EXISTS public.{table_name} CASCADE;")
                print(f"  [OK] Dropped from public")
                
                total_moved += 1
        
        print("\n" + "="*60)
        print("[SUMMARY] Tables by Schema")
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
        
        print("\n" + "="*60)
        print(f"[SUCCESS] Moved {total_moved} tables to correct schemas!")
        print("="*60)
        
    except Exception as e:
        print(f"\n[ERROR] {str(e)}")
        raise
    finally:
        await conn.close()

asyncio.run(main())
