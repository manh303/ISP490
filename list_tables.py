import asyncio
import asyncpg

DATABASE_URL = "postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1"

async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # List all schemas
        schemas = await conn.fetch("""
            SELECT schema_name 
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
            ORDER BY schema_name
        """)
        
        print("[SCHEMAS]")
        for schema in schemas:
            print(f"  - {schema['schema_name']}")
        
        # List all tables in each schema
        for schema in schemas:
            schema_name = schema['schema_name']
            tables = await conn.fetch(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{schema_name}'
                ORDER BY table_name
            """)
            
            print(f"\n[{schema_name.upper()}] ({len(tables)} tables)")
            for table in tables:
                # Get row count
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {schema_name}.{table['table_name']}")
                print(f"  - {table['table_name']} ({count:,} rows)")
        
    finally:
        await conn.close()

asyncio.run(main())
