"""
Run database optimization migration (improved error handling)
"""
import asyncio
import asyncpg
import time

async def run_optimization():
    conn = await asyncpg.connect(
        host="dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com",
        port=5432,
        database="ecommerce_dss_1",
        user="dss_user",
        password="6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G",
        ssl="require"
    )
    
    try:
        print("=" * 70)
        print("DSS Query Optimization - Creating Indexes")
        print("=" * 70)
        
        with open("backend/migrations/optimize_dss_queries.sql", "r", encoding="utf-8") as f:
            sql_content = f.read()
        
        # Split by statement and execute one by one
        statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]
        
        created = []
        skipped = []
        errors = []
        
        for i, stmt in enumerate(statements, 1):
            if not stmt or stmt.startswith('COMMENT'):
                continue
                
            try:
                # Extract index name if it's a CREATE INDEX statement
                if 'CREATE INDEX' in stmt.upper():
                    idx_name = stmt.split('IF NOT EXISTS')[1].split('ON')[0].strip() if 'IF NOT EXISTS' in stmt else 'unknown'
                    print(f"\n[{i}/{len(statements)}] Creating {idx_name}...")
                    start = time.perf_counter()
                    await conn.execute(stmt)
                    duration = time.perf_counter() - start
                    print(f"  ✅ Created in {duration:.2f}s")
                    created.append(idx_name)
                elif 'ANALYZE' in stmt.upper():
                    table_name = stmt.split('ANALYZE')[1].strip()
                    print(f"\n[{i}/{len(statements)}] Analyzing {table_name}...")
                    await conn.execute(stmt)
                    print(f"  ✅ Done")
                else:
                    await conn.execute(stmt)
                    
            except asyncpg.exceptions.DuplicateTableError:
                print(f"  ⚠️  Already exists, skipping")
                skipped.append(idx_name)
            except Exception as e:
                error_msg = str(e)[:100]
                print(f"  ❌ Error: {error_msg}")
                errors.append((idx_name, error_msg))
        
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"✅ Created: {len(created)} indexes")
        print(f"⚠️  Skipped: {len(skipped)} (already exist)")
        print(f"❌ Errors: {len(errors)}")
        
        if created:
            print("\nCreated indexes:")
            for idx in created:
                print(f"  - {idx}")
        
        if errors:
            print("\nErrors:")
            for idx, err in errors:
                print(f"  - {idx}: {err}")
        
        # Verify what we have now
        print("\n" + "=" * 70)
        print("Current indexes on key tables:")
        print("=" * 70)
        
        for table in ['fact_product_daily', 'fact_price_prediction']:
            indexes = await conn.fetch("""
                SELECT indexname 
                FROM pg_indexes 
                WHERE tablename = $1
                ORDER BY indexname
            """, table)
            print(f"\n{table} ({len(indexes)} indexes):")
            for idx in indexes:
                print(f"  - {idx['indexname']}")
                
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(run_optimization())
