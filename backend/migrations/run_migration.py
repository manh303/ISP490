"""
Run database migration for async AI generation tracking
"""
import asyncio
import asyncpg
import os

async def run_migration():
    # Render database credentials
    conn = await asyncpg.connect(
        host="dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com",
        port=5432,
        database="ecommerce_dss_1",
        user="dss_user",
        password="6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G",
        ssl="require"
    )
    
    try:
        print("🔄 Running migration: add_ai_generation_status.sql")
        
        # Read migration file
        with open("backend/migrations/add_ai_generation_status.sql", "r", encoding="utf-8") as f:
            sql = f.read()
        
        # Execute migration
        await conn.execute(sql)
        
        print("✅ Migration completed successfully!")
        
        # Verify columns were added
        result = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'dss' 
            AND table_name = 'dss_analysis_session'
            AND column_name LIKE 'ai_%'
            ORDER BY column_name
        """)
        
        print("\n📋 AI-related columns in dss_analysis_session:")
        for row in result:
            print(f"  - {row['column_name']}: {row['data_type']}")
            
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        raise
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(run_migration())
