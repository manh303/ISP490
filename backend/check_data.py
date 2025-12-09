import asyncio
from backend.main import db_manager

async def check():
    await db_manager.connect()
    
    # Check columns in table
    query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'ods_review_clean'
        ORDER BY ordinal_position
    """
    
    result = await db_manager.execute_query(query)
    print(f"Columns in ods_review_clean:")
    for col in result:
        print(f"  {col['column_name']}: {col['data_type']} (nullable: {col['is_nullable']})")
    
    print(f"\nTotal columns: {len(result)}")
    
    # Test exact query from analytics API
    query2 = """
        SELECT *
        FROM ods_review_clean 
        LIMIT 1
    """
    
    result2 = await db_manager.execute_query(query2)
    print(f'\nSample data: {result2}')

asyncio.run(check())
