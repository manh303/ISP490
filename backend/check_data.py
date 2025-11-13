import asyncio
from app.main import db_manager

async def check():
    await db_manager.connect()
    
    # Test exact query from analytics API
    query = """
        SELECT 
            p.product_name,
            p.rating_avg,
            p.review_count,
            p.price_current as price,
            p.category
        FROM ods_product_clean p
        WHERE p.review_count >= 10
        ORDER BY p.rating_avg DESC, p.review_count DESC
        LIMIT 5
    """
    
    result = await db_manager.execute_query(query)
    print(f'Query result: {result}')
    print(f'Result length: {len(result)}')
    
    if result:
        print(f'First item: {result[0]}')

asyncio.run(check())
