import asyncio
import asyncpg
import sys
import io
from datetime import date

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

async def truncate_ods():
    conn = await asyncpg.connect(
        host="dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com",
        port=5432,
        database="ecommerce_dss",
        user="dss_user",
        password="IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4"
    )
    
    today = date.today()
    print(f"Deleting data for date: {today}")
    
    # Delete products
    result = await conn.execute(
        "DELETE FROM ods_product_clean WHERE DATE(crawled_at) = $1",
        today
    )
    print(f"✓ Deleted products: {result}")
    
    # Delete reviews
    result = await conn.execute(
        "DELETE FROM ods_review_clean WHERE DATE(crawled_at) = $1",
        today
    )
    print(f"✓ Deleted reviews: {result}")
    
    await conn.close()
    print("✅ Today partition cleaned")

if __name__ == "__main__":
    asyncio.run(truncate_ods())
