import asyncio
import logging
from app.db_pool import init_pool, get_pool, close_pool
from app.db_config import DATABASE_URL
from app.services.dss_service import DSSService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_dss_flow():
    print("1. Initializing pool...")
    try:
        await init_pool(DATABASE_URL, min_size=1, max_size=5)
        print("✅ Pool initialized")
    except Exception as e:
        print(f"❌ Pool init failed: {e}")
        return

    print("\n2. Getting connection...")
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            print("✅ Connection acquired")
            
            print("\n3. Testing DSS Service...")
            service = DSSService(conn)
            
            # Mock request
            request = {
                "from_date": "2025-11-28",
                "to_date": "2025-11-28",
                "platforms": ["lazada"],
                "categories": ["1"],
                "page": 1,
                "page_size": 10
            }
            
            # Run price prediction (mocking AI to avoid rate limits/costs for test)
            # We just want to test DB query
            print("Running query...")
            result = await service._query_price_predictions(request)
            print(f"✅ Query successful. Found {len(result.get('items', []))} items")
            
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n4. Closing pool...")
        await close_pool()
        print("✅ Pool closed")

if __name__ == "__main__":
    asyncio.run(test_dss_flow())
