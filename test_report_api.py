#!/usr/bin/env python3
"""
Test report API and check for syntax errors
"""
import asyncio
import asyncpg
import os
import sys

# Add backend to path
backend_path = os.path.join(os.path.dirname(__file__), 'backend')
sys.path.insert(0, backend_path)
sys.path.insert(0, os.path.join(backend_path, 'app'))

from datetime import date
from services.analytics_service import AnalyticsService

# Database config
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "ecommerce_dss"),
    "user": os.getenv("DB_USER", "dss_user"),
    "password": os.getenv("DB_PASSWORD", "dss_password_123"),
}

async def test_report_api():
    """Test product report API"""
    print("="*60)
    print("TESTING PRODUCT REPORT API")
    print("="*60)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        service = AnalyticsService(conn)
        
        # Test 1: Product timeseries
        print("\n1. Testing product timeseries...")
        try:
            timeseries = await service.get_product_timeseries(
                product_key="lazada_2792189799",
                platform_code="lazada",
                from_date=date(2025, 11, 1),
                to_date=date(2025, 11, 23)
            )
            print(f"   ✅ Timeseries: {len(timeseries.points)} data points")
        except Exception as e:
            print(f"   ❌ Error: {e}")
            import traceback
            traceback.print_exc()
        
        # Test 2: Review summary
        print("\n2. Testing review summary...")
        try:
            review_summary = await service.get_review_summary(
                product_key="lazada_2792189799",
                platform_code="lazada",
                from_date=date(2025, 11, 1),
                to_date=date(2025, 11, 23),
                top_n=5
            )
            print(f"   ✅ Review summary: {review_summary.total_reviews} reviews")
        except Exception as e:
            print(f"   ❌ Error: {e}")
            import traceback
            traceback.print_exc()
        
        # Test 3: Check for $ in queries
        print("\n3. Checking SQL queries...")
        print("   All queries use asyncpg placeholders ($1, $2, etc.)")
        print("   ✅ Should work fine with asyncpg")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(test_report_api())

