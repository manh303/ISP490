#!/usr/bin/env python3
"""Direct test of analytics service with pool"""
import asyncio
import sys
sys.path.insert(0, 'backend')

from app.db_pool import get_pool
from app.services.analytics_service import AnalyticsService
from datetime import date

async def test_pool():
    try:
        print("Getting pool...")
        pool = await get_pool()
        print(f"✅ Got pool: {pool}")
        
        print("\nCreating service...")
        service = AnalyticsService(pool)
        print(f"✅ Created service")
        
        print("\nTesting get_platform_filters...")
        platforms = await service.get_platform_filters()
        print(f"✅ Got {len(platforms)} platforms")
        
        print("\nTesting get_overview_kpis...")
        kpis = await service.get_overview_kpis(
            from_date=date(2025, 11, 25),
            to_date=date(2025, 12, 2)
        )
        print(f"✅ Got KPIs: revenue={kpis.total_revenue}")
        
        print("\n✅ ALL TESTS PASSED! Pool is working correctly.")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_pool())
