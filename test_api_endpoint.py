#!/usr/bin/env python3
"""
Test Analytics API Endpoint directly
"""
import asyncio
import sys
import os

# Add backend to path
backend_path = os.path.join(os.path.dirname(__file__), 'backend')
sys.path.insert(0, backend_path)
sys.path.insert(0, os.path.join(backend_path, 'app'))

from datetime import date, timedelta
import asyncpg
from services.analytics_service import AnalyticsService

# Database config
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "ecommerce_dss"),
    "user": os.getenv("DB_USER", "dss_user"),
    "password": os.getenv("DB_PASSWORD", "dss_password_123"),
}

async def test_overview_trends():
    """Test get_overview_trends service method"""
    print("="*60)
    print("TESTING OVERVIEW TRENDS SERVICE")
    print("="*60)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        # Create service
        service = AnalyticsService(conn)
        
        # Test 1: Get trends for last 7 days (no filters)
        print("\n1. Testing with last 7 days (no filters):")
        print("-" * 60)
        
        to_date = date(2025, 11, 23)  # Latest date in database
        from_date = date(2025, 11, 16)  # 7 days before
        
        print(f"   From: {from_date}")
        print(f"   To: {to_date}")
        
        result = await service.get_overview_trends(
            from_date=from_date,
            to_date=to_date,
            platform_code=None,
            category_key=None
        )
        
        print(f"\n   Result type: {type(result)}")
        print(f"   From date: {result.from_date}")
        print(f"   To date: {result.to_date}")
        print(f"   Number of data points: {len(result.points)}")
        
        if result.points:
            print("\n   Sample data points:")
            for i, point in enumerate(result.points[:3], 1):
                print(f"      {i}. Date: {point.date}")
                print(f"         Revenue: {point.revenue:,.0f}")
                print(f"         Orders: {point.total_orders:,}")
                print(f"         Avg Price: {point.avg_price}")
                print(f"         Avg Rating: {point.avg_rating}")
                print(f"         Reviews: {point.total_reviews:,}")
                print()
        else:
            print("   ❌ NO DATA POINTS RETURNED!")
            
        # Test 2: With platform filter
        print("\n2. Testing with platform filter (tiki):")
        print("-" * 60)
        
        result2 = await service.get_overview_trends(
            from_date=from_date,
            to_date=to_date,
            platform_code="tiki",
            category_key=None
        )
        
        print(f"   Number of data points: {len(result2.points)}")
        if result2.points:
            print(f"   First point: {result2.points[0].date} - Revenue: {result2.points[0].revenue:,.0f}")
        
        # Test 3: With category filter
        print("\n3. Testing with category filter (SK=1):")
        print("-" * 60)
        
        result3 = await service.get_overview_trends(
            from_date=from_date,
            to_date=to_date,
            platform_code=None,
            category_key="1"
        )
        
        print(f"   Number of data points: {len(result3.points)}")
        if result3.points:
            print(f"   First point: {result3.points[0].date} - Revenue: {result3.points[0].revenue:,.0f}")
            
        # Test 4: Convert to JSON-like dict to see API response
        print("\n4. API Response Preview (as dict):")
        print("-" * 60)
        
        response_dict = {
            "from_date": str(result.from_date),
            "to_date": str(result.to_date),
            "platform_code": result.platform_code,
            "category_key": result.category_key,
            "points": [
                {
                    "date": str(p.date),
                    "revenue": p.revenue,
                    "total_orders": p.total_orders,
                    "avg_price": p.avg_price,
                    "avg_rating": p.avg_rating,
                    "total_reviews": p.total_reviews
                }
                for p in result.points[:2]  # Show first 2
            ]
        }
        
        import json
        print(json.dumps(response_dict, indent=2))
        
        print("\n" + "="*60)
        print("✅ SERVICE TEST COMPLETED SUCCESSFULLY!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(test_overview_trends())

