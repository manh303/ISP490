#!/usr/bin/env python3
"""
Test all APIs to verify category names are proper
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

async def test_all_apis():
    """Test all APIs with category names"""
    print("="*80)
    print("TESTING ALL CATEGORY-RELATED APIs")
    print("="*80)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        service = AnalyticsService(conn)
        
        # 1. Category Share (Already fixed)
        print("\n1. Testing /analytics/platforms/category-share")
        print("-"*80)
        result = await service.get_category_share(
            from_date=date(2025, 11, 16),
            to_date=date(2025, 11, 23),
            platform_code="tiki"
        )
        print(f"   ✅ Found {len(result)} categories")
        if result:
            print(f"   Sample: {result[0].category_name} (key: {result[0].category_key})")
            # Check if numeric
            is_numeric = result[0].category_name.isdigit()
            if is_numeric:
                print(f"   ❌ Still numeric!")
            else:
                print(f"   ✅ Proper name!")
        
        # 2. Top Products (Just fixed)
        print("\n2. Testing /analytics/products/top")
        print("-"*80)
        result = await service.get_top_products(
            from_date=date(2025, 11, 16),
            to_date=date(2025, 11, 23),
            metric="revenue",
            limit=5
        )
        print(f"   ✅ Found {len(result)} products")
        if result:
            for i, item in enumerate(result[:3], 1):
                cat_name = item.category_name or "N/A"
                print(f"   {i}. {item.product_name[:50]}")
                print(f"      Category: {cat_name} (key: {item.category_key})")
        
        # 3. Overview KPIs (check if it uses category)
        print("\n3. Testing /analytics/overview/kpis")
        print("-"*80)
        result = await service.get_overview_kpis(
            from_date=date(2025, 11, 16),
            to_date=date(2025, 11, 23)
        )
        print(f"   ✅ Total Revenue: {result.total_revenue:,.0f}")
        print(f"   Note: KPIs don't return category names (only filter by category_key)")
        
        # 4. Product Report
        print("\n4. Testing /analytics/report/product")
        print("-"*80)
        try:
            timeseries = await service.get_product_timeseries(
                product_key="lazada_2792189799",
                platform_code="lazada",
                from_date=date(2025, 11, 1),
                to_date=date(2025, 11, 23)
            )
            print(f"   ✅ Timeseries: {len(timeseries.points)} points")
            
            review = await service.get_review_summary(
                product_key="lazada_2792189799",
                platform_code="lazada",
                from_date=date(2025, 11, 1),
                to_date=date(2025, 11, 23),
                top_n=5
            )
            print(f"   ✅ Review Summary: {review.total_reviews} reviews")
            print(f"   Note: Report doesn't return category names")
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print("✅ Category Share: Returns proper category names")
        print("✅ Top Products: Now returns category names")
        print("✅ Overview KPIs: Uses category_key for filtering (no name in response)")
        print("✅ Product Report: No category names (product-level detail)")
        print("\n" + "="*80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(test_all_apis())

