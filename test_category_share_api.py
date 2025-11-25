#!/usr/bin/env python3
"""
Test category share API after fixing category names
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
    "host": os.getenv("DB_HOST", "dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "ecommerce_dss_1"),
    "user": os.getenv("DB_USER", "dss_user"),
    "password": os.getenv("DB_PASSWORD", "6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G"),
}

async def test_category_share():
    """Test category share with proper category names"""
    print("="*60)
    print("TESTING CATEGORY SHARE API")
    print("="*60)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        service = AnalyticsService(conn)
        
        # Test for tiki
        print("\n1. Testing Category Share for Tiki...")
        
        result = await service.get_category_share(
            from_date=date(2025, 11, 16),
            to_date=date(2025, 11, 23),
            platform_code="tiki"
        )
        
        print(f"\n   Found {len(result)} categories")
        print(f"\n   Top 10 categories by revenue:")
        print("-" * 100)
        print(f"   {'Category Name':<35} {'Revenue':>15} {'Share':>10}")
        print("-" * 100)
        
        for i, item in enumerate(result[:10], 1):
            revenue_str = f"{item.revenue:,.0f}"
            share_str = f"{item.revenue_share*100:.1f}%"
            
            print(f"   {item.category_name:<35} {revenue_str:>15} {share_str:>10}")
        
        # Verify category names are not just numbers
        print("\n2. Verifying category names...")
        
        numeric_names = [item for item in result if item.category_name.isdigit()]
        proper_names = [item for item in result if not item.category_name.isdigit()]
        
        print(f"   ✅ Proper names: {len(proper_names)}/{len(result)}")
        print(f"   ❌ Numeric names: {len(numeric_names)}/{len(result)}")
        
        if numeric_names:
            print("\n   ⚠️  Still has numeric names:")
            for item in numeric_names[:5]:
                print(f"      - Category {item.category_key}: '{item.category_name}'")
        else:
            print("\n   ✅ All category names are proper!")
        
        # Test for lazada
        print("\n3. Testing Category Share for Lazada...")
        
        result_lazada = await service.get_category_share(
            from_date=date(2025, 11, 16),
            to_date=date(2025, 11, 23),
            platform_code="lazada"
        )
        
        print(f"\n   Found {len(result_lazada)} categories")
        print(f"\n   Top 5 categories:")
        for i, item in enumerate(result_lazada[:5], 1):
            print(f"   {i}. {item.category_name:<35} - {item.revenue:>15,.0f}")
        
        # API Response Format
        print("\n" + "="*60)
        print("4. API Response Format (JSON):")
        print("="*60)
        
        if result:
            sample = result[0]
            response_dict = {
                "category_key": sample.category_key,
                "category_name": sample.category_name,  # Should be name, not number
                "platform_code": sample.platform_code,
                "revenue": sample.revenue,
                "revenue_share": sample.revenue_share
            }
            
            import json
            print(json.dumps(response_dict, indent=2))
        
        print("\n" + "="*60)
        print("✅ CATEGORY SHARE TEST COMPLETED!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(test_category_share())

