#!/usr/bin/env python3
"""
DSS Drill-Down API - Usage Examples & Sample Queries

Run these examples to test the drill-down analytics API
"""

import asyncio
import aiohttp
from datetime import datetime, timedelta
import json

# API Base URL (update based on your deployment)
BASE_URL = "http://localhost:8000/api/v1/dss/drilldown"


# ====================================
# EXAMPLE 1: Overall Dashboard
# ====================================

async def example_1_overall_dashboard():
    """
    Get overall dashboard showing total revenue, top categories, top platforms
    and key alerts
    """
    print("\n" + "="*80)
    print("EXAMPLE 1: Overall Dashboard")
    print("="*80)
    
    # 30 days ago to today
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    url = f"{BASE_URL}/overall"
    params = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()
            print("\n✓ Response Status:", response.status)
            print("\nRevenue Summary:")
            metrics = data['revenue_metrics']
            print(f"  - Total Revenue: {metrics['total_revenue']:,.0f} VND")
            print(f"  - Previous Period: {metrics['previous_period_revenue']:,.0f} VND")
            print(f"  - Change: {metrics['revenue_change_percent']:.1f}%")
            print(f"  - Trend: {metrics['revenue_trend']}")
            
            print("\nTop Categories:")
            for i, cat in enumerate(data['top_categories'][:3], 1):
                print(f"  {i}. {cat['category_name']}: {cat['revenue']:,.0f} VND ({cat['revenue_percent']:.1f}%)")
            
            print("\nTop Platforms:")
            for i, plat in enumerate(data['top_platforms'][:3], 1):
                print(f"  {i}. {plat['platform_name']}: {plat['revenue']:,.0f} VND ({plat['revenue_percent']:.1f}%)")
            
            if data['key_alerts']:
                print("\nAlerts:")
                for alert in data['key_alerts']:
                    print(f"  ⚠️  [{alert['severity'].upper()}] {alert['title']}")
                    print(f"     → {alert['action']}")
    
    return data


# ====================================
# EXAMPLE 2: Platform Drill-Down
# ====================================

async def example_2_platform_drilldown():
    """
    Drill down into Lazada platform to see category breakdown
    """
    print("\n" + "="*80)
    print("EXAMPLE 2: Platform Drill-Down (Lazada)")
    print("="*80)
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    url = f"{BASE_URL}/platform/lazada"
    params = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()
            print("\n✓ Response Status:", response.status)
            
            print(f"\n{data['platform_name']} Performance:")
            metrics = data['platform_revenue_metrics']
            print(f"  - Revenue: {metrics['total_revenue']:,.0f} VND")
            print(f"  - Change: {metrics['revenue_change_percent']:.1f}%")
            
            print("\nCategory Breakdown:")
            for i, cat in enumerate(data['top_categories'][:5], 1):
                print(f"  {i}. {cat['category_name']}")
                print(f"     Revenue: {cat['revenue']:,.0f} VND ({cat['revenue_percent']:.1f}%)")
                print(f"     Change: {cat['revenue_change_percent']:.1f}%")
                print(f"     Out of stock: {cat['out_of_stock_count']} products")
            
            if data['problematic_categories']:
                print("\nProblematic Categories (>10% decline):")
                for cat in data['problematic_categories']:
                    print(f"  ⚠️  {cat['category_name']}: {cat['change_percent']:.1f}% decline")
                    print(f"     Action: {cat['action']}")
    
    return data


# ====================================
# EXAMPLE 3: Category Drill-Down
# ====================================

async def example_3_category_drilldown():
    """
    Drill down into Electronics category on Lazada
    See top brands, products, price changes, out of stock items
    """
    print("\n" + "="*80)
    print("EXAMPLE 3: Category Drill-Down (Lazada - Electronics)")
    print("="*80)
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    url = f"{BASE_URL}/category/electronics"
    params = {
        "platform_code": "lazada",
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()
            print("\n✓ Response Status:", response.status)
            
            print(f"\n{data['category_name']} Performance:")
            metrics = data['category_revenue_metrics']
            print(f"  - Revenue: {metrics['total_revenue']:,.0f} VND")
            print(f"  - Change: {metrics['revenue_change_percent']:.1f}%")
            
            print("\nTop Brands:")
            for i, brand in enumerate(data['top_brands'][:5], 1):
                print(f"  {i}. {brand['brand_name']}")
                print(f"     Revenue: {brand['revenue']:,.0f} VND ({brand['revenue_percent']:.1f}%)")
                print(f"     Products: {brand['product_count']}")
                print(f"     Avg Price: {brand['avg_price']:,.0f} VND")
            
            print("\nTop Products (Available):")
            for i, prod in enumerate(data['top_products'][:5], 1):
                print(f"  {i}. {prod['product_name']} - {prod['brand_name']}")
                print(f"     Current Price: {prod['current_price']:,.0f} VND")
                print(f"     Price Change: {prod['price_change_percent']:.1f}%")
                print(f"     Sales: {prod['sold_count']} units")
                print(f"     Rating: {prod['avg_rating']:.1f}⭐")
            
            if data['out_of_stock_products']:
                print("\nOut of Stock Products:")
                for prod in data['out_of_stock_products'][:5]:
                    print(f"  ⚠️  {prod['product_name']} - {prod['brand_name']}")
                    print(f"     Reason: {prod['out_of_stock_reason']}")
            
            if data['price_changes']:
                print("\nPrice Changes (>5%):")
                for change in data['price_changes'][:5]:
                    print(f"  • {change['product_name']}")
                    print(f"    {change['previous_price']:,.0f} → {change['current_price']:,.0f} VND ({change['price_change_percent']:+.1f}%)")
    
    return data


# ====================================
# EXAMPLE 4: Product Detail
# ====================================

async def example_4_product_detail():
    """
    Get detailed product information including:
    - Price history
    - Availability history
    - Competitor prices
    - Reviews summary
    
    Note: Replace 'prod_123' with actual product ID from your database
    """
    print("\n" + "="*80)
    print("EXAMPLE 4: Product Detail Analysis")
    print("="*80)
    
    # You need to get actual product ID from your database
    global_product_id = "prod_123"  # Replace with actual ID
    
    url = f"{BASE_URL}/product/{global_product_id}"
    params = {
        "platform_code": "lazada",
        "days": 30
    }
    
    print(f"\n📦 Product: {global_product_id}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 404:
                    print("⚠️  Product not found. Update global_product_id with actual product from database.")
                    return None
                
                data = await response.json()
                print("\n✓ Response Status:", response.status)
                
                product = data['product_info']
                print(f"\nProduct Info:")
                print(f"  - Name: {product['product_name']}")
                print(f"  - Brand: {product['brand_name']}")
                print(f"  - Category: {product['category_name']}")
                print(f"  - Platform: {product['platform_name']}")
                print(f"  - Current Price: {product['current_price']:,.0f} VND")
                print(f"  - Price Change: {product['price_change_percent']:+.1f}%")
                print(f"  - Available: {'Yes' if product['is_available'] else 'No'}")
                print(f"  - Rating: {product['avg_rating']:.1f}⭐")
                print(f"  - Units Sold: {product['sold_count']}")
                
                print("\nPrice History (Last 30 days):")
                if data['price_history']:
                    for entry in data['price_history'][-10:]:  # Last 10 entries
                        print(f"  {entry['date']}: {entry['price']:,.0f} VND (discount: {entry['discount_percent']:.1f}%)")
                else:
                    print("  No price history available")
                
                print("\nAvailability History:")
                if data['availability_history']:
                    for entry in data['availability_history'][-10:]:
                        status = "✓ Available" if entry['available'] else "✗ Out of Stock"
                        print(f"  {entry['date']}: {status}")
                else:
                    print("  No availability history available")
                
                print("\nCompetitor Prices (Same Product):")
                if data['competitor_prices']:
                    for comp in data['competitor_prices']:
                        print(f"  - {comp['platform']}: {comp['price']:,.0f} VND (Rating: {comp['rating']:.1f}⭐)")
                else:
                    print("  No competitors found")
                
                print("\nReviews Summary:")
                if data['reviews_summary']:
                    reviews = data['reviews_summary']
                    print(f"  - Avg Rating: {reviews.get('avg_rating', 'N/A')}")
                    print(f"  - Total Reviews: {reviews.get('total_reviews', 'N/A')}")
                    print(f"  - Positive: {reviews.get('positive_reviews', 'N/A')}")
                    print(f"  - Negative: {reviews.get('negative_reviews', 'N/A')}")
                else:
                    print("  No review data available")
                
                return data
    except Exception as e:
        print(f"Error: {str(e)}")
        return None


# ====================================
# EXAMPLE 5: Comparison Analysis
# ====================================

async def example_5_comparison():
    """
    Compare metrics across different dimensions
    """
    print("\n" + "="*80)
    print("EXAMPLE 5: Comparison Analysis")
    print("="*80)
    
    print("\nExample 1: Revenue comparison by Platform")
    url = f"{BASE_URL}/compare"
    params = {
        "metric_type": "revenue",
        "group_by": "platform",
        "start_date": "2024-10-01",
        "end_date": "2024-10-31"
    }
    
    print(f"URL: {url}?{json.dumps(params, indent=2)}")
    print("(Endpoint ready for implementation)")
    
    print("\nExample 2: Product count comparison by Category")
    params = {
        "metric_type": "products",
        "group_by": "category",
        "filters": '{"platform": "lazada"}'
    }
    print(f"URL: {url}?{json.dumps(params, indent=2)}")
    print("(Endpoint ready for implementation)")


# ====================================
# EXAMPLE 6: Real-World Scenario
# ====================================

async def example_6_real_world_scenario():
    """
    Real analyst workflow: Investigate revenue decline
    
    Scenario: Overall revenue down 20% YoY
    Task: Find root cause and identify action items
    """
    print("\n" + "="*80)
    print("EXAMPLE 6: Real-World Scenario - Revenue Investigation")
    print("="*80)
    print("\nScenario: Overall revenue declined 20% last month")
    print("Task: Identify root cause")
    
    # Step 1: Check overall dashboard
    print("\n--- STEP 1: Check Overall Dashboard ---")
    overall = await example_1_overall_dashboard()
    
    # Assume Lazada is the issue
    if overall['top_platforms'][0]['revenue_change_percent'] < -10:
        print(f"\n✓ Found: {overall['top_platforms'][0]['platform_name']} has {overall['top_platforms'][0]['revenue_change_percent']:.1f}% decline")
        
        # Step 2: Drill into platform
        print("\n--- STEP 2: Drill into Lazada ---")
        platform_data = await example_2_platform_drilldown()
        
        # Step 3: Find problematic category
        if platform_data['problematic_categories']:
            category_code = platform_data['problematic_categories'][0]['category_name'].lower()
            print(f"\n✓ Found: {category_code} category has major decline")
            
            # Step 4: Analyze category
            print(f"\n--- STEP 3: Analyze {category_code.upper()} Category ---")
            category_data = await example_3_category_drilldown()
            
            # Step 5: Recommendations
            print("\n--- STEP 4: Recommendations ---")
            if category_data['price_changes']:
                print("⚠️  Action 1: Price Increases Detected")
                print(f"   → {len(category_data['price_changes'])} products with significant price increases")
                print("   → Action: Review pricing strategy, consider promotions")
            
            if category_data['out_of_stock_products']:
                print("\n⚠️  Action 2: Out of Stock Products")
                print(f"   → {len(category_data['out_of_stock_products'])} products out of stock")
                print("   → Action: Urgent inventory replenishment needed")
            
            print("\n⚠️  Action 3: Competitive Analysis")
            print("   → Compare prices with competitors")
            print("   → Consider temporary discount campaign")


# ====================================
# MANUAL API TESTING GUIDE
# ====================================

def print_manual_testing_guide():
    """Print curl commands for manual API testing"""
    print("\n" + "="*80)
    print("MANUAL API TESTING GUIDE")
    print("="*80)
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    print("\n1. Overall Dashboard")
    print(f"""
curl -X GET "http://localhost:8000/api/v1/dss/drilldown/overall" \\
  -H "Content-Type: application/json" \\
  -d '{{"start_date": "{start_date.strftime('%Y-%m-%d')}", "end_date": "{end_date.strftime('%Y-%m-%d')}"}}' | python -m json.tool
""")
    
    print("\n2. Platform Drill-Down (Lazada)")
    print(f"""
curl -X GET "http://localhost:8000/api/v1/dss/drilldown/platform/lazada?start_date={start_date.strftime('%Y-%m-%d')}&end_date={end_date.strftime('%Y-%m-%d')}" | python -m json.tool
""")
    
    print("\n3. Category Drill-Down (Electronics on Lazada)")
    print(f"""
curl -X GET "http://localhost:8000/api/v1/dss/drilldown/category/electronics?platform_code=lazada&start_date={start_date.strftime('%Y-%m-%d')}&end_date={end_date.strftime('%Y-%m-%d')}" | python -m json.tool
""")
    
    print("\n4. Product Detail (Replace prod_123 with actual product ID)")
    print(f"""
curl -X GET "http://localhost:8000/api/v1/dss/drilldown/product/prod_123?platform_code=lazada&days=30" | python -m json.tool
""")


# ====================================
# MAIN EXECUTION
# ====================================

async def main():
    """Run all examples"""
    print("\n" + "█"*80)
    print("DSS DRILL-DOWN ANALYTICS - USAGE EXAMPLES")
    print("█"*80)
    
    try:
        # Example 1
        await example_1_overall_dashboard()
        
        # Example 2
        await example_2_platform_drilldown()
        
        # Example 3
        await example_3_category_drilldown()
        
        # Example 4 (requires actual product ID)
        # await example_4_product_detail()
        
        # Example 5
        await example_5_comparison()
        
        # Example 6: Real-world scenario
        # Uncomment to run full investigation
        # await example_6_real_world_scenario()
        
    except aiohttp.ClientConnectorError:
        print("\n❌ Error: Cannot connect to API server")
        print("Make sure FastAPI server is running on http://localhost:8000")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
    finally:
        # Print manual testing guide
        print_manual_testing_guide()


if __name__ == "__main__":
    print("""
    Instructions:
    1. Make sure FastAPI backend is running: python -m uvicorn app.main:app --reload
    2. Run this script: python backend/app/api/v1/dss_drilldown_examples.py
    3. Or use curl commands from manual testing guide above
    
    Data Requirements:
    - Make sure database has data in dwh_fact_product_daily
    - Platforms: lazada, tiki, fptshop, etc.
    - Categories: electronics, home, fashion, etc.
    """)
    
    asyncio.run(main())
