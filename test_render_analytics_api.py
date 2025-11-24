#!/usr/bin/env python3
"""
Test Analytics API on Render deployment
"""
import requests
from datetime import date, timedelta

# Render API URL (thay bằng URL thực của bạn)
BASE_URL = "https://ecommerce-dss-backend.onrender.com"  # Thay đổi URL này nếu khác
API_PREFIX = "/api/v1"

def test_health():
    """Test health endpoint"""
    print("\n" + "="*60)
    print("1. TESTING HEALTH ENDPOINT")
    print("="*60)
    
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=10)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_analytics_filters():
    """Test analytics filter endpoints"""
    print("\n" + "="*60)
    print("2. TESTING ANALYTICS FILTER ENDPOINTS")
    print("="*60)
    
    # Test platforms
    try:
        print("\n📍 Testing /analytics/filters/platforms")
        response = requests.get(f"{BASE_URL}{API_PREFIX}/analytics/filters/platforms", timeout=10)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Found {len(data)} platforms")
            for p in data:
                print(f"   - {p}")
        else:
            print(f"❌ Error: {response.text}")
    except Exception as e:
        print(f"❌ Exception: {e}")
    
    # Test categories
    try:
        print("\n📍 Testing /analytics/filters/categories")
        response = requests.get(f"{BASE_URL}{API_PREFIX}/analytics/filters/categories", timeout=10)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Found {len(data)} categories")
            for c in data[:3]:  # Show first 3
                print(f"   - {c}")
        else:
            print(f"❌ Error: {response.text}")
    except Exception as e:
        print(f"❌ Exception: {e}")

def test_overview_trends():
    """Test overview trends endpoint"""
    print("\n" + "="*60)
    print("3. TESTING OVERVIEW TRENDS ENDPOINT")
    print("="*60)
    
    # Use date range that exists in database
    to_date = date(2025, 11, 23)  # Latest date in database
    from_date = date(2025, 11, 16)  # 7 days before
    
    print(f"\n📍 Testing GET /analytics/overview/trends")
    print(f"   Date range: {from_date} to {to_date}")
    
    params = {
        "from_date": str(from_date),
        "to_date": str(to_date)
    }
    
    try:
        response = requests.get(
            f"{BASE_URL}{API_PREFIX}/analytics/overview/trends",
            params=params,
            timeout=30
        )
        
        print(f"\nStatus Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ SUCCESS! API returned data:")
            print(f"   From: {data.get('from_date')}")
            print(f"   To: {data.get('to_date')}")
            print(f"   Points: {len(data.get('points', []))}")
            
            if data.get('points'):
                print(f"\n   Sample data points:")
                for i, point in enumerate(data['points'][:3], 1):
                    print(f"      {i}. Date: {point['date']}")
                    print(f"         Revenue: {point['revenue']:,.0f}")
                    print(f"         Orders: {point['total_orders']:,}")
                    print(f"         Avg Price: {point.get('avg_price')}")
                    print(f"         Avg Rating: {point.get('avg_rating')}")
            else:
                print("\n   ⚠️  No data points returned (empty list)")
        else:
            print(f"\n❌ ERROR Response:")
            try:
                error_data = response.json()
                print(f"   {error_data}")
            except:
                print(f"   {response.text}")
                
    except requests.exceptions.Timeout:
        print(f"❌ Request timeout after 30 seconds")
    except Exception as e:
        print(f"❌ Exception: {e}")
        import traceback
        traceback.print_exc()

def test_overview_kpis():
    """Test overview KPIs endpoint"""
    print("\n" + "="*60)
    print("4. TESTING OVERVIEW KPIS ENDPOINT")
    print("="*60)
    
    to_date = date(2025, 11, 23)
    from_date = date(2025, 11, 16)
    
    print(f"\n📍 Testing GET /analytics/overview/kpis")
    print(f"   Date range: {from_date} to {to_date}")
    
    params = {
        "from_date": str(from_date),
        "to_date": str(to_date)
    }
    
    try:
        response = requests.get(
            f"{BASE_URL}{API_PREFIX}/analytics/overview/kpis",
            params=params,
            timeout=30
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ SUCCESS!")
            print(f"   Total Revenue: {data.get('total_revenue', 0):,.0f}")
            print(f"   Total Orders: {data.get('total_orders', 0):,}")
            print(f"   Avg Price: {data.get('avg_price')}")
            print(f"   Avg Rating: {data.get('avg_rating')}")
        else:
            print(f"❌ Error: {response.text}")
    except Exception as e:
        print(f"❌ Exception: {e}")

def main():
    """Run all tests"""
    print("="*60)
    print("RENDER ANALYTICS API TEST")
    print("="*60)
    print(f"Base URL: {BASE_URL}")
    print(f"API Prefix: {API_PREFIX}")
    
    # Run tests
    if not test_health():
        print("\n⚠️  Health check failed. API might not be running.")
        print("Please check:")
        print("  1. Backend is deployed on Render")
        print("  2. BASE_URL is correct")
        print("  3. Service is not sleeping (free tier)")
        return
    
    test_analytics_filters()
    test_overview_trends()
    test_overview_kpis()
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print("If overview/trends returned data → API works correctly ✅")
    print("If it returned error → Check:")
    print("  1. Environment variables (DB_HOST, DB_PORT, etc.) on Render")
    print("  2. Database connection from Render")
    print("  3. Date range matches data in database")
    print("="*60)

if __name__ == "__main__":
    # Cập nhật URL này với URL thực của bạn
    print("\n⚠️  IMPORTANT: Update BASE_URL with your actual Render URL!")
    print("Current BASE_URL:", BASE_URL)
    
    user_input = input("\nPress Enter to continue or type new URL: ").strip()
    if user_input:
        BASE_URL = user_input.rstrip('/')
        print(f"Using URL: {BASE_URL}")
    
    main()

