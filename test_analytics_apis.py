"""
Test script for analytics APIs that were reported as broken:
1. /analytics/overview/trends
2. /analytics/platforms/comparison  
3. /analytics/platforms/category-share
"""

import requests
import json
from datetime import date, timedelta

BASE_URL = "http://localhost:8000/api/v1"

# Authentication - Replace with actual token from /api/v1/auth/signin
# Use analyst@dss.com / analyst123 to get ANALYST role token
AUTH_HEADERS = {
    "Authorization": "Bearer YOUR_ANALYST_TOKEN_HERE"  # Replace with actual token
}

# Test parameters
end_date = date.today()
start_date = end_date - timedelta(days=30)

def test_overview_trends():
    """Test /analytics/overview/trends API"""
    print("\n" + "="*60)
    print("Testing: /api/v1/analytics/overview/trends")
    print("="*60)

    url = f"{BASE_URL}/analytics/overview/trends"
    params = {
        "from_date": str(start_date),
        "to_date": str(end_date),
        "platform_code": "tiki"
    }
    
    try:
        response = requests.get(url, params=params, headers=AUTH_HEADERS)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print(f"[OK] SUCCESS - Points returned: {len(data.get('points', []))}")
            print(f"Date range: {data.get('from_date')} to {data.get('to_date')}")
            print(f"Platform: {data.get('platform_code')}")
        else:
            print(f"[FAIL] FAILED - Response: {response.text}")
    except Exception as e:
        print(f"[ERROR] ERROR: {str(e)}")

def test_platform_comparison():
    """Test /analytics/platforms/comparison API"""
    print("\n" + "="*60)
    print("Testing: /api/v1/analytics/platforms/comparison")
    print("="*60)

    url = f"{BASE_URL}/analytics/platforms/comparison"
    params = {
        "from_date": str(start_date),
        "to_date": str(end_date),
    }
    
    try:
        response = requests.get(url, params=params, headers=AUTH_HEADERS)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            platforms = data.get('platforms', [])
            print(f"[OK] SUCCESS - Platforms returned: {len(platforms)}")
            for p in platforms:
                print(f"  - {p.get('platform_code')}: Revenue={p.get('total_revenue')}, Products={p.get('total_products')}")
        else:
            print(f"[FAIL] FAILED - Response: {response.text}")
    except Exception as e:
        print(f"[ERROR] ERROR: {str(e)}")

def test_category_share():
    """Test /analytics/platforms/category-share API"""
    print("\n" + "="*60)
    print("Testing: /api/v1/analytics/platforms/category-share")
    print("="*60)

    url = f"{BASE_URL}/analytics/platforms/category-share"
    params = {
        "from_date": str(start_date),
        "to_date": str(end_date),
        "platform_code": "tiki"
    }
    
    try:
        response = requests.get(url, params=params, headers=AUTH_HEADERS)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print(f"[OK] SUCCESS - Categories returned: {len(data)}")
            for item in data[:5]:  # Show first 5 categories
                print(f"  - {item.get('category_name')}: {item.get('revenue_share')*100:.1f}% (Revenue: {item.get('revenue')})")
        else:
            print(f"[FAIL] FAILED - Response: {response.text}")
    except Exception as e:
        print(f"[ERROR] ERROR: {str(e)}")

if __name__ == "__main__":
    print("\n[*] Testing Analytics APIs")
    print(f"Date range: {start_date} to {end_date}")
    
    # Note: These APIs require authentication with ANALYST role
    print("\n[!] NOTE: These APIs require authentication with ANALYST role")
    print("    1. Sign in with analyst@dss.com / analyst123 at /api/v1/auth/signin")
    print("    2. Copy the access_token and replace YOUR_ANALYST_TOKEN_HERE")
    print("    3. Run this script again")
    
    test_overview_trends()
    test_platform_comparison()
    test_category_share()
    
    print("\n" + "="*60)
    print("Testing Complete!")
    print("="*60)
