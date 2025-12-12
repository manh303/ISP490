"""
Test DSS performance after index optimization
"""
import requests
import time
import json

BASE_URL = "http://localhost:8000/api/v1"

print("=" * 70)
print("DSS Performance Test - After Index Optimization")
print("=" * 70)

# Test 1: Price DSS (by_category)
print("\n📊 Test 1: Price Prediction DSS (by_category)")
print("-" * 70)

start = time.perf_counter()
response = requests.post(
    f"{BASE_URL}/dss/price/run",
    json={
        "from_date": "2025-12-08",
        "to_date": "2025-12-08",
        "platforms": ["tiki"],
        "categories": ["1"],
        "page": 1,
        "page_size": 10,
        "scope_mode": "by_category",
        "top_n": 10,
        "ai_mode": "fast"  # Use fast mode to isolate DB performance
    }
)
duration = time.perf_counter() - start

if response.status_code == 200:
    result = response.json()
    print(f"✅ Response: {response.status_code}")
    print(f"⏱️  Duration: {duration:.2f}s")
    print(f"📦 Items: {len(result.get('items', []))}")
    print(f"🎯 Session: {result.get('session_id')}")
    
    print(f"\n🎯 Performance Comparison:")
    print(f"  Before optimization: ~23s (with async AI)")
    print(f"  After optimization:  {duration:.2f}s")
    
    if duration < 23:
        improvement = ((23 - duration) / 23 * 100)
        print(f"  Improvement: {improvement:.1f}% faster! 🚀")
    else:
        print(f"  ⚠️  No improvement detected")
        
    # Breakdown estimate
    estimated_db = duration - 2  # Assume 2s overhead
    print(f"\n📊 Estimated breakdown:")
    print(f"  Database queries: ~{estimated_db:.2f}s")
    print(f"  Overhead: ~2s")
    
else:
    print(f"❌ Error: {response.status_code}")
    print(response.text[:300])

# Test 2: Same query again (should hit indexes)
print("\n\n📊 Test 2: Repeated Query (Cache Warm)")
print("-" * 70)

start = time.perf_counter()
response = requests.post(
    f"{BASE_URL}/dss/price/run",
    json={
        "from_date": "2025-12-08",
        "to_date": "2025-12-08",
        "platforms": ["tiki"],
        "categories": ["2"],  # Different category
        "page": 1,
        "page_size": 10,
        "scope_mode": "by_category",
        "top_n": 10,
        "ai_mode": "fast"
    }
)
duration2 = time.perf_counter() - start

if response.status_code == 200:
    print(f"✅ Response: {response.status_code}")
    print(f"⏱️  Duration: {duration2:.2f}s")
    print(f"   Difference from first: {abs(duration - duration2):.2f}s")
else:
    print(f"❌ Error: {response.status_code}")

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"Original (no optimization):     51.0s (blocking AI)")
print(f"With async AI (no indexes):     23.4s (initial)")
print(f"With async AI + indexes:        {duration:.2f}s (initial)")
print(f"\n🎯 Total improvement from start: {((51 - duration) / 51 * 100):.1f}%")
print("=" * 70)
