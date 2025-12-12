"""
Test product recommendation DSS endpoint
"""
import requests
import json
import time

BASE_URL = "http://localhost:8000/api/v1"

print("=" * 60)
print("Testing Product Recommendation DSS")
print("=" * 60)

# Test 1: By category
print("\n📊 Test 1: Reco DSS (by_category)")
start = time.perf_counter()

response = requests.post(
    f"{BASE_URL}/dss/reco/run",
    json={
        "from_date": "2025-12-08",
        "to_date": "2025-12-08",
        "platforms": ["tiki"],
        "categories": ["1"],
        "scope_mode": "by_category",
        "top_k": 10,
        "min_similarity": 0.5,
        "ai_mode": "fast"
    }
)

duration = time.perf_counter() - start

if response.status_code == 200:
    result = response.json()
    print(f"✅ Status: {response.status_code}")
    print(f"⏱️  Duration: {duration:.2f}s")
    print(f"📦 Recommendations: {len(result.get('table_data', []))}")
    print(f"🎯 Session ID: {result.get('session_id')}")
    print(f"🤖 AI Model: {result.get('ai_model_used')}")
    
    if result.get('table_data'):
        print(f"\nSample recommendation:")
        sample = result['table_data'][0]
        print(f"  {sample.get('source_product_name', 'N/A')[:50]}")
        print(f"  → {sample.get('recommended_product_name', 'N/A')[:50]}")
        print(f"  Similarity: {sample.get('similarity_score', 0):.2f}")
else:
    print(f"❌ Error: {response.status_code}")
    print(response.text[:500])

print("\n" + "=" * 60)
