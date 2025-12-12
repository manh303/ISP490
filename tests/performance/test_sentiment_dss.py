"""
Test review sentiment DSS endpoint
"""
import requests
import json
import time

BASE_URL = "http://localhost:8000/api/v1"

print("=" * 60)
print("Testing Review Sentiment DSS")
print("=" * 60)

# Test: By category
print("\n📊 Test: Sentiment DSS (by_category)")
start = time.perf_counter()

response = requests.post(
    f"{BASE_URL}/dss/sentiment/run",
    json={
        "from_date": "2025-12-08",
        "to_date": "2025-12-08",
        "platforms": ["tiki"],
        "categories": ["1"],
        "scope_mode": "by_category",
        "top_n": 10,
        "ai_mode": "fast"
    }
)

duration = time.perf_counter() - start

if response.status_code == 200:
    result = response.json()
    print(f"✅ Status: {response.status_code}")
    print(f"⏱️  Duration: {duration:.2f}s")
    print(f"📦 Products analyzed: {len(result.get('table_data', []))}")
    print(f"🎯 Session ID: {result.get('session_id')}")
    print(f"🤖 AI Model: {result.get('ai_model_used')}")
    
    kpis = result.get('kpi_summary', {})
    print(f"\nSentiment Summary:")
    print(f"  Positive: {kpis.get('positive_count', 0)}")
    print(f"  Neutral: {kpis.get('neutral_count', 0)}")
    print(f"  Negative: {kpis.get('negative_count', 0)}")
else:
    print(f"❌ Error: {response.status_code}")
    print(response.text[:500])

print("\n" + "=" * 60)
