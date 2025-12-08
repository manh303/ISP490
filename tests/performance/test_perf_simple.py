"""
Simple performance test
"""
import requests
import time

BASE_URL = "http://localhost:8000/api/v1"

print("Testing DSS Performance After Index Optimization")
print("=" * 60)

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
        "ai_mode": "fast"
    }
)
duration = time.perf_counter() - start

print(f"Status: {response.status_code}")
print(f"Duration: {duration:.2f}s")

if response.status_code == 200:
    result = response.json()
    print(f"Items: {len(result.get('items', []))}")
    print(f"Session: {result.get('session_id')}")
    
    print("\nPerformance Comparison:")
    print(f"  Original (with blocking AI):  51.0s")
    print(f"  With async AI (no indexes):   23.4s")
    print(f"  With async AI + indexes:      {duration:.2f}s")
    
    total_improvement = ((51 - duration) / 51 * 100)
    index_improvement = ((23.4 - duration) / 23.4 * 100) if duration < 23.4 else 0
    
    print(f"\nTotal improvement: {total_improvement:.1f}%")
    print(f"Index improvement: {index_improvement:.1f}%")
else:
    print(f"Error: {response.text[:200]}")
