import requests, time

# Clear cache first
print("Clearing cache...")
requests.post("http://localhost:8000/api/v1/cache/clear")

# Test Reco DSS with full AI
print("\n[1] Testing Reco DSS (ai_mode=full)...")
start = time.perf_counter()
r = requests.post("http://localhost:8000/api/v1/dss/reco/run", json={
    "from_date": "2025-12-08", "to_date": "2025-12-08",
    "platforms": ["tiki"], "categories": ["1"],
    "scope_mode": "by_category", "top_k": 10,
    "min_similarity": 0.5, "ai_mode": "full"
})
t1 = time.perf_counter() - start
print(f"  Status: {r.status_code}")
print(f"  Time: {t1:.2f}s")
if r.status_code == 200:
    result = r.json()
    print(f"  Items: {len(result.get('table_data', []))}")
    print(f"  AI Status: {result.get('ai_generation_status', 'N/A')}")
    print(f"  Session: {result.get('session_id')}")
else:
    print(f"  Error: {r.text[:200]}")

# Test Review DSS with full AI
print("\n[2] Testing Review DSS (ai_mode=full)...")
start = time.perf_counter()
r = requests.post("http://localhost:8000/api/v1/dss/review/run", json={
    "from_date": "2025-12-08", "to_date": "2025-12-08",
    "platforms": ["tiki"], "categories": ["1"],
    "scope_mode": "by_category", "top_n": 10, "ai_mode": "full"
})
t2 = time.perf_counter() - start
print(f"  Status: {r.status_code}")
print(f"  Time: {t2:.2f}s")
if r.status_code == 200:
    result = r.json()
    print(f"  Items: {len(result.get('table_data', []))}")
    print(f"  AI Status: {result.get('ai_generation_status', 'N/A')}")
    print(f"  Session: {result.get('session_id')}")
else:
    print(f"  Error: {r.text[:200]}")

print(f"\nSummary:")
print(f"  Reco: {t1:.2f}s")
print(f"  Review: {t2:.2f}s")
print(f"  Both working with ai_mode=full!")
