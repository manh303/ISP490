import requests, time

# Test Reco DSS
print("Testing Reco DSS...")
start = time.perf_counter()
r = requests.post("http://localhost:8000/api/v1/dss/reco/run", json={
    "from_date": "2025-12-08", "to_date": "2025-12-08",
    "platforms": ["tiki"], "categories": ["1"],
    "scope_mode": "by_category", "top_k": 10,
    "min_similarity": 0.5, "ai_mode": "fast"
})
print(f"Reco: {r.status_code} in {time.perf_counter()-start:.2f}s - {len(r.json().get('table_data', []))} items" if r.status_code == 200 else f"ERROR: {r.status_code}")

# Test Sentiment DSS
print("\nTesting Sentiment DSS...")
start = time.perf_counter()
r = requests.post("http://localhost:8000/api/v1/dss/review/run", json={
    "from_date": "2025-12-08", "to_date": "2025-12-08",
    "platforms": ["tiki"], "categories": ["1"],
    "scope_mode": "by_category", "top_n": 10, "ai_mode": "fast"
})
print(f"Sentiment: {r.status_code} in {time.perf_counter()-start:.2f}s" if r.status_code == 200 else f"ERROR: {r.status_code} - {r.text[:100]}")
