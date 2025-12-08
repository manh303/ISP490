"""
Test DSS endpoints with ai_mode=full (async AI)
"""
import requests
import time

BASE_URL = "http://localhost:8000/api/v1"

print("=" * 70)
print("Testing DSS with ai_mode=full (Async AI Pattern)")
print("=" * 70)

# Test 1: Price DSS with full AI
print("\n[1] Price DSS (ai_mode=full)")
print("-" * 70)
start = time.perf_counter()

r = requests.post(f"{BASE_URL}/dss/price/run", json={
    "from_date": "2025-12-08",
    "to_date": "2025-12-08",
    "platforms": ["tiki"],
    "categories": ["1"],
    "scope_mode": "by_category",
    "page": 1,
    "page_size": 10,
    "top_n": 10,
    "ai_mode": "full"  # Full AI with async
})

initial_time = time.perf_counter() - start

if r.status_code == 200:
    result = r.json()
    session_id = result.get("session_id")
    ai_status = result.get("ai_generation_status")
    
    print(f"Initial Response: {r.status_code}")
    print(f"  Time: {initial_time:.2f}s")
    print(f"  Session ID: {session_id}")
    print(f"  AI Status: {ai_status}")
    print(f"  Items: {len(result.get('items', []))}")
    print(f"  AI Model: {result.get('ai_model_used')}")
    
    if ai_status == "pending":
        print(f"\n  Polling for AI completion...")
        for i in range(15):
            time.sleep(2)
            poll = requests.get(f"{BASE_URL}/dss/price/{session_id}/ai-summary")
            if poll.status_code == 200:
                status_data = poll.json()
                status = status_data.get("ai_generation_status")
                print(f"  Poll {i+1}: {status}")
                
                if status == "completed":
                    duration = status_data.get("generation_duration_seconds")
                    model = status_data.get("ai_model_used")
                    print(f"\n  AI Completed!")
                    print(f"    Model: {model}")
                    print(f"    AI Time: {duration:.2f}s" if duration else "")
                    print(f"    User Wait: {initial_time:.2f}s (not {initial_time + (duration or 0):.2f}s)")
                    break
                elif status == "failed":
                    print(f"  AI Failed: {status_data.get('ai_generation_error')}")
                    break
else:
    print(f"ERROR: {r.status_code}")
    print(r.text[:300])

# Test 2: Reco DSS with full AI
print("\n\n[2] Reco DSS (ai_mode=full)")
print("-" * 70)
start = time.perf_counter()

r = requests.post(f"{BASE_URL}/dss/reco/run", json={
    "from_date": "2025-12-08",
    "to_date": "2025-12-08",
    "platforms": ["tiki"],
    "categories": ["1"],
    "scope_mode": "by_category",
    "top_k": 10,
    "min_similarity": 0.5,
    "ai_mode": "full"
})

reco_time = time.perf_counter() - start

if r.status_code == 200:
    result = r.json()
    print(f"Response: {r.status_code} in {reco_time:.2f}s")
    print(f"  Items: {len(result.get('table_data', []))}")
    print(f"  Session: {result.get('session_id')}")
    print(f"  AI Status: {result.get('ai_generation_status', 'N/A')}")
else:
    print(f"ERROR: {r.status_code}")

# Test 3: Review DSS with full AI
print("\n[3] Review DSS (ai_mode=full)")
print("-" * 70)
start = time.perf_counter()

r = requests.post(f"{BASE_URL}/dss/review/run", json={
    "from_date": "2025-12-08",
    "to_date": "2025-12-08",
    "platforms": ["tiki"],
    "categories": ["1"],
    "scope_mode": "by_category",
    "top_n": 10,
    "ai_mode": "full"
})

review_time = time.perf_counter() - start

if r.status_code == 200:
    result = r.json()
    print(f"Response: {r.status_code} in {review_time:.2f}s")
    print(f"  Items: {len(result.get('table_data', []))}")
    print(f"  Session: {result.get('session_id')}")
    print(f"  AI Status: {result.get('ai_generation_status', 'N/A')}")
else:
    print(f"ERROR: {r.status_code}")

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"Price DSS (full AI):  {initial_time:.2f}s initial response")
print(f"Reco DSS (full AI):   {reco_time:.2f}s")
print(f"Review DSS (full AI): {review_time:.2f}s")
print("\nNote: With async AI, users only wait for initial response!")
print("Full AI generates in background and can be polled separately.")
print("=" * 70)
