"""
Test async AI with proper ai_mode
"""
import requests
import json
import time

BASE_URL = "http://localhost:8000/api/v1"

print("=" * 60)
print("Testing Async AI Generation (Fixed Version)")
print("=" * 60)

print("\n📤 Calling /dss/price/run with ai_mode='full'...")
start = time.perf_counter()

response = requests.post(
    f"{BASE_URL}/dss/price/run",
    json={
        "from_date": "2025-12-08",
        "to_date": "2025-12-08",
        "page": 1,
        "page_size": 5,  # Small for fast test
        "ai_mode": "full"  # This should trigger async AI
    }
)

initial_duration = time.perf_counter() - start
print(f"✅ Response received in {initial_duration:.2f}s")

if response.status_code != 200:
    print(f"❌ Error: {response.status_code}")
    print(response.text[:500])
else:
    result = response.json()
    session_id = result.get("session_id")
    ai_status = result.get("ai_generation_status")
    
    print(f"\n📊 Result:")
    print(f"  - Session ID: {session_id}")
    print(f"  - AI Status: {ai_status}")
    print(f"  - Items: {len(result.get('items', []))}")
    print(f"  - Response time: {initial_duration:.2f}s")
    
    if session_id and ai_status == "pending":
        print(f"\n📡 Polling for AI completion...")
        for i in range(15):  # Max 30s wait
            time.sleep(2)
            poll_resp = requests.get(f"{BASE_URL}/dss/price/{session_id}/ai-summary")
            if poll_resp.status_code == 200:
                poll_data = poll_resp.json()
                status = poll_data.get("ai_generation_status")
                print(f"  Poll #{i+1}: {status}")
                
                if status == "completed":
                    duration = poll_data.get("generation_duration_seconds")
                    print(f"\n  ✅ AI completed in {duration:.2f}s!")
                    print(f"  Total: {initial_duration:.2f}s initial + {duration:.2f}s AI")
                    break
                elif status == "failed":
                    print(f"  ❌ Failed: {poll_data.get('ai_generation_error')}")
                    break
