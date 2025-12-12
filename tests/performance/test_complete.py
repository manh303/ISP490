"""
Final complete test with all required params
"""
import requests
import json
import time

BASE_URL = "http://localhost:8000/api/v1"

print("=" * 70)
print("Async AI Generation - Complete Test")
print("=" * 70)

print("\n📤 Step 1: Calling /dss/price/run...")
start = time.perf_counter()

response = requests.post(
    f"{BASE_URL}/dss/price/run",
    json={
        "from_date": "2025-12-08",
        "to_date": "2025-12-08",
        "platforms": ["tiki"],  # Required when by_category
        "categories": ["1"],     # Required when by_category  
        "page": 1,
        "page_size": 5,
        "scope_mode": "by_category",
        "top_n": 5,
        "ai_mode": "full"
    }
)

initial_duration = time.perf_counter() - start

if response.status_code != 200:
    print(f"❌ Error {response.status_code}")
    print(response.text[:300])
    exit(1)

result = response.json()
session_id = result.get("session_id")
ai_status = result.get("ai_generation_status")

print(f"✅ Initial response in {initial_duration:.2f}s")
print(f"\n📊 Response:")
print(f"  Session ID: {session_id}")
print(f"  AI Status: {ai_status}")
print(f"  Items: {len(result.get('items', []))}")
print(f"  Model: {result.get('ai_model_used')}")

print(f"\n🎯 PERFORMANCE:")
print(f"  OLD: ~51s (blocking)")
print(f"  NEW: {initial_duration:.2f}s (async)")
print(f"  Improvement: {((51 - initial_duration) / 51 * 100):.1f}%")

if session_id and ai_status in ["pending", "generating"]:
    print(f"\n📡 Step 2: Polling for AI completion...")
    for i in range(20):
        time.sleep(2)
        poll_resp = requests.get(f"{BASE_URL}/dss/price/{session_id}/ai-summary")
        
        if poll_resp.status_code != 200:
            print(f"  Poll #{i+1}: Error {poll_resp.status_code}")
            continue
            
        poll_data = poll_resp.json()
        status = poll_data.get("ai_generation_status")
        print(f"  Poll #{i+1}: {status}")
        
        if status == "completed":
            duration = poll_data.get("generation_duration_seconds")
            model = poll_data.get("ai_model_used")
            print(f"\n  ✅ AI Generation Complete!")
            print(f"  - Model: {model}")
            print(f"  - AI time: {duration:.2f}s" if duration else "  - AI time: N/A")
            print(f"  - User waited: {initial_duration:.2f}s (not {initial_duration + duration:.2f}s)")
            
            insights = poll_data.get("ai_summary_insights", [])
            print(f"\n  📝 AI Insights ({len(insights)} items):")
            for idx, insight in enumerate(insights[:2], 1):
                print(f"    {idx}. {insight[:80]}...")
            break
            
        elif status == "failed":
            error = poll_data.get("ai_generation_error")
            print(f"\n  ❌ AI Failed: {error}")
            break
else:
    print(f"\nAI status: {ai_status} - no polling needed")

print("\n" + "=" * 70)
print("✅ TEST COMPLETE")
print("=" * 70)
