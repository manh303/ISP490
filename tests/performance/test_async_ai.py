"""
Test async AI generation flow
"""
import requests
import time
import json

BASE_URL = "http://localhost:8000/api/v1"

def test_async_ai_generation():
    print("=" * 80)
    print("Testing Async AI Generation Performance")
    print("=" * 80)
    
    # 1. Call DSS endpoint (should return immediately with rule-based AI)
    print("\n📤 Step 1: Calling /dss/price/run...")
    start = time.perf_counter()
    
    response = requests.post(
        f"{BASE_URL}/dss/price/run",
        json={
            "from_date": "2025-12-08",
            "to_date": "2025-12-08",
            "platforms": ["tiki", "lazada"],
            "categories": ["1", "2"],
            "product_keys": ["string"],
            "page": 1,
            "page_size": 10,  # Reduced for faster test
            "scope_mode": "by_category",
            "top_n": 10,
            "max_discount_pct": 0.15,
            "min_margin_pct": 0.1,
            "min_confidence": 0.7,
            "min_price_change_pct": 0.02,
            "ai_mode": "full"  # Enable async AI generation
        }
    )
    
    initial_duration = time.perf_counter() - start
    print(f"✅ Initial response received in {initial_duration:.2f}s")
    
    if response.status_code != 200:
        print(f"❌ Error: {response.status_code} - {response.text}")
        return
    
    result = response.json()
    session_id = result.get("session_id")
    ai_status = result.get("ai_generation_status")
    
    print(f"\n📊 Response summary:")
    print(f"  - Session ID: {session_id}")
    print(f"  - AI Status: {ai_status}")
    print(f"  - Items: {len(result.get('items', []))}")
    print(f"  - AI Model: {result.get('ai_model_used')}")
    print(f"  - Total response time: {initial_duration:.2f}s")
    
    if ai_status == "skipped":
        print("\n⚠️  AI generation skipped (mode was not 'full')")
        return
    
    if not session_id:
        print("\n⚠️  No session_id in response")
        return
    
    # 2. Poll for AI completion
    if ai_status in ["pending", "generating"]:
        print(f"\n📡 Step 2: Polling /dss/price/{session_id}/ai-summary...")
        poll_count = 0
        max_polls = 20  # 20 polls * 2s = 40s max wait
        
        while poll_count < max_polls:
            poll_count += 1
            time.sleep(2)  # Poll every 2 seconds
            
            poll_response = requests.get(f"{BASE_URL}/dss/price/{session_id}/ai-summary")
            
            if poll_response.status_code != 200:
                print(f"  ❌ Poll failed: {poll_response.status_code}")
                break
            
            poll_data = poll_response.json()
            status = poll_data.get("ai_generation_status")
            
            print(f"  📋 Poll #{poll_count}: status={status}")
            
            if status == "completed":
                duration = poll_data.get("generation_duration_seconds")
                model = poll_data.get("ai_model_used")
                print(f"\n  ✅ AI generation completed!")
                print(f"  - Model: {model}")
                print(f"  - Generation time: {duration:.2f}s" if duration else "  - Generation time: N/A")
                print(f"  - Total time (initial + generation): {initial_duration + (poll_count * 2):.2f}s")
                print(f"\n  🎯 Performance improvement:")
                print(f"  - Old approach: ~51s (blocking)")
                print(f"  - New approach: {initial_duration:.2f}s initial + {duration:.2f}s background" if duration else f"  - New approach: {initial_duration:.2f}s initial")
                
                insights = poll_data.get("ai_summary_insights", [])
                print(f"\n  📝 AI Insights ({len(insights)} items):")
                for i, insight in enumerate(insights[:3], 1):
                    print(f"    {i}. {insight[:100]}...")
                
                break
            
            elif status == "failed":
                error = poll_data.get("ai_generation_error")
                print(f"\n  ❌ AI generation failed: {error}")
                break
        
        if poll_count >= max_polls:
            print(f"\n  ⏱️  Timeout after {max_polls * 2}s")
    
    else:
        print(f"\n✅ AI already {ai_status}")

if __name__ == "__main__":
    test_async_ai_generation()
