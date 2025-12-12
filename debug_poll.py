"""
Debug polling endpoint error
"""
import requests
import json

BASE_URL = "http://localhost:8000/api/v1"

# Test with a known session_id from previous test
session_id = 188  # From previous test run

print(f"🔍 Testing polling endpoint with session_id={session_id}")
print("=" * 60)

try:
    response = requests.get(f"{BASE_URL}/dss/price/{session_id}/ai-summary")
    
    print(f"Status Code: {response.status_code}")
    print(f"Headers: {dict(response.headers)}")
    
    if response.status_code == 200:
        print("✅ Success!")
        data = response.json()
        print(json.dumps(data, indent=2))
    else:
        print(f"❌ Error: {response.status_code}")
        print(f"Response text: {response.text}")
        
except Exception as e:
    print(f"❌ Exception: {e}")
