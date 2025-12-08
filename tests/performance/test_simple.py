"""
Simple test để check backend có chạy không
"""
import requests
import json

BASE_URL = "http://localhost:8000/api/v1"

print("Testing DSS health...")
response = requests.get(f"{BASE_URL}/dss/health")
print(f"Health check: {response.status_code}")
if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))

print("\nTesting DSS price endpoint with minimal params...")
response = requests.post(
    f"{BASE_URL}/dss/price/run",
    json={
        "from_date": "2025-12-08",
        "to_date": "2025-12-08",
        "page": 1,
        "page_size": 5,
        "ai_mode": "skip"  # Skip AI to test basic flow
    }
)

print(f"Status: {response.status_code}")
if response.status_code == 200:
    result = response.json()
    print(f"Success! Session: {result.get('session_id')}, Items: {len(result.get('items', []))}")
else:
    print(f"Error: {response.text[:500]}")
