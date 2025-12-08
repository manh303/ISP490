"""
DSS API Testing Script
Tests all DSS endpoints and reports on response completeness
"""
import requests
import json
from typing import Dict, List, Any
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000/api/v1"

def test_endpoint(method: str, endpoint: str, data: Dict = None, params: Dict = None) -> Dict[str, Any]:
    """Test an API endpoint and return results"""
    url = f"{BASE_URL}{endpoint}"
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, params=params, timeout=30)
        elif method.upper() == "POST":
            response = requests.post(url, json=data, timeout=30)
        else:
            return {"error": f"Unsupported method: {method}"}
        
        return {
            "status": "success",
            "status_code": response.status_code,
            "response": response.json() if response.status_code == 200 else response.text,
            "headers": dict(response.headers)
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error": "Request timeout"}
    except requests.exceptions.ConnectionError:
        return {"status": "error", "error": "Connection error"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def print_section(title: str):
    """Print a section header"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def print_result(endpoint: str, result: Dict):
    """Print test result"""
    print(f"Endpoint: {endpoint}")
    if result.get("status") == "success":
        print(f"✅ Status Code: {result['status_code']}")
        if result['status_code'] == 200:
            response = result['response']
            # Print pretty JSON with limited depth
            print("Response preview:")
            print(json.dumps(response, indent=2, ensure_ascii=False)[:1000])
            if len(json.dumps(response)) > 1000:
                print("... (truncated)")
        else:
            print(f"Response: {result['response']}")
    else:
        print(f"❌ Error: {result.get('error')}")
    print()


def main():
    """Run all DSS API tests"""
    print("\n🧪 DSS API TESTING SUITE")
    print(f"Started at: {datetime.now().isoformat()}")
    
    results = {}
    
    # ========================================
    # 1. HEALTH & STATUS ENDPOINTS
    # ========================================
    print_section("1. HEALTH & STATUS ENDPOINTS")
    
    # Health check
    result = test_endpoint("GET", "/dss/health")
    print_result("GET /dss/health", result)
    results["health"] = result
    
    # Data status
    result = test_endpoint("GET", "/dss/data/status")
    print_result("GET /dss/data/status", result)
    results["data_status"] = result
    
    # Scenarios
    result = test_endpoint("GET", "/dss/scenarios")
    print_result("GET /dss/scenarios", result)
    results["scenarios"] = result
    
    # ========================================
    # 2. PRICE PREDICTION DSS
    # ========================================
    print_section("2. PRICE PREDICTION DSS")
    
    # Determine dates to use based on data status
    if results["data_status"].get("status") == "success":
        data_status = results["data_status"]["response"]
        latest_fact_date = data_status.get("latest_fact_date")
        if latest_fact_date:
            to_date = latest_fact_date
            from_date = (datetime.fromisoformat(to_date) - timedelta(days=7)).date().isoformat()
        else:
            to_date = "2025-12-05"
            from_date = "2025-11-28"
    else:
        to_date = "2025-12-05"
        from_date = "2025-11-28"
    
    price_request = {
        "from_date": from_date,
        "to_date": to_date,
        "platforms": ["tiki"],
        "categories": ["1", "2"],  # Added required field for by_category mode
        "scope_mode": "by_category",
        "top_n": 10,
        "min_confidence": 0.6,
        "min_price_change_pct": 0.02
    }
    
    result = test_endpoint("POST", "/dss/price/run", data=price_request)
    print_result("POST /dss/price/run", result)
    results["price_prediction"] = result
    
    # ========================================
    # 3. PRODUCT RECOMMENDATION DSS
    # ========================================
    print_section("3. PRODUCT RECOMMENDATION DSS")
    
    # Test by_category mode
    reco_request = {
        "scope_mode": "by_category",
        "from_date": from_date,  # Added required field
        "to_date": to_date,      # Added required field
        "platforms": ["tiki"],
        "categories": ["1", "2"],  # Added categories for better results
        "top_k": 10,
        "min_similarity": 0.5
    }
    
    result = test_endpoint("POST", "/dss/reco/run", data=reco_request)
    print_result("POST /dss/reco/run (by_category)", result)
    results["reco_by_category"] = result
    
    # ========================================
    # 4. REVIEW SENTIMENT DSS
    # ========================================
    print_section("4. REVIEW SENTIMENT DSS")
    
    review_request = {
        "from_date": from_date,
        "to_date": to_date,
        "platforms": ["tiki"],
        "min_reviews_per_product": 5,
        "negative_threshold": 0.3,
        "sentiment_focus": "only_negative"
    }
    
    result = test_endpoint("POST", "/dss/review/run", data=review_request)
    print_result("POST /dss/review/run", result)
    results["review_sentiment"] = result
    
    # ========================================
    # 5. DECISION MANAGEMENT
    # ========================================
    print_section("5. DECISION MANAGEMENT")
    
    # List decisions
    result = test_endpoint("GET", "/dss/decisions", params={"page": 1, "page_size": 5})
    print_result("GET /dss/decisions", result)
    results["list_decisions"] = result
    
    # ========================================
    # 6. SUMMARY
    # ========================================
    print_section("6. TEST SUMMARY")
    
    total_tests = len(results)
    passed = sum(1 for r in results.values() if r.get("status") == "success" and r.get("status_code") == 200)
    failed = total_tests - passed
    
    print(f"Total tests: {total_tests}")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print()
    
    # Check for missing data
    print("COMPLETENESS CHECK:")
    print("-" * 80)
    
    # Check Price Prediction response
    if results["price_prediction"].get("status") == "success" and results["price_prediction"].get("status_code") == 200:
        price_resp = results["price_prediction"]["response"]
        if isinstance(price_resp, dict):
            print("\n📊 Price Prediction DSS:")
            print(f"  - Session ID: {'✅' if price_resp.get('session_id') else '❌ MISSING'}")
            print(f"  - KPIs: {'✅' if price_resp.get('kpis') else '❌ MISSING'}")
            print(f"  - AI Summary: {'✅' if price_resp.get('ai_summary') else '❌ MISSING'}")
            print(f"  - Products: {'✅' if price_resp.get('products') else '❌ MISSING'}")
            
            if price_resp.get('ai_summary'):
                ai_summary = price_resp['ai_summary']
                print(f"    - Insights: {len(ai_summary.get('insights', []))} items")
                print(f"    - Actions: {len(ai_summary.get('recommended_actions', []))} items")
    else:
        print("\n📊 Price Prediction DSS: ❌ Request failed")
    
    # Check Product Recommendation response
    if results["reco_by_category"].get("status") == "success" and results["reco_by_category"].get("status_code") == 200:
        reco_resp = results["reco_by_category"]["response"]
        if isinstance(reco_resp, dict):
            print("\n📊 Product Recommendation DSS:")
            print(f"  - Session ID: {'✅' if reco_resp.get('session_id') else '❌ MISSING'}")
            print(f"  - KPIs: {'✅' if reco_resp.get('kpis') else '❌ MISSING'}")
            print(f"  - AI Summary: {'✅' if reco_resp.get('ai_summary') else '❌ MISSING'}")
            print(f"  - Recommendations: {'✅' if reco_resp.get('recommendations') else '❌ MISSING'}")
            
            if reco_resp.get('ai_summary'):
                ai_summary = reco_resp['ai_summary']
                print(f"    - Insights: {len(ai_summary.get('insights', []))} items")
                print(f"    - Actions: {len(ai_summary.get('recommended_actions', []))} items")
    else:
        print("\n📊 Product Recommendation DSS: ❌ Request failed")
    
    # Check Review Sentiment response
    if results["review_sentiment"].get("status") == "success" and results["review_sentiment"].get("status_code") == 200:
        review_resp = results["review_sentiment"]["response"]
        if isinstance(review_resp, dict):
            print("\n📊 Review Sentiment DSS:")
            print(f"  - Session ID: {'✅' if review_resp.get('session_id') else '❌ MISSING'}")
            print(f"  - KPIs: {'✅' if review_resp.get('kpis') else '❌ MISSING'}")
            print(f"  - AI Summary: {'✅' if review_resp.get('ai_summary') else '❌ MISSING'}")
            print(f"  - Products: {'✅' if review_resp.get('products') else '❌ MISSING'}")
            
            if review_resp.get('ai_summary'):
                ai_summary = review_resp['ai_summary']
                print(f"    - Insights: {len(ai_summary.get('insights', []))} items")
                print(f"    - Actions: {len(ai_summary.get('recommended_actions', []))} items")
    else:
        print("\n📊 Review Sentiment DSS: ❌ Request failed")
    
    print("\n" + "=" * 80)
    print(f"Testing completed at: {datetime.now().isoformat()}")
    print("=" * 80 + "\n")
    
    # Save full results to file
    with open("dss_test_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    print("📝 Full results saved to: dss_test_results.json")


if __name__ == "__main__":
    main()
