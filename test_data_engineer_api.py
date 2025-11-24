#!/usr/bin/env python3
"""
Quick Test Script for Data Engineer API
Tests all major endpoints
"""

import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8000/api/v1/data-engineer"

def print_test(endpoint, description):
    """Print test header"""
    print(f"\n{'='*70}")
    print(f"TEST: {description}")
    print(f"Endpoint: {endpoint}")
    print('='*70)

def test_endpoint(endpoint, params=None):
    """Test an endpoint and print results"""
    url = BASE_URL + endpoint
    
    try:
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"[OK] Status: {response.status_code} OK")
            
            # Pretty print response
            if isinstance(data, list):
                print(f"[INFO] Returned {len(data)} items")
                if len(data) > 0:
                    print("\n[DATA] Sample (first item):")
                    print(json.dumps(data[0], indent=2, default=str))
                if len(data) > 3:
                    print(f"\n... and {len(data) - 1} more items")
            elif isinstance(data, dict):
                print("\n[DATA] Response:")
                print(json.dumps(data, indent=2, default=str))
            
            return True
        else:
            print(f"[ERROR] Status: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("[ERROR] Connection Error: Backend not running?")
        print("   Start with: docker-compose up -d backend")
        return False
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        return False

def main():
    print("\n" + "="*70)
    print("DATA ENGINEER API - ENDPOINT TESTS")
    print("="*70)
    print(f"Started at: {datetime.now()}")
    
    results = []
    
    # 1. Health Check
    print_test("/health", "API Health Check")
    results.append(("Health Check", test_endpoint("/health")))
    
    # 2. ETL Jobs
    print_test("/etl/jobs", "Get All ETL Jobs Status")
    results.append(("ETL Jobs", test_endpoint("/etl/jobs")))
    
    # 3. ETL Runs
    print_test("/etl/runs/MINIO_ECOMMERCE_DWH_PIPELINE", "Get ETL Run History")
    results.append(("ETL Runs", test_endpoint("/etl/runs/MINIO_ECOMMERCE_DWH_PIPELINE", {"limit": 5})))
    
    # 4. Table Health - All
    print_test("/tables/health", "Get All Table Health Status")
    results.append(("Table Health (All)", test_endpoint("/tables/health")))
    
    # 5. Table Health - DWH Only
    print_test("/tables/health?schema_name=dwh", "Get DWH Table Health")
    results.append(("Table Health (DWH)", test_endpoint("/tables/health", {"schema_name": "dwh"})))
    
    # 6. Table Growth
    print_test("/tables/growth/dwh/fact_product_daily", "Get Table Growth History")
    results.append(("Table Growth", test_endpoint("/tables/growth/dwh/fact_product_daily", {"days": 7})))
    
    # 7. Data Quality Issues
    print_test("/data-quality/issues", "Get Data Quality Issues")
    results.append(("DQ Issues", test_endpoint("/data-quality/issues", {"status": "OPEN"})))
    
    # 8. Data Quality Summary
    print_test("/data-quality/summary", "Get Data Quality Summary")
    results.append(("DQ Summary", test_endpoint("/data-quality/summary")))
    
    # 9. Database Health
    print_test("/database/health", "Get Database Health")
    results.append(("Database Health", test_endpoint("/database/health")))
    
    # 10. Data Lineage
    print_test("/lineage/table/dwh/fact_product_daily", "Get Data Lineage")
    results.append(("Data Lineage", test_endpoint("/lineage/table/dwh/fact_product_daily", {"direction": "both"})))
    
    # 11. Alert Summary
    print_test("/alerts/summary", "Get Alert Summary")
    results.append(("Alert Summary", test_endpoint("/alerts/summary")))
    
    # 12. Alert History
    print_test("/alerts/history", "Get Alert History")
    results.append(("Alert History", test_endpoint("/alerts/history", {"hours": 24})))
    
    # 13. Pipeline Performance
    print_test("/stats/pipeline-performance", "Get Pipeline Performance Stats")
    results.append(("Pipeline Performance", test_endpoint("/stats/pipeline-performance", {"days": 7})))
    
    # 14. Data Volume
    print_test("/stats/data-volume", "Get Data Volume Trends")
    results.append(("Data Volume", test_endpoint("/stats/data-volume", {"days": 30})))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "[OK]" if result else "[FAIL]"
        print(f"{status} {name}")
    
    print(f"\n[INFO] Results: {passed}/{total} tests passed ({passed*100/total:.1f}%)")
    
    if passed == total:
        print("\n[SUCCESS] All tests passed!")
    else:
        print(f"\n[WARN] {total - passed} tests failed")
        print("\nTroubleshooting:")
        print("1. Make sure backend is running: docker-compose ps")
        print("2. Check backend logs: docker logs ecommerce-dss-project-backend-1")
        print("3. Verify database connection: python setup_data_engineer_api.py")
        print("4. Run metrics collector: python backend/scripts/collect_metadata_metrics.py")
    
    print("="*70)
    
    return 0 if passed == total else 1

if __name__ == "__main__":
    import sys
    sys.exit(main())


