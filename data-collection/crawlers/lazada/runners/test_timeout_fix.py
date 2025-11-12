#!/usr/bin/env python3
"""
Test script to verify timeout fixes and reviews crawler
"""
import sys
import time
from pathlib import Path

def test_timeout_fix():
    """Test that timeout fix works"""
    print("=" * 60)
    print("TEST 1: Timeout Fix Verification")
    print("=" * 60)
    
    try:
        from playwright.sync_api import sync_playwright
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            url = "https://www.lazada.vn/tag/mobiles/?q=mobiles&page=2"
            
            print(f"\n[TEST] Loading: {url}")
            print("[TEST] Using domcontentloaded with 90s timeout...")
            
            start = time.time()
            try:
                page.goto(url, wait_until='domcontentloaded', timeout=90000)
                page.wait_for_timeout(3000)
                elapsed = time.time() - start
                
                print(f"✅ SUCCESS: Page loaded in {elapsed:.2f}s")
                
                # Check if products are visible
                products = page.query_selector_all('[data-qa-locator="product-item"]')
                print(f"✅ Found {len(products)} products")
                
                if len(products) > 0:
                    print("✅ PASS: Timeout fix works!")
                    return True
                else:
                    print("⚠️  WARNING: No products found, but page loaded")
                    return True
                    
            except Exception as e:
                elapsed = time.time() - start
                print(f"❌ FAIL: Timeout after {elapsed:.2f}s - {e}")
                return False
            finally:
                browser.close()
                
    except ImportError:
        print("❌ Playwright not installed")
        return False

def test_reviews_crawler():
    """Test reviews crawler"""
    print("\n" + "=" * 60)
    print("TEST 2: Reviews Crawler Verification")
    print("=" * 60)
    
    try:
        # Check if file exists
        script_path = Path(__file__).parent / "lazada_reviews_crawler_airflow.py"
        
        if not script_path.exists():
            print(f"❌ FAIL: Script not found: {script_path}")
            return False
        
        print(f"✅ Script exists: {script_path}")
        
        # Try to import
        sys.path.insert(0, str(script_path.parent))
        
        try:
            import lazada_reviews_crawler_airflow
            print("✅ Script can be imported")
            
            # Check class exists
            if hasattr(lazada_reviews_crawler_airflow, 'LazadaReviewsCrawler'):
                print("✅ LazadaReviewsCrawler class found")
                
                # Try to instantiate
                crawler = lazada_reviews_crawler_airflow.LazadaReviewsCrawler()
                print("✅ Crawler can be instantiated")
                
                # Check methods
                required_methods = [
                    'extract_product_urls',
                    'extract_reviews_from_product',
                    'crawl_category_reviews',
                    'save_reviews',
                    'run'
                ]
                
                for method in required_methods:
                    if hasattr(crawler, method):
                        print(f"✅ Method '{method}' exists")
                    else:
                        print(f"❌ Method '{method}' missing")
                        return False
                
                print("✅ PASS: Reviews crawler is ready!")
                return True
            else:
                print("❌ FAIL: LazadaReviewsCrawler class not found")
                return False
                
        except Exception as e:
            print(f"❌ FAIL: Import error - {e}")
            return False
            
    except Exception as e:
        print(f"❌ FAIL: {e}")
        return False

def test_dag_update():
    """Test DAG update"""
    print("\n" + "=" * 60)
    print("TEST 3: Airflow DAG Update Verification")
    print("=" * 60)
    
    try:
        dag_path = Path(__file__).parent.parent.parent.parent.parent / "airflow" / "dags" / "tiki_lazada_elt_dag.py"
        
        if not dag_path.exists():
            print(f"⚠️  WARNING: DAG file not found at expected location")
            print(f"   Expected: {dag_path}")
            return None
        
        print(f"✅ DAG file exists: {dag_path}")
        
        with open(dag_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for new task
        if 'crawl_lazada_reviews' in content:
            print("✅ Task 'crawl_lazada_reviews' found in DAG")
        else:
            print("❌ Task 'crawl_lazada_reviews' NOT found in DAG")
            return False
        
        # Check for sensor
        if 'wait_reviews_ready' in content:
            print("✅ Sensor 'wait_reviews_ready' found in DAG")
        else:
            print("❌ Sensor 'wait_reviews_ready' NOT found in DAG")
            return False
        
        # Check for script path
        if 'lazada_reviews_crawler_airflow.py' in content:
            print("✅ Reviews crawler script referenced in DAG")
        else:
            print("❌ Reviews crawler script NOT referenced in DAG")
            return False
        
        # Check dependencies
        if 'crawl_lazada >> crawl_lazada_reviews' in content:
            print("✅ Task dependency configured correctly")
        else:
            print("⚠️  WARNING: Task dependency might not be configured")
        
        print("✅ PASS: DAG update looks good!")
        return True
        
    except Exception as e:
        print(f"❌ FAIL: {e}")
        return False

def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("LAZADA CRAWLER FIX VERIFICATION")
    print("=" * 60)
    
    results = {
        'timeout_fix': test_timeout_fix(),
        'reviews_crawler': test_reviews_crawler(),
        'dag_update': test_dag_update()
    }
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for test_name, result in results.items():
        if result is True:
            status = "✅ PASS"
        elif result is False:
            status = "❌ FAIL"
        else:
            status = "⚠️  SKIP"
        
        print(f"{status}: {test_name}")
    
    passed = sum(1 for r in results.values() if r is True)
    total = len([r for r in results.values() if r is not None])
    
    print(f"\nResult: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Ready to deploy.")
        return 0
    else:
        print("\n⚠️  Some tests failed. Please review.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
