#!/usr/bin/env python3
"""Test script to verify seeds.txt integration works correctly"""

import os
import sys
from lazada_crawler import LazadaCrawler

def test_seed_loading():
    """Test if seed URLs are loaded correctly"""
    print("=== TESTING SEED URLS LOADING ===")

    crawler = LazadaCrawler()

    print(f"Number of seed URLs loaded: {len(crawler.seed_urls)}")
    print("\nLoaded seed URLs:")
    for category, url in crawler.seed_urls.items():
        print(f"  {category}: {url[:80]}...")

    return crawler

def test_single_seed_crawl(crawler, category_name="Mobiles"):
    """Test crawling a single seed URL with pagination"""
    print(f"\n=== TESTING SINGLE SEED CRAWL: {category_name} ===")

    if category_name not in crawler.seed_urls:
        print(f"Category {category_name} not found in seed URLs!")
        return

    seed_url = crawler.seed_urls[category_name]
    print(f"Testing URL: {seed_url}")

    # Test getting product URLs from this seed
    try:
        product_urls = crawler.get_product_urls_from_seed(seed_url, max_pages=2)
        print(f"Found {len(product_urls)} product URLs")

        if product_urls:
            print("Sample URLs:")
            for i, url in enumerate(product_urls[:5]):
                print(f"  {i+1}. {url}")
        else:
            print("No product URLs found!")

        return product_urls
    except Exception as e:
        print(f"Error during seed crawl: {e}")
        return []

def main():
    """Main test function"""
    try:
        # Test 1: Load seed URLs
        crawler = test_seed_loading()

        # Test 2: Try crawling one seed URL
        if crawler.seed_urls:
            first_category = list(crawler.seed_urls.keys())[0]
            product_urls = test_single_seed_crawl(crawler, first_category)

            # Test 3: Try extracting one product if URLs found
            if product_urls:
                print(f"\n=== TESTING PRODUCT EXTRACTION ===")
                test_url = product_urls[0]
                print(f"Testing product extraction from: {test_url}")

                driver = crawler.setup_driver(headless=True)
                try:
                    driver.get(test_url)
                    import time
                    time.sleep(3)

                    product_data = crawler.extract_product_data(driver, test_url)
                    if product_data:
                        print("✅ Product extraction SUCCESS")
                        print(f"  Name: {product_data.get('product_name', 'N/A')}")
                        print(f"  Price: {product_data.get('price_current', 'N/A')}")
                        print(f"  Brand: {product_data.get('brand', 'N/A')}")
                    else:
                        print("❌ Product extraction FAILED")

                finally:
                    driver.quit()

    except Exception as e:
        print(f"Test error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if 'crawler' in locals():
            crawler.cleanup()

if __name__ == "__main__":
    main()