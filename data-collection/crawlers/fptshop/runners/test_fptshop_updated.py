#!/usr/bin/env python3
"""
Quick test script for updated FPTShop crawler selectors
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'crawlers'))

from fptshop_crawler import FPTShopCrawler

def test_fptshop_crawler():
    print("Testing updated FPTShop crawler...")

    crawler = FPTShopCrawler()

    try:
        # Test with phone category - limit to 1 page and 5 products for quick test
        print("Testing with dien-thoai category...")
        products = crawler.crawl_category("dien-thoai", max_pages=1, max_products=5)

        if products:
            print(f"✅ Successfully crawled {len(products)} products")

            # Print sample product data
            for i, product in enumerate(products[:2], 1):
                print(f"\n--- Product {i} ---")
                print(f"Name: {product.get('product_name', 'N/A')}")
                print(f"Price: {product.get('price_current', 'N/A')}")
                print(f"Brand: {product.get('brand', 'N/A')}")
                print(f"Category: {product.get('category', 'N/A')}")
                print(f"URL: {product.get('url', 'N/A')}")
                print(f"Images: {len(product.get('image_urls', []))} images")
                print(f"Sold Count: {product.get('sold_count', 'N/A')} (Expected: None)")

            # Save test results
            filename = f"fptshop_test_results_{crawler.source_name}_{int(__import__('time').time())}.json"
            crawler.save_data(products, filename)
            print(f"\n✅ Test results saved to: {filename}")

        else:
            print("❌ No products were crawled - check selectors")

    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
    finally:
        crawler.cleanup()

if __name__ == "__main__":
    test_fptshop_crawler()