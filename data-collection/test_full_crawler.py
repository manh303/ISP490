#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.dirname(__file__))

from fptshop_full_crawler import FPTShopFullCrawler

def test_quick():
    """Quick test with first few pages"""
    print("=== Quick Test - First 50 Products ===")

    crawler = FPTShopFullCrawler()

    # Get first 50 products
    product_urls = crawler.get_all_product_urls_from_category("dien-thoai", max_pages=3)
    if product_urls:
        product_urls = product_urls[:50]  # Limit to 50 for testing

        print(f"Testing with {len(product_urls)} products...")
        products = crawler.crawl_products_parallel(product_urls, max_workers=3)

        if products:
            import time
            timestamp = int(time.time())
            filename = f"fptshop_quick_test_{timestamp}.json"
            crawler.save_data(products, filename)

            print(f"\n=== TEST RESULTS ===")
            print(f"Products crawled: {len(products)}")
            with_prices = sum(1 for p in products if p.get('price_current'))
            with_images = sum(1 for p in products if p.get('image_urls'))
            print(f"With prices: {with_prices} ({with_prices/len(products)*100:.1f}%)")
            print(f"With images: {with_images} ({with_images/len(products)*100:.1f}%)")

            return products
    else:
        print("No products found!")
        return []

def test_full_category():
    """Test full category crawling"""
    print("=== Full Category Test ===")

    crawler = FPTShopFullCrawler()
    products = crawler.crawl_full_category("dien-thoai", max_workers=3)

    return products

if __name__ == "__main__":
    # Run quick test first
    mode = sys.argv[1] if len(sys.argv) > 1 else "quick"

    if mode == "full":
        test_full_category()
    else:
        test_quick()